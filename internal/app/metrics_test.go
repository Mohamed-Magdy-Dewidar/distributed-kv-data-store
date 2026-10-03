package app

import (
	"context"
	"net/http"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/config"
	"distributed-kv-datastore/internal/rpc/pb"
)

// scrapeMetrics GETs addr's /metrics and parses the text format it serves.
func scrapeMetrics(t *testing.T, addr string) map[string]*dto.MetricFamily {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics: status %d", resp.StatusCode)
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		t.Fatalf("parse /metrics: %v", err)
	}
	return families
}

// TestProbeServerServesTheNodesEpochOnMetrics: /metrics answers as soon as
// the probe server is up, with no kv metrics; once the node is set it serves
// the node's epoch, as it is when scraped.
func TestProbeServerServesTheNodesEpochOnMetrics(t *testing.T) {
	addr := knownAddr()
	p := newProbeServer(addr)
	if err := p.start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.stop() })
	waitForProbeUp(t, addr)

	if f, ok := scrapeMetrics(t, addr)["kv_membership_epoch"]; ok {
		t.Fatalf("kv_membership_epoch served before the node is set: %v", f)
	}

	nd := fastNode(t, "node-1", "localhost:1", nil)
	p.setNode(nd)
	epoch := func() float64 {
		f, ok := scrapeMetrics(t, addr)["kv_membership_epoch"]
		if !ok || len(f.Metric) != 1 {
			t.Fatalf("kv_membership_epoch: want one sample once the node is set, got %v", f)
		}
		return f.Metric[0].GetGauge().GetValue()
	}
	if got := epoch(); got != 0 {
		t.Fatalf("kv_membership_epoch = %v, want 0", got)
	}
	if _, err := nd.SetMembership(2, map[string]string{"node-1": "localhost:1"}); err != nil {
		t.Fatalf("SetMembership(2): %v", err)
	}
	if got := epoch(); got != 2 {
		t.Fatalf("kv_membership_epoch = %v after SetMembership(2), want 2", got)
	}
}

// runReady starts Run on a test config, waits until it is ready, and stops
// it when the test ends.
func runReady(t *testing.T) *config.Config {
	t.Helper()
	cfg := testConfig(t, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- Run(ctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(10 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})

	waitForProbeUp(t, cfg.Listen.HTTP)
	eventuallyTrue(t, 5*time.Second, "readyz 200", func() bool { return getReadyz(t, cfg.Listen.HTTP) == http.StatusOK })
	return cfg
}

// kvClient dials cfg's gRPC address; the connection closes when the test ends.
func kvClient(t *testing.T, cfg *config.Config) pb.KVClientClient {
	t.Helper()
	conn, err := grpc.NewClient(cfg.Listen.GRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewKVClientClient(conn)
}

// TestRunTimesClientRequests: Run serves gRPC with the telemetry
// interceptor, so requests to a running node show up on its /metrics under
// their method and status code.
func TestRunTimesClientRequests(t *testing.T) {
	cfg := runReady(t)
	client := kvClient(t, cfg)
	ctx := context.Background()
	if _, err := client.Put(ctx, &pb.PutRequest{Key: "k", Value: "v"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := client.Get(ctx, &pb.GetRequest{Key: ""}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Get with an empty key: %v, want InvalidArgument", err)
	}

	f, ok := scrapeMetrics(t, cfg.Listen.HTTP)["kv_client_request_duration_seconds"]
	if !ok {
		t.Fatal("kv_client_request_duration_seconds missing from /metrics")
	}
	counts := make(map[string]uint64)
	for _, m := range f.Metric {
		labels := make(map[string]string)
		for _, l := range m.Label {
			labels[l.GetName()] = l.GetValue()
		}
		counts[labels["method"]+" "+labels["code"]] = m.GetHistogram().GetSampleCount()
	}
	want := map[string]uint64{"Put OK": 1, "Get InvalidArgument": 1}
	if len(counts) != len(want) || counts["Put OK"] != 1 || counts["Get InvalidArgument"] != 1 {
		t.Fatalf("request counts %v, want %v", counts, want)
	}
}

// TestRunTimesWALSyncs: Run opens the node with the telemetry storage
// observers, so a write's WAL fsync shows up on /metrics under the data
// engine, and nothing under the hint store's.
func TestRunTimesWALSyncs(t *testing.T) {
	cfg := runReady(t)
	if _, err := kvClient(t, cfg).Put(context.Background(), &pb.PutRequest{Key: "k", Value: "v"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	f, ok := scrapeMetrics(t, cfg.Listen.HTTP)["kv_wal_fsync_duration_seconds"]
	if !ok {
		t.Fatal("kv_wal_fsync_duration_seconds missing from /metrics")
	}
	counts := make(map[string]uint64)
	for _, m := range f.Metric {
		counts[m.Label[0].GetValue()] = m.GetHistogram().GetSampleCount()
	}
	if counts["data"] != 1 || counts["hints"] != 0 {
		t.Fatalf("fsyncs by engine %v, want data 1 and hints 0 after one Put", counts)
	}
}
