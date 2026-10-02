package app

import (
	"net/http"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
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
