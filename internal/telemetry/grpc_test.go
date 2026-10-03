package telemetry

import (
	"context"
	"math"
	"slices"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/node"
)

func noNode() (node.Stats, bool) { return node.Stats{}, false }

// histograms returns family name's histograms by "method code", checking
// that it is a histogram with exactly those two labels.
func histograms(t *testing.T, families map[string]*dto.MetricFamily, name string) map[string]*dto.Histogram {
	t.Helper()
	f, ok := families[name]
	if !ok {
		t.Fatalf("%s missing from /metrics", name)
	}
	if f.GetType() != dto.MetricType_HISTOGRAM {
		t.Fatalf("%s has type %v, want HISTOGRAM", name, f.GetType())
	}
	out := make(map[string]*dto.Histogram)
	for _, m := range f.Metric {
		labels := make(map[string]string)
		for _, l := range m.Label {
			labels[l.GetName()] = l.GetValue()
		}
		if len(labels) != 2 || labels["method"] == "" || labels["code"] == "" {
			t.Fatalf("%s: want labels method and code only, got %v", name, labels)
		}
		out[labels["method"]+" "+labels["code"]] = m.GetHistogram()
	}
	return out
}

func call(c *clientRequests, method string, handler grpc.UnaryHandler) {
	c.intercept(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)
}

// TestClientRequestsAreTimedByMethodAndCode: each KVClient call is counted
// under its method and status code, its duration observed in seconds; any
// other service's calls are not.
func TestClientRequestsAreTimedByMethodAndCode(t *testing.T) {
	m := New(noNode)
	c := m.clientRequests

	ok := func(context.Context, any) (any, error) { return nil, nil }
	call(c, "/kvstore.KVClient/Put", ok)
	call(c, "/kvstore.KVClient/Put", func(context.Context, any) (any, error) {
		return nil, status.Error(codes.Unavailable, "write quorum not reached")
	})
	call(c, "/kvstore.KVClient/Get", func(context.Context, any) (any, error) {
		time.Sleep(20 * time.Millisecond)
		return nil, nil
	})
	call(c, "/kvstore.KVReplication/Replicate", ok)

	got := histograms(t, scrape(t, m.Handler()), "kv_client_request_duration_seconds")
	want := map[string]uint64{"Put OK": 1, "Put Unavailable": 1, "Get OK": 1}
	if len(got) != len(want) {
		t.Fatalf("series %v, want exactly %v", keys(got), want)
	}
	for series, count := range want {
		h, found := got[series]
		if !found || h.GetSampleCount() != count {
			t.Fatalf("series %q: got %v, want count %d", series, h, count)
		}
	}
	if sum := got["Get OK"].GetSampleSum(); sum < 0.02 || sum > 5 {
		t.Fatalf("Get took 20ms but its observed sum is %vs", sum)
	}

	var bounds []float64
	for _, b := range got["Put OK"].Bucket {
		bounds = append(bounds, b.GetUpperBound())
	}
	// Written out, not taken from requestBuckets, so a change to the buckets
	// is a deliberate change to this test too.
	wantBounds := []float64{0.0005, .001, .0025, .005, .0075, .01, .015, .02, .025, .03, .04, .05, .1, .25, .5, 1, 2.5, 5, math.Inf(1)}
	if !slices.Equal(bounds, wantBounds) {
		t.Fatalf("bucket bounds %v, want %v", bounds, wantBounds)
	}
}

func keys[V any](m map[string]V) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// TestClientInterceptorDoesNotAllocate: once a method and code have been
// seen, timing another such call allocates nothing.
func TestClientInterceptorDoesNotAllocate(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector's instrumentation allocates")
	}
	c := New(noNode).clientRequests
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{FullMethod: "/kvstore.KVClient/Put"}
	handler := func(context.Context, any) (any, error) { return nil, nil }
	c.intercept(ctx, nil, info, handler) // creates the Put/OK child
	if allocs := testing.AllocsPerRun(1000, func() { c.intercept(ctx, nil, info, handler) }); allocs != 0 {
		t.Fatalf("intercept allocates %v times per call, want 0", allocs)
	}
}
