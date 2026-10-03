package telemetry

import (
	"math"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// fsyncHistograms returns kv_wal_fsync_duration_seconds's histograms by
// engine, checking that engine is their only label.
func fsyncHistograms(t *testing.T, families map[string]*dto.MetricFamily) map[string]*dto.Histogram {
	t.Helper()
	const name = "kv_wal_fsync_duration_seconds"
	f, ok := families[name]
	if !ok {
		t.Fatalf("%s missing from /metrics", name)
	}
	if f.GetType() != dto.MetricType_HISTOGRAM {
		t.Fatalf("%s has type %v, want HISTOGRAM", name, f.GetType())
	}
	out := make(map[string]*dto.Histogram)
	for _, m := range f.Metric {
		if len(m.Label) != 1 || m.Label[0].GetName() != "engine" {
			t.Fatalf("%s: want the engine label only, got %v", name, m.Label)
		}
		out[m.Label[0].GetValue()] = m.GetHistogram()
	}
	return out
}

// TestWALSyncsAreObservedByEngine: each engine's observer records its own
// fsyncs, in seconds, under its own engine label.
func TestWALSyncsAreObservedByEngine(t *testing.T) {
	m := New(noNode)
	data, hints := m.StorageObservers()
	data.WALSynced(2 * time.Millisecond)
	hints.WALSynced(1 * time.Millisecond)
	hints.WALSynced(3 * time.Millisecond)

	got := fsyncHistograms(t, scrape(t, m.Handler()))
	if len(got) != 2 {
		t.Fatalf("engines %v, want data and hints", keys(got))
	}
	for engine, want := range map[string]struct {
		count uint64
		sum   float64
	}{"data": {1, 0.002}, "hints": {2, 0.004}} {
		h := got[engine]
		if h.GetSampleCount() != want.count || math.Abs(h.GetSampleSum()-want.sum) > 1e-9 {
			t.Fatalf("engine %s: count %d sum %v, want count %d sum %v", engine, h.GetSampleCount(), h.GetSampleSum(), want.count, want.sum)
		}
	}

	var bounds []float64
	for _, b := range got["data"].Bucket {
		bounds = append(bounds, b.GetUpperBound())
	}
	// Written out, not taken from fsyncBuckets, so a change to the buckets is
	// a deliberate change to this test too.
	wantBounds := []float64{0.0001, .00025, .0005, .001, .0025, .005, .0075, .01, .015, .02, .025, .05, .1, .25, 1, math.Inf(1)}
	if !slices.Equal(bounds, wantBounds) {
		t.Fatalf("bucket bounds %v, want %v", bounds, wantBounds)
	}
}

// TestWALSyncObserverDoesNotAllocate: reporting an fsync allocates nothing;
// in particular, the engine label is not looked up per call.
func TestWALSyncObserverDoesNotAllocate(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector's instrumentation allocates")
	}
	data, _ := New(noNode).StorageObservers()
	if allocs := testing.AllocsPerRun(1000, func() { data.WALSynced(time.Millisecond) }); allocs != 0 {
		t.Fatalf("WALSynced allocates %v times per call, want 0", allocs)
	}
}

// TestWALSyncObserverKeepsTheChildItWasBuiltWith: the engine label is
// chosen when the observer is built, not looked up per fsync. A lookup
// would re-create a series deleted from the histogram; the child the
// observer holds is detached from it instead.
func TestWALSyncObserverKeepsTheChildItWasBuiltWith(t *testing.T) {
	vec := newFsyncHistogram()
	data := newWALSyncObserver(vec, "data")
	if !vec.DeleteLabelValues("data") {
		t.Fatal("the data series did not exist after building its observer")
	}
	data.WALSynced(time.Millisecond)
	reg := prometheus.NewRegistry()
	reg.MustRegister(vec)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if len(families) != 0 {
		t.Fatalf("an fsync re-created the series after the observer was built; its label was looked up per call: %v", families)
	}
}
