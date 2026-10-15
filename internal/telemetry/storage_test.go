package telemetry

import (
	"math"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// engineHistograms returns name's histograms by engine, checking that
// engine is their only label.
func engineHistograms(t *testing.T, families map[string]*dto.MetricFamily, name string) map[string]*dto.Histogram {
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
		if len(m.Label) != 1 || m.Label[0].GetName() != "engine" {
			t.Fatalf("%s: want the engine label only, got %v", name, m.Label)
		}
		out[m.Label[0].GetValue()] = m.GetHistogram()
	}
	return out
}

func bounds(h *dto.Histogram) []float64 {
	var out []float64
	for _, b := range h.Bucket {
		out = append(out, b.GetUpperBound())
	}
	return out
}

// TestStorageObserversReportByEngine: each engine's observer records its
// own fsyncs and commit waits, in seconds, and its batches, in writes,
// each in its own histogram under its own engine label, with the buckets
// below. They are written out, not taken from the bucket variables, so a
// change to the buckets is a deliberate change to this test too.
func TestStorageObserversReportByEngine(t *testing.T) {
	m := New(noNode)
	data, hints := m.StorageObservers()
	data.WALSynced(2 * time.Millisecond)
	hints.WALSynced(1 * time.Millisecond)
	hints.WALSynced(3 * time.Millisecond)
	data.WALBatch(31)
	hints.WALBatch(1)
	hints.WALBatch(4)
	data.WALCommitWait(5 * time.Millisecond)
	data.WALCommitWait(7 * time.Millisecond)
	hints.WALCommitWait(time.Millisecond)

	type want struct {
		count uint64
		sum   float64
	}
	families := scrape(t, m.Handler())
	for _, tc := range []struct {
		name   string
		want   map[string]want
		bounds []float64
	}{
		{"kv_wal_fsync_duration_seconds", map[string]want{"data": {1, 0.002}, "hints": {2, 0.004}},
			[]float64{0.0001, .00025, .0005, .001, .0025, .005, .0075, .01, .015, .02, .025, .05, .1, .25, 1, math.Inf(1)}},
		{"kv_wal_batch_writes", map[string]want{"data": {1, 31}, "hints": {2, 5}},
			[]float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000, math.Inf(1)}},
		{"kv_wal_commit_wait_seconds", map[string]want{"data": {2, 0.012}, "hints": {1, 0.001}},
			[]float64{0.0001, .00025, .0005, .001, .0025, .005, .0075, .01, .015, .02, .025, .05, .1, .25, .5, 1, 2.5, 10, math.Inf(1)}},
	} {
		got := engineHistograms(t, families, tc.name)
		if len(got) != 2 {
			t.Fatalf("%s: engines %v, want data and hints", tc.name, keys(got))
		}
		for engine, w := range tc.want {
			h := got[engine]
			if h.GetSampleCount() != w.count || math.Abs(h.GetSampleSum()-w.sum) > 1e-9 {
				t.Errorf("%s engine %s: count %d sum %v, want count %d sum %v", tc.name, engine, h.GetSampleCount(), h.GetSampleSum(), w.count, w.sum)
			}
		}
		if b := bounds(got["data"]); !slices.Equal(b, tc.bounds) {
			t.Errorf("%s: bucket bounds %v, want %v", tc.name, b, tc.bounds)
		}
	}
}

// TestStorageObserverDoesNotAllocate: reporting an fsync, a batch or a
// commit wait allocates nothing; in particular, the engine label is not
// looked up per call. The engine reports a commit wait for every write.
func TestStorageObserverDoesNotAllocate(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector's instrumentation allocates")
	}
	data, _ := New(noNode).StorageObservers()
	for name, report := range map[string]func(){
		"WALSynced":     func() { data.WALSynced(time.Millisecond) },
		"WALBatch":      func() { data.WALBatch(8) },
		"WALCommitWait": func() { data.WALCommitWait(time.Millisecond) },
	} {
		if allocs := testing.AllocsPerRun(1000, report); allocs != 0 {
			t.Errorf("%s allocates %v times per call, want 0", name, allocs)
		}
	}
}

// TestStorageObserverKeepsTheChildrenItWasBuiltWith: the engine label is
// chosen when the observer is built, not looked up per report. A lookup
// would re-create a series deleted from a histogram; the children the
// observer holds are detached from it instead.
func TestStorageObserverKeepsTheChildrenItWasBuiltWith(t *testing.T) {
	h := newStorageHistograms()
	data := h.observer("data")
	for _, vec := range []*prometheus.HistogramVec{h.fsync, h.batch, h.wait} {
		if !vec.DeleteLabelValues("data") {
			t.Fatal("a data series did not exist after building its observer")
		}
	}
	data.WALSynced(time.Millisecond)
	data.WALBatch(2)
	data.WALCommitWait(time.Millisecond)
	reg := prometheus.NewRegistry()
	reg.MustRegister(h.collectors()...)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if len(families) != 0 {
		t.Fatalf("a report re-created a series after the observer was built; its label was looked up per call: %v", families)
	}
}
