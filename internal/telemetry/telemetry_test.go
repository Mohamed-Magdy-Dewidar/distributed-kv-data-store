package telemetry

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"distributed-kv-datastore/internal/node"
)

// scrape GETs /metrics from h and parses the text format it serves.
func scrape(t *testing.T, h http.Handler) map[string]*dto.MetricFamily {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics: status %d, body %q", rec.Code, rec.Body.String())
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(rec.Body)
	if err != nil {
		t.Fatalf("parse /metrics: %v\n%s", err, rec.Body.String())
	}
	return families
}

// gaugeValue returns the value of the single, unlabeled gauge name.
func gaugeValue(t *testing.T, families map[string]*dto.MetricFamily, name string) float64 {
	t.Helper()
	f, ok := families[name]
	if !ok {
		t.Fatalf("%s missing from /metrics", name)
	}
	if f.GetType() != dto.MetricType_GAUGE {
		t.Fatalf("%s has type %v, want GAUGE", name, f.GetType())
	}
	if len(f.Metric) != 1 || len(f.Metric[0].Label) != 0 {
		t.Fatalf("%s: want one unlabeled sample, got %v", name, f.Metric)
	}
	return f.Metric[0].GetGauge().GetValue()
}

// TestEpochIsReadAtEachScrape: kv_membership_epoch is a gauge holding the
// epoch Stats reports at the time of the scrape.
func TestEpochIsReadAtEachScrape(t *testing.T) {
	var epoch atomic.Uint64
	epoch.Store(4)
	h := New(func() (node.Stats, bool) { return node.Stats{Epoch: epoch.Load()}, true }).Handler()

	if got := gaugeValue(t, scrape(t, h), "kv_membership_epoch"); got != 4 {
		t.Fatalf("kv_membership_epoch = %v, want 4", got)
	}
	epoch.Store(5)
	if got := gaugeValue(t, scrape(t, h), "kv_membership_epoch"); got != 5 {
		t.Fatalf("kv_membership_epoch = %v after the epoch moved to 5, want 5", got)
	}
}

// TestNoKVMetricsWithoutANode: before the node exists, /metrics answers but
// serves no kv metrics; the Go runtime's are served all the same.
func TestNoKVMetricsWithoutANode(t *testing.T) {
	h := New(func() (node.Stats, bool) { return node.Stats{}, false }).Handler()
	families := scrape(t, h)
	if f, ok := families["kv_membership_epoch"]; ok {
		t.Fatalf("kv_membership_epoch served without a node: %v", f)
	}
	if got := gaugeValue(t, families, "go_goroutines"); got < 1 {
		t.Fatalf("go_goroutines = %v, want at least 1", got)
	}
}
