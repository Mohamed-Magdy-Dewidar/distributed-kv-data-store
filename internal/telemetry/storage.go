package telemetry

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"distributed-kv-datastore/internal/storage/engine"
)

// fsyncBuckets are the latency buckets, in seconds, of WAL fsyncs: from
// 0.1ms (a page-cache-backed volume) to 1s (a stalled disk). Provisional:
// revisit once measured on kind.
var fsyncBuckets = []float64{0.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, 1}

// walSyncObserver is one storage engine's engine.Observer. Its histogram
// child, and so its engine label, is chosen when it is built: reporting an
// fsync neither looks up a label nor allocates.
type walSyncObserver struct {
	fsync prometheus.Observer
}

func (o *walSyncObserver) WALSynced(d time.Duration) {
	o.fsync.Observe(d.Seconds())
}

// newWALSyncObserver returns engine's observer, writing to its child of vec.
func newWALSyncObserver(vec *prometheus.HistogramVec, engine string) *walSyncObserver {
	return &walSyncObserver{fsync: vec.WithLabelValues(engine)}
}

func newFsyncHistogram() *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kv_wal_fsync_duration_seconds",
		Help:    "Time to fsync a write to the write-ahead log, by storage engine (data or hints). WAL replay at startup is not included.",
		Buckets: fsyncBuckets,
	}, []string{"engine"})
}

// storageObservers builds the observers of a node's two storage engines,
// its data and its hint store.
func storageObservers(reg prometheus.Registerer) (data, hints *walSyncObserver) {
	vec := newFsyncHistogram()
	reg.MustRegister(vec)
	return newWALSyncObserver(vec, "data"), newWALSyncObserver(vec, "hints")
}

var _ engine.Observer = (*walSyncObserver)(nil)
