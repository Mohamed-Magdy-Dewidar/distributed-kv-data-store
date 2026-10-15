package telemetry

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"distributed-kv-datastore/internal/storage/engine"
)

// fsyncBuckets are the latency buckets, in seconds, of WAL fsyncs: from
// 0.1ms (a page-cache-backed volume) to 1s (a stalled disk), with finer
// steps between 5 and 25ms, where fsyncs fell on the kind cluster (p50 7ms,
// p99 24ms; see docs/observability.md).
var fsyncBuckets = []float64{0.0001, .00025, .0005, .001, .0025, .005, .0075, .01, .015, .02, .025, .05, .1, .25, 1}

// commitWaitBuckets are the buckets, in seconds, of how long a write waits
// for its batch's WAL write to start: from 0.1ms (nothing ahead of it) to
// 10s (writes queued behind a slow or stalled disk). With group commit a
// write waits for about one fsync, so the steps between 1 and 25ms follow
// fsyncBuckets; without it, for every write ahead of it, hundreds of
// milliseconds at the session 2 knee (docs/benchmarks.md).
var commitWaitBuckets = []float64{0.0001, .00025, .0005, .001, .0025, .005, .0075, .01, .015, .02, .025, .05, .1, .25, .5, 1, 2.5, 10}

// batchBuckets are the buckets of writes per WAL batch: powers of two up
// to the engine's cap of 1,000 writes per batch.
var batchBuckets = []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000}

// storageObserver is one storage engine's engine.Observer. Its histogram
// children, and so its engine label, are chosen when it is built:
// reporting neither looks up a label nor allocates.
type storageObserver struct {
	fsync, batch, wait prometheus.Observer
}

func (o *storageObserver) WALSynced(d time.Duration) {
	o.fsync.Observe(d.Seconds())
}

func (o *storageObserver) WALBatch(writes int) {
	o.batch.Observe(float64(writes))
}

func (o *storageObserver) WALCommitWait(d time.Duration) {
	o.wait.Observe(d.Seconds())
}

// storageHistograms are the histograms every storage engine's observer
// writes to, one child per engine.
type storageHistograms struct {
	fsync, batch, wait *prometheus.HistogramVec
}

func newStorageHistograms() storageHistograms {
	return storageHistograms{
		fsync: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kv_wal_fsync_duration_seconds",
			Help:    "Time to fsync a batch of writes to the write-ahead log, by storage engine (data or hints): one write without group commit, every write in the batch with it. WAL replay at startup is not included.",
			Buckets: fsyncBuckets,
		}, []string{"engine"}),
		batch: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kv_wal_batch_writes",
			Help:    "Writes in each batch written to the write-ahead log with one write and one fsync, by storage engine: always 1 without group commit.",
			Buckets: batchBuckets,
		}, []string{"engine"}),
		wait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kv_wal_commit_wait_seconds",
			Help:    "Time each write waited, once encoded, for its batch's write-ahead log write to start, by storage engine: the wait for the batches ahead of it (and for the write lock without group commit). Its own write and fsync follow.",
			Buckets: commitWaitBuckets,
		}, []string{"engine"}),
	}
}

func (h storageHistograms) collectors() []prometheus.Collector {
	return []prometheus.Collector{h.fsync, h.batch, h.wait}
}

// observer returns engine's observer, writing to its children of h.
func (h storageHistograms) observer(engine string) *storageObserver {
	return &storageObserver{
		fsync: h.fsync.WithLabelValues(engine),
		batch: h.batch.WithLabelValues(engine),
		wait:  h.wait.WithLabelValues(engine),
	}
}

// storageObservers builds the observers of a node's two storage engines,
// its data and its hint store.
func storageObservers(reg prometheus.Registerer) (data, hints *storageObserver) {
	h := newStorageHistograms()
	reg.MustRegister(h.collectors()...)
	return h.observer("data"), h.observer("hints")
}

var _ engine.Observer = (*storageObserver)(nil)
