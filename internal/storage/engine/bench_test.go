package engine

import (
	"fmt"
	"strings"
	"testing"

	"distributed-kv-datastore/internal/benchtest"
)

// benchMemtableBytes and benchValueSize are what the cluster runs and what
// k6 writes: deploy/k8s/configmap.yaml's memtableBytes, and
// deploy/bench/script.js's default VALUE_SIZE.
const (
	benchMemtableBytes = 4 << 20
	benchValueSize     = 256
)

// benchWriters are the concurrent writers each write benchmark runs with:
// one, a few, and as many as a node sees near the session 2 knee.
var benchWriters = []int{1, 8, 64}

// BenchmarkPutParallel measures Put from a fixed number of writers, each
// write a new key, with flushes running as they would in a node. Besides
// ns/op (the inverse of throughput) and each Put's p50 and p99, it reports
// fsyncs/op: how many WAL fsyncs each write cost, 1 when every write has
// its own and less when writes share one.
//
// fsync is what it measures, so run it where fsync is real: on Linux, in a
// container's own filesystem rather than a Windows bind mount
// (docs/testing.md, "Write benchmarks").
func BenchmarkPutParallel(b *testing.B) {
	value := strings.Repeat("x", benchValueSize)
	for _, writers := range benchWriters {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			obs := &countingObserver{}
			e, err := Open(b.TempDir(), benchMemtableBytes, WithObserver(obs))
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			b.Cleanup(func() {
				if err := e.Close(); err != nil {
					b.Errorf("Close: %v", err)
				}
			})

			benchtest.Run(b, writers, func(i int) error {
				return e.Put(fmt.Sprintf("key-%d", i), sampleItem(value))
			})
			b.ReportMetric(float64(obs.syncs.Load())/float64(b.N), "fsyncs/op")
		})
	}
}
