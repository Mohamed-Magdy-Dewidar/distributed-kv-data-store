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
// write a new key, with flushes running as they would in a node, with
// group commit off and on. Besides ns/op (the inverse of throughput) and
// each Put's p50 and p99, it reports fsyncs/op, how many WAL fsyncs each
// write cost (1 when every write has its own), and writes/fsync, the
// average batch.
//
// fsync is what it measures, so run it where fsync is real: on Linux, in a
// container's own filesystem rather than a Windows bind mount
// (docs/testing.md, "Write benchmarks").
func BenchmarkPutParallel(b *testing.B) {
	value := strings.Repeat("x", benchValueSize)
	for _, mode := range writeModes {
		for _, writers := range benchWriters {
			b.Run(fmt.Sprintf("mode=%s/writers=%d", mode.name, writers), func(b *testing.B) {
				benchmarkPuts(b, mode.opt, writers, value)
			})
		}
	}
}

func benchmarkPuts(b *testing.B, mode Option, writers int, value string) {
	obs := &countingObserver{}
	e, err := Open(b.TempDir(), benchMemtableBytes, mode, WithObserver(obs))
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
	syncs := float64(obs.syncs.Load())
	b.ReportMetric(syncs/float64(b.N), "fsyncs/op")
	b.ReportMetric(float64(b.N)/syncs, "writes/fsync")
}
