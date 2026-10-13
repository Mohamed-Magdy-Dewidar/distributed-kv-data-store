// Package benchtest runs a benchmark's operations from a fixed number of
// goroutines and reports their latency, for the storage write benchmarks
// (docs/testing.md).
package benchtest

import (
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Run calls op(i) once for every i in [0, b.N), from n goroutines at once,
// each taking the next i, and reports the ops' p50 and p99 latency in
// milliseconds. b.RunParallel can't do this: it starts GOMAXPROCS×p
// goroutines, so the number of concurrent writers would depend on the
// machine. The first error fails b and stops every goroutine.
func Run(b *testing.B, n int, op func(i int) error) {
	b.Helper()
	latencies := make([]time.Duration, b.N)
	var next atomic.Int64
	var failed atomic.Bool
	var wg sync.WaitGroup

	b.ResetTimer()
	for range n {
		wg.Go(func() {
			for !failed.Load() {
				i := int(next.Add(1) - 1)
				if i >= b.N {
					return
				}
				start := time.Now()
				if err := op(i); err != nil {
					b.Errorf("op %d: %v", i, err)
					failed.Store(true)
					return
				}
				latencies[i] = time.Since(start)
			}
		})
	}
	wg.Wait()
	b.StopTimer()

	slices.Sort(latencies)
	b.ReportMetric(quantile(latencies, 0.50), "p50-ms")
	b.ReportMetric(quantile(latencies, 0.99), "p99-ms")
}

// quantile returns the q-quantile of sorted, in milliseconds (nearest rank).
func quantile(sorted []time.Duration, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q * float64(len(sorted)-1))
	return float64(sorted[i]) / float64(time.Millisecond)
}
