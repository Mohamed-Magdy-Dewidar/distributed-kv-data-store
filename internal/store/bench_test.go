package store_test

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"distributed-kv-datastore/internal/benchtest"
	"distributed-kv-datastore/internal/storage/engine"
	"distributed-kv-datastore/internal/store"
)

// The write benchmarks' shape, as in internal/storage/engine's: the
// cluster's memtableBytes and k6's VALUE_SIZE.
const (
	benchMemtableBytes = 4 << 20
	benchValueSize     = 256
)

var benchWriters = []int{1, 8, 64}

// countingObserver counts the data engine's WAL fsyncs.
type countingObserver struct{ syncs atomic.Int64 }

func (o *countingObserver) WALSynced(time.Duration) { o.syncs.Add(1) }

func openBenchStore(b *testing.B) (*store.DataStore, *countingObserver) {
	b.Helper()
	obs := &countingObserver{}
	e, err := engine.Open(b.TempDir(), benchMemtableBytes, engine.WithObserver(obs))
	if err != nil {
		b.Fatalf("engine.Open: %v", err)
	}
	b.Cleanup(func() {
		if err := e.Close(); err != nil {
			b.Errorf("Close: %v", err)
		}
	})
	return store.NewDataStoreWithPersister("node-1", e), obs
}

// BenchmarkDataStorePutParallel is engine's BenchmarkPutParallel one layer
// up, where a node's writes enter: DataStore.Put with no context, as a
// client Put without one does (a read of the key, then the write), from a
// fixed number of writers, each write a new key. It reports the same
// fsyncs/op and p50/p99.
func BenchmarkDataStorePutParallel(b *testing.B) {
	value := strings.Repeat("x", benchValueSize)
	for _, writers := range benchWriters {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			ds, obs := openBenchStore(b)
			benchtest.Run(b, writers, func(i int) error {
				if ds.Put(fmt.Sprintf("key-%d", i), value, nil) == nil {
					return fmt.Errorf("Put key-%d failed", i)
				}
				return nil
			})
			b.ReportMetric(float64(obs.syncs.Load())/float64(b.N), "fsyncs/op")
		})
	}
}

// BenchmarkDataStoreGetWhileWriting measures Gets of existing keys, from 8
// readers, while 8 writers Put other keys without pause: how much a read
// waits behind writes it has nothing to do with. Its ns/op and p50/p99 are
// the Gets'.
func BenchmarkDataStoreGetWhileWriting(b *testing.B) {
	const readers, writers, preloaded = 8, 8, 100
	value := strings.Repeat("x", benchValueSize)

	ds, _ := openBenchStore(b)
	for i := range preloaded {
		if ds.Put(fmt.Sprintf("read-%d", i), value, nil) == nil {
			b.Fatalf("preload read-%d failed", i)
		}
	}

	// Timing starts only once every writer has finished a Put. Otherwise
	// the benchmark's first, small b.N can finish before the writers get
	// going, and the b.N extrapolated from that run takes minutes at the
	// real rate.
	stop := make(chan struct{})
	var wg, started sync.WaitGroup
	var written atomic.Int64
	started.Add(writers)
	for range writers {
		wg.Go(func() {
			signal := sync.OnceFunc(started.Done)
			defer signal() // a writer that fails must not leave the benchmark waiting
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := written.Add(1)
				if ds.Put(fmt.Sprintf("write-%d", i), value, nil) == nil {
					b.Errorf("background Put write-%d failed", i)
					return
				}
				signal()
			}
		})
	}
	started.Wait()
	b.Cleanup(func() { // before the store's Close, registered earlier
		close(stop)
		wg.Wait()
	})

	benchtest.Run(b, readers, func(i int) error {
		key := fmt.Sprintf("read-%d", i%preloaded)
		if _, found, err := ds.Get(key); err != nil || !found {
			return fmt.Errorf("Get %s: found=%v err=%v", key, found, err)
		}
		return nil
	})
}
