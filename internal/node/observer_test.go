package node

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"distributed-kv-datastore/internal/storetest"
)

type countingObserver struct{ syncs atomic.Int64 }

func (o *countingObserver) WALSynced(time.Duration) { o.syncs.Add(1) }

// TestStorageObserversReportEachEngineSeparately: the node's data engine
// reports to the data observer and its hint store's engine to the hints
// observer, each only for its own writes.
func TestStorageObserversReportEachEngineSeparately(t *testing.T) {
	data, hintsObs := &countingObserver{}, &countingObserver{}
	nd, err := New("node-1", "localhost:1", 1, 1, 1, nil, t.TempDir(), storetest.MemtableBytes,
		WithStorageObservers(data, hintsObs))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer nd.Close()

	if err := nd.Put(context.Background(), "k", "v", nil); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if d, h := data.syncs.Load(), hintsObs.syncs.Load(); d != 1 || h != 0 {
		t.Fatalf("after one Put: data told of %d fsyncs, hints of %d; want 1 and 0", d, h)
	}

	items, _, err := nd.Store.Get("k")
	if err != nil || len(items) != 1 {
		t.Fatalf("Store.Get: %v, %v", items, err)
	}
	if err := nd.hints.Add("node-2", "k", items[0]); err != nil {
		t.Fatalf("hints.Add: %v", err)
	}
	if d, h := data.syncs.Load(), hintsObs.syncs.Load(); d != 1 || h != 1 {
		t.Fatalf("after one hint: data told of %d fsyncs, hints of %d; want 1 and 1", d, h)
	}
}
