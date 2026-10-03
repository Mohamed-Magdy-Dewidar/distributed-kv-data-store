package hints

import (
	"sync/atomic"
	"testing"
	"time"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/storage/engine"
)

type countingObserver struct{ syncs atomic.Int64 }

func (o *countingObserver) WALSynced(time.Duration) { o.syncs.Add(1) }

// TestOpenPassesEngineOptions: the hint store's engine reports to the
// observer it was opened with, for adding a hint and for marking it
// delivered.
func TestOpenPassesEngineOptions(t *testing.T) {
	obs := &countingObserver{}
	s, err := Open(t.TempDir(), 1<<20, engine.WithObserver(obs))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	it := item("v", map[string]uint32{"node-1": 1})
	mustAdd(t, s, "node-2", "k", it)
	if err := s.MarkDelivered("node-2", "k", []*model.DataItem{it}); err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}
	if got := obs.syncs.Load(); got != 2 {
		t.Fatalf("observer told of %d fsyncs for an Add and a MarkDelivered, want 2", got)
	}
}
