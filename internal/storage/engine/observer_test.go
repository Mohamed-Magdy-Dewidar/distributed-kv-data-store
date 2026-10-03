package engine

import (
	"sync/atomic"
	"testing"
	"time"
)

// countingObserver counts the fsyncs it is told of.
type countingObserver struct{ syncs atomic.Int64 }

func (o *countingObserver) WALSynced(time.Duration) { o.syncs.Add(1) }

// TestObserverIsToldOfWALSyncsButNotReplay: every WAL-logged write (Put
// and Restore) reports its fsync; reopening, which replays the WAL, does
// not.
func TestObserverIsToldOfWALSyncsButNotReplay(t *testing.T) {
	dir := t.TempDir()
	obs := &countingObserver{}
	e, err := Open(dir, 1<<20, WithObserver(obs))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := e.Put("k", sampleItem("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := e.Restore("k", nil); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if got := obs.syncs.Load(); got != 2 {
		t.Fatalf("observer told of %d fsyncs for a Put and a Restore, want 2", got)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := &countingObserver{}
	e2, err := Open(dir, 1<<20, WithObserver(reopened))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()
	if got := reopened.syncs.Load(); got != 0 {
		t.Fatalf("replaying the WAL reported %d fsyncs, want 0", got)
	}
	if err := e2.Put("k", sampleItem("v2")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := reopened.syncs.Load(); got != 1 {
		t.Fatalf("observer told of %d fsyncs after one Put on the reopened engine, want 1", got)
	}
}
