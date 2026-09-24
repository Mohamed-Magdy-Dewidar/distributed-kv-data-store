package engine

import (
	"testing"

	"distributed-kv-datastore/internal/store"
	"distributed-kv-datastore/internal/vectorclock"
)

func sampleItem(value any) *store.DataItem {
	vc := vectorclock.New()
	vc.Increment("node-1")
	return &store.DataItem{
		Value:         value,
		VectorClock:   vc,
		LastUpdatedBy: "node-1",
	}
}

func TestPutThenGetFromActiveMemtable(t *testing.T) {
	e, err := Open(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	if err := e.Put("foo", sampleItem("bar")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	item, found, err := e.Get("foo")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found || item.Value != "bar" {
		t.Fatalf("expected foo=bar, got found=%v value=%v", found, item.Value)
	}
}

func TestGetMissingKey(t *testing.T) {
	e, err := Open(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	_, found, err := e.Get("does-not-exist")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if found {
		t.Error("expected not found")
	}
}

func TestFlushMakesDataAvailableFromSSTableAfterMemtableIsCleared(t *testing.T) {
	// Tiny threshold: the first Put alone should cross it and trigger a flush.
	e, err := Open(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	if err := e.Put("foo", sampleItem("bar")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	e.WaitForPendingFlushes()

	e.mu.RLock()
	sstCount := len(e.sstables)
	flushingNil := e.flushing == nil
	e.mu.RUnlock()

	if sstCount != 1 {
		t.Fatalf("expected 1 sstable after flush, got %d", sstCount)
	}
	if !flushingNil {
		t.Error("expected flushing to be nil after flush completes")
	}

	item, found, err := e.Get("foo")
	if err != nil {
		t.Fatalf("Get after flush failed: %v", err)
	}
	if !found || item.Value != "bar" {
		t.Fatalf("expected foo=bar from sstable after flush, got found=%v value=%v", found, item.Value)
	}
}

func TestGetFindsDataInFlushingMemtableDuringFlushWindow(t *testing.T) {
	// This directly tests the property discussed before writing Engine:
	// data must remain visible via the frozen memtable for the whole
	// flush duration, not just before/after it.
	e, err := Open(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	if err := e.Put("foo", sampleItem("bar")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Deliberately do NOT wait for the flush here — Get must still find
	// the value whether it lands in active, flushing, or (once the flush
	// finishes) an sstable. Any of these outcomes is correct; what must
	// never happen is "not found."
	item, found, err := e.Get("foo")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found || item.Value != "bar" {
		t.Fatalf("expected foo=bar to be visible during/around the flush window, got found=%v value=%v", found, item.Value)
	}

	e.WaitForPendingFlushes() // clean up before test ends
}

func TestOverwriteReturnsNewestValueAcrossMemtableAndSSTable(t *testing.T) {
	e, err := Open(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	if err := e.Put("foo", sampleItem("first")); err != nil {
		t.Fatalf("first Put failed: %v", err)
	}
	e.WaitForPendingFlushes() // "first" is now in an sstable

	if err := e.Put("foo", sampleItem("second")); err != nil {
		t.Fatalf("second Put failed: %v", err)
	}
	// "second" is in the (new) active memtable, not yet flushed.

	item, found, err := e.Get("foo")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found || item.Value != "second" {
		t.Fatalf("expected the newest value 'second' (from memtable, checked before sstables), got found=%v value=%v", found, item.Value)
	}
}

func TestReopenAfterCrashRecoversFromWALAndSSTables(t *testing.T) {
	dir := t.TempDir()

	e1, err := Open(dir, 10)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := e1.Put("flushed-key", sampleItem("on-disk")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	e1.WaitForPendingFlushes() // this one reaches an sstable

	if err := e1.Put("unflushed-key", sampleItem("in-wal-only")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	// Simulate a crash: close without waiting for any further flush, and
	// without any clean shutdown beyond what Close already guarantees.
	if err := e1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	e2, err := Open(dir, 10)
	if err != nil {
		t.Fatalf("re-Open failed: %v", err)
	}
	defer e2.Close()

	item1, found1, err := e2.Get("flushed-key")
	if err != nil {
		t.Fatalf("Get(flushed-key) failed: %v", err)
	}
	if !found1 || item1.Value != "on-disk" {
		t.Fatalf("expected flushed-key to survive via sstable, got found=%v value=%v", found1, item1.Value)
	}

	item2, found2, err := e2.Get("unflushed-key")
	if err != nil {
		t.Fatalf("Get(unflushed-key) failed: %v", err)
	}
	if !found2 || item2.Value != "in-wal-only" {
		t.Fatalf("expected unflushed-key to survive via WAL replay, got found=%v value=%v", found2, item2.Value)
	}
}

func TestBloomFilterAvoidsFalseHitsAcrossMultipleSSTables(t *testing.T) {
	e, err := Open(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	// Force several separate flushes, producing several sstables.
	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		if err := e.Put(key, sampleItem("value-"+key)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		e.WaitForPendingFlushes()
	}

	e.mu.RLock()
	sstCount := len(e.sstables)
	e.mu.RUnlock()
	if sstCount < 2 {
		t.Fatalf("expected multiple sstables from repeated small flushes, got %d", sstCount)
	}

	_, found, err := e.Get("definitely-never-written")
	if err != nil {
		t.Fatalf("Get for a genuinely absent key failed: %v", err)
	}
	if found {
		t.Error("expected a genuinely absent key to not be found")
	}

	// Confirm every real key across every sstable is still findable.
	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		item, found, err := e.Get(key)
		if err != nil || !found || item.Value != "value-"+key {
			t.Errorf("key %q: expected value-%s, got found=%v value=%v err=%v", key, key, found, item, err)
		}
	}
}
