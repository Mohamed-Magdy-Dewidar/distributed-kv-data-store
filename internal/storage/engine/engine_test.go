package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"distributed-kv-datastore/internal/storage/manifest"
	"distributed-kv-datastore/internal/storage/memtable"
	"distributed-kv-datastore/internal/storage/sstable"
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

func TestOrphanedSSTableNotRegisteredInManifestIsIgnoredOnRestart(t *testing.T) {
	// Simulates the exact crash scenario Manifest exists to handle: an
	// .sst file physically present on disk, but never durably registered
	// as live. A Glob-based engine would incorrectly load it; a
	// Manifest-based one must not.
	dir := t.TempDir()

	e1, err := Open(dir, 1<<20) // large threshold: nothing auto-flushes
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := e1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Manually write a well-formed .sst file directly, bypassing
	// Engine/Manifest entirely — as if a flush's sstable.Write succeeded
	// but the process crashed before manifest.Add ever ran.
	orphan, err := sstable.Write(dir, []memtable.Entry{
		{Key: "orphaned-key", Item: sampleItem("should-not-be-visible")},
	})
	if err != nil {
		t.Fatalf("failed to write orphan sstable: %v", err)
	}
	_ = orphan

	e2, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("re-Open failed: %v", err)
	}
	defer e2.Close()

	_, found, err := e2.Get("orphaned-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if found {
		t.Error("expected the orphaned (never-registered-in-manifest) sstable to be ignored")
	}
}

func TestFailedManifestAddDoesNotStallFutureFlushes(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, 10)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	// Close the manifest's file out from under the engine: the next
	// flush's sstable.Write succeeds, but its manifest.Add fails.
	if err := e.manifest.Close(); err != nil {
		t.Fatalf("closing manifest failed: %v", err)
	}

	if err := e.Put("first", sampleItem("v1")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	e.WaitForPendingFlushes()

	e.mu.RLock()
	flushingNil := e.flushing == nil
	sstCount := len(e.sstables)
	e.mu.RUnlock()
	if !flushingNil {
		t.Fatal("expected flushing to be cleared even though manifest.Add failed")
	}
	if sstCount != 0 {
		t.Fatalf("expected the unregistered sstable not to be published, got %d sstables", sstCount)
	}
	// The .sst file exists on disk, so sstable.Write succeeded: the failure
	// that kept it unpublished was manifest.Add's, not Write's.
	onDisk, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}
	if len(onDisk) != 1 {
		t.Fatalf("expected exactly 1 orphaned .sst file from the failed flush, got %d", len(onDisk))
	}
	if item, found, err := e.Get("first"); err != nil || !found || item.Value != "v1" {
		t.Fatalf("expected first=v1 to stay visible after the failed flush, got found=%v item=%v err=%v", found, item, err)
	}

	// Restore a working manifest. No flush is in flight (we waited above),
	// and the next flush goroutine starts after this write, so no lock is needed.
	mf, err := manifest.Open(dir)
	if err != nil {
		t.Fatalf("reopening manifest failed: %v", err)
	}
	e.manifest = mf

	if err := e.Put("second", sampleItem("v2")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	e.WaitForPendingFlushes()

	e.mu.RLock()
	flushingNil = e.flushing == nil
	sstables := e.sstables
	e.mu.RUnlock()
	if !flushingNil {
		t.Fatal("expected flushing to be nil after the retry flush completes")
	}
	if len(sstables) != 1 {
		t.Fatalf("expected a new flush to complete after the earlier failure, got %d sstables", len(sstables))
	}
	live := mf.LiveIDs()
	if len(live) != 1 || live[0] != sstables[0].ID {
		t.Fatalf("expected manifest to register %s, got %v", sstables[0].ID, live)
	}

	// Both keys (the retried one and the new one) must now come from the sstable.
	for key, want := range map[string]string{"first": "v1", "second": "v2"} {
		item, found, err := sstables[0].Get(key)
		if err != nil || !found || item.Value != want {
			t.Errorf("sstable key %q: expected %s, got found=%v item=%v err=%v", key, want, found, item, err)
		}
	}
}

func itemWithClock(value any, counts map[string]uint32) *store.DataItem {
	return &store.DataItem{
		Value:         value,
		VectorClock:   vectorclock.FromSnapshot(counts),
		LastUpdatedBy: "node-1",
	}
}

func TestCompactMergesFlushedSSTablesAndDropsSupersededVersions(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, 10) // tiny threshold: every Put flushes its own sstable
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	// k0 is written, flushed, then overwritten with a causally newer
	// version and flushed again, so compaction has a real superseded
	// version to drop, not just distinct keys to concatenate.
	puts := []struct {
		key  string
		item *store.DataItem
	}{
		{"k0", itemWithClock("k0-v1", map[string]uint32{"node-1": 1})},
		{"k1", sampleItem("value-k1")},
		{"k0", itemWithClock("k0-v2", map[string]uint32{"node-1": 2})},
		{"k2", sampleItem("value-k2")},
		{"k3", sampleItem("value-k3")},
	}
	for _, p := range puts {
		if err := e.Put(p.key, p.item); err != nil {
			t.Fatalf("Put(%s) failed: %v", p.key, err)
		}
		e.WaitForPendingFlushes()
	}

	e.mu.RLock()
	before := append([]*sstable.SSTable(nil), e.sstables...)
	e.mu.RUnlock()
	if len(before) != len(puts) {
		t.Fatalf("test setup: expected %d sstables before compaction, got %d", len(puts), len(before))
	}

	if err := e.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	e.mu.RLock()
	after := append([]*sstable.SSTable(nil), e.sstables...)
	e.mu.RUnlock()
	if len(after) >= len(before) {
		t.Fatalf("expected fewer sstables after compaction, had %d, now %d", len(before), len(after))
	}
	if len(after) != 1 {
		t.Fatalf("expected all %d same-tier sstables to merge into 1, got %d", len(before), len(after))
	}
	merged := after[0]

	want := map[string]string{"k0": "k0-v2", "k1": "value-k1", "k2": "value-k2", "k3": "value-k3"}
	for key, value := range want {
		item, found, err := e.Get(key)
		if err != nil || !found || item.Value != value {
			t.Errorf("Get(%s) after compaction: expected %s, got found=%v item=%v err=%v", key, value, found, item, err)
		}
	}

	// The stale k0 version must be gone from the merged file itself, not
	// merely shadowed by a newer one.
	versions, found, err := merged.GetAll("k0")
	if err != nil || !found || len(versions) != 1 || versions[0].Value != "k0-v2" {
		t.Fatalf("expected merged sstable to hold only k0-v2, got found=%v versions=%v err=%v", found, versions, err)
	}

	for _, old := range before {
		if _, err := os.Stat(old.Path); !os.IsNotExist(err) {
			t.Errorf("expected old sstable %s to be deleted, stat err=%v", old.Path, err)
		}
	}
	onDisk, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}
	if len(onDisk) != 1 || onDisk[0] != merged.Path {
		t.Errorf("expected only %s on disk, got %v", merged.Path, onDisk)
	}

	live := e.manifest.LiveIDs()
	if len(live) != 1 || live[0] != merged.ID {
		t.Errorf("expected manifest live set to be exactly [%s], got %v", merged.ID, live)
	}

	// Nothing left to compact: a second call is a clean no-op.
	if err := e.Compact(); err != nil {
		t.Fatalf("second Compact failed: %v", err)
	}
	e.mu.RLock()
	stillOne := len(e.sstables) == 1 && e.sstables[0] == merged
	e.mu.RUnlock()
	if !stillOne {
		t.Error("expected a second Compact with nothing to do to leave sstables unchanged")
	}
}

func TestCompactedSSTableKeepsItsPlaceInOrderAcrossRestart(t *testing.T) {
	// Restart orders SSTables by sorting live IDs. If the compacted file
	// got a fresh timestamp ID, it would sort ahead of a newer SSTable that
	// wasn't part of the run, and its stale data would shadow that
	// SSTable's newer data after restart.
	dir := t.TempDir()
	e, err := Open(dir, 10)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for _, p := range []struct{ key, value string }{
		{"shared", "old"}, {"b", "vb"}, {"c", "vc"}, {"d", "vd"},
	} {
		if err := e.Put(p.key, itemWithClock(p.value, map[string]uint32{"node-1": 1})); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		e.WaitForPendingFlushes()
	}
	// Newest sstable is large (a different size tier), so it stays out of
	// the run that gets compacted.
	big := strings.Repeat("x", 5000)
	if err := e.Put("shared", itemWithClock(big, map[string]uint32{"node-1": 2})); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	e.WaitForPendingFlushes()

	if err := e.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	e.mu.RLock()
	var memoryOrder []string
	for _, sst := range e.sstables {
		memoryOrder = append(memoryOrder, sst.ID)
	}
	e.mu.RUnlock()
	if len(memoryOrder) != 2 {
		t.Fatalf("expected [large, compacted] after compaction, got %v", memoryOrder)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	e2, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("re-Open failed: %v", err)
	}
	defer e2.Close()

	var restartOrder []string
	for _, sst := range e2.sstables {
		restartOrder = append(restartOrder, sst.ID)
	}
	if strings.Join(restartOrder, ",") != strings.Join(memoryOrder, ",") {
		t.Fatalf("sstable order changed across restart: in memory %v, after restart %v", memoryOrder, restartOrder)
	}

	// Check the SSTable layer directly: WAL replay puts every key back in
	// the memtable on restart, which would mask an ordering bug in Get.
	for _, sst := range e2.sstables {
		item, found, err := sst.Get("shared")
		if err != nil {
			t.Fatalf("sstable Get failed: %v", err)
		}
		if found {
			if item.Value != big {
				t.Fatalf("expected the newest sstable holding 'shared' to have the newer value, got %v", item.Value)
			}
			break
		}
	}
}

func TestCompactionLoopCompactsInBackground(t *testing.T) {
	e, err := Open(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	for i := 0; i < 4; i++ {
		key := string(rune('a' + i))
		if err := e.Put(key, sampleItem("value-"+key)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		e.WaitForPendingFlushes()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.StartCompactionLoop(ctx, 10*time.Millisecond)

	deadline := time.Now().Add(5 * time.Second)
	for {
		e.mu.RLock()
		n := len(e.sstables)
		e.mu.RUnlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected background loop to compact 4 sstables into 1, still have %d", n)
		}
		time.Sleep(10 * time.Millisecond)
	}

	for i := 0; i < 4; i++ {
		key := string(rune('a' + i))
		item, found, err := e.Get(key)
		if err != nil || !found || item.Value != "value-"+key {
			t.Errorf("Get(%s) after background compaction: got found=%v item=%v err=%v", key, found, item, err)
		}
	}
}

func TestGetDuringCompactionNeverFails(t *testing.T) {
	// Compaction deletes the files it replaced; a Get that snapshotted the
	// old sstable list must still be able to read them.
	e, err := Open(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	keys := []string{"a", "b", "c", "d", "e"}
	for _, key := range keys {
		if err := e.Put(key, sampleItem("value-"+key)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		e.WaitForPendingFlushes()
	}

	stop := make(chan struct{})
	errs := make(chan error, 64)
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, key := range keys {
					item, found, err := e.Get(key)
					if err == nil && (!found || item.Value != "value-"+key) {
						err = fmt.Errorf("Get(%s): found=%v item=%v", key, found, item)
					}
					if err != nil {
						select {
						case errs <- err:
						default:
						}
					}
				}
			}
		}()
	}

	if err := e.Compact(); err != nil {
		t.Errorf("Compact failed: %v", err)
	}
	close(stop)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent Get failed during compaction: %v", err)
	}
}

func TestCompactWaitsForInFlightReadersBeforeDeletingFiles(t *testing.T) {
	// Deterministic version of the race TestGetDuringCompactionNeverFails
	// can only hope to hit: hold filesMu for reading exactly as an
	// in-flight Get does (taken before snapshotting e.sstables), and
	// check that Compact swaps the run out but doesn't delete its files
	// until that reader is done.
	e, err := Open(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer e.Close()

	for _, key := range []string{"a", "b", "c", "d"} {
		if err := e.Put(key, sampleItem("value-"+key)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		e.WaitForPendingFlushes()
	}

	e.filesMu.RLock()
	e.mu.RLock()
	snapshot := e.sstables
	e.mu.RUnlock()

	done := make(chan error, 1)
	go func() { done <- e.Compact() }()

	// Wait for the swap: Compact gets that far without filesMu.
	deadline := time.Now().Add(5 * time.Second)
	for {
		e.mu.RLock()
		n := len(e.sstables)
		e.mu.RUnlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			e.filesMu.RUnlock()
			t.Fatalf("expected Compact to swap in the merged sstable, still have %d", n)
		}
		time.Sleep(5 * time.Millisecond)
	}

	select {
	case err := <-done:
		e.filesMu.RUnlock()
		t.Fatalf("Compact finished (err=%v) while a reader still held the old sstables", err)
	case <-time.After(100 * time.Millisecond):
	}

	// The reader's snapshot is still fully readable.
	for i, key := range []string{"d", "c", "b", "a"} {
		item, found, err := snapshot[i].Get(key)
		if err != nil || !found || item.Value != "value-"+key {
			e.filesMu.RUnlock()
			t.Fatalf("reading old sstable %s mid-compaction: found=%v item=%v err=%v", snapshot[i].Path, found, item, err)
		}
	}

	e.filesMu.RUnlock()
	if err := <-done; err != nil {
		t.Fatalf("Compact failed: %v", err)
	}
	for _, old := range snapshot {
		if _, err := os.Stat(old.Path); !os.IsNotExist(err) {
			t.Errorf("expected %s to be deleted once the reader finished, stat err=%v", old.Path, err)
		}
	}
}
