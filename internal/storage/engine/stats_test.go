package engine

import (
	"fmt"
	"testing"

	"distributed-kv-datastore/internal/storage/manifest"
)

// breakManifest closes e's manifest, so that registering an SSTable fails,
// and gives e a working one again when the test ends, so Close can close
// the rest.
func breakManifest(t *testing.T, e *StorageEngine, dir string) {
	t.Helper()
	if err := e.manifest.Close(); err != nil {
		t.Fatalf("closing manifest: %v", err)
	}
	t.Cleanup(func() {
		mf, err := manifest.Open(dir)
		if err != nil {
			t.Errorf("reopening manifest: %v", err)
			return
		}
		e.manifest = mf
		if err := e.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
}

// TestStatsFollowFlushes: a write is counted in the memtable's size until
// it is flushed; a successful flush is counted and adds an SSTable.
func TestStatsFollowFlushes(t *testing.T) {
	e, err := Open(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := e.Put("k", sampleItem("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if s := e.Stats(); s.MemtableBytes <= 0 || s.SSTables != 0 || s.Flushes != 0 {
		t.Fatalf("after one unflushed Put: %+v, want MemtableBytes > 0, no SSTables, no flushes", s)
	}
	e.Close()

	e, err = Open(t.TempDir(), 10) // tiny threshold: every Put flushes
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()
	if err := e.Put("k", sampleItem("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	e.WaitForPendingFlushes()
	if s := e.Stats(); s.Flushes != 1 || s.FlushFailures != 0 || s.SSTables != 1 || s.MemtableBytes != 0 {
		t.Fatalf("after one flushed Put: %+v, want 1 flush, 1 SSTable, empty memtable", s)
	}
}

// TestStatsCountAFailedFlush: a flush whose SSTable can't be registered is
// counted as failed, not as a flush, and its data stays in the memtable.
func TestStatsCountAFailedFlush(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, 10)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	breakManifest(t, e, dir)
	if err := e.Put("k", sampleItem("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	e.WaitForPendingFlushes()
	if s := e.Stats(); s.FlushFailures != 1 || s.Flushes != 0 || s.SSTables != 0 || s.MemtableBytes <= 0 {
		t.Fatalf("after a failed flush: %+v, want 1 failure, no flush, no SSTable, data still in memory", s)
	}
}

// putFlushed writes n keys on an engine whose every Put flushes, so it ends
// with n SSTables of about the same size: one compaction tier.
func putFlushed(t *testing.T, e *StorageEngine, n int) {
	t.Helper()
	for i := range n {
		if err := e.Put(fmt.Sprintf("k%d", i), sampleItem("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		e.WaitForPendingFlushes()
	}
}

// TestStatsCountCompactions: a compaction that merges a run is counted; one
// with nothing to compact is not.
func TestStatsCountCompactions(t *testing.T) {
	e, err := Open(t.TempDir(), 10)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer e.Close()
	putFlushed(t, e, 5)
	if s := e.Stats(); s.SSTables != 5 || s.Compactions != 0 {
		t.Fatalf("before compacting: %+v, want 5 SSTables, no compactions", s)
	}
	if err := e.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := e.Compact(); err != nil { // nothing left to compact
		t.Fatalf("second Compact: %v", err)
	}
	if s := e.Stats(); s.Compactions != 1 || s.CompactionFailures != 0 || s.SSTables != 1 {
		t.Fatalf("after one compaction and one with nothing to do: %+v, want 1 compaction, 1 SSTable", s)
	}
}

// TestStatsCountAFailedCompaction: a compaction whose merged SSTable can't
// be registered is counted as failed, not as a compaction.
func TestStatsCountAFailedCompaction(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, 10)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	putFlushed(t, e, 5)
	breakManifest(t, e, dir)
	if err := e.Compact(); err == nil {
		t.Fatal("Compact succeeded with a closed manifest")
	}
	if s := e.Stats(); s.CompactionFailures != 1 || s.Compactions != 0 || s.SSTables != 5 {
		t.Fatalf("after a failed compaction: %+v, want 1 failure, no compaction, 5 SSTables", s)
	}
}
