package engine

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// countingObserver counts what it is told of: fsyncs, batches and the
// writes in them, and commit waits.
type countingObserver struct {
	syncs, batches, batched, waits atomic.Int64
}

func (o *countingObserver) WALSynced(time.Duration) { o.syncs.Add(1) }

func (o *countingObserver) WALBatch(writes int) {
	o.batches.Add(1)
	o.batched.Add(int64(writes))
}

func (o *countingObserver) WALCommitWait(time.Duration) { o.waits.Add(1) }

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

// TestObserverIsToldOfEachBatchAndEachWritesWait: every batch that reaches
// the WAL reports its size once, and each of its writes its commit wait:
// with group commit, the writes queued during an fsync are one batch;
// without, every write is a batch of one.
func TestObserverIsToldOfEachBatchAndEachWritesWait(t *testing.T) {
	const behind = 8
	for _, m := range writeModes {
		t.Run(m.name, func(t *testing.T) {
			obs := &countingObserver{}
			faults := newWALFaults()
			e := mustOpen(t, t.TempDir(), 1<<20, m.opt, WithObserver(obs), WithWALOpenFile(faults.open))
			defer e.Close()

			release := faults.holdSyncs()
			writes := []<-chan error{putAsync(e, "first", "v")}
			<-faults.syncEntered
			for i := range behind {
				writes = append(writes, putAsync(e, fmt.Sprintf("k%d", i), "v"))
			}
			if m.name == "group" {
				waitFor(t, "the writes to queue", func() bool { return queued(e)() == behind })
			}
			release()
			for i, done := range writes {
				if err := result(t, "Put", done); err != nil {
					t.Fatalf("Put %d: %v", i, err)
				}
			}

			wantBatches := int64(behind + 1) // serial: one per write
			if m.name == "group" {
				wantBatches = 2 // the first write, then everything queued behind it
			}
			if got := obs.batches.Load(); got != wantBatches {
				t.Fatalf("told of %d batches, want %d", got, wantBatches)
			}
			if got := obs.batched.Load(); got != behind+1 {
				t.Fatalf("batches held %d writes, want %d", got, behind+1)
			}
			if got := obs.waits.Load(); got != behind+1 {
				t.Fatalf("told of %d commit waits, want one per write, %d", got, behind+1)
			}
			if got := obs.syncs.Load(); got != wantBatches {
				t.Fatalf("told of %d fsyncs, want one per batch, %d", got, wantBatches)
			}
		})
	}
}

// TestStatsCountDurableWritesAndTheHalt: Writes counts the Puts and
// Restores made durable, not those that failed, and WritesHalted turns
// true when a WAL fsync fails. Replay counts no writes.
func TestStatsCountDurableWritesAndTheHalt(t *testing.T) {
	forEachWriteMode(t, func(t *testing.T, mode Option) {
		dir := t.TempDir()
		faults := newWALFaults()
		e := mustOpen(t, dir, 1<<20, mode, WithWALOpenFile(faults.open))
		if err := e.Put("a", sampleItem("a")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := e.Restore("a", nil); err != nil {
			t.Fatalf("Restore: %v", err)
		}
		if s := e.Stats(); s.Writes != 2 || s.WritesHalted || e.WritesHalted() {
			t.Fatalf("after two writes: Writes=%d WritesHalted=%v/%v, want 2 and false", s.Writes, s.WritesHalted, e.WritesHalted())
		}

		faults.mu.Lock()
		faults.failSyncAt = faults.syncs + 1
		faults.mu.Unlock()
		if err := e.Put("b", sampleItem("b")); err == nil {
			t.Fatal("Put with a failing fsync succeeded")
		}
		if err := e.Put("c", sampleItem("c")); !errors.Is(err, ErrWritesHalted) {
			t.Fatalf("Put after the failure returned %v, want ErrWritesHalted", err)
		}
		if s := e.Stats(); s.Writes != 2 || !s.WritesHalted || !e.WritesHalted() {
			t.Fatalf("after the failure: Writes=%d WritesHalted=%v/%v, want 2 and true", s.Writes, s.WritesHalted, e.WritesHalted())
		}
		if err := e.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		reopened := mustOpen(t, dir, 1<<20, mode)
		defer reopened.Close()
		if s := reopened.Stats(); s.Writes != 0 || s.WritesHalted {
			t.Fatalf("after reopening: Writes=%d WritesHalted=%v, want 0 and false", s.Writes, s.WritesHalted)
		}
	})
}
