package engine

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/storage/memtable"
	"distributed-kv-datastore/internal/store"
)

// simulateCrashDeath releases e's OS lock on its data dir without going
// through Close, standing in for what process death does at the OS level.
// A test that tears down other engine internals directly (bypassing
// Close) to simulate a crash, then reopens the *same* dir within the same
// test process, must call this too — otherwise the reopen sees the dir as
// still locked, since only a real process exit (or Close) would have
// released it.
func simulateCrashDeath(e *StorageEngine) {
	e.lock.Unlock()
}

// crashImage copies dir's files to a fresh directory, simulating a crash:
// the copy holds exactly what's on disk right now, with no Close, flush or
// other clean shutdown step. The engine using dir must be quiescent (no
// flush in flight). Only one engine ever opens the copy, so the original
// can't interfere with the replay being tested.
//
// The LOCK file is skipped: it holds no data (Open recreates it as needed),
// and the original engine still holds an OS lock on it — which, on
// Windows, blocks even a read from this second handle.
func crashImage(t *testing.T, dir string) string {
	t.Helper()
	dst := t.TempDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("crashImage: read %s: %v", dir, err)
	}
	for _, de := range entries {
		if de.Name() == lockFileName {
			continue
		}
		if !de.Type().IsRegular() {
			t.Fatalf("crashImage: unexpected non-file %s", de.Name())
		}
		src, err := os.Open(filepath.Join(dir, de.Name()))
		if err != nil {
			t.Fatalf("crashImage: %v", err)
		}
		out, err := os.Create(filepath.Join(dst, de.Name()))
		if err != nil {
			src.Close()
			t.Fatalf("crashImage: %v", err)
		}
		_, err = io.Copy(out, src)
		src.Close()
		if cerr := out.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			t.Fatalf("crashImage: copy %s: %v", de.Name(), err)
		}
	}
	return dst
}

// assertSet fails unless key's visible set is exactly want: same values
// and vector clocks, same (newest-first) order.
func assertSet(t *testing.T, e *StorageEngine, key string, want []*model.DataItem) {
	t.Helper()
	got, found, err := e.GetAll(key)
	if err != nil {
		t.Fatalf("GetAll(%q) failed: %v", key, err)
	}
	if found != (len(want) > 0) || len(got) != len(want) {
		t.Fatalf("%q: expected %v, got found=%v %v", key, values(want), found, values(got))
	}
	for i := range want {
		if got[i].Value != want[i].Value || !reflect.DeepEqual(got[i].VectorClock.Snapshot(), want[i].VectorClock.Snapshot()) {
			t.Fatalf("%q[%d]: expected %v %v, got %v %v", key, i,
				want[i].Value, want[i].VectorClock.Snapshot(), got[i].Value, got[i].VectorClock.Snapshot())
		}
	}
}

// TestRestoreSurvivesCrashAndReplay: a rolled-back write must stay rolled
// back after a crash — the restored sibling set comes back exactly, a
// Restore that removed a key keeps it removed, and a Put made after a
// Restore still lands on top of it (replay keeps their order).
func TestRestoreSurvivesCrashAndReplay(t *testing.T) {
	dir := t.TempDir()
	e := mustOpen(t, dir, 1<<20) // nothing flushes: everything lives in the WAL
	defer e.Close()

	// k: two concurrent siblings, then a write superseding both, rolled back.
	prev := []*model.DataItem{
		itemWithClock("b", map[string]uint32{"node-2": 1}),
		itemWithClock("a", map[string]uint32{"node-1": 1}),
	}
	for i := len(prev) - 1; i >= 0; i-- {
		if err := e.Put("k", prev[i]); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	if err := e.Put("k", itemWithClock("undo-me", map[string]uint32{"node-1": 2, "node-2": 1})); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := e.Restore("k", prev); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// fresh: a first write, rolled back to "no versions".
	if err := e.Put("fresh", itemWithClock("undo-me", map[string]uint32{"node-1": 1})); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := e.Restore("fresh", nil); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// j: removed by a Restore, then written again.
	if err := e.Put("j", itemWithClock("undo-me", map[string]uint32{"node-1": 1})); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := e.Restore("j", nil); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}
	after := []*model.DataItem{itemWithClock("after", map[string]uint32{"node-3": 1})}
	if err := e.Put("j", after[0]); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	e2 := mustOpen(t, crashImage(t, dir), 1<<20)
	defer e2.Close()
	assertSet(t, e2, "k", prev)
	assertSet(t, e2, "fresh", nil)
	assertSet(t, e2, "j", after)
	if keys, _ := e2.Keys(); !reflect.DeepEqual(keys, []string{"j", "k"}) {
		t.Fatalf("expected keys [j k] after replay, got %v", keys)
	}
}

// TestIncompleteRestoreOverUnflushedFrozenWriteIsHiddenAfterReplay pins
// down the accepted divergence documented on Restore. The write being
// rolled back is in the frozen memtable, so Restore reports incomplete;
// the flush then fails and puts it back, so it stays visible. It never
// reached an SSTable, so replay puts it and then applies the Restore over
// it in one memtable: after the crash it's hidden. The state is built by
// hand because there's no deterministic way to run Restore mid-flush.
func TestIncompleteRestoreOverUnflushedFrozenWriteIsHiddenAfterReplay(t *testing.T) {
	dir := t.TempDir()
	e := mustOpen(t, dir, 1<<20)
	defer e.Close()

	w := itemWithClock("rolled-back", map[string]uint32{"node-1": 1})
	if err := e.Put("k", w); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	e.mu.Lock() // freeze, as Put's swap does
	e.flushing = e.active
	e.active = memtable.New(e.maxBytes)
	e.mu.Unlock()

	if err := e.Restore("k", nil); !errors.Is(err, store.ErrRestoreIncomplete) {
		t.Fatalf("expected ErrRestoreIncomplete, got %v", err)
	}

	e.mu.Lock() // the flush fails, as flush handles it
	e.active.PutBackOlder(e.flushing.Snapshot())
	e.flushing = nil
	e.mu.Unlock()
	assertSet(t, e, "k", []*model.DataItem{w}) // visible before the crash

	e2 := mustOpen(t, crashImage(t, dir), 1<<20)
	defer e2.Close()
	assertSet(t, e2, "k", nil) // hidden after: the rollback wins
}

// TestWriteMuKeepsRestoreAppendAndReplaceInTheSameSegment is
// TestWriteMuKeepsAppendAndInsertInTheSameSegment for Restore: Restore R
// appends to segment S, then — before R replaces — Put B crosses the
// threshold, rotates, and its flush deletes S. R's replace would then land
// in the new memtable while its only durable record is gone. With writeMu,
// B waits for R to finish, and R's result survives a restart.
//
// R installs a version nothing else holds (as anti-entropy's installs do),
// so losing its WAL record loses the key outright rather than falling back
// to some earlier copy. It also doesn't touch the memtable size, so only B
// can trigger the flush.
func TestWriteMuKeepsRestoreAppendAndReplaceInTheSameSegment(t *testing.T) {
	dir := t.TempDir()
	e := mustOpen(t, dir, 200)
	big := strings.Repeat("x", 300)
	installed := []*model.DataItem{itemWithClock("installed", map[string]uint32{"node-2": 1})}

	appended := make(chan struct{})
	release := make(chan struct{})
	testHookAfterWALAppend = func(key string) {
		if key == "r" {
			close(appended)
			<-release
		}
	}
	defer func() { testHookAfterWALAppend = nil }()

	rDone := make(chan error, 1)
	go func() { rDone <- e.Restore("r", installed) }()
	<-appended // R's record is in the current segment; R hasn't replaced yet

	bDone := make(chan error, 1)
	go func() { bDone <- e.Put("b", sampleItem(big)) }()
	select {
	case err := <-bDone: // only possible without writeMu
		if err != nil {
			t.Fatalf("Put b failed: %v", err)
		}
		bDone <- nil
	case <-time.After(200 * time.Millisecond): // with writeMu, B is waiting on R
	}
	e.WaitForPendingFlushes() // let B's flush (and segment deletion), if any, finish

	close(release)
	if err := <-rDone; err != nil {
		t.Fatalf("Restore r failed: %v", err)
	}
	if err := <-bDone; err != nil {
		t.Fatalf("Put b failed: %v", err)
	}
	e.WaitForPendingFlushes()
	assertSet(t, e, "r", installed) // visible before the crash either way

	e2 := mustOpen(t, crashImage(t, dir), 200)
	defer e2.Close()
	assertSet(t, e2, "r", installed)
	assertPresent(t, e2, "b", big)
	if err := e.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}
