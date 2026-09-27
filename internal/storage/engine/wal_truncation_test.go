package engine

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func walSegments(t *testing.T, dir string) []string {
	t.Helper()
	segs, err := filepath.Glob(filepath.Join(dir, "wal_*.log"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	return segs
}

func mustOpen(t *testing.T, dir string, maxBytes int) *StorageEngine {
	t.Helper()
	e, err := Open(dir, maxBytes)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	return e
}

func assertPresent(t *testing.T, e *StorageEngine, key string, want any) {
	t.Helper()
	items, found, err := e.GetAll(key)
	if err != nil || !found || len(items) != 1 || items[0].Value != want {
		t.Fatalf("expected %q = [%v], got found=%v %v (err %v)", key, want, found, values(items), err)
	}
}

// TestWALReplayStaysBoundedAcrossFlushes: across repeated write-then-restart
// rounds, a restart must replay only what hasn't been flushed yet — not
// every write the engine has ever taken — while every write stays readable.
func TestWALReplayStaysBoundedAcrossFlushes(t *testing.T) {
	dir := t.TempDir()
	const rounds, perRound = 5, 20

	for round := 0; round < rounds; round++ {
		e := mustOpen(t, dir, 200) // a few entries per memtable
		if replayed := e.active.Len(); replayed >= perRound {
			t.Fatalf("round %d: restart replayed %d keys into the memtable; expected only the unflushed tail (< %d)",
				round, replayed, perRound)
		}
		for i := 0; i < perRound; i++ {
			if err := e.Put(fmt.Sprintf("r%d-k%02d", round, i), sampleItem(i)); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
			e.WaitForPendingFlushes() // one flush at a time: let each finish
		}
		if err := e.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
		if segs := walSegments(t, dir); len(segs) > 2 {
			t.Errorf("round %d: expected at most 2 WAL segments left, found %d: %v", round, len(segs), segs)
		}
	}

	e := mustOpen(t, dir, 200)
	defer e.Close()
	for round := 0; round < rounds; round++ {
		for i := 0; i < perRound; i++ {
			// JSON round-trip through the WAL/SSTables turns ints into float64.
			assertPresent(t, e, fmt.Sprintf("r%d-k%02d", round, i), float64(i))
		}
	}
}

// TestFailedFlushKeepsItsWALSegments: a flush that fails must delete no WAL
// segment — its entries only live in memory and the WAL until a later
// flush succeeds. Here every flush fails (the Manifest is closed under the
// engine), and a crash must still recover every write from the WAL.
func TestFailedFlushKeepsItsWALSegments(t *testing.T) {
	dir := t.TempDir()
	e := mustOpen(t, dir, 10) // every Put crosses the threshold
	e.manifest.Close()        // every Manifest.Add — and so every flush — now fails

	for _, key := range []string{"k1", "k2", "k3"} {
		if err := e.Put(key, sampleItem(key)); err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
		e.WaitForPendingFlushes()
	}

	e.wal.Close() // "crash": release the WAL without a clean Close
	reopened := mustOpen(t, dir, 10)
	defer reopened.Close()
	for _, key := range []string{"k1", "k2", "k3"} {
		assertPresent(t, reopened, key, key)
	}
}

// TestWriteMuKeepsAppendAndInsertInTheSameSegment forces the interleaving
// writeMu rules out: Put A appends to segment S, then — before A inserts —
// Put B crosses the threshold, rotates, and its flush deletes S. A's insert
// would then land in the new memtable while its only durable record is
// gone. With writeMu, B waits for A to finish, and both survive a restart.
//
// A's entry is small enough not to cross the threshold on its own (so A
// never triggers a flush that would rescue its write into an SSTable); B's
// is big enough to cross it alone.
func TestWriteMuKeepsAppendAndInsertInTheSameSegment(t *testing.T) {
	dir := t.TempDir()
	e := mustOpen(t, dir, 200)
	big := strings.Repeat("x", 300)

	appended := make(chan struct{})
	release := make(chan struct{})
	testHookAfterWALAppend = func(key string) {
		if key == "a" {
			close(appended)
			<-release
		}
	}
	defer func() { testHookAfterWALAppend = nil }()

	aDone := make(chan error, 1)
	go func() { aDone <- e.Put("a", sampleItem("a")) }()
	<-appended // A's record is in the current segment; A hasn't inserted yet

	bDone := make(chan error, 1)
	go func() { bDone <- e.Put("b", sampleItem(big)) }()
	select {
	case err := <-bDone: // only possible without writeMu
		if err != nil {
			t.Fatalf("Put b failed: %v", err)
		}
		bDone <- nil
	case <-time.After(200 * time.Millisecond): // with writeMu, B is waiting on A
	}
	e.WaitForPendingFlushes() // let B's flush (and segment deletion), if any, finish

	close(release)
	if err := <-aDone; err != nil {
		t.Fatalf("Put a failed: %v", err)
	}
	if err := <-bDone; err != nil {
		t.Fatalf("Put b failed: %v", err)
	}
	e.WaitForPendingFlushes()
	if err := e.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened := mustOpen(t, dir, 200)
	defer reopened.Close()
	assertPresent(t, reopened, "a", "a")
	assertPresent(t, reopened, "b", big)
}
