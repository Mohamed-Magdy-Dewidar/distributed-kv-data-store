package engine

import "testing"

// assertNoHalfAppliedWrite checks e/dir's post-race state against putErr:
// the write must be visible in memory if and only if it's durably
// recoverable from the WAL after a fresh reopen — any disagreement between
// those two views is exactly a half-applied write, independent of what Put
// itself reported. t.Helper() so a failure blames the caller's line.
func assertNoHalfAppliedWrite(t *testing.T, e *StorageEngine, dir string, key string, putErr error) {
	t.Helper()

	inMemtable, _ := e.active.GetAll(key)

	e2, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer e2.Close()
	afterReopen, foundAfterReopen, err := e2.GetAll(key)
	if err != nil {
		t.Fatalf("GetAll after reopen failed: %v", err)
	}

	if (len(inMemtable) > 0) != foundAfterReopen {
		t.Fatalf("in-memory and reopened views disagree: inMemtable=%v foundAfterReopen=%v (putErr=%v)",
			inMemtable, foundAfterReopen, putErr)
	}

	if putErr == nil {
		if !foundAfterReopen || len(afterReopen) != 1 || afterReopen[0].Value != "v" {
			t.Fatalf("Put reported success but the write did not survive reopen correctly: found=%v items=%v",
				foundAfterReopen, afterReopen)
		}
	} else if foundAfterReopen {
		t.Fatalf("Put reported failure (%v) but the write is visible after reopen: %v", putErr, afterReopen)
	}
}

// TestCloseDuringInFlightPutNeverHalfAppliesAWrite pins down the property
// that makes rpc.Listener.StopWithin's fire-and-forget design safe (see
// internal/rpc/listen.go): an RPC handler still running when shutdown
// proceeds past it can race its local Put against a concurrent Close, but
// can never observe — or leave behind — a half-applied write: durable in
// the WAL but missing from the memtable, or vice versa.
//
// Put is paused, via testHookAfterWALAppend, exactly between its WAL
// append (already durably fsynced) and its memtable insert — the one
// window where the two could disagree. Close runs concurrently from
// another goroutine while Put sits there, then both are allowed to
// proceed at once.
func TestCloseDuringInFlightPutNeverHalfAppliesAWrite(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	hookStarted := make(chan struct{})
	release := make(chan struct{})
	testHookAfterWALAppend = func(string) {
		close(hookStarted)
		<-release
	}
	defer func() { testHookAfterWALAppend = nil }()

	putDone := make(chan error, 1)
	go func() {
		putDone <- e.Put("k", sampleItem("v"))
	}()
	<-hookStarted // WAL append durably completed; memtable insert not yet run

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- e.Close()
	}()

	close(release) // let Put's memtable insert proceed, racing against Close
	putErr := <-putDone
	if closeErr := <-closeDone; closeErr != nil {
		t.Fatalf("Close failed: %v", closeErr)
	}

	assertNoHalfAppliedWrite(t, e, dir, "k", putErr)
}

// TestCloseRacingPutRepeatedlyNeverHalfAppliesAWrite is
// TestCloseDuringInFlightPutNeverHalfAppliesAWrite's unsynchronized
// counterpart: Put and Close fired concurrently with no pause point at
// all, repeated many times. testHookAfterWALAppend only pins down the
// specific WAL-append-then-memtable-insert ordering Put documents today;
// it can't catch a change to that ordering itself, since the pause would
// move with whichever step ends up calling it. Repeating the race
// unsynchronized instead exercises every interleaving directly, including
// ones a swapped step order would produce.
func TestCloseRacingPutRepeatedlyNeverHalfAppliesAWrite(t *testing.T) {
	for i := 0; i < 100; i++ {
		dir := t.TempDir()
		e, err := Open(dir, 1<<20)
		if err != nil {
			t.Fatalf("run %d: Open failed: %v", i, err)
		}

		putDone := make(chan error, 1)
		go func() { putDone <- e.Put("k", sampleItem("v")) }()
		closeDone := make(chan error, 1)
		go func() { closeDone <- e.Close() }()

		putErr := <-putDone
		if closeErr := <-closeDone; closeErr != nil {
			t.Fatalf("run %d: Close failed: %v", i, closeErr)
		}

		assertNoHalfAppliedWrite(t, e, dir, "k", putErr)
	}
}
