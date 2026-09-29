package engine

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSecondOpenOnSameDirFailsWithErrLocked: a data dir already opened by
// one StorageEngine must reject a second Open immediately — not after
// blocking — with an error wrapping ErrLocked and naming the dir.
func TestSecondOpenOnSameDirFailsWithErrLocked(t *testing.T) {
	dir := t.TempDir()
	e := mustOpen(t, dir, 1<<20)
	defer e.Close()

	_, err := Open(dir, 1<<20)
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("expected errors.Is(err, ErrLocked), got %v", err)
	}
	if !strings.Contains(err.Error(), dir) {
		t.Fatalf("expected the error to mention the locked dir %q, got %q", dir, err.Error())
	}
}

// TestOpenSucceedsAgainAfterClose: Close must release the lock, so a
// following Open on the same dir succeeds rather than inheriting ErrLocked.
func TestOpenSucceedsAgainAfterClose(t *testing.T) {
	dir := t.TempDir()
	e := mustOpen(t, dir, 1<<20)
	if err := e.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	e2, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("expected Open to succeed after Close released the lock, got %v", err)
	}
	if err := e2.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// TestOpenReleasesLockWhenLoadingSSTablesFails: a failure that happens
// after the lock is already acquired — here, loadLiveSSTables failing
// because the Manifest records an SSTable as live whose file is missing
// from disk — must still release the lock. Otherwise one such failure
// would leave the dir permanently locked, since nothing else would ever
// release it.
func TestOpenReleasesLockWhenLoadingSSTablesFails(t *testing.T) {
	dir := t.TempDir()
	e := mustOpen(t, dir, 10) // tiny threshold: the Put below flushes to an sstable
	if err := e.Put("k", sampleItem("v")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	e.WaitForPendingFlushes()
	if err := e.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	ssts, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil || len(ssts) != 1 {
		t.Fatalf("test setup: expected exactly 1 sstable file, got %v (err %v)", ssts, err)
	}
	if err := os.Remove(ssts[0]); err != nil {
		t.Fatalf("failed to remove sstable file: %v", err)
	}

	if _, err := Open(dir, 10); err == nil {
		t.Fatal("expected Open to fail: the Manifest still lists the removed sstable as live")
	}

	// If Open hadn't released the lock on that failure, this second Open
	// would fail with ErrLocked instead of hitting the same missing-file
	// error again.
	_, err = Open(dir, 10)
	if err == nil {
		t.Fatal("expected the second Open to fail the same way (missing sstable)")
	}
	if errors.Is(err, ErrLocked) {
		t.Fatalf("expected the lock to have been released after the first failed Open, got ErrLocked: %v", err)
	}
}

// TestUnlockIsIdempotent: flock's Unlock is documented to short-circuit
// once already unlocked. Pin that down at the engine level: calling it a
// second time after Close must not error.
func TestUnlockIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	e := mustOpen(t, dir, 1<<20)
	if err := e.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := e.lock.Unlock(); err != nil {
		t.Fatalf("second Unlock after Close returned an error: %v", err)
	}
}
