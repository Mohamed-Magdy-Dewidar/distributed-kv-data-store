package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/storage/engine"
)

// openSoloNode opens a persistent single-node "cluster" (N=W=R=1, no
// neighbors) at dir, so Node.Put and Node.Get run entirely locally.
func openSoloNode(t *testing.T, dir string, maxMemtableBytes int) *Node {
	t.Helper()
	nd, err := New("node-1", "unused", 1, 1, 1, nil, dir, maxMemtableBytes)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	return nd
}

func sstFiles(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	return files
}

// assertRecovered checks that nd serves key as exactly one version, want,
// with clock wantClock.
func assertRecovered(t *testing.T, nd *Node, key string, want any, wantClock map[string]uint32) {
	t.Helper()
	items, err := nd.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get %q after reopening failed: %v", key, err)
	}
	if len(items) != 1 || items[0].Value != want {
		t.Fatalf("expected %q to recover as [%v], got %v", key, want, itemValues(items))
	}
	if got := items[0].VectorClock.Snapshot(); !reflect.DeepEqual(got, wantClock) {
		t.Fatalf("expected %q's clock to recover as %v, got %v", key, wantClock, got)
	}
}

// TestPersistentNodeRecoversFromWAL: writes still in the memtable when the
// node closes live only in the WAL, and a node reopened on the same
// directory must replay them.
func TestPersistentNodeRecoversFromWAL(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	nd := openSoloNode(t, dir, 1<<20) // large threshold: nothing flushes
	clockID := nd.ClockID()
	for _, v := range []string{"v1", "v2"} {
		if err := nd.Put(ctx, "k", v, nil); err != nil {
			t.Fatalf("Put %s failed: %v", v, err)
		}
	}
	if err := nd.Put(ctx, "other", "x", nil); err != nil {
		t.Fatalf("Put other failed: %v", err)
	}
	if err := nd.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if files := sstFiles(t, dir); len(files) != 0 {
		t.Fatalf("expected no SSTables (recovery must come from the WAL), found %v", files)
	}

	reopened := openSoloNode(t, dir, 1<<20)
	t.Cleanup(func() { reopened.Close() })
	assertRecovered(t, reopened, "k", "v2", map[string]uint32{clockID: 2})
	assertRecovered(t, reopened, "other", "x", map[string]uint32{clockID: 1})
}

// TestPersistentNodeRecoversFromSSTables: writes flushed to SSTables must
// come back from them. Once flushed, their WAL segments are deleted, so
// the WAL left behind must hold nothing: the SSTables and Manifest are the
// only source on reopening.
func TestPersistentNodeRecoversFromSSTables(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	nd := openSoloNode(t, dir, 10) // tiny threshold: every Put flushes
	clockID := nd.ClockID()
	for _, v := range []string{"v1", "v2"} {
		if err := nd.Put(ctx, "k", v, nil); err != nil {
			t.Fatalf("Put %s failed: %v", v, err)
		}
		nd.engine.WaitForPendingFlushes() // one flush at a time: let each finish
	}
	if err := nd.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if files := sstFiles(t, dir); len(files) < 2 {
		t.Fatalf("expected both writes flushed to SSTables, found %v", files)
	}
	segments, err := filepath.Glob(filepath.Join(dir, "wal_*.log"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	for _, seg := range segments {
		if info, err := os.Stat(seg); err != nil || info.Size() != 0 {
			t.Fatalf("expected the flushed writes' WAL segments to be gone, but %s holds %v bytes (err %v)", seg, info.Size(), err)
		}
	}

	reopened := openSoloNode(t, dir, 10)
	t.Cleanup(func() { reopened.Close() })
	assertRecovered(t, reopened, "k", "v2", map[string]uint32{clockID: 2})
}

// TestNewClosesMainEngineWhenHintsOpenFails: if hints.Open fails
// (here because another engine already holds the hints dir's lock),
// New must close the main engine it already opened — releasing
// its own lock — rather than leaking a locked, orphaned StorageEngine.
func TestNewClosesMainEngineWhenHintsOpenFails(t *testing.T) {
	dir := t.TempDir()

	// Pre-lock the hints dir ourselves, so New's internal
	// hints.Open call fails with engine.ErrLocked.
	blocker, err := engine.Open(filepath.Join(dir, "hints"), 1<<20)
	if err != nil {
		t.Fatalf("failed to pre-lock the hints dir: %v", err)
	}
	defer blocker.Close()

	if _, err := New("node-1", "unused", 1, 1, 1, nil, dir, 1<<20); err == nil {
		t.Fatal("expected New to fail while the hints dir is locked")
	} else if !errors.Is(err, engine.ErrLocked) {
		t.Fatalf("expected the error to wrap engine.ErrLocked, got %v", err)
	}

	// The main engine must have been closed on that failure path. If it
	// weren't, its lock would still be held and this Open would itself
	// fail with ErrLocked.
	e, err := engine.Open(dir, 1<<20)
	if err != nil {
		t.Fatalf("expected the main engine's dir to be unlocked after the failed New, got %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// TestCloseIsIdempotentAndClosesPeerClients: Close closes every cached
// peer connection and can be called again safely, although closing the
// engine's WAL twice would otherwise fail.
func TestCloseIsIdempotentAndClosesPeerClients(t *testing.T) {
	a := knownAddrs("node-1", "node-2") // never served
	nd := newTestNode(t, "node-1", a["node-1"], 2, 1, 1, map[string]string{"node-2": a["node-2"]})
	client, err := nd.getOrDialClient("node-2")
	if err != nil {
		t.Fatalf("dial node-2: %v", err)
	}

	for i := 1; i <= 2; i++ {
		if err := nd.Close(); err != nil {
			t.Fatalf("Close #%d failed: %v", i, err)
		}
	}
	if len(nd.clients) != 0 {
		t.Fatalf("expected Close to empty the client cache, got %d entries", len(nd.clients))
	}
	if _, _, err := client.FetchItem(context.Background(), "k"); status.Code(err) != codes.Canceled {
		t.Fatalf("expected the cached client's connection to be closed (codes.Canceled), got %v", err)
	}
}
