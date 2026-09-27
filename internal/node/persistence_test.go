package node

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// openSoloNode opens a persistent single-node "cluster" (N=W=R=1, no
// neighbors) at dir, so Node.Put and Node.Get run entirely locally.
func openSoloNode(t *testing.T, dir string, maxMemtableBytes int) *Node {
	t.Helper()
	nd, err := NewPersistent("node-1", "localhost:0", 1, 1, 1, nil, dir, maxMemtableBytes)
	if err != nil {
		t.Fatalf("NewPersistent failed: %v", err)
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
	assertRecovered(t, reopened, "k", "v2", map[string]uint32{"node-1": 2})
	assertRecovered(t, reopened, "other", "x", map[string]uint32{"node-1": 1})
}

// TestPersistentNodeRecoversFromSSTables: writes flushed to SSTables must
// come back from them. Once flushed, their WAL segments are deleted, so
// the WAL left behind must hold nothing: the SSTables and Manifest are the
// only source on reopening.
func TestPersistentNodeRecoversFromSSTables(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	nd := openSoloNode(t, dir, 10) // tiny threshold: every Put flushes
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
	assertRecovered(t, reopened, "k", "v2", map[string]uint32{"node-1": 2})
}

// TestCloseIsIdempotentAndClosesPeerClients: Close closes every cached
// peer connection, has no engine to close on an in-memory node, and can
// be called again safely — on a persistent node too, where closing the
// engine's WAL twice would otherwise fail.
func TestCloseIsIdempotentAndClosesPeerClients(t *testing.T) {
	nd := New("node-1", "localhost:60481", 2, 1, 1, map[string]string{"node-2": "localhost:60482"})
	if nd.engine != nil {
		t.Fatal("expected an in-memory node to have no storage engine")
	}
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

	persistent := openSoloNode(t, t.TempDir(), 1<<20)
	for i := 1; i <= 2; i++ {
		if err := persistent.Close(); err != nil {
			t.Fatalf("persistent Close #%d failed: %v", i, err)
		}
	}
}
