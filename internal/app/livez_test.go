package app

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"distributed-kv-datastore/internal/node"
	"distributed-kv-datastore/internal/storage/engine"
	"distributed-kv-datastore/internal/storage/wal"
)

var errDiskFailed = errors.New("injected fsync failure")

// failingSyncs opens WAL segments whose fsync fails once arm has been
// called for a directory they are in.
type failingSyncs struct {
	armed atomic.Pointer[string] // the directory whose segments fail
}

func (f *failingSyncs) arm(dir string) { f.armed.Store(&dir) }

func (f *failingSyncs) open(path string) (wal.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &failingSyncFile{File: file, dir: filepath.Dir(path), f: f}, nil
}

type failingSyncFile struct {
	*os.File
	dir string
	f   *failingSyncs
}

func (s *failingSyncFile) Sync() error {
	if dir := s.f.armed.Load(); dir != nil && *dir == s.dir {
		return errDiskFailed
	}
	return s.File.Sync()
}

func livez(t *testing.T, base string) (int, string) {
	t.Helper()
	resp, err := http.Get(base + "/livez")
	if err != nil {
		t.Fatalf("GET /livez: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// TestLivezFailsOnceAStorageEngineHaltsWrites: /livez answers 200 until a
// WAL fsync fails in either of the node's storage engines, its data or its
// hint store, and 503 from then on, so that Kubernetes restarts the
// container: a halted engine accepts writes again only once reopened.
func TestLivezFailsOnceAStorageEngineHaltsWrites(t *testing.T) {
	t.Run("data", func(t *testing.T) {
		dataDir := t.TempDir()
		faults := &failingSyncs{}
		addr := knownAddr()
		nd, err := node.New("node-1", addr, 1, 1, 1, nil, dataDir, 1<<20,
			node.WithEngineOptions(engine.WithWALOpenFile(faults.open)))
		if err != nil {
			t.Fatalf("node.New: %v", err)
		}
		t.Cleanup(func() { nd.Close() })
		base := adminServer(t, nd)

		if nd.Store.Put("before", "v", nil) == nil {
			t.Fatal("Put before the fault failed")
		}
		if status, body := livez(t, base); status != http.StatusOK {
			t.Fatalf("/livez before the fault: %d %q, want 200", status, body)
		}

		faults.arm(dataDir)
		if nd.Store.Put("during", "v", nil) != nil {
			t.Fatal("Put with a failing fsync succeeded")
		}
		if !nd.WritesHalted() {
			t.Fatal("WritesHalted is false after the data engine's fsync failed")
		}
		if status, body := livez(t, base); status != http.StatusServiceUnavailable || !strings.Contains(body, "halted") {
			t.Fatalf("/livez after the fault: %d %q, want 503 saying writes are halted", status, body)
		}
	})

	t.Run("hints", func(t *testing.T) {
		dataDir := t.TempDir()
		faults := &failingSyncs{}
		addr := knownAddr()
		dead := knownAddr() // nothing listens: replicating to it is Unavailable, so it gets a hint
		nd, err := node.New("node-1", addr, 2, 1, 1, map[string]string{"node-2": dead}, dataDir, 1<<20,
			node.WithEngineOptions(engine.WithWALOpenFile(faults.open)))
		if err != nil {
			t.Fatalf("node.New: %v", err)
		}
		t.Cleanup(func() { nd.Close() })
		nd.QuorumConfig.ReplicationTimeout = time.Second
		nd.QuorumConfig.MaxReconnectBackoff = 50 * time.Millisecond
		base := adminServer(t, nd)

		faults.arm(filepath.Join(dataDir, "hints"))
		if err := nd.Put(context.Background(), "k", "v", nil); err != nil {
			t.Fatalf("Put with W=1: %v", err) // the local write is enough
		}
		eventuallyTrue(t, 5*time.Second, "the failed hint write to halt the hint store", nd.WritesHalted)
		if status, body := livez(t, base); status != http.StatusServiceUnavailable || !strings.Contains(body, "halted") {
			t.Fatalf("/livez after the hint store's fault: %d %q, want 503 saying writes are halted", status, body)
		}
		if nd.Store.Put("still", "v", nil) == nil { // the data engine is unaffected
			t.Fatal("a data write failed after only the hint store halted")
		}
	})
}
