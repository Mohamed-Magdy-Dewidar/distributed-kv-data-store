package app

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"distributed-kv-datastore/internal/config"
	"distributed-kv-datastore/internal/node"
	"distributed-kv-datastore/internal/storage/engine"
	"distributed-kv-datastore/internal/storage/wal"
)

// heldSyncs opens WAL segments whose fsync, once hold has been called for
// the directory they are in, waits until release.
type heldSyncs struct {
	dir     atomic.Pointer[string]
	release chan struct{}
	waiting atomic.Int32 // fsyncs waiting for release
}

func (h *heldSyncs) hold(dir string) {
	h.release = make(chan struct{})
	h.dir.Store(&dir)
}

func (h *heldSyncs) open(path string) (wal.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &heldSyncFile{File: file, dir: filepath.Dir(path), h: h}, nil
}

type heldSyncFile struct {
	*os.File
	dir string
	h   *heldSyncs
}

func (f *heldSyncFile) Sync() error {
	if dir := f.h.dir.Load(); dir != nil && *dir == f.dir {
		f.h.waiting.Add(1)
		<-f.h.release
		f.h.waiting.Add(-1)
	}
	return f.File.Sync()
}

// batchSizes records the size of every WAL batch an engine reports.
type batchSizes struct {
	mu    sync.Mutex
	sizes []int
}

func (b *batchSizes) WALSynced(time.Duration)     {}
func (b *batchSizes) WALCommitWait(time.Duration) {}
func (b *batchSizes) WALBatch(writes int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sizes = append(b.sizes, writes)
}

func (b *batchSizes) take() []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.sizes
	b.sizes = nil
	return s
}

// TestGroupCommitFollowsTheConfig: storage.groupCommit chooses the write
// path of the node's storage engines. The first write's fsync is held while
// more writers arrive; once it is released, group commit (the default, or
// true) writes the writes that queued behind it as one batch, and false
// writes every one alone, one fsync each, as before group commit.
func TestGroupCommitFollowsTheConfig(t *testing.T) {
	const writers = 16
	for _, tc := range []struct {
		name    string
		setting *bool
		group   bool
	}{
		{"unset", nil, true},
		{"true", new(true), true},
		{"false", new(false), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := knownAddr()
			cfg := &config.Config{
				NodeID:  "node-1",
				DataDir: t.TempDir(),
				Cluster: config.Cluster{N: 1, W: 1, R: 1, Members: []config.Member{{ID: "node-1", Address: addr}}},
				Storage: config.Storage{MemtableBytes: 1 << 20, GroupCommit: tc.setting},
			}
			syncs := &heldSyncs{}
			batches := &batchSizes{}
			nd, err := openNode(cfg, node.WithStorageObservers(batches, nil),
				node.WithEngineOptions(engine.WithWALOpenFile(syncs.open)))
			if err != nil {
				t.Fatalf("openNode: %v", err)
			}
			t.Cleanup(func() { nd.Close() })
			batches.take() // anything written while opening

			syncs.hold(cfg.DataDir)
			released := false
			release := func() {
				if !released {
					released = true
					close(syncs.release)
				}
			}
			t.Cleanup(release) // before Close, which would wait for the held fsync

			errs := make(chan error, writers+1)
			put := func(key string) {
				if nd.Store.Put(key, "v", nil) == nil {
					errs <- fmt.Errorf("Put(%s) failed", key)
					return
				}
				errs <- nil
			}
			go put("first")
			eventuallyTrue(t, 5*time.Second, "the first write's fsync to start", func() bool { return syncs.waiting.Load() == 1 })
			for i := range writers {
				go put(fmt.Sprintf("k%d", i))
			}
			// Time for the writers to read their keys and reach the commit
			// path: the committer's queue with group commit, the write lock
			// without. Only the group case needs it, to batch them.
			time.Sleep(200 * time.Millisecond)
			release()
			for range writers + 1 {
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
			}

			sizes := batches.take()
			total := 0
			for _, n := range sizes {
				total += n
			}
			if total != writers+1 {
				t.Fatalf("batches %v hold %d writes, want %d", sizes, total, writers+1)
			}
			if tc.group {
				if slices.Max(sizes) < 2 {
					t.Errorf("group commit on: batches %v, want the writes that queued during the held fsync in one batch", sizes)
				}
			} else if slices.Max(sizes) != 1 {
				t.Errorf("group commit off: batches %v, want every write alone", sizes)
			}
		})
	}
}
