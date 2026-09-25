package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"distributed-kv-datastore/internal/storage/manifest"
	"distributed-kv-datastore/internal/storage/memtable"
	"distributed-kv-datastore/internal/storage/sstable"
	"distributed-kv-datastore/internal/storage/wal"
	"distributed-kv-datastore/internal/store"
)

// Engine orchestrates WAL, MemTable(s), SSTables, and the Manifest for one
// node's local storage. This is a single-value-per-key engine — it does
// NOT perform sibling/vector-clock conflict resolution; that logic lives
// in store.DataStore/resolve(). Wiring DataStore to use Engine as its
// backing store is a separate future integration step.
//
// Known limitations, documented deliberately rather than over-built:
//   - Only one flush runs at a time. If a new Put crosses the memtable
//     size threshold while a previous flush is still in progress, the
//     swap is skipped and the active memtable is allowed to grow past
//     maxBytes temporarily, rather than losing track of the in-flight
//     frozen memtable. A real system would queue multiple immutable
//     memtables; that's more complexity than this phase needs.
type Engine struct {
	mu       sync.RWMutex
	active   *memtable.MemTable
	flushing *memtable.MemTable // non-nil only while a flush is in progress
	sstables []*sstable.SSTable // newest first

	wal      *wal.WAL
	manifest *manifest.Manifest
	dataDir  string
	maxBytes int

	flushWG sync.WaitGroup // lets tests/Close wait for background flushes
}

// Open creates or opens an Engine rooted at dataDir: replays the WAL to
// reconstruct the active memtable, then loads every SSTable the Manifest
// records as live. On any error, every resource already opened is closed
// before returning.
func Open(dataDir string, maxMemtableBytes int) (_ *Engine, err error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("engine: create data dir %s: %w", dataDir, err)
	}

	w, err := wal.Open(filepath.Join(dataDir, "wal.log"))
	if err != nil {
		return nil, fmt.Errorf("engine: open wal: %w", err)
	}
	defer func() {
		if err != nil {
			w.Close()
		}
	}()

	mf, err := manifest.Open(dataDir)
	if err != nil {
		return nil, fmt.Errorf("engine: open manifest: %w", err)
	}
	defer func() {
		if err != nil {
			mf.Close()
		}
	}()

	e := &Engine{
		active:   memtable.New(maxMemtableBytes),
		wal:      w,
		manifest: mf,
		dataDir:  dataDir,
		maxBytes: maxMemtableBytes,
	}

	entries, err := w.Replay()
	if err != nil {
		return nil, fmt.Errorf("engine: replay wal: %w", err)
	}
	for _, entry := range entries {
		e.active.Put(entry.Key, entry.Item) // ignore threshold signal on replay: don't auto-flush during startup
	}

	if err := e.loadLiveSSTables(); err != nil {
		return nil, err
	}

	return e, nil
}

// loadLiveSSTables opens exactly the SSTable files the Manifest says are
// currently live — replacing the earlier filepath.Glob-based discovery,
// which could not distinguish "every .sst file physically present" from
// "every SSTable that's actually current" once compaction can leave
// obsolete files behind across a crash.
func (e *Engine) loadLiveSSTables() error {
	ids := e.manifest.LiveIDs()
	sort.Strings(ids) // IDs embed a nanosecond timestamp: lexicographic == chronological

	for i := len(ids) - 1; i >= 0; i-- { // newest first
		path := filepath.Join(e.dataDir, ids[i]+".sst")
		sst, err := sstable.Open(path)
		if err != nil {
			return fmt.Errorf("engine: open live sstable %s (id %s): %w", path, ids[i], err)
		}
		e.sstables = append(e.sstables, sst)
	}
	return nil
}

// Put durably writes key/item: WAL append+fsync first, then the in-memory
// insert — the WAL append is the true point of durability.
func (e *Engine) Put(key string, item *store.DataItem) error {
	if err := e.wal.Append(wal.Entry{Key: key, Item: item}); err != nil {
		return fmt.Errorf("engine: wal append for key %q: %w", key, err)
	}

	e.mu.Lock()
	crossed := e.active.Put(key, item)

	if crossed && e.flushing == nil {
		e.flushing = e.active
		e.active = memtable.New(e.maxBytes)

		e.flushWG.Add(1)
		frozen := e.flushing
		go e.flush(frozen)
	}
	e.mu.Unlock()

	return nil
}

// flush writes frozen's contents to a new SSTable in the background and
// registers it in the Manifest. frozen remains fully queryable via Get for
// the entire duration of this call — it is only detached from e.flushing
// (and thus stops being consulted by Get) once its data is reachable some
// other way.
//
// e.flushing is cleared on every exit path, so one failed flush never
// stops later flushes. On failure, frozen's entries are merged back into
// the active memtable (skipping keys active already holds, since those
// are newer) so they stay visible to Get and are retried by the next
// flush. Flush errors are not otherwise reported; the data also remains
// recoverable from the WAL on restart.
func (e *Engine) flush(frozen *memtable.MemTable) {
	defer e.flushWG.Done()

	entries := frozen.Snapshot() // read-only: frozen stays queryable throughout the write below

	sst, err := e.writeAndRegister(entries)

	e.mu.Lock()
	defer e.mu.Unlock()

	if err != nil {
		for _, entry := range entries {
			if _, found := e.active.Get(entry.Key); !found {
				e.active.Put(entry.Key, entry.Item) // ignore threshold signal: the next Put triggers the retry
			}
		}
	} else {
		e.sstables = append([]*sstable.SSTable{sst}, e.sstables...) // newest first
	}
	if e.flushing == frozen {
		e.flushing = nil
	}
}

// writeAndRegister writes entries to a new SSTable, then durably records
// it as live. The Manifest write is the durable "this file is now live"
// fact — it must happen after sstable.Write's atomic rename has already
// guaranteed the file itself is fully, safely on disk. If the Manifest
// write fails, the .sst file is left behind as a harmless orphan: never
// registered as live, so never loaded on restart.
func (e *Engine) writeAndRegister(entries []memtable.Entry) (*sstable.SSTable, error) {
	sst, err := sstable.Write(e.dataDir, entries)
	if err != nil {
		return nil, err
	}
	if err := e.manifest.Add(sst.ID); err != nil {
		return nil, err
	}
	return sst, nil
}

// Get checks the active memtable, then the in-flight frozen memtable (if
// any), then every SSTable newest-to-oldest (each of which cheaply rules
// itself out via a range check and Bloom filter before touching disk).
func (e *Engine) Get(key string) (*store.DataItem, bool, error) {
	e.mu.RLock()
	active := e.active
	flushing := e.flushing
	sstables := e.sstables // slice header copy; safe, existing elements are never mutated
	e.mu.RUnlock()

	if item, found := active.Get(key); found {
		return item, true, nil
	}
	if flushing != nil {
		if item, found := flushing.Get(key); found {
			return item, true, nil
		}
	}

	for _, sst := range sstables {
		item, found, err := sst.Get(key)
		if err != nil {
			return nil, false, fmt.Errorf("engine: read sstable %s: %w", sst.Path, err)
		}
		if found {
			return item, true, nil
		}
	}

	return nil, false, nil
}

// WaitForPendingFlushes blocks until every currently in-flight background
// flush completes. Intended for tests and graceful shutdown.
func (e *Engine) WaitForPendingFlushes() {
	e.flushWG.Wait()
}

func (e *Engine) Close() error {
	e.WaitForPendingFlushes()
	if err := e.manifest.Close(); err != nil {
		return err
	}
	return e.wal.Close()
}
