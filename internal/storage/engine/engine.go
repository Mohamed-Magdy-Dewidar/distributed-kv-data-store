package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"distributed-kv-datastore/internal/storage/memtable"
	"distributed-kv-datastore/internal/storage/sstable"
	"distributed-kv-datastore/internal/storage/wal"
	"distributed-kv-datastore/internal/store"
)

// Engine orchestrates WAL, MemTable(s), and SSTables for one node's local
// storage. This is a single-value-per-key engine — it does NOT perform
// sibling/vector-clock conflict resolution; that logic lives in
// store.DataStore/resolve(). Wiring DataStore to use Engine as its
// backing store is a separate future integration step.
//
// Known limitations, documented deliberately rather than over-built:
//   - No Manifest yet: existing SSTables are discovered via a directory
//     glob on startup, sorted by filename (which embeds a nanosecond
//     timestamp). A crash mid-compaction could leave orphaned files this
//     engine has no way to detect — acceptable for now, fixed later.
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
	dataDir  string
	maxBytes int

	flushWG sync.WaitGroup // lets tests/Close wait for background flushes
}

// Open creates or opens an Engine rooted at dataDir: replays the WAL to
// reconstruct the active memtable, then loads any existing SSTables from
// disk.
func Open(dataDir string, maxMemtableBytes int) (*Engine, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("engine: create data dir %s: %w", dataDir, err)
	}

	w, err := wal.Open(filepath.Join(dataDir, "wal.log"))
	if err != nil {
		return nil, fmt.Errorf("engine: open wal: %w", err)
	}

	e := &Engine{
		active:   memtable.New(maxMemtableBytes),
		wal:      w,
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

	if err := e.loadExistingSSTables(); err != nil {
		return nil, err
	}

	return e, nil
}

func (e *Engine) loadExistingSSTables() error {
	matches, err := filepath.Glob(filepath.Join(e.dataDir, "*.sst"))
	if err != nil {
		return fmt.Errorf("engine: glob sstables: %w", err)
	}
	sort.Strings(matches) // filenames embed a nanosecond timestamp: lexicographic == chronological

	for i := len(matches) - 1; i >= 0; i-- { // newest first
		sst, err := sstable.Open(matches[i])
		if err != nil {
			return fmt.Errorf("engine: open existing sstable %s: %w", matches[i], err)
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

// flush writes frozen's contents to a new SSTable in the background.
// frozen remains fully queryable via Get for the entire duration of this
// call — it is only detached from e.flushing (and thus stops being
// consulted by Get) after the new SSTable is safely registered.
func (e *Engine) flush(frozen *memtable.MemTable) {
	defer e.flushWG.Done()

	entries := frozen.Snapshot() // read-only: frozen stays queryable throughout the write below

	sst, err := sstable.Write(e.dataDir, entries)
	if err != nil {
		// Documented limitation: flush errors are not retried. The data
		// remains safely recoverable from the WAL on the next restart,
		// so nothing is lost — but this memtable's data won't reach an
		// SSTable until a future successful flush is triggered.
		return
	}

	e.mu.Lock()
	e.sstables = append([]*sstable.SSTable{sst}, e.sstables...) // newest first
	if e.flushing == frozen {
		e.flushing = nil
	}
	e.mu.Unlock()
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
	return e.wal.Close()
}
