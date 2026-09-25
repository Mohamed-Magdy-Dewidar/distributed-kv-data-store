package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/storage/compaction"
	"distributed-kv-datastore/internal/storage/manifest"
	"distributed-kv-datastore/internal/storage/memtable"
	"distributed-kv-datastore/internal/storage/sstable"
	"distributed-kv-datastore/internal/storage/wal"
	"distributed-kv-datastore/internal/store"
	"distributed-kv-datastore/internal/versioning"
)

// StorageEngine is DataStore's durable backend. This check is the only
// reason this package imports internal/store; store never imports engine.
var _ store.Persister = (*StorageEngine)(nil)

// StorageEngine orchestrates WAL, MemTable(s), SSTables, and the Manifest
// for one node's local storage. Every layer holds sibling sets, not single
// values: the memtable merges writes with versioning.Resolve, compaction
// merges SSTables with versioning.MergeSiblings, and GetAll folds all
// layers together the same way, returning every surviving sibling newest
// first. Wiring DataStore to use StorageEngine as its backing store is a
// separate future integration step.
//
// Known limitations, documented deliberately rather than over-built:
//   - Only one flush runs at a time. If a new Put crosses the memtable
//     size threshold while a previous flush is still in progress, the
//     swap is skipped and the active memtable is allowed to grow past
//     maxBytes temporarily, rather than losing track of the in-flight
//     frozen memtable. A real system would queue multiple immutable
//     memtables; that's more complexity than this phase needs.
//   - GetAll can't stop at the first layer holding the key: an older
//     SSTable may hold a sibling Concurrent with a newer layer's version.
//     It reads every SSTable whose range and Bloom filter admit the key,
//     so a key spread across many files costs one read per file until
//     compaction folds them together.
//   - .sst files left behind by a failed flush or compaction (written but
//     never registered, or unregistered but not yet deleted) are never
//     loaded, but are also never cleaned up.
type StorageEngine struct {
	mu       sync.RWMutex
	active   *memtable.MemTable
	flushing *memtable.MemTable // non-nil only while a flush is in progress
	sstables []*sstable.SSTable // newest first

	wal      *wal.WAL
	manifest *manifest.Manifest
	dataDir  string
	maxBytes int

	flushWG sync.WaitGroup // lets tests/Close wait for background flushes

	// compactMu serializes Compact calls (and Close against them). Two
	// concurrent compactions could select overlapping runs and both
	// replace them; holding it for the whole of Compact rules that out.
	compactMu sync.Mutex
	closed    bool // guarded by compactMu; Compact is a no-op once set

	// filesMu keeps compaction from deleting an .sst file that a Get is
	// still reading. Get holds it for reading from before it snapshots
	// e.sstables until it's done with them; Compact takes it for writing
	// only to delete files already swapped out of e.sstables, so every Get
	// that could still see those files has finished.
	filesMu sync.RWMutex
}

// Open creates or opens a StorageEngine rooted at dataDir: replays the
// WAL to reconstruct the active memtable, then loads every SSTable the
// Manifest records as live. On any error, every resource already opened
// is closed before returning.
func Open(dataDir string, maxMemtableBytes int) (_ *StorageEngine, err error) {
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

	e := &StorageEngine{
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
func (e *StorageEngine) loadLiveSSTables() error {
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
func (e *StorageEngine) Put(key string, item *model.DataItem) error {
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
// registers it in the Manifest. frozen remains fully queryable via GetAll for
// the entire duration of this call — it is only detached from e.flushing
// (and thus stops being consulted by GetAll) once its data is reachable some
// other way.
//
// e.flushing is cleared on every exit path, so one failed flush never
// stops later flushes. On failure, every one of frozen's versions is
// merged back into the active memtable behind active's own (all newer)
// versions via PutBackOlder, so GetAll's answer doesn't change and the
// next flush retries them. Flush errors are not otherwise reported; the
// data also remains recoverable from the WAL on restart.
func (e *StorageEngine) flush(frozen *memtable.MemTable) {
	defer e.flushWG.Done()

	entries := frozen.Snapshot() // read-only: frozen stays queryable throughout the write below

	sst, err := e.writeAndRegister(entries)

	e.mu.Lock()
	defer e.mu.Unlock()

	if err != nil {
		e.active.PutBackOlder(entries) // no threshold signal: the next Put triggers the retry
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
func (e *StorageEngine) writeAndRegister(entries []memtable.Entry) (*sstable.SSTable, error) {
	sst, err := sstable.Write(e.dataDir, entries)
	if err != nil {
		return nil, err
	}
	if err := e.manifest.Add(sst.ID); err != nil {
		return nil, err
	}
	return sst, nil
}

// GetAll returns every sibling version stored for key, newest first. It
// folds the active memtable, then the in-flight frozen memtable (if any),
// then every SSTable newest-to-oldest (each of which cheaply rules itself
// out via a range check and Bloom filter before touching disk) together
// with versioning.MergeSiblings, newest layer first — the same fold
// compaction.Merge uses — so a version superseded by a newer layer is
// dropped and a Concurrent one survives. Tombstones are returned like any
// other version, as sstable.GetAll does.
func (e *StorageEngine) GetAll(key string) ([]*model.DataItem, bool, error) {
	e.filesMu.RLock()
	defer e.filesMu.RUnlock()

	e.mu.RLock()
	active := e.active
	flushing := e.flushing
	sstables := e.sstables // slice header copy; safe, existing elements are never mutated
	e.mu.RUnlock()

	return mergeLayers(key, active, flushing, sstables)
}

// mergeLayers is GetAll's fold over one consistent snapshot of the layers.
// Callers must keep sstables' files from being deleted (hold e.filesMu).
func mergeLayers(key string, active, flushing *memtable.MemTable, sstables []*sstable.SSTable) ([]*model.DataItem, bool, error) {
	var merged []*model.DataItem
	if items, found := active.GetAll(key); found {
		merged = versioning.MergeSiblings(merged, items)
	}
	if flushing != nil {
		if items, found := flushing.GetAll(key); found {
			merged = versioning.MergeSiblings(merged, items)
		}
	}

	for _, sst := range sstables {
		items, found, err := sst.GetAll(key)
		if err != nil {
			return nil, false, fmt.Errorf("engine: read sstable %s: %w", sst.Path, err)
		}
		if found {
			merged = versioning.MergeSiblings(merged, items)
		}
	}

	return merged, len(merged) > 0, nil
}

// Keys returns every distinct key held in any layer — active memtable,
// in-flight frozen memtable, and every SSTable — in sorted order,
// tombstoned keys included (the same visibility as GetAll). It reads only
// in-memory state (memtables and SSTable indexes), never disk; the error
// is part of store.Persister's contract and is currently always nil.
func (e *StorageEngine) Keys() ([]string, error) {
	e.mu.RLock()
	active := e.active
	flushing := e.flushing
	sstables := e.sstables
	e.mu.RUnlock()

	seen := make(map[string]bool)
	add := func(keys []string) {
		for _, k := range keys {
			seen[k] = true
		}
	}
	add(active.Keys())
	if flushing != nil {
		add(flushing.Keys())
	}
	for _, sst := range sstables {
		add(sst.Keys())
	}

	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}

// Restore replaces key's sibling set in the active memtable with items
// verbatim (items newest first; nil/empty removes the key from the active
// memtable).
//
// ROLLBACK ONLY. It exists solely so DataStore.RestoreVersions can undo a
// local write whose quorum write failed (see Node.Put in
// internal/node/node.go). It deliberately skips versioning.Resolve, so it
// must never be used for ordinary writes — that's what Put is for.
//
// Known limitations, both accepted:
//   - It is not written to the WAL, and does not undo anything already in
//     it. A crash after a rollback replays the rolled-back write on
//     restart. Rollback has never been WAL-aware in this codebase.
//   - It can only change the active memtable. If the write being rolled
//     back was already frozen for flushing (the Put that wrote it, or any
//     concurrent Put, crossed the memtable threshold) or already flushed,
//     GetAll still merges it back in. Restore detects this rather than
//     silently succeeding: after replacing, it re-runs GetAll's fold and,
//     unless the result is exactly items, returns an error wrapping
//     store.ErrRestoreIncomplete. The replacement is kept either way.
//
// The replace and the check happen atomically under e.mu (with filesMu
// held so compaction can't delete a file being read), so a concurrent Put
// can't make the check pass or fail spuriously. That means Puts wait
// while the check reads any SSTable holding key — acceptable for a
// rollback path.
func (e *StorageEngine) Restore(key string, items []*model.DataItem) error {
	e.filesMu.RLock()
	defer e.filesMu.RUnlock()
	e.mu.Lock()
	defer e.mu.Unlock()

	e.active.Replace(key, items)

	visible, _, err := mergeLayers(key, e.active, e.flushing, e.sstables)
	if err != nil {
		return fmt.Errorf("engine: verify restore of key %q: %w", key, err)
	}
	if !sameVersions(visible, items) {
		return fmt.Errorf("engine: restore of key %q: %w", key, store.ErrRestoreIncomplete)
	}
	return nil
}

// sameVersions reports whether a and b hold the identical *DataItem
// pointers in the same order. mergeLayers starts from the active
// memtable's set and keeps those pointers unless another layer's version
// supersedes one or survives beside them, so pointer identity is exactly
// "no other layer changed the answer".
func sameVersions(a, b []*model.DataItem) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// WaitForPendingFlushes blocks until every currently in-flight background
// flush completes. Intended for tests and graceful shutdown.
func (e *StorageEngine) WaitForPendingFlushes() {
	e.flushWG.Wait()
}

// Compact runs one size-tiered compaction step: if a run of at least
// four adjacent, similarly-sized SSTables exists (see
// compaction.SelectTierForCompaction), it merges them into one new
// SSTable and swaps that in for the run. It returns nil when there is
// nothing to compact.
//
// Steps are ordered so a crash between any two loses nothing, mirroring
// flush's write-then-register sequencing:
//  1. Write the merged SSTable (atomic rename; not yet live).
//  2. Manifest.Add it. From here it's live. Its ID sorts immediately
//     after the run's newest source and before anything newer, so if a
//     crash leaves both it and its sources live, it shadows them with
//     equivalent data and is itself shadowed by every newer SSTable,
//     exactly as the run was.
//  3. Manifest.Remove all sources in one record.
//  4. Swap the merged SSTable into e.sstables at the run's position.
//  5. Delete the sources' files.
//
// Merging and writing happen without e.mu, like flush: sources are
// immutable and only Compact removes them, so they can't change
// underneath it. Only the swap holds e.mu.
func (e *StorageEngine) Compact() error {
	e.compactMu.Lock()
	defer e.compactMu.Unlock()
	if e.closed {
		return nil
	}

	e.mu.RLock()
	current := e.sstables
	e.mu.RUnlock()

	sources, err := compaction.SelectTierForCompaction(current)
	if err != nil {
		return fmt.Errorf("engine: select compaction tier: %w", err)
	}
	if len(sources) == 0 {
		return nil
	}

	entries, err := compaction.Merge(sources) // sources are newest first, as Merge requires
	if err != nil {
		return fmt.Errorf("engine: merge for compaction: %w", err)
	}

	merged, err := sstable.WriteWithID(e.dataDir, e.compactedID(sources[0].ID), entries)
	if err != nil {
		return fmt.Errorf("engine: write compacted sstable: %w", err)
	}
	if err := e.manifest.Add(merged.ID); err != nil {
		os.Remove(merged.Path) // best effort: it was never registered, so it's harmless either way
		return fmt.Errorf("engine: register compacted sstable %s: %w", merged.ID, err)
	}

	oldIDs := make([]string, len(sources))
	for i, src := range sources {
		oldIDs[i] = src.ID
	}
	if err := e.manifest.Remove(oldIDs); err != nil {
		// Merged and sources are all live now. That's safe — see step 2 —
		// so leave e.sstables as it is; a restart loads both, correctly
		// ordered, and a later compaction folds them together.
		return fmt.Errorf("engine: unregister compacted sources: %w", err)
	}

	e.mu.Lock()
	swapped, ok := replaceRun(e.sstables, sources, merged)
	if ok {
		e.sstables = swapped
	}
	e.mu.Unlock()
	if !ok {
		// Can't happen: only Compact removes SSTables and flush only
		// prepends, so the run stays contiguous. The Manifest is already
		// correct for a restart; just don't delete anything still in use.
		return fmt.Errorf("engine: compacted run no longer contiguous in memory; old files kept until restart")
	}

	e.filesMu.Lock()
	for _, src := range sources {
		os.Remove(src.Path) // failure leaves an unregistered file: never loaded, just wasted space
	}
	e.filesMu.Unlock()

	return nil
}

// compactedID derives the merged SSTable's ID from its run's newest
// source: "<newest>_c" sorts after "<newest>" and before any later ID,
// since IDs are fixed-width timestamps. A crash can leave a file with that
// name behind (e.g. registered but its sources never removed), so keep
// extending the suffix until the name is free; each extension still sorts
// after the one before it.
func (e *StorageEngine) compactedID(newestSourceID string) string {
	id := newestSourceID + "_c"
	for {
		if _, err := os.Stat(filepath.Join(e.dataDir, id+".sst")); os.IsNotExist(err) {
			return id
		}
		id += "_c"
	}
}

// replaceRun returns a new slice with run (a contiguous, in-order
// sub-slice of sstables, by identity) replaced by merged. It allocates
// rather than editing in place because Get reads slice headers copied
// out from under e.mu. It reports false if run isn't found intact.
func replaceRun(sstables, run []*sstable.SSTable, merged *sstable.SSTable) ([]*sstable.SSTable, bool) {
	for start := range sstables {
		if sstables[start] != run[0] {
			continue
		}
		if start+len(run) > len(sstables) {
			return nil, false
		}
		for i, src := range run {
			if sstables[start+i] != src {
				return nil, false
			}
		}
		out := make([]*sstable.SSTable, 0, len(sstables)-len(run)+1)
		out = append(out, sstables[:start]...)
		out = append(out, merged)
		out = append(out, sstables[start+len(run):]...)
		return out, true
	}
	return nil, false
}

// StartCompactionLoop calls Compact every interval until ctx is canceled,
// mirroring node.StartAntiEntropyLoop. Compact errors are dropped, like
// flush errors: every failure path leaves data intact and correctly
// ordered, and the next tick simply tries again. Cancel ctx before or
// after Close; once Close has run, ticks are no-ops.
func (e *StorageEngine) StartCompactionLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = e.Compact()
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (e *StorageEngine) Close() error {
	e.WaitForPendingFlushes()

	e.compactMu.Lock() // waits out any in-progress compaction
	e.closed = true
	e.compactMu.Unlock()

	if err := e.manifest.Close(); err != nil {
		return err
	}
	return e.wal.Close()
}
