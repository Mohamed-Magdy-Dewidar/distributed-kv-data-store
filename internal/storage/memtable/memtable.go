package memtable

import (
	"encoding/json"
	"sync"

	"github.com/google/btree"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/versioning"
)

// entry is the btree.Item wrapping a key and its current sibling set,
// newest first (by arrival at this memtable).
type entry struct {
	key   string
	items []*model.DataItem
}

func (e *entry) Less(than btree.Item) bool {
	return e.key < than.(*entry).key
}

// estimatedSize returns a rough byte-size estimate for a key/item pair,
// used purely to decide when to flush — not an exact accounting. Value is
// `any`, so it's marshaled the same way the rest of this project already
// serializes DataItem.Value (see internal/rpc/convert.go) rather than
// assuming any particular concrete type.
func estimatedSize(key string, item *model.DataItem) int {
	valueBytes, err := json.Marshal(item.Value)
	if err != nil {
		// A value that can't be marshaled is a real problem elsewhere in
		// this project too (the wire format and WAL both already assume
		// this succeeds) — fall back to a conservative non-zero estimate
		// rather than silently sizing this entry as free.
		valueBytes = []byte{}
	}
	return len(key) + len(valueBytes) + 64 // +64: rough overhead for VectorClock/metadata
}

// setSize sizes a whole sibling set. The key counts once per item because
// an SSTable writes it once per sibling record.
func setSize(key string, items []*model.DataItem) int {
	total := 0
	for _, item := range items {
		total += estimatedSize(key, item)
	}
	return total
}

// MemTable is a sorted, in-memory, size-tracked table of the most
// recently written data. Each key holds a sibling set, not a single
// value: writes are merged with versioning.Resolve, exactly as DataStore
// merges them, so a Concurrent write survives alongside what's already
// there instead of overwriting it. Safe for concurrent use.
type MemTable struct {
	mu        sync.RWMutex
	tree      *btree.BTree
	sizeSoFar int
	maxBytes  int
}

// New creates an empty MemTable that reports itself ready to flush once
// its estimated contents reach maxBytes.
func New(maxBytes int) *MemTable {
	return &MemTable{
		tree:     btree.New(32),
		maxBytes: maxBytes,
	}
}

// current returns key's sibling set, newest first, or nil. Callers must
// hold m.mu.
func (m *MemTable) current(key string) []*model.DataItem {
	found := m.tree.Get(&entry{key: key})
	if found == nil {
		return nil
	}
	return found.(*entry).items
}

// replace installs items as key's sibling set and adjusts sizeSoFar by the
// difference from the old set. Callers must hold m.mu for writing.
func (m *MemTable) replace(key string, old, items []*model.DataItem) {
	m.tree.ReplaceOrInsert(&entry{key: key, items: items})
	m.sizeSoFar += setSize(key, items) - setSize(key, old)
}

// Put merges item into key's sibling set via versioning.Resolve: it can
// supersede existing versions, be superseded (and dropped), duplicate an
// existing version (and be dropped — Resolve keeps the one already here),
// or survive as a Concurrent sibling. A surviving item becomes the newest
// entry in the set. Returns true if the table has now reached or exceeded
// maxBytes and should be flushed.
func (m *MemTable) Put(key string, item *model.DataItem) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	old := m.current(key)
	survivors := versioning.Resolve(old, item)

	// Resolve keeps existing survivors in their original (newest-first)
	// order; move item to the front if it survived.
	items := make([]*model.DataItem, 0, len(survivors))
	for _, s := range survivors {
		if s == item {
			items = append(items, item)
			break
		}
	}
	for _, s := range survivors {
		if s != item {
			items = append(items, s)
		}
	}

	m.replace(key, old, items)
	return m.sizeSoFar >= m.maxBytes
}

// PutBackOlder merges entries — which must all be older than everything
// already in this MemTable, like a failed flush's frozen snapshot — behind
// the existing versions of each key. Per key it folds the older siblings
// in with versioning.MergeSiblings(existing, older), the same newest-first
// fold StorageEngine.GetAll uses across layers, so putting a failed
// flush's data back changes nothing a reader sees. entries may repeat a
// key (one Entry per sibling, newest first, as Snapshot returns them).
// Unlike Put, it doesn't report the flush threshold: the next Put does.
func (m *MemTable) PutBackOlder(entries []Entry) {
	var keys []string
	older := make(map[string][]*model.DataItem)
	for _, e := range entries {
		if _, seen := older[e.Key]; !seen {
			keys = append(keys, e.Key)
		}
		older[e.Key] = append(older[e.Key], e.Item)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, key := range keys {
		old := m.current(key)
		m.replace(key, old, versioning.MergeSiblings(old, older[key]))
	}
}

// Replace installs items as key's sibling set verbatim — no Resolve, no
// merging — or removes key entirely when items is empty. It exists only
// for StorageEngine.Restore's rollback path; every ordinary write must go
// through Put so causality is respected. items must be newest first.
func (m *MemTable) Replace(key string, items []*model.DataItem) {
	m.mu.Lock()
	defer m.mu.Unlock()

	old := m.current(key)
	if len(items) == 0 {
		m.tree.Delete(&entry{key: key})
		m.sizeSoFar -= setSize(key, old)
		return
	}
	m.replace(key, old, append([]*model.DataItem(nil), items...))
}

// Keys returns every key currently held, in sorted order.
func (m *MemTable) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]string, 0, m.tree.Len())
	m.tree.Ascend(func(i btree.Item) bool {
		keys = append(keys, i.(*entry).key)
		return true
	})
	return keys
}

// GetAll returns every sibling version currently held for key, newest
// first. The returned slice is the caller's own copy.
func (m *MemTable) GetAll(key string) ([]*model.DataItem, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	items := m.current(key)
	if len(items) == 0 {
		return nil, false
	}
	return append([]*model.DataItem(nil), items...), true
}

// Entry is the exported shape SnapshotAndClear hands to callers (an
// SSTable writer, eventually) — deliberately not exposing the internal
// btree.Item wrapper type. It holds one version: a key with several
// siblings appears as several consecutive Entries, the same convention
// SSTables and compaction use.
type Entry struct {
	Key  string
	Item *model.DataItem
}

// entriesLocked flattens the tree into one Entry per sibling: keys in
// sorted order, each key's siblings newest first. Callers must hold m.mu.
func (m *MemTable) entriesLocked() []Entry {
	items := make([]Entry, 0, m.tree.Len())
	m.tree.Ascend(func(i btree.Item) bool {
		e := i.(*entry)
		for _, item := range e.items {
			items = append(items, Entry{Key: e.key, Item: item})
		}
		return true
	})
	return items
}

// SnapshotAndClear freezes the current contents (returned in sorted key
// order, each key's siblings newest first, ready for an SSTable writer to
// consume), then resets this MemTable to empty so it can immediately
// accept new writes. The returned slice is a stable, independent copy of
// the ordering at the moment of the call — subsequent Puts on this
// MemTable do not affect it.
func (m *MemTable) SnapshotAndClear() []Entry {
	m.mu.Lock()
	defer m.mu.Unlock()

	items := m.entriesLocked()
	m.tree.Clear(false)
	m.sizeSoFar = 0
	return items
}

// Snapshot returns every entry in the same order as SnapshotAndClear
// WITHOUT clearing the table. Used when a frozen memtable must remain
// fully queryable (via GetAll) while its contents are being written to an
// SSTable in the background.
func (m *MemTable) Snapshot() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.entriesLocked()
}

// Len returns the current number of distinct keys (not versions).
func (m *MemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tree.Len()
}
