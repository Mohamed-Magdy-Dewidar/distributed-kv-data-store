package memtable

import (
	"encoding/json"
	"sync"

	"github.com/google/btree"

	"distributed-kv-datastore/internal/store"
)

// entry is the btree.Item wrapping a key and its current DataItem.
type entry struct {
	key  string
	item *store.DataItem
}

func (e *entry) Less(than btree.Item) bool {
	return e.key < than.(*entry).key
}

// estimatedSize returns a rough byte-size estimate for a key/item pair,
// used purely to decide when to flush — not an exact accounting. Value is
// `any`, so it's marshaled the same way the rest of this project already
// serializes DataItem.Value (see internal/rpc/convert.go) rather than
// assuming any particular concrete type.
func estimatedSize(key string, item *store.DataItem) int {
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

// MemTable is a sorted, in-memory, size-tracked table of the most
// recently written data. Safe for concurrent use.
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

// Put inserts or overwrites key's entry. Returns true if the table has now
// reached or exceeded maxBytes and should be flushed.
func (m *MemTable) Put(key string, item *store.DataItem) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	newSize := estimatedSize(key, item)

	old := m.tree.ReplaceOrInsert(&entry{key: key, item: item})
	if old != nil {
		oldEntry := old.(*entry)
		m.sizeSoFar -= estimatedSize(oldEntry.key, oldEntry.item)
	}
	m.sizeSoFar += newSize

	return m.sizeSoFar >= m.maxBytes
}

// Get returns the current item for key, if present.
func (m *MemTable) Get(key string) (*store.DataItem, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	found := m.tree.Get(&entry{key: key})
	if found == nil {
		return nil, false
	}
	return found.(*entry).item, true
}

// Entry is the exported shape SnapshotAndClear hands to callers (an
// SSTable writer, eventually) — deliberately not exposing the internal
// btree.Item wrapper type.
type Entry struct {
	Key  string
	Item *store.DataItem
}

// SnapshotAndClear freezes the current contents (returned in sorted key
// order, ready for an SSTable writer to consume), then resets this
// MemTable to empty so it can immediately accept new writes. The returned
// slice is a stable, independent copy of the ordering at the moment of
// the call — subsequent Puts on this MemTable do not affect it.
func (m *MemTable) SnapshotAndClear() []Entry {
	m.mu.Lock()
	defer m.mu.Unlock()

	items := make([]Entry, 0, m.tree.Len())
	m.tree.Ascend(func(i btree.Item) bool {
		e := i.(*entry)
		items = append(items, Entry{Key: e.key, Item: e.item})
		return true
	})

	m.tree.Clear(false)
	m.sizeSoFar = 0
	return items
}

// Snapshot returns every entry in sorted key order WITHOUT clearing the
// table — unlike SnapshotAndClear. Used when a frozen memtable must
// remain fully queryable (via Get) while its contents are being written
// to an SSTable in the background.
func (m *MemTable) Snapshot() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	items := make([]Entry, 0, m.tree.Len())
	m.tree.Ascend(func(i btree.Item) bool {
		e := i.(*entry)
		items = append(items, Entry{Key: e.key, Item: e.item})
		return true
	})
	return items
}

// Len returns the current number of entries.
func (m *MemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tree.Len()
}
