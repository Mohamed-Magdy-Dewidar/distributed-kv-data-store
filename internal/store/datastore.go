package store

import (
	"sync"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/vectorclock"
	"distributed-kv-datastore/internal/versioning"
)

type DataStore struct {
	id    string
	store map[string][]*model.DataItem
	mu    sync.Mutex
}

func NewDataStore(id string) *DataStore {
	return &DataStore{
		id:    id,
		store: make(map[string][]*model.DataItem),
	}
}

// Keys returns every key currently present in the store (including
// tombstoned keys — same "raw" visibility as Get, not GetLiveItems).
// Needed by anti-entropy to enumerate what a bucket actually contains.
func (ds *DataStore) Keys() []string {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	keys := make([]string, 0, len(ds.store))
	for k := range ds.store {
		keys = append(keys, k)
	}
	return keys
}

func (ds *DataStore) Get(key string) ([]*model.DataItem, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	items, exists := ds.store[key]
	if !exists {
		return nil, false
	}
	return items, len(items) > 0
}

func (ds *DataStore) GetLiveItems(key string) ([]*model.DataItem, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	items, exists := ds.store[key]
	if !exists {
		return nil, false
	}

	var aliveItems []*model.DataItem
	for _, item := range items {
		if !item.IsDeleted {
			aliveItems = append(aliveItems, item)
		}
	}
	return aliveItems, len(aliveItems) > 0
}
func (ds *DataStore) Put(key string, value any, context map[string]uint32) *model.DataItem {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	existing, ok := ds.store[key]

	base := context
	if base == nil && ok {
		base = versioning.UnionVectorClock(existing)
	}

	incoming := &model.DataItem{
		Value:         value,
		VectorClock:   vectorclock.BuildFromContext(base, ds.id),
		LastUpdatedBy: ds.id,
	}

	if !ok {
		ds.store[key] = []*model.DataItem{incoming}
		return incoming
	}
	ds.store[key] = versioning.Resolve(existing, incoming)
	return incoming
}

// BuildItem constructs the DataItem Put would produce for value/context —
// same vector-clock derivation, same LastUpdatedBy attribution — without
// writing it into the store. For a coordinator that isn't itself one of
// key's replicas: it still needs to hand replicas a properly versioned
// item, but must not end up holding a local copy of its own.
func (ds *DataStore) BuildItem(key string, value any, context map[string]uint32) *model.DataItem {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	base := context
	if base == nil {
		if existing, ok := ds.store[key]; ok {
			base = versioning.UnionVectorClock(existing)
		}
	}

	return &model.DataItem{
		Value:         value,
		VectorClock:   vectorclock.BuildFromContext(base, ds.id),
		LastUpdatedBy: ds.id,
	}
}

// Delete treats deletion as just another write — a tombstoned DataItem
// with its own vector clock, run through the same resolve() path.
func (ds *DataStore) Delete(key string, context map[string]uint32) (bool, string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	existing, ok := ds.store[key]
	if !ok {
		return false, "Key not found"
	}

	base := context
	if base == nil {
		base = versioning.UnionVectorClock(existing)
	}

	tombstone := &model.DataItem{
		Value:         nil,
		VectorClock:   vectorclock.BuildFromContext(base, ds.id),
		LastUpdatedBy: ds.id,
		IsDeleted:     true,
	}

	ds.store[key] = versioning.Resolve(existing, tombstone)
	return true, "Key deleted"
}

// RestoreVersions overwrites key's sibling set with items, bypassing the
// vector-clock/tombstone machinery in Put/Delete. It exists for rolling back
// a local write that never reached quorum — an internal undo, not a
// semantic delete — so it must not itself be recorded as a new version. A
// nil/empty items removes the key entirely, restoring "no prior write"
// rather than leaving behind an empty-but-present slice.
func (ds *DataStore) RestoreVersions(key string, items []*model.DataItem) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if len(items) == 0 {
		delete(ds.store, key)
		return
	}
	ds.store[key] = items
}

// MergeReplicated applies an item that arrived from another node through
// the same conflict-resolution path a local Put uses. This owns its own
// locking, so a caller (Node.Replicate) never has to reach into DataStore
// internals directly — the encapsulation the old direct field access broke.
func (ds *DataStore) MergeReplicated(key string, item *model.DataItem) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	existing := ds.store[key]
	ds.store[key] = versioning.Resolve(existing, item)
}
