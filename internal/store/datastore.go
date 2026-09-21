package store

import (
	"sync"

	"distributed-kv-datastore/internal/vectorclock"
)

type DataItem struct {
	Value         any
	VectorClock   *vectorclock.VectorClock
	LastUpdatedBy string
	IsDeleted     bool
}

type DataStore struct {
	id    string
	store map[string][]*DataItem
	mu    sync.Mutex
}

func NewDataStore(id string) *DataStore {
	return &DataStore{
		id:    id,
		store: make(map[string][]*DataItem),
	}
}

func (ds *DataStore) Get(key string) ([]*DataItem, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	items, exists := ds.store[key]
	if !exists {
		return nil, false
	}
	return items, len(items) > 0
}

func (ds *DataStore) GetLiveItems(key string) ([]*DataItem, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	items, exists := ds.store[key]
	if !exists {
		return nil, false
	}

	var aliveItems []*DataItem
	for _, item := range items {
		if !item.IsDeleted {
			aliveItems = append(aliveItems, item)
		}
	}
	return aliveItems, len(aliveItems) > 0
}

func (ds *DataStore) Put(key string, value any, context map[string]uint32) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	existing, ok := ds.store[key]

	base := context
	if base == nil && ok {
		base = unionVectorClock(existing)
	}

	incoming := &DataItem{
		Value:         value,
		VectorClock:   vectorclock.BuildFromContext(base, ds.id),
		LastUpdatedBy: ds.id,
	}

	if !ok {
		ds.store[key] = []*DataItem{incoming}
		return
	}
	ds.store[key] = resolve(existing, incoming)
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
		base = unionVectorClock(existing)
	}

	tombstone := &DataItem{
		Value:         nil,
		VectorClock:   vectorclock.BuildFromContext(base, ds.id),
		LastUpdatedBy: ds.id,
		IsDeleted:     true,
	}

	ds.store[key] = resolve(existing, tombstone)
	return true, "Key deleted"
}

// MergeReplicated applies an item that arrived from another node through
// the same conflict-resolution path a local Put uses. This owns its own
// locking, so a caller (Node.Replicate) never has to reach into DataStore
// internals directly — the encapsulation the old direct field access broke.
func (ds *DataStore) MergeReplicated(key string, item *DataItem) bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	existing := ds.store[key]
	ds.store[key] = resolve(existing, item)

	// if at least one of the existing items was removed, then the merge was successful
	return len(ds.store[key]) < len(existing)
}
