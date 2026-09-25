package store

import (
	"errors"
	"log"
	"sync"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/vectorclock"
	"distributed-kv-datastore/internal/versioning"
)

// DataStore runs in one of two modes, fixed at construction:
//   - in-memory (NewDataStore): versions live in store, nothing persists.
//   - persister-backed (NewDataStoreWithPersister): every read and write
//     goes through to persister, and store is nil — there is no second,
//     in-memory copy.
//
// Both modes behave the same, including sibling order (oldest first), with
// one known difference: two versions with Equal vector clocks but
// different values (a client reusing the same explicit context) keep the
// first one in memory, but in persister mode the second one wins if the
// first was already flushed out of the persister's memtable.
//
// Persister errors (PLACEHOLDER until these methods return error): none of
// DataStore's methods can return an error yet, so a failed persister call
// is logged with the operation, key and underlying error, and the return
// value says that nothing happened — Put and BuildItem return nil, Get and
// GetLiveItems return not-found, Delete returns false with a "storage
// error: ..." message, Keys returns nil, MergeReplicated returns the
// error (the one method already widened, because an rpc.Server must not
// acknowledge a replicated write it failed to persist). RestoreVersions
// has no return value, so for it the log is the only signal. Callers
// can't yet tell "not found" from "storage failed" on the read paths.
type DataStore struct {
	id        string
	store     map[string][]*model.DataItem // in-memory mode only; nil when persister != nil
	persister Persister                    // persister-backed mode only
	mu        sync.Mutex
}

func NewDataStore(id string) *DataStore {
	return &DataStore{
		id:    id,
		store: make(map[string][]*model.DataItem),
	}
}

// NewDataStoreWithPersister returns a DataStore whose versions live in p
// instead of in memory. The caller owns p's lifecycle (opening and closing
// it).
func NewDataStoreWithPersister(id string, p Persister) *DataStore {
	return &DataStore{
		id:        id,
		persister: p,
	}
}

// load reads key's versions from the persister, oldest first — the order
// in-memory mode keeps them in, since Resolve appends each surviving write
// after the existing ones. A persister error is logged here and returned.
func (ds *DataStore) load(op, key string) ([]*model.DataItem, bool, error) {
	items, found, err := ds.persister.GetAll(key)
	if err != nil {
		log.Printf("store: %s %q: persister read failed: %v", op, key, err)
		return nil, false, err
	}
	return reversed(items), found, nil
}

// reversed returns a reversed copy of items: persister order (newest
// first) <-> DataStore order (oldest first).
func reversed(items []*model.DataItem) []*model.DataItem {
	if items == nil {
		return nil
	}
	out := make([]*model.DataItem, len(items))
	for i, item := range items {
		out[len(items)-1-i] = item
	}
	return out
}

// Keys returns every key currently present in the store (including
// tombstoned keys — same "raw" visibility as Get, not GetLiveItems).
// Needed by anti-entropy to enumerate what a bucket actually contains.
func (ds *DataStore) Keys() []string {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.persister != nil {
		keys, err := ds.persister.Keys()
		if err != nil {
			log.Printf("store: Keys: persister read failed: %v", err)
			return nil
		}
		return keys
	}

	keys := make([]string, 0, len(ds.store))
	for k := range ds.store {
		keys = append(keys, k)
	}
	return keys
}

func (ds *DataStore) Get(key string) ([]*model.DataItem, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.persister != nil {
		items, found, err := ds.load("Get", key)
		if err != nil {
			return nil, false
		}
		return items, found
	}

	items, exists := ds.store[key]
	if !exists {
		return nil, false
	}
	return items, len(items) > 0
}

func (ds *DataStore) GetLiveItems(key string) ([]*model.DataItem, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	var items []*model.DataItem
	if ds.persister != nil {
		loaded, found, err := ds.load("GetLiveItems", key)
		if err != nil || !found {
			return nil, false
		}
		items = loaded
	} else {
		stored, exists := ds.store[key]
		if !exists {
			return nil, false
		}
		items = stored
	}

	var aliveItems []*model.DataItem
	for _, item := range items {
		if !item.IsDeleted {
			aliveItems = append(aliveItems, item)
		}
	}
	return aliveItems, len(aliveItems) > 0
}

// Put writes value as a new version of key and returns it. In
// persister-backed mode it returns nil if the write could not be
// persisted.
func (ds *DataStore) Put(key string, value any, context map[string]uint32) *model.DataItem {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.persister != nil {
		base := context
		if base == nil {
			existing, found, err := ds.load("Put", key)
			if err != nil {
				return nil
			}
			if found {
				base = versioning.UnionVectorClock(existing)
			}
		}

		incoming := &model.DataItem{
			Value:         value,
			VectorClock:   vectorclock.BuildFromContext(base, ds.id),
			LastUpdatedBy: ds.id,
		}
		if err := ds.persister.Put(key, incoming); err != nil {
			log.Printf("store: Put %q: persister write failed: %v", key, err)
			return nil
		}
		return incoming
	}

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
// item, but must not end up holding a local copy of its own. In
// persister-backed mode it returns nil if it needed the existing versions
// (context is nil) and could not read them — building from an empty base
// instead would silently produce a version that fails to supersede them.
func (ds *DataStore) BuildItem(key string, value any, context map[string]uint32) *model.DataItem {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	base := context
	if base == nil {
		if ds.persister != nil {
			existing, found, err := ds.load("BuildItem", key)
			if err != nil {
				return nil
			}
			if found {
				base = versioning.UnionVectorClock(existing)
			}
		} else if existing, ok := ds.store[key]; ok {
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
// with its own vector clock, run through the same resolve() path. In
// persister-backed mode a failed read or write returns false with a
// "storage error: ..." message.
func (ds *DataStore) Delete(key string, context map[string]uint32) (bool, string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	var existing []*model.DataItem
	if ds.persister != nil {
		loaded, found, err := ds.load("Delete", key)
		if err != nil {
			return false, "storage error: " + err.Error()
		}
		if !found {
			return false, "Key not found"
		}
		existing = loaded
	} else {
		stored, ok := ds.store[key]
		if !ok {
			return false, "Key not found"
		}
		existing = stored
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

	if ds.persister != nil {
		if err := ds.persister.Put(key, tombstone); err != nil {
			log.Printf("store: Delete %q: persister write failed: %v", key, err)
			return false, "storage error: " + err.Error()
		}
		return true, "Key deleted"
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
//
// In persister-backed mode this calls Persister.Restore, which cannot
// always fully undo the write (see ErrRestoreIncomplete): that case is
// logged as a warning, and the rolled-back write stays visible on this
// node, the usual meaning of "a failed write may still have been applied".
// Any other persister error is logged too; neither is fatal.
func (ds *DataStore) RestoreVersions(key string, items []*model.DataItem) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.persister != nil {
		err := ds.persister.Restore(key, reversed(items))
		switch {
		case errors.Is(err, ErrRestoreIncomplete):
			log.Printf("store: RestoreVersions %q: rollback incomplete, the rolled-back write is still visible: %v", key, err)
		case err != nil:
			log.Printf("store: RestoreVersions %q: persister restore failed: %v", key, err)
		}
		return
	}

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
// It returns an error only in persister-backed mode, when the write could
// not be persisted (also logged); in-memory mode always returns nil.
func (ds *DataStore) MergeReplicated(key string, item *model.DataItem) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.persister != nil {
		if err := ds.persister.Put(key, item); err != nil {
			log.Printf("store: MergeReplicated %q: persister write failed: %v", key, err)
			return err
		}
		return nil
	}

	existing := ds.store[key]
	ds.store[key] = versioning.Resolve(existing, item)
	return nil
}
