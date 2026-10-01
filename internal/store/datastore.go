package store

import (
	"errors"
	"log"
	"sync"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/vectorclock"
	"distributed-kv-datastore/internal/versioning"
)

// DataStore versions a node's writes on top of a Persister: every read and
// write goes through to it, and there is no second, in-memory copy. It
// returns a key's siblings oldest first. Two versions with Equal vector
// clocks but different values (a client reusing the same explicit context)
// keep the first one while both are in the persister's memtable; the second
// one wins if the first was already flushed out of it.
//
// Persister errors: every persister failure is logged with the
// operation, key and underlying error. Get, Keys, MergeReplicated and
// RestoreVersions return it, so callers can tell "not found" from
// "storage failed". The rest still signal it in-band: Put returns nil,
// GetLiveItems returns not-found, and Delete returns false with a
// "storage error: ..." message.
//
// A DataStore has two identifiers. id names the node and is recorded in each
// item's LastUpdatedBy. clockID names the writer in the vector clocks of
// items this store versions; a node passes its node ID plus incarnation (see
// internal/identity) so that a replacement node reusing the ID starts a
// fresh clock entry instead of restarting an old one. The plain constructors
// use id for both.
type DataStore struct {
	id        string
	clockID   string
	persister Persister
	mu        sync.Mutex
}

// NewDataStoreWithPersister returns a DataStore whose versions live in p.
// The caller owns p's lifecycle (opening and closing it).
func NewDataStoreWithPersister(id string, p Persister) *DataStore {
	return NewDataStoreWithPersisterAndClockID(id, id, p)
}

// NewDataStoreWithPersisterAndClockID is NewDataStoreWithPersister with
// vector-clock entries named clockID instead of id.
func NewDataStoreWithPersisterAndClockID(id, clockID string, p Persister) *DataStore {
	return &DataStore{
		id:        id,
		clockID:   clockID,
		persister: p,
	}
}

// load reads key's versions from the persister, oldest first: the order
// DataStore returns them in, the one Resolve keeps when it appends each
// surviving write after the existing ones. A persister error is logged here
// and returned.
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
// It returns the error if the persister read fails.
//
// It does not take ds.mu: listing every key can be long on a big store, and
// the persister's Keys is safe to call concurrently with its other methods
// (StorageEngine.Keys only reads a snapshot of its layers), so a scan must
// not stall every Get and Put behind it. The result is not a point-in-time
// snapshot: a key written meanwhile may or may not be in it.
func (ds *DataStore) Keys() ([]string, error) {
	keys, err := ds.persister.Keys()
	if err != nil {
		log.Printf("store: Keys: persister read failed: %v", err)
		return nil, err
	}
	return keys, nil
}

// Get returns every sibling version stored for key, tombstones included.
// found is false when there are none. It returns the error if the
// persister read fails — never a not-found in its place.
func (ds *DataStore) Get(key string) ([]*model.DataItem, bool, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	return ds.load("Get", key)
}

func (ds *DataStore) GetLiveItems(key string) ([]*model.DataItem, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	items, found, err := ds.load("GetLiveItems", key)
	if err != nil || !found {
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

// Put writes value as a new version of key and returns it. It returns nil
// if the write could not be persisted.
func (ds *DataStore) Put(key string, value any, context map[string]uint32) *model.DataItem {
	ds.mu.Lock()
	defer ds.mu.Unlock()

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
		VectorClock:   vectorclock.BuildFromContext(base, ds.clockID),
		LastUpdatedBy: ds.id,
	}
	if err := ds.persister.Put(key, incoming); err != nil {
		log.Printf("store: Put %q: persister write failed: %v", key, err)
		return nil
	}
	return incoming
}

// Delete treats deletion as just another write — a tombstoned DataItem
// with its own vector clock, run through the same resolve() path. A failed
// read or write returns false with a "storage error: ..." message.
func (ds *DataStore) Delete(key string, context map[string]uint32) (bool, string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	existing, found, err := ds.load("Delete", key)
	if err != nil {
		return false, "storage error: " + err.Error()
	}
	if !found {
		return false, "Key not found"
	}

	base := context
	if base == nil {
		base = versioning.UnionVectorClock(existing)
	}

	tombstone := &model.DataItem{
		Value:         nil,
		VectorClock:   vectorclock.BuildFromContext(base, ds.clockID),
		LastUpdatedBy: ds.id,
		IsDeleted:     true,
	}
	if err := ds.persister.Put(key, tombstone); err != nil {
		log.Printf("store: Delete %q: persister write failed: %v", key, err)
		return false, "storage error: " + err.Error()
	}
	return true, "Key deleted"
}

// RestoreVersions overwrites key's sibling set with items, bypassing the
// vector-clock/tombstone machinery in Put/Delete. It exists for rolling back
// a local write that never reached quorum — an internal undo, not a
// semantic delete — so it must not itself be recorded as a new version. A
// nil/empty items removes the key entirely, restoring "no prior write"
// rather than leaving behind an empty-but-present slice.
//
// It calls Persister.Restore, which cannot always fully undo the write (see
// ErrRestoreIncomplete): that case is logged as a warning, and the
// rolled-back write stays visible on this node, the usual meaning of "a
// failed write may still have been applied". Any other persister error is
// logged too. Either one is returned.
func (ds *DataStore) RestoreVersions(key string, items []*model.DataItem) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	err := ds.persister.Restore(key, reversed(items))
	switch {
	case errors.Is(err, ErrRestoreIncomplete):
		log.Printf("store: RestoreVersions %q: rollback incomplete, the rolled-back write is still visible: %v", key, err)
	case err != nil:
		log.Printf("store: RestoreVersions %q: persister restore failed: %v", key, err)
	}
	return err
}

// MergeReplicated applies an item that arrived from another node through
// the same conflict-resolution path a local Put uses. This owns its own
// locking, so a caller (Node.Replicate) never has to reach into DataStore
// internals directly — the encapsulation the old direct field access broke.
// It returns an error when the write could not be persisted (also logged).
func (ds *DataStore) MergeReplicated(key string, item *model.DataItem) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if err := ds.persister.Put(key, item); err != nil {
		log.Printf("store: MergeReplicated %q: persister write failed: %v", key, err)
		return err
	}
	return nil
}
