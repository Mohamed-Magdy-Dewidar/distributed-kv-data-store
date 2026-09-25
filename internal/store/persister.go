package store

import (
	"errors"

	"distributed-kv-datastore/internal/model"
)

// Persister is the durable storage a DataStore can run on top of (see
// NewDataStoreWithPersister). internal/storage/engine.StorageEngine is the
// real implementation; DataStore depends only on this interface, never on
// the engine package itself.
//
// Every read returns a key's sibling set newest first.
type Persister interface {
	// Put durably merges item into key's sibling set, with the same
	// causality rules as versioning.Resolve.
	Put(key string, item *model.DataItem) error

	// GetAll returns every sibling version stored for key, newest first,
	// tombstones included. found is false when there are none.
	GetAll(key string) ([]*model.DataItem, bool, error)

	// Restore replaces key's sibling set with items verbatim (nil/empty
	// removes the key), bypassing causality-aware merging. It exists only
	// for rolling back a local write that never reached quorum. It returns
	// an error wrapping ErrRestoreIncomplete when it could not make items
	// the key's whole visible set.
	Restore(key string, items []*model.DataItem) error

	// Keys returns every key currently stored, tombstoned keys included.
	Keys() ([]string, error)
}

// ErrRestoreIncomplete reports that Persister.Restore installed the
// requested versions but other versions of the key it cannot remove are
// still visible alongside or instead of them — for StorageEngine, because
// the write being rolled back was already frozen for flushing (or flushed)
// before the rollback ran. The rolled-back write stays readable on this
// node. It's defined here rather than in the engine package so DataStore
// can recognize it without importing the engine.
var ErrRestoreIncomplete = errors.New("restore incomplete: versions outside the active memtable are still visible")
