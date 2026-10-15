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
// Every read returns a key's sibling set newest first. Every method must be
// safe to call concurrently with every other: DataStore locks only per key
// for writes and not at all for reads, so reads and writes of different keys,
// and reads of the key being written, reach the persister at once. A write
// must become visible to reads all at once.
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

// ErrWritesHalted reports that a Persister has stopped accepting writes
// after its storage failed, and will again only once reopened (for
// StorageEngine, after a failed WAL write or fsync; see
// engine.ErrWritesHalted). Every write that fails because of the halt
// wraps it: the writes made after it, which wrote nothing, and the ones
// whose failure caused it, which may or may not be durable. Either way the
// failure is the storage's, not the write's, and goes away with a restart:
// a replica answers it as Unavailable, so the sender stores a hint
// (internal/rpc, Server.Replicate). It's defined here rather than in the
// engine package so DataStore's callers can recognize it without importing
// the engine.
var ErrWritesHalted = errors.New("writes halted until the store is reopened")
