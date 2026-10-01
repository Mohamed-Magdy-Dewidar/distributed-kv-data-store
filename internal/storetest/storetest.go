// Package storetest gives tests a store.DataStore backed by a real storage
// engine in a temporary directory: the same persistence a node runs on,
// rather than a test double.
package storetest

import (
	"testing"

	"distributed-kv-datastore/internal/storage/engine"
	"distributed-kv-datastore/internal/store"
)

// MemtableBytes is the flush threshold of the engines NewStore opens, the
// same 1 MiB the node tests use.
const MemtableBytes = 1 << 20

// NewStore returns a DataStore for node id on a StorageEngine opened in a
// fresh t.TempDir(). Vector-clock entries are named id. The engine is closed
// when the test ends, before the directory is removed: its cleanup is
// registered after TempDir's, so it runs first, and Windows can delete the
// files only once they are closed.
func NewStore(t testing.TB, id string) *store.DataStore {
	t.Helper()
	dir := t.TempDir()
	e, err := engine.Open(dir, MemtableBytes)
	if err != nil {
		t.Fatalf("storetest: open engine in %s: %v", dir, err)
	}
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Errorf("storetest: close engine in %s: %v", dir, err)
		}
	})
	return store.NewDataStoreWithPersister(id, e)
}
