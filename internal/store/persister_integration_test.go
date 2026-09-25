package store_test

// Integration tests against a real *engine.StorageEngine. They live in the
// external store_test package because engine imports store (for its
// store.Persister compile-time check), so package store's own tests can't
// import engine.

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/storage/engine"
	"distributed-kv-datastore/internal/store"
	"distributed-kv-datastore/internal/vectorclock"
)

func openStore(t *testing.T, dir string, maxMemtableBytes int) (*store.DataStore, *engine.StorageEngine) {
	t.Helper()
	e, err := engine.Open(dir, maxMemtableBytes)
	if err != nil {
		t.Fatalf("engine.Open failed: %v", err)
	}
	return store.NewDataStoreWithPersister("node-1", e), e
}

func values(items []*model.DataItem) []any {
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, item.Value)
	}
	return out
}

// TestPersisterBackedDataStoreSurvivesRestart writes through a DataStore
// backed by a real StorageEngine — with a threshold small enough that some
// data is flushed to SSTables and some stays only in the WAL — closes it,
// reopens a fresh DataStore/StorageEngine pair on the same directory, and
// checks every value, tombstone, sibling set and key survived.
func TestPersisterBackedDataStoreSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ds1, e1 := openStore(t, dir, 1000)

	ds1.Put("big", strings.Repeat("x", 1000), nil) // alone crosses the threshold: flushed to an SSTable
	e1.WaitForPendingFlushes()
	ds1.Put("plain", "v1", nil)
	ds1.Put("plain", "v2", nil) // supersedes v1
	ds1.Put("gone", "soon-deleted", nil)
	if ok, msg := ds1.Delete("gone", nil); !ok {
		t.Fatalf("Delete failed: %s", msg)
	}
	base := map[string]uint32{"node-1": 1}
	ds1.Put("conflict", "from-node-1", base)
	ds1.MergeReplicated("conflict", &model.DataItem{
		Value:         "from-node-2",
		VectorClock:   vectorclock.BuildFromContext(base, "node-2"),
		LastUpdatedBy: "node-2",
	})
	// Everything after "big" totals well under 1000 bytes, so it's still
	// only in the active memtable and the WAL when the engine closes.

	want := map[string][]any{
		"big":      {strings.Repeat("x", 1000)},
		"plain":    {"v2"},
		"gone":     {nil},
		"conflict": {"from-node-1", "from-node-2"},
	}
	check := func(label string, ds *store.DataStore) {
		t.Helper()
		for key, wantValues := range want {
			items, found := ds.Get(key)
			if !found || !reflect.DeepEqual(values(items), wantValues) {
				t.Errorf("%s: Get(%s): expected %v, got found=%v %v", label, key, wantValues, found, values(items))
			}
		}
		if raw, _ := ds.Get("gone"); len(raw) != 1 || !raw[0].IsDeleted {
			t.Errorf("%s: expected gone to be a tombstone, got %+v", label, raw)
		}
		keys := ds.Keys()
		sort.Strings(keys)
		if wantKeys := []string{"big", "conflict", "gone", "plain"}; !reflect.DeepEqual(keys, wantKeys) {
			t.Errorf("%s: Keys: expected %v, got %v", label, wantKeys, keys)
		}
	}
	check("before restart", ds1)

	if err := e1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	ds2, e2 := openStore(t, dir, 1000)
	defer e2.Close()
	check("after restart", ds2)

	// The reopened store keeps working on top of what it recovered.
	if item := ds2.Put("plain", "v3", nil); item == nil || item.VectorClock.Snapshot()["node-1"] != 3 {
		t.Fatalf("expected a post-restart Put to build on the recovered clock, got %+v", item)
	}
}

// TestRollbackThroughRealEngine mirrors Node.Put's quorum-failure path:
// read the previous versions, write, then RestoreVersions. While the write
// is still in the active memtable the rollback fully succeeds; once a
// write has crossed the flush threshold it can't be undone, and the store
// logs that instead of pretending (see engine.Restore).
func TestRollbackThroughRealEngine(t *testing.T) {
	t.Run("write still in active memtable", func(t *testing.T) {
		ds, e := openStore(t, t.TempDir(), 1<<20)
		defer e.Close()

		ds.Put("k", "committed", nil)
		prev, _ := ds.Get("k")
		ds.Put("k", "failed-quorum", nil)
		ds.RestoreVersions("k", prev)

		if items, _ := ds.Get("k"); !reflect.DeepEqual(values(items), []any{"committed"}) {
			t.Fatalf("expected rollback to restore [committed], got %v", values(items))
		}

		prevNone, _ := ds.Get("new-key")
		ds.Put("new-key", "failed-quorum", nil)
		ds.RestoreVersions("new-key", prevNone)
		if _, found := ds.Get("new-key"); found {
			t.Fatal("expected rolling back a first write to remove the key")
		}
	})

	t.Run("write already flushed", func(t *testing.T) {
		ds, e := openStore(t, t.TempDir(), 200)
		defer e.Close()

		ds.Put("k", "committed", nil)
		prev, _ := ds.Get("k")
		ds.Put("k", strings.Repeat("x", 200), nil) // crosses the threshold: frozen, then flushed
		e.WaitForPendingFlushes()

		ds.RestoreVersions("k", prev) // logs ErrRestoreIncomplete; must not panic

		if items, _ := ds.Get("k"); !reflect.DeepEqual(values(items), []any{strings.Repeat("x", 200)}) {
			t.Fatalf("expected the flushed write to remain visible (known limitation), got %v", values(items))
		}
	})
}
