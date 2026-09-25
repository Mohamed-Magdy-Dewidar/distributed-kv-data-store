package store

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/vectorclock"
	"distributed-kv-datastore/internal/versioning"
)

// fakePersister is a single-layer, in-memory Persister with the same
// contract as StorageEngine: Put merges via versioning.Resolve, reads
// return siblings newest first, Restore replaces verbatim. Setting err
// makes every call fail; setting restoreErr makes only Restore fail.
type fakePersister struct {
	data       map[string][]*model.DataItem // newest first
	err        error
	restoreErr error
}

func newFakePersister() *fakePersister {
	return &fakePersister{data: make(map[string][]*model.DataItem)}
}

func (f *fakePersister) Put(key string, item *model.DataItem) error {
	if f.err != nil {
		return f.err
	}
	survivors := versioning.Resolve(f.data[key], item)
	var items []*model.DataItem
	for _, s := range survivors {
		if s == item {
			items = append(items, item) // a surviving write is the newest
		}
	}
	for _, s := range survivors {
		if s != item {
			items = append(items, s)
		}
	}
	f.data[key] = items
	return nil
}

func (f *fakePersister) GetAll(key string) ([]*model.DataItem, bool, error) {
	if f.err != nil {
		return nil, false, f.err
	}
	items := f.data[key]
	return append([]*model.DataItem(nil), items...), len(items) > 0, nil
}

func (f *fakePersister) Restore(key string, items []*model.DataItem) error {
	if f.err != nil {
		return f.err
	}
	if len(items) == 0 {
		delete(f.data, key)
	} else {
		f.data[key] = append([]*model.DataItem(nil), items...)
	}
	return f.restoreErr
}

func (f *fakePersister) Keys() ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	keys := make([]string, 0, len(f.data))
	for k := range f.data {
		keys = append(keys, k)
	}
	return keys, nil
}

// bothModes runs fn against an in-memory DataStore and a persister-backed
// one, as separate subtests: every behavior it checks must hold in both.
func bothModes(t *testing.T, id string, fn func(t *testing.T, ds *DataStore)) {
	t.Run("in-memory", func(t *testing.T) { fn(t, NewDataStore(id)) })
	t.Run("persister", func(t *testing.T) { fn(t, NewDataStoreWithPersister(id, newFakePersister())) })
}

func values(items []*model.DataItem) []any {
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, item.Value)
	}
	return out
}

func TestPersisterModeKeepsNoInMemoryCopy(t *testing.T) {
	p := newFakePersister()
	ds := NewDataStoreWithPersister("node-1", p)
	ds.Put("foo", "bar", nil)

	if ds.store != nil {
		t.Fatalf("expected no in-memory map in persister mode, got %v", ds.store)
	}
	if items := p.data["foo"]; len(items) != 1 || items[0].Value != "bar" {
		t.Fatalf("expected the write to reach the persister, got %v", values(items))
	}
}

func TestBothModesPutGetAndSequentialUpdate(t *testing.T) {
	bothModes(t, "node-1", func(t *testing.T, ds *DataStore) {
		ds.Put("foo", "bar", nil)
		ds.Put("foo", "baz", nil) // no context: builds on what's there, so it supersedes

		items, found := ds.Get("foo")
		if !found || !reflect.DeepEqual(values(items), []any{"baz"}) {
			t.Fatalf("expected [baz], got found=%v %v", found, values(items))
		}
		if vc := items[0].VectorClock.Snapshot(); vc["node-1"] != 2 {
			t.Errorf("expected vc[node-1]=2, got %v", vc)
		}
		if _, found := ds.Get("missing"); found {
			t.Error("expected a missing key to be not found")
		}
	})
}

// TestBothModesReturnSiblingsInTheSameOrder: in-memory mode keeps
// siblings oldest first; persister mode must present the persister's
// newest-first order the same way.
func TestBothModesReturnSiblingsInTheSameOrder(t *testing.T) {
	base := map[string]uint32{"node-1": 1}
	bothModes(t, "node-1", func(t *testing.T, ds *DataStore) {
		ds.Put("foo", "first", base)
		for _, item := range []*model.DataItem{
			{Value: "second", VectorClock: vectorclock.BuildFromContext(base, "node-2")},
			{Value: "third", VectorClock: vectorclock.BuildFromContext(base, "node-3")},
		} {
			if err := ds.MergeReplicated("foo", item); err != nil {
				t.Fatalf("expected MergeReplicated to succeed, got %v", err)
			}
		}

		items, _ := ds.Get("foo")
		if want := []any{"first", "second", "third"}; !reflect.DeepEqual(values(items), want) {
			t.Fatalf("expected siblings oldest first %v, got %v", want, values(items))
		}
	})
}

func TestBothModesDeleteAndLiveItems(t *testing.T) {
	bothModes(t, "node-1", func(t *testing.T, ds *DataStore) {
		if ok, msg := ds.Delete("missing", nil); ok || msg != "Key not found" {
			t.Fatalf("expected Delete of a missing key to report not found, got %v %q", ok, msg)
		}

		ds.Put("foo", "bar", nil)
		if ok, msg := ds.Delete("foo", nil); !ok || msg != "Key deleted" {
			t.Fatalf("expected Delete to succeed, got %v %q", ok, msg)
		}

		if _, found := ds.GetLiveItems("foo"); found {
			t.Error("expected no live items after Delete")
		}
		raw, found := ds.Get("foo")
		if !found || len(raw) != 1 || !raw[0].IsDeleted {
			t.Fatalf("expected Get to return the tombstone, got found=%v %v", found, raw)
		}
		if keys := ds.Keys(); !reflect.DeepEqual(keys, []string{"foo"}) {
			t.Errorf("expected Keys to include the tombstoned key, got %v", keys)
		}
	})
}

func TestBothModesBuildItemUsesUnionClockWithoutWriting(t *testing.T) {
	bothModes(t, "node-1", func(t *testing.T, ds *DataStore) {
		ds.Put("foo", "bar", nil)

		built := ds.BuildItem("foo", "next", nil)
		if built == nil || built.VectorClock.Snapshot()["node-1"] != 2 {
			t.Fatalf("expected a version built on the existing clock, got %+v", built)
		}
		if items, _ := ds.Get("foo"); !reflect.DeepEqual(values(items), []any{"bar"}) {
			t.Fatalf("expected BuildItem not to write, got %v", values(items))
		}
	})
}

func TestBothModesRestoreVersionsReplacesOrRemoves(t *testing.T) {
	bothModes(t, "node-1", func(t *testing.T, ds *DataStore) {
		ds.Put("foo", "v1", nil)
		prev, _ := ds.Get("foo")
		ds.Put("foo", "v2", nil)

		ds.RestoreVersions("foo", prev)
		if items, _ := ds.Get("foo"); !reflect.DeepEqual(values(items), []any{"v1"}) {
			t.Fatalf("expected rollback to restore [v1], got %v", values(items))
		}

		ds.RestoreVersions("foo", nil)
		if _, found := ds.Get("foo"); found {
			t.Fatal("expected RestoreVersions(nil) to remove the key")
		}
		if keys := ds.Keys(); len(keys) != 0 {
			t.Errorf("expected no keys after removal, got %v", keys)
		}
	})
}

func TestRestoreVersionsHandsPersisterNewestFirst(t *testing.T) {
	p := newFakePersister()
	ds := NewDataStoreWithPersister("node-1", p)
	base := map[string]uint32{"node-1": 1}
	older := &model.DataItem{Value: "older", VectorClock: vectorclock.BuildFromContext(base, "node-1")}
	newer := &model.DataItem{Value: "newer", VectorClock: vectorclock.BuildFromContext(base, "node-2")}

	ds.RestoreVersions("foo", []*model.DataItem{older, newer}) // DataStore order: oldest first

	if got := values(p.data["foo"]); !reflect.DeepEqual(got, []any{"newer", "older"}) {
		t.Fatalf("expected the persister to receive newest first, got %v", got)
	}
}

// TestPersisterErrorsNeverLookLikeSuccess: with Decision 3's error-free
// signatures, every method must still make a failed persist visible.
func TestPersisterErrorsNeverLookLikeSuccess(t *testing.T) {
	p := newFakePersister()
	ds := NewDataStoreWithPersister("node-1", p)
	ds.Put("foo", "bar", nil)
	p.err = errors.New("disk on fire")

	if item := ds.Put("foo", "x", nil); item != nil {
		t.Errorf("expected Put to return nil when the read fails, got %+v", item)
	}
	if item := ds.Put("foo", "x", map[string]uint32{"node-1": 5}); item != nil {
		t.Errorf("expected Put to return nil when the write fails, got %+v", item)
	}
	if items, found := ds.Get("foo"); found || items != nil {
		t.Errorf("expected Get to report not found, got found=%v %v", found, items)
	}
	if items, found := ds.GetLiveItems("foo"); found || items != nil {
		t.Errorf("expected GetLiveItems to report not found, got found=%v %v", found, items)
	}
	if ok, msg := ds.Delete("foo", nil); ok || !strings.HasPrefix(msg, "storage error: ") || !strings.Contains(msg, "disk on fire") {
		t.Errorf("expected Delete to report the storage error, got %v %q", ok, msg)
	}
	if item := ds.BuildItem("foo", "x", nil); item != nil {
		t.Errorf("expected BuildItem to return nil when it can't read the existing clock, got %+v", item)
	}
	if item := ds.BuildItem("foo", "x", map[string]uint32{"node-1": 5}); item == nil {
		t.Error("expected BuildItem with an explicit context to need no read and succeed")
	}
	if keys := ds.Keys(); keys != nil {
		t.Errorf("expected Keys to return nil, got %v", keys)
	}
	if err := ds.MergeReplicated("foo", &model.DataItem{Value: "x", VectorClock: vectorclock.New()}); err == nil || !strings.Contains(err.Error(), "disk on fire") {
		t.Errorf("expected MergeReplicated to return the persister's error, got %v", err)
	}
	ds.RestoreVersions("foo", nil) // logged; must not panic

	p.err = nil
	if items, _ := ds.Get("foo"); !reflect.DeepEqual(values(items), []any{"bar"}) {
		t.Fatalf("expected the failed calls to have changed nothing, got %v", values(items))
	}

	// Delete's write failing after a successful read is reported the same way.
	ds2 := NewDataStoreWithPersister("node-1", &failingWritePersister{fakePersister: p})
	if ok, msg := ds2.Delete("foo", nil); ok || !strings.HasPrefix(msg, "storage error: ") {
		t.Errorf("expected Delete to report a failed tombstone write, got %v %q", ok, msg)
	}
}

// failingWritePersister reads normally but fails every Put.
type failingWritePersister struct{ *fakePersister }

func (f *failingWritePersister) Put(string, *model.DataItem) error {
	return errors.New("write failed")
}

func TestIncompleteRestoreIsLoggedNotFatal(t *testing.T) {
	p := newFakePersister()
	p.restoreErr = fmt.Errorf("engine: restore of key %q: %w", "foo", ErrRestoreIncomplete)
	ds := NewDataStoreWithPersister("node-1", p)
	ds.Put("foo", "bar", nil)

	ds.RestoreVersions("foo", nil) // logged as a warning; must not panic
}
