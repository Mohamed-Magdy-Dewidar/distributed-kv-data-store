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

// newFakeStore returns a DataStore for id on a fresh fakePersister.
func newFakeStore(id string) *DataStore {
	return NewDataStoreWithPersister(id, newFakePersister())
}

func values(items []*model.DataItem) []any {
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, item.Value)
	}
	return out
}

func TestPutGetAndSequentialUpdate(t *testing.T) {
	ds := newFakeStore("node-1")
	ds.Put("foo", "bar", nil)
	ds.Put("foo", "baz", nil) // no context: builds on what's there, so it supersedes

	items, found, _ := ds.Get("foo")
	if !found || !reflect.DeepEqual(values(items), []any{"baz"}) {
		t.Fatalf("expected [baz], got found=%v %v", found, values(items))
	}
	if vc := items[0].VectorClock.Snapshot(); vc["node-1"] != 2 {
		t.Errorf("expected vc[node-1]=2, got %v", vc)
	}
	if _, found, _ := ds.Get("missing"); found {
		t.Error("expected a missing key to be not found")
	}
}

// TestSiblingsComeBackOldestFirst: the persister returns siblings newest
// first; DataStore must present them oldest first.
func TestSiblingsComeBackOldestFirst(t *testing.T) {
	base := map[string]uint32{"node-1": 1}
	ds := newFakeStore("node-1")
	ds.Put("foo", "first", base)
	for _, item := range []*model.DataItem{
		{Value: "second", VectorClock: vectorclock.BuildFromContext(base, "node-2")},
		{Value: "third", VectorClock: vectorclock.BuildFromContext(base, "node-3")},
	} {
		if err := ds.MergeReplicated("foo", item); err != nil {
			t.Fatalf("expected MergeReplicated to succeed, got %v", err)
		}
	}

	items, _, _ := ds.Get("foo")
	if want := []any{"first", "second", "third"}; !reflect.DeepEqual(values(items), want) {
		t.Fatalf("expected siblings oldest first %v, got %v", want, values(items))
	}
}

func TestDeleteAndLiveItems(t *testing.T) {
	ds := newFakeStore("node-1")
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
	raw, found, _ := ds.Get("foo")
	if !found || len(raw) != 1 || !raw[0].IsDeleted {
		t.Fatalf("expected Get to return the tombstone, got found=%v %v", found, raw)
	}
	if keys, _ := ds.Keys(); !reflect.DeepEqual(keys, []string{"foo"}) {
		t.Errorf("expected Keys to include the tombstoned key, got %v", keys)
	}
}

func TestRestoreVersionsReplacesOrRemoves(t *testing.T) {
	ds := newFakeStore("node-1")
	ds.Put("foo", "v1", nil)
	prev, _, _ := ds.Get("foo")
	ds.Put("foo", "v2", nil)

	ds.RestoreVersions("foo", prev)
	if items, _, _ := ds.Get("foo"); !reflect.DeepEqual(values(items), []any{"v1"}) {
		t.Fatalf("expected rollback to restore [v1], got %v", values(items))
	}

	ds.RestoreVersions("foo", nil)
	if _, found, _ := ds.Get("foo"); found {
		t.Fatal("expected RestoreVersions(nil) to remove the key")
	}
	if keys, _ := ds.Keys(); len(keys) != 0 {
		t.Errorf("expected no keys after removal, got %v", keys)
	}
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

// TestPersisterErrorsNeverLookLikeSuccess: every method must make a failed
// persist visible — Get, Keys, MergeReplicated and RestoreVersions by
// returning the error (never a not-found or empty result in its place),
// the rest through their in-band signals.
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
	if items, found, err := ds.Get("foo"); err == nil || !strings.Contains(err.Error(), "disk on fire") || found || items != nil {
		t.Errorf("expected Get to return the persister's error, got found=%v %v err=%v", found, items, err)
	}
	if items, found := ds.GetLiveItems("foo"); found || items != nil {
		t.Errorf("expected GetLiveItems to report not found, got found=%v %v", found, items)
	}
	if ok, msg := ds.Delete("foo", nil); ok || !strings.HasPrefix(msg, "storage error: ") || !strings.Contains(msg, "disk on fire") {
		t.Errorf("expected Delete to report the storage error, got %v %q", ok, msg)
	}
	if keys, err := ds.Keys(); err == nil || !strings.Contains(err.Error(), "disk on fire") || keys != nil {
		t.Errorf("expected Keys to return the persister's error, got %v err=%v", keys, err)
	}
	if err := ds.MergeReplicated("foo", &model.DataItem{Value: "x", VectorClock: vectorclock.New()}); err == nil || !strings.Contains(err.Error(), "disk on fire") {
		t.Errorf("expected MergeReplicated to return the persister's error, got %v", err)
	}
	if err := ds.RestoreVersions("foo", nil); err == nil || !strings.Contains(err.Error(), "disk on fire") {
		t.Errorf("expected RestoreVersions to return the persister's error, got %v", err)
	}

	p.err = nil
	if items, _, _ := ds.Get("foo"); !reflect.DeepEqual(values(items), []any{"bar"}) {
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

func TestIncompleteRestoreIsReturned(t *testing.T) {
	p := newFakePersister()
	p.restoreErr = fmt.Errorf("engine: restore of key %q: %w", "foo", ErrRestoreIncomplete)
	ds := NewDataStoreWithPersister("node-1", p)
	ds.Put("foo", "bar", nil)

	if err := ds.RestoreVersions("foo", nil); !errors.Is(err, ErrRestoreIncomplete) {
		t.Fatalf("expected an error wrapping ErrRestoreIncomplete, got %v", err)
	}
}
