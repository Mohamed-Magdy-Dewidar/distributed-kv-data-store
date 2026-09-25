package store

import (
	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/vectorclock"
	"sync"
	"testing"
)

func TestBasicPutGet(t *testing.T) {
	ds := NewDataStore("node-1")
	ds.Put("foo", "bar", nil)

	items, ok := ds.Get("foo")
	if !ok {
		t.Fatalf("expected key to exist")
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].Value != "bar" {
		t.Errorf("expected value 'bar', got %v", items[0].Value)
	}
	vc := items[0].VectorClock.Snapshot()
	if vc["node-1"] != 1 {
		t.Errorf("expected vc[node-1]=1, got %v", vc)
	}
}

func TestSequentialUpdateSameNodeDoesNotCreateSibling(t *testing.T) {
	ds := NewDataStore("node-1")
	ds.Put("foo", "bar", nil)
	ds.Put("foo", "baz", nil)

	items, _ := ds.Get("foo")
	if len(items) != 1 {
		t.Fatalf("expected 1 item after sequential same-node update, got %d — regression in nil-context union logic", len(items))
	}
	if items[0].Value != "baz" {
		t.Errorf("expected value 'baz', got %v", items[0].Value)
	}
	if vc := items[0].VectorClock.Snapshot(); vc["node-1"] != 2 {
		t.Errorf("expected vc[node-1]=2, got %v", vc)
	}
}

func TestGetMissingKey(t *testing.T) {
	ds := NewDataStore("node-1")
	items, ok := ds.Get("does-not-exist")
	if ok {
		t.Errorf("expected ok=false for missing key")
	}
	if items != nil {
		t.Errorf("expected nil items for missing key, got %v", items)
	}
}

func TestDeleteTombstoneVsGetLiveItems(t *testing.T) {
	ds := NewDataStore("node-1")
	ds.Put("foo", "bar", nil)

	deleted, _ := ds.Delete("foo", nil)
	if !deleted {
		t.Fatalf("expected delete to succeed")
	}

	rawItems, rawOk := ds.Get("foo")
	if !rawOk || len(rawItems) != 1 || !rawItems[0].IsDeleted {
		t.Fatalf("expected raw get to still see 1 tombstoned item, got ok=%v items=%v", rawOk, rawItems)
	}

	liveItems, liveOk := ds.GetLiveItems("foo")
	if liveOk {
		t.Errorf("expected GetLiveItems ok=false after delete")
	}
	if len(liveItems) != 0 {
		t.Errorf("expected 0 live items after delete, got %d", len(liveItems))
	}
}

func TestDeleteMissingKey(t *testing.T) {
	ds := NewDataStore("node-1")
	deleted, msg := ds.Delete("never-existed", nil)
	if deleted {
		t.Errorf("expected delete of missing key to fail")
	}
	if msg != "Key not found" {
		t.Errorf("unexpected message: %q", msg)
	}
}

func TestConcurrentWritersSameNodeSerialize(t *testing.T) {
	ds := NewDataStore("node-1")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ds.Put("counter", n, nil)
		}(i)
	}
	wg.Wait()

	items, _ := ds.Get("counter")
	if len(items) != 1 {
		t.Fatalf("expected 1 surviving item after 50 same-node writers, got %d", len(items))
	}
	if vc := items[0].VectorClock.Snapshot(); vc["node-1"] != 50 {
		t.Errorf("expected vc[node-1]=50, got %v", vc)
	}
}

func TestMergeReplicatedCreatesSiblingsOnGenuineConflict(t *testing.T) {
	// This proves DataStore.MergeReplicated itself (not just the pure
	// resolve() function) correctly creates siblings end-to-end through
	// its own locking, matching what Node.Replicate will call in practice.
	ds := NewDataStore("node-2")
	base := map[string]uint32{"node-1": 1, "node-2": 1}

	ds.store["foo"] = []*model.DataItem{
		{Value: "local-value", VectorClock: vectorclock.BuildFromContext(base, "node-2")},
	}

	incoming := &model.DataItem{
		Value:       "remote-value",
		VectorClock: vectorclock.BuildFromContext(base, "node-1"),
	}
	ds.MergeReplicated("foo", incoming)

	items, _ := ds.Get("foo")
	if len(items) != 2 {
		t.Fatalf("expected 2 siblings after MergeReplicated with a genuine concurrent item, got %d", len(items))
	}
}
