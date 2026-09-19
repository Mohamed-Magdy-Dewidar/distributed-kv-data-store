package main

import (
	"sync"
	"testing"
)

func TestBasicPutGet(t *testing.T) {
	ds := NewDataStore("node-1")
	Put(ds, "foo", "bar", nil)

	items, ok := get(ds, "foo")
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
	Put(ds, "foo", "bar", nil)
	Put(ds, "foo", "baz", nil)

	items, _ := get(ds, "foo")
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
	items, ok := get(ds, "does-not-exist")
	if ok {
		t.Errorf("expected ok=false for missing key")
	}
	if items != nil {
		t.Errorf("expected nil items for missing key, got %v", items)
	}
}

func TestDeleteTombstoneVsGetLiveItems(t *testing.T) {
	ds := NewDataStore("node-1")
	Put(ds, "foo", "bar", nil)

	deleted, _ := Delete(ds, "foo", nil)
	if !deleted {
		t.Fatalf("expected delete to succeed")
	}

	rawItems, rawOk := get(ds, "foo")
	if !rawOk || len(rawItems) != 1 || !rawItems[0].IsDeleted {
		t.Fatalf("expected raw get to still see 1 tombstoned item, got ok=%v items=%v", rawOk, rawItems)
	}

	liveItems, liveOk := getLiveItems(ds, "foo")
	if liveOk {
		t.Errorf("expected getLiveItems ok=false after delete")
	}
	if len(liveItems) != 0 {
		t.Errorf("expected 0 live items after delete, got %d", len(liveItems))
	}
}

func TestDeleteMissingKey(t *testing.T) {
	ds := NewDataStore("node-1")
	deleted, msg := Delete(ds, "never-existed", nil)
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
			Put(ds, "counter", n, nil)
		}(i)
	}
	wg.Wait()

	items, _ := get(ds, "counter")
	if len(items) != 1 {
		t.Fatalf("expected 1 surviving item after 50 same-node writers, got %d", len(items))
	}
	if vc := items[0].VectorClock.Snapshot(); vc["node-1"] != 50 {
		t.Errorf("expected vc[node-1]=50, got %v", vc)
	}
}

func TestVectorClockRelations(t *testing.T) {
	t.Run("equal", func(t *testing.T) {
		a := NewVectorClock()
		a.Increment("s0")
		b := NewVectorClock()
		b.Increment("s0")
		if rel := a.Compare(b); rel != Equal {
			t.Errorf("expected Equal, got %v", rel)
		}
	})

	t.Run("after", func(t *testing.T) {
		a := NewVectorClock()
		a.Increment("s0")
		a.Increment("s0")
		b := NewVectorClock()
		b.Increment("s0")
		if rel := a.Compare(b); rel != After {
			t.Errorf("expected After, got %v", rel)
		}
	})

	t.Run("concurrent, repeated to catch map-iteration-order regressions", func(t *testing.T) {
		x := &VectorClock{state: map[string]uint32{"s0": 1, "s1": 2}}
		y := &VectorClock{state: map[string]uint32{"s0": 2, "s1": 1}}
		for i := 0; i < 20; i++ {
			if rel := x.Compare(y); rel != Concurrent {
				t.Fatalf("run %d: expected Concurrent, got %v", i, rel)
			}
		}
	})
}

func TestResolveHandConstructedSiblings(t *testing.T) {
	base := map[string]uint32{"node-1": 1, "node-2": 1}
	itemA := &DataItem{Value: "value-from-node-1", VectorClock: buildVectorClockFromContext(base, "node-1")}
	itemB := &DataItem{Value: "value-from-node-2", VectorClock: buildVectorClockFromContext(base, "node-2")}

	if rel := itemA.VectorClock.Compare(itemB.VectorClock); rel != Concurrent {
		t.Fatalf("expected Concurrent, got %v", rel)
	}

	siblings := resolve([]*DataItem{itemA}, itemB)
	if len(siblings) != 2 {
		t.Fatalf("expected 2 siblings, got %d", len(siblings))
	}

	mergedContext := unionVectorClock(siblings)
	resolved := &DataItem{Value: "client-merged-value", VectorClock: buildVectorClockFromContext(mergedContext, "node-1")}
	final := resolve(siblings, resolved)

	if len(final) != 1 {
		t.Fatalf("expected merged write to collapse siblings to 1, got %d", len(final))
	}
	if final[0].Value != "client-merged-value" {
		t.Errorf("expected merged value to survive, got %v", final[0].Value)
	}
}

// --- Replication tests: this is the new part — conflicts arising from
// real independent writes via Node.Replicate, not hand-built DataItems. ---

func TestReplicationCreatesSiblingsNaturally(t *testing.T) {
	node1 := NewNode("node-1", "addr-1")
	node2 := NewNode("node-2", "addr-2")

	// Simulate a partition: each node writes independently, with no
	// knowledge of the other's write.
	Put(node1.dataStore, "foo", "from-node1", nil)
	Put(node2.dataStore, "foo", "from-node2", nil)

	node1Items, _ := get(node1.dataStore, "foo")
	if len(node1Items) != 1 {
		t.Fatalf("expected node1 to have exactly its own item before replication, got %d", len(node1Items))
	}

	// Replicate node1's item into node2.
	node1.Replicate(node2, "foo", node1Items[0])

	node2Items, _ := get(node2.dataStore, "foo")
	if len(node2Items) != 2 {
		t.Fatalf("expected 2 siblings on node2 after replicating a genuine concurrent write, got %d", len(node2Items))
	}
}

func TestReplicationConvergesBothDirections(t *testing.T) {
	node1 := NewNode("node-1", "addr-1")
	node2 := NewNode("node-2", "addr-2")

	Put(node1.dataStore, "foo", "from-node1", nil)
	Put(node2.dataStore, "foo", "from-node2", nil)

	node1Items, _ := get(node1.dataStore, "foo")
	node2Items, _ := get(node2.dataStore, "foo")

	// Cross-replicate: each node learns about the other's original write.
	node1.Replicate(node2, "foo", node1Items[0])
	node2.Replicate(node1, "foo", node2Items[0])

	finalNode1, _ := get(node1.dataStore, "foo")
	finalNode2, _ := get(node2.dataStore, "foo")

	if len(finalNode1) != 2 || len(finalNode2) != 2 {
		t.Fatalf("expected both replicas to converge to 2 siblings, got node1=%d node2=%d",
			len(finalNode1), len(finalNode2))
	}

	// Convergence check: both nodes should agree on the *set* of values,
	// even though they arrived at it independently.
	valuesOf := func(items []*DataItem) map[string]bool {
		set := make(map[string]bool)
		for _, it := range items {
			set[it.Value.(string)] = true
		}
		return set
	}
	v1, v2 := valuesOf(finalNode1), valuesOf(finalNode2)
	for val := range v1 {
		if !v2[val] {
			t.Errorf("node1 has value %q that node2's sibling set is missing", val)
		}
	}
	for val := range v2 {
		if !v1[val] {
			t.Errorf("node2 has value %q that node1's sibling set is missing", val)
		}
	}
}
