package node

import (
	"distributed-kv-datastore/internal/store"
	"testing"
)

func TestReplicationCreatesSiblingsNaturally(t *testing.T) {
	node1 := New("node-1", "addr-1")
	node2 := New("node-2", "addr-2")

	// Simulate a partition: each node writes independently, with no
	// knowledge of the other's write.
	node1.Store.Put("foo", "from-node1", nil)
	node2.Store.Put("foo", "from-node2", nil)

	node1Items, _ := node1.Store.Get("foo")
	if len(node1Items) != 1 {
		t.Fatalf("expected node1 to have exactly its own item before replication, got %d", len(node1Items))
	}

	// Replicate node1's item into node2.
	node1.Replicate(node2, "foo", node1Items[0])

	node2Items, _ := node2.Store.Get("foo")
	if len(node2Items) != 2 {
		t.Fatalf("expected 2 siblings on node2 after replicating a genuine concurrent write, got %d", len(node2Items))
	}
}

func TestReplicationConvergesBothDirections(t *testing.T) {
	node1 := New("node-1", "addr-1")
	node2 := New("node-2", "addr-2")

	node1.Store.Put("foo", "from-node1", nil)
	node2.Store.Put("foo", "from-node2", nil)

	node1Items, _ := node1.Store.Get("foo")
	node2Items, _ := node2.Store.Get("foo")

	// Cross-replicate: each node learns about the other's original write.
	node1.Replicate(node2, "foo", node1Items[0])
	node2.Replicate(node1, "foo", node2Items[0])

	finalNode1, _ := node1.Store.Get("foo")
	finalNode2, _ := node2.Store.Get("foo")

	if len(finalNode1) != 2 || len(finalNode2) != 2 {
		t.Fatalf("expected both replicas to converge to 2 siblings, got node1=%d node2=%d",
			len(finalNode1), len(finalNode2))
	}

	// Convergence check: both nodes should agree on the *set* of values,
	// even though they arrived at it independently.
	valuesOf := func(items []*store.DataItem) map[string]bool {
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
