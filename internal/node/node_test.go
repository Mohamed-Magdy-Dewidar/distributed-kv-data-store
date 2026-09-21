package node

import (
	"context"
	"testing"

	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/store"
)

func startTestNode(t *testing.T, id, address string, neighbors map[string]string) *Node {
	t.Helper()

	n := New(id, address, 2, 1, neighbors)

	listener, err := rpc.Serve(address, n.Store)
	if err != nil {
		t.Fatalf("failed to start server for %s: %v", id, err)
	}
	t.Cleanup(listener.Stop)

	return n
}

func TestReplicationCreatesSiblingsNaturally(t *testing.T) {
	// TestReplicationCreatesSibling
	node1 := startTestNode(t, "node-1", "localhost:60101", map[string]string{
		"node-2": "localhost:60102",
	})
	node2 := startTestNode(t, "node-2", "localhost:60102", map[string]string{
		"node-1": "localhost:60101",
	})

	node1.Store.Put("foo", "from-node1", nil)
	node2.Store.Put("foo", "from-node2", nil)

	node1Items, _ := node1.Store.Get("foo")
	if len(node1Items) != 1 {
		t.Fatalf("expected node1 to have exactly its own item before replication, got %d", len(node1Items))
	}

	ctx := context.Background()
	if err := node1.Replicate(ctx, "node-2", "foo", node1Items[0]); err != nil {
		t.Fatalf("Replicate failed: %v", err)
	}

	node2Items, _ := node2.Store.Get("foo")
	if len(node2Items) != 2 {
		t.Fatalf("expected 2 siblings on node2 after replicating a genuine concurrent write, got %d", len(node2Items))
	}
}

func TestReplicationConvergesBothDirections(t *testing.T) {
	node1 := startTestNode(t, "node-1", "localhost:60111", map[string]string{
		"node-2": "localhost:60112",
	})
	node2 := startTestNode(t, "node-2", "localhost:60112", map[string]string{
		"node-1": "localhost:60111",
	})

	node1.Store.Put("foo", "from-node1", nil)
	node2.Store.Put("foo", "from-node2", nil)

	node1Items, _ := node1.Store.Get("foo")
	node2Items, _ := node2.Store.Get("foo")

	ctx := context.Background()
	if err := node1.Replicate(ctx, "node-2", "foo", node1Items[0]); err != nil {
		t.Fatalf("node1->node2 Replicate failed: %v", err)
	}
	if err := node2.Replicate(ctx, "node-1", "foo", node2Items[0]); err != nil {
		t.Fatalf("node2->node1 Replicate failed: %v", err)
	}

	finalNode1, _ := node1.Store.Get("foo")
	finalNode2, _ := node2.Store.Get("foo")

	if len(finalNode1) != 2 || len(finalNode2) != 2 {
		t.Fatalf("expected both replicas to converge to 2 siblings, got node1=%d node2=%d",
			len(finalNode1), len(finalNode2))
	}

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

func TestFetchItem(t *testing.T) {
	node1 := startTestNode(t, "node-1", "localhost:60121", map[string]string{
		"node-2": "localhost:60122",
	})
	_ = startTestNode(t, "node-2", "localhost:60122", map[string]string{
		"node-1": "localhost:60121",
	})

	ctx := context.Background()
	items, found, err := node1.FetchItem(ctx, "node-2", "does-not-exist")
	if err != nil {
		t.Fatalf("FetchItem failed: %v", err)
	}
	if found {
		t.Errorf("expected found=false for a key node2 never received")
	}
	if items != nil {
		t.Errorf("expected nil items, got %v", items)
	}
}
