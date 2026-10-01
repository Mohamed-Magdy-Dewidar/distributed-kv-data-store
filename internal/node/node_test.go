package node

import (
	"context"
	"testing"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/storetest"
)

// newTestNode builds node id with New on a fresh t.TempDir(), with the
// memtable size storetest uses. It is closed when the test ends: after
// anything registered later (a listener serving it stops first) and before
// its directory is removed, which Windows can do only once its files are
// closed.
func newTestNode(t testing.TB, id, address string, n, w, r int, neighbors map[string]string) *Node {
	t.Helper()
	dir := t.TempDir()
	nd, err := New(id, address, n, w, r, neighbors, dir, storetest.MemtableBytes)
	if err != nil {
		t.Fatalf("New(%s): %v", id, err)
	}
	t.Cleanup(func() {
		if err := nd.Close(); err != nil {
			t.Errorf("close %s: %v", id, err)
		}
	})
	return nd
}

func startTestNode(t *testing.T, id, address string, neighbors map[string]string) *Node {
	t.Helper()

	// N=2: full replication across this 2-node test cluster, preserving
	// these pre-partitioning tests' original "every node has every key"
	// assumption — none of them go through replicaSetFor's hash-ring
	// selection anyway (they call Store.Put/Replicate/FetchItem directly).
	n := newTestNode(t, id, address, 2, 2, 1, neighbors)

	serveAt(t, address, n.Store, n)

	return n
}

func TestReplicationCreatesSiblingsNaturally(t *testing.T) {
	// TestReplicationCreatesSibling
	a := reserveAddrs(t, "node-1", "node-2")
	node1 := startTestNode(t, "node-1", a["node-1"], map[string]string{
		"node-2": a["node-2"],
	})
	node2 := startTestNode(t, "node-2", a["node-2"], map[string]string{
		"node-1": a["node-1"],
	})

	node1.Store.Put("foo", "from-node1", nil)
	node2.Store.Put("foo", "from-node2", nil)

	node1Items, _, _ := node1.Store.Get("foo")
	if len(node1Items) != 1 {
		t.Fatalf("expected node1 to have exactly its own item before replication, got %d", len(node1Items))
	}

	ctx := context.Background()
	if err := node1.Replicate(ctx, "node-2", "foo", node1Items[:1]); err != nil {
		t.Fatalf("Replicate failed: %v", err)
	}

	node2Items, _, _ := node2.Store.Get("foo")
	if len(node2Items) != 2 {
		t.Fatalf("expected 2 siblings on node2 after replicating a genuine concurrent write, got %d", len(node2Items))
	}
}

func TestReplicationConvergesBothDirections(t *testing.T) {
	a := reserveAddrs(t, "node-1", "node-2")
	node1 := startTestNode(t, "node-1", a["node-1"], map[string]string{
		"node-2": a["node-2"],
	})
	node2 := startTestNode(t, "node-2", a["node-2"], map[string]string{
		"node-1": a["node-1"],
	})

	node1.Store.Put("foo", "from-node1", nil)
	node2.Store.Put("foo", "from-node2", nil)

	node1Items, _, _ := node1.Store.Get("foo")
	node2Items, _, _ := node2.Store.Get("foo")

	ctx := context.Background()
	if err := node1.Replicate(ctx, "node-2", "foo", node1Items[:1]); err != nil {
		t.Fatalf("node1->node2 Replicate failed: %v", err)
	}
	if err := node2.Replicate(ctx, "node-1", "foo", node2Items[:1]); err != nil {
		t.Fatalf("node2->node1 Replicate failed: %v", err)
	}

	finalNode1, _, _ := node1.Store.Get("foo")
	finalNode2, _, _ := node2.Store.Get("foo")

	if len(finalNode1) != 2 || len(finalNode2) != 2 {
		t.Fatalf("expected both replicas to converge to 2 siblings, got node1=%d node2=%d",
			len(finalNode1), len(finalNode2))
	}

	valuesOf := func(items []*model.DataItem) map[string]bool {
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
	a := reserveAddrs(t, "node-1", "node-2")
	node1 := startTestNode(t, "node-1", a["node-1"], map[string]string{
		"node-2": a["node-2"],
	})
	_ = startTestNode(t, "node-2", a["node-2"], map[string]string{
		"node-1": a["node-1"],
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
