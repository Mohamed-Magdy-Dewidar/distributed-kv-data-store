package node

import (
	"context"
	"testing"
)

func TestAntiEntropyConvergesAfterMissedReplication(t *testing.T) {
	addrs := map[string]string{
		"node-1": "localhost:60301",
		"node-2": "localhost:60302",
	}
	nodes, _ := startTestCluster(t, addrs, nil)

	nodes["node-1"].Store.Put("orphaned-key", "value-from-node-1", nil)

	if items, found := nodes["node-2"].Store.Get("orphaned-key"); found {
		t.Fatalf("test setup invalid: node-2 should not have the key yet, got %v", items)
	}

	ctx := context.Background()
	if err := nodes["node-1"].RunAntiEntropy(ctx, "node-2"); err != nil {
		t.Fatalf("RunAntiEntropy failed: %v", err)
	}

	items, found := nodes["node-2"].Store.Get("orphaned-key")
	if !found || len(items) != 1 || items[0].Value != "value-from-node-1" {
		t.Fatalf("expected node-2 to have converged after anti-entropy, got found=%v items=%v", found, items)
	}
}

func TestAntiEntropyIsIdempotentWhenAlreadyInSync(t *testing.T) {
	addrs := map[string]string{
		"node-1": "localhost:60311",
		"node-2": "localhost:60312",
	}
	nodes, _ := startTestCluster(t, addrs, nil)

	ctx := context.Background()
	if err := nodes["node-1"].Put(ctx, "foo", "bar", nil); err != nil {
		t.Fatalf("setup Put failed: %v", err)
	}

	if err := nodes["node-1"].RunAntiEntropy(ctx, "node-2"); err != nil {
		t.Fatalf("first RunAntiEntropy failed: %v", err)
	}
	if err := nodes["node-1"].RunAntiEntropy(ctx, "node-2"); err != nil {
		t.Fatalf("second RunAntiEntropy (already in sync) failed: %v", err)
	}

	items, found := nodes["node-2"].Store.Get("foo")
	if !found || len(items) != 1 || items[0].Value != "bar" {
		t.Fatalf("expected foo=bar on node-2 after convergence, got found=%v items=%v", found, items)
	}
}

func TestAntiEntropyReconcilesGenuineSiblingsFromBothSides(t *testing.T) {
	addrs := map[string]string{
		"node-1": "localhost:60321",
		"node-2": "localhost:60322",
	}
	nodes, _ := startTestCluster(t, addrs, nil)

	nodes["node-1"].Store.Put("contested-key", "from-node-1", nil)
	nodes["node-2"].Store.Put("contested-key", "from-node-2", nil)

	ctx := context.Background()
	if err := nodes["node-1"].RunAntiEntropy(ctx, "node-2"); err != nil {
		t.Fatalf("RunAntiEntropy failed: %v", err)
	}

	items1, _ := nodes["node-1"].Store.Get("contested-key")
	items2, _ := nodes["node-2"].Store.Get("contested-key")

	if len(items1) != 2 || len(items2) != 2 {
		t.Fatalf("expected both nodes to hold 2 siblings after reconciling a genuine conflict, got node1=%d node2=%d",
			len(items1), len(items2))
	}
}
