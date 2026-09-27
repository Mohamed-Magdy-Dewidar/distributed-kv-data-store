package node

import (
	"context"
	"fmt"
	"slices"
	"testing"
)

// keyReplicatedBy returns a key whose preference list on n is exactly the
// given nodes, in any order.
func keyReplicatedBy(t *testing.T, n *Node, prefix string, want ...string) string {
	t.Helper()
	for i := range 10000 {
		key := fmt.Sprintf("%s-%d", prefix, i)
		got := n.Ring.GetPreferenceList(key, n.QuorumConfig.N)
		if len(got) == len(want) && !slices.ContainsFunc(want, func(id string) bool { return !slices.Contains(got, id) }) {
			return key
		}
	}
	t.Fatalf("no key found whose preference list is %v", want)
	return ""
}

// TestAntiEntropySkipsKeysThePeerDoesNotReplicate: in a 3-node, N=2
// cluster, node-1 reconciling with node-2 must converge the keys they both
// replicate, and leave alone every key only one of them replicates — in
// both directions — instead of copying it onto a node outside its
// preference list.
func TestAntiEntropySkipsKeysThePeerDoesNotReplicate(t *testing.T) {
	addrs := map[string]string{
		"node-1": "localhost:60501",
		"node-2": "localhost:60502",
		"node-3": "localhost:60503",
	}
	n2 := quorumOverride{n: 2, w: 1, r: 1}
	nodes, _ := startTestCluster(t, addrs, map[string]quorumOverride{"node-1": n2, "node-2": n2, "node-3": n2})
	node1, node2 := nodes["node-1"], nodes["node-2"]

	shared := keyReplicatedBy(t, node1, "shared", "node-1", "node-2")
	node1Only := keyReplicatedBy(t, node1, "n1n3", "node-1", "node-3") // node-2 isn't a replica
	node2Only := keyReplicatedBy(t, node1, "n2n3", "node-2", "node-3") // node-1 isn't a replica

	// Stored directly, bypassing Put's routing: this is the divergence
	// anti-entropy sees, whatever put it there.
	node1.Store.Put(shared, "shared-value", nil)
	node1.Store.Put(node1Only, "node-1-value", nil)
	node2.Store.Put(node2Only, "node-2-value", nil)

	if err := node1.RunAntiEntropy(context.Background(), "node-2"); err != nil {
		t.Fatalf("RunAntiEntropy failed: %v", err)
	}

	if items, found, _ := node2.Store.Get(shared); !found || len(items) != 1 || items[0].Value != "shared-value" {
		t.Fatalf("expected shared key %q to converge on node-2, got found=%v %v", shared, found, items)
	}
	if items, found, _ := node2.Store.Get(node1Only); found {
		t.Fatalf("key %q isn't replicated by node-2 but was pushed to it: %v", node1Only, items)
	}
	if items, found, _ := node1.Store.Get(node2Only); found {
		t.Fatalf("key %q isn't replicated by node-1 but was pulled onto it: %v", node2Only, items)
	}
}
