package node

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/store"
)

func sortedValues(items []*model.DataItem) []any {
	vals := itemValues(items)
	sort.Slice(vals, func(i, j int) bool { return vals[i].(string) < vals[j].(string) })
	return vals
}

// TestReconcileDoesNotClobberAWriteLandingAfterItsReads forces the race
// the old Get+merge+RestoreVersions install had: node-1 and node-2 hold
// concurrent versions of k; after reconcileBucket has read both, node-1
// takes a new local write superseding its own version. Installing the
// merge of the stale reads verbatim would erase that write and resurrect
// the version it replaced. Merging node-2's version in instead keeps it.
func TestReconcileDoesNotClobberAWriteLandingAfterItsReads(t *testing.T) {
	node1 := startTestNode(t, "node-1", "localhost:60511", map[string]string{"node-2": "localhost:60512"})
	node2 := startTestNode(t, "node-2", "localhost:60512", map[string]string{"node-1": "localhost:60511"})
	node1.Store.Put("k", "old-node-1", nil)
	node2.Store.Put("k", "from-node-2", nil) // concurrent with node-1's

	testHookBeforeReconcileInstall = func(key string) {
		if key == "k" {
			node1.Store.Put("k", "late-node-1", nil) // supersedes old-node-1
		}
	}
	defer func() { testHookBeforeReconcileInstall = nil }()

	if err := reconcileKey(t, node1, "k"); err != nil {
		t.Fatalf("reconcileBucket failed: %v", err)
	}
	testHookBeforeReconcileInstall = nil

	items, _, _ := node1.Store.Get("k")
	if got, want := sortedValues(items), []any{"from-node-2", "late-node-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("expected node-1 to keep its late write beside node-2's version %v, got %v", want, got)
	}

	// A second round converges node-2 too: it gets the late write, which
	// drops the old-node-1 version the first round pushed.
	if err := reconcileKey(t, node1, "k"); err != nil {
		t.Fatalf("second reconcileBucket failed: %v", err)
	}
	for id, nd := range map[string]*Node{"node-1": node1, "node-2": node2} {
		items, _, _ := nd.Store.Get("k")
		if got, want := sortedValues(items), []any{"from-node-2", "late-node-1"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("expected %s to converge on %v, got %v", id, want, got)
		}
	}
}

// TestReconcileSendsNothingForAKeyAlreadyInSync: when both sides already
// hold the same versions, nothing is installed on either side. node-2's
// store fails every write, so any push to it fails the reconcile, as does
// any install on node-1.
func TestReconcileSendsNothingForAKeyAlreadyInSync(t *testing.T) {
	p1, p2 := newFlakyPersister(), newFlakyPersister()
	node1, _ := reconcileSetup(t, p1, 60521)

	// reconcileSetup's node-2 already serves an in-memory store; this one
	// serves p2 on its own port so the fault reaches its gRPC handler.
	node2 := New("node-2", "localhost:60523", 2, 2, 1, map[string]string{"node-1": "localhost:60521"})
	node2.Store = store.NewDataStoreWithPersister("node-2", p2)
	listener, err := rpc.Serve("localhost:60523", node2.Store, node2)
	if err != nil {
		t.Fatalf("serve node-2: %v", err)
	}
	t.Cleanup(listener.Stop)
	node1.NeighborAddrs["node-2"] = "localhost:60523"

	item := node1.Store.Put("k", "in-sync", nil)
	if err := node1.Replicate(context.Background(), "node-2", "k", []*model.DataItem{item}); err != nil {
		t.Fatalf("setup Replicate failed: %v", err)
	}

	p1.fail(faults{put: errDisk})
	p2.fail(faults{put: errDisk})
	if err := reconcileKey(t, node1, "k"); err != nil {
		t.Fatalf("expected an in-sync key to send nothing either way, got %v", err)
	}
}
