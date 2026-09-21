package node

import (
	"context"
	"testing"
	"time"

	"distributed-kv-datastore/internal/rpc"
)

// quorumOverride specifies non-default W/R for one node in a test cluster.
type quorumOverride struct {
	w, r int
}

// startTestCluster starts a full-mesh cluster of len(addrs) nodes, one per
// id->address entry in addrs — every node is a neighbor of every other.
// Every node gets N=len(addrs) (full replication across the whole test
// cluster) — these tests predate partitioning and were designed/reasoned
// about assuming every node holds every key; N=3 keeps replicaSetFor
// returning the same "every other node" set the old flat neighbor list
// gave them, so quorum math already reasoned about by name (e.g. "W=2
// requires 2 of 3 nodes up") still holds unchanged.
// Unless overrides[id] says otherwise, a node gets W=2, R=1 (mirroring
// startTestNode's default); these tests only care about the *coordinating*
// node's quorum settings; peers that never initiate a Put/Get don't need an
// entry.
//
// Listeners are returned (keyed by id) so a test can Stop() one mid-test to
// simulate that node going down. Stop is also registered via t.Cleanup so
// tests that don't explicitly stop a node still get torn down — Stop is
// safe to call twice.
func startTestCluster(t *testing.T, addrs map[string]string, overrides map[string]quorumOverride) (map[string]*Node, map[string]*rpc.Listener) {
	t.Helper()

	nodes := make(map[string]*Node, len(addrs))
	listeners := make(map[string]*rpc.Listener, len(addrs))

	for id, addr := range addrs {
		neighbors := make(map[string]string, len(addrs)-1)
		for peerID, peerAddr := range addrs {
			if peerID != id {
				neighbors[peerID] = peerAddr
			}
		}

		w, r := 2, 1
		if o, ok := overrides[id]; ok {
			w, r = o.w, o.r
		}

		n := New(id, addr, len(addrs), w, r, neighbors)
		listener, err := rpc.Serve(addr, n.Store)
		if err != nil {
			t.Fatalf("failed to start server for %s: %v", id, err)
		}
		t.Cleanup(listener.Stop)

		nodes[id] = n
		listeners[id] = listener
	}

	return nodes, listeners
}

// N=3, W=2. Stopping one non-coordinator node leaves 2 reachable nodes
// (the coordinator's own local write, plus 1 surviving peer) — exactly W —
// so the write must still succeed.
func TestPutQuorumMetDespiteOneNodeDown(t *testing.T) {
	addrs := map[string]string{
		"node-1": "localhost:60201",
		"node-2": "localhost:60202",
		"node-3": "localhost:60203",
	}
	nodes, listeners := startTestCluster(t, addrs, nil)

	listeners["node-3"].Stop()

	ctx := context.Background()
	if err := nodes["node-1"].Put(ctx, "foo", "bar", nil); err != nil {
		t.Fatalf("expected write quorum W=2 to be met with 1/3 nodes down, got error: %v", err)
	}
}

// N=3, W=3 on the coordinator specifically. Stopping one node leaves only
// 2 reachable nodes; 2 < W=3 makes the quorum mathematically unreachable
// the moment the down node's failure is observed, so Put must fail fast via
// the totalNodes-failures<needed early exit — not by blocking until ctx's
// deadline.
func TestPutQuorumNotReachableFailsFast(t *testing.T) {
	addrs := map[string]string{
		"node-1": "localhost:60211",
		"node-2": "localhost:60212",
		"node-3": "localhost:60213",
	}
	nodes, listeners := startTestCluster(t, addrs, map[string]quorumOverride{
		"node-1": {w: 3, r: 1},
	})

	listeners["node-3"].Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	err := nodes["node-1"].Put(ctx, "foo", "bar", nil)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected Put to fail: W=3 requires all 3 nodes, but node-3 is down")
	}
	if ctx.Err() != nil {
		t.Fatalf("Put only returned once the 5s context deadline expired (ctx.Err()=%v) — expected it to fail via the unreachable-quorum check, not the timeout path", ctx.Err())
	}
	if elapsed > time.Second {
		t.Fatalf("Put took %v to fail; expected a fast failure (well under the 5s deadline), proving the early-exit-on-unreachable-quorum path fired", elapsed)
	}
}

// N=3, R=2 on the coordinator. Write while all 3 nodes are up, then stop
// one non-coordinator node, leaving exactly 2 reachable nodes (the
// coordinator's own local read, plus 1 surviving peer) — exactly R — so
// Get must still succeed and return the value just written.
func TestGetQuorumMetDespiteOneNodeDown(t *testing.T) {
	addrs := map[string]string{
		"node-1": "localhost:60221",
		"node-2": "localhost:60222",
		"node-3": "localhost:60223",
	}
	nodes, listeners := startTestCluster(t, addrs, map[string]quorumOverride{
		"node-1": {w: 2, r: 2},
	})

	ctx := context.Background()
	if err := nodes["node-1"].Put(ctx, "foo", "bar", nil); err != nil {
		t.Fatalf("setup Put failed: %v", err)
	}

	listeners["node-3"].Stop()

	items, err := nodes["node-1"].Get(ctx, "foo")
	if err != nil {
		t.Fatalf("expected read quorum R=2 to be met with 1/3 nodes down, got error: %v", err)
	}
	if len(items) != 1 || items[0].Value != "bar" {
		t.Fatalf("expected exactly [%q], got %v", "bar", items)
	}
}

// N=3, R=3 on the coordinator (no node is stopped here — R=3 forces the
// coordinator to wait for every peer's response before returning, so the
// test doesn't race goroutine scheduling to decide whether node-2's
// sibling made it into the merge before quorum was declared met).
// node-1 and node-2 each get a genuinely concurrent write applied directly
// to their local Store (bypassing Node.Put/Replicate entirely — the same
// way TestReplicationCreatesSiblingsNaturally simulates concurrent writes
// on a partitioned cluster). Get from node-1 must fan out over the real
// network to both peers and, via store.MergeSiblings, surface both values
// as siblings — proving the merge works across an actual RPC fan-out, not
// just against in-memory data.
func TestGetSurfacesGenuineSiblingConflictsAcrossNodes(t *testing.T) {
	addrs := map[string]string{
		"node-1": "localhost:60231",
		"node-2": "localhost:60232",
		"node-3": "localhost:60233",
	}
	nodes, _ := startTestCluster(t, addrs, map[string]quorumOverride{
		"node-1": {w: 2, r: 3},
	})

	nodes["node-1"].Store.Put("foo", "from-node1", nil)
	nodes["node-2"].Store.Put("foo", "from-node2", nil)

	ctx := context.Background()
	items, err := nodes["node-1"].Get(ctx, "foo")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if len(items) != 2 {
		t.Fatalf("expected 2 genuine siblings merged across node-1 (local) and node-2 (peer), got %d: %v", len(items), items)
	}

	values := map[string]bool{}
	for _, it := range items {
		values[it.Value.(string)] = true
	}
	if !values["from-node1"] || !values["from-node2"] {
		t.Fatalf("expected siblings {%q, %q}, got %v", "from-node1", "from-node2", values)
	}
}

// N=3, W=2 on the coordinator. Stopping the other 2 nodes leaves only the
// coordinator itself up (1/3), so W=2 is unreachable and Put must fail. The
// bug this guards against: Put applied the local write unconditionally
// before fanning out, and never rolled it back on failure — so a "failed"
// write was left sitting in the local store, indistinguishable from a
// committed one. This is the very first write to "foo", so a correct
// rollback must remove the key entirely rather than leave an orphaned
// version behind.
func TestPutRollsBackLocalWriteWhenQuorumUnreachableFirstWrite(t *testing.T) {
	addrs := map[string]string{
		"node-1": "localhost:60241",
		"node-2": "localhost:60242",
		"node-3": "localhost:60243",
	}
	nodes, listeners := startTestCluster(t, addrs, map[string]quorumOverride{
		"node-1": {w: 2, r: 1},
	})

	listeners["node-2"].Stop()
	listeners["node-3"].Stop()

	ctx := context.Background()
	if err := nodes["node-1"].Put(ctx, "foo", "bar", nil); err == nil {
		t.Fatal("expected Put to fail: W=2 requires 2 nodes, but only the coordinator is up")
	}

	items, found := nodes["node-1"].Store.Get("foo")
	if found || len(items) != 0 {
		t.Fatalf("expected failed first write to be rolled back (key absent), got found=%v items=%v", found, items)
	}
}

// Same setup, but "foo" already has a committed value before the peers go
// down. A failed Put attempting to overwrite it must restore the local
// store to exactly the pre-attempt version, not leave the unacknowledged
// overwrite in place.
func TestPutRollsBackLocalWriteWhenQuorumUnreachableOverwrite(t *testing.T) {
	addrs := map[string]string{
		"node-1": "localhost:60251",
		"node-2": "localhost:60252",
		"node-3": "localhost:60253",
	}
	nodes, listeners := startTestCluster(t, addrs, map[string]quorumOverride{
		"node-1": {w: 2, r: 1},
	})

	ctx := context.Background()
	if err := nodes["node-1"].Put(ctx, "foo", "committed", nil); err != nil {
		t.Fatalf("setup Put failed: %v", err)
	}
	before, _ := nodes["node-1"].Store.Get("foo")

	listeners["node-2"].Stop()
	listeners["node-3"].Stop()

	if err := nodes["node-1"].Put(ctx, "foo", "orphan", nil); err == nil {
		t.Fatal("expected Put to fail: W=2 requires 2 nodes, but only the coordinator is up")
	}

	after, found := nodes["node-1"].Store.Get("foo")
	if !found || len(after) != 1 || after[0].Value != "committed" {
		t.Fatalf("expected rollback to restore exactly the pre-attempt version %v, got found=%v items=%v", before, found, after)
	}
}

// Two consecutive failed Puts against the same never-before-written key
// must not leave behind multiple orphaned sibling versions, nor keep
// advancing the vector clock across attempts — each failed attempt should
// be fully undone before the next one starts.
func TestPutRollbackDoesNotAccumulateAcrossRepeatedFailures(t *testing.T) {
	addrs := map[string]string{
		"node-1": "localhost:60261",
		"node-2": "localhost:60262",
		"node-3": "localhost:60263",
	}
	nodes, listeners := startTestCluster(t, addrs, map[string]quorumOverride{
		"node-1": {w: 2, r: 1},
	})

	listeners["node-2"].Stop()
	listeners["node-3"].Stop()

	ctx := context.Background()
	if err := nodes["node-1"].Put(ctx, "foo", "attempt-1", nil); err == nil {
		t.Fatal("expected first Put to fail: only the coordinator is up")
	}
	if err := nodes["node-1"].Put(ctx, "foo", "attempt-2", nil); err == nil {
		t.Fatal("expected second Put to fail: only the coordinator is up")
	}

	items, found := nodes["node-1"].Store.Get("foo")
	if found || len(items) != 0 {
		t.Fatalf("expected repeated failed writes to leave no trace (key absent), got found=%v items=%v", found, items)
	}
}
