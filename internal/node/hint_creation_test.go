package node

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"distributed-kv-datastore/internal/hints"
	"distributed-kv-datastore/internal/store"
)

// hintCluster addresses three nodes; N=3, so every key's preference list
// is all of them and node-1 coordinates every write it's given. Nodes in
// down get a known address, which refuses connections until a node is served
// there (see ports_test.go); the others get reserved listeners.
func hintCluster(t *testing.T, down ...string) (addrs map[string]string, peersOf func(id string) map[string]string) {
	t.Helper()
	addrs = map[string]string{}
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		if slices.Contains(down, id) {
			addrs[id] = knownAddr()
		} else {
			addrs[id] = reserveAddrs(t, id)[id]
		}
	}
	peersOf = func(id string) map[string]string {
		peers := map[string]string{}
		for peer, addr := range addrs {
			if peer != id {
				peers[peer] = addr
			}
		}
		return peers
	}
	return addrs, peersOf
}

// serveNode serves nd at addr (see serveAt), stopped at cleanup.
func serveNode(t *testing.T, nd *Node, addr string) {
	t.Helper()
	serveAt(t, addr, nd.Store, nd)
}

// persistentCoordinator opens node-1 as a persistent node at dir with
// write quorum w. It isn't served: nothing needs to reach it.
func persistentCoordinator(t *testing.T, dir string, peers map[string]string, w int) *Node {
	t.Helper()
	nd, err := New("node-1", "unused", 3, w, 1, peers, dir, 1<<20)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	t.Cleanup(func() { nd.Close() })
	return nd
}

// pendingHintKeys opens node-1's hint store (node-1 must be closed) and
// returns the keys pending for target.
func pendingHintKeys(t *testing.T, dir, target string) []string {
	t.Helper()
	s, err := hints.Open(filepath.Join(dir, "hints"), 1<<20)
	if err != nil {
		t.Fatalf("open hint store: %v", err)
	}
	defer s.Close()
	pending, err := s.Pending(target)
	if err != nil {
		t.Fatalf("Pending(%q): %v", target, err)
	}
	var keys []string
	for _, h := range pending {
		keys = append(keys, h.Key)
	}
	return keys
}

// TestUnreachableReplicaGetsHintEvenWhenItsFailureArrivesAfterQuorum: with
// node-3 down, node-1 and node-2 reach W=2 — and node-3's Unavailable is
// held until after that, the common case where the write's outcome is
// decided before the down replica's failure is known. It must still get a
// durable hint; node-2, which acked, must not.
func TestUnreachableReplicaGetsHintEvenWhenItsFailureArrivesAfterQuorum(t *testing.T) {
	dir := t.TempDir()
	addrs, peersOf := hintCluster(t, "node-3")
	node1 := persistentCoordinator(t, dir, peersOf("node-1"), 2)
	serveNode(t, newTestNode(t, "node-2", addrs["node-2"], 3, 2, 1, peersOf("node-2")), addrs["node-2"])
	// node-3 is never started: Unavailable.

	release := make(chan struct{})
	testHookBeforeReplicate = func(peerID string) {
		if peerID == "node-3" {
			<-release
		}
	}
	defer func() { testHookBeforeReplicate = nil }()

	if err := node1.Put(context.Background(), "k", "v", nil); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	close(release) // node-3's attempt runs only now, after the decision
	if err := node1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if got := pendingHintKeys(t, dir, "node-3"); len(got) != 1 || got[0] != "k" {
		t.Fatalf("expected a hint for k pending for node-3, got %v", got)
	}
	if got := pendingHintKeys(t, dir, "node-2"); len(got) != 0 {
		t.Fatalf("expected no hint for node-2, which acked, got %v", got)
	}
}

// TestReplicaFailingOtherwiseGetsNoHint: a replica that's reachable but
// fails the write (here its storage fails: Internal) may or may not have
// applied it — not a case for a hint, only for anti-entropy.
func TestReplicaFailingOtherwiseGetsNoHint(t *testing.T) {
	dir := t.TempDir()
	addrs, peersOf := hintCluster(t)
	node1 := persistentCoordinator(t, dir, peersOf("node-1"), 2)
	serveNode(t, newTestNode(t, "node-2", addrs["node-2"], 3, 2, 1, peersOf("node-2")), addrs["node-2"])
	node3 := newTestNode(t, "node-3", addrs["node-3"], 3, 2, 1, peersOf("node-3"))
	node3.Store = store.NewDataStoreWithPersister("node-3", failingPersister{})
	serveNode(t, node3, addrs["node-3"])

	if err := node1.Put(context.Background(), "k", "v", nil); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := node1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if got := pendingHintKeys(t, dir, "node-3"); len(got) != 0 {
		t.Fatalf("expected no hint for a replica that failed with a non-Unavailable error, got %v", got)
	}
}

// TestRolledBackWriteGetsNoHint: with W=3 and node-3 down the write fails
// and node-1 rolls its local copy back. A hint would later deliver that
// undone write to node-3, which never saw it.
func TestRolledBackWriteGetsNoHint(t *testing.T) {
	dir := t.TempDir()
	addrs, peersOf := hintCluster(t, "node-3")
	node1 := persistentCoordinator(t, dir, peersOf("node-1"), 3)
	serveNode(t, newTestNode(t, "node-2", addrs["node-2"], 3, 3, 1, peersOf("node-2")), addrs["node-2"])

	if err := node1.Put(context.Background(), "k", "v", nil); err == nil {
		t.Fatal("test setup: expected the write to miss W=3 with node-3 down")
	}
	if items, found, _ := node1.Store.Get("k"); found {
		t.Fatalf("test setup: expected the local write rolled back, got %v", itemValues(items))
	}
	if err := node1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if got := pendingHintKeys(t, dir, "node-3"); len(got) != 0 {
		t.Fatalf("expected no hint for a rolled-back write, got %v", got)
	}
}

// TestCloseWaitsForADrainStoringHints: a write's drain is about to store a
// hint when the node is closed. Close must wait for it, so the hint lands
// in the still-open hint store rather than being lost.
func TestCloseWaitsForADrainStoringHints(t *testing.T) {
	dir := t.TempDir()
	addrs, peersOf := hintCluster(t, "node-3")
	node1 := persistentCoordinator(t, dir, peersOf("node-1"), 2)
	serveNode(t, newTestNode(t, "node-2", addrs["node-2"], 3, 2, 1, peersOf("node-2")), addrs["node-2"])

	reached := make(chan struct{})
	release := make(chan struct{})
	testHookBeforeStoringHints = func(string) {
		close(reached)
		<-release
	}
	defer func() { testHookBeforeStoringHints = nil }()

	if err := node1.Put(context.Background(), "k", "v", nil); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	<-reached

	closeDone := make(chan error, 1)
	go func() { closeDone <- node1.Close() }()
	select {
	case err := <-closeDone:
		close(release)
		t.Fatalf("Close returned (err %v) while a drain was still storing hints", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-closeDone; err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if got := pendingHintKeys(t, dir, "node-3"); len(got) != 1 || got[0] != "k" {
		t.Fatalf("expected the drain's hint durable after Close, got %v", got)
	}
}

// TestSlowReplicaStillReceivesWriteAfterCallerCancels: node-3's RPC is
// held until the caller has its answer and cancels its context. The fan-out
// runs detached from that cancellation, so node-3 still gets the write.
func TestSlowReplicaStillReceivesWriteAfterCallerCancels(t *testing.T) {
	addrs, peersOf := hintCluster(t)
	node1 := newTestNode(t, "node-1", addrs["node-1"], 3, 2, 1, peersOf("node-1"))
	t.Cleanup(func() { node1.Close() })
	serveNode(t, newTestNode(t, "node-2", addrs["node-2"], 3, 2, 1, peersOf("node-2")), addrs["node-2"])
	node3 := newTestNode(t, "node-3", addrs["node-3"], 3, 2, 1, peersOf("node-3"))
	serveNode(t, node3, addrs["node-3"])

	release := make(chan struct{})
	testHookBeforeReplicate = func(peerID string) {
		if peerID == "node-3" {
			<-release
		}
	}
	defer func() { testHookBeforeReplicate = nil }()

	ctx, cancel := context.WithCancel(context.Background())
	if err := node1.Put(ctx, "k", "v", nil); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	cancel() // the caller is done
	close(release)

	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, found, _ := node3.Store.Get("k"); found {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the slow replica never received the write once the caller canceled")
		}
	}
}

// TestReplicationTimeoutBoundsStragglers: detached from the caller doesn't
// mean unbounded — a replica RPC still pending past ReplicationTimeout is
// abandoned rather than delivered.
func TestReplicationTimeoutBoundsStragglers(t *testing.T) {
	addrs, peersOf := hintCluster(t)
	node1 := newTestNode(t, "node-1", addrs["node-1"], 3, 2, 1, peersOf("node-1"))
	node1.QuorumConfig.ReplicationTimeout = 100 * time.Millisecond
	serveNode(t, newTestNode(t, "node-2", addrs["node-2"], 3, 2, 1, peersOf("node-2")), addrs["node-2"])
	node3 := newTestNode(t, "node-3", addrs["node-3"], 3, 2, 1, peersOf("node-3"))
	serveNode(t, node3, addrs["node-3"])

	testHookBeforeReplicate = func(peerID string) {
		if peerID == "node-3" {
			time.Sleep(300 * time.Millisecond) // past the timeout
		}
	}
	defer func() { testHookBeforeReplicate = nil }()

	if err := node1.Put(context.Background(), "k", "v", nil); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := node1.Close(); err != nil { // waits for node-3's attempt
		t.Fatalf("Close failed: %v", err)
	}
	if _, found, _ := node3.Store.Get("k"); found {
		t.Fatal("node-3 received a write whose replication had timed out")
	}
}
