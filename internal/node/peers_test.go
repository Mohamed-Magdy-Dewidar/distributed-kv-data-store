package node

import (
	"context"
	"testing"
	"time"
)

// Heartbeats record each peer's epoch from its ping replies; the record goes
// when the peer leaves the view, and a node removed from the view says so.
func TestPeerEpochsAreRecordedAndPrunedWithTheView(t *testing.T) {
	nodes, addrs := startLoopCluster(t, 61201, "node-1", "node-2", "node-3")
	a := nodes["node-1"]
	for _, nd := range nodes {
		if _, err := nd.SetMembership(4, addrs); err != nil {
			t.Fatal(err)
		}
	}
	beat(t, a)

	eventually(t, 5*time.Second, "both peers to be seen at epoch 4", func() bool {
		st := a.PeerStatuses()
		return st["node-2"].LastSeenEpoch == 4 && st["node-3"].LastSeenEpoch == 4
	})
	if st := a.PeerStatuses(); len(st) != 2 || !st["node-2"].Alive || !st["node-3"].Alive {
		t.Fatalf("PeerStatuses = %v, want the two other members, alive", st)
	}
	if !a.IsMember() {
		t.Fatal("a member reports it is not one")
	}

	// node-3 leaves the view: its record goes, and only node-2 is left.
	if _, err := a.SetMembership(5, membersOf(addrs, "node-1", "node-2")); err != nil {
		t.Fatal(err)
	}
	if st := a.PeerStatuses(); len(st) != 1 || st["node-2"].LastSeenEpoch != 4 {
		t.Fatalf("after node-3 left: %v", st)
	}
	a.healthMu.RLock()
	_, kept := a.health["node-3"]
	a.healthMu.RUnlock()
	if kept {
		t.Fatal("node-3's health record survived its removal from the view")
	}
	eventually(t, 5*time.Second, "node-2 to be seen at epoch 5", func() bool {
		return a.PeerStatuses()["node-2"].LastSeenEpoch == 5
	})

	// node-1 itself is removed.
	if _, err := a.SetMembership(6, membersOf(addrs, "node-2", "node-3")); err != nil {
		t.Fatal(err)
	}
	if a.IsMember() {
		t.Fatal("a node absent from its view reports it is a member")
	}
}

// WaitDrained: a member is done with its handoff; a node that is out of the
// view also needs the remaining members to have been seen at the epoch.
func TestWaitDrainedNeedsPeersOnlyForANodeOutsideTheView(t *testing.T) {
	nodes, addrs := startLoopCluster(t, 61211, "node-1", "node-2", "node-3")
	a := nodes["node-1"]

	quick := func() context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		t.Cleanup(cancel)
		return ctx
	}

	// In the view: the (empty) handoff is all it takes; no heartbeats needed.
	if _, err := a.SetMembership(1, addrs); err != nil {
		t.Fatal(err)
	}
	if err := a.WaitDrained(quick(), 1); err != nil {
		t.Fatalf("a member with nothing to hand off: %v", err)
	}

	// Out of the view, with the remaining members never pinged: not drained.
	rest := membersOf(addrs, "node-2", "node-3")
	if _, err := a.SetMembership(2, rest); err != nil {
		t.Fatal(err)
	}
	if err := a.WaitDrained(quick(), 2); err == nil {
		t.Fatal("drained although the remaining members were never seen at epoch 2")
	}

	// They are at epoch 2 and heartbeats are running: drained.
	for _, id := range []string{"node-2", "node-3"} {
		if _, err := nodes[id].SetMembership(2, rest); err != nil {
			t.Fatal(err)
		}
	}
	beat(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.WaitDrained(ctx, 2); err != nil {
		t.Fatalf("expected drained once node-2 and node-3 were seen at epoch 2: %v", err)
	}
}
