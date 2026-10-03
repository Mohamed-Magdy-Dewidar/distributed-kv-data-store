package node

import (
	"context"
	"testing"
)

// TestStatsReportsTheEpochOfTheHeldView: Stats follows the view the node
// holds, not the one it started with.
func TestStatsReportsTheEpochOfTheHeldView(t *testing.T) {
	nd, members := twoNode(t)
	if got := nd.Stats().Epoch; got != 0 {
		t.Fatalf("Stats().Epoch = %d before any SetMembership, want 0", got)
	}
	if _, err := nd.SetMembership(3, members); err != nil {
		t.Fatalf("SetMembership(3): %v", err)
	}
	if got := nd.Stats().Epoch; got != 3 {
		t.Fatalf("Stats().Epoch = %d after SetMembership(3), want 3", got)
	}
}

// TestStatsCountQuorumFailures: a write that can't reach W acks and a read
// that can't reach R responses are each counted once, under their own
// counter; successful ones are not counted.
func TestStatsCountQuorumFailures(t *testing.T) {
	addrs := reserveAddrs(t, "node-1", "node-2", "node-3")
	nodes, listeners := startTestCluster(t, addrs, map[string]quorumOverride{"node-1": {w: 3, r: 3}})
	node1 := nodes["node-1"]
	ctx := context.Background()

	if err := node1.Put(ctx, "k", "v", nil); err != nil {
		t.Fatalf("Put with every node up: %v", err)
	}
	if _, err := node1.Get(ctx, "k"); err != nil {
		t.Fatalf("Get with every node up: %v", err)
	}
	if s := node1.Stats(); s.WriteQuorumFailures != 0 || s.ReadQuorumFailures != 0 {
		t.Fatalf("after a successful Put and Get: write %d, read %d quorum failures; want 0 and 0", s.WriteQuorumFailures, s.ReadQuorumFailures)
	}

	listeners["node-3"].Stop() // W=3 and R=3 are now out of reach
	if err := node1.Put(ctx, "k", "v2", nil); err == nil {
		t.Fatal("Put reached W=3 with a node down")
	}
	if s := node1.Stats(); s.WriteQuorumFailures != 1 || s.ReadQuorumFailures != 0 {
		t.Fatalf("after a failed Put: write %d, read %d quorum failures; want 1 and 0", s.WriteQuorumFailures, s.ReadQuorumFailures)
	}
	if _, err := node1.Get(ctx, "k"); err == nil {
		t.Fatal("Get reached R=3 with a node down")
	}
	if s := node1.Stats(); s.WriteQuorumFailures != 1 || s.ReadQuorumFailures != 1 {
		t.Fatalf("after a failed Get: write %d, read %d quorum failures; want 1 and 1", s.WriteQuorumFailures, s.ReadQuorumFailures)
	}
}

// TestStatsReportPendingHintsAsOfTheLastRound: hints created are counted at
// once, but the pending count is unknown until a delivery round has run,
// then reflects that round, and is cleared by the round that delivers them.
func TestStatsReportPendingHintsAsOfTheLastRound(t *testing.T) {
	node1, addrs, peersOf := hintedWrite(t, "a", "b") // node-3 is down
	s := node1.Stats()
	if s.Hints.Created != 2 || s.HintsPendingKnown {
		t.Fatalf("before any delivery round: %d hints created, pending known=%v; want 2 and false", s.Hints.Created, s.HintsPendingKnown)
	}
	if s.Data.MemtableBytes <= 0 || s.Hints.Engine.MemtableBytes <= 0 {
		t.Fatalf("after writes and hints: data engine %+v, hint engine %+v; want both memtables non-empty", s.Data, s.Hints.Engine)
	}

	node1.deliverHints(context.Background()) // node-3 still down: nothing delivered
	if s := node1.Stats(); !s.HintsPendingKnown || s.HintsPending != 2 || s.Hints.Delivered != 0 {
		t.Fatalf("after a round with the target down: pending %d (known %v), delivered %d; want 2, known, 0", s.HintsPending, s.HintsPendingKnown, s.Hints.Delivered)
	}

	node3 := newTestNode(t, "node-3", addrs["node-3"], 3, 2, 1, peersOf("node-3"))
	serveNode(t, node3, addrs["node-3"])
	waitReachable(t, node1, "node-3")
	node1.deliverHints(context.Background())
	if s := node1.Stats(); s.HintsPending != 0 || s.Hints.Delivered != 2 || s.Hints.Created != 2 {
		t.Fatalf("after delivering: pending %d, delivered %d, created %d; want 0, 2, 2", s.HintsPending, s.Hints.Delivered, s.Hints.Created)
	}
}

// TestStatsTimeAntiEntropyRoundsAndCountRepairs: each round is counted and
// timed; a key installed locally counts as pulled, one sent to the peer as
// pushed; a peer the round can't reconcile with counts as a failure.
func TestStatsTimeAntiEntropyRoundsAndCountRepairs(t *testing.T) {
	addrs := reserveAddrs(t, "node-1", "node-2")
	nodes, listeners := startTestCluster(t, addrs, nil)
	node1 := nodes["node-1"]
	node1.Store.Put("only-on-1", "v", nil) // written locally, never replicated
	nodes["node-2"].Store.Put("only-on-2", "v", nil)
	nodes["node-2"].Store.Put("also-only-on-2", "v", nil) // 2 to pull, 1 to push

	node1.runAntiEntropyRound(context.Background())
	s := node1.Stats()
	if s.AntiEntropyRounds != 1 || s.AntiEntropyKeysPulled != 2 || s.AntiEntropyKeysPushed != 1 || s.AntiEntropyPeerFailures != 0 {
		t.Fatalf("after one round: %d rounds, %d pulled, %d pushed, %d peer failures; want 1, 2, 1, 0",
			s.AntiEntropyRounds, s.AntiEntropyKeysPulled, s.AntiEntropyKeysPushed, s.AntiEntropyPeerFailures)
	}
	if s.AntiEntropyLastRound <= 0 || s.AntiEntropyTotal != s.AntiEntropyLastRound {
		t.Fatalf("after one round: last %v, total %v; want the same positive duration", s.AntiEntropyLastRound, s.AntiEntropyTotal)
	}
	first := s.AntiEntropyLastRound

	node1.runAntiEntropyRound(context.Background()) // in sync now: nothing to repair
	listeners["node-2"].Stop()
	node1.runAntiEntropyRound(context.Background())
	s = node1.Stats()
	if s.AntiEntropyRounds != 3 || s.AntiEntropyPeerFailures != 1 || s.AntiEntropyKeysPulled != 2 || s.AntiEntropyKeysPushed != 1 {
		t.Fatalf("after 3 rounds, the last with node-2 down: %d rounds, %d peer failures, %d pulled, %d pushed; want 3, 1, 2, 1",
			s.AntiEntropyRounds, s.AntiEntropyPeerFailures, s.AntiEntropyKeysPulled, s.AntiEntropyKeysPushed)
	}
	if s.AntiEntropyTotal < first+s.AntiEntropyLastRound {
		t.Fatalf("total %v is less than the first round (%v) plus the last (%v)", s.AntiEntropyTotal, first, s.AntiEntropyLastRound)
	}
}

// TestStatsTimeCompletedHandoffs: Stats reports the handoff to the newest
// view and counts and times each completed one.
func TestStatsTimeCompletedHandoffs(t *testing.T) {
	nd, members := twoNode(t)
	if s := nd.Stats(); s.HandoffsCompleted != 0 || !s.Handoff.Done {
		t.Fatalf("before any membership change: %d handoffs, done=%v; want 0 and done", s.HandoffsCompleted, s.Handoff.Done)
	}
	for epoch := uint64(1); epoch <= 2; epoch++ {
		if _, err := nd.SetMembership(epoch, members); err != nil {
			t.Fatalf("SetMembership(%d): %v", epoch, err)
		}
		waitHandoff(t, nd, epoch)
	}
	s := nd.Stats()
	if s.HandoffsCompleted != 2 || s.Handoff.Epoch != 2 || !s.Handoff.Done {
		t.Fatalf("after two handoffs: %d completed, status %+v; want 2, epoch 2, done", s.HandoffsCompleted, s.Handoff)
	}
	if s.HandoffLast <= 0 || s.HandoffTotal < s.HandoffLast {
		t.Fatalf("after two handoffs: last %v, total %v; want last > 0 and total >= last", s.HandoffLast, s.HandoffTotal)
	}
}

// TestStatsCountPeersAndMembership: peers are counted from the view and
// what heartbeats recorded; Member follows the view; pings and conflicts
// are the node's own counters.
func TestStatsCountPeersAndMembership(t *testing.T) {
	members := map[string]string{"node-1": "h1:1", "node-2": "h2:1", "node-3": "h3:1"}
	nd := newTestNode(t, "node-1", "h1:1", 2, 1, 1, map[string]string{"node-2": "h2:1", "node-3": "h3:1"})

	s := nd.Stats()
	if s.Peers != 2 || s.PeersAlive != 2 || s.PeersReachable != 0 || !s.Member {
		t.Fatalf("fresh node: %d peers, %d alive, %d reachable, member=%v; want 2, 2 (never pinged counts as alive), 0, true",
			s.Peers, s.PeersAlive, s.PeersReachable, s.Member)
	}

	nd.recordHeartbeat("node-2", true)
	for range nd.QuorumConfig.MaxMissedHeartbeats {
		nd.recordHeartbeat("node-3", false)
	}
	if s := nd.Stats(); s.Peers != 2 || s.PeersAlive != 1 || s.PeersReachable != 1 {
		t.Fatalf("node-2 answered, node-3 missed every ping: %d peers, %d alive, %d reachable; want 2, 1, 1", s.Peers, s.PeersAlive, s.PeersReachable)
	}

	nd.noteConflict("node-2", 0, "other")
	nd.pingsReceived.Add(3)
	if s := nd.Stats(); s.MembershipConflicts != 1 || s.PingsReceived != 3 {
		t.Fatalf("%d conflicts, %d pings received; want 1 and 3", s.MembershipConflicts, s.PingsReceived)
	}

	delete(members, "node-1")
	if _, err := nd.SetMembership(1, members); err != nil {
		t.Fatalf("SetMembership without node-1: %v", err)
	}
	if s := nd.Stats(); s.Member || s.Peers != 2 {
		t.Fatalf("after leaving the view: member=%v, %d peers; want false, 2", s.Member, s.Peers)
	}
}
