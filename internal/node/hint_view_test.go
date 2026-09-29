package node

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/rpc/pb"
	"distributed-kv-datastore/internal/vectorclock"
)

// countingPeer is a gRPC server that accepts every Replicate and counts them.
type countingPeer struct {
	pb.UnimplementedKVReplicationServer
	replicates atomic.Int32
}

func (c *countingPeer) Replicate(context.Context, *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	c.replicates.Add(1)
	return &pb.ReplicateResponse{Accepted: true}, nil
}

func startCountingPeer(t *testing.T, addr string) *countingPeer {
	t.Helper()
	lis := listenAt(t, addr)
	p := &countingPeer{}
	srv := grpc.NewServer()
	pb.RegisterKVReplicationServer(srv, p)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return p
}

func hintItem() *model.DataItem {
	return &model.DataItem{Value: "hinted", VectorClock: vectorclock.FromSnapshot(map[string]uint32{"x": 1}), LastUpdatedBy: "x"}
}

// countHintAttempts counts the hints delivery tries to send, per target.
func countHintAttempts(t *testing.T) map[string]*atomic.Int32 {
	t.Helper()
	counts := map[string]*atomic.Int32{"node-2": {}, "node-3": {}}
	testHookBeforeDeliveringHint = func(target, _ string) {
		if c := counts[target]; c != nil {
			c.Add(1)
		}
	}
	t.Cleanup(func() { testHookBeforeDeliveringHint = nil })
	return counts
}

func hintNode(t *testing.T) (*Node, map[string]string) {
	t.Helper()
	addrs := reserveAddrs(t, "node-1", "node-2", "node-3")
	nd, err := NewPersistent("node-1", addrs["node-1"], 1, 1, 1, neighborsOf(addrs, "node-1"), t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nd.Close() })
	nd.QuorumConfig.MaxReconnectBackoff = 50 * time.Millisecond
	return nd, addrs
}

func pendingKeys(t *testing.T, nd *Node, target string) int {
	t.Helper()
	pending, err := nd.hints.Pending(target)
	if err != nil {
		t.Fatal(err)
	}
	return len(pending)
}

// (a) A hint for a node that has left the membership is neither read nor
// sent — not in any round — and stays in the store. A hint for a node that
// remains is delivered as usual.
func TestHintsForARemovedNodeAreNeverDelivered(t *testing.T) {
	nd, addrs := hintNode(t)
	removed := startCountingPeer(t, addrs["node-2"])
	stays := startCountingPeer(t, addrs["node-3"])
	attempts := countHintAttempts(t)
	for _, target := range []string{"node-2", "node-3"} {
		if err := nd.hints.Add(target, "k", hintItem()); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := nd.SetMembership(1, membersOf(addrs, "node-1", "node-3")); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		nd.deliverHints(context.Background())
	}

	if got := attempts["node-2"].Load(); got != 0 {
		t.Errorf("delivery tried %d hints for the removed node", got)
	}
	if got := removed.replicates.Load(); got != 0 {
		t.Errorf("the removed node received %d hints", got)
	}
	if got := pendingKeys(t, nd, "node-2"); got != 1 {
		t.Errorf("the removed node's hint should be kept, %d pending", got)
	}
	if got := stays.replicates.Load(); got != 1 || pendingKeys(t, nd, "node-3") != 0 {
		t.Errorf("the remaining member's hint: %d delivered, %d pending; want 1 and 0", got, pendingKeys(t, nd, "node-3"))
	}
}

// (b) A hint for a node marked dead waits; when the node is alive again it
// is delivered.
func TestHintsForADeadNodeWaitUntilItIsAlive(t *testing.T) {
	nd, addrs := hintNode(t)
	target := startCountingPeer(t, addrs["node-2"])
	attempts := countHintAttempts(t)
	if err := nd.hints.Add("node-2", "k", hintItem()); err != nil {
		t.Fatal(err)
	}
	for range nd.QuorumConfig.MaxMissedHeartbeats {
		nd.recordHeartbeat("node-2", false)
	}
	if !nd.isDead("node-2") {
		t.Fatal("setup: node-2 should be marked dead")
	}

	for range 3 {
		nd.deliverHints(context.Background())
	}
	if got := attempts["node-2"].Load(); got != 0 || target.replicates.Load() != 0 {
		t.Fatalf("delivery tried %d hints (target received %d) while the node was dead", got, target.replicates.Load())
	}
	if got := pendingKeys(t, nd, "node-2"); got != 1 {
		t.Fatalf("the hint should still be pending, %d are", got)
	}

	nd.recordHeartbeat("node-2", true)
	nd.deliverHints(context.Background())
	if got := target.replicates.Load(); got != 1 {
		t.Fatalf("after the node came back it received %d hints, want 1", got)
	}
	if got := pendingKeys(t, nd, "node-2"); got != 0 {
		t.Fatalf("%d hints still pending after delivery", got)
	}
}
