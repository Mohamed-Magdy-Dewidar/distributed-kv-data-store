package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/rpc/pb"
)

const testBeat = 50 * time.Millisecond

// fastHealth gives nd heartbeat settings suited to tests: a short ping
// timeout, three misses to be dead, and quick reconnects.
func fastHealth(nd *Node) {
	nd.QuorumConfig.HeartbeatTimeout = 40 * time.Millisecond
	nd.QuorumConfig.MaxMissedHeartbeats = 3
	nd.QuorumConfig.MaxReconnectBackoff = 50 * time.Millisecond
}

// beat starts nd's heartbeat loop, stopped at cleanup.
func beat(t *testing.T, nd *Node) {
	t.Helper()
	nd.StartHeartbeatLoop(context.Background(), testBeat)
	t.Cleanup(nd.StopBackgroundLoops)
}

func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", within, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func misses(nd *Node, peerID string) int {
	nd.healthMu.RLock()
	defer nd.healthMu.RUnlock()
	if h := nd.health[peerID]; h != nil {
		return h.misses
	}
	return 0
}

// tcpBlackHole listens on addr, accepts connections and never answers on
// them. gRPC gives up on such a connection after about a second (no HTTP/2
// handshake) and then fails calls fast, so it is slow for only that long.
func tcpBlackHole(t *testing.T, addr string) {
	t.Helper()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen %s: %v", addr, err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		lis.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
}

// hungPeer is a gRPC server that completes the handshake and then never
// answers: every call blocks until the caller's deadline. That is what a
// wedged process behind a live socket looks like, and it holds a call for as
// long as the caller allows.
type hungPeer struct {
	pb.UnimplementedKVReplicationServer
	done chan struct{}
}

func (h *hungPeer) block(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.done:
		return context.Canceled
	}
}

func (h *hungPeer) Replicate(ctx context.Context, _ *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	return nil, h.block(ctx)
}
func (h *hungPeer) FetchItem(ctx context.Context, _ *pb.FetchItemRequest) (*pb.FetchItemResponse, error) {
	return nil, h.block(ctx)
}
func (h *hungPeer) GetMerkleTree(ctx context.Context, _ *pb.GetMerkleTreeRequest) (*pb.GetMerkleTreeResponse, error) {
	return nil, h.block(ctx)
}
func (h *hungPeer) GetBucketKeys(ctx context.Context, _ *pb.GetBucketKeysRequest) (*pb.GetBucketKeysResponse, error) {
	return nil, h.block(ctx)
}
func (h *hungPeer) Ping(ctx context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	return nil, h.block(ctx)
}
func (h *hungPeer) CoordinatePut(ctx context.Context, _ *pb.CoordinatePutRequest) (*pb.CoordinatePutResponse, error) {
	return nil, h.block(ctx)
}

func startHungPeer(t *testing.T, addr string) {
	t.Helper()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen %s: %v", addr, err)
	}
	h := &hungPeer{done: make(chan struct{})}
	srv := grpc.NewServer()
	pb.RegisterKVReplicationServer(srv, h)
	go srv.Serve(lis)
	t.Cleanup(func() {
		close(h.done)
		srv.Stop()
	})
}

func membersOf(addrs map[string]string, ids ...string) map[string]string {
	m := make(map[string]string, len(ids))
	for _, id := range ids {
		m[id] = addrs[id]
	}
	return m
}

func neighborsOf(addrs map[string]string, self string) map[string]string {
	m := maps.Clone(addrs)
	delete(m, self)
	return m
}

func sameMembership(a, b *Node) bool {
	va, vb := a.membership.Load(), b.membership.Load()
	return va.epoch == vb.epoch && va.fingerprint == vb.fingerprint
}

// (a) A peer that stops answering is marked dead after MaxMissedHeartbeats
// misses; one that answers again is alive.
func TestHeartbeatMarksAStoppedPeerDeadAndARestartedOneAlive(t *testing.T) {
	addrs := map[string]string{"node-1": "localhost:61001", "node-2": "localhost:61002"}
	node1 := New("node-1", addrs["node-1"], 2, 1, 1, neighborsOf(addrs, "node-1"))
	node2 := New("node-2", addrs["node-2"], 2, 1, 1, neighborsOf(addrs, "node-2"))
	fastHealth(node1)
	listener, err := rpc.Serve(addrs["node-2"], node2.Store, node2)
	if err != nil {
		t.Fatal(err)
	}
	beat(t, node1)

	eventually(t, 3*time.Second, "node-2 to be pinged successfully", func() bool {
		return node2.pingsReceived.Load() > 2
	})
	if node1.isDead("node-2") {
		t.Fatal("a peer that answers was marked dead")
	}

	listener.Stop()
	eventually(t, 3*time.Second, "node-2 to be marked dead", func() bool { return node1.isDead("node-2") })
	if got := misses(node1, "node-2"); got < node1.QuorumConfig.MaxMissedHeartbeats {
		t.Fatalf("dead after only %d misses, want at least %d", got, node1.QuorumConfig.MaxMissedHeartbeats)
	}

	listener2, err := rpc.Serve(addrs["node-2"], node2.Store, node2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(listener2.Stop)
	eventually(t, 5*time.Second, "node-2 to be alive again", func() bool { return !node1.isDead("node-2") })
	if got := misses(node1, "node-2"); got != 0 {
		t.Fatalf("expected the miss counter reset on success, got %d", got)
	}
}

// hungCluster is node-1 (persistent, N=3, W=2, R=3) with a live node-2 and a
// node-3 that is hung (see hungPeer). ReplicationTimeout is long, so anything
// that waits on node-3 is slow.
func hungCluster(t *testing.T) *Node {
	t.Helper()
	addrs := map[string]string{"node-1": "localhost:61011", "node-2": "localhost:61012", "node-3": "localhost:61013"}
	serveNode(t, New("node-2", addrs["node-2"], 3, 2, 3, neighborsOf(addrs, "node-2")), addrs["node-2"])
	startHungPeer(t, addrs["node-3"])

	nd, err := NewPersistent("node-1", addrs["node-1"], 3, 2, 3, neighborsOf(addrs, "node-1"), t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nd.Close() })
	fastHealth(nd)
	nd.QuorumConfig.ReplicationTimeout = 5 * time.Second
	beat(t, nd)
	eventually(t, 5*time.Second, "the hung peer to be marked dead", func() bool { return nd.isDead("node-3") })
	return nd
}

func hintPending(nd *Node, target, key string) bool {
	pending, err := nd.hints.Pending(target)
	if err != nil {
		return false
	}
	for _, h := range pending {
		if h.Key == key {
			return true
		}
	}
	return false
}

// (b) Without the dead check, a write's RPC to a hung peer waits out
// ReplicationTimeout, and then isn't even Unavailable, so no hint is made.
// With it, the peer is reported Unavailable at once and the hint follows.
func TestWriteToADeadPeerStoresItsHintAtOnce(t *testing.T) {
	nd := hungCluster(t)

	start := time.Now()
	if err := nd.Put(context.Background(), "k", "v", nil); err != nil {
		t.Fatalf("Put: %v", err)
	}
	eventually(t, time.Second, "the hint for the dead peer", func() bool { return hintPending(nd, "node-3", "k") })
	if took := time.Since(start); took > time.Second {
		t.Fatalf("hint took %v", took)
	}
}

// A dead peer's vote fails at once instead of holding the read until its
// deadline.
func TestReadCountsADeadPeerAsFailedWithoutAnRPC(t *testing.T) {
	nd := hungCluster(t) // R=3: every replica is needed

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	_, err := nd.Get(ctx, "k")
	if err == nil || !strings.Contains(err.Error(), "read quorum not reached") {
		t.Fatalf("expected a read quorum failure, got %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("Get took %v: it waited for the dead peer", took)
	}
}

// Anti-entropy skips a dead peer instead of waiting on it.
func TestAntiEntropyRoundSkipsDeadPeers(t *testing.T) {
	nd := hungCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	nd.runAntiEntropyRound(ctx)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("the round took %v: it waited for the dead peer", took)
	}
}

// startLoopCluster starts len(ids) served nodes (N=2, W=1, R=1), all with the
// membership of ids at epoch 0. Their heartbeat loops are not started.
func startLoopCluster(t *testing.T, base int, ids ...string) (map[string]*Node, map[string]string) {
	t.Helper()
	addrs := map[string]string{}
	for i, id := range ids {
		addrs[id] = fmt.Sprintf("localhost:%d", base+i)
	}
	nodes := map[string]*Node{}
	for _, id := range ids {
		nd := New(id, addrs[id], 2, 1, 1, neighborsOf(addrs, id))
		fastHealth(nd)
		serveNode(t, nd, addrs[id])
		nodes[id] = nd
	}
	return nodes, addrs
}

// (c) A node that adopts a newer membership passes it on: peers that see the
// higher epoch in a ping reply fetch it. node-1 announces an address nobody
// can fetch from, so only the pinger side can work.
func TestNewerMembershipPropagatesThroughPingReplies(t *testing.T) {
	nodes, addrs := startLoopCluster(t, 61021, "node-1", "node-2", "node-3")
	nodes["node-1"].Address = "localhost:1" // a receiver can't fetch from here

	next := withMember(addrs, "node-4", "localhost:61029")
	if changed, err := nodes["node-1"].SetMembership(1, next); err != nil || !changed {
		t.Fatalf("SetMembership: changed=%v err=%v", changed, err)
	}
	for _, nd := range nodes {
		beat(t, nd)
	}

	eventually(t, 5*time.Second, "node-2 and node-3 to adopt epoch 1", func() bool {
		return sameMembership(nodes["node-1"], nodes["node-2"]) && sameMembership(nodes["node-1"], nodes["node-3"])
	})
	if epoch, members := nodes["node-2"].Membership(); epoch != 1 || len(members) != 4 {
		t.Fatalf("node-2 has epoch %d %v", epoch, members)
	}
}

// (d) A new member announces itself: nodes that don't list it can't ping it,
// but its ping carries a higher epoch and its address, and they fetch from it.
func TestNewMemberIsDiscoveredFromItsFirstPing(t *testing.T) {
	nodes, addrs := startLoopCluster(t, 61031, "node-1", "node-2", "node-3")
	for _, nd := range nodes {
		beat(t, nd)
	}

	addrs["node-4"] = "localhost:61034"
	node4 := New("node-4", addrs["node-4"], 2, 1, 1, neighborsOf(addrs, "node-4"))
	fastHealth(node4)
	serveNode(t, node4, addrs["node-4"])
	if _, err := node4.SetMembership(1, addrs); err != nil {
		t.Fatal(err)
	}
	beat(t, node4)

	eventually(t, 5*time.Second, "node-1, node-2 and node-3 to adopt node-4's epoch 1", func() bool {
		for _, id := range []string{"node-1", "node-2", "node-3"} {
			if !sameMembership(nodes[id], node4) {
				return false
			}
		}
		return true
	})
}

// (e) The same epoch with different members is a conflict nobody resolves:
// neither side adopts, and neither treats the other as dead.
func TestConflictingMembershipsAtOneEpochAreLeftAlone(t *testing.T) {
	nodes, addrs := startLoopCluster(t, 61041, "node-1", "node-2")
	a, b := nodes["node-1"], nodes["node-2"]
	if _, err := a.SetMembership(1, withMember(addrs, "node-x", "localhost:61048")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.SetMembership(1, withMember(addrs, "node-y", "localhost:61049")); err != nil {
		t.Fatal(err)
	}
	viewA, viewB := a.membership.Load(), b.membership.Load()
	beat(t, a)
	beat(t, b)

	eventually(t, 5*time.Second, "both sides to notice the conflict", func() bool {
		return a.membershipConflicts.Load() > 0 && b.membershipConflicts.Load() > 0
	})
	time.Sleep(10 * testBeat) // plenty of further rounds
	if a.membership.Load() != viewA || b.membership.Load() != viewB {
		t.Fatal("a node adopted the other's membership at the same epoch")
	}
	if a.isDead("node-2") || b.isDead("node-1") {
		t.Fatal("a conflicting peer was marked dead")
	}
}

// (f) A member whose address serves a different node is unreachable as far as
// the membership goes, and nothing it says is adopted.
func TestImpostorAtAMembersAddressIsDeadAndNotTrusted(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	log.SetOutput(writerFunc(func(p []byte) (int, error) { mu.Lock(); defer mu.Unlock(); return buf.Write(p) }))
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	addrs := map[string]string{"node-1": "localhost:61051", "node-2": "localhost:61052", "node-3": "localhost:61053"}
	// node-3 really is node-3, at an epoch far ahead of node-1's...
	node3 := New("node-3", addrs["node-3"], 2, 1, 1, neighborsOf(addrs, "node-3"))
	if _, err := node3.SetMembership(9, addrs); err != nil {
		t.Fatal(err)
	}
	serveNode(t, node3, addrs["node-3"])
	// ...but node-1 is told that "node-2" lives at node-3's address.
	node1 := New("node-1", addrs["node-1"], 2, 1, 1, map[string]string{"node-2": addrs["node-3"]})
	fastHealth(node1)
	beat(t, node1)

	eventually(t, 5*time.Second, "the impostor's address to be marked dead", func() bool { return node1.isDead("node-2") })
	time.Sleep(5 * testBeat)
	if epoch, _ := node1.Membership(); epoch != 0 {
		t.Fatalf("node-1 adopted epoch %d from an impostor", epoch)
	}
	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(buf.String(), "ADDRESS MISMATCH") {
		t.Fatalf("expected a loud log line about the mismatch, got:\n%s", buf.String())
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// A membership taken off the wire is adopted only if its claimed fingerprint
// is the one computed from its members.
func TestFetchedMembershipWithAWrongFingerprintIsRejected(t *testing.T) {
	nd, base := twoNode()
	held := nd.membership.Load()
	next := withMember(base, "node-3", "h3:1")

	if changed, err := nd.setMembership(4, next, "0000"); !errors.Is(err, ErrFingerprintMismatch) || changed {
		t.Fatalf("changed=%v err=%v, want ErrFingerprintMismatch", changed, err)
	}
	nd.adoptFetched("node-2", rpc.MembershipInfo{Epoch: 4, Members: next, Fingerprint: "0000"})
	if nd.membership.Load() != held {
		t.Fatal("a membership with a wrong fingerprint was adopted")
	}

	good := fingerprint(next, nd.QuorumConfig.N)
	nd.adoptFetched("node-2", rpc.MembershipInfo{Epoch: 4, Members: next, Fingerprint: good})
	if epoch, _ := nd.Membership(); epoch != 4 {
		t.Fatalf("a correctly fingerprinted membership was not adopted (epoch %d)", epoch)
	}
}

// Only one fetch-and-adopt runs at a time; the others are dropped.
func TestFetchAndAdoptIsSingleFlight(t *testing.T) {
	nd, _ := twoNode()
	release := make(chan struct{})
	var fetches atomic.Int32
	started := make(chan struct{}, 2)
	slow := func(context.Context) (rpc.MembershipInfo, error) {
		fetches.Add(1)
		started <- struct{}{}
		<-release
		return rpc.MembershipInfo{}, errors.New("nothing to give")
	}

	nd.adoptFrom("node-2", slow)
	<-started
	nd.adoptFrom("node-2", slow) // dropped: one is in flight
	nd.adoptFrom("node-2", slow)
	time.Sleep(50 * time.Millisecond)
	if got := fetches.Load(); got != 1 {
		t.Fatalf("%d fetches ran at once, want 1", got)
	}
	close(release)
	eventually(t, 2*time.Second, "the flight to land", func() bool { return !nd.adopting.Load() })

	nd.adoptFrom("node-2", func(context.Context) (rpc.MembershipInfo, error) {
		fetches.Add(1)
		return rpc.MembershipInfo{}, errors.New("x")
	})
	eventually(t, 2*time.Second, "a later fetch to run", func() bool { return fetches.Load() == 2 })
	nd.StopBackgroundLoops()
}

// (g) Heartbeats, membership changes and traffic together, under -race.
func TestHeartbeatsMembershipChangesAndTrafficTogether(t *testing.T) {
	addrs := map[string]string{"node-1": "localhost:61061", "node-2": "localhost:61062", "node-3": "localhost:61063"}
	nodes := map[string]*Node{}
	for id, addr := range addrs {
		nd := New(id, addr, 3, 2, 2, neighborsOf(addrs, id))
		fastHealth(nd)
		nd.QuorumConfig.HeartbeatTimeout = 300 * time.Millisecond // no false deaths under -race
		serveNode(t, nd, addr)
		nodes[id] = nd
	}
	for _, nd := range nodes {
		nd.StartHeartbeatLoop(context.Background(), 20*time.Millisecond)
		t.Cleanup(nd.StopBackgroundLoops)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var stop atomic.Bool
	var bg sync.WaitGroup
	bg.Add(1)
	go func() { // node-1 keeps announcing newer memberships
		defer bg.Done()
		for epoch := uint64(1); !stop.Load(); epoch++ {
			if _, err := nodes["node-1"].SetMembership(epoch, addrs); err != nil && !errors.Is(err, ErrStaleEpoch) {
				t.Errorf("SetMembership(%d): %v", epoch, err)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	errs := make(chan error, 4)
	var ops sync.WaitGroup
	for _, id := range []string{"node-1", "node-2"} {
		ops.Add(1)
		go func() {
			defer ops.Done()
			for i := range 100 {
				key := fmt.Sprintf("%s-k-%d", id, i%7)
				if err := nodes[id].Put(ctx, key, i, nil); err != nil {
					errs <- fmt.Errorf("%s Put: %w", id, err)
					return
				}
				if _, err := nodes[id].Get(ctx, key); err != nil {
					errs <- fmt.Errorf("%s Get: %w", id, err)
					return
				}
			}
		}()
	}
	ops.Wait()
	stop.Store(true)
	bg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// (h) StopBackgroundLoops stops the heartbeat loop: no pings after it returns.
func TestStopBackgroundLoopsStopsTheHeartbeatLoop(t *testing.T) {
	nodes, _ := startLoopCluster(t, 61071, "node-1", "node-2")
	pinger, peer := nodes["node-1"], nodes["node-2"]
	pinger.StartHeartbeatLoop(context.Background(), 20*time.Millisecond)

	eventually(t, 3*time.Second, "pings to arrive", func() bool { return peer.pingsReceived.Load() >= 3 })

	stopped := make(chan struct{})
	go func() { pinger.StopBackgroundLoops(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("StopBackgroundLoops did not return")
	}

	time.Sleep(100 * time.Millisecond) // let a ping already on the wire land
	before := peer.pingsReceived.Load()
	time.Sleep(300 * time.Millisecond)
	if after := peer.pingsReceived.Load(); after != before {
		t.Fatalf("%d pings arrived after the heartbeat loop was stopped", after-before)
	}
}

// A peer that accepts connections but never speaks is found dead too.
func TestHeartbeatMarksARawTCPBlackHoleDead(t *testing.T) {
	addrs := map[string]string{"node-1": "localhost:61081", "node-2": "localhost:61082"}
	tcpBlackHole(t, addrs["node-2"])
	node1 := New("node-1", addrs["node-1"], 2, 1, 1, neighborsOf(addrs, "node-1"))
	fastHealth(node1)
	beat(t, node1)
	eventually(t, 5*time.Second, "the black hole to be marked dead", func() bool { return node1.isDead("node-2") })
}
