package node

import (
	"bytes"
	"context"
	"log"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/rpc"
)

// slowBackoff makes every peer connection dialed during the test wait 8-12 s
// (base and maximum delay 10 s, with gRPC's jitter) between reconnect
// attempts, so a reconnect over a new connection can't be mistaken for one the
// backoff allowed.
func slowBackoff(t *testing.T) {
	t.Helper()
	prev := dialPeer
	dialPeer = func(addr string, _ time.Duration) (*rpc.Client, error) {
		cfg := backoff.DefaultConfig
		cfg.BaseDelay, cfg.MaxDelay = 10*time.Second, 10*time.Second
		return rpc.DialBackoff(addr, cfg)
	}
	t.Cleanup(func() { dialPeer = prev })
}

// countPingBacks counts the ping-backs started to each peer.
func countPingBacks(t *testing.T) func(peerID string) int {
	t.Helper()
	var mu sync.Mutex
	counts := make(map[string]int)
	testHookPingBack = func(peerID string) {
		mu.Lock()
		defer mu.Unlock()
		counts[peerID]++
	}
	t.Cleanup(func() { testHookPingBack = nil })
	return func(peerID string) int {
		mu.Lock()
		defer mu.Unlock()
		return counts[peerID]
	}
}

// pingBackRunning reports whether nd has a ping-back to peerID in progress.
func pingBackRunning(nd *Node, peerID string) bool {
	nd.healthMu.RLock()
	defer nd.healthMu.RUnlock()
	return nd.pingingBack[peerID]
}

// serveClosing serves nd at addr; at cleanup the listener is stopped, then nd
// closed (which waits for its background work, ping-backs included).
func serveClosing(t *testing.T, nd *Node, addr string) {
	t.Helper()
	t.Cleanup(func() { nd.Close() })
	serveNode(t, nd, addr)
}

// coldStart is the cold-start case: node-a (N=3, W=2) is up while node-b and
// node-c, on known addresses, are not. node-a's heartbeat loop is not started;
// the test drives its rounds. After three rounds node-a has marked both dead,
// and its connections to them are waiting out a slow backoff (slowBackoff).
func coldStart(t *testing.T) (a, b, c *Node) {
	t.Helper()
	slowBackoff(t)
	addrs := reserveAddrs(t, "node-a")
	maps.Copy(addrs, knownAddrs("node-b", "node-c"))
	a = New("node-a", addrs["node-a"], 3, 2, 2, neighborsOf(addrs, "node-a"))
	b = New("node-b", addrs["node-b"], 3, 2, 2, neighborsOf(addrs, "node-b"))
	c = New("node-c", addrs["node-c"], 3, 2, 2, neighborsOf(addrs, "node-c"))
	for _, nd := range []*Node{a, b, c} {
		nd.QuorumConfig.HeartbeatTimeout = 500 * time.Millisecond
		nd.QuorumConfig.MaxMissedHeartbeats = 3
	}
	serveClosing(t, a, addrs["node-a"])

	for range a.QuorumConfig.MaxMissedHeartbeats {
		a.heartbeatRound(context.Background())
	}
	if !a.isDead("node-b") || !a.isDead("node-c") {
		t.Fatalf("node-a should have marked node-b and node-c dead: %+v", a.PeerStatuses())
	}
	return a, b, c
}

// A node that started before its peers has marked them dead, and its
// connections to them are in reconnect backoff. Once they are up and have
// pinged it, it pings them back at once over a new connection, so a write
// through it succeeds right away, without waiting for its next heartbeat round
// or for the backoff to run out.
func TestPeersThatStartLaterAreWritableOnceTheyPing(t *testing.T) {
	a, b, c := coldStart(t)
	serveClosing(t, b, b.Address)
	serveClosing(t, c, c.Address)
	b.heartbeatRound(context.Background()) // pings node-a (and node-c)
	c.heartbeatRound(context.Background())

	start := time.Now()
	var err error
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err = a.Put(ctx, "greeting", "hello", nil)
		cancel()
		if err == nil || time.Since(start) > time.Second {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("a write through node-a still failed %v after node-b and node-c pinged it: %v (peers: %+v)",
			time.Since(start).Round(time.Millisecond), err, a.PeerStatuses())
	}
}

// A ping that claims to come from a member but gives an address other than
// the one the membership lists for it starts nothing: the member stays dead
// and is not pinged. The mismatch is logged once, not on every such ping.
func TestPingFromAWrongAddressTriggersNothing(t *testing.T) {
	var buf bytes.Buffer
	var mu sync.Mutex
	log.SetOutput(writerFunc(func(p []byte) (int, error) { mu.Lock(); defer mu.Unlock(); return buf.Write(p) }))
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	pingBacks := countPingBacks(t)

	a, b, _ := coldStart(t)
	serveClosing(t, b, b.Address) // up, so a ping sent to it would land
	missesBefore := misses(a, "node-b")
	pingsBefore := b.pingsReceived.Load()

	const wrongAddr = "127.0.0.1:1"
	for range 3 {
		a.HandlePing(context.Background(), rpc.PingInfo{SenderID: "node-b", SenderAddress: wrongAddr})
	}
	a.StopBackgroundLoops() // waits for any ping-back that was started

	if n := pingBacks("node-b"); n != 0 {
		t.Fatalf("%d ping-backs started to node-b for pings from the wrong address", n)
	}
	if n := b.pingsReceived.Load() - pingsBefore; n != 0 {
		t.Fatalf("node-b received %d pings from node-a", n)
	}
	if !a.isDead("node-b") || misses(a, "node-b") != missesBefore {
		t.Fatalf("node-b's health changed: dead=%v misses=%d (was %d)", a.isDead("node-b"), misses(a, "node-b"), missesBefore)
	}
	mu.Lock()
	defer mu.Unlock()
	logged := 0
	for _, line := range strings.Split(buf.String(), "\n") {
		if strings.Contains(line, "ADDRESS MISMATCH") && strings.Contains(line, wrongAddr) {
			logged++
		}
	}
	if logged != 1 {
		t.Fatalf("the mismatch was logged %d times, want once:\n%s", logged, buf.String())
	}
}

// Only a peer this node failed to reach (or hasn't pinged yet) is pinged
// back: a ping from one it reaches starts nothing.
func TestPingFromAReachablePeerTriggersNoPingBack(t *testing.T) {
	pingBacks := countPingBacks(t)
	addrs := reserveAddrs(t, "node-a", "node-b")
	a := New("node-a", addrs["node-a"], 2, 1, 1, neighborsOf(addrs, "node-a"))
	b := New("node-b", addrs["node-b"], 2, 1, 1, neighborsOf(addrs, "node-b"))
	serveClosing(t, a, addrs["node-a"])
	serveClosing(t, b, addrs["node-b"])

	// node-a has never pinged node-b, so node-b's first ping is answered with
	// a ping-back, after which node-a reaches node-b.
	b.heartbeatRound(context.Background())
	eventually(t, 3*time.Second, "node-a's ping-back to node-b", func() bool {
		return a.PeerStatuses()["node-b"].Reachable && !pingBackRunning(a, "node-b")
	})
	if n := pingBacks("node-b"); n != 1 {
		t.Fatalf("%d ping-backs to node-b for its first ping, want 1", n)
	}

	pingsBefore := b.pingsReceived.Load()
	b.heartbeatRound(context.Background())
	a.StopBackgroundLoops()
	if n := pingBacks("node-b"); n != 1 {
		t.Fatalf("a ping from node-b, which node-a reaches, started a ping-back (%d in all)", n)
	}
	if n := b.pingsReceived.Load() - pingsBefore; n != 0 {
		t.Fatalf("node-b received %d pings from node-a", n)
	}
}

// isRetired reports whether c is on nd's retired list.
func isRetired(nd *Node, c *rpc.Client) bool {
	nd.clientsMu.Lock()
	defer nd.clientsMu.Unlock()
	return slices.Contains(nd.retired, c)
}

// The connection a ping-back replaces is retired at once and, after the grace
// period, closed and dropped from the retired list, so ping-backs (one per
// peer restart) don't pile up connections that keep reconnecting until Close.
func TestAPingBacksOldConnectionIsClosedAfterAGracePeriod(t *testing.T) {
	a, b, _ := coldStart(t)
	a.QuorumConfig.ReplicationTimeout = 50 * time.Millisecond // a 100 ms grace period
	a.clientsMu.Lock()
	old := a.clients["node-b"].client
	a.clientsMu.Unlock()
	if old == nil {
		t.Fatal("node-a has no cached connection to node-b after pinging it")
	}

	serveClosing(t, b, b.Address)
	b.heartbeatRound(context.Background()) // node-a pings back over a new connection
	eventually(t, time.Second, "node-a to reach node-b", func() bool { return a.PeerStatuses()["node-b"].Reachable })
	a.clientsMu.Lock()
	replaced := a.clients["node-b"].client != old
	a.clientsMu.Unlock()
	if !replaced {
		t.Fatal("the ping-back did not replace node-a's connection to node-b")
	}

	eventually(t, 3*time.Second, "the old connection to leave the retired list", func() bool { return !isRetired(a, old) })
	_, err := old.Ping(context.Background(), rpc.PingInfo{SenderID: "node-a"})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("a Ping over the old connection returned %v, want Canceled (connection closed)", err)
	}
}
