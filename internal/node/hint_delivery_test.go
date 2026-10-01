package node

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// hintedWrite has node-1 (persistent, W=2) write each key with node-2 up
// and node-3 down, then waits until node-3's hints for them are stored.
func hintedWrite(t *testing.T, keys ...string) (node1 *Node, addrs map[string]string, peersOf func(string) map[string]string) {
	t.Helper()
	addrs, peersOf = hintCluster(t, "node-3") // node-3 is started later, on this known address
	node1 = persistentCoordinator(t, t.TempDir(), peersOf("node-1"), 2)
	node1.QuorumConfig.ReplicationTimeout = 300 * time.Millisecond // a down target costs one of these per round
	serveNode(t, newTestNode(t, "node-2", addrs["node-2"], 3, 2, 1, peersOf("node-2")), addrs["node-2"])
	for _, key := range keys {
		if err := node1.Put(context.Background(), key, "v-"+key, nil); err != nil {
			t.Fatalf("Put %q failed: %v", key, err)
		}
	}
	for deadline := time.Now().Add(2 * time.Second); len(pendingFor(t, node1, "node-3")) < len(keys); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("test setup: expected hints for %v, have %v", keys, pendingFor(t, node1, "node-3"))
		}
	}
	return node1, addrs, peersOf
}

func pendingFor(t *testing.T, nd *Node, target string) []string {
	t.Helper()
	pending, err := nd.hints.Pending(target)
	if err != nil {
		t.Fatalf("Pending(%q): %v", target, err)
	}
	var keys []string
	for _, h := range pending {
		keys = append(keys, h.Key)
	}
	return keys
}

// waitReachable waits until from's connection to target works again.
// from's connection failed while target was down, and gRPC backs off
// before reconnecting, failing every call at once until it does; this
// waits that out, so a test's delivery round isn't testing the backoff.
func waitReachable(t *testing.T, from *Node, target string) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if _, _, err := from.FetchItem(context.Background(), target, "probe"); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never became reachable from %s", target, from.ID)
		}
	}
}

// TestHintsAreDeliveredOnceTheTargetIsBack: node-3 comes back up; one
// delivery round sends it every hinted write, versions intact, and retires
// the hints.
func TestHintsAreDeliveredOnceTheTargetIsBack(t *testing.T) {
	node1, addrs, peersOf := hintedWrite(t, "a", "b")
	node3 := newTestNode(t, "node-3", addrs["node-3"], 3, 2, 1, peersOf("node-3"))
	serveNode(t, node3, addrs["node-3"])
	waitReachable(t, node1, "node-3")

	node1.deliverHints(context.Background())

	for _, key := range []string{"a", "b"} {
		want, _, _ := node1.Store.Get(key)
		got, found, _ := node3.Store.Get(key)
		if !found || len(got) != 1 || got[0].Value != want[0].Value ||
			!reflect.DeepEqual(got[0].VectorClock.Snapshot(), want[0].VectorClock.Snapshot()) {
			t.Fatalf("%q: expected node-3 to hold node-1's version %v, got found=%v %v", key, itemValues(want), found, itemValues(got))
		}
	}
	if pending := pendingFor(t, node1, "node-3"); len(pending) != 0 {
		t.Fatalf("expected delivered hints retired, still pending: %v", pending)
	}
}

// TestHintsStayPendingWhileTheTargetIsDown: a round against a target
// that's still unreachable delivers nothing and must retire nothing.
func TestHintsStayPendingWhileTheTargetIsDown(t *testing.T) {
	node1, _, _ := hintedWrite(t, "a", "b")

	node1.deliverHints(context.Background())

	if got := pendingFor(t, node1, "node-3"); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("expected both hints still pending, got %v", got)
	}
}

// TestUnreachableTargetIsSkippedForTheRestOfTheRound: once one hint finds
// its target Unavailable, the round moves on rather than trying (and
// timing out on) every other hint for it.
func TestUnreachableTargetIsSkippedForTheRestOfTheRound(t *testing.T) {
	node1, _, _ := hintedWrite(t, "a", "b", "c")

	var attempts atomic.Int32
	testHookBeforeDeliveringHint = func(target, key string) { attempts.Add(1) }
	defer func() { testHookBeforeDeliveringHint = nil }()

	node1.deliverHints(context.Background())

	if n := attempts.Load(); n != 1 {
		t.Fatalf("expected 1 delivery attempt to the unreachable target, got %d", n)
	}
}

// TestStopWaitsForAHintDeliveryInProgress: the delivery loop has just
// delivered a hint and is about to retire it when its node's background
// loops are stopped. Stopping must wait for that — the hint is retired in
// the still-open hint store — rather than return while the round is still
// using it.
func TestStopWaitsForAHintDeliveryInProgress(t *testing.T) {
	node1, addrs, peersOf := hintedWrite(t, "a")
	node3 := newTestNode(t, "node-3", addrs["node-3"], 3, 2, 1, peersOf("node-3"))
	serveNode(t, node3, addrs["node-3"])
	waitReachable(t, node1, "node-3")

	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	testHookAfterDeliveringHint = func(target, key string) {
		once.Do(func() { close(reached) })
		<-release
	}
	defer func() { testHookAfterDeliveringHint = nil }()

	node1.StartHintDeliveryLoop(context.Background(), 10*time.Millisecond)
	<-reached

	stopped := make(chan struct{})
	go func() {
		node1.StopBackgroundLoops()
		close(stopped)
	}()
	select {
	case <-stopped:
		close(release)
		t.Fatal("StopBackgroundLoops returned while a hint delivery was in progress")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	<-stopped

	if _, found, _ := node3.Store.Get("a"); !found {
		t.Fatal("test setup: expected the hint delivered to node-3")
	}
	if pending := pendingFor(t, node1, "node-3"); len(pending) != 0 {
		t.Fatalf("expected the delivered hint retired before stopping, still pending: %v", pending)
	}
}

// TestStopDuringHintLoopFirstDelayReturnsPromptly: like anti-entropy's
// loop, the delivery loop waits a random fraction of its interval first;
// stopping must not wait that out.
func TestStopDuringHintLoopFirstDelayReturnsPromptly(t *testing.T) {
	node1 := persistentCoordinator(t, t.TempDir(), nil, 1)
	node1.StartHintDeliveryLoop(context.Background(), time.Hour)

	stopped := make(chan struct{})
	go func() {
		node1.StopBackgroundLoops()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("StopBackgroundLoops is still waiting out the hint loop's first delay")
	}
}
