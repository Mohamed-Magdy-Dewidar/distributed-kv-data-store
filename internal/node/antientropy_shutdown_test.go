package node

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestCloseWaitsForAnAntiEntropyRoundInProgress: a persistent node's
// anti-entropy round has fetched a peer's version and is about to install
// it when the node is closed. Close must wait for the round — so the
// install lands in the still-open engine and survives a restart — rather
// than close the engine underneath it.
func TestCloseWaitsForAnAntiEntropyRoundInProgress(t *testing.T) {
	dir := t.TempDir()
	a := reserveAddrs(t, "node-1", "node-2")
	a1, a2 := a["node-1"], a["node-2"]

	node1, err := New("node-1", a1, 2, 2, 1, map[string]string{"node-2": a2}, dir, 1<<20)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	serveAt(t, a1, node1.Store, node1)
	node2 := startTestNode(t, "node-2", a2, map[string]string{"node-1": a1})
	node2.Store.Put("k", "from-node-2", nil) // node-1 must install it

	reached := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	testHookBeforeReconcileInstall = func(key string) {
		if key == "k" {
			once.Do(func() { close(reached) })
			<-release
		}
	}
	defer func() { testHookBeforeReconcileInstall = nil }()

	node1.StartAntiEntropyLoop(context.Background(), 10*time.Millisecond)
	<-reached // a round has read both sides and is about to install

	closeDone := make(chan error, 1)
	go func() { closeDone <- node1.Close() }()
	select {
	case err := <-closeDone:
		close(release)
		t.Fatalf("Close returned (err %v) while an anti-entropy round was still in progress", err)
	case <-time.After(200 * time.Millisecond): // Close is waiting on the round
	}

	close(release)
	if err := <-closeDone; err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := New("node-1", a1, 2, 2, 1, map[string]string{"node-2": a2}, dir, 1<<20)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()
	items, found, err := reopened.Store.Get("k")
	if err != nil || !found || len(items) != 1 || items[0].Value != "from-node-2" {
		t.Fatalf("expected the round's install to be durable, got found=%v %v (err %v)", found, itemValues(items), err)
	}
}

// TestStartAntiEntropyLoopAfterStopDoesNothing: once StopBackgroundLoops has
// run (as Close runs it), no new loop may start — it would run against a
// closed engine with nothing left to wait for it.
func TestStartAntiEntropyLoopAfterStopDoesNothing(t *testing.T) {
	n := newTestNode(t, "node-1", "unused", 1, 1, 1, nil)
	n.StopBackgroundLoops()
	n.StartAntiEntropyLoop(context.Background(), time.Millisecond)

	n.bgMu.Lock()
	loops := len(n.bgCancels)
	n.bgMu.Unlock()
	if loops != 0 {
		t.Fatalf("expected no loop to start after StopBackgroundLoops, %d did", loops)
	}
	n.StopBackgroundLoops() // idempotent; must not block
}

// TestStopBackgroundLoopsDuringFirstDelayReturnsPromptly: the loop waits a
// random fraction of interval before its first round. Stopping it in that
// window must not wait the delay out — at a 30s anti-entropy interval that
// would hold shutdown for up to 30s.
func TestStopBackgroundLoopsDuringFirstDelayReturnsPromptly(t *testing.T) {
	n := newTestNode(t, "node-1", "unused", 1, 1, 1, nil)
	n.StartAntiEntropyLoop(context.Background(), time.Hour)

	stopped := make(chan struct{})
	go func() {
		n.StopBackgroundLoops()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("StopBackgroundLoops is still waiting out the loop's first delay")
	}
}
