package node

import (
	"context"
	"sync"
	"testing"
	"time"

	"distributed-kv-datastore/internal/rpc"
)

// TestCloseWaitsForAnAntiEntropyRoundInProgress: a persistent node's
// anti-entropy round has fetched a peer's version and is about to install
// it when the node is closed. Close must wait for the round — so the
// install lands in the still-open engine and survives a restart — rather
// than close the engine underneath it.
func TestCloseWaitsForAnAntiEntropyRoundInProgress(t *testing.T) {
	dir := t.TempDir()
	a1, a2 := "localhost:60531", "localhost:60532"

	node1, err := NewPersistent("node-1", a1, 2, 2, 1, map[string]string{"node-2": a2}, dir, 1<<20)
	if err != nil {
		t.Fatalf("NewPersistent failed: %v", err)
	}
	listener, err := rpc.Serve(a1, node1.Store, node1)
	if err != nil {
		t.Fatalf("serve node-1: %v", err)
	}
	t.Cleanup(listener.Stop)
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

	reopened, err := NewPersistent("node-1", a1, 2, 2, 1, map[string]string{"node-2": a2}, dir, 1<<20)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()
	items, found, err := reopened.Store.Get("k")
	if err != nil || !found || len(items) != 1 || items[0].Value != "from-node-2" {
		t.Fatalf("expected the round's install to be durable, got found=%v %v (err %v)", found, itemValues(items), err)
	}
}

// TestStartAntiEntropyLoopAfterStopDoesNothing: once StopAntiEntropy has
// run (as Close runs it), no new loop may start — it would run against a
// closed engine with nothing left to wait for it.
func TestStartAntiEntropyLoopAfterStopDoesNothing(t *testing.T) {
	n := New("node-1", "localhost:60533", 1, 1, 1, nil)
	n.StopAntiEntropy()
	n.StartAntiEntropyLoop(context.Background(), time.Millisecond)

	n.aeMu.Lock()
	loops := len(n.aeCancels)
	n.aeMu.Unlock()
	if loops != 0 {
		t.Fatalf("expected no loop to start after StopAntiEntropy, %d did", loops)
	}
	n.StopAntiEntropy() // idempotent; must not block
}
