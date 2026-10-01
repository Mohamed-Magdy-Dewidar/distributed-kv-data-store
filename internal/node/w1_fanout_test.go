package node

import (
	"context"
	"testing"
)

// TestW1FansOutToLivePeerEvenThoughLocalWriteAloneSatisfiesW: with W=1, the
// local write alone reaches quorum before any peer answers — but Dynamo's
// model is send to all N, wait for W, not send only as many as W requires.
// node-2 must still receive the write, not be left to anti-entropy alone.
func TestW1FansOutToLivePeerEvenThoughLocalWriteAloneSatisfiesW(t *testing.T) {
	addrs, peersOf := hintCluster(t, "node-3")
	node1 := persistentCoordinator(t, t.TempDir(), peersOf("node-1"), 1)
	node2 := newTestNode(t, "node-2", addrs["node-2"], 3, 1, 1, peersOf("node-2"))
	serveNode(t, node2, addrs["node-2"])

	if err := node1.Put(context.Background(), "k", "v", nil); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := node1.Close(); err != nil { // waits for the drain
		t.Fatalf("Close failed: %v", err)
	}

	if _, found, _ := node2.Store.Get("k"); !found {
		t.Fatal("expected node-2 to receive the write even though W=1 was already satisfied locally")
	}
}

// TestW1StillHintsUnreachablePeers: with W=1, the local write alone
// satisfies quorum immediately — but every unreachable replica must still
// get a durable hint, just as it would at a higher W, not be silently left
// unwritten until anti-entropy happens to reconcile it.
func TestW1StillHintsUnreachablePeers(t *testing.T) {
	dir := t.TempDir()
	_, peersOf := hintCluster(t, "node-2", "node-3")
	node1 := persistentCoordinator(t, dir, peersOf("node-1"), 1)
	// node-2 and node-3 are never started: both Unavailable.

	if err := node1.Put(context.Background(), "k", "v", nil); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := node1.Close(); err != nil { // waits for the drain
		t.Fatalf("Close failed: %v", err)
	}

	for _, target := range []string{"node-2", "node-3"} {
		if got := pendingHintKeys(t, dir, target); len(got) != 1 || got[0] != "k" {
			t.Fatalf("expected a hint for k pending for %s, got %v", target, got)
		}
	}
}
