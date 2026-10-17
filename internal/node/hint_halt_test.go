package node

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"distributed-kv-datastore/internal/storage/engine"
	"distributed-kv-datastore/internal/storetest"
)

// TestHaltedReplicaGetsHintsAndReceivesThemAfterReopening: a replica whose
// data engine has halted writes answers Replicate with Unavailable, both
// for the write whose failed fsync halted it and for the writes after, so
// the coordinator stores a hint for each, as it does for a node that is
// down. While the replica is still halted, delivery stops at its first
// hint and keeps them all; once it has restarted on the same data, one
// delivery round brings it every write it missed, without anti-entropy.
func TestHaltedReplicaGetsHintsAndReceivesThemAfterReopening(t *testing.T) {
	addrs, peersOf := hintCluster(t)
	node1 := persistentCoordinator(t, t.TempDir(), peersOf("node-1"), 2) // N=3, W=2: node-1 and node-2 make the quorum
	node1.QuorumConfig.ReplicationTimeout = time.Second
	serveNode(t, newTestNode(t, "node-2", addrs["node-2"], 3, 2, 1, peersOf("node-2")), addrs["node-2"])

	dir3 := t.TempDir()
	// Armed for dir3 only: node-3's hint store, in dir3/hints, keeps working.
	faults := &storetest.FailingSyncs{}
	node3, err := New("node-3", addrs["node-3"], 3, 2, 1, peersOf("node-3"), dir3, 1<<20,
		WithEngineOptions(engine.WithWALOpenFile(faults.Open)))
	if err != nil {
		t.Fatalf("New(node-3): %v", err)
	}
	listener3 := serveAt(t, addrs["node-3"], node3.Store, node3)
	faults.Arm(dir3)

	// "first" is node-3's first write: its failed fsync halts the engine.
	// "second" comes after the halt and fails fast.
	keys := []string{"first", "second"}
	for _, key := range keys {
		if err := node1.Put(context.Background(), key, "v-"+key, nil); err != nil {
			t.Fatalf("Put %q: %v (W=2 needs only node-1 and node-2)", key, err)
		}
	}
	if !node3.WritesHalted() {
		t.Fatal("node-3's writes are not halted after its fsync failed")
	}
	for deadline := time.Now().Add(5 * time.Second); !reflect.DeepEqual(pendingFor(t, node1, "node-3"), keys); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("hints pending for node-3: %v, want %v: a halted replica's Unavailable must be hinted", pendingFor(t, node1, "node-3"), keys)
		}
	}

	// Still halted: the first hint finds node-3 Unavailable, which ends its
	// turn this round, and every hint stays pending.
	var attempts atomic.Int32
	testHookBeforeDeliveringHint = func(target, key string) { attempts.Add(1) }
	node1.deliverHints(context.Background())
	testHookBeforeDeliveringHint = nil
	if n := attempts.Load(); n != 1 {
		t.Fatalf("%d delivery attempts to the halted replica, want 1: Unavailable ends its turn", n)
	}
	if got := pendingFor(t, node1, "node-3"); !reflect.DeepEqual(got, keys) {
		t.Fatalf("hints pending after delivering to the halted replica: %v, want %v", got, keys)
	}

	// Restart node-3 on its data, without the fault: it replays its WAL and
	// accepts writes again.
	listener3.Stop()
	if err := node3.Close(); err != nil {
		t.Fatalf("close node-3: %v", err)
	}
	restarted, err := New("node-3", addrs["node-3"], 3, 2, 1, peersOf("node-3"), dir3, 1<<20)
	if err != nil {
		t.Fatalf("reopen node-3: %v", err)
	}
	t.Cleanup(func() { restarted.Close() })
	serveAt(t, addrs["node-3"], restarted.Store, restarted)
	waitReachable(t, node1, "node-3")

	node1.deliverHints(context.Background())

	for _, key := range keys {
		items, found, err := restarted.Store.Get(key)
		if err != nil || !found || len(items) != 1 || items[0].Value != "v-"+key {
			t.Fatalf("%q on the restarted node-3: found=%v %v err=%v, want v-%s from its hint", key, found, itemValues(items), err, key)
		}
	}
	if pending := pendingFor(t, node1, "node-3"); len(pending) != 0 {
		t.Fatalf("hints still pending after delivery: %v", pending)
	}
}
