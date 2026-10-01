package node

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"distributed-kv-datastore/internal/hashring"
	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/store"
	"distributed-kv-datastore/internal/vectorclock"
)

// startUniformCluster starts a full-mesh cluster in which every node has
// the same N/W/R — as a real cluster must, now that a non-replica forwards
// writes to a replica that coordinates with its own settings. stores
// optionally replaces a node's store; it's installed before the node's
// server starts, so server goroutines never race with the swap.
func startUniformCluster(t *testing.T, addrs map[string]string, n, w, r int, stores map[string]*store.DataStore) (map[string]*Node, map[string]*rpc.Listener) {
	t.Helper()

	nodes := make(map[string]*Node, len(addrs))
	listeners := make(map[string]*rpc.Listener, len(addrs))
	for id, addr := range addrs {
		neighbors := make(map[string]string, len(addrs)-1)
		for peerID, peerAddr := range addrs {
			if peerID != id {
				neighbors[peerID] = peerAddr
			}
		}
		nd := newTestNode(t, id, addr, n, w, r, neighbors)
		if ds, ok := stores[id]; ok {
			nd.Store = ds
		}
		listener := serveAt(t, addr, nd.Store, nd)
		nodes[id] = nd
		listeners[id] = listener
	}
	return nodes, listeners
}

// keyReplicatedOnlyBy finds a key whose N-node preference list (computed
// on a ring of ids, exactly as every node builds its own) avoids every
// node in excluded, returning the key and that preference list.
func keyReplicatedOnlyBy(t *testing.T, ids []string, n int, excluded ...string) (string, []string) {
	t.Helper()
	ring := hashring.NewHashRing(defaultVirtualNodesPerPhysical)
	for _, id := range ids {
		ring.AddNode(id)
	}
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("forward-key-%d", i)
		list := ring.GetPreferenceList(key, n)
		ok := len(list) == n
		for _, id := range list {
			for _, ex := range excluded {
				if id == ex {
					ok = false
				}
			}
		}
		if ok {
			return key, list
		}
	}
	t.Fatalf("no key whose N=%d preference list excludes %v", n, excluded)
	return "", nil
}

// sortedClockNodes returns the sorted node IDs a clock names. Clock entries
// are "<nodeID>#<incarnation>" (see Node.ClockID); this strips the
// incarnation so they can be compared with ring members.
func sortedClockNodes(vc *vectorclock.VectorClock) []string {
	var ids []string
	for entry := range vc.Snapshot() {
		ids = append(ids, nodeOfClockEntry(entry))
	}
	sort.Strings(ids)
	return ids
}

// nodeOfClockEntry strips the "#<incarnation>" suffix from a clock entry.
func nodeOfClockEntry(entry string) string {
	if i := strings.LastIndex(entry, "#"); i >= 0 {
		return entry[:i]
	}
	return entry
}

// assertOnlyReplicaVersion checks that replica holds exactly one version of
// key, with value want, whose clock names only nodes in replicas.
func assertOnlyReplicaVersion(t *testing.T, nd *Node, key string, want any, replicas []string) *vectorclock.VectorClock {
	t.Helper()
	items, _, err := nd.Store.Get(key)
	if err != nil {
		t.Fatalf("%s: Get failed: %v", nd.ID, err)
	}
	if len(items) != 1 || items[0].Value != want {
		clocks := make([]map[string]uint32, 0, len(items))
		for _, it := range items {
			clocks = append(clocks, it.VectorClock.Snapshot())
		}
		t.Fatalf("%s: expected exactly one version %v, got values=%v clocks=%v", nd.ID, want, itemValues(items), clocks)
	}
	isReplica := make(map[string]bool, len(replicas))
	for _, id := range replicas {
		isReplica[id] = true
	}
	for _, id := range sortedClockNodes(items[0].VectorClock) {
		if !isReplica[id] {
			t.Errorf("%s: clock %v has an entry for %s, which is not one of key's replicas %v",
				nd.ID, items[0].VectorClock.Snapshot(), id, replicas)
		}
	}
	return items[0].VectorClock
}

// TestSequentialWritesViaDifferentCoordinatorsStaySequential: two
// sequential writes to one key, routed through two different non-replica
// coordinators, must leave clocks naming only actual replicas — never
// either coordinator — and the second must supersede the first rather than
// compare as Concurrent.
func TestSequentialWritesViaDifferentCoordinatorsStaySequential(t *testing.T) {
	addrs := reserveAddrs(t, "node-1", "node-2", "node-3", "node-4")
	nodes, _ := startUniformCluster(t, addrs, 2, 2, 2, nil)
	key, replicas := keyReplicatedOnlyBy(t, []string{"node-1", "node-2", "node-3", "node-4"}, 2, "node-1", "node-4")
	ctx := context.Background()

	if err := nodes["node-1"].Put(ctx, key, "v1", nil); err != nil {
		t.Fatalf("first Put (via node-1) failed: %v", err)
	}
	firstClocks := make(map[string]*vectorclock.VectorClock, len(replicas))
	for _, id := range replicas {
		firstClocks[id] = assertOnlyReplicaVersion(t, nodes[id], key, "v1", replicas)
	}

	if err := nodes["node-4"].Put(ctx, key, "v2", nil); err != nil {
		t.Fatalf("second Put (via node-4) failed: %v", err)
	}
	for _, id := range replicas {
		second := assertOnlyReplicaVersion(t, nodes[id], key, "v2", replicas)
		if got := second.Compare(firstClocks[id]); got != vectorclock.After {
			t.Fatalf("%s: expected v2's clock %v to come After v1's %v, got %v",
				id, second.Snapshot(), firstClocks[id].Snapshot(), got)
		}
	}
}

// TestSequentialWritesViaSameNonReplicaCoordinatorAreNotLost: the second
// of two nil-context writes through the same non-replica coordinator used
// to get an identical clock, which Resolve drops as a duplicate — while
// Put still reported success.
func TestSequentialWritesViaSameNonReplicaCoordinatorAreNotLost(t *testing.T) {
	addrs := reserveAddrs(t, "node-1", "node-2", "node-3", "node-4")
	nodes, _ := startUniformCluster(t, addrs, 2, 2, 2, nil)
	key, replicas := keyReplicatedOnlyBy(t, []string{"node-1", "node-2", "node-3", "node-4"}, 2, "node-1")
	ctx := context.Background()

	for _, v := range []string{"v1", "v2"} {
		if err := nodes["node-1"].Put(ctx, key, v, nil); err != nil {
			t.Fatalf("Put %s failed: %v", v, err)
		}
	}
	for _, id := range replicas {
		assertOnlyReplicaVersion(t, nodes[id], key, "v2", replicas)
	}
}

// TestForwardFailsOverWhenReplicaUnavailable: if the first replica in the
// preference list is down, the write goes to the next one, which versions
// it on its own entry.
func TestForwardFailsOverWhenReplicaUnavailable(t *testing.T) {
	addrs := reserveAddrs(t, "node-1", "node-2", "node-3")
	key, replicas := keyReplicatedOnlyBy(t, []string{"node-1", "node-2", "node-3"}, 2, "node-1")
	nodes, listeners := startUniformCluster(t, addrs, 2, 1, 1, nil)
	listeners[replicas[0]].Stop()

	if err := nodes["node-1"].Put(context.Background(), key, "v", nil); err != nil {
		t.Fatalf("expected the write to fail over to %s, got %v", replicas[1], err)
	}
	vc := assertOnlyReplicaVersion(t, nodes[replicas[1]], key, "v", replicas)
	if got := sortedClockNodes(vc); !reflect.DeepEqual(got, []string{replicas[1]}) {
		t.Fatalf("expected the clock to name only %s, got %v", replicas[1], vc.Snapshot())
	}
}

// TestForwardDoesNotFailOverWhenReplicaFailsTheWrite: a replica that
// received the write and then failed it (here, its store is broken) may
// already have applied it, so the forwarder must return the error rather
// than version the write again on another replica. With W=1, failing over
// would have succeeded on the healthy replica.
func TestForwardDoesNotFailOverWhenReplicaFailsTheWrite(t *testing.T) {
	addrs := reserveAddrs(t, "node-1", "node-2", "node-3")
	key, replicas := keyReplicatedOnlyBy(t, []string{"node-1", "node-2", "node-3"}, 2, "node-1")
	nodes, _ := startUniformCluster(t, addrs, 2, 1, 1, map[string]*store.DataStore{
		replicas[0]: store.NewDataStoreWithPersister(replicas[0], failingPersister{}),
	})

	if err := nodes["node-1"].Put(context.Background(), key, "v", nil); err == nil {
		t.Fatal("expected the first replica's failure to fail the write")
	}
	if items, found, _ := nodes[replicas[1]].Store.Get(key); found {
		t.Fatalf("expected no failover to %s, but it holds %v", replicas[1], itemValues(items))
	}
}

// TestCoordinatePutRefusesWhenNotAReplica: a node asked to coordinate a
// key it doesn't replicate must refuse, never forward the write onward.
func TestCoordinatePutRefusesWhenNotAReplica(t *testing.T) {
	key, _ := keyReplicatedOnlyBy(t, []string{"node-1", "node-2", "node-3"}, 2, "node-1")
	a := knownAddrs("node-1", "node-2", "node-3") // never dialed
	nd := newTestNode(t, "node-1", a["node-1"], 2, 1, 1, map[string]string{"node-2": a["node-2"], "node-3": a["node-3"]})
	if err := nd.CoordinatePut(context.Background(), key, "v", nil); !errors.Is(err, rpc.ErrNotReplica) {
		t.Fatalf("expected rpc.ErrNotReplica, got %v", err)
	}
}
