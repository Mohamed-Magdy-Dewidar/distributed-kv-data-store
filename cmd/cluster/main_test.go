package main

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/httpapi"
	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/node"
	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/vectorclock"
)

func replicateTo(t *testing.T, address string) error {
	t.Helper()
	client, err := rpc.Dial(address, 0)
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	item := &model.DataItem{Value: "late", VectorClock: vectorclock.FromSnapshot(map[string]uint32{"outsider": 1}), LastUpdatedBy: "outsider"}
	return client.Replicate(ctx, "late-key", []*model.DataItem{item})
}

// TestShutdownClusterStopsEverythingBeforeClosingEngines runs the real
// shutdown sequence on two persistent nodes whose anti-entropy loops are
// running back to back (a 1ms interval, so rounds are almost always in
// progress — the deterministic version of "a round in progress at
// shutdown" is node's TestCloseWaitsForAnAntiEntropyRoundInProgress).
// Shutdown must close every engine cleanly, leave no listener accepting
// writes, and lose nothing anti-entropy installed.
func TestShutdownClusterStopsEverythingBeforeClosingEngines(t *testing.T) {
	base := t.TempDir()
	addrs := map[string]string{"node-1": "localhost:60541", "node-2": "localhost:60542"}
	open := func(id string) *node.Node {
		neighbors := map[string]string{}
		for peer, addr := range addrs {
			if peer != id {
				neighbors[peer] = addr
			}
		}
		nd, err := node.NewPersistent(id, addrs[id], 2, 2, 1, neighbors, filepath.Join(base, id), maxMemtableBytes)
		if err != nil {
			t.Fatalf("open %s: %v", id, err)
		}
		return nd
	}

	dashboard := httpapi.NewServer()
	var nodes []*node.Node
	for _, id := range []string{"node-1", "node-2"} {
		nd := open(id)
		nodes = append(nodes, nd)
		listener, err := rpc.Serve(addrs[id], nd.Store, nd)
		if err != nil {
			t.Fatalf("serve %s: %v", id, err)
		}
		dashboard.Register(id, nd, listener, addrs[id], 2, 1, nil)
	}
	const perNode = 20
	for i, nd := range nodes {
		for k := range perNode {
			nd.Store.Put(fmt.Sprintf("n%d-k%d", i+1, k), "v", nil) // only on this node: anti-entropy must copy it
		}
	}

	compactionCtx, stopCompaction := context.WithCancel(context.Background())
	for _, nd := range nodes {
		nd.StartAntiEntropyLoop(context.Background(), time.Millisecond)
	}
	converged := func() bool {
		for _, nd := range nodes {
			keys, err := nd.Store.Keys()
			if err != nil || len(keys) != 2*perNode {
				return false
			}
		}
		return true
	}
	for deadline := time.Now().Add(5 * time.Second); !converged(); {
		if time.Now().After(deadline) {
			t.Fatal("test setup: anti-entropy didn't converge the nodes")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if err := shutdownCluster(dashboard, nodes, stopCompaction); err != nil {
		t.Fatalf("shutdownCluster failed: %v", err)
	}
	if compactionCtx.Err() == nil {
		t.Fatal("shutdownCluster didn't stop compaction")
	}
	for id, addr := range addrs {
		if err := replicateTo(t, addr); status.Code(err) != codes.Unavailable {
			t.Fatalf("%s: expected its listener stopped (Unavailable) after shutdown, got %v", id, err)
		}
	}

	for _, id := range []string{"node-1", "node-2"} {
		nd := open(id)
		keys, err := nd.Store.Keys()
		if err != nil || len(keys) != 2*perNode {
			t.Errorf("%s: expected all %d keys durable after shutdown, got %d (err %v)", id, 2*perNode, len(keys), err)
		}
		if err := nd.Close(); err != nil {
			t.Errorf("close reopened %s: %v", id, err)
		}
	}
}
