package node

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/rpc"
)

// membersWithAddr returns a copy of nd's current members with id's address
// changed (or id added).
func membersWithAddr(nd *Node, id, addr string) map[string]string {
	_, members := nd.Membership()
	members[id] = addr
	return members
}

// (a) Publishing new views while Put, Get and anti-entropy run must not race,
// and none of them may fail: every view holds the same members, so every
// operation is valid whichever one it loads.
func TestNewViewsPublishedWhileOperationsRun(t *testing.T) {
	addrs := reserveAddrs(t, "node-1", "node-2", "node-3")
	nodes, _ := startUniformCluster(t, addrs, 3, 2, 2, nil)
	node1 := nodes["node-1"]

	_, members := node1.Membership()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var stop atomic.Bool
	var swaps sync.WaitGroup
	swaps.Add(1)
	go func() {
		defer swaps.Done()
		for epoch := uint64(1); !stop.Load(); epoch++ {
			if changed, err := node1.SetMembership(epoch, members); err != nil || !changed {
				t.Errorf("SetMembership(%d): changed=%v err=%v", epoch, changed, err)
				return
			}
		}
	}()

	var ops sync.WaitGroup
	fail := make(chan error, 3)
	run := func(name string, op func(i int) error) {
		ops.Add(1)
		go func() {
			defer ops.Done()
			for i := range 30 {
				if err := op(i); err != nil {
					fail <- fmt.Errorf("%s #%d: %w", name, i, err)
					return
				}
			}
		}()
	}
	run("Put", func(i int) error { return node1.Put(ctx, fmt.Sprintf("k-%d", i%5), i, nil) })
	run("Get", func(i int) error { _, err := node1.Get(ctx, fmt.Sprintf("k-%d", i%5)); return err })
	run("RunAntiEntropy", func(int) error { return node1.RunAntiEntropy(ctx, "node-2") })

	ops.Wait()
	stop.Store(true)
	swaps.Wait()
	close(fail)
	for err := range fail {
		t.Error(err)
	}
}

// (b) When a peer's address changes in a new view, the next RPC dials the new
// address; the old connection is retired (still open, so an RPC in flight on
// it can finish) until Close closes it.
func TestAddressChangeDialsNewAddressAndRetiresTheOldClient(t *testing.T) {
	a := reserveAddrs(t, "node-1", "old", "new")
	oldAddr, newAddr := a["old"], a["new"]
	node1 := New("node-1", a["node-1"], 2, 1, 1, map[string]string{"node-2": oldAddr})

	// Two distinguishable servers: the one at the old address holds "old".
	oldPeer := New("node-2", oldAddr, 2, 1, 1, map[string]string{"node-1": a["node-1"]})
	newPeer := New("node-2", newAddr, 2, 1, 1, map[string]string{"node-1": a["node-1"]})
	oldPeer.Store.Put("k", "old", nil)
	newPeer.Store.Put("k", "new", nil)
	serveNode(t, oldPeer, oldAddr)
	serveNode(t, newPeer, newAddr)

	fetch := func() any {
		t.Helper()
		items, found, err := node1.FetchItem(context.Background(), "node-2", "k")
		if err != nil || !found || len(items) != 1 {
			t.Fatalf("FetchItem: %v found=%v items=%v", err, found, itemValues(items))
		}
		return items[0].Value
	}

	if got := fetch(); got != "old" {
		t.Fatalf("before the change: got %v from node-2, want the old address's %q", got, "old")
	}
	oldClient, err := node1.getOrDialClient("node-2")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := node1.SetMembership(1, membersWithAddr(node1, "node-2", newAddr)); err != nil {
		t.Fatal(err)
	}

	if got := fetch(); got != "new" {
		t.Fatalf("after the change: got %v from node-2, want the new address's %q (traffic still goes to the old address)", got, "new")
	}
	newClient, err := node1.getOrDialClient("node-2")
	if err != nil {
		t.Fatal(err)
	}
	if newClient == oldClient {
		t.Fatal("the cached client was not replaced")
	}

	// Retired, not closed: the old client still works.
	if items, _, err := oldClient.FetchItem(context.Background(), "k"); err != nil || len(items) != 1 || items[0].Value != "old" {
		t.Fatalf("the retired client should still be usable, got %v, %v", itemValues(items), err)
	}
	node1.clientsMu.Lock()
	active, retired := len(node1.clients), len(node1.retired)
	node1.clientsMu.Unlock()
	if active != 1 || retired != 1 {
		t.Fatalf("expected 1 active and 1 retired client, got %d and %d", active, retired)
	}

	if err := node1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for name, c := range map[string]*rpc.Client{"retired": oldClient, "active": newClient} {
		if _, _, err := c.FetchItem(context.Background(), "k"); status.Code(err) != codes.Canceled {
			t.Errorf("after Close the %s client should be closed (codes.Canceled), got %v", name, err)
		}
	}
	if len(node1.retired) != 0 {
		t.Errorf("expected Close to empty the retired list, got %d", len(node1.retired))
	}
}

// (c) Once Close has closed the client cache, nothing dials: a late caller
// can't leave a connection nobody will close.
func TestDialAfterCloseIsRefused(t *testing.T) {
	a := knownAddrs("node-1", "node-2") // never served
	node1 := New("node-1", a["node-1"], 2, 1, 1, map[string]string{"node-2": a["node-2"]})
	if err := node1.Close(); err != nil {
		t.Fatal(err)
	}

	if client, err := node1.getOrDialClient("node-2"); err == nil {
		client.Close()
		t.Fatal("expected a dial after Close to be refused")
	}
	node1.clientsMu.Lock()
	cached := len(node1.clients)
	node1.clientsMu.Unlock()
	if cached != 0 {
		t.Fatalf("a refused dial left %d cached clients", cached)
	}
}
