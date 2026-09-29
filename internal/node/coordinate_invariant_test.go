package node

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/store"
)

// countingPersister counts every call into the store's persister, whatever
// its kind, on top of a working in-memory one.
type countingPersister struct {
	*flakyPersister
	accesses atomic.Int32
}

func newCountingPersister() *countingPersister {
	return &countingPersister{flakyPersister: newFlakyPersister()}
}

func (c *countingPersister) Put(key string, item *model.DataItem) error {
	c.accesses.Add(1)
	return c.flakyPersister.Put(key, item)
}

func (c *countingPersister) GetAll(key string) ([]*model.DataItem, bool, error) {
	c.accesses.Add(1)
	return c.flakyPersister.GetAll(key)
}

func (c *countingPersister) Restore(key string, items []*model.DataItem) error {
	c.accesses.Add(1)
	return c.flakyPersister.Restore(key, items)
}

func (c *countingPersister) Keys() ([]string, error) {
	c.accesses.Add(1)
	return c.flakyPersister.Keys()
}

// forwardPut fails over from a replica that answers FailedPrecondition on the
// strength of one fact: Node.CoordinatePut refuses a key it doesn't own
// before it touches its store, so the write was applied nowhere. This test
// holds that line. It sends CoordinatePut over real gRPC to a node that does
// not own the key, and requires FailedPrecondition and not a single access to
// the node's store; a key it does own is the control that the counter works.
func TestCoordinatePutRefusesANonOwnerBeforeAnyStoreAccess(t *testing.T) {
	addrs := reserveAddrs(t, "node-1")
	maps.Copy(addrs, knownAddrs("node-2", "node-3")) // down: an owned write's replication to them fails fast
	nd := New("node-1", addrs["node-1"], 2, 1, 1, neighborsOf(addrs, "node-1"))
	persister := newCountingPersister()
	nd.Store = store.NewDataStoreWithPersister("node-1", persister) // before serving, so no handler races the swap
	serveNode(t, nd, addrs["node-1"])

	var notOwned, owned string
	for i := 0; notOwned == "" || owned == ""; i++ {
		key := fmt.Sprintf("inv-key-%d", i)
		if isOwner, _ := nd.isReplicaFor(nd.membership.Load(), key); isOwner {
			owned = key
		} else {
			notOwned = key
		}
	}

	client, err := rpc.Dial(addrs["node-1"], 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.CoordinatePut(ctx, notOwned, "v", nil)
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("CoordinatePut for a key the node doesn't own: code %v (%v), want FailedPrecondition", got, err)
	}
	if got := persister.accesses.Load(); got != 0 {
		t.Fatalf("the store was accessed %d times before the ownership refusal; forwardPut's failover is only safe with 0", got)
	}

	// The same refusal from the node itself, not just through the server.
	if err := nd.CoordinatePut(ctx, notOwned, "v", nil); !errors.Is(err, rpc.ErrNotReplica) {
		t.Fatalf("Node.CoordinatePut = %v, want rpc.ErrNotReplica", err)
	}
	if got := persister.accesses.Load(); got != 0 {
		t.Fatalf("Node.CoordinatePut accessed the store %d times before refusing", got)
	}

	// Control: a key it owns does reach the store, so the counter can count.
	if err := client.CoordinatePut(ctx, owned, "v", nil); err != nil {
		t.Fatalf("CoordinatePut for an owned key: %v", err)
	}
	if persister.accesses.Load() == 0 {
		t.Fatal("control failed: an owned write never touched the store, so the count above proves nothing")
	}
}
