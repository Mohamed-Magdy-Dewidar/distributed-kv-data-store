package node

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/store"
)

// failingPersister fails every call, standing in for a broken disk under
// a persister-backed DataStore.
type failingPersister struct{}

var errDisk = errors.New("disk on fire")

func (failingPersister) Put(string, *model.DataItem) error { return errDisk }
func (failingPersister) GetAll(string) ([]*model.DataItem, bool, error) {
	return nil, false, errDisk
}
func (failingPersister) Restore(string, []*model.DataItem) error { return errDisk }
func (failingPersister) Keys() ([]string, error)                 { return nil, errDisk }

// TestPutFailsWithoutReplicatingWhenLocalWriteFails: a replica whose local
// store can't persist must return an error and send peers nothing — not a
// nil item, not anything.
func TestPutFailsWithoutReplicatingWhenLocalWriteFails(t *testing.T) {
	node1 := startTestNode(t, "node-1", "localhost:60331", map[string]string{"node-2": "localhost:60332"})
	node2 := startTestNode(t, "node-2", "localhost:60332", map[string]string{"node-1": "localhost:60331"})
	// N=2 of 2 nodes: node-1 is a replica for every key, so Put writes locally.
	node1.Store = store.NewDataStoreWithPersister("node-1", failingPersister{})

	err := node1.Put(context.Background(), "foo", "bar", nil)
	if err == nil || !strings.Contains(err.Error(), "local write failed") {
		t.Fatalf("expected a local write failure, got %v", err)
	}
	if items, found := node2.Store.Get("foo"); found {
		t.Fatalf("expected nothing replicated to node-2, got %v", items)
	}
}

// TestCoordinatorPutFailsWithoutReplicatingWhenBuildItemFails: a
// non-replica coordinator that can't read the versions BuildItem builds
// on must also fail before fan-out.
func TestCoordinatorPutFailsWithoutReplicatingWhenBuildItemFails(t *testing.T) {
	addrs := map[string]string{
		"node-1": "localhost:60341",
		"node-2": "localhost:60342",
		"node-3": "localhost:60343",
	}
	nodes, _ := startTestCluster(t, addrs, map[string]quorumOverride{
		"node-1": {n: 2, w: 2, r: 2},
	})

	var key string
	for i := 0; i < 10000; i++ {
		candidate := fmt.Sprintf("probe-key-%d", i)
		list := nodes["node-1"].Ring.GetPreferenceList(candidate, 2)
		if len(list) == 2 && list[0] != "node-1" && list[1] != "node-1" {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Fatal("could not find a probe key whose N=2 preference list excludes node-1")
	}
	nodes["node-1"].Store = store.NewDataStoreWithPersister("node-1", failingPersister{})

	err := nodes["node-1"].Put(context.Background(), key, "v", nil)
	if err == nil || !strings.Contains(err.Error(), "local write failed") {
		t.Fatalf("expected the coordinator's BuildItem failure to fail Put, got %v", err)
	}
	for _, replicaID := range nodes["node-1"].Ring.GetPreferenceList(key, 2) {
		if items, found := nodes[replicaID].Store.Get(key); found {
			t.Fatalf("expected nothing replicated to %s, got %v", replicaID, items)
		}
	}
}
