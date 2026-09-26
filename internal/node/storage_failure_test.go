package node

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"distributed-kv-datastore/internal/merkle"
	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/store"
	"distributed-kv-datastore/internal/versioning"
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

// flakyPersister is a working in-memory Persister whose operations can be
// made to fail one at a time — for failures that hit one path (say, a
// read) while another (the write after it) still succeeds.
type flakyPersister struct {
	mu     sync.Mutex
	data   map[string][]*model.DataItem // oldest first
	faults faults
}

// faults says which flakyPersister operations fail, and with what; a nil
// field lets that operation work.
type faults struct {
	get, keys, restore, put error
}

func newFlakyPersister() *flakyPersister {
	return &flakyPersister{data: make(map[string][]*model.DataItem)}
}

// fail replaces which operations fail from now on.
func (f *flakyPersister) fail(fs faults) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.faults = fs
}

func (f *flakyPersister) Put(key string, item *model.DataItem) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.faults.put != nil {
		return f.faults.put
	}
	f.data[key] = versioning.Resolve(f.data[key], item)
	return nil
}

func (f *flakyPersister) GetAll(key string) ([]*model.DataItem, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.faults.get != nil {
		return nil, false, f.faults.get
	}
	items := f.data[key]
	out := make([]*model.DataItem, len(items))
	for i, item := range items {
		out[len(items)-1-i] = item // newest first
	}
	return out, len(items) > 0, nil
}

func (f *flakyPersister) Restore(key string, items []*model.DataItem) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.faults.restore != nil {
		return f.faults.restore
	}
	if len(items) == 0 {
		delete(f.data, key)
		return nil
	}
	stored := make([]*model.DataItem, len(items))
	for i, item := range items {
		stored[len(items)-1-i] = item // back to oldest first
	}
	f.data[key] = stored
	return nil
}

func (f *flakyPersister) Keys() ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.faults.keys != nil {
		return nil, f.faults.keys
	}
	keys := make([]string, 0, len(f.data))
	for k := range f.data {
		keys = append(keys, k)
	}
	return keys, nil
}

func itemValues(items []*model.DataItem) []any {
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, item.Value)
	}
	return out
}

// TestPutFailsWithoutReplicatingWhenLocalWriteFails: a replica whose local
// store can't persist must return an error and send peers nothing — not a
// nil item, not anything. Reads still work, so Put gets past its
// prevVersions read to the write itself.
func TestPutFailsWithoutReplicatingWhenLocalWriteFails(t *testing.T) {
	node1 := startTestNode(t, "node-1", "localhost:60331", map[string]string{"node-2": "localhost:60332"})
	node2 := startTestNode(t, "node-2", "localhost:60332", map[string]string{"node-1": "localhost:60331"})
	// N=2 of 2 nodes: node-1 is a replica for every key, so Put writes locally.
	p := newFlakyPersister()
	p.fail(faults{put: errDisk})
	node1.Store = store.NewDataStoreWithPersister("node-1", p)

	err := node1.Put(context.Background(), "foo", "bar", nil)
	if err == nil || !strings.Contains(err.Error(), "local write failed") {
		t.Fatalf("expected a local write failure, got %v", err)
	}
	if items, found, _ := node2.Store.Get("foo"); found {
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
		if items, found, _ := nodes[replicaID].Store.Get(key); found {
			t.Fatalf("expected nothing replicated to %s, got %v", replicaID, items)
		}
	}
}

// TestGetCountsLocalReadFailureAsFailedVote: with R=1, a replica whose
// local read fails must not answer "not found" on its own — the failed
// read isn't a vote, so it has to get its one response from a peer.
func TestGetCountsLocalReadFailureAsFailedVote(t *testing.T) {
	node1 := startTestNode(t, "node-1", "localhost:60351", map[string]string{"node-2": "localhost:60352"})
	node2 := startTestNode(t, "node-2", "localhost:60352", map[string]string{"node-1": "localhost:60351"})
	// N=2, R=1: node-1 is a replica for every key.
	p := newFlakyPersister()
	p.fail(faults{get: errDisk})
	node1.Store = store.NewDataStoreWithPersister("node-1", p)
	node2.Store.Put("foo", "bar", nil)

	items, err := node1.Get(context.Background(), "foo")
	if err != nil {
		t.Fatalf("expected node-2's response to satisfy R=1, got error %v", err)
	}
	if got := itemValues(items); !reflect.DeepEqual(got, []any{"bar"}) {
		t.Fatalf("expected [bar] from node-2, got %v (a failed local read was counted as a not-found vote)", got)
	}
}

// TestGetQuorumFailsWhenLocalReadFailsAndRNeedsIt: N=2, R=2. With the
// local read failed, only one response is possible, so the read must fail
// rather than count the failed read toward R.
func TestGetQuorumFailsWhenLocalReadFailsAndRNeedsIt(t *testing.T) {
	addrs := map[string]string{
		"node-1": "localhost:60361",
		"node-2": "localhost:60362",
	}
	nodes, _ := startTestCluster(t, addrs, map[string]quorumOverride{
		"node-1": {n: 2, w: 2, r: 2},
	})
	p := newFlakyPersister()
	p.fail(faults{get: errDisk})
	nodes["node-1"].Store = store.NewDataStoreWithPersister("node-1", p)
	nodes["node-2"].Store.Put("foo", "bar", nil)

	items, err := nodes["node-1"].Get(context.Background(), "foo")
	if err == nil || !strings.Contains(err.Error(), "read quorum not reached") {
		t.Fatalf("expected a read quorum failure, got items=%v err=%v", itemValues(items), err)
	}
}

// deadPeerNode starts node-1 (N=2, W=2) whose only peer, node-2, has no
// listener: every replication fails, so every Put misses quorum and rolls
// back. Its store is persister-backed by p.
func deadPeerNode(t *testing.T, p *flakyPersister, port int) *Node {
	t.Helper()
	n := startTestNode(t, "node-1", fmt.Sprintf("localhost:%d", port),
		map[string]string{"node-2": fmt.Sprintf("localhost:%d", port+1)})
	n.Store = store.NewDataStoreWithPersister("node-1", p)
	return n
}

// TestPutDoesNotDeleteCommittedDataWhenPrevVersionsReadFails: Put reads
// prevVersions to roll back to. If that read fails but the write after it
// succeeds (an explicit context needs no read), a missed quorum used to
// roll back to "no prior versions" — deleting the committed value.
func TestPutDoesNotDeleteCommittedDataWhenPrevVersionsReadFails(t *testing.T) {
	p := newFlakyPersister()
	node1 := deadPeerNode(t, p, 60371)
	committed := node1.Store.Put("foo", "committed", nil)

	p.fail(faults{get: errDisk})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := node1.Put(ctx, "foo", "new", committed.VectorClock.Snapshot())
	if err == nil || !strings.Contains(err.Error(), "local read failed") {
		t.Errorf("expected Put to fail on the local read, got %v", err)
	}

	p.fail(faults{})
	items, _, err := node1.Store.Get("foo")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got := itemValues(items); !reflect.DeepEqual(got, []any{"committed"}) {
		t.Fatalf("expected the committed value to survive, got %v", got)
	}
}

// TestPutReportsFailedRollback: when quorum fails and the rollback itself
// fails, the error must say so — the failed write may still be visible.
func TestPutReportsFailedRollback(t *testing.T) {
	p := newFlakyPersister()
	node1 := deadPeerNode(t, p, 60381)
	p.fail(faults{restore: errDisk})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := node1.Put(ctx, "foo", "v", nil)
	if err == nil || !strings.Contains(err.Error(), "write quorum not reached") {
		t.Fatalf("expected a write quorum failure, got %v", err)
	}
	if !strings.Contains(err.Error(), "rollback") || !errors.Is(err, errDisk) {
		t.Fatalf("expected the error to report the failed rollback, got %v", err)
	}
}

// reconcileSetup starts node-1 (persister-backed by p) and node-2
// (in-memory), full replication between them.
func reconcileSetup(t *testing.T, p *flakyPersister, port int) (*Node, *Node) {
	t.Helper()
	a1, a2 := fmt.Sprintf("localhost:%d", port), fmt.Sprintf("localhost:%d", port+1)
	node1 := startTestNode(t, "node-1", a1, map[string]string{"node-2": a2})
	node2 := startTestNode(t, "node-2", a2, map[string]string{"node-1": a1})
	node1.Store = store.NewDataStoreWithPersister("node-1", p)
	return node1, node2
}

// reconcileKey runs node-1's reconcileBucket against node-2 for key's bucket.
func reconcileKey(t *testing.T, node1 *Node, key string) error {
	t.Helper()
	client, err := node1.getOrDialClient("node-2")
	if err != nil {
		t.Fatalf("dial node-2: %v", err)
	}
	return node1.reconcileBucket(context.Background(), "node-2", client, merkle.BucketFor(key, antiEntropyNumBuckets))
}

// TestReconcileBucketAbortsWhenLocalReadFails: a failed local read must
// abort the bucket, not be taken as "node-1 holds nothing" — which would
// overwrite node-1's own concurrent version with node-2's.
func TestReconcileBucketAbortsWhenLocalReadFails(t *testing.T) {
	p := newFlakyPersister()
	node1, node2 := reconcileSetup(t, p, 60391)
	node1.Store.Put("k", "from-node-1", nil)
	node2.Store.Put("k", "from-node-2", nil) // concurrent with node-1's

	p.fail(faults{get: errDisk})
	if err := reconcileKey(t, node1, "k"); err == nil || !errors.Is(err, errDisk) {
		t.Fatalf("expected reconcileBucket to fail with the read error, got %v", err)
	}

	p.fail(faults{})
	items, _, _ := node1.Store.Get("k")
	if got := itemValues(items); !reflect.DeepEqual(got, []any{"from-node-1"}) {
		t.Fatalf("expected node-1's own version untouched, got %v", got)
	}
}

// TestReconcileBucketAbortsWhenLocalKeysFail: a failed key listing must
// abort the bucket, not reconcile as if node-1 held no keys in it.
func TestReconcileBucketAbortsWhenLocalKeysFail(t *testing.T) {
	p := newFlakyPersister()
	node1, _ := reconcileSetup(t, p, 60401)
	node1.Store.Put("k", "from-node-1", nil)

	p.fail(faults{keys: errDisk})
	if err := reconcileKey(t, node1, "k"); err == nil || !errors.Is(err, errDisk) {
		t.Fatalf("expected reconcileBucket to fail with the keys error, got %v", err)
	}
}

// TestReconcileBucketAbortsWhenLocalInstallFails: if installing the merged
// versions locally fails, reconcileBucket must report it, not return
// success with node-1 still unreconciled.
func TestReconcileBucketAbortsWhenLocalInstallFails(t *testing.T) {
	p := newFlakyPersister()
	node1, node2 := reconcileSetup(t, p, 60411)
	node2.Store.Put("k", "from-node-2", nil) // node-1 has nothing: merged must be installed locally

	p.fail(faults{restore: errDisk})
	if err := reconcileKey(t, node1, "k"); err == nil || !errors.Is(err, errDisk) {
		t.Fatalf("expected reconcileBucket to fail with the restore error, got %v", err)
	}
}

// TestRunAntiEntropyFailsWhenLocalTreeCannotBeBuilt: a local tree built
// over a store that failed to read would describe it as empty.
func TestRunAntiEntropyFailsWhenLocalTreeCannotBeBuilt(t *testing.T) {
	node1, _ := reconcileSetup(t, newFlakyPersister(), 60421)
	node1.Store = store.NewDataStoreWithPersister("node-1", failingPersister{})

	err := node1.RunAntiEntropy(context.Background(), "node-2")
	if err == nil || !errors.Is(err, errDisk) {
		t.Fatalf("expected RunAntiEntropy to fail building the local tree, got %v", err)
	}
}
