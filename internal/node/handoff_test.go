package node

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/vectorclock"
)

// fastHandoffRetries shortens the retry backoff for a test.
func fastHandoffRetries(t *testing.T) {
	t.Helper()
	oldBase, oldMax := handoffRetryBase, handoffRetryMax
	handoffRetryBase, handoffRetryMax = 10*time.Millisecond, 50*time.Millisecond
	t.Cleanup(func() { handoffRetryBase, handoffRetryMax = oldBase, oldMax })
}

// handoffHook installs testHookHandoffKey for one test.
func handoffHook(t *testing.T, hook func(ctx context.Context, nodeID string, epoch uint64, key string)) {
	t.Helper()
	testHookHandoffKey = hook
	t.Cleanup(func() { testHookHandoffKey = nil })
}

// newHandoffNode is a node (see newTestNode) with quick reconnects and a bounded
// push timeout. The heartbeat and anti-entropy loops are not started: handoff
// is the only thing that moves data.
func newHandoffNode(t *testing.T, id, addr string, n, w int, neighbors map[string]string) *Node {
	nd := newTestNode(t, id, addr, n, w, 1, neighbors)
	nd.QuorumConfig.MaxReconnectBackoff = 50 * time.Millisecond
	nd.QuorumConfig.ReplicationTimeout = 2 * time.Second
	return nd
}

// canonSet renders a sibling set order-independently, values and clocks
// included, so two nodes' copies can be compared exactly.
func canonSet(items []*model.DataItem) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, fmt.Sprintf("%v|%v|%t|%s", it.Value, it.VectorClock.Snapshot(), it.IsDeleted, it.LastUpdatedBy))
	}
	sort.Strings(out)
	return out
}

func heldBy(t *testing.T, nd *Node, key string) []string {
	t.Helper()
	items, _, err := nd.Store.Get(key)
	if err != nil {
		t.Fatalf("%s: Get(%q): %v", nd.ID, key, err)
	}
	return canonSet(items)
}

func waitHandoff(t *testing.T, nd *Node, epoch uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := nd.WaitHandoff(ctx, epoch); err != nil {
		t.Fatalf("%s: handoff to epoch %d did not complete: %v (status %+v)", nd.ID, epoch, err, nd.HandoffStatus())
	}
}

func owners(nd *Node, key string, n int) []string {
	return nd.membership.Load().ring.GetPreferenceList(key, n)
}

// putSiblings leaves key on its three owners as two concurrent versions.
func putSiblings(nodes map[string]*Node, ring *Node, key string) {
	prefs := owners(ring, key, 3)
	a := nodes[prefs[0]].Store.Put(key, "a-"+key, nil)
	b := nodes[prefs[1]].Store.Put(key, "b-"+key, nil)
	for _, id := range prefs {
		nodes[id].Store.MergeReplicated(key, a)
		nodes[id].Store.MergeReplicated(key, b)
	}
}

// (a), (g) Growing the cluster from four nodes to five: every owner in the
// new view ends up with exactly the sibling set the old owners had, and the
// old owners keep theirs.
func TestScaleUpMovesEveryKeyToItsNewOwners(t *testing.T) {
	fastHandoffRetries(t)
	ids := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}
	addrs := map[string]string{}
	for _, id := range ids {
		addrs[id] = reserveAddr(t)
	}
	nodes := map[string]*Node{}
	for _, id := range ids[:4] {
		nodes[id] = newHandoffNode(t, id, addrs[id], 3, 3, neighborsOf(membersOf(addrs, ids[:4]...), id))
	}
	nodes["node-5"] = newHandoffNode(t, "node-5", addrs["node-5"], 3, 3, nil) // outside the cluster so far
	for _, id := range ids {
		serveNode(t, nodes[id], addrs[id])
	}

	ctx := context.Background()
	const numKeys = 300
	expected := map[string][]string{}
	for i := range numKeys {
		key := fmt.Sprintf("key-%03d", i)
		if i%10 == 0 {
			putSiblings(nodes, nodes["node-1"], key)
		} else if err := nodes["node-1"].Put(ctx, key, "v-"+key, nil); err != nil {
			t.Fatalf("Put %s: %v", key, err)
		}
		expected[key] = heldBy(t, nodes[owners(nodes["node-1"], key, 3)[0]], key)
		if i%10 == 0 && len(expected[key]) != 2 {
			t.Fatalf("setup: %s should have two siblings, has %v", key, expected[key])
		}
	}
	oldView := nodes["node-1"].membership.Load()

	all := membersOf(addrs, ids...)
	for _, id := range ids {
		if _, err := nodes[id].SetMembership(1, all); err != nil {
			t.Fatalf("%s SetMembership: %v", id, err)
		}
	}
	for _, id := range ids {
		waitHandoff(t, nodes[id], 1)
		if st := nodes[id].HandoffStatus(); !st.Done || st.Pending != 0 || st.Epoch != 1 {
			t.Fatalf("%s: status after WaitHandoff: %+v", id, st)
		}
	}

	movedToNode5 := 0
	for key, want := range expected {
		for _, id := range owners(nodes["node-1"], key, 3) {
			if got := heldBy(t, nodes[id], key); !reflect.DeepEqual(got, want) {
				t.Fatalf("new owner %s of %s holds %v, want %v", id, key, got, want)
			}
			if id == "node-5" {
				movedToNode5++
			}
		}
		for _, id := range oldView.ring.GetPreferenceList(key, 3) { // nothing is deleted
			if got := heldBy(t, nodes[id], key); !reflect.DeepEqual(got, want) {
				t.Fatalf("old owner %s of %s now holds %v, want it to keep %v", id, key, got, want)
			}
		}
	}
	if movedToNode5 == 0 {
		t.Fatal("no key moved to node-5: the test exercised nothing")
	}
}

// (b) A node leaving the cluster may hold the only copy of a key, so it
// pushes to every new owner, even those that were owners before.
func TestLeavingNodePushesToEveryNewOwner(t *testing.T) {
	fastHandoffRetries(t)
	addrs := reserveAddrs(t, "node-1", "node-2", "node-3")
	nodes := map[string]*Node{}
	for id := range addrs {
		nodes[id] = newHandoffNode(t, id, addrs[id], 2, 2, neighborsOf(addrs, id))
		serveNode(t, nodes[id], addrs[id])
	}
	x := nodes["node-3"]

	// A key that node-1 and node-2 own already, that only node-3 holds.
	var key string
	for i := 0; ; i++ {
		key = fmt.Sprintf("only-on-x-%d", i)
		if p := owners(x, key, 2); (p[0] == "node-1" && p[1] == "node-2") || (p[0] == "node-2" && p[1] == "node-1") {
			break
		}
	}
	item := &model.DataItem{Value: "sole-copy", VectorClock: vectorclock.FromSnapshot(map[string]uint32{"external": 1}), LastUpdatedBy: "external"}
	if err := x.Store.MergeReplicated(key, item); err != nil {
		t.Fatal(err)
	}
	want := heldBy(t, x, key)

	without := membersOf(addrs, "node-1", "node-2")
	for _, nd := range nodes {
		if _, err := nd.SetMembership(1, without); err != nil {
			t.Fatalf("%s SetMembership: %v", nd.ID, err)
		}
	}
	waitHandoff(t, x, 1)

	for _, id := range []string{"node-1", "node-2", "node-3"} {
		if got := heldBy(t, nodes[id], key); !reflect.DeepEqual(got, want) {
			t.Errorf("%s holds %v, want %v", id, got, want)
		}
	}
}

// (c) A newer view cancels the running handoff but not the base: a node that
// stays an owner still pushes to the owner that joined in the cancelled view.
// N=2 and the keys exist only on node-1, so nobody else can supply them.
func TestNewerViewKeepsTheBaseOfTheCancelledHandoff(t *testing.T) {
	fastHandoffRetries(t)
	addrs := reserveAddrs(t, "node-1", "node-2", "node-3")
	nodes := map[string]*Node{
		"node-1": newHandoffNode(t, "node-1", addrs["node-1"], 2, 1, map[string]string{"node-2": addrs["node-2"]}),
		"node-2": newHandoffNode(t, "node-2", addrs["node-2"], 2, 1, map[string]string{"node-1": addrs["node-1"]}),
		"node-3": newHandoffNode(t, "node-3", addrs["node-3"], 2, 1, nil),
	}
	for id, nd := range nodes {
		serveNode(t, nd, addrs[id])
	}

	// In the first view, {node-1, node-2}, node-1 owns every key.
	expected := map[string][]string{}
	for i := range 60 {
		key := fmt.Sprintf("key-%03d", i)
		nodes["node-1"].Store.Put(key, "v-"+key, nil)
		expected[key] = heldBy(t, nodes["node-1"], key)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	handoffHook(t, func(ctx context.Context, nodeID string, epoch uint64, key string) {
		if nodeID == "node-1" && epoch == 1 {
			once.Do(func() { close(entered) })
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	})

	second := membersOf(addrs, "node-1", "node-2", "node-3")
	for _, id := range []string{"node-2", "node-3", "node-1"} {
		if _, err := nodes[id].SetMembership(1, second); err != nil {
			t.Fatal(err)
		}
	}
	<-entered // node-1's handoff to the second view is paused before its first key

	// The third view: same ring, next epoch. It supersedes the paused handoff.
	if _, err := nodes["node-1"].SetMembership(2, second); err != nil {
		t.Fatal(err)
	}
	close(release)
	waitHandoff(t, nodes["node-1"], 2)

	moved := 0
	for key, want := range expected {
		if !slices.Contains(owners(nodes["node-1"], key, 2), "node-3") {
			continue
		}
		moved++
		if got := heldBy(t, nodes["node-3"], key); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s should have reached its new owner node-3, which holds %v", key, got)
		}
	}
	if moved == 0 {
		t.Fatal("no key has node-3 as an owner: the test exercised nothing")
	}
	t.Logf("%d keys reached node-3", moved)
}

// openHandoffNode opens node-1 persistent, alone in its configuration.
func openHandoffNode(t *testing.T, dir, addr string) *Node {
	t.Helper()
	nd, err := New("node-1", addr, 1, 1, 1, nil, dir, 1<<20)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	nd.QuorumConfig.MaxReconnectBackoff = 50 * time.Millisecond
	nd.QuorumConfig.ReplicationTimeout = 2 * time.Second
	return nd
}

// (d) A handoff cut short by a restart resumes from what HANDOFF says, and a
// completed one is not repeated.
func TestHandoffResumesAfterRestartAndIsRecordedWhenDone(t *testing.T) {
	fastHandoffRetries(t)
	addrs := reserveAddrs(t, "node-1", "node-2")
	dir := t.TempDir()
	node2 := newHandoffNode(t, "node-2", addrs["node-2"], 1, 1, nil)
	serveNode(t, node2, addrs["node-2"])
	both := membersOf(addrs, "node-1", "node-2")

	// Life 1: forty keys, all on node-1 (its only view is {node-1}); the
	// handoff to {node-1, node-2} is interrupted before it moves anything.
	nd := openHandoffNode(t, dir, addrs["node-1"])
	expected := map[string][]string{}
	for i := range 40 {
		key := fmt.Sprintf("key-%03d", i)
		nd.Store.Put(key, "v-"+key, nil)
		expected[key] = heldBy(t, nd, key)
	}
	entered := make(chan struct{})
	var once sync.Once
	handoffHook(t, func(ctx context.Context, _ string, _ uint64, _ string) {
		once.Do(func() { close(entered) })
		<-ctx.Done()
	})
	if _, err := nd.SetMembership(1, both); err != nil {
		t.Fatal(err)
	}
	<-entered
	if err := nd.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, handoffFileName)); !os.IsNotExist(err) {
		t.Fatalf("an interrupted handoff left a HANDOFF file (stat err %v)", err)
	}

	// Life 2: MEMBERSHIP says epoch 1, nothing says it was handed off.
	var keysHandled atomic.Int32
	handoffHook(t, func(context.Context, string, uint64, string) { keysHandled.Add(1) })
	nd = openHandoffNode(t, dir, addrs["node-1"])
	if epoch, _ := nd.Membership(); epoch != 1 {
		t.Fatalf("reopened at epoch %d, want the persisted 1", epoch)
	}
	if nd.HandoffStatus().Done {
		t.Fatal("a handoff that never completed is reported done after a restart")
	}
	nd.ResumeHandoff()
	waitHandoff(t, nd, 1)
	moved := 0
	for key, want := range expected {
		if owners(nd, key, 1)[0] == "node-2" {
			moved++
			if got := heldBy(t, node2, key); !reflect.DeepEqual(got, want) {
				t.Fatalf("%s on node-2: %v, want %v", key, got, want)
			}
		}
	}
	if moved == 0 || int(keysHandled.Load()) != moved {
		t.Fatalf("moved %d keys, hook saw %d", moved, keysHandled.Load())
	}
	if _, err := os.Stat(filepath.Join(dir, handoffFileName)); err != nil {
		t.Fatalf("a completed handoff should be recorded in HANDOFF: %v", err)
	}
	if err := nd.Close(); err != nil {
		t.Fatal(err)
	}

	// Life 3: HANDOFF says epoch 1 is done; nothing is pushed again.
	keysHandled.Store(0)
	nd = openHandoffNode(t, dir, addrs["node-1"])
	defer nd.Close()
	if st := nd.HandoffStatus(); !st.Done || st.Epoch != 1 {
		t.Fatalf("after a completed handoff, status is %+v", st)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := nd.WaitHandoff(ctx, 1); err != nil {
		t.Fatalf("WaitHandoff on a finished handoff: %v", err)
	}
	nd.ResumeHandoff()
	time.Sleep(100 * time.Millisecond)
	if got := keysHandled.Load(); got != 0 {
		t.Fatalf("a completed handoff was repeated: %d keys handled", got)
	}
}

// (e) A node that stays in the view hints the pushes its dead target can't
// take, finishes, and delivers them when the target is back.
func TestUnreachableTargetBecomesAHintForANodeThatStays(t *testing.T) {
	fastHandoffRetries(t)
	addrs := reserveAddrs(t, "node-1")
	maps.Copy(addrs, knownAddrs("node-2")) // down, then started later
	nd := openHandoffNode(t, t.TempDir(), addrs["node-1"])
	defer nd.Close()
	expected := map[string][]string{}
	for i := range 40 {
		key := fmt.Sprintf("key-%03d", i)
		nd.Store.Put(key, "v-"+key, nil)
		expected[key] = heldBy(t, nd, key)
	}

	if _, err := nd.SetMembership(1, membersOf(addrs, "node-1", "node-2")); err != nil { // node-2 isn't listening
		t.Fatal(err)
	}
	waitHandoff(t, nd, 1)
	st := nd.HandoffStatus()
	if st.Hinted == 0 || st.Pushed != 0 {
		t.Fatalf("expected only hints for the unreachable owner, got %+v", st)
	}

	node2 := newHandoffNode(t, "node-2", addrs["node-2"], 1, 1, nil)
	serveNode(t, node2, addrs["node-2"])
	eventually(t, 10*time.Second, "the hints to reach node-2", func() bool {
		nd.deliverHints(context.Background())
		for key, want := range expected {
			if owners(nd, key, 1)[0] == "node-2" && !reflect.DeepEqual(heldBy(t, node2, key), want) {
				return false
			}
		}
		return true
	})
}

// (f) A leaving node keeps retrying an unreachable target: its handoff is not
// done until the target has the data.
func TestLeavingNodeWaitsForAnUnreachableTarget(t *testing.T) {
	fastHandoffRetries(t)
	addrs := reserveAddrs(t, "node-1")
	maps.Copy(addrs, knownAddrs("node-2")) // down, then started later
	nd := newHandoffNode(t, "node-1", addrs["node-1"], 1, 1, map[string]string{"node-2": addrs["node-2"]})

	expected := map[string][]string{}
	for i := 0; len(expected) < 30; i++ {
		key := fmt.Sprintf("key-%03d", i)
		if owners(nd, key, 1)[0] == "node-1" {
			nd.Store.Put(key, "v-"+key, nil)
			expected[key] = heldBy(t, nd, key)
		}
	}

	if _, err := nd.SetMembership(1, membersOf(addrs, "node-2")); err != nil { // node-1 leaves; node-2 is down
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if err := nd.WaitHandoff(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the handoff finished (or failed oddly) with its target down: %v", err)
	}
	if st := nd.HandoffStatus(); st.Done || st.Pending == 0 {
		t.Fatalf("status with the target down: %+v", st)
	}

	node2 := newHandoffNode(t, "node-2", addrs["node-2"], 1, 1, nil)
	serveNode(t, node2, addrs["node-2"])
	waitHandoff(t, nd, 1)
	for key, want := range expected {
		if got := heldBy(t, node2, key); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s on node-2: %v, want %v", key, got, want)
		}
	}
}

// (h) Membership changes while traffic continues. Writes coordinated while
// nodes disagree about the membership may fail (a node can still be routing
// by the old view); what must hold is that every write that was acknowledged
// is, once every handoff has completed, on every one of its owners in the
// final view. node-4 starts the way docs/membership.md says a new node does,
// with a view listing every member.
//
// Each write has its own key, so no acknowledged value is legitimately
// superseded by a later one. (A write coordinated with the old view that
// lands on an old owner after that owner listed its keys for the handoff is
// left to anti-entropy, which doesn't run here; see known-limitations.md.
// The coordinator makes its own copy before it adopts the new view and lists
// its keys, so that would take a write landing within that instant.)
func TestHandoffRacesWithTrafficAndFurtherViewChanges(t *testing.T) {
	fastHandoffRetries(t)
	ids := []string{"node-1", "node-2", "node-3", "node-4"}
	addrs := reserveAddrs(t, ids...)
	nodes := map[string]*Node{}
	for _, id := range ids[:3] {
		nodes[id] = newHandoffNode(t, id, addrs[id], 3, 2, neighborsOf(membersOf(addrs, ids[:3]...), id))
	}
	nodes["node-4"] = newHandoffNode(t, "node-4", addrs["node-4"], 3, 2, neighborsOf(addrs, "node-4"))
	for _, id := range ids {
		serveNode(t, nodes[id], addrs[id])
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var mu sync.Mutex
	acked := map[string]string{} // key -> value of every acknowledged write
	for i := range 100 {
		key, value := fmt.Sprintf("pre-%03d", i), fmt.Sprintf("v-pre-%03d", i)
		if err := nodes["node-1"].Put(ctx, key, value, nil); err != nil {
			t.Fatal(err)
		}
		acked[key] = value
	}

	var stop atomic.Bool
	var failedWrites, failedReads atomic.Int32
	var traffic sync.WaitGroup
	for _, id := range []string{"node-1", "node-2"} {
		traffic.Add(1)
		go func() {
			defer traffic.Done()
			for i := 0; !stop.Load(); i++ {
				key := fmt.Sprintf("w-%s-%05d", id, i)
				if err := nodes[id].Put(ctx, key, "v-"+key, nil); err != nil {
					failedWrites.Add(1)
					continue
				}
				mu.Lock()
				acked[key] = "v-" + key
				mu.Unlock()
				if _, err := nodes[id].Get(ctx, key); err != nil {
					failedReads.Add(1)
				}
			}
		}()
	}

	all := membersOf(addrs, ids...)
	for epoch := uint64(1); epoch <= 3; epoch++ {
		for _, id := range ids {
			if _, err := nodes[id].SetMembership(epoch, all); err != nil {
				t.Errorf("%s SetMembership(%d): %v", id, epoch, err)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop.Store(true)
	traffic.Wait()
	for _, id := range ids {
		waitHandoff(t, nodes[id], 3)
	}
	t.Logf("%d writes acknowledged, %d failed while the views differed; %d reads failed",
		len(acked)-100, failedWrites.Load(), failedReads.Load())

	missing := 0
	for key, value := range acked {
		for _, owner := range owners(nodes["node-1"], key, 3) {
			items, _, err := nodes[owner].Store.Get(key)
			if err != nil {
				t.Fatalf("%s: Get(%q): %v", owner, key, err)
			}
			if !slices.ContainsFunc(items, func(it *model.DataItem) bool { return it.Value == value }) {
				if missing < 5 {
					t.Errorf("acknowledged write %s=%s is missing from its owner %s (holds %v)", key, value, owner, itemValues(items))
				}
				missing++
			}
		}
	}
	if missing > 0 {
		t.Fatalf("%d acknowledged writes missing from owners in the final view", missing)
	}
}
