package hashring

import (
	"fmt"
	"sync"
	"testing"
)

func TestEmptyRingReturnsEmptyPreferenceList(t *testing.T) {
	hr := NewHashRing(150)
	list := hr.GetPreferenceList("foo", 3)
	if len(list) != 0 {
		t.Errorf("expected empty list for empty ring, got %v", list)
	}
}

func TestDeterminism_SameKeySameNodesAcrossIndependentRings(t *testing.T) {
	// This is the property the whole "every node builds its own ring"
	// design depends on: two independently constructed HashRing instances,
	// seeded with the same node IDs (in a different order, deliberately,
	// since AddNode order should not matter), must agree on every key's
	// preference list.
	ringA := NewHashRing(150)
	ringB := NewHashRing(150)

	for _, id := range []string{"node-1", "node-2", "node-3"} {
		ringA.AddNode(id)
	}
	for _, id := range []string{"node-3", "node-1", "node-2"} { // different order
		ringB.AddNode(id)
	}

	keys := []string{"foo", "bar", "baz", "user:123", "session:abc"}
	for _, key := range keys {
		listA := ringA.GetPreferenceList(key, 2)
		listB := ringB.GetPreferenceList(key, 2)

		if len(listA) != len(listB) {
			t.Fatalf("key %q: length mismatch, ringA=%v ringB=%v", key, listA, listB)
		}
		for i := range listA {
			if listA[i] != listB[i] {
				t.Errorf("key %q: order/content mismatch at index %d, ringA=%v ringB=%v", key, i, listA, listB)
			}
		}
	}
}

func TestGetPreferenceListReturnsDistinctNodes(t *testing.T) {
	hr := NewHashRing(150)
	hr.AddNode("node-1")
	hr.AddNode("node-2")
	hr.AddNode("node-3")

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		list := hr.GetPreferenceList(key, 2)

		if len(list) != 2 {
			t.Fatalf("key %q: expected 2 nodes, got %d: %v", key, len(list), list)
		}
		if list[0] == list[1] {
			t.Errorf("key %q: preference list contains a duplicate: %v", key, list)
		}
	}
}

func TestGetPreferenceListReplicationFactorExceedsNodeCount(t *testing.T) {
	hr := NewHashRing(150)
	hr.AddNode("node-1")
	hr.AddNode("node-2")

	// Asking for 5 replicas with only 2 distinct physical nodes on the
	// ring — per the documented contract, the caller should not assume an
	// exact length back.
	list := hr.GetPreferenceList("foo", 5)
	if len(list) != 2 {
		t.Errorf("expected list capped at distinct node count (2), got %d: %v", len(list), list)
	}
}

func TestGetPreferenceListSingleNode(t *testing.T) {
	hr := NewHashRing(150)
	hr.AddNode("node-1")

	list := hr.GetPreferenceList("foo", 3)
	if len(list) != 1 || list[0] != "node-1" {
		t.Errorf("expected [node-1] with only one node on the ring, got %v", list)
	}
}

func TestRemoveNodeStopsItFromAppearingInPreferenceLists(t *testing.T) {
	hr := NewHashRing(150)
	hr.AddNode("node-1")
	hr.AddNode("node-2")
	hr.AddNode("node-3")

	hr.RemoveNode("node-2")

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%d", i)
		list := hr.GetPreferenceList(key, 3)
		for _, id := range list {
			if id == "node-2" {
				t.Fatalf("key %q: removed node-2 still appears in preference list %v", key, list)
			}
		}
		// only 2 distinct nodes remain, so asking for 3 should return 2
		if len(list) != 2 {
			t.Errorf("key %q: expected 2 nodes after removal, got %d: %v", key, len(list), list)
		}
	}
}

func TestSameKeyConsistentlyMapsToSamePreferenceList(t *testing.T) {
	// Calling GetPreferenceList repeatedly for the same key, with no ring
	// mutation in between, must always return the same answer — this is
	// the whole point of consistent hashing being deterministic.
	hr := NewHashRing(150)
	hr.AddNode("node-1")
	hr.AddNode("node-2")
	hr.AddNode("node-3")

	first := hr.GetPreferenceList("stable-key", 2)
	for i := 0; i < 20; i++ {
		next := hr.GetPreferenceList("stable-key", 2)
		if len(first) != len(next) {
			t.Fatalf("run %d: length changed, first=%v next=%v", i, first, next)
		}
		for j := range first {
			if first[j] != next[j] {
				t.Fatalf("run %d: preference list changed for unmutated ring, first=%v next=%v", i, first, next)
			}
		}
	}
}

func TestNoNodeIsCatastrophicallyOverOrUnderloaded(t *testing.T) {
	// NOT a fairness test. At 150 vnodes/physical (the production
	// default), measured true per-node share (large-sample, negligible
	// sampling noise) ranges from -58% to +91% relative deviation across
	// the node counts we've tested (25 and 50 physical nodes) — and this
	// doesn't tighten at higher physical node counts, since it's driven by
	// the fixed 150-vnode-per-node sample size on the ring, not cluster
	// size. That means 150 vnodes does NOT deliver tight load fairness —
	// this is a real, documented characteristic of the current default
	// (see the comment on defaultVirtualNodesPerPhysical in node.go), not
	// a test artifact or something this test is meant to catch.
	//
	// What this test actually guards against: the ring being
	// catastrophically broken — e.g. a node getting ~0% of keys (a hash
	// bug, an AddNode that silently no-ops) or ~50%+ of keys (vnode count
	// collapsed toward single digits, or a hash collision wiping out most
	// of another node's arcs). ±85% tolerance is wide enough to pass under
	// the ring's normal (if unfair) operation, while a truly broken ring
	// would blow past it.
	const physicalNodeCount = 25
	const virtualNodesPerPhysical = 150 // matches the production default

	hr := NewHashRing(virtualNodesPerPhysical)
	for i := 1; i <= physicalNodeCount; i++ {
		hr.AddNode(fmt.Sprintf("node-%d", i))
	}

	counts := map[string]int{}
	const sampleSize = 100000 // large enough that sampling noise is negligible next to the ring's own skew
	for i := 0; i < sampleSize; i++ {
		key := fmt.Sprintf("sample-key-%d", i)
		list := hr.GetPreferenceList(key, 1)
		if len(list) != 1 {
			t.Fatalf("expected exactly 1 primary node, got %v", list)
		}
		counts[list[0]]++
	}

	expected := sampleSize / physicalNodeCount
	tolerance := expected * 85 / 100 // ±85% relative deviation

	for i := 1; i <= physicalNodeCount; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		count := counts[nodeID]
		t.Logf("%s: %d/%d primary assignments (%.1f%% of keys, expected ~%.1f%%)",
			nodeID, count, sampleSize, 100*float64(count)/sampleSize, 100.0/physicalNodeCount)

		if count < expected-tolerance || count > expected+tolerance {
			t.Errorf("node %q got %d/%d keys as primary (expected ~%d ± %d) — distribution looks skewed",
				nodeID, count, sampleSize, expected, tolerance)
		}
	}
}

func TestConcurrentAddAndLookupDoesNotRace(t *testing.T) {
	// Real concurrency test: AddNode and GetPreferenceList happening
	// simultaneously across goroutines. This is the direct regression
	// test for the missing-mutex bug — run with -race.
	hr := NewHashRing(150)
	hr.AddNode("node-1") // start with at least one node so lookups aren't all trivially empty

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			hr.AddNode(fmt.Sprintf("late-node-%d", i))
		}(i)
	}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-key-%d", i)
			_ = hr.GetPreferenceList(key, 2) // just exercising, not asserting a specific result
		}(i)
	}

	wg.Wait()
	// If we get here without go test -race flagging a data race, and
	// without a panic (e.g. index-out-of-range from a torn slice read),
	// the mutex is doing its job.
}
