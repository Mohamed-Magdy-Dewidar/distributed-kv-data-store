package hashring

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The pair below collides at four vnode positions under the real 150-vnode
// hashKey (see TestCollidingVnodesResolveDeterministicallyRegardlessOfInsertionOrder).
const (
	collideLow  = "node-21"   // lexicographically lower: wins while both are members
	collideHigh = "node-2622" // takes the slots once collideLow is gone
	testVnodes  = 150
)

func mustBuild(t *testing.T, ids []string) *HashRing {
	t.Helper()
	hr, err := NewHashRingFromMembers(testVnodes, ids)
	if err != nil {
		t.Fatalf("NewHashRingFromMembers(%v): %v", ids, err)
	}
	return hr
}

// collidingPositions returns every position both a and b's vnodes hash to.
func collidingPositions(a, b string) []uint32 {
	inA := make(map[uint32]bool)
	for i := range testVnodes {
		inA[hashKey(a+"-"+strconv.Itoa(i))] = true
	}
	var out []uint32
	for i := range testVnodes {
		if pos := hashKey(b + "-" + strconv.Itoa(i)); inA[pos] {
			out = append(out, pos)
		}
	}
	return out
}

func permutations(ids []string) [][]string {
	if len(ids) <= 1 {
		return [][]string{append([]string(nil), ids...)}
	}
	var out [][]string
	for i := range ids {
		rest := make([]string, 0, len(ids)-1)
		rest = append(rest, ids[:i]...)
		rest = append(rest, ids[i+1:]...)
		for _, p := range permutations(rest) {
			out = append(out, append([]string{ids[i]}, p...))
		}
	}
	return out
}

// (a) The ring is a pure function of the member set.
func TestFromMembersIsIndependentOfIDOrder(t *testing.T) {
	members := []string{collideLow, collideHigh, "node-3", "node-7", "node-9"}
	perms := permutations(members) // all 120 orderings
	base := mustBuild(t, perms[0])

	for _, order := range perms[1:] {
		hr := mustBuild(t, order)
		if !reflect.DeepEqual(base.nodeMap, hr.nodeMap) {
			t.Fatalf("nodeMap differs for order %v", order)
		}
		if !reflect.DeepEqual(base.ring, hr.ring) {
			t.Fatalf("ring differs for order %v", order)
		}
		for i := range 100 {
			key := fmt.Sprintf("sample-key-%d", i)
			if a, b := base.GetPreferenceList(key, 3), hr.GetPreferenceList(key, 3); !reflect.DeepEqual(a, b) {
				t.Fatalf("key %q: preference list differs for order %v: %v vs %v", key, order, a, b)
			}
		}
	}
}

// (b) The one-call builder equals AddNode, whichever order AddNode is fed.
func TestFromMembersEqualsAddNodeInEitherOrder(t *testing.T) {
	members := []string{collideLow, collideHigh, "node-3"}
	want := mustBuild(t, members)

	for _, order := range [][]string{
		{collideLow, collideHigh, "node-3"},
		{"node-3", collideHigh, collideLow},
	} {
		hr := NewHashRing(testVnodes)
		for _, id := range order {
			hr.AddNode(id)
		}
		if !reflect.DeepEqual(want.nodeMap, hr.nodeMap) || !reflect.DeepEqual(want.ring, hr.ring) {
			t.Fatalf("AddNode in order %v built a different ring than NewHashRingFromMembers", order)
		}
	}
}

// (c) Dropping the slot winner from the member set hands its colliding slots
// to the loser, exactly as a fresh build without the winner would.
func TestFromMembersWithoutSlotWinnerHandsSlotsToLoser(t *testing.T) {
	collisions := collidingPositions(collideLow, collideHigh)
	if len(collisions) != 4 {
		t.Fatalf("expected the known 4 colliding positions, found %d: %v", len(collisions), collisions)
	}

	with := mustBuild(t, []string{collideLow, collideHigh, "node-x"})
	without := mustBuild(t, []string{collideHigh, "node-x"})

	for _, pos := range collisions {
		if owner := with.nodeMap[pos]; owner != collideLow {
			t.Errorf("with both members: position %d owned by %q, want %q", pos, owner, collideLow)
		}
		if owner := without.nodeMap[pos]; owner != collideHigh {
			t.Errorf("without %s: position %d owned by %q, want %q", collideLow, pos, owner, collideHigh)
		}
		if !hasPosition(without, pos) {
			t.Errorf("without %s: position %d is missing from the ring", collideLow, pos)
		}
	}

	// Every position of the smaller ring is owned by a member, none by the
	// departed node, and none of the loser's positions were lost.
	for pos, owner := range without.nodeMap {
		if owner == collideLow {
			t.Errorf("position %d still owned by absent %s", pos, collideLow)
		}
	}
	if got, want := len(without.ring), 2*testVnodes; got != want {
		t.Errorf("without %s: ring has %d positions, want %d", collideLow, got, want)
	}
}

func hasPosition(hr *HashRing, pos uint32) bool {
	return slices.Contains(hr.ring, pos)
}

// (d) Bad member lists are rejected, naming the offender.
func TestFromMembersRejectsEmptyAndDuplicateIDs(t *testing.T) {
	if _, err := NewHashRingFromMembers(testVnodes, []string{"node-1", "", "node-3"}); err == nil ||
		!strings.Contains(err.Error(), "empty") {
		t.Errorf("empty id: got err=%v, want an error saying the id is empty", err)
	}
	if _, err := NewHashRingFromMembers(testVnodes, []string{"node-1", "node-2", "node-1"}); err == nil ||
		!strings.Contains(err.Error(), `"node-1"`) {
		t.Errorf("duplicate id: got err=%v, want an error naming \"node-1\"", err)
	}
	if hr, err := NewHashRingFromMembers(testVnodes, nil); err != nil || len(hr.ring) != 0 {
		t.Errorf("no members: got ring=%v err=%v, want an empty ring and no error", hr, err)
	}
}

// (e) Positions are sorted, strictly increasing, and each has exactly one owner.
func TestFromMembersPositionsAreStrictlyIncreasingAndOwned(t *testing.T) {
	hr := mustBuild(t, []string{collideLow, collideHigh, "node-3", "node-7"})

	for i := 1; i < len(hr.ring); i++ {
		if hr.ring[i-1] >= hr.ring[i] {
			t.Fatalf("ring not strictly increasing at %d: %d then %d", i, hr.ring[i-1], hr.ring[i])
		}
	}
	if len(hr.ring) != len(hr.nodeMap) {
		t.Fatalf("ring has %d positions but nodeMap has %d", len(hr.ring), len(hr.nodeMap))
	}
	for _, pos := range hr.ring {
		if _, ok := hr.nodeMap[pos]; !ok {
			t.Fatalf("ring position %d has no owner in nodeMap", pos)
		}
	}
	if want := 4*testVnodes - 4; len(hr.ring) != want {
		t.Fatalf("ring has %d positions, want %d (600 minus the 4 collisions)", len(hr.ring), want)
	}
}
