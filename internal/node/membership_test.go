package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// twoNode is a node-1 in a cluster of node-1 and node-2 with N=2.
func twoNode() (*Node, map[string]string) {
	members := map[string]string{"node-1": "h1:1", "node-2": "h2:1"}
	return New("node-1", "h1:1", 2, 1, 1, map[string]string{"node-2": "h2:1"}), members
}

func withMember(members map[string]string, id, addr string) map[string]string {
	out := maps.Clone(members)
	out[id] = addr
	return out
}

func assertMembership(t *testing.T, nd *Node, epoch uint64, members map[string]string) {
	t.Helper()
	gotEpoch, gotMembers := nd.Membership()
	if gotEpoch != epoch || !reflect.DeepEqual(gotMembers, members) {
		t.Fatalf("membership is epoch %d %v, want epoch %d %v", gotEpoch, gotMembers, epoch, members)
	}
}

// (a) One epoch means one membership.
func TestSetMembershipEpochRules(t *testing.T) {
	nd, base := twoNode()
	three := withMember(base, "node-3", "h3:1")

	if changed, err := nd.SetMembership(5, three); err != nil || !changed {
		t.Fatalf("adopting a higher epoch: changed=%v err=%v", changed, err)
	}
	assertMembership(t, nd, 5, three)
	held := nd.membership.Load()

	if changed, err := nd.SetMembership(5, three); err != nil || changed {
		t.Fatalf("same epoch, same members: changed=%v err=%v, want a no-op", changed, err)
	}
	if nd.membership.Load() != held {
		t.Fatal("a no-op replaced the view")
	}

	for name, tc := range map[string]struct {
		epoch   uint64
		members map[string]string
		want    error
	}{
		"same epoch, different address": {5, withMember(three, "node-3", "h3:2"), ErrMembershipConflict},
		"same epoch, different members": {5, withMember(three, "node-4", "h4:1"), ErrMembershipConflict},
		"lower epoch":                   {4, three, ErrStaleEpoch},
		"lower epoch, other members":    {0, base, ErrStaleEpoch},
	} {
		changed, err := nd.SetMembership(tc.epoch, tc.members)
		if !errors.Is(err, tc.want) || changed {
			t.Errorf("%s: changed=%v err=%v, want %v", name, changed, err, tc.want)
		}
		if nd.membership.Load() != held {
			t.Errorf("%s: the view changed", name)
		}
	}
	assertMembership(t, nd, 5, three)
}

// (b) A member set that breaks the rules changes nothing.
func TestSetMembershipValidation(t *testing.T) {
	nd, base := twoNode()
	if _, err := nd.SetMembership(3, base); err != nil {
		t.Fatal(err)
	}
	held := nd.membership.Load()

	for name, members := range map[string]map[string]string{
		"nil":                  nil,
		"empty":                {},
		"fewer members than N": {"node-1": "h1:1"},
		"duplicate address":    {"node-1": "h1:1", "node-2": "h1:1"},
		"empty id":             {"node-1": "h1:1", "": "h2:1"},
		"id containing '#'":    {"node-1": "h1:1", "node#2": "h2:1"},
		"id containing NUL":    {"node-1": "h1:1", "node\x002": "h2:1"},
		"empty address":        {"node-1": "h1:1", "node-2": ""},
	} {
		if changed, err := nd.SetMembership(9, members); err == nil || changed {
			t.Errorf("%s: changed=%v err=%v, want an error", name, changed, err)
		}
		if nd.membership.Load() != held {
			t.Errorf("%s: the view changed", name)
		}
	}
}

// This node may leave: omitting it from the members is valid.
func TestSetMembershipAllowsOmittingSelf(t *testing.T) {
	nd, _ := twoNode()
	others := map[string]string{"node-2": "h2:1", "node-3": "h3:1"}
	if changed, err := nd.SetMembership(1, others); err != nil || !changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
	assertMembership(t, nd, 1, others)
}

// (e) The fingerprint identifies placement: members, addresses, N, vnodes and
// ring scheme; not the epoch, and not map order.
func TestFingerprint(t *testing.T) {
	ids := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	build := func(order []string) map[string]string {
		m := map[string]string{}
		for _, id := range order {
			m[id] = id + ":1"
		}
		return m
	}
	reversed := make([]string, len(ids))
	for i, id := range ids {
		reversed[len(ids)-1-i] = id
	}
	want := fingerprint(build(ids), 3)
	for range 20 { // map iteration order varies per run
		if got := fingerprint(build(reversed), 3); got != want {
			t.Fatalf("fingerprint depends on map order: %s vs %s", got, want)
		}
	}

	base := build(ids)
	differ := map[string]string{
		"N":               fingerprint(base, 2),
		"an address":      fingerprint(withMember(base, "c", "elsewhere:1"), 3),
		"an added id":     fingerprint(withMember(base, "z", "z:1"), 3),
		"a renamed id":    fingerprint(renamed(base, "c", "cc"), 3),
		"swapped address": fingerprint(swappedAddrs(base, "a", "b"), 3),
	}
	for what, got := range differ {
		if got == want {
			t.Errorf("fingerprint did not change with %s", what)
		}
	}

	v1, err1 := buildView(1, base, 3)
	v9, err9 := buildView(9, base, 3)
	if err1 != nil || err9 != nil {
		t.Fatal(err1, err9)
	}
	if v1.fingerprint != v9.fingerprint || v1.fingerprint != want {
		t.Errorf("fingerprint depends on the epoch: %s, %s, want %s", v1.fingerprint, v9.fingerprint, want)
	}
	if len(want) != 64 {
		t.Errorf("expected a hex sha256, got %q", want)
	}
}

func renamed(m map[string]string, from, to string) map[string]string {
	out := maps.Clone(m)
	out[to] = out[from]
	delete(out, from)
	return out
}

func swappedAddrs(m map[string]string, a, b string) map[string]string {
	out := maps.Clone(m)
	out[a], out[b] = m[b], m[a]
	return out
}

func openTwoNode(t *testing.T, dir string) *Node {
	t.Helper()
	nd, err := NewPersistent("node-1", "localhost:60911", 2, 1, 1, map[string]string{"node-2": "localhost:60912"}, dir, 1<<20)
	if err != nil {
		t.Fatalf("NewPersistent: %v", err)
	}
	return nd
}

// (c) A membership the node adopted survives a restart, whatever the
// configuration's member list says on the next start.
func TestSetMembershipPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	three := map[string]string{"node-1": "localhost:60911", "node-2": "localhost:60912", "node-3": "localhost:60913"}

	nd := openTwoNode(t, dir)
	if changed, err := nd.SetMembership(3, three); err != nil || !changed {
		t.Fatalf("SetMembership: changed=%v err=%v", changed, err)
	}
	fp := nd.membership.Load().fingerprint
	if err := nd.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openTwoNode(t, dir) // configuration still lists only node-2 as a neighbor
	defer reopened.Close()
	assertMembership(t, reopened, 3, three)
	if got := reopened.membership.Load().fingerprint; got != fp {
		t.Fatalf("fingerprint after reopen %s, want %s", got, fp)
	}
}

// A node with no MEMBERSHIP file starts from its configuration, at epoch 0,
// and doesn't write one until it adopts something.
func TestFreshPersistentNodeStartsAtEpochZeroWithoutAFile(t *testing.T) {
	dir := t.TempDir()
	nd := openTwoNode(t, dir)
	defer nd.Close()
	assertMembership(t, nd, 0, map[string]string{"node-1": "localhost:60911", "node-2": "localhost:60912"})
	if _, err := os.Stat(filepath.Join(dir, membershipFileName)); !os.IsNotExist(err) {
		t.Fatalf("expected no MEMBERSHIP file yet (stat err %v)", err)
	}
}

// (d) The view is stored before it is published: if storing fails, the node
// keeps the view it had.
func TestSetMembershipDoesNotPublishWhenPersistFails(t *testing.T) {
	dir := t.TempDir()
	nd := openTwoNode(t, dir)
	defer nd.Close()
	held := nd.membership.Load()

	var attempted bool
	nd.persistView = func(*view) error {
		attempted = true
		return errors.New("disk full")
	}
	changed, err := nd.SetMembership(1, map[string]string{"node-1": "localhost:60911", "node-2": "localhost:60912", "node-3": "localhost:60913"})
	if err == nil || !strings.Contains(err.Error(), "disk full") || changed {
		t.Fatalf("changed=%v err=%v, want the persist failure", changed, err)
	}
	if !attempted {
		t.Fatal("the persist step never ran")
	}
	if nd.membership.Load() != held {
		t.Fatal("the view was published although persisting it failed")
	}
}

// (f) Removing a member retires its client without closing it; Close closes it.
func TestRemovedMembersClientIsRetiredNotClosed(t *testing.T) {
	addrs := map[string]string{"node-1": "localhost:60921", "node-2": "localhost:60922", "node-3": "localhost:60923"}
	node1 := New("node-1", addrs["node-1"], 2, 1, 1, map[string]string{"node-2": addrs["node-2"], "node-3": addrs["node-3"]})
	for _, id := range []string{"node-2", "node-3"} {
		peer := New(id, addrs[id], 2, 1, 1, nil)
		peer.Store.Put("k", id, nil)
		serveNode(t, peer, addrs[id])
	}
	client2, err := node1.getOrDialClient("node-2")
	if err != nil {
		t.Fatal(err)
	}
	client3, err := node1.getOrDialClient("node-3")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := node1.SetMembership(1, map[string]string{"node-1": addrs["node-1"], "node-2": addrs["node-2"]}); err != nil {
		t.Fatal(err)
	}

	node1.clientsMu.Lock()
	_, has2 := node1.clients["node-2"]
	_, has3 := node1.clients["node-3"]
	retired := len(node1.retired)
	node1.clientsMu.Unlock()
	if !has2 || has3 || retired != 1 {
		t.Fatalf("expected node-2 kept, node-3 retired: has2=%v has3=%v retired=%d", has2, has3, retired)
	}
	if items, _, err := client3.FetchItem(context.Background(), "k"); err != nil || len(items) != 1 {
		t.Fatalf("the retired client should still work, got %v, %v", itemValues(items), err)
	}
	if _, err := node1.getOrDialClient("node-3"); err == nil {
		t.Fatal("expected node-3 to be an unknown peer now")
	}

	if err := node1.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := client3.FetchItem(context.Background(), "k"); status.Code(err) != codes.Canceled {
		t.Fatalf("Close should close the retired client (codes.Canceled), got %v", err)
	}
	if _, _, err := client2.FetchItem(context.Background(), "k"); status.Code(err) != codes.Canceled {
		t.Fatalf("Close should close the active client (codes.Canceled), got %v", err)
	}
}

// (g) Concurrent callers for one epoch: exactly one wins, and the view is the
// winner's.
func TestConcurrentSetMembershipHasOneWinnerPerEpoch(t *testing.T) {
	nd, base := twoNode()
	const callers = 8

	for epoch := uint64(1); epoch <= 20; epoch++ {
		// Different members per caller: one wins, the rest conflict.
		variants := make([]map[string]string, callers)
		for i := range variants {
			variants[i] = withMember(base, "node-3", fmt.Sprintf("h3:%d", i))
		}
		results := make([]struct {
			changed bool
			err     error
		}, callers)
		var wg sync.WaitGroup
		for i := range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i].changed, results[i].err = nd.SetMembership(epoch, variants[i])
			}()
		}
		wg.Wait()

		winner, conflicts := -1, 0
		for i, r := range results {
			switch {
			case r.err == nil && r.changed:
				if winner >= 0 {
					t.Fatalf("epoch %d: two winners, %d and %d", epoch, winner, i)
				}
				winner = i
			case errors.Is(r.err, ErrMembershipConflict):
				conflicts++
			default:
				t.Fatalf("epoch %d, caller %d: changed=%v err=%v", epoch, i, r.changed, r.err)
			}
		}
		if winner < 0 || conflicts != callers-1 {
			t.Fatalf("epoch %d: winner=%d conflicts=%d, want one winner and %d conflicts", epoch, winner, conflicts, callers-1)
		}
		assertMembership(t, nd, epoch, variants[winner])
	}

	// Callers agreeing on the members: one adopts, the others see a no-op.
	var wg sync.WaitGroup
	var mu sync.Mutex
	adopted := 0
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			changed, err := nd.SetMembership(21, base)
			if err != nil {
				t.Errorf("agreeing caller: %v", err)
			}
			if changed {
				mu.Lock()
				adopted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if adopted != 1 {
		t.Fatalf("%d callers adopted epoch 21, want exactly 1", adopted)
	}
}

func writeRawMembership(t *testing.T, dir string, content any) {
	t.Helper()
	var data []byte
	switch c := content.(type) {
	case string:
		data = []byte(c)
	default:
		var err error
		if data, err = json.Marshal(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, membershipFileName), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// (i) A MEMBERSHIP file the node can't trust stops it from starting.
func TestNewPersistentRefusesAnUntrustworthyMembershipFile(t *testing.T) {
	good := map[string]string{"node-1": "localhost:60911", "node-2": "localhost:60912"}
	other := map[string]string{"node-1": "localhost:60911", "node-2": "localhost:60999"}
	dupAddr := map[string]string{"node-1": "localhost:60911", "node-2": "localhost:60911"}

	for name, tc := range map[string]struct {
		content any
		wantErr string
	}{
		"not JSON":              {"node-1=localhost", "corrupt"},
		"truncated":             {`{"epoch": 4, "members": {"node-1": "loc`, "corrupt"},
		"unknown field":         {`{"epoch":1,"members":{},"fingerprint":"","extra":1}`, "corrupt"},
		"wrong fingerprint":     {membershipFile{Epoch: 4, Members: good, Fingerprint: strings.Repeat("0", 64)}, "fingerprint"},
		"tampered members":      {membershipFile{Epoch: 4, Members: other, Fingerprint: fingerprint(good, 2)}, "fingerprint"},
		"written for another N": {membershipFile{Epoch: 4, Members: good, Fingerprint: fingerprint(good, 3)}, "fingerprint"},
		"no members":            {membershipFile{Epoch: 4, Members: map[string]string{}, Fingerprint: fingerprint(nil, 2)}, "no members"},
		"duplicate address":     {membershipFile{Epoch: 4, Members: dupAddr, Fingerprint: fingerprint(dupAddr, 2)}, "share address"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeRawMembership(t, dir, tc.content)

			nd, err := NewPersistent("node-1", "localhost:60911", 2, 1, 1, map[string]string{"node-2": "localhost:60912"}, dir, 1<<20)
			if err == nil {
				nd.Close()
				t.Fatal("expected NewPersistent to refuse the file")
			}
			if !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), membershipFileName) {
				t.Fatalf("error %q should name the file and mention %q", err, tc.wantErr)
			}

			// The refusal released the directory: with the file gone it opens.
			if err := os.Remove(filepath.Join(dir, membershipFileName)); err != nil {
				t.Fatal(err)
			}
			nd = openTwoNode(t, dir)
			nd.Close()
		})
	}
}
