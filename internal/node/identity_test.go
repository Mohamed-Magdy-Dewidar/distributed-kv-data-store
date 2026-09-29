package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"distributed-kv-datastore/internal/identity"
	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/storage/engine"
	"distributed-kv-datastore/internal/vectorclock"
)

var clockIDPattern = regexp.MustCompile(`^node-1#[0-9a-f]{16}$`)

func TestInMemoryNodesTakeANewIncarnationEach(t *testing.T) {
	a := New("node-1", "unused", 1, 1, 1, nil)
	b := New("node-1", "unused", 1, 1, 1, nil)
	if !clockIDPattern.MatchString(a.ClockID()) || !clockIDPattern.MatchString(b.ClockID()) {
		t.Fatalf("clock IDs %q, %q are not node-1#<16 hex>", a.ClockID(), b.ClockID())
	}
	if a.ClockID() == b.ClockID() {
		t.Fatalf("two in-memory nodes shared clock ID %q", a.ClockID())
	}
	if a.ID != "node-1" {
		t.Fatalf("node ID changed to %q", a.ID)
	}
}

// The incarnation survives a reopen of the same dir, a wiped dir gets a
// different one, and LastUpdatedBy stays the plain node ID.
func TestPersistentIncarnationSurvivesReopenAndChangesOnWipe(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	nd := openSoloNode(t, dir, 1<<20)
	first := nd.ClockID()
	if !clockIDPattern.MatchString(first) {
		t.Fatalf("clock ID %q is not node-1#<16 hex>", first)
	}
	if err := nd.Put(ctx, "k", "v", nil); err != nil {
		t.Fatal(err)
	}
	items, _, _ := nd.Store.Get("k")
	if len(items) != 1 || items[0].LastUpdatedBy != "node-1" {
		t.Fatalf("expected LastUpdatedBy to stay the plain node ID, got %+v", items)
	}
	nd.Close()

	reopened := openSoloNode(t, dir, 1<<20)
	if reopened.ClockID() != first {
		t.Fatalf("reopened dir changed clock ID: %q -> %q", first, reopened.ClockID())
	}
	reopened.Close()

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	wiped := openSoloNode(t, dir, 1<<20)
	defer wiped.Close()
	if wiped.ClockID() == first {
		t.Fatalf("a wiped dir kept clock ID %q", first)
	}
}

// A data dir written before incarnations existed has no IDENTITY: it gets one,
// and its old plain-ID clock entries stay valid history that a new write
// supersedes rather than conflicts with.
func TestDataDirWithoutIdentityGetsOneAndKeepsOldHistory(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	e, err := engine.Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	legacy := &model.DataItem{
		Value:         "legacy",
		VectorClock:   vectorclock.FromSnapshot(map[string]uint32{"node-1": 3}),
		LastUpdatedBy: "node-1",
	}
	if err := e.Put("k", legacy); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, identity.FileName)); !os.IsNotExist(err) {
		t.Fatalf("setup: expected no IDENTITY yet (stat err %v)", err)
	}

	nd := openSoloNode(t, dir, 1<<20)
	defer nd.Close()
	if !clockIDPattern.MatchString(nd.ClockID()) {
		t.Fatalf("clock ID %q is not node-1#<16 hex>", nd.ClockID())
	}
	if _, err := os.Stat(filepath.Join(dir, identity.FileName)); err != nil {
		t.Fatalf("IDENTITY was not written: %v", err)
	}

	if err := nd.Put(ctx, "k", "new", nil); err != nil {
		t.Fatal(err)
	}
	items, _, _ := nd.Store.Get("k")
	if len(items) != 1 || items[0].Value != "new" {
		t.Fatalf("expected the new write to supersede the legacy one, got %v", itemValues(items))
	}
	want := map[string]uint32{"node-1": 3, nd.ClockID(): 1}
	if got := items[0].VectorClock.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("clock %v, want %v (legacy entry kept, new entry added)", got, want)
	}
}

func TestNewPersistentRefusesADirOwnedByAnotherNodeID(t *testing.T) {
	dir := t.TempDir()
	openSoloNode(t, dir, 1<<20).Close() // node-1 claims dir

	_, err := NewPersistent("node-2", "unused", 1, 1, 1, nil, dir, 1<<20)
	if err == nil {
		t.Fatal("expected NewPersistent to refuse a dir owned by node-1")
	}
	for _, want := range []string{"node-1", "node-2", dir} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}

	// The refusal released the dir's lock: the rightful owner can open it.
	again, err := NewPersistent("node-1", "unused", 1, 1, 1, nil, dir, 1<<20)
	if errors.Is(err, engine.ErrLocked) {
		t.Fatalf("the refused open left the dir locked: %v", err)
	}
	if err != nil {
		t.Fatalf("reopen as node-1: %v", err)
	}
	again.Close()
}

// TestReusedNodeIDOnAnEmptyDataDirDoesNotLoseWrites: node-1 writes a key until
// its replica holds {node-1#<old>: 5}. A replacement with the same ID on an
// empty data dir then writes the key with no context. Keyed by the plain node
// ID, that write would carry {node-1: 1}, which the replica drops as older
// than {node-1: 5} — while acking it. Keyed by incarnation it is a concurrent
// sibling: the replica keeps it and a quorum read returns it.
func TestReusedNodeIDOnAnEmptyDataDirDoesNotLoseWrites(t *testing.T) {
	ctx := context.Background()
	addrs := reserveAddrs(t, "node-2")
	addrs["node-1"] = knownAddr() // restarted on the same address
	dir := t.TempDir()
	const n, w, r = 2, 2, 2

	node2 := New("node-2", addrs["node-2"], n, w, r, map[string]string{"node-1": addrs["node-1"]})
	serveNode(t, node2, addrs["node-2"])

	start := func() (*Node, *rpc.Listener) {
		nd, err := NewPersistent("node-1", addrs["node-1"], n, w, r, map[string]string{"node-2": addrs["node-2"]}, dir, 1<<20)
		if err != nil {
			t.Fatalf("NewPersistent: %v", err)
		}
		l, err := rpc.Serve(addrs["node-1"], nd.Store, nd)
		if err != nil {
			nd.Close()
			t.Fatalf("serve node-1: %v", err)
		}
		return nd, l
	}

	first, listener := start()
	for i := range 5 {
		if err := first.Put(ctx, "k", "before-wipe", nil); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	oldClockID := first.ClockID()
	held, _, _ := node2.Store.Get("k")
	if len(held) != 1 || held[0].VectorClock.Snapshot()[oldClockID] != 5 {
		t.Fatalf("setup: expected node-2 to hold {%s: 5}, got %+v", oldClockID, held)
	}
	listener.Stop()
	first.Close()

	// Replace node-1: same ID, empty data dir.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	second, listener2 := start()
	t.Cleanup(func() { listener2.Stop(); second.Close() })

	if err := second.Put(ctx, "k", "after-wipe", nil); err != nil {
		t.Fatalf("Put after wipe: %v", err)
	}

	want := []any{"after-wipe", "before-wipe"}
	replicaItems, _, err := node2.Store.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if got := sortedValues(replicaItems); !reflect.DeepEqual(got, want) {
		t.Fatalf("replica dropped the replacement's write: it has %v, want siblings %v", got, want)
	}
	got, err := second.Get(ctx, "k")
	if err != nil {
		t.Fatalf("quorum Get: %v", err)
	}
	if v := sortedValues(got); !reflect.DeepEqual(v, want) {
		t.Fatalf("quorum Get returned %v, want siblings %v", v, want)
	}
}
