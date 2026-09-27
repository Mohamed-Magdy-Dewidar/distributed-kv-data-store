package hints

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/vectorclock"
)

func item(value any, counts map[string]uint32) *model.DataItem {
	return &model.DataItem{Value: value, VectorClock: vectorclock.FromSnapshot(counts), LastUpdatedBy: "node-1"}
}

func tombstone(counts map[string]uint32) *model.DataItem {
	return &model.DataItem{VectorClock: vectorclock.FromSnapshot(counts), LastUpdatedBy: "node-1", IsDeleted: true}
}

func mustOpen(t *testing.T, dir string, maxBytes int) *Store {
	t.Helper()
	s, err := Open(dir, maxBytes)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	return s
}

func mustAdd(t *testing.T, s *Store, target, key string, it *model.DataItem) {
	t.Helper()
	if err := s.Add(target, key, it); err != nil {
		t.Fatalf("Add(%q, %q) failed: %v", target, key, err)
	}
}

func mustMark(t *testing.T, s *Store, target, key string, delivered []*model.DataItem) {
	t.Helper()
	if err := s.MarkDelivered(target, key, delivered); err != nil {
		t.Fatalf("MarkDelivered(%q, %q) failed: %v", target, key, err)
	}
}

// pendingView flattens Pending(target) to key -> values, for comparison.
func pendingView(t *testing.T, s *Store, target string) map[string][]any {
	t.Helper()
	hints, err := s.Pending(target)
	if err != nil {
		t.Fatalf("Pending(%q) failed: %v", target, err)
	}
	view := make(map[string][]any)
	for _, h := range hints {
		for _, it := range h.Items {
			if it.IsDeleted {
				view[h.Key] = append(view[h.Key], "<deleted>")
			} else {
				view[h.Key] = append(view[h.Key], it.Value)
			}
		}
	}
	return view
}

// TestHintsSurviveRestart: hints are durable — a reopened store returns
// the same pending hints, clocks included, per target.
func TestHintsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, 1<<20)
	mustAdd(t, s, "node-2", "a", item("a1", map[string]uint32{"node-1": 1}))
	mustAdd(t, s, "node-2", "b", item("b1", map[string]uint32{"node-1": 1}))
	mustAdd(t, s, "node-3", "a", item("a1", map[string]uint32{"node-1": 1}))
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	s = mustOpen(t, dir, 1<<20)
	defer s.Close()
	if targets, _ := s.Targets(); !reflect.DeepEqual(targets, []string{"node-2", "node-3"}) {
		t.Fatalf("expected targets [node-2 node-3], got %v", targets)
	}
	if got, want := pendingView(t, s, "node-2"), map[string][]any{"a": {"a1"}, "b": {"b1"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node-2: expected %v, got %v", want, got)
	}
	hints, _ := s.Pending("node-3")
	if len(hints) != 1 || !reflect.DeepEqual(hints[0].Items[0].VectorClock.Snapshot(), map[string]uint32{"node-1": 1}) {
		t.Fatalf("node-3: expected one hint with clock {node-1:1}, got %+v", hints)
	}
}

// TestKeysAreScopedToTheirTarget: a target's hints never include another
// target's, even when one node ID is a prefix of another or the key
// itself contains the separator.
func TestKeysAreScopedToTheirTarget(t *testing.T) {
	s := mustOpen(t, t.TempDir(), 1<<20)
	defer s.Close()
	mustAdd(t, s, "node-1", "k", item("for-node-1", map[string]uint32{"node-9": 1}))
	mustAdd(t, s, "node-10", "k", item("for-node-10", map[string]uint32{"node-9": 1}))
	mustAdd(t, s, "node-1", "x\x00y", item("sep-in-key", map[string]uint32{"node-9": 1}))

	if got, want := pendingView(t, s, "node-1"), map[string][]any{"k": {"for-node-1"}, "x\x00y": {"sep-in-key"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node-1: expected %v, got %v", want, got)
	}
	if got, want := pendingView(t, s, "node-10"), map[string][]any{"k": {"for-node-10"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node-10: expected %v, got %v", want, got)
	}
}

// TestMarkDeliveredRetiresExactlyWhatWasDelivered: after delivering one
// key's hints, that key has nothing pending; other keys and other targets
// are untouched.
func TestMarkDeliveredRetiresExactlyWhatWasDelivered(t *testing.T) {
	s := mustOpen(t, t.TempDir(), 1<<20)
	defer s.Close()
	a := []*model.DataItem{ // concurrent siblings: both delivered together
		item("a-from-1", map[string]uint32{"node-1": 1}),
		item("a-from-3", map[string]uint32{"node-3": 1}),
	}
	mustAdd(t, s, "node-2", "a", a[0])
	mustAdd(t, s, "node-2", "a", a[1])
	mustAdd(t, s, "node-2", "b", item("b1", map[string]uint32{"node-1": 1}))
	mustAdd(t, s, "node-4", "a", item("a-for-4", map[string]uint32{"node-1": 1}))

	hints, _ := s.Pending("node-2")
	if len(hints) != 2 || hints[0].Key != "a" || len(hints[0].Items) != 2 {
		t.Fatalf("test setup: expected a (2 siblings) and b pending for node-2, got %+v", hints)
	}
	mustMark(t, s, "node-2", "a", hints[0].Items)

	if got, want := pendingView(t, s, "node-2"), map[string][]any{"b": {"b1"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node-2: expected only b pending after delivering a, got %v", got)
	}
	if got, want := pendingView(t, s, "node-4"), map[string][]any{"a": {"a-for-4"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node-4: expected its hint untouched, got %v", got)
	}
}

// TestHintAddedAfterDeliveryStaysPending: a hint that arrives between a
// delivery's read and its MarkDelivered wasn't delivered, so the marker
// must not retire it — the race a read-then-delete would lose.
func TestHintAddedAfterDeliveryStaysPending(t *testing.T) {
	s := mustOpen(t, t.TempDir(), 1<<20)
	defer s.Close()
	mustAdd(t, s, "node-2", "k", item("v1", map[string]uint32{"node-1": 1}))
	hints, _ := s.Pending("node-2")

	mustAdd(t, s, "node-2", "k", item("v2", map[string]uint32{"node-1": 2})) // lands mid-delivery
	mustMark(t, s, "node-2", "k", hints[0].Items)                              // only v1 was delivered

	if got, want := pendingView(t, s, "node-2"), map[string][]any{"k": {"v2"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("expected v2 still pending, got %v", got)
	}

	// And once v2 is delivered too, the key is clear.
	hints, _ = s.Pending("node-2")
	mustMark(t, s, "node-2", "k", hints[0].Items)
	if got := pendingView(t, s, "node-2"); len(got) != 0 {
		t.Fatalf("expected nothing pending after delivering v2, got %v", got)
	}
}

// TestMarkDeliveredRetiresAFlushedHintAcrossRestart: a hint already
// flushed to an SSTable can't be removed from the memtable; the marker
// must still retire it, and keep it retired after a restart.
func TestMarkDeliveredRetiresAFlushedHintAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	s := mustOpen(t, dir, 200)
	mustAdd(t, s, "node-2", "k", item(strings.Repeat("x", 300), map[string]uint32{"node-1": 1})) // crosses: flushed
	s.engine.WaitForPendingFlushes()
	if ssts, _ := filepath.Glob(filepath.Join(dir, "*.sst")); len(ssts) == 0 {
		t.Fatal("test setup: expected the hint to have been flushed to an SSTable")
	}

	hints, _ := s.Pending("node-2")
	if len(hints) != 1 {
		t.Fatalf("test setup: expected the flushed hint pending, got %+v", hints)
	}
	mustMark(t, s, "node-2", "k", hints[0].Items)
	if got := pendingView(t, s, "node-2"); len(got) != 0 {
		t.Fatalf("expected the flushed hint retired, got %v", got)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	s = mustOpen(t, dir, 200)
	defer s.Close()
	if got := pendingView(t, s, "node-2"); len(got) != 0 {
		t.Fatalf("expected the flushed hint still retired after restart, got %v", got)
	}
}

// TestHintedDeleteIsNotMistakenForAMarker: a client delete is a tombstone
// version and gets hinted like any other write. It must stay pending
// until delivered, not be filtered out as if it were a delivery marker.
func TestHintedDeleteIsNotMistakenForAMarker(t *testing.T) {
	s := mustOpen(t, t.TempDir(), 1<<20)
	defer s.Close()
	mustAdd(t, s, "node-2", "k", tombstone(map[string]uint32{"node-1": 3}))

	if got, want := pendingView(t, s, "node-2"), map[string][]any{"k": {"<deleted>"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("expected the hinted delete pending, got %v", got)
	}
	hints, _ := s.Pending("node-2")
	mustMark(t, s, "node-2", "k", hints[0].Items)
	if got := pendingView(t, s, "node-2"); len(got) != 0 {
		t.Fatalf("expected the delivered delete retired, got %v", got)
	}
}

// TestAddRejectsInvalidInput: a target containing the separator would make
// keys ambiguous, and an item carrying the reserved entry would be taken
// for a marker and silently never delivered.
func TestAddRejectsInvalidInput(t *testing.T) {
	s := mustOpen(t, t.TempDir(), 1<<20)
	defer s.Close()
	ok := item("v", map[string]uint32{"node-1": 1})
	for _, target := range []string{"", "bad" + sep + "id"} {
		if err := s.Add(target, "k", ok); err == nil {
			t.Errorf("expected Add to reject target %q", target)
		}
	}
	if err := s.Add("node-2", "k", item("v", map[string]uint32{deliveredEntry: 1})); err == nil {
		t.Error("expected Add to reject an item carrying the delivery-marker entry")
	}
	if targets, _ := s.Targets(); len(targets) != 0 {
		t.Fatalf("expected nothing stored after rejected adds, got targets %v", targets)
	}
}
