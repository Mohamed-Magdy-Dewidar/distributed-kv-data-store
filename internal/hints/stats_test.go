package hints

import (
	"testing"

	"distributed-kv-datastore/internal/model"
)

// TestStatsCountHintsCreatedAndDelivered: each hinted item stored is
// counted as created, and each one marked delivered as delivered;
// PendingItems is what remains, across every target.
func TestStatsCountHintsCreatedAndDelivered(t *testing.T) {
	s := mustOpen(t, t.TempDir(), 1<<20)
	defer s.Close()

	a1 := item("a1", map[string]uint32{"node-1": 1})
	a2 := item("a2", map[string]uint32{"node-3": 1}) // concurrent with a1: two siblings
	b := item("b", map[string]uint32{"node-1": 1})
	mustAdd(t, s, "node-2", "a", a1)
	mustAdd(t, s, "node-2", "a", a2)
	mustAdd(t, s, "node-4", "b", b)

	if st := s.Stats(); st.Created != 3 || st.Delivered != 0 {
		t.Fatalf("after 3 hints: %+v, want 3 created, 0 delivered", st)
	}
	if n, err := s.PendingItems(); err != nil || n != 3 {
		t.Fatalf("PendingItems = %d, %v; want 3", n, err)
	}

	if err := s.MarkDelivered("node-2", "a", []*model.DataItem{a1, a2}); err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}
	if st := s.Stats(); st.Created != 3 || st.Delivered != 2 {
		t.Fatalf("after delivering both of node-2's: %+v, want 3 created, 2 delivered", st)
	}
	if n, err := s.PendingItems(); err != nil || n != 1 {
		t.Fatalf("PendingItems = %d, %v; want 1 (node-4's)", n, err)
	}
	if st := s.Stats(); st.Engine.MemtableBytes <= 0 {
		t.Fatalf("hint store engine stats %+v: want its memtable to hold the hints", st.Engine)
	}
}

// TestStatsDoNotCountRejectedHints: a hint the store refuses is not counted.
func TestStatsDoNotCountRejectedHints(t *testing.T) {
	s := mustOpen(t, t.TempDir(), 1<<20)
	defer s.Close()
	if err := s.Add("bad\x00target", "k", item("v", map[string]uint32{"node-1": 1})); err == nil {
		t.Fatal("Add accepted a target containing the separator")
	}
	if st := s.Stats(); st.Created != 0 {
		t.Fatalf("after a rejected hint: %+v, want 0 created", st)
	}
}
