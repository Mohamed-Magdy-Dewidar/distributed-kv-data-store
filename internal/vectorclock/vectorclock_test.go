package vectorclock

import "testing"

func TestRelations(t *testing.T) {
	t.Run("equal", func(t *testing.T) {
		a := New()
		a.Increment("s0")
		b := New()
		b.Increment("s0")
		if rel := a.Compare(b); rel != Equal {
			t.Errorf("expected Equal, got %v", rel)
		}
	})

	t.Run("after", func(t *testing.T) {
		a := New()
		a.Increment("s0")
		a.Increment("s0")
		b := New()
		b.Increment("s0")
		if rel := a.Compare(b); rel != After {
			t.Errorf("expected After, got %v", rel)
		}
	})

	t.Run("concurrent, repeated to catch map-iteration-order regressions", func(t *testing.T) {
		x := &VectorClock{state: map[string]uint32{"s0": 1, "s1": 2}}
		y := &VectorClock{state: map[string]uint32{"s0": 2, "s1": 1}}
		for i := 0; i < 20; i++ {
			if rel := x.Compare(y); rel != Concurrent {
				t.Fatalf("run %d: expected Concurrent, got %v", i, rel)
			}
		}
	})

	t.Run("concurrent when one clock has never seen the other's node", func(t *testing.T) {
		// Regression test for the union-of-keys fix: two clocks that have
		// each only ever incremented their own node should compare as
		// Concurrent, not silently treat the other's missing key as "loses."
		x := &VectorClock{state: map[string]uint32{"node-1": 1}}
		y := &VectorClock{state: map[string]uint32{"node-2": 1}}
		if rel := x.Compare(y); rel != Concurrent {
			t.Errorf("expected Concurrent for disjoint node sets, got %v", rel)
		}
	})
}

func TestBuildFromContext(t *testing.T) {
	t.Run("nil context behaves like a fresh clock", func(t *testing.T) {
		vc := BuildFromContext(nil, "node-1")
		snap := vc.Snapshot()
		if snap["node-1"] != 1 {
			t.Errorf("expected node-1=1, got %v", snap)
		}
	})

	t.Run("builds on top of existing context, does not reset it", func(t *testing.T) {
		context := map[string]uint32{"node-1": 2, "node-2": 1}
		vc := BuildFromContext(context, "node-1")
		snap := vc.Snapshot()
		if snap["node-1"] != 3 {
			t.Errorf("expected node-1=3 (incremented on top of context), got %v", snap)
		}
		if snap["node-2"] != 1 {
			t.Errorf("expected node-2=1 (carried over from context), got %v", snap)
		}
	})

	t.Run("does not mutate the caller's context map", func(t *testing.T) {
		context := map[string]uint32{"node-1": 1}
		_ = BuildFromContext(context, "node-1")
		if context["node-1"] != 1 {
			t.Errorf("BuildFromContext mutated the caller's map, got %v", context)
		}
	})
}
