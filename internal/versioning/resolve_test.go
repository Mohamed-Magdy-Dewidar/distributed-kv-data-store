package versioning

import (
	"testing"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/vectorclock"
)

func TestResolveHandConstructedSiblings(t *testing.T) {
	base := map[string]uint32{"node-1": 1, "node-2": 1}
	itemA := &model.DataItem{Value: "value-from-node-1", VectorClock: vectorclock.BuildFromContext(base, "node-1")}
	itemB := &model.DataItem{Value: "value-from-node-2", VectorClock: vectorclock.BuildFromContext(base, "node-2")}

	if rel := itemA.VectorClock.Compare(itemB.VectorClock); rel != vectorclock.Concurrent {
		t.Fatalf("expected Concurrent, got %v", rel)
	}

	siblings := Resolve([]*model.DataItem{itemA}, itemB)
	if len(siblings) != 2 {
		t.Fatalf("expected 2 siblings, got %d", len(siblings))
	}

	mergedContext := UnionVectorClock(siblings)
	resolved := &model.DataItem{Value: "client-merged-value", VectorClock: vectorclock.BuildFromContext(mergedContext, "node-1")}
	final := Resolve(siblings, resolved)

	if len(final) != 1 {
		t.Fatalf("expected merged write to collapse siblings to 1, got %d", len(final))
	}
	if final[0].Value != "client-merged-value" {
		t.Errorf("expected merged value to survive, got %v", final[0].Value)
	}
}
