package compaction

import (
	"strings"
	"testing"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/storage/memtable"
	"distributed-kv-datastore/internal/storage/sstable"
	"distributed-kv-datastore/internal/vectorclock"
)

func itemWithClock(value any, vc *vectorclock.VectorClock, by string) *model.DataItem {
	return &model.DataItem{Value: value, VectorClock: vc, LastUpdatedBy: by}
}

func clockOf(counts map[string]uint32) *vectorclock.VectorClock {
	return vectorclock.FromSnapshot(counts)
}

// writeSST writes entries to a real SSTable in its own temp dir. Separate
// dirs avoid relying on sstable.Write's timestamp IDs being distinct for
// files written in quick succession.
func writeSST(t *testing.T, entries ...memtable.Entry) *sstable.SSTable {
	t.Helper()
	sst, err := sstable.Write(t.TempDir(), entries)
	if err != nil {
		t.Fatalf("sstable.Write failed: %v", err)
	}
	return sst
}

func mustMerge(t *testing.T, sources ...*sstable.SSTable) []memtable.Entry {
	t.Helper()
	merged, err := Merge(sources)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	return merged
}

func TestMergeWithNoOverlappingKeysConcatenatesSorted(t *testing.T) {
	vc := clockOf(map[string]uint32{"node-1": 1})
	src1 := writeSST(t,
		memtable.Entry{Key: "alpha", Item: itemWithClock("1", vc, "node-1")},
		memtable.Entry{Key: "charlie", Item: itemWithClock("3", vc, "node-1")},
	)
	src2 := writeSST(t,
		memtable.Entry{Key: "bravo", Item: itemWithClock("2", vc, "node-1")},
		memtable.Entry{Key: "delta", Item: itemWithClock("4", vc, "node-1")},
	)

	merged := mustMerge(t, src1, src2)

	wantKeys := []string{"alpha", "bravo", "charlie", "delta"}
	wantValues := []string{"1", "2", "3", "4"}
	if len(merged) != len(wantKeys) {
		t.Fatalf("expected %d entries, got %d: %v", len(wantKeys), len(merged), merged)
	}
	for i, e := range merged {
		if e.Key != wantKeys[i] || e.Item.Value != wantValues[i] {
			t.Errorf("entry %d: expected %s=%s, got %s=%v", i, wantKeys[i], wantValues[i], e.Key, e.Item.Value)
		}
	}
}

func TestMergeDropsCausallySupersededVersion(t *testing.T) {
	// Shared causal history: newer is built on top of older's clock and
	// then advanced, so older happens-before newer.
	older := clockOf(map[string]uint32{"node-1": 1})
	newer := clockOf(older.Snapshot())
	newer.Increment("node-1")
	if newer.Compare(older) != vectorclock.After {
		t.Fatalf("test setup: expected newer to dominate older")
	}

	oldSrc := writeSST(t, memtable.Entry{Key: "k", Item: itemWithClock("old", older, "node-1")})
	newSrc := writeSST(t, memtable.Entry{Key: "k", Item: itemWithClock("new", newer, "node-1")})

	// Domination must win regardless of source order.
	for name, sources := range map[string][]*sstable.SSTable{
		"newest-first": {newSrc, oldSrc},
		"oldest-first": {oldSrc, newSrc},
	} {
		merged := mustMerge(t, sources...)
		if len(merged) != 1 {
			t.Fatalf("%s: expected superseded version to be dropped, got %d entries: %v", name, len(merged), merged)
		}
		if merged[0].Item.Value != "new" {
			t.Errorf("%s: expected surviving value 'new', got %v", name, merged[0].Item.Value)
		}
		if merged[0].Item.VectorClock.Compare(newer) != vectorclock.Equal {
			t.Errorf("%s: expected surviving clock to equal the newer clock, got %v", name, merged[0].Item.VectorClock.Snapshot())
		}
	}
}

func TestMergePreservesTombstone(t *testing.T) {
	written := clockOf(map[string]uint32{"node-1": 1})
	deleted := clockOf(written.Snapshot())
	deleted.Increment("node-1")

	valueSrc := writeSST(t, memtable.Entry{Key: "k", Item: itemWithClock("alive", written, "node-1")})
	tombSrc := writeSST(t, memtable.Entry{Key: "k", Item: &model.DataItem{
		VectorClock:   deleted,
		LastUpdatedBy: "node-1",
		IsDeleted:     true,
	}})

	merged := mustMerge(t, tombSrc, valueSrc)

	if len(merged) != 1 {
		t.Fatalf("expected only the tombstone to survive, got %d entries: %v", len(merged), merged)
	}
	if merged[0].Key != "k" || !merged[0].Item.IsDeleted {
		t.Fatalf("expected tombstone for k to survive the merge, got key=%s IsDeleted=%v value=%v",
			merged[0].Key, merged[0].Item.IsDeleted, merged[0].Item.Value)
	}
}

func TestMergePreservesGenuineConcurrentSiblings(t *testing.T) {
	// Mirrors TestGetSurfacesGenuineSiblingConflictsAcrossNodes: two
	// writes originating independently on different nodes, with no shared
	// causal history, so neither clock dominates the other.
	fromNode1 := clockOf(map[string]uint32{"node-1": 1})
	fromNode2 := clockOf(map[string]uint32{"node-2": 1})
	if fromNode1.Compare(fromNode2) != vectorclock.Concurrent {
		t.Fatalf("test setup: expected independently-originated clocks to be Concurrent")
	}

	src1 := writeSST(t, memtable.Entry{Key: "foo", Item: itemWithClock("from-node1", fromNode1, "node-1")})
	src2 := writeSST(t, memtable.Entry{Key: "foo", Item: itemWithClock("from-node2", fromNode2, "node-2")})

	merged := mustMerge(t, src1, src2)

	if len(merged) != 2 {
		t.Fatalf("expected 2 genuine siblings, got %d: %v", len(merged), merged)
	}
	values := map[string]bool{}
	for _, e := range merged {
		if e.Key != "foo" {
			t.Errorf("expected every entry to be for key foo, got %q", e.Key)
		}
		values[e.Item.Value.(string)] = true
	}
	if !values["from-node1"] || !values["from-node2"] {
		t.Fatalf("expected siblings {from-node1, from-node2}, got %v", values)
	}

	// The merged output must round-trip through Write/GetAll with both
	// siblings intact — this is what compaction actually does with it.
	out, err := sstable.Write(t.TempDir(), merged)
	if err != nil {
		t.Fatalf("writing merged siblings failed: %v", err)
	}
	items, found, err := out.GetAll("foo")
	if err != nil || !found || len(items) != 2 {
		t.Fatalf("expected both siblings readable via GetAll after Write, got found=%v count=%d err=%v", found, len(items), err)
	}
}

func TestMergeEqualClocksKeepNewestSource(t *testing.T) {
	// StorageEngine overwrites don't have to advance the vector clock, so
	// two versions of a key can carry Equal clocks. Merge's contract is
	// that sources come newest first, so the newest write wins the tie.
	vc := clockOf(map[string]uint32{"node-1": 1})
	older := writeSST(t, memtable.Entry{Key: "k", Item: itemWithClock("older", vc, "node-1")})
	newer := writeSST(t, memtable.Entry{Key: "k", Item: itemWithClock("newer", clockOf(vc.Snapshot()), "node-1")})

	merged := mustMerge(t, newer, older)

	if len(merged) != 1 || merged[0].Item.Value != "newer" {
		t.Fatalf("expected the newest source's version to win an Equal-clock tie, got %v", merged)
	}
}

func TestMergeWithNoSourcesReturnsEmpty(t *testing.T) {
	merged, err := Merge(nil)
	if err != nil {
		t.Fatalf("expected nil error for no sources, got %v", err)
	}
	if len(merged) != 0 {
		t.Fatalf("expected empty result for no sources, got %v", merged)
	}
}

func smallSST(t *testing.T) *sstable.SSTable {
	t.Helper()
	sst := writeSST(t, memtable.Entry{Key: "k", Item: itemWithClock("v", clockOf(map[string]uint32{"node-1": 1}), "node-1")})
	if tierOf(sst.Size) != 0 {
		t.Fatalf("test setup: expected a small sstable in tier 0, got size %d", sst.Size)
	}
	return sst
}

func largeSST(t *testing.T) *sstable.SSTable {
	t.Helper()
	big := strings.Repeat("x", 5000)
	sst := writeSST(t, memtable.Entry{Key: "k", Item: itemWithClock(big, clockOf(map[string]uint32{"node-1": 1}), "node-1")})
	if tierOf(sst.Size) == 0 {
		t.Fatalf("test setup: expected a large sstable outside tier 0, got size %d", sst.Size)
	}
	return sst
}

func TestSelectTierReturnsNilBelowMinTierSize(t *testing.T) {
	sstables := make([]*sstable.SSTable, 0, minTierSize-1)
	for i := 0; i < minTierSize-1; i++ {
		sstables = append(sstables, smallSST(t))
	}

	selected, err := SelectTierForCompaction(sstables)
	if err != nil {
		t.Fatalf("SelectTierForCompaction failed: %v", err)
	}
	if selected != nil {
		t.Fatalf("expected no tier to qualify with %d sstables, got %d selected", len(sstables), len(selected))
	}

	selected, err = SelectTierForCompaction(nil)
	if err != nil || selected != nil {
		t.Fatalf("expected nil, nil for no sstables, got %v, %v", selected, err)
	}
}

func TestSelectTierIdentifiesQualifyingTier(t *testing.T) {
	// Newest first, as StorageEngine orders them: minTierSize fresh small files,
	// then one older, much larger (already-compacted-sized) file.
	var small []*sstable.SSTable
	for i := 0; i < minTierSize; i++ {
		small = append(small, smallSST(t))
	}
	large := largeSST(t)
	sstables := append(append([]*sstable.SSTable{}, small...), large)

	selected, err := SelectTierForCompaction(sstables)
	if err != nil {
		t.Fatalf("SelectTierForCompaction failed: %v", err)
	}
	if len(selected) != minTierSize {
		t.Fatalf("expected the %d small sstables to be selected, got %d", minTierSize, len(selected))
	}
	for i, sst := range selected {
		if sst != small[i] {
			t.Errorf("selected[%d]: expected small sstable %s (in newest-first order), got %s", i, small[i].ID, sst.ID)
		}
	}
}

func TestSelectTierIgnoresNonAdjacentSameTierFiles(t *testing.T) {
	// minTierSize small files in total, but split by a large file between
	// them in age order: merging across it would reorder versions relative
	// to it, so no run qualifies.
	sstables := []*sstable.SSTable{smallSST(t), smallSST(t), largeSST(t)}
	for i := 0; i < minTierSize-2; i++ {
		sstables = append(sstables, smallSST(t))
	}

	selected, err := SelectTierForCompaction(sstables)
	if err != nil {
		t.Fatalf("SelectTierForCompaction failed: %v", err)
	}
	if selected != nil {
		t.Fatalf("expected no contiguous run to qualify, got %d selected", len(selected))
	}
}
