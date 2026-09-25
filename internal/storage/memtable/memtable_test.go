package memtable

import (
	"reflect"
	"sync"
	"testing"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/vectorclock"
)

func sampleItem(value any) *model.DataItem {
	vc := vectorclock.New()
	vc.Increment("node-1")
	return &model.DataItem{
		Value:         value,
		VectorClock:   vc,
		LastUpdatedBy: "node-1",
	}
}

func TestPutThenGet(t *testing.T) {
	m := New(1 << 20) // 1MB, won't trigger flush in this test
	m.Put("foo", sampleItem("bar"))

	items, found := m.GetAll("foo")
	if !found {
		t.Fatal("expected to find foo")
	}
	if items[0].Value != "bar" {
		t.Errorf("expected value 'bar', got %v", items)
	}
}

func TestGetMissingKey(t *testing.T) {
	m := New(1 << 20)
	_, found := m.GetAll("does-not-exist")
	if found {
		t.Error("expected not found")
	}
}

// TestPutWithNonStringValueDoesNotPanic guards against assuming Value is
// always a string — Value is `any` throughout this project (ints are used
// in tests elsewhere), and size estimation must not assume a concrete type.
func TestPutWithNonStringValueDoesNotPanic(t *testing.T) {
	m := New(1 << 20)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Put panicked on a non-string value: %v", r)
		}
	}()

	m.Put("count", sampleItem(42))
	items, found := m.GetAll("count")
	if !found || items[0].Value != 42 {
		t.Fatalf("expected to retrieve int value 42, got found=%v value=%v", found, items)
	}
}

func TestOverwriteDoesNotLeakSizeAccounting(t *testing.T) {
	// Overwriting the same key repeatedly with same-sized values must not
	// cause sizeSoFar to grow unboundedly — only the key's current entry
	// should count.
	m := New(1 << 20)

	for i := 0; i < 1000; i++ {
		m.Put("stable-key", sampleItem("same-length-value"))
	}

	if m.Len() != 1 {
		t.Fatalf("expected exactly 1 entry after 1000 overwrites of the same key, got %d", m.Len())
	}

	singleEntrySize := estimatedSize("stable-key", sampleItem("same-length-value"))
	if m.sizeSoFar > singleEntrySize*2 {
		t.Errorf("sizeSoFar=%d looks like it's accumulating across overwrites, expected roughly %d", m.sizeSoFar, singleEntrySize)
	}
}

func TestPutReturnsTrueWhenThresholdCrossed(t *testing.T) {
	m := New(300) // large enough that one entry (~68 bytes) doesn't immediately cross it

	first := m.Put("a", sampleItem("x"))
	if first {
		t.Error("did not expect threshold crossed on first small insert")
	}

	var crossed bool
	for i := 0; i < 20; i++ {
		if m.Put(string(rune('b'+i)), sampleItem("some longer value here")) {
			crossed = true
			break
		}
	}
	if !crossed {
		t.Error("expected threshold to be crossed after enough inserts")
	}
}

func TestSnapshotAndClearReturnsSortedOrder(t *testing.T) {
	m := New(1 << 20)
	m.Put("charlie", sampleItem("3"))
	m.Put("alpha", sampleItem("1"))
	m.Put("bravo", sampleItem("2"))

	items := m.SnapshotAndClear()

	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(items))
	}
	wantOrder := []string{"alpha", "bravo", "charlie"}
	for i, want := range wantOrder {
		if items[i].Key != want {
			t.Errorf("position %d: expected key %q, got %q", i, want, items[i].Key)
		}
	}
}

func TestSnapshotAndClearResetsTable(t *testing.T) {
	m := New(1 << 20)
	m.Put("foo", sampleItem("bar"))

	m.SnapshotAndClear()

	if m.Len() != 0 {
		t.Errorf("expected empty table after SnapshotAndClear, got Len=%d", m.Len())
	}
	if _, found := m.GetAll("foo"); found {
		t.Error("expected foo to be gone after SnapshotAndClear")
	}

	// The table must immediately accept new writes after clearing.
	m.Put("new-key", sampleItem("new-value"))
	if _, found := m.GetAll("new-key"); !found {
		t.Error("expected table to accept writes immediately after SnapshotAndClear")
	}
}

// TestConcurrentPutAndGetDoesNotRace exercises the same concurrency
// discipline proven elsewhere in this project (DataStore, VectorClock) —
// many goroutines reading and writing simultaneously, checked under
// -race.
func TestConcurrentPutAndGetDoesNotRace(t *testing.T) {
	m := New(1 << 20)
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := string(rune('a' + i%26))
			m.Put(key, sampleItem(i))
		}(i)
	}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := string(rune('a' + i%26))
			m.GetAll(key)
		}(i)
	}

	wg.Wait()
}

func itemWithClock(value any, counts map[string]uint32) *model.DataItem {
	return &model.DataItem{
		Value:         value,
		VectorClock:   vectorclock.FromSnapshot(counts),
		LastUpdatedBy: "node-1",
	}
}

func values(items []*model.DataItem) []any {
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, item.Value)
	}
	return out
}

// TestConcurrentWritesBothSurviveNewestFirst is the bug this package used
// to have: a Concurrent write overwrote the sibling already in the table.
// Both must survive, newest first, in GetAll and in Snapshot, and both
// must count toward the flush threshold.
func TestConcurrentWritesBothSurviveNewestFirst(t *testing.T) {
	m := New(1 << 20)
	fromNode1 := itemWithClock("from-node-1", map[string]uint32{"node-1": 1})
	fromNode2 := itemWithClock("from-node-2", map[string]uint32{"node-2": 1})
	m.Put("k", fromNode1)
	m.Put("k", fromNode2)

	items, found := m.GetAll("k")
	if want := []any{"from-node-2", "from-node-1"}; !found || !reflect.DeepEqual(values(items), want) {
		t.Fatalf("expected both siblings newest first %v, got found=%v %v", want, found, values(items))
	}
	if m.Len() != 1 {
		t.Errorf("expected 1 key holding 2 siblings, got Len=%d", m.Len())
	}

	snap := m.Snapshot()
	if len(snap) != 2 || snap[0].Key != "k" || snap[1].Key != "k" ||
		snap[0].Item != fromNode2 || snap[1].Item != fromNode1 {
		t.Fatalf("expected Snapshot to hold one Entry per sibling, newest first, got %+v", snap)
	}

	if want := estimatedSize("k", fromNode1) + estimatedSize("k", fromNode2); m.sizeSoFar != want {
		t.Errorf("expected sizeSoFar to count both siblings (%d), got %d", want, m.sizeSoFar)
	}
}

func TestPutDropsSupersededSiblingsAndTheirSize(t *testing.T) {
	m := New(1 << 20)
	m.Put("k", itemWithClock("from-node-1", map[string]uint32{"node-1": 1}))
	m.Put("k", itemWithClock("from-node-2", map[string]uint32{"node-2": 1}))

	// A write that has seen both siblings supersedes both.
	resolved := itemWithClock("resolved", map[string]uint32{"node-1": 2, "node-2": 1})
	m.Put("k", resolved)

	items, _ := m.GetAll("k")
	if want := []any{"resolved"}; !reflect.DeepEqual(values(items), want) {
		t.Fatalf("expected only %v, got %v", want, values(items))
	}
	if want := estimatedSize("k", resolved); m.sizeSoFar != want {
		t.Errorf("expected sizeSoFar to drop the superseded siblings (%d), got %d", want, m.sizeSoFar)
	}
}

func TestPutOfCausallyOlderVersionIsDropped(t *testing.T) {
	m := New(1 << 20)
	m.Put("k", itemWithClock("new", map[string]uint32{"node-1": 2}))
	m.Put("k", itemWithClock("stale", map[string]uint32{"node-1": 1})) // e.g. delivered late by anti-entropy

	items, _ := m.GetAll("k")
	if want := []any{"new"}; !reflect.DeepEqual(values(items), want) {
		t.Fatalf("expected the stale write to be dropped, leaving %v, got %v", want, values(items))
	}
}

// TestPutBackOlderFoldsEverySiblingBehindExistingVersions mirrors a failed
// flush: every sibling from the frozen table comes back, behind the
// (newer) versions the active table already holds.
func TestPutBackOlderFoldsEverySiblingBehindExistingVersions(t *testing.T) {
	frozen := New(1 << 20)
	frozen.Put("k", itemWithClock("frozen-node-1", map[string]uint32{"node-1": 1}))
	frozen.Put("k", itemWithClock("frozen-node-2", map[string]uint32{"node-2": 1}))
	frozen.Put("only-frozen", itemWithClock("x", map[string]uint32{"node-1": 1}))

	active := New(1 << 20)
	activeItem := itemWithClock("active-node-3", map[string]uint32{"node-3": 1}) // Concurrent with both frozen siblings
	active.Put("k", activeItem)

	entries := frozen.Snapshot()
	active.PutBackOlder(entries)

	items, _ := active.GetAll("k")
	if want := []any{"active-node-3", "frozen-node-2", "frozen-node-1"}; !reflect.DeepEqual(values(items), want) {
		t.Fatalf("expected every sibling back, behind active's own, newest first: %v, got %v", want, values(items))
	}
	if items, found := active.GetAll("only-frozen"); !found || len(items) != 1 {
		t.Fatalf("expected a key only the frozen table held to come back, got found=%v %v", found, values(items))
	}

	want := estimatedSize("k", activeItem)
	for _, e := range entries {
		want += estimatedSize(e.Key, e.Item)
	}
	if active.sizeSoFar != want {
		t.Errorf("expected sizeSoFar %d after putting everything back, got %d", want, active.sizeSoFar)
	}
}

// TestPutBackOlderKeepsNewerVersionOnTiesAndDominance: the older versions
// go second in the fold, so an Equal-clock tie keeps the newer version
// already here and a dominated older version is dropped — the same answer
// StorageEngine.GetAll gave while they sat in separate tables.
func TestPutBackOlderKeepsNewerVersionOnTiesAndDominance(t *testing.T) {
	active := New(1 << 20)
	active.Put("k", itemWithClock("newer", map[string]uint32{"node-1": 2}))

	active.PutBackOlder([]Entry{
		{Key: "k", Item: itemWithClock("equal-but-older", map[string]uint32{"node-1": 2})},
		{Key: "k", Item: itemWithClock("dominated", map[string]uint32{"node-1": 1})},
	})

	items, _ := active.GetAll("k")
	if want := []any{"newer"}; !reflect.DeepEqual(values(items), want) {
		t.Fatalf("expected only %v to survive, got %v", want, values(items))
	}
}

func TestGetAllReturnsACopy(t *testing.T) {
	m := New(1 << 20)
	m.Put("k", itemWithClock("v", map[string]uint32{"node-1": 1}))

	items, _ := m.GetAll("k")
	items[0] = nil

	again, _ := m.GetAll("k")
	if again[0] == nil {
		t.Fatal("mutating GetAll's result must not change the table")
	}
}
