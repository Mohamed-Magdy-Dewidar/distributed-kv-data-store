package memtable

import (
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

	item, found := m.Get("foo")
	if !found {
		t.Fatal("expected to find foo")
	}
	if item.Value != "bar" {
		t.Errorf("expected value 'bar', got %v", item.Value)
	}
}

func TestGetMissingKey(t *testing.T) {
	m := New(1 << 20)
	_, found := m.Get("does-not-exist")
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
	item, found := m.Get("count")
	if !found || item.Value != 42 {
		t.Fatalf("expected to retrieve int value 42, got found=%v value=%v", found, item.Value)
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
	if _, found := m.Get("foo"); found {
		t.Error("expected foo to be gone after SnapshotAndClear")
	}

	// The table must immediately accept new writes after clearing.
	m.Put("new-key", sampleItem("new-value"))
	if _, found := m.Get("new-key"); !found {
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
			m.Get(key)
		}(i)
	}

	wg.Wait()
}
