package main

import (
	"fmt"
	"sync"
)

type Relation int

const (
	Equal Relation = iota
	Before
	After
	Concurrent
)

type VectorClock struct {
	state        map[string]uint32
	LastModified int64
	mu           sync.Mutex
}

func NewVectorClock() *VectorClock {
	return &VectorClock{state: make(map[string]uint32)}
}

func (vc *VectorClock) Increment(nodeID string) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.state[nodeID]++
}

// Snapshot copies out the current state as a plain map — no mutex attached.
// This is the only method that touches vc.mu for reading.
func (vc *VectorClock) Snapshot() map[string]uint32 {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	snap := make(map[string]uint32, len(vc.state))
	for k, v := range vc.state {
		snap[k] = v
	}
	return snap
}

// CompareSnapshots is a pure function: no locks, no shared mutable state,
// just two plain maps in, one Relation out. Safe to call from anywhere,
// any number of times, concurrently, with no risk of deadlock.
func CompareSnapshots(this, other map[string]uint32) Relation {
	if len(this) != len(other) {
		panic("Vector clocks are not comparable, different number of nodes")
	}

	isThisGreater := false
	isOtherGreater := false

	for node, version := range this {
		if version > other[node] {
			isThisGreater = true
		} else if version < other[node] {
			isOtherGreater = true
		}
	}

	if isThisGreater && !isOtherGreater {
		return After
	}
	if !isThisGreater && isOtherGreater {
		return Before
	}
	if !isThisGreater && !isOtherGreater {
		return Equal
	}
	return Concurrent
}

// Compare is a convenience wrapper: snapshot both sides, then compare purely.
func (vc *VectorClock) Compare(other *VectorClock) Relation {
	return CompareSnapshots(vc.Snapshot(), other.Snapshot())
}
func buildVectorClockFromContext(context map[string]uint32, nodeID string) *VectorClock {
	vc := NewVectorClock()

	for node, version := range context {
		vc.state[node] = version
	}
	vc.Increment(nodeID)
	return vc
}

// if context is nil, we will use the union of the vector clocks of the existing items as the base for the new vector clock.
// This ensures that we are aware of all previous versions and can correctly determine the relationship between the incoming item and the existing items.
func unionVectorClock(items []*DataItem) map[string]uint32 {
	union := make(map[string]uint32)
	for _, item := range items {
		for node, version := range item.VectorClock.Snapshot() {
			if version > union[node] {
				union[node] = version
			}
		}
	}
	return union
}

type Node struct {
	id        string
	dataStore *DataStore
	address   string
}

func NewNode(id string, address string) *Node {
	return &Node{
		id:        id,
		dataStore: NewDataStore(id),
		address:   address,
	}
}

type DataItem struct {
	Value         any
	VectorClock   *VectorClock
	LastUpdatedBy string
	IsDeleted     bool
}

type DataStore struct {
	id    string
	store map[string][]*DataItem
	// Most of the time this slice has exactly one element (no conflict). It only grows past one when Concurrent writes are detected and haven't been reconciled yet.
	mu sync.Mutex
}

func NewDataStore(id string) *DataStore {
	return &DataStore{
		id:    id,
		store: make(map[string][]*DataItem),
	}
}

func get(ds *DataStore, key string) ([]*DataItem, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	items, exists := ds.store[key]
	if !exists {
		return nil, false
	}
	return items, len(items) > 0
}

func getLiveItems(ds *DataStore, key string) ([]*DataItem, bool) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	items, exists := ds.store[key]
	if !exists {
		return nil, false
	}

	var aliveItems []*DataItem
	for _, item := range items {
		if !item.IsDeleted {
			aliveItems = append(aliveItems, item)
		}
	}

	return aliveItems, len(aliveItems) > 0
}

func resolve(existing []*DataItem, incoming *DataItem) []*DataItem {
	var survivors []*DataItem
	incomingSurvives := true

	for _, item := range existing {
		relation := item.VectorClock.Compare(incoming.VectorClock)
		switch relation {

		case Concurrent:
			survivors = append(survivors, item)
		case After:
			incomingSurvives = false
			survivors = append(survivors, item)
		case Before:

		case Equal:
			survivors = append(survivors, item)
			incomingSurvives = false
		}
	}

	if incomingSurvives {
		survivors = append(survivors, incoming)
	}

	return survivors
}

func Put(ds *DataStore, key string, value any, context map[string]uint32) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	existing, ok := ds.store[key]

	base := context
	if base == nil && ok {
		base = unionVectorClock(existing)
	}

	incoming := &DataItem{
		Value:         value,
		VectorClock:   buildVectorClockFromContext(base, ds.id),
		LastUpdatedBy: ds.id,
	}

	if !ok {
		ds.store[key] = []*DataItem{incoming}
		return
	}
	ds.store[key] = resolve(existing, incoming)
}

// treate del as just a anthor write operation howver we will set the IsDeleted flag to true and the Value to nil.
func Delete(ds *DataStore, key string, context map[string]uint32) (bool, string) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	existing, ok := ds.store[key]
	if !ok {
		return false, "Key not found"
	}

	base := context
	if base == nil {
		base = unionVectorClock(existing)
	}

	tombstone := &DataItem{
		Value:         nil,
		VectorClock:   buildVectorClockFromContext(base, ds.id),
		LastUpdatedBy: ds.id,
		IsDeleted:     true,
	}

	ds.store[key] = resolve(existing, tombstone)
	return true, "Key deleted"
}

func main() {
	ds := NewDataStore("node-1")

	fmt.Println("--- Basic Put/Get ---")
	Put(ds, "foo", "bar", nil)
	items, _ := get(ds, "foo")
	fmt.Printf("get(foo) -> value=%v vc=%v\n", items[0].Value, items[0].VectorClock.Snapshot())
	// Expect: value=bar, vc={node-1:1}

	fmt.Println("\n--- Update existing key (sequential, same node) ---")
	Put(ds, "foo", "baz", nil)
	items, _ = get(ds, "foo")
	fmt.Printf("get(foo) -> count=%d value=%v vc=%v\n", len(items), items[0].Value, items[0].VectorClock.Snapshot())
	// Expect: count=1 (baz cleanly supersedes bar, not a sibling), vc={node-1:2}
	// If this prints count=2 or value=bar, the nil-context-union fix regressed.

	fmt.Println("\n--- Get missing key ---")
	_, ok := get(ds, "does-not-exist")
	fmt.Printf("get(does-not-exist) -> exists=%v\n", ok)
	// Expect: false

	fmt.Println("\n--- Delete via tombstone, then plain get vs getLiveItems ---")
	deleted, msg := Delete(ds, "foo", nil)
	fmt.Printf("Delete(foo) -> %v %q\n", deleted, msg)

	rawItems, rawOk := get(ds, "foo")
	fmt.Printf("get(foo) [raw]      -> exists=%v count=%d isDeleted=%v\n", rawOk, len(rawItems), rawItems[0].IsDeleted)
	// Expect: exists=true, count=1, isDeleted=true — the tombstone is still visible to raw get

	liveItems, liveOk := getLiveItems(ds, "foo")
	fmt.Printf("getLiveItems(foo)    -> exists=%v count=%d\n", liveOk, len(liveItems))
	// Expect: exists=false, count=0 — this is where "deleted" is actually surfaced

	fmt.Println("\n--- Delete missing key ---")
	deleted, msg = Delete(ds, "never-existed", nil)
	fmt.Printf("Delete(never-existed) -> %v %q\n", deleted, msg)
	// Expect: false, "Key not found"

	fmt.Println("\n--- Concurrent writers on the same key (same node) ---")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			Put(ds, "counter", n, nil)
		}(i)
	}
	wg.Wait()
	items, _ = get(ds, "counter")
	fmt.Printf("get(counter) -> count=%d value=%v vc=%v\n", len(items), items[0].Value, items[0].VectorClock.Snapshot())
	// Expect: count=1 (all 50 writes serialize through the same node, no real conflict),
	// vc={node-1:50} — if count!=1 here, Put's own-node serialization is broken.

	fmt.Println("\n--- Simulated cross-node conflict: real siblings ---")
	// Simulate what replication would eventually do: two nodes independently
	// write to the same key from the SAME starting point, then their results
	// land in one store without either knowing about the other's write.
	base := map[string]uint32{"node-1": 1, "node-2": 1} // both start in sync
	itemA := &DataItem{
		Value:         "value-from-node-1",
		VectorClock:   buildVectorClockFromContext(base, "node-1"),
		LastUpdatedBy: "node-1",
	}
	itemB := &DataItem{
		Value:         "value-from-node-2",
		VectorClock:   buildVectorClockFromContext(base, "node-2"),
		LastUpdatedBy: "node-2",
	}
	fmt.Printf("itemA.vc=%v itemB.vc=%v relation=%v (expect Concurrent=%v)\n",
		itemA.VectorClock.Snapshot(), itemB.VectorClock.Snapshot(),
		itemA.VectorClock.Compare(itemB.VectorClock), Concurrent)

	conflicted := resolve([]*DataItem{itemA}, itemB)
	fmt.Printf("resolve(A, B) -> siblings count=%d\n", len(conflicted))
	for i, s := range conflicted {
		fmt.Printf("  sibling[%d]: value=%v vc=%v\n", i, s.Value, s.VectorClock.Snapshot())
	}
	// Expect: count=2 — both A and B survive as genuine siblings

	fmt.Println("\n--- Client resolves the conflict: merged write dominates both siblings ---")
	mergedContext := unionVectorClock(conflicted) // {node-1:2, node-2:2}
	fmt.Printf("union context for resolution = %v\n", mergedContext)
	resolvedItem := &DataItem{
		Value:         "client-merged-value",
		VectorClock:   buildVectorClockFromContext(mergedContext, "node-1"),
		LastUpdatedBy: "node-1",
	}
	final := resolve(conflicted, resolvedItem)
	fmt.Printf("resolve(siblings, mergedWrite) -> count=%d\n", len(final))
	for i, s := range final {
		fmt.Printf("  final[%d]: value=%v vc=%v\n", i, s.Value, s.VectorClock.Snapshot())
	}
	// Expect: count=1 — the merged write's vc dominates both prior siblings,
	// collapsing the conflict back to a single value.

	fmt.Println("\n--- Vector clock comparison sanity checks ---")
	a := NewVectorClock()
	a.Increment("s0")
	b := NewVectorClock()
	b.Increment("s0")
	fmt.Printf("Equal check    -> %v (expect %v)\n", a.Compare(b), Equal)

	a.Increment("s0")
	fmt.Printf("After check    -> %v (expect %v)\n", a.Compare(b), After)

	x := &VectorClock{state: map[string]uint32{"s0": 1, "s1": 2}}
	y := &VectorClock{state: map[string]uint32{"s0": 2, "s1": 1}}
	for i := 0; i < 5; i++ {
		fmt.Printf("Concurrent check run %d -> %v (expect %v)\n", i, x.Compare(y), Concurrent)
	}
}
