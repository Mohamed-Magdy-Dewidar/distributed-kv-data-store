package vectorclock

import "sync"

type Relation int

const (
	Equal Relation = iota
	Before
	After
	Concurrent
)

type VectorClock struct {
	state map[string]uint32
	mu    sync.Mutex
}

func New() *VectorClock {
	return &VectorClock{state: make(map[string]uint32)}
}

func (vc *VectorClock) Increment(nodeID string) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.state[nodeID]++
}

// Snapshot copies out the current state as a plain map — no mutex attached.
func (vc *VectorClock) Snapshot() map[string]uint32 {
	vc.mu.Lock()
	defer vc.mu.Unlock()

	snap := make(map[string]uint32, len(vc.state))
	for k, v := range vc.state {
		snap[k] = v
	}
	return snap
}

// CompareSnapshots is a pure function over the union of both sides' node
// keys, so two clocks that have never heard of each other's nodes yet are
// still comparable (a missing entry is implicitly 0).
func CompareSnapshots(this, other map[string]uint32) Relation {
	isThisGreater := false
	isOtherGreater := false

	seen := make(map[string]bool, len(this)+len(other))
	for node := range this {
		seen[node] = true
	}
	for node := range other {
		seen[node] = true
	}

	for node := range seen {
		if this[node] > other[node] {
			isThisGreater = true
		} else if this[node] < other[node] {
			isOtherGreater = true
		}
	}

	switch {
	case isThisGreater && !isOtherGreater:
		return After
	case !isThisGreater && isOtherGreater:
		return Before
	case !isThisGreater && !isOtherGreater:
		return Equal
	default:
		return Concurrent
	}
}

func (vc *VectorClock) Compare(other *VectorClock) Relation {
	return CompareSnapshots(vc.Snapshot(), other.Snapshot())
}

// BuildFromContext seeds a new VectorClock from context (causal history the
// caller already knows about), then increments nodeID's own entry on top.
func BuildFromContext(context map[string]uint32, nodeID string) *VectorClock {
	vc := New()
	for node, version := range context {
		vc.state[node] = version
	}
	vc.Increment(nodeID)
	return vc
}
