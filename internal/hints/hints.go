// Package hints is a durable store of hinted-handoff writes: versioned
// items a coordinator couldn't replicate to one of their replicas because
// that replica was unreachable, kept until they can be delivered to it.
//
// It runs on its own StorageEngine, separate from a node's real data, and
// relies on that engine's causal merge rather than on any bookkeeping of
// its own:
//
//   - Each hint is stored under target + sep + key, so every hint for one
//     target shares a prefix. Node IDs may not contain sep; keys may.
//   - The value is the hinted DataItem itself. Several hints for the same
//     target and key merge like any sibling set: a newer write replaces an
//     older one, and concurrent ones are kept side by side.
//   - The engine can't delete, so a delivered hint is retired by a marker
//     version: a DataItem whose clock is the union of the delivered items'
//     clocks plus a reserved entry (deliveredEntry). It's causally after
//     everything it retires, so the merge drops those items from every
//     layer, SSTables included — while a hint added later, which it doesn't
//     cover, survives beside it. Markers are never delivered, and never
//     removed: one stays per (target, key) ever hinted.
package hints

import (
	"fmt"
	"sort"
	"strings"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/storage/engine"
	"distributed-kv-datastore/internal/vectorclock"
	"distributed-kv-datastore/internal/versioning"
)

// sep separates the target node ID from the key in a hint's storage key.
const sep = "\x00"

// deliveredEntry is the vector-clock entry that makes an item a delivery
// marker. It starts with sep, so it can't be a node ID.
const deliveredEntry = sep + "delivered"

// Hint is one key's pending hints for a target: every version not yet
// delivered, newest first.
type Hint struct {
	Key   string
	Items []*model.DataItem
}

// Store holds hints durably. Safe for concurrent use: every write is a
// causal merge in the engine, so no two operations can undo each other.
type Store struct {
	engine *engine.StorageEngine
}

// Open opens (or creates) the hint store at dir, replaying anything it
// held before. maxMemtableBytes is its engine's flush threshold; opts
// configure that engine.
func Open(dir string, maxMemtableBytes int, opts ...engine.Option) (*Store, error) {
	e, err := engine.Open(dir, maxMemtableBytes, opts...)
	if err != nil {
		return nil, fmt.Errorf("hints: open store at %s: %w", dir, err)
	}
	return &Store{engine: e}, nil
}

func (s *Store) Close() error {
	return s.engine.Close()
}

func storageKey(target, key string) string {
	return target + sep + key
}

// splitKey reverses storageKey, splitting at the first sep: target can't
// contain one, key can.
func splitKey(stored string) (target, key string, ok bool) {
	return strings.Cut(stored, sep)
}

func isMarker(item *model.DataItem) bool {
	_, ok := item.VectorClock.Snapshot()[deliveredEntry]
	return ok
}

// Add durably records item as a hint for target under key. It merges with
// any hints already held for target and key.
func (s *Store) Add(target, key string, item *model.DataItem) error {
	if target == "" || strings.Contains(target, sep) {
		return fmt.Errorf("hints: invalid target node ID %q", target)
	}
	if isMarker(item) {
		return fmt.Errorf("hints: item for %q/%q carries the reserved delivery-marker clock entry", target, key)
	}
	if err := s.engine.Put(storageKey(target, key), item); err != nil {
		return fmt.Errorf("hints: add for %q/%q: %w", target, key, err)
	}
	return nil
}

// Targets returns every node ID the store has ever held a hint for, sorted.
// Some may have nothing pending any more; Pending tells.
func (s *Store) Targets() ([]string, error) {
	keys, err := s.engine.Keys()
	if err != nil {
		return nil, fmt.Errorf("hints: list keys: %w", err)
	}
	seen := make(map[string]bool)
	var targets []string
	for _, k := range keys {
		target, _, ok := splitKey(k)
		if ok && !seen[target] {
			seen[target] = true
			targets = append(targets, target)
		}
	}
	sort.Strings(targets)
	return targets, nil
}

// Pending returns target's undelivered hints, one Hint per key that has
// any, in key order.
func (s *Store) Pending(target string) ([]Hint, error) {
	keys, err := s.engine.Keys() // sorted
	if err != nil {
		return nil, fmt.Errorf("hints: list keys: %w", err)
	}
	var pending []Hint
	for _, k := range keys {
		t, key, ok := splitKey(k)
		if !ok || t != target {
			continue
		}
		items, _, err := s.engine.GetAll(k)
		if err != nil {
			return nil, fmt.Errorf("hints: read %q/%q: %w", target, key, err)
		}
		var live []*model.DataItem
		for _, item := range items {
			if !isMarker(item) {
				live = append(live, item)
			}
		}
		if len(live) > 0 {
			pending = append(pending, Hint{Key: key, Items: live})
		}
	}
	return pending, nil
}

// MarkDelivered retires delivered — hints for target and key that target
// has now durably accepted — by adding a marker causally after all of
// them. Hints for the same key that aren't in delivered and aren't older
// than one of them stay pending.
func (s *Store) MarkDelivered(target, key string, delivered []*model.DataItem) error {
	if len(delivered) == 0 {
		return nil
	}
	clock := versioning.UnionVectorClock(delivered)
	clock[deliveredEntry] = 1
	marker := &model.DataItem{
		VectorClock:   vectorclock.FromSnapshot(clock),
		LastUpdatedBy: deliveredEntry,
		IsDeleted:     true,
	}
	if err := s.engine.Put(storageKey(target, key), marker); err != nil {
		return fmt.Errorf("hints: mark %q/%q delivered: %w", target, key, err)
	}
	return nil
}
