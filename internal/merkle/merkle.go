package merkle

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"

	"distributed-kv-datastore/internal/store"
)

// Hash is a fixed-size digest.
type Hash [32]byte

// Tree is a flat, single-level Merkle tree: one hash per bucket, plus a
// root hash summarizing all of them. No internal/intermediate level yet —
// deliberate simplification; a real multi-level tree is a planned future
// step once this flat version is proven.
type Tree struct {
	NumBuckets   int
	BucketHashes map[int]Hash
	Root         Hash
}

// bucketFor deterministically maps a key to a bucket index in
// [0, numBuckets). Distinct from hashring's consistent-hash ring: this is
// a coarse, fixed partitioning used only to group keys for efficient
// diffing between two replicas, not to decide which nodes own a key.
func bucketFor(key string, numBuckets int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % numBuckets
}

// canonicalItemString produces a deterministic string representation of a
// single DataItem, used as an input to hashing. Decision: the hash covers
// Value, VectorClock (sorted, so map iteration order can't affect it),
// LastUpdatedBy, and IsDeleted — i.e. the full causal/version state, not
// just the current value. This means two replicas that hold the same
// value but arrived via different causal histories (different vector
// clocks) will correctly show up as divergent, which is the behavior
// anti-entropy needs: it must catch cases where histories disagree even
// if the "current" value looks the same.
func canonicalItemString(item *store.DataItem) string {
	valueBytes, err := json.Marshal(item.Value)
	if err != nil {
		// Value should always be JSON-marshalable in this project (see
		// internal/rpc/convert.go, which makes the same assumption for
		// the wire format) — treat a failure here as a real bug, not
		// something to silently paper over.
		panic(fmt.Sprintf("merkle: failed to marshal item value: %v", err))
	}

	vc := item.VectorClock.Snapshot()
	nodes := make([]string, 0, len(vc))
	for node := range vc {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)

	vcParts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		vcParts = append(vcParts, fmt.Sprintf("%s:%d", node, vc[node]))
	}

	return fmt.Sprintf("value=%s|vc={%s}|by=%s|deleted=%t",
		string(valueBytes), fmt.Sprintf("%v", vcParts), item.LastUpdatedBy, item.IsDeleted)
}

// hashBucketContents computes a single hash summarizing everything in one
// bucket. Keys are processed in sorted order, and each key's sibling set
// is itself sorted by its canonical string form, so the result is fully
// deterministic regardless of Go's randomized map iteration order.
func hashBucketContents(ds *store.DataStore, bucketIndex, numBuckets int) Hash {
	allKeys := ds.Keys()

	var bucketKeys []string
	for _, k := range allKeys {
		if bucketFor(k, numBuckets) == bucketIndex {
			bucketKeys = append(bucketKeys, k)
		}
	}
	sort.Strings(bucketKeys)

	h := sha256.New()
	for _, key := range bucketKeys {
		items, _ := ds.Get(key) // raw, tombstones included — anti-entropy must reconcile deletes too

		itemStrings := make([]string, 0, len(items))
		for _, item := range items {
			itemStrings = append(itemStrings, canonicalItemString(item))
		}
		sort.Strings(itemStrings) // siblings have no inherent order — sort for determinism

		h.Write([]byte(key))
		h.Write([]byte{0}) // separator, avoid key/value concatenation ambiguity
		for _, s := range itemStrings {
			h.Write([]byte(s))
			h.Write([]byte{0})
		}
	}

	var out Hash
	copy(out[:], h.Sum(nil))
	return out
}

// combineHashes deterministically combines a sequence of hashes into one
// summary hash. Reusable later for internal-node hashes once a
// multi-level tree is added.
func combineHashes(hashes []Hash) Hash {
	h := sha256.New()
	for _, hh := range hashes {
		h.Write(hh[:])
	}
	var out Hash
	copy(out[:], h.Sum(nil))
	return out
}

// Build constructs a full Tree over ds's current contents, partitioned
// into numBuckets fixed buckets.
func Build(ds *store.DataStore, numBuckets int) *Tree {
	t := &Tree{
		NumBuckets:   numBuckets,
		BucketHashes: make(map[int]Hash, numBuckets),
	}

	ordered := make([]Hash, numBuckets)
	for i := 0; i < numBuckets; i++ {
		h := hashBucketContents(ds, i, numBuckets)
		t.BucketHashes[i] = h
		ordered[i] = h // bucket-index order — deterministic regardless of map iteration
	}

	t.Root = combineHashes(ordered)
	return t
}

// DivergentBuckets compares two trees built with the same NumBuckets and
// returns the indices of every bucket whose hash disagrees.
func DivergentBuckets(a, b *Tree) []int {
	if a.NumBuckets != b.NumBuckets {
		panic("merkle: cannot compare trees built with different NumBuckets")
	}

	var diverged []int
	for i := 0; i < a.NumBuckets; i++ {
		if a.BucketHashes[i] != b.BucketHashes[i] {
			diverged = append(diverged, i)
		}
	}
	return diverged
}
