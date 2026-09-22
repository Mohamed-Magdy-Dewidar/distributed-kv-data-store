package merkle

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/bits"
	"sort"

	"distributed-kv-datastore/internal/store"
)

// Hash is a fixed-size digest.
type Hash [32]byte

// Tree is a complete binary tree stored as a flat, heap-indexed array —
// no pointers. Nodes[0] is the root; node i's children live at
// Nodes[2i+1] and Nodes[2i+2]. The final NumBuckets entries of Nodes are
// the leaves, one per bucket in bucket-index order.
//
// NumBuckets is required to be 4^k for some k >= 0 (1, 4, 16, 64, 256,
// ...) — i.e. a power of 2 whose exponent is itself even. This keeps the
// tree a clean, evenly-halving complete binary tree at every level with
// no odd-node special-casing.
type Tree struct {
	NumBuckets int
	Nodes      []Hash
}

// validateBucketCount enforces NumBuckets == 4^k. Checks: must be a power
// of two (n & (n-1) == 0), and its base-2 exponent must be even.
func validateBucketCount(numBuckets int) error {
	if numBuckets < 1 {
		return fmt.Errorf("merkle: numBuckets must be positive, got %d", numBuckets)
	}
	if numBuckets&(numBuckets-1) != 0 {
		return fmt.Errorf("merkle: numBuckets must be a power of 2, got %d", numBuckets)
	}
	exponent := bits.Len(uint(numBuckets)) - 1 // e.g. 16 -> exponent 4
	if exponent%2 != 0 {
		return fmt.Errorf("merkle: numBuckets must be 4^k (exponent of 2 must be even), got %d (exponent %d)", numBuckets, exponent)
	}
	return nil
}

// leafIndex returns the position in Nodes of bucket bucketIndex's leaf
// hash. For a complete binary tree with numBuckets leaves stored
// heap-indexed in a (2*numBuckets - 1)-length array, the leaves occupy
// the final numBuckets slots, starting at index numBuckets-1.
func leafIndex(bucketIndex, numBuckets int) int {
	return numBuckets - 1 + bucketIndex
}

// bucketFor deterministically maps a key to a bucket index in
// [0, numBuckets). Distinct from hashring's consistent-hash ring, which
// decides node ownership rather than comparison grouping.
func bucketFor(key string, numBuckets int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % numBuckets
}

// canonicalItemString produces a deterministic string representation of a
// single DataItem — Value, sorted VectorClock, LastUpdatedBy, IsDeleted —
// so two replicas with matching values but divergent causal histories are
// still correctly flagged as out of sync.
func canonicalItemString(item *store.DataItem) string {
	valueBytes, err := json.Marshal(item.Value)
	if err != nil {
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
// is sorted by its canonical string form, so the result is deterministic
// regardless of Go's randomized map iteration order.
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
		items, _ := ds.Get(key)

		itemStrings := make([]string, 0, len(items))
		for _, item := range items {
			itemStrings = append(itemStrings, canonicalItemString(item))
		}
		sort.Strings(itemStrings)

		h.Write([]byte(key))
		h.Write([]byte{0})
		for _, s := range itemStrings {
			h.Write([]byte(s))
			h.Write([]byte{0})
		}
	}

	var out Hash
	copy(out[:], h.Sum(nil))
	return out
}

// combineHashes deterministically combines two child hashes into one
// parent hash.
func combineHashes(left, right Hash) Hash {
	h := sha256.New()
	h.Write(left[:])
	h.Write(right[:])
	var out Hash
	copy(out[:], h.Sum(nil))
	return out
}

// Build constructs the full multi-level tree over ds's current contents.
// Leaves are computed first (per-bucket content hashes), then each
// internal level is built bottom-up by hashing pairs of children, up to
// the root at Nodes[0]. Panics if numBuckets doesn't satisfy
// validateBucketCount — a malformed bucket count is a programming error,
// not a runtime condition callers should need to handle gracefully.
func Build(ds *store.DataStore, numBuckets int) *Tree {
	if err := validateBucketCount(numBuckets); err != nil {
		panic(err)
	}

	nodeCount := 2*numBuckets - 1
	nodes := make([]Hash, nodeCount)

	// Leaves first.
	for b := 0; b < numBuckets; b++ {
		nodes[leafIndex(b, numBuckets)] = hashBucketContents(ds, b, numBuckets)
	}

	// Internal levels, bottom-up. The last internal index is numBuckets-2
	// (when numBuckets == 1, this range is empty and Nodes[0] is simply
	// the sole leaf, which is also the root).
	for i := numBuckets - 2; i >= 0; i-- {
		nodes[i] = combineHashes(nodes[2*i+1], nodes[2*i+2])
	}

	return &Tree{NumBuckets: numBuckets, Nodes: nodes}
}

// isLeafIndex reports whether nodeIndex refers to a leaf in a tree with
// numBuckets buckets.
func isLeafIndex(nodeIndex, numBuckets int) bool {
	return nodeIndex >= numBuckets-1
}

// bucketIndexOfLeaf converts a leaf's position in Nodes back to its
// bucket index — the inverse of leafIndex.
func bucketIndexOfLeaf(nodeIndex, numBuckets int) int {
	return nodeIndex - (numBuckets - 1)
}

// DivergentBuckets walks two fully-built trees (same NumBuckets),
// starting from the root, recursing only into subtrees whose hash
// disagrees, and returns the leaf bucket indices where they ultimately
// differ. Both trees must already be fully populated locally — this
// function makes no network calls; by the time it's invoked, a bulk
// fetch of the peer's tree has already completed (see project notes on
// "bulk fetch, local compare" vs. per-step recursive RPCs).
func DivergentBuckets(local, remote *Tree) []int {
	if local.NumBuckets != remote.NumBuckets {
		panic("merkle: cannot compare trees built with different NumBuckets")
	}

	var diverged []int
	walk(local, remote, 0, &diverged)
	return diverged
}

// walk is the recursive step: compare the hash at nodeIndex in both
// trees. If they match, the entire subtree is provably identical — stop,
// nothing beneath this point needs examining. If they differ and this is
// a leaf, record the divergent bucket. If they differ and this is an
// internal node, recurse into both children.
func walk(local, remote *Tree, nodeIndex int, diverged *[]int) {
	if local.Nodes[nodeIndex] == remote.Nodes[nodeIndex] {
		return
	}

	if isLeafIndex(nodeIndex, local.NumBuckets) {
		*diverged = append(*diverged, bucketIndexOfLeaf(nodeIndex, local.NumBuckets))
		return
	}

	walk(local, remote, 2*nodeIndex+1, diverged)
	walk(local, remote, 2*nodeIndex+2, diverged)
}
