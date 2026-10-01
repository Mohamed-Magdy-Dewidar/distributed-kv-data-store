package merkle

import (
	"errors"
	"testing"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/store"
	"distributed-kv-datastore/internal/storetest"
)

const testNumBuckets = 16 // satisfies 4^k (k=2)

func mustBuild(t *testing.T, ds *store.DataStore, numBuckets int) *Tree {
	t.Helper()
	tree, err := Build(ds, numBuckets)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	return tree
}

func TestIdenticalDataProducesIdenticalTrees(t *testing.T) {
	dsA := storetest.NewStore(t, "node-1")
	dsB := storetest.NewStore(t, "node-1") // same node ID: identical vector clocks on identical writes

	for i := 0; i < 30; i++ {
		key := keyN(i)
		dsA.Put(key, "value-"+key, nil)
		dsB.Put(key, "value-"+key, nil)
	}

	treeA := mustBuild(t, dsA, testNumBuckets)
	treeB := mustBuild(t, dsB, testNumBuckets)

	if treeA.Nodes[0] != treeB.Nodes[0] {
		t.Fatalf("expected identical root hashes for identical data, got %x vs %x", treeA.Nodes[0], treeB.Nodes[0])
	}
	for i := range treeA.Nodes {
		if treeA.Nodes[i] != treeB.Nodes[i] {
			t.Errorf("node index %d: hashes differ despite identical data", i)
		}
	}
}

func TestSingleChangedKeyOnlyAffectsItsOwnBucket(t *testing.T) {
	dsA := storetest.NewStore(t, "node-1")
	dsB := storetest.NewStore(t, "node-1")

	for i := 0; i < 30; i++ {
		key := keyN(i)
		dsA.Put(key, "value-"+key, nil)
		dsB.Put(key, "value-"+key, nil)
	}

	changedKey := keyN(5)
	dsB.Put(changedKey, "a-different-value", nil)

	treeA := mustBuild(t, dsA, testNumBuckets)
	treeB := mustBuild(t, dsB, testNumBuckets)

	if treeA.Nodes[0] == treeB.Nodes[0] {
		t.Fatal("expected root hashes to differ after changing one key")
	}

	diverged := DivergentBuckets(treeA, treeB)
	changedBucket := BucketFor(changedKey, testNumBuckets)

	if len(diverged) != 1 || diverged[0] != changedBucket {
		t.Fatalf("expected exactly bucket %d to diverge, got %v", changedBucket, diverged)
	}

	for i := 0; i < testNumBuckets; i++ {
		if i == changedBucket {
			continue
		}
		idx := leafIndex(i, testNumBuckets)
		if treeA.Nodes[idx] != treeB.Nodes[idx] {
			t.Errorf("bucket %d: expected leaf hash to stay identical, but it changed too", i)
		}
	}
}

// TestTwoDivergentLeavesInDifferentSubtreesAreBothFound is the case the
// flat-tree tests couldn't exercise: it forces DivergentBuckets to recurse
// into BOTH children of the root (not just one), proving the walk doesn't
// stop after finding the first mismatch and correctly explores every
// subtree whose hash disagrees, independently.
func TestTwoDivergentLeavesInDifferentSubtreesAreBothFound(t *testing.T) {
	dsA := storetest.NewStore(t, "node-1")
	dsB := storetest.NewStore(t, "node-1")

	for i := 0; i < 30; i++ {
		key := keyN(i)
		dsA.Put(key, "value-"+key, nil)
		dsB.Put(key, "value-"+key, nil)
	}

	// Find two keys whose buckets land in different halves of the tree
	// (bucket index < testNumBuckets/2 vs >= testNumBuckets/2), so their
	// divergence is guaranteed to require recursing into both of the
	// root's children, not just one.
	var keyLow, keyHigh string
	for i := 0; i < 1000; i++ {
		k := keyN(1000 + i)
		b := BucketFor(k, testNumBuckets)
		if b < testNumBuckets/2 && keyLow == "" {
			keyLow = k
		}
		if b >= testNumBuckets/2 && keyHigh == "" {
			keyHigh = k
		}
		if keyLow != "" && keyHigh != "" {
			break
		}
	}
	if keyLow == "" || keyHigh == "" {
		t.Fatal("failed to find probe keys in both halves of the bucket space")
	}

	dsB.Put(keyLow, "changed-low", nil)
	dsB.Put(keyHigh, "changed-high", nil)

	treeA := mustBuild(t, dsA, testNumBuckets)
	treeB := mustBuild(t, dsB, testNumBuckets)

	diverged := DivergentBuckets(treeA, treeB)
	wantLow := BucketFor(keyLow, testNumBuckets)
	wantHigh := BucketFor(keyHigh, testNumBuckets)

	if len(diverged) != 2 {
		t.Fatalf("expected exactly 2 divergent buckets, got %v", diverged)
	}
	got := map[int]bool{diverged[0]: true, diverged[1]: true}
	if !got[wantLow] || !got[wantHigh] {
		t.Fatalf("expected buckets {%d, %d} to diverge, got %v", wantLow, wantHigh, diverged)
	}
}

func TestDivergentBucketsEmptyForIdenticalTrees(t *testing.T) {
	dsA := storetest.NewStore(t, "node-1")
	dsB := storetest.NewStore(t, "node-1")

	dsA.Put("foo", "bar", nil)
	dsB.Put("foo", "bar", nil)

	treeA := mustBuild(t, dsA, testNumBuckets)
	treeB := mustBuild(t, dsB, testNumBuckets)

	if diverged := DivergentBuckets(treeA, treeB); len(diverged) != 0 {
		t.Errorf("expected no divergent buckets, got %v", diverged)
	}
}

func TestDivergentBucketsPanicsOnMismatchedNumBuckets(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected a panic when comparing trees with different NumBuckets")
		}
	}()

	dsA := storetest.NewStore(t, "node-1")
	dsA.Put("foo", "bar", nil)

	treeA := mustBuild(t, dsA, 16)
	treeB := mustBuild(t, dsA, 4) // valid on its own (4 = 4^1), just mismatched with treeA

	DivergentBuckets(treeA, treeB)
}

func TestValidateBucketCountAcceptsPowersOfFour(t *testing.T) {
	for _, n := range []int{1, 4, 16, 64, 256} {
		if err := validateBucketCount(n); err != nil {
			t.Errorf("expected %d to be valid (4^k), got error: %v", n, err)
		}
	}
}

func TestValidateBucketCountRejectsNonPowersOfFour(t *testing.T) {
	// 8 and 32 are powers of 2 but NOT powers of 4 (odd exponent) — the
	// specific case this project's constraint is meant to catch, distinct
	// from just "not a power of 2" (e.g. 15, 0, -1).
	for _, n := range []int{0, -1, 3, 8, 15, 32} {
		if err := validateBucketCount(n); err == nil {
			t.Errorf("expected %d to be rejected, got no error", n)
		}
	}
}

func TestBuildPanicsOnInvalidBucketCount(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected Build to panic on an invalid numBuckets")
		}
	}()

	ds := storetest.NewStore(t, "node-1")
	Build(ds, 8) // power of 2, but not power of 4 — should be rejected
}

var errDisk = errors.New("disk on fire")

// brokenPersister lists keys (or fails to, when keysErr is set) but fails
// every read of a key's versions.
type brokenPersister struct {
	keys    []string
	keysErr error
}

func (brokenPersister) Put(string, *model.DataItem) error { return errDisk }
func (brokenPersister) GetAll(string) ([]*model.DataItem, bool, error) {
	return nil, false, errDisk
}
func (brokenPersister) Restore(string, []*model.DataItem) error { return errDisk }
func (p brokenPersister) Keys() ([]string, error)               { return p.keys, p.keysErr }

// TestBuildReturnsStoreErrors: a store read failure — listing keys, or
// reading one key's versions — must fail Build, not be hashed as if the
// store (or the key) were empty.
func TestBuildReturnsStoreErrors(t *testing.T) {
	for name, p := range map[string]brokenPersister{
		"keys fail": {keysErr: errDisk},
		"get fails": {keys: []string{"k"}},
	} {
		t.Run(name, func(t *testing.T) {
			tree, err := Build(store.NewDataStoreWithPersister("node-1", p), testNumBuckets)
			if !errors.Is(err, errDisk) {
				t.Fatalf("expected Build to return the store error, got tree=%v err=%v", tree, err)
			}
		})
	}
}

func TestBucketForIsDeterministic(t *testing.T) {
	first := BucketFor("stable-key", testNumBuckets)
	for i := 0; i < 20; i++ {
		if got := BucketFor("stable-key", testNumBuckets); got != first {
			t.Fatalf("run %d: bucketFor returned %d, expected consistently %d", i, got, first)
		}
	}
}

func TestBucketForDistributesKeysReasonably(t *testing.T) {
	counts := make(map[int]int)
	const sampleSize = 1000
	for i := 0; i < sampleSize; i++ {
		b := BucketFor(keyN(i), testNumBuckets)
		counts[b]++
	}

	if len(counts) < testNumBuckets/2 {
		t.Errorf("expected keys spread across most of %d buckets, only hit %d", testNumBuckets, len(counts))
	}
	expected := sampleSize / testNumBuckets
	for b, c := range counts {
		if c == 0 || c > expected*5 {
			t.Errorf("bucket %d looks degenerate: %d keys (expected ~%d)", b, c, expected)
		}
	}
}

func keyN(i int) string {
	return "key-" + string(rune('a'+i%26)) + "-" + string(rune('0'+i%10)) + "-" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
