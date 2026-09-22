package merkle

import (
	"testing"

	"distributed-kv-datastore/internal/store"
)

const testNumBuckets = 16

func TestIdenticalDataProducesIdenticalTrees(t *testing.T) {
	dsA := store.NewDataStore("node-1")
	dsB := store.NewDataStore("node-1") // same node ID: identical vector clocks on identical writes

	for i := 0; i < 30; i++ {
		key := keyN(i)
		dsA.Put(key, "value-"+key, nil)
		dsB.Put(key, "value-"+key, nil)
	}

	treeA := Build(dsA, testNumBuckets)
	treeB := Build(dsB, testNumBuckets)

	if treeA.Root != treeB.Root {
		t.Fatalf("expected identical root hashes for identical data, got %x vs %x", treeA.Root, treeB.Root)
	}
	for i := 0; i < testNumBuckets; i++ {
		if treeA.BucketHashes[i] != treeB.BucketHashes[i] {
			t.Errorf("bucket %d: hashes differ despite identical data", i)
		}
	}
}

func TestSingleChangedKeyOnlyAffectsItsOwnBucket(t *testing.T) {
	dsA := store.NewDataStore("node-1")
	dsB := store.NewDataStore("node-1")

	for i := 0; i < 30; i++ {
		key := keyN(i)
		dsA.Put(key, "value-"+key, nil)
		dsB.Put(key, "value-"+key, nil)
	}

	changedKey := keyN(5)
	dsB.Put(changedKey, "a-different-value", nil)

	treeA := Build(dsA, testNumBuckets)
	treeB := Build(dsB, testNumBuckets)

	if treeA.Root == treeB.Root {
		t.Fatal("expected root hashes to differ after changing one key")
	}

	diverged := DivergentBuckets(treeA, treeB)
	changedBucket := bucketFor(changedKey, testNumBuckets)

	if len(diverged) != 1 || diverged[0] != changedBucket {
		t.Fatalf("expected exactly bucket %d to diverge, got %v", changedBucket, diverged)
	}

	for i := 0; i < testNumBuckets; i++ {
		if i == changedBucket {
			continue
		}
		if treeA.BucketHashes[i] != treeB.BucketHashes[i] {
			t.Errorf("bucket %d: expected to stay identical, but it changed too", i)
		}
	}
}

func TestDivergentBucketsEmptyForIdenticalTrees(t *testing.T) {
	dsA := store.NewDataStore("node-1")
	dsB := store.NewDataStore("node-1")

	dsA.Put("foo", "bar", nil)
	dsB.Put("foo", "bar", nil)

	treeA := Build(dsA, testNumBuckets)
	treeB := Build(dsB, testNumBuckets)

	if diverged := DivergentBuckets(treeA, treeB); len(diverged) != 0 {
		t.Errorf("expected no divergent buckets, got %v", diverged)
	}
}

func TestBucketForIsDeterministic(t *testing.T) {
	first := bucketFor("stable-key", testNumBuckets)
	for i := 0; i < 20; i++ {
		if got := bucketFor("stable-key", testNumBuckets); got != first {
			t.Fatalf("run %d: bucketFor returned %d, expected consistently %d", i, got, first)
		}
	}
}

func TestBucketForDistributesKeysReasonably(t *testing.T) {
	counts := make(map[int]int)
	const sampleSize = 1000
	for i := 0; i < sampleSize; i++ {
		b := bucketFor(keyN(i), testNumBuckets)
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
