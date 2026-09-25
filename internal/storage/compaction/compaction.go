package compaction

import (
	"fmt"
	"sort"

	"distributed-kv-datastore/internal/storage/memtable"
	"distributed-kv-datastore/internal/storage/sstable"
	"distributed-kv-datastore/internal/store"
)

// minTierSize is the smallest number of SSTables in a size tier that
// triggers a compaction of that tier — matches the project's earlier
// "size-tiered: >= 4 SSTables of roughly similar size" decision.
const minTierSize = 4

// tierOf buckets an SSTable's on-disk size by order of magnitude: tier 0
// is under 1,000 bytes, tier 1 is 1,000–9,999, tier 2 is 10,000–99,999,
// and so on. Powers of 10 are coarse on purpose: a file only lands in the
// same tier as files within ~10x of its size, so one large,
// already-compacted file is never re-merged over and over with a stream
// of tiny new ones, while freshly flushed files (roughly memtable-sized)
// still group together reliably.
func tierOf(size int64) int {
	tier := 0
	for size >= 1000 {
		size /= 10
		tier++
	}
	return tier
}

// SelectTierForCompaction returns the SSTables to compact next, or nil if
// nothing qualifies. sstables must be ordered newest first
// (StorageEngine's order), and the result keeps that order.
//
// A candidate is a maximal run of *adjacent* SSTables (in age order) that
// share a size tier and number at least minTierSize. Adjacency is required
// for correctness, not tidiness: StorageEngine.Get resolves a key by the
// newest SSTable containing it, so merging same-tier files A and C while
// skipping B (between them in age, but in a different tier) would put the
// merged result on the wrong side of B for at least one of them — either
// hiding B's newer values behind C's older ones, or hiding A's newer
// values behind B's. A contiguous run can be replaced in place by its merge
// without changing what any Get returns.
//
// If several runs qualify, the longest wins (it removes the most files);
// ties go to the newest run, since new, small files are the ones that
// accumulate fastest.
//
// The error return is currently always nil; sizes come from
// SSTable.Size, which is populated at construction time without I/O.
func SelectTierForCompaction(sstables []*sstable.SSTable) ([]*sstable.SSTable, error) {
	var best []*sstable.SSTable

	for start := 0; start < len(sstables); {
		tier := tierOf(sstables[start].Size)
		end := start + 1
		for end < len(sstables) && tierOf(sstables[end].Size) == tier {
			end++
		}
		if run := sstables[start:end]; len(run) >= minTierSize && len(run) > len(best) {
			best = run
		}
		start = end
	}

	if best == nil {
		return nil, nil
	}
	return append([]*sstable.SSTable(nil), best...), nil // copy: don't alias the caller's slice
}

// Merge reads every entry from all of sources, resolves multi-version
// keys via store.MergeSiblings (never naive last-write-wins — mirrors
// internal/node/node.go's reconcileBucket pattern), and returns the
// result sorted by key, ready for sstable.Write. A key present in only
// one source passes through unchanged. A key present in multiple
// sources is folded pairwise through store.MergeSiblings; the result
// may still hold multiple *DataItem values for that key if they are
// genuinely Concurrent siblings — compaction does not force resolution,
// only merges what the existing causality logic can merge. Tombstones
// are ordinary versions and are always kept: a Node can't know whether
// every replica has seen a delete, so dropping one here could let an
// older value reappear.
//
// sources must be ordered newest first. Pairwise folding yields the same
// surviving set in any order (a version dominated by an already-dropped
// one is also dominated by whatever dropped it) with one exception:
// versions with Equal vector clocks, where resolve keeps whichever it saw
// first. StorageEngine writes aren't required to advance the clock, so
// Equal clocks are normal for overwrites — newest-first order makes the
// most recent write win those ties, matching what StorageEngine.Get
// returned before compaction. It also keeps the newest surviving sibling
// first, which is the one sstable.Get (and so StorageEngine.Get) returns.
//
// With no sources, Merge returns an empty result and a nil error.
func Merge(sources []*sstable.SSTable) ([]memtable.Entry, error) {
	byKey := make(map[string][]*store.DataItem)

	for _, src := range sources {
		entries, err := src.All()
		if err != nil {
			return nil, fmt.Errorf("compaction: read %s: %w", src.Path, err)
		}
		for _, e := range entries {
			byKey[e.Key] = store.MergeSiblings(byKey[e.Key], []*store.DataItem{e.Item})
		}
	}

	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var result []memtable.Entry
	for _, k := range keys {
		for _, item := range byKey[k] {
			result = append(result, memtable.Entry{Key: k, Item: item})
		}
	}

	return result, nil
}
