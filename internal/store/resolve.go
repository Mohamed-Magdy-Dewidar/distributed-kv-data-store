package store

import "distributed-kv-datastore/internal/vectorclock"

// resolve folds an incoming write against the existing version set for a
// key. This is the Dynamo-style sibling logic: an incoming write can
// supersede existing items, be superseded by them, be a duplicate, or
// survive alongside them as a genuine concurrent sibling.
func resolve(existing []*DataItem, incoming *DataItem) []*DataItem {
	var survivors []*DataItem
	incomingSurvives := true

	for _, item := range existing {
		switch item.VectorClock.Compare(incoming.VectorClock) {
		case vectorclock.Concurrent:
			survivors = append(survivors, item)
		case vectorclock.After:
			incomingSurvives = false
			survivors = append(survivors, item)
		case vectorclock.Before:
			// existing is superseded by incoming — drop it
		case vectorclock.Equal:
			survivors = append(survivors, item)
			incomingSurvives = false
		}
	}

	if incomingSurvives {
		survivors = append(survivors, incoming)
	}
	return survivors
}

// unionVectorClock computes the causal union across a sibling set — the
// vector clock a client's next write should build on to dominate all of
// them at once.
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
