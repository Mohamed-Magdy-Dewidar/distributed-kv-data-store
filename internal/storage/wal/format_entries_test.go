package wal

import (
	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/vectorclock"
)

// formatEntries are the entries of testdata/format.wal, the on-disk format
// pinned as the WAL wrote it before batched writes existed: every op, a
// tombstone, siblings, an empty restore, and values JSON must escape.
func formatEntries() []Entry {
	return []Entry{
		{Key: "plain", Item: itemWithClock("v1", map[string]uint32{"node-1": 1})},
		{Key: "multi-clock", Item: &model.DataItem{
			Value:         "v2",
			VectorClock:   vectorclock.FromSnapshot(map[string]uint32{"node-2": 7, "node-1": 3, "node-3-a1b2c3d4e5f60718": 1}),
			LastUpdatedBy: "node-2",
		}},
		{Key: "tombstone", Item: &model.DataItem{
			VectorClock:   vectorclock.FromSnapshot(map[string]uint32{"node-1": 2}),
			LastUpdatedBy: "node-1",
			IsDeleted:     true,
		}},
		{Op: OpRestore, Key: "restored", Items: []*model.DataItem{
			itemWithClock("newer", map[string]uint32{"node-2": 1}),
			itemWithClock("older", map[string]uint32{"node-1": 1}),
		}},
		{Op: OpRestore, Key: "removed"},
		{Key: "escapes", Item: itemWithClock("quote \" backslash \\ newline \n tab \t <html> & ünïcødé 日本", map[string]uint32{"node-1": 1})},
		{Key: "", Item: itemWithClock("", map[string]uint32{})},
	}
}
