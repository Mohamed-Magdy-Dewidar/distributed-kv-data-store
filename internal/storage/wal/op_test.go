package wal

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/vectorclock"
)

// oldFormatPutPayload is a put record exactly as binaries from before
// OpRestore wrote it: no Op, no Items.
const oldFormatPutPayload = `{"Key":"legacy","Value":"v1","VectorClock":{"node-1":1},"LastUpdatedBy":"node-1","IsDeleted":false}`

// rawRecord frames payload the way Append does: length, CRC32, payload.
func rawRecord(payload []byte) []byte {
	header := make([]byte, headerSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))
	return append(header, payload...)
}

func itemWithClock(value string, counts map[string]uint32) *model.DataItem {
	return &model.DataItem{Value: value, VectorClock: vectorclock.FromSnapshot(counts), LastUpdatedBy: "node-1"}
}

// TestRestoreEntriesRoundTripInOrder: restore entries, including one that
// removes the key (no Items), replay with their Op, Items and clocks
// intact, in order among ordinary puts.
func TestRestoreEntriesRoundTripInOrder(t *testing.T) {
	path := tempWALPath(t)
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	restored := []*model.DataItem{
		itemWithClock("b", map[string]uint32{"node-2": 1}),
		itemWithClock("a", map[string]uint32{"node-1": 1}),
	}
	written := []Entry{
		sampleEntry("k", "undo-me"),
		{Op: OpRestore, Key: "k", Items: restored},
		sampleEntry("gone", "undo-me"),
		{Op: OpRestore, Key: "gone"},
	}
	for _, e := range written {
		if err := w.Append(e); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}
	w.Close()

	w, err = Open(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer w.Close()
	got, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if len(got) != len(written) {
		t.Fatalf("expected %d entries, got %d", len(written), len(got))
	}
	for i, want := range written {
		g := got[i]
		if g.Op != want.Op || g.Key != want.Key || len(g.Items) != len(want.Items) || (g.Item == nil) != (want.Item == nil) {
			t.Fatalf("entry %d: expected op=%d key=%q items=%d hasItem=%v, got op=%d key=%q items=%d hasItem=%v",
				i, want.Op, want.Key, len(want.Items), want.Item != nil, g.Op, g.Key, len(g.Items), g.Item != nil)
		}
		for j, item := range want.Items {
			if g.Items[j].Value != item.Value || !reflect.DeepEqual(g.Items[j].VectorClock.Snapshot(), item.VectorClock.Snapshot()) {
				t.Fatalf("entry %d item %d: expected %v %v, got %v %v", i, j,
					item.Value, item.VectorClock.Snapshot(), g.Items[j].Value, g.Items[j].VectorClock.Snapshot())
			}
		}
	}
}

// TestOldPutRecordsReplayAsPutsAndKeepTheirShape: a record written before
// Op existed replays as a put, and a put written now is still encoded
// byte-for-byte the same way.
func TestOldPutRecordsReplayAsPutsAndKeepTheirShape(t *testing.T) {
	path := tempWALPath(t)
	if err := os.WriteFile(path, rawRecord([]byte(oldFormatPutPayload)), 0644); err != nil {
		t.Fatalf("writing old-format record: %v", err)
	}

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()
	got, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if len(got) != 1 || got[0].Op != OpPut || got[0].Key != "legacy" || got[0].Item == nil || got[0].Items != nil {
		t.Fatalf("expected one put entry for %q, got %+v", "legacy", got)
	}
	if got[0].Item.Value != "v1" || !reflect.DeepEqual(got[0].Item.VectorClock.Snapshot(), map[string]uint32{"node-1": 1}) {
		t.Fatalf("expected v1 {node-1:1}, got %v %v", got[0].Item.Value, got[0].Item.VectorClock.Snapshot())
	}

	rec, err := toRecord(Entry{Key: "legacy", Item: itemWithClock("v1", map[string]uint32{"node-1": 1})})
	if err != nil {
		t.Fatalf("toRecord failed: %v", err)
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	if string(payload) != oldFormatPutPayload {
		t.Fatalf("put record encoding changed:\n got  %s\n want %s", payload, oldFormatPutPayload)
	}
}

// TestReplayRejectsUnknownOpWithoutTruncating: a checksum-valid record
// with an op this binary doesn't know is not a torn tail. Replay must
// fail and leave it, and everything after it, on disk.
func TestReplayRejectsUnknownOpWithoutTruncating(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal_00000000000000000001.log")
	data := rawRecord([]byte(oldFormatPutPayload))
	data = append(data, rawRecord([]byte(`{"Key":"future","Op":7}`))...)
	data = append(data, rawRecord([]byte(oldFormatPutPayload))...)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("writing records: %v", err)
	}

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if _, err := w.Replay(); err == nil {
		t.Fatal("expected Replay to fail on an unknown op")
	}
	w.Close()
	if info, err := os.Stat(path); err != nil || info.Size() != int64(len(data)) {
		t.Fatalf("expected the file left at %d bytes, got %v (err %v)", len(data), info.Size(), err)
	}

	if _, _, err := OpenLog(dir); err == nil {
		t.Fatal("expected OpenLog to fail on an unknown op")
	}
}
