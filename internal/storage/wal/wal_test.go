package wal

import (
	"os"
	"path/filepath"
	"testing"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/vectorclock"
)

func tempWALPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.wal")
}

func sampleEntry(key, value string) Entry {
	vc := vectorclock.New()
	vc.Increment("node-1")
	return Entry{
		Key: key,
		Item: &model.DataItem{
			Value:         value,
			VectorClock:   vc,
			LastUpdatedBy: "node-1",
		},
	}
}

func TestAppendThenReplayReturnsEntriesInOrder(t *testing.T) {
	path := tempWALPath(t)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	entries := []Entry{
		sampleEntry("foo", "1"),
		sampleEntry("bar", "2"),
		sampleEntry("baz", "3"),
	}
	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatalf("Append(%q) failed: %v", e.Key, err)
		}
	}

	got, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}
	for i, e := range entries {
		if got[i].Key != e.Key {
			t.Errorf("entry %d: expected key %q, got %q", i, e.Key, got[i].Key)
		}
		if got[i].Item.Value != e.Item.Value {
			t.Errorf("entry %d: expected value %v, got %v", i, e.Item.Value, got[i].Item.Value)
		}
	}
}

func TestReplaySurvivesProcessRestart(t *testing.T) {
	// Simulates a real crash-recovery scenario: write, close the file
	// handle entirely (as if the process exited), then Open a fresh WAL
	// instance against the same path and Replay — proving durability
	// doesn't depend on any in-memory state surviving.
	path := tempWALPath(t)

	w1, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := w1.Append(sampleEntry("foo", "bar")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("re-Open failed: %v", err)
	}
	defer w2.Close()

	got, err := w2.Replay()
	if err != nil {
		t.Fatalf("Replay after reopen failed: %v", err)
	}
	if len(got) != 1 || got[0].Key != "foo" {
		t.Fatalf("expected [foo] after reopening, got %v", got)
	}
}

func TestReplayStopsCleanlyAtTornTrailingRecord(t *testing.T) {
	// This is the core crash-safety proof: write two good entries, then
	// manually corrupt/truncate the file to simulate a crash mid-Append
	// on a third entry, and confirm Replay returns exactly the two good
	// entries with no error — not a failure, not a partial/garbage
	// third entry.
	path := tempWALPath(t)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := w.Append(sampleEntry("foo", "1")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := w.Append(sampleEntry("bar", "2")); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Simulate a torn write: append a plausible-looking but incomplete
	// header (claims a large payload that was never actually written).
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to reopen for corruption: %v", err)
	}
	torn := make([]byte, headerSize)
	torn[0] = 0xFF // absurd length in the header, no payload follows
	torn[1] = 0xFF
	if _, err := f.Write(torn); err != nil {
		t.Fatalf("failed to write torn record: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close corrupted file: %v", err)
	}

	w2, err := Open(path)
	if err != nil {
		t.Fatalf("re-Open failed: %v", err)
	}
	defer w2.Close()

	got, err := w2.Replay()
	if err != nil {
		t.Fatalf("expected Replay to succeed despite a torn trailing record, got error: %v", err)
	}
	if len(got) != 2 || got[0].Key != "foo" || got[1].Key != "bar" {
		t.Fatalf("expected exactly [foo, bar] (torn record ignored), got %v", got)
	}
}

func TestReplayOnEmptyFileReturnsNoEntries(t *testing.T) {
	path := tempWALPath(t)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	got, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay on empty file failed: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no entries from empty file, got %v", got)
	}
}

func TestAppendAfterReplayContinuesCorrectly(t *testing.T) {
	// Proves Replay's end-of-file reseek actually works: append, replay,
	// then append again and replay again — the second Append must not
	// overwrite or corrupt what replay already validated.
	path := tempWALPath(t)

	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer w.Close()

	if err := w.Append(sampleEntry("foo", "1")); err != nil {
		t.Fatalf("first Append failed: %v", err)
	}
	if _, err := w.Replay(); err != nil {
		t.Fatalf("first Replay failed: %v", err)
	}
	if err := w.Append(sampleEntry("bar", "2")); err != nil {
		t.Fatalf("second Append failed: %v", err)
	}

	got, err := w.Replay()
	if err != nil {
		t.Fatalf("second Replay failed: %v", err)
	}
	if len(got) != 2 || got[0].Key != "foo" || got[1].Key != "bar" {
		t.Fatalf("expected [foo, bar] after Append-Replay-Append-Replay, got %v", got)
	}
}
