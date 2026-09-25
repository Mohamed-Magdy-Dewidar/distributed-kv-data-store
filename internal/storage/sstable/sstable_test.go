package sstable

import (
	"os"
	"path/filepath"
	"testing"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/storage/memtable"
	"distributed-kv-datastore/internal/vectorclock"
)

func sampleEntry(key string, value any) memtable.Entry {
	vc := vectorclock.New()
	vc.Increment("node-1")
	return memtable.Entry{
		Key: key,
		Item: &model.DataItem{
			Value:         value,
			VectorClock:   vc,
			LastUpdatedBy: "node-1",
		},
	}
}

func TestWriteThenGetRoundTrips(t *testing.T) {
	dir := t.TempDir()
	entries := []memtable.Entry{
		sampleEntry("alpha", "1"),
		sampleEntry("bravo", "2"),
		sampleEntry("charlie", "3"),
	}

	sst, err := Write(dir, entries)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	item, found, err := sst.Get("bravo")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found || item.Value != "2" {
		t.Fatalf("expected bravo=2, got found=%v value=%v", found, item.Value)
	}
}

func TestGetMissingKeyWithinRange(t *testing.T) {
	dir := t.TempDir()
	sst, err := Write(dir, []memtable.Entry{
		sampleEntry("alpha", "1"),
		sampleEntry("charlie", "3"),
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	_, found, err := sst.Get("bravo") // between alpha and charlie, but never written
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if found {
		t.Error("expected bravo to be absent")
	}
}

func TestGetKeyOutsideRangeSkipsDiskEntirely(t *testing.T) {
	dir := t.TempDir()
	sst, err := Write(dir, []memtable.Entry{
		sampleEntry("bravo", "2"),
		sampleEntry("delta", "4"),
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	_, found, err := sst.Get("alpha") // before MinKey
	if err != nil {
		t.Fatalf("Get(before range) failed: %v", err)
	}
	if found {
		t.Error("expected alpha (before range) to be absent")
	}

	_, found, err = sst.Get("zulu") // after MaxKey
	if err != nil {
		t.Fatalf("Get(after range) failed: %v", err)
	}
	if found {
		t.Error("expected zulu (after range) to be absent")
	}
}

func TestWriteRejectsUnsortedEntries(t *testing.T) {
	dir := t.TempDir()
	_, err := Write(dir, []memtable.Entry{
		sampleEntry("charlie", "3"),
		sampleEntry("alpha", "1"), // out of order
	})
	if err == nil {
		t.Fatal("expected an error for unsorted entries")
	}
}

func TestWriteRejectsEmptyEntries(t *testing.T) {
	dir := t.TempDir()
	_, err := Write(dir, nil)
	if err == nil {
		t.Fatal("expected an error for empty entries")
	}
}

// TestNonStringValueRoundTrips guards against the same class of bug
// flagged elsewhere in this project: Value is `any`, and the SSTable
// format must not assume a concrete type.
func TestNonStringValueRoundTrips(t *testing.T) {
	dir := t.TempDir()
	sst, err := Write(dir, []memtable.Entry{sampleEntry("count", 42)})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	item, found, err := sst.Get("count")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	// Note: JSON round-trips numbers as float64 by default — this is
	// expected Go/JSON behavior, not a bug, but worth asserting
	// explicitly so it's a documented, known characteristic rather than
	// a surprise later.
	if !found {
		t.Fatal("expected count to be found")
	}
	if f, ok := item.Value.(float64); !ok || f != 42 {
		t.Errorf("expected value 42 (as float64 after JSON round-trip), got %v (%T)", item.Value, item.Value)
	}
}

func TestVectorClockRoundTrips(t *testing.T) {
	dir := t.TempDir()
	vc := vectorclock.New()
	vc.Increment("node-1")
	vc.Increment("node-1")
	vc.Increment("node-2")

	entry := memtable.Entry{
		Key: "foo",
		Item: &model.DataItem{
			Value:         "bar",
			VectorClock:   vc,
			LastUpdatedBy: "node-1",
		},
	}

	dirEntries := []memtable.Entry{entry}
	sst, err := Write(dir, dirEntries)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	item, found, err := sst.Get("foo")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("expected foo to be found")
	}

	gotSnap := item.VectorClock.Snapshot()
	wantSnap := vc.Snapshot()
	if len(gotSnap) != len(wantSnap) {
		t.Fatalf("vector clock size mismatch: got %v, want %v", gotSnap, wantSnap)
	}
	for node, version := range wantSnap {
		if gotSnap[node] != version {
			t.Errorf("vector clock entry %q: got %d, want %d", node, gotSnap[node], version)
		}
	}
}

func TestTombstoneRoundTrips(t *testing.T) {
	dir := t.TempDir()
	entry := sampleEntry("deleted-key", nil)
	entry.Item.IsDeleted = true

	sst, err := Write(dir, []memtable.Entry{entry})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	item, found, err := sst.Get("deleted-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Fatal("expected the tombstone record itself to be found (raw, like store.Get)")
	}
	if !item.IsDeleted {
		t.Error("expected IsDeleted to be true")
	}
}

func TestOpenExistingSSTable(t *testing.T) {
	dir := t.TempDir()
	entries := []memtable.Entry{
		sampleEntry("alpha", "1"),
		sampleEntry("bravo", "2"),
	}
	written, err := Write(dir, entries)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Simulate a process restart: Open a fresh SSTable handle against the
	// same file, independent of the in-memory `written` value.
	reopened, err := Open(written.Path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	item, found, err := reopened.Get("alpha")
	if err != nil {
		t.Fatalf("Get after reopen failed: %v", err)
	}
	if !found || item.Value != "1" {
		t.Fatalf("expected alpha=1 after reopen, got found=%v value=%v", found, item.Value)
	}
	if reopened.MinKey != "alpha" || reopened.MaxKey != "bravo" {
		t.Errorf("expected MinKey/MaxKey alpha/bravo after reopen, got %s/%s", reopened.MinKey, reopened.MaxKey)
	}
}

func TestAllReturnsEverythingInSortedOrder(t *testing.T) {
	dir := t.TempDir()
	entries := []memtable.Entry{
		sampleEntry("alpha", "1"),
		sampleEntry("bravo", "2"),
		sampleEntry("charlie", "3"),
	}
	sst, err := Write(dir, entries)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	all, err := sst.All()
	if err != nil {
		t.Fatalf("All failed: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(all))
	}
	for i, want := range []string{"alpha", "bravo", "charlie"} {
		if all[i].Key != want {
			t.Errorf("position %d: expected key %q, got %q", i, want, all[i].Key)
		}
	}
}

func TestNoTmpFileLeftBehindAfterSuccessfulWrite(t *testing.T) {
	// Confirms atomic publication actually cleans up: only a .sst file
	// should exist afterward, never a lingering .tmp.
	dir := t.TempDir()
	sst, err := Write(dir, []memtable.Entry{sampleEntry("foo", "bar")})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("expected no .tmp files left behind, found %v", matches)
	}

	sstMatches, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}
	if len(sstMatches) != 1 || sstMatches[0] != sst.Path {
		t.Errorf("expected exactly one .sst file at %s, found %v", sst.Path, sstMatches)
	}
}

func TestGetAllReturnsMultipleSiblingsForSameKey(t *testing.T) {
	dir := t.TempDir()

	vcA := vectorclock.New()
	vcA.Increment("node-1")
	vcB := vectorclock.New()
	vcB.Increment("node-2")

	// Two genuinely concurrent siblings for the same key, written
	// adjacently since Write requires sorted input and both share a key.
	entries := []memtable.Entry{
		{Key: "contested", Item: &model.DataItem{Value: "from-node-1", VectorClock: vcA, LastUpdatedBy: "node-1"}},
		{Key: "contested", Item: &model.DataItem{Value: "from-node-2", VectorClock: vcB, LastUpdatedBy: "node-2"}},
		sampleEntry("zzz-other-key", "unrelated"),
	}

	sst, err := Write(dir, entries)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	items, found, err := sst.GetAll("contested")
	if err != nil {
		t.Fatalf("GetAll failed: %v", err)
	}
	if !found || len(items) != 2 {
		t.Fatalf("expected 2 siblings for 'contested', got found=%v count=%d", found, len(items))
	}

	values := map[string]bool{}
	for _, item := range items {
		values[item.Value.(string)] = true
	}
	if !values["from-node-1"] || !values["from-node-2"] {
		t.Fatalf("expected both sibling values present, got %v", values)
	}
}

func TestGetReturnsOneOfMultipleSiblingsAsConvenienceWrapper(t *testing.T) {
	dir := t.TempDir()
	vcA := vectorclock.New()
	vcA.Increment("node-1")
	vcB := vectorclock.New()
	vcB.Increment("node-2")

	entries := []memtable.Entry{
		{Key: "contested", Item: &model.DataItem{Value: "a", VectorClock: vcA, LastUpdatedBy: "node-1"}},
		{Key: "contested", Item: &model.DataItem{Value: "b", VectorClock: vcB, LastUpdatedBy: "node-2"}},
	}

	sst, err := Write(dir, entries)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	item, found, err := sst.Get("contested")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found || item == nil {
		t.Fatal("expected Get to return one sibling, not fail")
	}
}

func TestSizeMatchesFileOnDiskAfterWriteAndOpen(t *testing.T) {
	dir := t.TempDir()
	sst, err := WriteWithID(dir, "sst_custom", []memtable.Entry{
		sampleEntry("alpha", "1"),
		sampleEntry("bravo", "2"),
	})
	if err != nil {
		t.Fatalf("WriteWithID failed: %v", err)
	}
	if sst.ID != "sst_custom" || sst.Path != filepath.Join(dir, "sst_custom.sst") {
		t.Fatalf("expected caller-chosen id to be used, got ID=%q Path=%q", sst.ID, sst.Path)
	}

	stat, err := os.Stat(sst.Path)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if sst.Size != stat.Size() {
		t.Errorf("Write reported Size=%d, file on disk is %d bytes", sst.Size, stat.Size())
	}

	reopened, err := Open(sst.Path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if reopened.Size != stat.Size() {
		t.Errorf("Open reported Size=%d, file on disk is %d bytes", reopened.Size, stat.Size())
	}
}
