package wal

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func mustOpenLog(t *testing.T, dir string) (*Log, []string) {
	t.Helper()
	l, entries, err := OpenLog(dir)
	if err != nil {
		t.Fatalf("OpenLog failed: %v", err)
	}
	keys := make([]string, 0, len(entries))
	for _, e := range entries {
		keys = append(keys, e.Key)
	}
	return l, keys
}

func mustAppend(t *testing.T, l *Log, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if err := l.Append(sampleEntry(key, "v")); err != nil {
			t.Fatalf("Append %s failed: %v", key, err)
		}
	}
}

func segmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	segs, err := listSegments(dir)
	if err != nil {
		t.Fatalf("listSegments: %v", err)
	}
	names := make([]string, 0, len(segs))
	for _, s := range segs {
		names = append(names, filepath.Base(s.path))
	}
	return names
}

// TestLogReplaysSegmentsInOrderAndStartsAFreshOne: entries come back in
// append order across rotations, and reopening never appends to a
// replayed segment.
func TestLogReplaysSegmentsInOrderAndStartsAFreshOne(t *testing.T) {
	dir := t.TempDir()
	l, keys := mustOpenLog(t, dir)
	if len(keys) != 0 {
		t.Fatalf("expected an empty log, got %v", keys)
	}
	mustAppend(t, l, "a")
	sealed, err := l.Rotate()
	if err != nil || sealed != 1 {
		t.Fatalf("expected Rotate to seal segment 1, got %d (err %v)", sealed, err)
	}
	mustAppend(t, l, "b", "c")
	l.Close()

	l2, keys := mustOpenLog(t, dir)
	defer l2.Close()
	if !reflect.DeepEqual(keys, []string{"a", "b", "c"}) {
		t.Fatalf("expected [a b c], got %v", keys)
	}
	want := []string{"wal_00000000000000000001.log", "wal_00000000000000000002.log", "wal_00000000000000000003.log"}
	if got := segmentFiles(t, dir); !reflect.DeepEqual(got, want) {
		t.Fatalf("expected segments %v (a fresh one for new writes), got %v", want, got)
	}
}

// TestLogDeleteThroughKeepsCurrentAndNewer: only sealed segments at or
// below the given number go; newer ones and the current one stay.
func TestLogDeleteThroughKeepsCurrentAndNewer(t *testing.T) {
	dir := t.TempDir()
	l, _ := mustOpenLog(t, dir)
	mustAppend(t, l, "a")
	s1, _ := l.Rotate()
	mustAppend(t, l, "b")
	l.Rotate()
	mustAppend(t, l, "c")

	if err := l.DeleteThrough(s1); err != nil {
		t.Fatalf("DeleteThrough failed: %v", err)
	}
	want := []string{"wal_00000000000000000002.log", "wal_00000000000000000003.log"}
	if got := segmentFiles(t, dir); !reflect.DeepEqual(got, want) {
		t.Fatalf("expected DeleteThrough(%d) to leave %v, got %v", s1, want, got)
	}

	if err := l.DeleteThrough(1 << 60); err != nil { // far past current: must still spare it
		t.Fatalf("DeleteThrough failed: %v", err)
	}
	mustAppend(t, l, "d") // current segment still usable
	l.Close()

	l2, keys := mustOpenLog(t, dir)
	defer l2.Close()
	if !reflect.DeepEqual(keys, []string{"c", "d"}) {
		t.Fatalf("expected only the current segment's [c d] to survive, got %v", keys)
	}
}

// TestLogReplaysLegacyWALFirst: a data directory from before segments has
// one wal.log; it replays first, as segment 0, and is deleted like any
// other segment once flushed.
func TestLogReplaysLegacyWALFirst(t *testing.T) {
	dir := t.TempDir()
	legacy, err := Open(filepath.Join(dir, legacyName))
	if err != nil {
		t.Fatalf("Open legacy: %v", err)
	}
	if err := legacy.Append(sampleEntry("old", "v")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	legacy.Close()

	l, keys := mustOpenLog(t, dir)
	if !reflect.DeepEqual(keys, []string{"old"}) {
		t.Fatalf("expected the legacy entry, got %v", keys)
	}
	mustAppend(t, l, "new")
	l.Close()

	l2, keys := mustOpenLog(t, dir)
	if !reflect.DeepEqual(keys, []string{"old", "new"}) {
		t.Fatalf("expected [old new] (legacy first), got %v", keys)
	}
	if err := l2.DeleteThrough(0); err != nil {
		t.Fatalf("DeleteThrough(0) failed: %v", err)
	}
	l2.Close()
	if _, err := os.Stat(filepath.Join(dir, legacyName)); !os.IsNotExist(err) {
		t.Fatalf("expected wal.log deleted once flushed, stat err = %v", err)
	}
}

// TestLogDeletesEmptySegmentsOnOpen: restarting without writing must not
// leave one empty segment file behind per restart.
func TestLogDeletesEmptySegmentsOnOpen(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 5; i++ {
		l, _ := mustOpenLog(t, dir)
		l.Close()
	}
	if got := segmentFiles(t, dir); len(got) != 1 {
		t.Fatalf("expected a single segment after 5 empty restarts, got %v", got)
	}
}

// TestLogTornTailInOlderSegmentDoesNotHideNewerOnes: a crash mid-append
// tears only its own segment's tail; replay still continues into every
// later segment.
func TestLogTornTailInOlderSegmentDoesNotHideNewerOnes(t *testing.T) {
	dir := t.TempDir()
	l, _ := mustOpenLog(t, dir)
	mustAppend(t, l, "a")
	s1, _ := l.Rotate()
	mustAppend(t, l, "b")
	l.Close()

	f, err := os.OpenFile(segmentPath(dir, s1), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open segment %d: %v", s1, err)
	}
	f.Write([]byte{0xFF, 0xFF, 0, 0}) // torn header
	f.Close()

	l2, keys := mustOpenLog(t, dir)
	defer l2.Close()
	if !reflect.DeepEqual(keys, []string{"a", "b"}) {
		t.Fatalf("expected [a b], got %v", keys)
	}
}
