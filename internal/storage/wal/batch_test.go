package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
)

// formatGolden is formatEntries Appended one by one, as the WAL wrote them
// before WriteBatch existed: the on-disk format every write path must keep.
const formatGolden = "testdata/format.wal"

func readGolden(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile(formatGolden)
	if err != nil {
		t.Fatalf("read %s: %v", formatGolden, err)
	}
	return b
}

func encodeAll(t *testing.T, entries []Entry) [][]byte {
	t.Helper()
	records := make([][]byte, len(entries))
	for i, e := range entries {
		r, err := Encode(e)
		if err != nil {
			t.Fatalf("Encode(%q): %v", e.Key, err)
		}
		records[i] = r
	}
	return records
}

// writeFile writes entries to a fresh WAL at path, in batches of the given
// sizes (0 means one Append each), and returns the file's bytes.
func writeFile(t *testing.T, path string, entries []Entry, batchSizes ...int) []byte {
	t.Helper()
	w, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if len(batchSizes) == 0 {
		for _, e := range entries {
			if err := w.Append(e); err != nil {
				t.Fatalf("Append(%q): %v", e.Key, err)
			}
		}
	} else {
		records := encodeAll(t, entries)
		for _, n := range batchSizes {
			if err := w.WriteBatch(records[:n]); err != nil {
				t.Fatalf("WriteBatch of %d: %v", n, err)
			}
			records = records[n:]
		}
		if len(records) != 0 {
			t.Fatalf("batch sizes leave %d records unwritten", len(records))
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return b
}

func replayPath(t *testing.T, path string) []Entry {
	t.Helper()
	return replayWith(t, path, openOSFile)
}

func replayWith(t *testing.T, path string, openFile OpenFileFunc) []Entry {
	t.Helper()
	entries, err := replayFile(path, openFile)
	if err != nil {
		t.Fatalf("replay %s: %v", path, err)
	}
	return entries
}

// TestEveryWritePathKeepsTheOnDiskFormat: Append one by one, one batch of
// everything, and batches of mixed sizes all write exactly the bytes the
// WAL wrote before batches existed, Encode makes each of its records, and
// every file replays to the same entries.
func TestEveryWritePathKeepsTheOnDiskFormat(t *testing.T) {
	golden := readGolden(t)
	entries := formatEntries()
	dir := t.TempDir()

	paths := map[string][]int{
		"one Append each":   nil,
		"one batch":         {len(entries)},
		"mixed batch sizes": {1, 3, len(entries) - 4},
	}
	for name, sizes := range paths {
		path := filepath.Join(dir, name+".wal")
		if got := writeFile(t, path, entries, sizes...); !bytes.Equal(got, golden) {
			t.Errorf("%s: wrote %d bytes that differ from %s (%d bytes)", name, len(got), formatGolden, len(golden))
		}
		if got, want := replayPath(t, path), replayPath(t, writeGoldenCopy(t, dir, golden)); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: replays to\n%+v\nwant\n%+v", name, got, want)
		}
	}

	if joined := bytes.Join(encodeAll(t, entries), nil); !bytes.Equal(joined, golden) {
		t.Errorf("Encode's records, joined, differ from %s", formatGolden)
	}
}

func writeGoldenCopy(t *testing.T, dir string, golden []byte) string {
	t.Helper()
	path := filepath.Join(dir, "golden-copy.wal")
	if err := os.WriteFile(path, golden, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

// TestTornBatchReplaysAsAValidPrefix cuts a file after an acknowledged
// batch at every byte of the batch written after it, as a crash during its
// Write or before its fsync can. Replay must return the first batch whole,
// then exactly the second batch's records that are complete, truncate the
// rest, and leave the file ready for the next write. Its files don't
// fsync: what it checks is bytes, and there are two fsyncs per cut.
func TestTornBatchReplaysAsAValidPrefix(t *testing.T) {
	entries := formatEntries()
	const firstBatch = 3
	dir := t.TempDir()
	full := writeFile(t, filepath.Join(dir, "full.wal"), entries, firstBatch, len(entries)-firstBatch)

	records := encodeAll(t, entries)
	ends := make([]int, len(records)) // ends[i]: the offset just past record i
	for i, r := range records {
		ends[i] = len(r)
		if i > 0 {
			ends[i] += ends[i-1]
		}
	}
	want := replayPath(t, writeGoldenCopy(t, dir, full))

	for cut := ends[firstBatch-1]; cut <= len(full); cut++ {
		complete := 0
		for complete < len(ends) && ends[complete] <= cut {
			complete++
		}
		validEnd := 0
		if complete > 0 {
			validEnd = ends[complete-1]
		}

		path := filepath.Join(dir, "torn.wal")
		if err := os.WriteFile(path, full[:cut], 0644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		got := replayWith(t, path, openNoSync)
		if !reflect.DeepEqual(got, want[:complete]) {
			t.Fatalf("cut at %d: replayed %d entries, want the first %d", cut, len(got), complete)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if info.Size() != int64(validEnd) {
			t.Fatalf("cut at %d: file is %d bytes after replay, want it truncated to %d", cut, info.Size(), validEnd)
		}

		// The next batch lands right after the valid prefix and replays.
		w, err := open(path, openNoSync)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if err := w.WriteBatch(records[len(records)-1:]); err != nil {
			t.Fatalf("WriteBatch after replay: %v", err)
		}
		w.Close()
		if got := replayWith(t, path, openNoSync); len(got) != complete+1 || !reflect.DeepEqual(got[complete], want[len(want)-1]) {
			t.Fatalf("cut at %d: after a new batch, replayed %d entries, want %d ending with it", cut, len(got), complete+1)
		}
	}
}

// noSyncFile is a real file whose Sync does nothing.
type noSyncFile struct{ File }

func (noSyncFile) Sync() error { return nil }

func openNoSync(path string) (File, error) {
	f, err := openOSFile(path)
	if err != nil {
		return nil, err
	}
	return noSyncFile{f}, nil
}

// faultyFile is a real file whose Write and Sync can be made to fail. A
// short write writes the first half of what it is given, then fails.
type faultyFile struct {
	File
	writeErr, syncErr error
	shortWrite        bool
}

func (f *faultyFile) Write(b []byte) (int, error) {
	if f.shortWrite {
		n, _ := f.File.Write(b[:len(b)/2])
		return n, errShortWrite
	}
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	return f.File.Write(b)
}

func (f *faultyFile) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.File.Sync()
}

var (
	errShortWrite = errors.New("injected short write")
	errSync       = errors.New("injected fsync failure")
)

// openFaulty returns an OpenFileFunc that wraps every file it opens in a
// faultyFile, and a pointer to the latest one, which is the current
// segment once OpenLog returns.
func openFaulty(opened *atomic.Int32) (OpenFileFunc, **faultyFile) {
	var latest *faultyFile
	return func(path string) (File, error) {
		f, err := openOSFile(path)
		if err != nil {
			return nil, err
		}
		opened.Add(1)
		latest = &faultyFile{File: f}
		return latest, nil
	}, &latest
}

// TestWriteBatchFailsWhenTheFileDoes: WithOpenFile's files are the ones a
// Log replays and writes, and a batch whose fsync or Write fails returns
// that error for the whole batch.
func TestWriteBatchFailsWhenTheFileDoes(t *testing.T) {
	records := encodeAll(t, formatEntries())
	for _, tc := range []struct {
		name   string
		inject func(*faultyFile)
		want   error
	}{
		{"fsync fails", func(f *faultyFile) { f.syncErr = errSync }, errSync},
		{"short write", func(f *faultyFile) { f.shortWrite = true }, errShortWrite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			var opened atomic.Int32
			open, current := openFaulty(&opened)

			l, _, err := OpenLog(dir, WithOpenFile(open))
			if err != nil {
				t.Fatalf("OpenLog: %v", err)
			}
			if err := l.WriteBatch(records[:2]); err != nil {
				t.Fatalf("healthy WriteBatch: %v", err)
			}
			l.Close()

			l, replayed, err := OpenLog(dir, WithOpenFile(open))
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer l.Close()
			if len(replayed) != 2 {
				t.Fatalf("replayed %d entries, want 2", len(replayed))
			}
			if got := opened.Load(); got != 3 { // first segment, its replay, the new segment
				t.Fatalf("WithOpenFile opened %d files, want 3: replayed and written segments both", got)
			}

			tc.inject(*current)
			if err := l.WriteBatch(records[2:]); !errors.Is(err, tc.want) {
				t.Fatalf("WriteBatch with %s returned %v, want %v", tc.name, err, tc.want)
			}
		})
	}
}

// TestEmptyBatchWritesAndSyncsNothing: a batch of no records costs no fsync.
func TestEmptyBatchWritesAndSyncsNothing(t *testing.T) {
	obs := &recordingObserver{}
	l, _, err := OpenLog(t.TempDir(), WithSyncObserver(obs))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	defer l.Close()
	if err := l.WriteBatch(nil); err != nil {
		t.Fatalf("WriteBatch(nil): %v", err)
	}
	if got := obs.count(); got != 0 {
		t.Fatalf("an empty batch fsynced %d times, want 0", got)
	}
}

// TestWriteBatchReusesItsBuffer: once a WAL has written a batch, writing
// another as large allocates nothing: the records are joined into the
// buffer the last batch left.
func TestWriteBatchReusesItsBuffer(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector's instrumentation allocates")
	}
	l, _, err := OpenLog(t.TempDir(), WithSyncObserver(discardObserver{}))
	if err != nil {
		t.Fatalf("OpenLog: %v", err)
	}
	defer l.Close()
	records := encodeAll(t, formatEntries())
	if allocs := testing.AllocsPerRun(50, func() {
		if err := l.WriteBatch(records); err != nil {
			t.Fatalf("WriteBatch: %v", err)
		}
	}); allocs != 0 {
		t.Fatalf("WriteBatch allocates %v times per batch, want 0", allocs)
	}
}
