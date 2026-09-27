package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"

	"distributed-kv-datastore/internal/storage/fsutil"
)

// legacyName is the single WAL file engines wrote before the log was split
// into segments. OpenLog replays it as segment 0, so existing data
// directories keep working, and it's deleted like any other segment once
// its contents are flushed.
const legacyName = "wal.log"

var segmentName = regexp.MustCompile(`^wal_(\d{20})\.log$`)

func segmentPath(dir string, seq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("wal_%020d.log", seq))
}

type segment struct {
	seq  uint64
	path string
}

// Log is a write-ahead log split into numbered segment files,
// wal_<seq>.log, so that the parts already flushed to SSTables can be
// deleted instead of growing, and being replayed, forever.
//
// Appends always go to the current (newest) segment. Rotate seals it and
// starts the next one; DeleteThrough removes sealed segments once their
// contents are durable elsewhere. A replayed segment is never appended to
// again: OpenLog always starts a fresh one.
//
// No record of which segments exist is kept anywhere but the directory.
// That's safe because a segment's presence only ever means "may not be
// flushed yet": replaying one whose contents were already flushed just
// re-applies versions the SSTables already hold, which merges away.
type Log struct {
	mu      sync.Mutex
	dir     string
	current *WAL
	seq     uint64    // current segment's sequence number
	sealed  []segment // older segments still on disk, oldest first
}

// OpenLog opens the segmented log in dir: it replays every existing
// segment in sequence order (the legacy wal.log first, as segment 0) and
// returns their entries in that order, then starts a new, empty segment
// for every write from now on. Segments that turn out to hold no valid
// entries are deleted on the spot.
func OpenLog(dir string) (_ *Log, _ []Entry, err error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, nil, fmt.Errorf("wal: create dir %s: %w", dir, err)
	}
	existing, err := listSegments(dir)
	if err != nil {
		return nil, nil, err
	}

	l := &Log{dir: dir}
	var entries []Entry
	var next uint64 = 1
	for _, seg := range existing {
		segEntries, err := replayFile(seg.path)
		if err != nil {
			return nil, nil, err
		}
		entries = append(entries, segEntries...)
		if seg.seq >= next {
			next = seg.seq + 1
		}
		if len(segEntries) == 0 {
			os.Remove(seg.path) // nothing in it to lose; best effort
			continue
		}
		l.sealed = append(l.sealed, seg)
	}

	if err := l.startSegment(next); err != nil {
		return nil, nil, err
	}
	return l, entries, nil
}

// listSegments returns every segment file in dir, oldest first.
func listSegments(dir string) ([]segment, error) {
	names, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: list %s: %w", dir, err)
	}
	var segs []segment
	for _, de := range names {
		name := de.Name()
		if name == legacyName {
			segs = append(segs, segment{seq: 0, path: filepath.Join(dir, name)})
			continue
		}
		m := segmentName.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		seq, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("wal: bad segment name %s: %w", name, err)
		}
		segs = append(segs, segment{seq: seq, path: filepath.Join(dir, name)})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].seq < segs[j].seq })
	return segs, nil
}

// replayFile replays one segment (truncating a torn tail, as WAL.Replay
// does) and closes it.
func replayFile(path string) ([]Entry, error) {
	w, err := Open(path)
	if err != nil {
		return nil, err
	}
	entries, err := w.Replay()
	closeErr := w.Close()
	if err != nil {
		return nil, fmt.Errorf("wal: replay %s: %w", path, err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("wal: close %s after replay: %w", path, closeErr)
	}
	return entries, nil
}

// startSegment creates segment seq, makes its directory entry durable, and
// makes it current. The caller holds l.mu (or owns l exclusively).
func (l *Log) startSegment(seq uint64) error {
	w, err := Open(segmentPath(l.dir, seq))
	if err != nil {
		return err
	}
	if err := fsutil.SyncDir(l.dir); err != nil {
		w.Close()
		os.Remove(segmentPath(l.dir, seq))
		return fmt.Errorf("wal: new segment %d: %w", seq, err)
	}
	l.current = w
	l.seq = seq
	return nil
}

// Append durably writes entry to the current segment (see WAL.Append).
func (l *Log) Append(entry Entry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.current.Append(entry)
}

// Rotate seals the current segment and starts the next one, returning the
// sealed segment's sequence number: every entry appended before Rotate is
// in a segment numbered at most that. If the new segment can't be created,
// nothing changes and appends keep going to the current segment.
func (l *Log) Rotate() (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	old, oldSeq := l.current, l.seq
	if err := l.startSegment(oldSeq + 1); err != nil {
		return 0, err
	}
	l.sealed = append(l.sealed, segment{seq: oldSeq, path: segmentPath(l.dir, oldSeq)})
	if err := old.Close(); err != nil {
		return oldSeq, fmt.Errorf("wal: close sealed segment %d: %w", oldSeq, err)
	}
	return oldSeq, nil
}

// DeleteThrough deletes every sealed segment numbered at most seq, oldest
// first; the current segment is never deleted. Call it only once
// everything those segments hold is durable elsewhere. If a deletion
// fails it stops there, keeping that segment and every newer one: they're
// only replayed harmlessly, and a later DeleteThrough retries them. The
// deletions aren't made durable with a directory fsync for the same
// reason — a segment a crash brings back is just replayed.
func (l *Log) DeleteThrough(seq uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	deleted := 0
	var err error
	for _, seg := range l.sealed {
		if seg.seq > seq {
			break
		}
		if rmErr := os.Remove(seg.path); rmErr != nil && !os.IsNotExist(rmErr) {
			err = fmt.Errorf("wal: delete segment %d: %w", seg.seq, rmErr)
			break
		}
		deleted++
	}
	l.sealed = l.sealed[deleted:]
	return err
}

// Close closes the current segment.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.current.Close()
}
