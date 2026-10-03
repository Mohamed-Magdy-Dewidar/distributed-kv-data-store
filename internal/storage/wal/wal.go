package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/vectorclock"
)

// headerSize is the fixed-size prefix before every record's payload:
// 4 bytes payload length + 4 bytes CRC32 checksum of the payload.
const headerSize = 8

// Op says what a WAL entry does when replayed.
type Op uint8

const (
	// OpPut merges Item into Key's sibling set (memtable.Put). It is the
	// zero value, so every record written before Op existed replays as a
	// put — which is all those records ever were.
	OpPut Op = iota
	// OpRestore installs Items as Key's sibling set verbatim
	// (memtable.Replace): StorageEngine.Restore's rollback.
	OpRestore
)

// Entry is one durable record: a single key's write or delete, reusing
// the same DataItem type the rest of the system already operates on — or,
// with Op OpRestore, a verbatim replacement of the key's whole sibling set
// with Items (newest first; empty removes the key entirely). Item is used
// only by OpPut, Items only by OpRestore.
type Entry struct {
	Op    Op
	Key   string
	Item  *model.DataItem
	Items []*model.DataItem
}

// record is Entry's on-disk form. VectorClock's fields are unexported, so
// json.Marshal of an Entry would write every clock as {} — the clock is
// stored as its Snapshot map instead, as sstable's record does.
//
// A put stores its item in the flat fields, and Op and Items are omitted,
// so put records are byte-for-byte what they were before OpRestore
// existed. A restore sets Op and Items and leaves the flat fields empty.
// Binaries from before OpRestore would misread a restore record as a put
// of a nil value: downgrading across this change is not supported.
type record struct {
	Key           string
	Value         json.RawMessage
	VectorClock   map[string]uint32
	LastUpdatedBy string
	IsDeleted     bool
	Op            Op           `json:",omitempty"`
	Items         []itemRecord `json:",omitempty"`
}

// itemRecord is one DataItem inside a restore record.
type itemRecord struct {
	Value         json.RawMessage
	VectorClock   map[string]uint32
	LastUpdatedBy string
	IsDeleted     bool
}

func toItemRecord(key string, item *model.DataItem) (itemRecord, error) {
	valueBytes, err := json.Marshal(item.Value)
	if err != nil {
		return itemRecord{}, fmt.Errorf("wal: marshal value for key %q: %w", key, err)
	}
	return itemRecord{
		Value:         valueBytes,
		VectorClock:   item.VectorClock.Snapshot(),
		LastUpdatedBy: item.LastUpdatedBy,
		IsDeleted:     item.IsDeleted,
	}, nil
}

func fromItemRecord(key string, r itemRecord) (*model.DataItem, error) {
	var value any
	if err := json.Unmarshal(r.Value, &value); err != nil {
		return nil, fmt.Errorf("wal: unmarshal value for key %q: %w", key, err)
	}
	return &model.DataItem{
		Value:         value,
		VectorClock:   vectorclock.FromSnapshot(r.VectorClock),
		LastUpdatedBy: r.LastUpdatedBy,
		IsDeleted:     r.IsDeleted,
	}, nil
}

func toRecord(e Entry) (record, error) {
	switch e.Op {
	case OpPut:
		ir, err := toItemRecord(e.Key, e.Item)
		if err != nil {
			return record{}, err
		}
		return record{
			Key:           e.Key,
			Value:         ir.Value,
			VectorClock:   ir.VectorClock,
			LastUpdatedBy: ir.LastUpdatedBy,
			IsDeleted:     ir.IsDeleted,
		}, nil
	case OpRestore:
		rec := record{Key: e.Key, Op: OpRestore}
		for _, item := range e.Items {
			ir, err := toItemRecord(e.Key, item)
			if err != nil {
				return record{}, err
			}
			rec.Items = append(rec.Items, ir)
		}
		return rec, nil
	default:
		return record{}, fmt.Errorf("wal: unknown op %d for key %q", e.Op, e.Key)
	}
}

func fromRecord(r record) (Entry, error) {
	switch r.Op {
	case OpPut:
		item, err := fromItemRecord(r.Key, itemRecord{
			Value:         r.Value,
			VectorClock:   r.VectorClock,
			LastUpdatedBy: r.LastUpdatedBy,
			IsDeleted:     r.IsDeleted,
		})
		if err != nil {
			return Entry{}, err
		}
		return Entry{Key: r.Key, Item: item}, nil
	case OpRestore:
		e := Entry{Op: OpRestore, Key: r.Key}
		for _, ir := range r.Items {
			item, err := fromItemRecord(r.Key, ir)
			if err != nil {
				return Entry{}, err
			}
			e.Items = append(e.Items, item)
		}
		return e, nil
	default:
		return Entry{}, &unknownOpError{op: r.Op, key: r.Key}
	}
}

// unknownOpError is a checksum-valid record whose Op this binary doesn't
// know — most likely written by a newer version. Replay fails on it
// rather than treating it as a torn tail: truncating there would silently
// discard it and every durable record after it.
type unknownOpError struct {
	op  Op
	key string
}

func (e *unknownOpError) Error() string {
	return fmt.Sprintf("wal: record for key %q has unknown op %d (written by a newer version?)", e.key, e.op)
}

// SyncObserver is told how long each successful fsync of a WAL took. It is
// called with the WAL's lock held, in the write path, so it must return at
// once: no blocking, no I/O.
type SyncObserver interface {
	WALSynced(d time.Duration)
}

// WAL is an append-only, crash-safe log file. Every Append fsyncs before
// returning, so a returned nil error means the entry is durable on disk
// even if the process crashes immediately after. Safe for concurrent use.
//
// obs, when set, is told of each fsync. Only a Log sets it, and only on the
// segments it starts for writing: a WAL opened to be replayed has none, so
// replay never reports.
type WAL struct {
	mu   sync.Mutex
	file *os.File
	obs  SyncObserver
}

// Open opens (creating if necessary) the WAL file at path for reading and
// appending. It deliberately doesn't use O_APPEND: Replay must be able to
// truncate a torn tail, which Windows refuses on an append-only handle.
// Appends still go to the end — Open and Replay both leave the offset
// there, and every write happens under w.mu.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: seek to end of %s: %w", path, err)
	}
	return &WAL{file: f}, nil
}

// Append serializes entry, writes it as a length-prefixed, checksummed
// record, and fsyncs before returning. The entry is not considered
// durable until this returns a nil error.
func (w *WAL) Append(entry Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec, err := toRecord(entry)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("wal: marshal record for key %q: %w", entry.Key, err)
	}

	checksum := crc32.ChecksumIEEE(payload)

	header := make([]byte, headerSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:8], checksum)

	// Write header+payload as one buffer so a single os.File.Write call
	// handles both — two separate Write calls would leave a window where
	// a crash between them produces a header with no payload at all,
	// which Replay would still handle correctly (as a torn record), but
	// one write is simpler and marginally reduces that window regardless.
	record := append(header, payload...)
	if _, err := w.file.Write(record); err != nil {
		return fmt.Errorf("wal: write record for key %q: %w", entry.Key, err)
	}

	if err := w.sync(); err != nil {
		return fmt.Errorf("wal: fsync after writing key %q: %w", entry.Key, err)
	}

	return nil
}

// Replay reads every valid, complete record from the start of the file,
// in order. It stops cleanly — without returning an error — at the first
// sign of a torn or corrupted trailing record (a short header, a short
// payload, or a checksum mismatch), since that is the expected on-disk
// shape of a process that crashed mid-Append, not a condition to fail on.
// Any fully-written, checksum-valid records before that point are
// returned normally. The one exception is a checksum-valid record with an
// unknown Op: that's not a torn write, so Replay returns an error and
// leaves the file untouched (see unknownOpError).
//
// It then truncates the file to the end of the last valid record (and
// fsyncs), discarding the torn bytes. Without that, the next Append would
// land after them, and every later Replay would stop at the same torn
// record, silently losing every write made since the crash.
func (w *WAL) Replay() ([]Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	info, err := w.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("wal: stat file for replay: %w", err)
	}
	fileSize := info.Size()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("wal: seek to start for replay: %w", err)
	}

	var entries []Entry
	header := make([]byte, headerSize)
	var pos int64
	var validEnd int64 // end of the last complete, checksum-valid record

	for {
		_, err := io.ReadFull(w.file, header)
		if err != nil {
			break // clean EOF or torn header — either way, stop here
		}
		pos += headerSize

		length := binary.BigEndian.Uint32(header[0:4])
		wantChecksum := binary.BigEndian.Uint32(header[4:8])

		// Sanity-bound length against what's actually left in the file —
		// a torn/corrupted header can claim an arbitrary length (as seen
		// when this test deliberately wrote 0xFFFF0000), and blindly
		// allocating that much is itself a real bug (a multi-gigabyte
		// allocation from a single corrupted byte), not just a
		// theoretical concern.
		remaining := fileSize - pos
		if int64(length) > remaining {
			break // claimed length exceeds what's actually on disk — torn record
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(w.file, payload); err != nil {
			break
		}
		pos += int64(length)

		if crc32.ChecksumIEEE(payload) != wantChecksum {
			break
		}

		var rec record
		if err := json.Unmarshal(payload, &rec); err != nil {
			break
		}
		entry, err := fromRecord(rec)
		if err != nil {
			if _, ok := errors.AsType[*unknownOpError](err); ok {
				return nil, fmt.Errorf("wal: record at offset %d: %w", validEnd, err)
			}
			break
		}

		entries = append(entries, entry)
		validEnd = pos
	}

	if validEnd < fileSize {
		if err := w.file.Truncate(validEnd); err != nil {
			return nil, fmt.Errorf("wal: truncate torn tail at offset %d: %w", validEnd, err)
		}
		if err := w.sync(); err != nil {
			return nil, fmt.Errorf("wal: fsync after truncating torn tail: %w", err)
		}
	}

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("wal: seek to end after replay: %w", err)
	}

	return entries, nil
}

// sync fsyncs the file and, if it succeeded, tells w.obs how long it took.
// Without an observer it is one branch more than the fsync. The caller holds
// w.mu.
func (w *WAL) sync() error {
	if w.obs == nil {
		return w.file.Sync()
	}
	start := time.Now()
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.obs.WALSynced(time.Since(start))
	return nil
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
