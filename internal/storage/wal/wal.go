package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/vectorclock"
)

// headerSize is the fixed-size prefix before every record's payload:
// 4 bytes payload length + 4 bytes CRC32 checksum of the payload.
const headerSize = 8

// Entry is one durable record: a single key's write or delete, reusing
// the same DataItem type the rest of the system already operates on.
type Entry struct {
	Key  string
	Item *model.DataItem
}

// record is Entry's on-disk form. VectorClock's fields are unexported, so
// json.Marshal of an Entry would write every clock as {} — the clock is
// stored as its Snapshot map instead, as sstable's record does.
type record struct {
	Key           string
	Value         json.RawMessage
	VectorClock   map[string]uint32
	LastUpdatedBy string
	IsDeleted     bool
}

func toRecord(e Entry) (record, error) {
	valueBytes, err := json.Marshal(e.Item.Value)
	if err != nil {
		return record{}, fmt.Errorf("wal: marshal value for key %q: %w", e.Key, err)
	}
	return record{
		Key:           e.Key,
		Value:         valueBytes,
		VectorClock:   e.Item.VectorClock.Snapshot(),
		LastUpdatedBy: e.Item.LastUpdatedBy,
		IsDeleted:     e.Item.IsDeleted,
	}, nil
}

func fromRecord(r record) (Entry, error) {
	var value any
	if err := json.Unmarshal(r.Value, &value); err != nil {
		return Entry{}, fmt.Errorf("wal: unmarshal value for key %q: %w", r.Key, err)
	}
	return Entry{
		Key: r.Key,
		Item: &model.DataItem{
			Value:         value,
			VectorClock:   vectorclock.FromSnapshot(r.VectorClock),
			LastUpdatedBy: r.LastUpdatedBy,
			IsDeleted:     r.IsDeleted,
		},
	}, nil
}

// WAL is an append-only, crash-safe log file. Every Append fsyncs before
// returning, so a returned nil error means the entry is durable on disk
// even if the process crashes immediately after. Safe for concurrent use.
type WAL struct {
	mu   sync.Mutex
	file *os.File
}

// Open opens (creating if necessary) the WAL file at path for reading and
// appending.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
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

	if err := w.file.Sync(); err != nil {
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
// returned normally.
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
			break
		}

		entries = append(entries, entry)
	}

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("wal: seek to end after replay: %w", err)
	}

	return entries, nil
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
