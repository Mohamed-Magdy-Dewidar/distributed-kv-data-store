package sstable

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/bits-and-blooms/bloom/v3"

	"distributed-kv-datastore/internal/storage/memtable"
	"distributed-kv-datastore/internal/store"
	"distributed-kv-datastore/internal/vectorclock"
)

const recordHeaderSize = 8

// bloomFalsePositiveRate is the target false-positive rate for each
// SSTable's Bloom filter — 1% is a standard, well-tested default that
// balances filter size against how often Get still has to do a real
// (wasted) disk read for a key that turns out not to be present.
const bloomFalsePositiveRate = 0.01

type indexEntry struct {
	Key    string
	Offset int64
}

// SSTable represents one immutable, on-disk, sorted file. MinKey/MaxKey
// and the Bloom filter both let a caller cheaply rule out a whole file —
// the range check via a plain string comparison, the filter via a
// probabilistic membership test — before ever opening it for a real read.
type SSTable struct {
	ID     string // base filename without extension, e.g. "sst_1234567890"
	Path   string
	MinKey string
	MaxKey string
	index  []indexEntry
	filter *bloom.BloomFilter
}

type record struct {
	Key           string
	Value         json.RawMessage
	VectorClock   map[string]uint32
	LastUpdatedBy string
	IsDeleted     bool
}

func toRecord(e memtable.Entry) (record, error) {
	valueBytes, err := json.Marshal(e.Item.Value)
	if err != nil {
		return record{}, fmt.Errorf("sstable: marshal value for key %q: %w", e.Key, err)
	}
	return record{
		Key:           e.Key,
		Value:         valueBytes,
		VectorClock:   e.Item.VectorClock.Snapshot(),
		LastUpdatedBy: e.Item.LastUpdatedBy,
		IsDeleted:     e.Item.IsDeleted,
	}, nil
}

func fromRecord(r record) (memtable.Entry, error) {
	var value any
	if err := json.Unmarshal(r.Value, &value); err != nil {
		return memtable.Entry{}, fmt.Errorf("sstable: unmarshal value for key %q: %w", r.Key, err)
	}
	return memtable.Entry{
		Key: r.Key,
		Item: &store.DataItem{
			Value:         value,
			VectorClock:   vectorclock.FromSnapshot(r.VectorClock),
			LastUpdatedBy: r.LastUpdatedBy,
			IsDeleted:     r.IsDeleted,
		},
	}, nil
}

func encodeRecord(e memtable.Entry) ([]byte, error) {
	rec, err := toRecord(e)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("sstable: marshal record for key %q: %w", e.Key, err)
	}

	checksum := crc32.ChecksumIEEE(payload)
	buf := make([]byte, recordHeaderSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], checksum)
	copy(buf[recordHeaderSize:], payload)
	return buf, nil
}

func decodeRecordAt(f *os.File, offset int64) (memtable.Entry, error) {
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return memtable.Entry{}, fmt.Errorf("sstable: seek to %d: %w", offset, err)
	}

	header := make([]byte, recordHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return memtable.Entry{}, fmt.Errorf("sstable: read header at %d: %w", offset, err)
	}
	length := binary.BigEndian.Uint32(header[0:4])
	wantChecksum := binary.BigEndian.Uint32(header[4:8])

	payload := make([]byte, length)
	if _, err := io.ReadFull(f, payload); err != nil {
		return memtable.Entry{}, fmt.Errorf("sstable: read payload at %d: %w", offset, err)
	}
	if crc32.ChecksumIEEE(payload) != wantChecksum {
		return memtable.Entry{}, fmt.Errorf("sstable: checksum mismatch at offset %d (file corrupted)", offset)
	}

	var rec record
	if err := json.Unmarshal(payload, &rec); err != nil {
		return memtable.Entry{}, fmt.Errorf("sstable: decode record at %d: %w", offset, err)
	}
	return fromRecord(rec)
}

// writeDataSection writes every entry sequentially — required, not just
// convention: each entry's index offset depends on the exact cumulative
// byte length of every prior entry, so this loop is inherently ordered.
func writeDataSection(f *os.File, entries []memtable.Entry) ([]indexEntry, int64, error) {
	var index []indexEntry
	var offset int64

	for _, e := range entries {
		buf, err := encodeRecord(e)
		if err != nil {
			return nil, 0, err
		}
		index = append(index, indexEntry{Key: e.Key, Offset: offset})

		n, err := f.Write(buf)
		if err != nil {
			return nil, 0, fmt.Errorf("sstable: write record for key %q: %w", e.Key, err)
		}
		offset += int64(n)
	}

	return index, offset, nil
}

func buildFilter(entries []memtable.Entry) *bloom.BloomFilter {
	filter := bloom.NewWithEstimates(uint(len(entries)), bloomFalsePositiveRate)
	for _, e := range entries {
		filter.AddString(e.Key)
	}
	return filter
}

func writeFooter(f *os.File, bloomStart, indexStart int64) error {
	footer := make([]byte, 16)
	binary.BigEndian.PutUint64(footer[0:8], uint64(bloomStart))
	binary.BigEndian.PutUint64(footer[8:16], uint64(indexStart))
	_, err := f.Write(footer)
	return err
}

// Write serializes entries (which must already be sorted by key — see
// memtable.Snapshot/SnapshotAndClear) to a new SSTable file in dir.
// Publication is atomic: fully written and fsynced under a temporary
// name, then renamed into place.
func Write(dir string, entries []memtable.Entry) (*SSTable, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("sstable: cannot write an empty SSTable")
	}
	if !sort.SliceIsSorted(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key }) {
		return nil, fmt.Errorf("sstable: entries must be sorted by key")
	}

	id := fmt.Sprintf("sst_%d", time.Now().UnixNano())
	tmpPath := filepath.Join(dir, id+".tmp")
	finalPath := filepath.Join(dir, id+".sst")

	f, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("sstable: create %s: %w", tmpPath, err)
	}

	sst, err := writeAndPublish(f, id, tmpPath, finalPath, entries)
	if err != nil {
		f.Close()
		os.Remove(tmpPath)
		return nil, err
	}
	return sst, nil
}

func writeAndPublish(f *os.File, id, tmpPath, finalPath string, entries []memtable.Entry) (*SSTable, error) {
	index, bloomStart, err := writeDataSection(f, entries)
	if err != nil {
		return nil, err
	}

	filter := buildFilter(entries)
	bloomBytesWritten, err := filter.WriteTo(f)
	if err != nil {
		return nil, fmt.Errorf("sstable: write bloom filter: %w", err)
	}
	indexStart := bloomStart + bloomBytesWritten

	indexBytes, err := json.Marshal(index)
	if err != nil {
		return nil, fmt.Errorf("sstable: marshal index: %w", err)
	}
	if _, err := f.Write(indexBytes); err != nil {
		return nil, fmt.Errorf("sstable: write index: %w", err)
	}

	if err := writeFooter(f, bloomStart, indexStart); err != nil {
		return nil, fmt.Errorf("sstable: write footer: %w", err)
	}
	if err := f.Sync(); err != nil {
		return nil, fmt.Errorf("sstable: fsync %s: %w", tmpPath, err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("sstable: close %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return nil, fmt.Errorf("sstable: rename to %s: %w", finalPath, err)
	}

	return &SSTable{
		ID:     id,
		Path:   finalPath,
		MinKey: entries[0].Key,
		MaxKey: entries[len(entries)-1].Key,
		index:  index,
		filter: filter,
	}, nil
}

// Open loads an existing SSTable's footer, Bloom filter, and index into
// memory (not its data records — those are read on demand by Get).
func Open(path string) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", path, err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("sstable: stat %s: %w", path, err)
	}
	if stat.Size() < 16 {
		return nil, fmt.Errorf("sstable: %s is too small to contain a valid footer", path)
	}

	footer := make([]byte, 16)
	if _, err := f.ReadAt(footer, stat.Size()-16); err != nil {
		return nil, fmt.Errorf("sstable: read footer of %s: %w", path, err)
	}
	bloomStart := int64(binary.BigEndian.Uint64(footer[0:8]))
	indexStart := int64(binary.BigEndian.Uint64(footer[8:16]))

	if bloomStart < 0 || indexStart < bloomStart || indexStart > stat.Size()-16 {
		return nil, fmt.Errorf("sstable: %s has a corrupt footer", path)
	}

	bloomLen := indexStart - bloomStart
	bloomBytes := make([]byte, bloomLen)
	if _, err := f.ReadAt(bloomBytes, bloomStart); err != nil {
		return nil, fmt.Errorf("sstable: read bloom filter of %s: %w", path, err)
	}
	filter := &bloom.BloomFilter{}
	if _, err := filter.ReadFrom(bytes.NewReader(bloomBytes)); err != nil {
		return nil, fmt.Errorf("sstable: decode bloom filter of %s: %w", path, err)
	}

	indexLen := stat.Size() - 16 - indexStart
	indexBytes := make([]byte, indexLen)
	if _, err := f.ReadAt(indexBytes, indexStart); err != nil {
		return nil, fmt.Errorf("sstable: read index of %s: %w", path, err)
	}
	var index []indexEntry
	if err := json.Unmarshal(indexBytes, &index); err != nil {
		return nil, fmt.Errorf("sstable: decode index of %s: %w", path, err)
	}
	if len(index) == 0 {
		return nil, fmt.Errorf("sstable: %s has an empty index", path)
	}

	return &SSTable{
		ID:     strings.TrimSuffix(filepath.Base(path), ".sst"),
		Path:   path,
		MinKey: index[0].Key,
		MaxKey: index[len(index)-1].Key,
		index:  index,
		filter: filter,
	}, nil
}

// Get looks up key: a cheap range check, then a Bloom filter test (both
// entirely in-memory, no disk I/O), and only if both pass does it open
// the file and read the one matching record.
func (s *SSTable) Get(key string) (*store.DataItem, bool, error) {
	if key < s.MinKey || key > s.MaxKey {
		return nil, false, nil
	}
	if s.filter != nil && !s.filter.TestString(key) {
		return nil, false, nil // definitely not here — zero disk I/O
	}

	i := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].Key >= key
	})
	if i >= len(s.index) || s.index[i].Key != key {
		return nil, false, nil // Bloom false positive — real, expected, rare
	}

	f, err := os.Open(s.Path)
	if err != nil {
		return nil, false, fmt.Errorf("sstable: open %s: %w", s.Path, err)
	}
	defer f.Close()

	entry, err := decodeRecordAt(f, s.index[i].Offset)
	if err != nil {
		return nil, false, err
	}
	return entry.Item, true, nil
}

// All reads and returns every entry in sorted order — used by compaction
// (future step) and tests, bypassing the Bloom filter entirely since it
// intentionally returns everything.
func (s *SSTable) All() ([]memtable.Entry, error) {
	f, err := os.Open(s.Path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", s.Path, err)
	}
	defer f.Close()

	entries := make([]memtable.Entry, 0, len(s.index))
	for _, ie := range s.index {
		entry, err := decodeRecordAt(f, ie.Offset)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
