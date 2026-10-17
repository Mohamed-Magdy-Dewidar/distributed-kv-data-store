package storetest

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"

	"distributed-kv-datastore/internal/storage/wal"
)

// ErrInjectedSync is the error a FailingSyncs segment's Sync returns.
var ErrInjectedSync = errors.New("injected fsync failure")

// FailingSyncs opens WAL segments, for engine.WithWALOpenFile, whose fsync
// fails once Arm has been called for the directory they are in. Segments
// in any other directory keep working, so arming a node's data directory
// leaves its hint store, in dir/hints, untouched, and the other way round.
// A segment opened before Arm fails too: Sync checks at each call.
type FailingSyncs struct {
	armed atomic.Pointer[string] // the directory whose segments fail
}

// Arm makes every Sync of a segment in dir fail with ErrInjectedSync from
// now on.
func (f *FailingSyncs) Arm(dir string) { f.armed.Store(&dir) }

// Open is a wal.OpenFileFunc.
func (f *FailingSyncs) Open(path string) (wal.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &failingSyncFile{File: file, dir: filepath.Dir(path), f: f}, nil
}

type failingSyncFile struct {
	*os.File
	dir string
	f   *FailingSyncs
}

func (s *failingSyncFile) Sync() error {
	if dir := s.f.armed.Load(); dir != nil && *dir == s.dir {
		return ErrInjectedSync
	}
	return s.File.Sync()
}
