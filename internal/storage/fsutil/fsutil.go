// Package fsutil holds small filesystem helpers shared by the storage
// packages.
package fsutil

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// WriteFileAtomic makes path hold exactly data, or leave it as it was: it
// writes path+".tmp", fsyncs it, renames it over path, then fsyncs the
// directory so the rename is durable (see SyncDir). A crash at any point
// leaves either the old file (or none) or the complete new one, never a
// partial write. A stale path+".tmp" from an earlier crash is overwritten.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("fsutil: create %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("fsutil: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsutil: sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("fsutil: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("fsutil: install %s: %w", path, err)
	}
	return SyncDir(filepath.Dir(path))
}

// SyncDir fsyncs directory dir, making the creation, rename or removal of
// an entry in it durable. fsyncing a file makes its contents durable but
// not necessarily its directory entry: on Linux (ext4 and others) a crash
// can lose a newly created or renamed file whose data was already synced.
//
// It's best-effort by platform: Windows can't fsync a directory (NTFS
// journals its metadata), so there it does nothing and returns nil.
// Elsewhere a failure is returned — it means the entry may not survive a
// crash.
func SyncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("fsutil: open dir %s for sync: %w", dir, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsutil: sync dir %s: %w", dir, err)
	}
	return nil
}
