// Package fsutil holds small filesystem helpers shared by the storage
// packages.
package fsutil

import (
	"fmt"
	"os"
	"runtime"
)

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
