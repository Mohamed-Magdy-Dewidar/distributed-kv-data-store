package fsutil

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestSyncDir(t *testing.T) {
	if err := SyncDir(t.TempDir()); err != nil {
		t.Fatalf("SyncDir on an existing directory failed: %v", err)
	}

	err := SyncDir(filepath.Join(t.TempDir(), "missing"))
	if runtime.GOOS == "windows" {
		if err != nil {
			t.Fatalf("expected SyncDir to be a no-op on Windows, got %v", err)
		}
		return
	}
	if err == nil {
		t.Fatal("expected SyncDir on a missing directory to fail")
	}
}
