package fsutil

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestWriteFileAtomicCreatesAndReplaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "FILE")

	for _, want := range []string{"first", "second, longer than the first"} {
		if err := WriteFileAtomic(path, []byte(want), 0o644); err != nil {
			t.Fatalf("WriteFileAtomic(%q): %v", want, err)
		}
		if got, err := os.ReadFile(path); err != nil || string(got) != want {
			t.Fatalf("read back %q, %v; want %q", got, err, want)
		}
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temp file left behind (stat err %v)", err)
	}
}

func TestWriteFileAtomicFailsInAMissingDirectory(t *testing.T) {
	if err := WriteFileAtomic(filepath.Join(t.TempDir(), "missing", "FILE"), []byte("x"), 0o644); err == nil {
		t.Fatal("expected an error writing into a missing directory")
	}
}

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
