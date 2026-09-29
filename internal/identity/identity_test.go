package identity

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var incarnationPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

func TestNewIncarnationIs16HexCharsAndVaries(t *testing.T) {
	a, b := NewIncarnation(), NewIncarnation()
	if !incarnationPattern.MatchString(a) || !incarnationPattern.MatchString(b) {
		t.Fatalf("expected 16 lowercase hex chars, got %q and %q", a, b)
	}
	if a == b {
		t.Fatalf("two incarnations were both %q", a)
	}
}

func TestClockID(t *testing.T) {
	if got := ClockID("node-1", "00112233aabbccdd"); got != "node-1#00112233aabbccdd" {
		t.Fatalf("got %q", got)
	}
}

// The incarnation is created once per data dir and read back after that; a
// wiped dir is a new one.
func TestLoadOrCreatePersistsAndAWipedDirGetsANewOne(t *testing.T) {
	dir := t.TempDir()

	first, err := LoadOrCreate(dir, "node-1")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !incarnationPattern.MatchString(first) {
		t.Fatalf("incarnation %q is not 16 hex chars", first)
	}
	again, err := LoadOrCreate(dir, "node-1")
	if err != nil || again != first {
		t.Fatalf("reopen: got %q, %v; want %q", again, err, first)
	}
	if _, err := os.Stat(filepath.Join(dir, FileName+".tmp")); !os.IsNotExist(err) {
		t.Fatalf("temp file left behind (stat err %v)", err)
	}

	if err := os.Remove(filepath.Join(dir, FileName)); err != nil {
		t.Fatal(err)
	}
	fresh, err := LoadOrCreate(dir, "node-1")
	if err != nil {
		t.Fatalf("recreate: %v", err)
	}
	if fresh == first {
		t.Fatalf("a wiped identity came back as the same incarnation %q", fresh)
	}
}

// A dir that belongs to another node ID must not be adopted or overwritten.
func TestLoadOrCreateRefusesADirOwnedByAnotherNode(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadOrCreate(dir, "node-1"); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filepath.Join(dir, FileName))

	_, err := LoadOrCreate(dir, "node-2")
	if err == nil {
		t.Fatal("expected an error for a mismatched node ID")
	}
	for _, want := range []string{`"node-1"`, `"node-2"`, dir} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
	if after, _ := os.ReadFile(filepath.Join(dir, FileName)); string(after) != string(before) {
		t.Errorf("the identity file was modified: %q -> %q", before, after)
	}
}

func TestLoadOrCreateRejectsAnUnreadableIdentity(t *testing.T) {
	for name, content := range map[string]string{
		"not json":          "node-1",
		"bad incarnation":   `{"node_id":"node-1","incarnation":"xyz"}`,
		"empty incarnation": `{"node_id":"node-1","incarnation":""}`,
	} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, FileName), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadOrCreate(dir, "node-1"); err == nil {
			t.Errorf("%s: expected an error, got none", name)
		}
	}
}

func TestLoadOrCreateRejectsAnEmptyNodeID(t *testing.T) {
	if _, err := LoadOrCreate(t.TempDir(), ""); err == nil {
		t.Fatal("expected an error for an empty node ID")
	}
}
