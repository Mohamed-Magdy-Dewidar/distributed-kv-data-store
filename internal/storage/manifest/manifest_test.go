package manifest

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func sortedIDs(m *Manifest) []string {
	ids := m.LiveIDs()
	sort.Strings(ids)
	return ids
}

func TestAddThenLiveIDsReflectsIt(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer m.Close()

	if err := m.Add("sst_1"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if err := m.Add("sst_2"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	got := sortedIDs(m)
	want := []string{"sst_1", "sst_2"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestRemoveTakesEffect(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer m.Close()

	m.Add("sst_1")
	m.Add("sst_2")
	m.Add("sst_3")

	if err := m.Remove([]string{"sst_1", "sst_2"}); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	got := sortedIDs(m)
	if len(got) != 1 || got[0] != "sst_3" {
		t.Fatalf("expected only sst_3 to remain, got %v", got)
	}
}

func TestSurvivesRestartReflectingAddsAndRemoves(t *testing.T) {
	// The core proof: a fresh Manifest instance, opened against the same
	// directory, must reconstruct the exact same live set purely from
	// replaying the file — no in-memory state carried over.
	dir := t.TempDir()

	m1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	m1.Add("sst_1")
	m1.Add("sst_2")
	m1.Add("sst_3")
	m1.Add("sst_4")
	m1.Add("sst_5") // simulates: compaction's merged output already durably added
	if err := m1.Remove([]string{"sst_1", "sst_2", "sst_3", "sst_4"}); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	if err := m1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	m2, err := Open(dir)
	if err != nil {
		t.Fatalf("re-Open failed: %v", err)
	}
	defer m2.Close()

	got := sortedIDs(m2)
	if len(got) != 1 || got[0] != "sst_5" {
		t.Fatalf("expected only sst_5 to remain after reopening, got %v", got)
	}
}

func TestAddingSameIDTwiceIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer m.Close()

	m.Add("sst_1")
	m.Add("sst_1") // duplicate ADD — should not create a second logical entry

	got := m.LiveIDs()
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 live ID after duplicate Add, got %v", got)
	}
}

func TestRemoveOfUnknownIDIsHarmless(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer m.Close()

	if err := m.Remove([]string{"never-added"}); err != nil {
		t.Fatalf("Remove of an unknown ID should not error, got: %v", err)
	}
	if len(m.LiveIDs()) != 0 {
		t.Error("expected an empty live set")
	}
}

func TestSurvivesTornFinalLine(t *testing.T) {
	// Simulates a crash mid-write of the last line: write two complete
	// lines normally, then manually append an incomplete line (no
	// trailing newline) directly to the file, bypassing writeLine.
	dir := t.TempDir()

	m1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	m1.Add("sst_1")
	m1.Add("sst_2")
	if err := m1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	f, err := os.OpenFile(filepath.Join(dir, filename), os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to reopen for corruption: %v", err)
	}
	if _, err := f.WriteString("ADD sst_3_but_never_finished_wri"); err != nil { // no trailing newline
		t.Fatalf("failed to write torn line: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	m2, err := Open(dir)
	if err != nil {
		t.Fatalf("expected Open to succeed despite a torn final line, got: %v", err)
	}
	defer m2.Close()

	got := sortedIDs(m2)
	want := []string{"sst_1", "sst_2"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("expected exactly [sst_1, sst_2] (torn line ignored), got %v", got)
	}
}
