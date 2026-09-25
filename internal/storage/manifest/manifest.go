package manifest

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const filename = "MANIFEST"

// Manifest is a durable, append-only record of which SSTable IDs (base
// filenames without the .sst extension, matching sstable.Write's
// generated IDs) are currently live. It exists specifically because
// compaction can make some SSTable files obsolete while others remain
// current — "every .sst file in the directory" and "every SSTable that's
// actually current" stop being the same set the moment compaction can
// run, and this file is the durable source of truth for the latter,
// immune to a crash leaving stale files sitting in the directory.
type Manifest struct {
	mu   sync.Mutex
	file *os.File
	live map[string]bool // SSTable ID -> currently live
}

// Open creates or opens the MANIFEST file in dir, replaying it to
// reconstruct the current set of live SSTable IDs.
func Open(dir string) (*Manifest, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("manifest: create dir %s: %w", dir, err)
	}

	path := filepath.Join(dir, filename)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("manifest: open %s: %w", path, err)
	}

	m := &Manifest{file: f, live: make(map[string]bool)}
	if err := m.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return m, nil
}

// replay reads every line from the start of the file, applying ADD/REMOVE
// in order to reconstruct the live set. A torn final line (no trailing
// newline — the expected shape of a crash mid-write) is skipped rather
// than treated as an error; every complete line before it still applies.
func (m *Manifest) replay() error {
	if _, err := m.file.Seek(0, 0); err != nil {
		return fmt.Errorf("manifest: seek to start: %w", err)
	}

	data, err := io.ReadAll(m.file)
	if err != nil {
		return fmt.Errorf("manifest: read for replay: %w", err)
	}

	// Split on newlines manually, rather than via bufio.Scanner, because
	// Scanner treats EOF as an implicit line terminator — it would
	// happily return a torn, newline-less final chunk as if it were a
	// complete line. We need to distinguish "terminated by a real \n"
	// (complete, trust it) from "whatever's left after the last \n"
	// (the expected shape of a crash mid-write) — only content that was
	// genuinely newline-terminated is applied.
	content := string(data)
	lastNewline := strings.LastIndexByte(content, '\n')
	if lastNewline == -1 {
		// No complete line has ever been written (empty file, or the
		// very first write was itself torn) — nothing to replay.
		if _, err := m.file.Seek(0, 2); err != nil {
			return fmt.Errorf("manifest: seek to end: %w", err)
		}
		return nil
	}

	complete := content[:lastNewline] // everything up to and including the last real \n
	for _, line := range strings.Split(complete, "\n") {
		if line == "" {
			continue
		}
		if err := m.applyLine(line); err != nil {
			return fmt.Errorf("manifest: malformed line %q: %w", line, err)
		}
	}

	if _, err := m.file.Seek(0, 2); err != nil {
		return fmt.Errorf("manifest: seek to end: %w", err)
	}
	return nil
}

func (m *Manifest) applyLine(line string) error {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return fmt.Errorf("expected at least an operation and one ID, got %q", line)
	}

	switch fields[0] {
	case "ADD":
		if len(fields) != 2 {
			return fmt.Errorf("ADD expects exactly one ID, got %q", line)
		}
		m.live[fields[1]] = true
	case "REMOVE":
		for _, id := range fields[1:] {
			delete(m.live, id)
		}
	default:
		return fmt.Errorf("unknown operation %q", fields[0])
	}
	return nil
}

// Add durably records that sstableID is now live, then updates the
// in-memory set. Callers should call this only after the corresponding
// .sst file has already been fully written and fsynced (sstable.Write
// already guarantees this via atomic rename) — Add is the next fact in
// the causal chain, not a replacement for that guarantee.
func (m *Manifest) Add(sstableID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.writeLine("ADD " + sstableID); err != nil {
		return err
	}
	m.live[sstableID] = true
	return nil
}

// Remove durably records that the given SSTable IDs are no longer live,
// as a single line (so "compaction's N old files are now obsolete" is
// one atomic fact, not N separate ones that could partially apply across
// a crash), then updates the in-memory set. Callers should call this
// only after the replacement SSTable's Add has already been durably
// recorded — never remove old files' liveness before the new file that
// replaces them is itself safely committed.
func (m *Manifest) Remove(sstableIDs []string) error {
	if len(sstableIDs) == 0 {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.writeLine("REMOVE " + strings.Join(sstableIDs, " ")); err != nil {
		return err
	}
	for _, id := range sstableIDs {
		delete(m.live, id)
	}
	return nil
}

func (m *Manifest) writeLine(line string) error {
	if _, err := m.file.WriteString(line + "\n"); err != nil {
		return fmt.Errorf("manifest: write line: %w", err)
	}
	return m.file.Sync()
}

// LiveIDs returns every currently live SSTable ID. The returned slice is
// a snapshot copy, safe to use without holding any lock.
func (m *Manifest) LiveIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	ids := make([]string, 0, len(m.live))
	for id := range m.live {
		ids = append(ids, id)
	}
	return ids
}

func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.file.Close()
}
