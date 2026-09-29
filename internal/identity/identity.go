// Package identity gives a node an incarnation: a random ID, fixed for the
// life of its data directory, that keeps the node's vector-clock entries
// apart from those of any earlier node that used the same node ID.
//
// A node ID names a place on the hash ring and in the membership; it can be
// reused by a replacement node started on an empty data directory. If clock
// entries were keyed by the node ID alone, the replacement would restart its
// counter at zero while replicas still hold that ID's old, higher counts, and
// they would drop its writes as older than what they have — while reporting
// success. Keying clock entries by ClockID (node ID plus incarnation) makes
// the replacement a new writer, so its writes survive beside the old ones.
//
// Only the vector clock uses the ClockID. The ring, membership, hint targets,
// peer connections and DataItem.LastUpdatedBy keep the plain node ID.
package identity

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"distributed-kv-datastore/internal/storage/fsutil"
)

// FileName is the identity file in a data directory.
const FileName = "IDENTITY"

const incarnationBytes = 8 // 16 hex characters

type file struct {
	NodeID      string `json:"node_id"`
	Incarnation string `json:"incarnation"`
}

// NewIncarnation returns a fresh random incarnation: 16 hex characters.
func NewIncarnation() string {
	var b [incarnationBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("identity: read random bytes: %v", err)) // crypto/rand does not fail in practice
	}
	return hex.EncodeToString(b[:])
}

// ClockID is the vector-clock entry name for nodeID's incarnation.
func ClockID(nodeID, incarnation string) string {
	return nodeID + "#" + incarnation
}

// LoadOrCreate returns dataDir's incarnation for nodeID, creating and durably
// storing a new one if the directory has none. The identity file is written
// atomically (temp file, fsync, rename, directory fsync), so a crash leaves
// either no file or a complete one.
//
// It fails, rather than adopting or replacing anything, if the file belongs to
// a different node ID (naming both and the directory) or is unreadable: a
// directory holding another node's data must not be silently taken over.
//
// The caller should hold the directory's exclusive lock (the storage engine
// takes it in Open), so two processes can't race to create the file.
func LoadOrCreate(dataDir, nodeID string) (string, error) {
	if nodeID == "" {
		return "", errors.New("identity: node ID must not be empty")
	}
	path := filepath.Join(dataDir, FileName)

	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		var f file
		if err := json.Unmarshal(data, &f); err != nil {
			return "", fmt.Errorf("identity: %s is corrupt: %w", path, err)
		}
		if f.NodeID != nodeID {
			return "", fmt.Errorf("identity: data dir %s belongs to node %q, but this node is %q", dataDir, f.NodeID, nodeID)
		}
		if !validIncarnation(f.Incarnation) {
			return "", fmt.Errorf("identity: %s has an invalid incarnation %q", path, f.Incarnation)
		}
		return f.Incarnation, nil
	case !errors.Is(err, os.ErrNotExist):
		return "", fmt.Errorf("identity: read %s: %w", path, err)
	}

	incarnation := NewIncarnation()
	if err := write(dataDir, file{NodeID: nodeID, Incarnation: incarnation}); err != nil {
		return "", err
	}
	return incarnation, nil
}

func validIncarnation(s string) bool {
	if len(s) != 2*incarnationBytes {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func write(dataDir string, f file) error {
	data, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("identity: encode: %w", err)
	}
	if err := fsutil.WriteFileAtomic(filepath.Join(dataDir, FileName), data, 0o644); err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	return nil
}
