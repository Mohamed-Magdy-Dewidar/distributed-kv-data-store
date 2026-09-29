package node

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"

	"distributed-kv-datastore/internal/storage/fsutil"
)

var (
	// ErrStaleEpoch is returned by SetMembership for an epoch older than the
	// node's current one. The node keeps its view.
	ErrStaleEpoch = errors.New("stale membership epoch")

	// ErrFingerprintMismatch is returned for a membership received from
	// another node whose claimed fingerprint isn't the one computed from its
	// members: it was corrupted, or built under a different N or ring scheme.
	ErrFingerprintMismatch = errors.New("membership fingerprint does not match its members")

	// ErrMembershipConflict is returned by SetMembership for the node's
	// current epoch with different members, addresses or replication factor.
	// One epoch must mean one membership; the node keeps its view.
	ErrMembershipConflict = errors.New("membership conflict at the same epoch")
)

// Membership returns the node's current epoch and a copy of its members
// (ID → address, this node included unless it has been removed).
func (n *Node) Membership() (epoch uint64, members map[string]string) {
	v := n.membership.Load()
	return v.epoch, maps.Clone(v.members)
}

// SetMembership declares the cluster's membership at epoch, replacing the
// node's view if it is newer. members maps every member's ID to its address;
// this node may be absent, which means it is leaving.
//
//   - members are validated first (see validateMembers); a bad set is an error
//     and changes nothing.
//   - epoch newer than the current one: the new view is written to disk (a
//     persistent node) and only then published, so a view that was announced
//     is one the node will still hold after a restart. If it can't be
//     written, nothing is published. Clients of members that are gone are
//     retired (not closed: an RPC in flight may be using one; Close closes
//     them).
//   - the current epoch with the same fingerprint: (false, nil), a no-op.
//   - the current epoch with a different fingerprint: ErrMembershipConflict.
//   - an older epoch: ErrStaleEpoch.
//
// Calls are serialized. It returns whether the view changed.
func (n *Node) SetMembership(epoch uint64, members map[string]string) (changed bool, err error) {
	return n.setMembership(epoch, members, "")
}

// setMembership is SetMembership for a membership that may have come from
// another node. If claimedFingerprint is non-empty it must equal the
// fingerprint computed from members (and this node's N and ring scheme), or
// the membership is rejected with ErrFingerprintMismatch: a fingerprint off
// the wire is never trusted, only compared.
func (n *Node) setMembership(epoch uint64, members map[string]string, claimedFingerprint string) (changed bool, err error) {
	n.membershipMu.Lock()
	defer n.membershipMu.Unlock()

	if err := validateMembers(members, n.QuorumConfig.N); err != nil {
		return false, err
	}
	v, err := buildView(epoch, members, n.QuorumConfig.N)
	if err != nil {
		return false, err
	}

	if claimedFingerprint != "" && v.fingerprint != claimedFingerprint {
		return false, fmt.Errorf("%w: claimed %.12s, computed %.12s", ErrFingerprintMismatch, claimedFingerprint, v.fingerprint)
	}

	cur := n.membership.Load()
	switch {
	case epoch < cur.epoch:
		return false, fmt.Errorf("%w: epoch %d, node is at %d", ErrStaleEpoch, epoch, cur.epoch)
	case epoch == cur.epoch && v.fingerprint == cur.fingerprint:
		return false, nil
	case epoch == cur.epoch:
		return false, fmt.Errorf("%w: epoch %d is already held with fingerprint %.12s, not %.12s",
			ErrMembershipConflict, epoch, cur.fingerprint, v.fingerprint)
	}

	if n.persistView != nil {
		if err := n.persistView(v); err != nil {
			return false, fmt.Errorf("persist membership epoch %d: %w", epoch, err)
		}
	}
	n.membership.Store(v)
	n.retireClientsNotIn(v)
	n.pruneHealth(v)
	return true, nil
}

// retireClientsNotIn moves the cached client of every peer that v doesn't
// list to the retired list. They are not closed: an RPC in flight (an
// anti-entropy round holds one for its whole run) may still be using one.
func (n *Node) retireClientsNotIn(v *view) {
	n.clientsMu.Lock()
	defer n.clientsMu.Unlock()
	for id, pc := range n.clients {
		if _, ok := v.members[id]; !ok {
			n.retired = append(n.retired, pc.client)
			delete(n.clients, id)
		}
	}
}

// membershipFileName is the persisted view in a persistent node's data dir.
const membershipFileName = "MEMBERSHIP"

type membershipFile struct {
	Epoch       uint64            `json:"epoch"`
	Members     map[string]string `json:"members"`
	Fingerprint string            `json:"fingerprint"`
}

// writeMembership durably stores v as dataDir's MEMBERSHIP file.
func writeMembership(dataDir string, v *view) error {
	data, err := json.MarshalIndent(membershipFile{Epoch: v.epoch, Members: v.members, Fingerprint: v.fingerprint}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	return fsutil.WriteFileAtomic(filepath.Join(dataDir, membershipFileName), data, 0o644)
}

// loadMembership reads dataDir's MEMBERSHIP file into a view for replication
// factor n. found is false if there is no file. A file that can't be parsed,
// holds a member set that fails validation, or whose stored fingerprint
// differs from the one recomputed (so it is corrupt, or was written for a
// different N or ring scheme) is an error: the node must not guess.
func loadMembership(dataDir string, n int) (v *view, found bool, err error) {
	path := filepath.Join(dataDir, membershipFileName)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("membership: read %s: %w", path, err)
	}

	var f membershipFile
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, false, fmt.Errorf("membership: %s is corrupt: %w", path, err)
	}
	if err := validateMembers(f.Members, n); err != nil {
		return nil, false, fmt.Errorf("%s: %w", path, err)
	}
	v, err = buildView(f.Epoch, f.Members, n)
	if err != nil {
		return nil, false, fmt.Errorf("membership: %s: %w", path, err)
	}
	if v.fingerprint != f.Fingerprint {
		return nil, false, fmt.Errorf("membership: %s has fingerprint %.12s, but its members, N=%d and ring scheme %s give %.12s: the file is corrupt or was written for a different configuration",
			path, f.Fingerprint, n, ringScheme, v.fingerprint)
	}
	return v, true, nil
}
