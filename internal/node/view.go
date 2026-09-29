package node

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strings"

	"distributed-kv-datastore/internal/hashring"
)

// ringScheme names how a member set becomes a ring (the vnode naming, the
// hash, the collision rule). It is part of the fingerprint, so nodes that
// would build different rings from the same members can't look alike. Change
// it whenever ring construction changes.
const ringScheme = "ring-v1"

// view is an immutable snapshot of cluster membership: who the members are,
// where each listens, and the hash ring built from exactly that member set.
// Node holds the current one in an atomic pointer; every operation loads it
// once and uses that single snapshot throughout, so a preference list and the
// addresses it is resolved to can never come from two different memberships.
//
// Neither members nor ring is modified after the view is published. A change
// of membership publishes a new view.
type view struct {
	epoch uint64

	// fingerprint identifies what the view means for placement: its members
	// and addresses, the replication factor N, the vnode count and the ring
	// scheme — not the epoch. Two nodes with equal fingerprints build the same
	// ring and route the same way.
	fingerprint string

	members map[string]string // node ID → address, this node included
	ring    *hashring.HashRing
}

// buildView is the view of members at epoch for a cluster with replication
// factor n. It copies members and does not validate them beyond what building
// the ring needs (see validateMembers for the rules SetMembership applies).
func buildView(epoch uint64, members map[string]string, n int) (*view, error) {
	members = maps.Clone(members)
	ring, err := hashring.NewHashRingFromMembers(defaultVirtualNodesPerPhysical, slices.Sorted(maps.Keys(members)))
	if err != nil {
		return nil, err
	}
	return &view{epoch: epoch, fingerprint: fingerprint(members, n), members: members, ring: ring}, nil
}

// initialView is the epoch-0 view of a node id at address with neighbors:
// members are id plus every neighbor. It copies neighborAddrs; the caller's
// map is not kept.
func initialView(id, address string, neighborAddrs map[string]string, n int) (*view, error) {
	members := maps.Clone(neighborAddrs)
	if members == nil {
		members = make(map[string]string, 1)
	}
	members[id] = address
	return buildView(0, members, n)
}

// fingerprint hashes what determines placement: the members and addresses in
// ID order, N, the vnode count and the ring scheme. It is independent of map
// order and of the epoch. Members are written quoted, so no choice of IDs or
// addresses can make two different member sets produce the same bytes.
func fingerprint(members map[string]string, n int) string {
	h := sha256.New()
	for _, id := range slices.Sorted(maps.Keys(members)) {
		fmt.Fprintf(h, "member %q %q\n", id, members[id])
	}
	fmt.Fprintf(h, "n %d\nvnodes %d\nscheme %s\n", n, defaultVirtualNodesPerPhysical, ringScheme)
	return hex.EncodeToString(h.Sum(nil))
}

// validateMembers checks the rules a member set must meet to become a view:
// it is non-empty and has at least n members; every ID is non-empty and free
// of '#' (the clock-ID separator) and NUL (the hint-key separator); every
// address is non-empty; no two members share an address. This node need not
// be a member: leaving is expressed by omitting it.
func validateMembers(members map[string]string, n int) error {
	if len(members) == 0 {
		return invalidf("membership: no members")
	}
	if len(members) < n {
		return invalidf("membership: %d members, fewer than the replication factor N=%d", len(members), n)
	}
	byAddr := make(map[string]string, len(members))
	for _, id := range slices.Sorted(maps.Keys(members)) {
		addr := members[id]
		switch {
		case id == "":
			return invalidf("membership: empty member id")
		case strings.ContainsAny(id, "#\x00"):
			return invalidf("membership: member id %q must not contain '#' or NUL", id)
		case addr == "":
			return invalidf("membership: member %q has an empty address", id)
		}
		if other, dup := byAddr[addr]; dup {
			return invalidf("membership: members %q and %q share address %q", other, id, addr)
		}
		byAddr[addr] = id
	}
	return nil
}
