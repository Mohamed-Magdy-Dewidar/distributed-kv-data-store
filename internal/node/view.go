package node

import (
	"maps"

	"distributed-kv-datastore/internal/hashring"
)

// view is an immutable snapshot of cluster membership: who the members are,
// where each listens, and the hash ring built from exactly that member set.
// Node holds the current one in an atomic pointer; every operation loads it
// once and uses that single snapshot throughout, so a preference list and the
// addresses it is resolved to can never come from two different memberships.
//
// Neither members nor ring is modified after the view is published. A change
// of membership publishes a new view.
type view struct {
	epoch   uint64
	members map[string]string // node ID → address, this node included
	ring    *hashring.HashRing
}

// initialView is the epoch-0 view of a node id at address with neighbors:
// members are id plus every neighbor, and the ring is a pure function of that
// set (see ringFor). It copies neighborAddrs; the caller's map is not kept.
func initialView(id, address string, neighborAddrs map[string]string) (*view, error) {
	ring, err := ringFor(id, neighborAddrs)
	if err != nil {
		return nil, err
	}
	members := maps.Clone(neighborAddrs)
	if members == nil {
		members = make(map[string]string, 1)
	}
	members[id] = address
	return &view{members: members, ring: ring}, nil
}
