package hashring

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// HashRing implements consistent hashing with virtual nodes. Each Node in
// the cluster builds its own independent HashRing from the cluster's member
// set (see NewHashRingFromMembers). Hashing is order-independent, so every
// node computes identical preference lists without ever communicating about
// the ring itself. A ring is never edited to drop a member: a changed member
// set gets a new ring.
type HashRing struct {
	mu                      sync.RWMutex
	ring                    []uint32
	nodeMap                 map[uint32]string
	virtualNodesPerPhysical int
}

func NewHashRing(virtualNodesPerPhysical int) *HashRing {
	return &HashRing{
		ring:                    make([]uint32, 0),
		nodeMap:                 make(map[uint32]string),
		virtualNodesPerPhysical: virtualNodesPerPhysical,
	}
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// place claims nodeID's virtualNodesPerPhysical positions in nodeMap and
// appends the newly claimed ones to ring, which the caller must re-sort.
//
// Two vnodes (from the same or different physical nodes) can hash to the
// same position. On that collision, whichever node ID sorts lexicographically
// lower keeps the slot, independent of which vnode was placed first — this is
// what makes every independently-built ring (see the package doc) resolve
// the collision the same way, whatever order its nodes were placed in.
// hr.ring never holds a position twice. The caller must hold hr.mu, or own hr
// exclusively.
func (hr *HashRing) place(nodeID string) {
	for i := 0; i < hr.virtualNodesPerPhysical; i++ {
		virtualNodeKey := nodeID + "-" + strconv.Itoa(i)
		pos := hashKey(virtualNodeKey)

		if existing, collided := hr.nodeMap[pos]; collided {
			if nodeID < existing {
				hr.nodeMap[pos] = nodeID
			}
			continue
		}

		hr.nodeMap[pos] = nodeID
		hr.ring = append(hr.ring, pos)
	}
}

func (hr *HashRing) sortRing() {
	sort.Slice(hr.ring, func(i, j int) bool {
		return hr.ring[i] < hr.ring[j]
	})
}

// NewHashRingFromMembers builds a ring holding exactly ids, each represented
// by virtualNodesPerPhysical positions. The ring is a pure function of the
// set of ids: their order doesn't matter, and it is identical to one built by
// AddNode-ing the same ids in any order. Collisions resolve as in AddNode
// (the lower id keeps the slot).
//
// Because a ring is never edited to drop a member, a departed node's
// colliding slots go to whoever else claims them, exactly as in a ring built
// without it. Build a new ring from the new member set instead.
//
// It returns an error naming the offender if ids contains an empty id or the
// same id twice.
func NewHashRingFromMembers(virtualNodesPerPhysical int, ids []string) (*HashRing, error) {
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			return nil, errors.New("hashring: member id must not be empty")
		}
		if _, dup := seen[id]; dup {
			return nil, fmt.Errorf("hashring: duplicate member id %q", id)
		}
		seen[id] = struct{}{}
	}

	hr := NewHashRing(virtualNodesPerPhysical)
	for _, id := range ids {
		hr.place(id)
	}
	hr.sortRing()
	return hr, nil
}

// AddNode adds a physical node to the ring, represented by
// virtualNodesPerPhysical positions, with the collision rule described on
// place.
func (hr *HashRing) AddNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	hr.place(nodeID)
	hr.sortRing()
}

// GetPreferenceList returns up to replicationFactor unique physical nodes
// responsible for key, walking clockwise from key's hash position. If
// fewer than replicationFactor distinct physical nodes exist on the ring,
// the returned list is shorter than requested — callers should not assume
// an exact length.
func (hr *HashRing) GetPreferenceList(key string, replicationFactor int) []string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.ring) == 0 {
		return []string{}
	}

	targetHash := hashKey(key)

	index := sort.Search(len(hr.ring), func(i int) bool {
		return hr.ring[i] >= targetHash
	})
	if index >= len(hr.ring) {
		index = 0
	}

	preferenceList := make([]string, 0, replicationFactor)
	seen := make(map[string]struct{})

	for i := 0; i < len(hr.ring); i++ {
		currentIdx := (index + i) % len(hr.ring)
		pos := hr.ring[currentIdx]
		nodeID := hr.nodeMap[pos]

		if _, alreadySeen := seen[nodeID]; !alreadySeen {
			seen[nodeID] = struct{}{}
			preferenceList = append(preferenceList, nodeID)

			if len(preferenceList) == replicationFactor {
				break
			}
		}
	}

	return preferenceList
}
