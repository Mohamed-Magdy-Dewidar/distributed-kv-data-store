package hashring

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// HashRing implements consistent hashing with virtual nodes. Each Node in
// the cluster builds its own independent HashRing at startup, seeded with
// the same deterministic AddNode calls (same node IDs, same order isn't
// even required — hashing is order-independent), so every node computes
// identical preference lists without ever communicating about the ring
// itself.
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

// AddNode adds a physical node to the ring, represented by
// virtualNodesPerPhysical positions.
func (hr *HashRing) AddNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	for i := 0; i < hr.virtualNodesPerPhysical; i++ {
		virtualNodeKey := nodeID + "-" + strconv.Itoa(i)
		pos := hashKey(virtualNodeKey)

		hr.nodeMap[pos] = nodeID
		hr.ring = append(hr.ring, pos)
	}

	sort.Slice(hr.ring, func(i, j int) bool {
		return hr.ring[i] < hr.ring[j]
	})
}

// RemoveNode removes a physical node from the ring. Implemented for
// completeness and future dynamic-membership work; not called anywhere
// yet — cluster membership is static for now (see package docs / project
// notes on why this is deliberately deferred).
func (hr *HashRing) RemoveNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	for i := 0; i < hr.virtualNodesPerPhysical; i++ {
		virtualNodeKey := nodeID + "-" + strconv.Itoa(i)
		pos := hashKey(virtualNodeKey)

		delete(hr.nodeMap, pos)

		idx := sort.Search(len(hr.ring), func(i int) bool {
			return hr.ring[i] >= pos
		})
		if idx < len(hr.ring) && hr.ring[idx] == pos {
			hr.ring = append(hr.ring[:idx], hr.ring[idx+1:]...)
		}
	}
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
