package node

import (
	"context"
	"fmt"
	"sync"
	"time"

	"distributed-kv-datastore/internal/hashring"
	"distributed-kv-datastore/internal/merkle"
	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/store"
	"distributed-kv-datastore/internal/versioning"
)

// defaultVirtualNodesPerPhysical is kept at 150 (not bumped to something
// like 500) as a deliberate tradeoff, not an oversight: this project's
// current goal is proving partitioning/routing mechanics work correctly,
// not delivering tight load-balancing. Measured relative deviation in
// per-node primary-key share at 150 vnodes ranges roughly -58% to +91%
// depending on physical node count (see
// internal/hashring.TestNoNodeIsCatastrophicallyOverOrUnderloaded) — a
// real, known limitation of this default, not a bug. Revisit if load
// fairness becomes a goal; 500 vnodes measured closer to ±30-40%.
const defaultVirtualNodesPerPhysical = 150

type Node struct {
	ID            string
	Store         *store.DataStore
	Address       string
	NeighborAddrs map[string]string
	QuorumConfig  QuorumConfig
	Ring          *hashring.HashRing

	clientsMu sync.Mutex
	clients   map[string]*rpc.Client
}

// New builds a Node and seeds its hash ring with itself plus every
// configured neighbor. Every node in the cluster must be constructed with
// the same set of node IDs (this node's own id plus every neighbor's) for
// all nodes to compute identical preference lists.
func New(id, address string, n, w, r int, neighborAddrs map[string]string) *Node {
	ring := hashring.NewHashRing(defaultVirtualNodesPerPhysical)
	ring.AddNode(id)
	for peerID := range neighborAddrs {
		ring.AddNode(peerID)
	}

	return &Node{
		ID:            id,
		Store:         store.NewDataStore(id),
		Address:       address,
		NeighborAddrs: neighborAddrs,
		QuorumConfig:  NewQuorumConfig(n, w, r),
		Ring:          ring,
		clients:       make(map[string]*rpc.Client),
	}
}

// isReplicaFor reports whether n itself is one of the N nodes the hash
// ring assigns as replicas for key, and returns peers: the other nodes in
// that preference list to fan Put/Get out to.
//
// One loop serves both cases: when n IS a replica, excluding self from the
// preference list leaves exactly the peers it needs acks from beyond its
// own local copy (length N-1). When n is NOT a replica, self was never in
// the list to begin with, so the same exclusion loop is a no-op and peers
// already comes back as the *full* preference list (length N) — exactly
// who a pure coordinator (one holding no local copy at all) needs to fan
// out to, since none of them is "self" to skip.
func (n *Node) isReplicaFor(key string) (isReplica bool, peers []string) {
	preferenceList := n.Ring.GetPreferenceList(key, n.QuorumConfig.N)

	peers = make([]string, 0, len(preferenceList))
	for _, nodeID := range preferenceList {
		if nodeID == n.ID {
			isReplica = true
			continue
		}
		peers = append(peers, nodeID)
	}
	return isReplica, peers
}

type replicateResult struct {
	peerID string
	err    error
}

// Put applies value locally (counting as one success toward W) when n is
// itself one of key's replicas, then fans out to every peer concurrently,
// returning success as soon as W total acks are collected — or failure as
// soon as reaching W becomes mathematically impossible, without waiting
// for every straggler.
//
// When n is NOT one of key's replicas, it acts as a pure coordinator: no
// local copy is written, peers is the full N-node preference list (see
// isReplicaFor), and the item it forwards is built via Store.BuildItem —
// the same vector-clock derivation Store.Put uses, minus the local write —
// so replicas still receive a properly versioned item without n ever
// holding a copy itself.
func (n *Node) Put(ctx context.Context, key string, value any, context map[string]uint32) error {
	isReplica, peers := n.isReplicaFor(key)

	var prevVersions []*model.DataItem
	var item *model.DataItem
	if isReplica {
		prevVersions, _ = n.Store.Get(key)
		item = n.Store.Put(key, value, context)
	} else {
		item = n.Store.BuildItem(key, value, context)
	}

	totalNodes := len(peers)
	successes := 0
	if isReplica {
		totalNodes++
		successes = 1 // the local write above
	}
	needed := n.QuorumConfig.W

	if successes >= needed {
		return nil
	}

	results := make(chan replicateResult, len(peers))
	for _, peerID := range peers {
		go func(peerID string) {
			err := n.Replicate(ctx, peerID, key, []*model.DataItem{item})
			results <- replicateResult{peerID: peerID, err: err}
		}(peerID)
	}

	// rollbackLocalWrite is a no-op when n isn't a replica — there is no
	// local write to undo. Guarding explicitly here (rather than relying
	// on prevVersions being nil) matters because RestoreVersions(key, nil)
	// means "delete key" — calling it unconditionally in the non-replica
	// branch would wipe out any unrelated data this node happens to hold
	// under key from before a ring topology change, which isn't this
	// failure path's job to touch.
	rollbackLocalWrite := func() {
		if isReplica {
			n.Store.RestoreVersions(key, prevVersions)
		}
	}

	failures := 0
	for i := 0; i < len(peers); i++ {
		select {
		case res := <-results:
			if res.err != nil {
				failures++
			} else {
				successes++
			}

			if successes >= needed {
				return nil
			}
			if totalNodes-failures < needed {
				rollbackLocalWrite()
				return fmt.Errorf("write quorum not reached for key %q: %d/%d acks, need W=%d",
					key, successes, totalNodes, needed)
			}
		case <-ctx.Done():
			return fmt.Errorf("put canceled for key %q: %w", key, ctx.Err())
		}
	}

	rollbackLocalWrite()
	return fmt.Errorf("write quorum not reached for key %q: %d/%d acks, need W=%d",
		key, successes, totalNodes, needed)
}

type fetchResult struct {
	peerID string
	items  []*model.DataItem
	err    error
}

// Get merges its own local sibling set with responses fanned out to peers,
// stopping as soon as R total responses (including local) are collected.
//
// When n is NOT one of key's replicas, it never had a local copy to merge
// in, and peers is the full N-node preference list — see isReplicaFor.
func (n *Node) Get(ctx context.Context, key string) ([]*model.DataItem, error) {
	isReplica, peers := n.isReplicaFor(key)

	var merged []*model.DataItem
	totalNodes := len(peers)
	responses := 0
	if isReplica {
		merged, _ = n.Store.Get(key)
		totalNodes++
		responses = 1 // the local read above
	}
	needed := n.QuorumConfig.R

	if responses >= needed {
		return merged, nil
	}

	results := make(chan fetchResult, len(peers))
	for _, peerID := range peers {
		go func(peerID string) {
			items, found, err := n.FetchItem(ctx, peerID, key)
			if err != nil {
				results <- fetchResult{peerID: peerID, err: err}
				return
			}
			if !found {
				items = nil
			}
			results <- fetchResult{peerID: peerID, items: items}
		}(peerID)
	}

	failures := 0
	for i := 0; i < len(peers); i++ {
		select {
		case res := <-results:
			if res.err != nil {
				failures++
			} else {
				responses++
				merged = versioning.MergeSiblings(merged, res.items)
			}

			if responses >= needed {
				return merged, nil
			}
			if totalNodes-failures < needed {
				return nil, fmt.Errorf("read quorum not reached for key %q: %d/%d responses, need R=%d",
					key, responses, totalNodes, needed)
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("get canceled for key %q: %w", key, ctx.Err())
		}
	}

	return nil, fmt.Errorf("read quorum not reached for key %q: %d/%d responses, need R=%d",
		key, responses, totalNodes, needed)
}

func (n *Node) getOrDialClient(peerID string) (*rpc.Client, error) {
	n.clientsMu.Lock()
	defer n.clientsMu.Unlock()

	if client, ok := n.clients[peerID]; ok {
		return client, nil
	}

	addr, ok := n.NeighborAddrs[peerID]
	if !ok {
		return nil, fmt.Errorf("unknown peer %q: no address configured", peerID)
	}

	client, err := rpc.Dial(addr)
	if err != nil {
		return nil, fmt.Errorf("dial peer %q at %s: %w", peerID, addr, err)
	}

	n.clients[peerID] = client
	return client, nil
}

func (n *Node) FetchItem(ctx context.Context, peerID string, key string) ([]*model.DataItem, bool, error) {
	client, err := n.getOrDialClient(peerID)
	if err != nil {
		return nil, false, err
	}
	return client.FetchItem(ctx, key)
}

const antiEntropyNumBuckets = 16

// Replicate now forwards a sibling set, matching Client.Replicate's widened signature.
func (n *Node) Replicate(ctx context.Context, peerID string, key string, items []*model.DataItem) error {
	client, err := n.getOrDialClient(peerID)
	if err != nil {
		return err
	}
	return client.Replicate(ctx, key, items)
}

// coReplicantPeers returns every configured neighbor as an anti-entropy
// candidate. This is a deliberate simplification, not a precise
// "which nodes actually share a preference list with me" calculation — an
// accepted approximation given the current small, static cluster size.
func (n *Node) coReplicantPeers() []string {
	seen := make(map[string]bool)
	for peerID := range n.NeighborAddrs {
		seen[peerID] = true
	}
	peers := make([]string, 0, len(seen))
	for id := range seen {
		peers = append(peers, id)
	}
	return peers
}

// RunAntiEntropy reconciles n's data with peerID's: it builds a local
// Merkle tree, fetches the peer's tree, and reconciles only the buckets
// that diverge.
func (n *Node) RunAntiEntropy(ctx context.Context, peerID string) error {
	localTree := merkle.Build(n.Store, antiEntropyNumBuckets)

	client, err := n.getOrDialClient(peerID)
	if err != nil {
		return fmt.Errorf("dial %q for anti-entropy: %w", peerID, err)
	}

	remoteTree, err := client.GetMerkleTree(ctx, antiEntropyNumBuckets)
	if err != nil {
		return fmt.Errorf("fetch merkle tree from %q: %w", peerID, err)
	}

	diverged := merkle.DivergentBuckets(localTree, remoteTree)
	if len(diverged) == 0 {
		return nil
	}

	for _, bucketIdx := range diverged {
		if err := n.reconcileBucket(ctx, peerID, client, bucketIdx); err != nil {
			return fmt.Errorf("reconcile bucket %d with %q: %w", bucketIdx, peerID, err)
		}
	}
	return nil
}

// reconcileBucket reconciles a single divergent bucket: for every key
// either side holds in that bucket, it fetches both sides' sibling sets,
// merges them via versioning.MergeSiblings, and pushes the merged result back to
// whichever side differs from it.
func (n *Node) reconcileBucket(ctx context.Context, peerID string, client *rpc.Client, bucketIdx int) error {
	localKeys := make(map[string]bool)
	for _, k := range n.Store.Keys() {
		if merkle.BucketFor(k, antiEntropyNumBuckets) == bucketIdx {
			localKeys[k] = true
		}
	}

	remoteKeys, err := client.GetBucketKeys(ctx, bucketIdx, antiEntropyNumBuckets)
	if err != nil {
		return err
	}

	allKeys := make(map[string]bool, len(localKeys))
	for k := range localKeys {
		allKeys[k] = true
	}
	for _, k := range remoteKeys {
		allKeys[k] = true
	}

	for key := range allKeys {
		localItems, _ := n.Store.Get(key)
		remoteItems, _, err := client.FetchItem(ctx, key)
		if err != nil {
			return fmt.Errorf("fetch %q from %q: %w", key, peerID, err)
		}

		merged := versioning.MergeSiblings(localItems, remoteItems)

		if !itemSetsEqual(merged, localItems) {
			n.Store.RestoreVersions(key, merged)
		}
		if !itemSetsEqual(merged, remoteItems) {
			if err := n.Replicate(ctx, peerID, key, merged); err != nil {
				return fmt.Errorf("push reconciled %q to %q: %w", key, peerID, err)
			}
		}
	}
	return nil
}

// itemSetsEqual is a shallow pointer-identity comparison: merged always
// contains either the exact same *DataItem pointers as one input (nothing
// changed) or a genuinely different set (resolve() dropped or added
// something), so pointer equality is sufficient.
func itemSetsEqual(a, b []*model.DataItem) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// StartAntiEntropyLoop runs RunAntiEntropy against every co-replicant peer
// on a repeating interval until ctx is canceled. Each node's ticker is
// deliberately independent/unsynchronized from other nodes' tickers, to
// avoid a thundering-herd effect.
func (n *Node) StartAntiEntropyLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				for _, peerID := range n.coReplicantPeers() {
					_ = n.RunAntiEntropy(ctx, peerID)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}
