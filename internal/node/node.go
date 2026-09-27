package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/hashring"
	"distributed-kv-datastore/internal/merkle"
	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/storage/engine"
	"distributed-kv-datastore/internal/store"
	"distributed-kv-datastore/internal/vectorclock"
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

	engine    *engine.StorageEngine // persistent nodes only (NewPersistent); nil in memory
	closeOnce sync.Once
	closeErr  error

	// Anti-entropy loops (see StartAntiEntropyLoop). aeWG counts running
	// loop goroutines — a round runs inline in its loop's goroutine, so
	// waiting on aeWG waits out any round in progress. Once aeStopped is
	// set, no new loop starts.
	aeMu      sync.Mutex
	aeCancels []context.CancelFunc
	aeStopped bool
	aeWG      sync.WaitGroup
}

// New builds an in-memory Node and seeds its hash ring with itself plus
// every configured neighbor. Every node in the cluster must be constructed
// with the same set of node IDs (this node's own id plus every neighbor's)
// for all nodes to compute identical preference lists.
func New(id, address string, n, w, r int, neighborAddrs map[string]string) *Node {
	return newNode(id, address, n, w, r, neighborAddrs, store.NewDataStore(id), nil)
}

// NewPersistent is New with the node's data kept on disk: it opens (or
// creates) a StorageEngine at dataDir — replaying its WAL and loading its
// live SSTables, so a node reopened on the same directory gets its data
// back — and runs the node's DataStore on top of it. maxMemtableBytes is
// the engine's flush threshold. The caller must Close the node to close
// the engine cleanly.
func NewPersistent(id, address string, n, w, r int, neighborAddrs map[string]string, dataDir string, maxMemtableBytes int) (*Node, error) {
	e, err := engine.Open(dataDir, maxMemtableBytes)
	if err != nil {
		return nil, fmt.Errorf("node %s: open storage at %s: %w", id, dataDir, err)
	}
	return newNode(id, address, n, w, r, neighborAddrs, store.NewDataStoreWithPersister(id, e), e), nil
}

func newNode(id, address string, n, w, r int, neighborAddrs map[string]string, ds *store.DataStore, e *engine.StorageEngine) *Node {
	ring := hashring.NewHashRing(defaultVirtualNodesPerPhysical)
	ring.AddNode(id)
	for peerID := range neighborAddrs {
		ring.AddNode(peerID)
	}

	return &Node{
		ID:            id,
		Store:         ds,
		Address:       address,
		NeighborAddrs: neighborAddrs,
		QuorumConfig:  NewQuorumConfig(n, w, r),
		Ring:          ring,
		clients:       make(map[string]*rpc.Client),
		engine:        e,
	}
}

// Close releases what n owns: it first stops its anti-entropy loops and
// waits for any round in progress (StopAntiEntropy), so no round can write
// to a closed engine; then closes its cached peer connections and, for a
// persistent node, its StorageEngine (which waits for in-flight flushes,
// stops compaction, and closes the Manifest and WAL). It's safe to call
// more than once; later calls return the first call's result. It does not
// stop n's gRPC listener — the caller owns that and must stop it first, so
// no inbound write reaches a closed engine. Using n after Close returns
// errors rather than panicking.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.StopAntiEntropy()

		var errs []error

		n.clientsMu.Lock()
		for peerID, client := range n.clients {
			if err := client.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close client for %q: %w", peerID, err))
			}
		}
		n.clients = make(map[string]*rpc.Client)
		n.clientsMu.Unlock()

		if n.engine != nil {
			if err := n.engine.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close storage engine: %w", err))
			}
		}
		n.closeErr = errors.Join(errs...)
	})
	return n.closeErr
}

// StartCompactionLoop compacts a persistent node's SSTables every interval
// until ctx is canceled (see StorageEngine.StartCompactionLoop). It's a
// no-op for an in-memory node, which has nothing to compact.
func (n *Node) StartCompactionLoop(ctx context.Context, interval time.Duration) {
	if n.engine != nil {
		n.engine.StartCompactionLoop(ctx, interval)
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
// the replicas a non-replica forwards a write to, in order (see
// forwardPut), or fans a read out to (see Get).
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

// Put stores value under key with W-quorum durability.
//
// When n is one of key's replicas, it coordinates the write itself (see
// putAsReplica). When it isn't, it forwards the raw write — value and
// context exactly as the client supplied them — to one of the replicas,
// which coordinates it instead (see forwardPut). Either way the write is
// versioned exactly once, by a node that stores it: a vector clock tracks
// who committed data, never who merely routed a request.
func (n *Node) Put(ctx context.Context, key string, value any, context map[string]uint32) error {
	isReplica, peers := n.isReplicaFor(key)
	if !isReplica {
		return n.forwardPut(ctx, key, value, context, peers)
	}
	return n.putAsReplica(ctx, key, value, context, peers)
}

// CoordinatePut implements rpc.WriteCoordinator: it coordinates a write
// another node forwarded here. It refuses (rpc.ErrNotReplica) rather than
// forwarding again when n isn't one of key's replicas — nodes whose rings
// disagree must fail the write, not bounce it between themselves.
func (n *Node) CoordinatePut(ctx context.Context, key string, value any, clientContext map[string]uint32) error {
	isReplica, peers := n.isReplicaFor(key)
	if !isReplica {
		return fmt.Errorf("node %s, key %q: %w", n.ID, key, rpc.ErrNotReplica)
	}
	return n.putAsReplica(ctx, key, value, clientContext, peers)
}

// putAsReplica applies value locally (counting as one success toward W) —
// the one place the write gets its vector clock, incremented on n's own
// entry — then replicates that versioned item to every peer concurrently,
// returning success as soon as W total acks are collected, or failure as
// soon as reaching W becomes mathematically impossible, without waiting
// for every straggler. peers is key's preference list minus n itself.
func (n *Node) putAsReplica(ctx context.Context, key string, value any, context map[string]uint32, peers []string) error {
	// prevVersions is what a failed quorum rolls back to, so a read
	// failure here must stop the write: rolling back to a nil
	// prevVersions would delete key's committed versions outright.
	prevVersions, _, err := n.Store.Get(key)
	if err != nil {
		return fmt.Errorf("local read failed for key %q: %w", key, err)
	}
	item := n.Store.Put(key, value, context)
	// A nil item means the local store couldn't persist the write; the
	// store has logged why. Nothing was written, so there's nothing to roll
	// back, and a nil item must never be sent to peers.
	if item == nil {
		return fmt.Errorf("local write failed for key %q: storage error (see log)", key)
	}

	totalNodes := len(peers) + 1
	successes := 1 // the local write above
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

	// A failed rollback is reported alongside the quorum error: the failed
	// write may still be visible locally.
	rollbackLocalWrite := func(quorumErr error) error {
		if err := n.Store.RestoreVersions(key, prevVersions); err != nil {
			return errors.Join(quorumErr, fmt.Errorf("rollback of local write for key %q failed: %w", key, err))
		}
		return quorumErr
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
				return rollbackLocalWrite(fmt.Errorf("write quorum not reached for key %q: %d/%d acks, need W=%d",
					key, successes, totalNodes, needed))
			}
		case <-ctx.Done():
			return fmt.Errorf("put canceled for key %q: %w", key, ctx.Err())
		}
	}

	return rollbackLocalWrite(fmt.Errorf("write quorum not reached for key %q: %d/%d acks, need W=%d",
		key, successes, totalNodes, needed))
}

// forwardPut hands a write for a key n doesn't replicate to one of the
// replicas that do (replicas is key's full preference list), trying them
// in preference-list order. It moves on to the next replica only when one
// is Unavailable — the request never reached it. Any other error (a missed
// quorum, a timeout, a refusal) is returned as-is: that replica may already
// have applied the write, and retrying elsewhere would version it a second
// time under a different replica's clock entry.
func (n *Node) forwardPut(ctx context.Context, key string, value any, clientContext map[string]uint32, replicas []string) error {
	var unavailable []error
	for _, replicaID := range replicas {
		client, err := n.getOrDialClient(replicaID)
		if err != nil {
			return fmt.Errorf("forward put for key %q: %w", key, err)
		}
		err = client.CoordinatePut(ctx, key, value, clientContext)
		if err == nil {
			return nil
		}
		if status.Code(err) != codes.Unavailable {
			return fmt.Errorf("forward put for key %q to replica %q: %w", key, replicaID, err)
		}
		unavailable = append(unavailable, fmt.Errorf("replica %q: %w", replicaID, err))
	}
	return fmt.Errorf("forward put for key %q: no replica reachable: %w", key, errors.Join(unavailable...))
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
//
// A local read error counts as a failed response, exactly like a peer RPC
// error — never as a "not found" vote toward R.
func (n *Node) Get(ctx context.Context, key string) ([]*model.DataItem, error) {
	isReplica, peers := n.isReplicaFor(key)

	var merged []*model.DataItem
	totalNodes := len(peers)
	responses := 0
	failures := 0
	if isReplica {
		totalNodes++
		local, _, err := n.Store.Get(key)
		if err != nil {
			failures = 1
		} else {
			merged = local
			responses = 1 // the local read above
		}
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
// candidate. It doesn't try to narrow that to the neighbors that share a
// preference list with n: with defaultVirtualNodesPerPhysical virtual
// nodes each, every pair of nodes shares some keys, so no neighbor would
// be dropped. Which keys are reconciled with each peer is decided per key,
// by sharesReplicaSet.
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
	localTree, err := merkle.Build(n.Store, antiEntropyNumBuckets)
	if err != nil {
		return fmt.Errorf("build local merkle tree: %w", err)
	}

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

// sharesReplicaSet reports whether both n and peerID are in key's
// preference list — the only keys anti-entropy may reconcile between them.
// A key outside it doesn't belong on one side or the other, and copying it
// there would silently add a replica beyond N that no read ever consults.
func (n *Node) sharesReplicaSet(key, peerID string) bool {
	var self, peer bool
	for _, nodeID := range n.Ring.GetPreferenceList(key, n.QuorumConfig.N) {
		self = self || nodeID == n.ID
		peer = peer || nodeID == peerID
	}
	return self && peer
}

// testHookBeforeReconcileInstall, when set, runs in reconcileBucket after
// both sides' sibling sets for key have been read and before anything is
// installed locally. Tests only: it lets one land a concurrent write in
// that window. Always nil in production.
var testHookBeforeReconcileInstall func(key string)

// reconcileBucket reconciles a single divergent bucket: for every key
// either side holds in that bucket that both n and peerID replicate (see
// sharesReplicaSet; any other key is skipped without being fetched), it
// fetches both sides' sibling sets and sends each side the versions it's
// missing. Every peer version not already covered locally is merged in
// through Store.MergeReplicated — the causal path an incoming Replicate
// uses — and every local version the peer doesn't cover is pushed with
// Replicate, which the peer merges the same way. Nothing is overwritten
// wholesale, so a write that lands on either side after the reads is
// merged with, never lost; the reads only decide what's worth sending.
// The result on both sides is what versioning.MergeSiblings of the two
// sets would give, and a key already in sync sends nothing either way.
//
// Any local storage error aborts the bucket, just as a remote fetch error
// does. Carrying on would treat a failed local read as "this node holds
// nothing here", and silently skip pushing the local versions.
func (n *Node) reconcileBucket(ctx context.Context, peerID string, client *rpc.Client, bucketIdx int) error {
	allLocalKeys, err := n.Store.Keys()
	if err != nil {
		return fmt.Errorf("list local keys: %w", err)
	}
	localKeys := make(map[string]bool)
	for _, k := range allLocalKeys {
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
		if n.sharesReplicaSet(k, peerID) {
			allKeys[k] = true
		}
	}
	for _, k := range remoteKeys {
		if n.sharesReplicaSet(k, peerID) {
			allKeys[k] = true
		}
	}

	for key := range allKeys {
		localItems, _, err := n.Store.Get(key)
		if err != nil {
			return fmt.Errorf("read local %q: %w", key, err)
		}
		remoteItems, _, err := client.FetchItem(ctx, key)
		if err != nil {
			return fmt.Errorf("fetch %q from %q: %w", key, peerID, err)
		}

		if testHookBeforeReconcileInstall != nil {
			testHookBeforeReconcileInstall(key)
		}

		for _, item := range missingFrom(localItems, remoteItems) {
			if err := n.Store.MergeReplicated(key, item); err != nil {
				return fmt.Errorf("install reconciled %q locally: %w", key, err)
			}
		}
		if toPush := missingFrom(remoteItems, localItems); len(toPush) > 0 {
			if err := n.Replicate(ctx, peerID, key, toPush); err != nil {
				return fmt.Errorf("push reconciled %q to %q: %w", key, peerID, err)
			}
		}
	}
	return nil
}

// missingFrom returns the versions in incoming that set doesn't already
// cover — those versioning.Resolve would keep if merged into set, rather
// than drop as superseded by or equal to one of set's own. It compares
// vector clocks, not pointers: the two sets come from different nodes.
func missingFrom(set, incoming []*model.DataItem) []*model.DataItem {
	var missing []*model.DataItem
	for _, item := range incoming {
		covered := false
		for _, have := range set {
			if c := have.VectorClock.Compare(item.VectorClock); c == vectorclock.After || c == vectorclock.Equal {
				covered = true
				break
			}
		}
		if !covered {
			missing = append(missing, item)
		}
	}
	return missing
}

// StartAntiEntropyLoop runs RunAntiEntropy against every co-replicant peer
// on a repeating interval until ctx is canceled or StopAntiEntropy (or
// Close) is called. Each node's ticker is deliberately
// independent/unsynchronized from other nodes' tickers, to avoid a
// thundering-herd effect. It does nothing once StopAntiEntropy has run.
//
// The loop only depends on n's outbound connections. A node whose gRPC
// listener is stopped (as the dashboard's "stop" does, to make it
// unreachable) keeps running anti-entropy outward, pulling from and
// pushing to its peers: "stopped" means unreachable by others, not paused.
//
// NOT YET ENABLED IN cmd/cluster.
func (n *Node) StartAntiEntropyLoop(ctx context.Context, interval time.Duration) {
	n.aeMu.Lock()
	defer n.aeMu.Unlock()
	if n.aeStopped {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	n.aeCancels = append(n.aeCancels, cancel)
	n.aeWG.Add(1)

	ticker := time.NewTicker(interval)
	go func() {
		defer n.aeWG.Done()
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				for _, peerID := range n.coReplicantPeers() {
					if err := n.RunAntiEntropy(ctx, peerID); err != nil {
						log.Printf("node %s: anti-entropy with %q failed: %v", n.ID, peerID, err)
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// StopAntiEntropy cancels every anti-entropy loop n has started and waits
// until each has exited, including any round in progress: a round's RPCs
// fail fast once canceled, and any local install it has already fetched
// data for completes first. After it returns, anti-entropy makes no more
// calls into n's store, and StartAntiEntropyLoop does nothing. Safe to call
// more than once, and when no loop was ever started.
func (n *Node) StopAntiEntropy() {
	n.aeMu.Lock()
	n.aeStopped = true
	for _, cancel := range n.aeCancels {
		cancel()
	}
	n.aeCancels = nil
	n.aeMu.Unlock()

	n.aeWG.Wait()
}
