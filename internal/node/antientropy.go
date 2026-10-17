package node

import (
	"context"
	"fmt"
	"log"
	"time"

	"distributed-kv-datastore/internal/merkle"
	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/vectorclock"
)

const antiEntropyNumBuckets = 16

// coReplicantPeers returns every member of v but n as an anti-entropy
// candidate. It doesn't try to narrow that to the neighbors that share a
// preference list with n: with defaultVirtualNodesPerPhysical virtual
// nodes each, every pair of nodes shares some keys, so no neighbor would
// be dropped. Which keys are reconciled with each peer is decided per key,
// by sharesReplicaSet.
func (n *Node) coReplicantPeers(v *view) []string {
	peers := make([]string, 0, len(v.members))
	for id := range v.members {
		if id != n.ID {
			peers = append(peers, id)
		}
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
func (n *Node) sharesReplicaSet(v *view, key, peerID string) bool {
	var self, peer bool
	for _, nodeID := range v.ring.GetPreferenceList(key, n.QuorumConfig.N) {
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
	v := n.membership.Load() // one view for the whole bucket
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
		if n.sharesReplicaSet(v, k, peerID) {
			allKeys[k] = true
		}
	}
	for _, k := range remoteKeys {
		if n.sharesReplicaSet(v, k, peerID) {
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

		toPull := missingFrom(localItems, remoteItems)
		for _, item := range toPull {
			if err := n.Store.MergeReplicated(key, item); err != nil {
				return fmt.Errorf("install reconciled %q locally: %w", key, err)
			}
		}
		if len(toPull) > 0 {
			n.aeKeysPulled.Add(1)
		}
		if toPush := missingFrom(remoteItems, localItems); len(toPush) > 0 {
			if err := n.replicate(ctx, v, peerID, key, toPush); err != nil {
				return fmt.Errorf("push reconciled %q to %q: %w", key, peerID, err)
			}
			n.aeKeysPushed.Add(1)
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
// every interval (see startLoop for scheduling and stopping).
//
// The loop only depends on n's outbound connections. A node whose gRPC
// listener is stopped (unreachable by others) keeps running anti-entropy
// outward, pulling from and pushing to its peers.
func (n *Node) StartAntiEntropyLoop(ctx context.Context, interval time.Duration) {
	n.startLoop(ctx, interval, n.runAntiEntropyRound, n.aeTrigger)
}

// testHookAntiEntropyRound, when set, is called with true as a round starts
// and false as it ends. Tests only, to observe how rounds overlap. Always nil
// in production.
var testHookAntiEntropyRound func(start bool)

// TriggerAntiEntropy asks the anti-entropy loop for a round now instead of at
// its next interval. It never blocks: triggers that arrive while one is
// pending or a round is running coalesce into a single further round. It does
// nothing useful on a node whose anti-entropy loop isn't running.
func (n *Node) TriggerAntiEntropy() {
	select {
	case n.aeTrigger <- struct{}{}:
	default:
	}
}

// runAntiEntropyRound runs RunAntiEntropy against every co-replicant peer
// in turn, logging (not stopping on) each failure. Once ctx is canceled it
// starts no further peer: those would only fail. Every round is counted and
// timed for Stats, a round cut short by shutdown included.
func (n *Node) runAntiEntropyRound(ctx context.Context) {
	start := time.Now()
	defer func() {
		d := int64(time.Since(start))
		n.aeRounds.Add(1)
		n.aeLastRound.Store(d)
		n.aeRoundTotal.Add(d)
	}()
	if testHookAntiEntropyRound != nil {
		testHookAntiEntropyRound(true)
		defer testHookAntiEntropyRound(false)
	}
	for _, peerID := range n.coReplicantPeers(n.membership.Load()) {
		if ctx.Err() != nil {
			return
		}
		if n.isDead(peerID) {
			continue // heartbeats say it's down; its turn comes back when it answers
		}
		if err := n.RunAntiEntropy(ctx, peerID); err != nil {
			n.aePeerFailures.Add(1)
			log.Printf("node %s: anti-entropy with %q failed: %v", n.ID, peerID, err)
		}
	}
}
