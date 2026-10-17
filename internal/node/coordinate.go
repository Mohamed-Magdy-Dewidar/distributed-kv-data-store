package node

import (
	"context"
	"errors"
	"fmt"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/versioning"
)

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
func (n *Node) isReplicaFor(v *view, key string) (isReplica bool, peers []string) {
	preferenceList := v.ring.GetPreferenceList(key, n.QuorumConfig.N)

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
	v := n.membership.Load()
	isReplica, peers := n.isReplicaFor(v, key)
	if !isReplica {
		return n.forwardPut(ctx, v, key, value, context, peers)
	}
	return n.putAsReplica(ctx, v, key, value, context, peers)
}

// CoordinatePut implements rpc.WriteCoordinator: it coordinates a write
// another node forwarded here. It refuses (rpc.ErrNotReplica) rather than
// forwarding again when n isn't one of key's replicas — nodes whose rings
// disagree must fail the write, not bounce it between themselves.
func (n *Node) CoordinatePut(ctx context.Context, key string, value any, clientContext map[string]uint32) error {
	v := n.membership.Load()
	isReplica, peers := n.isReplicaFor(v, key)
	// INVARIANT: this refusal happens before any access to n.Store, so a
	// FailedPrecondition (rpc.ErrNotReplica) always means the write was
	// applied nowhere. forwardPut relies on that to fail over to the next
	// replica without risking a second version of the write under another
	// replica's clock entry. Do not read or write the store above this line.
	// TestCoordinatePutRefusesANonOwnerBeforeAnyStoreAccess guards it.
	if !isReplica {
		return fmt.Errorf("node %s, key %q: %w", n.ID, key, rpc.ErrNotReplica)
	}
	return n.putAsReplica(ctx, v, key, value, clientContext, peers)
}

// putAsReplica applies value locally (counting as one success toward W) —
// the one place the write gets its vector clock, incremented on n's own
// entry — then replicates that versioned item to every peer concurrently,
// returning success as soon as W total acks are collected, or failure as
// soon as reaching W becomes mathematically impossible, without waiting
// for every straggler. peers is key's preference list minus n itself.
//
// The fan-out to every peer always starts, even when the local write alone
// already satisfies W (so the call returns before any peer result arrives):
// Dynamo's model is send to all N, wait for W, not send only as many as W
// requires. Skipping it would leave the other replicas relying solely on
// anti-entropy, and unreachable ones would never get a hint.
//
// The replica RPCs run on a context detached from ctx's cancellation and
// bounded by QuorumConfig.ReplicationTimeout, so they outlive the decision
// and the caller: a slow replica still receives the write. Once decided,
// drainReplication collects the stragglers' results and stores a hint for
// every replica that was Unavailable — only if the local write stands
// (quorum reached, or the caller gave up, which leaves it in place), never
// after a rollback: a hint would deliver an undone write to a node that
// never saw it.
func (n *Node) putAsReplica(ctx context.Context, v *view, key string, value any, clientContext map[string]uint32, peers []string) error {
	// prevVersions is what a failed quorum rolls back to, so a read
	// failure here must stop the write: rolling back to a nil
	// prevVersions would delete key's committed versions outright.
	prevVersions, _, err := n.Store.Get(key)
	if err != nil {
		return fmt.Errorf("local read failed for key %q: %w", key, err)
	}
	item := n.Store.Put(key, value, clientContext)
	// A nil item means the local store couldn't persist the write; the
	// store has logged why. Nothing was written, so there's nothing to roll
	// back, and a nil item must never be sent to peers.
	if item == nil {
		return fmt.Errorf("local write failed for key %q: storage error (see log)", key)
	}

	totalNodes := len(peers) + 1
	successes := 1 // the local write above
	needed := n.QuorumConfig.W

	replCtx, cancelRepl := context.WithTimeout(context.WithoutCancel(ctx), n.QuorumConfig.ReplicationTimeout)
	results := make(chan replicateResult, len(peers))
	for _, peerID := range peers {
		go func(peerID string) {
			if testHookBeforeReplicate != nil {
				testHookBeforeReplicate(peerID)
			}
			if n.isDead(peerID) {
				// Don't wait out ReplicationTimeout on a peer heartbeats have
				// given up on: report it as unreachable, so quorum accounting
				// and hints treat it like any Unavailable.
				results <- replicateResult{peerID: peerID, err: errPeerDead(peerID)}
				return
			}
			err := n.replicate(replCtx, v, peerID, key, []*model.DataItem{item})
			results <- replicateResult{peerID: peerID, err: err}
		}(peerID)
	}

	received := 0
	var unavailable []string
	// decided hands the stragglers to drainReplication; keep says whether
	// the local write stands, and so whether hints may be stored.
	decided := func(keep bool) {
		n.drainReplication(key, item, results, len(peers)-received, unavailable, keep, cancelRepl)
	}

	if successes >= needed {
		decided(true)
		return nil
	}

	// A failed rollback is reported alongside the quorum error: the failed
	// write may still be visible locally.
	rollbackLocalWrite := func(quorumErr error) error {
		n.writeQuorumFailures.Add(1)
		decided(false)
		if err := n.Store.RestoreVersions(key, prevVersions); err != nil {
			return errors.Join(quorumErr, fmt.Errorf("rollback of local write for key %q failed: %w", key, err))
		}
		return quorumErr
	}

	failures := 0
	for i := 0; i < len(peers); i++ {
		select {
		case res := <-results:
			received++
			if res.err != nil {
				failures++
				if status.Code(res.err) == codes.Unavailable {
					unavailable = append(unavailable, res.peerID)
				}
			} else {
				successes++
			}

			if successes >= needed {
				decided(true)
				return nil
			}
			if totalNodes-failures < needed {
				return rollbackLocalWrite(fmt.Errorf("write quorum not reached for key %q: %d/%d acks, need W=%d",
					key, successes, totalNodes, needed))
			}
		case <-ctx.Done():
			decided(true) // no rollback here: the local write stands
			return fmt.Errorf("put canceled for key %q: %w", key, ctx.Err())
		}
	}

	return rollbackLocalWrite(fmt.Errorf("write quorum not reached for key %q: %d/%d acks, need W=%d",
		key, successes, totalNodes, needed))
}

// testHookBeforeReplicate, when set, runs in putAsReplica's per-replica
// goroutine before its Replicate RPC; testHookBeforeStoringHints runs in
// drainReplication before it stores hints. Tests only: they let one hold a
// replica's RPC past the quorum decision, or hold a drain open. Always nil
// in production.
var (
	testHookBeforeReplicate    func(peerID string)
	testHookBeforeStoringHints func(key string)
)

// drainReplication runs once a write's quorum is decided, in the
// background: it waits for the remaining replica results (at most
// ReplicationTimeout away), then — if keep, and n has a hint store —
// stores a hint for every replica that was Unavailable, whether its result
// came before the decision (unavailable) or after. Other errors never
// produce a hint: the replica may have applied the write, and anti-entropy
// covers it otherwise. A hint that can't be stored is only logged: the
// write itself is already decided.
//
// Close waits for every drain. One that would start once Close has begun
// doesn't: it stores no hints and cancels its stragglers.
func (n *Node) drainReplication(key string, item *model.DataItem, results <-chan replicateResult, remaining int, unavailable []string, keep bool, cancel context.CancelFunc) {
	n.drainMu.Lock()
	if n.drainClosing {
		n.drainMu.Unlock()
		cancel()
		return
	}
	n.drainWG.Add(1)
	n.drainMu.Unlock()

	go func() {
		defer n.drainWG.Done()
		defer cancel()
		for range remaining {
			if res := <-results; status.Code(res.err) == codes.Unavailable {
				unavailable = append(unavailable, res.peerID)
			}
		}
		if !keep || len(unavailable) == 0 {
			return
		}
		if testHookBeforeStoringHints != nil {
			testHookBeforeStoringHints(key)
		}
		for _, peerID := range unavailable {
			if err := n.hints.Add(peerID, key, item); err != nil {
				log.Printf("node %s: storing hint for %q (key %q) failed; anti-entropy will cover it: %v", n.ID, peerID, key, err)
			}
		}
	}()
}

// forwardPut hands a write for a key n doesn't replicate to one of the
// replicas that do (replicas is key's full preference list), trying them
// in preference-list order.
//
// It moves on to the next replica only when the write was certainly not
// applied on this one:
//   - the replica is marked dead by heartbeats, or n can't get a connection
//     to it at all;
//   - it is Unavailable — the request never reached it;
//   - it refuses with FailedPrecondition (rpc.ErrNotReplica): its membership
//     says it isn't an owner of key, and it checks that before touching the
//     store or replicating anything.
//
// Any other error (a missed quorum, a timeout) is returned as-is: that
// replica may already have applied the write, and retrying elsewhere would
// version it a second time under a different replica's clock entry.
//
// If no replica takes the write, the error is codes.Unavailable — nothing was
// applied anywhere — with each replica's failure in the message.
func (n *Node) forwardPut(ctx context.Context, v *view, key string, value any, clientContext map[string]uint32, replicas []string) error {
	var failures []error
	for _, replicaID := range replicas {
		if n.isDead(replicaID) {
			failures = append(failures, fmt.Errorf("replica %q: marked dead by heartbeats", replicaID))
			continue
		}
		client, err := n.clientFor(v, replicaID)
		if err != nil {
			failures = append(failures, fmt.Errorf("replica %q: %w", replicaID, err))
			continue
		}
		err = client.CoordinatePut(ctx, key, value, clientContext)
		if err == nil {
			return nil
		}
		if code := status.Code(err); code != codes.Unavailable && code != codes.FailedPrecondition {
			return fmt.Errorf("forward put for key %q to replica %q: %w", key, replicaID, err)
		}
		failures = append(failures, fmt.Errorf("replica %q: %w", replicaID, err))
	}
	return status.Errorf(codes.Unavailable, "forward put for key %q: no replica accepted the write: %v", key, errors.Join(failures...))
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
	v := n.membership.Load()
	isReplica, peers := n.isReplicaFor(v, key)

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
		if n.isDead(peerID) {
			results <- fetchResult{peerID: peerID, err: errPeerDead(peerID)} // a failed vote, without an RPC
			continue
		}
		go func(peerID string) {
			items, found, err := n.fetchItem(ctx, v, peerID, key)
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
				n.readQuorumFailures.Add(1)
				return nil, fmt.Errorf("read quorum not reached for key %q: %d/%d responses, need R=%d",
					key, responses, totalNodes, needed)
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("get canceled for key %q: %w", key, ctx.Err())
		}
	}

	n.readQuorumFailures.Add(1)
	return nil, fmt.Errorf("read quorum not reached for key %q: %d/%d responses, need R=%d",
		key, responses, totalNodes, needed)
}
