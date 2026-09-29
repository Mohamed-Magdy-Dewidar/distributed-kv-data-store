package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/rpc"
)

// peerHealth is what heartbeats have shown about one peer.
type peerHealth struct {
	misses int    // consecutive failed pings
	dead   bool   // misses reached QuorumConfig.MaxMissedHeartbeats
	epoch  uint64 // the membership epoch in its latest ping reply

	// reachable is whether the most recent ping to the peer was answered.
	// Unlike dead it drops on the first failure: a single failed dial leaves
	// the connection in reconnect backoff, and RPCs on it fail fast until it
	// reconnects, before any threshold is reached.
	reachable bool
}

// errPeerDead is the result a write, read or anti-entropy step gets for a
// peer marked dead, in place of an RPC that would wait for its timeout. It is
// a codes.Unavailable, exactly like a connection the peer refused, so quorum
// accounting and hint creation treat it the same way.
func errPeerDead(peerID string) error {
	return status.Errorf(codes.Unavailable, "peer %q is marked dead by heartbeats", peerID)
}

// isDead reports whether heartbeats have marked peerID dead. A peer that has
// never been pinged, or has left the membership, is not.
func (n *Node) isDead(peerID string) bool {
	n.healthMu.RLock()
	defer n.healthMu.RUnlock()
	h := n.health[peerID]
	return h != nil && h.dead
}

// recordHeartbeat notes the outcome of a ping to peerID. A success resets its
// miss counter and marks it alive; MaxMissedHeartbeats failures in a row mark
// it dead. A peer that is no longer a member is ignored, so a ping that was in
// flight when it left can't bring its entry back.
func (n *Node) recordHeartbeat(peerID string, ok bool) {
	n.healthMu.Lock()
	defer n.healthMu.Unlock()
	if _, member := n.membership.Load().members[peerID]; !member {
		return
	}
	h := n.health[peerID]
	if h == nil {
		h = &peerHealth{}
		n.health[peerID] = h
	}
	wasDead := h.dead
	h.reachable = ok
	if ok {
		h.misses, h.dead = 0, false
	} else {
		h.misses++
		if h.misses >= n.QuorumConfig.MaxMissedHeartbeats {
			h.dead = true
		}
	}
	switch {
	case h.dead && !wasDead:
		log.Printf("node %s: peer %q is dead: %d heartbeats missed", n.ID, peerID, h.misses)
	case !h.dead && wasDead:
		log.Printf("node %s: peer %q is alive again", n.ID, peerID)
	}
}

// pruneHealth drops the health (and conflict) records of peers v doesn't list.
// A peer that joins later starts with none, which reads as alive.
func (n *Node) pruneHealth(v *view) {
	n.healthMu.Lock()
	defer n.healthMu.Unlock()
	for id := range n.health {
		if _, ok := v.members[id]; !ok {
			delete(n.health, id)
		}
	}
	for id := range n.conflictSeen {
		if _, ok := v.members[id]; !ok {
			delete(n.conflictSeen, id)
		}
	}
}

// StartHeartbeatLoop pings the other members every interval (see
// heartbeatRound, and startLoop for scheduling and stopping).
func (n *Node) StartHeartbeatLoop(ctx context.Context, interval time.Duration) {
	n.startLoop(ctx, interval, n.heartbeatRound, nil)
}

// heartbeatRound pings every member of the current view but n, in parallel,
// each bounded by HeartbeatTimeout, and returns once all have answered or
// timed out — so rounds never overlap.
func (n *Node) heartbeatRound(ctx context.Context) {
	v := n.membership.Load()
	var wg sync.WaitGroup
	for id := range v.members {
		if id == n.ID {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.pingPeer(ctx, v, id)
		}()
	}
	wg.Wait()
}

// pingPeer pings one peer, records the outcome, and reacts to the membership
// the peer reports. A reply from a node with a different ID than the one
// pinged means the membership has a wrong address in it: that is a failure,
// and nothing the reply says is acted on.
func (n *Node) pingPeer(ctx context.Context, v *view, peerID string) {
	client, err := n.clientFor(v, peerID)
	if err != nil {
		if ctx.Err() == nil {
			n.recordHeartbeat(peerID, false)
		}
		return
	}

	own := n.membership.Load() // announce the newest membership we hold
	pingCtx, cancel := context.WithTimeout(ctx, n.QuorumConfig.HeartbeatTimeout)
	defer cancel()
	reply, err := client.Ping(pingCtx, rpc.PingInfo{
		SenderID:      n.ID,
		SenderAddress: n.Address,
		Epoch:         own.epoch,
		Fingerprint:   own.fingerprint,
	})
	if ctx.Err() != nil {
		return // shutting down: not the peer's fault
	}
	if err != nil {
		n.recordHeartbeat(peerID, false)
		return
	}
	if reply.NodeID != peerID {
		log.Printf("node %s: ADDRESS MISMATCH: %q at %s answered as node %q; treating %q as unreachable and ignoring its membership",
			n.ID, peerID, v.members[peerID], reply.NodeID, peerID)
		n.recordHeartbeat(peerID, false)
		return
	}
	n.recordHeartbeat(peerID, true)
	n.recordPeerEpoch(peerID, reply.Epoch)

	cur := n.membership.Load()
	switch {
	case reply.Epoch > cur.epoch:
		n.adoptFrom(peerID, func(ctx context.Context) (rpc.MembershipInfo, error) {
			return client.GetMembership(ctx)
		})
	case reply.Epoch == cur.epoch && reply.Fingerprint != cur.fingerprint:
		n.noteConflict(peerID, cur.epoch, reply.Fingerprint)
	}
}

// HandlePing implements rpc.MembershipService: it answers with this node's ID
// and membership, and reacts to the sender's. A sender with a newer epoch has
// its membership fetched in the background, from the address it announced
// (with a one-off connection: the sender may not be a member here yet).
func (n *Node) HandlePing(_ context.Context, from rpc.PingInfo) rpc.PingReply {
	n.pingsReceived.Add(1)
	cur := n.membership.Load()
	switch {
	case from.Epoch > cur.epoch && from.SenderAddress != "":
		address := from.SenderAddress
		n.adoptFrom(from.SenderID, func(ctx context.Context) (rpc.MembershipInfo, error) {
			return rpc.FetchMembership(ctx, address, n.QuorumConfig.ReplicationTimeout, n.QuorumConfig.MaxReconnectBackoff)
		})
	case from.Epoch == cur.epoch && from.Fingerprint != cur.fingerprint:
		n.noteConflict(from.SenderID, cur.epoch, from.Fingerprint)
	}
	return rpc.PingReply{NodeID: n.ID, Epoch: cur.epoch, Fingerprint: cur.fingerprint}
}

// CurrentMembership implements rpc.MembershipService.
func (n *Node) CurrentMembership() rpc.MembershipInfo {
	v := n.membership.Load() // one snapshot: epoch, members and fingerprint agree
	return rpc.MembershipInfo{Epoch: v.epoch, Members: maps.Clone(v.members), Fingerprint: v.fingerprint}
}

// adoptFrom fetches a newer membership with fetch and adopts it, in the
// background and single-flight: while one fetch-and-adopt is running, others
// are dropped (the next heartbeat brings them back if they still matter).
// It does nothing once background work has been stopped.
func (n *Node) adoptFrom(peerID string, fetch func(context.Context) (rpc.MembershipInfo, error)) {
	if !n.adopting.CompareAndSwap(false, true) {
		return
	}
	started := n.goBackground(func(ctx context.Context) {
		defer n.adopting.Store(false)
		fetchCtx, cancel := context.WithTimeout(ctx, n.QuorumConfig.ReplicationTimeout)
		defer cancel()
		info, err := fetch(fetchCtx)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("node %s: fetching membership from %q failed: %v", n.ID, peerID, err)
			}
			return
		}
		n.adoptFetched(peerID, info)
	})
	if !started {
		n.adopting.Store(false)
	}
}

// adoptFetched applies a membership received from peerID. The fingerprint it
// claims is checked against the one computed from its members.
func (n *Node) adoptFetched(peerID string, info rpc.MembershipInfo) {
	changed, err := n.setMembership(info.Epoch, info.Members, info.Fingerprint)
	switch {
	case errors.Is(err, ErrStaleEpoch):
		// We moved on while fetching: nothing to do.
	case errors.Is(err, ErrMembershipConflict):
		n.noteConflict(peerID, info.Epoch, info.Fingerprint)
	case err != nil:
		log.Printf("node %s: rejected membership epoch %d from %q: %v", n.ID, info.Epoch, peerID, err)
	case changed:
		log.Printf("node %s: adopted membership epoch %d from %q", n.ID, info.Epoch, peerID)
	}
}

// noteConflict records that peerID holds a different membership under the
// epoch this node holds. Nothing is adopted and the peer is not marked dead:
// an operator has to reconcile the two. It is counted every time and logged
// when the peer's conflicting fingerprint changes, not on every heartbeat.
func (n *Node) noteConflict(peerID string, epoch uint64, fingerprint string) {
	n.membershipConflicts.Add(1)
	key := fmt.Sprintf("%d/%s", epoch, fingerprint)
	n.healthMu.Lock()
	first := n.conflictSeen[peerID] != key
	n.conflictSeen[peerID] = key
	n.healthMu.Unlock()
	if first {
		log.Printf("node %s: MEMBERSHIP CONFLICT: %q holds epoch %d with fingerprint %.12s, different from ours; neither side will adopt the other's",
			n.ID, peerID, epoch, fingerprint)
	}
}

// goBackground runs fn on its own goroutine, covered by StopBackgroundLoops:
// fn's context is canceled by it, and it waits for fn to return. It reports
// false, without running fn, once background work has been stopped.
func (n *Node) goBackground(fn func(ctx context.Context)) bool {
	n.bgMu.Lock()
	defer n.bgMu.Unlock()
	if n.bgStopped {
		return false
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.bgCancels = append(n.bgCancels, cancel)
	n.bgWG.Add(1)
	go func() {
		defer n.bgWG.Done()
		defer cancel()
		fn(ctx)
	}()
	return true
}
