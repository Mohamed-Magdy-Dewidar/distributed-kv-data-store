package node

import (
	"context"
	"time"
)

// PeerStatus is what this node knows about another member from heartbeats.
type PeerStatus struct {
	// Alive is false once the peer has missed MaxMissedHeartbeats pings; a
	// peer that hasn't been pinged yet counts as alive.
	Alive bool `json:"alive"`

	// LastSeenEpoch is the membership epoch in the peer's most recent ping
	// reply, 0 if it has never answered.
	LastSeenEpoch uint64 `json:"lastSeenEpoch"`
}

// IsMember reports whether this node is in its current view. A node that has
// been removed from the membership is not: it is draining or waiting to be
// stopped.
func (n *Node) IsMember() bool {
	_, ok := n.membership.Load().members[n.ID]
	return ok
}

// PeerStatuses returns the health and last-seen epoch of every other member
// of the current view.
func (n *Node) PeerStatuses() map[string]PeerStatus {
	v := n.membership.Load()
	n.healthMu.RLock()
	defer n.healthMu.RUnlock()
	out := make(map[string]PeerStatus, len(v.members))
	for id := range v.members {
		if id == n.ID {
			continue
		}
		st := PeerStatus{Alive: true}
		if h := n.health[id]; h != nil {
			st.Alive = !h.dead
			st.LastSeenEpoch = h.epoch
		}
		out[id] = st
	}
	return out
}

// recordPeerEpoch notes the epoch peerID reported in a ping reply. Like
// recordHeartbeat, it ignores a peer that is no longer a member.
func (n *Node) recordPeerEpoch(peerID string, epoch uint64) {
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
	h.epoch = epoch
}

// allPeersAtLeast reports whether every other member of v has been seen at
// epoch or later.
func (n *Node) allPeersAtLeast(v *view, epoch uint64) bool {
	n.healthMu.RLock()
	defer n.healthMu.RUnlock()
	for id := range v.members {
		if id == n.ID {
			continue
		}
		if h := n.health[id]; h == nil || h.epoch < epoch {
			return false
		}
	}
	return true
}

// drainPollInterval is how often WaitDrained rechecks the peers' epochs.
const drainPollInterval = 50 * time.Millisecond

// WaitDrained blocks until this node has nothing left to do for the membership
// at epoch, or ctx ends.
//
// A node that is in the view is done when its handoff to epoch has completed
// (see WaitHandoff). A node that is not in the view — it is being removed — is
// done when, in addition, every member of the view has been seen (in a ping
// reply) at epoch or later: its data has been pushed, and the nodes that hold
// it now know the membership it was pushed under, so stopping this node loses
// nothing. Which case applies is decided by the view the node holds when its
// handoff completes, so a node that learns of its own removal while waiting
// is held to the stricter condition.
func (n *Node) WaitDrained(ctx context.Context, epoch uint64) error {
	for {
		if err := n.WaitHandoff(ctx, epoch); err != nil {
			return err
		}
		v := n.membership.Load()
		if _, member := v.members[n.ID]; member || n.allPeersAtLeast(v, epoch) {
			return nil
		}
		if !sleepCtx(ctx, drainPollInterval) {
			return ctx.Err()
		}
	}
}
