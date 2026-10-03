package node

import (
	"time"

	"distributed-kv-datastore/internal/hints"
	"distributed-kv-datastore/internal/storage/engine"
)

// Stats is a snapshot of what the node can report about itself, for
// internal/telemetry to export. Counters count since the node was opened;
// everything else is read from state the node already keeps. Stats knows
// nothing about any metrics format.
type Stats struct {
	// Epoch is the membership epoch of the view the node holds, and Member
	// whether the node is in it.
	Epoch  uint64
	Member bool

	// Peers is the number of other members in the view; PeersAlive those
	// heartbeats have not marked dead, and PeersReachable those whose last
	// ping was answered (see PeerStatus).
	Peers, PeersAlive, PeersReachable int

	// MembershipConflicts counts the times a peer was seen holding a
	// different membership at the same epoch (every heartbeat that shows it,
	// not once per peer); PingsReceived counts heartbeat pings received.
	MembershipConflicts, PingsReceived uint64

	// WriteQuorumFailures and ReadQuorumFailures count writes this node
	// coordinated that did not reach W acks (and were rolled back), and
	// reads that did not reach R responses.
	WriteQuorumFailures, ReadQuorumFailures uint64

	// Data is the node's storage engine; Hints its hint store.
	Data  engine.Stats
	Hints hints.Stats

	// HintsPending is the number of undelivered hinted items as of the end
	// of the last hint-delivery round; HintsPendingKnown is false until a
	// round has run to the end.
	HintsPending      uint64
	HintsPendingKnown bool

	// Handoff is the handoff to the newest view.
	Handoff HandoffStatus
	// HandoffsCompleted counts completed handoffs, HandoffTotal is how long
	// they took together, and HandoffLast how long the last one took.
	HandoffsCompleted         uint64
	HandoffLast, HandoffTotal time.Duration

	// AntiEntropyRounds counts anti-entropy rounds; AntiEntropyLastRound and
	// AntiEntropyTotal are how long the last one and all of them took.
	AntiEntropyRounds                      uint64
	AntiEntropyLastRound, AntiEntropyTotal time.Duration
	// AntiEntropyPeerFailures counts reconciliations with one peer that
	// failed. AntiEntropyKeysPulled and AntiEntropyKeysPushed count keys
	// for which a reconciliation installed versions locally, or sent them
	// to the peer.
	AntiEntropyPeerFailures                      uint64
	AntiEntropyKeysPulled, AntiEntropyKeysPushed uint64
}

// Stats returns the node's current Stats. It takes the heartbeat and
// handoff locks for reading only long enough to copy what it needs, and
// each storage engine's lock likewise (see engine.StorageEngine.Stats).
func (n *Node) Stats() Stats {
	v := n.membership.Load()
	_, member := v.members[n.ID]
	s := Stats{
		Epoch:                   v.epoch,
		Member:                  member,
		MembershipConflicts:     n.membershipConflicts.Load(),
		PingsReceived:           n.pingsReceived.Load(),
		WriteQuorumFailures:     n.writeQuorumFailures.Load(),
		ReadQuorumFailures:      n.readQuorumFailures.Load(),
		Data:                    n.engine.Stats(),
		Hints:                   n.hints.Stats(),
		HintsPending:            n.hintsPending.Load(),
		HintsPendingKnown:       n.hintsPendingKnown.Load(),
		AntiEntropyRounds:       n.aeRounds.Load(),
		AntiEntropyLastRound:    time.Duration(n.aeLastRound.Load()),
		AntiEntropyTotal:        time.Duration(n.aeRoundTotal.Load()),
		AntiEntropyPeerFailures: n.aePeerFailures.Load(),
		AntiEntropyKeysPulled:   n.aeKeysPulled.Load(),
		AntiEntropyKeysPushed:   n.aeKeysPushed.Load(),
	}

	// Peer health, as PeerStatuses reports it, counted rather than copied.
	n.healthMu.RLock()
	for id := range v.members {
		if id == n.ID {
			continue
		}
		s.Peers++
		h := n.health[id]
		if h == nil || !h.dead {
			s.PeersAlive++
		}
		if h != nil && h.reachable {
			s.PeersReachable++
		}
	}
	n.healthMu.RUnlock()

	n.handoffMu.Lock()
	s.Handoff = n.handoffStatus
	s.HandoffsCompleted = n.handoffsCompleted
	s.HandoffLast = n.handoffLast
	s.HandoffTotal = n.handoffTotal
	n.handoffMu.Unlock()

	return s
}
