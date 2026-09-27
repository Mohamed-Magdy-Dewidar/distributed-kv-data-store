package node

import "time"

// DefaultReplicationTimeout is the ReplicationTimeout NewQuorumConfig
// starts with. Deployments set their own (cmd/cluster does).
const DefaultReplicationTimeout = 5 * time.Second

// DefaultMaxReconnectBackoff is the MaxReconnectBackoff NewQuorumConfig
// starts with. Deployments set their own (cmd/cluster does).
const DefaultMaxReconnectBackoff = 5 * time.Second

// QuorumConfig holds cluster-wide replication and quorum settings.
type QuorumConfig struct {
	N int // replication factor — how many nodes hold each key
	W int // write quorum
	R int // read quorum

	// ReplicationTimeout bounds each replica RPC of a write's fan-out.
	// Those RPCs deliberately don't end when the caller's context does:
	// once the write is decided, the replicas still outstanding keep
	// receiving it (or, if unreachable, get a hint) — see putAsReplica.
	ReplicationTimeout time.Duration

	// MaxReconnectBackoff caps gRPC's exponential reconnect backoff (up to
	// 120s by default) for connections this node dials to peers. Without a
	// cap, a peer that comes back after a long outage isn't retried again
	// until whatever delay the backoff had climbed to elapses — silently
	// delaying every kind of traffic to that peer (replication fan-out,
	// anti-entropy, hint delivery), not just the reconnect itself.
	MaxReconnectBackoff time.Duration
}

func NewQuorumConfig(n, w, r int) QuorumConfig {
	return QuorumConfig{N: n, W: w, R: r, ReplicationTimeout: DefaultReplicationTimeout, MaxReconnectBackoff: DefaultMaxReconnectBackoff}
}
