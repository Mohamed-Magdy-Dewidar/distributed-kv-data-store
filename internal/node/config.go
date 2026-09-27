package node

import "time"

// DefaultReplicationTimeout is the ReplicationTimeout NewQuorumConfig
// starts with. Deployments set their own (cmd/cluster does).
const DefaultReplicationTimeout = 5 * time.Second

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
}

func NewQuorumConfig(n, w, r int) QuorumConfig {
	return QuorumConfig{N: n, W: w, R: r, ReplicationTimeout: DefaultReplicationTimeout}
}
