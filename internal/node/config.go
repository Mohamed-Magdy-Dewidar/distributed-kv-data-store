package node

// QuorumConfig holds cluster-wide quorum settings. N is implicit — it's
// just len(NeighborAddrs)+1 (every node in the cluster, until consistent
// hashing introduces real partitioning).
type QuorumConfig struct {
	W int
	R int
}

func NewQuorumConfig(w, r int) QuorumConfig {
	return QuorumConfig{W: w, R: r}
}
