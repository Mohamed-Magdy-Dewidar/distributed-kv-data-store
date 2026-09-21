package node

// QuorumConfig holds cluster-wide replication and quorum settings.
type QuorumConfig struct {
	N int // replication factor — how many nodes hold each key
	W int // write quorum
	R int // read quorum
}

func NewQuorumConfig(n, w, r int) QuorumConfig {
	return QuorumConfig{N: n, W: w, R: r}
}
