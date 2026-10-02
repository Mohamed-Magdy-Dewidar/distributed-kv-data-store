package node

// Stats is a snapshot of what the node can report about itself, for
// internal/telemetry to export. Every field is read from state the node
// already keeps; Stats knows nothing about any metrics format.
type Stats struct {
	// Epoch is the membership epoch of the view the node holds.
	Epoch uint64
}

// Stats returns the node's current Stats. It takes no lock: the view is
// read through its atomic pointer.
func (n *Node) Stats() Stats {
	return Stats{Epoch: n.membership.Load().epoch}
}
