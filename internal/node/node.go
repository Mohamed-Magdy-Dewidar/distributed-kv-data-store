package node

import "distributed-kv-datastore/internal/store"

type Node struct {
	ID      string
	Store   *store.DataStore
	Address string
}

func New(id, address string) *Node {
	return &Node{
		ID:      id,
		Store:   store.NewDataStore(id),
		Address: address,
	}
}

// Replicate pushes item into peer's store for key. This is the in-memory
// stand-in for what an RPC call will do once the network layer exists —
// same method signature, different transport underneath.
func (n *Node) Replicate(peer *Node, key string, item *store.DataItem) {
	peer.Store.MergeReplicated(key, item)
}
