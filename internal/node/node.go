package node

import (
	"context"
	"fmt"
	"sync"

	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/store"
)

// Node stores its own data plus static knowledge of its neighbors'
// addresses. Connections to neighbors are dialed lazily on first use and
// cached — see getOrDialClient.
type Node struct {
	ID            string
	Store         *store.DataStore
	Address       string
	NeighborAddrs map[string]string

	clientsMu sync.Mutex
	clients   map[string]*rpc.Client // peer ID -> dialed connection, built lazily
}

func New(id, address string, neighborAddrs map[string]string) *Node {
	return &Node{
		ID:            id,
		Store:         store.NewDataStore(id),
		Address:       address,
		NeighborAddrs: neighborAddrs,
		clients:       make(map[string]*rpc.Client),
	}
}

// getOrDialClient returns a cached connection for peerID, dialing one if
// this is the first time this node has needed to talk to it. Mirrors the
// same "check existing, act under lock" shape as DataStore.Put.
func (n *Node) getOrDialClient(peerID string) (*rpc.Client, error) {
	n.clientsMu.Lock()
	defer n.clientsMu.Unlock()

	if client, ok := n.clients[peerID]; ok {
		return client, nil
	}

	addr, ok := n.NeighborAddrs[peerID]
	if !ok {
		return nil, fmt.Errorf("unknown peer %q: no address configured", peerID)
	}

	client, err := rpc.Dial(addr)
	if err != nil {
		return nil, fmt.Errorf("dial peer %q at %s: %w", peerID, addr, err)
	}

	n.clients[peerID] = client
	return client, nil
}

func (n *Node) Replicate(ctx context.Context, peerID string, key string, item *store.DataItem) error {
	client, err := n.getOrDialClient(peerID)
	if err != nil {
		return err
	}
	return client.Replicate(ctx, key, item)
}

func (n *Node) FetchItem(ctx context.Context, peerID string, key string) ([]*store.DataItem, bool, error) {
	client, err := n.getOrDialClient(peerID)
	if err != nil {
		return nil, false, err
	}
	return client.FetchItem(ctx, key)
}
