package node

import (
	"context"
	"fmt"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/rpc"
)

// peerClient is a cached connection and the address it was dialed at.
type peerClient struct {
	addr   string
	client *rpc.Client
}

// getOrDialClient is clientFor against the node's current view.
func (n *Node) getOrDialClient(peerID string) (*rpc.Client, error) {
	return n.clientFor(n.membership.Load(), peerID)
}

// dialPeer opens a connection to a peer. Tests replace it to control the
// reconnect backoff; it is rpc.Dial in production.
var dialPeer = rpc.Dial

// clientFor returns the connection to peerID at the address v lists for it,
// dialing one if there is none. A cached connection to a different address
// (the peer moved) is retired rather than closed — an RPC in flight on it
// finishes — and replaced. It refuses to dial once Close has closed the
// cache, so a late caller can't leave a connection nothing will close.
func (n *Node) clientFor(v *view, peerID string) (*rpc.Client, error) {
	addr, ok := v.members[peerID]
	if !ok {
		return nil, fmt.Errorf("unknown peer %q: no address configured", peerID)
	}

	n.clientsMu.Lock()
	defer n.clientsMu.Unlock()

	if n.clientsClosed {
		return nil, fmt.Errorf("node %s is closed: not dialing peer %q", n.ID, peerID)
	}
	if pc, ok := n.clients[peerID]; ok {
		if pc.addr == addr {
			return pc.client, nil
		}
		n.retired = append(n.retired, pc.client)
		delete(n.clients, peerID)
	}

	client, err := dialPeer(addr, n.QuorumConfig.MaxReconnectBackoff)
	if err != nil {
		return nil, fmt.Errorf("dial peer %q at %s: %w", peerID, addr, err)
	}

	n.clients[peerID] = peerClient{addr: addr, client: client}
	return client, nil
}

func (n *Node) FetchItem(ctx context.Context, peerID string, key string) ([]*model.DataItem, bool, error) {
	return n.fetchItem(ctx, n.membership.Load(), peerID, key)
}

func (n *Node) fetchItem(ctx context.Context, v *view, peerID string, key string) ([]*model.DataItem, bool, error) {
	client, err := n.clientFor(v, peerID)
	if err != nil {
		return nil, false, err
	}
	return client.FetchItem(ctx, key)
}

// Replicate now forwards a sibling set, matching Client.Replicate's widened signature.
func (n *Node) Replicate(ctx context.Context, peerID string, key string, items []*model.DataItem) error {
	return n.replicate(ctx, n.membership.Load(), peerID, key, items)
}

func (n *Node) replicate(ctx context.Context, v *view, peerID string, key string, items []*model.DataItem) error {
	client, err := n.clientFor(v, peerID)
	if err != nil {
		return err
	}
	return client.Replicate(ctx, key, items)
}
