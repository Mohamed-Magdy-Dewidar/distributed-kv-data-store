package node

import (
	"context"
	"fmt"
	"sync"

	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/store"
)

type Node struct {
	ID            string
	Store         *store.DataStore
	Address       string
	NeighborAddrs map[string]string
	QuorumConfig  QuorumConfig

	clientsMu sync.Mutex
	clients   map[string]*rpc.Client
}

func New(id, address string, w, r int, neighborAddrs map[string]string) *Node {
	return &Node{
		ID:            id,
		Store:         store.NewDataStore(id),
		Address:       address,
		NeighborAddrs: neighborAddrs,
		QuorumConfig:  NewQuorumConfig(w, r),
		clients:       make(map[string]*rpc.Client),
	}
}

// replicaSetFor returns the peer IDs responsible for key. Currently this is
// every configured neighbor (no partitioning yet); once consistent hashing
// exists, this becomes the seam where it plugs in — the fan-out logic
// itself never needs to change.
func (n *Node) replicaSetFor(key string) []string {
	peers := make([]string, 0, len(n.NeighborAddrs))
	for peerID := range n.NeighborAddrs {
		peers = append(peers, peerID)
	}
	return peers
}

type replicateResult struct {
	peerID string
	err    error
}

// Put applies value locally (counting as one success toward W), then fans
// out to every peer in the replica set concurrently, returning success as
// soon as W total acks are collected — or failure as soon as reaching W
// becomes mathematically impossible, without waiting for every straggler.
func (n *Node) Put(ctx context.Context, key string, value any, context map[string]uint32) error {
	prevVersions, _ := n.Store.Get(key)
	item := n.Store.Put(key, value, context)

	peers := n.replicaSetFor(key)
	totalNodes := len(peers) + 1 // +1 for this node
	needed := n.QuorumConfig.W

	successes := 1 // the local write above
	if successes >= needed {
		return nil
	}

	results := make(chan replicateResult, len(peers))
	for _, peerID := range peers {
		go func(peerID string) {
			err := n.Replicate(ctx, peerID, key, item)
			results <- replicateResult{peerID: peerID, err: err}
		}(peerID)
	}

	failures := 0
	for i := 0; i < len(peers); i++ {
		select {
		case res := <-results:
			if res.err != nil {
				failures++
			} else {
				successes++
			}

			if successes >= needed {
				return nil
			}
			if totalNodes-failures < needed {
				n.Store.RestoreVersions(key, prevVersions)
				return fmt.Errorf("write quorum not reached for key %q: %d/%d acks, need W=%d",
					key, successes, totalNodes, needed)
			}
		case <-ctx.Done():
			return fmt.Errorf("put canceled for key %q: %w", key, ctx.Err())
		}
	}

	n.Store.RestoreVersions(key, prevVersions)
	return fmt.Errorf("write quorum not reached for key %q: %d/%d acks, need W=%d",
		key, successes, totalNodes, needed)
}

type fetchResult struct {
	peerID string
	items  []*store.DataItem
	err    error
}

// Get merges its own local sibling set with responses fanned out to peers,
// stopping as soon as R total responses (including local) are collected.
func (n *Node) Get(ctx context.Context, key string) ([]*store.DataItem, error) {
	localItems, _ := n.Store.Get(key)
	merged := localItems

	peers := n.replicaSetFor(key)
	totalNodes := len(peers) + 1
	needed := n.QuorumConfig.R

	responses := 1 // the local read above
	if responses >= needed {
		return merged, nil
	}

	results := make(chan fetchResult, len(peers))
	for _, peerID := range peers {
		go func(peerID string) {
			items, found, err := n.FetchItem(ctx, peerID, key)
			if err != nil {
				results <- fetchResult{peerID: peerID, err: err}
				return
			}
			if !found {
				items = nil
			}
			results <- fetchResult{peerID: peerID, items: items}
		}(peerID)
	}

	failures := 0
	for i := 0; i < len(peers); i++ {
		select {
		case res := <-results:
			if res.err != nil {
				failures++
			} else {
				responses++
				merged = store.MergeSiblings(merged, res.items)
			}

			if responses >= needed {
				return merged, nil
			}
			if totalNodes-failures < needed {
				return nil, fmt.Errorf("read quorum not reached for key %q: %d/%d responses, need R=%d",
					key, responses, totalNodes, needed)
			}
		case <-ctx.Done():
			return nil, fmt.Errorf("get canceled for key %q: %w", key, ctx.Err())
		}
	}

	return nil, fmt.Errorf("read quorum not reached for key %q: %d/%d responses, need R=%d",
		key, responses, totalNodes, needed)
}

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
