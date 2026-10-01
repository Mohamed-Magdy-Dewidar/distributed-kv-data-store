package rpc

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	"distributed-kv-datastore/internal/merkle"
	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/rpc/pb"
)

// Client wraps a gRPC connection to one peer's KVReplication service.
type Client struct {
	conn *grpc.ClientConn
	stub pb.KVReplicationClient
}

// Dial connects to a peer at address. The connection is not pooled or
// retried here — one Client per peer, held for the connection's lifetime.
//
// maxReconnectBackoff caps gRPC's exponential reconnect backoff, which
// otherwise climbs (1s base, x1.6 per attempt) up to a 120s default max
// delay between reconnect attempts to an unreachable peer — every other
// setting (base delay, multiplier, jitter) is left at gRPC's default. Zero
// leaves gRPC's own default max delay in place.
func Dial(address string, maxReconnectBackoff time.Duration) (*Client, error) {
	if maxReconnectBackoff <= 0 {
		return dial(address)
	}
	cfg := backoff.DefaultConfig
	cfg.MaxDelay = maxReconnectBackoff
	return DialBackoff(address, cfg)
}

// DialBackoff is Dial with the whole reconnect backoff given, not just its
// maximum delay. Tests use it to make the wait between reconnect attempts
// long and predictable.
func DialBackoff(address string, cfg backoff.Config) (*Client, error) {
	return dial(address, grpc.WithConnectParams(grpc.ConnectParams{Backoff: cfg}))
}

func dial(address string, opts ...grpc.DialOption) (*Client, error) {
	opts = append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)
	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", address, err)
	}
	return &Client{conn: conn, stub: pb.NewKVReplicationClient(conn)}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// FetchItem retrieves the sibling set this peer currently holds for key.
func (c *Client) FetchItem(ctx context.Context, key string) ([]*model.DataItem, bool, error) {
	resp, err := c.stub.FetchItem(ctx, &pb.FetchItemRequest{Key: key})
	if err != nil {
		return nil, false, err
	}
	if !resp.Found {
		return nil, false, nil
	}

	items := make([]*model.DataItem, 0, len(resp.Items))
	for _, pi := range resp.Items {
		item, err := fromProtoDataItem(pi)
		if err != nil {
			return nil, false, fmt.Errorf("convert response item for key %q: %w", key, err)
		}
		items = append(items, item)
	}
	return items, true, nil
}

// Replicate sends one or more sibling items to this peer for key, in a
// single round-trip.
func (c *Client) Replicate(ctx context.Context, key string, items []*model.DataItem) error {
	protoItems := make([]*pb.DataItem, 0, len(items))
	for _, item := range items {
		protoItem, err := toProtoDataItem(item)
		if err != nil {
			return fmt.Errorf("convert item for key %q: %w", key, err)
		}
		protoItems = append(protoItems, protoItem)
	}

	_, err := c.stub.Replicate(ctx, &pb.ReplicateRequest{Key: key, Items: protoItems})
	return err
}

// CoordinatePut forwards a raw client write — value and context exactly as
// the client supplied them, nil context included — to this peer, which
// must be one of key's replicas, to version, replicate and quorum-check.
func (c *Client) CoordinatePut(ctx context.Context, key string, value any, clientContext map[string]uint32) error {
	valueBytes, err := marshalValue(value)
	if err != nil {
		return fmt.Errorf("convert value for key %q: %w", key, err)
	}

	req := &pb.CoordinatePutRequest{Key: key, Value: valueBytes}
	if clientContext != nil {
		req.Context = &pb.VectorContext{Entries: clientContext}
	}
	_, err = c.stub.CoordinatePut(ctx, req)
	return err
}

func (c *Client) GetMerkleTree(ctx context.Context, numBuckets int) (*merkle.Tree, error) {
	resp, err := c.stub.GetMerkleTree(ctx, &pb.GetMerkleTreeRequest{NumBuckets: int32(numBuckets)})
	if err != nil {
		return nil, err
	}
	return fromProtoTree(resp), nil
}

func (c *Client) GetBucketKeys(ctx context.Context, bucketIndex, numBuckets int) ([]string, error) {
	resp, err := c.stub.GetBucketKeys(ctx, &pb.GetBucketKeysRequest{
		BucketIndex: int32(bucketIndex),
		NumBuckets:  int32(numBuckets),
	})
	if err != nil {
		return nil, err
	}
	return resp.Keys, nil
}
