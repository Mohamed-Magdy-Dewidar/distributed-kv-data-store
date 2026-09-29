package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/rpc/pb"
	"distributed-kv-datastore/internal/versioning"
)

// KV is what the client-facing service needs from the node: quorum reads and
// writes. node.Node implements it; it is an interface here because node
// imports rpc, not the other way round. A server whose coordinator doesn't
// implement it doesn't serve KVClient.
type KV interface {
	// Get returns every sibling version of key, tombstones included.
	Get(ctx context.Context, key string) ([]*model.DataItem, error)

	// Put writes value under key. A nil context builds on whatever versions the
	// coordinator has; a non-nil one (even empty) builds on exactly that.
	Put(ctx context.Context, key string, value any, context map[string]uint32) error
}

// clientService implements pb.KVClientServer over a KV.
type clientService struct {
	pb.UnimplementedKVClientServer
	kv KV
}

// contextFromProto turns a wire context into the map Store.Put takes, keeping
// the difference between an unset context (nil: "build on whatever versions
// exist") and a set one (non-nil, even when empty: "build on nothing").
func contextFromProto(c *pb.VectorContext) map[string]uint32 {
	if c == nil {
		return nil
	}
	out := make(map[string]uint32, len(c.Entries))
	for node, version := range c.Entries {
		out[node] = version
	}
	return out
}

// Get returns the live siblings of a key and the context for the next Put.
// Tombstoned siblings are not returned as values, but their clocks are part of
// the context, so a Put with it supersedes them too. found is false when no
// live sibling remains; the context is returned regardless.
func (s *clientService) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	items, err := s.kv.Get(ctx, req.Key)
	if err != nil {
		return nil, clientError(err)
	}

	values := make([]string, 0, len(items))
	for _, item := range items {
		if item.IsDeleted {
			continue
		}
		v, err := valueString(item.Value)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "value of key %q cannot be returned as a string: %v", req.Key, err)
		}
		values = append(values, v)
	}
	return &pb.GetResponse{
		Values:  values,
		Context: &pb.VectorContext{Entries: versioning.UnionVectorClock(items)},
		Found:   len(values) > 0,
	}, nil
}

// Put writes a value through the node, which coordinates the write itself or
// forwards it to a replica.
func (s *clientService) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if err := s.kv.Put(ctx, req.Key, req.Value, contextFromProto(req.Context)); err != nil {
		return nil, clientError(err)
	}
	return &pb.PutResponse{}, nil
}

// clientError maps an error from the node onto what a client sees: the
// matching code for a canceled or expired call, and Unavailable, with the
// message, for anything else.
func clientError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return status.Error(codes.Unavailable, err.Error())
}

// valueString renders a stored value for a string-typed API. Values put
// through KVClient are strings and come back unchanged; anything else (a value
// written another way) is shown as its JSON encoding.
func valueString(v any) (string, error) {
	if s, ok := v.(string); ok {
		return s, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("encode %T: %w", v, err)
	}
	return string(b), nil
}
