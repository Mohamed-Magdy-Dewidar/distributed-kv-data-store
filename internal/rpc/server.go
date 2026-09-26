package rpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/merkle"
	"distributed-kv-datastore/internal/rpc/pb"
	"distributed-kv-datastore/internal/store"
)

// Server implements pb.KVReplicationServer, wiring the gRPC-facing
// Replicate/FetchItem methods to the underlying DataStore. It holds no
// state of its own beyond a reference to the store — all locking and
// conflict resolution already live in DataStore/resolve.
type Server struct {
	pb.UnimplementedKVReplicationServer
	ds *store.DataStore
}

func NewServer(ds *store.DataStore) *Server {
	return &Server{ds: ds}
}

// Replicate accepts one or more sibling items from a peer for a single key
// and merges each into the local store via the same resolve() path a local
// Put uses. If persisting any item fails it returns a gRPC Internal error
// rather than Accepted: false — rpc.Client.Replicate only reports gRPC
// errors, so that's what keeps the sender from counting a failed write
// toward its quorum. Items merged before the failure stay merged: merges
// are causal and idempotent, so a retry or anti-entropy completes them.
func (s *Server) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	if len(req.Items) == 0 {
		return nil, status.Error(codes.InvalidArgument, "items must not be empty")
	}

	for _, protoItem := range req.Items {
		item, err := fromProtoDataItem(protoItem)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid item for key %q: %v", req.Key, err)
		}
		if err := s.ds.MergeReplicated(req.Key, item); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to persist item for key %q: %v", req.Key, err)
		}
	}

	return &pb.ReplicateResponse{Accepted: true}, nil
}

// GetMerkleTree builds a fresh merkle tree over the local store's current
// contents at the requested bucket count and returns it whole — anti-entropy
// does a bulk fetch of the peer's tree and compares locally rather than
// recursing over the network one level at a time. If the local store can't
// be read it returns a gRPC Internal error: a tree built without the data
// would tell the peer this node holds nothing.
func (s *Server) GetMerkleTree(ctx context.Context, req *pb.GetMerkleTreeRequest) (*pb.GetMerkleTreeResponse, error) {
	tree, err := merkle.Build(s.ds, int(req.NumBuckets))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to build merkle tree: %v", err)
	}
	return toProtoTree(tree), nil
}

// GetBucketKeys returns the keys this node currently holds that fall into
// bucketIndex under the same key->bucket mapping merkle.Build uses, so a
// peer that found this bucket divergent via GetMerkleTree can fetch exactly
// the keys it needs to reconcile. A failure to list the local keys is a
// gRPC Internal error, not an empty list.
func (s *Server) GetBucketKeys(ctx context.Context, req *pb.GetBucketKeysRequest) (*pb.GetBucketKeysResponse, error) {
	numBuckets := int(req.NumBuckets)
	bucketIndex := int(req.BucketIndex)

	allKeys, err := s.ds.Keys()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list keys: %v", err)
	}

	var keys []string
	for _, k := range allKeys {
		if merkle.BucketFor(k, numBuckets) == bucketIndex {
			keys = append(keys, k)
		}
	}

	return &pb.GetBucketKeysResponse{Keys: keys}, nil
}

// FetchItem returns the full sibling set the local store currently holds
// for a key. A key that genuinely doesn't exist is not an error — it's
// reported via Found=false, matching store.DataStore.Get's own found flag.
// A failed local read, like a conversion failure partway through, is a
// gRPC Internal error (mirroring Replicate): answering Found=false instead
// would count as a legitimate "not found" vote in the caller's read quorum.
func (s *Server) FetchItem(ctx context.Context, req *pb.FetchItemRequest) (*pb.FetchItemResponse, error) {
	items, found, err := s.ds.Get(req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to read key %q: %v", req.Key, err)
	}
	if !found {
		return &pb.FetchItemResponse{Items: nil, Found: false}, nil
	}

	protoItems := make([]*pb.DataItem, 0, len(items))
	for _, item := range items {
		protoItem, err := toProtoDataItem(item)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to convert stored item for key %q: %v", req.Key, err)
		}
		protoItems = append(protoItems, protoItem)
	}

	return &pb.FetchItemResponse{Items: protoItems, Found: true}, nil
}
