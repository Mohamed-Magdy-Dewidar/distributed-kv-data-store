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
// Put uses.
func (s *Server) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	if len(req.Items) == 0 {
		return nil, status.Error(codes.InvalidArgument, "items must not be empty")
	}

	for _, protoItem := range req.Items {
		item, err := fromProtoDataItem(protoItem)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid item for key %q: %v", req.Key, err)
		}
		s.ds.MergeReplicated(req.Key, item)
	}

	return &pb.ReplicateResponse{Accepted: true}, nil
}

// GetMerkleTree builds a fresh merkle tree over the local store's current
// contents at the requested bucket count and returns it whole — anti-entropy
// does a bulk fetch of the peer's tree and compares locally rather than
// recursing over the network one level at a time.
func (s *Server) GetMerkleTree(ctx context.Context, req *pb.GetMerkleTreeRequest) (*pb.GetMerkleTreeResponse, error) {
	tree := merkle.Build(s.ds, int(req.NumBuckets))
	return toProtoTree(tree), nil
}

// GetBucketKeys returns the keys this node currently holds that fall into
// bucketIndex under the same key->bucket mapping merkle.Build uses, so a
// peer that found this bucket divergent via GetMerkleTree can fetch exactly
// the keys it needs to reconcile.
func (s *Server) GetBucketKeys(ctx context.Context, req *pb.GetBucketKeysRequest) (*pb.GetBucketKeysResponse, error) {
	numBuckets := int(req.NumBuckets)
	bucketIndex := int(req.BucketIndex)

	var keys []string
	for _, k := range s.ds.Keys() {
		if merkle.BucketFor(k, numBuckets) == bucketIndex {
			keys = append(keys, k)
		}
	}

	return &pb.GetBucketKeysResponse{Keys: keys}, nil
}

// FetchItem returns the full sibling set the local store currently holds
// for a key. A key that genuinely doesn't exist is not an error — it's
// reported via Found=false, matching store.DataStore.Get's own (items, bool)
// contract. A conversion failure partway through, however, is treated as a
// real error: returning a partial sibling set would silently mislead a
// caller doing quorum-read merging.
func (s *Server) FetchItem(ctx context.Context, req *pb.FetchItemRequest) (*pb.FetchItemResponse, error) {
	items, found := s.ds.Get(req.Key)
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
