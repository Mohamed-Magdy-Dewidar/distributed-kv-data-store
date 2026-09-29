package rpc

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/merkle"
	"distributed-kv-datastore/internal/rpc/pb"
	"distributed-kv-datastore/internal/store"
)

// ErrNotReplica is returned by a WriteCoordinator asked to coordinate a
// write for a key it isn't a replica for. It's never forwarded onward: two
// nodes whose rings disagree must fail the write, not bounce it between
// themselves.
var ErrNotReplica = errors.New("not a replica for this key")

// WriteCoordinator coordinates a client write for a key this node
// replicates: it versions the write once, on itself, then replicates it
// and waits for the write quorum. node.Node implements it; it's an
// interface here because node imports rpc, not the other way round.
type WriteCoordinator interface {
	CoordinatePut(ctx context.Context, key string, value any, clientContext map[string]uint32) error
}

// Server implements pb.KVReplicationServer, wiring the gRPC-facing
// Replicate/FetchItem methods to the underlying DataStore, and
// CoordinatePut to coord. It holds no state of its own beyond those
// references — all locking and conflict resolution already live in
// DataStore/resolve.
type Server struct {
	pb.UnimplementedKVReplicationServer
	ds         *store.DataStore
	coord      WriteCoordinator
	membership MembershipService // nil unless coord also implements it
}

// NewServer returns a Server over ds. coord may be nil, in which case
// CoordinatePut is refused with codes.Unimplemented. If coord also implements
// MembershipService (node.Node does), Ping and GetMembership are served by
// it; otherwise they are refused with codes.Unimplemented.
func NewServer(ds *store.DataStore, coord WriteCoordinator) *Server {
	ms, _ := coord.(MembershipService)
	return &Server{ds: ds, coord: coord, membership: ms}
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

// CoordinatePut runs a raw client write forwarded by a node that isn't one
// of key's replicas: the value and context arrive exactly as the client
// supplied them, and s.coord versions the write once, on this node.
//
// It never returns codes.Unavailable. The forwarding node fails over to the
// next replica only on Unavailable — meaning the request never got here —
// so a write this node received and then failed must not look like one.
func (s *Server) CoordinatePut(ctx context.Context, req *pb.CoordinatePutRequest) (*pb.CoordinatePutResponse, error) {
	if s.coord == nil {
		return nil, status.Error(codes.Unimplemented, "this server does not coordinate writes")
	}
	value, err := unmarshalValue(req.Value)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid value for key %q: %v", req.Key, err)
	}

	// An unset context stays nil ("build on whatever versions exist"); a set
	// one stays non-nil even when empty ("build on nothing").
	var clientContext map[string]uint32
	if req.Context != nil {
		clientContext = make(map[string]uint32, len(req.Context.Entries))
		for node, version := range req.Context.Entries {
			clientContext[node] = version
		}
	}

	if err := s.coord.CoordinatePut(ctx, req.Key, value, clientContext); err != nil {
		switch {
		case errors.Is(err, ErrNotReplica):
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return nil, status.FromContextError(err).Err()
		default:
			return nil, status.Errorf(codes.Internal, "coordinated write for key %q failed: %v", req.Key, err)
		}
	}
	return &pb.CoordinatePutResponse{}, nil
}
