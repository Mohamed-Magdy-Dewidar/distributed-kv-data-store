package rpc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/rpc/pb"
	"distributed-kv-datastore/internal/store"
	"distributed-kv-datastore/internal/storetest"
	"distributed-kv-datastore/internal/vectorclock"
)

// failingPersister fails every call, standing in for a broken disk.
type failingPersister struct{}

var errDisk = errors.New("disk on fire")

func (failingPersister) Put(string, *model.DataItem) error { return errDisk }
func (failingPersister) GetAll(string) ([]*model.DataItem, bool, error) {
	return nil, false, errDisk
}
func (failingPersister) Restore(string, []*model.DataItem) error { return errDisk }
func (failingPersister) Keys() ([]string, error)                 { return nil, errDisk }

func replicateRequest(t *testing.T) *pb.ReplicateRequest {
	t.Helper()
	item, err := toProtoDataItem(&model.DataItem{
		Value:         "v",
		VectorClock:   vectorclock.FromSnapshot(map[string]uint32{"node-2": 1}),
		LastUpdatedBy: "node-2",
	})
	if err != nil {
		t.Fatalf("toProtoDataItem failed: %v", err)
	}
	return &pb.ReplicateRequest{Key: "foo", Items: []*pb.DataItem{item}}
}

func TestReplicateAcceptsAndStoresOnSuccess(t *testing.T) {
	ds := storetest.NewStore(t, "node-1")
	resp, err := NewServer(ds, nil).Replicate(context.Background(), replicateRequest(t))
	if err != nil || !resp.Accepted {
		t.Fatalf("expected Accepted with no error, got resp=%v err=%v", resp, err)
	}
	if items, found, _ := ds.Get("foo"); !found || len(items) != 1 || items[0].Value != "v" {
		t.Fatalf("expected the replicated item to be stored, got found=%v %v", found, items)
	}
}

// TestReplicateReturnsErrorWhenPersistFails: the sender only sees gRPC
// errors (rpc.Client.Replicate ignores Accepted), so a failed persist must
// surface as one, or the sender counts it toward its write quorum.
func TestReplicateReturnsErrorWhenPersistFails(t *testing.T) {
	ds := store.NewDataStoreWithPersister("node-1", failingPersister{})
	resp, err := NewServer(ds, nil).Replicate(context.Background(), replicateRequest(t))
	if err == nil {
		t.Fatalf("expected an error when persisting fails, got resp=%v", resp)
	}
	if code := status.Code(err); code != codes.Internal {
		t.Fatalf("expected codes.Internal, got %v (%v)", code, err)
	}
}

// TestFetchItemReportsMissingKeyAsNotFound: a key that genuinely doesn't
// exist is Found=false with no error — only a failed read is an error.
func TestFetchItemReportsMissingKeyAsNotFound(t *testing.T) {
	resp, err := NewServer(storetest.NewStore(t, "node-1"), nil).FetchItem(context.Background(), &pb.FetchItemRequest{Key: "missing"})
	if err != nil || resp.Found {
		t.Fatalf("expected Found=false with no error, got resp=%v err=%v", resp, err)
	}
}

// TestFetchItemReturnsErrorWhenReadFails: Found=false would count as a
// legitimate "not found" vote in the caller's read quorum, so a failed
// read must be a gRPC error, as it is for Replicate.
func TestFetchItemReturnsErrorWhenReadFails(t *testing.T) {
	ds := store.NewDataStoreWithPersister("node-1", failingPersister{})
	resp, err := NewServer(ds, nil).FetchItem(context.Background(), &pb.FetchItemRequest{Key: "foo"})
	if err == nil {
		t.Fatalf("expected an error when the read fails, got resp=%v", resp)
	}
	if code := status.Code(err); code != codes.Internal {
		t.Fatalf("expected codes.Internal, got %v (%v)", code, err)
	}
}

// TestGetBucketKeysReturnsErrorWhenKeysFail: an empty key list would tell
// the peer this node holds nothing in the bucket.
func TestGetBucketKeysReturnsErrorWhenKeysFail(t *testing.T) {
	ds := store.NewDataStoreWithPersister("node-1", failingPersister{})
	resp, err := NewServer(ds, nil).GetBucketKeys(context.Background(), &pb.GetBucketKeysRequest{BucketIndex: 0, NumBuckets: 16})
	if err == nil {
		t.Fatalf("expected an error when listing keys fails, got resp=%v", resp)
	}
	if code := status.Code(err); code != codes.Internal {
		t.Fatalf("expected codes.Internal, got %v (%v)", code, err)
	}
}

// TestGetMerkleTreeReturnsErrorWhenBuildFails: a tree built over a store
// that failed to read would describe it to the peer as empty.
func TestGetMerkleTreeReturnsErrorWhenBuildFails(t *testing.T) {
	ds := store.NewDataStoreWithPersister("node-1", failingPersister{})
	resp, err := NewServer(ds, nil).GetMerkleTree(context.Background(), &pb.GetMerkleTreeRequest{NumBuckets: 16})
	if err == nil {
		t.Fatalf("expected an error when building the tree fails, got resp=%v", resp)
	}
	if code := status.Code(err); code != codes.Internal {
		t.Fatalf("expected codes.Internal, got %v (%v)", code, err)
	}
}

// recordingCoordinator records the write it's asked to coordinate and
// fails it with err.
type recordingCoordinator struct {
	key     string
	value   any
	context map[string]uint32
	err     error
}

func (c *recordingCoordinator) CoordinatePut(_ context.Context, key string, value any, clientContext map[string]uint32) error {
	c.key, c.value, c.context = key, value, clientContext
	return c.err
}

// TestCoordinatePutPreservesNilVersusEmptyContext: Store.Put builds on the
// existing versions for a nil context but on nothing for an empty one, so
// the wire must keep the two apart.
func TestCoordinatePutPreservesNilVersusEmptyContext(t *testing.T) {
	for name, tc := range map[string]struct {
		req     *pb.VectorContext
		wantNil bool
	}{
		"unset context arrives nil":       {req: nil, wantNil: true},
		"empty context arrives non-nil":   {req: &pb.VectorContext{}, wantNil: false},
		"populated context arrives as-is": {req: &pb.VectorContext{Entries: map[string]uint32{"node-2": 3}}},
	} {
		t.Run(name, func(t *testing.T) {
			coord := &recordingCoordinator{}
			_, err := NewServer(storetest.NewStore(t, "node-1"), coord).CoordinatePut(context.Background(),
				&pb.CoordinatePutRequest{Key: "foo", Value: []byte(`"v"`), Context: tc.req})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if coord.key != "foo" || coord.value != "v" {
				t.Fatalf("expected key foo, value v; got %q %v", coord.key, coord.value)
			}
			if (coord.context == nil) != tc.wantNil {
				t.Fatalf("expected nil context=%v, got %#v", tc.wantNil, coord.context)
			}
			if tc.req != nil && len(tc.req.Entries) > 0 && coord.context["node-2"] != 3 {
				t.Fatalf("expected the context to arrive unmodified, got %v", coord.context)
			}
		})
	}
}

// TestCoordinatePutNeverReturnsUnavailable: the forwarder fails over only
// on Unavailable, so a write this server received and failed must map to
// some other code.
func TestCoordinatePutNeverReturnsUnavailable(t *testing.T) {
	for name, tc := range map[string]struct {
		coord WriteCoordinator
		want  codes.Code
	}{
		"not a replica":  {coord: &recordingCoordinator{err: fmt.Errorf("node-1: %w", ErrNotReplica)}, want: codes.FailedPrecondition},
		"write failed":   {coord: &recordingCoordinator{err: errors.New("write quorum not reached")}, want: codes.Internal},
		"deadline":       {coord: &recordingCoordinator{err: fmt.Errorf("put canceled: %w", context.DeadlineExceeded)}, want: codes.DeadlineExceeded},
		"no coordinator": {coord: nil, want: codes.Unimplemented},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewServer(storetest.NewStore(t, "node-1"), tc.coord).CoordinatePut(context.Background(),
				&pb.CoordinatePutRequest{Key: "foo", Value: []byte(`"v"`)})
			if code := status.Code(err); code != tc.want {
				t.Fatalf("expected %v, got %v (%v)", tc.want, code, err)
			}
		})
	}
}
