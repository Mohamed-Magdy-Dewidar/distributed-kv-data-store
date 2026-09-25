package rpc

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/rpc/pb"
	"distributed-kv-datastore/internal/store"
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
	ds := store.NewDataStore("node-1")
	resp, err := NewServer(ds).Replicate(context.Background(), replicateRequest(t))
	if err != nil || !resp.Accepted {
		t.Fatalf("expected Accepted with no error, got resp=%v err=%v", resp, err)
	}
	if items, found := ds.Get("foo"); !found || len(items) != 1 || items[0].Value != "v" {
		t.Fatalf("expected the replicated item to be stored, got found=%v %v", found, items)
	}
}

// TestReplicateReturnsErrorWhenPersistFails: the sender only sees gRPC
// errors (rpc.Client.Replicate ignores Accepted), so a failed persist must
// surface as one, or the sender counts it toward its write quorum.
func TestReplicateReturnsErrorWhenPersistFails(t *testing.T) {
	ds := store.NewDataStoreWithPersister("node-1", failingPersister{})
	resp, err := NewServer(ds).Replicate(context.Background(), replicateRequest(t))
	if err == nil {
		t.Fatalf("expected an error when persisting fails, got resp=%v", resp)
	}
	if code := status.Code(err); code != codes.Internal {
		t.Fatalf("expected codes.Internal, got %v (%v)", code, err)
	}
}
