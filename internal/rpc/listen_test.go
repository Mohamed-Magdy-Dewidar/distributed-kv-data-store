package rpc

import (
	"context"
	"testing"
	"time"

	"distributed-kv-datastore/internal/store"
)

// blockingCoordinator's CoordinatePut signals started, then blocks until
// release is closed — standing in for an RPC that's still in flight when
// shutdown begins.
type blockingCoordinator struct {
	started chan struct{}
	release chan struct{}
}

func (c *blockingCoordinator) CoordinatePut(context.Context, string, any, map[string]uint32) error {
	close(c.started)
	<-c.release
	return nil
}

// TestStopWithinReturnsWithinDeadlineDespiteBlockedRPC: with one RPC
// permanently in flight (until the test releases it, well after this
// assertion), StopWithin given a short deadline must still return promptly
// — falling back to a forced stop — rather than blocking on GracefulStop
// forever. The bound below is this test's own safety net (so a failure
// reports a clear reason), not a reliance on go test's suite-level timeout.
func TestStopWithinReturnsWithinDeadlineDespiteBlockedRPC(t *testing.T) {
	coord := &blockingCoordinator{started: make(chan struct{}), release: make(chan struct{})}

	listener, err := Serve("localhost:0", store.NewDataStore("node-1"), coord)
	if err != nil {
		t.Fatalf("Serve failed: %v", err)
	}

	client, err := Dial(listener.Addr(), 0)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer client.Close()

	rpcDone := make(chan error, 1)
	go func() {
		rpcDone <- client.CoordinatePut(context.Background(), "k", "v", nil)
	}()
	<-coord.started // the RPC handler is now blocked server-side

	const deadline = 100 * time.Millisecond
	const bound = 2 * time.Second // generous margin over deadline; this test's own timeout, not go test's

	stopCtx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	stopDone := make(chan struct{})
	go func() {
		listener.StopWithin(stopCtx)
		close(stopDone)
	}()

	select {
	case <-stopDone:
	case <-time.After(bound):
		t.Fatalf("StopWithin did not return within %v despite a blocked RPC and a %v deadline", bound, deadline)
	}

	close(coord.release) // let the blocked handler finish so nothing is left running
	<-rpcDone
}
