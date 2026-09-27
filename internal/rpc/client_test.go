package rpc

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"distributed-kv-datastore/internal/rpc/pb"
	"distributed-kv-datastore/internal/store"
)

// serveOn starts a raw gRPC server at addr (an empty addr picks a free
// ephemeral port) and returns its actual address and its (abrupt) Stop —
// unlike Listener.Stop's GracefulStop, which sends a GOAWAY that gets an
// already-connected client to reconnect right away regardless of any
// backoff cap. Stop severs the connection outright, the way a crashed or
// partitioned peer would, so a client's reconnect really does go through
// gRPC's backoff.
func serveOn(t *testing.T, addr string) (actualAddr string, stop func()) {
	t.Helper()
	if addr == "" {
		addr = "localhost:0"
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen on %s: %v", addr, err)
	}
	server := grpc.NewServer()
	pb.RegisterKVReplicationServer(server, NewServer(store.NewDataStore("peer"), nil))
	go func() { _ = server.Serve(lis) }()
	return lis.Addr().String(), server.Stop
}

// waitForState blocks until conn reaches want, failing the test if it
// doesn't by deadline.
func waitForState(t *testing.T, conn *grpc.ClientConn, want connectivity.State, deadline time.Time) {
	t.Helper()
	for state := conn.GetState(); state != want; state = conn.GetState() {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		ok := conn.WaitForStateChange(ctx, state)
		cancel()
		if !ok {
			t.Fatalf("never reached state %v (stuck at %v)", want, state)
		}
	}
}

// TestMaxReconnectBackoffLetsAClientReconnectQuickly: gRPC's very first
// reconnect attempt after a connection breaks always waits out the backoff's
// base delay (1s) regardless of any cap — only the attempts after that climb
// toward, and are bounded by, MaxDelay. So a peer that's back well after
// that first attempt has already failed relies entirely on the cap to be
// retried again soon: uncapped, the next attempt isn't due for another
// ~1.6s (and keeps climbing, up to gRPC's 120s default, the longer the
// outage runs); with maxReconnectBackoff capped far below that, the next
// attempt — and so reconnection — follows within roughly the capped
// interval.
//
// The client must be kept actively retrying (grpc.WaitForReady) across the
// outage: an idle channel doesn't retry in the background at all, and a
// plain (non-wait-for-ready) call from idle just redials once immediately,
// bypassing the backoff schedule entirely rather than exercising it.
func TestMaxReconnectBackoffLetsAClientReconnectQuickly(t *testing.T) {
	addr, stop := serveOn(t, "")

	client, err := Dial(addr, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer client.Close()

	client.conn.Connect()
	waitForState(t, client.conn, connectivity.Ready, time.Now().Add(2*time.Second))

	stop()
	// Two belt-and-suspenders ways of keeping the channel actively
	// retrying rather than settling to IDLE (where nothing reconnects in
	// the background until something asks): a standing WaitForReady RPC,
	// and a periodic Connect() nudge. Both bounded to this test's own
	// lifetime and joined before returning, so a run under -count doesn't
	// pile up prior iterations' goroutines still retrying in the
	// background.
	keepAliveCtx, cancelKeepAlive := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancelKeepAlive()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = client.stub.FetchItem(keepAliveCtx, &pb.FetchItemRequest{Key: "probe"}, grpc.WaitForReady(true))
	}()
	nudgeDone := make(chan struct{})
	go func() {
		defer close(nudgeDone)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				client.conn.Connect()
			case <-keepAliveCtx.Done():
				return
			}
		}
	}()

	// Past gRPC's guaranteed 1s first backoff, so the peer comes back only
	// once the client has already failed once and is waiting for its next
	// scheduled attempt — the scenario the cap is meant to shorten.
	time.Sleep(1100 * time.Millisecond)
	restartedAt := time.Now()
	_, stop = serveOn(t, addr)
	defer stop()

	waitForState(t, client.conn, connectivity.Ready, restartedAt.Add(900*time.Millisecond))
	cancelKeepAlive()
	<-done
	<-nudgeDone
}
