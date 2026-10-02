package rpc

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"distributed-kv-datastore/internal/rpc/pb"
	"distributed-kv-datastore/internal/store"
)

// Listener bundles a running gRPC server with the means to stop it —
// resolving the "no graceful shutdown" gap from earlier.
type Listener struct {
	grpcServer *grpc.Server
	addr       net.Addr
	lis        net.Listener
	serveDone  chan struct{} // closed when the Serve goroutine returns
}

// testHookBeforeServe, when set, runs at the start of a Listener's Serve
// goroutine. Tests only: it lets one stop the server before Serve has seen its
// listener. Always nil in production.
var testHookBeforeServe func()

// Serve starts a gRPC server backed by ds, with coord coordinating the
// client writes other nodes forward here (see Server.CoordinatePut). It
// blocks until the listener is bound and ready, then returns a Listener the
// caller can use to find its actual address and to Stop() it — resolving
// the "no readiness signal" race from before.
//
// opts are passed to the gRPC server as they are: internal/app uses them to
// add interceptors (see internal/telemetry). None are needed to serve.
func Serve(address string, ds *store.DataStore, coord WriteCoordinator, opts ...grpc.ServerOption) (*Listener, error) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", address, err)
	}
	return ServeListener(lis, ds, coord, opts...), nil
}

// ServeListener is Serve on a listener the caller has already opened, for
// example on port 0 so the OS picks a free port that stays bound from then
// on. The returned Listener owns lis: stopping it closes lis.
func ServeListener(lis net.Listener, ds *store.DataStore, coord WriteCoordinator, opts ...grpc.ServerOption) *Listener {
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterKVReplicationServer(grpcServer, NewServer(ds, coord))
	// The client-facing service shares the server (so StopWithin covers it),
	// when the coordinator can serve it. Reflection lets tools like grpcurl
	// list and call the services without the .proto files.
	if kv, ok := coord.(KV); ok {
		pb.RegisterKVClientServer(grpcServer, &clientService{kv: kv})
	}
	reflection.Register(grpcServer)

	l := &Listener{grpcServer: grpcServer, addr: lis.Addr(), lis: lis, serveDone: make(chan struct{})}
	hook := testHookBeforeServe // read here: the goroutine may outlive the test that set it
	go func() {
		defer close(l.serveDone)
		if hook != nil {
			hook()
		}
		_ = grpcServer.Serve(lis) // returns when Stop() is called
	}()
	return l
}

// A stop only closes listeners Serve has already registered, and Serve runs
// in its own goroutine: if the stop came first, the listener would stay bound
// until that goroutine got to run. So every stop closes the listener itself
// (closing it twice is harmless), which frees the port at once.
//
// Waiting for Serve to return as well is only possible once the server has
// fully stopped: gRPC's Serve doesn't return before then. Stop, and
// StopWithin's graceful path, have got there and wait; StopWithin's forced
// path may be behind a handler that never returns, so it only closes.
func (l *Listener) closeListener() {
	l.lis.Close()
}

func (l *Listener) Addr() string {
	return l.addr.String()
}

// Stop stops the server gracefully, waiting for in-flight RPCs. When it
// returns, the port is free.
func (l *Listener) Stop() {
	l.grpcServer.GracefulStop()
	l.closeListener()
	<-l.serveDone
}

// StopWithin runs a graceful stop — waiting for in-flight RPCs to finish,
// as Stop does — but falls back to a forced stop if ctx is done first.
//
// With a handler that ignores cancellation, calling Stop() while a
// GracefulStop() is still in flight sometimes never returns (observed
// blocked at grpc-go v1.84.0 server.go:1709, on Windows and Linux); with
// handlers that honour cancellation it was bounded in 20/20 runs. Stop()
// alone returns immediately even with a blocked handler. StopWithin
// therefore returns as soon as ctx is done and fires Stop() in the
// background without waiting on it. Callers must not assume the gRPC
// server has fully stopped when StopWithin returns; in-flight handlers may
// still be running. The port, though, is free either way (see closeListener).
func (l *Listener) StopWithin(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		l.grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		l.closeListener()
		<-l.serveDone
	case <-ctx.Done():
		go l.grpcServer.Stop()
		l.closeListener() // the port is free even though handlers may still run
	}
}
