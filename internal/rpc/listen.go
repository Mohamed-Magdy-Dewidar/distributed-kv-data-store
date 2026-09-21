package rpc

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	"distributed-kv-datastore/internal/rpc/pb"
	"distributed-kv-datastore/internal/store"
)

// Listener bundles a running gRPC server with the means to stop it —
// resolving the "no graceful shutdown" gap from earlier.
type Listener struct {
	grpcServer *grpc.Server
	addr       net.Addr
}

// Serve starts a gRPC server backed by ds. It blocks until the listener is
// bound and ready, then returns a Listener the caller can use to find its
// actual address and to Stop() it — resolving the "no readiness signal"
// race from before.
func Serve(address string, ds *store.DataStore) (*Listener, error) {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", address, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKVReplicationServer(grpcServer, NewServer(ds))

	go func() {
		_ = grpcServer.Serve(lis) // returns when Stop() is called
	}()

	return &Listener{grpcServer: grpcServer, addr: lis.Addr()}, nil
}

func (l *Listener) Addr() string {
	return l.addr.String()
}

func (l *Listener) Stop() {
	l.grpcServer.GracefulStop()
}
