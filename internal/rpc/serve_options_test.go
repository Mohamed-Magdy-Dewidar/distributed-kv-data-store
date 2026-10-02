package rpc

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/grpc"

	"distributed-kv-datastore/internal/storetest"
)

// TestServePassesServerOptionsToTheGRPCServer: a unary interceptor given to
// Serve sees every RPC the server handles, with its full method name.
func TestServePassesServerOptionsToTheGRPCServer(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	intercept := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		mu.Lock()
		seen = append(seen, info.FullMethod)
		mu.Unlock()
		return handler(ctx, req)
	}

	listener, err := Serve("localhost:0", storetest.NewStore(t, "node-1"), nil, grpc.UnaryInterceptor(intercept))
	if err != nil {
		t.Fatalf("Serve failed: %v", err)
	}
	defer listener.Stop()

	client, err := Dial(listener.Addr(), 0)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer client.Close()

	if _, _, err := client.FetchItem(context.Background(), "k"); err != nil {
		t.Fatalf("FetchItem failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 1 || seen[0] != "/kvstore.KVReplication/FetchItem" {
		t.Fatalf("interceptor saw %v, want exactly [/kvstore.KVReplication/FetchItem]", seen)
	}
}
