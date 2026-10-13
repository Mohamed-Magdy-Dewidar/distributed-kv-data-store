package rpc

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"distributed-kv-datastore/internal/rpc/pb"
	"distributed-kv-datastore/internal/storetest"
)

// The server allows the pings the client sends: a server that refused them
// would close every idle peer connection with a GOAWAY.
func TestKeepaliveEnforcementAllowsThePeerKeepalive(t *testing.T) {
	if KeepaliveEnforcement.MinTime > PeerKeepalive.Time {
		t.Errorf("the server allows a ping every %v at most, the client pings every %v", KeepaliveEnforcement.MinTime, PeerKeepalive.Time)
	}
	if PeerKeepalive.PermitWithoutStream && !KeepaliveEnforcement.PermitWithoutStream {
		t.Error("the client pings with no RPC open, and the server does not allow it")
	}
}

// gatedProxy forwards TCP connections to a backend, and can freeze them:
// frozen, it keeps every connection open and reads what arrives, but
// forwards nothing either way. To the client, the peer has vanished without
// a FIN or a RST, as a partition or a lost VM makes it.
type gatedProxy struct {
	lis  net.Listener
	gate sync.RWMutex // held for writing while frozen
}

func startGatedProxy(t *testing.T, backend string) *gatedProxy {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &gatedProxy{lis: lis}
	t.Cleanup(func() { lis.Close() })
	go func() {
		for {
			client, err := lis.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", backend)
			if err != nil {
				client.Close()
				continue
			}
			go p.copy(server, client)
			go p.copy(client, server)
		}
	}()
	return p
}

func (p *gatedProxy) copy(dst, src net.Conn) {
	defer dst.Close()
	defer src.Close()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			p.gate.RLock()
			_, werr := dst.Write(buf[:n])
			p.gate.RUnlock()
			if werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *gatedProxy) freeze()      { p.gate.Lock() }
func (p *gatedProxy) unfreeze()    { p.gate.Unlock() }
func (p *gatedProxy) addr() string { return p.lis.Addr().String() }

// A peer that vanishes without closing the connection: with PeerKeepalive,
// the client closes the connection about Time+Timeout after its last answer,
// and once the peer is back, the next RPC goes over a new connection. A
// client without keepalive keeps the dead connection Ready meanwhile.
func TestPeerKeepaliveReplacesAConnectionThatWentSilent(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out a keepalive (about 15s)")
	}
	listener, err := Serve("127.0.0.1:0", storetest.NewStore(t, "peer"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Stop()
	proxy := startGatedProxy(t, listener.Addr())

	client, err := Dial(proxy.addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	// The control: the same connection, without keepalive.
	plain, err := grpc.NewClient(proxy.addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	fetch := func(c pb.KVReplicationClient, d time.Duration) error {
		ctx, cancel := context.WithTimeout(context.Background(), d)
		defer cancel()
		_, err := c.FetchItem(ctx, &pb.FetchItemRequest{Key: "k"})
		return err
	}
	if _, _, err := client.FetchItem(context.Background(), "k"); err != nil {
		t.Fatalf("before the freeze: %v", err)
	}
	if err := fetch(pb.NewKVReplicationClient(plain), 5*time.Second); err != nil {
		t.Fatalf("before the freeze, without keepalive: %v", err)
	}

	proxy.freeze()
	frozen := time.Now()
	if err := fetch(client.stub, 500*time.Millisecond); err == nil {
		t.Fatal("an RPC was answered through the frozen proxy")
	}
	// Leaves Ready once the keepalive gives up: Time+Timeout after the last
	// answer, with some slack.
	limit := frozen.Add(PeerKeepalive.Time + PeerKeepalive.Timeout + 5*time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), limit)
	left := client.conn.WaitForStateChange(ctx, connectivity.Ready)
	cancel()
	if !left {
		t.Fatalf("the connection was still Ready %v after the peer went silent", time.Since(frozen).Round(time.Second))
	}
	t.Logf("with keepalive: the connection left Ready %v after the freeze", time.Since(frozen).Round(100*time.Millisecond))
	if s := plain.GetState(); s != connectivity.Ready {
		t.Errorf("without keepalive the connection left Ready too (%v): the test does not show what keepalive adds", s)
	}

	proxy.unfreeze()
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := fetch(client.stub, time.Second)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no RPC got through 10s after the peer came back: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// An idle peer connection outlives several rounds of keepalive pings on a
// server from Serve; on a server with gRPC's default enforcement, the pings
// get the connection closed ("too_many_pings": with no RPC open, a ping more
// often than every 2 hours is a strike; measured, the third ping, at 30s,
// closed it).
func TestIdlePeerConnectionSurvivesKeepalivePings(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out three keepalive pings (about 35s)")
	}
	// Pings at 10, 20 and 30s.
	idle := 3*PeerKeepalive.Time + 5*time.Second

	ours, err := Serve("127.0.0.1:0", storetest.NewStore(t, "peer"), nil)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup, not defer: the parallel subtests run after this function
	// returns.
	t.Cleanup(ours.Stop)
	defaultAddr, stopDefault := serveOn(t, "127.0.0.1:0") // grpc.NewServer() with no options
	t.Cleanup(stopDefault)

	for _, tc := range []struct {
		name, addr string
		keep       bool
	}{
		{"Serve's enforcement", ours.Addr(), true},
		{"gRPC's default enforcement", defaultAddr, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, err := Dial(tc.addr, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if _, _, err := client.FetchItem(context.Background(), "k"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), idle)
			changed := client.conn.WaitForStateChange(ctx, connectivity.Ready)
			cancel()
			switch {
			case tc.keep && changed:
				t.Fatalf("the idle connection left Ready (now %v): the server refused the keepalive pings", client.conn.GetState())
			case !tc.keep && !changed:
				t.Fatalf("still Ready after %v of pings: the default policy did not close it, so this test does not show the enforcement matters", idle)
			}
		})
	}
}
