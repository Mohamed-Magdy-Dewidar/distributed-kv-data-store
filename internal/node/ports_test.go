package node

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/store"
)

// Test addresses come in two kinds.
//
// Most nodes are served from the start: reserveAddrs opens a listener on
// 127.0.0.1:0 for each, before any node is built, and serveAt serves on that
// very listener later. The port is never released and bound again, so nothing
// else (another test package, an outbound connection) can take it in between.
//
// A node that must be unreachable while its peers dial it (down from the
// start, or stopped and later restarted on the same address) can't hold a
// listener meanwhile: an open socket nobody serves accepts connections and
// hangs, where a closed port refuses them at once, and that is the
// difference between a timeout and the Unavailable that hints depend on. Such
// nodes get a knownAddr: a port from a fixed range below 32768, which Linux
// (ephemeral ports from 32768) and Windows (from 49152) never hand out to
// anyone else.

var reserved sync.Map // address -> net.Listener not yet served

// reserveAddrs opens a listener on 127.0.0.1:0 for each id and returns their
// addresses. Listeners that are never served are closed when the test ends.
func reserveAddrs(t *testing.T, ids ...string) map[string]string {
	t.Helper()
	addrs := make(map[string]string, len(ids))
	for _, id := range ids {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserving a port for %s: %v", id, err)
		}
		addr := lis.Addr().String()
		reserved.Store(addr, lis)
		addrs[id] = addr
		t.Cleanup(func() {
			if l, ok := reserved.LoadAndDelete(addr); ok {
				l.(net.Listener).Close()
			}
		})
	}
	return addrs
}

// reserveAddr is reserveAddrs for one address.
func reserveAddr(t *testing.T) string {
	t.Helper()
	return reserveAddrs(t, "x")["x"]
}

// This package's known ports are knownPortBase+1 .. knownPortBase+knownPortCount
// (see above). Every test package that needs known ports has its own range:
// node 20001-24999, app 25001-25999, httpapi 26001, rpc 26501.
const (
	knownPortBase  = 20000
	knownPortCount = 4999
)

var nextKnownPort atomic.Int32

// knownAddr returns an address, unique within this test binary, on a port
// nothing else is handed: for a node that is down (connections refused) while
// peers dial it, or that is restarted on the same address.
//
// Ports are never reused within a process: a port handed out earlier may still
// be the target of an earlier test's lingering connection. The counter runs
// on across -count repetitions, so a long enough run exhausts the range; that
// panics rather than spill into another package's range.
func knownAddr() string {
	n := int(nextKnownPort.Add(1))
	if n > knownPortCount {
		panic(fmt.Sprintf("knownAddr: this package's %d known ports (%d-%d) are used up; run fewer -count repetitions, or widen the range",
			knownPortCount, knownPortBase+1, knownPortBase+knownPortCount))
	}
	return fmt.Sprintf("127.0.0.1:%d", knownPortBase+n)
}

// knownAddrs is knownAddr for each id.
func knownAddrs(ids ...string) map[string]string {
	addrs := make(map[string]string, len(ids))
	for _, id := range ids {
		addrs[id] = knownAddr()
	}
	return addrs
}

// serveAt starts serving ds and coord at addr: on its reserved listener if it
// has one, otherwise by binding addr (a known address). The listener is
// stopped at cleanup.
func serveAt(t *testing.T, addr string, ds *store.DataStore, coord rpc.WriteCoordinator) *rpc.Listener {
	t.Helper()
	var l *rpc.Listener
	if lis, ok := reserved.LoadAndDelete(addr); ok {
		l = rpc.ServeListener(lis.(net.Listener), ds, coord)
	} else {
		var err error
		if l, err = rpc.Serve(addr, ds, coord); err != nil {
			t.Fatalf("serve at %s: %v", addr, err)
		}
	}
	t.Cleanup(l.Stop)
	return l
}

// listenAt returns addr's reserved listener, or binds addr (a known address),
// for a test server that isn't a node (a hung or counting peer). The caller
// owns the listener.
func listenAt(t *testing.T, addr string) net.Listener {
	t.Helper()
	if lis, ok := reserved.LoadAndDelete(addr); ok {
		return lis.(net.Listener)
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen %s: %v", addr, err)
	}
	return lis
}
