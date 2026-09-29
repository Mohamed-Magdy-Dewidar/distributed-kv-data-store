package app

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"distributed-kv-datastore/internal/node"
	"distributed-kv-datastore/internal/rpc"
)

// Addresses in this package's tests (see internal/node's ports_test.go for
// the reasoning). A node served by the test itself gets a reserved listener,
// opened on 127.0.0.1:0 and served as is. Anything that must bind by address
// itself (Run and its probe server read addresses from a config), or be down
// while peers dial it, gets a knownAddr: a port below 32768, from a range only
// this package uses, which neither Linux nor Windows hands out as an
// ephemeral port.

var reserved sync.Map // address -> net.Listener not yet served

// reserveAddr opens a listener on 127.0.0.1:0 and returns its address; it is
// closed at the end of the test unless serveRPC takes it.
func reserveAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a port: %v", err)
	}
	addr := lis.Addr().String()
	reserved.Store(addr, lis)
	t.Cleanup(func() {
		if l, ok := reserved.LoadAndDelete(addr); ok {
			l.(net.Listener).Close()
		}
	})
	return addr
}

// This package's known ports are knownPortBase+1 .. knownPortBase+knownPortCount
// (node 20001-24999, app 25001-25999, httpapi 26001, rpc 26501).
const (
	knownPortBase  = 25000
	knownPortCount = 999
)

var nextKnownPort atomic.Int32

// knownAddr returns an address on a port unique within this test binary that
// nothing else is handed. Ports are never reused within a process; the
// counter runs on across -count repetitions and panics when the range is used
// up rather than spill into another package's range.
func knownAddr() string {
	n := int(nextKnownPort.Add(1))
	if n > knownPortCount {
		panic(fmt.Sprintf("knownAddr: this package's %d known ports (%d-%d) are used up; run fewer -count repetitions, or widen the range",
			knownPortCount, knownPortBase+1, knownPortBase+knownPortCount))
	}
	return fmt.Sprintf("127.0.0.1:%d", knownPortBase+n)
}

// serveRPC serves nd at addr — on its reserved listener if it has one,
// otherwise by binding addr — stopped at cleanup.
func serveRPC(t *testing.T, nd *node.Node, addr string) {
	t.Helper()
	var l *rpc.Listener
	if lis, ok := reserved.LoadAndDelete(addr); ok {
		l = rpc.ServeListener(lis.(net.Listener), nd.Store, nd)
	} else {
		var err error
		if l, err = rpc.Serve(addr, nd.Store, nd); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(l.Stop)
}
