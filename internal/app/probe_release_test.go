package app

import (
	"net"
	"testing"
	"time"
)

// The probe server stopped before its Serve goroutine has started (Run failing
// right after start) still frees its port by the time stop returns.
func TestProbeStopFreesThePortEvenBeforeServeStarts(t *testing.T) {
	gate := make(chan struct{})
	testHookBeforeProbeServe = func() { <-gate }
	t.Cleanup(func() { testHookBeforeProbeServe = nil })

	addr := knownAddr()
	p := newProbeServer(addr)
	if err := p.start(); err != nil {
		t.Fatal(err)
	}

	// stop runs while Serve is held; let Serve go a moment after stop starts.
	go func() {
		time.Sleep(50 * time.Millisecond)
		close(gate)
	}()
	if err := p.stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}

	again, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("the probe port is still bound after stop returned: %v", err)
	}
	again.Close()
}
