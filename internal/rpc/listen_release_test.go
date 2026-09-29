package rpc

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"distributed-kv-datastore/internal/store"
)

// delayServe holds every Listener's Serve goroutine until the returned
// release function is called, so a test can stop the server before Serve has
// seen its listener.
func delayServe(t *testing.T) (release func()) {
	t.Helper()
	gate := make(chan struct{})
	testHookBeforeServe = func() { <-gate }
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(func() {
		release()
		testHookBeforeServe = nil
	})
	return release
}

// A Listener stopped before its Serve goroutine has started still frees its
// port by the time Stop or StopWithin returns.
func TestStopFreesThePortEvenBeforeServeStarts(t *testing.T) {
	for name, stop := range map[string]func(*Listener){
		"Stop":       (*Listener).Stop,
		"StopWithin": func(l *Listener) { l.StopWithin(context.Background()) },
		"StopWithin, expired": func(l *Listener) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			l.StopWithin(ctx)
		},
	} {
		t.Run(name, func(t *testing.T) {
			release := delayServe(t)
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := lis.Addr().String()
			l := ServeListener(lis, store.NewDataStore("n"), nil)

			// Stop runs while Serve is held; it can only return once Serve has
			// run and returned, so let Serve go a moment after stop starts.
			go func() {
				time.Sleep(50 * time.Millisecond)
				release()
			}()
			stop(l)

			again, err := net.Listen("tcp", addr)
			if err != nil {
				t.Fatalf("the port is still bound after %s returned: %v", name, err)
			}
			again.Close()
		})
	}
}
