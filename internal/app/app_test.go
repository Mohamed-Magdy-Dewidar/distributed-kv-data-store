package app

import (
	"context"
	"errors"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

	"distributed-kv-datastore/internal/config"
	"distributed-kv-datastore/internal/storage/engine"
)

// freeAddr finds a currently-free TCP port by binding to it and closing it
// immediately. There's a small window where something else could grab it
// before Run does; acceptable for a single-process test suite, and the
// standard way to get a free port when the code under test binds its own
// listener rather than accepting one.
func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find a free port: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

// testConfig returns a valid single-node config (N=W=R=1, no peers) with
// free ports and short-but-not-instant timeouts, suitable for driving Run
// directly in tests. dataDir == "" gives an in-memory node.
func testConfig(t *testing.T, dataDir string) *config.Config {
	t.Helper()
	grpcAddr := freeAddr(t)
	return &config.Config{
		NodeID:  "kv-0",
		Listen:  config.Listen{GRPC: grpcAddr, HTTP: freeAddr(t)},
		DataDir: dataDir,
		Cluster: config.Cluster{
			N: 1, W: 1, R: 1,
			Members: []config.Member{{ID: "kv-0", Address: grpcAddr}},
		},
		Storage: config.Storage{MemtableBytes: 1 << 20},
		Intervals: config.Intervals{
			Compaction:   time.Hour, // effectively disabled: these tests don't exercise the loops themselves
			AntiEntropy:  time.Hour,
			HintDelivery: time.Hour,
			Heartbeat:    time.Hour,
		},
		Timeouts: config.Timeouts{
			Replication:         time.Second,
			MaxReconnectBackoff: time.Second,
			Shutdown:            2 * time.Second,
			Heartbeat:           time.Second,
		},
		Health: config.Health{MaxMissedHeartbeats: 3},
	}
}

// TestRunOrderingThroughShutdown records every step Run reports, from
// startup through a clean shutdown triggered by canceling ctx, and checks
// the exact sequence — both the specific steps and their order, which is
// deliberately not just startup reversed (see Run's doc comment).
func TestRunOrderingThroughShutdown(t *testing.T) {
	cfg := testConfig(t, "")

	stepCh := make(chan string, 32)
	testHookStep = func(s string) { stepCh <- s }
	defer func() { testHookStep = nil }()

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- Run(ctx, cfg) }()

	var got []string
	for s := range stepCh {
		got = append(got, s)
		if s == "ready" {
			break
		}
	}

	cancel()

	for s := range stepCh {
		got = append(got, s)
		if s == "probe-stopped" {
			break
		}
	}

	if err := <-runDone; err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	want := []string{
		"probe-started", "node-opened", "grpc-serving", "loops-started", "ready",
		"not-ready", "grpc-stopped", "loops-stopped", "compaction-stopped", "node-closed", "probe-stopped",
	}
	if !reflect.DeepEqual(got, want) {
		for i := 0; i < len(got) && i < len(want); i++ {
			if got[i] != want[i] {
				t.Fatalf("step %d differs: got %q, want %q\n full got:  %v\n full want: %v", i, got[i], want[i], got, want)
			}
		}
		t.Fatalf("step sequences differ in length:\n got:  %v\n want: %v", got, want)
	}
}

// stepGate lets a test pause Run at named steps and release them one at a
// time, via testHookStep.
type stepGate struct {
	gates map[string]chan struct{}
}

func newStepGate(names ...string) *stepGate {
	g := &stepGate{gates: make(map[string]chan struct{}, len(names))}
	for _, n := range names {
		g.gates[n] = make(chan struct{})
	}
	return g
}

func (g *stepGate) hook(step string) {
	if ch, ok := g.gates[step]; ok {
		<-ch
	}
}

func (g *stepGate) release(step string) {
	close(g.gates[step])
}

func getReadyz(t *testing.T, httpAddr string) int {
	t.Helper()
	resp, err := http.Get("http://" + httpAddr + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz failed: %v", err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

// waitForProbeUp polls /livez until the probe server answers or fails the
// test after a generous bound — the server starts almost immediately, so
// this is a startup-ordering formality, not a real wait.
func waitForProbeUp(t *testing.T, httpAddr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + httpAddr + "/livez")
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("probe server never became reachable")
}

// TestReadyzReflectsReadiness: /readyz is 503 before Run marks the node
// ready, 200 once it does, and 503 again once shutdown marks it not-ready
// — checked while the probe server is still definitely up (paused exactly
// at the "not-ready" step), not merely inferred from timing.
func TestReadyzReflectsReadiness(t *testing.T) {
	cfg := testConfig(t, "")
	httpAddr := cfg.Listen.HTTP

	gate := newStepGate("loops-started", "not-ready")
	testHookStep = gate.hook
	defer func() { testHookStep = nil }()

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- Run(ctx, cfg) }()

	waitForProbeUp(t, httpAddr)
	if code := getReadyz(t, httpAddr); code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 before ready, got %d", code)
	}

	gate.release("loops-started") // let Run proceed to mark itself ready
	deadline := time.Now().Add(5 * time.Second)
	for {
		if code := getReadyz(t, httpAddr); code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("readyz never became 200 after loops-started was released")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	// Run is now paused exactly at "not-ready": setReady(false) has already
	// run, but nothing has been stopped yet, so the probe server is
	// definitely still serving.
	if code := getReadyz(t, httpAddr); code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 once not-ready, got %d", code)
	}
	gate.release("not-ready")

	if err := <-runDone; err != nil {
		t.Fatalf("Run failed: %v", err)
	}
}

// TestStartupFailureWithLockedDataDirReleasesProbePort: opening a
// persistent node whose data dir is already locked by another engine must
// fail Run with an error naming/wrapping engine.ErrLocked, and must not
// leak the probe server's port — a fresh listener must be able to bind it
// once Run has returned.
func TestStartupFailureWithLockedDataDirReleasesProbePort(t *testing.T) {
	cfg := testConfig(t, t.TempDir())

	blocker, err := engine.Open(cfg.DataDir, cfg.Storage.MemtableBytes)
	if err != nil {
		t.Fatalf("failed to pre-lock the data dir: %v", err)
	}
	defer blocker.Close()

	err = Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected Run to fail while the data dir is locked")
	}
	if !errors.Is(err, engine.ErrLocked) {
		t.Fatalf("expected the error to wrap engine.ErrLocked, got %v", err)
	}

	lis, err := net.Listen("tcp", cfg.Listen.HTTP)
	if err != nil {
		t.Fatalf("expected the probe port to be released after the failed Run, got %v", err)
	}
	lis.Close()
}
