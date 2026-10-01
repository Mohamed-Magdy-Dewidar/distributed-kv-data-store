// Package app wires cmd/node's config into one running node: an HTTP
// probe server, the node itself, its gRPC
// listener, and its background loops — brought up in order, torn down in
// a different, specific order on shutdown (see Run).
package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"distributed-kv-datastore/internal/config"
	"distributed-kv-datastore/internal/node"
	"distributed-kv-datastore/internal/rpc"
)

// probeShutdownTimeout bounds the probe server's own Shutdown call. It's
// the last thing Run stops, after everything else is already down, so
// nothing depends on this finishing quickly — the bound exists only so a
// wedged probe server can't hang Run forever.
const probeShutdownTimeout = 5 * time.Second

// testHookStep, when set, is called with a short name at each startup and
// shutdown transition Run makes (see the step names inline in Run). Tests
// only: it lets one observe the exact sequence, or block Run at a specific
// point, without any exported API. Always nil in production.
var testHookStep func(step string)

func step(name string) {
	if testHookStep != nil {
		testHookStep(name)
	}
}

// probeServer serves /livez (always 200) and /readyz (200 once ready and
// while the node is in its membership, 503 otherwise) on one address —
// cmd/node's Kubernetes liveness/readiness probes — and the admin API (see
// admin.go) once the node is set.
type probeServer struct {
	httpServer *http.Server
	ready      atomic.Bool
	node       atomic.Pointer[node.Node]
	done       chan struct{} // closed when the server starts stopping
	stopOnce   sync.Once

	lis       net.Listener  // set by start; stop closes it itself
	serveDone chan struct{} // closed when the Serve goroutine returns
}

// testHookBeforeProbeServe, when set, runs at the start of the probe
// server's Serve goroutine. Tests only: it lets one stop the server before
// Serve has seen its listener. Always nil in production.
var testHookBeforeProbeServe func()

func newProbeServer(addr string) *probeServer {
	p := &probeServer{done: make(chan struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		// A node that has been removed from the membership is not ready: it
		// should stop receiving traffic while it drains.
		if nd := p.node.Load(); p.ready.Load() && (nd == nil || nd.IsMember()) {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	p.registerAdmin(mux)
	p.httpServer = &http.Server{Addr: addr, Handler: mux}
	return p
}

// start binds and begins serving in the background, returning once the
// listener is bound — mirroring rpc.Serve's own readiness guarantee, so a
// caller that gets a nil error knows the probe port is already live.
func (p *probeServer) start() error {
	lis, err := net.Listen("tcp", p.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("app: probe server listen on %s: %w", p.httpServer.Addr, err)
	}
	p.lis, p.serveDone = lis, make(chan struct{})
	hook := testHookBeforeProbeServe // read here, not in the goroutine
	go func() {
		defer close(p.serveDone)
		if hook != nil {
			hook()
		}
		_ = p.httpServer.Serve(lis) // returns http.ErrServerClosed once Shutdown/Close runs
	}()
	return nil
}

func (p *probeServer) setReady(ready bool) {
	p.ready.Store(ready)
}

// setNode gives the admin API (and /readyz) the node to act on.
func (p *probeServer) setNode(nd *node.Node) {
	p.node.Store(nd)
}

// stop gracefully shuts the probe server down within probeShutdownTimeout,
// falling back to an immediate close if that isn't enough. When it returns,
// the probe port is free.
//
// Shutdown only closes listeners Serve has already registered, and Serve runs
// in its own goroutine: if stop comes first (Run failing right after start),
// the listener would stay bound until that goroutine got to run. So stop
// closes the listener itself (closing it twice is harmless) and waits for the
// Serve goroutine to return.
func (p *probeServer) stop() error {
	p.stopOnce.Do(func() { close(p.done) })
	ctx, cancel := context.WithTimeout(context.Background(), probeShutdownTimeout)
	defer cancel()
	err := p.httpServer.Shutdown(ctx)
	if err != nil {
		err = p.httpServer.Close()
	}
	if p.lis != nil {
		p.lis.Close()
		<-p.serveDone
	}
	return err
}

// Run brings up one node from cfg and blocks until ctx is done, then shuts
// everything down and returns.
//
// Startup order: probe server, then the node itself (node.New on
// cfg.DataDir — a locked data dir surfaces here as an error wrapping
// engine.ErrLocked and naming the dir), then this node's ReplicationTimeout
// and MaxReconnectBackoff, then the configured membership (see
// applyConfiguredMembership), then its gRPC listener, then its background
// loops (compaction on its own cancelable context; anti-entropy and hint
// delivery, which Node.Close already knows how to stop and drain), then
// the probe server is marked ready.
//
// Shutdown, once ctx is done, is a different, specific order — not simply
// startup reversed: mark not-ready first (so a load balancer stops sending
// new traffic before anything else changes), bound-stop the gRPC listener
// (see rpc.Listener.StopWithin — a still-running handler is not waited
// for; see docs/known-limitations.md), stop the background loops and wait
// for any round in progress, cancel the compaction loop, close the node
// (which waits for in-flight flushes and closes its storage), and only
// then stop the probe server — kept alive throughout so /livez and
// /readyz keep answering while everything else winds down.
//
// A startup failure tears down whatever had already started, in reverse,
// before returning the error.
func Run(ctx context.Context, cfg *config.Config) error {
	probe := newProbeServer(cfg.Listen.HTTP)
	if err := probe.start(); err != nil {
		return err
	}
	step("probe-started")

	nd, err := openNode(cfg)
	if err != nil {
		probe.stop()
		step("probe-stopped")
		return fmt.Errorf("app: open node at %q: %w", cfg.DataDir, err)
	}
	step("node-opened")

	nd.QuorumConfig.ReplicationTimeout = cfg.Timeouts.Replication
	nd.QuorumConfig.MaxReconnectBackoff = cfg.Timeouts.MaxReconnectBackoff
	nd.QuorumConfig.HeartbeatTimeout = cfg.Timeouts.Heartbeat
	nd.QuorumConfig.MaxMissedHeartbeats = cfg.Health.MaxMissedHeartbeats

	if err := applyConfiguredMembership(nd, cfg); err != nil {
		nd.Close()
		step("node-closed")
		probe.stop()
		step("probe-stopped")
		return err
	}
	probe.setNode(nd)

	listener, err := rpc.Serve(cfg.Listen.GRPC, nd.Store, nd)
	if err != nil {
		nd.Close()
		step("node-closed")
		probe.stop()
		step("probe-stopped")
		return fmt.Errorf("app: serve gRPC on %s: %w", cfg.Listen.GRPC, err)
	}
	step("grpc-serving")

	compactionCtx, stopCompaction := context.WithCancel(context.Background())
	nd.StartCompactionLoop(compactionCtx, cfg.Intervals.Compaction)
	nd.StartAntiEntropyLoop(context.Background(), cfg.Intervals.AntiEntropy)
	nd.StartHintDeliveryLoop(context.Background(), cfg.Intervals.HintDelivery)
	nd.StartHeartbeatLoop(context.Background(), cfg.Intervals.Heartbeat)
	nd.ResumeHandoff() // a no-op unless a restart interrupted a handoff
	step("loops-started")

	probe.setReady(true)
	step("ready")
	log.Printf("app: node %s ready (grpc %s, http %s)", cfg.NodeID, cfg.Listen.GRPC, cfg.Listen.HTTP)

	<-ctx.Done()
	log.Printf("app: node %s shutting down", cfg.NodeID)

	probe.setReady(false)
	step("not-ready")

	stopCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeouts.Shutdown)
	listener.StopWithin(stopCtx)
	cancel()
	step("grpc-stopped")

	nd.StopBackgroundLoops()
	step("loops-stopped")

	stopCompaction()
	step("compaction-stopped")

	closeErr := nd.Close()
	step("node-closed")

	probeErr := probe.stop()
	step("probe-stopped")

	if err := errors.Join(closeErr, probeErr); err != nil {
		return fmt.Errorf("app: shutdown: %w", err)
	}
	log.Printf("app: node %s shutdown complete", cfg.NodeID)
	return nil
}

// applyConfiguredMembership offers the configuration's member list, at
// cluster.epoch, to nd. The node keeps a newer membership it already holds
// (persisted from an earlier run): that is logged and startup continues. A
// list that contradicts the membership held for the same epoch, or is
// invalid, fails startup, so an operator who changed the members without
// raising the epoch finds out instead of running on a mismatched ring.
func applyConfiguredMembership(nd *node.Node, cfg *config.Config) error {
	changed, err := nd.SetMembership(cfg.Cluster.Epoch, cfg.MemberAddrs())
	switch {
	case errors.Is(err, node.ErrStaleEpoch):
		held, _ := nd.Membership()
		log.Printf("app: node %s: persisted membership epoch %d is newer than the config's cluster.epoch %d; keeping the persisted membership",
			cfg.NodeID, held, cfg.Cluster.Epoch)
		return nil
	case err != nil:
		return fmt.Errorf("app: apply cluster.members at cluster.epoch %d: %w", cfg.Cluster.Epoch, err)
	case changed:
		log.Printf("app: node %s: adopted membership epoch %d from the config", cfg.NodeID, cfg.Cluster.Epoch)
	}
	return nil
}

// openNode builds cfg's node on cfg.DataDir. node.New's error is returned
// as-is (it already names the data dir and wraps engine.ErrLocked when
// that's the cause). An empty DataDir is refused rather than opened, which
// would put the node's files in the working directory: config.Load already
// rejects it, and this guards callers that build a Config without Load.
func openNode(cfg *config.Config) (*node.Node, error) {
	if cfg.DataDir == "" {
		return nil, errors.New("dataDir is required")
	}
	return node.New(cfg.NodeID, cfg.SelfAddress(), cfg.Cluster.N, cfg.Cluster.W, cfg.Cluster.R,
		cfg.NeighborAddrs(), cfg.DataDir, cfg.Storage.MemtableBytes)
}
