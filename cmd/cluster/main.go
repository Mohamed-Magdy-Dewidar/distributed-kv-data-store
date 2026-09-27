package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"distributed-kv-datastore/internal/httpapi"
	"distributed-kv-datastore/internal/node"
	"distributed-kv-datastore/internal/rpc"
)

const (
	// maxMemtableBytes is each persistent node's StorageEngine flush
	// threshold: once a node's active memtable passes it, it's written out
	// as an SSTable.
	maxMemtableBytes = 4 << 20 // 4 MiB

	// compactionInterval is how often each persistent node compacts its
	// SSTables.
	compactionInterval = 30 * time.Second

	// antiEntropyInterval is how often each node reconciles with its peers
	// (after a random first delay, so nodes don't all run at once). Each
	// round rebuilds Merkle trees over every key a node holds, so this is
	// kept well above the cost of that scan.
	antiEntropyInterval = 30 * time.Second

	httpShutdownTimeout = 5 * time.Second
)

func main() {
	dataDir := flag.String("data", "data",
		`base directory for node data: each node gets <data>/<node-id>; "" keeps every node in memory`)
	flag.Parse()

	nodeCount := 3
	basePort := 60100
	replicationFactor, w, r := 2, 2, 2

	addresses := make(map[string]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		addresses[id] = fmt.Sprintf("localhost:%d", basePort+i)
	}

	dashboard := httpapi.NewServer()

	var nodes []*node.Node
	compactionCtx, stopCompaction := context.WithCancel(context.Background())

	// startupFailed releases what already started, then exits: log.Fatal
	// alone would skip the cleanup. The HTTP server isn't running yet, and
	// every listener started so far is registered with the dashboard.
	startupFailed := func(format string, args ...any) {
		if err := shutdownCluster(dashboard, nodes, stopCompaction); err != nil {
			log.Print(err)
		}
		log.Fatalf(format, args...)
	}

	for i := 0; i < nodeCount; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		addr := addresses[id]

		neighbors := make(map[string]string, nodeCount-1)
		for peerID, peerAddr := range addresses {
			if peerID != id {
				neighbors[peerID] = peerAddr
			}
		}

		var nd *node.Node
		if *dataDir == "" {
			nd = node.New(id, addr, replicationFactor, w, r, neighbors)
		} else {
			var err error
			nd, err = node.NewPersistent(id, addr, replicationFactor, w, r, neighbors,
				filepath.Join(*dataDir, id), maxMemtableBytes)
			if err != nil {
				startupFailed("failed to open %s: %v", id, err)
			}
		}
		nodes = append(nodes, nd)

		listener, err := rpc.Serve(addr, nd.Store, nd)
		if err != nil {
			startupFailed("failed to start %s on %s: %v", id, addr, err)
		}

		nd.StartCompactionLoop(compactionCtx, compactionInterval) // no-op in memory

		// Stopped and drained by shutdownCluster before any node closes.
		nd.StartAntiEntropyLoop(context.Background(), antiEntropyInterval)

		dashboard.Register(id, nd, listener, addr, w, r, neighbors)
		if *dataDir == "" {
			log.Printf("%s listening on %s (in memory)", id, listener.Addr())
		} else {
			log.Printf("%s listening on %s (data in %s)", id, listener.Addr(), filepath.Join(*dataDir, id))
		}
	}

	httpServer := &http.Server{Addr: ":8080", Handler: dashboard.Handler()}
	httpFailed := make(chan error, 1)
	go func() {
		log.Println("dashboard on http://localhost:8080")
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			httpFailed <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	exitCode := 0
	select {
	case <-stop:
		log.Println("shutting down")
	case err := <-httpFailed:
		log.Printf("dashboard failed: %v; shutting down", err)
		exitCode = 1
	}

	// 1. Stop taking client requests. Shutdown waits for in-flight handlers
	// — which may still need peers' gRPC listeners to finish a quorum
	// write — and after it no handleStart can start a new listener.
	// Accepted residual risk: a handler still running when the timeout
	// expires keeps running, and a write it makes after step 5 fails with
	// a clean closed-engine error (not corruption). Handlers use request
	// timeouts of the same length, so in practice they finish in time.
	ctx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("dashboard shutdown: %v", err)
	}
	cancel()

	// 2 to 5.
	if err := shutdownCluster(dashboard, nodes, stopCompaction); err != nil {
		log.Print(err)
	}
	log.Println("shutdown complete")
	os.Exit(exitCode)
}

// shutdownCluster is shutdown's steps 2 to 5, run once nothing can start a
// new listener (the dashboard's HTTP server is stopped, or never started).
// Each step must finish before the next begins:
//
//  2. Stop every node's current gRPC listener (dashboard.StopListeners).
//     New Replicate/CoordinatePut RPCs are refused and in-flight ones
//     finish, so from here no write arrives from outside any node — in
//     particular no peer's anti-entropy push can reach a node whose loop is
//     stopped next and whose engine is closed after that.
//  3. Stop every node's anti-entropy and wait for rounds in progress. Their
//     remaining RPCs fail fast; installs they already fetched data for
//     complete while engines are still open. From here nothing inside the
//     process writes either. Done for all nodes before any is closed.
//  4. Stop compaction. Its position doesn't matter for safety — engine
//     Close waits out a compaction in progress — but all background loops
//     stop here together.
//  5. Close every node (Close would also run step 3 itself; doing it
//     explicitly above drains every node before the first engine closes).
//
// Close errors are joined into the result; every node is closed regardless.
func shutdownCluster(dashboard *httpapi.Server, nodes []*node.Node, stopCompaction context.CancelFunc) error {
	dashboard.StopListeners()
	for _, nd := range nodes {
		nd.StopAntiEntropy()
	}
	stopCompaction()

	var errs []error
	for _, nd := range nodes {
		if err := nd.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing %s: %w", nd.ID, err))
		}
	}
	return errors.Join(errs...)
}
