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
	var listeners []*rpc.Listener
	compactionCtx, stopCompaction := context.WithCancel(context.Background())

	// stopNodes is shutdown's steps 2 and 3: every listener is stopped
	// before any node closes, so no inbound Replicate/CoordinatePut — from
	// any node — can reach a closed StorageEngine.
	stopNodes := func() {
		for _, listener := range listeners {
			listener.Stop()
		}
		for _, nd := range nodes {
			if err := nd.Close(); err != nil {
				log.Printf("closing %s: %v", nd.ID, err)
			}
		}
	}
	// startupFailed releases what already started, then exits: log.Fatal
	// alone would skip the cleanup.
	startupFailed := func(format string, args ...any) {
		stopCompaction()
		stopNodes()
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
		listeners = append(listeners, listener)

		nd.StartCompactionLoop(compactionCtx, compactionInterval) // no-op in memory

		// Anti-entropy (nd.StartAntiEntropyLoop) is deliberately NOT
		// started: its loop can't yet be waited on, so shutdown can't stop
		// it before closing the nodes — see Node.StartAntiEntropyLoop.

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

	// 1. Stop background compaction, then stop taking client requests.
	stopCompaction()
	ctx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("dashboard shutdown: %v", err)
	}
	cancel()

	// 2 and 3. Stop every gRPC listener, then close every node.
	stopNodes()
	log.Println("shutdown complete")
	os.Exit(exitCode)
}
