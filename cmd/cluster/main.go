package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"distributed-kv-datastore/internal/node"
	"distributed-kv-datastore/internal/rpc"
)

func main() {
	nodeCount := 3
	basePort := 60100

	// Build every node's ID -> address up front, so each node can be told
	// about all its peers before any of them start.
	addresses := make(map[string]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		addresses[id] = fmt.Sprintf("localhost:%d", basePort+i)
	}

	nodes := make([]*node.Node, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		addr := addresses[id]

		neighbors := make(map[string]string, nodeCount-1)
		for peerID, peerAddr := range addresses {
			if peerID != id {
				neighbors[peerID] = peerAddr
			}
		}

		n := node.New(id, addr, neighbors)
		listener, err := rpc.Serve(addr, n.Store)
		if err != nil {
			log.Fatalf("failed to start %s on %s: %v", id, addr, err)
		}
		defer listener.Stop()

		log.Printf("%s listening on %s", id, listener.Addr())
		nodes = append(nodes, n)
	}

	log.Printf("cluster of %d nodes running — press Ctrl+C to stop", nodeCount)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("shutting down")
	_ = nodes // nodes are in scope here if you want to add graceful pre-shutdown logic later
}
