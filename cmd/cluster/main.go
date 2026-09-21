package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"distributed-kv-datastore/internal/httpapi"
	"distributed-kv-datastore/internal/node"
	"distributed-kv-datastore/internal/rpc"
)

func main() {
	nodeCount := 3
	basePort := 60100
	w, r := 2, 1

	addresses := make(map[string]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		addresses[id] = fmt.Sprintf("localhost:%d", basePort+i)
	}

	dashboard := httpapi.NewServer()

	for i := 0; i < nodeCount; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		addr := addresses[id]

		neighbors := make(map[string]string, nodeCount-1)
		for peerID, peerAddr := range addresses {
			if peerID != id {
				neighbors[peerID] = peerAddr
			}
		}

		n := node.New(id, addr, w, r, neighbors)
		listener, err := rpc.Serve(addr, n.Store)
		if err != nil {
			log.Fatalf("failed to start %s on %s: %v", id, addr, err)
		}
		defer listener.Stop()

		dashboard.Register(id, n, listener, addr, w, r, neighbors)
		log.Printf("%s listening on %s", id, listener.Addr())
	}

	go func() {
		log.Println("dashboard on http://localhost:8080")
		log.Fatal(http.ListenAndServe(":8080", dashboard.Handler()))
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Println("shutting down")
}
