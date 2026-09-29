// Command node runs a single distributed-kv-datastore node from a YAML
// config file (see internal/config), for a real deployment (one node per
// process, e.g. one per Kubernetes pod) — as opposed to cmd/cluster, which
// runs several nodes in one process for local demos.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"distributed-kv-datastore/internal/app"
	"distributed-kv-datastore/internal/config"
)

func main() {
	configPath := flag.String("config", os.Getenv("KV_CONFIG"), "path to the node's YAML config file (default from KV_CONFIG)")
	flag.Parse()
	if *configPath == "" {
		log.Fatal("cmd/node: -config (or KV_CONFIG) is required")
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("cmd/node: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop() // a second signal now kills the process immediately, rather than being absorbed here
	}()

	if err := app.Run(ctx, cfg); err != nil {
		log.Fatalf("cmd/node: %v", err)
	}
}
