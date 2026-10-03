// Package telemetry is the only code that knows a metrics format. It reads
// what the core packages expose as plain values (node.Stats) and serves it
// in the Prometheus text format; the core packages import nothing from it,
// or from Prometheus (TestCorePackagesDoNotImportPrometheus guards that).
//
// Values that come from Stats are read when /metrics is scraped, not
// copied in as they change.
package telemetry

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"distributed-kv-datastore/internal/node"
	"distributed-kv-datastore/internal/storage/engine"
)

// Metrics holds one node's metrics and serves them.
type Metrics struct {
	registry       *prometheus.Registry
	clientRequests *clientRequests
	dataStorage    *walSyncObserver
	hintsStorage   *walSyncObserver
}

// New returns the metrics for one node. stats is called once per scrape;
// it reports false while there is no node yet (Run starts the probe server
// before the node is open), and then no kv metrics are served. The Go
// runtime's and the process's own metrics (go_*, process_*) are always
// served.
func New(stats func() (node.Stats, bool)) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		&nodeCollector{stats: stats},
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	m := &Metrics{registry: reg, clientRequests: newClientRequests(reg)}
	m.dataStorage, m.hintsStorage = storageObservers(reg)
	return m
}

// ServerOptions are the gRPC server options that time the node's requests:
// pass them to rpc.Serve.
func (m *Metrics) ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.ChainUnaryInterceptor(m.clientRequests.intercept)}
}

// StorageObservers are the observers of a node's data engine and hint store:
// pass them to node.New with node.WithStorageObservers.
func (m *Metrics) StorageObservers() (data, hints engine.Observer) {
	return m.dataStorage, m.hintsStorage
}

// Handler serves the metrics in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
