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
)

// Metrics holds one node's metrics and serves them.
type Metrics struct {
	registry       *prometheus.Registry
	clientRequests *clientRequests
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
	return &Metrics{registry: reg, clientRequests: newClientRequests(reg)}
}

// ServerOptions are the gRPC server options that time the node's requests:
// pass them to rpc.Serve.
func (m *Metrics) ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.ChainUnaryInterceptor(m.clientRequests.intercept)}
}

// Handler serves the metrics in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

var membershipEpoch = prometheus.NewDesc(
	"kv_membership_epoch",
	"Membership epoch of the view this node holds.",
	nil, nil,
)

// nodeCollector turns one node.Stats, read at scrape time, into metrics.
type nodeCollector struct {
	stats func() (node.Stats, bool)
}

func (c *nodeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- membershipEpoch
}

func (c *nodeCollector) Collect(ch chan<- prometheus.Metric) {
	s, ok := c.stats()
	if !ok {
		return
	}
	ch <- prometheus.MustNewConstMetric(membershipEpoch, prometheus.GaugeValue, float64(s.Epoch))
}
