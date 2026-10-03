package telemetry

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/rpc/pb"
)

// requestBuckets are the latency buckets, in seconds, of request histograms:
// 1-2.5-5 steps from 0.5ms, up to timeouts.replication's 5s. Provisional:
// revisit once measured on kind.
var requestBuckets = []float64{0.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}

// numCodes is the number of gRPC status codes, OK (0) to Unauthenticated (16).
const numCodes = int(codes.Unauthenticated) + 1

// methodObservers holds one method's histogram children, one per status
// code, created the first time that code is seen, so a code that never
// occurs exports no series.
type methodObservers struct {
	method string
	vec    *prometheus.HistogramVec
	byCode [numCodes]atomic.Pointer[prometheus.Observer]
}

// observer returns the child for code. Once a code's child exists this
// neither locks nor allocates. Two calls racing to create the same child
// get the same one from the vector, so either store is fine.
func (m *methodObservers) observer(code codes.Code) prometheus.Observer {
	if int(code) >= numCodes {
		return m.vec.WithLabelValues(m.method, code.String())
	}
	slot := &m.byCode[code]
	if o := slot.Load(); o != nil {
		return *o
	}
	o := m.vec.WithLabelValues(m.method, code.String())
	slot.Store(&o)
	return o
}

// clientRequests times the client-facing service, KVClient.
type clientRequests struct {
	byMethod map[string]*methodObservers // by full method name; read-only once built
}

func newClientRequests(reg prometheus.Registerer) *clientRequests {
	vec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kv_client_request_duration_seconds",
		Help:    "Time to serve a client request (KVClient), by method and gRPC status code.",
		Buckets: requestBuckets,
	}, []string{"method", "code"})
	reg.MustRegister(vec)

	c := &clientRequests{byMethod: make(map[string]*methodObservers)}
	for _, md := range pb.KVClient_ServiceDesc.Methods {
		full := "/" + pb.KVClient_ServiceDesc.ServiceName + "/" + md.MethodName
		c.byMethod[full] = &methodObservers{method: md.MethodName, vec: vec}
	}
	return c
}

// intercept times every KVClient call; any other method passes through
// untimed.
func (c *clientRequests) intercept(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	m, ok := c.byMethod[info.FullMethod]
	if !ok {
		return handler(ctx, req)
	}
	start := time.Now()
	resp, err := handler(ctx, req)
	m.observer(status.Code(err)).Observe(time.Since(start).Seconds())
	return resp, err
}
