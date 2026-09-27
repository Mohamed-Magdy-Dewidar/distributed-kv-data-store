package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"distributed-kv-datastore/internal/model"
	"distributed-kv-datastore/internal/node"
	"distributed-kv-datastore/internal/rpc"
	"distributed-kv-datastore/internal/vectorclock"
)

// replicateTo sends one write straight to address's gRPC listener, as a
// peer's Replicate or anti-entropy push would.
func replicateTo(t *testing.T, address string) error {
	t.Helper()
	client, err := rpc.Dial(address, 0)
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	item := &model.DataItem{Value: "v", VectorClock: vectorclock.FromSnapshot(map[string]uint32{"node-2": 1}), LastUpdatedBy: "node-2"}
	return client.Replicate(ctx, "k", []*model.DataItem{item})
}

func post(t *testing.T, h http.Handler, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
	return rec.Code
}

// TestStopListenersStopsListenersTheDashboardRestarted: a node stopped and
// restarted from the dashboard is served by a listener created by
// handleStart, not the one it was registered with. StopListeners must stop
// that one — otherwise the node keeps accepting writes through shutdown —
// and handleStart must refuse to start another afterwards.
func TestStopListenersStopsListenersTheDashboardRestarted(t *testing.T) {
	const addr = "localhost:60551"
	nd := node.New("node-1", addr, 1, 1, 1, nil)
	listener, err := rpc.Serve(addr, nd.Store, nd)
	if err != nil {
		t.Fatalf("serve: %v", err)
	}
	dashboard := NewServer()
	dashboard.Register("node-1", nd, listener, addr, 1, 1, nil)
	h := dashboard.Handler()

	if code := post(t, h, "/api/nodes/node-1/stop"); code != http.StatusOK {
		t.Fatalf("stop: status %d", code)
	}
	if code := post(t, h, "/api/nodes/node-1/start"); code != http.StatusOK {
		t.Fatalf("start: status %d", code)
	}
	t.Cleanup(dashboard.StopListeners) // if the test fails before StopListeners below
	if err := replicateTo(t, addr); err != nil {
		t.Fatalf("test setup: the restarted listener should accept writes, got %v", err)
	}

	dashboard.StopListeners()

	if err := replicateTo(t, addr); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable after StopListeners, got %v: the dashboard-restarted listener is still serving", err)
	}
	if code := post(t, h, "/api/nodes/node-1/start"); code != http.StatusServiceUnavailable {
		t.Fatalf("expected start to be refused after StopListeners, got status %d", code)
	}
	if err := replicateTo(t, addr); status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable after a refused start, got %v", err)
	}
}
