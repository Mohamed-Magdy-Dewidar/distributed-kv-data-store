package kvadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kvv1 "github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/api/v1alpha1"
)

// fixtureDir is the admin API contract, checked in with the node's code;
// the database module's internal/app tests that real responses match it.
var fixtureDir = filepath.Join("..", "..", "..", "internal", "app", "testdata", "admin")

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixtureDir, name))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

const (
	fixNotStarted  = "node-not-started-503.json"
	codeNotStarted = "node not started"
)

// fixtureTypes maps every fixture to the type the operator decodes it into.
var fixtureTypes = map[string]func() any{
	"membership-get-200.json":           func() any { return &Membership{} },
	"membership-post-request.json":      func() any { return &SetMembershipRequest{} },
	"membership-post-200.json":          func() any { return &SetMembershipResult{} },
	"membership-post-409-stale.json":    func() any { return &APIError{} },
	"membership-post-409-conflict.json": func() any { return &APIError{} },
	"membership-post-400.json":          func() any { return &APIError{} },
	fixNotStarted:                       func() any { return &APIError{} },
	"handoff-200.json":                  func() any { return &Handoff{} },
	"handoff-504.json":                  func() any { return &Handoff{} },
	"handoff-503-stopping.json":         func() any { return &Handoff{} },
	"handoff-400.json":                  func() any { return &APIError{} },
}

// keys returns the object keys at every path of a JSON value, with the
// node-ID maps (members, peers) collapsed to one entry.
func keys(path string, v any, out map[string]bool) {
	switch v := v.(type) {
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(v)) {
			p := path + "." + k
			if path == ".members" || path == ".peers" {
				p = path + "[*]"
			}
			out[p] = true
			keys(p, v[k], out)
		}
	case []any:
		for _, e := range v {
			keys(path+"[]", e, out)
		}
	}
}

// TestFixturesDecodeStrictly decodes every contract fixture into its type
// with unknown fields rejected (no field of the node's is missing from the
// type), then encodes it back and compares the field sets (no field of the
// type's is missing from the node's).
func TestFixturesDecodeStrictly(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(fixtureDir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatalf("no fixtures under %s", fixtureDir)
	}
	for _, f := range files {
		name := filepath.Base(f)
		t.Run(name, func(t *testing.T) {
			newV, ok := fixtureTypes[name]
			if !ok {
				t.Fatalf("fixture %s has no type here; add it to fixtureTypes", name)
			}
			raw := fixture(t, name)
			v := newV()
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.DisallowUnknownFields()
			if err := dec.Decode(v); err != nil {
				t.Fatalf("strict decode into %T: %v", v, err)
			}

			back, err := json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
			var want, got any
			_ = json.Unmarshal(raw, &want)
			_ = json.Unmarshal(back, &got)
			wk, gk := map[string]bool{}, map[string]bool{}
			keys("", want, wk)
			keys("", got, gk)
			if !maps.Equal(wk, gk) {
				t.Errorf("%T encodes fields %v, the fixture has %v", v, slices.Sorted(maps.Keys(gk)), slices.Sorted(maps.Keys(wk)))
			}
		})
	}
	for name := range fixtureTypes {
		if _, err := os.Stat(filepath.Join(fixtureDir, name)); err != nil {
			t.Errorf("fixtureTypes lists %s, which is not a fixture: %v", name, err)
		}
	}
}

// seen is what the fake node received.
type seen struct {
	method, host, path, query string
	body                      []byte
}

// fakeNode answers every request with status and body, and records the
// requests. The client it returns dials it whatever host a URL names, so
// URLs keep their real pod hosts.
func fakeNode(t *testing.T, status int, body []byte) (*Client, *[]seen) {
	t.Helper()
	var reqs []seen
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := new(bytes.Buffer)
		_, _ = b.ReadFrom(r.Body)
		reqs = append(reqs, seen{r.Method, r.Host, r.URL.Path, r.URL.RawQuery, b.Bytes()})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return redirectedClient(srv), &reqs
}

func redirectedClient(srv *httptest.Server) *Client {
	addr := srv.Listener.Addr().String()
	return &Client{HTTP: &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		},
	}}}
}

func cluster(name, namespace string) *kvv1.KVCluster {
	return &kvv1.KVCluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
}

// TestRequestsGoToPodHost checks every call's host, path, query and body,
// for a cluster whose name and namespace are not the defaults.
func TestRequestsGoToPodHost(t *testing.T) {
	kv := cluster("db", "prod")
	const host = "db-3.db.prod.svc.cluster.local:8080"

	c, reqs := fakeNode(t, 200, fixture(t, "membership-get-200.json"))
	if _, err := c.GetMembership(t.Context(), kv, 3); err != nil {
		t.Fatal(err)
	}
	c2, reqs2 := fakeNode(t, 200, fixture(t, "membership-post-200.json"))
	members := map[string]string{"db-0": "db-0.db.prod.svc.cluster.local:7000"}
	if _, err := c2.SetMembership(t.Context(), kv, 3, SetMembershipRequest{Epoch: 4, Members: members}); err != nil {
		t.Fatal(err)
	}
	c3, reqs3 := fakeNode(t, 200, fixture(t, "handoff-200.json"))
	if _, _, err := c3.Handoff(t.Context(), kv, 3, 7); err != nil {
		t.Fatal(err)
	}

	all := append(append(append([]seen{}, *reqs...), *reqs2...), *reqs3...)
	want := []seen{
		{method: "GET", host: host, path: "/admin/membership"},
		{method: "POST", host: host, path: "/admin/membership"},
		{method: "GET", host: host, path: "/admin/handoff", query: "epoch=7&timeout=1s"},
	}
	if len(all) != len(want) {
		t.Fatalf("got %d requests, want %d", len(all), len(want))
	}
	for i, w := range want {
		g := all[i]
		if g.method != w.method || g.host != w.host || g.path != w.path || g.query != w.query {
			t.Errorf("request %d = %s %s%s?%s, want %s %s%s?%s", i, g.method, g.host, g.path, g.query, w.method, w.host, w.path, w.query)
		}
	}

	// The POST body is exactly what the node accepts: the request fixture's
	// fields, nothing else (the node rejects unknown fields).
	var got map[string]any
	if err := json.Unmarshal(all[1].body, &got); err != nil {
		t.Fatal(err)
	}
	var shape map[string]any
	_ = json.Unmarshal(fixture(t, "membership-post-request.json"), &shape)
	if !slices.Equal(slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(shape))) {
		t.Errorf("POST body fields %v, want the fixture's %v", slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(shape)))
	}
	if got["epoch"] != float64(4) || !reflect.DeepEqual(got["members"], map[string]any{"db-0": "db-0.db.prod.svc.cluster.local:7000"}) {
		t.Errorf("POST body = %s", all[1].body)
	}
}

func TestURL(t *testing.T) {
	got := URL(cluster("kv", "kvstore"), 10, "/admin/handoff", map[string][]string{"epoch": {"2"}})
	if want := "http://kv-10.kv.kvstore.svc.cluster.local:8080/admin/handoff?epoch=2"; got != want {
		t.Errorf("URL = %s, want %s", got, want)
	}
}

func TestGetMembership(t *testing.T) {
	c, _ := fakeNode(t, 200, fixture(t, "membership-get-200.json"))
	m, err := c.GetMembership(t.Context(), cluster("kv", "kvstore"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch != 3 || len(m.Members) != 3 || m.Members["kv-2"] != "kv-2.kv.kvstore.svc.cluster.local:7000" ||
		!m.Handoff.Done || m.Handoff.Pushed != 1393 || m.Peers["kv-1"] != (PeerStatus{Alive: true, LastSeenEpoch: 3, Reachable: true}) {
		t.Errorf("membership = %+v", m)
	}
}

func TestErrors(t *testing.T) {
	kv := cluster("kv", "kvstore")
	for _, tt := range []struct {
		fixture      string
		status       int
		code         string
		currentEpoch *uint64
	}{
		{"membership-post-409-stale.json", 409, "stale epoch", new(uint64(3))},
		{"membership-post-409-conflict.json", 409, "membership conflict", new(uint64(3))},
		{"membership-post-400.json", 400, "invalid membership", nil},
		{fixNotStarted, 503, codeNotStarted, nil},
	} {
		t.Run(tt.fixture, func(t *testing.T) {
			c, _ := fakeNode(t, tt.status, fixture(t, tt.fixture))
			for call, err := range map[string]error{
				"SetMembership": func() error {
					_, err := c.SetMembership(t.Context(), kv, 0, SetMembershipRequest{Epoch: 2})
					return err
				}(),
				"GetMembership": func() error { _, err := c.GetMembership(t.Context(), kv, 0); return err }(),
			} {
				var e *APIError
				if !errors.As(err, &e) {
					t.Fatalf("%s: err = %v, want *APIError", call, err)
				}
				if e.Status != tt.status || e.Code != tt.code || e.Message == "" && tt.code != codeNotStarted {
					t.Errorf("%s: %+v", call, e)
				}
				if !reflect.DeepEqual(e.CurrentEpoch, tt.currentEpoch) {
					t.Errorf("%s: CurrentEpoch = %v, want %v", call, e.CurrentEpoch, tt.currentEpoch)
				}
				if !IsStatus(err, tt.status) {
					t.Errorf("%s: IsStatus(%d) = false", call, tt.status)
				}
			}
		})
	}
}

func TestNonJSONErrorBody(t *testing.T) {
	c, _ := fakeNode(t, 405, []byte("Method Not Allowed\n"))
	_, err := c.GetMembership(t.Context(), cluster("kv", "kvstore"), 0)
	var e *APIError
	if !errors.As(err, &e) || e.Status != 405 || e.Message != "Method Not Allowed" {
		t.Fatalf("err = %#v", err)
	}
}

func TestHandoff(t *testing.T) {
	kv := cluster("kv", "kvstore")
	for _, tt := range []struct {
		fixture  string
		status   int
		wantBody bool
		wantDone bool
		wantErr  string // the APIError's code; "" for no error
	}{
		{"handoff-200.json", 200, true, true, ""},
		{"handoff-504.json", 504, true, false, ""},
		{"handoff-503-stopping.json", 503, true, false, "node stopping"},
		{fixNotStarted, 503, false, false, codeNotStarted},
		{"handoff-400.json", 400, false, false, "bad epoch"},
	} {
		t.Run(tt.fixture, func(t *testing.T) {
			c, _ := fakeNode(t, tt.status, fixture(t, tt.fixture))
			h, done, err := c.Handoff(t.Context(), kv, 0, 4)
			if done != tt.wantDone {
				t.Errorf("done = %v, want %v", done, tt.wantDone)
			}
			if (h != nil) != tt.wantBody {
				t.Errorf("body = %+v, want body: %v", h, tt.wantBody)
			}
			if h != nil && h.Epoch != 4 {
				t.Errorf("body epoch = %d, want 4", h.Epoch)
			}
			if tt.wantErr == "" {
				if err != nil {
					t.Errorf("err = %v, want nil", err)
				}
				return
			}
			var e *APIError
			if !errors.As(err, &e) || e.Code != tt.wantErr || e.Status != tt.status {
				t.Errorf("err = %v, want *APIError %d %s", err, tt.status, tt.wantErr)
			}
		})
	}

	// 504's body says what is left.
	c, _ := fakeNode(t, 504, fixture(t, "handoff-504.json"))
	h, _, _ := c.Handoff(t.Context(), kv, 0, 4)
	if h.Drained || h.Handoff.Done || h.Handoff.Pending != 2975 || h.Peers["kv-0"].LastSeenEpoch != 4 {
		t.Errorf("504 body = %+v", h)
	}
}

// TestUnknownFieldsAreIgnored: a newer node may add fields, at any level; an
// older operator must still decode its answers.
func TestUnknownFieldsAreIgnored(t *testing.T) {
	kv := cluster("kv", "kvstore")
	extend := func(t *testing.T, name string) []byte {
		var v map[string]any
		if err := json.Unmarshal(fixture(t, name), &v); err != nil {
			t.Fatal(err)
		}
		v["addedLater"] = map[string]any{"x": 1}
		if h, ok := v["handoff"].(map[string]any); ok {
			h["bytesPushed"] = 123
		}
		b, _ := json.Marshal(v)
		return b
	}

	c, _ := fakeNode(t, 200, extend(t, "membership-get-200.json"))
	if m, err := c.GetMembership(t.Context(), kv, 0); err != nil || m.Epoch != 3 {
		t.Errorf("GetMembership with extra fields: %+v, %v", m, err)
	}
	c, _ = fakeNode(t, 200, extend(t, "membership-post-200.json"))
	if r, err := c.SetMembership(t.Context(), kv, 0, SetMembershipRequest{}); err != nil || !r.Changed {
		t.Errorf("SetMembership with extra fields: %+v, %v", r, err)
	}
	c, _ = fakeNode(t, 504, extend(t, "handoff-504.json"))
	if h, done, err := c.Handoff(t.Context(), kv, 0, 4); err != nil || done || h.Handoff.Pending != 2975 {
		t.Errorf("Handoff with extra fields: %+v, %v, %v", h, done, err)
	}
	c, _ = fakeNode(t, 409, extend(t, "membership-post-409-stale.json"))
	if _, err := c.SetMembership(t.Context(), kv, 0, SetMembershipRequest{}); !IsStatus(err, 409) {
		t.Errorf("409 with extra fields: %v", err)
	}
}

// TestEveryCallHasADeadline: against a node that accepts the connection and
// never answers, every call must give up after its own timeout, even with a
// context that never ends.
func TestEveryCallHasADeadline(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) }) // runs first: unblock, then close
	c := redirectedClient(srv)
	c.Timeout = 100 * time.Millisecond
	kv := cluster("kv", "kvstore")

	for name, call := range map[string]func() error{
		"GetMembership": func() error { _, err := c.GetMembership(context.Background(), kv, 0); return err },
		"SetMembership": func() error {
			_, err := c.SetMembership(context.Background(), kv, 0, SetMembershipRequest{})
			return err
		},
		// The handoff call's deadline is HandoffWait plus Timeout.
		"Handoff": func() error { _, _, err := c.Handoff(context.Background(), kv, 0, 1); return err },
	} {
		t.Run(name, func(t *testing.T) {
			done := make(chan error, 1)
			start := time.Now()
			go func() { done <- call() }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("err = %v, want a deadline error", err)
				}
				if limit := HandoffWait + c.Timeout + time.Second; time.Since(start) > limit {
					t.Errorf("took %v, more than %v", time.Since(start), limit)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("still waiting after 5s: the request has no deadline of its own")
			}
		})
	}
}

// TestHandoffDeadlineCoversTheWait: a real node holds a handoff request for
// the timeout it was sent (1s) before answering 504. The client's deadline
// must outlast that, however short Timeout is.
func TestHandoffDeadlineCoversTheWait(t *testing.T) {
	body := fixture(t, "handoff-504.json")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wait, err := time.ParseDuration(r.URL.Query().Get("timeout"))
		if err != nil {
			w.WriteHeader(400)
			return
		}
		// Capped, so a client that sends a long timeout fails this test
		// quickly instead of hanging it.
		time.Sleep(min(wait, 3*time.Second))
		w.WriteHeader(504)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	c := redirectedClient(srv)
	c.Timeout = 100 * time.Millisecond
	h, done, err := c.Handoff(t.Context(), cluster("kv", "kvstore"), 0, 4)
	if err != nil || done || h == nil {
		t.Fatalf("Handoff = %+v, %v, %v; want the 504 body", h, done, err)
	}
}

func TestAPIErrorMessage(t *testing.T) {
	e := &APIError{Status: 409, Code: "stale epoch", Message: "epoch 2, node is at 3", CurrentEpoch: new(uint64(3))}
	if got := e.Error(); !strings.Contains(got, "409 stale epoch: epoch 2, node is at 3 (node at epoch 3)") {
		t.Errorf("Error() = %q", got)
	}
	if got := fmt.Sprint(&APIError{Status: 503, Code: codeNotStarted}); got != "admin API: 503 "+codeNotStarted {
		t.Errorf("Error() = %q", got)
	}
}
