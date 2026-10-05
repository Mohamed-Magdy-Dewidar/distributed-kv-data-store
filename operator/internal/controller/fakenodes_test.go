package controller

import (
	"context"
	"encoding/json"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/kvadmin"
)

// fakeNodes serves the admin API of every pod of a cluster from one
// httptest server, routing by the Host the client sends (the pod's DNS name).
// It follows the node's semantics (internal/app/admin.go and
// node.SetMembership): 409 for an older epoch or a different membership at
// the node's epoch, 400 below N, changed=false for the membership it holds,
// and a handoff answer of 200 only once the node is done for the epoch (a
// member: its handoff; a node outside its membership: drained).
type fakeNodes struct {
	mu    sync.Mutex
	n     int
	nodes map[string]*fakeNode
	// posts and handoffAsks record what the operator asked, by node ID.
	posts       []fakePost
	handoffAsks []string
	srv         *httptest.Server
}

type fakeNode struct {
	up          bool
	epoch       uint64
	members     map[string]string
	handoffDone bool // its handoff to its epoch has completed
	drained     bool // for a node outside its membership
}

type fakePost struct {
	node    string
	epoch   uint64
	members map[string]string
}

func newFakeNodes(t *testing.T, n int) *fakeNodes {
	f := &fakeNodes{n: n, nodes: map[string]*fakeNode{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

// client is a kvadmin.Client that reaches every pod host at this server.
func (f *fakeNodes) client() *kvadmin.Client {
	addr := f.srv.Listener.Addr().String()
	return &kvadmin.Client{HTTP: &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		},
	}}}
}

// set brings node id up holding (epoch, members).
func (f *fakeNodes) set(id string, epoch uint64, members map[string]string, handoffDone bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[id] = &fakeNode{up: true, epoch: epoch, members: maps.Clone(members), handoffDone: handoffDone}
}

func (f *fakeNodes) update(id string, fn func(*fakeNode)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f.nodes[id])
}

func (f *fakeNodes) takeHandoffAsks() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	a := f.handoffAsks
	f.handoffAsks = nil
	return a
}

func (f *fakeNodes) takePosts() []fakePost {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := f.posts
	f.posts = nil
	return p
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (f *fakeNodes) serve(w http.ResponseWriter, r *http.Request) {
	id, _, _ := strings.Cut(r.Host, ".")
	f.mu.Lock()
	defer f.mu.Unlock()
	nd := f.nodes[id]
	if nd == nil || !nd.up {
		// Unreachable: drop the connection.
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	_, member := nd.members[id]
	status := kvadmin.HandoffStatus{Epoch: nd.epoch, Done: nd.handoffDone}

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/admin/membership":
		writeJSON(w, 200, kvadmin.Membership{Epoch: nd.epoch, Members: nd.members, Fingerprint: "f" + strconv.FormatUint(nd.epoch, 10), Handoff: status, Peers: map[string]kvadmin.PeerStatus{}})

	case r.Method == http.MethodPost && r.URL.Path == "/admin/membership":
		var req kvadmin.SetMembershipRequest
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			writeJSON(w, 400, kvadmin.APIError{Code: "bad JSON", Message: err.Error()})
			return
		}
		f.posts = append(f.posts, fakePost{id, req.Epoch, maps.Clone(req.Members)})
		cur := nd.epoch
		switch {
		case len(req.Members) < f.n:
			writeJSON(w, 400, kvadmin.APIError{Code: "invalid membership", Message: "fewer than n members"})
		case req.Epoch < cur:
			writeJSON(w, 409, kvadmin.APIError{Code: "stale epoch", Message: "older epoch", CurrentEpoch: &cur})
		case req.Epoch == cur && !maps.Equal(req.Members, nd.members):
			writeJSON(w, 409, kvadmin.APIError{Code: "membership conflict", Message: "different members", CurrentEpoch: &cur})
		case req.Epoch == cur:
			writeJSON(w, 200, kvadmin.SetMembershipResult{Changed: false, Epoch: cur})
		default:
			nd.epoch, nd.members, nd.handoffDone, nd.drained = req.Epoch, maps.Clone(req.Members), false, false
			writeJSON(w, 200, kvadmin.SetMembershipResult{Changed: true, Epoch: req.Epoch})
		}

	case r.Method == http.MethodGet && r.URL.Path == "/admin/handoff":
		f.handoffAsks = append(f.handoffAsks, id)
		epoch, err := strconv.ParseUint(r.URL.Query().Get("epoch"), 10, 64)
		if err != nil {
			writeJSON(w, 400, kvadmin.APIError{Code: "bad epoch"})
			return
		}
		done := nd.epoch >= epoch && nd.handoffDone && (member || nd.drained)
		body := kvadmin.Handoff{Epoch: epoch, Member: member, Drained: done, Handoff: status, Peers: map[string]kvadmin.PeerStatus{}}
		if done {
			writeJSON(w, 200, body)
		} else {
			writeJSON(w, 504, body)
		}

	default:
		http.NotFound(w, r)
	}
}
