package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"distributed-kv-datastore/internal/node"
)

// The admin API rides on the probe server (listen.http). It has no
// authentication: anyone who can reach the port can change the membership.
// See docs/known-limitations.md.
const (
	// maxAdminBody bounds a request body; a membership is a few hundred bytes.
	maxAdminBody = 64 << 10

	defaultHandoffWait = time.Minute
	maxHandoffWait     = 10 * time.Minute
)

func (p *probeServer) registerAdmin(mux *http.ServeMux) {
	mux.HandleFunc("GET /admin/membership", p.handleGetMembership)
	mux.HandleFunc("POST /admin/membership", p.handleSetMembership)
	mux.HandleFunc("GET /admin/handoff", p.handleHandoff)
}

type errorBody struct {
	Error        string  `json:"error"`
	Message      string  `json:"message,omitempty"`
	CurrentEpoch *uint64 `json:"currentEpoch,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// adminNode returns the node the admin API acts on, or answers 503 if Run
// hasn't built it yet.
func (p *probeServer) adminNode(w http.ResponseWriter) *node.Node {
	nd := p.node.Load()
	if nd == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "node not started"})
	}
	return nd
}

type membershipResponse struct {
	Epoch       uint64                     `json:"epoch"`
	Members     map[string]string          `json:"members"`
	Fingerprint string                     `json:"fingerprint"`
	Handoff     node.HandoffStatus         `json:"handoff"`
	Peers       map[string]node.PeerStatus `json:"peers"`
}

func (p *probeServer) handleGetMembership(w http.ResponseWriter, _ *http.Request) {
	nd := p.adminNode(w)
	if nd == nil {
		return
	}
	m := nd.CurrentMembership()
	writeJSON(w, http.StatusOK, membershipResponse{
		Epoch:       m.Epoch,
		Members:     m.Members,
		Fingerprint: m.Fingerprint,
		Handoff:     nd.HandoffStatus(),
		Peers:       nd.PeerStatuses(),
	})
}

type setMembershipRequest struct {
	Epoch   *uint64           `json:"epoch"`
	Members map[string]string `json:"members"`
}

// handleSetMembership applies a membership: 200 {changed, epoch} on success
// (changed=false for an identical membership already held); 409 for an epoch
// older than the node's, or the node's epoch with different members; 400 for
// a body that is malformed, too large, or names an invalid member set.
func (p *probeServer) handleSetMembership(w http.ResponseWriter, r *http.Request) {
	nd := p.adminNode(w)
	if nd == nil {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxAdminBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req setMembershipRequest
	if err := dec.Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeJSON(w, http.StatusBadRequest, errorBody{Error: "request body too large", Message: fmt.Sprintf("limit is %d bytes", maxAdminBody)})
			return
		}
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "bad JSON", Message: err.Error()})
		return
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "bad JSON", Message: "unexpected data after the request object"})
		return
	}
	if req.Epoch == nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "bad request", Message: "epoch is required"})
		return
	}

	changed, err := nd.SetMembership(*req.Epoch, req.Members)
	current, _ := nd.Membership()
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]any{"changed": changed, "epoch": current})
	case errors.Is(err, node.ErrStaleEpoch):
		writeJSON(w, http.StatusConflict, errorBody{Error: "stale epoch", Message: err.Error(), CurrentEpoch: &current})
	case errors.Is(err, node.ErrMembershipConflict):
		writeJSON(w, http.StatusConflict, errorBody{Error: "membership conflict", Message: err.Error(), CurrentEpoch: &current})
	case errors.Is(err, node.ErrInvalidMembership):
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "invalid membership", Message: err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, errorBody{Error: "could not apply membership", Message: err.Error(), CurrentEpoch: &current})
	}
}

type handoffResponse struct {
	Epoch   uint64                     `json:"epoch"`
	Member  bool                       `json:"member"` // whether this node is in its current view
	Drained bool                       `json:"drained"`
	Handoff node.HandoffStatus         `json:"handoff"`
	Peers   map[string]node.PeerStatus `json:"peers"`
}

// parseHandoffWait reads the timeout query parameter: a Go duration, default
// one minute, capped at ten. Zero and negative durations are rejected.
func parseHandoffWait(s string) (time.Duration, error) {
	if s == "" {
		return defaultHandoffWait, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("timeout %q is not a duration like 30s or 5m", s)
	}
	if d <= 0 {
		return 0, fmt.Errorf("timeout must be positive, got %v", d)
	}
	return min(d, maxHandoffWait), nil
}

// handleHandoff blocks until this node has finished handing data off for
// the membership at ?epoch=E (see node.Node.WaitDrained): 200 with the status
// when it has, 504 with the status when ?timeout runs out first. A node that
// has been removed from the membership is drained only once its handoff is
// done and every remaining member has been seen at epoch E or later — after
// that, stopping it loses nothing.
func (p *probeServer) handleHandoff(w http.ResponseWriter, r *http.Request) {
	nd := p.adminNode(w)
	if nd == nil {
		return
	}
	q := r.URL.Query()
	epoch, err := strconv.ParseUint(q.Get("epoch"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "bad epoch", Message: "epoch is required and must be a non-negative integer"})
		return
	}
	wait, err := parseHandoffWait(q.Get("timeout"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: "bad timeout", Message: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), wait)
	defer cancel()
	go func() { // a shutting-down server doesn't wait out a long poll
		select {
		case <-p.done:
			cancel()
		case <-ctx.Done():
		}
	}()

	waitErr := nd.WaitDrained(ctx, epoch)
	body := handoffResponse{
		Epoch:   epoch,
		Member:  nd.IsMember(),
		Drained: waitErr == nil,
		Handoff: nd.HandoffStatus(),
		Peers:   nd.PeerStatuses(),
	}
	switch {
	case waitErr == nil:
		writeJSON(w, http.StatusOK, body)
	case errors.Is(waitErr, context.DeadlineExceeded):
		writeJSON(w, http.StatusGatewayTimeout, body)
	default: // the caller went away, or the server is stopping
		writeJSON(w, http.StatusServiceUnavailable, body)
	}
}
