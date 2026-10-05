// Package kvadmin is a client for a node's admin HTTP API (internal/app's
// admin.go in the database module): GET and POST /admin/membership and GET
// /admin/handoff. Its types mirror the node's JSON; the contract is pinned by
// the fixtures in internal/app/testdata/admin, which both modules test
// against.
//
// Responses are decoded leniently (unknown fields ignored), so a node that
// adds a field never breaks an older operator. Every request carries its own
// short deadline, and the handoff call never long-polls: a reconcile must not
// block on it.
package kvadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	kvv1 "github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/api/v1alpha1"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/render"
)

const (
	// DefaultTimeout bounds each membership request.
	DefaultTimeout = 5 * time.Second

	// HandoffWait is the timeout every handoff request sends: the node waits
	// at most this long before answering 504 with the status.
	HandoffWait = time.Second

	// maxBody bounds a response body read; a status is a few kilobytes.
	maxBody = 1 << 20
)

// HandoffStatus is a node's progress moving data to the owners a membership
// gives it (node.HandoffStatus).
type HandoffStatus struct {
	Epoch   uint64 `json:"epoch"`
	Done    bool   `json:"done"`
	Pushed  int    `json:"pushed"`
	Hinted  int    `json:"hinted"`
	Pending int    `json:"pending"`
}

// PeerStatus is what a node's heartbeats have shown of another member
// (node.PeerStatus).
type PeerStatus struct {
	Alive         bool   `json:"alive"`
	LastSeenEpoch uint64 `json:"lastSeenEpoch"`
	Reachable     bool   `json:"reachable"`
}

// Membership is GET /admin/membership's 200 body.
type Membership struct {
	Epoch       uint64                `json:"epoch"`
	Members     map[string]string     `json:"members"`
	Fingerprint string                `json:"fingerprint"`
	Handoff     HandoffStatus         `json:"handoff"`
	Peers       map[string]PeerStatus `json:"peers"`
}

// SetMembershipRequest is POST /admin/membership's body. The node rejects
// unknown fields in it.
type SetMembershipRequest struct {
	Epoch   uint64            `json:"epoch"`
	Members map[string]string `json:"members"`
}

// SetMembershipResult is POST /admin/membership's 200 body. Changed is false
// when the node already held exactly this membership.
type SetMembershipResult struct {
	Changed bool   `json:"changed"`
	Epoch   uint64 `json:"epoch"`
}

// Handoff is GET /admin/handoff's body (200, 504, and 503 while the node
// stops).
type Handoff struct {
	Epoch   uint64                `json:"epoch"`
	Member  bool                  `json:"member"`
	Drained bool                  `json:"drained"`
	Handoff HandoffStatus         `json:"handoff"`
	Peers   map[string]PeerStatus `json:"peers"`
}

// APIError is a non-2xx answer: 400 and 409 from POST /admin/membership, 503
// while the node has not started, 400 from a bad handoff query, or anything
// else. CurrentEpoch is the node's epoch, when the node sent it (409, 500).
type APIError struct {
	Status       int     `json:"-"`
	Code         string  `json:"error"`
	Message      string  `json:"message,omitempty"`
	CurrentEpoch *uint64 `json:"currentEpoch,omitempty"`
}

func (e *APIError) Error() string {
	s := fmt.Sprintf("admin API: %d %s", e.Status, e.Code)
	if e.Message != "" {
		s += ": " + e.Message
	}
	if e.CurrentEpoch != nil {
		s += fmt.Sprintf(" (node at epoch %d)", *e.CurrentEpoch)
	}
	return s
}

// IsStatus reports whether err is an *APIError with this status.
func IsStatus(err error, status int) bool {
	var e *APIError
	return errors.As(err, &e) && e.Status == status
}

// Client talks to the nodes of KVClusters. The zero value is ready to use.
type Client struct {
	// HTTP sends the requests; http.DefaultClient when nil. Deadlines come
	// from Timeout, not from the client.
	HTTP *http.Client
	// Timeout bounds each request; DefaultTimeout when zero.
	Timeout time.Duration
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func (c *Client) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return DefaultTimeout
}

// URL is the admin endpoint path (with its query) on the pod with this
// ordinal: always render.PodHost on the HTTP port, so the operator reaches
// exactly the pods the headless Service names.
func URL(kv *kvv1.KVCluster, ordinal int, path string, query url.Values) string {
	u := url.URL{
		Scheme:   "http",
		Host:     fmt.Sprintf("%s:%d", render.PodHost(kv, ordinal), render.HTTPPort),
		Path:     path,
		RawQuery: query.Encode(),
	}
	return u.String()
}

// do sends one request with its own deadline and returns the status and
// the body.
func (c *Client) do(ctx context.Context, wait time.Duration, method, u string, body any) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, r)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("admin API: %s %s: %w", method, u, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return 0, nil, fmt.Errorf("admin API: %s %s: read body: %w", method, u, err)
	}
	return resp.StatusCode, raw, nil
}

// apiError builds the error for a non-2xx answer. A body that is not the
// node's error JSON (a proxy's page, the mux's plain-text 405) becomes the
// message.
func apiError(status int, raw []byte) *APIError {
	e := &APIError{}
	if err := json.Unmarshal(raw, e); err != nil || e.Code == "" {
		e = &APIError{Message: string(bytes.TrimSpace(raw))}
	}
	e.Status = status
	return e
}

func decode(raw []byte, v any) error {
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("admin API: decode %T: %w", v, err)
	}
	return nil
}

// GetMembership reads the membership the pod with this ordinal holds.
func (c *Client) GetMembership(ctx context.Context, kv *kvv1.KVCluster, ordinal int) (*Membership, error) {
	status, raw, err := c.do(ctx, c.timeout(), http.MethodGet, URL(kv, ordinal, "/admin/membership", nil), nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, apiError(status, raw)
	}
	m := &Membership{}
	if err := decode(raw, m); err != nil {
		return nil, err
	}
	return m, nil
}

// SetMembership applies a membership on the pod with this ordinal. 409
// (stale epoch, conflict) and 400 (invalid) come back as *APIError, with
// CurrentEpoch when the node sent it.
func (c *Client) SetMembership(ctx context.Context, kv *kvv1.KVCluster, ordinal int, req SetMembershipRequest) (*SetMembershipResult, error) {
	status, raw, err := c.do(ctx, c.timeout(), http.MethodPost, URL(kv, ordinal, "/admin/membership", nil), req)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, apiError(status, raw)
	}
	res := &SetMembershipResult{}
	if err := decode(raw, res); err != nil {
		return nil, err
	}
	return res, nil
}

// Handoff asks the pod with this ordinal whether it is done for epoch: for a
// member, its handoff to that membership has completed; for a node being
// removed, it has drained. It sends timeout=1s, so it answers within about a
// second.
//
//   - 200: the body, done.
//   - 504: the body, not done (yet); err is nil.
//   - 503 while the node stops: the body, not done, and an *APIError.
//   - 503 before the node has started, 400, others: nil, not done, *APIError.
func (c *Client) Handoff(ctx context.Context, kv *kvv1.KVCluster, ordinal int, epoch uint64) (h *Handoff, done bool, err error) {
	q := url.Values{
		"epoch":   {strconv.FormatUint(epoch, 10)},
		"timeout": {HandoffWait.String()},
	}
	status, raw, err := c.do(ctx, HandoffWait+c.timeout(), http.MethodGet, URL(kv, ordinal, "/admin/handoff", q), nil)
	if err != nil {
		return nil, false, err
	}
	switch status {
	case http.StatusOK, http.StatusGatewayTimeout:
		h = &Handoff{}
		if err := decode(raw, h); err != nil {
			return nil, false, err
		}
		return h, status == http.StatusOK, nil
	case http.StatusServiceUnavailable:
		// Either the handoff body (the node is stopping) or the error body
		// (it has not started); they share no field.
		var probe struct {
			Error   *string          `json:"error"`
			Handoff *json.RawMessage `json:"handoff"`
		}
		if json.Unmarshal(raw, &probe) == nil && probe.Error == nil && probe.Handoff != nil {
			h = &Handoff{}
			if err := decode(raw, h); err != nil {
				return nil, false, err
			}
			return h, false, &APIError{Status: status, Code: "node stopping", Message: "the handoff wait was cut short"}
		}
		return nil, false, apiError(status, raw)
	default:
		return nil, false, apiError(status, raw)
	}
}
