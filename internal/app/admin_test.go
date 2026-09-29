package app

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"distributed-kv-datastore/internal/node"
	"distributed-kv-datastore/internal/rpc"
)

// adminServer runs a probe server for nd on a free port, ready, stopped at
// cleanup, and returns its base URL.
func adminServer(t *testing.T, nd *node.Node) string {
	t.Helper()
	addr := freeAddr(t)
	p := newProbeServer(addr)
	if err := p.start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.stop() })
	p.setNode(nd)
	p.setReady(true)
	waitForProbeUp(t, addr)
	return "http://" + addr
}

// call sends one request and returns the status and the decoded JSON object
// (nil for an empty body).
func call(t *testing.T, method, url, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			out = map[string]any{"text": string(raw)} // e.g. the mux's own 405 body
		}
	}
	return resp.StatusCode, out
}

func membershipJSON(epoch uint64, members map[string]string) string {
	b, _ := json.Marshal(map[string]any{"epoch": epoch, "members": members})
	return string(b)
}

func fastNode(id, addr string, neighbors map[string]string) *node.Node {
	nd := node.New(id, addr, 1, 1, 1, neighbors)
	nd.QuorumConfig.HeartbeatTimeout = 40 * time.Millisecond
	nd.QuorumConfig.MaxReconnectBackoff = 50 * time.Millisecond
	nd.QuorumConfig.ReplicationTimeout = time.Second
	return nd
}

func serveRPC(t *testing.T, nd *node.Node, addr string) {
	t.Helper()
	l, err := rpc.Serve(addr, nd.Store, nd)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l.Stop)
}

func eventuallyTrue(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", within, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// (a) POST /admin/membership maps SetMembership's outcomes onto statuses.
func TestPostMembershipStatusMapping(t *testing.T) {
	addr := freeAddr(t)
	nd := fastNode("kv-0", addr, nil)
	base := adminServer(t, nd)
	own := map[string]string{"kv-0": addr}

	status, body := call(t, "POST", base+"/admin/membership", membershipJSON(1, own))
	if status != 200 || body["changed"] != true || body["epoch"] != float64(1) {
		t.Fatalf("adopting: %d %v", status, body)
	}
	status, body = call(t, "POST", base+"/admin/membership", membershipJSON(1, own))
	if status != 200 || body["changed"] != false {
		t.Fatalf("the same membership again: %d %v, want 200 changed=false", status, body)
	}

	status, body = call(t, "POST", base+"/admin/membership", membershipJSON(1, map[string]string{"kv-0": addr, "kv-1": "127.0.0.1:1"}))
	if status != 409 || body["error"] != "membership conflict" || body["currentEpoch"] != float64(1) {
		t.Fatalf("conflict: %d %v", status, body)
	}
	status, body = call(t, "POST", base+"/admin/membership", membershipJSON(0, own))
	if status != 409 || body["error"] != "stale epoch" || body["currentEpoch"] != float64(1) {
		t.Fatalf("stale: %d %v", status, body)
	}

	for name, req := range map[string]string{
		"no members":        membershipJSON(9, map[string]string{}),
		"duplicate address": membershipJSON(9, map[string]string{"kv-0": addr, "kv-1": addr}),
		"id with '#'":       membershipJSON(9, map[string]string{"kv#0": addr}),
		"empty address":     membershipJSON(9, map[string]string{"kv-0": ""}),
		"not JSON":          `{"epoch": 9, "members":`,
		"unknown field":     `{"epoch": 9, "members": {"kv-0": "a:1"}, "extra": true}`,
		"trailing data":     membershipJSON(9, own) + ` {}`,
		"epoch missing":     `{"members": {"kv-0": "a:1"}}`,
		"negative epoch":    `{"epoch": -1, "members": {"kv-0": "a:1"}}`,
	} {
		if status, body := call(t, "POST", base+"/admin/membership", req); status != 400 {
			t.Errorf("%s: %d %v, want 400", name, status, body)
		}
	}

	huge := map[string]string{"kv-0": strings.Repeat("x", maxAdminBody+1)}
	if status, body := call(t, "POST", base+"/admin/membership", membershipJSON(9, huge)); status != 400 || body["error"] != "request body too large" {
		t.Errorf("oversized body: %d %v, want 400 request body too large", status, body)
	}

	if epoch, _ := nd.Membership(); epoch != 1 {
		t.Fatalf("a rejected request changed the epoch to %d", epoch)
	}
	if status, _ := call(t, "PUT", base+"/admin/membership", "{}"); status != 405 {
		t.Errorf("PUT: %d, want 405", status)
	}
}

// (b) GET /admin/membership reports the view, the handoff and what
// heartbeats have shown of the peers.
func TestGetMembershipReflectsViewHandoffAndPeers(t *testing.T) {
	ax, ay := freeAddr(t), freeAddr(t)
	x := fastNode("kv-0", ax, map[string]string{"kv-1": ay})
	y := fastNode("kv-1", ay, map[string]string{"kv-0": ax})
	serveRPC(t, y, ay)
	members := map[string]string{"kv-0": ax, "kv-1": ay}
	for _, nd := range []*node.Node{x, y} {
		if _, err := nd.SetMembership(2, members); err != nil {
			t.Fatal(err)
		}
	}
	x.StartHeartbeatLoop(t.Context(), 50*time.Millisecond)
	t.Cleanup(x.StopBackgroundLoops)
	base := adminServer(t, x)

	var body map[string]any
	eventuallyTrue(t, 5*time.Second, "kv-1 to be seen at epoch 2", func() bool {
		var status int
		status, body = call(t, "GET", base+"/admin/membership", "")
		peers, _ := body["peers"].(map[string]any)
		peer, _ := peers["kv-1"].(map[string]any)
		return status == 200 && peer["lastSeenEpoch"] == float64(2)
	})
	if body["epoch"] != float64(2) || body["fingerprint"] != x.CurrentMembership().Fingerprint {
		t.Fatalf("epoch/fingerprint: %v", body)
	}
	got, _ := body["members"].(map[string]any)
	if len(got) != 2 || got["kv-0"] != ax || got["kv-1"] != ay {
		t.Fatalf("members: %v", body["members"])
	}
	if h, _ := body["handoff"].(map[string]any); h["epoch"] != float64(2) || h["done"] != true {
		t.Fatalf("handoff: %v", body["handoff"])
	}
	if peer := body["peers"].(map[string]any)["kv-1"].(map[string]any); peer["alive"] != true {
		t.Fatalf("peer: %v", peer)
	}
}

// (c) /readyz says 503 once the view no longer includes this node.
func TestReadyzIsNotReadyOnceRemovedFromTheMembership(t *testing.T) {
	ax, ay := freeAddr(t), freeAddr(t)
	nd := fastNode("kv-0", ax, map[string]string{"kv-1": ay})
	base := adminServer(t, nd)

	if status, _ := call(t, "GET", base+"/readyz", ""); status != 200 {
		t.Fatalf("/readyz while a member: %d", status)
	}
	if status, body := call(t, "POST", base+"/admin/membership", membershipJSON(1, map[string]string{"kv-1": ay})); status != 200 {
		t.Fatalf("removing kv-0: %d %v", status, body)
	}
	if status, _ := call(t, "GET", base+"/readyz", ""); status != 503 {
		t.Fatalf("/readyz after removal: %d, want 503", status)
	}
	if status, _ := call(t, "GET", base+"/livez", ""); status != 200 {
		t.Fatalf("/livez after removal: %d, want 200", status)
	}
}

func TestParseHandoffWait(t *testing.T) {
	for in, want := range map[string]time.Duration{"": time.Minute, "30s": 30 * time.Second, "5m": 5 * time.Minute, "24h": 10 * time.Minute} {
		if got, err := parseHandoffWait(in); err != nil || got != want {
			t.Errorf("parseHandoffWait(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"soon", "0s", "-5s", "10"} {
		if _, err := parseHandoffWait(in); err == nil {
			t.Errorf("parseHandoffWait(%q): expected an error", in)
		}
	}
}

// (d) GET /admin/handoff blocks until the handoff is done: 504 with the
// status while a leaving node's target is down, 200 once it is up.
func TestHandoffWait(t *testing.T) {
	ax, ay := freeAddr(t), freeAddr(t)
	x := fastNode("kv-0", ax, map[string]string{"kv-1": ay})
	for i := range 20 {
		x.Store.Put(fmt.Sprintf("key-%02d", i), "v", nil)
	}
	x.StartHeartbeatLoop(t.Context(), 50*time.Millisecond)
	t.Cleanup(x.StopBackgroundLoops)
	base := adminServer(t, x)

	for name, q := range map[string]string{
		"epoch missing":      "",
		"epoch not a number": "?epoch=abc",
		"epoch negative":     "?epoch=-1",
		"timeout bogus":      "?epoch=1&timeout=bogus",
		"timeout negative":   "?epoch=1&timeout=-5s",
	} {
		if status, _ := call(t, "GET", base+"/admin/handoff"+q, ""); status != 400 {
			t.Errorf("%s: %d, want 400", name, status)
		}
	}

	// kv-0 leaves; kv-1, which must receive its keys, is not up.
	if status, body := call(t, "POST", base+"/admin/membership", membershipJSON(1, map[string]string{"kv-1": ay})); status != 200 {
		t.Fatalf("removing kv-0: %d %v", status, body)
	}
	status, body := call(t, "GET", base+"/admin/handoff?epoch=1&timeout=300ms", "")
	if status != 504 || body["drained"] != false || body["member"] != false {
		t.Fatalf("with the target down: %d %v, want 504 not drained", status, body)
	}
	if h := body["handoff"].(map[string]any); h["done"] != false || h["pending"].(float64) == 0 {
		t.Fatalf("handoff status while blocked: %v", h)
	}

	y := fastNode("kv-1", ay, map[string]string{"kv-0": ax})
	if _, err := y.SetMembership(1, map[string]string{"kv-1": ay}); err != nil {
		t.Fatal(err)
	}
	serveRPC(t, y, ay)
	status, body = call(t, "GET", base+"/admin/handoff?epoch=1&timeout=20s", "")
	if status != 200 || body["drained"] != true {
		t.Fatalf("with the target up: %d %v, want 200 drained", status, body)
	}
	if got, _, _ := y.Store.Get("key-00"); len(got) == 0 {
		t.Fatal("the leaving node reported drained before its data reached kv-1")
	}

	// A node that stays in the view is done as soon as its own handoff is.
	baseY := adminServer(t, y)
	if status, body := call(t, "GET", baseY+"/admin/handoff?epoch=1&timeout=5s", ""); status != 200 || body["member"] != true {
		t.Fatalf("a member's handoff wait: %d %v", status, body)
	}
}

// (e) A leaving node isn't drained just because its own handoff is done: the
// members that remain must have been seen at the new epoch too.
func TestDrainedNeedsTheRemainingMembersToAcknowledge(t *testing.T) {
	ax, ay, az := freeAddr(t), freeAddr(t), freeAddr(t)
	x := fastNode("kv-0", ax, map[string]string{"kv-1": ay, "kv-2": az})
	y := fastNode("kv-1", ay, map[string]string{"kv-0": ax, "kv-2": az})
	z := fastNode("kv-2", az, map[string]string{"kv-0": ax, "kv-1": ay})
	serveRPC(t, y, ay)
	remaining := map[string]string{"kv-1": ay, "kv-2": az}
	for _, nd := range []*node.Node{x, y, z} {
		if _, err := nd.SetMembership(1, remaining); err != nil {
			t.Fatal(err)
		}
	}
	x.StartHeartbeatLoop(t.Context(), 50*time.Millisecond)
	t.Cleanup(x.StopBackgroundLoops)
	base := adminServer(t, x)

	// kv-0 has no data to move, so its handoff is done at once. kv-2 is down
	// and has never been seen at epoch 1.
	status, body := call(t, "GET", base+"/admin/handoff?epoch=1&timeout=700ms", "")
	if status != 504 || body["drained"] != false {
		t.Fatalf("with a remaining member unseen: %d %v, want 504", status, body)
	}
	if h := body["handoff"].(map[string]any); h["done"] != true {
		t.Fatalf("the handoff itself should be done: %v", h)
	}
	if peers := body["peers"].(map[string]any); peers["kv-1"].(map[string]any)["lastSeenEpoch"] != float64(1) {
		t.Fatalf("kv-1 should have been seen at epoch 1: %v", peers)
	}

	serveRPC(t, z, az)
	status, body = call(t, "GET", base+"/admin/handoff?epoch=1&timeout=20s", "")
	if status != 200 || body["drained"] != true {
		t.Fatalf("with every remaining member seen: %d %v, want 200", status, body)
	}
}
