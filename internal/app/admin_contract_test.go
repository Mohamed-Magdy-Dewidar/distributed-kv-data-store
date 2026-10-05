package app

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// The admin API's JSON contract is checked in as fixtures under
// testdata/admin: one file per response shape, plus the POST request body.
// This test makes a real node produce each response and asserts it has the
// fixture's shape: the same fields at every level, with the same JSON types
// (values may differ). The operator (operator/internal/kvadmin) decodes the
// same files with unknown fields rejected, so a field added to, renamed in or
// dropped from a response here fails on one side or the other until both
// agree.

const contractDir = "testdata/admin"

// dynamicMaps are the object paths whose keys are node IDs: their keys are
// data, not fields, so each entry is compared with the fixture's entries'
// shape instead.
var dynamicMaps = []string{".members", ".peers"}

func loadFixture(t *testing.T, name string) any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(contractDir, name))
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return v
}

func jsonType(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case float64:
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	}
	return fmt.Sprintf("%T", v)
}

// shapeDiff lists where got's shape differs from want's.
func shapeDiff(path string, want, got any, out *[]string) {
	if jsonType(want) != jsonType(got) {
		*out = append(*out, fmt.Sprintf("%s: fixture has %s, response has %s", path, jsonType(want), jsonType(got)))
		return
	}
	wm, ok := want.(map[string]any)
	if !ok {
		return
	}
	gm := got.(map[string]any)
	if slices.Contains(dynamicMaps, path) {
		if len(wm) == 0 || len(gm) == 0 {
			*out = append(*out, fmt.Sprintf("%s: needs at least one entry on both sides to compare (fixture %d, response %d)", path, len(wm), len(gm)))
			return
		}
		var entry any
		for _, e := range wm {
			entry = e
			break
		}
		for k, e := range gm {
			shapeDiff(path+"["+k+"]", entry, e, out)
		}
		return
	}
	for k, w := range wm {
		g, ok := gm[k]
		if !ok {
			*out = append(*out, fmt.Sprintf("%s.%s: in the fixture, missing from the response", path, k))
			continue
		}
		shapeDiff(path+"."+k, w, g, out)
	}
	for k := range gm {
		if _, ok := wm[k]; !ok {
			*out = append(*out, fmt.Sprintf("%s.%s: in the response, missing from the fixture", path, k))
		}
	}
}

// response is one real admin response.
type response struct {
	status int
	body   any
}

func get(t *testing.T, method, url, body string) response {
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
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("%s %s: body is not JSON: %q", method, url, raw)
	}
	return response{resp.StatusCode, v}
}

func TestAdminResponsesMatchContractFixtures(t *testing.T) {
	got := map[string]response{} // fixture name -> the real response

	// A node not yet built: every endpoint answers 503 "node not started".
	{
		addr := knownAddr()
		p := newProbeServer(addr)
		if err := p.start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { p.stop() })
		waitForProbeUp(t, addr)
		got["node-not-started-503.json"] = get(t, "GET", "http://"+addr+"/admin/membership", "")
		// The other two endpoints answer the same.
		for _, r := range []struct{ method, path string }{{"POST", "/admin/membership"}, {"GET", "/admin/handoff?epoch=1"}} {
			resp := get(t, r.method, "http://"+addr+r.path, membershipJSON(1, map[string]string{"kv-0": "a:1"}))
			if resp.status != 503 {
				t.Errorf("%s %s before the node exists: %d, want 503", r.method, r.path, resp.status)
				continue
			}
			var d []string
			shapeDiff("", loadFixture(t, "node-not-started-503.json"), resp.body, &d)
			for _, l := range d {
				t.Errorf("%s %s: %s", r.method, r.path, l)
			}
		}
	}

	// kv-0 with a peer kv-1 that is down: GET, the POST outcomes, a
	// member's handoff.
	{
		a0, a1 := knownAddr(), knownAddr()
		x := fastNode(t, "kv-0", a0, map[string]string{"kv-1": a1})
		base := adminServer(t, x)
		got["membership-get-200.json"] = get(t, "GET", base+"/admin/membership", "")
		got["handoff-200.json"] = get(t, "GET", base+"/admin/handoff?epoch=0&timeout=5s", "")

		var req map[string]any
		raw, _ := json.Marshal(loadFixture(t, "membership-post-request.json"))
		_ = json.Unmarshal(raw, &req)
		req["epoch"] = 1
		req["members"] = map[string]string{"kv-0": a0, "kv-1": a1}
		body, _ := json.Marshal(req)
		got["membership-post-200.json"] = get(t, "POST", base+"/admin/membership", string(body))
		got["membership-post-409-stale.json"] = get(t, "POST", base+"/admin/membership", membershipJSON(0, map[string]string{"kv-0": a0}))
		got["membership-post-409-conflict.json"] = get(t, "POST", base+"/admin/membership", membershipJSON(1, map[string]string{"kv-0": a0}))
		got["membership-post-400.json"] = get(t, "POST", base+"/admin/membership", membershipJSON(2, map[string]string{"kv-0": a0, "kv-1": a0}))
		got["handoff-400.json"] = get(t, "GET", base+"/admin/handoff?epoch=abc", "")
	}

	// kv-0 leaving with data for kv-1, which is down: the handoff blocks,
	// so 504 when the timeout runs out, and 503 when the server stops
	// during the wait.
	{
		a0, a1 := knownAddr(), knownAddr()
		x := fastNode(t, "kv-0", a0, map[string]string{"kv-1": a1})
		for i := range 20 {
			x.Store.Put(fmt.Sprintf("key-%02d", i), "v", nil)
		}
		p := newProbeServer(a0)
		if err := p.start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { p.stop() })
		p.setNode(x)
		p.setReady(true)
		waitForProbeUp(t, a0)
		base := "http://" + a0
		if r := get(t, "POST", base+"/admin/membership", membershipJSON(1, map[string]string{"kv-1": a1})); r.status != 200 {
			t.Fatalf("removing kv-0: %d %v", r.status, r.body)
		}
		got["handoff-504.json"] = get(t, "GET", base+"/admin/handoff?epoch=1&timeout=300ms", "")

		stopping := make(chan response, 1)
		go func() {
			req, _ := http.NewRequest("GET", base+"/admin/handoff?epoch=1&timeout=1m", nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				stopping <- response{0, err.Error()}
				return
			}
			defer resp.Body.Close()
			var v any
			_ = json.NewDecoder(resp.Body).Decode(&v)
			stopping <- response{resp.StatusCode, v}
		}()
		time.Sleep(200 * time.Millisecond) // the request is waiting in the handler
		go p.stop()
		select {
		case r := <-stopping:
			got["handoff-503-stopping.json"] = r
		case <-time.After(10 * time.Second):
			t.Fatal("the handoff wait did not end when the server stopped")
		}
	}

	wantStatus := map[string]int{
		"node-not-started-503.json":         503,
		"membership-get-200.json":           200,
		"membership-post-200.json":          200,
		"membership-post-409-stale.json":    409,
		"membership-post-409-conflict.json": 409,
		"membership-post-400.json":          400,
		"handoff-200.json":                  200,
		"handoff-504.json":                  504,
		"handoff-503-stopping.json":         503,
		"handoff-400.json":                  400,
	}

	// Every fixture is covered, and nothing is covered without a fixture.
	files, err := filepath.Glob(filepath.Join(contractDir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		name := filepath.Base(f)
		if _, ok := wantStatus[name]; !ok && name != "membership-post-request.json" {
			t.Errorf("fixture %s is not checked against a real response", name)
		}
	}

	for name, status := range wantStatus {
		t.Run(name, func(t *testing.T) {
			r, ok := got[name]
			if !ok {
				t.Fatal("no response captured")
			}
			if r.status != status {
				t.Fatalf("status %d, want %d (body %v)", r.status, status, r.body)
			}
			var d []string
			shapeDiff("", loadFixture(t, name), r.body, &d)
			for _, l := range d {
				t.Error(l)
			}
		})
	}
}

// The request fixture is what the operator sends: a node must accept it.
func TestMembershipRequestFixtureIsAccepted(t *testing.T) {
	x := fastNode(t, "kv-0", knownAddr(), nil)
	base := adminServer(t, x)
	raw, err := os.ReadFile(filepath.Join(contractDir, "membership-post-request.json"))
	if err != nil {
		t.Fatal(err)
	}
	// The fixture's members are pod addresses, not kv-0's own: kv-0 leaves.
	if r := get(t, "POST", base+"/admin/membership", string(raw)); r.status != 200 {
		t.Fatalf("POST of the request fixture: %d %v, want 200", r.status, r.body)
	}
}
