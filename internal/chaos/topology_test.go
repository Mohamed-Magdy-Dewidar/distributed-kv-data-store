package chaos

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"distributed-kv-datastore/internal/node"
)

// testTiming is the operator's (operator/internal/render, ConfigMap).
var testTiming = Timing{
	HeartbeatInterval: Duration(time.Second), HeartbeatTimeout: Duration(500 * time.Millisecond), MaxMissedHeartbeats: 3,
	ReplicationTimeout: Duration(5 * time.Second), MaxReconnectBackoff: Duration(5 * time.Second),
	HintDelivery: Duration(10 * time.Second), AntiEntropy: Duration(15 * time.Minute),
}

func addr(id string) string { return id + ".kv.kvstore.svc.cluster.local:7000" }

// sixPods is six pods, two per zone, with kv-0 and kv-2 sharing zone-a.
func sixPods(t *testing.T) *Topology {
	t.Helper()
	zones := map[string]string{"kv-0": "zone-a", "kv-2": "zone-a", "kv-1": "zone-b", "kv-3": "zone-b", "kv-4": "zone-c", "kv-5": "zone-c"}
	var pods []Pod
	for i := range 6 {
		id := fmt.Sprintf("kv-%d", i)
		pods = append(pods, Pod{ID: id, Address: addr(id), Node: "node-" + id, Zone: zones[id]})
	}
	topo, err := NewTopology(pods, 3, 2, 2, testTiming)
	if err != nil {
		t.Fatal(err)
	}
	return topo
}

// benchPreloadKeys is the k6 preload's 20,000 keys (deploy/bench/script.js).
func benchPreloadKeys() []string {
	keys := make([]string, 20000)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench-pre-%06d", i)
	}
	return keys
}

// TestSixPodRingSkew pins what the ring does with six pods (the GKE
// cluster's kv-0 … kv-5) and the k6 preload's keys: the zone-loss share of
// a fault depends on which two pods share the zone, from 6.7% to 45.2%,
// not the 20% an even ring would give (4 of the 20 three-pod sets hold a
// given pair), and each pod's share of keys ranges from 33.8% to 71.0%,
// not 50% (docs/known-limitations.md). A change to the ring changes these.
func TestSixPodRingSkew(t *testing.T) {
	topo := sixPods(t)
	keys := benchPreloadKeys()

	s := topo.Summarize(Fault{Kind: FaultDown, Pods: []string{"kv-0", "kv-2"}}, keys)
	const wantPair = 9044 // 45.2%
	if got := s.Fails[SideLive][OpPut]; got != wantPair {
		t.Errorf("zone {kv-0,kv-2} down: %d puts fail, want %d", got, wantPair)
	}
	if got := s.Fails[SideLive][OpGet]; got != wantPair {
		t.Errorf("zone {kv-0,kv-2} down: %d gets fail, want %d (strict quorums: the same keys as puts)", got, wantPair)
	}

	s = topo.Summarize(Fault{Kind: FaultDown, Pods: []string{"kv-0", "kv-1"}}, keys)
	const wantLeast = 1336 // 6.7%
	if got := s.Fails[SideLive][OpPut]; got != wantLeast {
		t.Errorf("{kv-0,kv-1} down: %d puts fail, want %d", got, wantLeast)
	}

	share := topo.Share(keys)
	for id, want := range map[string]float64{"kv-0": 0.710, "kv-1": 0.338} {
		if got := share[id]; got < want-0.0005 || got > want+0.0005 {
			t.Errorf("%s holds %.4f of the keys, want %.3f", id, got, want)
		}
	}
}

func TestTopologyJSONRoundTrip(t *testing.T) {
	topo := sixPods(t)
	b, err := topo.JSON()
	if err != nil {
		t.Fatal(err)
	}
	back, err := LoadTopology(b)
	if err != nil {
		t.Fatal(err)
	}
	if back.Timing != topo.Timing || back.N != 3 || len(back.Pods) != 6 {
		t.Fatalf("round trip changed the topology: %+v", back)
	}
	for _, k := range []string{"a", "chaos-pre-0000001", "z"} {
		if fmt.Sprint(back.Replicas(k)) != fmt.Sprint(topo.Replicas(k)) {
			t.Errorf("replicas of %q differ after a round trip", k)
		}
	}
}

func TestLoadTopologyRefusesAForeignFingerprint(t *testing.T) {
	b, _ := sixPods(t).JSON()
	b = []byte(strings.Replace(string(b), `"n": 3`, `"n": 2`, 1))
	if _, err := LoadTopology(b); err == nil || !strings.Contains(err.Error(), "fingerprint") {
		t.Fatalf("a topology whose fingerprint is not its members' and N's loaded: %v", err)
	}
}

// cluster is the kubectl and admin API JSON of a three-pod cluster that
// BuildTopology accepts; each test breaks one thing.
type cluster struct {
	epoch     uint64
	zones     map[string]string // node → zone
	podNode   map[string]string // pod → node
	phase     map[string]string // pod → phase (default Running)
	members   map[string]map[string]string
	epochs    map[string]uint64
	unreached map[string]string // pod → a peer it reports unreachable
}

func newCluster() *cluster {
	c := &cluster{
		zones:     map[string]string{"n0": "z0", "n1": "z1", "n2": "z2"},
		podNode:   map[string]string{"kv-0": "n0", "kv-1": "n1", "kv-2": "n2"},
		phase:     map[string]string{},
		members:   map[string]map[string]string{},
		epochs:    map[string]uint64{},
		unreached: map[string]string{},
	}
	return c
}

func (c *cluster) all() map[string]string {
	return map[string]string{"kv-0": addr("kv-0"), "kv-1": addr("kv-1"), "kv-2": addr("kv-2")}
}

func (c *cluster) inputs(t *testing.T) TopologyInputs {
	t.Helper()
	mustJSON := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	var pods, nodes []any
	for p, n := range c.podNode {
		phase := c.phase[p]
		if phase == "" {
			phase = "Running"
		}
		pods = append(pods, map[string]any{"metadata": map[string]any{"name": p}, "spec": map[string]any{"nodeName": n}, "status": map[string]any{"phase": phase}})
	}
	for n, z := range c.zones {
		labels := map[string]string{}
		if z != "" {
			labels[ZoneLabel] = z
		}
		nodes = append(nodes, map[string]any{"metadata": map[string]any{"name": n, "labels": labels}})
	}
	var y strings.Builder
	fmt.Fprintf(&y, "listen:\n  grpc: \":7000\"\n  http: \":8080\"\ndataDir: /data\ncluster:\n  n: 3\n  w: 2\n  r: 2\n  epoch: %d\n  members:\n", c.epoch)
	for _, id := range []string{"kv-0", "kv-1", "kv-2"} {
		fmt.Fprintf(&y, "    - id: %s\n      address: %s\n", id, addr(id))
	}
	y.WriteString("intervals:\n  heartbeat: 1s\n  hintDelivery: 10s\n  antiEntropy: 15m\n  compaction: 30s\ntimeouts:\n  replication: 5s\n  maxReconnectBackoff: 5s\n  shutdown: 20s\n  heartbeat: 500ms\nhealth:\n  maxMissedHeartbeats: 3\n")

	in := TopologyInputs{
		Pods:        mustJSON(map[string]any{"items": pods}),
		Nodes:       mustJSON(map[string]any{"items": nodes}),
		ConfigMap:   mustJSON(map[string]any{"data": map[string]string{ConfigKey: y.String()}}),
		Memberships: map[string][]byte{},
	}
	for id := range c.all() {
		members := c.all()
		if m, ok := c.members[id]; ok {
			members = m
		}
		epoch := c.epoch
		if e, ok := c.epochs[id]; ok {
			epoch = e
		}
		peers := map[string]any{}
		for p := range c.all() {
			if p != id {
				peers[p] = map[string]any{"alive": true, "reachable": c.unreached[id] != p, "lastSeenEpoch": epoch}
			}
		}
		in.Memberships[id] = mustJSON(map[string]any{"epoch": epoch, "members": members, "fingerprint": node.Fingerprint(members, 3), "peers": peers})
	}
	return in
}

func TestBuildTopology(t *testing.T) {
	c := newCluster()
	c.epoch = 4
	topo, err := BuildTopology(c.inputs(t))
	if err != nil {
		t.Fatal(err)
	}
	if topo.Epoch != 4 || topo.N != 3 || topo.W != 2 || topo.R != 2 {
		t.Errorf("epoch/N/W/R = %d/%d/%d/%d, want 4/3/2/2", topo.Epoch, topo.N, topo.W, topo.R)
	}
	if p, _ := topo.Pod("kv-1"); p.Zone != "z1" || p.Node != "n1" || p.Address != addr("kv-1") {
		t.Errorf("kv-1 = %+v", p)
	}
	if got := topo.Timing.Detection(); got != 3500*time.Millisecond {
		t.Errorf("detection = %v, want 3.5s (3 missed 1s heartbeats + a 500ms timeout)", got)
	}
	if topo.Timing.AntiEntropy != Duration(15*time.Minute) {
		t.Errorf("anti-entropy interval = %v", time.Duration(topo.Timing.AntiEntropy))
	}
}

func TestBuildTopologyRefusesAClusterInFlux(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(*cluster)
		want   string
	}{
		{"a member at another epoch", func(c *cluster) { c.epochs["kv-1"] = 1 }, "kv-1: at epoch 1"},
		{"a member with other members", func(c *cluster) {
			c.members["kv-2"] = map[string]string{"kv-0": addr("kv-0"), "kv-2": addr("kv-2"), "kv-9": addr("kv-9")}
		}, "kv-2: its members differ"},
		{"a pod not running", func(c *cluster) { c.phase["kv-0"] = "Pending" }, "kv-0: no running pod"},
		{"a node without a zone", func(c *cluster) { c.zones["n2"] = "" }, `node "n2" has no`},
		{"an unreachable peer", func(c *cluster) { c.unreached["kv-0"] = "kv-1" }, "kv-0: peer kv-1 is not alive and reachable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster()
			tc.break_(c)
			_, err := BuildTopology(c.inputs(t))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one containing %q", err, tc.want)
			}
		})
	}
}

func TestBuildTopologyNeedsEveryMembership(t *testing.T) {
	in := newCluster().inputs(t)
	delete(in.Memberships, "kv-2")
	if _, err := BuildTopology(in); err == nil || !strings.Contains(err.Error(), "kv-2: no GET /admin/membership") {
		t.Fatalf("err = %v", err)
	}
}
