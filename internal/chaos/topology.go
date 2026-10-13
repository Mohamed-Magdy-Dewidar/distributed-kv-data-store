// Package chaos predicts and checks the failure tests of docs/gke.md (session
// 3): which keys a fault should make fail (Topology, Fault, Expect), a load
// that records every request it sends (Record), a read-back of every write
// (Readback), and a check that compares what was measured with the
// prediction, request by request (Check). cmd/kvchaos is its command line.
//
// Placement is computed with the nodes' own ring code
// (hashring.NewHashRingFromMembers with node.VirtualNodesPerPhysical), and a
// Topology refuses a membership whose fingerprint differs from the one it
// computes: then its ring is the nodes' ring.
package chaos

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"distributed-kv-datastore/internal/config"
	"distributed-kv-datastore/internal/hashring"
	"distributed-kv-datastore/internal/node"
)

// ZoneLabel is the node label a pod's zone is read from.
const ZoneLabel = "topology.kubernetes.io/zone"

// Duration is a time.Duration that reads and writes JSON as "1.5s".
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// Timing is the part of the nodes' configuration that decides how long a
// fault takes to be noticed and to heal.
type Timing struct {
	HeartbeatInterval   Duration `json:"heartbeatInterval"`
	HeartbeatTimeout    Duration `json:"heartbeatTimeout"`
	MaxMissedHeartbeats int      `json:"maxMissedHeartbeats"`
	ReplicationTimeout  Duration `json:"replicationTimeout"`
	MaxReconnectBackoff Duration `json:"maxReconnectBackoff"`
	HintDelivery        Duration `json:"hintDelivery"`
	AntiEntropy         Duration `json:"antiEntropy"`
}

// Detection is how long after a peer stops answering the other nodes mark
// it dead: until then, requests that need it wait for their timeouts.
func (t Timing) Detection() time.Duration {
	return time.Duration(t.MaxMissedHeartbeats)*time.Duration(t.HeartbeatInterval) + time.Duration(t.HeartbeatTimeout)
}

// Recovery is how long after a peer is back the others may still treat it
// as dead: a connection in reconnect backoff, then a heartbeat round or two.
func (t Timing) Recovery() time.Duration {
	return time.Duration(t.MaxReconnectBackoff) + 2*time.Duration(t.HeartbeatInterval) + time.Duration(t.HeartbeatTimeout)
}

// Pod is one member: its ID, gRPC address, Kubernetes node and zone.
type Pod struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Node    string `json:"node"`
	Zone    string `json:"zone"`
}

// Topology is a cluster's membership, quorum settings and placement, as
// captured before a test. Build one with BuildTopology or NewTopology, or
// read one with LoadTopology.
type Topology struct {
	Epoch       uint64 `json:"epoch"`
	N           int    `json:"n"`
	W           int    `json:"w"`
	R           int    `json:"r"`
	Fingerprint string `json:"fingerprint"`
	Timing      Timing `json:"timing"`
	Pods        []Pod  `json:"pods"` // in ID order

	ring *hashring.HashRing
	byID map[string]Pod
}

// NewTopology is the topology of pods with the given quorum settings, at
// epoch 0. Tests use it; a real cluster's comes from BuildTopology.
func NewTopology(pods []Pod, n, w, r int, timing Timing) (*Topology, error) {
	t := &Topology{N: n, W: w, R: r, Timing: timing, Pods: slices.Clone(pods)}
	t.Fingerprint = node.Fingerprint(t.Members(), n)
	return t, t.init()
}

// LoadTopology reads a topology written by BuildTopology (as JSON).
func LoadTopology(b []byte) (*Topology, error) {
	t := &Topology{}
	if err := json.Unmarshal(b, t); err != nil {
		return nil, fmt.Errorf("topology: %w", err)
	}
	return t, t.init()
}

// init validates t, sorts its pods and builds its ring. The ring is built
// exactly as a node builds its view's (internal/node, buildView), and the
// fingerprint must be the one node.Fingerprint gives for t's members and N.
func (t *Topology) init() error {
	if len(t.Pods) == 0 {
		return errors.New("topology: no pods")
	}
	if t.N < 1 || t.W < 1 || t.R < 1 || t.W > t.N || t.R > t.N || t.N > len(t.Pods) {
		return fmt.Errorf("topology: N=%d W=%d R=%d with %d pods: want 1 <= W, R <= N <= pods", t.N, t.W, t.R, len(t.Pods))
	}
	slices.SortFunc(t.Pods, func(a, b Pod) int { return strings.Compare(a.ID, b.ID) })
	t.byID = make(map[string]Pod, len(t.Pods))
	for _, p := range t.Pods {
		if p.ID == "" || p.Address == "" || p.Zone == "" {
			return fmt.Errorf("topology: pod %+v: id, address and zone are required", p)
		}
		if _, dup := t.byID[p.ID]; dup {
			return fmt.Errorf("topology: pod %s listed twice", p.ID)
		}
		t.byID[p.ID] = p
	}
	ring, err := NewRing(t.Members(), t.N, t.Fingerprint)
	if err != nil {
		return fmt.Errorf("topology: %w", err)
	}
	t.ring = ring
	return nil
}

// NewRing is the ring of members, built as a node builds its view's
// (internal/node, buildView), provided fingerprint is the one
// node.Fingerprint gives for members and n: then it is the nodes' ring.
func NewRing(members map[string]string, n int, fingerprint string) (*hashring.HashRing, error) {
	if want := node.Fingerprint(members, n); fingerprint != want {
		return nil, fmt.Errorf("fingerprint %q, but these members with N=%d give %q: the nodes' ring is not the one this tool builds", fingerprint, n, want)
	}
	return hashring.NewHashRingFromMembers(node.VirtualNodesPerPhysical, slices.Sorted(maps.Keys(members)))
}

// Members is the membership: pod ID to address.
func (t *Topology) Members() map[string]string {
	m := make(map[string]string, len(t.Pods))
	for _, p := range t.Pods {
		m[p.ID] = p.Address
	}
	return m
}

// Pod returns the pod with id.
func (t *Topology) Pod(id string) (Pod, bool) {
	p, ok := t.byID[id]
	return p, ok
}

// Zones maps every zone to its pods, in ID order.
func (t *Topology) Zones() map[string][]string {
	z := map[string][]string{}
	for _, p := range t.Pods {
		z[p.Zone] = append(z[p.Zone], p.ID)
	}
	return z
}

// SortedZones is the zones' names, sorted.
func (t *Topology) SortedZones() []string { return slices.Sorted(maps.Keys(t.Zones())) }

// Replicas is key's preference list: the N pods that hold it, in order.
func (t *Topology) Replicas(key string) []string { return t.ring.GetPreferenceList(key, t.N) }

// JSON is t as LoadTopology reads it.
func (t *Topology) JSON() ([]byte, error) { return json.MarshalIndent(t, "", "  ") }

// TopologyInputs is what BuildTopology reads: kubectl's JSON for the pods,
// the nodes and the cluster's ConfigMap, and every member's
// GET /admin/membership body, by member ID.
type TopologyInputs struct {
	Pods, Nodes, ConfigMap []byte
	Memberships            map[string][]byte
}

// MembershipBody is the part of GET /admin/membership a topology uses.
type MembershipBody struct {
	Epoch       uint64            `json:"epoch"`
	Members     map[string]string `json:"members"`
	Fingerprint string            `json:"fingerprint"`
	Peers       map[string]struct {
		Alive     bool `json:"alive"`
		Reachable bool `json:"reachable"`
	} `json:"peers"`
}

type k8sList struct {
	Items []struct {
		Metadata struct {
			Name   string            `json:"name"`
			Labels map[string]string `json:"labels"`
		} `json:"metadata"`
		Spec struct {
			NodeName string `json:"nodeName"`
		} `json:"spec"`
		Status struct {
			Phase string `json:"phase"`
		} `json:"status"`
	} `json:"items"`
}

type k8sConfigMap struct {
	Data map[string]string `json:"data"`
}

// ConfigKey is the key of the cluster ConfigMap's node configuration
// (operator/internal/render.ConfigKey).
const ConfigKey = "config.yaml"

// BuildTopology captures a cluster that is healthy and settled: the
// ConfigMap's membership is the one every member reports, at the same epoch
// and fingerprint, every member sees every other alive and reachable, and
// every member is a running pod on a node with a zone label. Anything else
// is an error: a prediction made from a cluster in flux would be wrong.
func BuildTopology(in TopologyInputs) (*Topology, error) {
	var cm k8sConfigMap
	if err := json.Unmarshal(in.ConfigMap, &cm); err != nil {
		return nil, fmt.Errorf("configmap: %w", err)
	}
	raw, ok := cm.Data[ConfigKey]
	if !ok {
		return nil, fmt.Errorf("configmap: no %s", ConfigKey)
	}
	var cfg config.Config
	if err := yaml.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, fmt.Errorf("configmap: %s: %w", ConfigKey, err)
	}
	members := make(map[string]string, len(cfg.Cluster.Members))
	for _, m := range cfg.Cluster.Members {
		members[m.ID] = m.Address
	}

	var pods, nodes k8sList
	if err := json.Unmarshal(in.Pods, &pods); err != nil {
		return nil, fmt.Errorf("pods: %w", err)
	}
	if err := json.Unmarshal(in.Nodes, &nodes); err != nil {
		return nil, fmt.Errorf("nodes: %w", err)
	}
	zoneOf := map[string]string{}
	for _, n := range nodes.Items {
		zoneOf[n.Metadata.Name] = n.Metadata.Labels[ZoneLabel]
	}
	nodeOf := map[string]string{}
	for _, p := range pods.Items {
		if p.Status.Phase == "Running" {
			nodeOf[p.Metadata.Name] = p.Spec.NodeName
		}
	}

	var errs []error
	t := &Topology{
		Epoch: cfg.Cluster.Epoch, N: cfg.Cluster.N, W: cfg.Cluster.W, R: cfg.Cluster.R,
		Timing: Timing{
			HeartbeatInterval:   Duration(cfg.Intervals.Heartbeat),
			HeartbeatTimeout:    Duration(cfg.Timeouts.Heartbeat),
			MaxMissedHeartbeats: cfg.Health.MaxMissedHeartbeats,
			ReplicationTimeout:  Duration(cfg.Timeouts.Replication),
			MaxReconnectBackoff: Duration(cfg.Timeouts.MaxReconnectBackoff),
			HintDelivery:        Duration(cfg.Intervals.HintDelivery),
			AntiEntropy:         Duration(cfg.Intervals.AntiEntropy),
		},
		Fingerprint: node.Fingerprint(members, cfg.Cluster.N),
	}
	for _, id := range slices.Sorted(maps.Keys(members)) {
		nodeName, running := nodeOf[id]
		switch {
		case !running:
			errs = append(errs, fmt.Errorf("member %s: no running pod", id))
		case zoneOf[nodeName] == "":
			errs = append(errs, fmt.Errorf("member %s: node %q has no %s label", id, nodeName, ZoneLabel))
		}
		t.Pods = append(t.Pods, Pod{ID: id, Address: members[id], Node: nodeName, Zone: zoneOf[nodeName]})

		body, ok := in.Memberships[id]
		if !ok {
			errs = append(errs, fmt.Errorf("member %s: no GET /admin/membership body", id))
			continue
		}
		var m MembershipBody
		if err := json.Unmarshal(body, &m); err != nil {
			errs = append(errs, fmt.Errorf("member %s: membership: %w", id, err))
			continue
		}
		switch {
		case m.Epoch != t.Epoch:
			errs = append(errs, fmt.Errorf("member %s: at epoch %d, the ConfigMap at %d", id, m.Epoch, t.Epoch))
		case !maps.Equal(m.Members, members):
			errs = append(errs, fmt.Errorf("member %s: its members differ from the ConfigMap's", id))
		case m.Fingerprint != t.Fingerprint:
			errs = append(errs, fmt.Errorf("member %s: fingerprint %q, want %q", id, m.Fingerprint, t.Fingerprint))
		}
		for _, peer := range slices.Sorted(maps.Keys(members)) {
			if peer == id {
				continue
			}
			if p, ok := m.Peers[peer]; !ok || !p.Alive || !p.Reachable {
				errs = append(errs, fmt.Errorf("member %s: peer %s is not alive and reachable", id, peer))
			}
		}
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("the cluster is not healthy and settled:\n%w", err)
	}
	return t, t.init()
}
