package chaos

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Op is a request's operation.
type Op string

const (
	OpPut Op = "put"
	OpGet Op = "get"
)

// FaultKind is what a fault does to its pods.
type FaultKind string

const (
	// FaultNone: nothing the database sees (test 4, the system node).
	FaultNone FaultKind = "none"
	// FaultDown: the pods are unreachable from everyone, clients included
	// (a deleted VM, a lost zone, a pod cut off from everything).
	FaultDown FaultKind = "down"
	// FaultPartition: the pods are cut off from the other members, and
	// clients still reach both sides.
	FaultPartition FaultKind = "partition"
)

// Fault is a fault and the pods it acts on: the pods that are down, or one
// side of a partition (the other side is every other member).
type Fault struct {
	Kind FaultKind `json:"kind"`
	Pods []string  `json:"pods,omitempty"`
}

func (f Fault) String() string {
	if f.Kind == FaultNone {
		return string(FaultNone)
	}
	return string(f.Kind) + ":" + strings.Join(f.Pods, ",")
}

// ParseFault reads a fault spec against t:
//
//	none
//	down:kv-3[,kv-4...]       the pods are down
//	zone:<zone>               every pod in the zone is down
//	partition:kv-0[,kv-3...]  the pods are cut off from the other members
//	partition-zone:<zone>     the zone's pods are cut off from the others
func ParseFault(spec string, t *Topology) (Fault, error) {
	kind, arg, _ := strings.Cut(spec, ":")
	var f Fault
	switch kind {
	case "none":
		if arg != "" {
			return Fault{}, fmt.Errorf("fault %q: none takes no pods", spec)
		}
		return Fault{Kind: FaultNone}, nil
	case "down", "partition":
		f = Fault{Kind: FaultKind(kind), Pods: strings.Split(arg, ",")}
	case "zone", "partition-zone":
		pods, ok := t.Zones()[arg]
		if !ok {
			return Fault{}, fmt.Errorf("fault %q: no zone %q (zones: %s)", spec, arg, strings.Join(t.SortedZones(), ", "))
		}
		f = Fault{Kind: FaultDown, Pods: pods}
		if kind == "partition-zone" {
			f.Kind = FaultPartition
		}
	default:
		return Fault{}, fmt.Errorf("fault %q: want none, or down, zone, partition or partition-zone with their pods or zone", spec)
	}
	slices.Sort(f.Pods)
	f.Pods = slices.Compact(f.Pods)
	for _, id := range f.Pods {
		if _, ok := t.Pod(id); !ok {
			return Fault{}, fmt.Errorf("fault %q: no member %q", spec, id)
		}
	}
	if len(f.Pods) == 0 || len(f.Pods) == len(t.Pods) {
		return Fault{}, fmt.Errorf("fault %q: must name some members, not all", spec)
	}
	return f, nil
}

// Outcome is what a request is predicted to get.
type Outcome string

const (
	// Ok: the request succeeds.
	Ok Outcome = "ok"
	// Fail: the key has too few replicas the coordinator can reach.
	Fail Outcome = "fail"
	// TargetDown: the pod the client sent it to is down.
	TargetDown Outcome = "target-down"
	// Unknowable: the client's target pod is not known (a request through
	// the Service) and the outcome depends on it.
	Unknowable Outcome = "unknowable"
)

// need is the quorum op needs.
func (t *Topology) need(op Op) int {
	if op == OpGet {
		return t.R
	}
	return t.W
}

// Expect is what a request for key, sent to the pod target, gets during f.
// Quorums are strict (internal/node: Put and Get count only key's N
// replicas, and a hint never counts toward W): with every replica reachable
// from the coordinator counted, the request succeeds iff they reach W (a
// Put) or R (a Get). The coordinator is target, or the replica target
// forwards to, which for a partition is always on target's side. An empty
// target is a request through the Service, to a pod nobody knows.
func (t *Topology) Expect(f Fault, op Op, key, target string) Outcome {
	if f.Kind == FaultNone {
		return Ok
	}
	in := func(id string) bool { return slices.Contains(f.Pods, id) }
	replicas := t.Replicas(key)
	reach := func(side bool) int { // replicas on one side: in f.Pods or not
		n := 0
		for _, r := range replicas {
			if in(r) == side {
				n++
			}
		}
		return n
	}
	ok := func(n int) Outcome {
		if n >= t.need(op) {
			return Ok
		}
		return Fail
	}
	switch f.Kind {
	case FaultDown:
		if target != "" && in(target) {
			return TargetDown
		}
		return ok(reach(false))
	case FaultPartition:
		if target == "" {
			if a, b := ok(reach(true)), ok(reach(false)); a == b {
				return a
			}
			return Unknowable
		}
		return ok(reach(in(target)))
	}
	return Ok
}

// Unreachable is how many of key's replicas a coordinator that target
// reaches cannot reach during f: each is a hint, for a Put that succeeds
// once they are marked dead. An unknown side (empty target, partition) is -1.
func (t *Topology) Unreachable(f Fault, key, target string) int {
	if f.Kind == FaultNone {
		return 0
	}
	if f.Kind == FaultPartition && target == "" {
		return -1
	}
	targetIn := f.Kind == FaultPartition && slices.Contains(f.Pods, target)
	n := 0
	for _, r := range t.Replicas(key) {
		in := slices.Contains(f.Pods, r)
		if f.Kind == FaultDown && in || f.Kind == FaultPartition && in != targetIn {
			n++
		}
	}
	return n
}

// KeyName is the i-th key with prefix, as the recorder writes them.
func KeyName(prefix string, i int) string { return fmt.Sprintf("%s-%07d", prefix, i) }

// ParseKeySpec reads "PREFIX:COUNT" as the keys KeyName(PREFIX, 0) to
// KeyName(PREFIX, COUNT-1).
func ParseKeySpec(spec string) ([]string, error) {
	prefix, count, ok := strings.Cut(spec, ":")
	n, err := strconv.Atoi(count)
	if !ok || prefix == "" || err != nil || n <= 0 {
		return nil, fmt.Errorf("keys %q: want PREFIX:COUNT", spec)
	}
	keys := make([]string, n)
	for i := range keys {
		keys[i] = KeyName(prefix, i)
	}
	return keys, nil
}

// Side is one side of a fault, as the per-key predictions name it: "live"
// for a down fault, "group" (the fault's pods) and "rest" for a partition.
type Side string

const (
	SideLive  Side = "live"
	SideGroup Side = "group"
	SideRest  Side = "rest"
)

// Sides is f's sides, with a target pod on each (empty for none).
func (t *Topology) Sides(f Fault) map[Side]string {
	var group, rest string
	for _, p := range t.Pods {
		if slices.Contains(f.Pods, p.ID) {
			if group == "" {
				group = p.ID
			}
		} else if rest == "" {
			rest = p.ID
		}
	}
	if f.Kind == FaultPartition {
		return map[Side]string{SideGroup: group, SideRest: rest}
	}
	return map[Side]string{SideLive: rest}
}

// KeyPrediction is one key's placement and predicted outcomes during a
// fault, by side.
type KeyPrediction struct {
	Key      string                  `json:"key"`
	Replicas []string                `json:"replicas"`
	Zones    []string                `json:"zones"`
	Expect   map[Side]map[Op]Outcome `json:"expect"`
}

// PredictKey is key's prediction during f.
func (t *Topology) PredictKey(f Fault, key string) KeyPrediction {
	kp := KeyPrediction{Key: key, Replicas: t.Replicas(key), Expect: map[Side]map[Op]Outcome{}}
	for _, r := range kp.Replicas {
		p, _ := t.Pod(r)
		kp.Zones = append(kp.Zones, p.Zone)
	}
	for side, target := range t.Sides(f) {
		kp.Expect[side] = map[Op]Outcome{OpPut: t.Expect(f, OpPut, key, target), OpGet: t.Expect(f, OpGet, key, target)}
	}
	return kp
}

// FaultSummary counts, for one fault, the keys that fail from each side.
type FaultSummary struct {
	Fault Fault
	Keys  int
	// Fails counts the keys each op fails for, from a target on each side.
	Fails map[Side]map[Op]int
	// Nowhere counts the keys each op fails for from every side.
	Nowhere map[Op]int
}

// Summarize is f's summary over keys.
func (t *Topology) Summarize(f Fault, keys []string) FaultSummary {
	s := FaultSummary{Fault: f, Keys: len(keys), Fails: map[Side]map[Op]int{}, Nowhere: map[Op]int{}}
	sides := t.Sides(f)
	for side := range sides {
		s.Fails[side] = map[Op]int{}
	}
	for _, k := range keys {
		for _, op := range []Op{OpPut, OpGet} {
			nowhere := true
			for side, target := range sides {
				if t.Expect(f, op, k, target) != Ok {
					s.Fails[side][op]++
				} else {
					nowhere = false
				}
			}
			if nowhere {
				s.Nowhere[op]++
			}
		}
	}
	return s
}

// Share is the fraction of keys each pod holds a replica of.
func (t *Topology) Share(keys []string) map[string]float64 {
	counts := map[string]int{}
	for _, k := range keys {
		for _, r := range t.Replicas(k) {
			counts[r]++
		}
	}
	share := make(map[string]float64, len(t.Pods))
	for _, p := range t.Pods {
		share[p.ID] = float64(counts[p.ID]) / float64(len(keys))
	}
	return share
}

// StandardFaults is every fault worth predicting before a session: each pod
// down, each zone down, and each zone partitioned from the rest.
func (t *Topology) StandardFaults() []Fault {
	var fs []Fault
	for _, p := range t.Pods {
		fs = append(fs, Fault{Kind: FaultDown, Pods: []string{p.ID}})
	}
	zones := t.Zones()
	for _, z := range t.SortedZones() {
		if len(zones[z]) < len(t.Pods) {
			fs = append(fs, Fault{Kind: FaultDown, Pods: zones[z]})
		}
	}
	for _, z := range t.SortedZones() {
		if len(zones[z]) < len(t.Pods) {
			fs = append(fs, Fault{Kind: FaultPartition, Pods: zones[z]})
		}
	}
	return fs
}
