// Package planner decides the operator's next step for a KVCluster. Plan is
// a pure function: it reads an Observation (the pods' admin API answers, the
// StatefulSet, the ConfigMap, the PVCs, the spec and status, and the time)
// and returns exactly one Action and the status to report. It does no I/O and
// reads no clock, so every decision can be tested from a constructed
// observation, and an operator that restarts between any two steps makes the
// same decision again from what it observes.
//
// Membership changes follow docs/kubernetes.md, one node at a time:
//
//	scale up, k -> k+1 (base epoch E):
//	  U1 write the ConfigMap at E+1 with members 0..k
//	  U2 set the StatefulSet to k+1 replicas
//	  U3 wait until every pod holds E+1 (pod k starts at E+1 and announces it)
//	  U4 wait until every member's handoff to E+1 is done
//	scale down, k+1 -> k (base epoch E):
//	  D1 POST {E+1, members without k} to pod 0
//	  D2 wait until pod k has drained at E+1
//	  D3 write the ConfigMap at E+1 without k
//	  D4 set the StatefulSet to k replicas
//	  D5 wait until pod k and its PVC are gone and every handoff to E+1 is done
//
// Which step comes next is read off the observation alone: the ConfigMap's
// member count against the StatefulSet's replicas, and the pods' epochs and
// member sets against the ConfigMap's epoch. A combination none of these
// steps produces is Degraded, and the operator then does nothing.
package planner

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	kvv1 "github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/api/v1alpha1"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/kvadmin"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/render"
)

// Observation is everything Plan decides from.
type Observation struct {
	// Now is the time of the observation; Plan reads no clock.
	Now time.Time
	// Cluster is the KVCluster: its spec, and the status last written.
	Cluster *kvv1.KVCluster
	// StatefulSet is nil when it does not exist.
	StatefulSet *StatefulSetObs
	// ConfigMap is nil when it does not exist.
	ConfigMap *ConfigMapObs
	// Pods has one entry per ordinal from 0 up to the higher of the
	// StatefulSet's replicas and the highest existing pod, in order.
	Pods []PodObs
	// PVCs lists the ordinals that have a data PVC.
	PVCs []int
}

// StatefulSetObs is the node StatefulSet.
type StatefulSetObs struct {
	Replicas      int32
	ReadyReplicas int32
	// RolledOut: every pod runs the StatefulSet's current revision.
	RolledOut bool
	// TemplateCurrent: the pod template is the one the spec renders (image,
	// resources).
	TemplateCurrent bool
}

// ConfigMapObs is the membership the node ConfigMap carries.
type ConfigMapObs struct {
	Epoch   uint64
	Members map[string]string
}

// PodObs is one ordinal's pod.
type PodObs struct {
	Ordinal     int
	Exists      bool
	Terminating bool
	Ready       bool
	// Since is when Ready last changed (the pod's Ready condition), or when
	// the pod was created if it has never been ready.
	Since time.Time
	// Membership is the pod's GET /admin/membership answer; nil when its
	// admin API did not answer.
	Membership *kvadmin.Membership
	// UnreachableSince is when the pod's admin API stopped answering, while
	// it does not; zero when it answers or nobody knows since when (the
	// operator restarted). Tracked by the controller between reconciles.
	UnreachableSince time.Time
	// Drain is the pod's GET /admin/handoff answer for its own epoch, asked
	// only of a pod outside its own membership; nil otherwise.
	Drain *kvadmin.Handoff
}

func (p PodObs) reachable() bool { return p.Membership != nil }

// healthy: the pod exists, is ready, and its admin API answers.
func (p PodObs) healthy() bool { return p.Exists && !p.Terminating && p.Ready && p.reachable() }

// ActionKind is what the operator does next.
type ActionKind int

const (
	// ActNone: nothing to do; the cluster is Ready.
	ActNone ActionKind = iota
	// ActWait: nothing to do until something changes; Reason says what.
	ActWait
	// ActWriteConfigMap: write the ConfigMap for (Epoch, Members).
	ActWriteConfigMap
	// ActScaleStatefulSet: set the StatefulSet's replicas, creating it from
	// the spec if it does not exist.
	ActScaleStatefulSet
	// ActUpdateTemplate: apply the spec's pod template (image, resources).
	ActUpdateTemplate
	// ActPostMembership: POST {Epoch, Members} to the pod at Ordinal.
	ActPostMembership
)

func (k ActionKind) String() string {
	return [...]string{"None", "Wait", "WriteConfigMap", "ScaleStatefulSet", "UpdateTemplate", "PostMembership"}[k]
}

// Action is the one thing to do now.
type Action struct {
	Kind         ActionKind
	Reason       string
	RequeueAfter time.Duration

	Epoch    uint64            // WriteConfigMap, PostMembership
	Members  map[string]string // WriteConfigMap, PostMembership
	Replicas int32             // ScaleStatefulSet, UpdateTemplate
	Ordinal  int               // PostMembership
}

func (a Action) String() string {
	switch a.Kind {
	case ActWriteConfigMap, ActPostMembership:
		ids := slices.Sorted(maps.Keys(a.Members))
		return fmt.Sprintf("%s(epoch %d, %d members %s)", a.Kind, a.Epoch, len(ids), strings.Join(ids, ","))
	case ActScaleStatefulSet, ActUpdateTemplate:
		return fmt.Sprintf("%s(%d)", a.Kind, a.Replicas)
	default:
		return fmt.Sprintf("%s(%s)", a.Kind, a.Reason)
	}
}

// Status is what to report in the KVCluster's status.
type Status struct {
	Phase   kvv1.Phase
	Reason  string // CamelCase, for conditions
	Message string

	Epoch         uint64
	Members       map[string]string
	Replicas      int32
	ReadyReplicas int32
	// DrainStartedAt is set while a removed node drains (D2).
	DrainStartedAt *time.Time
}

// Status reasons, one per situation; conditions carry them.
const (
	ReasonCreatingConfigMap       = "CreatingConfigMap"       // the ConfigMap is being created (bootstrap)
	ReasonCreatingStatefulSet     = "CreatingStatefulSet"     // the StatefulSet is being created (bootstrap)
	ReasonConfigMapMissing        = "ConfigMapMissing"        // the ConfigMap is gone while the StatefulSet exists
	ReasonConfigMapUnexpected     = "ConfigMapUnexpected"     // the ConfigMap holds a membership the operator does not write
	ReasonAddressMismatch         = "AddressMismatch"         // a pod holds a member the operator did not write
	ReasonMembershipConflict      = "MembershipConflict"      // two pods hold different memberships at one epoch
	ReasonUnexplainedDivergence   = "UnexplainedDivergence"   // the state matches no step of a membership change
	ReasonPodUnavailable          = "PodUnavailable"          // a pod has been unready or unreachable for longer than Grace
	ReasonAddingPod               = "AddingPod"               // U1 or U2
	ReasonWaitingForEpoch         = "WaitingForEpoch"         // U3
	ReasonWaitingForHandoff       = "WaitingForHandoff"       // U4 or D5: a member's handoff
	ReasonRemovingPod             = "RemovingPod"             // D1 or D4
	ReasonDraining                = "Draining"                // D2
	ReasonDrainBlocked            = "DrainBlocked"            // D2 past drainTimeout
	ReasonDrained                 = "Drained"                 // D3
	ReasonWaitingForPodRemoval    = "WaitingForPodRemoval"    // D5: the removed pod
	ReasonWaitingForVolumeRemoval = "WaitingForVolumeRemoval" // D5: the removed pod's volume
	ReasonWaitingForOldVolume     = "WaitingForOldVolume"     // U1/U2: an earlier incarnation's pod or volume at the new ordinal
	ReasonWaitingForPods          = "WaitingForPods"          // a pod is not ready
	ReasonRollingUpdate           = "RollingUpdate"           // the pod template is being applied or rolled out
	ReasonReady                   = "Ready"                   // stable, at the spec
)

// Decision is what Plan returns.
type Decision struct {
	Action Action
	Status Status
}

// Config holds the planner's timings.
type Config struct {
	// Grace is how long a pod that should be serving may be unready or
	// unreachable before the cluster is Degraded.
	Grace time.Duration
	// Fast is the requeue while a step should complete within seconds
	// (an epoch spreading, a pod starting).
	Fast time.Duration
	// Slow is the requeue while waiting on handoffs and drains.
	Slow time.Duration
	// Idle is the requeue of a Ready or Degraded cluster.
	Idle time.Duration
}

// DefaultConfig: handoffs took 17-260s and drains 24-239s in the kind
// measurements (docs/kubernetes.md); a pod is ready seconds after it starts.
var DefaultConfig = Config{Grace: 2 * time.Minute, Fast: 2 * time.Second, Slow: 10 * time.Second, Idle: time.Minute}

// view is one pod's membership.
type view struct {
	ordinal int
	epoch   uint64
	members map[string]string
}

type planner struct {
	obs Observation
	cfg Config
	kv  *kvv1.KVCluster
	st  Status
}

// Plan returns the next action for the observed cluster and the status to
// report.
func Plan(obs Observation, cfg Config) Decision {
	p := &planner{obs: obs, cfg: cfg, kv: obs.Cluster}
	if obs.StatefulSet != nil {
		p.st.Replicas = obs.StatefulSet.Replicas
		p.st.ReadyReplicas = obs.StatefulSet.ReadyReplicas
	}
	a := p.plan()
	return Decision{Action: a, Status: p.st}
}

func (p *planner) members(k int) map[string]string { return render.Members(p.kv, k) }

func (p *planner) wait(phase kvv1.Phase, reason string, after time.Duration, format string, args ...any) Action {
	p.st.Phase, p.st.Reason, p.st.Message = phase, reason, fmt.Sprintf(format, args...)
	return Action{Kind: ActWait, Reason: reason, RequeueAfter: after}
}

// degraded reports a state the operator will not act on. A drain's start
// time is kept, so being Degraded for a while does not restart drainTimeout.
func (p *planner) degraded(reason, format string, args ...any) Action {
	if s := p.kv.Status.DrainStartedAt; s != nil {
		t := s.Time
		p.st.DrainStartedAt = &t
	}
	return p.wait(kvv1.PhaseDegraded, reason, p.cfg.Idle, format, args...)
}

func (p *planner) act(a Action, phase kvv1.Phase, reason, format string, args ...any) Action {
	p.st.Phase, p.st.Reason, p.st.Message = phase, reason, fmt.Sprintf(format, args...)
	a.Reason = reason
	return a
}

// prevPhase is the phase last reported, Pending if none.
func (p *planner) prevPhase() kvv1.Phase {
	if p.kv.Status.Phase == "" {
		return kvv1.PhasePending
	}
	return p.kv.Status.Phase
}

// waitingPhase is the phase for a cluster that is not mid-change but not
// stable either (a pod restarting, a rollout): Pending until it has first
// been Ready, otherwise the phase it had, except that Degraded clears.
func (p *planner) waitingPhase() kvv1.Phase {
	switch ph := p.prevPhase(); ph {
	case kvv1.PhaseDegraded, kvv1.PhaseDrainBlocked:
		return kvv1.PhaseScaling
	default:
		return ph
	}
}

func (p *planner) pod(ordinal int) PodObs {
	if ordinal < len(p.obs.Pods) {
		return p.obs.Pods[ordinal]
	}
	return PodObs{Ordinal: ordinal}
}

func (p *planner) hasPVC(ordinal int) bool { return slices.Contains(p.obs.PVCs, ordinal) }

// validMembers checks that a member set is this cluster's pods at exactly
// render.MemberAddress, and returns its size if it is ordinals 0..size-1.
func (p *planner) validMembers(m map[string]string) (size int, contiguous bool, err error) {
	for id, addr := range m {
		i, ok := render.Ordinal(p.kv, id)
		if !ok {
			return 0, false, fmt.Errorf("member %q is not a pod of %s", id, p.kv.Name)
		}
		if want := render.MemberAddress(p.kv, i); addr != want {
			return 0, false, fmt.Errorf("member %s has address %q, want %q", id, addr, want)
		}
	}
	return len(m), maps.Equal(m, p.members(len(m))), nil
}

func (p *planner) plan() Action {
	c, a, done := p.bootstrap()
	if done {
		return a
	}
	S := int(p.obs.StatefulSet.Replicas)
	views, a, bad := p.readViews(S)
	if bad {
		return a
	}
	a, exempt, ok := p.classify(S, c, views)
	if !ok {
		return a
	}
	if bad, d := p.unavailable(S, exempt); bad {
		return d
	}
	if a.Kind != ActWait {
		p.st.DrainStartedAt = nil
	}
	return a
}

// bootstrap creates the ConfigMap, then the StatefulSet, and checks the
// ConfigMap's membership. It returns the ConfigMap's member count, or an
// action and done.
func (p *planner) bootstrap() (members int, a Action, done bool) {
	obs := p.obs
	if obs.ConfigMap == nil {
		if obs.StatefulSet == nil {
			m := p.members(int(p.kv.Spec.Replicas))
			return 0, p.act(Action{Kind: ActWriteConfigMap, Epoch: 0, Members: m}, kvv1.PhasePending, ReasonCreatingConfigMap,
				"Creating the ConfigMap with %d members at epoch 0", len(m)), true
		}
		return 0, p.degraded(ReasonConfigMapMissing, "The ConfigMap is missing while the StatefulSet exists; restore it from GET /admin/membership (docs/operator.md)"), true
	}
	c, contiguous, err := p.validMembers(obs.ConfigMap.Members)
	if err != nil || !contiguous {
		if err == nil {
			err = fmt.Errorf("members are not %s-0..%s-%d", p.kv.Name, p.kv.Name, c-1)
		}
		return 0, p.degraded(ReasonConfigMapUnexpected, "The ConfigMap's membership is not one the operator writes: %v", err), true
	}
	if obs.StatefulSet == nil {
		return 0, p.act(Action{Kind: ActScaleStatefulSet, Replicas: int32(c)}, kvv1.PhasePending, ReasonCreatingStatefulSet,
			"Creating the StatefulSet with %d replicas", c), true
	}
	return c, Action{}, false
}

// readViews collects the memberships of pods 0..S-1 (pods at ordinals >= S
// are being removed and say nothing about the membership), and rejects
// views the operator's steps cannot produce.
func (p *planner) readViews(S int) (views []view, a Action, bad bool) {
	for i := range S {
		pd := p.pod(i)
		if !pd.reachable() {
			continue
		}
		if _, _, err := p.validMembers(pd.Membership.Members); err != nil {
			return nil, p.degraded(ReasonAddressMismatch, "%s holds a membership the operator did not write: %v", render.MemberID(p.kv, i), err), true
		}
		views = append(views, view{i, pd.Membership.Epoch, pd.Membership.Members})
	}
	byEpoch := map[uint64]map[string]string{}
	for _, v := range views {
		if prev, ok := byEpoch[v.epoch]; ok && !maps.Equal(prev, v.members) {
			return nil, p.degraded(ReasonMembershipConflict, "Two pods hold different memberships at epoch %d", v.epoch), true
		}
		byEpoch[v.epoch] = v.members
	}
	epochs := slices.Sorted(maps.Keys(byEpoch))
	if len(epochs) > 2 || len(epochs) == 2 && epochs[1] != epochs[0]+1 {
		return nil, p.degraded(ReasonUnexplainedDivergence, "Pods hold epochs %v: more than one membership change in flight", epochs), true
	}
	if len(epochs) > 0 {
		hi := epochs[len(epochs)-1]
		p.st.Epoch, p.st.Members = hi, maps.Clone(byEpoch[hi])
	}
	return views, Action{}, false
}

// state is one membership: size members (ordinals 0..size-1) at epoch.
type state struct {
	epoch uint64
	size  int
}

// allIn reports whether every view is one of allowed, and which were seen.
func (p *planner) allIn(views []view, allowed ...state) (ok bool, seen map[state]bool) {
	seen = map[state]bool{}
	for _, v := range views {
		s := state{v.epoch, len(v.members)}
		if !slices.Contains(allowed, s) || !maps.Equal(v.members, p.members(s.size)) {
			return false, nil
		}
		seen[s] = true
	}
	return true, seen
}

// classify places the observation at a step of a membership change and
// returns that step's action, and the ordinal (if any) whose ill health the
// step expects. ok is false for a combination no step produces.
func (p *planner) classify(S, c int, views []view) (a Action, exempt int, ok bool) {
	Ec := p.obs.ConfigMap.Epoch
	switch {
	case c == S+1 && Ec >= 1:
		// U1 done: the ConfigMap names pod S, which does not exist yet. A
		// pod that restarted in between may already have adopted E+1 from
		// the ConfigMap and spread it.
		if ok, _ := p.allIn(views, state{Ec - 1, S}, state{Ec, S + 1}); ok {
			return p.scaleUp(S), -1, true
		}
	case c == S:
		if ok, _ := p.allIn(views, state{Ec, S}); ok {
			return p.converged(S, views), -1, true
		}
		if ok, seen := p.allIn(views, state{Ec - 1, S - 1}, state{Ec, S}); ok && Ec >= 1 && seen[state{Ec - 1, S - 1}] {
			// U3: pod S-1 started at E+1; the others are adopting it.
			return p.wait(kvv1.PhaseScaling, ReasonWaitingForEpoch, p.cfg.Fast,
				"Adding %s: waiting for every pod to hold epoch %d", render.MemberID(p.kv, S-1), Ec), -1, true
		}
		if ok, seen := p.allIn(views, state{Ec, S}, state{Ec + 1, S - 1}); ok && seen[state{Ec + 1, S - 1}] {
			// D2: pod 0 has taken the membership without pod S-1.
			return p.drain(S, Ec+1), S - 1, true
		}
	case c == S-1 && Ec >= 1:
		// D3 done: pod S-1 drained and the ConfigMap no longer lists it. It
		// may have restarted and be failing to start (it is not in the
		// ConfigMap); that is expected.
		if ok, _ := p.allIn(views, state{Ec, c}); ok {
			return p.act(Action{Kind: ActScaleStatefulSet, Replicas: int32(c)}, kvv1.PhaseScaling, ReasonRemovingPod,
				"Removing %s: it has drained; scaling the StatefulSet to %d", render.MemberID(p.kv, S-1), c), S - 1, true
		}
	}
	return p.unexplained(S, c), -1, false
}

// unavailable finds a pod that should be serving and has not been for
// longer than Grace: not ready since its Ready condition last changed, or
// ready but not answering on the admin API since UnreachableSince.
func (p *planner) unavailable(S, exempt int) (bool, Action) {
	for i := range S {
		pd := p.pod(i)
		if i == exempt || pd.healthy() || !pd.Exists {
			continue
		}
		since, what := pd.Since, "not ready"
		if pd.Ready {
			since, what = pd.UnreachableSince, "not answering on the admin API"
		}
		if since.IsZero() {
			continue
		}
		if d := p.obs.Now.Sub(since); d > p.cfg.Grace {
			return true, p.degraded(ReasonPodUnavailable, "%s has been %s for %v", render.MemberID(p.kv, i), what, d.Round(time.Second))
		}
	}
	return false, Action{}
}

func (p *planner) unexplained(S, c int) Action {
	return p.degraded(ReasonUnexplainedDivergence,
		"The StatefulSet (%d replicas), the ConfigMap (%d members at epoch %d) and the pods' memberships match no step of a membership change",
		S, c, p.obs.ConfigMap.Epoch)
}

// scaleUp is U2: the ConfigMap names pod S; create it.
func (p *planner) scaleUp(S int) Action {
	for i := range S {
		if !p.pod(i).healthy() {
			return p.wait(kvv1.PhaseScaling, ReasonWaitingForPods, p.cfg.Fast,
				"Adding %s: waiting for %s before creating the pod", render.MemberID(p.kv, S), render.MemberID(p.kv, i))
		}
	}
	if blocked, why := p.ordinalTaken(S); blocked {
		return p.wait(kvv1.PhaseScaling, ReasonWaitingForOldVolume, p.cfg.Fast, "Adding %s: %s", render.MemberID(p.kv, S), why)
	}
	return p.act(Action{Kind: ActScaleStatefulSet, Replicas: int32(S + 1)}, kvv1.PhaseScaling, ReasonAddingPod,
		"Adding %s: scaling the StatefulSet to %d", render.MemberID(p.kv, S), S+1)
}

// ordinalTaken reports whether a pod or PVC from an earlier incarnation of
// this ordinal still exists. A new pod there would reuse the old data dir.
func (p *planner) ordinalTaken(i int) (bool, string) {
	if pd := p.pod(i); pd.Exists {
		return true, fmt.Sprintf("the old pod %s is still there", render.MemberID(p.kv, i))
	}
	if p.hasPVC(i) {
		return true, fmt.Sprintf("the old volume data-%s still exists", render.MemberID(p.kv, i))
	}
	return false, ""
}

// drain is D2 (and D3 once pod S-1 has drained at epoch).
func (p *planner) drain(S int, epoch uint64) Action {
	leaving := p.pod(S - 1)
	id := render.MemberID(p.kv, S-1)
	if d := leaving.Drain; d != nil && d.Epoch == epoch && d.Drained && leaving.Membership != nil && leaving.Membership.Epoch == epoch {
		hi := p.viewAt(epoch)
		p.st.DrainStartedAt = nil
		return p.act(Action{Kind: ActWriteConfigMap, Epoch: epoch, Members: hi}, kvv1.PhaseScaling, ReasonDrained,
			"Removing %s: drained; writing the ConfigMap at epoch %d without it", id, epoch)
	}

	started := p.obs.Now
	if s := p.kv.Status.DrainStartedAt; s != nil {
		started = s.Time
	}
	p.st.DrainStartedAt = &started
	detail := "it has not answered the drain check yet"
	if d := leaving.Drain; d != nil {
		var lagging []string
		for _, id := range slices.Sorted(maps.Keys(d.Peers)) {
			if d.Peers[id].LastSeenEpoch < epoch {
				lagging = append(lagging, id)
			}
		}
		detail = fmt.Sprintf("%d keys still to hand off", d.Handoff.Pending)
		if len(lagging) > 0 {
			detail += fmt.Sprintf("; not yet seen at epoch %d: %s", epoch, strings.Join(lagging, ", "))
		}
	}
	timeout := p.kv.Spec.DrainTimeout.Duration
	if waited := p.obs.Now.Sub(started); timeout > 0 && waited > timeout {
		return p.wait(kvv1.PhaseDrainBlocked, ReasonDrainBlocked, p.cfg.Slow,
			"Removing %s: not drained after %v (drainTimeout %v): %s; see docs/operator.md", id, waited.Round(time.Second), timeout, detail)
	}
	return p.wait(kvv1.PhaseScaling, ReasonDraining, p.cfg.Slow, "Removing %s: draining at epoch %d: %s", id, epoch, detail)
}

// viewAt is the member map a pod at epoch returned: the cluster's own
// strings, never the ConfigMap's or the spec's. Every pod at one epoch holds
// the same map (plan checks), so any of them will do.
func (p *planner) viewAt(epoch uint64) map[string]string {
	for _, pd := range p.obs.Pods {
		if pd.Membership != nil && pd.Membership.Epoch == epoch {
			return maps.Clone(pd.Membership.Members)
		}
	}
	return nil
}

// converged: every reachable pod holds the ConfigMap's membership (S
// members at the ConfigMap's epoch). Finish U4/D5 and the rollout, then act
// on the spec.
func (p *planner) converged(S int, views []view) Action {
	cm := p.obs.ConfigMap
	sts := p.obs.StatefulSet

	// D5: the removed pod and its volume must be gone.
	for _, pd := range p.obs.Pods {
		if pd.Ordinal >= S && pd.Exists {
			return p.wait(kvv1.PhaseScaling, ReasonWaitingForPodRemoval, p.cfg.Fast, "Waiting for %s to stop", render.MemberID(p.kv, pd.Ordinal))
		}
	}
	for _, i := range p.obs.PVCs {
		if i >= S {
			return p.wait(kvv1.PhaseScaling, ReasonWaitingForVolumeRemoval, p.cfg.Fast, "Waiting for the volume data-%s to be deleted", render.MemberID(p.kv, i))
		}
	}
	// U4/D5: every member's handoff to the ConfigMap's epoch.
	var pending []string
	for _, v := range views {
		h := p.pod(v.ordinal).Membership.Handoff
		if h.Epoch < cm.Epoch || !h.Done {
			pending = append(pending, render.MemberID(p.kv, v.ordinal))
		}
	}
	if len(pending) > 0 {
		return p.wait(kvv1.PhaseScaling, ReasonWaitingForHandoff, p.cfg.Slow, "Waiting for the handoff to epoch %d on %s", cm.Epoch, strings.Join(pending, ", "))
	}
	for i := range S {
		if pd := p.pod(i); !pd.healthy() {
			return p.wait(p.waitingPhase(), ReasonWaitingForPods, p.cfg.Fast, "Waiting for %s to be ready", render.MemberID(p.kv, i))
		}
	}
	if !sts.RolledOut || sts.ReadyReplicas < int32(S) {
		return p.wait(kvv1.PhaseScaling, ReasonRollingUpdate, p.cfg.Fast, "Waiting for the StatefulSet to roll out")
	}

	// Stable. One change at a time: the template first, then replicas.
	E := cm.Epoch
	desired := int(p.kv.Spec.Replicas)
	switch {
	case !sts.TemplateCurrent:
		return p.act(Action{Kind: ActUpdateTemplate, Replicas: int32(S)}, kvv1.PhaseScaling, ReasonRollingUpdate,
			"Applying the spec's pod template (image %s)", p.kv.Spec.Image)
	case S < desired:
		if blocked, why := p.ordinalTaken(S); blocked {
			return p.wait(kvv1.PhaseScaling, ReasonWaitingForOldVolume, p.cfg.Fast, "Adding %s: %s", render.MemberID(p.kv, S), why)
		}
		m := p.viewAt(E) // the cluster's member strings
		m[render.MemberID(p.kv, S)] = render.MemberAddress(p.kv, S)
		return p.act(Action{Kind: ActWriteConfigMap, Epoch: E + 1, Members: m}, kvv1.PhaseScaling, ReasonAddingPod,
			"Adding %s: writing the ConfigMap at epoch %d", render.MemberID(p.kv, S), E+1)
	case S > desired:
		// Pod 0 is healthy: stable means every pod is.
		m := maps.Clone(p.pod(0).Membership.Members) // the cluster's member strings
		delete(m, render.MemberID(p.kv, S-1))
		return p.act(Action{Kind: ActPostMembership, Ordinal: 0, Epoch: E + 1, Members: m}, kvv1.PhaseScaling, ReasonRemovingPod,
			"Removing %s: sending the membership at epoch %d without it", render.MemberID(p.kv, S-1), E+1)
	}
	p.st.Phase, p.st.Reason = kvv1.PhaseReady, ReasonReady
	p.st.Message = fmt.Sprintf("%d members at epoch %d, every handoff done", S, E)
	return Action{Kind: ActNone, Reason: ReasonReady, RequeueAfter: p.cfg.Idle}
}
