package planner

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kvv1 "github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/api/v1alpha1"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/kvadmin"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/render"
)

// The simulator: a fake cluster of nodes, a StatefulSet controller and a
// kubelet, driven by the planner. One tick is one second. The nodes follow
// the real ones where it matters to the planner:
//
//   - startup (internal/config, node.New): a node reads the ConfigMap as the
//     kubelet last synced it; it refuses to start if it is not a member, adopts
//     the ConfigMap's membership if it is newer than the one in its data dir,
//     keeps its own if older, and refuses a different membership at the same
//     epoch;
//   - heartbeats spread a newer membership to the members of either view;
//   - a node adopting a membership hands off its data, which takes a while
//     (sometimes a long while for a node being removed: a slow drain);
//   - a node outside its membership is drained once its handoff is done and
//     every remaining member holds the epoch (node.WaitDrained);
//   - a node outside its membership is not ready (/readyz).

type simPVC struct {
	epoch       uint64            // the persisted MEMBERSHIP
	view        map[string]string // nil: a fresh volume
	handoffDone uint64            // the persisted HANDOFF
	stale       bool              // left behind by a pod scaled away
	deleteAt    int
}

type simPod struct {
	ord         int
	running     bool
	startAt     int // when a stopped pod (re)starts
	terminating bool
	goneAt      int
	epoch       uint64
	view        map[string]string
	handoffAt   int
	since       int  // tick its readiness last changed
	wasReady    bool // readiness at the last tick
	version     int  // the template version it runs
	drainedOnce bool // has been observed drained
	unreachable int  // tick its admin API stopped answering; -1 when it answers
}

type cmVersion struct {
	tick int
	cm   *ConfigMapObs
}

type world struct {
	rng  *rand.Rand
	kv   *kvv1.KVCluster
	tick int

	cm     *ConfigMapObs
	cmHist []cmVersion

	stsExists   bool
	replicas    int
	tmplVersion int
	specVersion int

	pods map[int]*simPod
	pvcs map[int]*simPVC

	// Knobs.
	slowDrain   float64 // chance a leaving node's handoff is slow
	podCrash    float64 // chance per tick that some running pod crashes
	flakyAdmin  float64 // chance a pod's admin API misses one observation
	maxCMLag    int     // kubelet ConfigMap sync lag, in ticks
	maxPVCDelay int     // how long deleting a released volume takes

	violations []string
	stats      map[string]int
}

func newWorld(seed uint64, replicas int32) *world {
	kv := kvc(replicas)
	kv.Status = kvv1.KVClusterStatus{}
	kv.Spec.DrainTimeout = metav1.Duration{Duration: 90 * time.Second}
	return &world{
		rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), kv: kv,
		pods: map[int]*simPod{}, pvcs: map[int]*simPVC{},
		slowDrain: 0.15, podCrash: 0.002, flakyAdmin: 0.02, maxCMLag: 3, maxPVCDelay: 40,
		stats: map[string]int{},
	}
}

func (w *world) now() time.Time { return t0.Add(time.Duration(w.tick) * time.Second) }

func (w *world) between(lo, hi int) int { return lo + w.rng.IntN(hi-lo+1) }

func (w *world) violate(format string, args ...any) {
	w.violations = append(w.violations, fmt.Sprintf("tick %d: ", w.tick)+fmt.Sprintf(format, args...))
}

func (w *world) id(i int) string { return render.MemberID(w.kv, i) }

func (w *world) ordinals() []int { return slices.Sorted(maps.Keys(w.pods)) }

func member(view map[string]string, id string) bool { _, ok := view[id]; return ok }

// visibleCM is the ConfigMap as the kubelet last synced it.
func (w *world) visibleCM() *ConfigMapObs {
	lagged := w.tick - w.between(0, w.maxCMLag)
	var cm *ConfigMapObs
	for _, v := range w.cmHist {
		if v.tick <= lagged {
			cm = v.cm
		}
	}
	return cm
}

func (w *world) ready(p *simPod) bool {
	return p.running && !p.terminating && member(p.view, w.id(p.ord))
}

func (w *world) handoffDelay(p *simPod) int {
	if !member(p.view, w.id(p.ord)) && w.rng.Float64() < w.slowDrain {
		w.stats["slow drains"]++
		return w.between(60, 400)
	}
	return w.between(1, 12)
}

// adopt is SetMembership: persist, publish, start the handoff.
func (w *world) adopt(p *simPod, epoch uint64, view map[string]string) {
	p.epoch, p.view = epoch, maps.Clone(view)
	pvc := w.pvcs[p.ord]
	pvc.epoch, pvc.view = epoch, maps.Clone(view)
	p.handoffAt = w.tick + w.handoffDelay(p)
}

func (w *world) handoffDone(p *simPod) bool { return w.pvcs[p.ord].handoffDone >= p.epoch }

func (w *world) drained(p *simPod) bool {
	if !p.running || member(p.view, w.id(p.ord)) || !w.handoffDone(p) {
		return false
	}
	for id := range p.view {
		i, _ := render.Ordinal(w.kv, id)
		q := w.pods[i]
		if q == nil || !q.running || q.epoch < p.epoch {
			return false
		}
	}
	return true
}

// maxEpoch is the highest epoch any node holds, running or persisted.
func (w *world) maxEpoch() uint64 {
	var e uint64
	for _, pvc := range w.pvcs {
		if !pvc.stale && pvc.view != nil {
			e = max(e, pvc.epoch)
		}
	}
	return e
}

func (w *world) start(p *simPod) {
	cm := w.visibleCM()
	retry := func() { p.startAt = w.tick + w.between(1, 3) }
	if cm == nil || !member(cm.Members, w.id(p.ord)) {
		w.stats["start refused: not in the ConfigMap"]++
		retry()
		return
	}
	pvc := w.pvcs[p.ord]
	switch {
	case pvc.view == nil || cm.Epoch > pvc.epoch:
		pvc.epoch, pvc.view = cm.Epoch, maps.Clone(cm.Members)
	case cm.Epoch == pvc.epoch && !maps.Equal(cm.Members, pvc.view):
		w.violate("%s refused to start: the ConfigMap holds a different membership at its epoch %d", w.id(p.ord), cm.Epoch)
		retry()
		return
	}
	p.running = true
	p.epoch, p.view = pvc.epoch, maps.Clone(pvc.view)
	if pvc.handoffDone < p.epoch {
		p.handoffAt = w.tick + w.handoffDelay(p)
	}
}

// advance moves the cluster on by one tick.
func (w *world) advance() {
	w.tick++
	w.statefulSetController()
	w.podsStartAndCrash()
	w.heartbeats()
	w.handoffsAndReadiness()
}

// statefulSetController creates and removes pods, rolls out the template,
// and deletes released volumes (whenScaled: Delete) after a delay.
func (w *world) statefulSetController() {
	if w.stsExists {
		for i := 0; i < w.replicas; i++ {
			if w.pods[i] != nil {
				continue
			}
			if pvc := w.pvcs[i]; pvc != nil && pvc.stale {
				w.violate("%s created on the volume a scaled-away %s left behind", w.id(i), w.id(i))
				pvc.stale = false
			}
			if w.pvcs[i] == nil {
				w.pvcs[i] = &simPVC{}
			}
			w.pods[i] = &simPod{ord: i, startAt: w.tick + w.between(1, 3), since: w.tick, version: w.tmplVersion, unreachable: -1}
		}
		for _, i := range w.ordinals() {
			if p := w.pods[i]; i >= w.replicas && !p.terminating {
				p.terminating, p.goneAt = true, w.tick+w.between(1, 6)
			}
		}
		// Rolling update, highest ordinal first, one pod at a time.
		allReady := true
		for i := 0; i < w.replicas; i++ {
			if p := w.pods[i]; p == nil || !p.running {
				allReady = false
			}
		}
		if allReady {
			for i := w.replicas - 1; i >= 0; i-- {
				if p := w.pods[i]; p.version != w.tmplVersion {
					p.running, p.startAt, p.version = false, w.tick+w.between(1, 3), w.tmplVersion
					break
				}
			}
		}
	}
	for _, i := range w.ordinals() {
		if p := w.pods[i]; p.terminating && w.tick >= p.goneAt {
			delete(w.pods, i)
			pvc := w.pvcs[i]
			pvc.stale, pvc.deleteAt = true, w.tick+w.between(1, w.maxPVCDelay)
		}
	}
	for _, i := range slices.Sorted(maps.Keys(w.pvcs)) {
		if pvc := w.pvcs[i]; pvc.stale && w.tick >= pvc.deleteAt {
			delete(w.pvcs, i)
		}
	}

}

// podsStartAndCrash starts stopped pods (reading the ConfigMap) and now and
// then crashes one.
func (w *world) podsStartAndCrash() {
	for _, i := range w.ordinals() {
		if p := w.pods[i]; !p.running && !p.terminating && w.tick >= p.startAt {
			w.start(p)
		}
	}
	if w.rng.Float64() < w.podCrash {
		var running []*simPod
		for _, i := range w.ordinals() {
			if p := w.pods[i]; p.running && !p.terminating {
				running = append(running, p)
			}
		}
		if len(running) > 0 {
			p := running[w.rng.IntN(len(running))]
			p.running, p.startAt = false, w.tick+w.between(1, 4)
			w.stats["pod crashes"]++
		}
	}

}

// heartbeats spread a newer membership: a node learns it from a node in
// either's view.
func (w *world) heartbeats() {
	for _, i := range w.ordinals() {
		p := w.pods[i]
		if !p.running {
			continue
		}
		var best *simPod
		for _, j := range w.ordinals() {
			q := w.pods[j]
			if q == p || !q.running || q.epoch <= p.epoch {
				continue
			}
			if !member(p.view, w.id(q.ord)) && !member(q.view, w.id(p.ord)) {
				continue
			}
			if best == nil || q.epoch > best.epoch {
				best = q
			}
		}
		if best != nil && w.rng.Float64() < 0.6 {
			w.adopt(p, best.epoch, best.view)
		}
	}

}

// handoffsAndReadiness completes handoffs and records readiness changes and
// drains.
func (w *world) handoffsAndReadiness() {
	for _, i := range w.ordinals() {
		if p := w.pods[i]; p.running && !w.handoffDone(p) && w.tick >= p.handoffAt {
			w.pvcs[i].handoffDone = p.epoch
		}
	}

	for _, i := range w.ordinals() {
		p := w.pods[i]
		if r := w.ready(p); r != p.wasReady {
			p.wasReady, p.since = r, w.tick
		}
		if w.drained(p) {
			p.drainedOnce = true
		}
	}
}

// observe builds the planner's observation from scratch, as the controller
// would.
func (w *world) observe() Observation {
	o := Observation{Now: w.now(), Cluster: w.kv.DeepCopy()}
	if w.cm != nil {
		o.ConfigMap = &ConfigMapObs{Epoch: w.cm.Epoch, Members: maps.Clone(w.cm.Members)}
	}
	n := 0
	if w.stsExists {
		ready := 0
		rolled := true
		for i := 0; i < w.replicas; i++ {
			p := w.pods[i]
			if p != nil && w.ready(p) {
				ready++
			}
			if p == nil || p.version != w.tmplVersion || !p.running {
				rolled = false
			}
		}
		o.StatefulSet = &StatefulSetObs{Replicas: int32(w.replicas), ReadyReplicas: int32(ready), RolledOut: rolled, TemplateCurrent: w.tmplVersion == w.specVersion}
		n = w.replicas
	}
	for _, i := range w.ordinals() {
		n = max(n, i+1)
	}
	for i := range n {
		pd := PodObs{Ordinal: i}
		if p := w.pods[i]; p != nil {
			pd.Exists, pd.Terminating = true, p.terminating
			pd.Ready = w.ready(p)
			pd.Since = t0.Add(time.Duration(p.since) * time.Second)
			answers := p.running && w.rng.Float64() >= w.flakyAdmin
			switch {
			case answers:
				p.unreachable = -1
			case p.unreachable < 0: // as the controller tracks it
				p.unreachable = w.tick
			}
			if p.unreachable >= 0 {
				pd.UnreachableSince = t0.Add(time.Duration(p.unreachable) * time.Second)
			}
			if answers {
				pd.Membership = &kvadmin.Membership{
					Epoch:   p.epoch,
					Members: maps.Clone(p.view),
					Handoff: kvadmin.HandoffStatus{Epoch: p.epoch, Done: w.handoffDone(p)},
				}
				if !member(p.view, w.id(i)) {
					pd.Drain = &kvadmin.Handoff{Epoch: p.epoch, Drained: w.drained(p), Handoff: kvadmin.HandoffStatus{Epoch: p.epoch, Done: w.handoffDone(p)}}
				}
			}
		}
		o.Pods = append(o.Pods, pd)
	}
	o.PVCs = slices.Sorted(maps.Keys(w.pvcs))
	return o
}

// trulyStable is the planner's stable, from the simulator's own state.
func (w *world) trulyStable() bool {
	if w.cm == nil || !w.stsExists {
		return false
	}
	want := render.Members(w.kv, w.replicas)
	if w.cm.Epoch != w.maxEpoch() || !maps.Equal(w.cm.Members, want) {
		return false
	}
	for _, i := range w.ordinals() {
		if i >= w.replicas {
			return false
		}
	}
	for i := range w.pvcs {
		if i >= w.replicas {
			return false
		}
	}
	for i := 0; i < w.replicas; i++ {
		p := w.pods[i]
		if p == nil || !w.ready(p) || p.epoch != w.cm.Epoch || !maps.Equal(p.view, want) || !w.handoffDone(p) {
			return false
		}
	}
	return true
}

// apply carries out an action, checking the invariants that concern it.
func (w *world) apply(a Action) {
	switch a.Kind {
	case ActWriteConfigMap:
		w.cm = &ConfigMapObs{Epoch: a.Epoch, Members: maps.Clone(a.Members)}
		w.cmHist = append(w.cmHist, cmVersion{w.tick, w.cm})
	case ActScaleStatefulSet:
		if !w.stsExists {
			w.stsExists, w.tmplVersion = true, w.specVersion
		}
		for i := int(a.Replicas); i < w.replicas; i++ {
			if p := w.pods[i]; p != nil && !p.drainedOnce {
				w.violate("StatefulSet scaled to %d while %s had not drained", a.Replicas, w.id(i))
			}
		}
		w.replicas = int(a.Replicas)
	case ActUpdateTemplate:
		if !w.trulyStable() {
			w.violate("template updated while a membership change was in flight")
		}
		w.tmplVersion = w.specVersion
	case ActPostMembership:
		p := w.pods[a.Ordinal]
		if p == nil || !p.running {
			return // the request fails; the planner will see nothing changed
		}
		switch {
		case a.Epoch < p.epoch: // 409 stale
		case a.Epoch == p.epoch:
			if !maps.Equal(a.Members, p.view) {
				w.violate("POST conflicts with the membership %s holds at epoch %d", w.id(p.ord), a.Epoch)
			}
		default:
			w.adopt(p, a.Epoch, a.Members)
		}
	}
}

// checkDecision checks the invariants on what the planner chose, before it
// is applied. (Applying it twice, as a retry would, is not a new decision.)
func (w *world) checkDecision(d Decision) {
	a := d.Action
	if a.Kind == ActPostMembership && a.Epoch <= w.maxEpoch() {
		w.violate("POST at epoch %d, not above the cluster's %d", a.Epoch, w.maxEpoch())
	}
	if d.Status.Phase == kvv1.PhaseDegraded && a.Kind != ActWait {
		w.violate("acted while Degraded: %v", a)
	}
}

// checkState checks the invariants that hold between actions.
func (w *world) checkState() {
	if w.cm == nil {
		return
	}
	// At most one membership change in flight: every epoch held is within
	// one of every other, and at most two memberships exist.
	epochs := map[uint64]bool{w.cm.Epoch: true}
	sets := map[string]bool{fmt.Sprint(slices.Sorted(maps.Keys(w.cm.Members))): true}
	var hi uint64
	var hiView map[string]string
	for _, i := range slices.Sorted(maps.Keys(w.pvcs)) {
		pvc := w.pvcs[i]
		if pvc.stale || pvc.view == nil {
			continue
		}
		epochs[pvc.epoch] = true
		sets[fmt.Sprint(slices.Sorted(maps.Keys(pvc.view)))] = true
		if pvc.epoch >= hi {
			hi, hiView = pvc.epoch, pvc.view
		}
	}
	es := slices.Sorted(maps.Keys(epochs))
	if es[len(es)-1]-es[0] > 1 || len(sets) > 2 {
		w.violate("more than one membership change in flight: epochs %v, %d member sets", es, len(sets))
	}
	// The ConfigMap is never behind the cluster, except between D1 and D3:
	// one epoch behind, still listing the node being removed.
	if w.cm.Epoch < hi {
		inD2 := w.cm.Epoch+1 == hi && maps.Equal(w.cm.Members, render.Members(w.kv, w.replicas)) &&
			maps.Equal(hiView, render.Members(w.kv, w.replicas-1))
		if !inD2 {
			w.violate("ConfigMap at epoch %d behind the cluster's %d outside a drain", w.cm.Epoch, hi)
		}
		w.stats["ticks with the ConfigMap one epoch behind (D2)"]++
	}
}

// snapshot is the cluster's state for comparisons.
func (w *world) snapshot() string {
	s := fmt.Sprintf("sts=%v/%d/%d ", w.stsExists, w.replicas, w.tmplVersion)
	if w.cm != nil {
		s += fmt.Sprintf("cm=%d%v ", w.cm.Epoch, slices.Sorted(maps.Keys(w.cm.Members)))
	}
	for _, i := range w.ordinals() {
		p := w.pods[i]
		s += fmt.Sprintf("pod%d=%v/%d%v ", i, p.running, p.epoch, slices.Sorted(maps.Keys(p.view)))
	}
	for _, i := range slices.Sorted(maps.Keys(w.pvcs)) {
		s += fmt.Sprintf("pvc%d=%d/%v ", i, w.pvcs[i].epoch, w.pvcs[i].stale)
	}
	return s
}

func (w *world) clone() *world {
	c := *w
	c.rng = rand.New(rand.NewPCG(uint64(w.tick)+1, 7)) // a clone's future is its own
	c.kv = w.kv.DeepCopy()
	c.cmHist = slices.Clone(w.cmHist)
	c.pods, c.pvcs = map[int]*simPod{}, map[int]*simPVC{}
	for i, p := range w.pods {
		cp := *p
		cp.view = maps.Clone(p.view)
		c.pods[i] = &cp
	}
	for i, v := range w.pvcs {
		cv := *v
		cv.view = maps.Clone(v.view)
		c.pvcs[i] = &cv
	}
	c.violations = slices.Clone(w.violations)
	c.stats = maps.Clone(w.stats)
	return &c
}

func (w *world) saveStatus(st Status) {
	w.kv.Status.Phase = st.Phase
	w.kv.Status.DrainStartedAt = nil
	if st.DrainStartedAt != nil {
		w.kv.Status.DrainStartedAt = &metav1.Time{Time: *st.DrainStartedAt}
	}
}

func (w *world) setReplicas(n int32) { w.kv.Spec.Replicas = n }

func (w *world) bumpImage() {
	w.specVersion++
	w.kv.Spec.Image = "kvnode:v" + strconv.Itoa(w.specVersion)
}

// quiet turns off the random faults, for the deterministic crash tests.
func (w *world) quiet() *world {
	w.slowDrain, w.podCrash, w.flakyAdmin = 0, 0, 0
	return w
}

// --- the crash test ---

// reconcileUntilAction runs a fresh operator (the planner, from a fresh
// observation every tick) until it chooses something other than waiting, and
// returns that. It fails the test on a violation or if nothing comes.
func reconcileUntilAction(t *testing.T, w *world, within int) Action {
	t.Helper()
	for range within {
		d := Plan(w.observe(), DefaultConfig)
		if d.Status.Phase == kvv1.PhaseDegraded {
			t.Fatalf("tick %d: Degraded: %s", w.tick, d.Status.Message)
		}
		w.checkDecision(d)
		if len(w.violations) > 0 {
			t.Fatalf("violations: %v", w.violations)
		}
		if d.Action.Kind != ActWait {
			return d.Action
		}
		w.saveStatus(d.Status)
		w.advance()
		w.checkState()
		if len(w.violations) > 0 {
			t.Fatalf("violations: %v", w.violations)
		}
	}
	t.Fatalf("no action within %d ticks: %s", within, w.snapshot())
	return Action{}
}

func sameAction(a, b Action) bool {
	return a.Kind == b.Kind && a.Epoch == b.Epoch && a.Replicas == b.Replicas && a.Ordinal == b.Ordinal && maps.Equal(a.Members, b.Members)
}

// path records the actions that take w from Ready to Ready at replicas
// target, and the world right before and right after each one.
func path(t *testing.T, w *world, target int32) (actions []Action, before, after []*world, start *world) {
	t.Helper()
	w.setReplicas(target)
	start = w.clone()
	for {
		a := reconcileUntilAction(t, w, 2000)
		if a.Kind == ActNone {
			return actions, before, after, start
		}
		before = append(before, w.clone())
		w.apply(a)
		actions = append(actions, a)
		after = append(after, w.clone())
		if len(actions) > 10 {
			t.Fatalf("runaway path: %v", actions)
		}
	}
}

func readyWorld(t *testing.T, replicas int32) *world {
	t.Helper()
	w := newWorld(1, replicas).quiet()
	for {
		a := reconcileUntilAction(t, w, 2000)
		if a.Kind == ActNone {
			return w
		}
		w.apply(a)
	}
}

// TestCrashBetweenSteps: for each scale path, the operator crashes right
// after step i (its status write lost: the status is the one from before the
// path), restarts with nothing but a fresh observation, and must choose step
// i+1; after the last step, it must reach Ready without another action. And
// applying any step twice leaves the same state as applying it once.
func TestCrashBetweenSteps(t *testing.T) {
	w := readyWorld(t, 4)
	for _, tc := range []struct {
		name   string
		target int32
		want   []ActionKind
	}{
		{"scale up 4 -> 5", 5, []ActionKind{ActWriteConfigMap, ActScaleStatefulSet}},
		{"scale down 5 -> 4", 4, []ActionKind{ActPostMembership, ActWriteConfigMap, ActScaleStatefulSet}},
		{"scale up 4 -> 6", 6, []ActionKind{ActWriteConfigMap, ActScaleStatefulSet, ActWriteConfigMap, ActScaleStatefulSet}},
		{"scale down 6 -> 3", 3, []ActionKind{
			ActPostMembership, ActWriteConfigMap, ActScaleStatefulSet,
			ActPostMembership, ActWriteConfigMap, ActScaleStatefulSet,
			ActPostMembership, ActWriteConfigMap, ActScaleStatefulSet,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actions, before, after, start := path(t, w, tc.target)
			kinds := make([]ActionKind, 0, len(actions))
			for _, a := range actions {
				kinds = append(kinds, a.Kind)
			}
			if !slices.Equal(kinds, tc.want) {
				t.Fatalf("path = %v, want %v", actions, tc.want)
			}
			for i := range actions {
				// Crash right after step i, status lost.
				c := after[i].clone()
				c.kv.Status = start.kv.Status
				next := reconcileUntilAction(t, c, 2000)
				if i+1 < len(actions) {
					if !sameAction(next, actions[i+1]) {
						t.Errorf("after step %d (%v) and a crash, chose %v; want %v", i+1, actions[i], next, actions[i+1])
					}
				} else if next.Kind != ActNone {
					t.Errorf("after the last step and a crash, chose %v; want Ready", next)
				}

				// The same step twice, from right before it.
				twice := before[i].clone()
				twice.apply(actions[i])
				twice.apply(actions[i])
				if got, want := twice.snapshot(), after[i].snapshot(); got != want {
					t.Errorf("step %d (%v) applied twice:\n got  %s\n want %s", i+1, actions[i], got, want)
				}
				if len(twice.violations) > 0 {
					t.Errorf("step %d applied twice: %v", i+1, twice.violations)
				}
			}
		})
	}
}

// --- the simulator ---

func simRuns(t *testing.T) int {
	if s := os.Getenv("KV_SIM_RUNS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("KV_SIM_RUNS=%q: %v", s, err)
		}
		return n
	}
	if testing.Short() {
		return 200
	}
	return 2000
}

// runSim drives one seeded run: bootstrap, then a random sequence of
// replica targets (and image changes), some changed mid-flight, with
// operator crashes that lose the step or its status write. It returns the
// violations.
func runSim(seed uint64) (violations []string, stats map[string]int) {
	w := newWorld(seed, int32(3+int(seed%4)))
	r := w.rng
	targets := make([]int32, 3+r.IntN(4))
	for i := range targets {
		targets[i] = int32(3 + r.IntN(5))
	}
	next := 0
	const maxTicks = 30000
	for w.tick < maxTicks {
		obs := w.observe()
		d := Plan(obs, DefaultConfig)
		a := d.Action

		if d.Status.Phase == kvv1.PhaseDegraded {
			w.violate("Degraded with no fault injected: %s", d.Status.Message)
		}
		w.checkDecision(d)
		if d.Status.Phase == kvv1.PhaseDrainBlocked {
			w.stats["ticks DrainBlocked"]++
		}

		if a.Kind == ActNone && next == len(targets) && int(w.kv.Spec.Replicas) == w.replicas {
			w.stats["runs converged"]++
			break
		}
		// The spec changes: when the cluster is Ready, or now and then in
		// the middle of a change.
		if next < len(targets) && (a.Kind == ActNone || r.Float64() < 0.004) {
			if a.Kind != ActNone {
				w.stats["spec changes mid-flight"]++
			}
			w.setReplicas(targets[next])
			next++
			if r.Float64() < 0.2 {
				w.bumpImage()
			}
		}

		switch x := r.Float64(); {
		case x < 0.02: // the operator crashes before acting
			w.stats["operator crashes before acting"]++
		case x < 0.04: // it acts, then crashes before writing status
			w.apply(a)
			w.stats["operator crashes after acting"]++
		default:
			w.apply(a)
			w.saveStatus(d.Status)
		}
		if a.Kind != ActWait && a.Kind != ActNone {
			w.stats["actions"]++
		}
		w.advance()
		w.checkState()
		if len(w.violations) > 0 {
			return w.violations, w.stats
		}
	}
	if w.tick >= maxTicks {
		w.violate("did not converge in %d ticks: spec %d, %s", maxTicks, w.kv.Spec.Replicas, w.snapshot())
	}
	return w.violations, w.stats
}

func TestSimulator(t *testing.T) {
	runs := simRuns(t)
	base := uint64(20261005)
	if s := os.Getenv("KV_SIM_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		base, runs = v, 1
	}
	total := map[string]int{}
	failed := 0
	for i := range runs {
		seed := base + uint64(i)
		v, stats := runSim(seed)
		for k, n := range stats {
			total[k] += n
		}
		if len(v) > 0 {
			failed++
			if failed <= 5 {
				t.Errorf("seed %d (rerun with KV_SIM_SEED=%d): %v", seed, seed, v)
			}
		}
	}
	t.Logf("%d runs, seeds %d..%d, %d failed", runs, base, base+uint64(runs)-1, failed)
	for _, k := range slices.Sorted(maps.Keys(total)) {
		t.Logf("  %s: %d", k, total[k])
	}
}
