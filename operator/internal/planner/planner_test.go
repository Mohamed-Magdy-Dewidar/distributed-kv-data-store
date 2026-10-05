package planner

import (
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kvv1 "github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/api/v1alpha1"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/kvadmin"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/render"
)

var t0 = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func kvc(replicas int32) *kvv1.KVCluster {
	return &kvv1.KVCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "kv", Namespace: "kvstore"},
		Spec: kvv1.KVClusterSpec{
			Replicas:     replicas,
			Replication:  kvv1.Replication{N: 3, W: 2, R: 2},
			Image:        "kvnode:dev",
			Storage:      kvv1.Storage{Size: resource.MustParse("1Gi")},
			DrainTimeout: metav1.Duration{Duration: 30 * time.Minute},
		},
		Status: kvv1.KVClusterStatus{Phase: kvv1.PhaseReady},
	}
}

// membership is a pod's GET /admin/membership answer: size members at
// epoch, handoff to epoch done or not.
func membership(kv *kvv1.KVCluster, epoch uint64, size int, handoffDone bool) *kvadmin.Membership {
	return &kvadmin.Membership{
		Epoch:   epoch,
		Members: render.Members(kv, size),
		Handoff: kvadmin.HandoffStatus{Epoch: epoch, Done: handoffDone},
	}
}

// stable is a cluster of S pods at epoch E with everything done, and the
// spec asking for desired.
func stable(S int, E uint64, desired int32) Observation {
	kv := kvc(desired)
	o := Observation{
		Now:         t0,
		Cluster:     kv,
		StatefulSet: &StatefulSetObs{Replicas: int32(S), ReadyReplicas: int32(S), RolledOut: true, TemplateCurrent: true},
		ConfigMap:   &ConfigMapObs{Epoch: E, Members: render.Members(kv, S)},
	}
	for i := range S {
		o.PVCs = append(o.PVCs, i)
		o.Pods = append(o.Pods, PodObs{Ordinal: i, Exists: true, Ready: true, Since: t0.Add(-time.Hour), Membership: membership(kv, E, S, true)})
	}
	return o
}

// at sets pod i's membership.
func at(o *Observation, i int, epoch uint64, size int, handoffDone bool) {
	o.Pods[i].Membership = membership(o.Cluster, epoch, size, handoffDone)
}

// addPod appends pod i (not yet ready, created at since).
func addPod(o *Observation, since time.Time) int {
	i := len(o.Pods)
	o.Pods = append(o.Pods, PodObs{Ordinal: i, Exists: true, Since: since})
	return i
}

func down(o *Observation, i int, since time.Time) {
	o.Pods[i].Ready, o.Pods[i].Membership, o.Pods[i].Since = false, nil, since
}

// drainAnswer sets the leaving pod kv-4's drain check answer.
func drainAnswer(o *Observation, epoch uint64, drained bool, pending int) {
	o.Pods[4].Drain = &kvadmin.Handoff{
		Epoch: epoch, Drained: drained,
		Handoff: kvadmin.HandoffStatus{Epoch: epoch, Done: pending == 0, Pending: pending},
		Peers:   map[string]kvadmin.PeerStatus{"kv-0": {Alive: true, LastSeenEpoch: epoch}, "kv-1": {Alive: true, LastSeenEpoch: epoch - 1}},
	}
}

func check(t *testing.T, d Decision, kind ActionKind, phase kvv1.Phase) {
	t.Helper()
	if d.Action.Kind != kind || d.Status.Phase != phase {
		t.Fatalf("got %v, phase %s (%s: %s); want %v, phase %s", d.Action, d.Status.Phase, d.Status.Reason, d.Status.Message, kind, phase)
	}
}

func checkMembers(t *testing.T, got map[string]string, kv *kvv1.KVCluster, size int) {
	t.Helper()
	if want := render.Members(kv, size); !maps.Equal(got, want) {
		t.Fatalf("members = %v, want %v", got, want)
	}
}

// --- scale up: 4 -> 5 at epoch 2 ---

func TestU1WritesConfigMapAtNextEpoch(t *testing.T) {
	o := stable(4, 2, 5)
	d := Plan(o, DefaultConfig)
	check(t, d, ActWriteConfigMap, kvv1.PhaseScaling)
	if d.Action.Epoch != 3 {
		t.Errorf("epoch = %d, want 3", d.Action.Epoch)
	}
	checkMembers(t, d.Action.Members, o.Cluster, 5)
}

func TestU1WaitsForOldVolume(t *testing.T) {
	o := stable(4, 2, 5)
	o.PVCs = append(o.PVCs, 4) // kv-4's volume from an earlier scale-down
	d := Plan(o, DefaultConfig)
	check(t, d, ActWait, kvv1.PhaseScaling)
	if d.Status.Reason != ReasonWaitingForVolumeRemoval && d.Status.Reason != ReasonWaitingForOldVolume {
		t.Errorf("reason = %s", d.Status.Reason)
	}
}

func TestU2ScalesStatefulSet(t *testing.T) {
	o := stable(4, 2, 5)
	o.ConfigMap = &ConfigMapObs{Epoch: 3, Members: render.Members(o.Cluster, 5)}
	d := Plan(o, DefaultConfig)
	check(t, d, ActScaleStatefulSet, kvv1.PhaseScaling)
	if d.Action.Replicas != 5 {
		t.Errorf("replicas = %d, want 5", d.Action.Replicas)
	}
}

// U2 does not create the new pod while an existing one is down: the new
// pod's handoffs and heartbeats would start against a missing member.
func TestU2WaitsForExistingPods(t *testing.T) {
	o := stable(4, 2, 5)
	o.ConfigMap = &ConfigMapObs{Epoch: 3, Members: render.Members(o.Cluster, 5)}
	down(&o, 2, t0.Add(-10*time.Second))
	d := Plan(o, DefaultConfig)
	check(t, d, ActWait, kvv1.PhaseScaling)
	if d.Status.Reason != ReasonWaitingForPods || !strings.Contains(d.Status.Message, "kv-2") {
		t.Errorf("status = %s: %s", d.Status.Reason, d.Status.Message)
	}
}

// A pod that restarted after U1 read the ConfigMap at epoch 3 and adopted
// it; the others are adopting it from it. Still U2.
func TestU2AfterEarlyAdoption(t *testing.T) {
	o := stable(4, 2, 5)
	o.ConfigMap = &ConfigMapObs{Epoch: 3, Members: render.Members(o.Cluster, 5)}
	at(&o, 1, 3, 5, false)
	at(&o, 2, 3, 5, false)
	check(t, Plan(o, DefaultConfig), ActScaleStatefulSet, kvv1.PhaseScaling)
}

func TestU3WaitsForEpochNotDegraded(t *testing.T) {
	for name, setup := range map[string]func(o *Observation){
		"kv-4 starting": func(o *Observation) { addPod(o, t0.Add(-5*time.Second)) },
		"kv-4 at 3, others at 2": func(o *Observation) {
			i := addPod(o, t0.Add(-5*time.Second))
			o.Pods[i].Ready = true
			at(o, i, 3, 5, false)
		},
		"half adopted": func(o *Observation) {
			i := addPod(o, t0.Add(-5*time.Second))
			o.Pods[i].Ready = true
			at(o, i, 3, 5, false)
			at(o, 0, 3, 5, false)
			at(o, 3, 3, 5, false)
		},
	} {
		t.Run(name, func(t *testing.T) {
			o := stable(4, 2, 5)
			o.ConfigMap = &ConfigMapObs{Epoch: 3, Members: render.Members(o.Cluster, 5)}
			o.StatefulSet.Replicas = 5
			setup(&o)
			d := Plan(o, DefaultConfig)
			check(t, d, ActWait, kvv1.PhaseScaling)
			if d.Status.Reason != ReasonWaitingForEpoch {
				t.Errorf("reason = %s (%s), want WaitingForEpoch", d.Status.Reason, d.Status.Message)
			}
		})
	}
}

func scaledUp() Observation {
	o := stable(4, 2, 5)
	o.ConfigMap = &ConfigMapObs{Epoch: 3, Members: render.Members(o.Cluster, 5)}
	o.StatefulSet.Replicas, o.StatefulSet.ReadyReplicas = 5, 5
	i := addPod(&o, t0.Add(-time.Minute))
	o.Pods[i].Ready = true
	for j := range 5 {
		at(&o, j, 3, 5, true)
	}
	o.PVCs = append(o.PVCs, 4)
	return o
}

func TestU4WaitsForEveryHandoff(t *testing.T) {
	o := scaledUp()
	at(&o, 2, 3, 5, false)
	d := Plan(o, DefaultConfig)
	check(t, d, ActWait, kvv1.PhaseScaling)
	if d.Status.Reason != ReasonWaitingForHandoff || !strings.Contains(d.Status.Message, "kv-2") {
		t.Errorf("status = %s: %s", d.Status.Reason, d.Status.Message)
	}
}

func TestScaleUpDone(t *testing.T) {
	d := Plan(scaledUp(), DefaultConfig)
	check(t, d, ActNone, kvv1.PhaseReady)
	if d.Status.Epoch != 3 || len(d.Status.Members) != 5 {
		t.Errorf("status epoch %d, %d members", d.Status.Epoch, len(d.Status.Members))
	}
}

// --- scale down: 5 -> 4 at epoch 3 ---

func TestD1PostsClusterMembersWithoutLeavingPod(t *testing.T) {
	o := stable(5, 3, 4)
	d := Plan(o, DefaultConfig)
	check(t, d, ActPostMembership, kvv1.PhaseScaling)
	if d.Action.Ordinal != 0 || d.Action.Epoch != 4 {
		t.Errorf("POST to pod %d at epoch %d, want pod 0 at epoch 4", d.Action.Ordinal, d.Action.Epoch)
	}
	checkMembers(t, d.Action.Members, o.Cluster, 4)
	// The body is a copy: changing it must not change pod 0's answer.
	d.Action.Members["x"] = "y"
	if _, ok := o.Pods[0].Membership.Members["x"]; ok {
		t.Error("the POST body aliases pod 0's membership")
	}
}

// With pod 0 (or any pod) not ready the cluster is not stable: no POST.
func TestD1WaitsForPod0(t *testing.T) {
	o := stable(5, 3, 4)
	down(&o, 0, t0.Add(-10*time.Second))
	d := Plan(o, DefaultConfig)
	check(t, d, ActWait, kvv1.PhaseReady)
	if d.Status.Reason != ReasonWaitingForPods || !strings.Contains(d.Status.Message, "kv-0") {
		t.Errorf("status = %s: %s", d.Status.Reason, d.Status.Message)
	}
}

// afterPost is the cluster right after D1: pod 0 at epoch 4 without kv-4.
func afterPost() Observation {
	o := stable(5, 3, 4)
	at(&o, 0, 4, 4, false)
	return o
}

func TestD2WaitsForDrainAndRecordsStart(t *testing.T) {
	o := afterPost()
	at(&o, 4, 4, 4, true)
	drainAnswer(&o, 4, false, 120)
	d := Plan(o, DefaultConfig)
	check(t, d, ActWait, kvv1.PhaseScaling)
	if d.Status.DrainStartedAt == nil || !d.Status.DrainStartedAt.Equal(t0) {
		t.Errorf("drainStartedAt = %v, want %v", d.Status.DrainStartedAt, t0)
	}
	if !strings.Contains(d.Status.Message, "120 keys") || !strings.Contains(d.Status.Message, "kv-1") {
		t.Errorf("message = %s", d.Status.Message)
	}
}

// Right after the POST the leaving pod has not even heard of epoch 4.
func TestD2BeforeTheLeavingPodKnows(t *testing.T) {
	o := afterPost()
	check(t, Plan(o, DefaultConfig), ActWait, kvv1.PhaseScaling)
}

func TestD2DrainBlocked(t *testing.T) {
	for _, tt := range []struct {
		waited time.Duration
		phase  kvv1.Phase
	}{
		{29 * time.Minute, kvv1.PhaseScaling},
		{30 * time.Minute, kvv1.PhaseScaling}, // not over the timeout yet
		{31 * time.Minute, kvv1.PhaseDrainBlocked},
	} {
		t.Run(tt.waited.String(), func(t *testing.T) {
			o := afterPost()
			at(&o, 4, 4, 4, false)
			drainAnswer(&o, 4, false, 7)
			o.Cluster.Status.DrainStartedAt = &metav1.Time{Time: t0.Add(-tt.waited)}
			d := Plan(o, DefaultConfig)
			check(t, d, ActWait, tt.phase)
			if !d.Status.DrainStartedAt.Equal(t0.Add(-tt.waited)) {
				t.Errorf("drainStartedAt moved to %v", d.Status.DrainStartedAt)
			}
		})
	}
}

func TestD3WritesConfigMapOnceDrained(t *testing.T) {
	o := afterPost()
	for i := range 5 {
		at(&o, i, 4, 4, true)
	}
	drainAnswer(&o, 4, true, 0)
	o.Cluster.Status.DrainStartedAt = &metav1.Time{Time: t0.Add(-time.Minute)}
	d := Plan(o, DefaultConfig)
	check(t, d, ActWriteConfigMap, kvv1.PhaseScaling)
	if d.Action.Epoch != 4 {
		t.Errorf("epoch = %d, want 4", d.Action.Epoch)
	}
	checkMembers(t, d.Action.Members, o.Cluster, 4)
	if d.Status.DrainStartedAt != nil {
		t.Error("drainStartedAt still set after the drain")
	}
}

// A drained answer for an older epoch is not a drain at this one.
func TestD3NeedsDrainAtThisEpoch(t *testing.T) {
	o := afterPost()
	at(&o, 4, 4, 4, true)
	drainAnswer(&o, 3, true, 0)
	check(t, Plan(o, DefaultConfig), ActWait, kvv1.PhaseScaling)
}

func afterD3() Observation {
	o := stable(5, 3, 4)
	for i := range 5 {
		at(&o, i, 4, 4, true)
	}
	o.ConfigMap = &ConfigMapObs{Epoch: 4, Members: render.Members(o.Cluster, 4)}
	return o
}

func TestD4ScalesStatefulSetDown(t *testing.T) {
	for name, setup := range map[string]func(o *Observation){
		"kv-4 still running":         func(o *Observation) {},
		"kv-4 restarted and failing": func(o *Observation) { down(o, 4, t0.Add(-time.Hour)) },
	} {
		t.Run(name, func(t *testing.T) {
			o := afterD3()
			setup(&o)
			d := Plan(o, DefaultConfig)
			check(t, d, ActScaleStatefulSet, kvv1.PhaseScaling)
			if d.Action.Replicas != 4 {
				t.Errorf("replicas = %d, want 4", d.Action.Replicas)
			}
		})
	}
}

func TestD5WaitsForPodAndVolume(t *testing.T) {
	o := afterD3()
	o.StatefulSet.Replicas, o.StatefulSet.ReadyReplicas = 4, 4
	o.Pods[4].Terminating = true
	check(t, Plan(o, DefaultConfig), ActWait, kvv1.PhaseScaling)

	o.Pods = o.Pods[:4] // the pod is gone, its PVC is not
	d := Plan(o, DefaultConfig)
	check(t, d, ActWait, kvv1.PhaseScaling)
	if d.Status.Reason != ReasonWaitingForVolumeRemoval {
		t.Errorf("reason = %s", d.Status.Reason)
	}

	o.PVCs = []int{0, 1, 2, 3}
	at(&o, 1, 4, 4, false)
	check(t, Plan(o, DefaultConfig), ActWait, kvv1.PhaseScaling) // kv-1's handoff

	at(&o, 1, 4, 4, true)
	check(t, Plan(o, DefaultConfig), ActNone, kvv1.PhaseReady)
}

// --- one change at a time ---

func TestSpecChangeMidStepFinishesTheStep(t *testing.T) {
	// U2 pending, and the spec now asks for 3 replicas: still U2.
	o := stable(4, 2, 3)
	o.ConfigMap = &ConfigMapObs{Epoch: 3, Members: render.Members(o.Cluster, 5)}
	check(t, Plan(o, DefaultConfig), ActScaleStatefulSet, kvv1.PhaseScaling)

	// D2 in progress, and the spec now asks for 6: keep draining, no POST.
	o = afterPost()
	o.Cluster.Spec.Replicas = 6
	check(t, Plan(o, DefaultConfig), ActWait, kvv1.PhaseScaling)

	// U4 in progress (a handoff pending), spec back to 4: wait, no POST.
	o = scaledUp()
	o.Cluster.Spec.Replicas = 4
	at(&o, 3, 3, 5, false)
	check(t, Plan(o, DefaultConfig), ActWait, kvv1.PhaseScaling)
}

func TestTemplateChangeOnlyWhenStable(t *testing.T) {
	o := stable(4, 2, 4)
	o.StatefulSet.TemplateCurrent = false
	d := Plan(o, DefaultConfig)
	check(t, d, ActUpdateTemplate, kvv1.PhaseScaling)
	if d.Action.Replicas != 4 {
		t.Errorf("replicas = %d", d.Action.Replicas)
	}

	// Mid scale-up (U3): no template change.
	o = stable(4, 2, 5)
	o.ConfigMap = &ConfigMapObs{Epoch: 3, Members: render.Members(o.Cluster, 5)}
	o.StatefulSet.Replicas = 5
	o.StatefulSet.TemplateCurrent = false
	addPod(&o, t0)
	check(t, Plan(o, DefaultConfig), ActWait, kvv1.PhaseScaling)

	// Template first, then replicas.
	o = stable(4, 2, 5)
	o.StatefulSet.TemplateCurrent = false
	check(t, Plan(o, DefaultConfig), ActUpdateTemplate, kvv1.PhaseScaling)

	// During the rollout: wait.
	o = stable(4, 2, 4)
	o.StatefulSet.RolledOut = false
	check(t, Plan(o, DefaultConfig), ActWait, kvv1.PhaseScaling)
}

// --- bootstrap ---

func TestBootstrap(t *testing.T) {
	kv := kvc(5)
	kv.Status = kvv1.KVClusterStatus{}
	o := Observation{Now: t0, Cluster: kv}
	d := Plan(o, DefaultConfig)
	check(t, d, ActWriteConfigMap, kvv1.PhasePending)
	if d.Action.Epoch != 0 {
		t.Errorf("epoch = %d", d.Action.Epoch)
	}
	checkMembers(t, d.Action.Members, kv, 5)

	o.ConfigMap = &ConfigMapObs{Epoch: 0, Members: render.Members(kv, 5)}
	d = Plan(o, DefaultConfig)
	check(t, d, ActScaleStatefulSet, kvv1.PhasePending)
	if d.Action.Replicas != 5 {
		t.Errorf("replicas = %d", d.Action.Replicas)
	}

	// Pods starting: Pending, not Degraded.
	o.StatefulSet = &StatefulSetObs{Replicas: 5, RolledOut: true, TemplateCurrent: true}
	for range 5 {
		addPod(&o, t0.Add(-10*time.Second))
	}
	check(t, Plan(o, DefaultConfig), ActWait, kvv1.PhasePending)

	// The spec changed before the StatefulSet existed: create it from the
	// ConfigMap, scale afterwards.
	o.StatefulSet, o.Pods = nil, nil
	o.Cluster.Spec.Replicas = 7
	d = Plan(o, DefaultConfig)
	check(t, d, ActScaleStatefulSet, kvv1.PhasePending)
	if d.Action.Replicas != 5 {
		t.Errorf("replicas = %d, want the ConfigMap's 5", d.Action.Replicas)
	}
}

// --- Degraded ---

func TestDegraded(t *testing.T) {
	for _, tt := range []struct {
		name   string
		setup  func(o *Observation)
		reason string
	}{
		{"address mismatch", func(o *Observation) {
			o.Pods[2].Membership.Members["kv-1"] = "kv-1.kv.other.svc.cluster.local:7000"
		}, ReasonAddressMismatch},
		{"foreign member", func(o *Observation) {
			o.Pods[2].Membership.Members["db-9"] = "db-9.db.kvstore.svc.cluster.local:7000"
		}, ReasonAddressMismatch},
		{"same epoch, different members", func(o *Observation) { at(o, 1, 2, 3, true) }, ReasonMembershipConflict},
		{"epochs two apart", func(o *Observation) { at(o, 1, 4, 4, true) }, ReasonUnexplainedDivergence},
		{"three epochs", func(o *Observation) { at(o, 1, 3, 4, true); at(o, 2, 1, 4, true) }, ReasonUnexplainedDivergence},
		// Epoch 3 adding kv-4, with the ConfigMap still at 2: no step of
		// the operator's does that (U1 writes the ConfigMap first). Removing
		// kv-3 at epoch 3 would be explained: it is D1's result.
		{"a foreign change at the next epoch", func(o *Observation) { at(o, 1, 3, 5, true) }, ReasonUnexplainedDivergence},
		{"ConfigMap with a gap", func(o *Observation) { delete(o.ConfigMap.Members, "kv-2") }, ReasonConfigMapUnexpected},
		{"ConfigMap two members ahead", func(o *Observation) {
			o.ConfigMap = &ConfigMapObs{Epoch: 3, Members: render.Members(o.Cluster, 6)}
		}, ReasonUnexplainedDivergence},
		{"ConfigMap missing", func(o *Observation) { o.ConfigMap = nil }, ReasonConfigMapMissing},
		{"pod down beyond grace", func(o *Observation) { down(o, 3, t0.Add(-3*time.Minute)) }, ReasonPodUnavailable},
		{"pod ready but admin silent beyond grace", func(o *Observation) {
			o.Pods[3].Membership = nil
			o.Pods[3].UnreachableSince = t0.Add(-3 * time.Minute)
		}, ReasonPodUnavailable},
		{"new pod never started (U3)", func(o *Observation) {
			o.ConfigMap = &ConfigMapObs{Epoch: 3, Members: render.Members(o.Cluster, 5)}
			o.StatefulSet.Replicas = 5
			addPod(o, t0.Add(-10*time.Minute))
		}, ReasonPodUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o := stable(4, 2, 6) // the spec wants a change: Degraded must block it
			tt.setup(&o)
			d := Plan(o, DefaultConfig)
			check(t, d, ActWait, kvv1.PhaseDegraded)
			if d.Status.Reason != tt.reason {
				t.Errorf("reason = %s (%s), want %s", d.Status.Reason, d.Status.Message, tt.reason)
			}
		})
	}
}

// One missed admin answer from a pod that has been ready for an hour is
// not Degraded: unreachability has its own clock.
func TestOneMissedAnswerIsNotDegraded(t *testing.T) {
	for name, since := range map[string]time.Time{
		"just now":                    t0,
		"unknown, operator restarted": {},
		"within grace":                t0.Add(-time.Minute),
	} {
		t.Run(name, func(t *testing.T) {
			o := stable(4, 2, 4)
			o.Pods[3].Membership = nil
			o.Pods[3].UnreachableSince = since
			check(t, Plan(o, DefaultConfig), ActWait, kvv1.PhaseReady)
		})
	}
}

func TestWithinGraceIsNotDegraded(t *testing.T) {
	o := stable(4, 2, 4)
	down(&o, 3, t0.Add(-time.Minute))
	d := Plan(o, DefaultConfig)
	check(t, d, ActWait, kvv1.PhaseReady) // the phase it had
	if d.Status.Reason != ReasonWaitingForPods {
		t.Errorf("reason = %s", d.Status.Reason)
	}
	o.Cluster.Spec.Replicas = 5 // no change while a pod is down
	check(t, Plan(o, DefaultConfig), ActWait, kvv1.PhaseReady)
}

// A Degraded drain keeps its start time.
func TestDegradedKeepsDrainStart(t *testing.T) {
	o := afterPost()
	o.Cluster.Status.DrainStartedAt = &metav1.Time{Time: t0.Add(-5 * time.Minute)}
	down(&o, 2, t0.Add(-10*time.Minute))
	d := Plan(o, DefaultConfig)
	check(t, d, ActWait, kvv1.PhaseDegraded)
	if d.Status.DrainStartedAt == nil || !d.Status.DrainStartedAt.Equal(t0.Add(-5*time.Minute)) {
		t.Errorf("drainStartedAt = %v", d.Status.DrainStartedAt)
	}
}

// The leaving pod being unreachable during D2 is DrainBlocked after the
// timeout, not Degraded.
func TestD2LeavingPodUnreachable(t *testing.T) {
	o := afterPost()
	down(&o, 4, t0.Add(-time.Hour))
	o.Cluster.Status.DrainStartedAt = &metav1.Time{Time: t0.Add(-time.Hour)}
	check(t, Plan(o, DefaultConfig), ActWait, kvv1.PhaseDrainBlocked)
}

func TestReadyIdle(t *testing.T) {
	d := Plan(stable(10, 0, 10), DefaultConfig)
	check(t, d, ActNone, kvv1.PhaseReady)
	if d.Action.RequeueAfter != DefaultConfig.Idle {
		t.Errorf("requeue = %v", d.Action.RequeueAfter)
	}
}

// --- a StatefulSet scaled by hand is scaled back ---

func TestManualScaleIsReverted(t *testing.T) {
	for _, E := range []uint64{0, 5} {
		t.Run(fmt.Sprintf("up at epoch %d", E), func(t *testing.T) {
			o := stable(3, E, 3)
			o.StatefulSet.Replicas = 4
			addPod(&o, t0) // kv-4 is not in the ConfigMap: it fails to start
			d := Plan(o, DefaultConfig)
			check(t, d, ActScaleStatefulSet, kvv1.PhaseScaling)
			if d.Action.Replicas != 3 {
				t.Errorf("replicas = %d, want 3", d.Action.Replicas)
			}
		})
		t.Run(fmt.Sprintf("down at epoch %d", E), func(t *testing.T) {
			o := stable(4, E, 4)
			o.StatefulSet.Replicas = 3
			// kv-3 is still terminating: wait for it and its volume.
			o.Pods[3].Terminating = true
			check(t, Plan(o, DefaultConfig), ActWait, kvv1.PhaseScaling)
			o.Pods = o.Pods[:3]
			o.PVCs = []int{0, 1, 2}
			d := Plan(o, DefaultConfig)
			check(t, d, ActScaleStatefulSet, kvv1.PhaseScaling)
			if d.Action.Replicas != 4 {
				t.Errorf("replicas = %d, want 4", d.Action.Replicas)
			}
		})
	}
}

func TestUnreadableConfigMap(t *testing.T) {
	o := stable(3, 1, 3)
	o.ConfigMap.Invalid = "config.yaml does not parse"
	d := Plan(o, DefaultConfig)
	check(t, d, ActWait, kvv1.PhaseDegraded)
	if d.Status.Reason != ReasonConfigMapUnexpected {
		t.Errorf("reason = %s", d.Status.Reason)
	}
}
