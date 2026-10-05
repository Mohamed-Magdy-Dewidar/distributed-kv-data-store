package controller

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kvv1 "github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/api/v1alpha1"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/kvadmin"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/planner"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/render"
)

const testImage = "kvnode:dev"

var testPlanner = planner.Config{Grace: 2 * time.Minute, Fast: 2 * time.Second, Slow: 10 * time.Second, Idle: time.Minute}

// harness drives one KVCluster: it calls Reconcile directly (as the
// operator's service account) and plays the StatefulSet controller, the
// kubelet and the nodes by hand.
type harness struct {
	t     *testing.T
	ctx   context.Context
	key   types.NamespacedName
	r     *KVClusterReconciler
	nodes *fakeNodes
	now   time.Time
}

func newHarness(t *testing.T, namespace string, replicas int32) *harness {
	t.Helper()
	h := &harness{
		t: t, ctx: t.Context(),
		key:   types.NamespacedName{Namespace: namespace, Name: "kv"},
		nodes: newFakeNodes(t, 3),
		now:   time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC),
	}
	h.r = &KVClusterReconciler{Client: operator, Scheme: scheme, Admin: h.nodes.client(), Planner: testPlanner, Now: func() time.Time { return h.now }}
	must(t, admin.Create(h.ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}))
	must(t, admin.Create(h.ctx, &kvv1.KVCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "kv", Namespace: namespace},
		Spec: kvv1.KVClusterSpec{
			Replicas: replicas, Image: testImage,
			DrainTimeout: metav1.Duration{Duration: time.Minute},
		},
	}))
	return h
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (h *harness) reconcile() ctrl.Result {
	h.t.Helper()
	res, err := h.r.Reconcile(h.ctx, ctrl.Request{NamespacedName: h.key})
	if err != nil {
		h.t.Fatalf("Reconcile: %v", err)
	}
	return res
}

func (h *harness) cluster() *kvv1.KVCluster {
	h.t.Helper()
	kv := &kvv1.KVCluster{}
	must(h.t, admin.Get(h.ctx, h.key, kv))
	return kv
}

func (h *harness) members(k int) map[string]string { return render.Members(h.cluster(), k) }

func (h *harness) setSpec(fn func(*kvv1.KVClusterSpec)) {
	h.t.Helper()
	kv := h.cluster()
	fn(&kv.Spec)
	must(h.t, admin.Update(h.ctx, kv))
}

func (h *harness) name(suffix string) types.NamespacedName {
	return types.NamespacedName{Namespace: h.key.Namespace, Name: suffix}
}

// createPod creates pod kv-i, ready since now, as the StatefulSet and the
// kubelet would.
func (h *harness) createPod(i int) {
	h.t.Helper()
	l := maps.Clone(render.SelectorLabels())
	l[render.LabelInstance] = "kv"
	l[render.LabelManagedBy] = render.ManagerName
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: render.MemberID(h.cluster(), i), Namespace: h.key.Namespace, Labels: l},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "kv", Image: testImage}}},
	}
	must(h.t, admin.Create(h.ctx, p))
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(h.now)}}
	must(h.t, admin.Status().Update(h.ctx, p))
}

func (h *harness) deletePod(i int) {
	h.t.Helper()
	p := &corev1.Pod{}
	must(h.t, admin.Get(h.ctx, h.name(render.MemberID(h.cluster(), i)), p))
	// No node is assigned, so the API server deletes it at once.
	must(h.t, admin.Delete(h.ctx, p, client.GracePeriodSeconds(0)))
}

func (h *harness) createPVC(i int) {
	h.t.Helper()
	must(h.t, admin.Create(h.ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data-" + render.MemberID(h.cluster(), i), Namespace: h.key.Namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
		},
	}))
}

func (h *harness) deletePVC(i int) {
	h.t.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	must(h.t, admin.Get(h.ctx, h.name("data-"+render.MemberID(h.cluster(), i)), pvc))
	pvc.Finalizers = nil // no PVC protection controller runs here
	must(h.t, admin.Update(h.ctx, pvc))
	must(h.t, admin.Delete(h.ctx, pvc))
}

func (h *harness) sts() *appsv1.StatefulSet {
	h.t.Helper()
	sts := &appsv1.StatefulSet{}
	must(h.t, admin.Get(h.ctx, h.name("kv"), sts))
	return sts
}

// rolledOut sets the StatefulSet's status as its controller would once
// every pod runs the current revision and ready of them are ready.
func (h *harness) rolledOut(ready int32, revision string) {
	h.t.Helper()
	sts := h.sts()
	sts.Status = appsv1.StatefulSetStatus{
		ObservedGeneration: sts.Generation,
		Replicas:           *sts.Spec.Replicas, ReadyReplicas: ready, UpdatedReplicas: *sts.Spec.Replicas,
		CurrentRevision: revision, UpdateRevision: revision,
	}
	must(h.t, admin.Status().Update(h.ctx, sts))
}

func (h *harness) configMap() (uint64, map[string]string) {
	h.t.Helper()
	cm := &corev1.ConfigMap{}
	must(h.t, admin.Get(h.ctx, h.name("kv-config"), cm))
	e, m, err := render.ParseConfigMap(cm)
	must(h.t, err)
	return e, m
}

// expect checks every status field and condition.
func (h *harness) expect(phase kvv1.Phase, reason string, epoch int64, members int, replicas int32) {
	h.t.Helper()
	kv := h.cluster()
	s := kv.Status
	if s.Phase != phase || s.ObservedGeneration != kv.Generation || s.Epoch != epoch || s.Replicas != replicas {
		h.t.Fatalf("status = phase %s, observedGeneration %d (generation %d), epoch %d, replicas %d; want %s, %d, %d, %d (%s)",
			s.Phase, s.ObservedGeneration, kv.Generation, s.Epoch, s.Replicas, phase, kv.Generation, epoch, replicas, conditionText(s))
	}
	want := render.Members(kv, members)
	if len(s.Members) != members {
		h.t.Fatalf("status members = %v, want %d", s.Members, members)
	}
	for i, m := range s.Members {
		if id := render.MemberID(kv, i); m.ID != id || m.Address != want[id] {
			h.t.Fatalf("status member %d = %+v, want %s at %s", i, m, id, want[id])
		}
	}
	if s.Selector != "app.kubernetes.io/name=kvstore" {
		h.t.Fatalf("selector = %q", s.Selector)
	}
	is := func(phases ...kvv1.Phase) metav1.ConditionStatus {
		if slices.Contains(phases, phase) {
			return metav1.ConditionTrue
		}
		return metav1.ConditionFalse
	}
	for typ, status := range map[string]metav1.ConditionStatus{
		ConditionAvailable:    is(kvv1.PhaseReady, kvv1.PhaseScaling, kvv1.PhaseDrainBlocked),
		ConditionProgressing:  is(kvv1.PhasePending, kvv1.PhaseScaling),
		ConditionDrainBlocked: is(kvv1.PhaseDrainBlocked),
		ConditionDegraded:     is(kvv1.PhaseDegraded),
	} {
		c := meta.FindStatusCondition(s.Conditions, typ)
		if c == nil || c.Status != status || c.Reason != reason || c.ObservedGeneration != kv.Generation || c.Message == "" {
			h.t.Fatalf("condition %s = %+v, want status %s reason %s at generation %d", typ, c, status, reason, kv.Generation)
		}
	}
}

func conditionText(s kvv1.KVClusterStatus) string {
	if c := meta.FindStatusCondition(s.Conditions, ConditionProgressing); c != nil {
		return c.Reason + ": " + c.Message
	}
	return "no conditions"
}

func (h *harness) expectDrainStartedAt(want *time.Time) {
	h.t.Helper()
	got := h.cluster().Status.DrainStartedAt
	switch {
	case want == nil && got != nil:
		h.t.Fatalf("drainStartedAt = %v, want unset", got)
	case want != nil && (got == nil || !got.Time.Equal(*want)):
		h.t.Fatalf("drainStartedAt = %v, want %v", got, *want)
	}
}

func (h *harness) expectReplicas(want int32) {
	h.t.Helper()
	if got := *h.sts().Spec.Replicas; got != want {
		h.t.Fatalf("StatefulSet replicas = %d, want %d", got, want)
	}
}

func (h *harness) expectConfigMap(epoch uint64, members int) {
	h.t.Helper()
	e, m := h.configMap()
	if e != epoch || !maps.Equal(m, h.members(members)) {
		h.t.Fatalf("ConfigMap = epoch %d, %d members; want epoch %d, %d members", e, len(m), epoch, members)
	}
}

// TestScaleUpAndDown drives a cluster from nothing to 3 nodes, up to 4
// (with the spec changed mid-step), down to 3 (through DrainBlocked),
// through a pod that stops answering, a StatefulSet scaled by hand, and an
// image change, checking every status field and condition at each step.
func TestScaleUpAndDown(t *testing.T) {
	h := newHarness(t, "scale", 3)

	// --- bootstrap ---
	h.reconcile()
	h.expect(kvv1.PhasePending, planner.ReasonCreatingConfigMap, 0, 0, 0)
	h.expectConfigMap(0, 3)
	if err := admin.Get(h.ctx, h.name("kv"), &appsv1.StatefulSet{}); err == nil {
		t.Fatal("the StatefulSet exists after the first reconcile: two actions in one reconcile")
	}
	for _, svc := range []string{"kv", "kv-client"} {
		must(t, admin.Get(h.ctx, h.name(svc), &corev1.Service{}))
	}

	h.reconcile()
	h.expect(kvv1.PhasePending, planner.ReasonCreatingStatefulSet, 0, 0, 0)
	sts := h.sts()
	if *sts.Spec.Replicas != 3 || sts.Spec.Template.Spec.Containers[0].Image != testImage || !metav1.IsControlledBy(sts, h.cluster()) {
		t.Fatalf("StatefulSet = %d replicas, image %s, owners %v", *sts.Spec.Replicas, sts.Spec.Template.Spec.Containers[0].Image, sts.OwnerReferences)
	}

	for i := range 3 {
		h.createPod(i)
		h.createPVC(i)
		h.nodes.set(render.MemberID(h.cluster(), i), 0, h.members(3), true)
	}
	h.rolledOut(3, "r1")
	if res := h.reconcile(); res.RequeueAfter != testPlanner.Idle {
		t.Errorf("Ready requeue = %v, want %v", res.RequeueAfter, testPlanner.Idle)
	}
	h.expect(kvv1.PhaseReady, planner.ReasonReady, 0, 3, 3)

	// --- scale up 3 -> 4; the spec asks for 5 at first ---
	h.setSpec(func(s *kvv1.KVClusterSpec) { s.Replicas = 5 })
	h.reconcile() // U1
	h.expect(kvv1.PhaseScaling, planner.ReasonAddingPod, 0, 3, 3)
	h.expectConfigMap(1, 4)
	h.expectReplicas(3)

	// The image changes in the middle of the scale-up: U2 changes replicas
	// only, keeping the image the pods run.
	h.setSpec(func(s *kvv1.KVClusterSpec) { s.Image = "kvnode:v1.5" })
	h.reconcile() // U2: one node, whatever the spec says
	if img := h.sts().Spec.Template.Spec.Containers[0].Image; img != testImage {
		t.Fatalf("U2 changed the image to %s mid-scale", img)
	}
	// The status describes what this reconcile observed: 3 replicas, before
	// it scaled.
	h.expect(kvv1.PhaseScaling, planner.ReasonAddingPod, 0, 3, 3)
	h.expectReplicas(4)
	h.setSpec(func(s *kvv1.KVClusterSpec) { s.Image = testImage })
	h.setSpec(func(s *kvv1.KVClusterSpec) { s.Replicas = 4 })

	h.createPod(3)
	h.createPVC(3)
	h.nodes.set("kv-3", 1, h.members(4), false)
	h.rolledOut(4, "r1")
	if res := h.reconcile(); res.RequeueAfter != testPlanner.Fast {
		t.Errorf("U3 requeue = %v, want %v", res.RequeueAfter, testPlanner.Fast)
	}
	h.expect(kvv1.PhaseScaling, planner.ReasonWaitingForEpoch, 1, 4, 4)

	for i := range 3 {
		h.nodes.set(render.MemberID(h.cluster(), i), 1, h.members(4), i != 1)
	}
	h.nodes.update("kv-3", func(n *fakeNode) { n.handoffDone = true })
	h.reconcile()
	h.expect(kvv1.PhaseScaling, planner.ReasonWaitingForHandoff, 1, 4, 4)

	h.nodes.update("kv-1", func(n *fakeNode) { n.handoffDone = true })
	h.reconcile()
	h.expect(kvv1.PhaseReady, planner.ReasonReady, 1, 4, 4)
	if asks := h.nodes.takeHandoffAsks(); len(asks) != 0 {
		t.Fatalf("handoff asked of %v, which are all members", asks)
	}

	// --- a pod stops answering: Degraded only after the grace period ---
	h.nodes.update("kv-2", func(n *fakeNode) { n.up = false })
	h.reconcile()
	h.expect(kvv1.PhaseReady, planner.ReasonWaitingForPods, 1, 4, 4)
	h.now = h.now.Add(time.Minute)
	h.reconcile()
	h.expect(kvv1.PhaseReady, planner.ReasonWaitingForPods, 1, 4, 4)
	h.now = h.now.Add(2 * time.Minute)
	h.reconcile()
	h.expect(kvv1.PhaseDegraded, planner.ReasonPodUnavailable, 1, 4, 4)
	h.setSpec(func(s *kvv1.KVClusterSpec) { s.Replicas = 3 })
	h.reconcile()
	h.expect(kvv1.PhaseDegraded, planner.ReasonPodUnavailable, 1, 4, 4)
	if posts := h.nodes.takePosts(); len(posts) != 0 {
		t.Fatalf("POST while Degraded: %v", posts)
	}
	h.nodes.update("kv-2", func(n *fakeNode) { n.up = true })

	// --- scale down 4 -> 3 (the spec already asks for 3) ---
	h.reconcile() // D1
	h.expect(kvv1.PhaseScaling, planner.ReasonRemovingPod, 1, 4, 4)
	posts := h.nodes.takePosts()
	if len(posts) != 1 || posts[0].node != "kv-0" || posts[0].epoch != 2 || !maps.Equal(posts[0].members, h.members(3)) {
		t.Fatalf("posts = %+v, want one to kv-0 at epoch 2 with kv-0..kv-2", posts)
	}

	drainStart := h.now
	h.reconcile() // D2: kv-0 holds epoch 2; kv-3 still a member at 1
	h.expect(kvv1.PhaseScaling, planner.ReasonDraining, 2, 3, 4)
	h.expectDrainStartedAt(&drainStart)
	if asks := h.nodes.takeHandoffAsks(); len(asks) != 0 {
		t.Fatalf("handoff asked of %v before any pod left its own membership", asks)
	}

	for i := 1; i < 3; i++ {
		h.nodes.set(render.MemberID(h.cluster(), i), 2, h.members(3), true)
	}
	h.nodes.update("kv-0", func(n *fakeNode) { n.handoffDone = true })
	h.nodes.set("kv-3", 2, h.members(3), false) // outside its membership, not drained
	h.now = h.now.Add(30 * time.Second)
	h.reconcile()
	h.expect(kvv1.PhaseScaling, planner.ReasonDraining, 2, 3, 4)
	h.expectDrainStartedAt(&drainStart)
	if asks := h.nodes.takeHandoffAsks(); !slices.Equal(asks, []string{"kv-3"}) {
		t.Fatalf("handoff asked of %v, want only kv-3", asks)
	}

	h.now = h.now.Add(time.Minute) // past drainTimeout (1m)
	h.reconcile()
	h.expect(kvv1.PhaseDrainBlocked, planner.ReasonDrainBlocked, 2, 3, 4)
	h.expectDrainStartedAt(&drainStart)
	h.expectReplicas(4)

	h.nodes.update("kv-3", func(n *fakeNode) { n.handoffDone, n.drained = true, true })
	h.reconcile() // D3
	h.expect(kvv1.PhaseScaling, planner.ReasonDrained, 2, 3, 4)
	h.expectConfigMap(2, 3)
	h.expectDrainStartedAt(nil)
	h.expectReplicas(4)

	h.reconcile() // D4
	h.expect(kvv1.PhaseScaling, planner.ReasonRemovingPod, 2, 3, 4)
	h.expectReplicas(3)

	h.deletePod(3)
	h.rolledOut(3, "r1")
	h.reconcile() // D5: the volume is still there
	h.expect(kvv1.PhaseScaling, planner.ReasonWaitingForVolumeRemoval, 2, 3, 3)

	h.deletePVC(3)
	h.reconcile()
	h.expect(kvv1.PhaseReady, planner.ReasonReady, 2, 3, 3)

	// --- the StatefulSet scaled by hand is scaled back ---
	sts = h.sts()
	patch := client.MergeFrom(sts.DeepCopy())
	sts.Spec.Replicas = new(int32(4))
	must(t, admin.Patch(h.ctx, sts, patch, client.FieldOwner("kubectl")))
	h.reconcile()
	h.expect(kvv1.PhaseScaling, planner.ReasonRemovingPod, 2, 3, 4)
	h.expectReplicas(3)
	h.rolledOut(3, "r1")
	h.reconcile()
	h.expect(kvv1.PhaseReady, planner.ReasonReady, 2, 3, 3)

	// --- an image change, applied while stable ---
	h.setSpec(func(s *kvv1.KVClusterSpec) { s.Image = "kvnode:v2" })
	h.reconcile()
	h.expect(kvv1.PhaseScaling, planner.ReasonRollingUpdate, 2, 3, 3)
	sts = h.sts()
	if img := sts.Spec.Template.Spec.Containers[0].Image; img != "kvnode:v2" || *sts.Spec.Replicas != 3 {
		t.Fatalf("StatefulSet after the image change: image %s, %d replicas", img, *sts.Spec.Replicas)
	}
	sts.Status = appsv1.StatefulSetStatus{ObservedGeneration: sts.Generation, Replicas: 3, ReadyReplicas: 3, UpdatedReplicas: 1, CurrentRevision: "r1", UpdateRevision: "r2"}
	must(t, admin.Status().Update(h.ctx, sts))
	h.reconcile()
	h.expect(kvv1.PhaseScaling, planner.ReasonRollingUpdate, 2, 3, 3)
	h.rolledOut(3, "r2")
	h.reconcile()
	h.expect(kvv1.PhaseReady, planner.ReasonReady, 2, 3, 3)
}

// TestUnreachableSinceFollowsThePod: the clock restarts for a new pod (a new
// UID) and when the pod answers again.
func TestUnreachableSinceFollowsThePod(t *testing.T) {
	r := &KVClusterReconciler{}
	t0 := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	pod := func(uid string) *corev1.Pod { return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid)}} }
	observe := func(now time.Time, p *corev1.Pod, answers bool) time.Time {
		o := &observed{Observation: planner.Observation{Now: now, Pods: make([]planner.PodObs, 1)}}
		if answers {
			o.Pods[0].Membership = &kvadmin.Membership{}
		}
		r.trackUnreachable(o, map[int]*corev1.Pod{0: p})
		return o.Pods[0].UnreachableSince
	}
	a := pod("a")
	if got := observe(t0, a, false); !got.Equal(t0) {
		t.Fatalf("first miss: %v", got)
	}
	if got := observe(t0.Add(time.Minute), a, false); !got.Equal(t0) {
		t.Fatalf("second miss: %v, want %v", got, t0)
	}
	if got := observe(t0.Add(2*time.Minute), pod("b"), false); !got.Equal(t0.Add(2 * time.Minute)) {
		t.Fatalf("a new pod: %v", got)
	}
	if got := observe(t0.Add(3*time.Minute), a, true); !got.IsZero() {
		t.Fatalf("answering: %v", got)
	}
	if got := observe(t0.Add(4*time.Minute), a, false); !got.Equal(t0.Add(4 * time.Minute)) {
		t.Fatalf("after answering: %v", got)
	}
}
