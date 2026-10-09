package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	kvv1 "github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/api/v1alpha1"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/planner"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/render"
)

// gkePool is the GKE database node pool's name, and its taint's value.
const gkePool = "kvstore"

// gkePlacement is every placement setting, as the GKE sample has them.
func gkePlacement() *kvv1.Placement {
	return &kvv1.Placement{
		OnePodPerNode: true,
		ZoneSpread:    true,
		NodeSelector:  map[string]string{"cloud.google.com/gke-nodepool": gkePool},
		Tolerations: []corev1.Toleration{
			{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: gkePool, Effect: corev1.TaintEffectNoSchedule},
		},
	}
}

// expectPlacement checks that the StatefulSet's pods are placed as p
// renders (nil: no placement at all), as read back from the API server.
func (h *harness) expectPlacement(p *kvv1.Placement) {
	h.t.Helper()
	kv := h.cluster()
	kv.Spec.Placement = p
	want := render.StatefulSet(kv, 0).Spec.Template.Spec
	got := h.sts().Spec.Template.Spec
	if !equality.Semantic.DeepEqual(placementOf(&got), placementOf(&want)) {
		h.t.Fatalf("StatefulSet placement = %+v, want %+v", placementOf(&got), placementOf(&want))
	}
}

// TestPodDisruptionBudget: the operator (as its service account, so the role
// allows it) creates the PodDisruptionBudget with the cluster, owned by the
// KVCluster, and recreates it when it is deleted.
func TestPodDisruptionBudget(t *testing.T) {
	h := newHarness(t, "pdb")
	h.reconcile()
	get := func() *policyv1.PodDisruptionBudget {
		t.Helper()
		pdb := &policyv1.PodDisruptionBudget{}
		must(t, admin.Get(h.ctx, h.name("kv"), pdb))
		return pdb
	}
	pdb := get()
	if !metav1.IsControlledBy(pdb, h.cluster()) {
		t.Fatalf("PodDisruptionBudget owners = %+v, want the controller reference to the KVCluster", pdb.OwnerReferences)
	}
	if mu := pdb.Spec.MaxUnavailable; mu == nil || *mu != intstr.FromInt32(1) {
		t.Fatalf("maxUnavailable = %v, want 1", mu)
	}
	if pdb.Spec.MinAvailable != nil {
		t.Fatalf("minAvailable = %v, want unset", pdb.Spec.MinAvailable)
	}
	if !equality.Semantic.DeepEqual(pdb.Spec.Selector, &metav1.LabelSelector{MatchLabels: render.SelectorLabels()}) {
		t.Fatalf("selector = %v, want the StatefulSet's", pdb.Spec.Selector)
	}

	must(t, admin.Delete(h.ctx, pdb))
	h.reconcile()
	if again := get(); again.UID == pdb.UID || !metav1.IsControlledBy(again, h.cluster()) {
		t.Fatalf("PodDisruptionBudget not recreated as owned: %+v", again.ObjectMeta)
	}
}

// TestPlacementWaitsForAStablePoint: placement set in the middle of a
// scale-up is not applied by the scale step (U2), only once the cluster is
// stable, as one template change; and a template read back from the API
// server is then current, so it is not applied again.
func TestPlacementWaitsForAStablePoint(t *testing.T) {
	h := newHarness(t, "place-mid")
	h.ready(3)
	h.expectPlacement(nil)

	h.setSpec(func(s *kvv1.KVClusterSpec) { s.Replicas = 4 })
	h.reconcile() // U1
	h.expect(kvv1.PhaseScaling, planner.ReasonAddingPod, 0, 3, 3)
	h.setSpec(func(s *kvv1.KVClusterSpec) { s.Placement = gkePlacement() })
	h.reconcile() // U2: replicas only
	h.expectReplicas(4)
	h.expectPlacement(nil)

	h.createPod(3)
	h.createPVC(3)
	for i := range 4 {
		h.nodes.set(render.MemberID(h.cluster(), i), 1, h.members(4), true)
	}
	h.rolledOut(4, "r1")
	h.reconcile() // stable: the template change
	h.expect(kvv1.PhaseScaling, planner.ReasonRollingUpdate, 1, 4, 4)
	h.expectReplicas(4)
	h.expectPlacement(gkePlacement())

	h.rolledOut(4, "r2")
	h.reconcile()
	h.expect(kvv1.PhaseReady, planner.ReasonReady, 1, 4, 4)
	gen := h.sts().Generation
	h.reconcile()
	h.expect(kvv1.PhaseReady, planner.ReasonReady, 1, 4, 4)
	if g := h.sts().Generation; g != gen {
		t.Fatalf("StatefulSet generation %d -> %d while Ready: the placement read back is not seen as current", gen, g)
	}
}

// TestPlacementKeptWhileScaling: a cluster created with placement keeps it
// through a scale step even when the spec drops it mid-scale; the removal
// is applied once stable.
func TestPlacementKeptWhileScaling(t *testing.T) {
	h := newHarness(t, "place-keep")
	h.setSpec(func(s *kvv1.KVClusterSpec) { s.Placement = gkePlacement() })
	h.ready(3)
	h.expectPlacement(gkePlacement())

	h.setSpec(func(s *kvv1.KVClusterSpec) { s.Replicas = 4 })
	h.reconcile() // U1
	h.setSpec(func(s *kvv1.KVClusterSpec) { s.Placement = nil })
	h.reconcile() // U2
	h.expectReplicas(4)
	h.expectPlacement(gkePlacement())

	h.createPod(3)
	h.createPVC(3)
	for i := range 4 {
		h.nodes.set(render.MemberID(h.cluster(), i), 1, h.members(4), true)
	}
	h.rolledOut(4, "r1")
	h.reconcile()
	h.expect(kvv1.PhaseScaling, planner.ReasonRollingUpdate, 1, 4, 4)
	h.expectPlacement(nil)
}

// TestApplyFailedIsRecorded: an apply the API server rejects, and a POST a
// node rejects (other than with 409), show in the ActionFailed condition
// with the error, and the condition clears once the next reconcile
// succeeds.
func TestApplyFailedIsRecorded(t *testing.T) {
	h := newHarness(t, "apply-failed")

	// The headless Service exists with a cluster IP, which is immutable:
	// applying it as headless fails before the planner runs.
	must(t, admin.Create(h.ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "kv", Namespace: h.key.Namespace},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "grpc", Port: 7000}}},
	}))
	if err := h.reconcileErr(); err == nil {
		t.Fatal("Reconcile succeeded over an immutable cluster IP")
	}
	h.expectActionFailed(metav1.ConditionTrue, ReasonApplyFailed, "apply Service kv", "clusterIP")
	svc := &corev1.Service{}
	must(t, admin.Get(h.ctx, h.name("kv"), svc))
	must(t, admin.Delete(h.ctx, svc))
	h.ready(3) // expect checks that ActionFailed is False again

	// A toleration key that is not a label key passes the CRD (its rules
	// do not check label syntax) and fails the StatefulSet's validation.
	h.setSpec(func(s *kvv1.KVClusterSpec) {
		s.Placement = &kvv1.Placement{Tolerations: []corev1.Toleration{{Key: "not a key", Operator: corev1.TolerationOpExists}}}
	})
	if err := h.reconcileErr(); err == nil {
		t.Fatal("Reconcile succeeded with an invalid toleration")
	}
	h.expectActionFailed(metav1.ConditionTrue, ReasonApplyFailed, "UpdateTemplate(3)", "apply StatefulSet kv", "tolerations")
	h.expectPlacement(nil)
	h.setSpec(func(s *kvv1.KVClusterSpec) { s.Placement = nil })
	h.reconcile()
	h.expect(kvv1.PhaseReady, planner.ReasonReady, 0, 3, 3)

	// A node refuses the POST that starts a scale-down.
	h.setSpec(func(s *kvv1.KVClusterSpec) { s.Replicas = 4 })
	h.reconcile() // U1
	h.reconcile() // U2
	h.createPod(3)
	h.createPVC(3)
	for i := range 4 {
		h.nodes.set(render.MemberID(h.cluster(), i), 1, h.members(4), true)
	}
	h.rolledOut(4, "r1")
	h.reconcile()
	h.expect(kvv1.PhaseReady, planner.ReasonReady, 1, 4, 4)
	h.setSpec(func(s *kvv1.KVClusterSpec) { s.Replicas = 3 })

	// A 409 is not a failure: the node has moved on, and the next
	// observation shows where.
	h.nodes.update("kv-0", func(n *fakeNode) { n.postStatus = 409 })
	h.reconcile()
	h.expect(kvv1.PhaseScaling, planner.ReasonRemovingPod, 1, 4, 4)
	h.nodes.update("kv-0", func(n *fakeNode) { n.postStatus = 503 })
	if err := h.reconcileErr(); err == nil {
		t.Fatal("Reconcile succeeded with the POST refused")
	}
	h.expectActionFailed(metav1.ConditionTrue, ReasonApplyFailed, "PostMembership(epoch 2", "503")
	h.nodes.update("kv-0", func(n *fakeNode) { n.postStatus = 0 })
	h.reconcile() // D1
	h.expect(kvv1.PhaseScaling, planner.ReasonRemovingPod, 1, 4, 4)
}
