package controller

import (
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kvv1 "github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/api/v1alpha1"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/planner"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/render"
)

// TestManagerWatches runs the reconciler in a manager, as the operator's
// service account, so its informers (one per watched or read type) need the
// role's list and watch: a KVCluster gets its ConfigMap and, through the
// ConfigMap's watch event, its StatefulSet, with nobody calling Reconcile.
func TestManagerWatches(t *testing.T) {
	const ns = "watch"
	mgr, err := ctrl.NewManager(operatorCfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	must(t, err)
	nodes := newFakeNodes(t, 3)
	r := &KVClusterReconciler{Client: mgr.GetClient(), Scheme: scheme, Admin: nodes.client(),
		// Long requeues: only watch events can move this cluster on.
		Planner: planner5m()}
	must(t, r.SetupWithManager(mgr))
	ctx := t.Context()
	go func() {
		if err := mgr.Start(ctx); err != nil {
			t.Errorf("manager: %v", err)
		}
	}()

	must(t, admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}))
	must(t, admin.Create(ctx, &kvv1.KVCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "kv", Namespace: ns},
		Spec:       kvv1.KVClusterSpec{Replicas: 3, Image: testImage},
	}))
	eventually(t, 20*time.Second, "the StatefulSet", func() bool {
		return admin.Get(ctx, types.NamespacedName{Namespace: ns, Name: "kv"}, &appsv1.StatefulSet{}) == nil
	})
	status := func() kvv1.KVClusterStatus {
		kv := &kvv1.KVCluster{}
		must(t, admin.Get(ctx, types.NamespacedName{Namespace: ns, Name: "kv"}, kv))
		return kv.Status
	}
	eventually(t, 10*time.Second, "status Pending, waiting for kv-0", func() bool {
		s := status()
		return s.Phase == kvv1.PhasePending && s.Conditions != nil && strings.Contains(conditionText(s), "kv-0")
	})

	// The nodes answer and the StatefulSet reports its pods ready; only the
	// pods are missing. Creating them is the last change, so only the pod
	// watch can bring the cluster to Ready before the 5-minute requeue.
	h := &harness{t: t, ctx: ctx, key: types.NamespacedName{Namespace: ns, Name: "kv"}, nodes: nodes, now: time.Now()}
	for i := range 3 {
		nodes.set(render.MemberID(h.cluster(), i), 0, h.members(3), true)
	}
	h.rolledOut(3, "r1")
	time.Sleep(time.Second) // let the StatefulSet's event be handled
	if s := status(); s.Phase == kvv1.PhaseReady {
		t.Fatalf("Ready before any pod exists: %+v", s)
	}
	for i := range 3 {
		h.createPod(i)
	}
	eventually(t, 10*time.Second, "Ready after the pods appeared", func() bool { return status().Phase == kvv1.PhaseReady })
}

func planner5m() planner.Config {
	c := testPlanner
	c.Fast, c.Slow, c.Idle = 5*time.Minute, 5*time.Minute, 5*time.Minute
	return c
}

func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", within, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestPodToCluster(t *testing.T) {
	pod := func(l map[string]string) client.Object {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "kv-1", Namespace: "prod", Labels: l}}
	}
	got := podToCluster(t.Context(), pod(map[string]string{
		"app.kubernetes.io/name": "kvstore", render.LabelInstance: "db", render.LabelManagedBy: render.ManagerName,
	}))
	want := []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "prod", Name: "db"}}}
	if len(got) != 1 || got[0] != want[0] {
		t.Errorf("podToCluster = %v, want %v", got, want)
	}
	for name, l := range map[string]map[string]string{
		"no labels":          nil,
		"not the operator's": {render.LabelInstance: "db"},
		"no instance":        {render.LabelManagedBy: render.ManagerName},
		"another manager's":  {render.LabelInstance: "db", render.LabelManagedBy: "helm"},
	} {
		if got := podToCluster(t.Context(), pod(l)); len(got) != 0 {
			t.Errorf("%s: podToCluster = %v, want none", name, got)
		}
	}
}

// TestRoleAllowsTheInformers: the manager caches every type the controller
// watches or reads, and a cache needs list and watch. Without watch an
// informer falls back to relisting with backoff, which still delivers
// changes, only late, so this checks the permission directly, as the
// operator's service account.
func TestRoleAllowsTheInformers(t *testing.T) {
	c, err := client.NewWithWatch(operatorCfg, client.Options{Scheme: scheme})
	must(t, err)
	for _, list := range []client.ObjectList{
		&kvv1.KVClusterList{}, &appsv1.StatefulSetList{}, &corev1.ConfigMapList{},
		&corev1.ServiceList{}, &corev1.PodList{}, &corev1.PersistentVolumeClaimList{},
	} {
		name := fmt.Sprintf("%T", list)
		if err := c.List(t.Context(), list); err != nil {
			t.Errorf("list %s: %v", name, err)
		}
		w, err := c.Watch(t.Context(), list)
		if err != nil {
			t.Errorf("watch %s: %v", name, err)
			continue
		}
		w.Stop()
	}
}
