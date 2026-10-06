package controller

import (
	"context"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kvv1 "github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/api/v1alpha1"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/kvadmin"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/planner"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/render"
)

// observed is what a reconcile read: the planner's observation, and the
// StatefulSet itself (whose pod template a scale keeps).
type observed struct {
	planner.Observation
	sts *appsv1.StatefulSet
}

// observe reads everything the planner decides from.
func (r *KVClusterReconciler) observe(ctx context.Context, kv *kvv1.KVCluster) (*observed, error) {
	o := &observed{Observation: planner.Observation{Now: r.now(), Cluster: kv}}
	ns := kv.Namespace

	sts := &appsv1.StatefulSet{}
	switch err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: render.StatefulSetName(kv)}, sts); {
	case err == nil:
		o.sts = sts
		o.StatefulSet = statefulSetObs(kv, sts)
	case !apierrors.IsNotFound(err):
		return nil, err
	}

	cm := &corev1.ConfigMap{}
	switch err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: render.ConfigMapName(kv)}, cm); {
	case err == nil:
		epoch, members, perr := render.ParseConfigMap(cm)
		o.ConfigMap = &planner.ConfigMapObs{Epoch: epoch, Members: members}
		if perr != nil {
			o.ConfigMap.Invalid = perr.Error()
		}
	case !apierrors.IsNotFound(err):
		return nil, err
	}

	var pvcs corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcs, client.InNamespace(ns)); err != nil {
		return nil, err
	}
	for _, pvc := range pvcs.Items {
		// The StatefulSet names its claims data-<pod>.
		if id, ok := strings.CutPrefix(pvc.Name, "data-"); ok {
			if i, ok := render.Ordinal(kv, id); ok {
				o.PVCs = append(o.PVCs, i)
			}
		}
	}

	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(ns), client.MatchingLabels(render.SelectorLabels())); err != nil {
		return nil, err
	}
	byOrdinal := map[int]*corev1.Pod{}
	n := 0
	if o.StatefulSet != nil {
		n = int(o.StatefulSet.Replicas)
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if ord, ok := render.Ordinal(kv, p.Name); ok {
			byOrdinal[ord] = p
			n = max(n, ord+1)
		}
	}
	o.Pods = make([]planner.PodObs, n)
	for i := range n {
		o.Pods[i] = podObs(i, byOrdinal[i])
	}
	r.askNodes(ctx, kv, o, byOrdinal)
	return o, nil
}

func statefulSetObs(kv *kvv1.KVCluster, sts *appsv1.StatefulSet) *planner.StatefulSetObs {
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	st := sts.Status
	rolledOut := st.ObservedGeneration >= sts.Generation &&
		st.UpdatedReplicas == replicas &&
		(st.UpdateRevision == "" || st.CurrentRevision == st.UpdateRevision)
	return &planner.StatefulSetObs{
		Replicas:        replicas,
		ReadyReplicas:   st.ReadyReplicas,
		RolledOut:       rolledOut,
		TemplateCurrent: templateCurrent(kv, sts),
	}
}

// templateCurrent: the pods are placed as spec.placement renders, and the
// node container runs the spec's image and resources: the template fields
// the spec controls. (The rest of the template is fixed by the renderer, and
// comparing it whole would trip over the API server's defaults.)
func templateCurrent(kv *kvv1.KVCluster, sts *appsv1.StatefulSet) bool {
	want := render.StatefulSet(kv, 0).Spec.Template.Spec
	if !equality.Semantic.DeepEqual(placementOf(&sts.Spec.Template.Spec), placementOf(&want)) {
		return false
	}
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name == "kv" {
			return c.Image == kv.Spec.Image && equality.Semantic.DeepEqual(c.Resources, kv.Spec.Resources)
		}
	}
	return false
}

// placementOf is the part of a pod spec that spec.placement controls, with
// an empty node selector or list as nil (the API server drops them).
func placementOf(p *corev1.PodSpec) corev1.PodSpec {
	out := corev1.PodSpec{Affinity: p.Affinity}
	if len(p.NodeSelector) > 0 {
		out.NodeSelector = p.NodeSelector
	}
	if len(p.Tolerations) > 0 {
		out.Tolerations = p.Tolerations
	}
	if len(p.TopologySpreadConstraints) > 0 {
		out.TopologySpreadConstraints = p.TopologySpreadConstraints
	}
	return *out.DeepCopy()
}

// setPlacement sets the fields placementOf reads.
func setPlacement(dst *corev1.PodSpec, from corev1.PodSpec) {
	dst.Affinity = from.Affinity
	dst.NodeSelector = from.NodeSelector
	dst.Tolerations = from.Tolerations
	dst.TopologySpreadConstraints = from.TopologySpreadConstraints
}

func podObs(ordinal int, p *corev1.Pod) planner.PodObs {
	o := planner.PodObs{Ordinal: ordinal}
	if p == nil {
		return o
	}
	o.Exists = true
	o.Terminating = p.DeletionTimestamp != nil
	o.Since = p.CreationTimestamp.Time
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			o.Ready = c.Status == corev1.ConditionTrue
			if !c.LastTransitionTime.IsZero() {
				o.Since = c.LastTransitionTime.Time
			}
		}
	}
	return o
}

// askNodes calls GET /admin/membership on every pod, at most Parallelism at
// a time, and GET /admin/handoff on each pod outside its own membership. It
// tracks how long each pod has not answered.
func (r *KVClusterReconciler) askNodes(ctx context.Context, kv *kvv1.KVCluster, o *observed, pods map[int]*corev1.Pod) {
	limit := r.Parallelism
	if limit <= 0 {
		limit = 8
	}
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	for ord := range pods {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			m, err := r.Admin.GetMembership(ctx, kv, ord)
			if err != nil {
				return
			}
			o.Pods[ord].Membership = m
			if _, member := m.Members[render.MemberID(kv, ord)]; !member {
				if h, _, err := r.Admin.Handoff(ctx, kv, ord, m.Epoch); h != nil && (err == nil || kvadmin.IsStatus(err, 503)) {
					o.Pods[ord].Drain = h
				}
			}
		})
	}
	wg.Wait()
	r.trackUnreachable(o, pods)
}

// trackUnreachable records when each pod's admin API stopped answering and
// fills in UnreachableSince. Entries for pods that are gone are dropped.
func (r *KVClusterReconciler) trackUnreachable(o *observed, pods map[int]*corev1.Pod) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.unreachable == nil {
		r.unreachable = map[types.UID]time.Time{}
	}
	seen := map[types.UID]bool{}
	for ord, p := range pods {
		seen[p.UID] = true
		if o.Pods[ord].Membership != nil {
			delete(r.unreachable, p.UID)
			continue
		}
		since, ok := r.unreachable[p.UID]
		if !ok {
			since = o.Now
			r.unreachable[p.UID] = since
		}
		o.Pods[ord].UnreachableSince = since
	}
	for uid := range r.unreachable {
		if !seen[uid] {
			delete(r.unreachable, uid)
		}
	}
}
