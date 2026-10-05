package controller

import (
	"context"
	"maps"
	"slices"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kvv1 "github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/api/v1alpha1"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/planner"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/render"
)

// Condition types.
const (
	// ConditionAvailable: the cluster serves (Ready, Scaling or
	// DrainBlocked); False while Pending or Degraded.
	ConditionAvailable = "Available"
	// ConditionProgressing: a change is under way (Pending or Scaling);
	// False when Ready, or stuck (DrainBlocked, Degraded).
	ConditionProgressing = "Progressing"
	// ConditionDrainBlocked: a node being removed has not drained within
	// drainTimeout.
	ConditionDrainBlocked = "DrainBlocked"
	// ConditionDegraded: the cluster is in a state the operator will not act
	// on.
	ConditionDegraded = "Degraded"
)

// writeStatus writes the planner's status through the status subresource,
// on every reconcile, waits included.
func (r *KVClusterReconciler) writeStatus(ctx context.Context, kv *kvv1.KVCluster, st planner.Status) error {
	orig := kv.DeepCopy()
	s := &kv.Status
	s.ObservedGeneration = kv.Generation
	s.Phase = st.Phase
	s.Epoch = int64(st.Epoch)
	s.Members = members(kv, st.Members)
	s.Replicas = st.Replicas
	s.ReadyReplicas = st.ReadyReplicas
	s.Selector = labels.SelectorFromSet(render.SelectorLabels()).String()
	s.DrainStartedAt = nil
	if st.DrainStartedAt != nil {
		s.DrainStartedAt = &metav1.Time{Time: *st.DrainStartedAt}
	}

	is := func(phases ...kvv1.Phase) metav1.ConditionStatus {
		if slices.Contains(phases, st.Phase) {
			return metav1.ConditionTrue
		}
		return metav1.ConditionFalse
	}
	for _, c := range []struct {
		typ    string
		status metav1.ConditionStatus
	}{
		{ConditionAvailable, is(kvv1.PhaseReady, kvv1.PhaseScaling, kvv1.PhaseDrainBlocked)},
		{ConditionProgressing, is(kvv1.PhasePending, kvv1.PhaseScaling)},
		{ConditionDrainBlocked, is(kvv1.PhaseDrainBlocked)},
		{ConditionDegraded, is(kvv1.PhaseDegraded)},
	} {
		meta.SetStatusCondition(&s.Conditions, metav1.Condition{
			Type:               c.typ,
			Status:             c.status,
			Reason:             st.Reason,
			Message:            st.Message,
			ObservedGeneration: kv.Generation,
		})
	}
	return r.Status().Patch(ctx, kv, client.MergeFrom(orig))
}

// members is the status's member list, in ordinal order.
func members(kv *kvv1.KVCluster, m map[string]string) []kvv1.Member {
	ids := slices.SortedFunc(maps.Keys(m), func(a, b string) int {
		i, _ := render.Ordinal(kv, a)
		j, _ := render.Ordinal(kv, b)
		return i - j
	})
	out := make([]kvv1.Member, 0, len(ids))
	for _, id := range ids {
		out = append(out, kvv1.Member{ID: id, Address: m[id]})
	}
	return out
}
