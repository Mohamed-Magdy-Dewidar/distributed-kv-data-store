package controller

import (
	"context"
	"errors"
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
	// ConditionActionFailed: the last reconcile could not carry out what it
	// attempted (an apply, or a POST to a node): reason ApplyFailed, the
	// error as the message. False once a reconcile carries out everything.
	// A POST answered 409 is not a failure: the node has moved on, and the
	// next observation shows where.
	ConditionActionFailed = "ActionFailed"

	// ActionFailed's reasons: ApplyFailed while True, ActionSucceeded
	// while False.
	ReasonApplyFailed     = "ApplyFailed"
	ReasonActionSucceeded = "ActionSucceeded"
)

// actionFailed is the ActionFailed condition for the last reconcile's
// error (nil: none).
func actionFailed(kv *kvv1.KVCluster, err error) metav1.Condition {
	c := metav1.Condition{
		Type:               ConditionActionFailed,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonActionSucceeded,
		Message:            "The last reconcile carried out everything it attempted",
		ObservedGeneration: kv.Generation,
	}
	if err != nil {
		c.Status, c.Reason, c.Message = metav1.ConditionTrue, ReasonApplyFailed, err.Error()
	}
	return c
}

// recordFailure records err, an apply that failed before the planner ran,
// as the ActionFailed condition, leaving the rest of the status as it was,
// and returns err.
func (r *KVClusterReconciler) recordFailure(ctx context.Context, kv *kvv1.KVCluster, err error) error {
	orig := kv.DeepCopy()
	meta.SetStatusCondition(&kv.Status.Conditions, actionFailed(kv, err))
	if perr := r.Status().Patch(ctx, kv, client.MergeFrom(orig)); perr != nil {
		return errors.Join(err, perr)
	}
	return err
}

// writeStatus writes the planner's status, and actErr (nil: none) as the
// ActionFailed condition, through the status subresource, on every
// reconcile, waits included.
func (r *KVClusterReconciler) writeStatus(ctx context.Context, kv *kvv1.KVCluster, st planner.Status, actErr error) error {
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
	meta.SetStatusCondition(&s.Conditions, actionFailed(kv, actErr))
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
