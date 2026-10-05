/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kvv1 "github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/api/v1alpha1"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/kvadmin"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/planner"
	"github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/internal/render"
)

// AdminAPI is the nodes' admin API; *kvadmin.Client implements it.
type AdminAPI interface {
	GetMembership(ctx context.Context, kv *kvv1.KVCluster, ordinal int) (*kvadmin.Membership, error)
	SetMembership(ctx context.Context, kv *kvv1.KVCluster, ordinal int, req kvadmin.SetMembershipRequest) (*kvadmin.SetMembershipResult, error)
	Handoff(ctx context.Context, kv *kvv1.KVCluster, ordinal int, epoch uint64) (*kvadmin.Handoff, bool, error)
}

// KVClusterReconciler reconciles a KVCluster. Every decision is the
// planner's: a reconcile observes the cluster, asks planner.Plan for the one
// action to take, takes it, and writes the status Plan returns.
type KVClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Admin talks to the nodes.
	Admin AdminAPI
	// Now is the clock; time.Now when nil.
	Now func() time.Time
	// Planner holds the planner's timings.
	Planner planner.Config
	// Parallelism bounds the concurrent admin requests; 8 when zero.
	Parallelism int

	// unreachable is when each pod's admin API stopped answering, by pod
	// UID. It lives in memory only: an operator restart forgets it, which
	// can only delay a Degraded verdict.
	mu          sync.Mutex
	unreachable map[types.UID]time.Time
}

// +kubebuilder:rbac:groups=kvstore.dewidar.dev,resources=kvclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=kvstore.dewidar.dev,resources=kvclusters/status,verbs=patch
// +kubebuilder:rbac:groups="",resources=configmaps;services,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups="",resources=pods;persistentvolumeclaims,verbs=get;list;watch

// Reconcile observes the cluster, executes the planner's one action, and
// writes the status.
func (r *KVClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	kv := &kvv1.KVCluster{}
	if err := r.Get(ctx, req.NamespacedName, kv); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !kv.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil // the owned objects go with it
	}

	// The Services are fixed by the cluster's name; keeping them as
	// rendered is not a decision.
	for _, svc := range []*corev1.Service{render.HeadlessService(kv), render.ClientService(kv)} {
		if err := r.apply(ctx, svc); err != nil {
			return ctrl.Result{}, err
		}
	}

	obs, err := r.observe(ctx, kv)
	if err != nil {
		return ctrl.Result{}, err
	}
	d := planner.Plan(obs.Observation, r.Planner)
	log.V(1).Info("Planned", "action", d.Action.String(), "phase", d.Status.Phase, "reason", d.Status.Reason)

	actErr := r.execute(ctx, kv, obs.sts, d.Action)
	if actErr != nil {
		log.Error(actErr, "Could not execute action", "action", d.Action.String())
	}
	if err := r.writeStatus(ctx, kv, d.Status); err != nil {
		return ctrl.Result{}, err
	}
	if actErr != nil && !kvadmin.IsStatus(actErr, 409) {
		return ctrl.Result{}, actErr
	}

	after := d.Action.RequeueAfter
	if d.Action.Kind != planner.ActWait && d.Action.Kind != planner.ActNone {
		// Took a step: look again soon (a POST changes nothing the
		// watches see).
		after = r.Planner.Fast
	}
	return ctrl.Result{RequeueAfter: after}, nil
}

func (r *KVClusterReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// FieldOwner is the field manager of every object the operator applies.
const FieldOwner = "kvstore-operator"

// apply server-side applies a rendered object as FieldOwner.
func (r *KVClusterReconciler) apply(ctx context.Context, obj client.Object) error {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return err
	}
	u := &unstructured.Unstructured{Object: m}
	unstructured.RemoveNestedField(u.Object, "status")
	unstructured.RemoveNestedField(u.Object, "metadata", "creationTimestamp")
	if err := r.Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(FieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply %s %s: %w", u.GetKind(), u.GetName(), err)
	}
	return nil
}

// execute carries out exactly one planner action.
func (r *KVClusterReconciler) execute(ctx context.Context, kv *kvv1.KVCluster, sts *appsv1.StatefulSet, a planner.Action) error {
	switch a.Kind {
	case planner.ActNone, planner.ActWait:
		return nil
	case planner.ActWriteConfigMap:
		cm, err := render.ConfigMap(kv, a.Epoch, a.Members)
		if err != nil {
			return err
		}
		return r.apply(ctx, cm)
	case planner.ActScaleStatefulSet:
		// Replicas from the planner; the pod template as it is now (a
		// template change is its own action), or the spec's for a new
		// StatefulSet.
		return r.apply(ctx, render.StatefulSet(templateSource(kv, sts), a.Replicas))
	case planner.ActUpdateTemplate:
		return r.apply(ctx, render.StatefulSet(kv, a.Replicas))
	case planner.ActPostMembership:
		_, err := r.Admin.SetMembership(ctx, kv, a.Ordinal, kvadmin.SetMembershipRequest{Epoch: a.Epoch, Members: a.Members})
		return err
	}
	return fmt.Errorf("unknown action %v", a)
}

// templateSource is kv with the image and resources sts runs now (kv
// itself when there is no StatefulSet yet), so that rendering it changes
// replicas and nothing else.
func templateSource(kv *kvv1.KVCluster, sts *appsv1.StatefulSet) *kvv1.KVCluster {
	if sts == nil {
		return kv
	}
	src := kv.DeepCopy()
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name == "kv" {
			src.Spec.Image = c.Image
			src.Spec.Resources = *c.Resources.DeepCopy()
		}
	}
	return src
}

// SetupWithManager watches the KVCluster, the objects it owns, and its pods
// (by the instance label).
func (r *KVClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		// Status writes do not trigger a reconcile; spec changes do.
		For(&kvv1.KVCluster{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(podToCluster)).
		Named("kvcluster").
		Complete(r)
}

// podToCluster maps a pod to its KVCluster by the instance label the
// StatefulSet's pod template carries.
func podToCluster(_ context.Context, obj client.Object) []reconcile.Request {
	l := obj.GetLabels()
	if l[render.LabelManagedBy] != render.ManagerName || l[render.LabelInstance] == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: l[render.LabelInstance]}}}
}
