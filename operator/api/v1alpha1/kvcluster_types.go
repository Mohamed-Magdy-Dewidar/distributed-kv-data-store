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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// KVClusterSpec defines the desired state of KVCluster.
//
// The cluster's name is used as the StatefulSet's and the headless Service's
// name, so pod <name>-<i> is reachable at
// <name>-<i>.<name>.<namespace>.svc.cluster.local, the address the nodes use
// for each other. One KVCluster per namespace: the owned objects select their
// pods by app.kubernetes.io/name=kvstore alone, as deploy/k8s/ does.
//
// +kubebuilder:validation:XValidation:rule="!has(self.replication) || self.replicas >= self.replication.n",message="replicas must be at least replication.n: a membership needs at least N members"
type KVClusterSpec struct {
	// replicas is the number of nodes. The operator changes the membership
	// one node at a time towards it, waiting for each handoff to finish.
	// +kubebuilder:validation:Minimum=1
	// +required
	Replicas int32 `json:"replicas"`

	// replication is the cluster's N, W and R. It cannot change after
	// creation: N is part of every membership's fingerprint, and every node
	// must have the same N, W and R.
	// +kubebuilder:default={n:3,w:2,r:2}
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="replication is immutable"
	// +optional
	Replication Replication `json:"replication,omitzero"`

	// image is the node image, e.g. kvnode:dev.
	// +kubebuilder:validation:MinLength=1
	// +required
	Image string `json:"image"`

	// storage is each node's data volume. It cannot change after creation:
	// a StatefulSet's volumeClaimTemplates are immutable.
	// +kubebuilder:default={size:"1Gi"}
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="storage is immutable"
	// +optional
	Storage Storage `json:"storage,omitzero"`

	// resources are the node container's requests and limits. The default
	// comes from measurements at 10 replicas on kind (docs/kubernetes.md). It
	// applies only when resources is left out entirely.
	// +kubebuilder:default={requests:{cpu:"50m",memory:"64Mi"},limits:{memory:"256Mi"}}
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitzero"`

	// drainTimeout is how long a node being removed may take to drain
	// before the cluster reports DrainBlocked. The operator keeps waiting
	// after that; it never stops a node that has not drained.
	// +kubebuilder:default="30m"
	// +kubebuilder:validation:XValidation:rule="duration(self) > duration('0s')",message="drainTimeout must be a positive duration"
	// +optional
	DrainTimeout metav1.Duration `json:"drainTimeout,omitzero"`
}

// Replication is the cluster's replication factor and quorum sizes.
//
// +kubebuilder:validation:XValidation:rule="self.w <= self.n",message="w must not exceed n"
// +kubebuilder:validation:XValidation:rule="self.r <= self.n",message="r must not exceed n"
type Replication struct {
	// n is the number of replicas of each key.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=3
	// +optional
	N int32 `json:"n,omitempty"`

	// w is the number of replicas that must acknowledge a write.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2
	// +optional
	W int32 `json:"w,omitempty"`

	// r is the number of replicas a read waits for.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2
	// +optional
	R int32 `json:"r,omitempty"`
}

// Storage is each node's data volume.
type Storage struct {
	// size is the volume's requested capacity.
	// +kubebuilder:default="1Gi"
	// +optional
	Size resource.Quantity `json:"size,omitzero"`

	// storageClassName is the volume's storage class; the cluster's default
	// class when unset.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
}

// Phase summarizes where a KVCluster is.
// +kubebuilder:validation:Enum=Pending;Ready;Scaling;DrainBlocked;Degraded
type Phase string

const (
	// PhasePending: the cluster is being created and not every node is
	// ready yet.
	PhasePending Phase = "Pending"
	// PhaseReady: every node is ready, at one epoch, with its handoff done,
	// and replicas matches the spec.
	PhaseReady Phase = "Ready"
	// PhaseScaling: a node is being added or removed.
	PhaseScaling Phase = "Scaling"
	// PhaseDrainBlocked: a node being removed has not drained within
	// drainTimeout. The operator keeps waiting.
	PhaseDrainBlocked Phase = "DrainBlocked"
	// PhaseDegraded: the cluster is in a state the operator will not act
	// on, e.g. nodes at different epochs, a member address that differs
	// from the rendered one, or an unreachable node.
	PhaseDegraded Phase = "Degraded"
)

// Member is one member of the cluster's membership.
type Member struct {
	// id is the node ID, the pod's name.
	// +required
	ID string `json:"id"`
	// address is the node's gRPC address, exactly as the nodes hold it.
	// +required
	Address string `json:"address"`
}

// KVClusterStatus defines the observed state of KVCluster.
type KVClusterStatus struct {
	// observedGeneration is the spec generation this status describes.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// phase summarizes the cluster's state.
	// +optional
	Phase Phase `json:"phase,omitempty"`

	// epoch is the membership epoch the nodes hold.
	// +kubebuilder:validation:Minimum=0
	// +optional
	Epoch int64 `json:"epoch,omitempty"`

	// members is the membership the nodes hold, in ordinal order.
	// +listType=map
	// +listMapKey=id
	// +optional
	Members []Member `json:"members,omitempty"`

	// replicas is the number of pods the StatefulSet has (the scale
	// subresource's status replicas).
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// readyReplicas is the number of ready pods.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// selector is the pods' label selector, for the scale subresource.
	// +optional
	Selector string `json:"selector,omitempty"`

	// drainStartedAt is when the node now being removed was taken out of the
	// membership; drainTimeout counts from here.
	// +optional
	DrainStartedAt *metav1.Time `json:"drainStartedAt,omitempty"`

	// conditions represent the current state of the KVCluster resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// The name becomes pod names (<name>-<i>, each a DNS label), Service names
// (<name>, <name>-client: DNS-1035 labels) and the StatefulSet's
// controller-revision-hash label (<name>-<10 chars>, at most 63), hence a
// DNS-1035 label of at most 52 characters.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:resource:shortName=kvc
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Epoch",type=integer,JSONPath=`.status.epoch`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:validation:XValidation:rule="self.metadata.name.matches('^[a-z]([-a-z0-9]*[a-z0-9])?$') && size(self.metadata.name) <= 52",message="name must be a DNS-1035 label of at most 52 characters: it is used in pod, Service and DNS names"

// KVCluster is the Schema for the kvclusters API
type KVCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of KVCluster
	// +required
	Spec KVClusterSpec `json:"spec"`

	// status defines the observed state of KVCluster
	// +optional
	Status KVClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// KVClusterList contains a list of KVCluster
type KVClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []KVCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &KVCluster{}, &KVClusterList{})
		return nil
	})
}
