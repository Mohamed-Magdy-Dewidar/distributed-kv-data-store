// Package render builds the objects a KVCluster owns: the node ConfigMap, the
// headless and client Services, the StatefulSet and the PodDisruptionBudget.
// For a cluster named kv in namespace kvstore at its initial membership,
// without spec.placement, the first four are the objects in deploy/k8s/ (see
// the golden test), plus the operator's labels and owner reference;
// deploy/k8s/ has no PodDisruptionBudget.
//
// Every member address is built by MemberAddress, from PodHost. The nodes
// compare addresses as strings, so the ConfigMap and every POST
// /admin/membership must carry exactly these.
package render

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	kvv1 "github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/api/v1alpha1"
)

const (
	// GRPCPort serves clients and other nodes (listen.grpc).
	GRPCPort = 7000
	// HTTPPort serves /livez, /readyz, /metrics and the admin API
	// (listen.http).
	HTTPPort = 8080

	// ConfigKey is the ConfigMap key the pods mount at /etc/kv, the path
	// KV_CONFIG names in the node image.
	ConfigKey = "config.yaml"

	// ManagerName is the managed-by label's value.
	ManagerName = "kvstore-operator"

	// LabelInstance names the KVCluster an object belongs to;
	// LabelManagedBy is ManagerName on every object the operator renders.
	LabelInstance  = "app.kubernetes.io/instance"
	LabelManagedBy = "app.kubernetes.io/managed-by"
)

// MemberID is the node ID of the pod with this ordinal: its pod name.
func MemberID(c *kvv1.KVCluster, ordinal int) string {
	return fmt.Sprintf("%s-%d", c.Name, ordinal)
}

// PodHost is the stable DNS name the headless Service gives the pod with
// this ordinal.
func PodHost(c *kvv1.KVCluster, ordinal int) string {
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local", MemberID(c, ordinal), c.Name, c.Namespace)
}

// MemberAddress is the gRPC address of the pod with this ordinal, as it
// appears in the membership.
func MemberAddress(c *kvv1.KVCluster, ordinal int) string {
	return fmt.Sprintf("%s:%d", PodHost(c, ordinal), GRPCPort)
}

// Members is the membership of ordinals 0..count-1 (ID → address), the
// shape POST /admin/membership takes.
func Members(c *kvv1.KVCluster, count int) map[string]string {
	m := make(map[string]string, count)
	for i := range count {
		m[MemberID(c, i)] = MemberAddress(c, i)
	}
	return m
}

// Ordinal returns the ordinal of the member ID <name>-<i>, or false for an
// ID that is not one of this cluster's pods.
func Ordinal(c *kvv1.KVCluster, id string) (int, bool) {
	s, ok := strings.CutPrefix(id, c.Name+"-")
	if !ok {
		return 0, false
	}
	i, err := strconv.Atoi(s)
	if err != nil || i < 0 || strconv.Itoa(i) != s {
		return 0, false
	}
	return i, true
}

// selectorLabels select the cluster's pods. They are deploy/k8s/'s labels,
// so one KVCluster per namespace.
func selectorLabels() map[string]string {
	return map[string]string{"app.kubernetes.io/name": "kvstore"}
}

// SelectorLabels select a cluster's pods (the StatefulSet's and the
// Services' selector).
func SelectorLabels() map[string]string { return selectorLabels() }

// operatorLabels mark an object as this cluster's and the operator's.
func operatorLabels(c *kvv1.KVCluster) map[string]string {
	return map[string]string{
		LabelInstance:  c.Name,
		LabelManagedBy: ManagerName,
	}
}

// labels are the selector labels plus the operator's.
func labels(c *kvv1.KVCluster) map[string]string {
	l := selectorLabels()
	maps.Copy(l, operatorLabels(c))
	return l
}

func objectMeta(c *kvv1.KVCluster, name string, l map[string]string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:            name,
		Namespace:       c.Namespace,
		Labels:          l,
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(c, kvv1.GroupVersion.WithKind("KVCluster"))},
	}
}

// ConfigMapName, ServiceName (headless) and ClientServiceName name the
// owned objects; the StatefulSet shares the headless Service's name.
func ConfigMapName(c *kvv1.KVCluster) string     { return c.Name + "-config" }
func ServiceName(c *kvv1.KVCluster) string       { return c.Name }
func ClientServiceName(c *kvv1.KVCluster) string { return c.Name + "-client" }
func StatefulSetName(c *kvv1.KVCluster) string   { return c.Name }

// PodDisruptionBudgetName names the PodDisruptionBudget.
func PodDisruptionBudgetName(c *kvv1.KVCluster) string { return c.Name }

// ConfigMap renders the node config every pod reads at startup, for the
// membership (epoch, members) — the one the cluster holds, or the one a new
// node is to start with — not for spec.replicas. Every member must be one of
// this cluster's pods at exactly MemberAddress, and there must be at least N
// of them; anything else is an error, since the nodes would refuse it or
// disagree with it.
func ConfigMap(c *kvv1.KVCluster, epoch uint64, members map[string]string) (*corev1.ConfigMap, error) {
	n := int(c.Spec.Replication.N)
	if len(members) < n {
		return nil, fmt.Errorf("membership has %d members, fewer than n=%d", len(members), n)
	}
	ordinals := make([]int, 0, len(members))
	for id, addr := range members {
		i, ok := Ordinal(c, id)
		if !ok {
			return nil, fmt.Errorf("member %q is not a pod of cluster %s", id, c.Name)
		}
		if want := MemberAddress(c, i); addr != want {
			return nil, fmt.Errorf("member %s has address %q, want %q", id, addr, want)
		}
		ordinals = append(ordinals, i)
	}
	slices.Sort(ordinals)

	var b strings.Builder
	fmt.Fprintf(&b, "listen:\n  grpc: \":%d\"\n  http: \":%d\"\n", GRPCPort, HTTPPort)
	b.WriteString("dataDir: /data\n")
	fmt.Fprintf(&b, "cluster:\n  n: %d\n  w: %d\n  r: %d\n  epoch: %d\n  members:\n",
		c.Spec.Replication.N, c.Spec.Replication.W, c.Spec.Replication.R, epoch)
	for _, i := range ordinals {
		fmt.Fprintf(&b, "    - id: %s\n      address: %s\n", MemberID(c, i), MemberAddress(c, i))
	}
	// Today's deploy/k8s/configmap.yaml values; see docs/kubernetes.md for
	// the anti-entropy interval.
	b.WriteString(`storage:
  memtableBytes: 4194304
intervals:
  compaction: 30s
  antiEntropy: 15m
  hintDelivery: 10s
  heartbeat: 1s
timeouts:
  replication: 5s
  maxReconnectBackoff: 5s
  shutdown: 20s
  heartbeat: 500ms
health:
  maxMissedHeartbeats: 3
`)

	return &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: objectMeta(c, ConfigMapName(c), operatorLabels(c)),
		Data:       map[string]string{ConfigKey: b.String()},
	}, nil
}

func grpcServicePort() corev1.ServicePort {
	return corev1.ServicePort{Name: "grpc", Port: GRPCPort, TargetPort: intstr.FromString("grpc")}
}

// HeadlessService gives each pod its DNS name (PodHost). It publishes
// not-ready addresses, so a pod's name resolves while it starts and while it
// drains after removal.
func HeadlessService(c *kvv1.KVCluster) *corev1.Service {
	return &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: objectMeta(c, ServiceName(c), labels(c)),
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 selectorLabels(),
			Ports: []corev1.ServicePort{
				grpcServicePort(),
				{Name: "http", Port: HTTPPort, TargetPort: intstr.FromString("http")},
			},
		},
	}
}

// ClientService is the client entry point: gRPC only, to ready pods. The
// unauthenticated admin port is deliberately not exposed.
func ClientService(c *kvv1.KVCluster) *corev1.Service {
	return &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: objectMeta(c, ClientServiceName(c), labels(c)),
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selectorLabels(),
			Ports:    []corev1.ServicePort{grpcServicePort()},
		},
	}
}

// PodDisruptionBudget lets an eviction (a node drain, a node upgrade) take
// at most one pod at a time: with N=3 and W=R=2, every key keeps a quorum.
// Evictions are refused while any pod is unready. It does not limit the
// StatefulSet's own deletions (a scale-down, a rolling update), which do
// not go through the eviction API.
func PodDisruptionBudget(c *kvv1.KVCluster) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		TypeMeta:   metav1.TypeMeta{APIVersion: "policy/v1", Kind: "PodDisruptionBudget"},
		ObjectMeta: objectMeta(c, PodDisruptionBudgetName(c), labels(c)),
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: new(intstr.FromInt32(1)),
			Selector:       &metav1.LabelSelector{MatchLabels: selectorLabels()},
		},
	}
}

// affinity is spec.placement.onePodPerNode: no two of the cluster's pods
// on one node.
func affinity(p *kvv1.Placement) *corev1.Affinity {
	if p == nil || !p.OnePodPerNode {
		return nil
	}
	return &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			LabelSelector: &metav1.LabelSelector{MatchLabels: selectorLabels()},
			TopologyKey:   corev1.LabelHostname,
		}},
	}}
}

// topologySpread is spec.placement.zoneSpread: the zones' pod counts differ
// by at most one, or the pod waits. The scheduler never chooses a node
// without the zone label for these pods.
func topologySpread(p *kvv1.Placement) []corev1.TopologySpreadConstraint {
	if p == nil || !p.ZoneSpread {
		return nil
	}
	return []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       corev1.LabelTopologyZone,
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector:     &metav1.LabelSelector{MatchLabels: selectorLabels()},
	}}
}

func nodeSelector(p *kvv1.Placement) map[string]string {
	if p == nil {
		return nil
	}
	return maps.Clone(p.NodeSelector)
}

func tolerations(p *kvv1.Placement) []corev1.Toleration {
	if p == nil || p.Tolerations == nil {
		return nil
	}
	out := make([]corev1.Toleration, len(p.Tolerations))
	for i := range p.Tolerations {
		p.Tolerations[i].DeepCopyInto(&out[i])
	}
	return out
}

// StatefulSet renders the node StatefulSet with the given number of pods.
// The planner chooses replicas (one node per membership change); the rest
// comes from the spec. The reasons behind each setting are in
// deploy/k8s/statefulset.yaml's comments.
func StatefulSet(c *kvv1.KVCluster, replicas int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"},
		ObjectMeta: objectMeta(c, StatefulSetName(c), labels(c)),
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         ServiceName(c),
			Replicas:            new(replicas),
			PodManagementPolicy: appsv1.ParallelPodManagement,
			// A pod scaled away has drained and left the membership; its
			// data dir must never come back (a stale incarnation).
			PersistentVolumeClaimRetentionPolicy: &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
				WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
				WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType,
			},
			MinReadySeconds: 5,
			Selector:        &metav1.LabelSelector{MatchLabels: selectorLabels()},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels(c)},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: new(int64(60)),
					Affinity:                      affinity(c.Spec.Placement),
					TopologySpreadConstraints:     topologySpread(c.Spec.Placement),
					NodeSelector:                  nodeSelector(c.Spec.Placement),
					Tolerations:                   tolerations(c.Spec.Placement),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:        new(true),
						RunAsUser:           new(int64(65532)),
						RunAsGroup:          new(int64(65532)),
						FSGroup:             new(int64(65532)),
						FSGroupChangePolicy: new(corev1.FSGroupChangeOnRootMismatch),
						SeccompProfile:      &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name:            "kv",
						Image:           c.Spec.Image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Env: []corev1.EnvVar{{
							Name:      "KV_NODE_ID",
							ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}},
						}},
						Lifecycle: &corev1.Lifecycle{
							PreStop: &corev1.LifecycleHandler{Sleep: &corev1.SleepAction{Seconds: 5}},
						},
						Ports: []corev1.ContainerPort{
							{Name: "grpc", ContainerPort: GRPCPort},
							{Name: "http", ContainerPort: HTTPPort},
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/livez", Port: intstr.FromString("http")}},
							PeriodSeconds:    10,
							FailureThreshold: 3,
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler:     corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/readyz", Port: intstr.FromString("http")}},
							PeriodSeconds:    5,
							FailureThreshold: 2,
						},
						Resources: *c.Spec.Resources.DeepCopy(),
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: new(false),
							ReadOnlyRootFilesystem:   new(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "config", MountPath: "/etc/kv", ReadOnly: true},
							{Name: "data", MountPath: "/data"},
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "config",
						VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: ConfigMapName(c)},
						}},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: c.Spec.Storage.StorageClassName,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: c.Spec.Storage.Size},
					},
				},
			}},
		},
	}
}
