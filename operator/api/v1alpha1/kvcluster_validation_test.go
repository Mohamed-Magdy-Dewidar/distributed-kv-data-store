package v1alpha1_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	kvv1 "github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/api/v1alpha1"
)

// These tests run the CRD's defaults and CEL rules in a real kube-apiserver
// (envtest). They need KUBEBUILDER_ASSETS (make test sets it) or the binaries
// under bin/k8s (make setup-envtest).

var k8s client.Client

func TestMain(m *testing.M) {
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		env.BinaryAssetsDirectory = firstDir(filepath.Join("..", "..", "bin", "k8s"))
	}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start envtest:", err)
		os.Exit(1)
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{kvv1.AddToScheme, autoscalingv1.AddToScheme} {
		if err := add(scheme); err != nil {
			panic(err)
		}
	}
	k8s, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		panic(err)
	}
	code := m.Run()
	if err := env.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stop envtest:", err)
	}
	os.Exit(code)
}

func firstDir(base string) string {
	entries, err := os.ReadDir(base)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if e.IsDir() {
			return filepath.Join(base, e.Name())
		}
	}
	return ""
}

// Substrings of the CEL rules' messages in kvcluster_types.go.
const (
	errReplicasBelowN       = "replicas must be at least replication.n"
	errW                    = "w must not exceed n"
	errR                    = "r must not exceed n"
	errReplicationImmutable = "replication is immutable"
	errStorageImmutable     = "storage is immutable"
	errDrainTimeout         = "drainTimeout must be a positive duration"
	errName                 = "name must be a DNS-1035 label"
	errTolerationOperator   = "toleration operator must be Equal or Exists"
	errTolerationEffect     = "toleration effect must be NoSchedule, PreferNoSchedule or NoExecute"
	errTolerationValue      = "toleration value must be empty when operator is Exists"
	errTolerationKey        = "toleration operator must be Exists when key is empty"
	errTolerationSeconds    = "toleration effect must be NoExecute when tolerationSeconds is set"
	errTolerationsMax       = "spec.placement.tolerations: Too many: 33: must have at most 32 items"
)

var nameSeq int

// newCluster returns a valid KVCluster with a fresh name, with only the
// required fields set.
func newCluster() *kvv1.KVCluster {
	nameSeq++
	return &kvv1.KVCluster{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("kv%d", nameSeq), Namespace: "default"},
		Spec:       kvv1.KVClusterSpec{Replicas: 10, Image: "kvnode:dev"},
	}
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

// wantInvalid fails unless err is a validation error whose message contains
// msg.
func wantInvalid(t *testing.T, err error, msg string) {
	t.Helper()
	if err == nil {
		t.Fatalf("accepted; want rejection containing %q", msg)
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("error is not Invalid: %v", err)
	}
	if !strings.Contains(err.Error(), msg) {
		t.Fatalf("rejection %q does not contain %q", err.Error(), msg)
	}
}

func TestDefaults(t *testing.T) {
	c := newCluster()
	if err := k8s.Create(ctx(t), c); err != nil {
		t.Fatal(err)
	}
	s := c.Spec // Create reads the defaulted object back
	if s.Replication != (kvv1.Replication{N: 3, W: 2, R: 2}) {
		t.Errorf("replication = %+v, want n=3 w=2 r=2", s.Replication)
	}
	if !s.Storage.Size.Equal(resource.MustParse("1Gi")) || s.Storage.StorageClassName != nil {
		t.Errorf("storage = %+v, want size 1Gi and no class", s.Storage)
	}
	if s.Placement != nil {
		t.Errorf("placement = %+v, want none: it is off unless set", s.Placement)
	}
	if s.DrainTimeout.Duration != 30*time.Minute {
		t.Errorf("drainTimeout = %v, want 30m", s.DrainTimeout.Duration)
	}
	wantRes := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")},
	}
	for _, l := range []struct {
		name      string
		got, want corev1.ResourceList
	}{{"requests", s.Resources.Requests, wantRes.Requests}, {"limits", s.Resources.Limits, wantRes.Limits}} {
		if len(l.got) != len(l.want) {
			t.Errorf("resources.%s = %v, want %v", l.name, l.got, l.want)
			continue
		}
		for k, v := range l.want {
			if g := l.got[k]; !g.Equal(v) {
				t.Errorf("resources.%s[%s] = %v, want %v", l.name, k, g.String(), v.String())
			}
		}
	}
}

func TestPartialReplicationIsDefaultedPerField(t *testing.T) {
	c := newCluster()
	c.Spec.Replication = kvv1.Replication{N: 5}
	if err := k8s.Create(ctx(t), c); err != nil {
		t.Fatal(err)
	}
	if c.Spec.Replication != (kvv1.Replication{N: 5, W: 2, R: 2}) {
		t.Errorf("replication = %+v, want n=5 w=2 r=2", c.Spec.Replication)
	}
}

func TestCreateValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*kvv1.KVCluster)
		reject string // "" means accepted
	}{
		{"replicas equal to n", func(c *kvv1.KVCluster) { c.Spec.Replicas = 3 }, ""},
		{"replicas below default n", func(c *kvv1.KVCluster) { c.Spec.Replicas = 2 }, errReplicasBelowN},
		{"replicas below explicit n", func(c *kvv1.KVCluster) {
			c.Spec.Replicas = 4
			c.Spec.Replication = kvv1.Replication{N: 5, W: 3, R: 3}
		}, errReplicasBelowN},
		{"replicas zero", func(c *kvv1.KVCluster) { c.Spec.Replicas = 0 }, "spec.replicas"},
		{"w equal to n", func(c *kvv1.KVCluster) { c.Spec.Replication = kvv1.Replication{N: 3, W: 3, R: 1} }, ""},
		{"w above n", func(c *kvv1.KVCluster) { c.Spec.Replication = kvv1.Replication{N: 3, W: 4, R: 2} }, errW},
		{"r above n", func(c *kvv1.KVCluster) { c.Spec.Replication = kvv1.Replication{N: 3, W: 2, R: 4} }, errR},
		{"w above defaulted n", func(c *kvv1.KVCluster) { c.Spec.Replication = kvv1.Replication{W: 4} }, errW},
		{"negative n", func(c *kvv1.KVCluster) { c.Spec.Replication = kvv1.Replication{N: -1, W: 1, R: 1} }, "spec.replication.n"},
		{"empty image", func(c *kvv1.KVCluster) { c.Spec.Image = "" }, "spec.image"},
		{"drainTimeout negative", func(c *kvv1.KVCluster) { c.Spec.DrainTimeout = metav1.Duration{Duration: -time.Minute} }, errDrainTimeout},
		{"name of 52 characters", func(c *kvv1.KVCluster) { c.Name = "k" + strings.Repeat("v", 51) }, ""},
		{"name of 53 characters", func(c *kvv1.KVCluster) { c.Name = "k" + strings.Repeat("v", 52) }, errName},
		{"name with a dot", func(c *kvv1.KVCluster) { c.Name = "kv.a" }, errName},
		{"name starting with a digit", func(c *kvv1.KVCluster) { c.Name = "1kv" }, errName},
		{"placement, everything set", func(c *kvv1.KVCluster) {
			c.Spec.Placement = &kvv1.Placement{
				OnePodPerNode: true, ZoneSpread: true,
				NodeSelector: map[string]string{"cloud.google.com/gke-nodepool": "kvstore"},
				Tolerations: []corev1.Toleration{
					{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "kvstore", Effect: corev1.TaintEffectNoSchedule},
					{Key: "dedicated", Value: "kvstore"}, // operator and effect left out: Equal, every effect
					{Operator: corev1.TolerationOpExists},
					{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: new(int64(30))},
				},
			}
		}, ""},
		{"toleration operator", tolerate(corev1.Toleration{Key: "k", Operator: "In", Value: "v"}), errTolerationOperator},
		{"toleration effect", tolerate(corev1.Toleration{Key: "k", Value: "v", Effect: "NoSchedul"}), errTolerationEffect},
		{"toleration Exists with a value", tolerate(corev1.Toleration{Key: "k", Operator: corev1.TolerationOpExists, Value: "v"}), errTolerationValue},
		{"toleration without key, Equal", tolerate(corev1.Toleration{Value: "v"}), errTolerationKey},
		{"32 tolerations", tolerations(32), ""},
		{"33 tolerations", tolerations(33), errTolerationsMax},
		{"tolerationSeconds without NoExecute", tolerate(corev1.Toleration{Key: "k", Value: "v", Effect: corev1.TaintEffectNoSchedule, TolerationSeconds: new(int64(30))}), errTolerationSeconds},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newCluster()
			tt.mutate(c)
			err := k8s.Create(ctx(t), c)
			if tt.reject == "" {
				if err != nil {
					t.Fatalf("rejected: %v", err)
				}
				return
			}
			wantInvalid(t, err, tt.reject)
		})
	}
}

// tolerate sets placement to the one toleration tl.
func tolerate(tl corev1.Toleration) func(*kvv1.KVCluster) {
	return func(c *kvv1.KVCluster) { c.Spec.Placement = &kvv1.Placement{Tolerations: []corev1.Toleration{tl}} }
}

// tolerations sets placement to n valid tolerations.
func tolerations(n int) func(*kvv1.KVCluster) {
	return func(c *kvv1.KVCluster) {
		c.Spec.Placement = &kvv1.Placement{}
		for i := range n {
			c.Spec.Placement.Tolerations = append(c.Spec.Placement.Tolerations, corev1.Toleration{Key: fmt.Sprintf("k%d", i), Operator: corev1.TolerationOpExists})
		}
	}
}

// TestDrainTimeoutStrings sends drainTimeout values the Go type cannot
// carry (a zero Duration is omitted and defaulted), as raw merge patches.
func TestDrainTimeoutStrings(t *testing.T) {
	for _, v := range []string{"0s", "-1m", "soon"} {
		t.Run(v, func(t *testing.T) {
			c := newCluster()
			if err := k8s.Create(ctx(t), c); err != nil {
				t.Fatal(err)
			}
			patch := client.RawPatch("application/merge-patch+json", fmt.Appendf(nil, `{"spec":{"drainTimeout":%q}}`, v))
			wantInvalid(t, k8s.Patch(ctx(t), c.DeepCopy(), patch), "drainTimeout")
		})
	}
}

func TestUpdateValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*kvv1.KVCluster)
		reject string
	}{
		{"replicas up", func(c *kvv1.KVCluster) { c.Spec.Replicas = 11 }, ""},
		{"replicas down to n", func(c *kvv1.KVCluster) { c.Spec.Replicas = 3 }, ""},
		{"replicas below n", func(c *kvv1.KVCluster) { c.Spec.Replicas = 2 }, errReplicasBelowN},
		{"image", func(c *kvv1.KVCluster) { c.Spec.Image = "kvnode:v2" }, ""},
		{"resources", func(c *kvv1.KVCluster) {
			c.Spec.Resources.Limits[corev1.ResourceMemory] = resource.MustParse("512Mi")
		}, ""},
		{"drainTimeout", func(c *kvv1.KVCluster) { c.Spec.DrainTimeout = metav1.Duration{Duration: time.Hour} }, ""},
		{"placement set", func(c *kvv1.KVCluster) { c.Spec.Placement = &kvv1.Placement{OnePodPerNode: true, ZoneSpread: true} }, ""},
		{"placement toleration invalid", tolerate(corev1.Toleration{Key: "k", Operator: corev1.TolerationOpExists, Value: "v"}), errTolerationValue},
		{"replication n", func(c *kvv1.KVCluster) { c.Spec.Replication.N = 5 }, errReplicationImmutable},
		{"replication w", func(c *kvv1.KVCluster) { c.Spec.Replication.W = 3 }, errReplicationImmutable},
		{"replication r", func(c *kvv1.KVCluster) { c.Spec.Replication.R = 1 }, errReplicationImmutable},
		{"storage size", func(c *kvv1.KVCluster) { c.Spec.Storage.Size = resource.MustParse("2Gi") }, errStorageImmutable},
		{"storage class", func(c *kvv1.KVCluster) { c.Spec.Storage.StorageClassName = new("fast") }, errStorageImmutable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newCluster()
			if err := k8s.Create(ctx(t), c); err != nil {
				t.Fatal(err)
			}
			tt.mutate(c)
			err := k8s.Update(ctx(t), c)
			if tt.reject == "" {
				if err != nil {
					t.Fatalf("rejected: %v", err)
				}
				return
			}
			wantInvalid(t, err, tt.reject)
		})
	}
}

// TestScaleSubresource checks that kubectl scale works and is held to the
// same replicas >= n rule.
func TestScaleSubresource(t *testing.T) {
	c := newCluster()
	if err := k8s.Create(ctx(t), c); err != nil {
		t.Fatal(err)
	}
	scale := func(n int32) error {
		patch := client.RawPatch("application/merge-patch+json", fmt.Appendf(nil, `{"spec":{"replicas":%d}}`, n))
		return k8s.SubResource("scale").Patch(ctx(t), c.DeepCopy(), patch,
			&client.SubResourcePatchOptions{SubResourceBody: &autoscalingv1.Scale{}})
	}
	if err := scale(11); err != nil {
		t.Fatalf("scale to 11: %v", err)
	}
	var got kvv1.KVCluster
	if err := k8s.Get(ctx(t), client.ObjectKeyFromObject(c), &got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Replicas != 11 {
		t.Fatalf("replicas after scale = %d, want 11", got.Spec.Replicas)
	}
	wantInvalid(t, scale(2), errReplicasBelowN)
}
