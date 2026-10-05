package render

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"

	yaml3 "go.yaml.in/yaml/v3"

	kvv1 "github.com/Mohamed-Magdy-Dewidar/distributed-kv-data-store/operator/api/v1alpha1"
)

// manualDir holds the hand-written manifests: the golden files.
var manualDir = filepath.Join("..", "..", "..", "deploy", "k8s")

// The deploy/k8s/ files the rendered objects are compared with.
const (
	fileConfigMap = "configmap.yaml"
	fileHeadless  = "service-headless.yaml"
	fileClient    = "service-client.yaml"
	fileSTS       = "statefulset.yaml"
)

var goldenFiles = []string{fileConfigMap, fileHeadless, fileClient, fileSTS}

// cluster returns a KVCluster as the API server stores it once defaulted
// (see the defaults in api/v1alpha1), with the given replicas.
func cluster(name, namespace string, replicas int32) *kvv1.KVCluster {
	return &kvv1.KVCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID("uid-" + name)},
		Spec: kvv1.KVClusterSpec{
			Replicas:    replicas,
			Replication: kvv1.Replication{N: 3, W: 2, R: 2},
			Image:       "kvnode:dev",
			Storage:     kvv1.Storage{Size: resource.MustParse("1Gi")},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
				Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")},
			},
			DrainTimeout: metav1.Duration{Duration: 30 * time.Minute},
		},
	}
}

// renderAll renders every owned object at the initial membership (epoch 0,
// ordinals 0..replicas-1), keyed by the deploy/k8s/ file it corresponds to.
func renderAll(t *testing.T, c *kvv1.KVCluster) map[string]any {
	t.Helper()
	cm, err := ConfigMap(c, 0, Members(c, int(c.Spec.Replicas)))
	if err != nil {
		t.Fatalf("ConfigMap: %v", err)
	}
	return map[string]any{
		fileConfigMap: cm,
		fileHeadless:  HeadlessService(c),
		fileClient:    ClientService(c),
		fileSTS:       StatefulSet(c, c.Spec.Replicas),
	}
}

func toMap(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := yaml.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := yaml.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// normalize removes what carries no meaning: null values, empty maps and
// lists (a Go zero value serialized, e.g. "updateStrategy: {}"), and the
// top-level status, which the operator never writes. It is applied to both
// sides.
func normalize(v any) any {
	switch v := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, e := range v {
			e = normalize(e)
			if e == nil {
				continue
			}
			if m, ok := e.(map[string]any); ok && len(m) == 0 {
				continue
			}
			if l, ok := e.([]any); ok && len(l) == 0 {
				continue
			}
			out[k] = e
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = normalize(e)
		}
		return out
	default:
		return v
	}
}

// diff lists every path where a and b differ.
func diff(path string, a, b any, out *[]string) {
	am, aok := a.(map[string]any)
	bm, bok := b.(map[string]any)
	if aok && bok {
		keys := map[string]bool{}
		for k := range am {
			keys[k] = true
		}
		for k := range bm {
			keys[k] = true
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		slices.Sort(sorted)
		for _, k := range sorted {
			diff(path+"."+k, am[k], bm[k], out)
		}
		return
	}
	al, alok := a.([]any)
	bl, blok := b.([]any)
	if alok && blok && len(al) == len(bl) {
		for i := range al {
			diff(fmt.Sprintf("%s[%d]", path, i), al[i], bl[i], out)
		}
		return
	}
	if !reflect.DeepEqual(a, b) {
		*out = append(*out, fmt.Sprintf("%s: deploy/k8s=%v rendered=%v", path, a, b))
	}
}

// intended is one difference the rendered objects are allowed to have from
// deploy/k8s/: a field the operator adds, at path, with exactly value.
type intended struct {
	file  string
	path  []string
	value any
}

// intendedDifferences is the complete list. Anything else that differs
// fails TestMatchesDeployK8s.
func intendedDifferences() []intended {
	ownerRef := []any{map[string]any{
		"apiVersion":         "kvstore.dewidar.dev/v1alpha1",
		"kind":               "KVCluster",
		"name":               "kv",
		"uid":                "uid-kv",
		"controller":         true,
		"blockOwnerDeletion": true,
	}}
	meta := func(keys ...string) []string { return append([]string{"metadata"}, keys...) }
	labelKey := func(key string) []string { return meta("labels", key) }
	out := make([]intended, 0, 3*len(goldenFiles)+2)
	for _, f := range goldenFiles {
		out = append(out,
			intended{f, meta("ownerReferences"), ownerRef},
			intended{f, labelKey("app.kubernetes.io/managed-by"), "kvstore-operator"},
			intended{f, labelKey("app.kubernetes.io/instance"), "kv"},
		)
	}
	tmpl := append([]string{"spec", "template"}, meta("labels")...)
	out = append(out,
		intended{fileSTS, append(slices.Clone(tmpl), "app.kubernetes.io/managed-by"), "kvstore-operator"},
		intended{fileSTS, append(slices.Clone(tmpl), "app.kubernetes.io/instance"), "kv"},
	)
	return out
}

// lookup returns the value at path and the map holding its last key.
func lookup(m map[string]any, path []string) (any, map[string]any, bool) {
	cur := m
	for _, k := range path[:len(path)-1] {
		next, ok := cur[k].(map[string]any)
		if !ok {
			return nil, nil, false
		}
		cur = next
	}
	v, ok := cur[path[len(path)-1]]
	return v, cur, ok
}

// TestMatchesDeployK8s renders the cluster deploy/k8s/ describes (kv in
// kvstore, 10 replicas, epoch 0, the defaults) and compares every object with
// its file. Each intended difference must be present with its exact value
// (and absent from the file); it is then removed, and what is left must be
// identical.
func TestMatchesDeployK8s(t *testing.T) {
	rendered := renderAll(t, cluster("kv", "kvstore", 10))
	byFile := map[string][]intended{}
	for _, d := range intendedDifferences() {
		byFile[d.file] = append(byFile[d.file], d)
	}

	for file, obj := range rendered {
		t.Run(file, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(manualDir, file))
			if err != nil {
				t.Fatal(err)
			}
			var manual map[string]any
			if err := yaml.Unmarshal(raw, &manual); err != nil {
				t.Fatal(err)
			}
			got := toMap(t, obj)

			for _, d := range byFile[file] {
				name := strings.Join(d.path, ".")
				if _, _, ok := lookup(manual, d.path); ok {
					t.Errorf("intended difference %s is already in deploy/k8s/%s; take it off the list", name, file)
				}
				v, parent, ok := lookup(got, d.path)
				if !ok {
					t.Errorf("intended difference %s is missing from the rendered object", name)
					continue
				}
				if !reflect.DeepEqual(v, d.value) {
					t.Errorf("%s = %v, want %v", name, v, d.value)
				}
				delete(parent, d.path[len(d.path)-1])
			}

			delete(got, "status")
			var diffs []string
			diff("", normalize(manual), normalize(got), &diffs)
			for _, l := range diffs {
				t.Errorf("unintended difference %s", l)
			}
		})
	}
}

// nodeConfig mirrors the node's config.Config (internal/config/config.go),
// field for field, so parseConfig decodes the rendered config.yaml as
// strictly as the node does: the same YAML library, unknown fields rejected.
// sigs.k8s.io/yaml would not do: as YAML 1.1 it reads the key "n" as false.
type nodeConfig struct {
	NodeID  string                      `yaml:"nodeId"`
	Listen  struct{ GRPC, HTTP string } `yaml:"listen"`
	DataDir string                      `yaml:"dataDir"`
	Cluster struct {
		N       int    `yaml:"n"`
		W       int    `yaml:"w"`
		R       int    `yaml:"r"`
		Epoch   uint64 `yaml:"epoch"`
		Members []struct {
			ID      string `yaml:"id"`
			Address string `yaml:"address"`
		} `yaml:"members"`
	} `yaml:"cluster"`
	Storage struct {
		MemtableBytes int `yaml:"memtableBytes"`
	} `yaml:"storage"`
	Intervals struct {
		Compaction   time.Duration `yaml:"compaction"`
		AntiEntropy  time.Duration `yaml:"antiEntropy"`
		HintDelivery time.Duration `yaml:"hintDelivery"`
		Heartbeat    time.Duration `yaml:"heartbeat"`
	} `yaml:"intervals"`
	Timeouts struct {
		Replication         time.Duration `yaml:"replication"`
		MaxReconnectBackoff time.Duration `yaml:"maxReconnectBackoff"`
		Shutdown            time.Duration `yaml:"shutdown"`
		Heartbeat           time.Duration `yaml:"heartbeat"`
	} `yaml:"timeouts"`
	Health struct {
		MaxMissedHeartbeats int `yaml:"maxMissedHeartbeats"`
	} `yaml:"health"`
}

func parseConfig(t *testing.T, cm *corev1.ConfigMap) nodeConfig {
	t.Helper()
	var cfg nodeConfig
	dec := yaml3.NewDecoder(strings.NewReader(cm.Data[ConfigKey]))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		t.Fatalf("config.yaml does not decode as the node decodes it: %v", err)
	}
	return cfg
}

// TestFollowsNameAndNamespace renders a cluster named db in namespace prod
// and checks that every name, address, label, selector and member string
// follows them, and that nothing from kv/kvstore is left anywhere.
func TestFollowsNameAndNamespace(t *testing.T) {
	const ns = "prod"
	c := cluster("db", ns, 5)
	objs := renderAll(t, c)
	cm := objs[fileConfigMap].(*corev1.ConfigMap)
	headless := objs[fileHeadless].(*corev1.Service)
	client := objs[fileClient].(*corev1.Service)
	sts := StatefulSet(c, 5)

	for i := range 5 {
		if got, want := MemberAddress(c, i), fmt.Sprintf("db-%d.db.prod.svc.cluster.local:7000", i); got != want {
			t.Errorf("MemberAddress(%d) = %q, want %q", i, got, want)
		}
	}
	cfg := parseConfig(t, cm)
	if len(cfg.Cluster.Members) != 5 {
		t.Fatalf("config lists %d members, want 5", len(cfg.Cluster.Members))
	}
	for i, m := range cfg.Cluster.Members {
		if m.ID != fmt.Sprintf("db-%d", i) || m.Address != fmt.Sprintf("db-%d.db.prod.svc.cluster.local:7000", i) {
			t.Errorf("config member %d = %s %s", i, m.ID, m.Address)
		}
	}

	names := map[string][2]string{
		"ConfigMap":       {cm.Name, "db-config"},
		"headless":        {headless.Name, "db"},
		"client":          {client.Name, "db-client"},
		"StatefulSet":     {sts.Name, "db"},
		"serviceName":     {sts.Spec.ServiceName, "db"},
		"config volume":   {sts.Spec.Template.Spec.Volumes[0].ConfigMap.Name, "db-config"},
		"ConfigMap ns":    {cm.Namespace, ns},
		"headless ns":     {headless.Namespace, ns},
		"client ns":       {client.Namespace, ns},
		"StatefulSet ns":  {sts.Namespace, ns},
		"instance label":  {sts.Labels["app.kubernetes.io/instance"], "db"},
		"pod instance":    {sts.Spec.Template.Labels["app.kubernetes.io/instance"], "db"},
		"ConfigMap label": {cm.Labels["app.kubernetes.io/instance"], "db"},
	}
	for what, g := range names {
		if g[0] != g[1] {
			t.Errorf("%s = %q, want %q", what, g[0], g[1])
		}
	}
	for _, o := range []metav1.Object{cm, headless, client, sts} {
		refs := o.GetOwnerReferences()
		if len(refs) != 1 || refs[0].Name != "db" || refs[0].UID != c.UID || refs[0].Controller == nil || !*refs[0].Controller {
			t.Errorf("%s owner references = %+v, want the controller reference to db", o.GetName(), refs)
		}
	}

	// The selectors must pick the pod template's pods.
	pod := sts.Spec.Template.Labels
	for what, sel := range map[string]map[string]string{
		"StatefulSet selector": sts.Spec.Selector.MatchLabels,
		"headless selector":    headless.Spec.Selector,
		"client selector":      client.Spec.Selector,
	} {
		if len(sel) == 0 {
			t.Errorf("%s is empty", what)
		}
		for k, v := range sel {
			if pod[k] != v {
				t.Errorf("%s %s=%s does not match the pod template (%q)", what, k, v, pod[k])
			}
		}
	}

	// Nothing of kv/kvstore may be left in any string, except the label
	// values that name the application and the operator.
	allowed := map[string]bool{"kvstore": true, ManagerName: true, "kvnode:dev": true}
	for file, o := range objs {
		m := toMap(t, o)
		if file == fileSTS { // the container's name is "kv" whatever the cluster's
			ct := m["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
			if ct["name"] != "kv" {
				t.Errorf("container name = %v, want kv", ct["name"])
			}
			delete(ct, "name")
		}
		walkStrings(m, func(s string) {
			if allowed[s] {
				return
			}
			if s == "kv" || strings.HasPrefix(s, "kv-") || strings.Contains(s, ".kv.") ||
				strings.Contains(s, "kvstore.svc") || strings.Contains(s, ".kvstore") || strings.Contains(s, "kv-") {
				t.Errorf("%s: %q still refers to kv/kvstore", file, s)
			}
		})
	}
}

func walkStrings(v any, f func(string)) {
	switch v := v.(type) {
	case map[string]any:
		for _, e := range v {
			walkStrings(e, f)
		}
	case []any:
		for _, e := range v {
			walkStrings(e, f)
		}
	case string:
		for line := range strings.SplitSeq(v, "\n") {
			f(strings.TrimSpace(line))
		}
	}
}

// TestConfigMapFollowsMembersNotReplicas renders the ConfigMap for a
// membership that differs from spec.replicas, both ways, and checks that it
// lists exactly the members passed in, in ordinal order (db-10 after db-9).
func TestConfigMapFollowsMembersNotReplicas(t *testing.T) {
	for _, tt := range []struct {
		name     string
		replicas int32
		members  int
		epoch    uint64
	}{
		{"scale up: 11 members, spec still 10", 10, 11, 4},
		{"scale down: 10 members, spec 9", 9, 10, 5},
		{"spec far ahead", 10, 3, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := cluster("db", "prod", tt.replicas)
			cm, err := ConfigMap(c, tt.epoch, Members(c, tt.members))
			if err != nil {
				t.Fatal(err)
			}
			cfg := parseConfig(t, cm)
			if cfg.Cluster.Epoch != tt.epoch {
				t.Errorf("epoch = %d, want %d", cfg.Cluster.Epoch, tt.epoch)
			}
			if len(cfg.Cluster.Members) != tt.members {
				t.Fatalf("config lists %d members, want %d", len(cfg.Cluster.Members), tt.members)
			}
			for i, m := range cfg.Cluster.Members {
				if want := MemberID(c, i); m.ID != want {
					t.Errorf("member %d is %s, want %s", i, m.ID, want)
				}
				if want := MemberAddress(c, i); m.Address != want {
					t.Errorf("member %s address %q, want %q", m.ID, m.Address, want)
				}
			}
			if cfg.Cluster.N != 3 || cfg.Cluster.W != 2 || cfg.Cluster.R != 2 {
				t.Errorf("n/w/r = %d/%d/%d, want 3/2/2", cfg.Cluster.N, cfg.Cluster.W, cfg.Cluster.R)
			}
		})
	}
}

// TestConfigMapMembershipWithGap renders a membership that is not 0..k-1
// (a member removed from the middle by hand): it lists exactly those.
func TestConfigMapMembershipWithGap(t *testing.T) {
	c := cluster("kv", "kvstore", 4)
	members := Members(c, 5)
	delete(members, "kv-2")
	cfg := parseConfig(t, mustConfigMap(t, c, 7, members))
	ids := make([]string, 0, len(cfg.Cluster.Members))
	for _, m := range cfg.Cluster.Members {
		ids = append(ids, m.ID)
	}
	if want := []string{"kv-0", "kv-1", "kv-3", "kv-4"}; !slices.Equal(ids, want) {
		t.Errorf("members = %v, want %v", ids, want)
	}
}

func mustConfigMap(t *testing.T, c *kvv1.KVCluster, epoch uint64, members map[string]string) *corev1.ConfigMap {
	t.Helper()
	cm, err := ConfigMap(c, epoch, members)
	if err != nil {
		t.Fatal(err)
	}
	return cm
}

func TestConfigMapRejects(t *testing.T) {
	c := cluster("kv", "kvstore", 10)
	for _, tt := range []struct {
		name    string
		members func() map[string]string
		want    string
	}{
		{"fewer than n members", func() map[string]string { return Members(c, 2) }, "fewer than n=3"},
		{"foreign member ID", func() map[string]string {
			m := Members(c, 3)
			m["other-3"] = "other-3.other.kvstore.svc.cluster.local:7000"
			return m
		}, `member "other-3" is not a pod`},
		{"ID with a leading zero", func() map[string]string {
			m := Members(c, 3)
			m["kv-03"] = MemberAddress(c, 3)
			return m
		}, `member "kv-03" is not a pod`},
		{"address without the cluster domain", func() map[string]string {
			m := Members(c, 3)
			m["kv-1"] = "kv-1.kv.kvstore:7000"
			return m
		}, "member kv-1 has address"},
		{"address in another namespace", func() map[string]string {
			m := Members(c, 3)
			m["kv-1"] = "kv-1.kv.prod.svc.cluster.local:7000"
			return m
		}, "member kv-1 has address"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ConfigMap(c, 1, tt.members())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want one containing %q", err, tt.want)
			}
		})
	}
}

// TestStatefulSetReplicasFromCaller: the StatefulSet's replicas is the one
// the planner passes, not spec.replicas.
func TestStatefulSetReplicasFromCaller(t *testing.T) {
	c := cluster("kv", "kvstore", 10)
	if got := *StatefulSet(c, 11).Spec.Replicas; got != 11 {
		t.Errorf("replicas = %d, want 11", got)
	}
}

func TestStatefulSetFollowsSpec(t *testing.T) {
	c := cluster("kv", "kvstore", 10)
	c.Spec.Image = "kvnode:v2"
	c.Spec.Storage = kvv1.Storage{Size: resource.MustParse("5Gi"), StorageClassName: new("fast")}
	c.Spec.Resources.Limits[corev1.ResourceMemory] = resource.MustParse("1Gi")
	sts := StatefulSet(c, 10)
	ct := sts.Spec.Template.Spec.Containers[0]
	if ct.Image != "kvnode:v2" {
		t.Errorf("image = %q", ct.Image)
	}
	if got := ct.Resources.Limits[corev1.ResourceMemory]; got.String() != "1Gi" {
		t.Errorf("memory limit = %s, want 1Gi", got.String())
	}
	pvc := sts.Spec.VolumeClaimTemplates[0].Spec
	if got := pvc.Resources.Requests[corev1.ResourceStorage]; got.String() != "5Gi" {
		t.Errorf("storage = %s, want 5Gi", got.String())
	}
	if pvc.StorageClassName == nil || *pvc.StorageClassName != "fast" {
		t.Errorf("storage class = %v, want fast", pvc.StorageClassName)
	}
	// The rendered resources are a copy: changing them must not change the
	// spec.
	ct.Resources.Limits[corev1.ResourceMemory] = resource.MustParse("2Gi")
	if got := c.Spec.Resources.Limits[corev1.ResourceMemory]; got.String() != "1Gi" {
		t.Errorf("rendering aliases spec.resources: spec limit is now %s", got.String())
	}
}

func TestOrdinal(t *testing.T) {
	c := cluster("kv", "kvstore", 3)
	for id, want := range map[string]int{"kv-0": 0, "kv-9": 9, "kv-10": 10} {
		if got, ok := Ordinal(c, id); !ok || got != want {
			t.Errorf("Ordinal(%q) = %d, %v; want %d", id, got, ok, want)
		}
	}
	for _, id := range []string{"kv", "kv-", "kv-x", "kv--1", "kv-01", "kvx-1", "db-1", "kv-1-2"} {
		if _, ok := Ordinal(c, id); ok {
			t.Errorf("Ordinal(%q) accepted", id)
		}
	}
}

func TestParseConfigMapRoundTrip(t *testing.T) {
	c := cluster("db", "prod", 4)
	members := Members(c, 11)
	cm := mustConfigMap(t, c, 9, members)
	epoch, got, err := ParseConfigMap(cm)
	if err != nil {
		t.Fatal(err)
	}
	if epoch != 9 || !maps.Equal(got, members) {
		t.Errorf("parsed epoch %d, members %v; want 9, %v", epoch, got, members)
	}
}

func TestParseConfigMapRejects(t *testing.T) {
	for name, data := range map[string]map[string]string{
		"no config.yaml": {},
		"not YAML":       {ConfigKey: "cluster: [\n"},
		"duplicate member": {ConfigKey: "cluster:\n  epoch: 1\n  members:\n" +
			"    - id: kv-0\n      address: a\n    - id: kv-0\n      address: b\n"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := ParseConfigMap(&corev1.ConfigMap{Data: data}); err == nil {
				t.Error("accepted")
			}
		})
	}
}
