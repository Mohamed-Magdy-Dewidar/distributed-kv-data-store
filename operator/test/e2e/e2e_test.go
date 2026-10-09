//go:build e2e

// Package e2e runs the operator on the kind cluster of docs/kubernetes.md:
// it builds and loads the node and operator images, installs the operator,
// creates the KVCluster kv in kvstore (6 replicas), preloads keys, runs the
// loadgen writer, scales 6 -> 7 -> 6 with kubectl scale on the KVCluster,
// repeats the scale-down with the operator pod deleted while kv-6 drains, and
// reads back every acknowledged write. Run it from the repository's operator
// directory, on the host (kind, kubectl and docker on PATH):
//
//	go test -tags e2e ./test/e2e/ -v -count=1 -timeout 60m
//
// KIND_CLUSTER names the kind cluster (default kvstore); E2E_SKIP_BUILD=1
// reuses the images already loaded. It leaves kv running afterwards, for the
// observability stack.
//
// It acts on the kind cluster only, whatever kubectl's current context is:
// every kubectl call names --context kind-$KIND_CLUSTER (kube_test.go), and
// the test stops at once if that cluster or context does not exist.
package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	operatorNS   = "kvstore-operator-system"
	operatorDep  = "kvstore-operator-controller-manager"
	operatorImg  = "kvstore-operator:dev"
	nodeImg      = "kvnode:dev"
	pollInterval = 250 * time.Millisecond
)

var (
	operatorDir = filepath.Join("..", "..")
	repoRoot    = filepath.Join("..", "..", "..")
)

func must(t *testing.T, stdin, name string, args ...string) string {
	t.Helper()
	out, err := run(stdin, name, args...)
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, out)
	}
	return out
}

// kubectl, kubectlIn (with stdin) and tryKubectl (errors returned) are the
// only ways the e2e runs kubectl, each pinned to the kind context by kube.
func kubectl(t *testing.T, args ...string) string {
	t.Helper()
	return must(t, "", kubectlBin, kube(args...)...)
}

func kubectlIn(t *testing.T, stdin string, args ...string) string {
	t.Helper()
	return must(t, stdin, kubectlBin, kube(args...)...)
}

func tryKubectl(args ...string) (string, error) { return run("", kubectlBin, kube(args...)...) }

// kvStatus is the part of the KVCluster's status the test reads.
type kvStatus struct {
	Phase          string `json:"phase"`
	Epoch          int64  `json:"epoch"`
	Replicas       int32  `json:"replicas"`
	ReadyReplicas  int32  `json:"readyReplicas"`
	DrainStartedAt string `json:"drainStartedAt"`
	Members        []struct {
		ID string `json:"id"`
	} `json:"members"`
	Conditions []struct {
		Type    string `json:"type"`
		Status  string `json:"status"`
		Reason  string `json:"reason"`
		Message string `json:"message"`
	} `json:"conditions"`
}

func (s kvStatus) reason() string {
	for _, c := range s.Conditions {
		if c.Type == "Progressing" {
			return c.Reason
		}
	}
	return ""
}

func (s kvStatus) message() string {
	for _, c := range s.Conditions {
		if c.Type == "Progressing" {
			return c.Message
		}
	}
	return ""
}

func status(t *testing.T) (kvStatus, bool) {
	t.Helper()
	out, err := tryKubectl("-n", ns, "get", "kvc", "kv", "-o", "jsonpath={.status}")
	if err != nil || strings.TrimSpace(out) == "" {
		return kvStatus{}, false
	}
	var s kvStatus
	if err := json.Unmarshal([]byte(out), &s); err != nil {
		t.Fatalf("status %q: %v", out, err)
	}
	return s, true
}

// timeline records how the KVCluster's status moves.
type timeline struct {
	t     *testing.T
	start time.Time
	last  string
	lines []string
}

func newTimeline(t *testing.T) *timeline { return &timeline{t: t, start: time.Now()} }

func (tl *timeline) mark(event string) {
	line := fmt.Sprintf("%7.1fs  %s", time.Since(tl.start).Seconds(), event)
	tl.lines = append(tl.lines, line)
	tl.t.Log(line)
}

// until polls the status until cond holds, recording every change of phase,
// reason and epoch, and fails on Degraded or after timeout. It returns the
// last status.
func (tl *timeline) until(what string, timeout time.Duration, cond func(kvStatus) bool) kvStatus {
	tl.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		s, ok := status(tl.t)
		if ok {
			key := fmt.Sprintf("phase=%s reason=%s epoch=%d replicas=%d ready=%d",
				s.Phase, s.reason(), s.Epoch, s.Replicas, s.ReadyReplicas)
			if key != tl.last {
				tl.last = key
				tl.mark(key)
			}
			if s.Phase == "Degraded" {
				tl.t.Fatalf("Degraded: %s", s.message())
			}
			if cond(s) {
				tl.mark("reached: " + what)
				return s
			}
		}
		if time.Now().After(deadline) {
			tl.t.Fatalf("timed out after %v waiting for %s (last: %s %s)", timeout, what, tl.last, s.message())
		}
		time.Sleep(pollInterval)
	}
}

func ready(replicas int32, epoch int64) func(kvStatus) bool {
	return func(s kvStatus) bool {
		return s.Phase == "Ready" && s.Replicas == replicas && s.ReadyReplicas == replicas &&
			len(s.Members) == int(replicas) && (epoch < 0 || s.Epoch == epoch)
	}
}

func topNode(tl *timeline, when string) {
	out, err := tryKubectl("top", "node", "--no-headers")
	if err == nil {
		tl.mark("kubectl top node (" + when + "): " + strings.Join(strings.Fields(out), " "))
	}
}

// writerPod is a loadgen writer (deploy/loadgen/loadgen.yaml's write.sh).
func writerPod(name, prefix string, count int, sleep string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata: {name: %s, namespace: %s, labels: {e2e: writer}}
spec:
  restartPolicy: Never
  containers:
    - name: writer
      image: fullstorydev/grpcurl:v1.9.3-alpine
      imagePullPolicy: IfNotPresent
      command: ["/scripts/write.sh"]
      env:
        - {name: PREFIX, value: %s}
        - {name: START, value: "1"}
        - {name: COUNT, value: "%d"}
        - {name: SLEEP, value: "%s"}
      volumeMounts: [{name: scripts, mountPath: /scripts}]
  volumes: [{name: scripts, configMap: {name: loadgen, defaultMode: 0555}}]
`, name, ns, prefix, count, sleep)
}

// writes reads a writer's log: the acknowledged keys and the failed writes.
func writes(t *testing.T, pod string) (acked []string, failed []string) {
	t.Helper()
	for line := range strings.SplitSeq(kubectl(t, "-n", ns, "logs", pod), "\n") {
		f := strings.Fields(line)
		switch {
		case len(f) >= 4 && f[3] == "ok":
			acked = append(acked, f[2])
		case len(f) >= 4 && f[3] == "ERR":
			failed = append(failed, line)
		}
	}
	return acked, failed
}

func podGone(kind, name string) bool {
	out, _ := tryKubectl("-n", ns, "get", kind, name, "--ignore-not-found", "-o", "name")
	return strings.TrimSpace(out) == ""
}

func TestOperatorScalesUnderLoad(t *testing.T) {
	tl := newTimeline(t)
	cluster := kindCluster()
	if err := checkKindContext(); err != nil {
		t.Fatalf("the e2e runs on the kind cluster only: %v", err)
	}

	// --- images ---
	if os.Getenv("E2E_SKIP_BUILD") == "" {
		must(t, "", "docker", "build", "-t", nodeImg, repoRoot)
		must(t, "", "docker", "build", "-t", operatorImg, operatorDir)
		tl.mark("images built")
	}
	must(t, "", "kind", "load", "docker-image", nodeImg, operatorImg, "--name", cluster)
	tl.mark("images loaded into kind")

	// --- a fresh start: no KVCluster, no volumes, no writers ---
	freshStart()

	// --- the operator ---
	manifests := kubectl(t, "kustomize", filepath.Join(operatorDir, "config", "default"))
	manifests = strings.ReplaceAll(manifests, "image: controller:latest", "image: "+operatorImg)
	kubectlIn(t, manifests, "apply", "--server-side", "--force-conflicts", "-f", "-")
	kubectl(t, "-n", operatorNS, "rollout", "restart", "deploy/"+operatorDep) // pick up a rebuilt image
	kubectl(t, "-n", operatorNS, "rollout", "status", "deploy/"+operatorDep, "--timeout=180s")
	tl.mark("operator running")

	// --- test tooling ---
	kubectl(t, "apply", "-f", filepath.Join(repoRoot, "deploy", "loadgen", "loadgen.yaml"))
	kubectl(t, "-n", ns, "wait", "pod/toolbox", "--for=condition=Ready", "--timeout=120s")

	// --- create kv: 6 replicas ---
	kubectl(t, "-n", ns, "apply", "-f", filepath.Join(operatorDir, "config", "samples", "kvstore_v1alpha1_kvcluster.yaml"))
	created := time.Now()
	s := tl.until("kv Ready with 6 members", 5*time.Minute, ready(6, -1))
	tl.mark(fmt.Sprintf("PHASE create -> Ready: %.1fs", time.Since(created).Seconds()))
	e0 := s.Epoch
	topNode(tl, "6 replicas, idle")

	// --- preload: 4 writers x 1000 keys ---
	for i := range 4 {
		p := fmt.Sprintf("pre%d", i)
		kubectlIn(t, writerPod(p, p, 1000, "0"), "apply", "-f", "-")
	}
	preloadStart := time.Now()
	for i := range 4 {
		kubectl(t, "-n", ns, "wait", "pod/pre"+strconv.Itoa(i), "--for=jsonpath={.status.phase}=Succeeded", "--timeout=15m")
	}
	tl.mark(fmt.Sprintf("PHASE preload 4000 keys: %.1fs", time.Since(preloadStart).Seconds()))

	// --- the steady writer, about 11 writes/s, through every scale ---
	kubectlIn(t, writerPod("load", "load", 0, "0.05"), "apply", "-f", "-")
	kubectl(t, "-n", ns, "wait", "pod/load", "--for=condition=Ready", "--timeout=120s")
	time.Sleep(5 * time.Second)
	topNode(tl, "6 replicas, steady load")

	// --- scale 6 -> 7 ---
	t1 := time.Now()
	kubectl(t, "-n", ns, "scale", "kvc/kv", "--replicas=7")
	tl.until("kv Ready with 7 members at the next epoch", 10*time.Minute, ready(7, e0+1))
	tl.mark(fmt.Sprintf("PHASE scale 6 -> 7: %.1fs", time.Since(t1).Seconds()))
	topNode(tl, "7 replicas, steady load")

	// --- scale 7 -> 6 ---
	t2 := time.Now()
	kubectl(t, "-n", ns, "scale", "kvc/kv", "--replicas=6")
	tl.until("kv Ready with 6 members, kv-6 and its volume gone", 15*time.Minute, func(s kvStatus) bool {
		return ready(6, e0+2)(s) && podGone("pod", "kv-6") && podGone("pvc", "data-kv-6")
	})
	tl.mark(fmt.Sprintf("PHASE scale 7 -> 6: %.1fs", time.Since(t2).Seconds()))

	// --- variant: the operator pod is deleted while kv-6 drains ---
	t3 := time.Now()
	kubectl(t, "-n", ns, "scale", "kvc/kv", "--replicas=7")
	tl.until("kv Ready with 7 members (variant)", 10*time.Minute, ready(7, e0+3))
	tl.mark(fmt.Sprintf("PHASE scale 6 -> 7 (variant): %.1fs", time.Since(t3).Seconds()))
	t4 := time.Now()
	kubectl(t, "-n", ns, "scale", "kvc/kv", "--replicas=6")
	tl.until("kv-6 draining (D2)", 5*time.Minute, func(s kvStatus) bool { return s.reason() == "Draining" })
	old := strings.TrimSpace(kubectl(t, "-n", operatorNS, "get", "pod", "-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].metadata.name}"))
	kubectl(t, "-n", operatorNS, "delete", "pod", old, "--wait=false")
	killed := time.Now()
	tl.mark("deleted the operator pod " + old + " during D2")
	for {
		out, _ := tryKubectl("-n", operatorNS, "get", "pod", "-l", "control-plane=controller-manager",
			"-o", `jsonpath={range .items[*]}{.metadata.name}={.status.containerStatuses[0].ready}{"\n"}{end}`)
		if strings.Contains(out, "=true") && !strings.Contains(out, old+"=") {
			tl.mark(fmt.Sprintf("new operator pod ready after %.1fs", time.Since(killed).Seconds()))
			break
		}
		if time.Since(killed) > 3*time.Minute {
			t.Fatalf("no new operator pod: %s", out)
		}
		time.Sleep(pollInterval)
	}
	s = tl.until("kv Ready with 6 members after the operator restart", 15*time.Minute, func(s kvStatus) bool {
		return ready(6, -1)(s) && podGone("pod", "kv-6") && podGone("pvc", "data-kv-6")
	})
	if s.Epoch != e0+4 {
		t.Fatalf("epoch %d after the variant, want %d: the restart caused an extra membership change", s.Epoch, e0+4)
	}
	tl.mark(fmt.Sprintf("PHASE scale 7 -> 6 with the operator deleted in D2: %.1fs "+
		"(operator gone to cluster Ready: %.1fs)", time.Since(t4).Seconds(), time.Since(killed).Seconds()))
	topNode(tl, "6 replicas, after the scales")

	// --- every acknowledged write reads back ---
	// The writer's log is the record of acknowledged writes: snapshot it,
	// then stop the writer (writes after the snapshot are not counted).
	var acked, failed []string
	for _, pod := range []string{"pre0", "pre1", "pre2", "pre3", "load"} {
		a, f := writes(t, pod)
		tl.mark(fmt.Sprintf("%s: %d acknowledged, %d failed", pod, len(a), len(f)))
		acked, failed = append(acked, a...), append(failed, f...)
	}
	kubectl(t, "-n", ns, "delete", "pod", "-l", "e2e=writer", "--wait=true")
	for _, f := range failed {
		t.Logf("failed write: %s", f)
	}
	var in strings.Builder
	for _, k := range acked {
		fmt.Fprintf(&in, "%s v-%s\n", k, k)
	}
	verifyStart := time.Now()
	out := kubectlIn(t, in.String(), "-n", ns, "exec", "-i", "toolbox", "--", "/scripts/verify.sh")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	summary := lines[len(lines)-1]
	tl.mark(fmt.Sprintf("verify (%.1fs): %s", time.Since(verifyStart).Seconds(), summary))
	if want := fmt.Sprintf("checked=%d missing=0", len(acked)); summary != want {
		t.Fatalf("read-back: %s, want %s\n%s", summary, want, out)
	}
	tl.mark(fmt.Sprintf("RESULT %d acknowledged writes, all read back; %d writes failed", len(acked), len(failed)))
}
