package e2e

// This file has no build tag: its tests run with every go test of the module
// (no kind cluster needed), and check that the e2e can only ever act on the
// kind cluster.

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// ns is the namespace of the KVCluster kv.
const ns = "kvstore"

func kindCluster() string {
	if c := os.Getenv("KIND_CLUSTER"); c != "" {
		return c
	}
	return "kvstore"
}

// kubeContext is the kubeconfig context kind creates for the cluster. Every
// kubectl call the e2e makes names it, never the current context: on a
// machine that also has a GKE context, the e2e deletes and installs things.
func kubeContext() string { return "kind-" + kindCluster() }

const contextFlag = "--context"

// kube returns kubectl's arguments pinned to kubeContext.
func kube(args ...string) []string { return append([]string{contextFlag, kubeContext()}, args...) }

// pinned: args start with --context kubeContext.
func pinned(args []string) bool {
	return len(args) >= 2 && args[0] == contextFlag && args[1] == kubeContext()
}

// execCommand runs a command with stdin and returns its combined output.
// The tests here replace it, to record commands instead of running them.
var execCommand = func(stdin, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Stdin = strings.NewReader(stdin)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

// run runs a command. A kubectl command that is not pinned to kubeContext is
// refused, never run.
func run(stdin, name string, args ...string) (string, error) {
	if name == kubectlBin && !pinned(args) {
		return "", fmt.Errorf("refusing kubectl %s: not pinned to --context %s", strings.Join(args, " "), kubeContext())
	}
	return execCommand(stdin, name, args...)
}

// kubectlBin is kubectl's name for run. The e2e's helpers (kubectl,
// kubectlIn, tryKubectl in e2e_test.go) pass it with kube(...); nothing in
// e2e_test.go names "kubectl" itself (TestNoKubectlOutsideTheHelpers).
const kubectlBin = "kubectl"

// checkKindContext: the kind cluster exists, and kubectl has its context.
// It reads the kubeconfig and asks kind; it contacts no cluster.
func checkKindContext() error {
	clusters, err := execCommand("", "kind", "get", "clusters")
	if err != nil {
		return fmt.Errorf("kind get clusters: %v\n%s", err, clusters)
	}
	if !slices.Contains(strings.Fields(clusters), kindCluster()) {
		return fmt.Errorf("no kind cluster %q (kind get clusters: %q); create it (make kind-up) or set KIND_CLUSTER",
			kindCluster(), strings.TrimSpace(clusters))
	}
	contexts, err := execCommand("", "kubectl", "config", "get-contexts", "-o", "name")
	if err != nil {
		return fmt.Errorf("kubectl config get-contexts: %v\n%s", err, contexts)
	}
	if !slices.Contains(strings.Fields(contexts), kubeContext()) {
		return fmt.Errorf("kubectl has no context %q (it has %q)", kubeContext(), strings.Fields(contexts))
	}
	return nil
}

// freshStart removes what an earlier run left: the writers, the KVCluster,
// its StatefulSet and volumes. Best effort: whatever is not there is fine.
func freshStart() {
	targets := make([][]string, 0, 13)
	targets = append(targets, []string{"pod", "-l", "e2e=writer"}, []string{"kvc", "kv"}, []string{"statefulset", "kv"})
	for i := range 10 {
		targets = append(targets, []string{"pvc", fmt.Sprintf("data-kv-%d", i)})
	}
	for _, target := range targets {
		args := append(append([]string{"-n", ns, "delete"}, target...), "--ignore-not-found", "--wait=true")
		_, _ = run("", "kubectl", kube(args...)...)
	}
}

// record replaces execCommand for the test, recording every command.
func record(t *testing.T, out map[string]string) *[][]string {
	t.Helper()
	var cmds [][]string
	orig := execCommand
	t.Cleanup(func() { execCommand = orig })
	execCommand = func(_, name string, args ...string) (string, error) {
		cmds = append(cmds, append([]string{name}, args...))
		return out[name], nil
	}
	return &cmds
}

// The tests' kind cluster, and the context kind gives it.
const (
	testCluster = "e2etest"
	testContext = "kind-" + testCluster
)

// TestFreshStartIsPinnedToKind runs the fresh start's deletes dry (recorded,
// not run): every one is kubectl pinned to kind-<KIND_CLUSTER>.
func TestFreshStartIsPinnedToKind(t *testing.T) {
	t.Setenv("KIND_CLUSTER", testCluster)
	cmds := record(t, nil)
	freshStart()
	if len(*cmds) != 13 {
		t.Fatalf("recorded %d commands, want 13 deletes (a refused kubectl is not recorded): %q", len(*cmds), *cmds)
	}
	for _, c := range *cmds {
		if c[0] != kubectlBin || !slices.Equal(c[1:3], []string{contextFlag, testContext}) {
			t.Errorf("%q is not pinned to --context %s", c, testContext)
		}
	}
}

// TestFreshStartTargetsKindNotCurrent: with a kubeconfig whose current
// context is a stand-in for GKE, every recorded delete resolves (kubectl
// config view --minify, which reads the kubeconfig only) to the kind
// context, not the current one.
func TestFreshStartTargetsKindNotCurrent(t *testing.T) {
	if _, err := exec.LookPath(kubectlBin); err != nil {
		t.Skip("kubectl not on PATH")
	}
	t.Setenv("KIND_CLUSTER", testCluster)
	kubeconfig := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(kubeconfig, []byte(`apiVersion: v1
kind: Config
current-context: gke-stand-in
clusters:
- {name: gke, cluster: {server: "https://127.0.0.1:1"}}
- {name: kind, cluster: {server: "https://127.0.0.1:2"}}
users:
- {name: u, user: {}}
contexts:
- {name: gke-stand-in, context: {cluster: gke, user: u}}
- {name: kind-e2etest, context: {cluster: kind, user: u}}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", kubeconfig)

	cmds := record(t, nil)
	freshStart()
	recorded := slices.Clone(*cmds)
	if len(recorded) == 0 {
		t.Fatal("nothing recorded")
	}
	for _, c := range recorded {
		// The global flags before the subcommand, then a read-only query
		// of the context they select.
		global := []string{}
		if len(c) >= 3 && c[1] == contextFlag {
			global = c[1:3]
		}
		args := append(append([]string{}, global...), "config", "view", "--minify", "-o", "jsonpath={.contexts[0].name}")
		out, err := exec.Command(kubectlBin, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("kubectl %q: %v\n%s", args, err, out)
		}
		if got := strings.TrimSpace(string(out)); got != testContext {
			t.Errorf("%q would act on context %q, not %s", c, got, testContext)
		}
	}
}

// TestRunRefusesUnpinnedKubectl: a kubectl without the kind context, or
// with another one, is refused and never run.
func TestRunRefusesUnpinnedKubectl(t *testing.T) {
	t.Setenv("KIND_CLUSTER", testCluster)
	cmds := record(t, map[string]string{kubectlBin: "the command's output"})
	const gke = "gke_kvstore-gke_europe-west1_kvstore"
	deleteKV := []string{"-n", ns, "delete", "kvc", "kv"}
	for _, args := range [][]string{
		deleteKV,
		append([]string{contextFlag, gke}, deleteKV...),
		append([]string{"-n", ns, contextFlag, testContext}, deleteKV[2:]...),
	} {
		if _, err := run("", kubectlBin, args...); err == nil {
			t.Errorf("kubectl %q was not refused", args)
		}
	}
	if len(*cmds) != 0 {
		t.Fatalf("refused commands ran: %q", *cmds)
	}
	out, err := run("", kubectlBin, kube("get", "pods")...)
	if err != nil || len(*cmds) != 1 || out != "the command's output" {
		t.Fatalf("a pinned kubectl was not run: %q, %v, %q", out, err, *cmds)
	}
}

func TestCheckKindContext(t *testing.T) {
	t.Setenv("KIND_CLUSTER", testCluster)
	const gke = "gke_kvstore-gke_europe-west1_kvstore\n"
	for _, tt := range []struct {
		name, clusters, contexts string
		ok                       bool
	}{
		{"cluster and context", testCluster + "\nother\n", gke + testContext + "\n", true},
		{"no such kind cluster", "other\n", testContext + "\n", false},
		{"no context", testCluster + "\n", gke, false},
		{"a context with the name as a prefix", testCluster + "\n", testContext + "-old\n", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			record(t, map[string]string{"kind": tt.clusters, kubectlBin: tt.contexts})
			if err := checkKindContext(); (err == nil) != tt.ok {
				t.Fatalf("checkKindContext() = %v, want ok=%v", err, tt.ok)
			}
		})
	}
}

// TestNoKubectlOutsideTheHelpers: e2e_test.go never names kubectl itself,
// and passes kubectlBin only with kube(...), so every call is pinned (and
// run's refusal backs that up).
func TestNoKubectlOutsideTheHelpers(t *testing.T) {
	src, err := os.ReadFile("e2e_test.go")
	if err != nil {
		t.Fatal(err)
	}
	literal := regexp.MustCompile(`"kubectl"`)
	uses := 0
	for i, line := range strings.Split(string(src), "\n") {
		if literal.MatchString(line) {
			t.Errorf("e2e_test.go:%d calls kubectl directly: %s", i+1, strings.TrimSpace(line))
		}
		if strings.Contains(line, "kubectlBin") {
			uses++
			if strings.Count(line, "kubectlBin") != strings.Count(line, "kubectlBin, kube(") {
				t.Errorf("e2e_test.go:%d passes kubectlBin without kube(...): %s", i+1, strings.TrimSpace(line))
			}
		}
	}
	if uses != 3 {
		t.Errorf("e2e_test.go uses kubectlBin %d times, want 3 (kubectl, kubectlIn, tryKubectl)", uses)
	}
}

// TestE2EChecksTheContextFirst: the e2e calls checkKindContext before it
// runs anything (docker, kind or kubectl).
func TestE2EChecksTheContextFirst(t *testing.T) {
	src, err := os.ReadFile("e2e_test.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	start := strings.Index(s, "func TestOperatorScalesUnderLoad(")
	if start < 0 {
		t.Fatal("TestOperatorScalesUnderLoad not found")
	}
	body := s[start:]
	check := strings.Index(body, "checkKindContext()")
	first := len(body)
	for _, call := range []string{"must(", "kubectl(", "kubectlIn(", "tryKubectl(", "freshStart("} {
		if i := strings.Index(body, call); i >= 0 && i < first {
			first = i
		}
	}
	if check < 0 || check > first {
		t.Fatalf("TestOperatorScalesUnderLoad runs a command before checkKindContext (check at %d, first command at %d)",
			check, first)
	}
}

// TestMakefileKindTargetsRefuseOtherContexts runs the repository Makefile's
// kind-only targets with fake kubectl, docker and kind that log every call:
// under any context but kind's they refuse and call nothing; under kind's
// they run, pinned to it. Needs make and sh (the golang container has both).
func TestMakefileKindTargetsRefuseOtherContexts(t *testing.T) {
	for _, tool := range []string{"make", "sh"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not on PATH", tool)
		}
	}
	bin := t.TempDir()
	calls := filepath.Join(t.TempDir(), "calls")
	fakes := map[string]string{
		kubectlBin: "if [ \"$1 $2\" = \"config current-context\" ]; then\n" +
			"  [ -n \"$FAKE_CTX\" ] && echo \"$FAKE_CTX\"; exit 0\n" +
			"fi\n" +
			"echo \"kubectl $*\" >> \"$CALLS\"\n",
		"docker": "echo \"docker $*\" >> \"$CALLS\"\n",
		"kind":   "echo \"kind $*\" >> \"$CALLS\"\n",
	}
	for name, body := range fakes {
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"+body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	root := filepath.Join("..", "..", "..") // the repository, from operator/test/e2e
	makeTarget := func(ctx, target string) (string, string, error) {
		_ = os.Remove(calls)
		// MAKE=true: operator-test-e2e's sub-make does nothing here.
		cmd := exec.Command("make", "--no-print-directory", "-C", root, target, "MAKE=true")
		path := bin + string(os.PathListSeparator) + os.Getenv("PATH")
		cmd.Env = append(os.Environ(), "PATH="+path, "FAKE_CTX="+ctx, "CALLS="+calls)
		out, err := cmd.CombinedOutput()
		logged, _ := os.ReadFile(calls)
		return string(out), strings.TrimSpace(string(logged)), err
	}
	for _, target := range []string{"image", "operator-deploy", "operator-test-e2e"} {
		for _, ctx := range []string{"gke_kvstore-gke_europe-west1_kvstore", "", "kind-kvstore-old"} {
			out, logged, err := makeTarget(ctx, target)
			if err == nil || !strings.Contains(out, "Refusing: this target is for the kind cluster only") {
				t.Errorf("make %s under context %q: err=%v, want a refusal:\n%s", target, ctx, err, out)
			}
			if logged != "" {
				t.Errorf("make %s under context %q called %q before refusing", target, ctx, logged)
			}
		}
		out, _, err := makeTarget("kind-kvstore", target)
		if err != nil {
			t.Errorf("make %s under kind-kvstore: %v\n%s", target, err, out)
		}
	}
	// Under kind's context, operator-deploy's kubectl calls that reach the
	// cluster name it.
	_, _, _ = makeTarget("kind-kvstore", "operator-deploy")
	logged, _ := os.ReadFile(calls)
	for line := range strings.SplitSeq(strings.TrimSpace(string(logged)), "\n") {
		local := strings.Contains(line, "kustomize") || strings.Contains(line, "--dry-run=client")
		if !local && !strings.HasPrefix(line, "kubectl --context kind-kvstore ") {
			t.Errorf("operator-deploy ran %q without --context kind-kvstore", line)
		}
	}
}
