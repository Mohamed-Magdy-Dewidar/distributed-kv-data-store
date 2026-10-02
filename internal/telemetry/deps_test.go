package telemetry

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

const module = "distributed-kv-datastore"

// Packages allowed to depend on a metrics library: this one, and the code
// that wires it in. cmd/node and e2e build on internal/app.
var metricsAllowed = []string{
	module + "/internal/telemetry",
	module + "/internal/app",
	module + "/cmd/node",
	module + "/e2e",
}

func goList(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("go", append([]string{"list"}, args...)...).Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			t.Fatalf("go list %v: %v\n%s", args, err, ee.Stderr)
		}
		t.Fatalf("go list %v: %v", args, err)
	}
	return string(out)
}

// TestCorePackagesDoNotImportPrometheus: no package outside metricsAllowed
// depends on Prometheus, in its code or its tests. Core packages expose
// plain values; only internal/telemetry turns them into metrics.
func TestCorePackagesDoNotImportPrometheus(t *testing.T) {
	var core []string
	for _, pkg := range strings.Fields(goList(t, module+"/...")) {
		if !slices.Contains(metricsAllowed, pkg) {
			core = append(core, pkg)
		}
	}
	if len(core) < 10 {
		t.Fatalf("go list found only %d core packages: %v", len(core), core)
	}

	// One line per package and per test variant ("pkg [pkg.test]"): the
	// package, then every package it depends on, directly or not.
	out := goList(t, append([]string{"-test", "-f", "{{.ImportPath}}|{{join .Deps \" \"}}"}, core...)...)
	for line := range strings.Lines(out) {
		pkg, deps, _ := strings.Cut(strings.TrimSpace(line), "|")
		for _, dep := range strings.Fields(deps) {
			if strings.HasPrefix(dep, "github.com/prometheus/") {
				t.Errorf("%s depends on %s; only %v may", pkg, dep, metricsAllowed)
				break
			}
		}
	}
}

// TestAppDoesNotImportPrometheusDirectly: internal/app wires telemetry in
// but leaves every metrics type to it, so swapping the format stays a
// change to internal/telemetry alone. Its tests may parse the output.
func TestAppDoesNotImportPrometheusDirectly(t *testing.T) {
	imports := strings.Fields(strings.Trim(strings.TrimSpace(goList(t, "-f", "{{.Imports}}", module+"/internal/app")), "[]"))
	if !slices.Contains(imports, module+"/internal/telemetry") {
		t.Fatalf("internal/app does not import internal/telemetry; imports: %v", imports)
	}
	for _, imp := range imports {
		if strings.HasPrefix(imp, "github.com/prometheus/") {
			t.Errorf("internal/app imports %s directly; only internal/telemetry may", imp)
		}
	}
}
