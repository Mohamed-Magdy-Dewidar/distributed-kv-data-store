package chaos

import (
	"slices"
	"strings"
	"testing"
)

func TestParseFault(t *testing.T) {
	topo := sixPods(t)
	for spec, want := range map[string]Fault{
		"none":                  {Kind: FaultNone},
		"down:kv-3":             {Kind: FaultDown, Pods: []string{"kv-3"}},
		"down:kv-4,kv-1,kv-4":   {Kind: FaultDown, Pods: []string{"kv-1", "kv-4"}},
		"zone:zone-b":           {Kind: FaultDown, Pods: []string{"kv-1", "kv-3"}},
		"partition:kv-5,kv-0":   {Kind: FaultPartition, Pods: []string{"kv-0", "kv-5"}},
		"partition-zone:zone-a": {Kind: FaultPartition, Pods: []string{"kv-0", "kv-2"}},
	} {
		got, err := ParseFault(spec, topo)
		if err != nil {
			t.Errorf("%s: %v", spec, err)
			continue
		}
		if got.Kind != want.Kind || !slices.Equal(got.Pods, want.Pods) {
			t.Errorf("%s = %+v, want %+v", spec, got, want)
		}
	}
	for _, spec := range []string{"", "down:", "down:kv-9", "zone:zone-x", "crash:kv-1", "none:kv-1",
		"down:kv-0,kv-1,kv-2,kv-3,kv-4,kv-5"} {
		if f, err := ParseFault(spec, topo); err == nil {
			t.Errorf("%q parsed as %+v", spec, f)
		}
	}
}

// keyWith finds a key whose replicas satisfy ok.
func keyWith(t *testing.T, topo *Topology, ok func(replicas []string) bool) string {
	t.Helper()
	for _, k := range benchPreloadKeys() {
		if ok(topo.Replicas(k)) {
			return k
		}
	}
	t.Fatal("no such key")
	return ""
}

func countIn(replicas []string, pods ...string) int {
	n := 0
	for _, r := range replicas {
		if slices.Contains(pods, r) {
			n++
		}
	}
	return n
}

func TestExpectDown(t *testing.T) {
	topo := sixPods(t)
	zoneA := Fault{Kind: FaultDown, Pods: []string{"kv-0", "kv-2"}}
	two := keyWith(t, topo, func(r []string) bool { return countIn(r, "kv-0", "kv-2") == 2 })
	one := keyWith(t, topo, func(r []string) bool { return countIn(r, "kv-0", "kv-2") == 1 })
	none := keyWith(t, topo, func(r []string) bool { return countIn(r, "kv-0", "kv-2") == 0 })

	for _, tc := range []struct {
		key, target string
		want        Outcome
	}{
		{two, "kv-1", Fail}, // one replica left, W=R=2
		{one, "kv-1", Ok},
		{none, "kv-1", Ok},
		{none, "kv-0", TargetDown}, // the client's pod is down, whatever the key
		{two, "", Fail},            // through the Service: the key decides
		{one, "", Ok},
	} {
		for _, op := range []Op{OpPut, OpGet} {
			if got := topo.Expect(zoneA, op, tc.key, tc.target); got != tc.want {
				t.Errorf("%s %s (replicas %v) via %q: %s, want %s", op, tc.key, topo.Replicas(tc.key), tc.target, got, tc.want)
			}
		}
	}
	if got := topo.Expect(Fault{Kind: FaultNone}, OpPut, two, "kv-0"); got != Ok {
		t.Errorf("no fault: %s", got)
	}
	if got := topo.Unreachable(zoneA, one, "kv-1"); got != 1 {
		t.Errorf("a key with one replica down: %d unreachable, want 1 (one hint)", got)
	}
}

// With two of six pods cut off and N=3, W=2, every key is writable from
// exactly one side: the group's if two of its replicas are in the group,
// the rest's otherwise.
func TestExpectPartitionSidesAreComplementary(t *testing.T) {
	topo := sixPods(t)
	f := Fault{Kind: FaultPartition, Pods: []string{"kv-0", "kv-2"}}
	for _, k := range benchPreloadKeys()[:2000] {
		group := topo.Expect(f, OpPut, k, "kv-0")
		rest := topo.Expect(f, OpPut, k, "kv-4")
		if (group == Ok) == (rest == Ok) {
			t.Fatalf("%s (replicas %v): group %s, rest %s: want exactly one side ok", k, topo.Replicas(k), group, rest)
		}
		if wantGroup := countIn(topo.Replicas(k), "kv-0", "kv-2") == 2; (group == Ok) != wantGroup {
			t.Fatalf("%s (replicas %v): group side %s", k, topo.Replicas(k), group)
		}
		if got := topo.Expect(f, OpPut, k, ""); got != Unknowable {
			t.Fatalf("%s through the Service: %s, want unknowable", k, got)
		}
	}
	s := topo.Summarize(f, benchPreloadKeys())
	if s.Nowhere[OpPut] != 0 || s.Fails[SideGroup][OpPut]+s.Fails[SideRest][OpPut] != s.Keys {
		t.Errorf("summary %+v: every key should fail from exactly one side", s)
	}
}

func TestStandardFaults(t *testing.T) {
	var got []string
	for _, f := range sixPods(t).StandardFaults() {
		got = append(got, f.String())
	}
	want := "down:kv-0 down:kv-1 down:kv-2 down:kv-3 down:kv-4 down:kv-5 " +
		"down:kv-0,kv-2 down:kv-1,kv-3 down:kv-4,kv-5 " +
		"partition:kv-0,kv-2 partition:kv-1,kv-3 partition:kv-4,kv-5"
	if strings.Join(got, " ") != want {
		t.Errorf("standard faults:\n %s\nwant\n %s", strings.Join(got, " "), want)
	}
}

func TestSingleDownNeverFailsAKey(t *testing.T) {
	topo := sixPods(t)
	for _, p := range topo.Pods {
		s := topo.Summarize(Fault{Kind: FaultDown, Pods: []string{p.ID}}, benchPreloadKeys())
		if s.Fails[SideLive][OpPut] != 0 || s.Fails[SideLive][OpGet] != 0 {
			t.Errorf("%s down: %+v keys fail, want none (N=3, W=R=2)", p.ID, s.Fails)
		}
	}
}

func TestParseKeySpec(t *testing.T) {
	keys, err := ParseKeySpec("chaos-pre:3")
	if err != nil || strings.Join(keys, " ") != "chaos-pre-0000000 chaos-pre-0000001 chaos-pre-0000002" {
		t.Fatalf("keys = %v, %v", keys, err)
	}
	for _, bad := range []string{"chaos-pre", ":3", "x:0", "x:-1", "x:y"} {
		if _, err := ParseKeySpec(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}

func TestPredictKey(t *testing.T) {
	topo := sixPods(t)
	f := Fault{Kind: FaultPartition, Pods: []string{"kv-0", "kv-2"}}
	k := keyWith(t, topo, func(r []string) bool { return countIn(r, "kv-0", "kv-2") == 2 })
	kp := topo.PredictKey(f, k)
	if len(kp.Replicas) != 3 || len(kp.Zones) != 3 {
		t.Fatalf("%+v", kp)
	}
	if kp.Expect[SideGroup][OpPut] != Ok || kp.Expect[SideRest][OpGet] != Fail {
		t.Errorf("%+v", kp.Expect)
	}
}
