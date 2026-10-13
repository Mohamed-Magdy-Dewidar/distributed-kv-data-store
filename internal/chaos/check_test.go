package chaos

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// keysWith returns n keys whose replicas satisfy ok.
func keysWith(t *testing.T, topo *Topology, n int, ok func([]string) bool) []string {
	t.Helper()
	var out []string
	for _, k := range benchPreloadKeys() {
		if ok(topo.Replicas(k)) {
			if out = append(out, k); len(out) == n {
				return out
			}
		}
	}
	t.Fatal("not enough keys")
	return nil
}

var t0 = time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)

// req is a request of test t1 that starts at offset from t0 and takes 10ms.
func req(op Op, key, target string, offset time.Duration, res Result) Request {
	start := t0.Add(offset).UnixNano()
	return Request{Test: "t1", Op: op, Key: key, Target: target, Start: start, End: start + int64(10*time.Millisecond), Result: res}
}

func zoneAEvents() []Event {
	return []Event{
		{Time: t0, Test: "t1", Event: EventInject, Fault: "zone:zone-a"},
		{Time: t0.Add(time.Minute), Test: "t1", Event: EventHeal},
	}
}

func TestCheckBucketsEveryRequest(t *testing.T) {
	topo := sixPods(t)
	inA := func(n int) func([]string) bool {
		return func(r []string) bool { return countIn(r, "kv-0", "kv-2") == n }
	}
	two := keysWith(t, topo, 2, inA(2))
	one := keysWith(t, topo, 4, inA(1))
	none := keysWith(t, topo, 3, inA(0))
	readKey := none[2]

	pre := req(OpPut, readKey, "kv-1", -100*time.Second, ResultOK)
	pre.Test = "pre"
	records := []Request{
		pre,
		req(OpPut, none[0], "kv-1", -10*time.Second, ResultOK),      // normal, ok
		req(OpPut, two[0], "kv-1", 20*time.Second, ResultFailed),    // quorum lost, as predicted
		req(OpPut, two[1], "kv-1", 21*time.Second, ResultOK),        // quorum lost, yet acknowledged
		req(OpPut, one[0], "kv-1", 22*time.Second, ResultOK),        // one replica down: ok, one hint
		req(OpPut, none[1], "kv-0", 23*time.Second, ResultUnknown),  // sent to a pod that is down
		req(OpPut, one[1], "kv-1", 24*time.Second, ResultFailed),    // should have worked
		req(OpPut, one[2], "kv-1", time.Second, ResultOK),           // before the fault was noticed
		req(OpGet, readKey, "kv-3", 25*time.Second, ResultNotFound), // a stale read
		req(OpPut, one[3], "svc-2", 26*time.Second, ResultUnknown),  // through the Service
		req(OpPut, "chaos-t1-x", "kv-1", 27*time.Second, ResultDropped),
		req(OpPut, none[0]+"-after", "kv-1", 90*time.Second, ResultOK), // after the heal
	}
	readback := []ReadbackEntry{
		{Key: readKey, Result: ResultOK},
		{Key: none[0], Result: ResultOK, Owners: topo.Replicas(none[0]), Holders: topo.Replicas(none[0])},
		{Key: two[1], Result: ResultNotFound},
		{Key: one[0], Result: ResultOK, Owners: topo.Replicas(one[0]), Holders: topo.Replicas(one[0])[:2]},
		{Key: one[2], Result: ResultOK, Owners: topo.Replicas(one[2]), Holders: topo.Replicas(one[2])[:2]},
		{Key: one[1], Result: ResultNotFound},
		{Key: none[0] + "-after", Result: ResultOK},
	}
	rep, err := Check(CheckConfig{Topology: topo, Events: zoneAEvents(), Records: records, Readback: readback, Tolerance: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var tr *TestReport
	for _, r := range rep.Tests {
		if r.Test == "t1" {
			tr = r
		}
	}
	want := map[Bucket]int{
		AsPredicted: 5, UnexpectedSuccess: 1, TargetDownOK: 0, UnexpectedFailure: 1,
		StaleRead: 1, ServiceFailure: 1, Transition: 1, Dropped: 1,
	}
	for b, n := range want {
		if tr.Buckets[b] != n {
			t.Errorf("bucket %s: %d, want %d (all: %v)", b, tr.Buckets[b], n, tr.Buckets)
		}
	}
	if tr.Hints != 3 || tr.UnderReplicated != 1 { // two[1]: 2 replicas down, one[0]: 1
		t.Errorf("hints %d, under-replicated %d: want 3 and 1", tr.Hints, tr.UnderReplicated)
	}
	if tr.HintsMaybe != 1 { // one[2], written before the fault was noticed
		t.Errorf("hints maybe %d, want 1", tr.HintsMaybe)
	}
	if rep.Acked != 6 || rep.Lost != 1 || rep.LostKeys[0] != two[1] || rep.NotReadBack != 0 {
		t.Errorf("acked %d lost %d %v not read back %d", rep.Acked, rep.Lost, rep.LostKeys, rep.NotReadBack)
	}
	if rep.UnackedAbsent != 1 || rep.UnderReplicated != 2 || rep.UnderReplicatedExpected != 1 ||
		len(rep.UnexpectedUnderReplicated) != 1 || rep.UnexpectedUnderReplicated[0] != one[0] {
		t.Errorf("unacked absent %d; under-replicated %d, expected %d, unexpected %v",
			rep.UnackedAbsent, rep.UnderReplicated, rep.UnderReplicatedExpected, rep.UnexpectedUnderReplicated)
	}
	if rep.OK() {
		t.Error("a check with a lost write passed")
	}
	var text bytes.Buffer
	rep.WriteText(&text)
	for _, s := range []string{"== test t1: down:kv-0,kv-2", "LOST " + two[1], "!unexpected-success=1", "== FAIL"} {
		if !strings.Contains(text.String(), s) {
			t.Errorf("report lacks %q:\n%s", s, text.String())
		}
	}
}

func TestCheckPassesAFaultThatWentAsPredicted(t *testing.T) {
	topo := sixPods(t)
	two := keysWith(t, topo, 1, func(r []string) bool { return countIn(r, "kv-0", "kv-2") == 2 })[0]
	early := keysWith(t, topo, 1, func(r []string) bool { return countIn(r, "kv-0", "kv-2") == 1 })[0]
	records := []Request{
		req(OpPut, early, "kv-3", 2*time.Second, ResultOK),
		req(OpPut, two, "kv-3", 30*time.Second, ResultFailed),
	}
	readback := []ReadbackEntry{{Key: early, Result: ResultOK, Owners: topo.Replicas(early), Holders: topo.Replicas(early)[:2]}}
	cfg := CheckConfig{Topology: topo, Events: zoneAEvents(), Records: records, Readback: readback}
	rep, err := Check(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() {
		var text bytes.Buffer
		rep.WriteText(&text)
		t.Fatalf("failed:\n%s", text.String())
	}

	// At the session's end, after anti-entropy, the same copy count fails.
	cfg.Converged = true
	if rep, _ = Check(cfg); rep.OK() || len(rep.UnexpectedUnderReplicated) != 1 {
		t.Errorf("converged: under-replication passed (%v)", rep.UnexpectedUnderReplicated)
	}
}

func TestCheckLatestReadbackCounts(t *testing.T) {
	topo := sixPods(t)
	records := []Request{req(OpPut, "k", "kv-1", -time.Minute, ResultOK)}
	readback := []ReadbackEntry{
		{Key: "k", Time: 2, Result: ResultOK},
		{Key: "k", Time: 1, Result: ResultUnknown},
	}
	rep, err := Check(CheckConfig{Topology: topo, Records: records, Readback: readback})
	if err != nil || !rep.OK() {
		t.Fatalf("the later read-back should count: %v %+v", err, rep)
	}
}

func TestCheckReportsTheRecoveryTimeline(t *testing.T) {
	topo := sixPods(t)
	events := append(zoneAEvents(),
		Event{Time: t0.Add(40 * time.Second), Test: "t1", Event: EventMark, Mark: "node-ready"},
		Event{Time: t0.Add(25 * time.Second), Test: "t1", Event: EventMark, Mark: "vm-recreated"})
	rep, err := Check(CheckConfig{Topology: topo, Events: events, Records: []Request{req(OpPut, "k", "kv-1", -time.Minute, ResultOK)}})
	if err != nil {
		t.Fatal(err)
	}
	var text bytes.Buffer
	rep.WriteText(&text)
	if want := "timeline:  +0s inject, +25.0s vm-recreated, +40.0s node-ready, +60.0s heal"; !strings.Contains(text.String(), want) {
		t.Errorf("report lacks %q:\n%s", want, text.String())
	}
}

// Once a down fault's pods answer again (serving), the other nodes stop
// treating them as dead before the kubelet reports them Ready (the heal):
// that stretch is a transition, and its hints are only possible.
func TestCheckServingEndsTheFault(t *testing.T) {
	topo := sixPods(t)
	one := keysWith(t, topo, 2, func(r []string) bool { return countIn(r, "kv-0", "kv-2") == 1 })
	events := append(zoneAEvents(),
		Event{Time: t0.Add(30 * time.Second), Test: "t1", Event: EventMark, Mark: MarkServing},
		Event{Time: t0.Add(90 * time.Second), Test: "t1", Event: EventMark, Mark: "hints-delivered"})
	records := []Request{
		req(OpPut, one[0], "kv-1", 20*time.Second, ResultOK), // the fault: one hint
		req(OpPut, one[1], "kv-1", 45*time.Second, ResultOK), // serving, not yet Ready
	}
	rep, err := Check(CheckConfig{Topology: topo, Events: events, Records: records})
	if err != nil {
		t.Fatal(err)
	}
	tr := rep.Tests[0]
	if tr.Hints != 1 || tr.HintsMaybe != 1 || tr.Buckets[Transition] != 1 {
		t.Errorf("hints %d, maybe %d, buckets %v: want 1, 1 and one transition", tr.Hints, tr.HintsMaybe, tr.Buckets)
	}
	var text bytes.Buffer
	rep.WriteText(&text)
	if want := "+30.0s serving, +60.0s heal, +90.0s hints-delivered"; !strings.Contains(text.String(), want) {
		t.Errorf("report lacks %q (marks and heal in time order):\n%s", want, text.String())
	}
}

func TestCheckRefusesABadTimeline(t *testing.T) {
	topo := sixPods(t)
	for name, events := range map[string][]Event{
		"injected twice": {{Time: t0, Test: "t", Event: EventInject, Fault: "down:kv-1"}, {Time: t0, Test: "t", Event: EventInject, Fault: "down:kv-1"}},
		"heal only":      {{Time: t0, Test: "t", Event: EventHeal}},
		"heal first":     {{Time: t0, Test: "t", Event: EventInject, Fault: "down:kv-1"}, {Time: t0.Add(-time.Second), Test: "t", Event: EventHeal}},
		"bad fault":      {{Time: t0, Test: "t", Event: EventInject, Fault: "down:kv-9"}},
		"bad event":      {{Time: t0, Test: "t", Event: "crash"}},
		"unnamed mark":   {{Time: t0, Test: "t", Event: EventInject, Fault: "down:kv-1"}, {Time: t0, Test: "t", Event: EventMark}},
		"mark only":      {{Time: t0, Test: "t", Event: EventMark, Mark: "node-ready"}},
	} {
		if _, err := Check(CheckConfig{Topology: topo, Events: events}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestReadEvents(t *testing.T) {
	in := `{"time":"2026-10-14T09:12:03.120Z","test":"t1","event":"inject","fault":"down:kv-3"}

{"time":"2026-10-14T09:15:40.002Z","test":"t1","event":"heal"}
`
	evs, err := ReadEvents(strings.NewReader(in))
	if err != nil || len(evs) != 2 || evs[0].Fault != "down:kv-3" || evs[1].Time.Sub(evs[0].Time) != 3*time.Minute+36882*time.Millisecond {
		t.Fatalf("%v %+v", err, evs)
	}
}
