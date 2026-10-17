package chaos

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"
)

// Event is one line of the fault timeline the runbook writes as it injects
// and heals each test's fault:
//
//	{"time":"2026-10-14T09:12:03.120Z","test":"t1","event":"inject","fault":"down:kv-3"}
//	{"time":"2026-10-14T09:13:31.877Z","test":"t1","event":"mark","mark":"node-ready"}
//	{"time":"2026-10-14T09:15:40.002Z","test":"t1","event":"heal"}
//
// A down fault heals when its pods are Ready again, a partition when its
// NetworkPolicy is deleted. Marks record the moments in between (a VM
// recreated, its node Ready, its volume attached), so the report can break
// the recovery time down; they change no prediction.
type Event struct {
	Time  time.Time `json:"time"`
	Test  string    `json:"test"`
	Event string    `json:"event"`           // inject, mark or heal
	Fault string    `json:"fault,omitempty"` // on inject: a ParseFault spec
	Mark  string    `json:"mark,omitempty"`  // on mark: what happened
}

// ReadEvents reads a fault timeline.
func ReadEvents(r io.Reader) ([]Event, error) {
	var out []Event
	sc := bufio.NewScanner(r)
	for line := 1; sc.Scan(); line++ {
		if len(strings.TrimSpace(sc.Text())) == 0 {
			continue
		}
		var e Event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("events line %d: %w", line, err)
		}
		out = append(out, e)
	}
	return out, sc.Err()
}

// Phase is where a request falls in its test's fault timeline.
type Phase string

const (
	// PhaseNormal: before the fault or after it has healed: expect ok.
	PhaseNormal Phase = "normal"
	// PhaseFault: the fault holds and has been noticed: expect Expect's
	// outcome.
	PhaseFault Phase = "fault"
	// PhaseTransition: the request overlaps the time the nodes take to
	// notice the fault (Timing.Detection) or its healing
	// (Timing.Recovery): any outcome but a lost or stale value is allowed.
	PhaseTransition Phase = "transition"
)

// Bucket is how a request compares with its prediction.
type Bucket string

const (
	AsPredicted Bucket = "as-predicted"
	// UnexpectedSuccess: predicted to fail (too few replicas reachable),
	// and it succeeded. A violation: quorums are strict, or the topology
	// is wrong.
	UnexpectedSuccess Bucket = "unexpected-success"
	// UnexpectedFailure: predicted to succeed, and it did not. A violation
	// for a request to a known pod: the prediction missed something.
	UnexpectedFailure Bucket = "unexpected-failure"
	// ServiceFailure: an unexpected failure through the Service, whose
	// pod is unknown (a connection to a pod that is gone): a finding.
	ServiceFailure Bucket = "service-failure"
	// TargetDownOK: sent to a pod that was down, and it answered.
	TargetDownOK Bucket = "target-down-ok"
	// StaleRead: a Get that found no value, or another value, for a key
	// whose Put was acknowledged before the Get was sent. A violation.
	StaleRead Bucket = "stale-read"
	// UnjudgedRead: a Get that found nothing for a key with no
	// acknowledged Put before it: nothing to compare with.
	UnjudgedRead Bucket = "unjudged-read"
	// Transition: in a transition phase, neither lost nor stale.
	Transition Bucket = "transition"
	// UnknowableBucket: through the Service during a partition, where the
	// outcome depends on the unknown pod.
	UnknowableBucket Bucket = "unknowable"
	Dropped          Bucket = "dropped"
)

// violations are the buckets that fail a check.
var violations = []Bucket{UnexpectedSuccess, UnexpectedFailure, StaleRead}

// CheckConfig is what Check compares.
type CheckConfig struct {
	Topology *Topology
	Events   []Event
	Records  []Request
	Readback []ReadbackEntry
	// Converged: every acknowledged write must be on all its owners (the
	// session's end, after a full anti-entropy interval). Otherwise writes
	// predicted to be under-replicated may be.
	Converged bool
	// Tolerance widens every window, for the difference between the
	// clocks of the machine that wrote the events and the recorder's pod.
	Tolerance time.Duration
	// Detection and Recovery override the topology's (Timing.Detection,
	// Timing.Recovery) when not zero.
	Detection, Recovery time.Duration
	// Examples is how many requests to keep per test and bucket.
	Examples int
}

// TestReport is one test's comparison.
type TestReport struct {
	Test         string
	Fault        *Fault // nil: no fault was injected
	Inject, Heal time.Time
	Marks        []Event // the test's marks, in time order
	Counts       map[Phase]map[Op]map[Result]int
	Buckets      map[Bucket]int
	Examples     map[Bucket][]Request
	// Hints is how many hints the acknowledged writes of the fault phase
	// should have created (one per replica the coordinator could not
	// reach); HintsUnknown counts those whose coordinator's side is
	// unknown. HintsMaybe counts those of the transitions: a replica that
	// was not yet marked dead (or already marked alive again) is not
	// hinted, so the count is between Hints and Hints+HintsMaybe.
	Hints, HintsUnknown, HintsMaybe int
	// UnderReplicated counts the acknowledged writes predicted to be left
	// under-replicated: written while the fault was not yet noticed, with
	// a replica the coordinator could not reach (a write to a peer that
	// times out leaves no hint; anti-entropy repairs it).
	UnderReplicated int
}

// Report is a check's result.
type Report struct {
	Tests []*TestReport
	// Writes acknowledged, and how the read-back found them.
	Acked, Lost, NotReadBack, Unreadable int
	LostKeys                             []string
	// Writes not acknowledged (failed or unknown) found present or absent.
	UnackedPresent, UnackedAbsent int
	// Copies: acknowledged writes on fewer than all their owners, those of
	// them predicted (and allowed, unless Converged), and the others.
	CopiesChecked, UnderReplicated, UnderReplicatedExpected int
	UnexpectedUnderReplicated                               []string
	HaveReadback, Converged                                 bool
}

// window is a closed time interval.
type window struct{ from, to time.Time }

func (w window) overlaps(r Request) bool {
	return !w.from.IsZero() && r.Start <= w.to.UnixNano() && r.End >= w.from.UnixNano()
}

type testTimeline struct {
	fault        *Fault
	inject, heal time.Time
	in, out      window
	marks        []Event
	// back is when each of a down fault's pods answered again
	// (MarkServingPod); backs, the transitions that follow.
	back  map[string]time.Time
	backs []window
}

// Check compares cfg's records with the prediction, request by request,
// and the read-back with the acknowledged writes, key by key.
func Check(cfg CheckConfig) (*Report, error) {
	if cfg.Detection == 0 {
		cfg.Detection = cfg.Topology.Timing.Detection()
	}
	if cfg.Recovery == 0 {
		cfg.Recovery = cfg.Topology.Timing.Recovery()
	}
	if cfg.Examples == 0 {
		cfg.Examples = 5
	}
	timelines, err := parseTimelines(cfg)
	if err != nil {
		return nil, err
	}

	// The first acknowledgment of each key, to judge reads by.
	ackedAt := map[string]int64{}
	for _, r := range cfg.Records {
		if r.Op == OpPut && r.Result == ResultOK {
			if at, ok := ackedAt[r.Key]; !ok || r.End < at {
				ackedAt[r.Key] = r.End
			}
		}
	}

	rep := &Report{HaveReadback: len(cfg.Readback) > 0, Converged: cfg.Converged}
	expectedUnder := compareRecords(cfg, timelines, ackedAt, rep)
	if rep.HaveReadback {
		joinReadback(cfg, ackedAt, expectedUnder, rep)
	}
	return rep, nil
}

// Event names.
const (
	EventInject = "inject"
	EventMark   = "mark"
	EventHeal   = "heal"

	// MarkServing is the mark of the moment a down fault's pods answer
	// again: from it to the heal is a transition, not the fault.
	MarkServing = "serving"
	// MarkServingPod prefixes the mark of the moment one of a down fault's
	// pods answers again (serving-kv-4): from then the fault is the pods
	// still down, after a transition while the others notice it is back.
	// With one mark for all of them, a pod back before the last one turned
	// requests that worked into unexpected successes (GKE session 3, t2).
	MarkServingPod = MarkServing + "-"
)

// parseTimelines is each test's fault and windows, from cfg.Events.
func parseTimelines(cfg CheckConfig) (map[string]*testTimeline, error) {
	timelines := map[string]*testTimeline{}
	for _, e := range cfg.Events {
		tl := timelines[e.Test]
		if tl == nil {
			tl = &testTimeline{}
			timelines[e.Test] = tl
		}
		switch e.Event {
		case EventInject:
			if tl.fault != nil {
				return nil, fmt.Errorf("test %s: injected twice", e.Test)
			}
			f, err := ParseFault(e.Fault, cfg.Topology)
			if err != nil {
				return nil, fmt.Errorf("test %s: %w", e.Test, err)
			}
			tl.fault, tl.inject = &f, e.Time
		case EventMark:
			if e.Mark == "" {
				return nil, fmt.Errorf("test %s: a mark without a name", e.Test)
			}
			tl.marks = append(tl.marks, e)
		case EventHeal:
			if !tl.heal.IsZero() {
				return nil, fmt.Errorf("test %s: healed twice", e.Test)
			}
			tl.heal = e.Time
		default:
			return nil, fmt.Errorf("test %s: event %q: want %s, %s or %s", e.Test, e.Event, EventInject, EventMark, EventHeal)
		}
	}
	for name, tl := range timelines {
		if tl.fault == nil {
			return nil, fmt.Errorf("test %s: marked or healed but never injected", name)
		}
		slices.SortStableFunc(tl.marks, func(a, b Event) int { return a.Time.Compare(b.Time) })
		if !tl.heal.IsZero() && tl.heal.Before(tl.inject) {
			return nil, fmt.Errorf("test %s: healed before it was injected", name)
		}
		tl.in = window{tl.inject.Add(-cfg.Tolerance), tl.inject.Add(cfg.Detection + cfg.Tolerance)}
		if !tl.heal.IsZero() {
			// The fault ends for the other nodes once its pods answer
			// again (MarkServing), which can be well before the kubelet
			// reports them Ready (the heal).
			from := tl.heal
			for _, m := range tl.marks {
				if m.Mark == MarkServing && m.Time.Before(from) {
					from = m.Time
				}
			}
			tl.out = window{from.Add(-cfg.Tolerance), tl.heal.Add(cfg.Recovery + cfg.Tolerance)}
		}
		for _, m := range tl.marks {
			pod, ok := strings.CutPrefix(m.Mark, MarkServingPod)
			if !ok {
				continue
			}
			if tl.fault.Kind != FaultDown || !slices.Contains(tl.fault.Pods, pod) {
				return nil, fmt.Errorf("test %s: mark %s: %s is not down in %s", name, m.Mark, pod, tl.fault)
			}
			if _, seen := tl.back[pod]; seen {
				continue // the marks are in time order: the first counts
			}
			if tl.back == nil {
				tl.back = map[string]time.Time{}
			}
			tl.back[pod] = m.Time
			tl.backs = append(tl.backs, window{m.Time.Add(-cfg.Tolerance), m.Time.Add(cfg.Recovery + cfg.Tolerance)})
		}
	}
	return timelines, nil
}

// faultAt is tl's fault for a request that starts at start: a down fault
// without its pods already serving again (MarkServingPod), none if no pod
// is left down.
func (tl *testTimeline) faultAt(start int64) Fault {
	if len(tl.back) == 0 {
		return *tl.fault
	}
	var down []string
	for _, p := range tl.fault.Pods {
		if at, ok := tl.back[p]; !ok || start < at.UnixNano() {
			down = append(down, p)
		}
	}
	if len(down) == 0 {
		return Fault{Kind: FaultNone}
	}
	return Fault{Kind: FaultDown, Pods: down}
}

// compareRecords fills rep's test reports, and returns the acknowledged
// writes predicted to be left under-replicated.
func compareRecords(cfg CheckConfig, timelines map[string]*testTimeline, ackedAt map[string]int64, rep *Report) map[string]bool {
	t := cfg.Topology
	byTest := map[string]*TestReport{}
	expectedUnder := map[string]bool{}
	for _, r := range cfg.Records {
		tl := timelines[r.Test]
		tr := byTest[r.Test]
		if tr == nil {
			tr = &TestReport{Test: r.Test, Counts: map[Phase]map[Op]map[Result]int{}, Buckets: map[Bucket]int{}, Examples: map[Bucket][]Request{}}
			if tl != nil {
				tr.Fault, tr.Inject, tr.Heal, tr.Marks = tl.fault, tl.inject, tl.heal, tl.marks
			}
			byTest[r.Test] = tr
			rep.Tests = append(rep.Tests, tr)
		}
		phase := phaseOf(tl, r)
		if tr.Counts[phase] == nil {
			tr.Counts[phase] = map[Op]map[Result]int{}
		}
		if tr.Counts[phase][r.Op] == nil {
			tr.Counts[phase][r.Op] = map[Result]int{}
		}
		tr.Counts[phase][r.Op][r.Result]++

		target := r.Target
		if _, isPod := t.Pod(target); !isPod {
			target = "" // through the Service
		}
		if r.Op == OpPut && r.Result == ResultOK && tl != nil {
			switch phase {
			case PhaseFault:
				if n := t.Unreachable(tl.faultAt(r.Start), r.Key, target); n < 0 {
					tr.HintsUnknown++
				} else {
					tr.Hints += n
				}
			case PhaseTransition:
				n := t.Unreachable(tl.faultAt(r.Start), r.Key, target)
				if n != 0 && tl.in.overlaps(r) {
					expectedUnder[r.Key] = true
					tr.UnderReplicated++
				}
				if n > 0 {
					tr.HintsMaybe += n
				}
			}
		}

		b := bucket(t, tl, phase, r, target, ackedAt)
		tr.Buckets[b]++
		if len(tr.Examples[b]) < cfg.Examples && b != AsPredicted && b != Dropped {
			tr.Examples[b] = append(tr.Examples[b], r)
		}
	}
	return expectedUnder
}

// maxListed bounds the keys a report lists.
const maxListed = 50

// joinReadback compares each key's latest read-back with its writes.
func joinReadback(cfg CheckConfig, ackedAt map[string]int64, expectedUnder map[string]bool, rep *Report) {
	latest := map[string]ReadbackEntry{}
	for _, e := range cfg.Readback {
		if l, ok := latest[e.Key]; !ok || e.Time >= l.Time {
			latest[e.Key] = e
		}
	}
	for _, k := range slices.Sorted(maps.Keys(ackedAt)) {
		rep.Acked++
		e, ok := latest[k]
		switch {
		case !ok:
			rep.NotReadBack++
			continue
		case e.Result == ResultNotFound || e.Result == ResultMismatch:
			rep.Lost++
			if len(rep.LostKeys) < maxListed {
				rep.LostKeys = append(rep.LostKeys, k)
			}
		case e.Result != ResultOK:
			rep.Unreadable++
		}
		if len(e.Owners) == 0 {
			continue
		}
		rep.CopiesChecked++
		if len(e.Holders) >= len(e.Owners) {
			continue
		}
		rep.UnderReplicated++
		if expectedUnder[k] && !cfg.Converged {
			rep.UnderReplicatedExpected++
		} else if len(rep.UnexpectedUnderReplicated) < maxListed {
			rep.UnexpectedUnderReplicated = append(rep.UnexpectedUnderReplicated, k)
		}
	}

	// Writes never acknowledged may or may not have persisted: count both.
	unacked := map[string]bool{}
	for _, r := range cfg.Records {
		if _, isAcked := ackedAt[r.Key]; r.Op == OpPut && !isAcked && (r.Result == ResultFailed || r.Result == ResultUnknown) {
			unacked[r.Key] = true
		}
	}
	for k := range unacked {
		switch latest[k].Result {
		case ResultOK:
			rep.UnackedPresent++
		case ResultNotFound:
			rep.UnackedAbsent++
		}
	}
}

func phaseOf(tl *testTimeline, r Request) Phase {
	if tl == nil {
		return PhaseNormal
	}
	if tl.in.overlaps(r) || tl.out.overlaps(r) || slices.ContainsFunc(tl.backs, func(w window) bool { return w.overlaps(r) }) {
		return PhaseTransition
	}
	if r.Start > tl.inject.UnixNano() && (tl.heal.IsZero() || r.End < tl.heal.UnixNano()) {
		return PhaseFault
	}
	return PhaseNormal
}

func bucket(t *Topology, tl *testTimeline, phase Phase, r Request, target string, ackedAt map[string]int64) Bucket {
	if r.Result == ResultDropped {
		return Dropped
	}
	if r.Op == OpGet && (r.Result == ResultNotFound || r.Result == ResultMismatch) {
		if at, ok := ackedAt[r.Key]; ok && at < r.Start {
			return StaleRead
		}
		if r.Result == ResultNotFound {
			return UnjudgedRead
		}
	}
	if phase == PhaseTransition {
		return Transition
	}
	expect := Ok
	if phase == PhaseFault {
		expect = t.Expect(tl.faultAt(r.Start), r.Op, r.Key, target)
	}
	answered := r.Result != ResultFailed && r.Result != ResultUnknown
	switch expect {
	case Fail:
		if answered {
			return UnexpectedSuccess
		}
	case TargetDown:
		if answered {
			return TargetDownOK
		}
	case Unknowable:
		return UnknowableBucket
	default:
		if !answered {
			if target == "" {
				return ServiceFailure
			}
			return UnexpectedFailure
		}
	}
	return AsPredicted
}

// OK reports whether the check passed: no acknowledged write lost, not read
// back or unreadable, no stale read, no request against its prediction, and
// no under-replication beyond the predicted.
func (rep *Report) OK() bool {
	for _, tr := range rep.Tests {
		for _, b := range violations {
			if tr.Buckets[b] > 0 {
				return false
			}
		}
	}
	return rep.Lost == 0 && rep.NotReadBack == 0 && rep.Unreadable == 0 && len(rep.UnexpectedUnderReplicated) == 0
}

// WriteText writes rep for a reader.
func (rep *Report) WriteText(w io.Writer) {
	for _, tr := range rep.Tests {
		fmt.Fprintf(w, "== test %s", tr.Test)
		if tr.Fault != nil {
			fmt.Fprintf(w, ": %s, injected %s", tr.Fault, tr.Inject.UTC().Format(time.RFC3339Nano))
			if !tr.Heal.IsZero() {
				fmt.Fprintf(w, ", healed %s (%v)", tr.Heal.UTC().Format(time.RFC3339Nano), tr.Heal.Sub(tr.Inject).Round(time.Second))
			}
		}
		fmt.Fprintln(w)
		if len(tr.Marks) > 0 {
			events := slices.Clone(tr.Marks)
			if !tr.Heal.IsZero() {
				events = append(events, Event{Time: tr.Heal, Mark: EventHeal})
			}
			slices.SortStableFunc(events, func(a, b Event) int { return a.Time.Compare(b.Time) })
			steps := []string{"+0s inject"}
			for _, m := range events {
				steps = append(steps, fmt.Sprintf("%+.1fs %s", m.Time.Sub(tr.Inject).Seconds(), m.Mark))
			}
			fmt.Fprintf(w, "  timeline:  %s\n", strings.Join(steps, ", "))
		}
		for _, ph := range []Phase{PhaseNormal, PhaseTransition, PhaseFault} {
			for _, op := range []Op{OpPut, OpGet} {
				c := tr.Counts[ph][op]
				if len(c) == 0 {
					continue
				}
				var parts []string
				for _, res := range slices.Sorted(maps.Keys(c)) {
					parts = append(parts, fmt.Sprintf("%s=%d", res, c[res]))
				}
				fmt.Fprintf(w, "  %-10s %-3s  %s\n", ph, op, strings.Join(parts, " "))
			}
		}
		var parts []string
		for _, b := range slices.Sorted(maps.Keys(tr.Buckets)) {
			mark := ""
			if slices.Contains(violations, b) {
				mark = "!"
			}
			parts = append(parts, fmt.Sprintf("%s%s=%d", mark, b, tr.Buckets[b]))
		}
		fmt.Fprintf(w, "  compared:  %s\n", strings.Join(parts, " "))
		if tr.Fault != nil {
			fmt.Fprintf(w, "  predicted: hints %d to %d", tr.Hints, tr.Hints+tr.HintsMaybe)
			if tr.HintsUnknown > 0 {
				fmt.Fprintf(w, " (+ up to %d through the Service)", tr.HintsUnknown)
			}
			fmt.Fprintf(w, "; under-replicated until anti-entropy %d\n", tr.UnderReplicated)
		}
		for _, b := range slices.Sorted(maps.Keys(tr.Examples)) {
			for _, r := range tr.Examples[b] {
				fmt.Fprintf(w, "  e.g. %-18s %s %s via %s at %s: %s %s %s\n", b, r.Op, r.Key, r.Target,
					time.Unix(0, r.Start).UTC().Format("15:04:05.000"), r.Result, r.Code, r.Msg)
			}
		}
	}
	if !rep.HaveReadback {
		fmt.Fprintln(w, "== read-back: none given")
	} else {
		fmt.Fprintf(w, "== read-back: %d acknowledged writes: lost %d, not read back %d, unreadable %d\n",
			rep.Acked, rep.Lost, rep.NotReadBack, rep.Unreadable)
		for _, k := range rep.LostKeys {
			fmt.Fprintf(w, "  LOST %s\n", k)
		}
		fmt.Fprintf(w, "  unacknowledged writes: present %d, absent %d (either is allowed)\n", rep.UnackedPresent, rep.UnackedAbsent)
		if rep.CopiesChecked > 0 {
			fmt.Fprintf(w, "  copies: %d checked, %d on fewer than all owners: %d predicted, %d not",
				rep.CopiesChecked, rep.UnderReplicated, rep.UnderReplicatedExpected, rep.UnderReplicated-rep.UnderReplicatedExpected)
			if rep.Converged {
				fmt.Fprint(w, " (converged: none allowed)")
			}
			fmt.Fprintln(w)
			for _, k := range rep.UnexpectedUnderReplicated {
				fmt.Fprintf(w, "  UNDER-REPLICATED %s\n", k)
			}
		}
	}
	if rep.OK() {
		fmt.Fprintln(w, "== PASS")
	} else {
		fmt.Fprintln(w, "== FAIL")
	}
}
