package telemetry

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"distributed-kv-datastore/internal/hints"
	"distributed-kv-datastore/internal/node"
	"distributed-kv-datastore/internal/storage/engine"
)

type sample struct {
	typ   dto.MetricType
	value float64
}

// kvSeries flattens the kv_* families of a scrape (histograms aside) to
// `name{label="value",...}` -> type and value.
func kvSeries(families map[string]*dto.MetricFamily) map[string]sample {
	out := make(map[string]sample)
	for name, f := range families {
		if !strings.HasPrefix(name, "kv_") || f.GetType() == dto.MetricType_HISTOGRAM {
			continue
		}
		for _, m := range f.Metric {
			var labels []string
			for _, l := range m.Label {
				labels = append(labels, fmt.Sprintf("%s=%q", l.GetName(), l.GetValue()))
			}
			sort.Strings(labels)
			key := name
			if len(labels) > 0 {
				key += "{" + strings.Join(labels, ",") + "}"
			}
			v := m.GetGauge().GetValue()
			if f.GetType() == dto.MetricType_COUNTER {
				v = m.GetCounter().GetValue()
			}
			out[key] = sample{f.GetType(), v}
		}
	}
	return out
}

// distinctStats has a different value in every field, so that a value
// exported under the wrong name or label shows.
func distinctStats() node.Stats {
	return node.Stats{
		Epoch: 7, Member: true,
		Peers: 9, PeersAlive: 8, PeersReachable: 6,
		MembershipConflicts: 3, PingsReceived: 1234,
		WriteQuorumFailures: 11, ReadQuorumFailures: 12,
		Data: engine.Stats{MemtableBytes: 4096, SSTables: 5, Flushes: 21, FlushFailures: 22, Compactions: 23, CompactionFailures: 24},
		Hints: hints.Stats{Created: 31, Delivered: 32,
			Engine: engine.Stats{MemtableBytes: 512, SSTables: 2, Flushes: 41, FlushFailures: 42, Compactions: 43, CompactionFailures: 44}},
		HintsPending: 33, HintsPendingKnown: true,
		Handoff:           node.HandoffStatus{Epoch: 6, Done: true, Pushed: 51, Hinted: 52, Pending: 53},
		HandoffsCompleted: 54, HandoffLast: 1500 * time.Millisecond, HandoffTotal: 90 * time.Second,
		AntiEntropyRounds: 61, AntiEntropyLastRound: 250 * time.Millisecond, AntiEntropyTotal: 30 * time.Second,
		AntiEntropyPeerFailures: 62, AntiEntropyKeysPulled: 63, AntiEntropyKeysPushed: 64,
	}
}

const (
	gauge   = dto.MetricType_GAUGE
	counter = dto.MetricType_COUNTER
)

// TestNodeStatsAreExported: every Stats value is served, under its own
// name, labels and type, and nothing else is.
func TestNodeStatsAreExported(t *testing.T) {
	s := distinctStats()
	got := kvSeries(scrape(t, New(func() (node.Stats, bool) { return s, true }).Handler()))
	want := map[string]sample{
		`kv_membership_epoch`:                                    {gauge, 7},
		`kv_membership_member`:                                   {gauge, 1},
		`kv_membership_conflicts_total`:                          {counter, 3},
		`kv_pings_received_total`:                                {counter, 1234},
		`kv_peers`:                                               {gauge, 9},
		`kv_peers_alive`:                                         {gauge, 8},
		`kv_peers_reachable`:                                     {gauge, 6},
		`kv_quorum_failures_total{quorum="read"}`:                {counter, 12},
		`kv_quorum_failures_total{quorum="write"}`:               {counter, 11},
		`kv_memtable_estimated_bytes{engine="data"}`:             {gauge, 4096},
		`kv_memtable_estimated_bytes{engine="hints"}`:            {gauge, 512},
		`kv_sstables{engine="data"}`:                             {gauge, 5},
		`kv_sstables{engine="hints"}`:                            {gauge, 2},
		`kv_flushes_total{engine="data",result="ok"}`:            {counter, 21},
		`kv_flushes_total{engine="data",result="error"}`:         {counter, 22},
		`kv_flushes_total{engine="hints",result="ok"}`:           {counter, 41},
		`kv_flushes_total{engine="hints",result="error"}`:        {counter, 42},
		`kv_compactions_total{engine="data",result="ok"}`:        {counter, 23},
		`kv_compactions_total{engine="data",result="error"}`:     {counter, 24},
		`kv_compactions_total{engine="hints",result="ok"}`:       {counter, 43},
		`kv_compactions_total{engine="hints",result="error"}`:    {counter, 44},
		`kv_hints_created_total`:                                 {counter, 31},
		`kv_hints_delivered_total`:                               {counter, 32},
		`kv_hints_pending`:                                       {gauge, 33},
		`kv_handoff_epoch`:                                       {gauge, 6},
		`kv_handoff_done`:                                        {gauge, 1},
		`kv_handoff_pushed`:                                      {gauge, 51},
		`kv_handoff_hinted`:                                      {gauge, 52},
		`kv_handoff_pending`:                                     {gauge, 53},
		`kv_handoffs_completed_total`:                            {counter, 54},
		`kv_handoff_last_duration_seconds`:                       {gauge, 1.5},
		`kv_handoff_duration_seconds_total`:                      {counter, 90},
		`kv_antientropy_rounds_total`:                            {counter, 61},
		`kv_antientropy_last_round_duration_seconds`:             {gauge, 0.25},
		`kv_antientropy_round_duration_seconds_total`:            {counter, 30},
		`kv_antientropy_peer_failures_total`:                     {counter, 62},
		`kv_antientropy_keys_repaired_total{direction="pulled"}`: {counter, 63},
		`kv_antientropy_keys_repaired_total{direction="pushed"}`: {counter, 64},
	}
	for key, w := range want {
		g, ok := got[key]
		if !ok {
			t.Errorf("%s missing", key)
			continue
		}
		if g != w {
			t.Errorf("%s = %v %v, want %v %v", key, g.typ, g.value, w.typ, w.value)
		}
	}
	for _, key := range slices.Sorted(maps.Keys(got)) {
		if _, ok := want[key]; !ok {
			t.Errorf("unexpected series %s", key)
		}
	}
}

// TestValuesNotYetKnownAreAbsent: the pending-hints gauge before a delivery
// round has completed, and the last-duration gauges before a handoff or an
// anti-entropy round has, are left out rather than served as 0. False
// booleans are 0.
func TestValuesNotYetKnownAreAbsent(t *testing.T) {
	got := kvSeries(scrape(t, New(func() (node.Stats, bool) { return node.Stats{}, true }).Handler()))
	for _, key := range []string{"kv_hints_pending", "kv_handoff_last_duration_seconds", "kv_antientropy_last_round_duration_seconds"} {
		if v, ok := got[key]; ok {
			t.Errorf("%s served as %v before it is known", key, v.value)
		}
	}
	for _, key := range []string{"kv_membership_member", "kv_handoff_done", "kv_hints_created_total", "kv_antientropy_rounds_total"} {
		if v, ok := got[key]; !ok || v.value != 0 {
			t.Errorf("%s = %v (present %v), want 0", key, v.value, ok)
		}
	}
}
