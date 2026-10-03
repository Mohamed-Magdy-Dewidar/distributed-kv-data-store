package telemetry

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"distributed-kv-datastore/internal/node"
	"distributed-kv-datastore/internal/storage/engine"
)

func desc(name, help string, labels ...string) *prometheus.Desc {
	return prometheus.NewDesc(name, help, labels, nil)
}

// What node.Stats reports, as metrics. Counters count since the node
// started; a restart starts them again from zero.
var (
	membershipEpoch     = desc("kv_membership_epoch", "Membership epoch of the view this node holds.")
	membershipMember    = desc("kv_membership_member", "1 if this node is in the view it holds, 0 if it has been removed (and is draining).")
	membershipConflicts = desc("kv_membership_conflicts_total", "Times a peer was seen holding a different membership at the same epoch (counted on every heartbeat that shows it).")
	pingsReceived       = desc("kv_pings_received_total", "Heartbeat pings received from other nodes.")
	peers               = desc("kv_peers", "Other members in this node's view.")
	peersAlive          = desc("kv_peers_alive", "Other members not marked dead by heartbeats (a peer never pinged counts as alive).")
	peersReachable      = desc("kv_peers_reachable", "Other members whose last heartbeat ping was answered.")

	quorumFailures = desc("kv_quorum_failures_total", "Reads this node coordinated that did not reach R responses, and writes that did not reach W acks (and were rolled back).", "quorum")

	memtableBytes = desc("kv_memtable_estimated_bytes", "Estimated size of a storage engine's memtables held in memory: the active one and one being flushed.", "engine")
	sstables      = desc("kv_sstables", "Live SSTables of a storage engine.", "engine")
	flushes       = desc("kv_flushes_total", "Memtable flushes of a storage engine, by result: ok (an SSTable was registered) or error (the data stays in memory and is retried).", "engine", "result")
	compactions   = desc("kv_compactions_total", "Compaction steps of a storage engine that merged a run, by result.", "engine", "result")

	hintsCreated   = desc("kv_hints_created_total", "Hinted items stored for unreachable replicas.")
	hintsDelivered = desc("kv_hints_delivered_total", "Hinted items delivered to their replica and retired.")
	hintsPending   = desc("kv_hints_pending", "Hinted items not yet delivered, as of the end of the last hint-delivery round (not live). Absent until a round has completed.")

	handoffEpoch        = desc("kv_handoff_epoch", "Epoch of the view the current (or last) handoff moves data to.")
	handoffDone         = desc("kv_handoff_done", "1 once every key that had to move for the current handoff has been pushed or hinted.")
	handoffPushed       = desc("kv_handoff_pushed", "Pushes to a new owner that succeeded, in the current handoff.")
	handoffHinted       = desc("kv_handoff_hinted", "Pushes that failed and became hints, in the current handoff.")
	handoffPending      = desc("kv_handoff_pending", "Keys the current handoff has not handled yet.")
	handoffsCompleted   = desc("kv_handoffs_completed_total", "Handoffs that completed.")
	handoffLastDuration = desc("kv_handoff_last_duration_seconds", "How long the last completed handoff took. Absent until one has completed.")
	handoffDuration     = desc("kv_handoff_duration_seconds_total", "How long all completed handoffs took, together.")

	aeRounds            = desc("kv_antientropy_rounds_total", "Anti-entropy rounds run (each against every other member).")
	aeLastRoundDuration = desc("kv_antientropy_last_round_duration_seconds", "How long the last anti-entropy round took. Absent until one has run.")
	aeRoundDuration     = desc("kv_antientropy_round_duration_seconds_total", "How long all anti-entropy rounds took, together.")
	aePeerFailures      = desc("kv_antientropy_peer_failures_total", "Reconciliations with one peer that failed.")
	aeKeysRepaired      = desc("kv_antientropy_keys_repaired_total", "Keys for which anti-entropy installed versions locally (pulled) or sent them to the peer (pushed).", "direction")
)

// nodeCollector turns one node.Stats, read at scrape time, into metrics.
type nodeCollector struct {
	stats func() (node.Stats, bool)
}

func (c *nodeCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		membershipEpoch, membershipMember, membershipConflicts, pingsReceived, peers, peersAlive, peersReachable,
		quorumFailures,
		memtableBytes, sstables, flushes, compactions,
		hintsCreated, hintsDelivered, hintsPending,
		handoffEpoch, handoffDone, handoffPushed, handoffHinted, handoffPending, handoffsCompleted, handoffLastDuration, handoffDuration,
		aeRounds, aeLastRoundDuration, aeRoundDuration, aePeerFailures, aeKeysRepaired,
	} {
		ch <- d
	}
}

func (c *nodeCollector) Collect(ch chan<- prometheus.Metric) {
	s, ok := c.stats()
	if !ok {
		return
	}
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	counter := func(d *prometheus.Desc, v uint64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, float64(v), labels...)
	}

	gauge(membershipEpoch, float64(s.Epoch))
	gauge(membershipMember, boolValue(s.Member))
	counter(membershipConflicts, s.MembershipConflicts)
	counter(pingsReceived, s.PingsReceived)
	gauge(peers, float64(s.Peers))
	gauge(peersAlive, float64(s.PeersAlive))
	gauge(peersReachable, float64(s.PeersReachable))

	counter(quorumFailures, s.ReadQuorumFailures, "read")
	counter(quorumFailures, s.WriteQuorumFailures, "write")

	for _, e := range []struct {
		name  string
		stats engine.Stats
	}{{"data", s.Data}, {"hints", s.Hints.Engine}} {
		gauge(memtableBytes, float64(e.stats.MemtableBytes), e.name)
		gauge(sstables, float64(e.stats.SSTables), e.name)
		counter(flushes, e.stats.Flushes, e.name, "ok")
		counter(flushes, e.stats.FlushFailures, e.name, "error")
		counter(compactions, e.stats.Compactions, e.name, "ok")
		counter(compactions, e.stats.CompactionFailures, e.name, "error")
	}

	counter(hintsCreated, s.Hints.Created)
	counter(hintsDelivered, s.Hints.Delivered)
	if s.HintsPendingKnown {
		gauge(hintsPending, float64(s.HintsPending))
	}

	gauge(handoffEpoch, float64(s.Handoff.Epoch))
	gauge(handoffDone, boolValue(s.Handoff.Done))
	gauge(handoffPushed, float64(s.Handoff.Pushed))
	gauge(handoffHinted, float64(s.Handoff.Hinted))
	gauge(handoffPending, float64(s.Handoff.Pending))
	counter(handoffsCompleted, s.HandoffsCompleted)
	if s.HandoffsCompleted > 0 {
		gauge(handoffLastDuration, seconds(s.HandoffLast))
	}
	ch <- prometheus.MustNewConstMetric(handoffDuration, prometheus.CounterValue, seconds(s.HandoffTotal))

	counter(aeRounds, s.AntiEntropyRounds)
	if s.AntiEntropyRounds > 0 {
		gauge(aeLastRoundDuration, seconds(s.AntiEntropyLastRound))
	}
	ch <- prometheus.MustNewConstMetric(aeRoundDuration, prometheus.CounterValue, seconds(s.AntiEntropyTotal))
	counter(aePeerFailures, s.AntiEntropyPeerFailures)
	counter(aeKeysRepaired, s.AntiEntropyKeysPulled, "pulled")
	counter(aeKeysRepaired, s.AntiEntropyKeysPushed, "pushed")
}

func boolValue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func seconds(d time.Duration) float64 { return d.Seconds() }
