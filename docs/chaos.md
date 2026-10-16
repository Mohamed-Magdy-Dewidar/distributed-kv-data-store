# Failure tests

The failure tests of GKE session 3 ([gke.md](gke.md)): a fault injected under
load, with a written prediction before it, and every request compared with
the prediction afterwards, key by key. The tests:

1. **VM loss**: one database VM deleted.
2. **Zone loss**: both database nodes of one zone gone.
3. **Network partition**: a group of pods cut off from the other members by
   a NetworkPolicy, then healed.
4. **System-node loss**: the node with the operator, Prometheus, Grafana and
   kube-dns gone; the database must keep serving with zero errors.

For all of them: zero acknowledged writes lost, and the operator recovers
without changing the membership (the epoch does not move).

This page describes the tools, and the rehearsal of all four on kind that
checked them before the paid session.

## The tools

`kvchaos` (`cmd/kvchaos`, logic in `internal/chaos`) ships in the node image
as `/kvchaos`. `deploy/chaos/chaos.sh` drives it, on kind or on GKE
(`chaos.sh` with no arguments lists its commands).

| Command | Does |
|---|---|
| `kvchaos topology` | Captures the membership, each pod's node and zone, and the quorum settings and timeouts, from kubectl's JSON and every pod's `GET /admin/membership`. Refuses a cluster that is not settled (epochs or members differ, a peer not alive and reachable, a pod not running, a node without a zone label), and a membership whose fingerprint is not the one it computes: then its ring is the nodes' ring. |
| `kvchaos predict` | For every single-pod, zone and zone-partition fault (or one, `-fault`), how many keys fail for a Put and a Get from each side, and with `-per-key` each key's replicas, zones and outcomes. |
| `kvchaos record` | An open-model load (writes of new keys, reads of preloaded ones), sent straight to each pod over a connection with gRPC keepalive, or through the Service (`-service`, the realism run). One JSON line per request: test, op, key, target pod, start, end, result (`ok`, `failed`, `unknown`, `notfound`, `mismatch`, `dropped`), gRPC code and message. |
| `kvchaos readback` | Reads back every key a run wrote, and with `-copies` asks each owner for its own copy (`FetchItem`). |
| `kvchaos check` | Compares each request with its prediction, for its phase of the fault timeline (`faults.jsonl`), and each acknowledged write with its read-back. Fails on a lost or stale value, a request against its prediction, or under-replication nobody predicted. |

**Quorums are strict.** A write counts acknowledgments only from the key's N
replicas (`internal/node/node.go`, `putAsReplica`: the local write is one,
the fan-out goes to the preference list only, and a hint is stored after
the decision and never counts toward W); a read likewise counts responses
from the N replicas only (`Get`). With N=3 and W=R=2, a key fails for writes
and reads alike exactly when two or more of its replicas are unreachable from
the coordinator. The ring is not zone-aware, so which keys a zone takes down
depends on which pods share it: `predict` computes it from the real
placement.

**Phases.** The check splits each test's requests into *normal* (before the
fault, after its healing: expect ok), *fault* (expect the prediction) and
*transition*: the requests that overlap the time the nodes take to notice
the fault (heartbeats: 3 missed at 1s, plus a 500ms timeout) or its healing
(from the `serving` mark, when the pods answer again, to the heal plus the
reconnect backoff and two heartbeat rounds), widened by a 2s clock
tolerance. A transition request may have any outcome but a lost or stale
value.

**The keepalive floor.** grpc-go raises a client keepalive time under 10s to
10s, so the recorder closes a connection to a pod that vanished silently
about 13s after the pod's last answer. Until then that pod is still picked,
and its requests time out: they are recorded `unknown`, predicted
`target-down`, and count as predicted.

## The kind rehearsal cluster

`deploy/chaos/kind-config.yaml`: the kind cluster `kvstore-chaos`, next to
`kvstore` (stop that one first: `docker stop kvstore-control-plane`; both
together do not fit a 16 GB laptop). One control plane and three workers,
labelled as zones `zone-a`, `zone-b`, `zone-c`; the KVCluster
(`deploy/chaos/kvcluster-kind.yaml`) puts two pods on each worker, so a
worker is a zone. The control plane stands for GKE's system node: `kind-up`
moves the operator and CoreDNS there, and the recorders run there.

```sh
export CHAOS_CONTEXT=kind-kvstore-chaos CHAOS_OUT=docs/results/chaos/<date>-kind
deploy/chaos/chaos.sh kind-up     # images, cluster, Cilium, operator, KVCluster
deploy/chaos/chaos.sh topology    # topology.json and predictions.txt
deploy/chaos/chaos.sh preload
deploy/chaos/chaos.sh watch-start # the operator's status changes, to operator.log
```

kind's own network plugin ignores NetworkPolicy, so `kind-up` installs
Cilium (1.19.8), which GKE's Dataplane V2 is built on, rendered with a pinned
helm image (no local helm needed). It uses Cilium's native routing: through
the default VXLAN tunnel, Docker Desktop's MTU stalled TLS between nodes (a
worker's pods could not reach the API server).

## Rehearsal on kind, 2026-10-13

From `feature/chaos` with these tools uncommitted; 6 pods, N=3 W=R=2;
writes and reads at 50/s each, straight to the pods (64 in flight at most);
5,000 preloaded keys, all acknowledged. Zones: a = {kv-2, kv-5}, b = {kv-0,
kv-4}, c = {kv-1, kv-3}. The results, removed once written up here, are in
git history at `ac62c36`, in `docs/results/chaos/20261013-kind/`: the
topology, predictions, fault timeline, hint counts per test, operator
status changes and the check's report (the per-request records and
read-backs, 66 MB, were never committed).

| Test | Fault on kind | Prediction | Measured | Verdict |
|---|---|---|---|---|
| t1 | kv-3 force-deleted | No key fails; requests to kv-3 fail | kv-3 shut down cleanly (a forced delete still sends SIGTERM) and was Ready again in 2s; the recorder got a GOAWAY and skipped it: 0 errors in 12,502 requests | As predicted, but too gentle to test much: t1s |
| t1s | kv-3 frozen (SIGSTOP), until liveness restarts it | No key fails; requests to kv-3 time out (target-down); writes in the detection window may stay under-replicated | 170 requests to kv-3 unknown, all predicted; 0 keys failed; 41 writes with 2 copies, all among the 100 predicted; restarted after **85s** | Pass |
| t1n | zone a's worker killed (`docker kill`), started 3 min later | 19.6% of keys fail; Degraded ~2 min after the pods turn unready | 20.0% of reads failed in the fault phase, every failure predicted; Degraded at +2m55s (pods unready at +52s), Ready at the heal, epoch 0 | Pass |
| t2 | zone b's worker frozen (`docker pause`) for 200s, plus a recorder through kv-client | 26.3% of keys fail; Degraded | Every request straight to a pod as predicted; Degraded at +2m53s, Ready at the heal, epoch 0. Through kv-client: **1,694 failures** the key prediction did not expect | Pass; kv-client finding below |
| t3a | zone c's pods partitioned from the others (egress-only NetworkPolicies) for 120s | From the group, 91.9% of keys fail; from the rest, 8.1%; every key works on exactly one side; the operator stays Ready | Every request as predicted; no pod restarted; the operator stayed Ready | Pass; heal finding below |
| t4 | CoreDNS and the operator scaled to 0 for 150s | Zero errors | 29,502 requests, 0 errors; the operator came back Ready at epoch 0 | Pass |

Over all six: 138,006 acknowledged writes, 0 lost, 0 stale reads, every
copy count as predicted. At 17:33 UTC, a full anti-entropy interval (15
minutes) after the last test, the convergence read-back found all 138,006 on
all three of their owners, the 53 that had two copies included
(`check-final.txt`). 3b (the group cut off from clients and the operator
too) was not rehearsed: it needs ingress rules, which block the kubelet's
probes on this Cilium (below), so the pods are restarted and the test
becomes a crash.

**Hints** (created, from each pod's `kv_hints_created_total` before and
after the test, `hints.txt`):

| Test | Predicted | Measured |
|---|---|---|
| t1 + t1s | 1,562 to 2,256 | 1,657 |
| t1n | 6,451 to 7,491 | between 6,185 and 7,842: kv-2 and kv-5 restarted, which reset their counters |
| t2 | 6,549 to 8,374 | 6,598 |
| t3a | 2,708 to 3,401 | 3,594: two pairs stayed cut after the heal (below) |
| t4 | 0 | 0 |

### Findings

- **NetworkPolicies cannot select pods by `statefulset.kubernetes.io/pod-name`
  under Cilium.** Cilium leaves that label out of its security identities, so
  a policy selecting on it selects nothing, and is not enforced at all: the
  first partition cut nothing (`cilium-dbg endpoint list` showed enforcement
  `Disabled`). `chaos.sh` labels the pods `kvchaos/partition=group|rest` and
  selects on that. A new label changes a pod's identity, which took 5 to 30s;
  `partition` labels first, waits for the new identities, then applies the
  policies, so the cut follows the inject within a second. GKE's Dataplane V2
  is Cilium: expect the same.
- **An ingress policy blocked the kubelet's probes**, although Cilium reports
  `AllowLocalhost: always`: liveness then killed the cut-off pods (twice each
  in two minutes), and their peers saw restarts, not a partition. `chaos.sh`
  partitions with egress-only policies on both sides: each side may open
  connections only to itself and DNS; inbound connections (the kubelet,
  the recorders, the operator) are untouched. Whether GKE's Dataplane V2
  lets probes through an ingress policy is open.
- **After a partition heals under load, some peer pairs stay cut for up to
  minutes.** In t3a, kv-1↔kv-5 and kv-3↔kv-4 saw each other again only 104s
  after the heal (both pairs at the same second); in a rerun (60s partition),
  kv-0↔kv-3 after 45 to 55s. Without load every pair recovered within 10s.
  The peer connections have no gRPC keepalive (`internal/rpc`, `dial`): a
  connection that had data in flight when the cut started backs off its TCP
  retransmissions, and resumes only at the next retransmission after the
  heal; heartbeats share it, so the peer stays dead meanwhile, and writes to
  it are hinted (the extra hints of t3a). **Fixed since** by a client
  keepalive on peer connections: see
  [Peer keepalive: the partition again](#peer-keepalive-the-partition-again).
- **A frozen pod takes 85s to be restarted**: liveness gives up after 30s
  (3 failures 10s apart), and the kubelet's SIGTERM waits out the pod's 60s
  termination grace, since a stopped process cannot exit.
- **Clients through kv-client fail where the database does not.** kv-client
  balances connections, not requests; a connection to a frozen pod has no
  keepalive in the client either, so every request on it timed out for the
  whole 200s (1,694 failures, against none unpredicted for the recorder
  that goes straight to the pods and avoids pods it cannot reach).
- **The recorder's workers were exhausted by a vanished pod.** Its requests
  hang for their 5s timeout until keepalive closes the connection (~13s), and
  at 64 in flight they took every worker (276 dropped in t1s, 4,654 in t2).
  `chaos.sh` uses 256 on GKE (`CONCURRENCY`).
- **Hint delivery is slow**: about 5 hints/s per sender on kind (one
  Replicate and one fsync at a time); kv-0 took about 2 minutes for 2,400.
  A read-back before delivery ends counts copies still on their way:
  `chaos.sh wait-hints` waits for every pod's `kv_hints_pending` to reach 0
  first (not created minus delivered: a hint delivered but not yet marked so
  is delivered again, and counted again).
- **A paused node's pods come back Ready with their old
  `lastTransitionTime`** (the kubelet still holds True): `wait-ready` counts
  a pod it has seen not Ready, not only a newer transition time.
- **A rerun must not write the keys of an earlier run**: same keys, same
  values, so the earlier run's copies would hide a lost write. Each run
  writes under a prefix of its own (`chaos-<test>-<start time>`).

## Peer keepalive: the partition again

The rehearsal's heal finding, fixed: every peer connection now has a gRPC
client keepalive (`internal/rpc`, `PeerKeepalive`: a ping after 10s with
nothing received, the connection closed if it is not answered within 3s,
with or without an RPC open), and the server allows those pings
(`KeepaliveEnforcement`: one every 5s at most, with or without an RPC
open; gRPC's default answers them with a GOAWAY, `too_many_pings`). A
connection a partition broke is now closed and redialed instead of waiting
for TCP's next retransmission. `internal/rpc/keepalive_test.go` shows both:
a connection to a peer that went silent leaves Ready 13s later and the next
RPC after the peer is back goes over a new connection, while a connection
without keepalive stays Ready; and an idle connection survives the pings on
a server from `Serve`, while gRPC's default policy closed it at the third
ping (30s).

Rerun on kind, 2026-10-13, on the same cluster with every pod restarted on
the new image (each pod's `/kvnode` checked against the build): the
rehearsal's t3a (zone c, kv-1 and kv-3, partitioned for 120s under load)
twice, with every member's view of every other polled each second after
the heal. Results in
[results/chaos/20261013-kind-keepalive/](results/chaos/20261013-kind-keepalive/).

| Run | Peer keepalive | Every pair alive again after the heal | Hints: predicted, measured |
|---|---|---|---|
| t3a (rehearsal) | no | **104s**: kv-1↔kv-5 and kv-3↔kv-4; the other pairs 1 to 7s | 2,708 to 3,401; 3,594 |
| control, 60s partition (rehearsal) | no | **45 to 55s**: kv-0↔kv-3 | not measured |
| control, no load (rehearsal) | no | within 10s | none |
| t3k1 | yes | **7s** | 2,489 to 3,246; 2,695 |
| t3k2 | yes | **7s** | 2,496 to 3,261; 2,731 |

In both reruns every request was as predicted, no pod restarted, and the
check passed: 25,231 acknowledged writes (with the preload), none lost,
all on every owner. The hints are back inside the predicted range: the
writes that went to hints in t3a only because a pair stayed cut are gone.

## A test, by hand

```sh
c=deploy/chaos/chaos.sh
$c hints-snapshot t2 before
$c run t2                              # the recorder, on the control plane
$c mark t2 inject zone:zone-b          # then: docker pause kvstore-chaos-worker2
$c wait-node-down t2 kvstore-chaos-worker2  # marks node-notready-kvstore-chaos-worker2
$c mark t2 vm-unpaused                 # any other event name is a mark; then: docker unpause kvstore-chaos-worker2
$c wait-node t2 kvstore-chaos-worker2  # marks node-ready-kvstore-chaos-worker2
$c wait-ready t2 kv-0 kv-4             # marks serving, pod-ready-*, heal
$c stop t2
$c wait-hints t2
$c hints-snapshot t2 after
$c readback t2
$c check
```

A partition: `$c partition t3a kv-1 kv-3` (marks inject), then
`$c unpartition t3a` (marks heal), and `$c unlabel` once the run is over.
After the last test and a full anti-entropy interval (15 minutes):
`$c converge` and `$c check-final`, which requires every acknowledged write
on all its owners.

On GKE, `$c vm-loss TEST POD...` (tests 1 and 2) and `$c system-loss TEST`
(test 4) inject the fault through the managed instance groups and mark
every recovery step themselves: see [gke.md](gke.md#session-3-the-failure-tests-runbook).
