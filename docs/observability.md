# Observability

Every node serves its metrics on `GET /metrics`, on the same port as its
probes and admin API (`listen.http`, 8080 in `deploy/k8s/`), in the
Prometheus text format. `deploy/observability/` runs a local Prometheus and
Grafana on the kind cluster from [kubernetes.md](kubernetes.md) to collect
and show them.

## Metrics

Counters count since the node started; a restart starts them from zero
(Prometheus's `rate` and `increase` handle that). Values marked *read at
scrape* are read from state the node already keeps when `/metrics` is
scraped.

**Requests and storage timings**

| Metric | Type | Labels | What it measures |
|---|---|---|---|
| `kv_client_request_duration_seconds` | histogram | `method` (`Get`, `Put`), `code` (gRPC status code) | Time to serve a client request on the node that received it, including forwarding to a replica and waiting for the quorum. Only `KVClient` calls; node-to-node calls are not timed. |
| `kv_wal_fsync_duration_seconds` | histogram | `engine` (`data`, `hints`) | Time to fsync one write to a storage engine's write-ahead log: the node's data, or its hint store. Replaying the WAL at startup is not included. |
| `kv_quorum_failures_total` | counter | `quorum` (`read`, `write`) | Reads this node coordinated that did not reach R responses, and writes that did not reach W acks (and were rolled back). A client sees both as `Unavailable`, like other failures. |

**Membership and peers**

| Metric | Type | What it measures |
|---|---|---|
| `kv_membership_epoch` | gauge | The epoch of the membership view the node holds. Read at scrape. |
| `kv_membership_member` | gauge | 1 while the node is in the view it holds, 0 once it has been removed and is draining. Read at scrape. |
| `kv_membership_conflicts_total` | counter | Times a peer was seen holding a different membership at the same epoch, counted on every heartbeat that shows it (not once per peer). |
| `kv_pings_received_total` | counter | Heartbeat pings received from other nodes. |
| `kv_peers` | gauge | Other members in the node's view. Read at scrape. |
| `kv_peers_alive` | gauge | Other members not marked dead by heartbeats (`health.maxMissedHeartbeats` misses in a row). A peer never pinged counts as alive. Read at scrape. |
| `kv_peers_reachable` | gauge | Other members whose last heartbeat ping was answered. It drops on the first miss, before the peer is marked dead. Read at scrape. |

**Storage engines** (`engine` is `data` for the node's data, `hints` for
its hint store)

| Metric | Type | Labels | What it measures |
|---|---|---|---|
| `kv_memtable_estimated_bytes` | gauge | `engine` | Estimated size of the memtables in memory: the active one and one being flushed. The estimate the flush threshold (`storage.memtableBytes`) is compared with. Read at scrape. |
| `kv_sstables` | gauge | `engine` | Live SSTables. Read at scrape. |
| `kv_flushes_total` | counter | `engine`, `result` (`ok`, `error`) | Memtable flushes. On `error` the data stays in memory and the next flush retries it. |
| `kv_compactions_total` | counter | `engine`, `result` (`ok`, `error`) | Compaction steps that merged a run. A step with nothing to compact is not counted. |

**Hinted handoff**

| Metric | Type | What it measures |
|---|---|---|
| `kv_hints_created_total` | counter | Hinted items stored for replicas that were unreachable (by a write, or by a membership handoff). Items, not keys: two concurrent versions of a key are two. |
| `kv_hints_delivered_total` | counter | Hinted items delivered to their replica and retired. |
| `kv_hints_pending` | gauge | Hinted items not yet delivered, **as of the end of the node's last hint-delivery round** (every `intervals.hintDelivery`), not live. Absent until a round has completed. |

**Membership handoff** (moving data after a membership change)

| Metric | Type | What it measures |
|---|---|---|
| `kv_handoff_epoch` | gauge | Epoch of the view the current (or last) handoff moves data to. Read at scrape. |
| `kv_handoff_done` | gauge | 1 once every key that had to move has been pushed or hinted. Read at scrape. |
| `kv_handoff_pushed`, `kv_handoff_hinted` | gauge | Pushes to new owners that succeeded, and pushes that failed and became hints, in the current handoff. They start again at 0 with each handoff. Read at scrape. |
| `kv_handoff_pending` | gauge | Keys the current handoff has not handled yet. Read at scrape. |
| `kv_handoffs_completed_total` | counter | Handoffs that completed. |
| `kv_handoff_last_duration_seconds` | gauge | How long the last completed handoff took. Absent until one has completed. |
| `kv_handoff_duration_seconds_total` | counter | How long all completed handoffs took, together. Divided by `kv_handoffs_completed_total`, the average. |

**Anti-entropy**

| Metric | Type | Labels | What it measures |
|---|---|---|---|
| `kv_antientropy_rounds_total` | counter | none | Rounds run, each against every other member in turn (one every `intervals.antiEntropy`, plus one after each handoff). |
| `kv_antientropy_last_round_duration_seconds` | gauge | none | How long the last round took. Absent until one has run. |
| `kv_antientropy_round_duration_seconds_total` | counter | none | How long all rounds took, together. |
| `kv_antientropy_peer_failures_total` | counter | none | Reconciliations with one peer that failed (the round moves on to the next peer). |
| `kv_antientropy_keys_repaired_total` | counter | `direction` (`pulled`, `pushed`) | Keys for which a reconciliation installed versions locally, or sent them to the peer. |

**Process**

| Metric | What it measures |
|---|---|
| `go_*`, `process_*` | The Go runtime's and the process's own metrics: goroutines, heap, GC, CPU, resident memory. |

Every label has a small, fixed set of values: there are no keys, peer IDs
or node incarnations in labels (peers are counted, not listed). A series
for a `code` appears the first time that code is returned. Until the node
is open (at startup), `/metrics` answers but serves only the `go_*` and
`process_*` metrics.

`/metrics` is on the unauthenticated admin port: anyone who can reach it
can also change the membership (see
[known-limitations.md](known-limitations.md)). `deploy/k8s/` exposes that
port only on the headless Service `kv`, not on `kv-client`.

### Where they come from

Only `internal/telemetry` knows a metrics format; the core packages
(`node`, `storage/...`, `rpc`, `hints`, `store`) expose plain values and
import nothing from Prometheus. A test fails if one does
(`TestCorePackagesDoNotImportPrometheus`), or if `internal/app` imports
Prometheus directly instead of going through `internal/telemetry`.

- **Counters and values read at scrape time.** `node.Stats()` returns a
  snapshot: counters (`atomic.Uint64`, bumped where the event happens: a
  quorum failure, a flush, a hint stored, an anti-entropy round) and values
  read from state the node already keeps (its view, peer health, the
  storage engines' memtables and SSTables, the handoff status).
  `internal/telemetry` reads it once per scrape. No counter adds a lock to
  the request path.
- **Timings at existing seams.** A gRPC unary interceptor, passed to
  `rpc.Serve` as a server option, times client requests.
- **Timings only the core can see.** `wal.SyncObserver`, passed down through
  `node.WithStorageObservers` and `engine.WithObserver`, is told how long
  each WAL fsync took. Without an observer an fsync costs one more branch;
  with one, reporting allocates nothing.

`internal/app` wires these together in `Run`.

## The local stack

`deploy/observability/` (namespace `observability`), applied with
kustomize:

| Component | Image | Memory request / limit |
|---|---|---|
| Prometheus | `prom/prometheus:v3.15.0`, pinned by its multi-arch index digest | 128Mi / 512Mi |
| Grafana | `grafana/grafana:13.2.3`, pinned by its multi-arch index digest | 384Mi / 768Mi |

```sh
kubectl apply -k deploy/observability/
kubectl -n observability rollout status deploy/prometheus deploy/grafana
kubectl -n observability port-forward svc/grafana 3000      # http://localhost:3000
kubectl -n observability port-forward svc/prometheus 9090   # http://localhost:9090
```

- **Prometheus** scrapes every kv pod every 15s. It finds them through the
  SRV records of the headless Service `kv`
  (`_http._tcp.kv.kvstore.svc.cluster.local`, one per pod), so it needs no
  access to the Kubernetes API and no RBAC, and it follows scaling. Because
  `kv` publishes not-ready addresses, a pod that is starting or draining is
  scraped too. Targets are relabelled to `instance="kv-N"`. It keeps 24h, on
  an emptyDir: its data is gone when the pod is.
- **Grafana** has the Prometheus data source (uid `prometheus`) and the
  `kvstore` dashboard folder provisioned from ConfigMaps. Dashboards are the
  JSON files in `deploy/observability/dashboards/`; Grafana loads them at
  startup and cannot save changes over them. To change one, edit the JSON
  (or change it in Grafana, export it, and replace the file), then
  `kubectl apply -k deploy/observability/`.
- The ConfigMaps keep fixed names, so changing a file under
  `deploy/observability/config/` does not restart anything: run
  `kubectl -n observability rollout restart deploy/<name>` after applying.

**Grafana runs with anonymous Admin access and no login form.** That is
acceptable only because it is reachable through `kubectl port-forward` and
nothing else: its Service is a ClusterIP. Do not expose it.

### The kvstore overview dashboard

`dashboards/kvstore-overview.json`, in the `kvstore` folder, one row per
area. Each panel's description names the metric it reads.

| Row | Panels |
|---|---|
| Requests | Put rate by status code; Put latency p50 / p99; quorum failures over the last 5 minutes, by `read` and `write` |
| Membership and peers | epoch by pod; member of its own view, by pod; peers alive by pod; peers reachable by pod |
| Storage | WAL fsync latency p50 / p99 (data engine); memtable size by pod; SSTables by pod; flushes and compactions over the last 5 minutes, by engine and result |
| Hinted handoff | hints created and delivered per second; hints pending by pod |
| Membership handoff | keys pending by pod; pushes and hints in the current handoff; last handoff duration by pod |
| Anti-entropy | rounds per pod over the last hour; last round duration by pod; keys repaired and peer failures over the last hour |

## Why two components, and no Collector yet

The format the nodes serve is the vendor-neutral part: the Prometheus text
format on `/metrics` can be scraped by Prometheus itself, by an
OpenTelemetry Collector, by Grafana Alloy, or by a hosted agent, without any
change to the store. So the local stack is the smallest one that shows the
metrics: Prometheus scraping the pods, and Grafana.

An OpenTelemetry Collector is deferred until there is something for it to
do: collecting logs, or sending the same metrics to a second backend. It
would then be added as configuration only (the same SRV discovery in its
`prometheus` receiver), with no change to the nodes.

The all-in-one `grafana/otel-lgtm` image was tried first and dropped: it
always starts Tempo and Pyroscope, which this project does not use, and has
no setting to leave them out (checked in 0.35.0 and on its main branch); it
fails as a whole if either does not start.

## Future work

- **Storage operation timings.** A decorator around `store.Persister`
  (timing `Put`, `GetAll`, `Restore` and `Keys` by result), passed to the
  node as an option, would time storage as the node sees it, lock waits
  included. It must pass errors through untouched, so that
  `errors.Is(err, store.ErrRestoreIncomplete)` still works.
- **Node-to-node requests.** Only client requests are timed. A client-side
  interceptor on the connections a node dials to its peers would time
  replication, forwarding and anti-entropy calls as the caller sees them.
- **Found and not found.** A `Get` that finds nothing is `OK` like one that
  does; telling them apart needs a counter in the node.
- **An OpenTelemetry Collector**, for logs or a second backend (see above).

## Measured on kind

On the 10-node kind cluster from [kubernetes.md](kubernetes.md) (`N=3, W=2,
R=2`, Docker Desktop on WSL2), with the load writer from `deploy/loadgen/`
writing one key at a time through `kv-client` (`SLEEP: "0.05"`), over the
10 minutes to 01:20:50 UTC on 2026-10-03:

| | Count | Rate | p50 | p90 | p99 | Mean |
|---|---|---|---|---|---|---|
| Client Put (all `OK`) | 7,081 | 11.8/s | 17.0 ms | 23.8 ms | 42.9 ms | 16.4 ms |
| WAL fsync, data engine | 21,147 | 35.3/s | 7.4 ms | 9.9 ms | 23.7 ms | 7.7 ms |

There are three fsyncs per Put, one on each of the key's three replicas.
The hint store recorded none: every replica was reachable. The writer's
own log shows no failed write.

The percentiles are interpolated within buckets, so they are only as fine
as the buckets around them:

| Bucket (ms) | Put | fsync |
|---|---|---|
| up to 2.5 | 0% | 0% |
| 2.5 to 5 | 0% | 12.3% |
| 5 to 10 | 8.8% | 78.9% |
| 10 to 25 | 88.3% | 8.5% |
| 25 to 50 | 2.6% | 0.1% |
| over 50 | 0.3% | 0.1% (slowest under 1 s) |

The buckets cover the whole range seen (nothing past the last finite
bound), but most observations fall into one bucket each: 88% of Puts
between 10 and 25 ms, 79% of fsyncs between 5 and 10 ms. A p50 or p99
inside such a bucket is a straight-line estimate across it. After this
measurement, finer bounds were added where the counts cluster:
7.5, 15, 20, 30 and 40 ms for requests, and 7.5, 15 and 20 ms for fsyncs
(`requestBuckets` and `fsyncBuckets` in `internal/telemetry`).

**Membership change.** `POST /admin/membership` to `kv-0` at epoch 5 with
the same ten members: all ten pods reported epoch 5 within 1.2 s
(`poll.sh`), and `kv_membership_epoch` showed 5 for every pod in Prometheus
15 s later, one scrape interval.

**Losing a pod under load.** With the load writer writing 2,000 keys
through `kv-client` (about 11.7 writes/s, 20:10:55 to 20:13:50 UTC), `kv-3`
was deleted at 20:11:41.9 (`kubectl delete pod`, so its 5 s `preStop`
sleep ran first). Its replacement was created at 20:11:48 and logged
`ready` at 20:11:49: the node was down for about 2 s. What the dashboard
showed:

| | Before | During and after |
|---|---|---|
| Client writes | all `OK` | all 2,000 `OK` (the writer's own log); quorum failures 0 for reads and writes throughout |
| Hints | none created | 24 hinted items created for `kv-3` across the other pods by 20:12:15 (20 by 20:12:00); 4 delivered by 20:12:00 and all 24 by 20:12:15. `kv_hints_pending` showed 3 on `kv-8` at 20:12:00 and 0 on every pod from the next round. |
| Peers | 9 alive and reachable everywhere | one scrape (20:12:00) caught a pod with 8 reachable; no scrape caught one with fewer than 9 alive: only `kv-7` marked `kv-3` dead (3 missed heartbeats), and saw it alive again within the same second (20:11:49) |
| `kv-3` | epoch 5, 9 peers | missing from one scrape (`up` 0 at 20:12:00), then back at epoch 5 (its persisted membership) with 9 peers alive and reachable |

The outage was shorter than a scrape interval (15 s), so the dashboard
shows it as one sample, and `kv_hints_pending`, updated once per
hint-delivery round (10 s), caught only part of it: the counters
(`kv_hints_created_total`, `kv_hints_delivered_total`) are what show that
every hint was delivered. Counters on the restarted pod start again from
zero; `rate` and `increase` account for that, but a plain `sum` of a
counter across pods drops by what the old process had counted.
