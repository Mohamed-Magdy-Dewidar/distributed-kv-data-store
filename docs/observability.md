# Observability

Every node serves its metrics on `GET /metrics`, on the same port as its
probes and admin API (`listen.http`, 8080 in `deploy/k8s/`), in the
Prometheus text format. `deploy/observability/` runs a local Prometheus and
Grafana on the kind cluster from [kubernetes.md](kubernetes.md) to collect
and show them.

## Metrics

| Metric | Type | Labels | What it measures |
|---|---|---|---|
| `kv_membership_epoch` | gauge | none | The epoch of the membership view the node holds. Read when `/metrics` is scraped. |
| `kv_client_request_duration_seconds` | histogram | `method` (`Get`, `Put`), `code` (gRPC status code) | Time to serve a client request on the node that received it, including forwarding to a replica and waiting for the quorum. Only `KVClient` calls; node-to-node calls are not timed. |
| `kv_wal_fsync_duration_seconds` | histogram | `engine` (`data`, `hints`) | Time to fsync one write to a storage engine's write-ahead log: the node's data, or its hint store. Replaying the WAL at startup is not included. |
| `go_*`, `process_*` | various | none | The Go runtime's and the process's own metrics (goroutines, heap, GC, CPU, resident memory). |

There is no label with an unbounded set of values: no keys, no node
incarnations. A series for a `code` appears the first time that code is
returned. Until the node has opened its storage (at startup), `/metrics`
answers but serves only the `go_*` and `process_*` metrics.

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

- **Values read at scrape time.** `node.Stats()` returns a snapshot of
  state the node already keeps (today, the epoch); `internal/telemetry`
  reads it once per scrape.
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

`dashboards/kvstore-overview.json`, in the `kvstore` folder:

| Panel | Query |
|---|---|
| Membership epoch by pod | `kv_membership_epoch{job="kvstore"}` |
| Put rate by status code | `sum by (code) (rate(kv_client_request_duration_seconds_count{job="kvstore",method="Put"}[1m]))` |
| Put latency p50 / p99 | `histogram_quantile(0.99, sum by (le) (rate(kv_client_request_duration_seconds_bucket{job="kvstore",method="Put"}[1m])))`, and 0.50 |
| WAL fsync latency p50 / p99 (data engine) | `histogram_quantile(0.99, sum by (le) (rate(kv_wal_fsync_duration_seconds_bucket{job="kvstore",engine="data"}[1m])))`, and 0.50 |

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
