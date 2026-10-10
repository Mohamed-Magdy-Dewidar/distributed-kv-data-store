# Benchmarks (k6)

Load tests of the store with [k6](https://grafana.com/docs/k6/latest/),
run inside the cluster from `deploy/bench/`, so the same scripts and
commands work on kind and on GKE. This page says what is measured and how,
what must stay the same between runs, and records every run: its versions,
cluster, data size, parameters and results.

> **kind runs are a script check and a rough baseline, not the capacity
> result.** kind is one node: k6, the six database pods, Prometheus and the
> control plane share the same Docker Desktop VM, its CPUs and its disk. A
> kind run proves the pipeline works and gives a number to compare kind runs
> with each other; the store's capacity is measured on GKE, where k6 has a
> node of its own.

## Versions

| Component | Version |
|---|---|
| k6 | v2.3.0 (2026-09-21), `grafana/k6:2.3.0@sha256:9c2dee7f8ed74d317e4027c06a10f169b625638189de8d4555d0b3486a5aeb34` (multi-arch index) |
| k6 gRPC client | `k6/net/grpc`, built in; loads `internal/rpc/pb/kvstore.proto` (not reflection) |
| k6-operator | not used. v1.6.0 was considered: its start barrier is what a plain Indexed Job lacks, and `START_AT` stands in for it (see [Two k6 pods](#is-the-database-the-bottleneck)) |
| Prometheus | v3.15.0 (`deploy/observability/`), 15s scrape interval |
| kind | v0.33.0, Kubernetes v1.35.8 (`docs/kubernetes.md`) |
| GKE | 1.35.8-gke.1225000 at session 1 (`docs/gke.md`) |

## The clusters

**kind (this laptop).** One node, `kind-kvstore`, on Docker Desktop's WSL2
VM, pinned in `%USERPROFILE%\.wslconfig` so runs do not depend on the host:

```
[wsl2]
kernelCommandLine = cgroup_no_v1=all
processors=8
memory=8GB
```

`docker info`: 8 CPUs, 8,332,005,376 bytes of memory, cgroup v2, kernel
5.15.167.4-microsoft-standard-WSL2. Volumes are kind's `standard` class
(local-path, on the VM's disk), 1Gi each. To apply a change to
`.wslconfig`: quit Docker Desktop (tray icon, Quit), run `wsl --shutdown`,
start Docker Desktop, check `docker info --format '{{.NCPU}} {{.MemTotal}}'`.

**GKE.** `infra/gke/` ([gke.md](gke.md)): the database pool, 6 ×
e2-standard-2 over 3 zones, one kv pod each, `standard-rwo` (pd-balanced)
10Gi volumes; the system pool (1 × e2-standard-2: Prometheus, Grafana, the
operator); and, for benchmarks, the loadgen pool (1 × n2-standard-4,
`loadgen_enabled = true`, off by default; 2 with `loadgen_nodes = 2` for
the two-pod check) where k6 runs, and nothing else.

## The bench KVCluster

Every run, on kind and GKE, is against the bench KVCluster,
`deploy/bench/kvcluster/kind.yaml` or `gke.yaml`: the operator's samples
with one change, a **512Mi memory limit** instead of 256Mi, so a run that
stores up to ~100k keys per pod is not OOM killed. Otherwise the defaults:

| Setting | Value |
|---|---|
| Replicas | 6 |
| Replication | n=3, w=2, r=2 |
| Requests | CPU 50m, memory 64Mi |
| Limits | memory 512Mi, no CPU limit |
| Anti-entropy interval | 15m (`intervals.antiEntropy`, the default) |
| Storage | kind: 1Gi `standard`; GKE: 10Gi `standard-rwo` |
| Placement | kind: none (one node); GKE: one pod per node, spread over zones, the tainted `kvstore` pool |

`run.sh` refuses to run unless the KVCluster `kv` is Ready with 6 replicas
and has the 512Mi limit.

## The workload

`deploy/bench/script.js`. Requests go to `KVClient` (`Put`, `Get`) through
the `kv-client` Service, like any client's.

| Scenario | What it does |
|---|---|
| `preload` | Writes `PRELOAD_KEYS` keys, `bench-pre-000000` onwards (default 20,000). Closed model (shared iterations) with `PRELOAD_VUS` VUs (default 40), only to load the data `reads` and `mix` need; not measured. Once per KVCluster, and it must be clean ([below](#a-clean-preload)). |
| `writes` | Puts of keys never written before, `bench-<run>-w-<n>`. |
| `reads` | Gets of random preloaded keys; each must return exactly the value preloaded, or it counts as an error. |
| `mix` | `writes` and `reads` at once, each at half the rate and with half the VUs, reported separately. |

Each of `writes`, `reads` and `mix` runs with one of two open-model
profiles (the rate is fixed whatever the latency; iterations that find no
free VU are `dropped_iterations`):

- **constant**: `RATE` iterations/s for `DURATION` seconds
  (`constant-arrival-rate`). Defaults 100/s, 300s.
- **ramp**: `ramping-arrival-rate` from `RAMP_FROM` to `RAMP_TO` in steps of
  `RAMP_STEP`, each rate held `STEP_SECONDS`, `RAMP_SECONDS` to move between
  them. Defaults 50 to 500/s, steps of 50, 30s held, 5s moves (5m45s). The
  knee is the rate where p99 turns up sharply, or errors or dropped
  iterations start.

**Values** are 256 ASCII characters (the key, `|`, `x` padding), fixed for
every run. The node stores a client value JSON-encoded, so 258 bytes.

**Connections.** gRPC keeps one connection per client, and `kv-client`
balances connections, not requests. Each VU opens its own connection on its
first iteration and keeps it; `preAllocatedVUs = maxVUs = VUS` (default 120,
about 20 per pod), and arrival-rate executors hand iterations to VUs in
turn, so every VU connects in the first seconds (`connects` in each run's
summary must equal `VUS`). Each pod's share of the load follows its share
of the connections, which is random: `window.env` records how many
requests each pod coordinated, and the busiest pod's count over the median
(`coordinated_max_over_median`, at most 1.5) and the quietest's
(`coordinated_min_over_median`). The dashboard panel "Requests coordinated
by pod" shows the same.

**Why 120 VUs.** The VUs bound the requests in flight, so by Little's law
they bound the latency a rate can have before iterations are dropped:
rate × latency ≤ VUs. At the default ramp's top, 500/s, 60 VUs drop
iterations as soon as the latency passes 120 ms; 120 VUs allow 240 ms, so
the knee shows up as latency first and only then as dropped iterations.
More connections also even out each pod's share: the smoke run's 60 gave
the pods 7 to 11 each.

The preload is the exception: it runs with `PRELOAD_VUS` (default 40), not
`VUS`. It is a closed model, so its VUs are the requests in flight whatever
the latency, and it is not measured, so it needn't match the runs. At 120
VUs it overloaded kind: the queue grew past the 5s client timeout and 4.5%
of its Puts failed ([the record](#kind-2026-10-10-baseline-series-stopped-after-the-preload)).
At 60 it had none; 40 leaves margin.

### The key cap

Every key a run writes stays in the store, and memory grows with the keys a
node holds (67 MiB at ~60k keys, [kubernetes.md](kubernetes.md#resources)).
`run.sh` counts, before each run, the writes it can make (rate × time) in a
ledger per KVCluster (the `k6-bench-ledger` ConfigMap, keyed by the
KVCluster's uid) and refuses a run that would take the total past
**130,000 keys**:

- with n=3 over 6 pods, each key is stored on 3/6 of the pods;
- the ring is skewed: in GKE session 1 the busiest pod stored 1.42× the
  mean (storage writes by pod); allowing 1.5×,
- busiest pod ≈ keys × 3/6 × 1.5 = 0.75 × keys, so 130,000 keys ≈ 97,500
  on the busiest pod, under ~100k.

The preload's 20,000 count against it, leaving 110,000 for writes until the
next `reset`. A constant run writes `RATE × DURATION` (the default, 30,000);
the default write ramp, about 96,000.

**The write ramp's ceiling is set by the cap, not by the store.** 50 to
500/s is what fits in 130,000 keys with 30s steps. GKE's knee for writes
may lie above 500/s; if session 2's first constant runs suggest it does,
the ramp must be revisited before the knee is claimed: start it higher
(`RAMP_FROM`), shorten the steps, or raise the memory limit and the cap
together.

## Running

The logic is in `deploy/bench/run.sh` (Git Bash on Windows, any bash on
Linux); `make bench SCENARIO=...` is a thin wrapper. Parameters are
environment variables. Both refuse to run unless kubectl's current context
is `BENCH_CONTEXT` (default `kind-kvstore`; on GKE,
`gke_<project>_<region>_kvstore`), and pass `--context` on every call.

```sh
kubectl config use-context kind-kvstore
deploy/bench/run.sh reset                       # deletes kv and its volumes; asks first
deploy/bench/run.sh preload
RATE=200 DURATION=300 deploy/bench/run.sh writes
PROFILE=ramp deploy/bench/run.sh reads
RATE=200 deploy/bench/run.sh mix
K6_PODS=2 PROFILE=ramp deploy/bench/run.sh reads   # the two-pod check
deploy/bench/run.sh clean                       # delete the Job and its ConfigMaps
```

### A clean preload

`reads` and `mix` choose among the preloaded keys and require each one's
exact value, so the preload must have written every key: the k6 Job
succeeded, k6 timed all `PRELOAD_KEYS` Puts (`put_count`), and none failed. Any
error fails the preload: `run.sh` exits 1, `run.env` says `preload=failed`,
and the ledger records it, so **every later run on that KVCluster is
refused** (`reads` and `mix` would count missing keys as errors, and the
series' data would not be what this page says). A preload that did not
finish leaves the ledger at `pending`, refused the same way. The only way
on is `reset` and a new preload.

There are no retries. A Put that failed may still have been applied on
some replicas, and a retry carries no context: it would version the key
again, and the store could keep the two as siblings. A preload either
writes each key once and is acknowledged for all of them, or it is
discarded.

**A clean tree.** `writes`, `reads` and `mix` refuse to run if a tracked
file outside `docs/` differs from HEAD, staged or not: a result must name
the code it ran, and `commit=` in `run.env` is that name. Untracked files
and `docs/` do not count, since each run's results are untracked until they
are committed and recording a run edits this page. Two exceptions, both
recorded in `run.env` (`commit=<sha>-dirty`): `preload`, which loads data
and is not a result, and `BENCH_SMOKE=1`, for a pipeline check while
`deploy/bench` is being changed (`smoke=1`; such a run is listed as a smoke
run, never as a result). The check covers the tree only: the images must be
built from the same commit (see [What must match](#what-must-match-between-kind-and-gke)).

On GKE: `BENCH_CONTEXT=gke_<project>_<region>_kvstore`, the loadgen pool
up, and `KV_IMAGE=<the node image's Artifact Registry path>` for `reset`.
Prometheus and Grafana (`deploy/observability/`) must be running: the
export reads Prometheus.

### What each run saves

`docs/results/bench/<run>-<cluster>-<scenario>-<profile>/` (`BENCH_OUT`
to change it):

| File | Contents |
|---|---|
| `run.env` | Every parameter, the KVCluster's uid, epoch, image and memory limit, the k6 image, the commit (`-dirty` if a tracked file outside `docs/` differed from HEAD), `smoke` (1 if run with `BENCH_SMOKE=1`), the script's sha256, k6's node and machine type, the measured window, and the totals: dropped iterations, errors, connections |
| `k6-<i>.log` | Each k6 pod's log |
| `summary-<i>.json` | k6's whole end-of-test summary data |
| `summary-<i>.env` | The same, flat: per op count, rate, p50/p95/p99/max (ms), errors; `dropped_iterations`, `vus_max`, `connects` |
| `top.txt` | `kubectl top pod --containers` every 15s through the run: k6's CPU next to every kv pod's |
| `prometheus/*.csv` | `deploy/bench/export.sh`: one CSV per query, 15s steps, from 60s before the window to 30s after, in the shape of [results/gke-session1/](results/gke-session1/): Put and Get rates by code and p50/p99, data-engine fsync p50/p99, requests coordinated by pod, storage writes by pod, CPU and memory by pod, anti-entropy rounds by pod, quorum failures, hints |
| `prometheus/window.env` | The whole window at once: request and fsync counts, quantiles over the window (`histogram_quantile` of the buckets' `increase`, interpolated inside a bucket), anti-entropy rounds, quorum failures, hints, CPU by pod, the spread |
| `prometheus/queries.txt` | Every query, as sent |

`export.sh` runs at the end of every run (`BENCH_EXPORT=0` skips it), and
can be run again on a results directory. It reaches Prometheus through a
port-forward that it stops when it exits.

## Is the database the bottleneck?

A result counts only if k6 was not the limit. For every run:

- **`dropped_iterations` is 0** (a constant run with any is not valid; in a
  ramp, dropped iterations past the knee are expected: higher latency holds
  the VUs longer, Little's law);
- **k6's CPU stays under half its 2-core request** (1000m in `top.txt`);
- **the database's side, for the same window**: Put/Get p99 and fsync p99
  from Prometheus, which k6 cannot inflate, and the kv pods' CPU;
- **the spread**: `coordinated_max_over_median` at most 1.5.

**Two k6 pods.** Repeat the knee run with `K6_PODS=2`: the Job runs two
pods (on GKE, each on its own loadgen node with its whole 2-CPU request:
`loadgen_nodes = 2`, and `run.sh` refuses with fewer nodes than pods; that
session must not scale the database, see [gke.md](gke.md#quotas)), each
with half the VUs and its own execution segment (`0:1/2`, `1/2:1`), both
starting at the same `START_AT` (they log
`k6-bench-start <unix seconds>`). If the knee's rate and the database-side
p99 at the knee move by less than 10%, one pod was enough; if throughput
rises clearly, the single-pod result was k6's limit and is not valid.
Percentiles do not add across pods, so the comparison uses Prometheus; the
two summaries' counts add.

**Anti-entropy rounds.** The interval stays 15m, so a round can fall inside
a run (each node's first starts at a random time within one interval). Runs
are not discarded for it: `window.env` counts the rounds in the window, and
a run with any is flagged in the table below.

## What must match between kind and GKE

Identical: the commit and images, the bench KVCluster (replicas, n/w/r,
requests and limits, ConfigMap intervals), the k6 version and script,
value size, `PRELOAD_KEYS`, `PRELOAD_VUS`, `VUS`, profiles and rates, a fresh KVCluster
(`reset`) before a series, the scrape interval and the export's queries.

Different, and recorded: the disks (local-path on the laptop's SSD vs
pd-balanced), the network (one VM vs three zones), the CPUs (8 shared
vCPUs vs six 2-vCPU nodes plus k6's own), and k6's placement (shared node
vs its own). So kind against GKE is not a like-for-like comparison: what
compares is each cluster's knee and its behaviour past it.

## Open questions

**Can a client tell slow from down?** Under overload (the failed preload at
120 VUs) most failed Puts reached k6 as `Unavailable`, with no quorum
failures and no pod down. `internal/rpc/client_service.go` maps every
error that is not a context error to `codes.Unavailable`, whatever its
cause, so a coordinator that ran out of time for its replicas and one that
found them gone look alike to a client. To investigate before reading
anything into GKE's errors past the knee: what the overloaded coordinators
actually returned (the error text, by cause), and whether a timeout inside
the coordinator should reach the client as `DeadlineExceeded` instead.

## Runs

Flags: **AE** anti-entropy rounds in the window; **SPREAD** the busiest
pod over 1.5× the median.

### kind, 2026-10-10: smoke run (pipeline check)

Commit `6eccab2` with `export.sh` not yet committed (hence `-dirty`);
kvnode and the operator built from `6eccab2` and loaded into kind
(`kvnode:dev` `sha256:558a373b0494…`, `kvstore-operator:dev`
`sha256:1ab809b7eda4…`, Docker image IDs), operator and CRD reinstalled,
then `reset`. Not a benchmark: it proves the pipeline, from `reset` to the
export.

| Run | Scenario | Parameters | Data before | k6 summary | Prometheus (window) | k6 CPU | Flags |
|---|---|---|---|---|---|---|---|
| [20261010-144345](results/bench/20261010-144345-kind-preload-shared/) | preload | 20,000 keys, 60 VUs (closed) | 0 keys | 20,000 Puts in 133.8s (149.5/s); p50 238.6 ms, p95 1,602.8 ms, p99 2,460.9 ms; 0 errors; 60 connections | Put p50 240.6 / p99 2,493.9 ms; fsync p50 8.94 / p99 14.87 ms; kv CPU 0.07-0.12 cores; 0 AE rounds | max 125m | SPREAD (1.58) |
| [20261010-144713](results/bench/20261010-144713-kind-writes-constant/) | writes, constant | 20/s, 60s, 60 VUs | 20,000 keys | 1,200 Puts (20.0/s); p50 13.67 ms, p95 16.39 ms, p99 19.20 ms; 0 errors; 0 dropped; 60 connections | Put p50 12.17 / p99 19.64 ms; fsync p50 5.87 / p99 13.10 ms; kv CPU 0.03-0.07 cores; 0 AE rounds | max 63m | |

What the smoke run showed besides "it works":

- The closed-model preload saturates kind: 60 VUs in flight held Put p50 at
  240 ms with the kv pods at about a tenth of a core each, and fsync p50 at
  9 ms (5.9 ms at 20/s). So CPU was not the limit and most of the latency
  was queueing; the shared disk's fsyncs are the likely constraint, not yet
  investigated. This is the preload, not a measured run, and it is kind.
- In the preload the busiest pod coordinated 3.3× the quietest (min/median
  0.48): in a closed model a pod with more connections, or faster
  answers, gets more requests. At a fixed 20/s every VU did the same work,
  so each pod's share was its share of the 60 connections: 220 requests
  for 11 connections, 140 for 7 (min/median 0.64, max/median 1.00).

### kind, 2026-10-10: baseline series (stopped after the preload)

Commit `607b3f1`, clean tree. Images as in the smoke run (`kvnode:dev`,
`kvstore-operator:dev`, built from `6eccab2`; nothing outside `docs/` and
`deploy/bench` has changed since). `reset`, then the preload with 120 VUs,
the runs' default at the time. The series stopped there: the preload had
errors, and the reads would choose among keys some of which were never
written. Kept as a result: **overload at 120 VUs in flight**, the queue
growing past the 5s client timeout. It is why the preload now runs with
`PRELOAD_VUS` (40) and must be [clean](#a-clean-preload).

| Run | Scenario | Parameters | Data before | k6 summary | Prometheus (window) | k6 CPU | Flags |
|---|---|---|---|---|---|---|---|
| [20261010-213341](results/bench/20261010-213341-kind-preload-shared/) | preload | 20,000 keys, 120 VUs (closed) | 0 keys | 20,000 iterations in 132.2s; 19,311 Puts with a duration (146.0/s; k6 recorded none for the other 689); p50 276.7 ms, p95 4,162.6 ms, p99 5,000.3 ms (the 5s client timeout); **895 errors (4.5%)**; 0 dropped; 120 connections | 19,938 Puts, 931 not OK (`Unavailable` most, then `Canceled`, a few `DeadlineExceeded`, from 21:34:28, 45s in, to the end); Put p50 284 / p99 5,000 ms; fsync p50 8.91 / p99 14.85 ms; kv CPU 0.06-0.11 cores; 0 quorum failures, 0 hints; 0 AE rounds | max 181m | SPREAD (1.72) |

With 60 VUs (the smoke run's preload) the same 20,000 Puts had no errors.
With 120 in flight, throughput did not rise (146/s vs 149.5/s); only the
queue grew, past the 5s client timeout for the slowest Puts. The kv pods
were at a tenth of a core and logged nothing, and no pod restarted.

### kind, 2026-10-10: baseline series, second attempt (stopped at run 2: no read knee)

Commit `74bbac0`, clean tree. Images as in the smoke run (`kvnode:dev`,
`kvstore-operator:dev`, built from `6eccab2`; nothing outside `docs/` and
`deploy/bench` has changed since). `reset`, the preload with
`PRELOAD_VUS=40`, then the approved series. It stopped after run 2: the
read ramp found no knee up to its top rate, 1000/s, and the stop rules say
not to extend a range without asking. Runs 3 to 7 (the two-pod read ramp,
writes, mix) were not run.

| Run | Scenario | Parameters | Data before | k6 summary | Prometheus (window) | k6 CPU | Flags |
|---|---|---|---|---|---|---|---|
| [20261010-221235](results/bench/20261010-221235-kind-preload-shared/) | preload | 20,000 keys, 40 VUs (closed) | 0 keys | 20,000 Puts in 139.5s (143.4/s); p50 187.1 ms, p95 915.4 ms, p99 1,563.6 ms; **0 errors, clean**; 40 connections | Put p50 196.1 / p99 2,190.5 ms; fsync p50 8.95 / p99 14.85 ms; kv CPU 0.06-0.11 cores; 0 AE rounds | max 130m | SPREAD (1.64) |
| [20261010-221549](results/bench/20261010-221549-kind-reads-constant/) | 1. reads, constant | 200/s, 300s, 120 VUs | 20,000 keys | 60,001 Gets (200.0/s); p50 0.89 ms, p95 1.58 ms, p99 2.00 ms; 0 errors; 0 dropped; 120 connections | Get p50 0.40 / p99 1.33 ms; no fsyncs (reads write nothing); kv CPU 0.08-0.13 cores; spread 1.12 / 0.73 | max 281m | AE (2) |
| [20261010-222146](results/bench/20261010-222146-kind-reads-ramp/) | 2. reads, ramp | 100 to 1000/s, steps of 100, 30s held, 5s moves (345s), 120 VUs | 20,000 keys | 189,746 Gets (550.0/s average); p50 0.58 ms, p95 1.26 ms, p99 1.76 ms, max 19.65 ms; 0 errors; 0 dropped; 120 connections | Get p50 0.29 / p99 0.99 ms over the window; no fsyncs; kv CPU 0.14-0.23 cores; spread 1.15 / 0.60 | max 578m | AE (2); **no knee** |

**No read knee up to 1000/s.** Get p99 in Prometheus, by 15s step, stayed
at 0.95-0.99 ms from 237/s to 959/s (1.1-2.3 ms at the low rates at the
start, where few requests make the quantile coarse); nothing turned up, no
error, no dropped iteration. The kv pods used at most 0.23 cores each, k6
0.58 cores at the top. A Get with r=2 does no fsync, so the disk that limits
the writes on kind is not in its path. Finding the read knee on kind needs
a higher range, which is a decision for the next series.
