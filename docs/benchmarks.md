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
| GKE | 1.35.8-gke.1225000 at session 1, 1.36.4-gke.1391002 at session 2 (the Regular channel's default moved; `docs/gke.md`) |

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

**Reading the knee.** In a ramp, k6 tags every iteration with the step it
starts in (each held rate is a step, numbered from 1; iterations that
start during a move between steps belong to none), and `run.sh` saves one
row per step and op as `steps.csv`: target and achieved rate, p50/p95/p99
and max, errors, dropped iterations. **The knee is read from `steps.csv`**
(client side, each step on its own) **and confirmed in Prometheus** (server
side: the Put/Get and fsync p99 series over the same minutes, which k6
cannot inflate). The Prometheus series alone cannot place it: they are 1m
rates, so each point blends the steps around it.

| `steps.csv` column | Meaning |
|---|---|
| `pod` | The k6 pod (0, or 0 and 1 with `K6_PODS=2`: rates and counts add across a step's two rows, percentiles do not) |
| `step`, `op` | The step, from 1, and `put` or `get` (mix has a row for each) |
| `target_rate`, `seconds` | The rate this pod and op were to hold (mix halves it per op, two pods per pod), and for how long |
| `started`, `achieved_rate` | Iterations that started in the step, and per second |
| `count`, `p50_ms` ... `max_ms` | Their requests that k6 timed, and the latency quantiles |
| `errors` | Their failed requests |
| `dropped` | Target arrivals (`target_rate` × `seconds`) less `started`. k6 tags `dropped_iterations` with the scenario only, so this is derived; the steps' sum is within about one per step of k6's total, which also counts the moves |

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
| `steps.csv` | Ramps only: one row per k6 pod, step and op ([Reading the knee](#the-workload)) |
| `top.txt` | `kubectl top pod --containers` every 15s through the run: k6's CPU next to every kv pod's |
| `prometheus/*.csv` | `deploy/bench/export.sh`: one CSV per query, 15s steps, from 60s before the window to 30s after, in the shape of [results/gke-session1/](results/gke-session1/): Put and Get rates by code and p50/p99, data-engine fsync p50/p99, WAL batch size p50/p99/mean and commit wait p50/p99 (data engine), requests coordinated by pod, storage writes by pod (`kv_storage_writes_total`), CPU and memory by pod, anti-entropy rounds by pod, quorum failures, hints |
| `prometheus/window.env` | The whole window at once: request, fsync and storage-write counts, writes per fsync, batch size and commit wait p50/p99, whether any engine halted writes, quantiles over the window (`histogram_quantile` of the buckets' `increase`, interpolated inside a bucket), anti-entropy rounds, quorum failures, hints, CPU by pod, the spread |
| `prometheus/queries.txt` | Every query, as sent |

`export.sh` runs at the end of every run (`BENCH_EXPORT=0` skips it), and
can be run again on a results directory. It reaches Prometheus through a
port-forward that it stops when it exits.

## Is the database the bottleneck?

A result counts only if k6 was not the limit. For every run:

- **`dropped_iterations` is 0** (a constant run with any is not valid; in a
  ramp, dropped iterations past the knee are expected: higher latency holds
  the VUs longer, Little's law);
- **k6's CPU stays under half its request**, in `top.txt`: 1000m on kind
  (2 CPUs requested), 1500m on GKE (3 CPUs: the GKE overlay raises the
  request, and `run.sh` checks on the loadgen node, before every GKE run,
  that GKE's own pods leave 3 CPUs free of its 3920m allocatable on an
  n2-standard-4; if not, it refuses and says to use n2-standard-8);
- **the database's side, for the same window**: Put/Get p99 and fsync p99
  from Prometheus, which k6 cannot inflate, and the kv pods' CPU;
- **the spread**: `coordinated_max_over_median` at most 1.5.

**Two k6 pods.** Repeat the knee run with `K6_PODS=2`: the Job runs two
pods (on GKE, each on its own loadgen node with its whole 3-CPU request:
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

## The read knee is measured on GKE

Measured in session 2: about 4000/s, with two k6 pods
([the record](#gke-2026-10-13-session-2-the-benchmarks)). The plan it
followed:

The kind read ramp found no knee up to 1000/s (Get p99 flat at about 1 ms,
[below](#kind-2026-10-10-baseline-series-second-attempt-stopped-at-run-2-no-read-knee)),
and it is not extended on kind: above about 1000/s k6 takes a growing
share of the one node's CPUs (0.58 cores at 1000/s, next to the database
it is loading), so a knee found there could be k6's, not the database's.
On GKE k6 has a node of its own, and the read ramp is sized for it:

- **Range: 1000 to at least 5000/s** (`RAMP_FROM=1000 RAMP_TO=5000
  RAMP_STEP=500`), after a constant run at 1000/s, the top of what kind
  showed. Reads write nothing, so the key cap does not limit them.
- **The two-pod check is part of the read result, not optional.** k6's CPU
  on kind grew from about 0.28 to 0.58 cores between 200/s and 1000/s; at
  that cost one pod passes GKE's 1500m validity limit (half its 3-CPU
  request) somewhere around 3500/s, inside the range. So the read
  ramp runs twice, once with one k6 pod and once with `K6_PODS=2` (two
  loadgen nodes, `loadgen_nodes = 2`), and the knee counts only where both
  agree within 10%. If k6's CPU passes the limit even with two pods before
  the database's latency turns up, the result is a lower bound on the read
  knee, and says so.
- **Steps long enough to place the knee.** The export's series are rates
  over 1m; a 30s step blends two steps into each point (see the validation
  below). Use `STEP_SECONDS=60` for the read ramps (9 rates, about 9.7
  minutes each), so each step has a point of its own.

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

**Why does write latency rise while CPU and fsync time stay flat?** In
GKE session 2, Put p99 went from 10 to 100-280 ms between 200 and 500
writes/s while the kv pods stayed under 0.4 cores and the fsync p99 near
5 ms. The likely cause is writes queueing for the engine's write lock
before each fsync, which no histogram times
([the record](#gke-2026-10-13-session-2-the-benchmarks)). To confirm with a
histogram of the wait for `writeMu`; if confirmed, group commit is the fix.

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

### kind, 2026-10-10: validation of the untested paths (not a baseline)

On the KVCluster of the second attempt (ledger at 20,000 keys, the clean
preload), from commit `0ea9d60`, clean tree: short runs to check the
mechanics nothing had exercised yet: two k6 pods, a write ramp, mix. **Not
a baseline, and kind's write capacity was not measured** beyond these
short runs; runs 3 to 7 of the approved series were replaced by these.

| Run | What | Parameters | Data before | k6 summary | Prometheus (window) | k6 CPU | Checked |
|---|---|---|---|---|---|---|---|
| [20261010-223145](results/bench/20261010-223145-kind-reads-constant/) | V1. two k6 pods | reads, constant 200/s, 60s, `K6_PODS=2`, VUS 120 | 20,000 keys | pod 0: 6,001 Gets, p50 1.00 / p95 1.69 / p99 2.00 ms; pod 1: 6,000, p50 0.80 / p95 1.57 / p99 1.88 ms; 0 errors; 0 dropped; **30 + 30 connections** | 12,001 Gets; Get p50 0.42 / p99 1.49 ms; spread 1.89 / 0.67 | max 127m | Both pods logged `k6-bench-start 1791671565`; 6,001 + 6,000 = 200/s × 60s, so the segments did not overlap; the summaries combine. **Bug: 30 VUs per pod, not 60** |
| [20261010-223456](results/bench/20261010-223456-kind-writes-ramp/) | V2. write ramp | 20 to 100/s, steps of 20, 15s held, 5s moves (95s) | 20,000 keys | 5,699 Puts (60.0/s average); p50 11.13 / p95 24.75 / p99 44.56 ms; 0 errors; 0 dropped; 120 connections | Put p50 11.67 / p99 43.04 ms; fsync p50 4.48 / p99 12.62 ms; spread 1.34 / 0.74 | max 85m | The ledger went from 20,000 to 25,700, the 5,700 planned (5,699 written). No knee is reported: see below |
| [20261010-223739](results/bench/20261010-223739-kind-mix-constant/) | V3. mix | constant 60/s (30 writes + 30 reads), 60s | 25,700 keys | Put: 1,800, p50 11.87 / p95 15.26 / p99 17.10 ms; Get: 1,801, p50 1.25 / p95 2.23 / p99 6.84 ms; 0 errors; 0 dropped; 120 connections | Put p50 12.07 / p99 18.54 ms; Get p50 0.69 / p99 5.68 ms; fsync p99 12.23 ms; spread 1.46 / 0.73 | max 92m | Reads and writes reported separately, by k6 and Prometheus; ledger +1,800 (to 27,500) |
| [20261010-223943](results/bench/20261010-223943-kind-reads-constant/) | V1 again, with the fix | as V1; `BENCH_SMOKE=1` (`commit=0ea9d60-dirty`, `smoke=1`: the fix uncommitted) | 27,500 keys | pod 0: 6,001 Gets, p99 2.32 ms; pod 1: 6,000, p99 2.25 ms; 0 errors; 0 dropped; **60 + 60 connections** | 12,001 Gets; Get p50 0.47 / p99 2.11 ms; spread 1.22 / 0.63 | max 127m | Same start second (`1791672043`); 120 connections of 120 VUs |

No anti-entropy rounds fell in these windows. k6's CPU median in the
two-pod runs is 0m because it includes the 60s the pods wait for
`START_AT`.

What the validation found:

- **The two-pod split halved the VUs twice.** `run.sh` gave each pod
  `VUS / K6_PODS` VUs, and k6's execution segment then took that pod's
  share of those again: 30 per pod, 60 in all, where 120 were meant. Fixed:
  `run.sh` passes the total, and the segments split it, as they already
  split the rates. Re-checked as a smoke run (the last row).
- **Nothing reports a knee; it is read off the series.** (Since then,
  k6 reports each step itself: `steps.csv`, [Reading the knee](#the-workload).) `run.sh` and the
  export give whole-run quantiles and per-15s series of 1m rates. In V2 the
  per-step Put p99 was 19-37 ms up to about 70/s, then 51-59 ms at the
  top, with fsync p99 from 10 to 14 ms: perhaps the start of a knee, but
  with 15s steps each point spans three or four steps, so it cannot be
  placed. The default 30s steps blend two. Ramps meant to locate a knee
  need steps of 60s or more ([the GKE plan](#the-read-knee-is-measured-on-gke)),
  or series over a shorter rate window; for writes, longer steps cost keys
  against the cap.

### kind, 2026-10-10: per-step report check (smoke)

`steps.csv` checked with one short mix ramp on the validation KVCluster
(ledger 27,500 keys before, 34,500 after), with the change uncommitted:
`commit=0e0c4df-dirty`, `smoke=1`. A pipeline check, not a result.

| Run | Parameters | k6 summary | Prometheus (window) | k6 CPU |
|---|---|---|---|---|
| [20261010-230442](results/bench/20261010-230442-kind-mix-ramp/) | mix, ramp 100 to 300/s (50 to 150 per op), steps of 100, 20s held, 5s moves (70s), 120 VUs | Put: 6,916, p99 1,411.6 ms; Get: 6,999, p99 394.9 ms; 0 errors; 83 dropped | Put p50 19.68 / p99 1,650.5 ms; Get p99 368.7 ms; fsync p99 14.61 ms; spread 1.56 / 0.86 | max 171m |

| Step | Op | Target/s | Achieved/s | p50 | p95 | p99 (ms) | Errors | Dropped |
|---|---|---|---|---|---|---|---|---|
| 1 | put | 50 | 50.0 | 11.35 | 14.35 | 17.37 | 0 | 0 |
| 1 | get | 50 | 50.0 | 1.06 | 1.82 | 6.59 | 0 | 0 |
| 2 | put | 100 | 100.0 | 12.62 | 35.94 | 100.09 | 0 | 0 |
| 2 | get | 100 | 100.0 | 0.95 | 10.31 | 21.03 | 0 | 0 |
| 3 | put | 150 | 145.8 | 173.57 | 1,286.17 | 1,519.30 | 0 | 84 |
| 3 | get | 150 | 149.9 | 35.18 | 279.22 | 690.29 | 0 | 1 |

What it checked: the three held rates came out as steps 1-3 with their
targets, each op on its own row; the steps' derived drops (85) match k6's
total (83) within one per step; and the knee shows where a whole-run
summary could not: Put p99 17, 100, then 1,519 ms. On kind, with this
short a ramp, mix's knee lies between 200 and 300/s in all (writes
between 100 and 150/s), in line with the preloads' 145/s. That is an
observation from a smoke run, not a measured knee.

### GKE, 2026-10-13: session 2 (the benchmarks)

The [Session 2 runbook](gke.md#session-2-the-benchmarks-runbook), from
commit `b7a905f`, clean tree. **Kubernetes v1.36.4-gke.1391002**: the
Regular channel's default had moved on since session 1's 1.35.8, so this
session's cluster is not session 1's version. europe-west1; the database
pool 6 × e2-standard-2 (two in each of zones b, c, d; 1930m allocatable
each), the system node e2-standard-2, the loadgen pool n2-standard-4 in
zone b (one node, two for the two-pod step). Images by digest:
`kvnode@sha256:7ddae67c7a32…`, `kvstore-operator@sha256:94eff1d78e73…`
(tag `b7a905f-20261013090908`). k6 requested 3 CPUs; on each loadgen node
GKE's own pods requested 409m of the 3920m allocatable (`run.sh` saw 403m
at run time), so it fit. `terraform apply` took 10m43s; the first run
started 15 minutes later, the last ended at 10:51 UTC.

Two KVClusters, each from `reset` and a clean preload of 20,000 keys, each
ending at the cap's 128,000: series A (reads and writes, uid `8b102624…`)
and series B (mix, then the two-pod step, uid `36f54332…`).

**The knees:**

| Load | Knee | How it was read |
|---|---|---|
| Reads | **about 4000/s** | Two k6 pods: dropped iterations start at 4000/s with k6 well under its limit, and the server-side Get p99 accelerates (12.4, 18.7, 23.6 ms at 4000, 4500, 5000/s) as the busiest kv pod reaches 1.3-1.6 of its node's 1.93 cores. One pod was not enough (below) |
| Writes, tail (p99) | **300-400/s** | Series A's write ramp at 300/s (p99 17 to 104 ms, no anti-entropy in the step); in the mix the tail turned at 400 writes/s |
| Writes, median (p50) | **500/s** | Series A's write ramp (p50 7.9 to 14.4 ms), **flagged**: kv-5's anti-entropy round fell in that step. The mix confirms it without anti-entropy: p50 16.1 ms at 500 writes/s |
| Mix | **tail at 800/s in all, median at 1000/s** (400 and 500 writes/s) | Flagged SPREAD (1.62) |

| Run | Scenario | Parameters | Data before | k6 summary | Prometheus (window) | k6 CPU | Flags |
|---|---|---|---|---|---|---|---|
| [20261013-092406](results/bench/20261013-092406-gke-preload-shared/) | A. preload | 20,000 keys, 40 VUs (closed) | 0 keys | 20,000 Puts in 30.2s (663.1/s); p50 37.8, p95 197.4, p99 385.9 ms; 0 errors, **clean** | Put p50 40.3 / p99 397.9 ms; fsync p50 1.79 / p99 4.55 ms; kv CPU 0.32-0.51 cores | not sampled (the run was shorter than metrics-server's first sample) | SPREAD (1.70; not measured) |
| [20261013-092600](results/bench/20261013-092600-gke-reads-constant/) | A. reads, constant | 1000/s, 300s | 20,000 keys | 300,001 Gets (1000.0/s); p50 1.55, p95 2.48, p99 4.74 ms; 0 errors; 0 dropped | Get p50 0.95 / p99 3.63 ms; kv CPU 0.44-0.66 cores; spread 1.37 / 0.74 | max 567m | AE (2) |
| [20261013-093216](results/bench/20261013-093216-gke-reads-ramp/) | A. reads, ramp | 1000 to 5000/s, steps of 500, 60s held (9m40s) | 20,000 keys | 1,739,897 Gets; p99 16.08 ms; 0 errors; 95 dropped (steps 7-9, and 3 in step 1) | Get p50 1.20 / p99 9.79 ms; kv CPU 0.84-1.07 cores; spread 1.07 / 0.84 | max 1944m: **over 1500m from step 6** | AE (4); valid to step 5 only |
| [20261013-094331](results/bench/20261013-094331-gke-writes-constant/) | A. writes, constant | 40/s, 300s | 20,000 keys | 12,001 Puts (40.0/s); p50 7.80, p95 9.43, p99 10.62 ms; 0 errors; 0 dropped | Put p50 6.68 / p99 9.96 ms; fsync p50 1.95 / p99 4.91 ms; kv CPU 0.04-0.06 cores; spread 1.28 / 0.77 | max 75m | |
| [20261013-094941](results/bench/20261013-094941-gke-writes-ramp/) | A. writes, ramp | 100 to 500/s, steps of 100, 60s held (5m20s) | 32,000 keys | 95,998 Puts; p99 150.10 ms; 0 errors; 0 dropped | Put p50 7.52 / p99 189.50 ms; fsync p50 1.87 / p99 4.92 ms; kv CPU 0.21-0.37 cores; spread 1.50 / 0.88 | max 304m | AE (2: kv-2 in steps 1-2, kv-5 in step 5) |
| [20261013-100305](results/bench/20261013-100305-gke-preload-shared/) | B. preload | 20,000 keys, 40 VUs (closed) | 0 keys | 20,000 Puts (615.8/s); p50 33.8, p95 269.4, p99 530.0 ms; 0 errors, **clean** | Put p50 32.6 / p99 712.7 ms; fsync p99 4.87 ms | not sampled | SPREAD (1.84; not measured) |
| [20261013-100457](results/bench/20261013-100457-gke-mix-constant/) | B. mix, constant | 80/s (40 writes + 40 reads), 300s | 20,000 keys | Put: 12,001, p50 7.48, p95 9.50, p99 11.13 ms; Get: 12,001, p50 2.23, p95 3.14, p99 4.85 ms; 0 errors; 0 dropped | Put p50 6.56 / p99 10.26 ms; Get p50 1.75 / p99 4.20 ms; fsync p99 4.94 ms; kv CPU 0.10-0.18 cores; spread 1.35 / 0.60 | max 93m | AE (2) |
| [20261013-101106](results/bench/20261013-101106-gke-mix-ramp/) | B. mix, ramp | 200 to 1000/s, steps of 200, 60s held (5m20s) | 32,000 keys | Put: 95,596, p99 243.54 ms; Get: 95,999, p99 74.22 ms; 0 errors; 403 dropped (step 5) | Put p50 7.17 / p99 225.38 ms; Get p50 2.16 / p99 67.40 ms; fsync p99 4.90 ms; kv CPU 0.31-0.47 cores; spread 1.62 / 0.83 | max 545m | **SPREAD (1.62)**; no AE |
| [20261013-102911](results/bench/20261013-102911-gke-reads-ramp/) | B. reads, ramp, 1 k6 pod | as A's read ramp | 128,000 keys | 1,739,357 Gets; p99 31.89 ms; 0 errors; 626 dropped (steps 7-9, and 4 in step 1) | Get p50 1.91 / p99 20.76 ms; kv CPU 0.86-1.19 cores; spread 1.15 / 0.74 | max 1938m: **over 1500m from step 6** | valid to step 5 only |
| [20261013-103959](results/bench/20261013-103959-gke-reads-ramp/) | B. reads, ramp, **2 k6 pods** | as A's read ramp, `K6_PODS=2`, one loadgen node each | 128,000 keys | pod 0: 869,885 Gets, p99 23.77 ms, 113 dropped; pod 1: 869,873, p99 24.11 ms, 124 dropped; 0 errors | Get p50 1.72 / p99 14.99 ms; kv CPU 0.96-1.17 cores; spread 1.34 / 0.83 | max 1511m (pod 0, step 9 only) | AE (2, in steps 1-3); valid to step 8 |

**Reads, series A, one k6 pod** (`steps.csv`; server-side Get p99 from
Prometheus, the 15s points inside each step; k6's CPU from `top.txt`):

| Step | Target/s | Achieved/s | p50 | p95 | p99 (ms) | Dropped | Server p99 (ms) | k6 CPU |
|---|---|---|---|---|---|---|---|---|
| 1 | 1000 | 1000.0 | 1.53 | 2.26 | 3.85 | 3 | 2.5 | 601m |
| 2 | 1500 | 1500.0 | 1.45 | 2.66 | 6.10 | 0 | 3.7-4.0 | 720m |
| 3 | 2000 | 2000.0 | 1.45 | 3.43 | 9.00 | 0 | 6.4-7.0 | 807-1020m |
| 4 | 2500 | 2500.0 | 1.48 | 3.84 | 10.05 | 0 | 5.1-5.4 | 1028-1205m |
| 5 | 3000 | 3000.0 | 1.60 | 5.39 | 13.06 | 0 | 7.2-8.0 | 1245-1371m |
| 6 | 3500 | 3500.0 | 1.75 | 6.66 | 15.20 | 0 | 9.0-9.3 | **1371-1512m** |
| 7 | 4000 | 3999.6 | 1.82 | 7.99 | 18.09 | 25 | 9.5-11.7 | **1502-1635m** |
| 8 | 4500 | 4499.1 | 2.09 | 9.17 | 18.83 | 57 | 12.0-12.4 | **1635-1790m** |
| 9 | 5000 | 4999.8 | 2.27 | 9.87 | 19.73 | 13 | 12.6-13.6 | **1790-1944m** |

**Writes, series A** (server side: Put p50 / p99 and data-engine fsync p99):

| Step | Target/s | Achieved/s | p50 | p95 | p99 (ms) | Dropped | Server Put p50 / p99 (ms) | fsync p99 (ms) |
|---|---|---|---|---|---|---|---|---|
| 1 | 100 | 100.0 | 7.36 | 9.09 | 10.35 | 1 | 6.4 / 9.9 | 4.9 |
| 2 | 200 | 200.0 | 7.21 | 10.27 | 16.61 | 0 | 6.4 / 9.9-14 | 4.9 |
| 3 | 300 | 300.0 | 7.97 | 19.59 | **104.24** | 0 | 7.0-7.4 / **94-135** | 6.0-6.1 |
| 4 | 400 | 400.0 | 7.85 | 19.81 | 41.42 | 0 | 7.2 / 33-178 | 4.7-5.0 |
| 5 (AE) | 500 | 500.0 | **14.44** | **96.08** | **241.49** | 0 | **8.9-14.4 / 248-280** | 4.8-4.9 |

**Mix, series B** (each op's target is half the step's total):

| Step | Total/s | Op | Target/s | Achieved/s | p50 | p95 | p99 (ms) | Dropped | Server p50 / p99 (ms) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 200 | put | 100 | 100.0 | 7.19 | 8.81 | 10.02 | 1 | 6.4 / 9.9 |
| | | get | 100 | 100.0 | 2.23 | 3.68 | 5.13 | 1 | - / 4.5-4.7 |
| 2 | 400 | put | 200 | 200.0 | 6.72 | 8.84 | 12.04 | 0 | 6.2 / 11.7-13.3 |
| | | get | 200 | 200.0 | 2.14 | 4.04 | 5.82 | 0 | - / 4.9-5.1 |
| 3 | 600 | put | 300 | 300.0 | 6.96 | 10.26 | 16.60 | 0 | 6.2-6.5 / 14.5-16.4 |
| | | get | 300 | 300.0 | 2.19 | 5.01 | 8.15 | 0 | - / 6.6-7.4 |
| 4 | 800 | put | 400 | 400.0 | 7.67 | 17.45 | **64.92** | 0 | 6.6-7.1 / **51-89** |
| | | get | 400 | 400.0 | 2.41 | 7.00 | **20.24** | 0 | - / **9-19** |
| 5 | 1000 | put | 500 | 493.3 | **16.07** | **152.48** | **550.36** | **402** | **8.9-16.4 / 170-743** |
| | | get | 500 | 500.0 | 4.60 | 49.95 | 170.03 | 0 | - / 39-207 |

**Reads, series B: one k6 pod against two, back to back on the same data**
(two pods: each pod's own p99; their achieved rates add to the target):

| Step | Total/s | 1 pod: p50 / p95 / p99 (ms) | 2 pods: p99, pod 0 / pod 1 (ms) | Server p99 (ms), 1 pod → 2 pods | Dropped, 1 pod → 2 pods | k6 CPU: 1 pod; pod 0 / pod 1 |
|---|---|---|---|---|---|---|
| 1 | 1000 | 1.67 / 2.65 / 5.65 | 8.62 / 8.54 | 4.7 → 5.3 | 4 → 2 | 592m; 412 / 308m |
| 2 | 1500 | 1.61 / 2.90 / 6.29 | 7.99 / 8.20 | 4.6 → 5.4 | 0 → 0 | 702m; 591 / 434m |
| 3 | 2000 | 1.65 / 3.96 / 10.29 | 8.74 / 8.71 | 6.5 → 5.0 | 0 → 1 | 1021m; 745 / 550m |
| 4 | 2500 | 1.76 / 5.84 / 16.79 | 16.35 / 16.55 | 7.5 → 10.1 | 0 → 1 | 1216m; 898 / 662m |
| 5 | 3000 | 2.13 / 9.76 / 24.17 | 15.91 / 15.34 | 14.9 → 9.9 | 0 → 0 | 1353m; 1001 / 744m |
| 6 | 3500 | 2.57 / 13.29 / 30.88 | 17.20 / 17.19 | 18.0 → 11.6 | 0 → 0 | **1506m**; 1168 / 857m |
| 7 | 4000 | 2.76 / 15.39 / 35.51 | 21.00 / 20.86 | **23.8 → 12.4** | **104 → 46** | **1602m**; 1292 / 950m |
| 8 | 4500 | 3.12 / 17.47 / 36.34 | 28.81 / 30.28 | 23.0 → 18.7 | 295 → 37 | **1696m**; 1413 / 1057m |
| 9 | 5000 | 3.69 / 20.83 / 39.74 | 33.27 / 34.48 | 28.2 → 23.6 | 223 → 157 | **1938m**; **1511** / 1102m |

The busiest kv pod's CPU in the two-pod run, by step: 1.02, 1.06, 0.99,
1.08, 1.15, 1.23, 1.29, 1.47, 1.58 cores, of 1.93 allocatable.

**Validity.** Errors: 0 in every run. Dropped iterations: 0 below each
knee, except a handful in the first step of each read ramp (3, 4, 2) while
the 120 connections open, and those of the one-pod read ramps above k6's
limit. k6's CPU against 1500m: within it everywhere but the two one-pod
read ramps from step 6, and pod 0 of the two-pod ramp in step 9. Spread:
within 1.5 everywhere but the mix ramp (1.62: kv-0 coordinated 47,633
requests against a median of about 29,400, from its share of the
connections). Anti-entropy rounds: flagged per run above; the steps that
locate the knees are free of them except series A's write step 5.

**One k6 pod was not enough for reads.** The two read ramps of series B
started dropping iterations at the same step (4000/s), but the server-side
Get p99 there was 23.8 ms with one pod and 12.4 ms with two, far outside
the 10% the [two-pod check](#is-the-database-the-bottleneck) allows, and
it differed from step 5 on, as the one pod passed about 1350m. A k6 pod
near its CPU limit sends its requests in bursts, and the database's own
latency shows it. So the read knee is the two-pod run's; the one-pod read
results above 3000/s (series A's included) do not count. With two pods,
the drops at 4000/s come with k6 at 950-1292m, so they are the database's
stalls (max latency about 180 ms), not k6's.

**Series A and B differ at the same rates.** One pod, same ramp: at
3000/s the server-side Get p99 was 7.2-8.0 ms in series A (20,000 keys)
and 14.4-15.6 ms in series B (128,000 keys, after the mix ramp). The data
size, or the state the mix ramp left (compactions, memtable flushes), may
explain it; this session does not separate the two.

**Why the writes' latency rises while CPU and fsync do not (the likely
explanation, to confirm).** Through the write ramp the kv pods stayed at
0.21-0.37 cores and the data-engine fsync p99 at 4.7-6.1 ms, while Put p99
went from 10 to 100-280 ms. That fits writes queueing for the engine's
write lock before each fsync: `Engine.Put` holds `writeMu` across the WAL
append (`internal/storage/engine/engine.go`), the append holds the WAL's
lock across the write and the fsync (`internal/storage/wal/wal.go`), and
`kv_wal_fsync_duration_seconds` times only the `Sync` call, not the wait
for the lock. Each Put is stored on 3 of the 6 nodes, so at 300 Puts/s
each node does about 150 local writes/s; at an fsync of about 2-3 ms
(p50 1.9 ms, p99 4.9 ms) that keeps one serialized fsync path busy 30-50%
of the time, and at 500/s 60-75%, where queueing grows quickly and the
tail first. To confirm with a histogram of the wait for `writeMu`; group
commit (one fsync for the writes that queued behind it) is the fix it
points to. See [Open questions](#open-questions).

**Operational notes.** Prometheus found kv-0 and kv-4 about half a minute
after the others (discovery lag, not a fault). A preload of about 30s is
too short for GKE's metrics-server to sample k6's CPU; the measured runs
were all sampled. The two-pod resize was made with `gcloud`, because
`terraform apply` also wanted an unrelated in-place change to the
cluster's `node_config`; see [gke.md](gke.md#session-2-the-benchmarks-runbook).
