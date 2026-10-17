# GKE session 1 (2026-10-10)

The record of the first session on the GKE cluster of
[gke.md](../../gke.md), which ran its [A session](../../gke.md#a-session)
runbook end to end. The CSVs here are its Prometheus time series.

The whole of [A session](../../gke.md#a-session) up to the teardown, on the cluster as
created (europe-west1; GKE 1.35.8-gke.1225000), from commit `cce60c7`. The
writer ran from step 7 to the read-back, about 11 writes/s.

| Step | Took | Result |
|---|---|---|
| 2. kubectl | 20s | 7 nodes: 2 `kvstore` in each of b, c, d; `system` in b |
| 3. Build and push both images | 4m26s | Digests recorded; the tagged digests are the ones pushed |
| 4. Operator | 24s | On the system node, from the digest |
| 5. KVCluster 6 | 23s | Ready 6/6; 6 distinct nodes, 2 per zone; `data-kv-*` 10Gi `standard-rwo`, Bound |
| 5. Membership | 11s | every pod `epoch=0 members=6 peers=5 alive=5 reachable=5` |
| 6. Observability | 59s | 6 of 6 targets up; Prometheus and Grafana on the system node |
| 7. Traffic | 98s | coordinated 1.6-2.0 Put/s per pod (10.9 in all); stored 3.3-7.4 fsync/s per pod (31.3 in all, about 3 per Put); 1,016 writes, 0 errors |
| 6 -> 7, a new node | 174s | Ready at epoch 1 |
| 7 -> 6 | 20s | Ready at epoch 2; `data-kv-6` deleted |
| 6 -> 7, node already there | 25s | Ready at epoch 3 |
| 7 -> 6, operator pod deleted in D2 | 41s | Ready at epoch 4 |
| 9. Read back | 167s | `checked=4994 missing=0`; the writer saw 0 failures |

**Scale-up onto a new node, 174s** (times from the scale command). kv-6
was unschedulable at once (6 nodes had a kvstore pod; the system node lacks
the pool's label), and the cluster autoscaler grew the zone-c group from 2 to 3 nodes 2s later
(`TriggeredScaleUp`; every zone had 2 pods, so any zone satisfied the
spread). The node was Ready at +86s, but kv-6 was scheduled only at +146s:
provisioning its zonal disk first failed with `no topology key found for
node` (the persistent disk CSI driver had not registered on the new node
yet) and succeeded a minute later. That minute put the scale-up past the
2-minute grace period, so the cluster was `Degraded`/`PodUnavailable`
(`kv-6 has been not ready for 2m2s`) from +127s to +149s, and cleared on
its own when the pod scheduled. The pod was Ready at +163s, every handoff
done at +174s. The next scale-up, onto the same node (the autoscaler had not
removed it yet), took 25s.

**Scale-down, 20s.** The drain took about 6s (kv-6 had been a member for
3 minutes). While it drained, kv-6 reported not ready, as a node outside
its own membership does ([membership.md](../../membership.md)).

**The operator deleted in D2.** Deleted 4s into the drain (`1243 keys
still to hand off`); its replacement was running 4s later and ready 15s
after the delete, and carried on from D2: the ConfigMap at epoch 4 at +20s
after the delete, Ready at +37s. The epoch moved exactly once for the
removal (3 to 4): from epoch 0 to 4 over the two cycles, one per membership
change.

**Latency.** Quantiles of every Put, and of every data-engine WAL fsync,
in each window: `histogram_quantile` over the buckets' `increase` across
the window, so each value is interpolated inside its bucket (Put buckets
5, 7.5, 10 ms; fsync buckets 1, 2.5, 5 ms around these values):

| Window | Puts | Put p50 | Put p99 | Fsyncs (data) | Fsync p50 | Fsync p99 |
|---|---|---|---|---|---|---|
| Before the first scale, 11:00:31-11:02:33 UTC (122s) | 1,308 | 6.31 ms | 9.63 ms | 3,797 | 1.88 ms | 4.85 ms |
| The whole writer run, 11:00:31-11:08:33 UTC (482s), both scale cycles | 4,968 | 6.28 ms | 9.93 ms | 36,789 | 1.85 ms | 4.85 ms |

The counts come from `increase()`, which extrapolates at the window's
edges: the writer's own log has 4,994 acknowledged Puts, all `OK`. Before
the first scale there were 2.9 fsyncs per Put (three replicas each); over
the whole run, 7.4, the rest being the four membership changes' handoffs
and repairs storing keys on their new owners. The two scale cycles moved
p99 by 0.3 ms. On kind (10 nodes, Docker Desktop;
[observability.md](../../observability.md#measured-on-kind)) the same writer saw
Put p50 / p99 17.0 / 42.9 ms and fsync 7.4 / 23.7 ms; the setups differ in
more than the disks, so this is not a like-for-like comparison.

Over 11:00:00-11:09:30: 347 hints created and 347 delivered (around the
membership changes), anti-entropy pulled 419 keys and pushed 2 (one peer
failure), and no quorum failures. Each pod's last handoff took 1.0-9.2 s.

The time series are in this directory, one CSV per query, 15 s steps
from 11:00:00 to 11:09:30 UTC (`time`, the series' labels, the value;
rates over 1 m):

| File | Query |
|---|---|
| `put_rate_by_code.csv` | `sum by (code) (rate(kv_client_request_duration_seconds_count{job="kvstore",method="Put"}[1m]))` |
| `put_latency_p50.csv`, `put_latency_p99.csv` | `histogram_quantile(0.50 / 0.99, sum by (le) (rate(kv_client_request_duration_seconds_bucket{job="kvstore",method="Put"}[1m])))` |
| `wal_fsync_latency_p50_data.csv`, `wal_fsync_latency_p99_data.csv` | the same over `kv_wal_fsync_duration_seconds_bucket{job="kvstore",engine="data"}` |
| `requests_coordinated_by_pod.csv` | `sum by (instance, method) (rate(kv_client_request_duration_seconds_count{job="kvstore"}[1m]))` |
| `storage_writes_by_pod.csv` | `sum by (instance) (rate(kv_wal_fsync_duration_seconds_count{job="kvstore",engine="data"}[1m]))` |
| `membership_epoch_by_pod.csv` | `kv_membership_epoch{job="kvstore"}` |
| `hints_created.csv`, `hints_delivered.csv` | `sum(rate(kv_hints_created_total{job="kvstore"}[1m]))`, the same for `kv_hints_delivered_total` |
| `antientropy_keys_repaired.csv` | `sum by (direction) (increase(kv_antientropy_keys_repaired_total{job="kvstore"}[1m]))` |
| `handoff_last_duration_by_pod.csv` | `kv_handoff_last_duration_seconds{job="kvstore"}` |

`kubectl top nodes`:

| When | `kvstore` nodes | `system` node |
|---|---|---|
| 6 replicas, no load | 124-153m CPU, about 960Mi | 691m (35%), 2,399Mi (39%) |
| 7 replicas, under load, just scaled | 235-381m, 920-982Mi | 936m (48%), 3,053Mi (50%) |
| 6 replicas, under load, after the cycles | 172-308m, 926-1,016Mi | 818m (42%), 3,042Mi (50%) |

The system node carries GKE's own components, the operator, Prometheus,
Grafana and the toolbox: half its memory, under half its CPU. Room for
more tooling (k6) there is limited; see the quotas above before adding a
second system node.

