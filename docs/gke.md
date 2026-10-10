# Running on GKE

`infra/gke/` is Terraform for everything the GKE experiments need, in one
project: a regional GKE Standard cluster with a dedicated database node pool
that matches the operator's GKE sample
(`operator/config/samples/kvstore_v1alpha1_kvcluster_gke.yaml`), a system
node pool, the network, and an Artifact Registry repository. The cluster
costs money every hour it exists: **every session ends with
`terraform destroy`**.

Session 1 (2026-10-10) ran this page end to end on the cluster it
describes: see [Session 1](#session-1-2026-10-10) for what was measured
and what differed from the plan. The prices and quotas below come from
Google's price list and the project's quotas.

> **Never run `make image`, `make operator-deploy` or the operator's e2e
> (`make operator-test-e2e`, `go test -tags e2e ./test/e2e/`) against GKE.**
> They are for the kind cluster: they install images that exist only on
> your machine, and the e2e deletes the KVCluster and its volumes before it
> starts. All three refuse to run unless kubectl's context is
> `kind-kvstore`, and the e2e names that context on every kubectl call, but
> the GKE path is this page, not the Makefile.

| Component | Version |
|---|---|
| Terraform | 1.16.x (`~> 1.16.0`), 64-bit |
| google provider | 8.6.x (`~> 8.6.0`), locked in `infra/gke/.terraform.lock.hcl` |
| GKE | Regular channel: the default for new clusters is 1.35.8-gke.1225000 (release notes of 2026-09-23, still the default on 2026-10-06) |

## What it creates

| Resource | Settings |
|---|---|
| APIs | compute, container, artifactregistry, iam. Left enabled on destroy. |
| Network | VPC `kvstore`, subnet `kvstore-nodes` (10.10.0.0/24) with secondary ranges `pods` (10.20.0.0/16) and `services` (10.30.0.0/20), Private Google Access, Cloud Router and Cloud NAT. |
| Cluster `kvstore` | Regional, release channel Regular, VPC-native, Dataplane V2 (enforces NetworkPolicy), private nodes, public control-plane endpoint open to `my_ip_cidr` only, the persistent disk CSI driver (`standard-rwo`), system-component logging and monitoring only, `deletion_protection = false`. The default node pool is removed. |
| Node pool `kvstore` | 3 zones; 2 nodes per zone, autoscaled 2-3 per zone; e2-standard-2; 20 GiB pd-balanced boot disks; taint `dedicated=kvstore:NoSchedule`; no Spot VMs; surge upgrades, 1 new node per zone before an old one is drained. |
| Node pool `system` | 1 e2-standard-2 in one zone, untainted: GKE's own components (kube-dns, metrics-server), the operator, Prometheus and Grafana. |
| Service account `kvstore-nodes` | The nodes run as it: `roles/container.defaultNodeServiceAccount`, and read access to the repository. |
| Artifact Registry `kvstore` | Docker, in the region. Destroyed with its images. |

Why these choices:

- **Private nodes behind Cloud NAT**, not a public address per node: a new
  billing account gets 8 in-use addresses per region, and 10 nodes at full
  autoscaling would need 10. NAT uses one.
- **e2-standard-2** is the smallest E2 machine that is not shared-core. A
  shared-core machine bursts, which would confound latency measurements.
- **No Spot VMs**: a preemption in the middle of a failure test would be a
  second, unplanned failure.
- **One system node**: see the quotas below.
- **Local state**, gitignored: one user, a short-lived project, and deleting
  the project is the final cleanup. A GCS backend needs a bucket that exists
  before the first apply and outlives every destroy, for no gain here. The
  state holds the cluster's endpoint and CA certificate; never commit it.

## Before the first session

1. **Terraform 1.16, 64-bit.** `terraform version` must say `windows_amd64`
   (a `windows_386` build is the 32-bit one). Download
   `terraform_1.16.5_windows_amd64.zip` and `terraform_1.16.5_SHA256SUMS`
   from [releases.hashicorp.com/terraform/1.16.5](https://releases.hashicorp.com/terraform/1.16.5/),
   check the hash, and put `terraform.exe` first on `PATH`:

   ```powershell
   Get-FileHash terraform_1.16.5_windows_amd64.zip -Algorithm SHA256
   Select-String windows_amd64 terraform_1.16.5_SHA256SUMS
   Expand-Archive terraform_1.16.5_windows_amd64.zip -DestinationPath C:\tools\terraform
   ```

   (or `winget install Hashicorp.Terraform`, which installs the current
   release; check that it is 1.16.x).
2. **The gcloud configuration `kvstore`** (account, project `kvstore-gke`,
   region): `gcloud config configurations activate kvstore`. Terraform does
   not read it (the project and region come from `terraform.tfvars`), but
   every `gcloud` and `kubectl` command below assumes it.
3. **Application Default Credentials**, which Terraform uses:
   `gcloud auth application-default login`. No key files.
4. **Variables**: copy `infra/gke/example.tfvars` to
   `infra/gke/terraform.tfvars` (gitignored) and set `my_ip_cidr` to your
   public address, `/32`.
5. **gke-gcloud-auth-plugin**, which kubectl uses to authenticate to GKE:
   `gcloud components install gke-gcloud-auth-plugin`, then
   `gke-gcloud-auth-plugin --version`. Without it, `kubectl` fails after
   `get-credentials`.
6. **Your public address, at the start of every session and again before
   the teardown.** The control plane accepts `my_ip_cidr` only; from any
   other address kubectl is refused, and the teardown's kubectl steps
   cannot run (the data disks would then outlive the cluster). Compare:

   ```sh
   curl -s https://checkip.amazonaws.com
   grep my_ip_cidr infra/gke/terraform.tfvars
   ```

   If they differ, set the new address in `terraform.tfvars` and run
   `terraform -chdir=infra/gke apply`: the plan must say `1 to change` (the
   cluster's authorized networks, updated in place; no node restarts).

### Quotas

The region's quotas for `kvstore-gke` (europe-west1): `CPUS` 200,
`E2_CPUS` 24, `CPUS_ALL_REGIONS` 32, `SSD_TOTAL_GB` 500,
`IN_USE_ADDRESSES` 8. `E2_CPUS` is the one that binds:

| Situation | Nodes | E2 vCPUs |
|---|---|---|
| Created (6 database + 1 system) | 7 | 14 |
| Scaled to 7 | 8 | 16 |
| Autoscaler at its maximum (9 + 1) | 10 | 20 |
| A database pool upgrade while at 7 replicas (8 + 3 surge) | 11 | 22 |
| A database pool upgrade at the maximum (10 + 3 surge) | 13 | 26 |

`maxSurge` counts **per zone**, so a surge upgrade of the 3-zone database
pool adds up to 3 nodes. Only the last row exceeds 24, and then the upgrade
waits: "If you don't have additional capacity, GKE won't start upgrading a
node until the resources are available"
([node pool upgrade strategies](https://cloud.google.com/kubernetes-engine/docs/concepts/node-pool-upgrade-strategies)).
A second system node would make the 7-replica upgrade 24 and leave nothing
for anything else, hence one.

**pd-balanced counts against `SSD_TOTAL_GB`**, both boot disks and the
data volumes (`standard-rwo` is pd-balanced): 13 boot disks of 20 GiB and
9 data volumes of 10 GiB is 350 GiB of the 500. The default node pool GKE
creates and removes while the cluster is created uses three more boot disks
(and three e2-small nodes, 6 E2 vCPUs) for a few minutes.

## Cost

europe-west1, on demand, from the Cloud Billing catalog on 2026-10-07:

| Item | Price | Created (7 nodes) | Maximum (10 nodes) |
|---|---|---|---|
| Regional cluster fee | $0.10/h | $0.100 | $0.100 |
| e2-standard-2 (2 E2 cores at $0.02399337/h, 8 GiB at $0.00321609/GiB·h) | $0.0737/h | $0.516 | $0.737 |
| Boot disks, 20 GiB pd-balanced each | $0.10/GiB·month | $0.019 | $0.027 |
| Data volumes, 10 GiB pd-balanced each (6, up to 9) | $0.10/GiB·month | $0.008 | $0.012 |
| Cloud NAT: gateway per VM, and its address | $0.0014/VM·h, $0.005/h | $0.015 | $0.019 |
| **Total** | | **≈ $0.66/h** | **≈ $0.90/h** |

Plus Cloud NAT data processing at $0.045/GiB (mostly image pulls, a GiB or
two per session) and Artifact Registry storage (negligible). The free
tier's GKE credit does not apply to regional clusters
([GKE pricing](https://cloud.google.com/kubernetes-engine/pricing)). The
same cluster in us-central1 would be about $0.61/h created: its
e2-standard-2 is $0.0670/h, 10% less.

## A session

From the repository root, in Git Bash, with the `kvstore` configuration
active. First check your address (item 6 above). The checks below parse
JSON with `python`.

**Every kubectl command names the GKE context** (`--context $CTX`).
`get-credentials` makes it the current context, but the kind cluster lives
on the same machine: a later `kubectl config use-context kind-kvstore`
would otherwise send this page's commands, the teardown's deletes among
them, to kind. In Git Bash, a `kubectl exec` whose command is an absolute
path needs `MSYS_NO_PATHCONV=1`, or Git Bash rewrites `/scripts/...` into a
Windows path.

```sh
R=europe-west1
CTX=gke_kvstore-gke_${R}_kvstore
```

1. **Create.**

   ```sh
   cd infra/gke
   terraform init        # once, or after a version change
   terraform apply       # review the plan before answering yes
   terraform output
   cd ../..
   ```

2. **Point kubectl at it**, and check that it does:

   ```sh
   $(terraform -chdir=infra/gke output -raw get_credentials)
   kubectl config get-contexts -o name | grep -x "$CTX"
   kubectl --context $CTX get nodes -L cloud.google.com/gke-nodepool,topology.kubernetes.io/zone
   ```

   Seven nodes: two `kvstore` nodes in each of three zones, one `system`.

3. **Push the images**, under a tag no earlier build used, and record
   the digests the registry gave them:

   ```sh
   REG=$(terraform -chdir=infra/gke output -raw registry)
   $(terraform -chdir=infra/gke output -raw configure_docker)   # once per machine
   TAG=$(git rev-parse --short HEAD)-$(date -u +%Y%m%d%H%M%S)
   docker build -t $REG/kvnode:$TAG .
   docker build -t $REG/kvstore-operator:$TAG operator
   docker push $REG/kvnode:$TAG
   docker push $REG/kvstore-operator:$TAG
   digest() { docker inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$REG/$1:$TAG" | grep "^$REG/$1@sha256:"; }
   NODE_IMG=$(digest kvnode)
   OPERATOR_IMG=$(digest kvstore-operator)
   echo "$NODE_IMG"; echo "$OPERATOR_IMG"     # each <registry>/<name>@sha256:<64 hex>
   ```

   Why a digest: neither the operator's Deployment nor the StatefulSet sets
   `imagePullPolicy`, so Kubernetes uses `IfNotPresent` for any tag other
   than `latest`. A node that already has an image under a tag never pulls
   that tag again, so an image rebuilt under a reused tag would silently not
   run. A digest names exactly one image: what was pushed is what runs, and
   the KVCluster's `spec.image` records which. The unique tag keeps the
   registry readable; the digest is what is deployed. (Docker Desktop
   pushes each image as an index with an attestation manifest, so the
   registry also lists two untagged digests per image; the tagged one is
   the index, and the one to deploy.)

4. **Install the operator** with the registry image, as in
   [operator.md](operator.md#install) with the kind image replaced:

   ```sh
   kubectl kustomize operator/config/default \
     | sed "s|image: controller:latest|image: $OPERATOR_IMG|" \
     | kubectl --context $CTX apply --server-side -f -
   kubectl --context $CTX -n kvstore-operator-system rollout status deploy/kvstore-operator-controller-manager
   kubectl --context $CTX -n kvstore-operator-system get pods -o wide   # on the system node
   ```

   It lands on the system node: it does not tolerate the database pool's
   taint.

5. **Create the cluster** from the GKE sample, with the image set:

   ```sh
   kubectl create namespace kvstore --dry-run=client -o yaml | kubectl --context $CTX apply -f -
   sed "s|REGION-docker.pkg.dev/PROJECT/REPOSITORY/kvnode:TAG|$NODE_IMG|" \
     operator/config/samples/kvstore_v1alpha1_kvcluster_gke.yaml \
     | kubectl --context $CTX -n kvstore apply -f -
   kubectl --context $CTX -n kvstore get kvc kv -w        # until Ready 6/6 (epoch 0 is not shown)
   ```

   **Placement**: every pod, its node and the node's zone. Expect 6
   distinct nodes, and each of the 3 zones twice:

   ```sh
   kubectl --context $CTX -n kvstore get pods -l app.kubernetes.io/name=kvstore \
     -o custom-columns=POD:.metadata.name,NODE:.spec.nodeName --no-headers \
     | while read pod node; do
         echo "$pod $node $(kubectl --context $CTX get node "$node" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}')"
       done
   ```

   **Membership, before any writes**: every pod's own view, from inside
   the cluster (the toolbox in `deploy/loadgen/loadgen.yaml`). Expect each
   of kv-0 .. kv-5 to print `epoch=0 members=6 peers=5 alive=5 reachable=5`:

   ```sh
   kubectl --context $CTX apply -f deploy/loadgen/loadgen.yaml
   kubectl --context $CTX -n kvstore wait pod/toolbox --for=condition=Ready --timeout=120s
   for i in 0 1 2 3 4 5; do
     kubectl --context $CTX -n kvstore exec toolbox -- wget -qO- "http://kv-$i.kv.kvstore.svc.cluster.local:8080/admin/membership" \
       | python -c "import json,sys; m=json.load(sys.stdin); p=m['peers'].values(); print('kv-$i', 'epoch=%d members=%d peers=%d alive=%d reachable=%d' % (m['epoch'], len(m['members']), len(m['peers']), sum(x['alive'] for x in p), sum(x['reachable'] for x in m['peers'].values())))"
   done
   ```

6. **Observability** (Prometheus and Grafana, on the system node). Wait
   for both, then check that Prometheus scrapes all six pods: expect six
   lines, `up` 1 for kv-0 .. kv-5.

   ```sh
   kubectl --context $CTX apply -k deploy/observability/
   kubectl --context $CTX -n observability rollout status deploy/prometheus
   kubectl --context $CTX -n observability rollout status deploy/grafana
   kubectl --context $CTX -n observability port-forward svc/prometheus 9090 &
   q() { curl -s localhost:9090/api/v1/query --data-urlencode "query=$1" \
         | python -c "import json,sys; [print(r['metric'], r['value'][1]) for r in json.load(sys.stdin)['data']['result']]"; }
   q 'up{job="kvstore"}'
   ```

   Grafana: `kubectl --context $CTX -n observability port-forward svc/grafana 3000`.
   GKE already runs metrics-server, so `kubectl --context $CTX top nodes`
   works without the kind-specific install of [kubernetes.md](kubernetes.md).

7. **Traffic reaches every pod.** Start the steady writer (about 11
   writes/s through `kv-client`), give it two minutes, and ask Prometheus:

   ```sh
   kubectl --context $CTX apply -f deploy/loadgen/writer.yaml
   sleep 120
   q 'sum by (instance, method) (rate(kv_client_request_duration_seconds_count{job="kvstore"}[1m]))'
   q 'sum by (instance) (rate(kv_wal_fsync_duration_seconds_count{engine="data"}[1m]))'
   ```

   The first is the requests each pod coordinated (the writer sends only
   Puts); the second, the writes each pod stored, as coordinator or replica
   (one data-engine fsync per write; three per Put across the cluster).
   **All six pods must be above zero on both.** A pod at zero on the first
   gets no client traffic (check `kubectl --context $CTX -n kvstore get
   endpoints kv-client`); at zero on the second, it stores nothing. Grafana
   shows the same two series by pod: "Requests coordinated by pod"
   (Requests row) and "Storage writes by pod (data engine)" (Storage row).
   Leave the writer running through the experiments.

8. **The experiments**, e.g. `kubectl --context $CTX -n kvstore scale
   kvc/kv --replicas=7` and back to 6 (see [Session 1](#session-1-2026-10-10)
   for what to expect).

9. **Read back every acknowledged write.** Save the writer's log before
   deleting it (the log goes with the pod), then check every key it
   acknowledged through the toolbox. Expect `checked=<n> missing=0`:

   ```sh
   kubectl --context $CTX -n kvstore logs load > load.log
   kubectl --context $CTX -n kvstore delete pod load
   awk '$4=="ok" {print $3, "v-" $3}' load.log > acked.txt
   MSYS_NO_PATHCONV=1 kubectl --context $CTX -n kvstore exec -i toolbox -- /scripts/verify.sh < acked.txt | tail -1
   ```

   It reads one key at a time: about 3 minutes for 5,000 keys.

10. **Destroy**, every time. Check your address first (item 6 above): the
   first steps need kubectl. The data disks go first, while the cluster can
   still delete them: deleting the KVCluster keeps its volumes (the
   StatefulSet's `whenDeleted: Retain`), and `terraform destroy` does not
   know about them.

   ```sh
   kubectl --context $CTX -n kvstore delete kvc kv
   kubectl --context $CTX -n kvstore delete pvc --all     # standard-rwo deletes each disk
   kubectl --context $CTX wait --for=delete pv --all --timeout=5m
   terraform -chdir=infra/gke destroy
   ```

   Then check that nothing is left. Every list must be empty (the
   repository goes with `terraform destroy`, its images with it):

   ```sh
   gcloud container clusters list --project kvstore-gke
   gcloud compute instances list --project kvstore-gke
   gcloud compute disks list --project kvstore-gke
   gcloud compute addresses list --project kvstore-gke
   gcloud artifacts repositories list --project kvstore-gke --location $R
   ```

   A disk still listed costs money until deleted
   (`gcloud compute disks delete <name> --zone <zone> --project kvstore-gke`).

## Session 1 (2026-10-10)

The whole of [A session](#a-session) up to the teardown, on the cluster as
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
its own membership does ([membership.md](membership.md)).

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
[observability.md](observability.md#measured-on-kind)) the same writer saw
Put p50 / p99 17.0 / 42.9 ms and fsync 7.4 / 23.7 ms; the setups differ in
more than the disks, so this is not a like-for-like comparison.

Over 11:00:00-11:09:30: 347 hints created and 347 delivered (around the
membership changes), anti-entropy pulled 419 keys and pushed 2 (one peer
failure), and no quorum failures. Each pod's last handoff took 1.0-9.2 s.

The time series are in [results/gke-session1/](results/gke-session1/), one
CSV per query, 15 s steps from 11:00:00 to 11:09:30 UTC (`time`, the
series' labels, the value; rates over 1 m):

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

When the project is no longer needed, delete it
(`gcloud projects delete kvstore-gke`): that removes anything Terraform's
state lost track of.

## The gcloud configuration

`kvstore` is a separate gcloud configuration, so that commands here never
run against another project by accident. `gcloud config configurations
list` shows them all; `gcloud config configurations activate default`
switches back to the one that was active before, and
`gcloud config configurations activate kvstore` returns here. A single
command can also name one: `gcloud --configuration=default …`.
