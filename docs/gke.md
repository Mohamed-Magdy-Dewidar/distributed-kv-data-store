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
| GKE | Regular channel: the default for new clusters is 1.35.8-gke.1225000 (release notes of 2026-09-23, still the default on 2026-10-06); session 2's cluster (2026-10-13) got 1.36.4-gke.1391002. The channel's default moves: record the version of every session |

## What it creates

| Resource | Settings |
|---|---|
| APIs | compute, container, artifactregistry, iam. Left enabled on destroy. |
| Network | VPC `kvstore`, subnet `kvstore-nodes` (10.10.0.0/24) with secondary ranges `pods` (10.20.0.0/16) and `services` (10.30.0.0/20), Private Google Access, Cloud Router and Cloud NAT. |
| Cluster `kvstore` | Regional, release channel Regular, VPC-native, Dataplane V2 (enforces NetworkPolicy), private nodes, public control-plane endpoint open to `my_ip_cidr` only, the persistent disk CSI driver (`standard-rwo`), system-component logging and monitoring only (and the workloads' logs too with `workload_logging = true`, off by default: for the failure tests, [Session 3](#session-3-the-failure-tests-runbook)), `deletion_protection = false`. The default node pool is removed. |
| Node pool `kvstore` | 3 zones; 2 nodes per zone, autoscaled 2-3 per zone; e2-standard-2; 20 GiB pd-balanced boot disks; taint `dedicated=kvstore:NoSchedule`; no Spot VMs; surge upgrades, 1 new node per zone before an old one is drained. |
| Node pool `system` | 1 e2-standard-2 in one zone, untainted: GKE's own components (kube-dns, metrics-server), the operator, Prometheus and Grafana. |
| Node pool `loadgen` | **Only with `loadgen_enabled = true`** (off by default), for the k6 benchmarks ([benchmarks.md](benchmarks.md)): `loadgen_nodes` n2-standard-4 (`loadgen_machine_type`) in the system pool's zone, 1 by default and 2 for the two-pod check, no autoscaling; taint `dedicated=loadgen:NoSchedule`, which the `deploy/bench` GKE overlay tolerates, selecting the pool by name. Set it in `terraform.tfvars` for the session, not with `-var`: a later `apply` without it (a new address, item 6) would remove the pool. |
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
- **The loadgen pool off by default**: it is for benchmark sessions, and
  costs about a third as much again while it exists. N2 rather than E2
  because the database pool at its maximum takes 20 of the 24 `E2_CPUS`;
  N2 has its own quota.
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

The loadgen pool's n2-standard-4 counts against `N2_CPUS` (200), not
`E2_CPUS`, and against `CPUS_ALL_REGIONS` (32) like every node: 4 more
vCPUs, so 18 created and 30 in the last row above. Its own surge upgrade
adds 4 more, which fits unless it coincides with that row's.

**Two loadgen nodes** (`loadgen_nodes = 2`, for the two-pod check, one k6
pod per node) are 8 N2 vCPUs: `CPUS_ALL_REGIONS` is 22 of 32 as created,
and that margin holds **only while the database is not scaled**. Scaling
eats it: 24 with a seventh database node, 28 at the autoscaler's maximum,
and a database pool upgrade there surges 3 nodes more, 34. Past 32 GKE
cannot create the nodes and a surge upgrade waits. So the two-pod session
runs the benchmarks at 6 replicas and never scales the database; scaling
experiments go in a session with one loadgen node or none.

**pd-balanced counts against `SSD_TOTAL_GB`**, both boot disks and the
data volumes (`standard-rwo` is pd-balanced): 13 boot disks of 20 GiB and
9 data volumes of 10 GiB is 350 GiB of the 500 (370 with the loadgen
node's). The default node pool GKE creates and removes while the cluster
is created uses three more boot disks (and three e2-small nodes, 6 E2
vCPUs) for a few minutes.

## Cost

europe-west1, on demand, from the Cloud Billing catalog on 2026-10-07 (N2 on 2026-10-11):

| Item | Price | Created (7 nodes) | Maximum (10 nodes) |
|---|---|---|---|
| Regional cluster fee | $0.10/h | $0.100 | $0.100 |
| e2-standard-2 (2 E2 cores at $0.02399337/h, 8 GiB at $0.00321609/GiB·h) | $0.0737/h | $0.516 | $0.737 |
| Boot disks, 20 GiB pd-balanced each | $0.10/GiB·month | $0.019 | $0.027 |
| Data volumes, 10 GiB pd-balanced each (6, up to 9) | $0.10/GiB·month | $0.008 | $0.012 |
| Cloud NAT: gateway per VM, and its address | $0.0014/VM·h, $0.005/h | $0.015 | $0.019 |
| **Total** | | **≈ $0.66/h** | **≈ $0.90/h** |
| Loadgen pool, if enabled, per node (1, or 2 for the two-pod check): n2-standard-4 (4 N2 cores at $0.034773/h, 16 GiB at $0.004661/GiB·h), its boot disk and NAT | $0.2137/h | +$0.218 | +$0.218 |

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

   If the apply fails while it creates the cluster because Terraform lost
   its connection (`error while retrieving operation: ... http2: client
   connection lost`, [Session 3](#session-3-results-2026-10-17)), GKE goes
   on creating it and Terraform's state marks it tainted: a plain `apply`
   would destroy it and create it again. Instead wait for the operation
   (`gcloud container operations list --region $R`) to be `DONE`, then
   `terraform -chdir=infra/gke untaint google_container_cluster.kvstore`;
   delete `default-pool` (`gcloud container node-pools delete default-pool
   --cluster kvstore --region $R`), which the provider removes only at the
   end of a create that succeeds; then `apply` (the node pools), and a
   `plan` must say `No changes`.

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
   q 'sum by (instance) (rate(kv_storage_writes_total{engine="data"}[1m]))'
   ```

   The first is the requests each pod coordinated (the writer sends only
   Puts); the second, the writes each pod stored, as coordinator or replica
   (three per Put across the cluster). It counts writes, not fsyncs: with
   group commit one fsync covers many. On a build from before group commit,
   which has no `kv_storage_writes_total`, use
   `kv_wal_fsync_duration_seconds_count` instead: one fsync per write there.
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

The whole of [A session](#a-session), end to end, on the cluster as
created (europe-west1; GKE 1.35.8-gke.1225000), from commit `cce60c7`,
under a steady writer of about 11 writes/s. The operator brought up 6
pods, two in each zone, and scaled them 6 → 7 → 6 twice: 174s onto a new
node (the autoscaler adding it, then the disk's CSI driver), 20-41s
otherwise, once with the operator's own pod deleted mid-drain. Every
acknowledged write read back (`checked=4994 missing=0`); Put p50 / p99
6.3 / 9.9 ms over the run, and the scale cycles moved p99 by 0.3 ms.
The record, step by step, with what differed from the plan and the
Prometheus series: [results/gke-session1/](results/gke-session1/README.md).

When the project is no longer needed, delete it
(`gcloud projects delete kvstore-gke`): that removes anything Terraform's
state lost track of.

## Session 2: the benchmarks (runbook)

The k6 benchmarks of [benchmarks.md](benchmarks.md) on this cluster. Run
on 2026-10-13; the results are in
[benchmarks.md](benchmarks.md#gke-2026-10-13-session-2-the-benchmarks). It follows [A session](#a-session) for the
set-up and the teardown, with the loadgen pool added, and replaces its
steps 7 to 9 (the steady writer and the experiments) with the benchmark
series. **No scaling of the database in this session**: the two-pod step
needs the CPU quota a scale would take ([Quotas](#quotas)).

**Estimate: about 2.5 hours, about $2.40** (the cluster with one loadgen
node at $0.88/h; the second loadgen node, for its half hour, $0.11; NAT
data processing $0.05-0.10). Budget 3.5 hours and $3.50; the decision
point for writes adds about 20 minutes if taken.

| Part | Time |
|---|---|
| Before, set-up (cluster creation about 15 min, not timed in session 1) | ~30 min |
| Series A: reads and writes | ~35 min |
| Decision point for writes above 500/s, if taken | ~20 min |
| Series B: mix | ~25 min |
| The two-pod step | ~28 min |
| Recording between runs | ~15 min |
| Teardown | ~15 min |

### Before

1. The tree is clean, at the commit the images will be built from:
   `git status --short` shows nothing tracked. `run.sh` refuses `writes`,
   `reads` and `mix` on a dirty tree ([benchmarks.md](benchmarks.md#running)).
2. Your address matches `my_ip_cidr` ([Before the first session](#before-the-first-session), item 6).
3. In `infra/gke/terraform.tfvars` (not with `-var`: a later plain
   `apply` would drop the pool):

   ```hcl
   loadgen_enabled = true
   loadgen_nodes   = 1
   ```

### Set-up

4. [A session](#a-session), steps 1 to 6, with `--context $CTX` on every
   kubectl call. The plan says 16 to add; `get nodes` shows 8: 6
   `kvstore`, 1 `system`, 1 `loadgen`.
5. **The loadgen node has room for k6** (3 CPUs requested; 3920m
   allocatable on an n2-standard-4). Before anything else lands on it:

   ```sh
   kubectl --context $CTX describe node -l cloud.google.com/gke-nodepool=loadgen \
     | sed -n '/Allocated resources/,/memory/p'
   ```

   CPU requests must be 920m or less. If more, set
   `loadgen_machine_type = "n2-standard-8"` in `terraform.tfvars` and
   `apply` (the pool is replaced; about $0.43/h instead of $0.21/h).
   `run.sh` makes the same check before every run and refuses if k6 does
   not fit.
6. Step 5 of [A session](#a-session) only up to the namespace: **the
   KVCluster is the bench one**, from `run.sh`, with the node image by
   digest:

   ```sh
   kubectl create namespace kvstore --dry-run=client -o yaml | kubectl --context $CTX apply -f -
   export BENCH_CONTEXT=$CTX
   KV_IMAGE=$NODE_IMG deploy/bench/run.sh reset      # type the context name to confirm
   ```

   Then the placement and membership checks of step 5 (6 distinct nodes,
   each zone twice; every pod `epoch=0 members=6`), and Prometheus's 6 of
   6 targets (step 6). **Do not start `deploy/loadgen/writer.yaml`**: its
   keys are not in the ledger, and its load would be in every window.

### Series A: reads and writes

On the fresh KVCluster. Ramps hold each rate 60s (`STEP_SECONDS=60`). After
each run: errors 0, `dropped_iterations` 0 below the knee, k6's CPU under
1500m (half its 3-CPU request), the spread at most 1.5
([Is the database the bottleneck?](benchmarks.md#is-the-database-the-bottleneck)).

| Run | Command | Keys | Time |
|---|---|---|---|
| preload | `deploy/bench/run.sh preload` (40 VUs; must be clean) | 20,000 | ~4 min |
| reads, constant | `RATE=1000 deploy/bench/run.sh reads` | 0 | ~6.5 min |
| reads, ramp | `PROFILE=ramp RAMP_FROM=1000 RAMP_TO=5000 RAMP_STEP=500 STEP_SECONDS=60 deploy/bench/run.sh reads` (9m40s) | 0 | ~11 min |
| writes, constant | `RATE=40 deploy/bench/run.sh writes` | 12,000 | ~6.5 min |
| writes, ramp | `PROFILE=ramp RAMP_FROM=100 RAMP_TO=500 RAMP_STEP=100 STEP_SECONDS=60 deploy/bench/run.sh writes` (5m20s) | 96,000 | ~7 min |
| | | **128,000 of 130,000** | |

**Reading each knee.** From the run's `steps.csv`: the first step where p99
turns up sharply, or errors start, or `dropped` is above 0, or the achieved
rate falls short of the target. Then confirm it in Prometheus over the
same minutes (`prometheus/put_latency_p99.csv` or `get_latency_p99.csv`,
and `wal_fsync_latency_p99_data.csv` for writes): the database's own p99
must turn up there too, or the knee is k6's or the network's, not the
store's.

### Decision point: writes above 500/s

If the write ramp's last step (500/s) shows no knee, the cap stops the
series from going higher on this KVCluster (the ledger is at 128,000).
The choice, made on the day:

- **Stop here**, and record the write knee as above 500/s on GKE.
- **Reset and ramp higher**: `reset`, `preload`, then
  `PROFILE=ramp RAMP_FROM=500 RAMP_TO=800 RAMP_STEP=100 STEP_SECONDS=30`
  (`writes`, 87,750 keys, 2m15s; 20 minutes in all with the reset).
  Steps of 30s fit the cap; `steps.csv` still places the knee step by
  step, while the Prometheus series (1m rates) confirm it less sharply.
  Any higher needs the memory limit and the cap raised together
  ([The key cap](benchmarks.md#the-key-cap)), which is a change to make
  before a session, not during one.

### Series B: mix

`reset`, `preload`, then:

| Run | Command | Keys | Time |
|---|---|---|---|
| mix, constant | `RATE=80 deploy/bench/run.sh mix` (40 writes + 40 reads) | 12,000 | ~6.5 min |
| mix, ramp | `PROFILE=ramp RAMP_FROM=200 RAMP_TO=1000 RAMP_STEP=200 STEP_SECONDS=60 deploy/bench/run.sh mix` (5m20s) | 96,000 | ~7 min |

`steps.csv` has a row per op in each step: the knee is where either op's
turns up.

### The two-pod step

On the Series B KVCluster (reads write nothing, so its 128,000 keys do not
matter).

7. Set `loadgen_nodes = 2` in `terraform.tfvars` and
   `terraform -chdir=infra/gke apply`: the plan says `1 to change` (the
   pool, resized in place), and nothing for the cluster. `get nodes` shows
   9. Check the new node as in step 5.

   In session 2 the plan also wanted an in-place change to the cluster
   itself: its `node_config` describes only the default pool, which GKE
   removes after creating the cluster, and from then on GKE reports
   another pool's node configuration as the cluster's (here the loadgen
   pool's n2-standard-4). Nothing had changed; applying it would have
   been a needless update to the cluster. The pool was resized with
   gcloud instead (for example `gcloud container clusters resize kvstore
   --node-pool loadgen --num-nodes 2 --region europe-west1`), which left
   Terraform's state at one node (harmless for the `destroy`). The cluster now has
   `lifecycle { ignore_changes = [node_config] }` (`infra/gke/cluster.tf`),
   so the plan shows only the pool. That was checked with `terraform
   validate` only: the cluster was gone by then, so the next session's
   first plan with the pool on is the real test.
8. The read ramp twice, back to back, on the same data:

   ```sh
   PROFILE=ramp RAMP_FROM=1000 RAMP_TO=5000 RAMP_STEP=500 STEP_SECONDS=60 deploy/bench/run.sh reads
   K6_PODS=2 PROFILE=ramp RAMP_FROM=1000 RAMP_TO=5000 RAMP_STEP=500 STEP_SECONDS=60 deploy/bench/run.sh reads
   ```

   With two pods, `steps.csv` has a row per pod: add their achieved rates
   per step; compare percentiles per pod. The read knee counts only where
   the two runs agree within 10% (rate and Prometheus's Get p99 at the
   knee). If k6 passes 1500m even with two pods before the database's p99
   turns up, the result is a lower bound on the read knee, and is recorded
   as one.
9. Set `loadgen_nodes = 1` again only if the session goes on; otherwise
   the teardown removes the pool.

### Teardown

10. [A session](#a-session), step 10: check your address, delete the
    KVCluster and its volumes, `terraform destroy`, and every list empty.
    Then set `loadgen_enabled = false` in `terraform.tfvars`, so the next
    session starts without the pool unless it asks for it.

## Session 3: the failure tests (runbook)

The four failure tests of [chaos.md](chaos.md) on this cluster, under load,
each with its prediction written down before it runs (part A); then the
write benchmarks again, with group commit off and on (part B). It follows [A session](#a-session) for the set-up and the
teardown, with the loadgen pool (the recorders run there: neither a
database VM's loss nor the system node's may take them down) and the
workloads' logs on, and replaces its steps 7 to 9.

**What the rehearsal on kind changes here**
([chaos.md](chaos.md#findings)):

- Partitions are **egress-only NetworkPolicies on both sides**, selecting a
  label of their own (`kvchaos/partition`): Cilium, which Dataplane V2 is
  built on, leaves `statefulset.kubernetes.io/pod-name` out of its
  identities, and an ingress policy blocked the kubelet's probes on kind.
  A new label takes a while to reach the pods' identities: `partition`
  waits for it (60s where there are no CiliumEndpoints to watch) before
  the cut and the inject.
- **After a partition healed under load, some peer pairs stayed cut for up
  to minutes** (TCP retransmission backoff on peer connections without
  keepalive). Fixed by the peer keepalive: on kind every pair saw the other
  again within 7s of the heal
  ([chaos.md](chaos.md#peer-keepalive-the-partition-again)). Build the
  images from a commit that has it, and record each pair's heal from the
  logs.
- **Hint delivery is slow** (about 5/s per sender on kind): every read-back
  waits for `wait-hints`.
- **Requests hung on a vanished pod take the recorder's workers**:
  `chaos.sh` runs 256 in flight on GKE (64 on kind dropped requests).
- Each run writes keys of its own; `wait-ready` and `wait-node` count a pod
  or node seen down, not only a newer transition time.

**Questions only GKE answers** (record each on the day):

1. Does deleting a VM shut its guest down (requests to its pods fail at
   once, as in t1 on kind), or does it vanish (they time out until the
   recorder's keepalive closes the connection, as in t1s)?
2. How long from the VM's deletion to its pods Ready again: the Node
   deleted or only not Ready, the VM recreated, the disk attached, the pod
   started; does the cluster autoscaler add a node meanwhile; and is the
   KVCluster `Degraded` (a pod unready for more than 2 minutes)?
3. Does Dataplane V2 enforce the egress-only partition, and how soon after
   the policies are applied?
4. While the system node is gone: do `kubectl exec` and the API server's
   proxy to pods still work (they go through GKE's konnectivity agents)?
   kube-dns is down: does anything fail for want of it?

**Estimate: about 3.75 hours, about $3.35** (the cluster with one loadgen
node at $0.88/h; a node the autoscaler may add in tests 1 and 2, about
$0.07/h each for a quarter of an hour; NAT data processing $0.05-0.10).
Without part B, about 2.75 hours and $2.40. Budget 4.5 hours and $4.50.
The workloads' logs are a few tens of MB, inside Cloud Logging's free
allotment per project and month (check the current allotment and price
before the session; this page has not recorded them).

| Part | Time |
|---|---|
| Before, set-up (cluster creation about 15 min) | ~40 min |
| Topology, predictions, preload, baseline | ~20 min |
| Test 1, VM loss | ~15 min |
| Test 2, zone loss (with the kv-client run) | ~20 min |
| Test 3, partition | ~15 min |
| Test 4, system-node loss | ~15 min |
| Anti-entropy interval, logs, convergence | ~20 min |
| Part B, the write benchmarks, group commit off and on | ~60 min |
| Teardown | ~15 min |

### Before

1. The tree is clean at the commit the images will be built from, and
   `make test` passes. The node image carries `/kvchaos` (`Dockerfile`).
2. **Group commit**: the images are from `e4627d6` or later, which have
   group commit and `spec.storage.groupCommit` (and the CRD with it, from
   the operator install). Part B needs both; the failure tests run with
   group commit on, the default. Record the commit.
3. Your address matches `my_ip_cidr` ([Before the first session](#before-the-first-session), item 6).
4. In `infra/gke/terraform.tfvars` (not with `-var`: a later plain `apply`
   would drop them):

   ```hcl
   loadgen_enabled  = true
   loadgen_nodes    = 1
   workload_logging = true
   ```

### Set-up

5. [A session](#a-session), steps 1 to 4 (create, kubectl, the images by
   digest, the operator) and step 6 (observability), with `--context $CTX`
   on every kubectl call. The plan says 16 to add; `get nodes` shows 8:
   6 `kvstore`, 1 `system`, 1 `loadgen`. Note the time:
   `SESSION_START=$(date -u +%Y-%m-%dT%H:%M:%SZ)`.
6. **The KVCluster is the bench one** (512Mi per pod, sized for about
   100,000 keys a pod; the GKE sample keeps the default 256Mi), as in
   [Session 2](#session-2-the-benchmarks-runbook), step 6:

   ```sh
   kubectl create namespace kvstore --dry-run=client -o yaml | kubectl --context $CTX apply -f -
   export BENCH_CONTEXT=$CTX
   KV_IMAGE=$NODE_IMG deploy/bench/run.sh reset      # type the context name to confirm
   ```

   Then the placement and membership checks of step 5 (6 distinct nodes,
   each zone twice; every pod `epoch=0 members=6 alive=5 reachable=5`).
   **Do not start `deploy/loadgen/writer.yaml`.** At `chaos.sh`'s GKE rates
   (40 writes/s, 100 reads/s) the session's runs store about 110,000 keys,
   within that sizing.
7. **The topology and every fault's prediction:**

   ```sh
   export CHAOS_CONTEXT=$CTX KV_IMAGE=$NODE_IMG CHAOS_OUT=docs/results/chaos/$(date -u +%Y%m%d)-gke
   c=deploy/chaos/chaos.sh
   $c topology        # topology.json, and predictions.txt over the 5,000 preload keys
   ```

   **Stop point**: `topology` refuses a cluster that is not settled (a pod
   not running, a node without a zone, epochs or members that differ, a
   peer not alive and reachable). Fix that before anything else.
8. **Choose the targets and write the predictions down, before any
   fault** (in a "Session 3 (results)" section of this page, dated), from
   `predictions.txt`:

   | Test | Target | Prediction |
   |---|---|---|
   | 1. VM loss | The VM of the pod that holds the most keys (most hints) | No key fails (N=3, W=R=2: one replica down never loses a quorum); requests to that pod fail or time out (target-down); `Degraded` if the pod is unready for more than 2 minutes; epoch unchanged |
   | 2. Zone loss | The zone whose `down:` row fails the most keys | That row's share of keys fails for Puts and Gets alike; `Degraded`; epoch unchanged |
   | 3. Partition | Another zone's two pods | Its `partition:` rows: the share failing from the group, and from the rest; every key works on exactly one side; no pod restarts; the operator stays `Ready` |
   | 4. System-node loss | The system node | Zero errors; the operator back `Ready`, epoch unchanged |

   For all four: no acknowledged write lost, no stale read.
9. **Preload, read it back, and watch the operator:**

   ```sh
   $c preload            # must be clean: 5,000 of 5,000 acknowledged
   $c readback pre
   $c watch-start        # a background loop: watch-stop at the teardown
   ```

10. **Baseline, no fault** (5 minutes):

    ```sh
    $c run t0; sleep 300; $c stop t0
    $c readback t0; $c check
    ```

    No `wait-hints` here: with no fault there are no hints, and its
    `hints-delivered` mark would be a mark on a test never injected, which
    `check` refuses (Session 3). **Stop point**: it must say `PASS` with no
    failure at all. If not, something is wrong before any fault: stop and
    find it.

### Part A: the failure tests

Every test is the same shape: hint counters before, a run, 2 minutes
steady, the fault (which marks every step of its own recovery), 2 minutes
after the heal, then the run stopped, the hints delivered, the counters
after, the read-back and the check. `vm-loss`, `system-loss` and `partition`
return when the fault has healed.

**Stop point after each test**: a check that fails on a lost or stale value
ends the tests. Keep the cluster up long enough to collect the evidence
(the records are fetched by `stop`; `$c peers`; the logs, below), then the
teardown. A request against its prediction that is not a loss (an
unexpected failure or success) is a finding: record it and go on.

11. **Test 1, VM loss.** `kv-N` is the pod chosen in step 8:

    ```sh
    $c hints-snapshot t1 before
    $c run t1; sleep 120
    $c vm-loss t1 kv-N     # node gone or not Ready, VM recreated, node Ready, volume attached, serving-kv-N, serving, Ready, heal
    sleep 120
    $c stop t1; $c wait-hints t1; $c hints-snapshot t1 after; $c readback t1; $c check
    kubectl --context $CTX get events -A --field-selector reason=TriggeredScaleUp
    ```

    Record the check's timeline line (question 2), whether the requests to
    kv-N in the fault were `unknown` or `failed` (question 1), and the
    operator's `Degraded` span in `operator.log`.
12. **Test 2, zone loss, with the kv-client run.** `kv-A kv-B` are the
    zone's two pods. A second recorder goes through the Service, as a naive
    client would (on kind its connections to the frozen pods timed out for
    the whole fault):

    ```sh
    $c hints-snapshot t2 before
    $c run t2
    $c start t2svc -test t2 -service kv-client.kvstore.svc.cluster.local:7000 -service-conns 12 \
      -write-prefix chaos-t2svc-$(date -u +%Y%m%d%H%M%S) -write-rate 10 -read-rate 20 \
      -read-prefix chaos-pre -read-keys 5000
    sleep 120
    $c vm-loss t2 kv-A kv-B
    sleep 120
    $c stop t2; $c stop t2svc; $c wait-hints t2; $c hints-snapshot t2 after
    $c readback t2; $c readback t2svc; $c check
    ```

    The autoscaler may add a node in the zone while its pods are pending
    (at most 3 per zone: 20 of the 24 `E2_CPUS` with the system node). The
    two pods come back one at a time: `vm-loss` marks each `serving-POD`,
    and `check` narrows the fault as each returns.
13. **Test 3, partition.** `kv-C kv-D` are the other zone's pods:

    ```sh
    $c hints-snapshot t3 before
    $c run t3; sleep 60
    $c partition t3 kv-C kv-D   # labels the sides, waits for their identities, applies the policies, marks inject
    $c peers                    # the rest's pods alive=3, the group's alive=1, within ~5s
    sleep 120
    $c unpartition t3; sleep 120
    $c stop t3; $c unlabel; $c wait-hints t3; $c hints-snapshot t3 after; $c readback t3; $c check
    kubectl --context $CTX -n kvstore get pods -l app.kubernetes.io/name=kvstore   # restarts unchanged
    ```

    **Stop point**: if `peers` still shows everyone `alive=5` a minute after
    the inject, the policies are not enforced (question 3): `unpartition`,
    `unlabel`, record it, and skip to test 4. After the heal, expect every
    pair to see the other again within about 15s (the peer keepalive's 13s,
    and the reconnect), and the hints within the predicted range; each
    pair's times are in the logs (step 16).
14. **Test 4, system-node loss.** Prometheus and Grafana keep their data on
    `emptyDir` on that node: their history goes with it. The test's evidence
    is in the records, the hint counters, `operator.log` and the workloads'
    logs; export Prometheus's series of the session so far first, with
    `deploy/bench/export.sh` given the window in a `run.env` of its own:

    ```sh
    E=$(mktemp -d)
    printf 'context=%s\nwindow_start=%s\nwindow_end=%s\n' $CTX $SESSION_START $(date -u +%Y-%m-%dT%H:%M:%SZ) >$E/run.env
    deploy/bench/export.sh $E && mv $E/prometheus $CHAOS_OUT/prometheus && cp $E/run.env $CHAOS_OUT/prometheus/window-source.env
    ```

    Then:

    ```sh
    $c hints-snapshot t4 before
    $c run t4; sleep 120
    $c system-loss t4      # node gone or not Ready, VM recreated, node Ready, operator-back, dns-back, heal
    sleep 120
    $c stop t4; $c wait-hints t4; $c hints-snapshot t4 after; $c readback t4; $c check
    ```

    `system-loss` uses only `kubectl get` and `gcloud`: with the system node
    gone, `kubectl exec` and the API server's proxy to pods fail (`No agent
    available`: every konnectivity agent runs there; Session 3), and so do
    `peers`, `hints-snapshot` and `wait-hints` until it is back. Afterwards:
    Prometheus's 6 of 6 targets up again (step 6).
15. **Convergence**, a full anti-entropy interval (15 minutes) after test
    4's last write; collect the logs (step 16) meanwhile:

    ```sh
    $c converge; $c check-final     # every acknowledged write on all three owners
    ```

16. **The workloads' logs** of the session (the kv pods, the recorders, the
    operator), from Cloud Logging, kept with the results:

    ```sh
    mkdir -p $CHAOS_OUT/logs
    for ns in kvstore kvstore-operator-system; do
      gcloud logging read "resource.type=\"k8s_container\" AND resource.labels.cluster_name=\"kvstore\" AND resource.labels.namespace_name=\"$ns\" AND timestamp>=\"$SESSION_START\"" \
        --project kvstore-gke --order=asc --format='value(timestamp,resource.labels.pod_name,textPayload)' \
        > $CHAOS_OUT/logs/$ns.log
    done
    grep -E 'is dead|alive again' $CHAOS_OUT/logs/kvstore.log    # every pair's cut and heal
    ```

### Part B: the write benchmarks with group commit

Session 2 put the write knee at 300-400/s for p99 and 500/s for p50, with
the kv pods under 0.4 cores and fsync p99 near 5 ms: writes queueing for
the engine's write lock, one fsync each
([benchmarks.md](benchmarks.md#open-questions)). Group commit is the fix,
and part B measures it as an A/B **in this session, on this cluster**:
the same write series with `spec.storage.groupCommit` off, then on. The
off side, not session 2, is the baseline: it has the same GKE version,
VMs and disks as the on side, and checks that the session reproduces
session 2. On kind the same A/B moved the write ceiling from about
104/s to past 300/s, the end of its ramp
([benchmarks.md](benchmarks.md#kind-2026-10-16-group-commit-off-and-on-ab)).

**After the convergence check only** (step 15): `run.sh reset` replaces
the failure tests' KVCluster and its data. `groupCommit` cannot change on
a KVCluster, so each side is its own `reset`. First delete the recorder
pods, which hold their output after their runs and request 250m each on
the loadgen node, where k6 needs 3,000m (`run.sh` refuses otherwise):
`kubectl --context $CTX -n kvstore delete pod -l app.kubernetes.io/name=kvchaos`.
Collect the logs again (step 16) at the end of the session, before the
teardown, for part B's.

17. **Write the prediction down first** (in the session's results section,
    before the first run):

    | Side | Prediction |
    |---|---|
    | Off | Session 2's series A again: Put p99 turns up at 300-400/s, p50 at 500/s; 1.00 writes per fsync at every step; commit wait p99 rising with the rate while fsync p99 stays near 5 ms and the kv pods under 0.4 cores |
    | On, to 500/s | No knee: Put p99 within about 2x its 100/s value at every step; writes per fsync above 1 and rising with the rate; commit wait p99 near one fsync (5-10 ms) rather than rising; constant 40/s no slower than off (a lone write waits only for its own fsync) |
    | On, past 500/s | A knee between 500 and 900/s, set by something other than the write lock: the busiest kv pod's CPU (1.93 cores allocatable), the replication fan-out, or fsync time itself growing with batch size. Which one, from the same window |

    What shows that group commit moved the knee, not something else: the
    two sides differ only in the flag (`group_commit` in `run.env`), and
    on the on side writes per fsync (`writes_per_fsync`,
    `wal_batch_writes_mean_data.csv`) rises above 1 exactly as the commit
    wait (`wal_commit_wait_p99_data.csv`) stops rising with the rate.

18. **Off**, keys as session 2's series A without its reads (128,000 of
    130,000):

    ```sh
    GROUP_COMMIT=false KV_IMAGE=$NODE_IMG deploy/bench/run.sh reset   # type the context name to confirm
    deploy/bench/run.sh preload                                         # must be clean
    RATE=40 deploy/bench/run.sh writes                                  # ~6.5 min
    PROFILE=ramp RAMP_FROM=100 RAMP_TO=500 RAMP_STEP=100 STEP_SECONDS=60 deploy/bench/run.sh writes   # 5m20s
    ```

    `BENCH_CONTEXT=$CTX` is still exported from step 6. Check `run.env`:
    `group_commit=false`.

19. **On**, the same four commands with `GROUP_COMMIT=true` (or left out:
    it is the default); `run.env` says `group_commit=true`.
20. **On, past 500/s**, if the on ramp shows no knee at 500/s. Writes read
    no preloaded keys, so there is no preload: the cap (130,000) leaves
    room for 119,000 keys, 500 to 900/s in steps of 30s:

    ```sh
    KV_IMAGE=$NODE_IMG deploy/bench/run.sh reset      # type the context name to confirm
    PROFILE=ramp RAMP_FROM=500 RAMP_TO=900 RAMP_STEP=100 STEP_SECONDS=30 deploy/bench/run.sh writes   # 2m50s
    ```

    This KVCluster starts empty where the 500/s step of step 19 had about
    98,000 keys behind it (20,000 preloaded), so compare its 500/s step
    with step 19's before reading the steps above it. k6's CPU must stay
    under 1500m (session 2's write ramp peaked at 304m at 500/s). Above
    900/s the cap and the memory limit have to be raised together
    ([The key cap](benchmarks.md#the-key-cap)), before a session, not
    during one.
21. **Read each run's knee** as in [Series A](#series-a-reads-and-writes),
    step by step from `steps.csv`, and confirmed in Prometheus over the
    same minutes: Put p99, fsync p99, writes per fsync and commit wait
    p99, and the kv pods' CPU. A quantile at the top bucket is a lower
    bound (`>=` in `window.env`; `5` for Put p99 and `10` for commit wait
    in the CSVs, [What each run saves](benchmarks.md#what-each-run-saves)).
    Then compare, run by run:

    | Compare | With | For |
    |---|---|---|
    | Off, constant 40/s | Session 2 `20261013-094331` (Put p50 6.68 / p99 9.96 ms, fsync p99 4.91 ms) | That the session reproduces session 2: within about 20%, or record the difference and compare on with off only |
    | Off, ramp | Session 2 `20261013-094941`, its step table (p99 104 ms at 300/s, p50 14.4 ms at 500/s) | The same; the knee step should match |
    | On, constant 40/s | Off, constant 40/s | No cost at a low rate: p50 within the noise, writes per fsync near 1 |
    | On, ramp | Off, ramp, step by step | The A/B: p99, p50, writes per fsync and commit wait at each rate |
    | On, past 500/s | Its own steps, and the busiest pod's CPU | Where the next knee is, and what sets it |

    Record them in [benchmarks.md](benchmarks.md#runs) as a "GKE, session 3,
    part B" section, in the shape of session 2's.

### Teardown

22. `$c watch-stop`; check that no `kubectl port-forward` of yours is left.
23. [A session](#a-session), step 10: check your address, delete the
    KVCluster and its volumes, `terraform destroy`, and every list empty.
    Then set `loadgen_enabled = false` and `workload_logging = false` in
    `terraform.tfvars`.
24. Record the session (a "Session 3 (results)" section here, the
    predictions of step 8 and step 17 first) and commit `$CHAOS_OUT` and
    the part B runs' `docs/results/bench/` directories: the per-request
    records and read-backs stay out (`docs/results/chaos/.gitignore`).

## Session 3 (results), 2026-10-17

Written during the session, 15:47-19:47Z: about 4 hours and $3.40 to the
last run (budget 4.5 hours, $4.50), plus the teardown. Part A, the four
failure tests and the convergence: **no acknowledged write lost, no stale
read**; part B, group commit off and on: the write ceiling from about
400/s to above 1,500/s. Torn down afterwards (step 23): every list empty,
`loadgen_enabled` and `workload_logging` back to `false`.

**Set-up.** Commit `e406077` (group commit included); europe-west1, GKE
1.36.4-gke.1391002. Images, tag `e406077-20261017154739`:

- `kvnode@sha256:9d93ce554652a7cb3dc3b2897d494901b3e569b5d9837abba3ba22ff612f21aa`
- `kvstore-operator@sha256:b00919a7b936e62f3d73ad0af5dcf0eb93a15efd87c3ee41d00d4e9d367b3d88`

`SESSION_START` 2026-10-17T16:07:36Z.

**The first apply failed, and was recovered without recreating the
cluster.** `terraform apply` (16 to add) started at 15:47:39Z; 1m10s into
the cluster's creation, Terraform lost its connection while polling the
operation (`error while retrieving operation: ... http2: client connection
lost`) and exited, leaving the cluster in its state **tainted**, with the
node pools not yet created. GKE's operation went on regardless and finished
`DONE`, without error, at about 15:54Z. A plain `apply` would then have
destroyed and recreated the cluster. Instead:

1. Waited for the operation (`gcloud container operations describe`) to be
   `DONE` and the cluster `RUNNING`.
2. `terraform -chdir=infra/gke untaint google_container_cluster.kvstore`;
   the new plan was `3 to add, 0 to change, 0 to destroy` (the three pools),
   nothing for the cluster.
3. Deleted `default-pool` by hand (16:00-16:04Z), the step the provider
   takes at the end of a successful create (`remove_default_node_pool`)
   and never reached: Terraform does not track that pool, and its three
   e2-small nodes (6 E2 vCPUs) would have taken the autoscaler's maximum
   past the `E2_CPUS` quota (26 of 24):
   `gcloud container node-pools delete default-pool --cluster kvstore --region europe-west1 --project kvstore-gke`.
4. Applied the saved plan: 3 added (16:04:40-16:06:57Z). A plan afterwards
   reported `No changes. Your infrastructure matches the configuration.`
   (`-detailed-exitcode` 0). 8 nodes Ready: 6 `kvstore` (two each in b, c,
   d), `system` and `loadgen` in b.

**The cluster.** The bench KVCluster (`run.sh reset`, `groupCommit: true`),
Ready 36s after its creation. 6 pods on 6 distinct nodes, each zone twice:
europe-west1-b = {kv-0, kv-5}, c = {kv-1, kv-4}, d = {kv-2, kv-3}; every
pod `epoch=0 members=6 alive=5 reachable=5`; Prometheus 6 of 6 targets up.
`topology` accepted it (N=3, W=R=2). Share of the 5,000 preload keys each
pod holds: kv-0 66.4%, kv-1 37.7%, kv-2 50.4%, kv-3 47.2%, kv-4 51.6%,
kv-5 46.8%.

**Targets and predictions** (from `predictions.txt`, written before any
fault):

| Test | Target | Prediction |
|---|---|---|
| 1. VM loss | kv-0 (holds the most keys, 66.4%), its VM `gke-kvstore-kvstore-a68c1bad-tx51` in b | No key fails (`down:kv-0`: 0 Puts, 0 Gets); requests to kv-0 fail or time out (target-down); `Degraded` if kv-0 is unready for more than 2 minutes; epoch unchanged |
| 2. Zone loss | Zone c, kv-1 and kv-4 (its `down:` row fails the most keys) | `down:kv-1,kv-4`: 1,419 of the 5,000 preload keys (28.4%) fail, for Puts and Gets alike; `Degraded`; epoch unchanged. Through kv-client: failures beyond the key prediction (kind's finding), recorded, not a loss |
| 3. Partition | Zone d, kv-2 and kv-3 (a third zone: each of tests 1-3 hits a different one) | From the group 4,132 keys (82.6%) fail, from the rest 868 (17.4%), on both sides 0: every key works on exactly one side; no pod restarts; the operator stays `Ready`; every pair alive again within about 15s of the heal |
| 4. System-node loss | `gke-kvstore-system-f675d0ba-g9d7` (operator, Prometheus, Grafana, kube-dns) | Zero errors; the operator back `Ready`, epoch unchanged |

For all four: no acknowledged write lost, no stale read.

**Preload and baseline.** Preload 5,000 of 5,000 acknowledged, read back
`ok` (16:12Z). Baseline t0, 16:12:57-16:18:20Z: 12,121 Puts and 30,301
Gets, all ok and as predicted; 17,121 acknowledged writes (with the
preload) read back, none lost, every copy on all owners: **PASS**.

**A runbook error, step 10.** `wait-hints t0` appends a `hints-delivered`
mark for t0, and `kvchaos check` refuses a test that has marks but was
never injected (`test t0: marked or healed but never injected`; on purpose:
the "mark only" case of `TestCheckRefusesABadTimeline`). The PASS above is
`kvchaos check` over the events without that line. The line was then moved
out of `faults.jsonl`, kept in `t0/marks.jsonl`, so that every later
`check` runs as `chaos.sh check`. **Fixed since**: step 10 no longer runs
`wait-hints` for the baseline.

**`vm-loss` could not run gcloud from Git Bash (first attempt of test 1,
aborted before the inject).** `chaos.sh` exports `MSYS_NO_PATHCONV=1` (for
kubectl's `/scripts/...` paths), and under it the Cloud SDK's `gcloud`
launcher cannot start: the POSIX path it hands to Python is not converted
(`python3.12.exe: can't open file 'D:/c/Users/pc/AppData/Local/Google/Cloud SDK/google-cloud-sdk/lib/gcloud.py'`).
`mig_of` failed before `mark inject`, so nothing was injected
(`faults.jsonl` stayed empty). The kind rehearsal never called gcloud. The
recorder of that attempt was stopped and read back: 6,041 Puts and 15,101
Gets, all ok, all 6,041 keys read back; kept as `t1-aborted/` (its records
say test `t1`, which has no fault: all normal, as predicted), and test 1
was run as **`t1b`**. For the rest of the session, a `gcloud` wrapper
first on `PATH` runs the real one with `MSYS_NO_PATHCONV` unset (`exec env
-u MSYS_NO_PATHCONV .../gcloud "$@"`), proven first with `mig_of`'s and
`wait_vm`'s exact commands under `set -euo pipefail` and
`MSYS_NO_PATHCONV=1`. **Fixed since** in `chaos.sh` itself: its `gcloud`
runs the real one with `MSYS_NO_PATHCONV` unset.

### Test 1, VM loss of kv-0 (t1b): PASS

`recreate-instances` of `gke-kvstore-kvstore-a68c1bad-tx51` (b), injected
16:30:44.9Z, healed 16:34:21.4Z (3m37s). Requests 16:28:38-16:36:21Z:
18,521 Puts and 46,301 Gets, **every one ok**; the check: 62,068 as
predicted, 2,753 in transition (all ok), 1 target-down that succeeded.
Read-back over the session so far: 41,683 acknowledged writes, **0 lost, 0
stale**, every copy on all its owners. Hints: 5,790 created and delivered
(kv-1 363, kv-2 1,793, kv-3 2,111, kv-4 781, kv-5 742), predicted 5,779 to
6,389; all delivered 2m53s after the heal. Epoch 0 throughout.

Timeline (`check`): +56.7s node not Ready, +103.2s VM recreated, +142.0s
node Ready, +192.5s volume attached, +215.9s serving, +216.4s kv-0 Ready.
From kv-0's events: the node was never deleted (same name, not Ready); at
16:32:59Z (+135s) the old pod was deleted (`TaintManagerEviction`) and the
StatefulSet's new kv-0 was unschedulable (its disk is in b, kv-5 holds the
other b node), which **triggered a scale-up of b, 2 to 3 nodes**
(`gke-kvstore-kvstore-a68c1bad-v67k`); kv-0 was scheduled back onto the
recreated tx51 at 16:33:37Z (the new node was not used); one
`FailedAttachVolume` (the CSINode not yet registered) then attached at
16:33:56Z; the image pull on the fresh VM took 22s.

- **Question 1: the VM shuts down, it does not vanish.** The recorder sent
  one request to kv-0 after the inject (+5.5s, ok) and none after: kv-0's
  connection closed at once, as in t1 on kind. No `failed`, no `unknown`.
- **Question 2:** 3m37s from the deletion to Ready; the Node object stays
  (not Ready); the autoscaler added a node in b that went unused.
- **Finding: never `Degraded`, although kv-0 was out for 3m37s.**
  `operator.log`: `Ready`, `ready=5` from 16:31:52Z to 16:34:22Z,
  `WaitingForPods`. The planner measures `Grace` (2 min) from the pod's
  creation or its Ready condition's last transition
  (`operator/internal/controller/observe.go`), and the pod was recreated
  at 16:32:59Z: by the new pod's clock it was unready 1m22s. A pod
  recreated during an outage restarts the clock. Not a loss; to be decided
  after the session whether Grace should follow the member (the ordinal),
  not the pod object.

### Test 2, zone loss of c (kv-1, kv-4): no loss; check FAIL on 451 early successes

`recreate-instances` of both c VMs, injected 16:54:07.4Z, healed
16:58:02.4Z (3m55s); both recorders under test name t2. Straight to the
pods: 19,760 Puts (2,320 failed), 49,400 Gets (6,094 failed). Through
kv-client (t2svc): 5,001 Puts (438 failed), 10,001 Gets (1,236 failed),
every failure `Unavailable`. Records verified complete (69,160 and
15,002). Read-back over the session so far: 63,686 acknowledged writes,
**0 lost, 0 stale**, every copy on all its owners; the 2,758 unacknowledged
Puts all absent. Epoch 0 throughout.

`check`: 80,056 as predicted, 226 target-down that succeeded, 3,429 in
transition (2 Gets failed), and **451 `unexpected-success`**: requests in
the fault phase, predicted to fail, that succeeded (281 Gets and 170 Puts;
84 of them through kv-client), all from 16:57:48Z on.
**Cause: the `serving` mark is one mark for all of a fault's pods.** kv-4
started at 16:57:47Z and was Ready at 16:57:48Z (`operator.log`: `ready=5`);
kv-1 started only at 16:57:59Z. `wait_ready` marks `serving` once every pod
answers, so the fault phase ran on to 16:58:01.3Z (less the 2s tolerance)
while keys with kv-4 among their two down replicas were already back. Not
a loss and not the database's: the tool's timeline. A per-pod serving mark
(or the fault narrowing as each pod returns) would place them. **Fixed
since**: `wait-ready` marks each pod's `serving-POD`, and `check` narrows
the fault as each pod returns (`MarkServingPod`, `internal/chaos`).

**The explanation, proved request by request** (offline, after the
session, from the records). `check -examples 100000` over t2's and t2svc's
records lists all 451; `predict -fault down:kv-1,kv-4 -keys-file ...
-per-key` gives each key's replicas; joined with the records for each
request's exact start, they are in `t2-unexpected-success.txt`.

- **Every one of the 451 started after kv-4 was Ready**: the earliest at
  16:57:48.194Z, after both 16:57:48Z (`operator.log`'s `ready=5` was
  seen at 16:57:48.029Z) and kv-4's own `node kv-4 ready` line
  (16:57:47.830Z). None before either.
- **Every one's key has kv-4 among its replicas**, and kv-1 too (a key
  fails for this fault only with both): with kv-4 back and kv-1 still
  down, 2 of its 3 replicas were up, which R=W=2 needs.
- **None fits neither, and none is unmatched** (451 of 451 found in the
  records). The latest started at 16:57:59.284Z, just inside the fault
  phase's end (the `serving` mark at 16:58:01.291Z less the 2s
  tolerance, 16:57:59.291Z).
- **The rest of that window agrees.** From kv-4's ready line to that end,
  1,947 requests started: 1,368 predicted to work, all ok; 579 predicted
  to fail, of which 451 ok (these), 78 ok sent to kv-4 itself (bucketed
  `target-down-ok`), and 50 `Unavailable`. The 50 are each coordinator's
  last requests before it saw kv-4 again: kv-2, kv-3 and kv-5 logged `peer
  "kv-4" is alive again` at 16:57:48.155Z and their last failures started
  by 16:57:48.579Z (in flight); kv-0 logged it only at 16:57:53.136Z, and
  its 24 failures run to 16:57:53.104Z. Why kv-0 saw kv-4 5s after the
  others is open.

Timeline (`check`): +50.9s and +61.2s the nodes not Ready, +99.8s and
+102.1s the VMs recreated, +161.2s and +162.0s the nodes Ready, +228.2s and
+228.7s the volumes attached (as observed by the sequential waits: kv-4's
attach event was at 16:57:42Z, +215s), +233.9s serving, +234.4s and
+235.0s the pods Ready. As in test 1 the old pods were deleted
(`TaintManagerEviction`, kv-4 at 16:56:27Z, kv-1 at 16:56:38Z) and the new
ones unschedulable until their nodes returned; kv-4's **triggered a
scale-up of c, 2 to 3 nodes** (`gke-kvstore-kvstore-de746ad4-wxtj`,
unused; the extra b node of test 1 had been removed by then). One
`FailedAttachVolume` each (the CSINode not yet registered).

- **kv-client: kind's finding did not reproduce.** Every t2svc failure was
  one the key prediction expected; no unexpected failure through the
  Service. On kind the frozen pods' connections hung (1,694 unpredicted
  failures); here the VMs shut down, the connections closed at once (the
  recorder sent kv-1 its last request at +4.3s, kv-4 at +5.4s), and
  kv-client's connections moved to live pods.
- **Not `Degraded`**, as test 1's finding led to expect (the written
  prediction said `Degraded`): `ready=4` from 16:55:18Z to 16:57:48Z
  (2m30s), but the pods were recreated at 16:56:27Z and 16:56:38Z, which
  restarted `Grace`.
- **Hints**: 3,681 created on the four live pods (kv-0 1,142, kv-2 1,037,
  kv-3 821, kv-5 681) plus kv-4's 142 after its restart (kv-1 and kv-4
  restarted, which reset their counters): about 3,823, predicted 4,071 to
  4,979. Below the range, consistent with kv-4 back 13s before the heal
  the prediction assumes; all delivered 2m28s after the heal.

From here on `chaos.sh check` covers t2 too, so its overall verdict stays
`FAIL` on those 451; each later test is read from its own section and the
read-back line.

### Test 3, partition of zone d (kv-2, kv-3): as predicted

Egress-only policies on both sides, injected 17:08:09.3Z, healed
17:10:10.9Z (2m2s). 14,001 Puts (1,926 failed) and 35,001 Gets (4,843
failed); records verified complete (49,002). `check`'s t3 section: 46,342
as predicted and 2,660 in transition, **no request against its
prediction**. Read-back over the session so far: 75,761 acknowledged
writes, **0 lost, 0 stale**, every copy on all its owners. No pod restarted
(all six at 0 restarts); the operator stayed `Ready` (no line in
`operator.log`); epoch 0. Hints 2,379 (kv-0 904, kv-1 270, kv-2 146, kv-3
142, kv-4 394, kv-5 523), predicted 2,220 to 2,921; all delivered 2m17s
after the heal. The partition labels were removed after the run.

- **CiliumEndpoints exist on GKE** (contrary to `chaos.sh`'s comment that
  Dataplane V2 does not expose them), with the pods' labels in their
  identities and, as on kind, without
  `statefulset.kubernetes.io/pod-name`. `partition` therefore waited for
  the new identities rather than 60s.
- **Question 3: Dataplane V2 enforces the egress-only partition at once.**
  `peers` (every member's own view, `t3-peers.txt`), in its first round
  (started 0.1s after the inject mark, which `partition` writes after
  applying both policies): kv-1, kv-2, kv-4, kv-5 already reached only
  their own side, kv-3 and kv-4/kv-5 already counted the other side dead;
  by the second round (+9s) every member saw exactly its side (the group
  `alive=1`, the rest `alive=3`), and so it stayed for the 2 minutes.
- **The heal**: in the first round after it (started +0.1s) every member
  but kv-0 saw all five peers (kv-0 four); from the second (started +3.4s)
  every member saw all five. Within the expected 15s, and well inside
  kind's 7s with the keepalive.
- **Each pair, from the kv pods' logs** (`logs/kvstore.log`): the 16
  directed pairs across the cut (2 group pods x 4 rest pods, both ways)
  logged `is dead` from 17:08:11.9Z to 17:08:13.1Z, 2.6 to 3.8s after the
  inject mark (3 missed heartbeats after the policies took effect, about a
  second before the mark), and `is alive again` from 17:10:11.700Z to
  17:10:12.845Z: **every pair within 2.0s of the heal**.

**Prometheus before test 4.** Test 4 takes Prometheus's `emptyDir` with
the system node, so the session's series so far were exported first, with
`deploy/bench/export.sh` (the chaos runbook has no export step of its
own) given a `run.env` of the context and the window SESSION_START
(16:07:36Z) to 17:16:18Z, into `prometheus/` (20 CSVs, none empty, 15s
steps, data from 16:10:21Z, when the kv pods came up, to 17:16:36Z;
`window-source.env` is that window). Over the window: Put p50 / p99 6.50 /
12.99 ms, Get 1.65 / 5.08 ms, fsync p99 5.62 ms, 1.01 writes per fsync
(group commit on, at 40 writes/s: as expected, no batching), kv CPU 0.08
to 0.14 cores per pod.

### Test 4, system-node loss: zero errors

`recreate-instances` of `gke-kvstore-system-f675d0ba-g9d7`, injected
17:19:49.0Z, healed 17:23:13.0Z (3m24s). 18,281 Puts and 45,701 Gets,
**every one ok** (records verified complete, 63,982); `check`'s t4
section: 61,321 as predicted, 2,661 in transition, nothing against its
prediction. Read-back over the session so far: 94,042 acknowledged writes,
**0 lost, 0 stale**, every copy on all its owners. Hints 0, as predicted.
No kv pod restarted; epoch 0. Prometheus and Grafana came back empty, with
6 of 6 targets up again.

Timeline: +64.6s node not Ready, +122.5s VM recreated, +155.4s node Ready,
+185.2s operator back, +201.9s kube-dns back. `operator.log` has nothing
between 17:13 and 17:23:07Z: with the operator down nobody updated the
KVCluster, whose status said `Ready` throughout (stale, not observed). On
its return it reported `WaitingForPods` for 5s, then `Ready` at 17:23:12Z,
epoch 0.

- **Question 4: with the system node gone, neither `kubectl exec` nor the
  API server's proxy to pods works** (`t4-question4.txt`, from 17:20:53Z,
  the node not Ready): `chaos.sh peers` found all six pods `unreachable`,
  and `kubectl exec` failed with `error dialing backend: No agent
  available`. All six konnectivity agents ran on that node (as did both
  kube-dns replicas, metrics-server and the operator). `kubectl get`
  worked throughout. kube-dns's absence broke nothing: zero errors, and
  the recorders' connections already existed (each node runs
  `node-local-dns`).
- **Two more broken fetches.** `stop`'s `kubectl exec ... cat` lost its
  stream twice in the session (t1b: `websocket: close 1006`, the file cut
  at 56,152 of 64,822 lines; t4: `wsarecv: An existing connection was
  forcibly closed`, before `recorder.log`). Both were fetched again from
  the held recorder and checked against its own totals. `fetch` should
  check that itself: **fixed since** (below).

### Convergence: every acknowledged write on all its owners

`converge` from 17:40:28Z, a full anti-entropy interval (15 minutes) after
t4's last request (17:25:20Z): every run's 98,726 written keys read back
with each owner's copy. **`check-final`: 94,042 acknowledged writes, 0
lost, 0 stale, every one on all three of its owners** (`converged`: no
under-replication allowed); the 4,684 unacknowledged writes all absent.
Its verdict is `FAIL` only on t2's 451 early successes; nothing else in
any test is against its prediction.

The fetch of the read-back broke three times in four tries (84,911, 87,708
and 89,132 of 98,726 lines; **one of them with exit status 0**), the fourth
complete (98,726 lines, each parsed). A silent truncation means a count
check is the only safe test: after it, every run's fetched records were
checked against its recorder's own totals, and every read-back against its
run's Puts, all complete. Long `kubectl exec` streams through the API
server were reset five times in the session; `fetch` and `converge` need a
count check and a retry, or a transfer in pieces (`kvchaos cat` has no
offset). **Fixed since**: `fetch`, `readback` and `converge` copy through
`fetch_file`, which checks the count (each recorder's own total, each
run's Puts) and tries three times.

## Session 3, part B (results), 2026-10-17

After the convergence check, the 8 held recorder pods deleted (all fetched
and verified; they requested 2,000m on the loadgen node, where k6 needs
3,000m). Same cluster, GKE version, VMs and disks for both sides; the
node image `kvnode@sha256:9d93ce55...`, commit `e406077`.

**Predictions, written before the first run** (step 17):

| Side | Prediction |
|---|---|
| Off | Session 2's series A again: Put p99 turns up at 300-400/s, p50 at 500/s; 1.00 writes per fsync at every step; commit wait p99 rising with the rate while fsync p99 stays near 5 ms and the kv pods under 0.4 cores. Constant 40/s within about 20% of session 2's (Put p50 6.68 / p99 9.96 ms, fsync p99 4.91 ms) |
| On, to 500/s | No knee: Put p99 within about 2x its 100/s value at every step; writes per fsync above 1 and rising with the rate; commit wait p99 near one fsync (5-10 ms) rather than rising; constant 40/s no slower than off (a lone write waits only for its own fsync; part A's 40/s with group commit on already showed 1.01 writes per fsync) |
| On, past 500/s | A knee between 500 and 900/s, set by something other than the write lock: the busiest kv pod's CPU (1.93 cores allocatable), the replication fan-out, or fsync time itself growing with batch size. Which one, from the same window |

### Off (`groupCommit: false`)

`run.sh reset` with `GROUP_COMMIT=false` (17:45-18:00Z: 15 minutes, the
old volumes deleted and new ones created; the ConfigMap says
`groupCommit: false`, and so does each run's `run.env`), a clean preload
of 20,000 keys (`20261017-180040`), then:

| Run | k6 | Prometheus (server side) | k6 CPU |
|---|---|---|---|
| [20261017-180221](results/bench/20261017-180221-gke-writes-constant/), constant 40/s, 300s | 12,000 Puts (40.0/s); p50 7.63, p95 9.45, p99 10.77 ms; 0 errors; 0 dropped | Put p50 6.46 / p99 9.92 ms; fsync p50 1.96 / p99 4.92 ms; 1.00 writes per fsync; commit wait p99 0.10 ms; kv CPU 0.06-0.14 cores | max 70m |
| [20261017-180827](results/bench/20261017-180827-gke-writes-ramp/), ramp 100-500/s, steps of 100, 60s | 88,399 Puts (276.2/s); p99 317.74 ms; **632 errors, 6,985 dropped (step 5)** | Put p50 7.40 / p99 3,491 ms; fsync p99 7.19 ms; 1.00 writes per fsync; commit wait p99 7,493 ms; kv CPU 0.24-0.44 cores (window), up to 0.80 in a 15s sample | max 427m |

| Step | Target/s | Achieved/s | p50 | p95 | p99 (ms) | Errors | Dropped | Session 2: p50 / p99 (ms) |
|---|---|---|---|---|---|---|---|---|
| 1 | 100 | 100.0 | 7.74 | 11.04 | 13.84 | 0 | 1 | 7.36 / 10.35 |
| 2 | 200 | 200.0 | 7.07 | 9.66 | 11.88 | 0 | 0 | 7.21 / 16.61 |
| 3 | 300 | 300.0 | 7.46 | 18.97 | **86.03** | 0 | 0 | 7.97 / **104.24** |
| 4 | 400 | 400.0 | 9.09 | 37.28 | 65.96 | 0 | 0 | 7.85 / 41.42 |
| 5 | 500 | **383.6** | **13.38** | 143.05 | **3,473** | **632** | **6,985** | **14.44** / 241.49, 0 errors |

- **Constant 40/s reproduces session 2** within 3% on the server side (Put
  p50 6.46 against 6.68, p99 9.92 against 9.96, fsync p99 4.92 against
  4.91 ms).
- **The ramp's knees are session 2's**: p99 turns up at 300/s (86 against
  104 ms), p50 at 500/s (13.4 against 14.4 ms). Writes per fsync 1.00 at
  every step; commit wait p99 rising with the rate (0.1 ms at 40/s, about
  1.6, 3.6, 86, 61 ms in the first four minutes of the ramp, 9.2-9.6 s in
  the last).
- **But step 5 collapsed where session 2's held**: 383.6 of 500/s
  achieved, 632 errors (5s timeouts, max 5,000.74 ms) and 6,985 dropped,
  Put p99 at the histogram's top bucket (`5` in the CSV, a lower bound).
  fsync p99 also rose, to about 10 ms in the last two minutes (session 2:
  4.8-4.9), and the kv pods reached 0.8 cores in 15s samples (the
  prediction said under 0.4: true of the window averages only). Off is the
  baseline the on side is compared with, on this cluster; why 500/s held
  in session 2 and not here is open.

### On (`groupCommit: true`)

`run.sh reset` with `GROUP_COMMIT=true` (about 5 minutes; `groupCommit:
true` in the ConfigMap and each `run.env`), a clean preload
(`20261017-182328`), then:

| Run | k6 | Prometheus (server side) | k6 CPU |
|---|---|---|---|
| [20261017-182442](results/bench/20261017-182442-gke-writes-constant/), constant 40/s, 300s | 12,000 Puts (40.0/s); p50 7.56, p95 9.63, p99 11.41 ms; 0 errors; 0 dropped | Put p50 6.46 / p99 9.97 ms; fsync p50 1.95 / p99 4.91 ms; 1.00 writes per fsync; commit wait p99 0.10 ms; kv CPU 0.05-0.14 cores | max 116m |
| [20261017-183051](results/bench/20261017-183051-gke-writes-ramp/), ramp 100-500/s, steps of 100, 60s | 95,999 Puts (300.0/s); p99 16.40 ms, max 127.36 ms; **0 errors; 0 dropped** | Put p50 6.22 / p99 14.62 ms; fsync p99 4.77 ms; 1.08 writes per fsync (batch p99 1.95); commit wait p99 2.65 ms; kv CPU 0.24-0.37 cores (window) | max 480m |

**The A/B, step by step** (k6's `steps.csv`; per minute from Prometheus,
the minute that holds most of each step: Put p99, fsync p99, writes per
fsync, commit wait p99, and the busiest pod's CPU in a 15s sample):

| Step | Off: achieved/s, p50 / p99 (ms), errors | On: achieved/s, p50 / p99 (ms), errors | Off: fsync p99, writes/fsync, commit wait p99 | On: fsync p99, writes/fsync, commit wait p99 |
|---|---|---|---|---|
| 100/s | 100.0, 7.74 / 13.84, 0 | 100.0, 7.21 / 10.50, 0 | 7.4 ms, 1.000, 0.9 ms | 4.9 ms, 1.001, 0.2 ms |
| 200/s | 200.0, 7.07 / 11.88, 0 | 200.0, 6.75 / 10.83, 0 | 5.0-7.5 ms, 1.000, 1.6-3.6 ms | 4.8 ms, 1.002, 2.2 ms |
| 300/s | 300.0, 7.46 / **86.03**, 0 | 300.0, 6.76 / 13.87, 0 | 6.8 ms, 1.000, **86 ms** | 4.9 ms, 1.024, 2.5 ms |
| 400/s | 400.0, 9.09 / 65.96, 0 | 400.0, 6.73 / 15.12, 0 | 5.0 ms, 1.000, 61 ms | 4.7 ms, 1.073, 2.5 ms |
| 500/s | **383.6**, **13.38 / 3,473**, **632** | **500.0, 6.88 / 25.58, 0** | 9.8 ms, 1.000, **9.2-9.6 s** | 4.7-4.8 ms, 1.15-1.19, 3.4-3.9 ms |

Against the predictions:

- **Constant 40/s: no cost.** On and off alike (server Put p50 6.46 / p99
  9.97 against 6.46 / 9.92 ms; 1.00 writes per fsync): as predicted.
- **On, to 500/s: no knee.** Every step achieved, 0 errors, 0 dropped, p50
  flat at 6.7-7.2 ms. Writes per fsync above 1 and rising with the rate
  (1.001 to 1.19), the commit wait p99 at 0.2-3.9 ms, under one fsync
  (fsync p99 4.7-4.9 ms, flat), where off's rose to seconds. As predicted,
  except that **Put p99 rose 2.4x** from 100/s to 500/s (10.50 to 25.58 ms;
  the server's 2.6x), a little past the predicted 2x: rising, not a knee.
- **What moved the knee is the flag**: the two sides differ only in
  `group_commit` (same commit, image, cluster, VMs and disks), and on the
  on side writes per fsync rises above 1 exactly as off's commit wait
  climbs (86 ms at 300/s, seconds at 500/s) and on's does not.
- The batches are small: at 500/s an fsync carries 1.19 writes on
  average. Even so, fsync stops being a queue: off's write lock serialised
  each write behind the last one's fsync.

### On, past 500/s

No knee by 500/s, so step 20: `run.sh reset` (`groupCommit: true`), no
preload, then [20261017-191704](results/bench/20261017-191704-gke-writes-ramp/),
500 to 900/s in steps of 100 held 30s. **The reset took 37 minutes**
(18:39-19:16Z), almost all of it **before** it deleted the old KVCluster:
the operator reconciled that one as `None(Ready)` once a minute until
19:16:20Z (`logs/kvstore-operator-system.log`), the new one was created at
19:16:40Z and `Ready` at 19:16:56Z. Before its `delete kvc`, `run.sh
reset` only reads the confirmation and runs `delete_job`, whose `kubectl
delete job k6-bench --wait=true` has no timeout: most likely the wait on
the finished k6 Job's deletion, not proven (the reset's output was not
kept beyond its last lines). The earlier resets took 15 and 5 minutes.
**Fixed since**: that delete has a 2-minute timeout, and fails the reset
with the Job's name rather than waiting.

| Step | Target/s | Achieved/s | p50 | p95 | p99 (ms) | Errors | Dropped |
|---|---|---|---|---|---|---|---|
| 1 | 500 | 499.9 | 8.47 | 12.68 | 16.16 | 0 | 2 |
| 2 | 600 | 600.0 | 8.23 | 18.19 | 32.36 | 0 | 0 |
| 3 | 700 | 700.0 | 7.90 | 12.79 | 20.22 | 0 | 0 |
| 4 | 800 | 800.0 | 8.06 | 15.04 | 37.49 | 0 | 0 |
| 5 | 900 | 900.0 | 8.38 | 27.07 | 60.61 | 0 | 0 |

Over the window (Prometheus, 170s): 116,919 Puts, 0 errors, p50 7.37 / p99
27.90 ms; fsync p50 1.94 / p99 6.98 ms; **1.56 writes per fsync** (batch
p99 5.85); commit wait p50 0.71 / p99 6.28 ms; kv CPU 0.40-0.46 cores per
pod (window averages); k6 at most 703m.

- **No knee up to 900/s, against the prediction** of one between 500 and
  900/s. Every step achieved with no error and no drop, p50 flat at
  7.9-8.5 ms; p99 rises (16 to 61 ms) and the batches grow (1.56 writes per
  fsync over the window), but nothing turns up sharply. The write ceiling
  with group commit on is above 900/s, the end of this ramp; past it the
  key cap and memory limit must be raised first (step 20).
- Its 500/s step against the on ramp's (which had about 98,000 keys
  behind it, this one none): p50 8.47 against 6.88 ms, p99 16.16 against
  25.58 ms. The same order: the empty cluster is no faster.
- Against the off side, the write ceiling moved from 400/s (off: p99 up at
  300/s, collapse at 500/s with 632 errors) to past 900/s.

### On, 900 to 1,500/s

One more run before the teardown, on a fresh KVCluster (`groupCommit:
true`). 900 to 1,500/s in steps of 100 held 15s would write 162,000 keys
(13,500 + 19,750 + 21,750 + 23,750 + 25,750 + 27,750 + 29,750), past the
130,000 cap and, at 0.75 x keys on the busiest pod (121,500), past what
512Mi is sized for (about 100,000). So steps of 150 held 15s: 13,500 +
20,625 + 23,625 + 26,625 + 29,625 = **114,000 keys** (busiest pod about
85,500). `VUS=300`, so that k6's pool cannot limit the top steps (1,500/s x
0.2s).

The reset, traced with a timestamp per command, took **33s**: `delete_job`
2s, the KVCluster and its pods 8s, the PVCs 2s, the new KVCluster Ready
in 19s. The 37 minutes of the previous reset did not recur; its cause is
unproven.

[20261017-194306](results/bench/20261017-194306-gke-writes-ramp/):
113,992 Puts, **0 errors, 7 dropped** (steps 1-2, k6's start), 300 of 300
VUs connected, k6 CPU at most 1,054m (median 889m). Server-side Put p99
per step: the export's quantiles use 1-minute rates, which span 3-4 of
these 20s steps, so it was taken from the Put histogram's increase between
consecutive 15s scrapes, each interval assigned to a step by its request
rate (the 15s grid does not align with the 20s steps; step 1's is
approximate).

| Step | Target/s | Achieved/s | p50 | p95 | p99 (ms) | Errors | Dropped | Server Put p99 (ms) |
|---|---|---|---|---|---|---|---|---|
| 1 | 900 | 899.6 | 8.55 | 14.18 | 19.80 | 0 | 6 | about 19 |
| 2 | 1,050 | 1,049.9 | 8.48 | 13.54 | 21.95 | 0 | 1 | 18.9-19.3 |
| 3 | 1,200 | 1,200.1 | 8.50 | 13.97 | 26.36 | 0 | 0 | 19.3-19.5 |
| 4 | 1,350 | 1,350.0 | 8.48 | 14.79 | 31.31 | 0 | 0 | 25.9 |
| 5 | 1,500 | 1,500.0 | 8.58 | 14.77 | 30.45 | 0 | 0 | 41.4, then 28.1 |

Over the window (95s): fsync p99 4.97 ms, flat; **2.18 writes per fsync**
(1.8 to 2.5 across the steps, batch p99 7.76); commit wait p99 4.7-5.1 ms,
about one fsync; kv CPU 0.49-0.61 cores per pod (window averages), the
busiest 0.74 in a 15s sample.

**No knee up to 1,500/s.** p50 flat, p99 up 1.6x, no errors; fsync time
does not grow with the batches, and the busiest pod uses under 40% of its
1.93 allocatable cores. With group commit the write ceiling on this
cluster is above 1,500/s; off's was about 400/s. The next limit (most
likely CPU) needs the key cap and the memory limit raised together, before
a session.

## The gcloud configuration

`kvstore` is a separate gcloud configuration, so that commands here never
run against another project by accident. `gcloud config configurations
list` shows them all; `gcloud config configurations activate default`
switches back to the one that was active before, and
`gcloud config configurations activate kvstore` returns here. A single
command can also name one: `gcloud --configuration=default …`.
