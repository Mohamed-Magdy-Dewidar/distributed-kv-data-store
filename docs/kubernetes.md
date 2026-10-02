# Running on Kubernetes (kind)

This runbook runs a 10-node cluster (`N=3, W=2, R=2`) on a local
[kind](https://kind.sigs.k8s.io/) cluster, from the manifests in
`deploy/k8s/`. It was written on Windows 11 with Docker Desktop (WSL2
backend); the commands are the same elsewhere, except for the cgroup step,
which is specific to Docker Desktop on WSL2.

| Tool | Version used |
|---|---|
| Docker Desktop (engine) | 28.1.1, WSL2 backend |
| kind | v0.33.0 |
| Kubernetes (kind node image) | v1.35.8 |
| kubectl | v1.35.9 |
| metrics-server | v0.9.0 |

Kubernetes 1.35 matches the default version of GKE's Regular release channel
at the time of writing (1.35.8-gke.1225000).

## Prerequisite: cgroup v2 (Docker Desktop on WSL2)

Kubernetes 1.35's kubelet refuses to run on a cgroup v1 host, and Docker
Desktop's WSL2 VM can still be on cgroup v1. Check:

```sh
docker info --format '{{.CgroupVersion}}'
```

If it prints `1`, `kind create cluster` fails while starting the control
plane, and the node's kubelet log says `kubelet is configured to not run on a
host using cgroup v1`. To switch WSL2 to cgroup v2, put this in
`%USERPROFILE%\.wslconfig` (it applies to every WSL distro, not only Docker's):

```ini
[wsl2]
kernelCommandLine = cgroup_no_v1=all
```

Then quit Docker Desktop **before** running `wsl --shutdown` (otherwise it
restarts the VM with the old settings), start Docker Desktop again, and check
that `docker info --format '{{.CgroupVersion}}'` prints `2`.

## Install the tools

kubectl must be within one minor version of the cluster (here 1.34–1.36).
Docker Desktop ships its own, older kubectl in the system `PATH`
(`C:\Program Files\Docker\Docker\resources\bin`); a newer one has to come
before it there, or be called by its full path. The official binary is
`https://dl.k8s.io/release/v1.35.9/bin/windows/amd64/kubectl.exe`, with its
SHA-256 at the same URL plus `.sha256`; check it before use.

kind, pinned to a release:

```sh
go install sigs.k8s.io/kind@v0.33.0
kind version
```

## Create the cluster

One node (control plane only), with the node image pinned by the digest
listed in the [kind v0.33.0 release notes](https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0):

```sh
kind create cluster --name kvstore --wait 120s \
  --image kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0
kubectl config current-context        # kind-kvstore
kubectl version                       # server v1.35.8
kubectl get nodes                     # kvstore-control-plane Ready
kubectl get storageclass              # standard (default), rancher.io/local-path
```

### Storage: deleting a PVC deletes that node's data

kind's default storage class, `standard` (`rancher.io/local-path`), has
reclaim policy `Delete`: when a PersistentVolumeClaim is deleted, its volume
and the node's data directory on it are deleted too. The StatefulSet keeps its
claims when the StatefulSet itself is deleted (`whenDeleted: Retain`), but
deletes a pod's claim when scaling down removes that pod
(`whenScaled: Delete`), and a claim deleted by hand is gone with its data.

## Install metrics-server

`kubectl top` needs metrics-server. Download the release manifest, check it
against the SHA-256 GitHub publishes for the asset on the
[v0.9.0 release page](https://github.com/kubernetes-sigs/metrics-server/releases/tag/v0.9.0)
(v0.9.x supports Kubernetes 1.34+), and apply the checked file:

```sh
curl -sSLo metrics-server-v0.9.0.yaml \
  https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.9.0/components.yaml
sha256sum metrics-server-v0.9.0.yaml
# must be 1cec29a5267809306a2c6ec74a3e449abbb705b4a8beed0c8a1963910f72c79b
kubectl apply -f metrics-server-v0.9.0.yaml
```

kind's kubelets serve self-signed certificates, so on kind (only) let
metrics-server skip verifying them:

```sh
kubectl -n kube-system patch deployment metrics-server --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
kubectl -n kube-system rollout status deployment/metrics-server
kubectl get apiservice v1beta1.metrics.k8s.io   # AVAILABLE True
kubectl top nodes
```

Right after the patch, the APIService can briefly report
`False (FailedDiscoveryCheck)` while the new pod starts; it turns `True` once
the pod is serving.

## Deploy

Build the image and load it into the kind node:

```sh
docker build -t kvnode:dev .
kind load docker-image kvnode:dev --name kvstore
```

Apply the manifests (`00-namespace.yaml` sorts first, so the namespace exists
before anything in it) and wait:

```sh
kubectl apply -f deploy/k8s/
kubectl -n kvstore rollout status sts/kv
kubectl -n kvstore get pods        # kv-0 .. kv-9, all 1/1 Running
```

| File | What it is |
|---|---|
| `00-namespace.yaml` | Namespace `kvstore`. |
| `configmap.yaml` | `kv-config`: the node config every pod reads, with no `nodeId` (each pod gets its pod name through `KV_NODE_ID`). Members `kv-0` .. `kv-9` at `kv-N.kv.kvstore.svc.cluster.local:7000`, epoch 0, `n: 3, w: 2, r: 2`, every interval and timeout explicit, anti-entropy every 15 minutes (see [the anti-entropy interval](#the-anti-entropy-interval)). |
| `service-headless.yaml` | `kv`: gives each pod its DNS name. `publishNotReadyAddresses: true` keeps the name resolvable while a pod starts and while a removed pod drains. |
| `service-client.yaml` | `kv-client`: gRPC (7000) only, to ready pods. The probe and admin port (8080) is not exposed. |
| `statefulset.yaml` | `kv`: 10 replicas, `podManagementPolicy: Parallel`, PVC retention `whenScaled: Delete, whenDeleted: Retain`, `minReadySeconds: 5`, a 5s `preStop` sleep, `/livez` and `/readyz` probes, `terminationGracePeriodSeconds: 60`, non-root with a read-only root filesystem, one 1Gi volume per pod at `/data`. |

**Measured:** all 10 pods were Ready 25.9s after `kubectl apply`, with no
restarts. Nearly all of that is the local-path provisioner creating the 10
volumes one after another (a pod is scheduled once its volume exists); a
node process logs `ready` well under a second after it starts.

The idle cluster then showed no `ADDRESS MISMATCH` or `MEMBERSHIP CONFLICT`
line. Pods that started first marked later ones dead (42 times), and each was
seen alive again 0.50s-1.50s (median 1.04s) after the later pod logged
`ready`: the ping-back described in the cold-start entry of
[known-limitations.md](known-limitations.md).

### Talking to the cluster

```sh
# Client API (any node accepts a read or write and forwards it)
kubectl -n kvstore port-forward svc/kv-client 7000:7000
grpcurl -plaintext -d '{"key": "greeting", "value": "hello"}' localhost:7000 kvstore.KVClient/Put
grpcurl -plaintext -d '{"key": "greeting"}' localhost:7000 kvstore.KVClient/Get

# Admin API of one pod (unauthenticated; not exposed through a Service)
kubectl -n kvstore port-forward pod/kv-3 8080:8080
curl -s localhost:8080/admin/membership
```

A port-forward to a Service picks one pod and dies when that pod restarts;
for anything that has to survive restarts, run the client inside the cluster
(see [Test tooling](#test-tooling)).

### Client notes

- **A connection failure is always safe to retry.** If the client could not
  connect at all (grpcurl: `Failed to dial target host`), the request was never
  sent and nothing was written.
- **A failed `Put` has an unknown outcome**: the write may have been applied.
  Retrying with the same context is safe and at worst leaves a duplicate
  sibling; see [known-limitations.md](known-limitations.md).

### Stopping a pod: the endpoint-removal gap

When a pod is told to stop, Kubernetes removes it from `kv-client`'s endpoints
asynchronously. Without a delay, the node stops its gRPC listener as soon as it
receives SIGTERM, and until the removal has spread, a new client connection can
still be routed to it and be refused. Measured: a rolling update without the
delay failed 2 of 730 writes this way, each exactly when a pod was stopped.

The StatefulSet therefore has `lifecycle.preStop.sleep: {seconds: 5}`
(Kubernetes' built-in sleep, which needs no shell in the distroless image): the
pod keeps serving for 5s, has left the endpoints by the time it gets SIGTERM,
and then shuts down as before. This only delays SIGTERM. It is not a drain: a
pod cannot tell a restart from a scale-down, and the node does the same thing
on either. It adds about 5s per stopped pod.

### The ConfigMap

- **It applies only when a pod starts.** Editing it changes nothing in running
  pods; membership changes reach running pods through heartbeats (see
  [membership.md](membership.md)).
- **Its member strings must be identical to those in every
  `POST /admin/membership`**, because nodes compare addresses as strings: use
  `kv-N.kv.kvstore.svc.cluster.local:7000` everywhere.
- **Keep its epoch and member list in step with what the cluster holds.** A pod
  that starts with the same epoch and a different list refuses to start; one
  that has persisted a newer epoch keeps it and logs that the config is older.
  `deploy/k8s/configmap.yaml` is the initial state (epoch 0): after membership
  changes, apply an edited copy, and don't re-apply the original over it (a pod
  added later would start at epoch 0).

## Test tooling

`deploy/loadgen/loadgen.yaml` is test tooling, not part of the deployment
(`kubectl apply -f deploy/k8s/` does not include it). It holds a ConfigMap of
scripts and a `toolbox` pod, both on `fullstorydev/grpcurl:v1.9.3-alpine` (the
newest grpcurl image published; `kind load` cannot load this multi-platform
image, so the kind node pulls it):

| Script | What it does |
|---|---|
| `write.sh` | Writes `PREFIX-000001`, `PREFIX-000002`, ... with value `v-<key>` through `kv-client`, one at a time, and logs `<UTC time> <uptime> <key> ok` or `... ERR <code> <message>` for every write. |
| `verify.sh` | Reads `key value` lines and checks each with a quorum `Get` through `kv-client`. |
| `copies.sh` | Reads `key owner owner owner` lines and asks each owner for its own copy with `KVReplication/FetchItem` (local storage only, no quorum, no forwarding). |
| `poll.sh` | `poll.sh EPOCH NODES`: waits until every node reports `EPOCH`, then until `/admin/handoff?epoch=EPOCH` returns 200 on all of them, printing the time of each. |

A writer runs as its own pod (its log is the record of acknowledged writes);
for the measurements below, a steady load wrote one key at a time with
`SLEEP: "0.05"` (about 11 writes/s):

```yaml
apiVersion: v1
kind: Pod
metadata: {name: load, namespace: kvstore}
spec:
  restartPolicy: Never
  containers:
    - name: writer
      image: fullstorydev/grpcurl:v1.9.3-alpine
      command: ["/scripts/write.sh"]
      env: [{name: PREFIX, value: load}, {name: START, value: "1"}, {name: COUNT, value: "0"}, {name: SLEEP, value: "0.05"}]
      volumeMounts: [{name: scripts, mountPath: /scripts}]
  volumes: [{name: scripts, configMap: {name: loadgen, defaultMode: 0555}}]
```

```sh
kubectl -n kvstore logs load | awk '$4=="ok" {print $3, "v-" $3}' > acked.txt
kubectl -n kvstore exec -i toolbox -- /scripts/verify.sh < acked.txt
```

A key's owners (for `copies.sh`) are its preference list:
`hashring.NewHashRingFromMembers(node.VirtualNodesPerPhysical, ids).GetPreferenceList(key, 3)`;
the measurements used a throwaway Go program around that call.

## Scenarios, measured

On the single-node kind cluster above, Docker Desktop on WSL2, 10 replicas,
`N=3, W=2, R=2`. 2000 keys were preloaded first (4 writers, 21s, no errors) so
that membership changes move data, and the steady load ran through every
scenario. After each one, every acknowledged write was read back with a quorum
`Get`, and 20 sampled keys were checked for exactly 3 local copies on their
owners. The ConfigMap ran anti-entropy every 60s during these measurements; it
now runs it every 15 minutes (see
[the anti-entropy interval](#the-anti-entropy-interval)).

| Scenario | Failed writes | Afterwards |
|---|---|---|
| Delete a pod (SIGTERM) | 0 of 350 | |
| Force-kill a pod | 0 of 727 | 3707 of 3707 read back; 20/20 with 3 copies |
| Rolling restart, no `preStop` sleep | 0 of 875 | 6101 of 6101; 20/20 |
| Rolling update, no `preStop` sleep | 2 of 730 (connection failures) | |
| Rolling restart with the sleep, 3 runs | 0 of 1396, 0 of 1490, 0 of 1456 | 52,135 of 52,135; 20/20 |
| Scale 10 → 11 | 0 of 548 | 7750 of 7750; 20/20 |
| Scale 11 → 10 | 0 | 9069 of 9069; 20/20 |
| Scale 11 → 10 with the sleep | 0 of 2611 | 60,603 of 60,603; 20/20 |

### Losing a pod

```sh
kubectl -n kvstore delete pod kv-4                                # normal SIGTERM
kubectl -n kvstore delete pod kv-7 --grace-period=0 --force       # hard kill
```

A pod whose disk is intact just restarts: same volume, same incarnation, same
data.

- **SIGTERM (`kv-4`):** the new pod was Ready about 2.4s after the delete, at a
  new IP. Only one peer had missed 3 pings and marked it dead; it saw `kv-4`
  alive again 0.70s after `kv-4` logged `ready`. 0 of 350 writes failed.
- **Hard kill (`kv-7`):** the new pod was Ready 1.6s after the kill, at a new
  IP. Peers were still pinging the old address and marked `kv-7` dead shortly
  after it was already up; all 9 saw it alive again 1.44s after it logged
  `ready`, when its first heartbeat reached them and each pinged back over a
  new connection (which resolves the name afresh). 0 of 727 writes failed, and
  the 34 keys `kv-7` owns that were written while it was down are all on it
  (delivered by hints or anti-entropy; the check shows they arrived, not which
  one carried them).
- **DNS:** gRPC re-resolves a peer's name at most every 30s on an existing
  connection, and backs off after a failed lookup. In both cases the ping-back's
  new connection hid that: peers were back within 1.5s of the restarted pod
  being ready, the same as a cold start.

**A pod whose disk is lost:** delete its PVC, then the pod (a claim in use is
only deleted once its pod is gone, so the PVC first, without waiting):

```sh
kubectl -n kvstore delete pvc data-kv-N --wait=false
kubectl -n kvstore delete pod kv-N
```

The StatefulSet recreates both: the pod comes back with an empty volume and a
new incarnation (a new writer, not its old self), and refills from hints and
anti-entropy; with W=2, every acknowledged write was on at least one other
replica. This procedure was not exercised in these measurements. Never put an old copy of a node's
data back instead (see "Never restore a node's data dir" in
[known-limitations.md](known-limitations.md)).

**Removing a pod from the middle** (say `kv-4` of `kv-0`..`kv-9`) is not
supported with a StatefulSet: it can only remove the highest ordinal. Replace
a broken pod in place instead (above).

### Rolling restart under load

```sh
kubectl -n kvstore rollout restart sts/kv
kubectl -n kvstore rollout status sts/kv
```

Pods restart one at a time from `kv-9` down. With `minReadySeconds: 5`, the
next pod stops only after the previous one has been Ready for 5s.

- **Without the `preStop` sleep:** 64.8s in all; each pod was down about 2s; no
  two pods were ever down at once (at least 3.6s between one pod being back,
  seen alive by its peers, and the next stopping). 0 of 875 writes failed in
  that run, but a second rolling update failed 2 of 730 writes with connection
  failures, each in the second a pod stopped (see
  [the endpoint-removal gap](#stopping-a-pod-the-endpoint-removal-gap)).
- **With the sleep, 3 runs:** 106.7s, 112.5s and 106.7s; 0 of 1396, 0 of 1490
  and 0 of 1456 writes failed.

### Scale up: add `kv-10`

Following [membership.md](membership.md): give the new node a configuration at
the next epoch listing everyone and itself, then start it.

1. Read the current epoch: `curl -s localhost:8080/admin/membership` on any pod.
2. Apply a copy of the ConfigMap with `epoch` raised by one and
   `kv-10` / `kv-10.kv.kvstore.svc.cluster.local:7000` added to `members`.
3. `kubectl -n kvstore scale sts/kv --replicas=11`. `kv-10` starts at the new
   epoch and announces it; the others fetch and adopt it.
4. Wait for `GET /admin/handoff?epoch=E` to return 200 on every pod.

Change `replicas` by exactly one: one node per membership change.

Measured, with about 7,400 keys: all 11 pods reported epoch 1 6.9s after the
scale (`kv-10` was ready at 5s, the others adopted the epoch from it within
1.6s), and every handoff was done at 36.1s: 6205 keys pushed to new owners,
none turned into hints. 0 of 548 writes failed. Afterwards `kv-10` held all
1393 of the keys it owns (18.8% of them; an even split would be 27%, see the
skew entry in [known-limitations.md](known-limitations.md)).

Repeated later with about 52,000 keys: the epoch spread in 5.1s, and the
handoff took 260s for 55,530 pushes, still with 0 failed writes. Handoff time
grows with the data that moves.

### Scale down: remove `kv-10`

1. `POST /admin/membership` to any pod, at the next epoch, with every member
   except `kv-10`, using exactly the ConfigMap's member strings:
   ```sh
   curl -s -X POST localhost:8080/admin/membership -d '{"epoch": 2, "members": {"kv-0": "kv-0.kv.kvstore.svc.cluster.local:7000", ..., "kv-9": "kv-9.kv.kvstore.svc.cluster.local:7000"}}'
   ```
2. On `kv-10`, wait for `GET /admin/handoff?epoch=2&timeout=10m` to return 200:
   it has pushed everything it holds and every remaining member has been seen
   at the new epoch.
3. Apply the ConfigMap at the new epoch without `kv-10`.
4. `kubectl -n kvstore scale sts/kv --replicas=10`. `whenScaled: Delete`
   deletes `kv-10`'s claim, and the storage class deletes its volume and data:
   its data directory must not come back if the StatefulSet grows again.

Measured, with about 9,000 keys: all 10 remaining members reported epoch 2
0.8s after the POST and finished their own handoffs at 17.4s; `kv-10` reported
drained at 24.2s, having pushed 4986 keys with no hints. The pod was gone
0.95s after the scale, then `data-kv-10` and its volume. 0 writes failed, and
all 1798 keys `kv-10` had owned were on their three new owners.

With the `preStop` sleep and about 52,000 keys: members at the new epoch in
0.9s, their handoffs done at 171s, `kv-10` drained at 239s (34,899 pushes, no
hints). The pod took 6.0s to go (the 5s sleep, then the same shutdown), the
claim and volume were deleted, and 0 of 2611 writes failed.

### Unplanned: the whole cluster stopped abruptly

During the measurements Docker Desktop's VM stopped overnight. The nodes got no
SIGTERM: their logs end mid-run. When it started again, all 10 pods restarted at
once, came back at their persisted epoch with every peer alive and reachable,
and all 45,870 writes acknowledged before the stop read back, with 3 copies on
their owners in the sample. This shows that data survives an abrupt stop of the
whole cluster; how the VM was stopped is not known, so it is not a power-loss
test.

## Resources

Measured with `kubectl top pods` (metrics-server, 15s resolution) at 10
replicas:

| Condition | CPU per pod | Memory per pod |
|---|---|---|
| Idle | 13-17m | 15-21 MiB |
| Steady load, 11 writes/s, ~9,000 keys | median 36m; 20-270m (spikes during anti-entropy rounds) | 23-30 MiB |
| Write burst, 113 writes/s, ~9,000 keys | up to 480m | up to 36 MiB |
| Steady load, ~60,600 keys | up to 812m | 38-67 MiB |

The kind node itself (control plane, CoreDNS, local-path, metrics-server) used
about 850 MiB before any kv pod.

Anti-entropy is the expensive part, and it is uneven: the nodes that own the
most keys (because of the ring's skew) do the most work, since each round
covers every key a node holds against every peer (see "Anti-entropy is
unscoped" in [known-limitations.md](known-limitations.md)).

### The anti-entropy interval

The ConfigMap sets `intervals.antiEntropy: 15m` (it was 60s during the
measurements above). The reasons:

- **Rounds don't collide anyway.** Each node starts its first round after a
  random delay within one interval, so nodes' rounds are spread out, not
  synchronized; a longer interval changes how often a round happens, not how
  they line up.
- **The cost is per round.** One round compares a node's keys against every
  peer; it showed up as 270m CPU per pod at ~9,000 keys and 812m at ~60,600
  keys. A longer interval makes those spikes rarer without making each one
  cheaper.
- **Anti-entropy is the safety net, and what it repairs is rare.** Normal
  writes reach all replicas directly or through hints. Anti-entropy is what
  carries writes a hung peer missed (a peer that times out instead of failing
  gets no hint), writes accepted with a stale view during a membership change,
  and the data of a pod that comes back with an empty disk. A longer interval
  only delays those repairs.
- **Membership changes don't wait for it.** When a node finishes its handoff
  to a new membership, it starts an anti-entropy round at once, whatever the
  interval.

The price is in those delayed repairs: a pod that comes back with an empty
disk under an unchanged membership (no handoff, so no triggered round) is
refilled by anti-entropy only from its first round, up to 15 minutes after it
starts; until then, reads that need its copy are served by the other replicas.

Requests in `statefulset.yaml`: **CPU 50m** (the steady-load median × 1.5,
rounded up) and **memory 64Mi** (the highest observed at ~9,000 keys × 1.5,
rounded up), with no CPU limit (rounds and bursts use spare CPU instead of being
throttled) and a **256Mi memory limit**. Memory grows with the number of keys
per node: at ~60,600 keys it already reached 67 MiB, above the request. The
requests must be measured again under the k6 load tests before being relied on.

## Clean up

```sh
kubectl delete namespace kvstore      # everything, PVCs and data included
kind delete cluster --name kvstore
```

Deleting only the StatefulSet keeps the PVCs (`whenDeleted: Retain`), so
applying it again brings the same nodes back with their data.
