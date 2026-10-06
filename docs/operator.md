# The KVCluster operator

The operator (`operator/`, its own Go module) runs a cluster from one
resource, a `KVCluster`, and changes its membership when its `replicas`
changes. It does what [kubernetes.md](kubernetes.md) does by hand — the
ConfigMap, the Services, the StatefulSet, and the membership procedure of
[membership.md](membership.md) — one node at a time, waiting for every
handoff, and it picks up where it left off after a restart.

It was written and measured on the kind cluster of
[kubernetes.md](kubernetes.md) (Kubernetes 1.35, Docker Desktop on WSL2),
at 6 replicas, the size of the GKE target. The settings for GKE
([Placement and GKE](#placement-and-gke)) are rendered and tested, but have
not been run on GKE yet.

| Component | Version |
|---|---|
| kubebuilder (scaffold) | v4.16.0 |
| controller-runtime | v0.25.2 |
| k8s.io/* client libraries | v0.37.0 |
| envtest (tests) | Kubernetes 1.35.0 |

## Install

On the kind cluster of [kubernetes.md](kubernetes.md), from the repository
root. Build the node and operator images and load them into the kind node:

```sh
docker build -t kvnode:dev .
docker build -t kvstore-operator:dev operator
kind load docker-image kvnode:dev kvstore-operator:dev --name kvstore
```

Install the CRD, the RBAC and the operator (namespace
`kvstore-operator-system`), with the image set to the one just loaded. In
Git Bash:

```sh
kubectl kustomize operator/config/default \
  | sed 's|image: controller:latest|image: kvstore-operator:dev|' \
  | kubectl apply --server-side -f -
kubectl -n kvstore-operator-system rollout status deploy/kvstore-operator-controller-manager
```

In PowerShell, the same:

```powershell
kubectl kustomize operator/config/default | ForEach-Object { $_ -replace 'image: controller:latest', 'image: kvstore-operator:dev' } | kubectl apply --server-side -f -
```

The operator runs with leader election (`--leader-elect`), so a second
replica would wait rather than act. Its own metrics are served on 8443 with
the scaffold's authentication; there is no Prometheus job for them yet.

The operator reaches each pod's admin API at
`<pod>.<cluster>.<namespace>.svc.cluster.local:8080`. Nothing restricts that
by default: the scaffold's network policy (`operator/config/network-policy`)
is not installed by `config/default`, and it only limits traffic *to* the
operator's metrics port. A cluster that enforces network policies must allow
egress from `kvstore-operator-system` to the pods' port 8080.

## Use

```sh
kubectl create namespace kvstore      # if it does not exist
kubectl -n kvstore apply -f operator/config/samples/kvstore_v1alpha1_kvcluster.yaml
kubectl -n kvstore get kvc kv -w
```

```
NAME   REPLICAS   READY   EPOCH   PHASE     AGE
kv     6          6       0       Ready     28s
```

The sample is `kv` with 6 replicas and the image `kvnode:dev`; everything
else is defaulted. (That is the kind sample. The GKE one,
`kvstore_v1alpha1_kvcluster_gke.yaml` next to it, is described in
[Placement and GKE](#placement-and-gke).)

| Field | Default | Changeable |
|---|---|---|
| `spec.replicas` | (required, at least `replication.n`) | yes: the operator moves there one node at a time |
| `spec.image` | (required) | yes: applied as a rolling update, only while the cluster is stable |
| `spec.replication` (`n`, `w`, `r`) | 3, 2, 2 | no: N is part of every membership's fingerprint |
| `spec.storage` (`size`, `storageClassName`) | 1Gi, the default class | no: a StatefulSet's volume templates are immutable |
| `spec.resources` | requests 50m / 64Mi, limit 256Mi memory (measured, see [kubernetes.md](kubernetes.md#resources)) | yes: rolling update while stable |
| `spec.drainTimeout` | 30m | yes |
| `spec.placement` (`onePodPerNode`, `zoneSpread`, `nodeSelector`, `tolerations`) | none: the scheduler places the pods freely | yes: rolling update while stable; see [Placement and GKE](#placement-and-gke) |

The cluster's name becomes the StatefulSet's and the Services' names and
every pod's DNS name, so it must be a DNS-1035 label of at most 52
characters. **One KVCluster per namespace**: the objects it owns select
their pods by `app.kubernetes.io/name=kvstore` alone, exactly as
`deploy/k8s/` does.

Scale it with:

```sh
kubectl -n kvstore scale kvc/kv --replicas=7
```

(or edit `spec.replicas`). A change of several nodes is carried out one node
at a time.

### What it owns

From a cluster `kv` in namespace `kvstore`:

| Object | What it is |
|---|---|
| ConfigMap `kv-config` | The node config, as in `deploy/k8s/configmap.yaml`, with the membership the cluster holds. |
| Service `kv` | Headless: each pod's DNS name, not-ready addresses published. |
| Service `kv-client` | gRPC (7000) only, to ready pods. |
| StatefulSet `kv` | As in `deploy/k8s/statefulset.yaml`, with the spec's replicas, image, resources, storage and placement. |
| PodDisruptionBudget `kv` | At most one of the pods evicted at a time (`maxUnavailable: 1`); see [the PodDisruptionBudget](#the-poddisruptionbudget). |

For `kv` in `kvstore` at 10 replicas, without `placement`, the first four
are the objects in `deploy/k8s/`, except for the owner references and the
`app.kubernetes.io/managed-by` and `app.kubernetes.io/instance` labels; a
test renders them and compares field by field. `deploy/k8s/` has no
PodDisruptionBudget. The intervals and timeouts in the ConfigMap are today's
`deploy/k8s/configmap.yaml` values.

The operator writes them by server-side apply, as the field manager
`kvstore-operator`. Do not edit them: a membership change rewrites the
ConfigMap and the StatefulSet.

### Scale the KVCluster, never the StatefulSet

`kubectl scale sts/kv` bypasses the membership procedure, and the
StatefulSet acts at once:

- **Down:** it stops the highest pod and, with `whenScaled: Delete`,
  **deletes its volume**. That node was still a member, with data no other
  node may have taken over yet; its copy of every key it held is gone.
- **Up:** it starts a pod that is not in the ConfigMap; the node refuses to
  start.

The StatefulSet's replicas are the operator's: it scales a hand-scaled
StatefulSet back. Up is harmless (the extra pod never joined). Down has
already done its damage by the time the operator sees it: it brings the pod
back once the old pod and its volume are gone, as a new incarnation with an
empty data directory, which refills from the other replicas through hints
and anti-entropy (see "A pod whose disk is lost" in
[kubernetes.md](kubernetes.md#losing-a-pod)). With W=2, every acknowledged
write was on at least one other replica.

Likewise, do not `POST /admin/membership` by hand while the operator manages
the cluster: a membership the operator did not produce makes it Degraded
(below), and it stops acting.

## Placement and GKE

`spec.placement` is off unless set, and the kind sample leaves it out. Each
setting is separate:

| Setting | What the pods get |
|---|---|
| `onePodPerNode: true` | A required pod anti-affinity on `kubernetes.io/hostname`, selecting `app.kubernetes.io/name=kvstore`: no two of the cluster's pods on one node. |
| `zoneSpread: true` | A topology spread constraint on `topology.kubernetes.io/zone`, `maxSkew: 1`, `whenUnsatisfiable: DoNotSchedule`, same selector: the zones' pod counts differ by at most one, or the pod waits. |
| `nodeSelector` | The node selector, as given. |
| `tolerations` | The tolerations, as given. The CRD checks them as a pod would (the operator, the effect, no value with `Exists`, an empty key only with `Exists`, `tolerationSeconds` only with `NoExecute`), except for label syntax: a malformed key passes, fails when the StatefulSet is applied, and shows as [`ActionFailed`](#conditions). |

A placement change is a pod template change, carried out like an image
change: only while the cluster is stable, before any change of replicas, and
rolled out one pod at a time. A scale keeps the placement the pods run.

### On kind

Leave placement out. The kind node has no `topology.kubernetes.io/zone`
label, and the scheduler bypasses nodes that lack a spread constraint's key:
"the incoming Pod has no chances to be scheduled onto this kind of nodes"
([Pod Topology Spread Constraints](https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/),
"Implicit conventions"). With `zoneSpread`, every pod stays Pending; with
`onePodPerNode` on the single node, all but one do. Either way the cluster
turns `Degraded` with `PodUnavailable` after the 2-minute grace period.

### The PodDisruptionBudget

It is always created, also on kind. It allows one eviction at a time among
the cluster's pods (`maxUnavailable: 1`), and none while any of them is
unready: with N=3 and W=R=2, one node down leaves every key a quorum.

It applies to evictions only (`kubectl drain`, GKE's node upgrades): "deleting
deployments or pods bypasses Pod Disruption Budgets", and StatefulSets "are
not limited by PDBs when doing rolling upgrades"
([Disruptions](https://kubernetes.io/docs/concepts/workloads/pods/disruptions/)).
So it never holds up the operator, whose scale-downs (D4) and template
rollouts are the StatefulSet's own deletions. On kind nothing evicts: there
is one node and it is never drained, and the e2e deletes only pods the
budget does not select (the operator's and the writers').

On GKE, node upgrades drain "respecting PodDisruptionBudget and
GracefulTerminationPeriod settings for up to one hour. After one hour, any
remaining Pods are forcefully evicted"
([node pool upgrade strategies](https://cloud.google.com/kubernetes-engine/docs/concepts/node-pool-upgrade-strategies)).
A scale-up in progress (a new pod not ready yet) makes an upgrade wait. With
`onePodPerNode` an evicted pod needs a free node to come back on; the surge
upgrade (GKE's default) provisions a new node before it drains an old one.

### Nodes: one per pod, and a free one to scale up

With `onePodPerNode`, the pool needs at least as many schedulable nodes as
replicas, and **a scale-up needs a free node first**: U2 creates the new
pod, which schedules only on a node without one of the cluster's pods (and,
with `zoneSpread`, in a zone with the fewest pods). The node counts of a
regional node pool are per zone: `--num-nodes 2` in a three-zone region is 6
nodes, as are the autoscaler's `--min-nodes` and `--max-nodes`
(`--total-min-nodes` and `--total-max-nodes` count the whole pool). The
intended setup is the cluster autoscaler on the database pool: the new pod,
Pending, is what makes it add a node.

If no node is free (no autoscaler, or the pool is at its maximum), the new
pod stays Pending, and `kubectl -n kvstore describe pod kv-6` says why
(`didn't match pod anti-affinity rules`, `didn't match pod topology spread
constraints`). The KVCluster shows `Scaling` with `WaitingForEpoch` (the
ConfigMap names kv-6; nothing has adopted its epoch yet), then, 2 minutes
after the pod was created, `Degraded` with `PodUnavailable`: `kv-6 has been
not ready for 2m…`. It acts on nothing else until then. Once a node is added
the pod schedules and the scale-up finishes on its own; or roll it back as in
[A scale-up that cannot start](#a-scale-up-that-cannot-start). An autoscaler
takes minutes to add a node, so a scale-up can pass through
`Degraded`/`PodUnavailable` on its way; it clears when the pod is ready.

### Losing a node

With `onePodPerNode`, `zoneSpread` and zonal disks (`standard-rwo` is a
zonal persistent disk), a pod whose node is lost can come back only on a
free node in the same zone: its volume cannot leave the zone, and the
anti-affinity rules out every node that already has a pod. With no spare
node it stays Pending until the node is replaced, by auto-repair (on by
default for new Standard node pools) or by the autoscaler. The StatefulSet
does not even replace a pod whose node stopped answering until that node is
gone, or the pod is force-deleted. **Expect minutes, not the seconds measured
on kind** (a replacement pod Ready 1.6-2.4s after the old one died, in
[kubernetes.md](kubernetes.md#losing-a-pod)).

Meanwhile the cluster serves with one node down (every key keeps a quorum),
writes for that node become hints, and the operator shows `WaitingForPods`,
then `Degraded`/`PodUnavailable` after 2 minutes, and starts no membership
change until the pod is back.

### Zones and keys

Zone spread places pods, not keys. The ring picks a key's three owners
without knowing zones, so with 6 pods over 3 zones some keys have two or all
three replicas in one zone, and losing that zone takes their quorum. Zone
spread limits how much a zone outage takes; it does not make the cluster
survive one.

### The GKE sample

`operator/config/samples/kvstore_v1alpha1_kvcluster_gke.yaml`: `kv`, 6
replicas, every placement setting, for a dedicated node pool named `kvstore`
tainted `dedicated=kvstore:NoSchedule`. It is not in the samples'
`kustomization.yaml`, since it is the same KVCluster as the kind sample.

- **Image:** push `kvnode` (and the operator image) to Artifact Registry and
  put the path in `spec.image` (and in place of `controller:latest`, as in
  [Install](#install)).
- **Storage:** `storageClassName: standard-rwo`, the balanced persistent
  disk class GKE installs with the Compute Engine persistent disk CSI driver.
  It is the default class on Autopilot clusters, but "for Standard clusters,
  the default StorageClass uses the Kubernetes in-tree gcePersistentDisk
  volume plugin"
  ([persistent disk CSI driver](https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/gce-pd-csi-driver)),
  so it is named rather than left to the default. It binds
  `WaitForFirstConsumer`, so each disk is created in the zone its pod was
  scheduled to, which zone spread needs. `size: 10Gi` is the smallest
  balanced disk. Storage cannot change after creation.
- **The node pool**, in a regional Standard cluster (2 nodes per zone, so 6,
  and up to 3 per zone for scale-ups):

  ```sh
  gcloud container node-pools create kvstore \
    --cluster CLUSTER --region REGION \
    --num-nodes 2 \
    --node-taints dedicated=kvstore:NoSchedule \
    --enable-autoscaling --min-nodes 2 --max-nodes 3
  ```

  GKE labels every node of the pool `cloud.google.com/gke-nodepool:
  kvstore`, the sample's node selector. The taint keeps other workloads off
  it; the operator itself runs in another pool.

```sh
kubectl create namespace kvstore
kubectl -n kvstore apply -f operator/config/samples/kvstore_v1alpha1_kvcluster_gke.yaml
```

## Status

```sh
kubectl -n kvstore get kvc kv -o yaml
```

| Field | Meaning |
|---|---|
| `phase` | One of the five phases below. |
| `epoch`, `members` | The membership the pods hold: the highest epoch any pod reports, and its members in ordinal order. |
| `replicas`, `readyReplicas` | The StatefulSet's, as this reconcile saw them (before acting). |
| `selector` | `app.kubernetes.io/name=kvstore`, for the scale subresource. |
| `drainStartedAt` | While a node drains: when it was taken out of the membership. `drainTimeout` counts from here. |
| `observedGeneration` | The spec generation the status describes. |
| `conditions` | Below. |

### Phases

| Phase | Meaning |
|---|---|
| `Pending` | Being created: the ConfigMap, the StatefulSet, the pods starting. |
| `Ready` | Every pod is ready and answering, all at one epoch with the ConfigMap's membership, every handoff done, the StatefulSet rolled out, and replicas as in the spec. |
| `Scaling` | A membership change, or an image, resources or placement rollout, is in progress. |
| `DrainBlocked` | A node being removed has not drained within `drainTimeout`. The operator keeps waiting; it never removes a node that has not drained. |
| `Degraded` | The cluster is in a state the operator's own steps cannot produce, or a pod has been unavailable for longer than the 2-minute grace period. The operator does nothing until that changes. |

While a pod restarts in an otherwise stable cluster, the phase stays what it
was (with reason `WaitingForPods`) until the grace period runs out.

### Conditions

The first four conditions follow the phase, and each carries the same
`reason` and `message`: the situation, in the operator's words (for example
`Removing kv-6: draining at epoch 4: 312 keys still to hand off`).
`ActionFailed` is separate, with reasons of its own.

| Condition | True when the phase is |
|---|---|
| `Available` | `Ready`, `Scaling` or `DrainBlocked` (the cluster serves) |
| `Progressing` | `Pending` or `Scaling` |
| `DrainBlocked` | `DrainBlocked` |
| `Degraded` | `Degraded` |

| Condition | Status, reason and message |
|---|---|
| `ActionFailed` | `True`, reason `ApplyFailed`, when the last reconcile could not carry out what it attempted: an apply the API server rejected (a Service, the PodDisruptionBudget, the ConfigMap, the StatefulSet) or a POST to a node that failed (refused, or not answered). The message names the action and the error, for example `UpdateTemplate(6): apply StatefulSet kv: … tolerations …`. `False`, reason `ActionSucceeded`, once a reconcile carries out everything it attempts. A node's 409 is not a failure: the node has moved on, and the next reconcile sees where. |

The phase says where the cluster is; `ActionFailed` says the operator could
not move it. The operator retries a failed action (with backoff), and the
condition stays `True` while it keeps failing. When a Service or the
PodDisruptionBudget fails to apply, the reconcile stops before the planner
runs, and the rest of the status is left as last written.

| Reason | Situation |
|---|---|
| `CreatingConfigMap`, `CreatingStatefulSet` | Creation. |
| `AddingPod` | Scale-up: the ConfigMap at the next epoch (U1) or the StatefulSet (U2) written. |
| `WaitingForEpoch` | U3: the new pod announced the next epoch; the others are adopting it. |
| `WaitingForHandoff` | U4/D5: a member is still moving data to its new owners. |
| `RemovingPod` | D1: the membership without the node POSTed; or D4: the StatefulSet scaled down. |
| `Draining` | D2: waiting for the node being removed to drain. |
| `DrainBlocked` | D2 past `drainTimeout`. |
| `Drained` | D3: the ConfigMap written without the drained node. |
| `WaitingForPodRemoval`, `WaitingForVolumeRemoval` | D5: the removed pod and its volume are not gone yet. |
| `WaitingForOldVolume` | A scale-up waits for an earlier incarnation's pod or volume at that ordinal. |
| `WaitingForPods` | A pod is not ready or not answering (within the grace period). |
| `RollingUpdate` | The spec's image, resources or placement are being applied, or rolled out. |
| `Ready` | Stable at the spec. |
| `AddressMismatch`, `MembershipConflict`, `UnexplainedDivergence`, `ConfigMapMissing`, `ConfigMapUnexpected`, `PodUnavailable` | Degraded: see [Degraded](#degraded). |

## How a membership change is carried out

Each reconcile observes the cluster from scratch — every pod's
`GET /admin/membership` (8 at a time, each with a 5-second deadline), the
StatefulSet, the ConfigMap, the PVCs, the KVCluster — and a pure function
(`operator/internal/planner`) decides the one thing to do next. The
reconcile does it, writes the status, and requeues: 2s while a step should
finish within seconds, 10s while waiting on handoffs and drains, 1 minute
when Ready. Nothing is remembered between reconciles except the status, and
how long each pod has not answered; which step comes next is read off the
cluster itself, so an operator that restarts between any two steps takes the
next one.

The planner places the cluster by comparing the ConfigMap's member count
with the StatefulSet's replicas, and each pod's epoch and members with the
ConfigMap's epoch.

### Scale up: add pod k (base epoch E)

| Step | The operator | Until | Reason |
|---|---|---|---|
| U1 | Writes the ConfigMap at E+1 with members 0..k, from the members the pods returned plus pod k's address. Only once the cluster is stable and no pod or volume from an earlier pod k exists. | | `AddingPod` |
| U2 | Sets the StatefulSet to k+1 replicas, keeping the running pod template. | every existing pod is ready | `AddingPod` |
| U3 | Waits. Pod k starts at E+1 from the ConfigMap and announces it; the others adopt it. | every pod holds E+1 | `WaitingForEpoch` |
| U4 | Waits. | every member's handoff to E+1 is done | `WaitingForHandoff` |

### Scale down: remove pod k (base epoch E)

| Step | The operator | Until | Reason |
|---|---|---|---|
| D1 | POSTs `{E+1, members without k}` to pod 0, built from the members pod 0 returned. Only once the cluster is stable. | | `RemovingPod` |
| D2 | Waits; asks pod k (and only a pod outside its own membership) `GET /admin/handoff?epoch=E+1&timeout=1s`. | pod k has drained: its data is pushed and every remaining member holds E+1 | `Draining`, then `DrainBlocked` after `drainTimeout` |
| D3 | Writes the ConfigMap at E+1 without k. | | `Drained` |
| D4 | Sets the StatefulSet to k replicas. Its claim on pod k's volume is deleted (`whenScaled: Delete`). | | `RemovingPod` |
| D5 | Waits. | pod k and `data-kv-k` are gone, and every member's handoff to E+1 is done | `WaitingForPodRemoval`, `WaitingForVolumeRemoval`, `WaitingForHandoff` |

### One change at a time

- A spec change in the middle of a step does not interrupt it: the step in
  progress finishes, and the next change starts from a stable cluster.
- An image, resources or placement change is applied only from a stable
  cluster, and before any change of replicas; a scale keeps the pod template
  the pods run.
- Every POST and every ConfigMap the operator writes carries the member
  strings the pods returned, never ones rebuilt from the spec.

## Invariants

These hold at every point of every change, and are checked after every tick
of a simulator that runs 2000 seeded sequences of scale-ups and scale-downs
with random propagation delays, operator crashes (before acting, and after
acting with the status write lost), slow drains, pod crashes and spec changes
in the middle of a change (`operator/internal/planner/sim_test.go`):

1. **At most one membership change is in flight**: every epoch any node
   holds is within one of every other, and at most two memberships exist.
2. **The StatefulSet is never scaled below a member that has not drained.**
3. **No pod is ever started on an earlier pod's volume** at its ordinal (a
   stale incarnation; see "Never restore a node's data dir" in
   [known-limitations.md](known-limitations.md)).
4. **The ConfigMap is never behind the cluster, except during a removal,
   between D1 and D3**: there it is at most one epoch behind, and it still
   lists the node being removed. This follows the documented order of
   [kubernetes.md](kubernetes.md#scale-down-remove-kv-10) (POST, drain, then
   the ConfigMap). Writing the ConfigMap first would make a node that
   restarts while it drains refuse to start, with data not yet handed off.
   Either way, **no pod ever refuses to start** because the ConfigMap holds a
   different membership at its epoch.
5. **No POST is at an epoch at or below one the cluster already holds.**
6. **No action while Degraded.**
7. **A pod template change (image, resources, placement) is applied only while stable.**

## Degraded

The operator acts only in states its own steps produce. Anything else is
Degraded, and it waits for a person:

| Reason | What it means | What to do |
|---|---|---|
| `AddressMismatch` | A pod holds a member that is not one of this cluster's pods at its exact address. | Someone POSTed a membership by hand. Find the right one, POST it at a higher epoch, and bring the ConfigMap to it. |
| `MembershipConflict` | Two pods hold different memberships at one epoch. | The nodes log `MEMBERSHIP CONFLICT`; POST the intended membership at a higher epoch ([membership.md](membership.md)). |
| `UnexplainedDivergence` | The ConfigMap, the StatefulSet and the pods match no step (for example, epochs two apart, or a change made by hand). | Read every pod's `GET /admin/membership`, decide the membership, and bring the pods, the ConfigMap and the StatefulSet to it. |
| `ConfigMapMissing` | The ConfigMap is gone while the StatefulSet exists. | Recreate it from a pod's `GET /admin/membership` (epoch and members). |
| `ConfigMapUnexpected` | The ConfigMap does not parse, or holds members the operator does not write. | As above. |
| `PodUnavailable` | A pod that should serve has been unready, or ready but not answering on 8080, for more than 2 minutes. | Find out why (`kubectl describe pod`, its log). When it is back the operator resumes. |

## Manual recovery

### A drain that does not finish (DrainBlocked)

The message says what is left: the keys still to hand off, and the members
not yet seen at the new epoch. The removed node must push everything it holds
and see every remaining member at the new epoch before it counts as drained.
The usual cause is a remaining member that is down or unreachable.

1. Read the details on the node being removed:
   ```sh
   kubectl -n kvstore port-forward pod/kv-6 8080:8080
   curl -s 'localhost:8080/admin/handoff?epoch=E&timeout=1s'
   ```
   `handoff.pending` is what is left to push; `peers` shows which members
   have not reported epoch E (`lastSeenEpoch`).
2. Bring the lagging member back (fix the pod, or the network). The drain
   resumes on its own, and the operator carries on from D2 when it completes:
   nothing else is needed.

Do not delete the pod or scale the StatefulSet to make it go: its data has
not all reached its new owners. Setting `spec.replicas` back does not cancel
the removal either: the operator finishes the step in progress. To keep the
node after all, put it back in the membership by hand: set `spec.replicas`
back first (so the operator does not start the removal again), POST the
membership with the node at the next epoch to pod 0, and bring the ConfigMap
to that epoch and member list. The operator then sees a stable cluster. This
cancellation has not been exercised in the tests.

### A scale-up that cannot start

If the new pod never becomes ready (the image cannot be pulled, it cannot be
scheduled), the cluster waits in `WaitingForEpoch` and turns Degraded with
`PodUnavailable` after 2 minutes. The operator does not roll back on its own.

- **Fix the cause** if you can: once the pod starts, it announces the new
  epoch and the scale-up completes.
- **Roll back** otherwise. First check on every pod
  (`GET /admin/membership`) that none holds the new epoch E+1. If none does,
  set `spec.replicas` back, scale the StatefulSet back to the old count by
  hand (`kubectl -n kvstore scale sts/kv --replicas=6`: the new pod never
  joined, so this deletes nothing anyone holds), delete its volume if one was
  created, and bring the ConfigMap back to epoch E without the new member.
  If some pod already holds E+1 (a pod that restarted read it from the
  ConfigMap), the new member is in the membership: remove it properly instead
  (POST `{E+2, members without it}` to pod 0, ConfigMap at E+2, StatefulSet
  back), as in [membership.md](membership.md).

Neither rollback has been exercised in the tests.

## Tests

Run the module's tests in the `golang:1.26` container (envtest does not build
on Windows with controller-runtime v0.25.0-v0.25.2), from the repository root
(the tests read `deploy/k8s/` and the admin API fixtures):

```powershell
docker run --rm -v "${PWD}:/src" -v kv-gomod:/go/pkg/mod -w /src/operator golang:1.26 make test
```

In Git Bash, prefix it with `MSYS_NO_PATHCONV=1`, or Git Bash rewrites
`/src/operator` into a Windows path.

| Package | What it covers |
|---|---|
| `api/v1alpha1` | Every default and validation rule (CEL) in a real kube-apiserver, and both samples. |
| `internal/render` | The owned objects against `deploy/k8s/` field by field, allowing only a declared list of differences; another name and namespace; the ConfigMap rendered from the membership passed in; every placement rule and the PodDisruptionBudget. |
| `internal/kvadmin` | The admin API client against the JSON contract fixtures in `internal/app/testdata/admin` (which the database module checks against a real node); deadlines; unknown fields. |
| `internal/planner` | Every step in table tests; the operator crashing after each step of each scale path; the simulator above. |
| `internal/controller` | envtest, as a service account bound to the generated RBAC role, against fake nodes with the admin API's semantics: a full 3 → 4 → 3, drain blocked, a pod unreachable past the grace period, a hand-scaled StatefulSet, an image rollout, placement held back until the cluster is stable, the PodDisruptionBudget, failed applies and POSTs in `ActionFailed`, and the watches. |

The end-to-end test runs on the kind cluster from the host
(`operator/test/e2e`, build tag `e2e`):

```sh
cd operator
go test -tags e2e ./test/e2e/ -v -count=1 -timeout 60m
```

It builds and loads both images, installs the operator, creates `kv` in
`kvstore` with 6 replicas, preloads 4000 keys (4 writers), runs the loadgen
writer (about 11 writes/s) throughout, scales to 7 and back to 6, does it
again with the operator pod deleted while kv-6 drains (and checks the epoch
moved exactly once for it), and reads back every acknowledged write. It
leaves `kv` running.

### Results

Two runs on Docker Desktop (WSL2), one kind node. Timings vary by up to half
from run to run on this setup, in either direction (run 2 created the
cluster more slowly but moved data faster throughout), so read them as
orders of magnitude:

| Phase | Run 1 | Run 2 |
|---|---|---|
| Create → Ready, 6 replicas | 14.2s | 27.8s |
| Preload 4000 keys | 53.1s | 31.4s |
| Scale 6 → 7 | 70.5s | 49.2s |
| Scale 7 → 6 | 64.0s | 41.4s |
| Scale 6 → 7 again | 91.2s | 60.3s |
| Scale 7 → 6 with the operator pod deleted in D2 | 76.8s | 40.0s |
| … of which the new operator pod becoming ready | 11.7s | 12.4s |
| Writes acknowledged / failed / read back | 7091 / 0 / 7091 | 6310 / 0 / 6310 |

- **The handoffs take the time; the operator's own steps take under a
  second.** In run 1's first scale-up, the StatefulSet was at 7 replicas
  within half a second of the scale and every pod held the new epoch 5.5s
later; the
  remaining 64s were the members moving data (`WaitingForHandoff`). In its
  first scale-down, kv-6 was draining 3s after the scale and drained 52s
  later; the ConfigMap and StatefulSet steps after it were done within one
  0.25s poll of the test. Handoff time grows with the data that moves (see
  [kubernetes.md](kubernetes.md#scale-up-add-kv-10)).
- **The operator restart cost about 12s** (the new pod becoming ready); the
  new leader then resumed the drain where it was, with no extra membership
  change.
- **Node usage** (`kubectl top node`): 257-508m CPU and 1.5-1.7 GiB with 6
  replicas idle; 2.0-2.5 cores right after the preload; 0.75-0.9 cores at 7
  replicas under the steady load; 1.9-2.0 GiB throughout the scaling.

### What scaling costs the clients

From the dashboard's queries over both runs (Prometheus, 1-minute windows,
Put latency across the cluster and the data WAL's fsync latency):

| Load | Put p99, run 1 | Put p99, run 2 | fsync p99, run 1 | fsync p99, run 2 |
|---|---|---|---|---|
| Steady writer (~11 writes/s), no handoff | 25-40 ms | 18-27 ms | 15-20 ms | 10-14 ms |
| Preload (~70 writes/s), no handoff | 99-347 ms | 40-43 ms | 27-61 ms | 14 ms |
| Steady writer during a membership handoff | 63-245 ms | 47-175 ms | 23-36 ms | 14-15 ms |

The Put p50 stayed between 12 and 35 ms throughout. No write failed and no
quorum failed, but while data moves the tail latency rises several-fold over
the steady load's.

The cause is not established:

- **Not slower fsyncs, within a run.** In run 2 the fsync p99 stayed at
  14-15 ms through every handoff while the Put p99 reached 175 ms.
- **More fsyncs.** Across the cluster there were 100-316 fsyncs a second
  during handoffs, against 15-50 at the steady load: every key pushed to a
  new owner is written there. The writes compete with that work for the
  node's disk and CPU; which of the two a Put waits on has not been measured.
- **Disk speed matters between runs.** Run 1's fsyncs were slower everywhere
  (Docker Desktop's virtual disk varies), and its latencies were higher
  everywhere, even in the preload with no handoff running: its highest p99,
  347 ms, came at the very start of the preload.

**A hypothesis, untested**, that fits these observations: client Puts queue
behind handoff writes for each node's write lock. Each write to a node's
storage engine (`StorageEngine.Put`; a handoff push arrives as an ordinary
replicated write, one `Put` per version) holds the engine's write lock
(`writeMu`) across its WAL append, and every append fsyncs on its own, with
no group commit (`internal/storage/wal/wal.go`). So a node's fsyncs run one at a time, and a
client write that arrives during a handoff waits for the pushes ahead of it.
That would raise the Put p99 while the time of each fsync stays flat and the
number of fsyncs rises several-fold, which is what run 2 showed. The fsync
histogram cannot show it: it times the `fsync` call alone, after the locks are
held, not the wait for them. Timing the lock wait would confirm or refute
it: the storage timings decorator around `store.Persister` deferred in
[observability.md](observability.md#future-work) (which would time storage
as the node sees it, lock waits included), or a histogram of the time spent
waiting for `writeMu`.

## Limits

- One KVCluster per namespace.
- The admin API it drives is unauthenticated (see
  [known-limitations.md](known-limitations.md)); anything that can reach a
  pod's port 8080 can change the membership.
- No automatic cancel of a blocked drain and no automatic rollback of a
  scale-up: both are manual, above.
- Only the highest ordinal can be removed (a StatefulSet's limit). A broken
  pod in the middle is replaced in place ([kubernetes.md](kubernetes.md#losing-a-pod)).
- PVCs are not watched; the operator polls (every 2s) while it waits for one
  to be deleted.
- `replication` and `storage` cannot change after creation.
