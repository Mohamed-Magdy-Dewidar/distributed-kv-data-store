#!/usr/bin/env bash
# Drives the failure tests of docs/chaos.md with kvchaos (cmd/kvchaos), on
# the kind cluster kvstore-chaos (the rehearsal) or on GKE (session 3,
# docs/gke.md). Git Bash on Windows, any bash on Linux. Needs kubectl and
# go; kind-up also needs kind and docker.
#
#   chaos.sh kind-up             create kvstore-chaos: Cilium, images, the
#                                operator and CoreDNS on the control plane,
#                                the KVCluster of kvcluster-kind.yaml
#   chaos.sh kind-down           delete kvstore-chaos
#   chaos.sh cilium              (re)apply Cilium on kvstore-chaos
#   chaos.sh topology            capture the topology, predict every fault
#   chaos.sh predict FAULT       one fault's prediction, key by key
#   chaos.sh preload             write the PRELOAD_KEYS keys reads use
#   chaos.sh run NAME [ARGS]     start recorder chaos-NAME: test NAME,
#                                writes and reads at the default rates
#   chaos.sh start NAME ARGS     start recorder chaos-NAME with these
#                                kvchaos record flags only
#   chaos.sh stop NAME           end chaos-NAME's run and fetch its records
#   chaos.sh readback NAME       read back chaos-NAME's writes, with copies
#   chaos.sh converge            read back every run's writes (the session's
#                                end, after a full anti-entropy interval)
#   chaos.sh delete NAME         delete pod chaos-NAME
#   chaos.sh mark TEST EVENT [ARG]
#                                append to faults.jsonl: inject FAULT, heal,
#                                or any other EVENT as a mark of that name
#   chaos.sh wait-node-down TEST NODE
#                                wait until NODE is gone or not Ready, mark it
#   chaos.sh wait-node TEST NODE wait until NODE is Ready again, mark
#                                node-ready-NODE
#   chaos.sh wait-volume TEST POD
#                                wait until POD's volume is attached again,
#                                mark volume-attached-POD
#   chaos.sh wait-vm TEST NODE ZONE
#                                (GKE) wait until NODE's VM is recreated and
#                                running, mark vm-recreated-NODE
#   chaos.sh vm-loss TEST POD... (GKE) tests 1 and 2: recreate the pods' VMs
#                                through their instance groups; inject, then
#                                every recovery step's mark, then heal
#   chaos.sh system-loss TEST    (GKE) test 4: recreate the system node's VM;
#                                inject none, every recovery step, heal
#   chaos.sh wait-ready TEST POD...
#                                wait until the pods answer, marking each
#                                serving-POD, then serving; until they are
#                                Ready, marking each pod-ready-POD; heal
#   chaos.sh wait-hints TEST     wait until every hint is delivered, mark
#                                hints-delivered (before any read-back)
#   chaos.sh hints-snapshot TEST WHEN
#                                append every member's hint counters to hints.txt
#   chaos.sh partition TEST POD... cut the pods off from the other members
#                                (clients still reach both sides); inject
#   chaos.sh unpartition TEST    delete the NetworkPolicies; heal
#   chaos.sh unlabel             remove the partition labels (between tests)
#   chaos.sh peers               each member's epoch and the peers it sees
#   chaos.sh watch-start|watch-stop
#                                log the KVCluster's status changes to
#                                operator.log (a background loop; stop it)
#   chaos.sh check [FLAGS]       kvchaos check over every run so far
#   chaos.sh check-final [FLAGS] the same, against the converge read-back
#
# Parameters (environment variables, defaults in brackets):
#   CHAOS_CONTEXT   kubectl context: kind-kvstore-chaos or gke_*_kvstore
#                   (required; every kubectl call names it)
#   CHAOS_OUT       results directory [docs/results/chaos/<UTC date>-<kind|gke>]
#   KV_IMAGE        on GKE: the node image (it has /kvchaos)
#   PRELOAD_KEYS    keys the preload writes and reads choose from [5000]
#   WRITE_RATE, READ_RATE
#                   run's writes and reads per second [50, 50 on kind;
#                   40, 100 on GKE]
#   CONCURRENCY     a run's requests in flight at most [64 on kind, 256 on
#                   GKE]: requests to a pod that vanished hang until their
#                   timeout (5s) for the ~13s keepalive takes to notice, and
#                   at 64 they took every worker on kind (t1s, t2: drops)
set -euo pipefail
export MSYS_NO_PATHCONV=1

# gcloud without MSYS_NO_PATHCONV: under it, the Cloud SDK's launcher hands
# Python a POSIX path Git Bash no longer converts, and gcloud cannot start
# ("can't open file 'D:/c/Users/.../gcloud.py'": GKE session 3).
gcloud() { env -u MSYS_NO_PATHCONV gcloud "$@"; }

NS=kvstore
KIND_NAME=kvstore-chaos
CILIUM_VERSION=1.19.8
HELM_IMAGE=alpine/helm:3.16.2
POLICY=chaos-partition

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"

die() { echo "chaos.sh: $*" >&2; exit 1; }
now() { date -u +%Y-%m-%dT%H:%M:%S.%3NZ; }

CTX=${CHAOS_CONTEXT:-}
[ -n "$CTX" ] || die "set CHAOS_CONTEXT (kind-$KIND_NAME, or the GKE context)"
case "$CTX" in
kind-$KIND_NAME) PLATFORM=kind ;;
gke_*_kvstore) PLATFORM=gke ;;
*) die "refusing context $CTX: want kind-$KIND_NAME or gke_*_kvstore" ;;
esac
k() { kubectl --context "$CTX" "$@"; }

OUT=${CHAOS_OUT:-docs/results/chaos/$(date -u +%Y%m%d)-$PLATFORM}
PRELOAD_KEYS=${PRELOAD_KEYS:-5000}
if [ "$PLATFORM" = kind ]; then
	IMAGE=kvnode:dev
	WRITE_RATE=${WRITE_RATE:-50}
	CONCURRENCY=${CONCURRENCY:-64}
	READ_RATE=${READ_RATE:-50}
	# The control plane: GKE's system node in this rehearsal.
	PLACEMENT='nodeSelector: {node-role.kubernetes.io/control-plane: ""}
  tolerations: [{key: node-role.kubernetes.io/control-plane, operator: Exists, effect: NoSchedule}]'
else
	IMAGE=${KV_IMAGE:-}
	# 40 writes/s: a session's runs (about 45 minutes of writes in all) then
	# store about 110,000 keys, within what the bench KVCluster's 512Mi is
	# sized for (deploy/bench/run.sh, KEY_CAP 130,000).
	WRITE_RATE=${WRITE_RATE:-40}
	CONCURRENCY=${CONCURRENCY:-256}
	READ_RATE=${READ_RATE:-100}
	# The loadgen pool: neither a database node nor the system node.
	PLACEMENT='nodeSelector: {cloud.google.com/gke-nodepool: loadgen}
  tolerations: [{key: dedicated, operator: Equal, value: loadgen, effect: NoSchedule}]'
fi

# kvchaos on this machine, for topology, predict and check.
BIN_DIR=${TMPDIR:-/tmp}/kvchaos-bin
# A path go (a Windows program under Git Bash) and bash both understand.
if command -v cygpath >/dev/null; then BIN_DIR=$(cygpath -m "$BIN_DIR"); fi
BIN=$BIN_DIR/kvchaos
kvchaos() {
	mkdir -p "$BIN_DIR"
	go build -o "$BIN" ./cmd/kvchaos
	"$BIN" "$@"
}

usage() {
	sed -n '2,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'
	exit 2
}

kind_up() {
	[ "$PLATFORM" = kind ] || die "kind-up is for kind-$KIND_NAME"
	! kind get clusters 2>/dev/null | grep -qx "$KIND_NAME" || die "$KIND_NAME exists; kind-down first"
	local node_image
	node_image=$(sed -n 's/^KIND_NODE_IMAGE ?= //p' Makefile)
	# The images first: building them while four kind nodes start needs more
	# memory than a 16 GB laptop has to spare.
	docker build -t kvnode:dev .
	docker build -t kvstore-operator:dev operator
	kind create cluster --config deploy/chaos/kind-config.yaml --image "$node_image"
	cilium
	k wait nodes --all --for=condition=Ready --timeout=300s

	kind load docker-image kvnode:dev kvstore-operator:dev --name "$KIND_NAME"
	kubectl kustomize operator/config/default | sed 's|image: controller:latest|image: kvstore-operator:dev|' |
		k apply --server-side -f - >/dev/null
	local patch='{"spec":{"template":{"spec":{"nodeSelector":{"node-role.kubernetes.io/control-plane":""},
		"tolerations":[{"key":"node-role.kubernetes.io/control-plane","operator":"Exists","effect":"NoSchedule"}]}}}}'
	k -n kvstore-operator-system patch deploy kvstore-operator-controller-manager --type merge -p "$patch"
	k -n kube-system patch deploy coredns --type merge -p "$patch"
	k -n kvstore-operator-system rollout status deploy/kvstore-operator-controller-manager --timeout=180s
	k -n kube-system rollout status deploy/coredns --timeout=180s
	k create namespace "$NS" --dry-run=client -o yaml | k apply -f -
	k apply -f deploy/chaos/kvcluster-kind.yaml
	wait_phase Ready 600
	k -n "$NS" get pods -o wide
}

# cilium installs (or updates) Cilium. Native routing, not the default VXLAN
# tunnel: through the tunnel, Docker Desktop's MTU stalls TLS between nodes
# (a worker's pods could not reach the API server), and GKE's Dataplane V2
# routes natively too. The kind nodes share one Docker network, so each
# node's pod range is a direct route. Piped, not through a file: with
# MSYS_NO_PATHCONV, a Windows kubectl cannot open a Git Bash path.
cilium() {
	docker run --rm --entrypoint sh "$HELM_IMAGE" -c "helm repo add cilium https://helm.cilium.io >/dev/null &&
		helm template cilium cilium/cilium --version $CILIUM_VERSION --namespace kube-system \
			--set ipam.mode=kubernetes --set image.pullPolicy=IfNotPresent --set operator.replicas=1 \
			--set routingMode=native --set autoDirectNodeRoutes=true --set ipv4NativeRoutingCIDR=10.244.0.0/16" |
		k apply -f - >/dev/null
	k -n kube-system rollout restart ds/cilium
	k -n kube-system rollout status ds/cilium --timeout=600s
}

wait_phase() {
	local want=$1 secs=$2 phase=""
	for _ in $(seq 1 $((secs / 2))); do
		phase=$(k -n "$NS" get kvc kv -o jsonpath='{.status.phase}' 2>/dev/null || true)
		[ "$phase" = "$want" ] && return 0
		sleep 2
	done
	die "KVCluster kv is $phase after ${secs}s, not $want"
}

members() { k -n "$NS" get pods -l app.kubernetes.io/name=kvstore -o jsonpath='{.items[*].metadata.name}'; }

membership() { k get --raw "/api/v1/namespaces/$NS/pods/$1:8080/proxy/admin/membership"; }

topology() {
	local d=$OUT/topology
	mkdir -p "$d/membership"
	k -n "$NS" get pods -l app.kubernetes.io/name=kvstore -o json >"$d/pods.json"
	k get nodes -o json >"$d/nodes.json"
	k -n "$NS" get configmap kv-config -o json >"$d/configmap.json"
	for id in $(members); do
		membership "$id" >"$d/membership/$id.json"
	done
	kvchaos topology -pods "$d/pods.json" -nodes "$d/nodes.json" -configmap "$d/configmap.json" \
		-membership-dir "$d/membership" -out "$OUT/topology.json"
	kvchaos predict -topology "$OUT/topology.json" -keys "chaos-pre:$PRELOAD_KEYS" | tee "$OUT/predictions.txt"
}

predict_one() {
	local f=$1
	kvchaos predict -topology "$OUT/topology.json" -keys "chaos-pre:$PRELOAD_KEYS" -fault "$f" \
		-per-key "$OUT/predict-${f//[:,]/_}.jsonl"
}

# start NAME FLAGS...: a pod chaos-NAME running kvchaos record with FLAGS,
# its output in /out; it holds after the run, so the output can be fetched.
start() {
	local name=$1 pod=chaos-$1
	shift
	[ -n "$IMAGE" ] || die "set KV_IMAGE: the node image, by digest (docs/gke.md, step 3)"
	local args="[record, -out, /out/records.jsonl, -stop-file, /out/STOP, -hold"
	for a in "$@"; do args="$args, \"$a\""; done
	args="$args]"
	k -n "$NS" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $pod
  namespace: $NS
  labels: {app.kubernetes.io/name: kvchaos, kvchaos/run: "$name"}
spec:
  restartPolicy: Never
  terminationGracePeriodSeconds: 5
  $PLACEMENT
  containers:
    - name: kvchaos
      image: $IMAGE
      imagePullPolicy: IfNotPresent
      command: [/kvchaos]
      args: $args
      resources: {requests: {cpu: 250m, memory: 64Mi}, limits: {memory: 512Mi}}
      volumeMounts: [{name: out, mountPath: /out}]
  volumes: [{name: out, emptyDir: {}}]
EOF
	k -n "$NS" wait "pod/$pod" --for=condition=Ready --timeout=120s >/dev/null
	wait_log "$pod" 'connections ready' 90
	k -n "$NS" logs "$pod" | tail -1
}

wait_log() {
	local pod=$1 pattern=$2 secs=$3
	for _ in $(seq 1 "$secs"); do
		k -n "$NS" logs "$pod" 2>/dev/null | grep -q "$pattern" && return 0
		[ "$(k -n "$NS" get pod "$pod" -o jsonpath='{.status.phase}')" = Running ] ||
			die "$pod is not running: $(k -n "$NS" logs "$pod" | tail -3)"
		sleep 1
	done
	die "$pod: no '$pattern' in its log after ${secs}s"
}

run() {
	local name=$1
	shift
	# A prefix of this run's own: a rerun must not write the keys (and the
	# values) an earlier run did, or that run's copies would hide a lost write.
	start "$name" -test "$name" -write-prefix "chaos-$name-$(date -u +%Y%m%d%H%M%S)" \
		-write-rate "$WRITE_RATE" -read-rate "$READ_RATE" -concurrency "$CONCURRENCY" -read-prefix chaos-pre -read-keys "$PRELOAD_KEYS" "$@"
}

# fetch_file POD FILE DEST LINES: copy FILE out of POD to DEST, which must
# then have LINES lines; up to three tries. A long kubectl exec stream can
# end early, sometimes with exit status 0 (GKE session 3: five times, once
# silently), so the count is the only test of a complete copy.
fetch_file() {
	local pod=$1 file=$2 dest=$3 want=$4 got=0 try
	for try in 1 2 3; do
		k -n "$NS" exec "$pod" -- /kvchaos cat "$file" >"$dest" || true
		got=$(wc -l <"$dest")
		[ "$got" -eq "$want" ] && return 0
		echo "chaos.sh: $pod:$file: $got of $want lines (try $try)" >&2
	done
	die "$pod:$file: $got of $want lines after 3 tries"
}

# puts FILE...: the Puts in these records, each a key a read-back reads.
puts() { cat "$@" | grep -c '"op":"put"' || true; }

fetch() {
	local name=$1 pod=chaos-$1 want
	mkdir -p "$OUT/$name"
	k -n "$NS" logs "$pod" >"$OUT/$name/recorder.log"
	# Every request the recorder made, from its last line ("kvchaos record
	# t2: 8m14s, get failed=6094, get ok=43306, ...").
	want=$(grep 'kvchaos record [^:]*: [0-9]' "$OUT/$name/recorder.log" | tail -1 | grep -oE '=[0-9]+' | tr -d = | awk '{s += $1} END {print s + 0}')
	fetch_file "$pod" /out/records.jsonl "$OUT/$name/records.jsonl" "$want"
}

stop() {
	local name=$1 pod=chaos-$1
	k -n "$NS" exec "$pod" -- /kvchaos stop /out/STOP
	wait_log "$pod" "kvchaos record [^:]*: [0-9]" 60
	fetch "$name"
	tail -1 "$OUT/$name/recorder.log"
}

preload() {
	start pre -test pre -write-prefix chaos-pre -writes "$PRELOAD_KEYS" -write-rate 200
	wait_log chaos-pre "kvchaos record [^:]*: [0-9]" 600
	fetch pre
	local ok
	ok=$(grep -c '"result":"ok"' "$OUT/pre/records.jsonl" || true)
	[ "$ok" = "$PRELOAD_KEYS" ] || die "preload: $ok of $PRELOAD_KEYS writes acknowledged; not clean"
	echo "preload: $ok writes, all acknowledged"
}

readback() {
	local name=$1 pod=chaos-$1
	k -n "$NS" exec "$pod" -- /kvchaos readback -records /out/records.jsonl -copies -out /out/readback.jsonl
	# One line per key the run wrote.
	fetch_file "$pod" /out/readback.jsonl "$OUT/$name/readback.jsonl" "$(puts "$OUT/$name/records.jsonl")"
}

converge() {
	k -n "$NS" get pod chaos-tools >/dev/null 2>&1 || start_tools
	cat "$OUT"/*/records.jsonl | k -n "$NS" exec -i chaos-tools -- /kvchaos readback -records - -copies -out /out/readback.jsonl
	# If the stream of records in was cut short, the read-back is too, and
	# no retry of the copy out reaches the count: run converge again.
	fetch_file chaos-tools /out/readback.jsonl "$OUT/final-readback.jsonl" "$(puts "$OUT"/*/records.jsonl)"
}

start_tools() {
	[ -n "$IMAGE" ] || die "set KV_IMAGE: the node image, by digest (docs/gke.md, step 3)"
	k -n "$NS" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: chaos-tools
  namespace: $NS
  labels: {app.kubernetes.io/name: kvchaos}
spec:
  restartPolicy: Never
  terminationGracePeriodSeconds: 5
  $PLACEMENT
  containers:
    - name: kvchaos
      image: $IMAGE
      imagePullPolicy: IfNotPresent
      command: [/kvchaos, hold]
      volumeMounts: [{name: out, mountPath: /out}]
  volumes: [{name: out, emptyDir: {}}]
EOF
	k -n "$NS" wait pod/chaos-tools --for=condition=Ready --timeout=120s >/dev/null
}

mark() {
	local test=$1 event=$2 arg=${3:-} line
	mkdir -p "$OUT"
	case "$event" in
	inject) [ -n "$arg" ] || die "mark $test inject FAULT"
		line="{\"time\":\"$(now)\",\"test\":\"$test\",\"event\":\"inject\",\"fault\":\"$arg\"}" ;;
	heal) line="{\"time\":\"$(now)\",\"test\":\"$test\",\"event\":\"heal\"}" ;;
	*) line="{\"time\":\"$(now)\",\"test\":\"$test\",\"event\":\"mark\",\"mark\":\"$event\"}" ;;
	esac
	echo "$line" >>"$OUT/faults.jsonl"
	echo "$line"
}

# inject_time TEST: the time of TEST's last inject, to the second (the
# timestamps Kubernetes and gcloud give have none finer), as UTC "...Z": a
# step counts only if it happened since.
inject_time() {
	local s
	s=$(grep "\"test\":\"$1\",\"event\":\"inject\"" "$OUT/faults.jsonl" 2>/dev/null | tail -1 |
		sed -E 's/.*"time":"([^".]*)[^"]*".*/\1Z/')
	[ -n "$s" ] || die "$1 has no inject in $OUT/faults.jsonl"
	echo "$s"
}

# wait_node_down TEST NODE: wait until NODE is gone (its VM deleted) or not
# Ready; mark node-gone-NODE or node-notready-NODE.
wait_node_down() {
	local test=$1 node=$2 st
	while :; do
		if ! st=$(k get node "$node" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null); then
			mark "$test" "node-gone-$node"
			return
		fi
		if [ "$st" != True ]; then
			mark "$test" "node-notready-$node"
			return
		fi
		sleep 1
	done
}

# wait_node TEST NODE: wait until NODE is Ready again, having been seen gone
# or not Ready here, or Ready since TEST's inject (a VM recreated under its
# name comes back as a node of the same name); mark node-ready-NODE.
wait_node() {
	local test=$1 node=$2 since st seen=no
	since=$(inject_time "$test")
	while :; do
		if st=$(k get node "$node" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status} {.status.conditions[?(@.type=="Ready")].lastTransitionTime}' 2>/dev/null) &&
			[ "${st% *}" = True ]; then
			{ [ $seen = yes ] || [[ ! "${st#* }" < "$since" ]]; } && break
		else
			seen=yes
		fi
		sleep 2
	done
	mark "$test" "node-ready-$node"
}

# wait_volume TEST POD: wait until the attach/detach controller reports
# POD's volume attached (its SuccessfulAttachVolume event) since TEST's
# inject; mark volume-attached-POD. A zonal disk follows its pod: detached
# from the old VM, attached to the new one.
wait_volume() {
	local test=$1 pod=$2 since
	since=$(inject_time "$test")
	until k -n "$NS" get events --field-selector "involvedObject.name=$pod,reason=SuccessfulAttachVolume" \
		-o jsonpath='{range .items[*]}{.lastTimestamp}{.eventTime}{"\n"}{end}' 2>/dev/null |
		awk -v s="$since" 'substr($0, 1, 19) "Z" >= s {f = 1} END {exit !f}'; do
		sleep 2
	done
	mark "$test" "volume-attached-$pod"
}

# wait_vm TEST NODE ZONE (GKE): wait until NODE's VM is RUNNING, created
# since TEST's inject; mark vm-recreated-NODE.
wait_vm() {
	local test=$1 node=$2 zone=$3 since st
	since=$(inject_time "$test")
	until st=$(gcloud compute instances describe "$node" --zone "$zone" \
		--format='value(status,creationTimestamp.date("%Y-%m-%dT%H:%M:%SZ",tz=UTC))' 2>/dev/null) &&
		[ "${st%%[[:space:]]*}" = RUNNING ] && [[ ! "${st##*[[:space:]]}" < "$since" ]]; do
		sleep 5
	done
	mark "$test" "vm-recreated-$node"
}

# mig_of NODE ZONE (GKE): the managed instance group NODE's VM belongs to.
mig_of() {
	gcloud compute instances describe "$1" --zone "$2" --format=json |
		python -c "import json, sys; print([i['value'] for i in json.load(sys.stdin)['metadata']['items'] if i['key'] == 'created-by'][0].rsplit('/', 1)[1])"
}

# vm_loss TEST POD... (GKE): tests 1 and 2. Deletes the VMs that run the
# pods through their managed instance groups (recreate-instances: each VM is
# deleted and created again under its name, the group's size unchanged, so
# the autoscaler has nothing to restore), marks inject down:PODS, and waits
# through the recovery, marking each step (wait_node_down, wait_vm,
# wait_node, wait_volume, wait_ready). Run it in the foreground: it returns
# at the heal.
vm_loss() {
	[ "$PLATFORM" = gke ] || die "vm-loss is for GKE (on kind: docker kill or pause a worker, docs/chaos.md)"
	local test=$1 p node zone mig zm
	shift
	[ $# -gt 0 ] || die "vm-loss TEST POD..."
	local -A instances # "zone mig" -> node,node
	local -a nodes     # "node zone"
	for p in "$@"; do
		node=$(k -n "$NS" get pod "$p" -o jsonpath='{.spec.nodeName}')
		zone=$(k get node "$node" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}')
		mig=$(mig_of "$node" "$zone")
		[ -n "$node" ] && [ -n "$zone" ] && [ -n "$mig" ] || die "vm-loss: $p: no node, zone or instance group"
		instances["$zone $mig"]="${instances["$zone $mig"]:+${instances["$zone $mig"]},}$node"
		nodes+=("$node $zone")
		echo "$p on $node ($zone, $mig)"
	done
	mark "$test" inject "down:$(IFS=,; echo "$*")"
	for zm in "${!instances[@]}"; do
		gcloud compute instance-groups managed recreate-instances "${zm#* }" --zone "${zm% *}" \
			--instances "${instances[$zm]}" >/dev/null
	done
	for p in "${nodes[@]}"; do wait_node_down "$test" "${p% *}"; done
	for p in "${nodes[@]}"; do wait_vm "$test" "${p% *}" "${p#* }"; done
	for p in "${nodes[@]}"; do wait_node "$test" "${p% *}"; done
	for p in "$@"; do wait_volume "$test" "$p"; done
	wait_ready "$test" "$@"
}

# ready_replicas NAMESPACE DEPLOYMENT: its ready replicas, 0 if unknown.
ready_replicas() {
	local r
	r=$(k -n "$1" get deploy "$2" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
	echo "${r:-0}"
}

# system_loss TEST (GKE): test 4. Recreates the system pool's VM (the
# operator, Prometheus, Grafana and kube-dns), marks inject none, and polls
# the recovery, marking each step as it is seen: the node gone or not Ready,
# the VM recreated, the node Ready, operator-back and dns-back (each once
# seen without a ready replica), then heal. One loop for all of them: the
# operator and kube-dns come back in either order. Only kubectl get and
# gcloud: kubectl exec and the API server's proxy to pods may not work while
# the node is gone.
system_loss() {
	[ "$PLATFORM" = gke ] || die "system-loss is for GKE (on kind: scale CoreDNS and the operator to 0, docs/chaos.md)"
	local test=$1 node zone mig since st
	node=$(k get nodes -l cloud.google.com/gke-nodepool=system -o jsonpath='{.items[0].metadata.name}')
	zone=$(k get node "$node" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}')
	mig=$(mig_of "$node" "$zone")
	[ -n "$node" ] && [ -n "$zone" ] && [ -n "$mig" ] || die "system-loss: no system node, zone or instance group"
	echo "system node $node ($zone, $mig)"
	mark "$test" inject none
	since=$(inject_time "$test")
	gcloud compute instance-groups managed recreate-instances "$mig" --zone "$zone" --instances "$node" >/dev/null
	# Each step: no (not yet), down (seen down), done (marked back).
	local node_s=no vm_s=no op_s=no dns_s=no
	while [ "$node_s$vm_s$op_s$dns_s" != donedonedonedone ]; do
		if st=$(k get node "$node" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null); then
			if [ "$st" != True ] && [ $node_s = no ]; then
				mark "$test" "node-notready-$node"
				node_s=down
			elif [ "$st" = True ] && [ $node_s = down ]; then
				mark "$test" "node-ready-$node"
				node_s=done
			fi
		elif [ $node_s = no ]; then
			mark "$test" "node-gone-$node"
			node_s=down
		fi
		if [ $vm_s != done ] && st=$(gcloud compute instances describe "$node" --zone "$zone" \
			--format='value(status,creationTimestamp.date("%Y-%m-%dT%H:%M:%SZ",tz=UTC))' 2>/dev/null) &&
			[ "${st%%[[:space:]]*}" = RUNNING ] && [[ ! "${st##*[[:space:]]}" < "$since" ]]; then
			mark "$test" "vm-recreated-$node"
			vm_s=done
		fi
		case "$op_s:$(ready_replicas kvstore-operator-system kvstore-operator-controller-manager)" in
		no:0) op_s=down ;;
		down:[1-9]*) mark "$test" operator-back; op_s=done ;;
		esac
		case "$dns_s:$(ready_replicas kube-system kube-dns)" in
		no:0) dns_s=down ;;
		down:[1-9]*) mark "$test" dns-back; dns_s=done ;;
		esac
		sleep 2
	done
	mark "$test" heal
}

# wait_ready TEST POD...: each pod Ready since after TEST's inject (its Ready
# condition changed since then: a pod that has not gone down yet, or still
# shows its old Ready, does not count).
wait_ready() {
	local test=$1 p since st
	shift
	since=$(inject_time "$test")
	# First every pod answering its admin API: the other nodes see it alive
	# from then, well before the kubelet reports it Ready. Each pod gets its
	# own mark (serving-POD) as it answers, in whatever order they come back:
	# from then check takes it as no longer down (in GKE session 3's t2 one
	# pod answered 12s before the other). Then serving, for all of them.
	local waiting=" $* "
	while [ -n "${waiting// /}" ]; do
		for p in $waiting; do
			if membership "$p" >/dev/null 2>&1; then
				mark "$test" "serving-$p"
				waiting=${waiting/ $p / }
			fi
		done
		sleep 1
	done
	mark "$test" serving
	# Ready, and either seen not Ready here or changed since the inject. A
	# paused node's pods come back Ready with their old lastTransitionTime
	# (the kubelet still holds True), so the time alone does not tell.
	local seen
	for p in "$@"; do
		seen=no
		until st=$(k -n "$NS" get pod "$p" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status} {.status.conditions[?(@.type=="Ready")].lastTransitionTime}' 2>/dev/null) &&
			[ "${st% *}" = True ] && { [ $seen = yes ] || [[ ! "${st#* }" < "$since" ]]; }; do
			[ "${st% *}" = True ] || seen=yes
			sleep 1
		done
		mark "$test" "pod-ready-$p"
	done
	mark "$test" heal
}

# hints_left: the hints still pending, over every member, as of each one's
# last delivery round (kv_hints_pending; a round runs every 10s). Not created
# minus delivered: a hint delivered but not yet marked so is delivered again,
# and counted again (seen on kind: 98 more deliveries than hints).
hints_left() {
	local id
	for id in $(members); do
		k get --raw "/api/v1/namespaces/$NS/pods/$id:8080/proxy/metrics" | grep -E '^kv_hints_pending ' ||
			echo "kv_hints_pending 1" # no round yet: not known to be done
	done | awk '{s += $2} END {print s + 0}'
}

# hints_snapshot TEST WHEN: append each member's hint counters to hints.txt,
# as "time TEST WHEN pod created delivered". Snapshots before and after a
# test give its hints; a pod that restarted in between counts from 0.
hints_snapshot() {
	local id t
	t=$(now)
	for id in $(members); do
		echo "$t $1 $2 $id $(k get --raw "/api/v1/namespaces/$NS/pods/$id:8080/proxy/metrics" |
			awk '$1 == "kv_hints_created_total" {c = $2} $1 == "kv_hints_delivered_total" {d = $2} END {print c + 0, d + 0}')"
	done | tee -a "$OUT/hints.txt"
}

# wait_hints TEST: wait until every hint is delivered, mark hints-delivered.
# Read back only after it: until then a write's missing copy may still be
# on its way.
wait_hints() {
	local left
	until left=$(hints_left) && [ "$left" = 0 ]; do
		echo "hints left: $left"
		sleep 10
	done
	mark "$1" hints-delivered
}

# A partition is two egress-only NetworkPolicies, one per side: a side's
# members may open connections to each other and to DNS, and to nothing else.
# No ingress rules: on kind's Cilium an ingress policy also drops the
# kubelet's probes (whatever allow-localhost says), and liveness then kills
# the cut-off pods, which turns the partition into a crash. Connections from
# the recorders, the operator and Prometheus still reach both sides, as the
# kubelet does.
#
# The sides are selected by a label of their own (SIDE_LABEL), not by
# statefulset.kubernetes.io/pod-name: Cilium (kind's, and GKE's Dataplane
# V2: seen in its CiliumEndpoints in GKE session 3) leaves that label out of
# its security identities, so a policy selecting on it selects nothing and
# is not enforced at all (seen on kind: the pods kept "Disabled" enforcement
# and the partition cut nothing).
SIDE_LABEL=kvchaos/partition

# label_sides POD...: label the pods group and every other member rest, and
# wait until Cilium has taken the labels into their identities (5 to 30s on
# kind; until then a policy on them selects nothing). GKE's Dataplane V2 has
# CiliumEndpoints too, with the pods' labels in their identities (GKE
# session 3); where there are none to watch, wait 60s instead.
label_sides() {
	local id side
	for id in $(members); do
		side=rest
		case " $* " in *" $id "*) side=group ;; esac
		k -n "$NS" label pod "$id" "$SIDE_LABEL=$side" --overwrite >/dev/null
	done
	if ! k get crd ciliumendpoints.cilium.io >/dev/null 2>&1; then
		sleep 60
		return
	fi
	for id in $(members); do
		until k -n "$NS" get ciliumendpoint "$id" -o jsonpath='{.status.identity.labels}' 2>/dev/null |
			grep -q "$SIDE_LABEL=$(k -n "$NS" get pod "$id" -o jsonpath="{.metadata.labels['kvchaos/partition']}")"; do
			sleep 1
		done
	done
}

partition() {
	local test=$1 side
	shift
	[ $# -gt 0 ] || die "partition TEST POD..."
	label_sides "$@"
	for side in group rest; do
		k -n "$NS" apply -f - >/dev/null <<YAML
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: $POLICY-$side
  namespace: $NS
spec:
  podSelector: {matchLabels: {$SIDE_LABEL: $side}}
  policyTypes: [Egress]
  egress:
    - to: [{podSelector: {matchLabels: {$SIDE_LABEL: $side}}}]
    - to: [{namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: kube-system}}, podSelector: {matchLabels: {k8s-app: kube-dns}}}]
YAML
	done
	mark "$test" inject "partition:$(IFS=,; echo "$*")"
}

# unpartition TEST: delete both policies; heal. The labels stay: removing
# them changes the pods' identities again, under load. unlabel removes them.
unpartition() {
	k -n "$NS" delete networkpolicy "$POLICY-group" "$POLICY-rest" >/dev/null
	mark "$1" heal
}

unlabel() { k -n "$NS" label pods -l "$SIDE_LABEL" "$SIDE_LABEL-" >/dev/null; }

peers() {
	local id m
	for id in $(members); do
		if ! m=$(membership "$id" 2>/dev/null | tr -d ' \n'); then
			echo "$id unreachable"
			continue
		fi
		echo "$id epoch=$(echo "$m" | grep -o '"epoch":[0-9]*' | head -1 | cut -d: -f2)" \
			"alive=$(echo "$m" | grep -o '"alive":true' | wc -l)" \
			"reachable=$(echo "$m" | grep -o '"reachable":true' | wc -l)"
	done
}

watch_start() {
	mkdir -p "$OUT"
	[ ! -f "$OUT/watch.pid" ] || die "a watch is running (pid $(cat "$OUT/watch.pid")); watch-stop first"
	(
		set +e
		prev=""
		while :; do
			s=$(k -n "$NS" get kvc kv -o jsonpath='{.status.phase} epoch={.status.epoch} ready={.status.readyReplicas} {.status.conditions[?(@.type=="Available")].reason}: {.status.conditions[?(@.type=="Available")].message}' 2>&1)
			if [ "$s" != "$prev" ]; then
				echo "$(now) $s" >>"$OUT/operator.log"
				prev=$s
			fi
			sleep 2
		done
	) &
	echo $! >"$OUT/watch.pid"
	echo "watching the KVCluster (pid $!) into $OUT/operator.log"
}

watch_stop() {
	[ -f "$OUT/watch.pid" ] || die "no watch running"
	kill "$(cat "$OUT/watch.pid")" 2>/dev/null || true
	rm "$OUT/watch.pid"
}

files() { # list FILE... of the runs that have one, comma-separated
	local f out=""
	for f in "$OUT"/*/"$1"; do [ -f "$f" ] && out="$out,$f"; done
	echo "${out#,}"
}

check() {
	kvchaos check -topology "$OUT/topology.json" -events "$OUT/faults.jsonl" \
		-records "$(files records.jsonl)" -readback "$(files readback.jsonl)" "$@" | tee "$OUT/check.txt"
}

check_final() {
	kvchaos check -topology "$OUT/topology.json" -events "$OUT/faults.jsonl" \
		-records "$(files records.jsonl)" -readback "$OUT/final-readback.jsonl" -converged "$@" | tee "$OUT/check-final.txt"
}

cmd=${1:-}
[ -n "$cmd" ] || usage
shift
case "$cmd" in
kind-up) kind_up ;;
cilium) [ "$PLATFORM" = kind ] || die "cilium is for kind"; cilium ;;
kind-down) [ "$PLATFORM" = kind ] || die "kind-down is for kind"; kind delete cluster --name "$KIND_NAME" ;;
topology) topology ;;
predict) predict_one "$@" ;;
preload) preload ;;
run) run "$@" ;;
start) start "$@" ;;
stop) stop "$@" ;;
readback) readback "$@" ;;
converge) converge ;;
delete) k -n "$NS" delete pod "chaos-$1" --wait=false ;;
mark) mark "$@" ;;
wait-node) wait_node "$@" ;;
wait-node-down) wait_node_down "$@" ;;
wait-volume) wait_volume "$@" ;;
wait-vm) wait_vm "$@" ;;
vm-loss) vm_loss "$@" ;;
system-loss) system_loss "$@" ;;
wait-ready) wait_ready "$@" ;;
wait-hints) wait_hints "$@" ;;
hints-snapshot) hints_snapshot "$@" ;;
partition) partition "$@" ;;
unlabel) unlabel ;;
unpartition) unpartition "$@" ;;
peers) peers ;;
watch-start) watch_start ;;
watch-stop) watch_stop ;;
check) check "$@" ;;
check-final) check_final "$@" ;;
*) usage ;;
esac
