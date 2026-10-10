#!/usr/bin/env bash
# Runs the k6 benchmarks of docs/benchmarks.md inside the cluster, on kind or
# GKE, and collects each run's results. Git Bash on Windows, any bash on
# Linux (make bench is a thin wrapper). Needs kubectl, and metrics-server in
# the cluster (kubectl top).
#
#   deploy/bench/run.sh reset                 fresh bench KVCluster, no data
#   deploy/bench/run.sh preload               write the PRELOAD_KEYS keys reads use
#   deploy/bench/run.sh writes|reads|mix      one benchmark run
#   deploy/bench/run.sh clean                 delete the k6 Job and its ConfigMaps
#
# Parameters (environment variables, defaults in brackets):
#   BENCH_CONTEXT  kubectl context to run on [kind-kvstore]. It must be the
#                  current context too, and kind-kvstore or gke_*_kvstore.
#   PROFILE        constant | ramp [constant]
#   RATE           constant: iterations/s, all k6 pods together [100]
#   DURATION       constant: seconds [300]
#   RAMP_FROM, RAMP_TO, RAMP_STEP
#                  ramp: rates (iterations/s) from RAMP_FROM to RAMP_TO in
#                  steps of RAMP_STEP [50, 500, 50]
#   STEP_SECONDS   ramp: how long each rate is held [30]
#   RAMP_SECONDS   ramp: how long the move to the next rate takes [5]
#   VUS            VUs, and so connections, all k6 pods together [120]
#   PRELOAD_KEYS   keys preload writes and reads choose from [20000]
#   K6_PODS        1 | 2: k6 pods sharing the load [1]
#   BENCH_OUT      results directory [docs/results/bench/<run>]
#   KV_IMAGE       reset on GKE: the node image's Artifact Registry path
#   BENCH_YES=1    reset without asking
#   BENCH_EXPORT=0 skip the Prometheus export (deploy/bench/export.sh)
#   BENCH_SMOKE=1  allow writes, reads and mix on a dirty tree (below); the
#                  run is recorded as smoke=1, not a result
# In mix, RATE (or every ramp rate) and VUS are split evenly between writes
# and reads, so they must be even.
#
# Keys: every write is counted, before the run, in the k6-bench-ledger
# ConfigMap of the KVCluster it went to (by uid), and a run that would take
# the total past KEY_CAP is refused. With n=3 replicas over 6 pods each key
# is stored on half the pods; the ring's skew put the busiest pod at 1.42x
# the mean in GKE session 1 (storage writes by pod), so allowing 1.5x:
#   keys per pod (busiest) = keys x 3/6 x 1.5 = 0.75 x keys
#   KEY_CAP = 130,000 keys -> 97,500 per pod, under the ~100k the 512Mi
#   memory limit of the bench KVCluster is sized for.
# reset starts a new ledger at 0.
#
# A dirty tree: writes, reads and mix refuse to run if a tracked file outside
# docs/ differs from HEAD (staged or not), so every result names the commit
# it ran. Untracked files and docs/ do not count: each run's results are
# untracked until committed, and recording runs edits docs/benchmarks.md.
# preload is exempt (it loads data, it is not a result), and so is a run
# with BENCH_SMOKE=1. Either way run.env records the commit, -dirty or not.
set -euo pipefail
export MSYS_NO_PATHCONV=1

NS=kvstore
JOB=k6-bench
KEY_CAP=130000
# Fixed, not a parameter: every run in docs/benchmarks.md uses it.
VALUE_SIZE=256
REPLICAS=6
MEMORY_LIMIT=512Mi

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"

die() { echo "run.sh: $*" >&2; exit 1; }

usage() {
	sed -n '2,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'
	exit 2
}

CMD=${1:-}
[ $# -le 1 ] || die "parameters are environment variables, e.g. RATE=200 deploy/bench/run.sh writes (make bench SCENARIO=writes RATE=200)."
case "$CMD" in
	reset | preload | writes | reads | mix | clean) ;;
	*) usage ;;
esac

# --- Context: the current one, and one this script expects --------------

BENCH_CONTEXT=${BENCH_CONTEXT:-kind-kvstore}
current=$(kubectl config current-context 2>/dev/null || true)
if [ "$current" != "$BENCH_CONTEXT" ]; then
	die "refusing: kubectl's current context is '$current', not BENCH_CONTEXT '$BENCH_CONTEXT'. Switch with kubectl config use-context, or set BENCH_CONTEXT."
fi
case "$BENCH_CONTEXT" in
	kind-kvstore) OVERLAY=kind ;;
	gke_*_kvstore) OVERLAY=gke ;;
	*) die "refusing: '$BENCH_CONTEXT' is neither kind-kvstore nor a GKE context of the cluster kvstore (gke_<project>_<region>_kvstore)." ;;
esac

k() { kubectl --context "$BENCH_CONTEXT" -n "$NS" "$@"; }

kvc() { k get kvc kv -o "jsonpath=$1" 2>/dev/null || true; }

ledger() { k get configmap k6-bench-ledger -o "jsonpath={.data.$1}" 2>/dev/null || true; }

write_ledger() { # uid keys preload runs
	k create configmap k6-bench-ledger --from-literal=uid="$1" --from-literal=keys="$2" \
		--from-literal=preload="$3" --from-literal=runs="$4" --dry-run=client -o yaml | k apply -f - >/dev/null
}

wait_ready() {
	local deadline=$(($(date +%s) + 900))
	while :; do
		local phase ready
		phase=$(kvc '{.status.phase}')
		ready=$(kvc '{.status.readyReplicas}')
		[ "$phase" = Ready ] && [ "$ready" = "$REPLICAS" ] && return 0
		[ "$(date +%s)" -lt "$deadline" ] || die "the KVCluster kv is not Ready with $REPLICAS replicas after 15m (phase '$phase', ready '$ready')."
		sleep 5
	done
}

delete_job() {
	k delete job "$JOB" --ignore-not-found --wait=true >/dev/null
	k wait --for=delete pod -l "job-name=$JOB" --timeout=120s >/dev/null 2>&1 || true
}

# --- clean ---------------------------------------------------------------

if [ "$CMD" = clean ]; then
	delete_job
	k delete configmap k6-bench-script k6-bench-params --ignore-not-found
	exit 0
fi

# --- reset: a fresh bench KVCluster with empty volumes -----------------

if [ "$CMD" = reset ]; then
	sample=deploy/bench/kvcluster/$OVERLAY.yaml
	if [ "$OVERLAY" = gke ]; then
		[ -n "${KV_IMAGE:-}" ] || die "reset on GKE needs KV_IMAGE, the node image's Artifact Registry path (docs/gke.md)."
	fi
	echo "This deletes the KVCluster kv in $NS on $BENCH_CONTEXT, its pods and its volumes (every key), and creates $sample."
	if [ "${BENCH_YES:-}" != 1 ]; then
		read -r -p "Type the context name to go on: " answer
		[ "$answer" = "$BENCH_CONTEXT" ] || die "not confirmed."
	fi
	delete_job
	k delete kvc kv --ignore-not-found --wait=true --timeout=600s
	k wait --for=delete pod -l app.kubernetes.io/name=kvstore --timeout=600s >/dev/null 2>&1 || true
	# The volumes outlive the KVCluster (whenDeleted: Retain).
	k delete pvc -l app.kubernetes.io/name=kvstore --wait=true --timeout=600s
	k delete configmap k6-bench-ledger --ignore-not-found >/dev/null
	if [ "$OVERLAY" = gke ]; then
		sed "s|image: KV_IMAGE|image: $KV_IMAGE|" "$sample" | k apply -f -
	else
		k apply -f "$sample"
	fi
	echo "Waiting for the KVCluster kv to be Ready with $REPLICAS replicas..."
	sleep 5
	wait_ready
	write_ledger "$(kvc '{.metadata.uid}')" 0 0 0
	echo "Ready: a fresh bench KVCluster, ledger at 0 keys. Next: deploy/bench/run.sh preload"
	exit 0
fi

# --- A run: parameters ----------------------------------------------------

SCENARIO=$CMD
PROFILE=${PROFILE:-constant}
RATE=${RATE:-100}
DURATION=${DURATION:-300}
RAMP_FROM=${RAMP_FROM:-50}
RAMP_TO=${RAMP_TO:-500}
RAMP_STEP=${RAMP_STEP:-50}
STEP_SECONDS=${STEP_SECONDS:-30}
RAMP_SECONDS=${RAMP_SECONDS:-5}
VUS=${VUS:-120}
PRELOAD_KEYS=${PRELOAD_KEYS:-20000}
K6_PODS=${K6_PODS:-1}

for v in RATE DURATION RAMP_FROM RAMP_TO RAMP_STEP STEP_SECONDS RAMP_SECONDS VUS PRELOAD_KEYS; do
	case "${!v}" in '' | *[!0-9]*) die "$v=${!v} is not a non-negative integer." ;; esac
done
[ "$VUS" -ge 1 ] || die "VUS must be at least 1."
case "$K6_PODS" in 1 | 2) ;; *) die "K6_PODS=$K6_PODS: want 1 or 2." ;; esac
[ "$SCENARIO" != preload ] || [ "$K6_PODS" = 1 ] || die "preload runs with K6_PODS=1 only."
[ $((VUS % K6_PODS)) = 0 ] || die "VUS=$VUS does not split evenly over K6_PODS=$K6_PODS."

STAGES=""
START_RATE=0
case "$SCENARIO:$PROFILE" in
	preload:*)
		PROFILE=shared
		SECONDS_PLANNED=1800
		WRITES=$PRELOAD_KEYS
		;;
	*:constant)
		if [ "$RATE" -lt 1 ] || [ "$DURATION" -lt 1 ]; then die "RATE and DURATION must be at least 1."; fi
		SECONDS_PLANNED=$DURATION
		WRITES=$((RATE * DURATION))
		;;
	*:ramp)
		if [ "$RAMP_STEP" -lt 1 ] || [ "$RAMP_TO" -lt "$RAMP_FROM" ] || [ "$STEP_SECONDS" -lt 1 ]; then
			die "need RAMP_STEP >= 1, RAMP_TO >= RAMP_FROM and STEP_SECONDS >= 1."
		fi
		START_RATE=$RAMP_FROM
		# Hold RAMP_FROM, then for each higher rate: move to it, hold it.
		STAGES="$RAMP_FROM:$STEP_SECONDS"
		SECONDS_PLANNED=$STEP_SECONDS
		WRITES=$((RAMP_FROM * STEP_SECONDS))
		prev=$RAMP_FROM
		r=$((RAMP_FROM + RAMP_STEP))
		while [ "$r" -le "$RAMP_TO" ]; do
			STAGES="$STAGES,$r:$RAMP_SECONDS,$r:$STEP_SECONDS"
			SECONDS_PLANNED=$((SECONDS_PLANNED + RAMP_SECONDS + STEP_SECONDS))
			# The move is linear: its average rate is the midpoint.
			WRITES=$((WRITES + (prev + r) * RAMP_SECONDS / 2 + r * STEP_SECONDS))
			prev=$r
			r=$((r + RAMP_STEP))
		done
		;;
	*) die "PROFILE=$PROFILE: want constant or ramp." ;;
esac
case "$SCENARIO" in
	reads) WRITES=0 ;;
	mix)
		WRITES=$((WRITES / 2))
		[ $((VUS % (2 * K6_PODS))) = 0 ] || die "mix splits VUS between writes and reads (and over K6_PODS): VUS=$VUS must be a multiple of $((2 * K6_PODS))."
		if [ "$PROFILE" = ramp ]; then rates="$RAMP_FROM $RAMP_STEP"; else rates=$RATE; fi
		for x in $rates; do
			[ $((x % 2)) = 0 ] || die "mix splits every rate between writes and reads: $x is odd (RATE, or RAMP_FROM and RAMP_STEP)."
		done
		;;
esac

# --- Preflight: the tree is a commit, unless told otherwise -------------

# Tracked changes outside docs/, staged or not (see the header).
COMMIT=$(git rev-parse --short HEAD)
git diff --quiet HEAD -- . ':(exclude)docs' || COMMIT=$COMMIT-dirty
SMOKE=${BENCH_SMOKE:-0}
case "$SMOKE" in 0 | 1) ;; *) die "BENCH_SMOKE=$SMOKE: want 0 or 1." ;; esac
if [ "$SCENARIO" != preload ] && [ "$SMOKE" = 0 ] && [ "$COMMIT" != "${COMMIT%-dirty}" ]; then
	die "refusing: tracked files outside docs/ differ from HEAD, so this run would not name the code it ran (git status). Commit or stash them, or set BENCH_SMOKE=1 for a run that is not a result."
fi

# --- Preflight: the cluster is the one docs/benchmarks.md describes ------

phase=$(kvc '{.status.phase}')
ready=$(kvc '{.status.readyReplicas}')
replicas=$(kvc '{.spec.replicas}')
memlimit=$(kvc '{.spec.resources.limits.memory}')
uid=$(kvc '{.metadata.uid}')
[ -n "$uid" ] || die "no KVCluster kv in $NS. Create the bench one: deploy/bench/run.sh reset"
if [ "$phase" != Ready ] || [ "$ready" != "$REPLICAS" ] || [ "$replicas" != "$REPLICAS" ]; then
	die "the KVCluster kv must be Ready with $REPLICAS replicas (phase '$phase', ready '$ready', replicas '$replicas')."
fi
[ "$memlimit" = "$MEMORY_LIMIT" ] ||
	die "the KVCluster kv has memory limit '$memlimit', not the bench sample's $MEMORY_LIMIT: deploy/bench/run.sh reset creates it."
[ "$(ledger uid)" = "$uid" ] ||
	die "no ledger for this KVCluster (uid $uid): its keys are unknown. Start from a fresh one: deploy/bench/run.sh reset"
keys=$(ledger keys)
preloaded=$(ledger preload)
runs=$(ledger runs)
if [ "$SCENARIO" = preload ]; then
	[ "$preloaded" = 0 ] || die "this KVCluster is already preloaded ($preloaded keys)."
elif [ "$SCENARIO" != writes ]; then
	[ "$preloaded" = "$PRELOAD_KEYS" ] ||
		die "$SCENARIO reads PRELOAD_KEYS=$PRELOAD_KEYS preloaded keys, but this KVCluster has $preloaded: run deploy/bench/run.sh preload (with the same PRELOAD_KEYS) first."
fi
if [ $((keys + WRITES)) -gt "$KEY_CAP" ]; then
	die "refusing: this run writes up to $WRITES keys and the KVCluster already holds $keys; together $((keys + WRITES)) > KEY_CAP $KEY_CAP (busiest pod ~ 0.75 x keys must stay under ~100k). Shorten the run, or deploy/bench/run.sh reset."
fi
active=$(k get job "$JOB" -o 'jsonpath={.status.active}' 2>/dev/null || true)
[ -z "$active" ] || [ "$active" = 0 ] || die "a $JOB Job is still running: wait for it, or deploy/bench/run.sh clean."
if [ "$OVERLAY" = gke ]; then
	[ -n "$(kubectl --context "$BENCH_CONTEXT" get nodes -l cloud.google.com/gke-nodepool=loadgen -o name)" ] ||
		die "no loadgen node: create the pool with loadgen_enabled = true in infra/gke (docs/benchmarks.md)."
fi

# --- The run ------------------------------------------------------------

RUN_ID=$(date -u +%Y%m%d-%H%M%S)
OUT=${BENCH_OUT:-docs/results/bench/$RUN_ID-$OVERLAY-$SCENARIO-$PROFILE}
mkdir -p "$OUT"
START_AT=0
[ "$K6_PODS" = 1 ] || START_AT=$(($(date +%s) + 60))

write_ledger "$uid" $((keys + WRITES)) "$([ "$SCENARIO" = preload ] && echo "$PRELOAD_KEYS" || echo "$preloaded")" $((runs + 1))

k create configmap k6-bench-script --from-file=deploy/bench/script.js \
	--from-file=internal/rpc/pb/kvstore.proto --dry-run=client -o yaml | k apply -f - >/dev/null
k create configmap k6-bench-params --dry-run=client -o yaml \
	--from-literal=SCENARIO="$SCENARIO" --from-literal=PROFILE="$PROFILE" \
	--from-literal=RATE="$RATE" --from-literal=DURATION="$DURATION" \
	--from-literal=START_RATE="$START_RATE" --from-literal=STAGES="$STAGES" \
	--from-literal=VUS="$((VUS / K6_PODS))" --from-literal=PRELOAD_KEYS="$PRELOAD_KEYS" \
	--from-literal=VALUE_SIZE="$VALUE_SIZE" --from-literal=RUN_ID="$RUN_ID" \
	--from-literal=K6_PODS="$K6_PODS" --from-literal=START_AT="$START_AT" | k apply -f - >/dev/null

# Each k6 pod gets VUS / K6_PODS VUs; the rates are split by the execution
# segments, so RATE stays the total.
delete_job
kubectl kustomize "deploy/bench/overlays/$OVERLAY" |
	sed -e "s/^  completions: 1$/  completions: $K6_PODS/" -e "s/^  parallelism: 1$/  parallelism: $K6_PODS/" |
	k apply -f - >/dev/null

{
	echo "run_id=$RUN_ID"
	echo "context=$BENCH_CONTEXT"
	echo "cluster=$OVERLAY"
	echo "scenario=$SCENARIO"
	echo "profile=$PROFILE"
	echo "rate=$RATE"
	echo "duration_s=$DURATION"
	echo "ramp_from=$RAMP_FROM"
	echo "ramp_to=$RAMP_TO"
	echo "ramp_step=$RAMP_STEP"
	echo "step_seconds=$STEP_SECONDS"
	echo "ramp_seconds=$RAMP_SECONDS"
	echo "stages=$STAGES"
	echo "vus=$VUS"
	echo "k6_pods=$K6_PODS"
	echo "preload_keys=$PRELOAD_KEYS"
	echo "value_size=$VALUE_SIZE"
	echo "planned_writes=$WRITES"
	echo "keys_before=$keys"
	echo "keys_after=$((keys + WRITES))"
	echo "kvcluster_uid=$uid"
	epoch=$(kvc '{.status.epoch}')
	echo "kvcluster_epoch=${epoch:-0}"
	echo "kv_image=$(kvc '{.spec.image}')"
	echo "kv_memory_limit=$memlimit"
	echo "k6_image=$(kubectl kustomize "deploy/bench/overlays/$OVERLAY" | sed -n 's/^ *image: //p')"
	echo "commit=$COMMIT"
	echo "smoke=$SMOKE"
	echo "script_sha256=$(sha256sum deploy/bench/script.js | cut -d' ' -f1)"
} >"$OUT/run.env"

echo "Run $RUN_ID: $SCENARIO, $PROFILE, $K6_PODS k6 pod(s), up to $WRITES writes; results in $OUT"

# kubectl top for the kv and k6 pods every 15s (metrics-server's
# resolution), for the whole run: k6's own CPU, next to the database's.
(
	while :; do
		ts=$(date -u +%FT%TZ)
		k top pod --containers --no-headers 2>/dev/null | awk -v ts="$ts" '{print ts, $0}'
		sleep 15
	done
) >"$OUT/top.txt" &
sampler=$!
trap 'kill $sampler 2>/dev/null || true' EXIT

deadline=$(($(date +%s) + SECONDS_PLANNED + 60 + 600))
while :; do
	succeeded=$(k get job "$JOB" -o 'jsonpath={.status.succeeded}' 2>/dev/null || true)
	failed=$(k get job "$JOB" -o 'jsonpath={.status.failed}' 2>/dev/null || true)
	[ "${succeeded:-0}" != "$K6_PODS" ] || break
	[ "${failed:-0}" = 0 ] || break
	[ "$(date +%s)" -lt "$deadline" ] || { echo "run.sh: the Job did not finish in time; collecting what there is." >&2; break; }
	sleep 5
done
kill $sampler 2>/dev/null || true

# --- Results --------------------------------------------------------------

status=0
[ "${failed:-0}" = 0 ] && [ "${succeeded:-0}" = "$K6_PODS" ] || status=1
k get pods -l "job-name=$JOB" --no-headers \
	-o 'custom-columns=NAME:.metadata.name,INDEX:.metadata.annotations.batch\.kubernetes\.io/job-completion-index,NODE:.spec.nodeName' |
	while read -r pod index node; do
		k logs "$pod" >"$OUT/k6-$index.log" 2>&1 || true
		sed -n '/^===K6_BENCH_SUMMARY_BEGIN===$/,/^===K6_BENCH_SUMMARY_END===$/p' "$OUT/k6-$index.log" | sed '1d;$d' >"$OUT/summary-$index.json"
		sed -n 's/^K6_BENCH //p' "$OUT/k6-$index.log" >"$OUT/summary-$index.env"
		echo "k6_node_$index=$node" >>"$OUT/run.env"
	done

# The measured window: from the first pod's start to the last pod's end.
window=$(for f in "$OUT"/k6-*.log; do
	i=${f##*/k6-}
	i=${i%.log}
	s=$(sed -n 's/^k6-bench-start \([0-9]*\).*/\1/p' "$f" | head -1)
	d=$(sed -n 's/^duration_s=//p' "$OUT/summary-$i.env")
	[ -n "$s" ] && [ -n "$d" ] && echo "$s $d"
done | awk 'NR == 1 || $1 < s {s = $1} {e = $1 + $2; if (e > m) m = e} END {if (NR) printf "%d %d\n", s, m + 0.999}')
if [ -n "$window" ]; then
	read -r wstart wend <<<"$window"
	{
		echo "window_start=$(date -u -d "@$wstart" +%FT%TZ)"
		echo "window_end=$(date -u -d "@$wend" +%FT%TZ)"
	} >>"$OUT/run.env"
fi
# The machine type (GKE's instance-type label; kind's node has none).
sed -n 's/^k6_node_[0-9]*=//p' "$OUT/run.env" | sort -u | while read -r node; do
	type=$(kubectl --context "$BENCH_CONTEXT" get node "$node" -o 'jsonpath={.metadata.labels.node\.kubernetes\.io/instance-type}')
	echo "k6_node_type_$node=${type:-none}" >>"$OUT/run.env"
done

echo
for f in "$OUT"/k6-*.log; do sed -n '/^k6 bench:/,/^  dropped_iterations/p' "$f"; done
dropped=$(cat "$OUT"/summary-*.env 2>/dev/null | awk -F= '$1 == "dropped_iterations" {s += $2} END {print s + 0}')
errors=$(cat "$OUT"/summary-*.env 2>/dev/null | awk -F= '$1 ~ /_errors$/ {s += $2} END {print s + 0}')
connects=$(cat "$OUT"/summary-*.env 2>/dev/null | awk -F= '$1 == "connects" {s += $2} END {print s + 0}')
{
	echo "dropped_iterations=$dropped"
	echo "errors=$errors"
	echo "connects=$connects"
} >>"$OUT/run.env"
echo
echo "dropped_iterations $dropped (must be 0 below the knee), errors $errors, connections $connects of $VUS VUs"
echo "k6 CPU (kubectl top, millicores): $(awk '$2 ~ /^k6-bench/ {print $4}' "$OUT/top.txt" | sort -n | awk '{a[NR] = $1} END {if (NR) print "max " a[NR] ", median " a[int((NR + 1) / 2)]; else print "no samples"}')"
[ "$status" = 0 ] || echo "run.sh: the k6 Job failed (succeeded '${succeeded:-0}', failed '${failed:-0}'); see $OUT/k6-*.log" >&2
if [ "${BENCH_EXPORT:-1}" != 0 ] && grep -q '^window_end=' "$OUT/run.env"; then
	echo
	deploy/bench/export.sh "$OUT" || status=1
fi
echo "Results: $OUT"
exit "$status"
