#!/usr/bin/env bash
# Exports the Prometheus view of one benchmark run (docs/benchmarks.md):
#
#   deploy/bench/export.sh RESULTS_DIR
#
# RESULTS_DIR is a directory run.sh wrote; its run.env gives the context and
# the measured window (window_start, window_end). run.sh calls this at the
# end of every run unless BENCH_EXPORT=0; run it by hand to export again.
#
# Writes RESULTS_DIR/prometheus/:
#   *.csv       one per query, 15s steps from 60s before the window to 30s
#               after it, in the shape of docs/results/gke-session1/ (time,
#               the series' labels, the value)
#   window.env  the whole window at once: request counts, Put/Get and fsync
#               quantiles over the window (histogram_quantile of the buckets'
#               increase), anti-entropy rounds, quorum failures, hints,
#               per-pod CPU and how evenly the pods coordinated requests
#   queries.txt every query, as sent
#
# It reaches Prometheus (observability/prometheus) through a port-forward on
# PROM_PORT [19090], which it stops when it exits. Refuses unless kubectl's
# current context is the run's. Needs python3 (or python) for the JSON.
set -euo pipefail
export MSYS_NO_PATHCONV=1

die() { echo "export.sh: $*" >&2; exit 1; }

[ $# = 1 ] || die "usage: deploy/bench/export.sh RESULTS_DIR"
DIR=${1%/}
[ -f "$DIR/run.env" ] || die "no $DIR/run.env: not a run.sh results directory."
get() { sed -n "s/^$1=//p" "$DIR/run.env" | head -1; }

CONTEXT=$(get context)
START=$(get window_start)
END=$(get window_end)
if [ -z "$START" ] || [ -z "$END" ]; then die "$DIR/run.env has no measured window (did the k6 pods finish?)."; fi
current=$(kubectl config current-context 2>/dev/null || true)
[ "$current" = "$CONTEXT" ] || die "refusing: the run was on '$CONTEXT', but kubectl's current context is '$current'."
case "$CONTEXT" in
	kind-kvstore | gke_*_kvstore) ;;
	*) die "refusing: '$CONTEXT' is neither kind-kvstore nor gke_<project>_<region>_kvstore." ;;
esac
PY=$(command -v python3 || command -v python || true)
[ -n "$PY" ] || die "needs python3 or python."

# Prometheus scrapes every 15s: wait until the 30s after the window, and one
# more scrape, are in.
end_epoch=$(date -u -d "$END" +%s)
wait=$((end_epoch + 45 - $(date +%s)))
if [ "$wait" -gt 0 ]; then
	echo "Waiting ${wait}s for Prometheus to scrape the end of the window..."
	sleep "$wait"
fi

PROM_PORT=${PROM_PORT:-19090}
mkdir -p "$DIR/prometheus"
kubectl --context "$CONTEXT" -n observability port-forward svc/prometheus "$PROM_PORT:9090" >"$DIR/prometheus/port-forward.log" 2>&1 &
pf=$!
trap 'kill $pf 2>/dev/null || true; wait $pf 2>/dev/null || true; rm -f "$DIR/prometheus/port-forward.log"' EXIT
for _ in $(seq 1 30); do
	grep -q "Forwarding from" "$DIR/prometheus/port-forward.log" 2>/dev/null && break
	kill -0 "$pf" 2>/dev/null || die "the port-forward to Prometheus failed: $(cat "$DIR/prometheus/port-forward.log")"
	sleep 1
done
grep -q "Forwarding from" "$DIR/prometheus/port-forward.log" || die "the port-forward to Prometheus did not come up."

"$PY" - "http://127.0.0.1:$PROM_PORT" "$START" "$END" "$DIR/prometheus" <<'EOF'
import csv, json, os, statistics, sys, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

base, start_s, end_s, out = sys.argv[1:5]
parse = lambda s: datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
start, end = parse(start_s), parse(end_s)
window = int((end - start).total_seconds())
J = 'job="kvstore"'
queries_log = []

def api(path, params):
    queries_log.append(f"{path} {params.get('query')}")
    url = f"{base}/api/v1/{path}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=30) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise SystemExit(f"export.sh: {path} {params.get('query')}: {body}")
    return body["data"]["result"]

def iso(t):
    return datetime.fromtimestamp(float(t), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# name -> (query, labels, value column)
def q_rate(method):
    return f'sum by (code) (rate(kv_client_request_duration_seconds_count{{{J},method="{method}"}}[1m]))'
def q_quant(q, metric, sel):
    return f'histogram_quantile({q}, sum by (le) (rate({metric}_bucket{{{J}{sel}}}[1m])))'
RANGE = {
    "put_rate_by_code": (q_rate("Put"), ["code"], "puts_per_second"),
    "get_rate_by_code": (q_rate("Get"), ["code"], "gets_per_second"),
    "put_latency_p50": (q_quant(0.50, "kv_client_request_duration_seconds", ',method="Put"'), [], "seconds"),
    "put_latency_p99": (q_quant(0.99, "kv_client_request_duration_seconds", ',method="Put"'), [], "seconds"),
    "get_latency_p50": (q_quant(0.50, "kv_client_request_duration_seconds", ',method="Get"'), [], "seconds"),
    "get_latency_p99": (q_quant(0.99, "kv_client_request_duration_seconds", ',method="Get"'), [], "seconds"),
    "wal_fsync_latency_p50_data": (q_quant(0.50, "kv_wal_fsync_duration_seconds", ',engine="data"'), [], "seconds"),
    "wal_fsync_latency_p99_data": (q_quant(0.99, "kv_wal_fsync_duration_seconds", ',engine="data"'), [], "seconds"),
    "requests_coordinated_by_pod": (f'sum by (instance, method) (rate(kv_client_request_duration_seconds_count{{{J}}}[1m]))', ["instance", "method"], "requests_per_second"),
    "storage_writes_by_pod": (f'sum by (instance) (rate(kv_wal_fsync_duration_seconds_count{{{J},engine="data"}}[1m]))', ["instance"], "fsyncs_per_second"),
    "cpu_by_pod": (f'sum by (instance) (rate(process_cpu_seconds_total{{{J}}}[1m]))', ["instance"], "cores"),
    "memory_by_pod": (f'sum by (instance) (process_resident_memory_bytes{{{J}}})', ["instance"], "bytes"),
    "antientropy_rounds_by_pod": (f'sum by (instance) (increase(kv_antientropy_rounds_total{{{J}}}[1m]))', ["instance"], "rounds"),
    "quorum_failures": (f'sum by (quorum) (increase(kv_quorum_failures_total{{{J}}}[1m]))', ["quorum"], "failures"),
    "hints_created": (f'sum(rate(kv_hints_created_total{{{J}}}[1m]))', [], "hints_per_second"),
}
rs, re_ = start - timedelta(seconds=60), end + timedelta(seconds=30)
for name, (query, labels, col) in RANGE.items():
    result = api("query_range", {"query": query, "start": rs.timestamp(), "end": re_.timestamp(), "step": 15})
    rows = []
    for series in result:
        lv = [series["metric"].get(l, "") for l in labels]
        for t, v in series["values"]:
            rows.append([iso(t)] + lv + [v])
    rows.sort()
    with open(os.path.join(out, f"{name}.csv"), "w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["time"] + labels + [col])
        w.writerows(rows)

# The whole window at once, evaluated at its end.
D = f"{window}s"
def inst(query):
    return api("query", {"query": query, "time": end.timestamp()})
def scalar(query, empty=0.0):
    r = inst(query)
    return float(r[0]["value"][1]) if r else empty
def by(query, label):
    return {s["metric"].get(label, ""): float(s["value"][1]) for s in inst(query)}
def wq(q, metric, sel):
    v = scalar(f'histogram_quantile({q}, sum by (le) (increase({metric}_bucket{{{J}{sel}}}[{D}])))', float("nan"))
    return "" if v != v else f"{v * 1000:.2f}"  # NaN or no series: no samples

summary = {"window_start": start_s, "window_end": end_s, "window_s": window}
for m in ("Put", "Get"):
    lm = m.lower()
    summary[f"{lm}_count"] = round(scalar(f'sum(increase(kv_client_request_duration_seconds_count{{{J},method="{m}"}}[{D}]))'))
    summary[f"{lm}_errors"] = round(scalar(f'sum(increase(kv_client_request_duration_seconds_count{{{J},method="{m}",code!="OK"}}[{D}]))'))
    summary[f"{lm}_p50_ms"] = wq(0.50, "kv_client_request_duration_seconds", f',method="{m}"')
    summary[f"{lm}_p99_ms"] = wq(0.99, "kv_client_request_duration_seconds", f',method="{m}"')
summary["fsync_count"] = round(scalar(f'sum(increase(kv_wal_fsync_duration_seconds_count{{{J},engine="data"}}[{D}]))'))
summary["fsync_p50_ms"] = wq(0.50, "kv_wal_fsync_duration_seconds", ',engine="data"')
summary["fsync_p99_ms"] = wq(0.99, "kv_wal_fsync_duration_seconds", ',engine="data"')
summary["antientropy_rounds"] = round(scalar(f'sum(increase(kv_antientropy_rounds_total{{{J}}}[{D}]))'))
summary["quorum_failures"] = round(scalar(f'sum(increase(kv_quorum_failures_total{{{J}}}[{D}]))'))
summary["hints_created"] = round(scalar(f'sum(increase(kv_hints_created_total{{{J}}}[{D}]))'))
cpu = by(f'sum by (instance) (rate(process_cpu_seconds_total{{{J}}}[{D}]))', "instance")
for pod in sorted(cpu):
    summary[f"cpu_cores_{pod}"] = f"{cpu[pod]:.3f}"
coord = by(f'sum by (instance) (increase(kv_client_request_duration_seconds_count{{{J}}}[{D}]))', "instance")
for pod in sorted(coord):
    summary[f"coordinated_{pod}"] = round(coord[pod])
if coord and statistics.median(coord.values()) > 0:
    # Spread over the pods: the busiest pod's requests over the median's
    # (docs/benchmarks.md: at most 1.5).
    summary["coordinated_max_over_median"] = f"{max(coord.values()) / statistics.median(coord.values()):.2f}"
    summary["coordinated_min_over_median"] = f"{min(coord.values()) / statistics.median(coord.values()):.2f}"
with open(os.path.join(out, "window.env"), "w", newline="\n") as f:
    for k, v in summary.items():
        f.write(f"{k}={v}\n")
with open(os.path.join(out, "queries.txt"), "w", newline="\n") as f:
    f.write("\n".join(queries_log) + "\n")

print(f"Prometheus, {start_s} - {end_s} ({window}s):")
for m in ("put", "get"):
    if summary[f"{m}_count"]:
        print(f"  {m}: {summary[f'{m}_count']} requests, {summary[f'{m}_errors']} errors, "
              f"p50 {summary[f'{m}_p50_ms']} ms, p99 {summary[f'{m}_p99_ms']} ms")
print(f"  fsync (data): {summary['fsync_count']}, p50 {summary['fsync_p50_ms']} ms, p99 {summary['fsync_p99_ms']} ms")
print(f"  coordinated by pod: {', '.join(f'{p} {coord[p]:.0f}' for p in sorted(coord))}; "
      f"max/median {summary.get('coordinated_max_over_median', '-')}, min/median {summary.get('coordinated_min_over_median', '-')}")
print(f"  kv CPU (cores, window average): {', '.join(f'{p} {cpu[p]:.3f}' for p in sorted(cpu))}")
print(f"  quorum failures {summary['quorum_failures']}, hints created {summary['hints_created']}")
if summary["antientropy_rounds"]:
    print(f"  FLAG: {summary['antientropy_rounds']} anti-entropy round(s) in the window (docs/benchmarks.md)")
EOF
echo "Exported: $DIR/prometheus"
