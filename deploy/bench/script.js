// The k6 load test of docs/benchmarks.md. It runs in the k6-bench Job that
// deploy/bench/run.sh creates, which passes every parameter below as an
// environment variable (the k6-bench-params ConfigMap) and mounts this file
// and internal/rpc/pb/kvstore.proto at /scripts (the k6-bench-script
// ConfigMap). Run it through run.sh, not by hand: run.sh also checks the
// cluster, caps the keys written and collects the results.
//
// SCENARIO picks the load:
//   preload  PRELOAD_KEYS Puts of bench-pre-000000, bench-pre-000001, ...
//            (shared-iterations: a closed model, only to load the data)
//   writes   Puts of keys never written before: bench-<RUN_ID>-w-<n>
//   reads    Gets of random preloaded keys, each checked for its value
//   mix      writes and reads at once, each at half the rate and with half
//            the VUs, reported separately (tag op)
// PROFILE picks the arrival rate (open model) of writes, reads and mix:
//   constant RATE iterations/s for DURATION seconds (constant-arrival-rate)
//   ramp     from START_RATE through STAGES, "rate:seconds,rate:seconds,..."
//            (ramping-arrival-rate), to find the knee
//
// Every value is VALUE_SIZE ASCII characters (256 by default): the key, a
// "|" and "x" padding, so a read can check the value it gets back.
//
// Each VU opens its own connection to kv-client on its first iteration and
// keeps it. kv-client balances connections, not requests, so the VUs are
// what spreads the load over the pods: preAllocatedVUs == maxVUs == VUS,
// and every VU gets iterations in turn, so all of them connect within the
// first seconds (kv_connects in the summary counts them).
import grpc from 'k6/net/grpc';
import exec from 'k6/execution';
import { Counter, Rate } from 'k6/metrics';

const SCENARIO = __ENV.SCENARIO;
const PROFILE = __ENV.PROFILE || 'constant';
const TARGET = __ENV.TARGET || 'kv-client.kvstore.svc.cluster.local:7000';
const RUN_ID = __ENV.RUN_ID || 'dev';
const VUS = int('VUS', 60);
const RATE = int('RATE', 100);
const DURATION = int('DURATION', 300);
const START_RATE = int('START_RATE', 0);
const PRELOAD_KEYS = int('PRELOAD_KEYS', 20000);
const VALUE_SIZE = int('VALUE_SIZE', 256);
const TIMEOUT = __ENV.REQUEST_TIMEOUT || '5s';

const SUMMARY_BEGIN = '===K6_BENCH_SUMMARY_BEGIN===';
const SUMMARY_END = '===K6_BENCH_SUMMARY_END===';

function int(name, def) {
  const v = __ENV[name];
  if (v === undefined || v === '') return def;
  const n = parseInt(v, 10);
  if (isNaN(n) || n < 0) throw new Error(`${name}=${v} is not a non-negative integer`);
  return n;
}

// "100:10,100:60,200:10" -> [{target: 100, duration: '10s'}, ...]; every
// target divided by div (2 for each half of mix; run.sh makes them even).
function stages(div) {
  return (__ENV.STAGES || '').split(',').filter((s) => s !== '').map((s) => {
    const [rate, secs] = s.split(':');
    return { target: parseInt(rate, 10) / div, duration: `${secs}s` };
  });
}

function arrival(execName, div) {
  const vus = VUS / div;
  if (PROFILE === 'constant') {
    return {
      executor: 'constant-arrival-rate', exec: execName,
      rate: RATE / div, timeUnit: '1s', duration: `${DURATION}s`,
      preAllocatedVUs: vus, maxVUs: vus,
    };
  }
  if (PROFILE === 'ramp') {
    return {
      executor: 'ramping-arrival-rate', exec: execName,
      startRate: START_RATE / div, timeUnit: '1s', stages: stages(div),
      preAllocatedVUs: vus, maxVUs: vus,
    };
  }
  throw new Error(`PROFILE=${PROFILE}: want constant or ramp`);
}

function scenarios() {
  switch (SCENARIO) {
    case 'preload':
      return {
        preload: {
          executor: 'shared-iterations', exec: 'preload',
          vus: VUS, iterations: PRELOAD_KEYS, maxDuration: '30m',
        },
      };
    case 'writes':
      return { writes: arrival('put', 1) };
    case 'reads':
      return { reads: arrival('get', 1) };
    case 'mix':
      return { writes: arrival('put', 2), reads: arrival('get', 2) };
    default:
      throw new Error(`SCENARIO=${SCENARIO}: want preload, writes, reads or mix`);
  }
}

// The ops this scenario runs. Each gets always-true thresholds on its
// submetrics, which is what makes k6 report them separately in the summary.
const OPS = { preload: ['put'], writes: ['put'], reads: ['get'], mix: ['put', 'get'] }[SCENARIO] || [];

function thresholds() {
  const t = {};
  for (const op of OPS) {
    t[`grpc_req_duration{op:${op}}`] = ['max>=0'];
    t[`kv_errors{op:${op}}`] = ['rate>=0'];
  }
  return t;
}

export const options = {
  scenarios: scenarios(),
  thresholds: thresholds(),
  summaryTrendStats: ['min', 'med', 'avg', 'p(90)', 'p(95)', 'p(99)', 'max', 'count'],
};

// kv_errors: one sample per request, true when it failed (a status other
// than OK, an exception, or a read that did not return the value written).
const errors = new Rate('kv_errors');
const connects = new Counter('kv_connects');

const client = new grpc.Client();
client.load(['/scripts'], 'kvstore.proto');
let connected = false;

const PAD = 'x'.repeat(VALUE_SIZE);

function valueFor(key) {
  return `${key}|${PAD}`.substring(0, VALUE_SIZE);
}

function preloadKey(i) {
  return `bench-pre-${String(i).padStart(6, '0')}`;
}

function invoke(method, req, op) {
  if (!connected) {
    client.connect(TARGET, { plaintext: true, timeout: '10s' });
    connected = true;
    connects.add(1);
  }
  try {
    return client.invoke(method, req, { timeout: TIMEOUT, tags: { op } });
  } catch (e) {
    // A broken connection: open a new one on the next iteration.
    console.error(`${method} ${req.key}: ${e}`);
    client.close();
    connected = false;
    return null;
  }
}

function record(res, op, ok) {
  errors.add(!ok, { op });
  if (res && res.status !== grpc.StatusOK && exec.scenario.iterationInTest % 1000 === 0) {
    console.warn(`${op}: status ${res.status} ${res.error ? res.error.message : ''}`);
  }
}

function putKey(key) {
  const res = invoke('kvstore.KVClient/Put', { key, value: valueFor(key) }, 'put');
  record(res, 'put', res !== null && res.status === grpc.StatusOK);
}

export function preload() {
  putKey(preloadKey(exec.scenario.iterationInTest));
}

// iterationInTest is unique across k6 pods (execution segments), so two
// pods never write the same key.
export function put() {
  putKey(`bench-${RUN_ID}-w-${exec.scenario.iterationInTest}`);
}

export function get() {
  const key = preloadKey(Math.floor(Math.random() * PRELOAD_KEYS));
  const res = invoke('kvstore.KVClient/Get', { key }, 'get');
  const m = res && res.status === grpc.StatusOK ? res.message : null;
  record(res, 'get', m !== null && m.found === true && m.values.length === 1 && m.values[0] === valueFor(key));
}

// The summary: a short table for people, then the whole summary data as one
// JSON line between markers, which run.sh cuts out of the pod's log, then
// flat KEY=VALUE lines that run.sh reads without a JSON parser.
function val(data, name, stat) {
  const m = data.metrics[name];
  return m && m.values[stat] !== undefined ? m.values[stat] : 0;
}

export function handleSummary(data) {
  const secs = data.state.testRunDurationMs / 1000;
  const flat = {
    duration_s: secs.toFixed(1),
    dropped_iterations: val(data, 'dropped_iterations', 'count'),
    vus_max: val(data, 'vus_max', 'value'),
    connects: val(data, 'kv_connects', 'count'),
  };
  const lines = [`k6 bench: scenario=${SCENARIO} profile=${PROFILE} run=${RUN_ID} ${flat.duration_s}s`];
  for (const op of OPS) {
    const d = `grpc_req_duration{op:${op}}`;
    const e = `kv_errors{op:${op}}`;
    const n = val(data, d, 'count');
    const failed = val(data, e, 'passes');
    flat[`${op}_count`] = n;
    flat[`${op}_rate`] = (n / secs).toFixed(1);
    for (const [k, s] of [['p50', 'med'], ['p95', 'p(95)'], ['p99', 'p(99)'], ['max', 'max']]) {
      flat[`${op}_${k}_ms`] = val(data, d, s).toFixed(2);
    }
    flat[`${op}_errors`] = failed;
    lines.push(`  ${op}: ${n} requests (${flat[`${op}_rate`]}/s), p50 ${flat[`${op}_p50_ms`]} ms, ` +
      `p95 ${flat[`${op}_p95_ms`]} ms, p99 ${flat[`${op}_p99_ms`]} ms, max ${flat[`${op}_max_ms`]} ms, errors ${failed}`);
  }
  lines.push(`  dropped_iterations ${flat.dropped_iterations}, vus_max ${flat.vus_max}, connects ${flat.connects}`);
  const kv = Object.entries(flat).map(([k, v]) => `K6_BENCH ${k}=${v}`);
  return {
    stdout: `${lines.join('\n')}\n${SUMMARY_BEGIN}\n${JSON.stringify(data)}\n${SUMMARY_END}\n${kv.join('\n')}\n`,
  };
}
