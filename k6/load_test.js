/**
 * k6 load test — Job Queue Engine
 *
 * Targets the enqueue path (POST /jobs) with a three-stage ramp:
 *   0 → 100 VUs over 30s  (ramp up)
 *   100 VUs for 2m        (sustained)
 *   100 → 0 over 30s      (ramp down)
 *
 * Usage:
 *   k6 run k6/load_test.js
 *   BASE_URL=https://job-queue-api-xxx.run.app API_KEY=<key> k6 run k6/load_test.js
 *
 * Pass/fail thresholds (CI-friendly):
 *   p99 enqueue latency < 500ms
 *   error rate          < 1%
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

// ── Config ─────────────────────────────────────────────────────────────────────

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const API_KEY  = __ENV.API_KEY  || 'local-dev-key';

const HEADERS = {
  'Content-Type': 'application/json',
  'X-API-Key': API_KEY,
};

// ── Custom metrics ─────────────────────────────────────────────────────────────

const enqueueLatency = new Trend('enqueue_latency_ms', true);
const enqueueErrors  = new Rate('enqueue_errors');

// ── Options ────────────────────────────────────────────────────────────────────

export const options = {
  stages: [
    { duration: '30s', target: 100 },
    { duration: '2m',  target: 100 },
    { duration: '30s', target: 0   },
  ],
  thresholds: {
    // Core SLOs
    'enqueue_latency_ms': ['p(99)<500'],   // 99th percentile under 500ms
    'enqueue_errors':     ['rate<0.01'],   // fewer than 1% failures
    // Built-in guards
    'http_req_failed':    ['rate<0.01'],
    'checks':             ['rate>0.99'],
  },
};

// ── Helpers ────────────────────────────────────────────────────────────────────

const PRIORITIES = [0, 1, 2];          // HIGH, NORMAL, LOW
const JOB_TYPES  = [
  'example.echo',
  'example.echo',
  'example.echo',  // 3× weight — echo is fast, won't starve workers
  'example.sleep',
];

function randomItem(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

// ── Scenarios ──────────────────────────────────────────────────────────────────

/** Primary scenario: enqueue jobs as fast as possible. */
function enqueueJob() {
  const body = JSON.stringify({
    type:     randomItem(JOB_TYPES),
    payload:  { vu: __VU, iter: __ITER, ts: Date.now() },
    priority: randomItem(PRIORITIES),
  });

  const res = http.post(`${BASE_URL}/jobs`, body, { headers: HEADERS });

  enqueueLatency.add(res.timings.duration);
  enqueueErrors.add(res.status !== 201);

  check(res, {
    'enqueue status 201': (r) => r.status === 201,
    'response has id':    (r) => {
      try { return !!JSON.parse(r.body).id; } catch { return false; }
    },
  });
}

/** Mix in a small fraction of reads to simulate real traffic. */
function listJobs() {
  const res = http.get(`${BASE_URL}/jobs?limit=10`, { headers: HEADERS });
  check(res, { 'list status 200': (r) => r.status === 200 });
}

function getMetrics() {
  const res = http.get(`${BASE_URL}/prometheus`);
  check(res, { 'prometheus status 200': (r) => r.status === 200 });
}

// ── Default function ───────────────────────────────────────────────────────────

export default function () {
  const roll = Math.random();

  if (roll < 0.85) {
    enqueueJob();          // 85% — enqueue (the hot path)
  } else if (roll < 0.95) {
    listJobs();            // 10% — list
  } else {
    getMetrics();          // 5%  — prometheus scrape
  }

  // Small think-time so 100 VUs don't all fire simultaneously
  sleep(Math.random() * 0.1);
}

// ── Setup: verify the API is reachable before the load starts ─────────────────

export function setup() {
  const res = http.get(`${BASE_URL}/health`);
  if (res.status !== 200) {
    throw new Error(`API not reachable at ${BASE_URL} — got HTTP ${res.status}`);
  }
  console.log(`Load test target: ${BASE_URL}`);
}

// ── Teardown: report final queue state ────────────────────────────────────────

export function teardown() {
  const res = http.get(`${BASE_URL}/metrics`, { headers: HEADERS });
  if (res.status === 200) {
    const m = JSON.parse(res.body);
    console.log('--- Final queue state ---');
    console.log(JSON.stringify(m, null, 2));
  }
}
