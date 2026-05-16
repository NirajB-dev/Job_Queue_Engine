/**
 * k6 smoke test — fast sanity check before a full load run.
 * 1 VU, 30 iterations — completes in ~10 seconds.
 *
 * Usage:
 *   k6 run k6/smoke_test.js
 */

import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const API_KEY  = __ENV.API_KEY  || 'local-dev-key';

const HEADERS = {
  'Content-Type': 'application/json',
  'X-API-Key': API_KEY,
};

export const options = {
  vus:        1,
  iterations: 30,
  thresholds: {
    'http_req_failed': ['rate<0.01'],
    'http_req_duration': ['p(95)<1000'],
  },
};

export function setup() {
  const res = http.get(`${BASE_URL}/health`);
  if (res.status !== 200) {
    throw new Error(`API unreachable — HTTP ${res.status}`);
  }
}

export default function () {
  // Enqueue
  const res = http.post(
    `${BASE_URL}/jobs`,
    JSON.stringify({ type: 'example.echo', payload: { smoke: true } }),
    { headers: HEADERS },
  );
  check(res, {
    'created': (r) => r.status === 201,
    'has id':  (r) => !!JSON.parse(r.body).id,
  });

  // Read it back
  if (res.status === 201) {
    const id = JSON.parse(res.body).id;
    const get = http.get(`${BASE_URL}/jobs/${id}`, { headers: HEADERS });
    check(get, { 'job found': (r) => r.status === 200 });
  }

  sleep(0.3);
}
