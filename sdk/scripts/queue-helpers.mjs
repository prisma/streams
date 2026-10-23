// A queue cell in memory: a mocked fetch that records every request and
// answers through `route`. Shared by queue.test.mjs and queue-wakeup.test.mjs.
import { StreamsClient } from "../dist/index.js";

export const KEY = "A".repeat(43);

export function json(body, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

/** A claim that parks until its request is aborted, as a long poll does. */
export function parked(signal) {
  return new Promise((_, reject) => {
    if (signal?.aborted) reject(signal.reason);
    signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
  });
}

/** Every settle operation answered `ok`. */
export function settledOk(request) {
  return json({ results: request.body.ops.map((op) => ({ token: op.token, outcome: "ok", acceptedLate: false })) });
}

/** A raw claimed message as the server returns it. */
export function wireMessage(n, body = { n }) {
  return {
    id: `msg_${n}`, token: `lt_${n}`, attempt: 1, cycle: 1, enqueuedAt: 1, availableAt: 1, leaseDeadline: 2,
    body, bodyBase64: null, contentType: "application/json", headers: {}, key: null, dedupKey: null,
    lineage: { originId: `msg_${n}`, republishedFrom: null },
  };
}

export function cell(route, queueOptions = { encryptionKey: KEY }) {
  const requests = [];
  const fetch = async (url, init) => {
    const u = new URL(url);
    const request = {
      method: init.method,
      path: decodeURIComponent(u.pathname) + u.search,
      headers: init.headers,
      body: typeof init.body === "string" ? JSON.parse(init.body) : undefined,
      at: Date.now(),
    };
    requests.push(request);
    return route(request, init.signal);
  };
  const client = new StreamsClient({ url: "https://cell.example", token: "tok", fetch });
  return { client, queue: client.queue("emails", queueOptions), requests };
}

export const settles = (requests) => requests.filter((r) => r.path.endsWith(":settle"));
export const claims = (requests) => requests.filter((r) => r.path.endsWith(":claim"));

export function deferred() {
  let resolve;
  const promise = new Promise((r) => { resolve = r; });
  return { promise, resolve };
}

export async function until(condition, timeoutMs = 5000) {
  const start = Date.now();
  while (!condition()) {
    if (Date.now() - start > timeoutMs) throw new Error("condition not reached in time");
    await new Promise((r) => setTimeout(r, 10));
  }
}
