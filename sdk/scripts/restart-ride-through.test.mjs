import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import test from "node:test";
import { AppendOutcomeUnknownError, StreamsClient, StreamsError } from "../dist/index.js";

// A restart or a deploy leaves a cell unable to serve for 7-9 s (field round
// 2: 7.1-9.0 s), answering 503 temporarily_unavailable, "retryable": true,
// with Retry-After. The SDK replays such a refusal, honouring Retry-After
// (at most 5 s a wait), until 15 s have passed since the request's first
// attempt (edge change #177): reads, plain appends (the server states that
// nothing was written) and producer appends ride through the restart.
const KEY = "A".repeat(43);
const BUSY = JSON.stringify({ error: { code: "temporarily_unavailable", message: "retry shortly", retryable: true } });
const UNKNOWN = JSON.stringify({ error: { code: "outcome_unknown", message: "may have been written", retryable: false } });

// A cell that refuses every request for `downMs` after its first one, then
// serves: appends are stored (a producer's once per sequence), reads answer
// what is stored.
async function restartingCell(downMs) {
  const stored = [];
  const producers = new Map();
  const requests = [];
  let first;
  const server = createServer(async (req, res) => {
    let body = "";
    for await (const chunk of req) body += chunk;
    const now = Date.now();
    first ??= now;
    const path = new URL(req.url, "http://cell").pathname;
    requests.push({ at: now - first, method: req.method, path });
    if (now - first < downMs) {
      res.writeHead(503, { "content-type": "application/json", "retry-after": "1", "cache-control": "no-store" });
      return res.end(BUSY);
    }
    res.setHeader("content-type", "application/json");
    if (req.method === "POST" && path === "/v1/streams/orders/records") {
      const producer = req.headers["producer-id"];
      const seq = req.headers["producer-seq"];
      const seen = producer === undefined ? undefined : producers.get(`${producer}/${seq}`);
      if (seen !== undefined) return res.end(JSON.stringify({ cursor: seen, count: 1, duplicate: true, sealed: false }));
      stored.push(JSON.parse(body));
      const cursor = `c${stored.length}`;
      if (producer !== undefined) producers.set(`${producer}/${seq}`, cursor);
      return res.end(JSON.stringify({ cursor, count: 1, duplicate: false, sealed: false }));
    }
    if (req.method === "GET" && path === "/v1/streams/orders/records") {
      res.setHeader("streams-next-cursor", `c${stored.length}`);
      res.setHeader("streams-up-to-date", "true");
      return res.end(JSON.stringify(stored));
    }
    res.writeHead(404);
    res.end(JSON.stringify({ error: { code: "not_found", message: path, retryable: false } }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const client = new StreamsClient({ url: `http://127.0.0.1:${server.address().port}` });
  return {
    stored,
    requests,
    orders: client.stream("orders", { encryptionKey: KEY }),
    close() {
      server.closeAllConnections();
      server.close();
    },
  };
}

// The clock the SDK reads: it stands still but for the waits the SDK asks
// for, each of which passes at once and moves Date.now by the wait.
// Returns the waits asked for.
async function virtualClock(run) {
  const nativeTimeout = globalThis.setTimeout;
  const nativeNow = Date.now;
  let offset = 0;
  const waits = [];
  globalThis.setTimeout = (callback, delay = 0, ...args) => {
    waits.push(delay);
    offset += delay;
    return nativeTimeout(callback, 0, ...args);
  };
  const base = nativeNow();
  Date.now = () => base + offset;
  try {
    return { value: await run(), waits };
  } finally {
    globalThis.setTimeout = nativeTimeout;
    Date.now = nativeNow;
  }
}

function busyClient(answer) {
  const sent = [];
  const client = new StreamsClient({
    url: "https://cell.example",
    fetch: async (url, init) => {
      sent.push({ at: Date.now(), method: init.method });
      return answer();
    },
  });
  return { sent, orders: client.stream("orders", { encryptionKey: KEY }) };
}

const busy = retryAfter => () => new Response(BUSY, {
  status: 503,
  headers: { "content-type": "application/json", ...(retryAfter === undefined ? {} : { "retry-after": retryAfter }) },
});

async function rejection(promise) {
  try {
    await promise;
  } catch (error) {
    return error;
  }
  assert.fail("the request resolved");
}

for (const [label, operation] of [
  ["read", orders => orders.read()],
  ["plain append", orders => orders.append({ n: 1 })],
  ["producer append", orders => orders.producer("p1", { ephemeral: true }).append({ n: 1 })],
]) {
  test(`${label}: a 503 + Retry-After: 1 that lasts is replayed every second until 15 s have passed, then surfaces`, async () => {
    const cell = busyClient(busy("1"));
    const { value: error, waits } = await virtualClock(() => rejection(operation(cell.orders)));
    assert.ok(error instanceof StreamsError && !(error instanceof AppendOutcomeUnknownError), String(error));
    assert.equal(error.status, 503);
    assert.equal(error.code, "temporarily_unavailable");
    assert.equal(error.retryable, true);
    assert.deepEqual(waits, Array(15).fill(1000));
    assert.equal(cell.sent.length, 16, "the first attempt and 15 replays");
  });
}

test("a long Retry-After is waited at most 5 s a time, and the last wait ends at 15 s", async () => {
  const cell = busyClient(busy("7"));
  const { waits } = await virtualClock(() => rejection(cell.orders.read()));
  assert.deepEqual(waits, [5000, 5000, 5000]);
  assert.equal(cell.sent.length, 4);
});

test("Retry-After: 0 is honoured for the first three replays and waited 1 s a time after them", async () => {
  const cell = busyClient(busy("0"));
  const { waits } = await virtualClock(() => rejection(cell.orders.read()));
  assert.deepEqual(waits, [0, 0, 0, ...Array(15).fill(1000)]);
});

test("a 503 without Retry-After keeps the three replays it had", async () => {
  const cell = busyClient(busy(undefined));
  const { value: error, waits } = await virtualClock(() => rejection(cell.orders.read()));
  assert.equal(error.code, "temporarily_unavailable");
  assert.deepEqual(waits, [1000, 1000, 1000]);
  assert.equal(cell.sent.length, 4);
});

test("a 429 keeps the three replays it had", async () => {
  const limited = () => new Response(JSON.stringify({ error: { code: "rate_limited", message: "x", retryable: true } }), {
    status: 429,
    headers: { "content-type": "application/json", "retry-after": "1" },
  });
  const cell = busyClient(limited);
  const { value: error, waits } = await virtualClock(() => rejection(cell.orders.read()));
  assert.equal(error.status, 429);
  assert.deepEqual(waits, [1000, 1000, 1000]);
});

test("a plain append's 503 outcome_unknown is never sent again, Retry-After or not", async () => {
  const cell = busyClient(() => new Response(UNKNOWN, {
    status: 503,
    headers: { "content-type": "application/json", "retry-after": "1" },
  }));
  const { value: error, waits } = await virtualClock(() => rejection(cell.orders.append({ n: 1 })));
  assert.ok(error instanceof AppendOutcomeUnknownError, String(error));
  assert.deepEqual(waits, []);
  assert.equal(cell.sent.length, 1);
});

test("a plain append's 503 without the server's body (an edge's answer) is never sent again", async () => {
  const cell = busyClient(() => new Response("", { status: 503, headers: { "retry-after": "1" } }));
  const { value: error } = await virtualClock(() => rejection(cell.orders.append({ n: 1 })));
  assert.ok(error instanceof AppendOutcomeUnknownError, String(error));
  assert.equal(cell.sent.length, 1);
});

// Last: the real fetch below leaves keep-alive timers that the virtual
// clock above would count.
test("a read, a plain append and a producer append ride through a 9 s restart of 503 + Retry-After and land once", { timeout: 30_000 }, async () => {
  const cell = await restartingCell(9_000);
  try {
    const started = Date.now();
    const producer = cell.orders.producer("p1", { ephemeral: true });
    const [read, plain, produced] = await Promise.all([
      cell.orders.read(),
      cell.orders.append({ n: "plain" }),
      producer.append({ n: "producer" }),
    ]);
    const elapsed = Date.now() - started;
    assert.ok(elapsed >= 9_000 && elapsed < 15_000, `elapsed ${elapsed} ms`);
    assert.deepEqual(read.records, []);
    assert.equal(plain.duplicate, false);
    assert.equal(produced.duplicate, false);
    assert.deepEqual(cell.stored.map(r => r.n).sort(), ["plain", "producer"]);
    // Each request was refused about once a second (Retry-After: 1), and
    // its first attempt after the restart was served.
    for (const method of ["GET", "POST"]) {
      const sent = cell.requests.filter(r => r.method === method).length;
      const expected = method === "GET" ? 1 : 2;
      assert.ok(sent >= expected * 9 && sent <= expected * 11, `${method}: ${sent} requests`);
    }
  } finally {
    cell.close();
  }
});
