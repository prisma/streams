import assert from "node:assert/strict";
import test from "node:test";
import { setImmediate } from "node:timers";
import * as sdk from "../dist/index.js";

const { StreamsClient, StreamsError, StreamsTransportError } = sdk;

const KEY = "A".repeat(43);
const EPOCH = "0".repeat(32);
const APPENDED = { cursor: "c", count: 1, duplicate: false, sealed: false };
const MESSAGE = { id: "m1", routingKey: "a", attempts: 1, leaseToken: "lease-1", value: { n: 1 } };
const BUSY = '{"error":{"code":"temporarily_unavailable","message":"retry shortly","retryable":true}}';

const json = (body, headers = {}) =>
  new Response(JSON.stringify(body), { headers: { "content-type": "application/json", ...headers } });
let failures = 0;
// fetch rejects as it does for a reset connection; each failure is distinct.
const thrown = () => { const failure = new TypeError(`fetch failed #${++failures}`); return () => { throw failure; }; };
const answer = (status, body = "", headers = {}) => () => new Response(body, { status, headers });
const busy = () => answer(503, BUSY, { "content-type": "application/json", "retry-after": "0" });
const ok = body => () => json(body);
const pathOf = call => new URL(call.url).pathname;

// The server's success for every request the tests make.
function serve(call) {
  const path = pathOf(call);
  if (path === "/v1/streams") return json({ streams: [] });
  if (path.endsWith("/watches")) return json({ watches: [{ name: "w", fields: ["/a"] }] });
  if (path.includes("/watches/w/keys/")) return json({ invalidated: false, cursor: "c1" });
  if (path.endsWith(":pull")) return json({ messages: [MESSAGE], backlog: 0 });
  if (path.endsWith(":settle")) return json({ acked: 1, retried: 0, extended: 0, dlq: 0, stale: 0, backlog: 0 });
  if (path.endsWith(":scan")) return json([], { "prisma-scan-complete": "true" });
  if (path.endsWith("/records") || path.endsWith("/records:batch")) {
    return call.method === "POST" ? json(APPENDED) : json([], { "prisma-next-cursor": "c1", "prisma-up-to-date": "true" });
  }
  if (call.method === "DELETE") return new Response(null, { status: 204 });
  if (path.includes("/consumers/")) return json({ name: "workers" }, { "prisma-consumer-version": "v1" });
  return json({ name: "orders", contentType: "application/json", sealed: false, epoch: EPOCH });
}

// Requests that `match` take the scripted answers in order, then the
// server's success; every other request is served at once.
function fixture({ match = () => true, answers = [], options = {} } = {}) {
  const calls = [], matched = [];
  const client = new StreamsClient({ url: "https://cell.example", project: "proj", ...options, fetch: async (url, init) => {
    const call = { url: String(url), method: init.method, headers: { ...init.headers }, body: init.body, signal: init.signal };
    calls.push(call);
    if (!match(call)) return serve(call);
    matched.push(call);
    const scripted = answers[matched.length - 1];
    return scripted ? scripted(call) : serve(call);
  } });
  return { client, stream: client.stream("orders", { encryptionKey: KEY }), calls, matched };
}

const route = (method, path) => call => call.method === method && pathOf(call) === path;
const pulls = route("POST", "/v1/streams/orders/consumers/workers:pull");
const waits = call => call.method === "GET" && pathOf(call).startsWith("/v1/streams/orders/watches/w/keys/");

// Every backoff runs at once; the delays the SDK asked for are recorded.
async function fastTimers(run) {
  const native = globalThis.setTimeout;
  const delays = [];
  globalThis.setTimeout = (callback, delay, ...args) => { delays.push(delay); return native(callback, 0, ...args); };
  try {
    return { value: await run(), delays };
  } finally {
    globalThis.setTimeout = native;
  }
}

async function rejection(call) {
  try {
    await call;
  } catch (error) {
    return error;
  }
  assert.fail("the request resolved");
}

const drain = async () => { for (let i = 0; i < 5; i++) await new Promise(setImmediate); };

// The first timer of `delay` ms is entered and withheld; clearing it is recorded.
function holdTimer(delay) {
  const nativeSet = globalThis.setTimeout, nativeClear = globalThis.clearTimeout;
  const held = {};
  let release, cleared = false, entered;
  const enteredBackoff = new Promise(resolve => { entered = resolve; });
  globalThis.setTimeout = (callback, ms, ...args) => {
    if (ms !== delay || release) return nativeSet(callback, ms, ...args);
    release = () => callback(...args);
    entered();
    return held;
  };
  globalThis.clearTimeout = timer => {
    if (timer === held) { cleared = true; return; }
    return nativeClear(timer);
  };
  return {
    entered: enteredBackoff,
    cleared: () => cleared,
    restore() {
      if (!cleared && release) release();
      globalThis.setTimeout = nativeSet;
      globalThis.clearTimeout = nativeClear;
    },
  };
}

function observe(promise) {
  const result = { settled: false };
  result.promise = promise.then(value => { result.settled = true; return { value }; },
    error => { result.settled = true; return { error }; });
  return result;
}

// ---- req(): one budget for transport failures and 429/503 ----------------

test("metadata replays a transport failure after 1 s and resolves", async () => {
  const f = fixture({ answers: [thrown()] });
  const { value, delays } = await fastTimers(() => f.stream.metadata());
  assert.equal(value.name, "orders");
  assert.equal(f.calls.length, 2);
  assert.deepEqual(f.calls.map(c => [c.method, c.url]), [
    ["GET", "https://cell.example/v1/streams/orders"], ["GET", "https://cell.example/v1/streams/orders"],
  ]);
  assert.deepEqual(delays, [1000]);
});

test("retry: { transport: false } sends metadata once and rejects with StreamsTransportError", async () => {
  const failure = thrown();
  const f = fixture({ answers: [failure], options: { retry: { transport: false } } });
  const { value: error, delays } = await fastTimers(() => rejection(f.stream.metadata()));
  assert.equal(f.calls.length, 1);
  assert.ok(error instanceof StreamsTransportError);
  assert.equal(error.code, "transport_error");
  assert.equal(error.retryable, true);
  assert.ok(error.cause instanceof TypeError);
  assert.deepEqual(delays, []);
});

test("four transport failures are four requests, then the last as StreamsTransportError", async () => {
  const answers = [thrown(), thrown(), thrown(), thrown()];
  const f = fixture({ answers });
  const { value: error, delays } = await fastTimers(() => rejection(f.stream.metadata()));
  assert.equal(f.calls.length, 4);
  assert.ok(error instanceof StreamsTransportError);
  assert.equal(error.cause.message, `fetch failed #${failures}`);
  assert.deepEqual(delays, [1000, 2000, 4000]);
});

test("a transport failure, a retryable 503, a transport failure and a 200 share one budget and succeed", async () => {
  const f = fixture({ answers: [thrown(), busy(), thrown()] });
  const { value, delays } = await fastTimers(() => f.stream.metadata());
  assert.equal(value.name, "orders");
  assert.equal(f.calls.length, 4);
  assert.deepEqual(delays, [1000, 0, 4000], "transport waits by attempt; the 503's Retry-After: 0");
});

test("a transport failure, a 503, a transport failure and a 503 are four requests, then the 503", async () => {
  const f = fixture({ answers: [thrown(), busy(), thrown(), busy()] });
  const { value: error, delays } = await fastTimers(() => rejection(f.stream.metadata()));
  assert.equal(f.calls.length, 4);
  assert.equal(error.name, "StreamsError");
  assert.equal(error.status, 503);
  assert.equal(error.code, "temporarily_unavailable");
  assert.equal(error.retryable, true);
  assert.deepEqual(delays, [1000, 0, 4000]);
});

// The 401 refresh is not bounded by the attempt count: a 401 at the fourth
// attempt still refreshes the token once, so a request makes at most five.
for (const [label, fifth, outcome] of [
  ["a 200 succeeds", undefined, "resolved"],
  ["a transport failure rejects", thrown, "StreamsTransportError"],
]) {
  test(`three transport failures and a 401, then ${label}: five requests, the last with the refreshed token`, async () => {
    let provided = 0;
    const f = fixture({
      answers: [thrown(), thrown(), thrown(), answer(401), ...(fifth ? [fifth()] : [])],
      options: { tokenProvider: () => `t${++provided}` },
    });
    const { value: error, delays } = await fastTimers(() => f.stream.metadata().then(() => "resolved", e => e));
    assert.equal(outcome === "resolved" ? error : error.name, outcome);
    assert.equal(f.calls.length, 5);
    assert.deepEqual(f.calls.map(c => c.headers.authorization),
      ["Bearer t1", "Bearer t1", "Bearer t1", "Bearer t1", "Bearer t2"]);
    assert.deepEqual(delays, [1000, 2000, 4000]);
  });
}

// ---- which requests replay ------------------------------------------------

const consumer = f => f.stream.consumer("workers");
const replayed = [
  { name: "createStream", method: "PUT", path: "/v1/streams/orders",
    run: f => f.client.createStream("orders", { encryptionKey: KEY, format: { kind: "json" } }) },
  { name: "listStreams", method: "GET", path: "/v1/streams",
    run: async f => { const out = []; for await (const s of f.client.listStreams()) out.push(s); return out; } },
  { name: "metadata", method: "GET", path: "/v1/streams/orders", run: f => f.stream.metadata() },
  { name: "read", method: "GET", path: "/v1/streams/orders/records", run: f => f.stream.read({ routingKey: "a" }) },
  { name: "scan", method: "GET", path: "/v1/streams/orders:scan",
    run: async f => { const out = []; for await (const r of f.stream.scan()) out.push(r); return out; } },
  { name: "watches", method: "GET", path: "/v1/streams/orders/watches", run: f => f.stream.watches() },
  { name: "seal", method: "POST", path: "/v1/streams/orders:seal", run: f => f.stream.seal() },
  { name: "consumer()", method: "PUT", path: "/v1/streams/orders/consumers/workers", run: consumer },
  { name: "consumer config", method: "GET", path: "/v1/streams/orders/consumers/workers",
    run: async f => (await consumer(f)).config() },
  { name: "consumer delete", method: "DELETE", path: "/v1/streams/orders/consumers/workers",
    run: async f => (await consumer(f)).delete() },
  { name: "pull", method: "POST", path: "/v1/streams/orders/consumers/workers:pull",
    run: async f => (await consumer(f)).pull({ max: 1 }) },
  { name: "watch wait", match: waits,
    run: async f => (await f.stream.watch("w", [1])).wait({ cursor: "c0" }) },
];

// A watch wait's URL carries a capability each attempt mints for itself.
const withoutCap = url => { const u = new URL(url); u.searchParams.delete("cap"); return String(u); };

for (const op of replayed) {
  test(`${op.name} replays a transport failure: the same request twice, then success`, async () => {
    const f = fixture({ match: op.match ?? route(op.method, op.path), answers: [thrown()] });
    const { delays } = await fastTimers(() => op.run(f));
    assert.equal(f.matched.length, 2);
    const [first, second] = f.matched;
    assert.equal(second.method, first.method);
    assert.equal(withoutCap(second.url), withoutCap(first.url));
    assert.equal(second.body, first.body);
    assert.deepEqual(delays, [1000]);
  });
}

const sentOnce = [
  { name: "append", path: "/v1/streams/orders/records", run: f => f.stream.append({ n: 1 }),
    error: "AppendOutcomeUnknownError" },
  { name: "appendBytes", path: "/v1/streams/orders/records", run: f => f.stream.appendBytes(new Uint8Array([1])),
    error: "AppendOutcomeUnknownError" },
  { name: "appendMany", path: "/v1/streams/orders/records:batch", run: f => f.stream.appendMany([{ n: 1 }]),
    error: "AppendOutcomeUnknownError" },
  { name: "stream delete", method: "DELETE", path: "/v1/streams/orders", run: f => f.stream.delete(),
    error: "StreamsTransportError" },
  { name: "a producer append", path: "/v1/streams/orders/records",
    run: f => f.stream.producer("p", { ephemeral: true }).append({ n: 1 }), error: "StreamsTransportError" },
  { name: "settle", path: "/v1/streams/orders/consumers/workers:settle",
    run: async f => {
      const batch = await (await consumer(f)).pull();
      batch.messages[0].ack();
      return batch.settle();
    },
    error: "StreamsTransportError" },
];

for (const op of sentOnce) {
  test(`${op.name}: a transport failure is sent once and rejects with ${op.error}`, async () => {
    const f = fixture({ match: route(op.method ?? "POST", op.path), answers: [thrown()] });
    const { value: error, delays } = await fastTimers(() => rejection(op.run(f)));
    assert.equal(f.matched.length, 1);
    assert.equal(error.name, op.error);
    assert.equal(error.retryable, op.error === "StreamsTransportError");
    assert.deepEqual(delays, []);
  });
}

test("abort during a transport backoff ends the request at once, with no further request", async () => {
  const f = fixture({ match: pulls, answers: [thrown()] });
  const workers = await consumer(f);
  const controller = new AbortController();
  const timer = holdTimer(1000);
  const outcome = observe(workers.pull({ signal: controller.signal }));
  try {
    assert.equal(await Promise.race([timer.entered.then(() => "backoff"), outcome.promise.then(() => "settled")]), "backoff");
    await drain();
    assert.equal(outcome.settled, false, "the pull waits in the withheld backoff");
    controller.abort(new Error("caller stopped"));
    await drain();
    assert.equal(outcome.settled, true, "the abort ends the wait before its timer fires");
    assert.equal(timer.cleared(), true);
    assert.equal((await outcome.promise).error.message, "caller stopped");
    assert.equal(f.matched.length, 1);
  } finally {
    timer.restore();
    await outcome.promise;
  }
});

// ---- consumer loop ------------------------------------------------------

test("consumer.messages() reconnects through transport failures and a 503 burst, then yields", async () => {
  const f = fixture({ match: pulls, answers: [
    thrown(), thrown(), thrown(), thrown(), busy(), busy(), busy(), busy(), ok({ messages: [MESSAGE], backlog: 0 }),
  ] });
  const workers = await consumer(f);
  const iterator = workers.messages();
  const { value: first, delays } = await fastTimers(() => iterator.next());
  assert.equal(first.done, false);
  assert.equal(first.value.id, "m1");
  assert.equal(f.matched.length, 9);
  assert.deepEqual(delays, [1000, 2000, 4000, 1000, 0, 0, 0, 2000],
    "the request's budget, then the loop's backoff, twice");
  assert.deepEqual(await iterator.return(), { value: undefined, done: true });
  assert.deepEqual(await iterator.closed, { status: "closed" });
});

test("the consumer loop's backoff doubles to 5 s, resets after a pull, and ignores retry.transport", async () => {
  const f = fixture({ match: pulls, options: { retry: { transport: false } }, answers: [
    thrown(), thrown(), thrown(), thrown(), thrown(), ok({ messages: [], backlog: 0 }), thrown(),
    ok({ messages: [MESSAGE], backlog: 0 }),
  ] });
  const iterator = (await consumer(f)).messages();
  const { value: first, delays } = await fastTimers(() => iterator.next());
  assert.equal(first.value.id, "m1");
  assert.equal(f.matched.length, 8);
  assert.deepEqual(delays, [1000, 2000, 4000, 5000, 5000, 1000]);
  await iterator.return();
});

for (const [label, refusal] of [
  ["a 404", answer(404, '{"error":{"code":"consumer_not_found","message":"no such consumer","retryable":false}}')],
  ["a permanent 503", answer(503, '{"error":{"code":"configuration_invalid","message":"bad","retryable":false}}')],
  ["a 421 wrong cell", answer(421, "")],
]) {
  test(`consumer.messages() ends at ${label} after one pull`, async () => {
    const f = fixture({ match: pulls, answers: [refusal] });
    const iterator = (await consumer(f)).messages();
    const { value: error } = await fastTimers(() => rejection(iterator.next()));
    assert.ok(error instanceof StreamsError);
    assert.equal(error.retryable, false);
    assert.equal(f.matched.length, 1);
    assert.equal((await iterator.closed).status, "failed");
  });
}

test("abort during the consumer loop's backoff ends the iterator at once", async () => {
  const f = fixture({ match: pulls, answers: [thrown()], options: { retry: { transport: false } } });
  const controller = new AbortController();
  const iterator = (await consumer(f)).messages({ signal: controller.signal });
  const timer = holdTimer(1000);
  const outcome = observe(iterator.next());
  try {
    assert.equal(await Promise.race([timer.entered.then(() => "backoff"), outcome.promise.then(() => "settled")]), "backoff");
    controller.abort();
    await drain();
    assert.equal(outcome.settled, true);
    assert.equal(timer.cleared(), true);
    assert.deepEqual(await outcome.promise, { value: { value: undefined, done: true } });
    assert.equal(f.matched.length, 1);
    assert.deepEqual(await iterator.closed, { status: "closed" });
  } finally {
    timer.restore();
    await outcome.promise;
  }
});

// ---- watch loop ---------------------------------------------------------

const CHANGED = { invalidated: true, reason: "changed", cursor: "c2" };

test("watch.subscribe() rides out a transport failure and a 503 and yields the event", async () => {
  const f = fixture({ match: waits, answers: [thrown(), busy(), ok(CHANGED)] });
  const iterator = (await f.stream.watch("w", [1])).subscribe()[Symbol.asyncIterator]();
  const { value: first, delays } = await fastTimers(() => iterator.next());
  assert.deepEqual(first, { value: CHANGED, done: false });
  assert.equal(f.matched.length, 3);
  assert.deepEqual(delays, [1000, 0]);
  await iterator.return();
});

test("watch.subscribe() reconnects past the request's budget at the same cursor", async () => {
  const f = fixture({ match: waits, answers: [thrown(), thrown(), thrown(), thrown(), ok(CHANGED)] });
  const iterator = (await f.stream.watch("w", [1])).subscribe({ from: "c0" })[Symbol.asyncIterator]();
  const { value: first, delays } = await fastTimers(() => iterator.next());
  assert.deepEqual(first, { value: CHANGED, done: false });
  assert.equal(f.matched.length, 5);
  assert.deepEqual(f.matched.map(c => new URL(c.url).searchParams.get("cursor")), ["c0", "c0", "c0", "c0", "c0"]);
  assert.ok(f.matched.every(c => new URL(c.url).searchParams.get("cap")?.startsWith("proj.")));
  assert.deepEqual(delays, [1000, 2000, 4000, 1000]);
  await iterator.return();
});

test("a watch wait's replay carries a capability minted for it, not one that expired while the dropped wait hung", async () => {
  const real = Date.now;
  let now = real();
  Date.now = () => now;
  try {
    // The connection dies silently; fetch gives up on the headers 300 s later.
    const f = fixture({ match: waits, answers: [() => { now += 300_000; throw new TypeError("headers timeout"); }] });
    const watch = await f.stream.watch("w", [1]);
    const { value } = await fastTimers(() => watch.wait({ cursor: "c0" }));
    assert.equal(value.cursor, "c1");
    assert.equal(f.matched.length, 2);
    const expiry = call => Number(new URL(call.url).searchParams.get("cap").split(".")[1]);
    const seconds = Math.floor(now / 1000);
    assert.ok(expiry(f.matched[0]) < seconds, "the first wait's capability expired while it hung");
    assert.equal(expiry(f.matched[1]), seconds + 240, "the replay's capability is minted at its own attempt");
  } finally {
    Date.now = real;
  }
});

test("watch.subscribe() ends at a permanent refusal after one wait", async () => {
  const f = fixture({ match: waits, answers: [answer(403,
    '{"error":{"code":"watch_unauthorized","message":"capability refused","retryable":false}}')] });
  const iterator = (await f.stream.watch("w", [1])).subscribe()[Symbol.asyncIterator]();
  const { value: error } = await fastTimers(() => rejection(iterator.next()));
  assert.equal(error.code, "watch_unauthorized");
  assert.equal(f.matched.length, 1);
});

test("abort during a parked watch wait ends watch.subscribe() instead of rejecting", async () => {
  let entered;
  const parked = new Promise(resolve => { entered = resolve; });
  const f = fixture({ match: waits, answers: [call => new Promise((_resolve, reject) => {
    entered();
    call.signal.addEventListener("abort", () => reject(call.signal.reason), { once: true });
  })] });
  const controller = new AbortController();
  const iterator = (await f.stream.watch("w", [1])).subscribe({ signal: controller.signal })[Symbol.asyncIterator]();
  const outcome = observe(iterator.next());
  await parked;
  controller.abort();
  assert.deepEqual(await outcome.promise, { value: { value: undefined, done: true } });
  assert.equal(f.matched.length, 1);
});

test("abort during the watch loop's backoff ends watch.subscribe() at once", async () => {
  const f = fixture({ match: waits, answers: [thrown()], options: { retry: { transport: false } } });
  const controller = new AbortController();
  const iterator = (await f.stream.watch("w", [1])).subscribe({ signal: controller.signal })[Symbol.asyncIterator]();
  const timer = holdTimer(1000);
  const outcome = observe(iterator.next());
  try {
    assert.equal(await Promise.race([timer.entered.then(() => "backoff"), outcome.promise.then(() => "settled")]), "backoff");
    controller.abort();
    await drain();
    assert.equal(outcome.settled, true);
    assert.equal(timer.cleared(), true);
    assert.deepEqual(await outcome.promise, { value: { value: undefined, done: true } });
    assert.equal(f.matched.length, 1);
  } finally {
    timer.restore();
    await outcome.promise;
  }
});
