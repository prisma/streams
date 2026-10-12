import assert from "node:assert/strict";
import test from "node:test";
import {
  AppendOutcomeUnknownError, ProducerFencedError, StreamsClient, StreamsError, StreamsTransportError,
} from "../dist/index.js";

function deferred() {
  let resolve;
  const promise = new Promise(yes => { resolve = yes; });
  return { promise, resolve };
}

// `send` may throw (a transport failure) or answer; by default the append lands.
function fixture(send, save = async () => {}) {
  let state = { epoch: 0, nextSeq: 0 };
  const store = {
    load: async () => ({ ...state }),
    save: async (_scope, next) => { await save(next); state = { ...next }; },
  };
  const calls = [];
  const s = new StreamsClient({ url: "https://cell.example", fetch: async (url, init) => {
    calls.push({ url: String(url), headers: { ...init.headers }, body: init.body });
    return await send(calls.length, calls.at(-1)) ??
      new Response(JSON.stringify({ cursor: "c", count: 1, duplicate: false, sealed: false }));
  } }).stream("orders", { encryptionKey: "A".repeat(43) });
  return { s, p: s.producer("p", { state: store }), calls, state: () => state };
}

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

test("append, epoch bump, batch append and final seal share one scope order", async () => {
  const entered = deferred(), release = deferred();
  const f = fixture(async n => { if (n === 1) { entered.resolve(); await release.promise; } });
  const first = f.p.append({ n: 1 }, { routingKey: "a" });
  await entered.promise;
  const bump = f.p.bumpEpoch("a");
  const batch = f.p.appendMany([{ n: 2 }, { n: 3 }], { routingKey: "a" });
  const seal = f.s.seal({ final: { n: 4 }, producer: f.p, routingKey: "a" });
  await new Promise(setImmediate);
  assert.deepEqual(f.state(), { epoch: 0, nextSeq: 0 }, "bump waits for earlier append");
  release.resolve();
  await Promise.all([first, bump, batch, seal]);
  assert.deepEqual(f.state(), { epoch: 1, nextSeq: 2 });
  assert.deepEqual(f.calls.map(c => [c.headers["producer-epoch"], c.headers["producer-seq"]]),
    [["0", "0"], ["1", "0"], ["1", "1"]]);
  assert.ok(f.calls[1].url.endsWith("/records:batch"));
  assert.ok(f.calls[2].url.endsWith(":seal"));
});

test("a fetch failure is replayed at the same sequence before later operations and lands once", async () => {
  // The server commits each (epoch, sequence) once and answers a repeat as
  // a duplicate (count 0); the first append's answer is lost after it committed.
  const committed = [];
  const f = fixture(async (n, call) => {
    const at = `${call.headers["producer-epoch"]}/${call.headers["producer-seq"]}`;
    const duplicate = committed.includes(at);
    if (!duplicate) committed.push(at);
    if (n === 1) throw new Error("connection lost after the server committed the append");
    return new Response(JSON.stringify({ cursor: "c", count: duplicate ? 0 : 1, duplicate, sealed: false }));
  });
  const { value: results, delays } = await fastTimers(() => Promise.all([
    f.p.append({ n: 1 }),
    f.p.append({ n: 2 }),
    f.p.bumpEpoch(),
    f.p.appendMany([{ n: 3 }, { n: 4 }]),
  ]));
  assert.deepEqual(delays, [1000]);
  assert.deepEqual(f.calls.map(c => [c.headers["producer-id"], c.headers["producer-epoch"], c.headers["producer-seq"], c.body]), [
    ["p", "0", "0", '{"n":1}'],
    ["p", "0", "0", '{"n":1}'],
    ["p", "0", "1", '{"n":2}'],
    ["p", "1", "0", '[{"n":3},{"n":4}]'],
  ], "the replay is the same append, sent before the next operation of the scope");
  assert.deepEqual(committed, ["0/0", "0/1", "1/0"], "each sequence lands once");
  assert.deepEqual([results[0].duplicate, results[1].duplicate, results[3].duplicate], [true, false, false]);
  assert.deepEqual(f.state(), { epoch: 1, nextSeq: 1 });
});

// One producer id's server, refusing in the product's order: a sealed
// collection (refuse_if_sealed) and a quota before the core, then
// decide_producer's stale epoch (403) before its duplicate check.
function producerServer() {
  const server = { epoch: -1, seq: -1, sealed: false, quota: false, stored: [] };
  const refuse = (status, code, details, headers = {}) => new Response(
    JSON.stringify({ error: { code, message: code, retryable: status === 429, details } }), { status, headers });
  server.answer = (epoch, seq, body) => {
    if (server.sealed) return refuse(409, "sealed");
    if (server.quota) return refuse(429, "rate_limited", undefined, { "retry-after": "0" });
    if (epoch < server.epoch) return refuse(403, "stale_producer_epoch", { currentEpoch: server.epoch });
    if (epoch === server.epoch && seq <= server.seq) {
      return new Response(JSON.stringify({ cursor: "c", count: 0, duplicate: true, sealed: false }));
    }
    Object.assign(server, { epoch, seq });
    server.stored.push(body);
    return new Response(JSON.stringify({ cursor: "c", count: 1, duplicate: false, sealed: false }));
  };
  return server;
}

const fence = server => server.answer(1, 0, '{"n":"successor"}');
for (const row of [
  { label: "another writer fences the producer id, autoClaim", autoClaim: true, between: fence,
    status: 403, code: "stale_producer_epoch", attempts: 2, delays: [1000] },
  { label: "another writer fences the producer id", autoClaim: false, between: fence,
    status: 403, code: "stale_producer_epoch", attempts: 2, delays: [1000] },
  { label: "the collection is sealed", autoClaim: true, between: server => { server.sealed = true; },
    status: 409, code: "sealed", attempts: 2, delays: [1000] },
  { label: "a quota refuses every replay", autoClaim: true, between: server => { server.quota = true; },
    status: 429, code: "rate_limited", attempts: 4, delays: [1000, 0, 0] },
]) {
  test(`a replay refused after a lost answer (${row.label}) is an unknown outcome, stored once`, async () => {
    // The first append commits and its answer is lost; the replay meets a
    // refusal the server answers before its duplicate check.
    const server = producerServer();
    const sent = [];
    let state = { epoch: 0, nextSeq: 0 };
    const s = new StreamsClient({ url: "https://cell.example", fetch: async (_url, init) => {
      const h = init.headers;
      sent.push([h["producer-epoch"], h["producer-seq"], init.body]);
      const answer = server.answer(Number(h["producer-epoch"]), Number(h["producer-seq"]), init.body);
      if (sent.length > 1) return answer;
      row.between(server);
      throw new Error("connection lost after the server committed the append");
    } }).stream("orders", { encryptionKey: "A".repeat(43) });
    const p = s.producer("p", { autoClaim: row.autoClaim, state: {
      load: async () => ({ ...state }), save: async (_scope, next) => { state = { ...next }; },
    } });
    const { value: outcome, delays } = await fastTimers(() =>
      p.append({ n: 1 }).then(value => ({ value }), error => ({ error })));
    assert.equal(server.stored.filter(body => body === '{"n":1}').length, 1, "stored once");
    assert.deepEqual(sent, Array(row.attempts).fill(["0", "0", '{"n":1}']), "the append and its replays only");
    const { error } = outcome;
    assert.ok(error instanceof AppendOutcomeUnknownError, `rejected as an unknown outcome, got ${error?.name}`);
    assert.deepEqual([error.status, error.code, error.retryable], [row.status, "outcome_unknown", false]);
    assert.equal(error.message,
      "the producer append's outcome is unknown: an attempt's answer was lost and the server refused its " +
      `replay (HTTP ${row.status} ${row.code}), so the SDK cannot tell whether it was written; no new ` +
      "epoch was claimed for it; read the stream before sending it again");
    assert.ok(error.cause instanceof StreamsError, "the cause is the refusal");
    assert.deepEqual([error.cause.status, error.cause.code], [row.status, row.code]);
    if (row.code === "stale_producer_epoch") {
      assert.ok(error.cause instanceof ProducerFencedError);
      assert.equal(error.cause.currentEpoch, 1);
    }
    assert.deepEqual(state, { epoch: 0, nextSeq: 0 }, "no epoch claimed, no sequence consumed");
    assert.deepEqual(delays, row.delays);
  });
}

test("fetch failure propagates and does not poison subsequent producer operations", async () => {
  const failure = new Error("connection lost: append outcome unknown");
  const f = fixture(async n => { if (n <= 4) throw failure; });
  const { delays } = await fastTimers(() =>
    assert.rejects(f.p.append({ n: 1 }),
      e => e instanceof StreamsTransportError && e.code === "transport_error" && e.cause === failure));
  assert.deepEqual(delays, [1000, 2000, 4000], "the failure outlasts the request's three replays");
  await f.p.append({ n: 1 });
  await f.p.bumpEpoch();
  assert.deepEqual(f.calls.map(c => c.headers["producer-seq"]), ["0", "0", "0", "0", "0"]);
  assert.deepEqual(f.state(), { epoch: 1, nextSeq: 0 });
});

test("state-save failure propagates and a same-payload retry preserves sequence", async () => {
  let saves = 0;
  const failure = new Error("state persistence failed");
  const f = fixture(async () => {}, async () => { if (++saves === 1) throw failure; });
  await assert.rejects(f.p.append({ n: 1 }), e => e === failure);
  await f.p.append({ n: 1 });
  await f.p.bumpEpoch();
  assert.deepEqual(f.calls.map(c => c.headers["producer-seq"]), ["0", "0"]);
  assert.deepEqual(f.state(), { epoch: 1, nextSeq: 0 });
});

test("automatic reclaim completes within the queue before a later epoch bump", async () => {
  const entered = deferred(), release = deferred();
  let state = { epoch: 0, nextSeq: 0 };
  const headers = [];
  const s = new StreamsClient({ url: "https://cell.example", fetch: async (_url, init) => {
    headers.push(init.headers);
    if (headers.length === 1) {
      entered.resolve();
      await release.promise;
      return new Response(JSON.stringify({ error: { code: "stale_producer_epoch", details: { currentEpoch: 4 } } }), { status: 409 });
    }
    return new Response("{}");
  } }).stream("orders", { encryptionKey: "A".repeat(43) });
  const p = s.producer("p", { autoClaim: true, state: {
    load: async () => ({ ...state }), save: async (_scope, next) => { state = next; },
  } });
  const append = p.append({ n: 1 });
  await entered.promise;
  const bump = p.bumpEpoch();
  release.resolve();
  await Promise.all([append, bump]);
  assert.deepEqual(headers.map(h => [h["producer-epoch"], h["producer-seq"]]), [["0", "0"], ["5", "0"]]);
  assert.deepEqual(state, { epoch: 6, nextSeq: 0 });
});

test("one million sparse producer scopes release every fulfilled and rejected chain", async () => {
  const f = fixture(async () => {});
  const scopes = 1_000_000, concurrency = 128;
  const failure = new Error("controlled queue operation failure");
  let entered = 0, rejected = 0, peak = 0;
  for (let first = 0; first < scopes; first += concurrency) {
    const release = deferred();
    const pending = [];
    for (let key = first; key < Math.min(first + concurrency, scopes); key++) {
      // Exercise the actual queue primitive used by append, bump and seal.
      // The held cohort proves in-flight cardinality before terminal cleanup.
      pending.push(f.p._chain(`sparse-${key}`, async () => {
        entered++;
        await release.promise;
        if (key % 17 === 0) throw failure;
      }));
    }
    peak = Math.max(peak, f.p.chains.size);
    assert.equal(f.p.chains.size, pending.length);
    release.resolve();
    const outcomes = await Promise.allSettled(pending);
    for (const outcome of outcomes) {
      if (outcome.status === "rejected") {
        assert.equal(outcome.reason, failure);
        rejected++;
      }
    }
    assert.equal(f.p.chains.size, 0, "completed routing keys must be evicted");
  }
  assert.equal(entered, scopes);
  assert.equal(rejected, Math.floor((scopes - 1) / 17) + 1);
  assert.equal(peak, concurrency);
});
