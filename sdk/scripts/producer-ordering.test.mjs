import assert from "node:assert/strict";
import test from "node:test";
import {
  AppendOutcomeUnknownError, ProducerFencedError, ProducerSequenceReusedError, StreamsClient, StreamsError,
  StreamsTransportError, WrongCellError,
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

// One producer id's server, refusing in the product's order: a cell that
// no longer serves the project (421, before the product handler), a sealed
// collection (refuse_if_sealed) and a quota before the core, then
// decide_producer's stale epoch (403) before its duplicate check, which
// keeps the last sequence and its request: that sequence again with another
// body is 409 producer_sequence_reused, and with the same body, or any lower
// sequence (whose body is not checked), is answered duplicate: true.
function producerServer() {
  const server = { epoch: -1, seq: -1, body: undefined, wrongCell: false, sealed: false, quota: false, stored: [] };
  const refuse = (status, code, details, headers = {}) => new Response(
    JSON.stringify({ error: { code, message: code, retryable: status === 429, details } }), { status, headers });
  server.answer = (epoch, seq, body) => {
    if (server.wrongCell) return new Response("", { status: 421 });
    if (server.sealed) return refuse(409, "sealed");
    if (server.quota) return refuse(429, "rate_limited", undefined, { "retry-after": "0" });
    if (epoch < server.epoch) return refuse(403, "stale_producer_epoch", { currentEpoch: server.epoch });
    if (epoch === server.epoch && seq === server.seq && body !== server.body) {
      return refuse(409, "producer_sequence_reused");
    }
    if (epoch === server.epoch && seq <= server.seq) {
      return new Response(JSON.stringify({ cursor: "c", count: 0, duplicate: true, sealed: false }));
    }
    Object.assign(server, { epoch, seq, body });
    server.stored.push(body);
    return new Response(JSON.stringify({ cursor: "c", count: 1, duplicate: false, sealed: false }));
  };
  return server;
}

// A producer append `{n: 1}` whose first attempt's answer is lost, after the
// server committed it unless `landed` is false; `between` then sets up the
// refusal its replays meet. Every backoff runs at once.
async function lostAnswer({ autoClaim = false, between, landed = true }) {
  const server = producerServer();
  const sent = [];
  let state = { epoch: 0, nextSeq: 0 };
  const s = new StreamsClient({ url: "https://cell.example", fetch: async (_url, init) => {
    const h = init.headers;
    sent.push([h["producer-epoch"], h["producer-seq"], init.body]);
    if (sent.length > 1) return server.answer(Number(h["producer-epoch"]), Number(h["producer-seq"]), init.body);
    if (landed) server.answer(0, 0, init.body);
    between(server);
    throw new Error(`connection lost ${landed ? "after the server committed" : "before the server read"} the append`);
  } }).stream("orders", { encryptionKey: "A".repeat(43) });
  const p = s.producer("p", { autoClaim, state: {
    load: async () => ({ ...state }), save: async (_scope, next) => { state = { ...next }; },
  } });
  const { value: outcome, delays } = await fastTimers(() =>
    p.append({ n: 1 }).then(value => ({ value }), error => ({ error })));
  return { server, sent, p, state: () => state, error: outcome.error, delays };
}

// The unknown outcome's message: what happened, then the recovery for its cause.
const refusedReplay = (answer, recovery) =>
  "the producer append's outcome is unknown: an attempt's answer was lost and the server refused its " +
  `replay (${answer}), so the SDK cannot tell whether it was written; no new epoch was claimed for it; ${recovery}`;
const FENCED =
  "another writer fenced this producer id at epoch 1: read the stream, and send the value again only if " +
  "it is absent, and only under an epoch above 1, which a producer with autoClaim claims when the resend " +
  "is refused (bumpEpoch() adds one to this producer's epoch, which can land on epoch 1, where the server " +
  "can answer the resend duplicate: true without writing it); without autoClaim, stop using this producer id";
const SEALED =
  "the collection is sealed, so the stream is final: read it to learn whether the value was written; " +
  "sending it again is refused 409 sealed before the server's duplicate check";
const RESEND =
  "send the same value on the same routing key through this producer before any other value on that key: " +
  "the server answers duplicate: true if it was written and writes it once if not; a different value sent " +
  "first is refused 409 producer_sequence_reused if it was written";
const MOVED =
  "send the same value on the same routing key through this producer id on a client for the project's new " +
  "endpoint, before any other value on that key: the server answers duplicate: true if it was written and " +
  "writes it once if not; a client made by connect keeps this producer's epoch and sequence there, while one " +
  "made with new StreamsClient({ url }) starts them at 0 unless your state store carries them over";

const fence = server => server.answer(1, 0, '{"n":"successor"}');
const quota = server => { server.quota = true; };
for (const row of [
  { label: "another writer fences the producer id, autoClaim", autoClaim: true, between: fence,
    status: 403, code: "stale_producer_epoch", attempts: 2, delays: [1000], recovery: FENCED },
  { label: "another writer fences the producer id", autoClaim: false, between: fence,
    status: 403, code: "stale_producer_epoch", attempts: 2, delays: [1000], recovery: FENCED },
  { label: "the collection is sealed", autoClaim: true, between: server => { server.sealed = true; },
    status: 409, code: "sealed", attempts: 2, delays: [1000], recovery: SEALED },
  { label: "a quota refuses every replay", autoClaim: true, between: quota,
    status: 429, code: "rate_limited", attempts: 4, delays: [1000, 0, 0], recovery: RESEND },
  { label: "the cell no longer serves the project", autoClaim: true, between: server => { server.wrongCell = true; },
    status: 421, code: "wrong_cell", attempts: 2, delays: [1000], recovery: MOVED },
]) {
  test(`a replay refused after a lost answer (${row.label}) is an unknown outcome, stored once`, async () => {
    // The first append commits and its answer is lost; the replay meets a
    // refusal the server answers before its duplicate check.
    const { server, sent, state, error, delays } = await lostAnswer(row);
    assert.equal(server.stored.filter(body => body === '{"n":1}').length, 1, "stored once");
    assert.deepEqual(sent, Array(row.attempts).fill(["0", "0", '{"n":1}']), "the append and its replays only");
    assert.ok(error instanceof AppendOutcomeUnknownError, `rejected as an unknown outcome, got ${error?.name}`);
    assert.deepEqual([error.status, error.code, error.retryable], [row.status, "outcome_unknown", false]);
    assert.equal(error.message, refusedReplay(`HTTP ${row.status} ${row.code}`, row.recovery),
      "the message names the recovery for the refusal");
    assert.ok(error.cause instanceof StreamsError, "the cause is the refusal");
    assert.deepEqual([error.cause.status, error.cause.code], [row.status, row.code]);
    if (row.code === "stale_producer_epoch") {
      assert.ok(error.cause instanceof ProducerFencedError);
      assert.equal(error.cause.currentEpoch, 1);
    }
    if (row.code === "wrong_cell") assert.ok(error.cause instanceof WrongCellError, "the cause is the wrong-cell refusal");
    assert.deepEqual(state(), { epoch: 0, nextSeq: 0 }, "no epoch claimed, no sequence consumed");
    assert.deepEqual(delays, row.delays);
  });
}

// The recoveries the messages name, followed against the same server.
for (const landed of [true, false]) {
  test(`a quota-refused unknown outcome is settled by the same value sent again (${landed ? "it was written: duplicate" : "it was not: written once"})`, async () => {
    const f = await lostAnswer({ between: quota, landed });
    assert.equal(f.error?.message, refusedReplay("HTTP 429 rate_limited", RESEND));
    f.server.quota = false;
    const result = await f.p.append({ n: 1 });
    assert.equal(result.duplicate, landed, landed ? "answered duplicate: true" : "written by the resend");
    assert.deepEqual(f.sent.at(-1), ["0", "0", '{"n":1}'], "sent again at the lost attempt's sequence");
    assert.deepEqual(f.server.stored, ['{"n":1}'], "stored once");
    assert.deepEqual(f.state(), { epoch: 0, nextSeq: 1 });
  });
}

test("after a quota-refused unknown outcome that was written, a different value is producer_sequence_reused, the same value then duplicate: true", async () => {
  const f = await lostAnswer({ between: quota });
  assert.equal(f.error?.message, refusedReplay("HTTP 429 rate_limited", RESEND));
  f.server.quota = false;
  await assert.rejects(f.p.append({ n: 2 }), e => e instanceof ProducerSequenceReusedError &&
    e.status === 409 && e.code === "producer_sequence_reused" && e.retryable === false);
  assert.deepEqual(f.state(), { epoch: 0, nextSeq: 0 }, "the refusal consumes no sequence");
  assert.equal((await f.p.append({ n: 1 })).duplicate, true, "the same value is answered duplicate: true");
  assert.equal((await f.p.append({ n: 2 })).duplicate, false, "the different value is written after it");
  assert.deepEqual(f.sent.slice(4), [["0", "0", '{"n":2}'], ["0", "0", '{"n":1}'], ["0", "1", '{"n":2}']]);
  assert.deepEqual(f.server.stored, ['{"n":1}', '{"n":2}'], "each stored once, in the order sent");
  assert.deepEqual(f.state(), { epoch: 0, nextSeq: 2 });
});

test("a fenced unknown outcome that was not written: autoClaim's resend writes it once above the fence, while bumpEpoch() can land on the fence's epoch and the resend is answered duplicate: true unwritten", async () => {
  // The other writer has gone past its epoch's sequence 0.
  const fenceTwice = server => { fence(server); server.answer(1, 1, '{"n":"successor 2"}'); };
  const claimed = await lostAnswer({ autoClaim: true, between: fenceTwice, landed: false });
  assert.equal(claimed.error?.message, refusedReplay("HTTP 403 stale_producer_epoch", FENCED));
  assert.equal((await claimed.p.append({ n: 1 })).duplicate, false, "written by the resend");
  assert.deepEqual(claimed.sent.slice(2), [["0", "0", '{"n":1}'], ["2", "0", '{"n":1}']], "refused, then claimed above 1");
  assert.deepEqual(claimed.server.stored, ['{"n":"successor"}', '{"n":"successor 2"}', '{"n":1}']);
  assert.deepEqual(claimed.state(), { epoch: 2, nextSeq: 1 });

  const bumped = await lostAnswer({ between: fenceTwice, landed: false });
  assert.equal(bumped.error?.message, refusedReplay("HTTP 403 stale_producer_epoch", FENCED));
  await assert.rejects(bumped.p.append({ n: 1 }), e => e instanceof ProducerFencedError && e.currentEpoch === 1);
  await bumped.p.bumpEpoch();
  assert.equal((await bumped.p.append({ n: 1 })).duplicate, true, "answered duplicate: true");
  assert.deepEqual(bumped.sent.at(-1), ["1", "0", '{"n":1}'], "sent on the other writer's epoch");
  assert.deepEqual(bumped.server.stored, ['{"n":"successor"}', '{"n":"successor 2"}'], "never written");
});

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
