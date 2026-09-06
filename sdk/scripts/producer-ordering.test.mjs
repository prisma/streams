import assert from "node:assert/strict";
import test from "node:test";
import { StreamsClient } from "../dist/index.js";

function deferred() {
  let resolve;
  const promise = new Promise(yes => { resolve = yes; });
  return { promise, resolve };
}

function fixture(send, save = async () => {}) {
  let state = { epoch: 0, nextSeq: 0 };
  const store = {
    load: async () => ({ ...state }),
    save: async (_scope, next) => { await save(next); state = { ...next }; },
  };
  const calls = [];
  const s = new StreamsClient({ url: "https://cell.example", fetch: async (url, init) => {
    calls.push({ url: String(url), headers: init.headers, body: init.body });
    await send(calls.length, calls.at(-1));
    return new Response(JSON.stringify({ cursor: "c", count: 1, duplicate: false, sealed: false }));
  } }).stream("orders", { encryptionKey: "A".repeat(43) });
  return { s, p: s.producer("p", { state: store }), calls, state: () => state };
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

test("fetch failure propagates and does not poison subsequent producer operations", async () => {
  const failure = new Error("connection lost: append outcome unknown");
  const f = fixture(async n => { if (n === 1) throw failure; });
  await assert.rejects(f.p.append({ n: 1 }), e => e === failure || e.cause === failure);
  await f.p.append({ n: 1 });
  await f.p.bumpEpoch();
  assert.deepEqual(f.calls.map(c => c.headers["producer-seq"]), ["0", "0"]);
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
