import assert from "node:assert/strict";
import test from "node:test";
import { StreamsClient } from "../dist/index.js";

function deferred() {
  let resolve;
  const promise = new Promise(yes => { resolve = yes; });
  return { promise, resolve };
}

async function fixture(settle = () => new Response('{"acked":1}')) {
  const decisions = [];
  const consumer = await new StreamsClient({ url: "https://cell.example", fetch: async (url, init) => {
    if (String(url).endsWith(":settle")) {
      decisions.push(JSON.parse(init.body));
      return settle();
    }
    if (String(url).endsWith(":pull")) return new Response(JSON.stringify({ backlog: 2, messages: [1, 2].map(n => ({
      id: String(n), routingKey: "a", attempts: 1, leaseToken: `lease-${n}`, value: n,
    })) }));
    return new Response('{"version":"v1"}');
  } }).stream("orders", { encryptionKey: "A".repeat(43) }).consumer("workers");
  return { consumer, decisions };
}

test("ack then break submits only the yielded message's decision", async () => {
  const f = await fixture();
  for await (const message of f.consumer) { message.ack(); break; }
  assert.deepEqual(f.decisions, [{ acks: [{ leaseToken: "lease-1" }], retries: [], extends: [] }]);
});

test("handler throw settles recorded decisions and retains the handler exception", async () => {
  const f = await fixture();
  const failure = new Error("handler failed");
  await assert.rejects(async () => {
    for await (const message of f.consumer) { message.ack(); throw failure; }
  }, e => e === failure);
  assert.equal(f.decisions.length, 1);
});

test("return settles retry/extend once; undecided messages produce no settlement", async () => {
  const f = await fixture();
  const iterator = f.consumer[Symbol.asyncIterator]();
  (await iterator.next()).value.retry({ delayMs: 5 });
  (await iterator.next()).value.extend({ visibilityMs: 100 });
  await iterator.return();
  await iterator.return();
  assert.deepEqual(f.decisions, [{ acks: [], retries: [{ leaseToken: "lease-1", delayMs: 5 }], extends: [{ leaseToken: "lease-2", visibilityMs: 100 }] }]);
  const undecided = f.consumer[Symbol.asyncIterator]();
  await undecided.next();
  await undecided.return();
  assert.equal(f.decisions.length, 1);
});

test("settlement failure surfaces on break and remains visible beside handler failure", async () => {
  const failure = new Error("settlement transport failed");
  const f = await fixture(() => { throw failure; });
  await assert.rejects(async () => {
    for await (const message of f.consumer) { message.ack(); break; }
  }, e => e === failure);
  const iterator = f.consumer[Symbol.asyncIterator]();
  const handler = new Error("handler failed too");
  await assert.rejects(async () => {
    for await (const message of iterator) { message.ack(); throw handler; }
  }, e => e === handler);
  assert.deepEqual(await iterator.closed, { status: "failed", error: failure });
});

test("explicit throw retains both processing and settlement failures", async () => {
  const settlement = new Error("settle failed");
  const handler = new Error("handler failed");
  const f = await fixture(() => { throw settlement; });
  const iterator = f.consumer[Symbol.asyncIterator]();
  (await iterator.next()).value.ack();
  await assert.rejects(iterator.throw(handler), e => e instanceof AggregateError && e.errors[0] === handler && e.errors[1] === settlement);
});

test("iterator return cancels an in-flight parked pull", async () => {
  const entered = deferred();
  let aborted = false;
  const consumer = await new StreamsClient({ url: "https://cell.example", fetch: async (url, init) => {
    if (!String(url).endsWith(":pull")) return new Response('{"version":"v1"}');
    entered.resolve();
    return new Promise((_resolve, reject) => {
      init.signal.addEventListener("abort", () => { aborted = true; reject(init.signal.reason); }, { once: true });
    });
  } }).stream("orders", { encryptionKey: "A".repeat(43) }).consumer("workers");
  const iterator = consumer[Symbol.asyncIterator]();
  const next = iterator.next();
  await entered.promise;
  const returned = iterator.return();
  assert.deepEqual(await next, { value: undefined, done: true });
  assert.deepEqual(await returned, { value: undefined, done: true });
  assert.equal(aborted, true);
  assert.deepEqual(await iterator.closed, { status: "closed" });
});

test("batch settlement is idempotent and failed decisions are never reported acked", async () => {
  const failure = new Error("settle failed");
  const f = await fixture(() => { throw failure; });
  const batch = await f.consumer.pull();
  batch.messages[0].ack();
  const results = await Promise.allSettled([batch.settle(), batch.settle()]);
  assert.ok(results.every(r => r.status === "rejected" && r.reason === failure));
  assert.equal(f.decisions.length, 1);
});
