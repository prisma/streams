import assert from "node:assert/strict";
import test from "node:test";
import { StreamsClient } from "../dist/index.js";

function deferred() {
  let resolve;
  const promise = new Promise(yes => { resolve = yes; });
  return { promise, resolve };
}

// `settle` answers each settle request (or throws, a transport failure) and
// receives its decisions.
async function fixture(settle = () => new Response('{"acked":1}')) {
  const decisions = [];
  const consumer = await new StreamsClient({ url: "https://cell.example", fetch: async (url, init) => {
    if (String(url).endsWith(":settle")) {
      decisions.push(JSON.parse(init.body));
      return settle(decisions.at(-1));
    }
    if (String(url).endsWith(":pull")) return new Response(JSON.stringify({ backlog: 2, messages: [1, 2, 3].map(n => ({
      id: String(n), routingKey: "a", attempts: 1, leaseToken: `lease-${n}`, value: n,
    })) }));
    return new Response('{"version":"v1"}');
  } }).stream("orders", { encryptionKey: "A".repeat(43) }).consumer("workers");
  return { consumer, decisions };
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
  const { delays } = await fastTimers(async () => {
    await assert.rejects(async () => {
      for await (const message of f.consumer) { message.ack(); break; }
    }, e => e.cause === failure);
    const iterator = f.consumer[Symbol.asyncIterator]();
    const handler = new Error("handler failed too");
    await assert.rejects(async () => {
      for await (const message of iterator) { message.ack(); throw handler; }
    }, e => e === handler);
    const outcome = await iterator.closed;
    assert.equal(outcome.status, "failed");
    assert.equal(outcome.error.cause, failure);
  });
  assert.deepEqual(delays, [1000, 2000, 4000, 1000, 2000, 4000], "each settlement fails after its three replays");
  assert.equal(f.decisions.length, 8);
});

test("explicit throw retains both processing and settlement failures", async () => {
  const settlement = new Error("settle failed");
  const handler = new Error("handler failed");
  const f = await fixture(() => { throw settlement; });
  const iterator = f.consumer[Symbol.asyncIterator]();
  (await iterator.next()).value.ack();
  const { delays } = await fastTimers(() => assert.rejects(iterator.throw(handler),
    e => e instanceof AggregateError && e.errors[0] === handler && e.errors[1].cause === settlement));
  assert.deepEqual(delays, [1000, 2000, 4000]);
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
  // The server as the committer's settle (settle.rs): a decision applies to
  // a lease whose generation it names, else it counts stale. An ack removes
  // the lease, so its replay counts stale; a retry or an extend keeps the
  // lease and its generation and rewrites its deadline from the time it is
  // applied, so its replay is applied again (only a pull that delivers the
  // message again changes the generation). The first settle's answer is
  // lost after the server applied it.
  const leases = new Map(["lease-1", "lease-2", "lease-3"].map(token => [token, []]));
  const acked = [];
  let requests = 0;
  const f = await fixture(body => {
    const result = { acked: 0, retried: 0, extended: 0, dlq: 0, stale: 0, backlog: 0 };
    requests++;
    for (const { leaseToken } of body.acks) {
      if (!leases.delete(leaseToken)) { result.stale++; continue; }
      acked.push(leaseToken);
      result.acked++;
    }
    for (const [decisions, counted] of [[body.retries, "retried"], [body.extends, "extended"]]) {
      for (const { leaseToken } of decisions) {
        if (!leases.has(leaseToken)) { result.stale++; continue; }
        leases.get(leaseToken).push(`deadline set by request ${requests}`);
        result[counted]++;
      }
    }
    if (requests === 1) throw new Error("settle answer lost after the server applied it");
    return new Response(JSON.stringify(result));
  });
  const batch = await f.consumer.pull();
  batch.messages[0].ack();
  batch.messages[1].retry({ delayMs: 5 });
  batch.messages[2].extend({ visibilityMs: 100 });
  const { value: results, delays } = await fastTimers(() => Promise.all([batch.settle(), batch.settle()]));
  assert.equal(results[0], results[1], "both calls share one settlement");
  assert.deepEqual(results[0], { acked: 0, retried: 1, extended: 1, dlq: 0, stale: 1, backlog: 0 },
    "the replay counts the ack its lost first attempt applied stale and applies the retry and the extend again");
  const decisions = {
    acks: [{ leaseToken: "lease-1" }],
    retries: [{ leaseToken: "lease-2", delayMs: 5 }],
    extends: [{ leaseToken: "lease-3", visibilityMs: 100 }],
  };
  assert.deepEqual(f.decisions, [decisions, decisions], "the settlement and its one replay, with the same decisions");
  assert.deepEqual(acked, ["lease-1"], "the record is acknowledged once");
  assert.deepEqual([...leases], [
    ["lease-2", ["deadline set by request 1", "deadline set by request 2"]],
    ["lease-3", ["deadline set by request 1", "deadline set by request 2"]],
  ], "the retried and the extended lease stay held, their deadlines set again by the replay");
  assert.deepEqual(delays, [1000]);

  // A settlement whose every attempt fails rejects for every caller.
  const failure = new Error("settle failed");
  const g = await fixture(() => { throw failure; });
  const failed = await g.consumer.pull();
  failed.messages[0].ack();
  const { value: outcomes } = await fastTimers(() => Promise.allSettled([failed.settle(), failed.settle()]));
  assert.ok(outcomes.every(r => r.status === "rejected" && r.reason.cause === failure));
  assert.equal(g.decisions.length, 4, "one settlement: its four attempts");
});
