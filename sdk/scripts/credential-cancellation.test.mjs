import assert from "node:assert/strict";
import test from "node:test";
import { setImmediate } from "node:timers";
import { Consumer, Stream } from "../dist/index.js";

function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

const turn = () => new Promise(setImmediate);
async function drain() { for (let i = 0; i < 5; i++) await turn(); }
function observed(promise) {
  const result = { settled: false };
  result.promise = promise.then(value => { result.settled = true; return value; }, error => {
    result.settled = true; throw error;
  });
  return result;
}

function fixture(mode) {
  const provider = deferred(), entered = deferred();
  let providerCalls = 0, fetches = 0;
  const authorizations = [];
  const ctx = {
    base: "https://controlled.invalid",
    tokenProvider: () => {
      providerCalls++;
      if (mode !== "initial" && providerCalls === 1) return "old";
      entered.resolve();
      return provider.promise;
    },
    fetch: async (_url, init) => {
      fetches++;
      authorizations.push(init.headers.authorization);
      if (init.headers.authorization === "Bearer old") return new Response("", { status: 401 });
      return new Response('{"version":"v1","messages":[],"backlog":0}');
    },
  };
  const consumer = new Consumer(ctx, new Stream(ctx, "orders", "A".repeat(43)), "workers", "v1");
  return { consumer, provider, entered, authorizations, counts: () => ({ providerCalls, fetches }) };
}

for (const mode of ["initial", "refresh"]) {
  test(`iterator return ends ${mode} credential wait while a second caller keeps shared acquisition`, async () => {
    const f = fixture(mode);
    const iterator = f.consumer.messages();
    const next = observed(iterator.next());
    await f.entered.promise;
    const other = observed(f.consumer.config());
    const returned = observed(iterator.return());
    const closed = observed(iterator.closed);
    try {
      await drain();
      assert.equal(returned.settled, true, "return must settle before provider release");
      assert.equal(next.settled, true);
      assert.equal(closed.settled, true);
      assert.equal(other.settled, false, "cancellation belongs only to this request");
      assert.deepEqual(await returned.promise, { value: undefined, done: true });
      assert.deepEqual(await closed.promise, { status: "closed" });
      assert.deepEqual(f.counts(), { providerCalls: mode === "initial" ? 1 : 2, fetches: mode === "initial" ? 0 : 1 });
    } finally {
      f.provider.resolve("new");
      await Promise.allSettled([next.promise, returned.promise, closed.promise, other.promise]);
    }
    assert.equal(f.authorizations.at(-1), "Bearer new");
    await f.consumer.config();
    assert.equal(f.counts().providerCalls, mode === "initial" ? 1 : 2);
  });
}

test("external abort leaves another caller's held refresh and generation intact", async () => {
  const f = fixture("refresh");
  const other = observed(f.consumer.config());
  await f.entered.promise;
  const controller = new AbortController();
  const iterator = f.consumer.messages({ signal: controller.signal });
  const next = observed(iterator.next());
  const closed = observed(iterator.closed);
  const reason = new Error("request cancelled");
  controller.abort(reason);
  try {
    await drain();
    assert.equal(next.settled, true);
    assert.equal(closed.settled, true);
    assert.equal(other.settled, false);
    assert.deepEqual(await closed.promise, { status: "closed" });
    assert.deepEqual(f.counts(), { providerCalls: 2, fetches: 1 });
  } finally {
    f.provider.resolve("new");
    await Promise.allSettled([next.promise, closed.promise, other.promise]);
  }
  assert.equal(f.authorizations.at(-1), "Bearer new");
  assert.equal(f.counts().providerCalls, 2);
});

test("late provider rejection is observed after cancelled waits and releases abort listeners", async () => {
  const f = fixture("initial");
  const controller = new AbortController();
  let listeners = 0;
  const add = controller.signal.addEventListener.bind(controller.signal);
  const remove = controller.signal.removeEventListener.bind(controller.signal);
  controller.signal.addEventListener = (...args) => { if (args[0] === "abort") listeners++; return add(...args); };
  controller.signal.removeEventListener = (...args) => { if (args[0] === "abort") listeners--; return remove(...args); };
  const cancelled = observed(f.consumer.pull({ signal: controller.signal }).catch(error => error));
  await f.entered.promise;
  const other = f.consumer.config().catch(error => error);
  const reason = new Error("only this request stops");
  controller.abort(reason);
  try {
    await drain();
    assert.equal(cancelled.settled, true);
    assert.equal(await cancelled.promise, reason);
    assert.equal(listeners, 0);
  } finally {
    const failure = new Error("late provider rejection");
    f.provider.reject(failure);
    assert.equal(await other, failure);
    await cancelled.promise;
    await drain(); // Node's test runner also rejects any unhandled rejection.
  }
  assert.equal(listeners, 0, "late completion must not remove a listener twice");
  assert.equal(f.counts().providerCalls, 1);
});
