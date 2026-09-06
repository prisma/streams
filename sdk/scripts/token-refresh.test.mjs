import assert from "node:assert/strict";
import test from "node:test";
import { StreamsClient } from "../dist/index.js";

function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

function stream(tokenProvider, fetch) {
  return new StreamsClient({ url: "https://cell.example", tokenProvider, fetch })
    .stream("orders", { encryptionKey: "A".repeat(43) });
}

const ok = () => new Response("{}");
const expired = () => new Response("", { status: 401 });

test("cached concurrent expiry shares one refresh and late 401s reuse it", async () => {
  const refresh = deferred();
  const allSent = deferred();
  const failures = Array.from({ length: 5 }, deferred);
  let primed = false, sent = 0, providerCalls = 0;
  const credentials = [];
  const s = stream(() => ++providerCalls === 1 ? "old" : refresh.promise, async (_url, init) => {
    credentials.push(init.headers.authorization);
    if (!primed) { primed = true; return ok(); }
    if (init.headers.authorization === "Bearer old") {
      const gate = failures[sent++];
      if (sent === failures.length) allSent.resolve();
      return gate.promise;
    }
    return ok();
  });
  await s.metadata();
  const pending = failures.map(() => s.metadata());
  await allSent.promise;
  for (const gate of failures.slice(0, 4)) gate.resolve(expired());
  // Drain promise continuations without using a timing race.
  await new Promise(setImmediate);
  assert.equal(providerCalls, 2, "one initial acquisition and one refresh");
  refresh.resolve("new");
  await Promise.all(pending.slice(0, 4));
  failures[4].resolve(expired());
  await pending[4];
  await s.metadata();
  assert.equal(providerCalls, 2, "a late rejection of old cannot replace new");
  assert.equal(credentials.at(-1), "Bearer new");
});

test("refresh rejection reaches every waiter and subsequent acquisition recovers", async () => {
  const refresh = deferred();
  const allSent = deferred();
  const failure = new Error("provider unavailable");
  let primed = false, sent = 0, calls = 0;
  const s = stream(() => {
    calls++;
    return calls === 1 ? "old" : calls === 2 ? refresh.promise : "recovered";
  }, async (_url, init) => {
    if (!primed) { primed = true; return ok(); }
    if (init.headers.authorization === "Bearer old") {
      if (++sent === 4) allSent.resolve();
      await allSent.promise;
      return expired();
    }
    return ok();
  });
  await s.metadata();
  const outcomes = Promise.allSettled(Array.from({ length: 4 }, () => s.metadata()));
  await allSent.promise;
  await new Promise(setImmediate);
  refresh.reject(failure);
  const results = await outcomes;
  assert.ok(results.every(result => result.status === "rejected" && result.reason === failure));
  assert.equal(calls, 2);
  await s.metadata();
  assert.equal(calls, 3);
});

test("synchronous provider failure is single-flight and recoverable", async () => {
  let calls = 0;
  const failure = new Error("bad provider");
  const s = stream(() => { if (++calls === 1) throw failure; return "recovered"; }, ok);
  const results = await Promise.allSettled([s.metadata(), s.metadata()]);
  assert.equal(calls, 1);
  assert.ok(results.every(result => result.status === "rejected" && result.reason === failure));
  await s.metadata();
  assert.equal(calls, 2);
});
