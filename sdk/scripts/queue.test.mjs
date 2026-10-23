// The queue handle's request shapes and the worker runner (QUEUES-DESIGN §9.1),
// against a mocked fetch.
import assert from "node:assert/strict";
import test from "node:test";
import { HandlerAbortedError, RetryLater, StreamsError } from "../dist/index.js";
import { KEY, cell, claims, deferred, json, parked, settledOk, settles, until, wireMessage } from "./queue-helpers.mjs";

const sent = { id: "msg_1", enqueuedAt: 10, availableAt: 20, duplicate: false };

// ---- send ---------------------------------------------------------------

test("send posts the body, its options and the stream key in the contract's shape", async () => {
  const { queue, requests } = cell(() => json(sent, 201));
  const result = await queue.send({ notificationId: "n1" }, {
    key: "workspace_123", dedupKey: "notification:1", delay: "2m", headers: { trace: "abc" },
  });
  assert.deepEqual(result, sent);
  assert.equal(requests.length, 1);
  const [r] = requests;
  assert.equal(r.method, "POST");
  assert.equal(r.path, "/v1/queues/emails/messages");
  assert.equal(r.headers["streams-encryption-key"], KEY);
  assert.equal(r.headers["authorization"], "Bearer tok");
  assert.deepEqual(r.body, {
    body: { notificationId: "n1" }, contentType: "application/json", headers: { trace: "abc" },
    key: "workspace_123", dedupKey: "notification:1", delayMs: 120_000,
  });
});

test("send takes deliverAt as a Date or Unix ms, and bytes as bodyBase64", async () => {
  const { queue, requests } = cell(() => json(sent, 201));
  await queue.send({ a: 1 }, { deliverAt: new Date(1_792_600_120_000) });
  await queue.send(new Uint8Array([0, 1, 2, 255]), { deliverAt: 5, contentType: "application/x-raw" });
  assert.deepEqual(requests[0].body, { body: { a: 1 }, contentType: "application/json", deliverAt: 1_792_600_120_000 });
  assert.deepEqual(requests[1].body, { bodyBase64: "AAEC/w==", contentType: "application/x-raw", deliverAt: 5 });
});

test("send refuses delay with deliverAt, and a missing key, before any request", async () => {
  const { queue, requests } = cell(() => json(sent, 201));
  await assert.rejects(queue.send({}, { delay: 5, deliverAt: 5 }),
    (e) => e instanceof StreamsError && e.code === "invalid_send_options" && e.status === 0);
  const keyless = cell(() => json(sent, 201), {});
  await assert.rejects(keyless.queue.send({}),
    (e) => e instanceof StreamsError && e.code === "encryption_key_required");
  await assert.rejects(queue.send({}, { delay: "soon" }), RangeError);
  assert.equal(requests.length + keyless.requests.length, 0);
});

test("sendBatch posts every message in one atomic batch and returns the results", async () => {
  const results = [sent, { ...sent, id: "msg_2", duplicate: true }];
  const { queue, requests } = cell(() => json({ results }, 201));
  const answer = await queue.sendBatch([
    { body: { n: 1 }, dedupKey: "d1" },
    { body: { n: 2 }, dedupKey: "d2", delay: 1500, key: "k" },
  ]);
  assert.deepEqual(answer, results);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].path, "/v1/queues/emails/messages:batch");
  assert.deepEqual(requests[0].body, { messages: [
    { body: { n: 1 }, contentType: "application/json", dedupKey: "d1" },
    { body: { n: 2 }, contentType: "application/json", key: "k", dedupKey: "d2", delayMs: 1500 },
  ] });
});

test("a send without dedupKey is not sent again after a lost answer; one with it is", async () => {
  let calls = 0;
  const flaky = () => {
    calls++;
    if (calls % 2 === 1) throw new TypeError("connection reset");
    return json(sent, 201);
  };
  const plain = cell(flaky);
  await assert.rejects(plain.queue.send({ n: 1 }), (e) => e.code === "transport_error");
  assert.equal(plain.requests.length, 1);
  calls = 0;
  const deduped = cell(flaky);
  assert.deepEqual(await deduped.queue.send({ n: 1 }, { dedupKey: "d" }), sent);
  assert.equal(deduped.requests.length, 2);
});

// ---- claim and the handle's other requests ------------------------------

test("claim posts its options, parses bodies, and marks a body parse rejects invalid", async () => {
  const { client, requests } = cell(() => json({ messages: [wireMessage(1, { n: 1 }), wireMessage(2, "bad")] }));
  const queue = client.queue("emails", {
    encryptionKey: KEY,
    parse: (b) => { if (typeof b !== "object") throw new Error("not an object"); return { parsed: b.n }; },
  });
  const { messages, paused } = await queue.claim({ max: 2, maxBytes: 4096, lease: "5m", waitMs: 0 });
  assert.equal(paused, false);
  assert.equal(requests[0].path, "/v1/queues/emails/groups/default:claim");
  assert.deepEqual(requests[0].body, { max: 2, maxBytes: 4096, leaseMs: 300_000, waitMs: 0 });
  assert.equal(requests[0].headers["streams-encryption-key"], KEY);
  assert.deepEqual(messages[0].body, { parsed: 1 });
  assert.equal(messages[0].invalid, undefined);
  assert.equal(messages[1].invalid.error.message, "not an object");
});

test("operator requests use the contract's paths and carry the key only where it is needed", async () => {
  const { queue, requests } = cell((r) => {
    if (r.path.endsWith("/wakeup") && r.method === "DELETE") return new Response(null, { status: 204 });
    if (r.path.includes(":republish")) return json({ results: [{ from: "msg_1", id: "msg_9" }] });
    if (r.path.includes("/failed") || r.path.includes("/pending")) return json({ messages: [], next: null });
    return json({});
  });
  await queue.pause();
  await queue.resume();
  await queue.deadLetters({ limit: 10, cursor: "c1" });
  await queue.peek();
  await queue.redrive(["msg_1"]);
  await queue.redrive({ all: true, limit: 50 });
  await queue.discard(["msg_2"]);
  await queue.cancel(["msg_3"]);
  assert.deepEqual(await queue.republish(["msg_1"]), [{ from: "msg_1", id: "msg_9" }]);
  await queue.setWakeup("https://example.com/wake");
  await queue.deleteWakeup();
  await queue.settle([{ token: "lt_1", op: "ack" }]);
  await queue.settle([{ token: "lt_1", op: "fail", code: "x", error: "text" }]);
  const shape = requests.map((r) => [r.method, r.path, r.body, "streams-encryption-key" in r.headers]);
  assert.deepEqual(shape, [
    ["POST", "/v1/queues/emails/groups/default:pause", undefined, false],
    ["POST", "/v1/queues/emails/groups/default:resume", undefined, false],
    ["GET", "/v1/queues/emails/groups/default/failed?limit=10&cursor=c1", undefined, true],
    ["GET", "/v1/queues/emails/groups/default/pending", undefined, true],
    ["POST", "/v1/queues/emails/groups/default:redrive", { ids: ["msg_1"] }, false],
    ["POST", "/v1/queues/emails/groups/default:redrive", { all: true, limit: 50 }, false],
    ["POST", "/v1/queues/emails/groups/default:discard", { ids: ["msg_2"] }, false],
    ["POST", "/v1/queues/emails:cancel", { ids: ["msg_3"] }, false],
    ["POST", "/v1/queues/emails:republish", { ids: ["msg_1"] }, true],
    ["PUT", "/v1/queues/emails/groups/default/wakeup", { url: "https://example.com/wake" }, false],
    ["DELETE", "/v1/queues/emails/groups/default/wakeup", undefined, false],
    ["POST", "/v1/queues/emails/groups/default:settle", { ops: [{ token: "lt_1", op: "ack" }] }, false],
    ["POST", "/v1/queues/emails/groups/default:settle",
      { ops: [{ token: "lt_1", op: "fail", code: "x", error: "text" }] }, true],
  ]);
});

test("create sends the config with the key and says whether it created the queue", async () => {
  let status = 201;
  const { queue, requests } = cell(() => json({ name: "emails", config: { retentionMs: 1 } }, status));
  assert.deepEqual(await queue.create({ retentionMs: 1 }), { name: "emails", config: { retentionMs: 1 }, created: true });
  status = 200;
  assert.equal((await queue.create({ retentionMs: 1 })).created, false);
  assert.deepEqual([requests[0].method, requests[0].path, requests[0].body], ["PUT", "/v1/queues/emails", { retentionMs: 1 }]);
  assert.equal(requests[0].headers["streams-encryption-key"], KEY);
});

// ---- the runner ---------------------------------------------------------

/** A cell whose first claim answers `batch`, later claims park; settles answer ok. */
function workerCell(batch, settle = settledOk) {
  let claimed = 0;
  return cell((r, signal) => {
    if (r.path.endsWith(":claim")) return claimed++ === 0 ? json({ messages: batch }) : parked(signal);
    if (r.path.endsWith(":settle")) return settle(r);
    throw new Error(`unexpected ${r.method} ${r.path}`);
  });
}

test("the runner claims its free slots and coalesces the settles of one window into one request", async () => {
  const { queue, requests } = workerCell([wireMessage(1), wireMessage(2), wireMessage(3)]);
  const settled = [];
  const worker = queue.process(async (msg) => {
    await new Promise((r) => setTimeout(r, 20 * msg.body.n));
  }, { concurrency: 3, lease: "5m", onSettle: (msg, op, outcome) => settled.push([msg.id, op.op, outcome.outcome]) });
  await until(() => settled.length === 3);
  assert.deepEqual(claims(requests)[0].body, { max: 3, leaseMs: 300_000, waitMs: 25_000 });
  const s = settles(requests);
  assert.equal(s.length, 1, "three acks inside 250 ms are one settle request");
  assert.deepEqual(s[0].body.ops, [
    { token: "lt_1", op: "ack" }, { token: "lt_2", op: "ack" }, { token: "lt_3", op: "ack" },
  ]);
  assert.equal("streams-encryption-key" in s[0].headers, false);
  await worker.stop();
  await worker.closed;
});

test("handler outcomes map to settle operations: RetryLater's delay, errors, fail and discard", async () => {
  const batch = [1, 2, 3, 4, 5].map((n) => wireMessage(n));
  const { queue, requests } = workerCell(batch);
  const worker = queue.process((msg) => {
    switch (msg.body.n) {
      case 1: throw new RetryLater({ after: "2m", code: "rate_limited" });
      case 2: throw new Error("boom");
      case 3: return msg.fail("unsubscribed", "the address unsubscribed");
      case 4: return msg.discard("obsolete");
      default: return "anything";
    }
  }, { concurrency: 5 });
  await until(() => settles(requests).length === 1);
  const [s] = settles(requests);
  assert.deepEqual(s.body.ops, [
    { token: "lt_1", op: "retry", delayMs: 120_000, code: "rate_limited" },
    { token: "lt_2", op: "retry", code: "handler_error", error: "Error: boom" },
    { token: "lt_3", op: "fail", code: "unsubscribed", error: "the address unsubscribed" },
    { token: "lt_4", op: "discard", code: "obsolete" },
    { token: "lt_5", op: "ack" },
  ]);
  assert.equal(s.headers["streams-encryption-key"], KEY, "error text needs the key");
  await worker.stop();
});

test("an invalid body is retried by default and failed with invalid_payload under onInvalid: fail", async () => {
  for (const [onInvalid, op] of [[undefined, "retry"], ["fail", "fail"]]) {
    let claimed = 0;
    const { client, requests } = cell((r, signal) => {
      if (r.path.endsWith(":claim")) return claimed++ === 0 ? json({ messages: [wireMessage(1, "x")] }) : parked(signal);
      return settledOk(r);
    });
    const queue = client.queue("emails", { encryptionKey: KEY, onInvalid, parse: () => { throw new Error("schema"); } });
    let called = false;
    const worker = queue.process(() => { called = true; });
    await until(() => settles(requests).length === 1);
    assert.equal(called, false);
    assert.deepEqual(settles(requests)[0].body.ops,
      [{ token: "lt_1", op, code: "invalid_payload", error: "Error: schema" }]);
    await worker.stop();
  }
});

test("auto-extend extends at half the lease while the handler runs", async () => {
  const { queue, requests } = workerCell([wireMessage(1)]);
  const worker = queue.process(() => new Promise((r) => setTimeout(r, 900)), { lease: 200 });
  await until(() => settles(requests).some((s) => s.body.ops.some((o) => o.op === "ack")));
  const ops = settles(requests).flatMap((s) => s.body.ops);
  const extendOps = ops.filter((o) => o.op === "extend");
  assert.ok(extendOps.length >= 2, `extended ${extendOps.length} times`);
  assert.deepEqual(extendOps[0], { token: "lt_1", op: "extend", leaseMs: 200 });
  assert.deepEqual(ops.at(-1), { token: "lt_1", op: "ack" });
  await worker.stop();
});

for (const outcome of ["stale", "expired"]) {
  test(`an extend answered ${outcome} aborts the handler's signal`, async () => {
    const { queue, requests } = workerCell([wireMessage(1)], (r) => json({
      results: r.body.ops.map((op) => ({ token: op.token, outcome: op.op === "extend" ? outcome : "stale", acceptedLate: false })),
    }));
    let reason;
    const worker = queue.process((_msg, { signal }) => new Promise((_, reject) => {
      signal.addEventListener("abort", () => { reason = signal.reason; reject(signal.reason); });
    }), { lease: 100 });
    await until(() => reason !== undefined);
    assert.ok(reason instanceof HandlerAbortedError);
    assert.equal(reason.reason, outcome === "stale" ? "lease_stale" : "lease_expired");
    await until(() => settles(requests).some((s) => s.body.ops.some((o) => o.op === "retry")));
    assert.equal(settles(requests).flatMap((s) => s.body.ops).filter((o) => o.op === "extend").length, 1,
      "no extend after the lease was lost");
    await worker.stop();
  });
}

test("stop() releases work claimed after it began and never starts it", async () => {
  const stopping = deferred();
  const { queue, requests } = cell(async (r) => {
    if (r.path.endsWith(":claim")) {
      // The answer arrives after stop(): the claim was already leased.
      await stopping.promise;
      return json({ messages: [wireMessage(1), wireMessage(2)] });
    }
    return settledOk(r);
  });
  let started = 0;
  const worker = queue.process(() => { started++; }, { concurrency: 2 });
  await until(() => claims(requests).length === 1);
  const stopped = worker.stop();
  stopping.resolve();
  await stopped;
  assert.equal(started, 0);
  assert.deepEqual(settles(requests).flatMap((s) => s.body.ops),
    [{ token: "lt_1", op: "release" }, { token: "lt_2", op: "release" }]);
  assert.equal(claims(requests).length, 1, "no claim after stop()");
});

test("stop() waits for started handlers, then aborts the rest and settles them retry", async () => {
  const { queue, requests } = workerCell([wireMessage(1), wireMessage(2)]);
  let reason;
  const worker = queue.process((msg, { signal }) => msg.body.n === 1
    ? new Promise((r) => setTimeout(r, 50))
    : new Promise(() => { signal.addEventListener("abort", () => { reason = signal.reason; }); }),
  { concurrency: 2 });
  await until(() => claims(requests).length === 2);
  const began = Date.now();
  await worker.stop({ timeout: 300 });
  const took = Date.now() - began;
  assert.ok(took >= 280 && took < 1500, `stop took ${took} ms`);
  assert.ok(reason instanceof HandlerAbortedError && reason.reason === "worker_stopped");
  assert.deepEqual(settles(requests).flatMap((s) => s.body.ops),
    [{ token: "lt_1", op: "ack" }, { token: "lt_2", op: "retry", code: "worker_stopped" }]);
  await worker.closed;
});

test("a paused group backs off 1 s, then 2 s, before claiming again", async () => {
  const { queue, requests } = cell(() => json({ messages: [], paused: true }));
  const worker = queue.process(() => {});
  await until(() => claims(requests).length === 3, 6000);
  const at = claims(requests).map((r) => r.at);
  const gaps = [at[1] - at[0], at[2] - at[1]];
  assert.ok(gaps[0] >= 950 && gaps[0] < 1500, `first gap ${gaps[0]} ms`);
  assert.ok(gaps[1] >= 1950 && gaps[1] < 2600, `second gap ${gaps[1]} ms`);
  await worker.stop();
});

test("empty claims back off only without a long poll", async () => {
  const polling = cell(() => json({ messages: [] }));
  const longPoll = polling.queue.process(() => {});
  await new Promise((r) => setTimeout(r, 200));
  await longPoll.stop();
  assert.ok(claims(polling.requests).length > 3, "a long-poll worker claims again at once");
  const zero = cell(() => json({ messages: [] }));
  const noWait = zero.queue.process(() => {}, { waitMs: 0 });
  await new Promise((r) => setTimeout(r, 200));
  await noWait.stop();
  assert.equal(claims(zero.requests).length, 1, "waitMs 0: the second claim waits 1 s");
  assert.equal(claims(zero.requests)[0].body.waitMs, 0);
});

test("a claim refusal that is not retryable stops the worker and rejects closed", async () => {
  const { queue } = cell(() => json({ error: { code: "queue_not_found", message: "no queue", retryable: false } }, 404));
  const errors = [];
  const worker = queue.process(() => {}, { onError: (e) => errors.push(e.code) });
  await assert.rejects(worker.closed, (e) => e instanceof StreamsError && e.code === "queue_not_found");
  assert.deepEqual(errors, ["queue_not_found"]);
});
