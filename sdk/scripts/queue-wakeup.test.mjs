// The wake-up receiver (QUEUES-DESIGN §9.2): signature, freshness and queue
// checks, the immediate 202, and the bounded drain registered with waitUntil.
import assert from "node:assert/strict";
import test from "node:test";
import { HandlerAbortedError } from "../dist/index.js";
import { cell, claims, deferred, json, settledOk, settles, wireMessage } from "./queue-helpers.mjs";

const SECRET = "c2VjcmV0LWJ5dGVzLWZvci10aGUtd2FrZXVwLXRlc3Q"; // base64url
const OTHER = "b3RoZXItc2VjcmV0LWJ5dGVz";

function b64url(text) {
  const std = text.replace(/-/g, "+").replace(/_/g, "/");
  return Uint8Array.from(atob(std.padEnd(std.length + ((4 - (std.length % 4)) % 4), "=")), (c) => c.charCodeAt(0));
}

async function sign(secret, sentAt, raw) {
  const key = await crypto.subtle.importKey("raw", b64url(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const mac = new Uint8Array(await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(`${sentAt}.${raw}`)));
  return Array.from(mac, (b) => b.toString(16).padStart(2, "0")).join("");
}

async function notification({ secret = SECRET, sentAt = Date.now(), queue = "emails", group = "default", tamper } = {}) {
  const raw = JSON.stringify({ queue, group, due: 3, oldestDueAgeMs: 120, nonce: "n-1", sentAt });
  const signature = await sign(secret, sentAt, raw);
  return new Request("https://receiver.example/wake", {
    method: "POST",
    headers: { "content-type": "application/json", "streams-signature": `v1=${signature}` },
    body: tamper ? raw.replace('"due":3', '"due":4') : raw,
  });
}

/** A cell that hands out `batch` once, then answers empty claims. */
function drainCell(batch) {
  let claimed = 0;
  return cell((r) => r.path.endsWith(":claim")
    ? json({ messages: claimed++ === 0 ? batch : [] })
    : settledOk(r));
}

test("a signed, fresh notification answers 202 at once and drains through waitUntil", async () => {
  const { queue, requests } = drainCell([wireMessage(1), wireMessage(2)]);
  const gate = deferred();
  const handled = [];
  const registered = [];
  const response = await queue.handleWakeup(await notification(), async (msg) => {
    await gate.promise;
    handled.push(msg.id);
  }, { secret: SECRET, concurrency: 4, waitUntil: (p) => registered.push(p) });
  assert.equal(response.status, 202);
  assert.equal(registered.length, 1, "the drain is registered with waitUntil");
  assert.deepEqual(handled, [], "the 202 does not wait for the handlers");
  gate.resolve();
  await registered[0];
  assert.deepEqual(handled.sort(), ["msg_1", "msg_2"]);
  assert.deepEqual(claims(requests)[0].body, { max: 4, leaseMs: 30_000, waitMs: 0 });
  assert.equal(claims(requests).length, 2, "the drain ends at the first empty claim");
  assert.deepEqual(settles(requests).flatMap((s) => s.body.ops),
    [{ token: "lt_1", op: "ack" }, { token: "lt_2", op: "ack" }]);
});

test("during a rotation either secret verifies", async () => {
  const { queue } = drainCell([]);
  const registered = [];
  const response = await queue.handleWakeup(await notification({ secret: OTHER }), () => {},
    { secret: [SECRET, OTHER], waitUntil: (p) => registered.push(p) });
  assert.equal(response.status, 202);
  await registered[0];
});

const refused = [
  ["a body changed after signing", { tamper: true }, 401, "invalid_signature"],
  ["another secret's signature", { secret: OTHER }, 401, "invalid_signature"],
  ["a sentAt 61 s old", { sentAt: Date.now() - 61_000 }, 401, "stale_notification"],
  ["a sentAt 61 s ahead", { sentAt: Date.now() + 61_000 }, 401, "stale_notification"],
  ["another queue", { queue: "sms" }, 400, "wrong_queue"],
  ["another group", { group: "billing" }, 400, "wrong_queue"],
];
for (const [label, options, status, code] of refused) {
  test(`a notification with ${label} is refused ${status} ${code} and drains nothing`, async () => {
    const { queue, requests } = drainCell([wireMessage(1)]);
    const registered = [];
    const response = await queue.handleWakeup(await notification(options), () => {},
      { secret: SECRET, waitUntil: (p) => registered.push(p) });
    assert.equal(response.status, status);
    assert.equal((await response.json()).error.code, code);
    assert.equal(registered.length, 0);
    assert.equal(requests.length, 0);
  });
}

test("a notification without a signature is refused 401", async () => {
  const { queue, requests } = drainCell([]);
  const request = new Request("https://receiver.example/wake", {
    method: "POST", body: JSON.stringify({ queue: "emails", group: "default", sentAt: Date.now() }),
  });
  const response = await queue.handleWakeup(request, () => {}, { secret: SECRET });
  assert.equal(response.status, 401);
  assert.equal(requests.length, 0);
});

test("the drain stops at its budget less the settle margin and settles a handler still running as retry", async () => {
  const { queue, requests } = drainCell([wireMessage(1), wireMessage(2)]);
  const registered = [];
  let reason;
  const began = Date.now();
  const response = await queue.handleWakeup(await notification(), (msg, { signal }) => msg.body.n === 1
    ? undefined
    : new Promise(() => { signal.addEventListener("abort", () => { reason = signal.reason; }); }),
  { secret: SECRET, concurrency: 2, drainBudgetMs: 600, settleMarginMs: 200, waitUntil: (p) => registered.push(p) });
  assert.equal(response.status, 202);
  await registered[0];
  const took = Date.now() - began;
  assert.ok(took >= 380 && took < 1200, `the drain took ${took} ms`);
  assert.ok(reason instanceof HandlerAbortedError && reason.reason === "worker_stopped");
  assert.deepEqual(settles(requests).flatMap((s) => s.body.ops),
    [{ token: "lt_1", op: "ack" }, { token: "lt_2", op: "retry", code: "worker_stopped" }]);
  // msg 1's slot freed early, so a second claim ran (empty); none after the stop point.
  assert.equal(claims(requests).length, 2);
  assert.ok(claims(requests).every((c) => c.at - began < 400), "no claim after budget minus margin");
});
