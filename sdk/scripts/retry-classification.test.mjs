import assert from "node:assert/strict";
import test from "node:test";
import { StreamsClient, WrongCellError } from "../dist/index.js";

const wrongCells = [
  { label: "421", status: 421, headers: {}, body: "" },
  { label: "503 header", status: 503, headers: { "prisma-error-code": "wrong_cell" }, body: "" },
  { label: "503 body", status: 503, headers: {}, body: '{"error":{"code":"wrong_cell"}}' },
];
for (const wrong of wrongCells) {
  for (const operation of ["metadata", "subscribe"]) {
    test(`${operation} surfaces ${wrong.label} wrong-cell after one request and no refresh`, async () => {
      let requests = 0, providerCalls = 0;
      const controller = new AbortController();
      const s = new StreamsClient({ url: "https://cell.example", tokenProvider: () => `token-${++providerCalls}`, fetch: async () => {
        if (++requests === 4) controller.abort(); // terminates the original broken subscription
        return new Response(wrong.body, { status: wrong.status, headers: { "retry-after": "0", ...wrong.headers } });
      } }).stream("orders", { encryptionKey: "A".repeat(43) });
      const call = operation === "metadata" ? s.metadata() : s.subscribe({ signal: controller.signal })[Symbol.asyncIterator]().next();
      let error;
      try { await call; } catch (caught) { error = caught; }
      assert.equal(requests, 1);
      assert.ok(error instanceof WrongCellError);
      assert.equal(error.retryable, false);
      assert.equal(providerCalls, 1);
    });
  }
}

for (const status of [429, 503]) {
  test(`ordinary ${status} retries and releases the consumed body`, async () => {
    let requests = 0;
    let failed;
    const s = new StreamsClient({ url: "https://cell.example", fetch: async () => {
      if (++requests === 1) return failed = new Response('{"error":{"code":"busy"}}', { status, headers: { "retry-after": "0" } });
      assert.equal(failed.bodyUsed, true);
      return new Response("{}");
    } }).stream("orders", { encryptionKey: "A".repeat(43) });
    await s.metadata();
    assert.equal(requests, 2);
  });
}

test("subscription propagates permanent token-provider failures without any fetch", async () => {
  let requests = 0, calls = 0;
  const failure = new TypeError("invalid credential configuration");
  const s = new StreamsClient({ url: "https://cell.example", tokenProvider: () => { calls++; throw failure; }, fetch: async () => { requests++; return new Response("[]"); } })
    .stream("orders", { encryptionKey: "A".repeat(43) });
  await assert.rejects(s.subscribe()[Symbol.asyncIterator]().next(), e => e === failure);
  assert.equal(calls, 1);
  assert.equal(requests, 0);
});

test("cancellation interrupts status backoff without replay", async () => {
  const controller = new AbortController();
  let requests = 0;
  const s = new StreamsClient({ url: "https://cell.example", fetch: async () => {
    requests++;
    const res = new Response("", { status: 503, headers: { "retry-after": "5" } });
    setImmediate(() => controller.abort());
    return res;
  } }).stream("orders", { encryptionKey: "A".repeat(43) });
  assert.deepEqual(await s.subscribe({ signal: controller.signal })[Symbol.asyncIterator]().next(), { value: undefined, done: true });
  assert.equal(requests, 1);
});

test("header wrong-cell cancels an unused response body", async () => {
  let cancelled = false;
  const s = new StreamsClient({ url: "https://cell.example", fetch: async () => new Response(new ReadableStream({
    cancel() { cancelled = true; },
  }), { status: 503, headers: { "prisma-error-code": "wrong_cell" } }) })
    .stream("orders", { encryptionKey: "A".repeat(43) });
  await assert.rejects(s.metadata(), WrongCellError);
  assert.equal(cancelled, true);
});

for (const failure of ["transport", "cursor_beyond_tail"]) {
  test(`applied subscription recovers from ${failure} at its durable cursor`, async () => {
    let requests = 0;
    const s = new StreamsClient({ url: "https://cell.example", fetch: async url => {
      if (++requests === 1) return new Response("[1]", { headers: {
        "prisma-next-cursor": "provisional", "prisma-durable-cursor": "durable", "prisma-up-to-date": "true",
      } });
      if (requests === 2) {
        if (failure === "transport") throw new TypeError("network disconnected");
        return new Response('{"error":{"code":"cursor_beyond_tail"}}', { status: 409 });
      }
      assert.equal(new URL(url).searchParams.get("cursor"), "durable");
      return new Response("[2]", { headers: { "prisma-up-to-date": "true", "prisma-sealed": "true" } });
    } }).stream("orders", { encryptionKey: "A".repeat(43) });
    const records = [];
    for await (const record of s.subscribe({ deliver: "applied" })) records.push(record);
    assert.deepEqual(records, [1, 2]);
    assert.equal(requests, 3);
  });
}

test("explicitly permanent 503 domain errors are not replayed", async () => {
  let requests = 0;
  const s = new StreamsClient({ url: "https://cell.example", fetch: async () => {
    requests++;
    return new Response('{"error":{"code":"configuration_invalid","retryable":false}}', { status: 503 });
  } }).stream("orders", { encryptionKey: "A".repeat(43) });
  await assert.rejects(s.subscribe()[Symbol.asyncIterator]().next(), e => e.code === "configuration_invalid");
  assert.equal(requests, 1);
});
