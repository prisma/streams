// The official client's schedule for a refused request, one row a refusal
// (refusal-schedule.json): when every attempt of a plain append or a read
// gets the same answer, the waits the SDK makes before it sends the request
// again, after which the answer surfaces. The #149 ramp's driver copies
// these rules (bench/population_rules.rs, in bench/ramp/population-mode.patch)
// and bench/ramp/test_ramp.py checks it against the same table, so the two
// cannot drift apart.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { StreamsClient, StreamsError } from "../dist/index.js";

const TABLE = JSON.parse(readFileSync(new URL("./refusal-schedule.json", import.meta.url), "utf8"));
const KEY = "A".repeat(43);

// The clock the SDK reads: it stands still but for the waits the SDK asks
// for, each of which passes at once and moves Date.now by the wait.
async function virtualClock(run) {
  const nativeTimeout = globalThis.setTimeout;
  const nativeNow = Date.now;
  let offset = 0;
  const waits = [];
  globalThis.setTimeout = (callback, delay = 0, ...args) => {
    waits.push(delay);
    offset += delay;
    return nativeTimeout(callback, 0, ...args);
  };
  const base = nativeNow();
  Date.now = () => base + offset;
  try {
    return { value: await run(), waits };
  } finally {
    globalThis.setTimeout = nativeTimeout;
    Date.now = nativeNow;
  }
}

function answer(row) {
  if (row.status === 0) throw new TypeError("fetch failed");
  const headers = { ...row.headers };
  if (row.body !== null) headers["content-type"] = "application/json";
  return new Response(row.body, { status: row.status, headers });
}

test("the refusal table is the one the ramp's driver is checked against", () => {
  assert.ok(TABLE.rows.length >= 20);
  for (const row of TABLE.rows) {
    assert.ok(row.kind === "append" || row.kind === "read", row.name);
    assert.ok(Array.isArray(row.waits), row.name);
  }
});

for (const row of TABLE.rows) {
  test(`refusal schedule: ${row.name}`, async () => {
    let requests = 0;
    const orders = new StreamsClient({
      url: "https://cell.example",
      fetch: async () => {
        requests++;
        return answer(row);
      },
    }).stream("orders", { encryptionKey: KEY });
    const run = row.kind === "append" ? () => orders.append({ n: 1 }) : () => orders.read();
    const { value: error, waits } = await virtualClock(async () => {
      try {
        await run();
      } catch (caught) {
        return caught;
      }
      assert.fail("the request resolved");
    });
    assert.ok(error instanceof StreamsError, String(error));
    assert.deepEqual(waits, row.waits);
    assert.equal(requests, row.waits.length + 1);
  });
}

// Edge change #264: a 409 not_stream_owner is decided before any write (the
// instance that answered does not own the stream's segment), so every request
// kind sends it again within its attempts after its Retry-After, and the
// second answer is the request's.
const OWNER_REFUSAL = JSON.stringify({
  error: {
    code: "not_stream_owner",
    message: "another instance owns the target segment; retry through the router",
    retryable: true,
  },
});
const APPENDED = '{"cursor":"c","count":1,"duplicate":false,"sealed":false}';
for (const [label, run, ok] of [
  ["append", s => s.append({ n: 1 }), APPENDED],
  ["appendMany", s => s.appendMany([{ n: 1 }, { n: 2 }]), APPENDED],
  ["producer append", s => s.producer("p1", { ephemeral: true }).append({ n: 1 }), APPENDED],
  ["read", s => s.read(), "[]"],
  ["metadata", s => s.metadata(), "{}"],
]) {
  test(`${label}: a 409 not_stream_owner is sent again after its Retry-After and the next answer is the request's`, async () => {
    const sent = [];
    const orders = new StreamsClient({
      url: "https://cell.example",
      fetch: async (url, init) => {
        sent.push({ url: String(url), method: init.method, body: init.body });
        return sent.length === 1
          ? new Response(OWNER_REFUSAL, {
            status: 409,
            headers: { "content-type": "application/json", "retry-after": "1", "streams-replay-to": "streams-2" },
          })
          : new Response(ok, { headers: { "content-type": "application/json" } });
      },
    }).stream("orders", { encryptionKey: KEY });
    const { waits } = await virtualClock(() => run(orders));
    assert.deepEqual(waits, [1000]);
    assert.equal(sent.length, 2);
    assert.deepEqual(sent[1], sent[0], "the same request is sent again");
  });
}
