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
