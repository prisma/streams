import assert from "node:assert/strict";
import test from "node:test";
import { setImmediate } from "node:timers";
import * as sdk from "../dist/index.js";

const { StreamsClient } = sdk;

const KEY = "A".repeat(43);
const T0 = Date.UTC(2026, 9, 12, 12, 0, 0);
const SECOND = 1000;

// The SDK reads the time through Date.now(); each test runs on this clock.
let clock = T0;
function clocked(name, run) {
  test(name, async () => {
    const real = Date.now;
    clock = T0;
    Date.now = () => clock;
    try {
      await run();
    } finally {
      Date.now = real;
    }
  });
}

async function rejection(call) {
  try {
    await call;
  } catch (error) {
    return error;
  }
  assert.fail("the request resolved");
}

const drain = async () => { for (let i = 0; i < 5; i++) await new Promise(setImmediate); };

// A tokenProvider that answers its scripted outcomes in order: a lifetime
// in ms is a { token: "t<n>", expiresAt } from the clock, an Error is
// thrown, a string is returned as it is; calls are counted.
function provider(...outcomes) {
  const p = async () => {
    const outcome = outcomes[p.calls++];
    if (outcome instanceof Error) throw outcome;
    if (typeof outcome === "number") return { token: `t${p.calls}`, expiresAt: clock + outcome };
    return outcome;
  };
  p.calls = 0;
  return p;
}

// A client whose server answers 401 to the tokens in `refused`; every
// request's Authorization header is recorded.
function client(tokenProvider, refused = []) {
  const sent = [];
  const c = new StreamsClient({ url: "https://cell.example", tokenProvider, fetch: async (_url, init) => {
    sent.push(init.headers.authorization);
    if (refused.includes(init.headers.authorization)) return new Response("", { status: 401 });
    return new Response("{}");
  } });
  return { stream: c.stream("orders", { encryptionKey: KEY }), sent };
}

// ---- tokens with an expiry ----------------------------------------------

clocked("an issuer token of 3600 s is refreshed by the first request at 2880 s, not at 2879 s", async () => {
  const p = provider(3600 * SECOND, 3600 * SECOND);
  const { stream, sent } = client(p);
  await stream.metadata();
  clock = T0 + 2879 * SECOND;
  await stream.metadata();
  clock = T0 + 2880 * SECOND;
  await stream.metadata();
  assert.deepEqual(sent, ["Bearer t1", "Bearer t1", "Bearer t2"]);
  assert.equal(p.calls, 2);
});

clocked("an emulator token of 600 s is refreshed at 480 s, not at 479 s, and its successor 480 s after it", async () => {
  const p = provider(600 * SECOND, 600 * SECOND, 600 * SECOND);
  const { stream, sent } = client(p);
  await stream.metadata();
  clock = T0 + 479 * SECOND;
  await stream.metadata();
  clock = T0 + 480 * SECOND;
  await stream.metadata();
  clock = T0 + 959 * SECOND;
  await stream.metadata();
  clock = T0 + 960 * SECOND;
  await stream.metadata();
  assert.deepEqual(sent, ["Bearer t1", "Bearer t1", "Bearer t2", "Bearer t2", "Bearer t3"]);
  assert.equal(p.calls, 3);
});

clocked("a 300 s token is refreshed two minutes before it expires, at 180 s", async () => {
  const p = provider(300 * SECOND, 300 * SECOND);
  const { stream, sent } = client(p);
  await stream.metadata();
  clock = T0 + 179 * SECOND;
  await stream.metadata();
  clock = T0 + 180 * SECOND;
  await stream.metadata();
  assert.deepEqual(sent, ["Bearer t1", "Bearer t1", "Bearer t2"]);
});

clocked("expiresAt may be a Date", async () => {
  let calls = 0;
  const { stream, sent } = client(() => ({ token: `d${++calls}`, expiresAt: new Date(clock + 600 * SECOND) }));
  await stream.metadata();
  clock = T0 + 479 * SECOND;
  await stream.metadata();
  clock = T0 + 480 * SECOND;
  await stream.metadata();
  assert.deepEqual(sent, ["Bearer d1", "Bearer d1", "Bearer d2"]);
});

clocked("requests at the refresh threshold share one refresh and all send its token", async () => {
  let release;
  const held = new Promise(resolve => { release = resolve; });
  let calls = 0;
  const { stream, sent } = client(async () => {
    calls++;
    if (calls === 2) await held;
    return { token: `t${calls}`, expiresAt: clock + 3600 * SECOND };
  });
  await stream.metadata();
  clock = T0 + 2880 * SECOND;
  const pending = Array.from({ length: 5 }, () => stream.metadata());
  await drain();
  assert.equal(calls, 2, "one refresh for five requests");
  assert.deepEqual(sent, ["Bearer t1"], "the requests wait for the refresh");
  release();
  await Promise.all(pending);
  assert.deepEqual(sent, ["Bearer t1", ...Array(5).fill("Bearer t2")]);
});

clocked("a failed refresh is not surfaced while the cached token is valid: the request sends it, and the next request refreshes again", async () => {
  const down = new Error("issuer unavailable");
  const p = provider(3600 * SECOND, down, 3600 * SECOND);
  const { stream, sent } = client(p);
  await stream.metadata();
  clock = T0 + 2880 * SECOND;
  const both = await Promise.all([stream.metadata(), stream.metadata()]);
  assert.equal(both.length, 2);
  assert.equal(p.calls, 2, "the two requests shared the failed refresh");
  clock = T0 + 2881 * SECOND;
  await stream.metadata();
  assert.equal(p.calls, 3);
  assert.deepEqual(sent, ["Bearer t1", "Bearer t1", "Bearer t1", "Bearer t3"]);
});

clocked("once the cached token has expired, a failed refresh rejects the request with the provider's error and sends nothing", async () => {
  const down = new Error("issuer unavailable");
  const p = provider(600 * SECOND, down, down);
  const { stream, sent } = client(p);
  await stream.metadata();
  clock = T0 + 599 * SECOND;
  await stream.metadata();
  clock = T0 + 600 * SECOND;
  assert.equal(await rejection(stream.metadata()), down);
  assert.equal(p.calls, 3);
  assert.deepEqual(sent, ["Bearer t1", "Bearer t1"]);
});

clocked("a 401 still refreshes once and replays with the new token", async () => {
  const p = provider(3600 * SECOND, 3600 * SECOND);
  const { stream, sent } = client(p, ["Bearer t1"]);
  await stream.metadata();
  assert.deepEqual(sent, ["Bearer t1", "Bearer t2"]);
  assert.equal(p.calls, 2);
});

clocked("a token the server refused is never sent again when the refresh after the 401 fails", async () => {
  const down = new Error("issuer unavailable");
  const p = provider(3600 * SECOND, down, 3600 * SECOND);
  const { stream, sent } = client(p, ["Bearer t1"]);
  assert.equal(await rejection(stream.metadata()), down);
  assert.deepEqual(sent, ["Bearer t1"]);
  await stream.metadata();
  assert.deepEqual(sent, ["Bearer t1", "Bearer t3"]);
});

clocked("a token sent after a failed refresh is dropped at its 401 and the refresh's failure surfaces", async () => {
  const down = new Error("issuer unavailable");
  const refused = [];
  const p = provider(3600 * SECOND, down, down, down);
  const { stream, sent } = client(p, refused);
  await stream.metadata();
  clock = T0 + 2880 * SECOND;
  await stream.metadata();
  refused.push("Bearer t1");
  clock = T0 + 2881 * SECOND;
  assert.equal(await rejection(stream.metadata()), down);
  assert.equal(p.calls, 4);
  assert.deepEqual(sent, ["Bearer t1", "Bearer t1", "Bearer t1"]);
});

clocked("a provider returning a string is refreshed only after a 401 (pin)", async () => {
  const refused = [];
  const p = provider("s1", "s2");
  const { stream, sent } = client(p, refused);
  await stream.metadata();
  clock = T0 + 10 * 3600 * SECOND;
  await stream.metadata();
  refused.push("Bearer s1");
  await stream.metadata();
  assert.deepEqual(sent, ["Bearer s1", "Bearer s1", "Bearer s1", "Bearer s2"]);
  assert.equal(p.calls, 2);
});

clocked("a { token } without a usable expiresAt is refreshed only after a 401", async () => {
  let calls = 0;
  const { stream, sent } = client(() => ({ token: `n${++calls}`, expiresAt: Number.NaN }));
  await stream.metadata();
  clock = T0 + 10 * 3600 * SECOND;
  await stream.metadata();
  assert.deepEqual(sent, ["Bearer n1", "Bearer n1"]);
});
