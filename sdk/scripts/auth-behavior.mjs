// CI pin for the MULTITENANCY Stage-4 SDK auth contract (§5, §8.1):
//
//   1. tokenProvider is lazy + single-flight: N concurrent requests
//      after expiry fetch ONE token.
//   2. 401 refreshes through the provider exactly once and replays.
//   3. wrong_cell (421, or the header-only fallback form) surfaces as
//      the typed WrongCellError and NEVER burns a token refresh —
//      refreshing a valid credential at the wrong cell is the exact
//      client behavior the contract forbids.
//   4. Producer state scopes carry project + endpoint, so one shared
//      durable store never mixes epochs across projects/deployments.
//
// Run: node sdk/scripts/auth-behavior.mjs   (requires a built dist/)
import { StreamsClient, WrongCellError } from "../dist/index.js";
import assert from "node:assert/strict";

const KEY = "A".repeat(43); // syntactically plausible base64url 32B key

function stubFetch(script) {
  const calls = [];
  const f = async (url, init) => {
    calls.push({ url: String(url), auth: init?.headers?.authorization });
    const step = script.shift() ?? { status: 200, body: "{}" };
    return new Response(step.body ?? "{}", {
      status: step.status,
      headers: { "content-type": "application/json", ...(step.headers ?? {}) },
    });
  };
  return { f, calls };
}

// ---- 1. lazy + single-flight provider ------------------------------
{
  let fetches = 0;
  const { f, calls } = stubFetch([
    { status: 200, body: "{}" },
    { status: 200, body: "{}" },
  ]);
  const client = new StreamsClient({
    url: "https://cell-a.example",
    project: "proj-a",
    tokenProvider: async () => {
      fetches += 1;
      return `tok-${fetches}`;
    },
    fetch: f,
  });
  const s = client.stream("orders", { encryptionKey: KEY });
  await Promise.all([s.metadata().catch(() => {}), s.metadata().catch(() => {})]);
  assert.equal(fetches, 1, `single-flight: ${fetches} provider calls`);
  assert.ok(calls.every((c) => c.auth === "Bearer tok-1"), "both used tok-1");
}

// ---- 2. 401 refreshes once and replays ------------------------------
{
  let fetches = 0;
  const { f, calls } = stubFetch([
    { status: 401, body: '{"error":{"code":"unauthorized","message":"expired"}}' },
    { status: 200, body: "{}" },
  ]);
  const client = new StreamsClient({
    url: "https://cell-a.example",
    tokenProvider: () => `tok-${++fetches}`,
    fetch: f,
  });
  await client.stream("orders", { encryptionKey: KEY }).metadata();
  assert.equal(fetches, 2, "401 fetched a fresh token");
  assert.equal(calls[0].auth, "Bearer tok-1");
  assert.equal(calls[1].auth, "Bearer tok-2", "replay carries the fresh token");
}

// ---- 3. wrong_cell: typed error, no refresh -------------------------
{
  let fetches = 0;
  // Header-only fallback form: no JSON body at all (contract §8.1).
  const { f, calls } = stubFetch([
    { status: 421, body: "", headers: { "prisma-error-code": "wrong_cell" } },
  ]);
  const client = new StreamsClient({
    url: "https://cell-a.example",
    tokenProvider: () => `tok-${++fetches}`,
    fetch: f,
  });
  let err;
  try {
    await client.stream("orders", { encryptionKey: KEY }).metadata();
  } catch (e) {
    err = e;
  }
  assert.ok(err instanceof WrongCellError, `typed: ${err?.name}`);
  assert.equal(err.code, "wrong_cell");
  assert.equal(err.retryable, false, "421 form is not blind-retryable");
  assert.equal(fetches, 1, "wrong_cell must not burn a token refresh");
  assert.equal(calls.length, 1, "wrong_cell must not be replayed here");
}

// ---- 4. producer scope carries project + endpoint --------------------
{
  const seen = [];
  const store = {
    load: async (scope) => {
      seen.push(scope);
      return undefined;
    },
    save: async () => {},
  };
  const ok = {
    status: 200,
    body: '{"offset":0,"count":1,"producer":{"epoch":0,"nextSeq":1}}',
  };
  const { f } = stubFetch([ok]);
  const client = new StreamsClient({
    url: "https://cell-a.example",
    project: "proj-a",
    token: "t",
    fetch: f,
  });
  const producer = client
    .stream("orders", { encryptionKey: KEY })
    .producer("p1", { state: store });
  await producer.append({ n: 1 }).catch(() => {});
  assert.ok(seen.length >= 1, "state store consulted");
  assert.equal(seen[0].project, "proj-a");
  assert.equal(seen[0].endpoint, "https://cell-a.example");
  assert.equal(seen[0].stream, "orders");
}

console.log("AUTH_BEHAVIOR_OK");
