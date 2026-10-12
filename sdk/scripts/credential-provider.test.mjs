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

// ---- the credential exchange (prismaStreamsCredential) -------------------

const { prismaStreamsCredential, CredentialExchangeError, StreamsError, StreamsTransportError } = sdk;
const CREDENTIAL = "prisma_streams_cred1.s3cr3t-value";
const ISSUER = "https://issuer.example";
const EXCHANGED = { accessToken: "jwt-1", tokenType: "Bearer", expiresIn: 3600, projectId: "proj-1", endpoint: "https://cell.example" };

const json = (body, status = 200, headers = {}) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });
const envelope = (status, code, retryable, headers = {}) =>
  json({ error: { code, message: `${code} message`, retryable } }, status, headers);

// The issuer answers its scripted responses in order (a function is
// called with the request; an Error is thrown as fetch throws), then 200.
function issuer(...answers) {
  const requests = [];
  const fetch = async (url, init) => {
    const call = { url: String(url), method: init.method, headers: { ...init.headers }, body: init.body };
    requests.push(call);
    const answer = answers[requests.length - 1];
    if (answer instanceof Error) throw answer;
    if (typeof answer === "function") return answer(call);
    return answer ?? json(EXCHANGED);
  };
  return { fetch, requests };
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

clocked("the exchange is one POST of the whole credential to the issuer's /v1/token/streams, without a body", async () => {
  const { fetch, requests } = issuer(json({ ...EXCHANGED, cellId: "cell-a" }));
  const provider = prismaStreamsCredential(CREDENTIAL, { issuerUrl: `${ISSUER}/`, fetch });
  assert.equal(provider.last, undefined);
  const exchanged = await provider.exchange();
  assert.deepEqual(exchanged, { accessToken: "jwt-1", expiresIn: 3600, projectId: "proj-1", endpoint: "https://cell.example" });
  assert.deepEqual(provider.last, exchanged);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].url, "https://issuer.example/v1/token/streams");
  assert.equal(requests[0].method, "POST");
  assert.deepEqual(requests[0].headers, { authorization: `StreamsCredential ${CREDENTIAL}`, accept: "application/json" });
  assert.equal(requests[0].body, undefined);
});

clocked("tokenProvider() exchanges and answers the token with its expiry; an issuer without endpoint answers null", async () => {
  const { fetch, requests } = issuer(json({ ...EXCHANGED, expiresIn: 600, endpoint: null }), json({ ...EXCHANGED, endpoint: undefined }));
  const provider = prismaStreamsCredential(CREDENTIAL, { issuerUrl: ISSUER, fetch });
  assert.deepEqual(await provider.tokenProvider(), { token: "jwt-1", expiresAt: T0 + 600 * SECOND });
  assert.equal(provider.last.endpoint, null);
  clock = T0 + 5 * SECOND;
  assert.deepEqual(await provider.tokenProvider(), { token: "jwt-1", expiresAt: T0 + 3605 * SECOND });
  assert.equal(provider.last.endpoint, null);
  assert.equal(requests.length, 2);
});

clocked("the exchange replays a 503's Retry-After and a transport failure within its four attempts", async () => {
  const { fetch, requests } = issuer(envelope(503, "issuer_unavailable", true, { "retry-after": "5" }), new TypeError("fetch failed"));
  const provider = prismaStreamsCredential(CREDENTIAL, { issuerUrl: ISSUER, fetch });
  const { value, delays } = await fastTimers(() => provider.exchange());
  assert.equal(value.accessToken, "jwt-1");
  assert.equal(requests.length, 3);
  assert.deepEqual(delays, [5000, 2000]);
});

clocked("a 401 invalid_credential rejects at once as CredentialExchangeError, carrying no credential", async () => {
  const { fetch, requests } = issuer(envelope(401, "invalid_credential", false));
  const provider = prismaStreamsCredential(CREDENTIAL, { issuerUrl: ISSUER, fetch });
  const error = await rejection(provider.exchange());
  assert.ok(error instanceof CredentialExchangeError);
  assert.ok(error instanceof StreamsError);
  assert.equal(error.name, "CredentialExchangeError");
  assert.deepEqual([error.status, error.code, error.message, error.retryable], [401, "invalid_credential", "invalid_credential message", false]);
  assert.equal(requests.length, 1);
  assert.equal(provider.last, undefined);
  assert.ok(!JSON.stringify({ ...error, message: error.message, stack: error.stack }).includes("s3cr3t"));
});

for (const code of ["credential_not_active", "project_not_active"]) {
  clocked(`a 403 ${code} rejects at once with its code`, async () => {
    const { fetch, requests } = issuer(envelope(403, code, false));
    const error = await rejection(prismaStreamsCredential(CREDENTIAL, { issuerUrl: ISSUER, fetch }).exchange());
    assert.ok(error instanceof CredentialExchangeError);
    assert.deepEqual([error.status, error.code, error.retryable, requests.length], [403, code, false, 1]);
  });
}

clocked("a 429 rate_limited is replayed on its Retry-After and surfaces, retryable, once the attempts are spent", async () => {
  const limited = () => envelope(429, "rate_limited", true, { "retry-after": "0" });
  const { fetch, requests } = issuer(limited, limited, limited, limited);
  const provider = prismaStreamsCredential(CREDENTIAL, { issuerUrl: ISSUER, fetch });
  const error = await rejection(provider.exchange());
  assert.ok(error instanceof CredentialExchangeError);
  assert.deepEqual([error.status, error.code, error.retryable, requests.length], [429, "rate_limited", true, 4]);
});

clocked("the emulator's { error: text } becomes code exchange_failed with that text; a body that is not JSON keeps the status", async () => {
  const { fetch } = issuer(json({ error: "unknown credential or bad secret" }, 401), new Response("bad gateway", { status: 502 }));
  const provider = prismaStreamsCredential(CREDENTIAL, { issuerUrl: ISSUER, fetch });
  const emulator = await rejection(provider.exchange());
  assert.ok(emulator instanceof CredentialExchangeError);
  assert.deepEqual([emulator.status, emulator.code, emulator.message, emulator.retryable], [401, "exchange_failed", "unknown credential or bad secret", false]);
  const gateway = await rejection(provider.exchange());
  assert.ok(gateway instanceof CredentialExchangeError);
  assert.deepEqual([gateway.status, gateway.code, gateway.message, gateway.retryable], [502, "exchange_failed", "HTTP 502", false]);
});

clocked("an answer without accessToken, expiresIn or projectId rejects as invalid_exchange_response", async () => {
  const { fetch } = issuer(json({ ...EXCHANGED, accessToken: undefined }), json({ ...EXCHANGED, expiresIn: "3600" }), json({ ...EXCHANGED, projectId: "" }), new Response("not json"));
  const provider = prismaStreamsCredential(CREDENTIAL, { issuerUrl: ISSUER, fetch });
  for (let i = 0; i < 4; i++) {
    const error = await rejection(provider.exchange());
    assert.ok(error instanceof CredentialExchangeError);
    assert.deepEqual([error.status, error.code, error.retryable], [200, "invalid_exchange_response", false]);
  }
  assert.equal(provider.last, undefined);
});

clocked("retry: { transport: false } sends the exchange once and rejects with StreamsTransportError", async () => {
  const failure = new TypeError("fetch failed");
  const { fetch, requests } = issuer(failure);
  const provider = prismaStreamsCredential(CREDENTIAL, { issuerUrl: ISSUER, fetch, retry: { transport: false } });
  const error = await rejection(provider.exchange());
  assert.ok(error instanceof StreamsTransportError);
  assert.equal(error.cause, failure);
  assert.equal(requests.length, 1);
});
