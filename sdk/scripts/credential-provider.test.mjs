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

// The request that reaches a token's refresh point starts the refresh and
// is sent with the cached token at once; the refresh runs beside it, and
// the requests after it arrives send its token.
clocked("an issuer token of 3600 s is refreshed from the first request at 2880 s, not at 2879 s", async () => {
  const p = provider(3600 * SECOND, 3600 * SECOND);
  const { stream, sent } = client(p);
  await stream.metadata();
  clock = T0 + 2879 * SECOND;
  await stream.metadata();
  assert.equal(p.calls, 1);
  clock = T0 + 2880 * SECOND;
  await stream.metadata();
  assert.equal(p.calls, 2, "the request at 2880 s started the refresh");
  await drain();
  await stream.metadata();
  assert.deepEqual(sent, ["Bearer t1", "Bearer t1", "Bearer t1", "Bearer t2"]);
  assert.equal(p.calls, 2);
});

clocked("an emulator token of 600 s is refreshed at 480 s, not at 479 s, and its successor 480 s after it arrives", async () => {
  const p = provider(600 * SECOND, 600 * SECOND, 600 * SECOND);
  const { stream, sent } = client(p);
  await stream.metadata();
  clock = T0 + 479 * SECOND;
  await stream.metadata();
  clock = T0 + 480 * SECOND;
  await stream.metadata();
  await drain();
  clock = T0 + 959 * SECOND;
  await stream.metadata();
  clock = T0 + 960 * SECOND;
  await stream.metadata();
  await drain();
  await stream.metadata();
  assert.deepEqual(sent, ["Bearer t1", "Bearer t1", "Bearer t1", "Bearer t2", "Bearer t2", "Bearer t3"]);
  assert.equal(p.calls, 3);
});

clocked("a 300 s token is refreshed two minutes before it expires, at 180 s", async () => {
  const p = provider(300 * SECOND, 300 * SECOND);
  const { stream, sent } = client(p);
  await stream.metadata();
  clock = T0 + 179 * SECOND;
  await stream.metadata();
  assert.equal(p.calls, 1);
  clock = T0 + 180 * SECOND;
  await stream.metadata();
  await drain();
  await stream.metadata();
  assert.deepEqual(sent, ["Bearer t1", "Bearer t1", "Bearer t1", "Bearer t2"]);
  assert.equal(p.calls, 2);
});

clocked("expiresAt may be a Date", async () => {
  let calls = 0;
  const { stream, sent } = client(() => ({ token: `d${++calls}`, expiresAt: new Date(clock + 600 * SECOND) }));
  await stream.metadata();
  clock = T0 + 479 * SECOND;
  await stream.metadata();
  clock = T0 + 480 * SECOND;
  await stream.metadata();
  await drain();
  await stream.metadata();
  assert.deepEqual(sent, ["Bearer d1", "Bearer d1", "Bearer d1", "Bearer d2"]);
});

clocked("requests past the refresh point are sent at once with the cached token while one refresh runs beside them", async () => {
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
  // An issuer answering 503 with Retry-After: 5 holds a refresh for 15 s.
  const pending = Array.from({ length: 5 }, () => stream.metadata());
  try {
    await drain();
    assert.equal(calls, 2, "one refresh for five requests");
    assert.deepEqual(sent, Array(6).fill("Bearer t1"), "no request waits for the refresh");
  } finally {
    release();
    await Promise.all(pending);
  }
  await drain();
  await stream.metadata();
  assert.equal(sent.at(-1), "Bearer t2");
  assert.equal(calls, 2);
});

clocked("a request made after the token expired waits for the refresh already running and sends its token", async () => {
  let release;
  const held = new Promise(resolve => { release = resolve; });
  let calls = 0;
  const { stream, sent } = client(async () => {
    calls++;
    if (calls === 2) await held;
    return { token: `t${calls}`, expiresAt: clock + 600 * SECOND };
  });
  await stream.metadata();
  clock = T0 + 480 * SECOND;
  const started = stream.metadata();
  await drain();
  clock = T0 + 600 * SECOND;
  const waiting = stream.metadata();
  try {
    await drain();
    assert.equal(calls, 2);
    assert.deepEqual(sent, ["Bearer t1", "Bearer t1"], "the request at 600 s does not send the expired token");
  } finally {
    release();
    await Promise.all([started, waiting]);
  }
  assert.deepEqual(sent, ["Bearer t1", "Bearer t1", "Bearer t2"]);
  assert.equal(calls, 2);
});

clocked("a failed refresh is not surfaced while the cached token is valid, and is tried again 30 s later, not by the next request", async () => {
  const down = new Error("issuer unavailable");
  const p = provider(3600 * SECOND, down, 3600 * SECOND);
  const { stream, sent } = client(p);
  await stream.metadata();
  clock = T0 + 2880 * SECOND;
  await Promise.all([stream.metadata(), stream.metadata()]);
  await drain();
  assert.equal(p.calls, 2, "the two requests shared the failed refresh");
  clock = T0 + 2909 * SECOND;
  await stream.metadata();
  assert.equal(p.calls, 2, "no refresh within 30 s of the failed one");
  clock = T0 + 2910 * SECOND;
  await stream.metadata();
  assert.equal(p.calls, 3);
  await drain();
  await stream.metadata();
  assert.deepEqual(sent, ["Bearer t1", "Bearer t1", "Bearer t1", "Bearer t1", "Bearer t1", "Bearer t3"]);
});

clocked("once the cached token has expired, a failed refresh rejects the request with the provider's error and sends nothing", async () => {
  const down = new Error("issuer unavailable");
  const p = provider(600 * SECOND, down, down);
  const { stream, sent } = client(p);
  await stream.metadata();
  clock = T0 + 599 * SECOND;
  await stream.metadata();
  await drain();
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

clocked("a cached token is dropped at its 401 after a failed refresh, and the refresh the request waits for surfaces its failure", async () => {
  const down = new Error("issuer unavailable");
  const refused = [];
  const p = provider(3600 * SECOND, down, down, down);
  const { stream, sent } = client(p, refused);
  await stream.metadata();
  clock = T0 + 2880 * SECOND;
  await stream.metadata();
  await drain();
  refused.push("Bearer t1");
  clock = T0 + 2881 * SECOND;
  assert.equal(await rejection(stream.metadata()), down);
  assert.equal(p.calls, 3, "the background refresh and the refresh after the 401");
  assert.deepEqual(sent, ["Bearer t1", "Bearer t1", "Bearer t1"]);
});

clocked("a 401 for the cached token while a refresh runs beside it drops that token: the requests surface the next refresh's failure and never resend it", async () => {
  const revoked = new Error("credential_not_active");
  let rejectHeld;
  const held = new Promise((_resolve, reject) => { rejectHeld = reject; });
  let calls = 0;
  const tokenProvider = async () => {
    calls++;
    if (calls === 1) return { token: "t1", expiresAt: clock + 3600 * SECOND };
    if (calls === 2) return held;
    throw revoked;
  };
  let releaseFirst;
  const firstAnswer = new Promise(resolve => { releaseFirst = resolve; });
  const sent = [];
  // The credential is revoked after the first request: the cell refuses
  // every later one 401, the request sent at 2879 s late.
  const c = new StreamsClient({ url: "https://cell.example", tokenProvider, fetch: async (_url, init) => {
    sent.push(init.headers.authorization);
    if (sent.length === 1) return new Response("{}");
    if (sent.length === 2) await firstAnswer;
    return new Response("", { status: 401 });
  } });
  const stream = c.stream("orders", { encryptionKey: KEY });
  await stream.metadata();
  clock = T0 + 2879 * SECOND;
  const r1 = rejection(stream.metadata());
  await drain();
  clock = T0 + 2880 * SECOND;
  const r2 = rejection(stream.metadata());
  await drain();
  releaseFirst();
  await drain();
  rejectHeld(revoked);
  assert.equal(await r1, revoked);
  assert.equal(await r2, revoked);
  assert.deepEqual(sent, ["Bearer t1", "Bearer t1", "Bearer t1"], "t1 is not sent again after a 401");
});

clocked("a late 401 for a token a later refresh replaced reuses the new token and costs no exchange", async () => {
  const down = new Error("issuer unavailable");
  const p = provider(3600 * SECOND, down, 3600 * SECOND, 3600 * SECOND);
  let releaseLate;
  const late = new Promise(resolve => { releaseLate = resolve; });
  const sent = [];
  const c = new StreamsClient({ url: "https://cell.example", tokenProvider: p, fetch: async (_url, init) => {
    sent.push(init.headers.authorization);
    if (sent.length === 2) {
      await late;
      return new Response("", { status: 401 });
    }
    return new Response("{}");
  } });
  const stream = c.stream("orders", { encryptionKey: KEY });
  await stream.metadata();
  clock = T0 + 2880 * SECOND;
  const ra = stream.metadata();
  await drain();
  clock = T0 + 2910 * SECOND;
  await stream.metadata();
  await drain();
  releaseLate();
  await ra;
  assert.equal(p.calls, 3, "the late 401 cost no exchange");
  assert.deepEqual(sent, ["Bearer t1", "Bearer t1", "Bearer t1", "Bearer t3"]);
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

// fetch rejects a header value with a control character in a TypeError
// whose message holds the value: the whole credential.
clocked("a credential that is not prisma_streams_<id>.<secret> in printable characters is refused before any request, without echoing it", async () => {
  for (const credential of [
    "prisma_streams_cred1.s3cr3t-value\nmore",
    "prisma_streams_cred1.s3cr3t value",
    "prisma_streams_cred1.s3cr3té",
    "s3cr3t-value",
    "prisma_streams_cred1",
  ]) {
    const { fetch, requests } = issuer();
    const thrown = (() => {
      try {
        prismaStreamsCredential(credential, { issuerUrl: ISSUER, fetch });
      } catch (error) {
        return error;
      }
    })();
    const refused = await rejection(StreamsClient.connect({ credential, issuerUrl: ISSUER, fetch }));
    for (const error of [thrown, refused]) {
      assert.ok(error instanceof CredentialExchangeError, JSON.stringify(credential));
      assert.deepEqual([error.status, error.code, error.retryable], [0, "invalid_credential_format", false]);
      const shown = JSON.stringify({ ...error, message: error.message, stack: error.stack, cause: String(error.cause) });
      assert.ok(!shown.includes("s3cr3t"), "the error does not carry the credential");
    }
    assert.equal(requests.length, 0);
  }
});

clocked("whitespace around the credential is trimmed, as fetch trims a header value", async () => {
  const { fetch, requests } = issuer();
  await prismaStreamsCredential(` ${CREDENTIAL}\n`, { issuerUrl: ISSUER, fetch }).exchange();
  assert.equal(requests[0].headers.authorization, `StreamsCredential ${CREDENTIAL}`);
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

// ---- StreamsClient.connect ----------------------------------------------

const { MemoryProducerStateStore } = sdk;
const APPENDED = { cursor: "c", count: 1, duplicate: false, sealed: false };

// The issuer answers its scripted exchanges in order (a field object is
// merged into EXCHANGED; an Error is thrown as fetch throws); every other
// host is the service, which refuses the tokens in `refused` with 401.
function platform(...exchanges) {
  const issued = [], served = [], refused = [];
  const fetch = async (url, init) => {
    const call = { url: String(url), method: init.method, headers: { ...init.headers } };
    if (call.url.startsWith(`${ISSUER}/`)) {
      issued.push(call);
      const answer = exchanges[issued.length - 1] ?? exchanges.at(-1) ?? {};
      if (answer instanceof Error) throw answer;
      if (answer instanceof Response) return answer;
      return json({ ...EXCHANGED, accessToken: `jwt-${issued.length}`, ...answer });
    }
    served.push(call);
    if (refused.includes(call.headers.authorization)) return new Response("", { status: 401 });
    return json(call.method === "POST" ? APPENDED : { name: "orders", contentType: "application/json", sealed: false });
  };
  return { fetch, issued, served, refused };
}

// A durable store that records every scope it is asked for.
class ScopeLog extends MemoryProducerStateStore {
  scopes = [];
  async load(scope) { this.scopes.push(scope); return super.load(scope); }
}

clocked("connect exchanges once, serves the issuer's endpoint with its token, and keys producer state on the issuer URL and the credential's project", async () => {
  const p = platform({ endpoint: "https://cell.example/" });
  const client = await StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: `${ISSUER}/`, fetch: p.fetch });
  assert.equal(p.issued.length, 1);
  const orders = client.stream("orders", { encryptionKey: KEY });
  await orders.metadata();
  const state = new ScopeLog();
  await orders.producer("checkout", { state }).append({ n: 1 });
  assert.equal(p.issued.length, 1, "connect seeded the token cache: one exchange");
  assert.deepEqual(p.served.map(c => [c.method, c.url, c.headers.authorization]), [
    ["GET", "https://cell.example/v1/streams/orders", "Bearer jwt-1"],
    ["POST", "https://cell.example/v1/streams/orders/records", "Bearer jwt-1"],
  ]);
  assert.deepEqual(state.scopes[0], { stream: "orders", producerId: "checkout", routingKey: "", project: "proj-1", endpoint: ISSUER });
});

clocked("an explicit url is the base whatever the issuer's endpoint, and an issuer without endpoint needs one", async () => {
  const p = platform({ endpoint: null });
  const client = await StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, url: "http://gateway.internal:8080/", fetch: p.fetch });
  await client.stream("orders", { encryptionKey: KEY }).metadata();
  const other = await StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, url: "https://gateway.example", fetch: platform({}).fetch });
  assert.ok(other instanceof StreamsClient);
  assert.deepEqual(p.served.map(c => c.url), ["http://gateway.internal:8080/v1/streams/orders"]);
  const q = platform({ endpoint: null });
  const error = await rejection(StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, fetch: q.fetch }));
  assert.ok(error instanceof StreamsError);
  assert.deepEqual([error.status, error.code, error.message], [400, "endpoint_required", "the issuer returned no endpoint; pass url"]);
  assert.deepEqual([q.issued.length, q.served.length], [1, 0]);
});

clocked("an issuer endpoint that is neither https nor loopback is refused before any request carries the stream key", async () => {
  for (const endpoint of ["http://cell.example", "ftp://cell.example", "not a url"]) {
    const p = platform({ endpoint });
    const error = await rejection(StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, fetch: p.fetch }));
    assert.ok(error instanceof StreamsError, endpoint);
    assert.deepEqual([error.status, error.code, p.served.length], [400, "insecure_endpoint", 0], endpoint);
  }
  for (const endpoint of ["http://localhost:8090", "http://app.localhost:8090", "http://127.0.0.1:8090", "http://[::1]:8090"]) {
    const p = platform({ endpoint });
    const client = await StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, fetch: p.fetch });
    await client.stream("orders", { encryptionKey: KEY }).metadata();
    assert.equal(p.served[0].url, `${endpoint}/v1/streams/orders`);
  }
  const p = platform({ endpoint: "http://cell.example" });
  const client = await StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, fetch: p.fetch, allowInsecureEndpoint: true });
  await client.stream("orders", { encryptionKey: KEY }).metadata();
  assert.equal(p.served[0].url, "http://cell.example/v1/streams/orders");
});

clocked("a refresh that names a new endpoint moves the requests after it to the new host, and the producer's state is found there", async () => {
  const p = platform(
    { endpoint: "https://cell-a.example", expiresIn: 600 },
    { endpoint: "https://cell-b.example", expiresIn: 600 },
  );
  const client = await StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, fetch: p.fetch });
  const state = new MemoryProducerStateStore();
  const producer = client.stream("orders", { encryptionKey: KEY }).producer("checkout", { state });
  await producer.append({ n: 1 });
  clock = T0 + 480 * SECOND;
  // The refresh runs beside this append, which keeps the cached token and host.
  await producer.append({ n: 2 });
  await drain();
  await producer.append({ n: 3 });
  assert.equal(p.issued.length, 2);
  assert.deepEqual(p.served.map(c => [c.url, c.headers.authorization, c.headers["producer-seq"]]), [
    ["https://cell-a.example/v1/streams/orders/records", "Bearer jwt-1", "0"],
    ["https://cell-a.example/v1/streams/orders/records", "Bearer jwt-1", "1"],
    ["https://cell-b.example/v1/streams/orders/records", "Bearer jwt-2", "2"],
  ]);
});

clocked("a request that waits for the exchange (its token expired, or refused 401) goes to the endpoint that exchange names", async () => {
  const expired = platform(
    { endpoint: "https://cell-a.example", expiresIn: 600 },
    { endpoint: "https://cell-b.example", expiresIn: 600 },
  );
  const late = await StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, fetch: expired.fetch });
  clock = T0 + 600 * SECOND;
  await late.stream("orders", { encryptionKey: KEY }).metadata();
  assert.deepEqual(expired.served.map(c => [c.url, c.headers.authorization]),
    [["https://cell-b.example/v1/streams/orders", "Bearer jwt-2"]]);
  const refused = platform({ endpoint: "https://cell-a.example" }, { endpoint: "https://cell-b.example" });
  refused.refused.push("Bearer jwt-1");
  const moved = await StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, fetch: refused.fetch });
  await moved.stream("orders", { encryptionKey: KEY }).metadata();
  assert.deepEqual(refused.served.map(c => [c.url, c.headers.authorization]), [
    ["https://cell-a.example/v1/streams/orders", "Bearer jwt-1"],
    ["https://cell-b.example/v1/streams/orders", "Bearer jwt-2"],
  ]);
});

clocked("an explicit url is never left for the issuer's endpoint, and an insecure endpoint on refresh is not followed", async () => {
  const explicit = platform({ endpoint: "https://cell-a.example", expiresIn: 600 }, { endpoint: "https://cell-b.example", expiresIn: 600 });
  const pinned = await StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, url: "https://gateway.example", fetch: explicit.fetch });
  const insecure = platform({ endpoint: "https://cell-a.example", expiresIn: 600 }, { endpoint: "http://cell-b.example", expiresIn: 600 });
  const guarded = await StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, fetch: insecure.fetch });
  clock = T0 + 480 * SECOND;
  for (const client of [pinned, guarded]) {
    await client.stream("orders", { encryptionKey: KEY }).metadata();
    await drain();
    await client.stream("orders", { encryptionKey: KEY }).metadata();
  }
  assert.deepEqual([explicit.issued.length, insecure.issued.length], [2, 2]);
  assert.deepEqual(explicit.served.map(c => [c.url, c.headers.authorization]), [
    ["https://gateway.example/v1/streams/orders", "Bearer jwt-1"],
    ["https://gateway.example/v1/streams/orders", "Bearer jwt-2"],
  ]);
  assert.deepEqual(insecure.served.map(c => [c.url, c.headers.authorization]), [
    ["https://cell-a.example/v1/streams/orders", "Bearer jwt-1"],
    ["https://cell-a.example/v1/streams/orders", "Bearer jwt-2"],
  ]);
});

// Timers of `ms` are parked until released; after that they run at once.
function parkTimers(ms) {
  const native = globalThis.setTimeout;
  const parked = [];
  let released = false;
  globalThis.setTimeout = (callback, delay, ...args) => {
    if (delay !== ms) return native(callback, delay, ...args);
    if (released) return native(callback, 0, ...args);
    parked.push(() => callback(...args));
    return {};
  };
  return {
    parked: () => parked.length,
    release() { released = true; for (const run of parked.splice(0)) run(); },
    restore() { globalThis.setTimeout = native; },
  };
}

clocked("a connected client past its refresh point is served at once while the issuer answers 503 with Retry-After: 5", async () => {
  const busy = () => envelope(503, "issuer_unavailable", true, { "retry-after": "5" });
  const p = platform({}, busy(), busy(), busy(), busy());
  const client = await StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, fetch: p.fetch });
  const orders = client.stream("orders", { encryptionKey: KEY });
  clock = T0 + 2880 * SECOND;
  const timers = parkTimers(5000);
  let first;
  try {
    first = orders.metadata().then(() => "served", e => e);
    // Until the refresh sits in its Retry-After (bounded: a request that
    // waited for the refresh would never be served meanwhile).
    for (let i = 0; i < 50 && (timers.parked() === 0 || p.served.length === 0); i++) await drain();
    assert.equal(p.issued.length, 2, "the refresh started and met the issuer's 503");
    assert.equal(timers.parked(), 1, "the refresh waits out the Retry-After");
    assert.deepEqual(p.served.map(c => c.headers.authorization), ["Bearer jwt-1"],
      "the request was sent with the cached token without waiting");
  } finally {
    timers.release();
    // The released Retry-After waits run as 0 ms timers: wait in real time
    // (bounded), not in event-loop turns, which can outrun a timer.
    const tick = () => new Promise(resolve => setTimeout(resolve, 1));
    for (let i = 0; i < 500 && p.issued.length < 5; i++) await tick();
    for (let i = 0; i < 5; i++) await tick();
    timers.restore();
  }
  assert.equal(await first, "served");
  assert.equal(p.issued.length, 5, "the refresh made its four attempts and failed");
  clock = T0 + 2890 * SECOND;
  await orders.metadata();
  assert.equal(p.issued.length, 5, "no new exchange within 30 s of the failed one");
  assert.deepEqual(p.served.map(c => c.headers.authorization), ["Bearer jwt-1", "Bearer jwt-1"]);
});

clocked("a plain append whose expired token's exchange the issuer answers 502 rejects with that CredentialExchangeError and sends nothing to the cell", async () => {
  const gateway = () => new Response("bad gateway", { status: 502 });
  const p = platform({ expiresIn: 600 }, gateway(), gateway(), gateway());
  const client = await StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, fetch: p.fetch });
  const orders = client.stream("orders", { encryptionKey: KEY });
  clock = T0 + 600 * SECOND;
  for (const append of [s => s.append({ n: 1 }), s => s.appendBytes(new Uint8Array([1])), s => s.appendMany([{ n: 1 }])]) {
    const error = await rejection(append(orders));
    assert.ok(error instanceof CredentialExchangeError, error.name);
    assert.deepEqual([error.status, error.code, error.retryable], [502, "exchange_failed", false]);
  }
  assert.equal(p.issued.length, 4);
  assert.equal(p.served.length, 0);
});

clocked("a 401 from the service exchanges the credential again and replays", async () => {
  const p = platform({});
  p.refused.push("Bearer jwt-1");
  const client = await StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, fetch: p.fetch });
  await client.stream("orders", { encryptionKey: KEY }).metadata();
  assert.equal(p.issued.length, 2);
  assert.deepEqual(p.served.map(c => c.headers.authorization), ["Bearer jwt-1", "Bearer jwt-2"]);
});

clocked("connect rejects with the exchange's refusal, and its retry option reaches the exchange", async () => {
  const p = platform(envelope(401, "invalid_credential", false));
  const refused = await rejection(StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, fetch: p.fetch }));
  assert.ok(refused instanceof CredentialExchangeError);
  assert.deepEqual([refused.status, refused.code, p.issued.length, p.served.length], [401, "invalid_credential", 1, 0]);
  const q = platform(new TypeError("fetch failed"));
  const lost = await rejection(StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, fetch: q.fetch, retry: { transport: false } }));
  assert.ok(lost instanceof StreamsTransportError);
  assert.equal(q.issued.length, 1);
});

// ---- generateEncryptionKey ------------------------------------------------

const { generateEncryptionKey } = sdk;

test("generateEncryptionKey answers 32 random bytes as 43 characters of unpadded base64url", async () => {
  const keys = [await generateEncryptionKey(), await generateEncryptionKey()];
  for (const key of keys) {
    assert.match(key, /^[A-Za-z0-9_-]{43}$/);
    assert.equal(Buffer.from(key, "base64url").length, 32);
  }
  assert.notEqual(keys[0], keys[1]);
});

test("generateEncryptionKey encodes the bytes WebCrypto fills, URL-safe and without padding", async () => {
  // Node 18 has no global crypto; the SDK then uses node:crypto's webcrypto.
  const webcrypto = globalThis.crypto ?? (await import("node:crypto")).webcrypto;
  const bytes = Uint8Array.from({ length: 32 }, (_, i) => 0xf8 + (i % 8));
  const expected = Buffer.from(bytes).toString("base64url");
  assert.match(expected, /[-_]/, "the bytes exercise both URL-safe characters");
  webcrypto.getRandomValues = (array) => { array.set(bytes); return array; };
  try {
    assert.equal(await generateEncryptionKey(), expected);
  } finally {
    delete webcrypto.getRandomValues;
  }
});

// ---- client.usage ---------------------------------------------------------

const PROJECT_USAGE = { accountId: "acct-1", projectId: "proj 1", month: "2026-10", ingestPayloadBytes: 10, effective: { storageByteSeconds: "0" } };

function usageClient(answers, options = {}) {
  const sent = [];
  const client = new StreamsClient({ url: "https://cell.example", token: "tok", project: "proj 1", ...options, fetch: async (url, init) => {
    sent.push({ url: String(url), method: init.method, headers: { ...init.headers } });
    const answer = answers[sent.length - 1];
    return answer ? answer() : json(PROJECT_USAGE);
  } });
  return { client, sent };
}

test("usage reads the project's usage, for a month when one is given, without the stream key", async () => {
  const { client, sent } = usageClient([]);
  assert.deepEqual(await client.usage(), PROJECT_USAGE);
  assert.deepEqual(await client.usage({ month: "2026-09" }), PROJECT_USAGE);
  assert.deepEqual(sent.map(s => [s.method, s.url]), [
    ["GET", "https://cell.example/v1/projects/proj%201/usage"],
    ["GET", "https://cell.example/v1/projects/proj%201/usage?month=2026-09"],
  ]);
  assert.deepEqual(sent[0].headers, { authorization: "Bearer tok" });
});

test("usage needs the client's project and sends nothing without it", async () => {
  const { client, sent } = usageClient([], { project: undefined });
  const error = await rejection(client.usage());
  assert.ok(error instanceof StreamsError);
  assert.deepEqual([error.status, error.code, sent.length], [400, "project_required", 0]);
});

test("a prefix-restricted credential's 403 prefix_denied and a 400 invalid_month surface as the server's StreamsError", async () => {
  const { client, sent } = usageClient([
    () => envelope(403, "prefix_denied", false),
    () => envelope(400, "invalid_month", false),
  ]);
  const denied = await rejection(client.usage());
  assert.ok(denied instanceof StreamsError);
  assert.deepEqual([denied.status, denied.code, denied.retryable], [403, "prefix_denied", false]);
  const month = await rejection(client.usage({ month: "2026-13" }));
  assert.deepEqual([month.status, month.code], [400, "invalid_month"]);
  assert.equal(sent.length, 2);
});

clocked("a connected client reads its credential's project's usage", async () => {
  const p = platform({});
  const client = await StreamsClient.connect({ credential: CREDENTIAL, issuerUrl: ISSUER, fetch: p.fetch });
  await client.usage({ month: "2026-10" });
  assert.deepEqual(p.served.map(c => [c.url, c.headers.authorization]), [
    ["https://cell.example/v1/projects/proj-1/usage?month=2026-10", "Bearer jwt-1"],
  ]);
});
