import assert from "node:assert/strict";
import test from "node:test";
import * as sdk from "../dist/index.js";

const { StreamsClient, StreamsError, StreamsTransportError, WrongCellError } = sdk;

const APPENDED = '{"cursor":"c","count":1,"duplicate":false,"sealed":false}';
const JSON_HEADERS = { "content-type": "application/json" };

const plainAppends = [
  { name: "append", path: "/records", run: s => s.append({ n: 1 }) },
  { name: "appendBytes", path: "/records", run: s => s.appendBytes(new Uint8Array([1, 2, 3])) },
  { name: "appendMany", path: "/records:batch", run: s => s.appendMany([{ n: 1 }, { n: 2 }]) },
];

// Each request takes the next answer; once they run out every request
// succeeds, so a replay the SDK must not make resolves instead of rejecting.
function stream(answers, options = {}) {
  const calls = [];
  const s = new StreamsClient({ url: "https://cell.example", ...options, fetch: async (url, init) => {
    calls.push({ url: String(url), method: init.method, headers: { ...init.headers }, body: init.body });
    const answer = answers[calls.length - 1];
    return answer ? answer() : new Response(APPENDED, { headers: JSON_HEADERS });
  } }).stream("orders", { encryptionKey: "A".repeat(43) });
  return { s, calls };
}

const thrown = value => () => { throw value; };
const answer = (status, body, headers = {}) => () => new Response(body, { status, headers });

async function rejection(call) {
  try {
    await call;
  } catch (error) {
    return error;
  }
  assert.fail("the request resolved");
}

const STRANDED = "the shard moved while this append was in flight; it may or may not have been written; " +
  "retry only if a second copy is acceptable, or append through a producer";

const unknownMessage = what =>
  `the append's last attempt ended in ${what}, so the SDK cannot tell whether it was written; ` +
  "it will not send it again; retry only if a second copy is acceptable, or append through a producer";

const unknownRows = [
  {
    label: "a transport failure",
    answer: thrown(new TypeError("fetch failed")),
    status: 0,
    what: "a transport failure",
  },
  {
    label: "a 408 append_failed",
    answer: answer(408, '{"error":{"code":"append_failed","message":"append failed","retryable":false}}', JSON_HEADERS),
    status: 408,
    what: "HTTP 408 append_failed",
    cause: { code: "append_failed", message: "append failed", retryable: false },
  },
  {
    label: "a 500 with the product body",
    answer: answer(500, '{"error":{"code":"append_failed","message":"append failed","retryable":false}}', JSON_HEADERS),
    status: 500,
    what: "HTTP 500 append_failed",
    cause: { code: "append_failed", message: "append failed", retryable: false },
  },
  {
    label: "a 502 with an empty body",
    answer: answer(502, ""),
    status: 502,
    what: "HTTP 502 http_error",
    cause: { code: "http_error", message: "HTTP 502", retryable: false },
  },
  {
    label: "a 504 with a text body",
    answer: answer(504, "upstream request timeout", { "content-type": "text/plain" }),
    status: 504,
    what: "HTTP 504 http_error",
    cause: { code: "http_error", message: "HTTP 504", retryable: false },
  },
  // A 503 is replayed only on the server's own `"retryable": true`; any
  // other 503 may follow a write. The server's answer for a group its
  // retiring engine stranded (src/product/append_error.rs):
  {
    label: "a 503 outcome_unknown",
    answer: answer(503, `{"error":{"code":"outcome_unknown","message":"${STRANDED}","retryable":false}}`,
      { ...JSON_HEADERS, "cache-control": "no-store", "retry-after": "1" }),
    status: 503,
    what: "HTTP 503 outcome_unknown",
    cause: { code: "outcome_unknown", message: STRANDED, retryable: false },
  },
  // The same code with the flag set: the code alone decides.
  {
    label: "a 503 outcome_unknown that says retryable",
    answer: answer(503, `{"error":{"code":"outcome_unknown","message":"${STRANDED}","retryable":true}}`,
      { ...JSON_HEADERS, "retry-after": "1" }),
    status: 503,
    what: "HTTP 503 outcome_unknown",
    cause: { code: "outcome_unknown", message: STRANDED, retryable: true },
  },
  // An edge's 503 without the server's body: the instance may have taken
  // the request before it went away. The cause keeps errorFrom's reading.
  {
    label: "a 503 with an empty body",
    answer: answer(503, ""),
    status: 503,
    what: "HTTP 503 http_error",
    cause: { code: "http_error", message: "HTTP 503", retryable: true },
  },
  // A JSON body without the field: the pre-auth survival refusal's body
  // before SDK-E2. No answer of this server has that shape now, so nothing
  // says that whoever sent it wrote nothing.
  {
    label: "a 503 whose JSON body names no retryable",
    answer: answer(503, '{"error":{"code":"overloaded","message":"retry"}}', { ...JSON_HEADERS, "retry-after": "1" }),
    status: 503,
    what: "HTTP 503 overloaded",
    cause: { code: "overloaded", message: "retry", retryable: true },
  },
];

for (const op of plainAppends) {
  for (const row of unknownRows) {
    test(`${op.name}: ${row.label} is AppendOutcomeUnknownError, sent once`, async () => {
      const { s, calls } = stream([row.answer]);
      const error = await rejection(op.run(s));
      assert.equal(calls.length, 1, "a plain append whose outcome is unknown is never replayed");
      assert.equal(calls[0].method, "POST");
      assert.ok(calls[0].url.endsWith(`/v1/streams/orders${op.path}`));
      assert.equal(error.name, "AppendOutcomeUnknownError");
      assert.ok(error instanceof sdk.AppendOutcomeUnknownError);
      assert.ok(error instanceof StreamsError);
      assert.equal(error.status, row.status);
      assert.equal(error.code, "outcome_unknown");
      assert.equal(error.retryable, false);
      assert.equal(error.message, unknownMessage(row.what));
      if (row.cause) {
        assert.ok(error.cause instanceof StreamsError, "the cause is the server's answer");
        assert.equal(error.cause.status, row.status);
        assert.equal(error.cause.code, row.cause.code);
        assert.equal(error.cause.message, row.cause.message);
        assert.equal(error.cause.retryable, row.cause.retryable);
      } else {
        assert.ok(error.cause instanceof TypeError, "the cause is the value fetch threw");
        assert.equal(error.cause.message, "fetch failed");
      }
    });
  }
}

// A token provider's rejection is not an answer to the append: the append
// was not sent with that token (or, after a 401, the cell had refused it),
// so the provider's error reaches the caller unchanged.
for (const op of plainAppends) {
  for (const [label, failure] of [
    ["a StreamsTransportError", new StreamsTransportError(new TypeError("issuer unreachable"))],
    ["a StreamsError 500", new StreamsError(500, "exchange_failed", "HTTP 500")],
    ["a StreamsError 408", new StreamsError(408, "exchange_failed", "HTTP 408")],
  ]) {
    test(`${op.name}: a token provider that rejects with ${label} rejects with that value and sends nothing`, async () => {
      const { s, calls } = stream([], { tokenProvider: () => Promise.reject(failure) });
      assert.equal(await rejection(op.run(s)), failure);
      assert.equal(calls.length, 0);
    });
  }

  test(`${op.name}: a 401 whose token refresh rejects gives the provider's error after one request`, async () => {
    const failure = new StreamsError(502, "exchange_failed", "HTTP 502");
    let provided = 0;
    const tokenProvider = () => ++provided === 1 ? "t1" : Promise.reject(failure);
    const { s, calls } = stream([answer(401, "")], { tokenProvider });
    assert.equal(await rejection(op.run(s)), failure);
    assert.equal(calls.length, 1);
    assert.equal(provided, 2);
  });
}

// A 2xx means the record was written: an answer that cannot be read after
// it is neither a retryable failure nor an unknown outcome.
const unreadable = [
  ["a 200 whose body is not JSON", () => new Response("<html>ok</html>", { headers: { "content-type": "text/html" } }), SyntaxError],
  ["a 200 whose body breaks off", () => new Response(new ReadableStream({ start(c) {
    c.enqueue(new TextEncoder().encode('{"cursor":'));
    c.error(new TypeError("terminated"));
  } }), { headers: JSON_HEADERS }), TypeError],
];
for (const op of plainAppends) {
  for (const [label, reply, Cause] of unreadable) {
    test(`${op.name}: ${label} rejects as written but unreadable, sent once`, async () => {
      const { s, calls } = stream([reply]);
      const error = await rejection(op.run(s));
      assert.equal(calls.length, 1);
      assert.ok(error instanceof StreamsError);
      assert.ok(!(error instanceof sdk.AppendOutcomeUnknownError));
      assert.equal(error.name, "StreamsError");
      assert.equal(error.status, 200);
      assert.equal(error.code, "unreadable_response");
      assert.equal(error.retryable, false);
      assert.equal(error.message,
        "the server accepted the append (HTTP 200), so it was written, but its answer could not be read; do not send it again");
      assert.ok(error.cause instanceof Cause, "the cause is the read failure");
    });
  }
}

// A producer append's 2xx whose body cannot be read is a lost answer: the
// SDK sends the same producer id, epoch, sequence and body again inside the
// transport replay (after 1, 2 and 4 s, within the four attempts), and the
// append resolves with the server's answer to the resend.

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

// One producer id's server, as decide_producer answers it: an epoch below
// its own is refused 403, its last sequence again with another body 409
// producer_sequence_reused, and with the same body (or a lower sequence)
// duplicate: true at that record's cursor; anything else is stored, its
// cursor the one after it.
function producerServer() {
  const server = { epoch: -1, seq: -1, body: undefined, cursor: undefined, stored: [] };
  const refuse = (status, code, details) => new Response(
    JSON.stringify({ error: { code, message: code, retryable: false, details } }), { status, headers: JSON_HEADERS });
  const appended = duplicate => new Response(
    JSON.stringify({ cursor: server.cursor, count: duplicate ? 0 : 1, duplicate, sealed: false }), { headers: JSON_HEADERS });
  server.answer = (epoch, seq, body) => {
    if (epoch < server.epoch) return refuse(403, "stale_producer_epoch", { currentEpoch: server.epoch });
    if (epoch === server.epoch && seq === server.seq && body !== server.body) {
      return refuse(409, "producer_sequence_reused");
    }
    if (epoch === server.epoch && seq <= server.seq) return appended(true);
    server.stored.push(body);
    Object.assign(server, { epoch, seq, body, cursor: `c${server.stored.length}` });
    return appended(false);
  };
  return server;
}

// Producer "p" against producerServer(); `script[i]`, when set, answers
// request i instead, given the server and the request's epoch, sequence
// and body.
function producerFixture(script, { autoClaim = false, client = {} } = {}) {
  const server = producerServer();
  const sent = [];
  let state = { epoch: 0, nextSeq: 0 };
  const s = new StreamsClient({ url: "https://cell.example", ...client, fetch: async (url, init) => {
    const h = init.headers;
    sent.push({ url: String(url), id: h["producer-id"], epoch: h["producer-epoch"], seq: h["producer-seq"], body: init.body });
    const request = [Number(h["producer-epoch"]), Number(h["producer-seq"]), init.body];
    const scripted = script[sent.length - 1];
    return scripted ? scripted(server, ...request) : server.answer(...request);
  } }).stream("orders", { encryptionKey: "A".repeat(43) });
  const p = s.producer("p", { autoClaim, state: {
    load: async () => ({ ...state }), save: async (_scope, next) => { state = { ...next }; },
  } });
  return { server, sent, p, state: () => state, wire: () => sent.map(c => [c.epoch, c.seq, c.body]) };
}

// A 2xx whose body cannot be read: the server took the request first
// (`written`), or something in front of it made the 2xx up and never
// forwarded the request.
const lost = (reply, written) => (server, ...request) => {
  if (written) server.answer(...request);
  return reply();
};

const producerAppends = [
  ["a producer append", "/records", (p, n) => p.append({ n }, { routingKey: "k" })],
  ["a producer appendMany", "/records:batch", (p, n) => p.appendMany([{ n }], { routingKey: "k" })],
];

for (const [name, path, run] of producerAppends) {
  for (const [label, reply] of unreadable) {
    for (const written of [true, false]) {
      test(`${name}: ${label} is sent again as a lost answer and resolves with the server's answer (${written ? "it was written: duplicate: true at its cursor" : "the 2xx was made up: written once"})`, async () => {
        const f = producerFixture([lost(reply, written)]);
        const { value: result, delays } = await fastTimers(() => run(f.p, 1));
        assert.deepEqual(delays, [1000], "sent again after 1 s, as after a transport failure");
        const first = f.sent[0];
        assert.equal(first.url, `https://cell.example/v1/streams/orders${path}`);
        assert.deepEqual(f.sent, [first, first].map(c => ({ ...c, id: "p", epoch: "0", seq: "0" })),
          "the same producer id, epoch, sequence and body again");
        assert.deepEqual(result, { cursor: "c1", count: written ? 0 : 1, duplicate: written, sealed: false },
          "the server's answer to the resend");
        assert.deepEqual(f.server.stored, [first.body], "stored once");
        assert.deepEqual(f.state(), { epoch: 0, nextSeq: 1 });

        assert.deepEqual(await run(f.p, 2), { cursor: "c2", count: 1, duplicate: false, sealed: false });
        assert.deepEqual(f.wire()[2].slice(0, 2), ["0", "1"], "the next value at the next sequence");
        assert.deepEqual(f.server.stored, [first.body, f.sent[2].body]);
        assert.deepEqual(f.state(), { epoch: 0, nextSeq: 2 });
      });
    }
  }
}

// Only when the last attempt's 2xx cannot be read either does the append
// reject: the SDK cannot tell whether it was written, so the recovery is the
// one a resend the server refused for anything but a fence or a seal names.
const RESEND =
  "send the same value on the same routing key through this producer before any other value on that key: " +
  "the server answers duplicate: true if it was written and writes it once if not; a different value sent " +
  "first is refused 409 producer_sequence_reused if it was written";
const PRODUCER_UNREADABLE =
  "the producer append's last attempt was answered HTTP 200, but its answer could not be read, so the SDK " +
  `cannot tell whether it was written; this producer keeps its sequence, so ${RESEND}`;
for (const [name, , run] of producerAppends) {
  for (const [label, reply, Cause] of unreadable) {
    for (const transport of [true, false]) {
      const attempts = transport ? 4 : 1;
      test(`${name}: ${label} at ${transport ? "each of the four attempts" : "its one attempt, the transport replay off,"} rejects unreadable_response, retryable, and the same value sent again is a duplicate`, async () => {
        const f = producerFixture(Array(attempts).fill(lost(reply, true)),
          transport ? {} : { client: { retry: { transport: false } } });
        const { value: error, delays } = await fastTimers(() => rejection(run(f.p, 1)));
        assert.deepEqual(delays, transport ? [1000, 2000, 4000] : []);
        assert.deepEqual(f.wire(), Array(attempts).fill(["0", "0", f.sent[0].body]));
        assert.ok(error instanceof StreamsError);
        assert.ok(!(error instanceof sdk.AppendOutcomeUnknownError));
        assert.equal(error.name, "StreamsError");
        assert.deepEqual([error.status, error.code, error.retryable], [200, "unreadable_response", true]);
        assert.equal(error.message, PRODUCER_UNREADABLE);
        assert.ok(error.cause instanceof Cause, "the cause is the last read failure");
        assert.deepEqual(f.state(), { epoch: 0, nextSeq: 0 }, "the producer keeps its sequence");

        assert.deepEqual(await run(f.p, 1), { cursor: "c1", count: 0, duplicate: true, sealed: false },
          "the same value again is answered duplicate: true");
        assert.deepEqual(f.server.stored, [f.sent[0].body], "stored once");
        assert.deepEqual(f.state(), { epoch: 0, nextSeq: 1 });
      });
    }
  }
}

// A resend after an unreadable 2xx is a replay after a lost answer: a fence
// refusing it is an unknown outcome (#148), never autoClaim's cue to claim
// an epoch above the fence and store the record a second time.
const FENCED =
  "another writer fenced this producer id at epoch 1: read the stream, and send the value again only if " +
  "it is absent, and only under an epoch above 1, which a producer with autoClaim claims when the resend " +
  "is refused (bumpEpoch() adds one to this producer's epoch, which can land on epoch 1, where the server " +
  "can answer the resend duplicate: true without writing it); without autoClaim, stop using this producer id";
test("a producer append: a resend after an unreadable 2xx that another writer's fence refuses is AppendOutcomeUnknownError, and autoClaim claims nothing", async () => {
  const fence = (server, ...request) => {
    server.answer(1, 0, '{"n":"successor"}');
    return server.answer(...request);
  };
  const f = producerFixture([lost(unreadable[0][1], true), fence], { autoClaim: true });
  const { value: { error }, delays } = await fastTimers(() =>
    f.p.append({ n: 1 }, { routingKey: "k" }).then(value => ({ value }), error => ({ error })));
  assert.deepEqual(f.server.stored, ['{"n":1}', '{"n":"successor"}'], "stored once");
  assert.deepEqual(f.wire(), [["0", "0", '{"n":1}'], ["0", "0", '{"n":1}']], "the append and its resend only");
  assert.ok(error instanceof sdk.AppendOutcomeUnknownError, `rejected as an unknown outcome, got ${error?.name}`);
  assert.deepEqual([error.status, error.code, error.retryable], [403, "outcome_unknown", false]);
  assert.equal(error.message,
    "the producer append's outcome is unknown: an attempt's answer was lost and the server refused its " +
    "replay (HTTP 403 stale_producer_epoch), so the SDK cannot tell whether it was written; no new epoch " +
    `was claimed for it; ${FENCED}`);
  assert.ok(error.cause instanceof sdk.ProducerFencedError, "the cause is the fence");
  assert.equal(error.cause.currentEpoch, 1);
  assert.deepEqual(f.state(), { epoch: 0, nextSeq: 0 }, "no epoch claimed, no sequence consumed");
  assert.deepEqual(delays, [1000]);
});

// The routing key's queue waits for the resend: appends queued behind it go
// out after it, at the next sequences.
for (const written of [true, false]) {
  test(`producer appends queued behind an unreadable 2xx go out after its resend, at the next sequences (${written ? "it was written" : "the 2xx was made up"})`, async () => {
    const f = producerFixture([lost(unreadable[0][1], written)]);
    const { value: settled, delays } = await fastTimers(() =>
      Promise.allSettled([1, 2, 3].map(n => f.p.append({ n }, { routingKey: "k" }))));
    assert.deepEqual(f.wire(), [
      ["0", "0", '{"n":1}'], ["0", "0", '{"n":1}'], ["0", "1", '{"n":2}'], ["0", "2", '{"n":3}'],
    ], "the resend, then the queued appends at the next sequences");
    assert.deepEqual(settled.map(r => r.status === "fulfilled" ? [r.value.cursor, r.value.duplicate] : r.reason.code),
      [["c1", written], ["c2", false], ["c3", false]]);
    assert.deepEqual(f.server.stored, ['{"n":1}', '{"n":2}', '{"n":3}'], "each stored once, in order");
    assert.deepEqual(f.state(), { epoch: 0, nextSeq: 3 });
    assert.deepEqual(delays, [1000]);
  });
}

// Pins: answers that prove nothing was written, or that S-1 leaves alone.

test("append: a retryable 503 and then a transport failure are two requests and the unknown outcome of the last", async () => {
  const failure = new TypeError("fetch failed");
  const { s, calls } = stream([
    answer(503, '{"error":{"code":"temporarily_unavailable","message":"retry shortly","retryable":true}}',
      { ...JSON_HEADERS, "retry-after": "0" }),
    thrown(failure),
  ]);
  const error = await rejection(s.append({ n: 1 }));
  assert.equal(calls.length, 2, "the refusal was replayed; the transport failure was not");
  assert.ok(error instanceof sdk.AppendOutcomeUnknownError);
  assert.equal(error.status, 0);
  assert.equal(error.cause, failure);
  assert.equal(error.message, unknownMessage("a transport failure"));
});

test("append: a retryable 503 and then a 503 outcome_unknown are two requests and the unknown outcome of the last", async () => {
  const { s, calls } = stream([
    answer(503, '{"error":{"code":"temporarily_unavailable","message":"retry shortly","retryable":true}}',
      { ...JSON_HEADERS, "retry-after": "0" }),
    answer(503, `{"error":{"code":"outcome_unknown","message":"${STRANDED}","retryable":false}}`,
      { ...JSON_HEADERS, "retry-after": "1" }),
  ]);
  const error = await rejection(s.append({ n: 1 }));
  assert.equal(calls.length, 2, "the refusal was replayed; the unknown outcome was not");
  assert.ok(error instanceof sdk.AppendOutcomeUnknownError);
  assert.equal(error.status, 503);
  assert.equal(error.cause.code, "outcome_unknown");
  assert.equal(error.message, unknownMessage("HTTP 503 outcome_unknown"));
});

for (const op of plainAppends) {
  test(`${op.name}: a retryable 503 temporarily_unavailable is replayed and the replay succeeds`, async () => {
    const { s, calls } = stream([answer(503,
      '{"error":{"code":"temporarily_unavailable","message":"retry shortly","retryable":true}}',
      { ...JSON_HEADERS, "retry-after": "0" })]);
    assert.deepEqual(await op.run(s), JSON.parse(APPENDED));
    assert.equal(calls.length, 2);
  });

  // The server's pre-auth survival refusal (src/http.rs track_inflight),
  // answered before any route, so nothing was written (SDK-E2).
  test(`${op.name}: the pre-auth survival refusal is replayed and the replay succeeds`, async () => {
    const { s, calls } = stream([answer(503,
      '{"error":{"code":"overloaded","message":"retry","retryable":true}}',
      { ...JSON_HEADERS, "retry-after": "0" })]);
    assert.deepEqual(await op.run(s), JSON.parse(APPENDED));
    assert.equal(calls.length, 2);
    assert.equal(calls[1].url, calls[0].url);
    assert.deepEqual(calls[1].body, calls[0].body);
  });

  test(`${op.name}: a 429 is replayed and the replay succeeds`, async () => {
    const { s, calls } = stream([answer(429,
      '{"error":{"code":"rate_limited","message":"slow down","retryable":true}}',
      { ...JSON_HEADERS, "retry-after": "0" })]);
    assert.deepEqual(await op.run(s), JSON.parse(APPENDED));
    assert.equal(calls.length, 2);
  });
}

test("append: a 503 still refused after the retry budget stays a retryable StreamsError", async () => {
  const busy = answer(503,
    '{"error":{"code":"temporarily_unavailable","message":"retry shortly","retryable":true}}',
    { ...JSON_HEADERS, "retry-after": "0" });
  const { s, calls } = stream([busy, busy, busy, busy]);
  const error = await rejection(s.append({ n: 1 }));
  assert.equal(calls.length, 4);
  assert.equal(error.name, "StreamsError");
  assert.equal(error.status, 503);
  assert.equal(error.code, "temporarily_unavailable");
  assert.equal(error.retryable, true);
});

test("append: a 421 is WrongCellError", async () => {
  const { s, calls } = stream([answer(421, "")]);
  const error = await rejection(s.append({ n: 1 }));
  assert.equal(calls.length, 1);
  assert.ok(error instanceof WrongCellError);
});

test("append: a 400 is the server's StreamsError", async () => {
  const { s, calls } = stream([answer(400,
    '{"error":{"code":"invalid_body","message":"body is not JSON","retryable":false}}', JSON_HEADERS)]);
  const error = await rejection(s.append({ n: 1 }));
  assert.equal(calls.length, 1);
  assert.equal(error.name, "StreamsError");
  assert.equal(error.status, 400);
  assert.equal(error.code, "invalid_body");
  assert.equal(error.retryable, false);
});

for (const [label, run] of [
  ["a producer append", s => s.producer("p", { ephemeral: true }).append({ n: 1 })],
  ["a producer appendMany", s => s.producer("p", { ephemeral: true }).appendMany([{ n: 1 }])],
  ["seal", s => s.seal()],
]) {
  // With the transport replay off (each of these replays one by default),
  // the failure is the request's own, not an append's unknown outcome.
  test(`${label}: a transport failure stays StreamsTransportError`, async () => {
    const failure = new TypeError("fetch failed");
    const { s, calls } = stream([thrown(failure)], { retry: { transport: false } });
    const error = await rejection(run(s));
    assert.equal(calls.length, 1);
    assert.ok(error instanceof StreamsTransportError);
    assert.equal(error.code, "transport_error");
    assert.equal(error.cause, failure);
  });
}

test("a producer append: a 408 stays the server's StreamsError", async () => {
  const { s, calls } = stream([answer(408,
    '{"error":{"code":"append_failed","message":"append failed","retryable":false}}', JSON_HEADERS)]);
  const error = await rejection(s.producer("p", { ephemeral: true }).append({ n: 1 }));
  assert.equal(calls.length, 1);
  assert.equal(error.name, "StreamsError");
  assert.equal(error.status, 408);
  assert.equal(error.code, "append_failed");
});

// Pins: the plain append's 503 rule is its own. Requests the server
// deduplicates (a producer's triple, a final seal's operation id) and every
// other request replay any 503 that does not say `"retryable": false`.
const edge503 = answer(503, "", { "retry-after": "0" });
const silent503 = answer(503, '{"error":{"code":"overloaded","message":"retry"}}', { ...JSON_HEADERS, "retry-after": "0" });

for (const [label, path, run] of [
  ["a producer append", "/records", s => s.producer("p", { ephemeral: true }).append({ n: 1 }, { routingKey: "k" })],
  ["a producer appendMany", "/records:batch", s => s.producer("p", { ephemeral: true }).appendMany([{ n: 1 }], { routingKey: "k" })],
  ["a seal with a final record", ":seal", s => s.seal({ final: { n: 1 }, routingKey: "k" })],
  ["a producer's seal with a final record", ":seal",
    s => s.seal({ final: { n: 1 }, routingKey: "k", producer: s.producer("p", { ephemeral: true }) })],
]) {
  for (const [what, reply] of [["an empty body", edge503], ["a JSON body that names no retryable", silent503]]) {
    test(`${label}: a 503 with ${what} is sent again, the same request, and the replay succeeds`, async () => {
      const { s, calls } = stream([reply]);
      await run(s);
      assert.equal(calls.length, 2);
      const [first, second] = calls;
      assert.ok(first.url.endsWith(`/v1/streams/orders${path}`));
      assert.equal(second.url, first.url);
      assert.equal(second.body, first.body);
      assert.deepEqual(second.headers, first.headers);
    });
  }
}

for (const [what, reply] of [["an empty body", edge503], ["a JSON body that names no retryable", silent503]]) {
  test(`metadata: a 503 with ${what} is sent again and the replay succeeds`, async () => {
    const { s, calls } = stream([reply, () => new Response('{"name":"orders"}', { headers: JSON_HEADERS })]);
    assert.deepEqual(await s.metadata(), { name: "orders" });
    assert.equal(calls.length, 2);
  });
}
