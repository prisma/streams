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
    calls.push({ url: String(url), method: init.method });
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
    cause: { code: "append_failed", message: "append failed" },
  },
  {
    label: "a 500 with the product body",
    answer: answer(500, '{"error":{"code":"append_failed","message":"append failed","retryable":false}}', JSON_HEADERS),
    status: 500,
    what: "HTTP 500 append_failed",
    cause: { code: "append_failed", message: "append failed" },
  },
  {
    label: "a 502 with an empty body",
    answer: answer(502, ""),
    status: 502,
    what: "HTTP 502 http_error",
    cause: { code: "http_error", message: "HTTP 502" },
  },
  {
    label: "a 504 with a text body",
    answer: answer(504, "upstream request timeout", { "content-type": "text/plain" }),
    status: 504,
    what: "HTTP 504 http_error",
    cause: { code: "http_error", message: "HTTP 504" },
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
        assert.equal(error.cause.retryable, false);
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

for (const op of plainAppends) {
  test(`${op.name}: a retryable 503 temporarily_unavailable is replayed and the replay succeeds`, async () => {
    const { s, calls } = stream([answer(503,
      '{"error":{"code":"temporarily_unavailable","message":"retry shortly","retryable":true}}',
      { ...JSON_HEADERS, "retry-after": "0" })]);
    assert.deepEqual(await op.run(s), JSON.parse(APPENDED));
    assert.equal(calls.length, 2);
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
  // With the transport replay off (seal replays one by default), the
  // failure is the request's own, not an append's unknown outcome.
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
