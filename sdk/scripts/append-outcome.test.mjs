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

const unknownRows = [
  {
    label: "a transport failure",
    answer: thrown(new TypeError("fetch failed")),
    status: 0,
    what: "transport failure",
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
      assert.equal(error.message,
        `the append was sent once and its outcome is unknown: the SDK cannot tell whether it was written (${row.what}); ` +
        "it was not and will not be replayed; retry only if a second copy is acceptable, or append through a producer");
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

// Pins: answers that prove nothing was written, or that S-1 leaves alone.

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
