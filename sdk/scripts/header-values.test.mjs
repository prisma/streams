import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";
import { MemoryProducerStateStore, StreamsClient, StreamsError } from "../dist/index.js";

// A cell on loopback, reached through the runtime's own fetch, so fetch's own
// header handling is what a value meets. It answers every request 200 and
// records what reached it: the method, the path and query as sent, and the
// routing key, producer id and authorization headers as their bytes (Node
// reads a header value as Latin-1, one character per byte). Assertions are on
// the SDK's own errors and on these bytes, never on a runtime's message text.
// A records read is answered up to date, and a long poll also sealed, so a
// subscription reads once, polls once and ends.
async function cell() {
  const seen = [];
  const server = http.createServer((req, res) => {
    const header = {};
    for (let i = 0; i < req.rawHeaders.length; i += 2) {
      header[req.rawHeaders[i].toLowerCase()] = req.rawHeaders[i + 1];
    }
    req.resume();
    req.on("end", () => {
      seen.push({
        method: req.method,
        url: req.url,
        routingKey: header["streams-routing-key"],
        producerId: header["producer-id"],
        authorization: header["authorization"],
      });
      if (req.method === "GET" && /\/records(:long-poll)?\?/.test(req.url)) {
        res.setHeader("prisma-next-cursor", "c");
        res.setHeader("prisma-up-to-date", "true");
        if (req.url.includes("/records:long-poll?")) res.setHeader("prisma-sealed", "true");
        res.setHeader("content-type", "application/json");
        res.end("[]");
        return;
      }
      res.setHeader("content-type", "application/json");
      res.end(req.url.startsWith("/v1/token/")
        ? '{"accessToken":"jwt","expiresIn":600,"projectId":"p","endpoint":null}'
        : '{"cursor":"c","count":1,"duplicate":false,"sealed":false}');
    });
  });
  await new Promise(listening => server.listen(0, "127.0.0.1", listening));
  const url = `http://127.0.0.1:${server.address().port}`;
  let calls = 0;
  const counted = (input, init) => { calls++; return fetch(input, init); };
  return {
    url,
    seen,
    fetch: counted,
    calls: () => calls,
    client: options => new StreamsClient({ url, fetch: counted, ...options }),
    close: () => new Promise(closed => server.close(closed)),
  };
}

// Every backoff runs at once, so a replay the SDK must not make is quick.
async function fastTimers(run) {
  const native = globalThis.setTimeout;
  globalThis.setTimeout = (callback, _delay, ...args) => native(callback, 0, ...args);
  try {
    return await run();
  } finally {
    globalThis.setTimeout = native;
  }
}

async function outcome(run) {
  try {
    return { value: await fastTimers(run) };
  } catch (error) {
    return { error };
  }
}

const KEY = "A".repeat(43);
const SECRET = "SECRETPART";
const ROUTING_KEY_REFUSAL = "the routing key is not text a header carries as it is (printable ASCII and " +
  "tabs, no whitespace at either end, at most 1,024 characters); nothing was sent";
const PRODUCER_REFUSAL = "the producer id is not text a header carries as it is (printable ASCII and " +
  "tabs, no whitespace at either end, not empty); nothing was sent";
const TOKEN_REFUSAL = "the bearer token (the client's token, or one a tokenProvider or the issuer " +
  "returned) is not text a header carries as it is (printable ASCII and tabs, no whitespace at either " +
  "end, not empty); nothing was sent";

function assertRefused(error, code, message, label) {
  assert.ok(error instanceof StreamsError, `${label}: got ${error?.name}: ${error?.message}`);
  assert.deepEqual([error.name, error.status, error.code, error.retryable],
    ["StreamsError", 0, code, false], label);
  assert.equal(error.message, message, label);
  assert.equal(error.cause, undefined, label);
  const shown = JSON.stringify({ ...error, message: error.message, stack: error.stack });
  assert.ok(!shown.includes(SECRET), `${label}: the error does not carry the value`);
}

// Values a header cannot carry byte for byte. fetch refuses a line break, a
// carriage return or a NUL inside a value, and any other control character
// but a tab, in a TypeError (quoting the value for the first three), and
// cannot convert a character above U+00FF; it strips a space, a tab, a line
// break or a carriage return at either end without a word, so the value
// would be sent as another; it sends a Latin-1 character, which the server
// refuses. Each holds SECRET, so an error that showed it would be seen.
const unsendable = [
  ["a line break inside it", `k\n${SECRET}`],
  ["a carriage return inside it", `k\r${SECRET}`],
  ["a NUL inside it", `k\0${SECRET}`],
  ["another control character inside it", `k\x01${SECRET}`],
  ["a DEL inside it", `k\x7f${SECRET}`],
  ["a character above U+00FF inside it", `k’${SECRET}`],
  ["an emoji inside it", `k\u{1f600}${SECRET}`],
  ["a Latin-1 character inside it", `ké${SECRET}`],
  ["a space at its start", ` ${SECRET}`],
  ["a space at its end", `${SECRET} `],
  ["a tab at its start", `\t${SECRET}`],
  ["a tab at its end", `${SECRET}\t`],
  ["a line break at its start", `\n${SECRET}`],
  ["a line break at its end", `${SECRET}\n`],
  ["a carriage return and line break at its end", `${SECRET}\r\n`],
  ["only whitespace", " \t "],
];

const routingKeys = [
  ...unsendable,
  ["1,025 characters", SECRET + "k".repeat(1025 - SECRET.length)],
  ["a number, not a string", 42],
];

// Every write that sends a routing key: the plain appends, a producer's, and
// a final seal's (in its JSON body), alone and through a producer.
function writes(stream, routingKey) {
  const producer = () => stream.producer("p", { ephemeral: true });
  return [
    ["append", () => stream.append({ n: 1 }, { routingKey })],
    ["appendBytes", () => stream.appendBytes(new Uint8Array([1]), { routingKey })],
    ["appendMany", () => stream.appendMany([{ n: 1 }], { routingKey })],
    ["Producer.append", () => producer().append({ n: 1 }, { routingKey })],
    ["Producer.appendMany", () => producer().appendMany([{ n: 1 }], { routingKey })],
    ["seal", () => stream.seal({ final: { n: 1 }, routingKey })],
    ["seal through a producer", () => stream.seal({ final: { n: 1 }, routingKey, producer: producer() })],
  ];
}

for (const [label, routingKey] of routingKeys) {
  test(`a routing key with ${label} is refused by every write before any request`, async () => {
    const c = await cell();
    try {
      const stream = c.client({}).stream("orders", { encryptionKey: KEY });
      for (const [write, run] of writes(stream, routingKey)) {
        assertRefused((await outcome(run)).error, "invalid_routing_key", ROUTING_KEY_REFUSAL, write);
      }
      assert.equal(c.calls(), 0, "no request is made");
      assert.deepEqual(c.seen, []);
    } finally {
      await c.close();
    }
  });
}

test("a routing key a header carries is sent byte for byte, and an empty or absent one is the default lane", async () => {
  const c = await cell();
  try {
    const stream = c.client({}).stream("orders", { encryptionKey: KEY });
    const kept = ["a\tb", "a b", "a+b", "a%20b", "!~", "k".repeat(1024)];
    for (const routingKey of kept) {
      await stream.append({ n: 1 }, { routingKey });
      await stream.appendMany([{ n: 1 }], { routingKey });
      await stream.producer("p", { ephemeral: true }).append({ n: 1 }, { routingKey });
    }
    for (const routingKey of ["", undefined]) {
      await stream.append({ n: 1 }, { routingKey });
      await stream.producer("p", { ephemeral: true }).appendMany([{ n: 1 }], { routingKey });
    }
    assert.deepEqual(c.seen.map(s => s.routingKey),
      [...kept.flatMap(k => [k, k, k]), undefined, undefined, undefined, undefined]);
  } finally {
    await c.close();
  }
});

// A query value as the server reads it (src/product.rs parse_query): the
// query split on '&', each pair at its first '=', and only %XX decoded; a
// '+' is not a space.
function queryValue(url, name) {
  for (const pair of url.slice(url.indexOf("?") + 1).split("&")) {
    const eq = pair.indexOf("=");
    if ((eq < 0 ? pair : pair.slice(0, eq)) !== name) continue;
    const value = eq < 0 ? "" : pair.slice(eq + 1);
    const bytes = [];
    for (let i = 0; i < value.length; i++) {
      const hex = value.slice(i + 1, i + 3);
      if (value[i] === "%" && /^[0-9A-Fa-f]{2}$/.test(hex)) {
        bytes.push(parseInt(hex, 16));
        i += 2;
      } else {
        bytes.push(...Buffer.from(value[i], "utf8"));
      }
    }
    return Buffer.from(bytes).toString("utf8");
  }
  return undefined;
}

test("read() and subscribe() ask for the lane a routing key's appends were written to", async () => {
  const c = await cell();
  try {
    const stream = c.client({}).stream("orders", { encryptionKey: KEY });
    const kept = ["a b", "a  b c", "a\tb", "a+b", "a%20b", "a&b=c", "!~", "k ".repeat(512).slice(0, 1023) + "k"];
    for (const routingKey of kept) {
      c.seen.length = 0;
      await stream.append({ n: 1 }, { routingKey });
      await stream.read({ routingKey });
      for await (const _ of stream.subscribe({ routingKey })) { /* the cell answers no records */ }
      const [appended, read, caughtUp, polled] = c.seen;
      assert.equal(appended.routingKey, routingKey, "the append's header");
      assert.match(polled.url, /\/records:long-poll\?/, "the subscription polled");
      assert.deepEqual([read, caughtUp, polled].map(r => queryValue(r.url, "routingKey")),
        [routingKey, routingKey, routingKey], JSON.stringify(routingKey));
    }
  } finally {
    await c.close();
  }
});

const producerIds = [
  ...unsendable,
  ["nothing in it", ""],
  ["no value (an unset environment variable)", undefined],
  ["a number, not a string", 42],
];

for (const [label, id] of producerIds) {
  test(`a producer id with ${label} is refused when the producer is made, before any request`, async () => {
    const c = await cell();
    try {
      const stream = c.client({}).stream("orders", { encryptionKey: KEY });
      const made = await outcome(async () => {
        const producer = stream.producer(id, { state: new MemoryProducerStateStore() });
        await producer.append({ n: 1 });
        await stream.seal({ final: { n: 1 }, producer });
      });
      assertRefused(made.error, "invalid_producer", PRODUCER_REFUSAL, "producer");
      assert.equal(c.calls(), 0, "no request is made");
    } finally {
      await c.close();
    }
  });
}

test("a producer id a header carries is sent byte for byte", async () => {
  const c = await cell();
  try {
    const stream = c.client({}).stream("orders", { encryptionKey: KEY });
    for (const id of ["p", "p q", "p\tq", "p+q"]) {
      await stream.producer(id, { ephemeral: true }).append({ n: 1 });
    }
    assert.deepEqual(c.seen.map(s => s.producerId), ["p", "p q", "p\tq", "p+q"]);
  } finally {
    await c.close();
  }
});

const tokens = [
  ...unsendable,
  ["a number, not a string", 42],
];

for (const [label, token] of tokens) {
  test(`a bearer token with ${label} is refused before any request`, async () => {
    const c = await cell();
    try {
      const fixed = await outcome(async () => {
        await c.client({ token }).stream("orders", { encryptionKey: KEY }).metadata();
      });
      assertRefused(fixed.error, "invalid_token_format", TOKEN_REFUSAL, "token");
      for (const [form, provided] of [["a string", token], ["with its expiry", { token, expiresAt: Date.now() + 3600_000 }]]) {
        const provider = await outcome(async () => {
          await c.client({ tokenProvider: () => provided }).stream("orders", { encryptionKey: KEY }).metadata();
        });
        assertRefused(provider.error, "invalid_token_format", TOKEN_REFUSAL, `tokenProvider, ${form}`);
      }
      assert.equal(c.calls(), 0, "no request is made");
    } finally {
      await c.close();
    }
  });
}

test("a tokenProvider that returned nothing, or an empty token, is refused before any request", async () => {
  const c = await cell();
  try {
    for (const provided of [undefined, "", { token: "", expiresAt: Date.now() + 3600_000 }]) {
      const provider = await outcome(async () => {
        await c.client({ tokenProvider: () => provided }).stream("orders", { encryptionKey: KEY }).metadata();
      });
      assertRefused(provider.error, "invalid_token_format", TOKEN_REFUSAL, `tokenProvider ${JSON.stringify(provided)}`);
    }
    assert.equal(c.calls(), 0, "no request is made");
  } finally {
    await c.close();
  }
});

test("a token refused from the tokenProvider is not kept: the next request asks the provider again", async () => {
  const c = await cell();
  try {
    let provided = 0;
    const stream = c.client({ tokenProvider: () => ++provided === 1 ? `t\n${SECRET}` : "t2" })
      .stream("orders", { encryptionKey: KEY });
    assertRefused((await outcome(() => stream.metadata())).error, "invalid_token_format", TOKEN_REFUSAL, "first");
    await stream.metadata();
    assert.equal(provided, 2);
    assert.deepEqual(c.seen.map(s => s.authorization), ["Bearer t2"]);
  } finally {
    await c.close();
  }
});

test("an issuer's access token a header cannot carry fails connect before any request carries it", async () => {
  const c = await cell();
  try {
    const issuer = (input, init) => input.endsWith("/v1/token/streams")
      ? Promise.resolve(new Response(JSON.stringify({ accessToken: `jwt\n${SECRET}`, expiresIn: 600, projectId: "p", endpoint: null })))
      : c.fetch(input, init);
    const connected = await outcome(() => StreamsClient.connect({
      credential: "prisma_streams_abc.def", issuerUrl: c.url, url: c.url, fetch: issuer,
    }).then(client => client.stream("orders", { encryptionKey: KEY }).metadata()));
    assertRefused(connected.error, "invalid_token_format", TOKEN_REFUSAL, "connect");
    assert.equal(c.calls(), 0, "no request carries the token");
  } finally {
    await c.close();
  }
});

test("a bearer token a header carries is sent byte for byte, and an empty client token sends none", async () => {
  const c = await cell();
  try {
    await c.client({ token: "a.b-c_d" }).stream("orders", { encryptionKey: KEY }).metadata();
    await c.client({ token: "a b" }).stream("orders", { encryptionKey: KEY }).metadata();
    await c.client({ tokenProvider: () => ({ token: "x\ty", expiresAt: Date.now() + 3600_000 }) })
      .stream("orders", { encryptionKey: KEY }).metadata();
    await c.client({ token: "" }).stream("orders", { encryptionKey: KEY }).metadata();
    await c.client({}).stream("orders", { encryptionKey: KEY }).metadata();
    assert.deepEqual(c.seen.map(s => s.authorization),
      ["Bearer a.b-c_d", "Bearer a b", "Bearer x\ty", undefined, undefined]);
  } finally {
    await c.close();
  }
});
