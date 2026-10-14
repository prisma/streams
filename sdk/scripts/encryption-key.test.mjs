import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";
import { StreamsClient, StreamsError, generateEncryptionKey } from "../dist/index.js";

// A cell on loopback, reached through the runtime's own fetch, whose header
// checks are the ones a garbled key meets: it answers every request 200 and
// records the encryption key it was sent.
async function cell() {
  const keys = [];
  const server = http.createServer((req, res) => {
    keys.push(req.headers["prisma-encryption-key"]);
    req.resume();
    req.on("end", () => {
      res.setHeader("content-type", "application/json");
      res.end('{"cursor":"c","count":1,"duplicate":false,"sealed":false}');
    });
  });
  await new Promise(listening => server.listen(0, "127.0.0.1", listening));
  let calls = 0;
  const client = new StreamsClient({
    url: `http://127.0.0.1:${server.address().port}`,
    fetch: (url, init) => { calls++; return fetch(url, init); },
  });
  return { client, keys, calls: () => calls, close: () => new Promise(closed => server.close(closed)) };
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
    return { value: await run() };
  } catch (error) {
    return { error };
  }
}

const SECRET = "SECRETKEYPART";
const REFUSAL =
  "the encryption key is not printable ASCII without spaces (a line break, a control character, a " +
  "space or a character beyond ASCII is inside it, or it is not a string); copy it again from where " +
  "you keep it";

// A key pasted with a line break (an env file, a wrapped terminal) or from a
// document. fetch refuses a line break, a carriage return or a NUL in an
// error that quotes the whole value, any other control character in one
// that does not, and a character above U+00FF while converting the value;
// it sends a tab, a space or a Latin-1 character, which the cell refuses.
const garbled = [
  ["a line break inside it", `AAAA\n${SECRET}`],
  ["a carriage return inside it", `AAAA\r${SECRET}`],
  ["a NUL inside it", `AAAA\0${SECRET}`],
  ["another control character inside it", `AAAA\x01${SECRET}`],
  ["a DEL inside it", `AAAA\x7f${SECRET}`],
  ["a vertical tab at its end", `AAAA${SECRET}\x0b`],
  ["a character above U+00FF inside it", `AAAA’${SECRET}`],
  ["a zero-width space inside it", `AAAA​${SECRET}`],
  ["a tab inside it", `AAAA\t${SECRET}`],
  ["a space inside it", `AAAA ${SECRET}`],
  ["a Latin-1 character inside it", `AAAAé${SECRET}`],
];

function assertRefused(error, label) {
  assert.ok(error instanceof StreamsError, `${label}: got ${error?.name}: ${error?.message}`);
  assert.deepEqual([error.name, error.status, error.code, error.retryable],
    ["StreamsError", 0, "invalid_encryption_key_format", false], label);
  assert.equal(error.message, REFUSAL, label);
  const shown = JSON.stringify({ ...error, message: error.message, stack: error.stack, cause: String(error.cause) });
  assert.ok(!shown.includes(SECRET), `${label}: the error does not carry the key`);
}

for (const [label, key] of garbled) {
  test(`a key with ${label} is refused before any request, without echoing it`, async () => {
    const c = await cell();
    try {
      const created = await fastTimers(() => outcome(() =>
        c.client.createStream("orders", { encryptionKey: key, format: { kind: "json" } })));
      assertRefused(created.error, "createStream");
      const handle = await fastTimers(() => outcome(async () => {
        const stream = c.client.stream("orders", { encryptionKey: key });
        await stream.metadata();
        await stream.append({ n: 1 });
        return stream;
      }));
      assertRefused(handle.error, "stream");
      assert.equal(c.calls(), 0, "no request is made");
      assert.deepEqual(c.keys, []);
    } finally {
      await c.close();
    }
  });
}

test("a key that is not a string is refused before any request", async () => {
  const c = await cell();
  try {
    assertRefused((await outcome(async () => c.client.stream("orders", {}))).error, "stream");
    assertRefused((await outcome(() => c.client.createStream("orders", { format: { kind: "json" } }))).error, "createStream");
    assert.equal(c.calls(), 0);
  } finally {
    await c.close();
  }
});

test("whitespace around the key is trimmed, as fetch trims a header value, and the key is sent as given", async () => {
  const generated = await generateEncryptionKey();
  const standard = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=";
  const c = await cell();
  try {
    await c.client.createStream("orders", { encryptionKey: ` ${generated}\r\n`, format: { kind: "json" } });
    await c.client.stream("orders", { encryptionKey: `\t${standard}\n` }).append({ n: 1 });
    await c.client.stream("orders", { encryptionKey: generated }).append({ n: 2 });
    assert.deepEqual(c.keys, [generated, standard, generated]);
    assert.match(generated, /^[A-Za-z0-9_-]{43}$/);
  } finally {
    await c.close();
  }
});
