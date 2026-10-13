// The cell watch's canary (public-beta alerting triage B7) against a local
// cell and issuer: a round exchanges the credential at the issuer when it
// holds no token or its token is two minutes from expiring, anchors at the
// stream's tail (creating the stream when it is missing), appends one
// record and reads it back durable;
// how each step's failure is reported (rules.ts's CanaryStep, which rows
// 19 and 22 judge) and what the next round does after it; where the
// credential and the token go. Run with `bun test ./deploy/cell-watch`.
import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import {
  Canary,
  type CanaryConfig,
  canaryStreamKey,
  EXCHANGE_PATH,
  PAGE_BYTES,
  RECORDS_PATH,
  RENEW_BEFORE_MS,
  STREAM_CONFIG,
  STREAM_PATH,
} from "./canary";
import { CANARY_CREDENTIAL, CANARY_TOKEN, FakeCell, FakeIssuer, FakeServer, type Seen } from "./fake-cell";
import { TIMEOUT_MS } from "./poll";
import { type CanaryResult, type CanaryStep, RESOLVE_AFTER } from "./rules";

const running: { stop(): void; seen: Seen[] }[] = [];
const everySeen: { server: "cell" | "issuer" | "other"; seen: Seen }[] = [];
afterEach(() => {
  for (const s of running.splice(0)) {
    const server = s instanceof FakeCell ? "cell" : s instanceof FakeIssuer ? "issuer" : "other";
    everySeen.push(...s.seen.map((seen) => ({ server, seen }) as const));
    s.stop();
  }
});

const KEY = canaryStreamKey("cell_sc1");
const T0 = 1_800_000_000_000;

class Rig {
  readonly cell = new FakeCell();
  readonly issuer = new FakeIssuer();
  at = T0;
  private n = 0;

  constructor() {
    running.push(this.cell, this.issuer);
    this.issuer.endpoint = this.cell.origin;
  }

  canary(more: Partial<CanaryConfig> = {}): Canary {
    const cfg: CanaryConfig = {
      cellOrigin: this.cell.origin,
      issuerOrigin: this.issuer.origin,
      credential: CANARY_CREDENTIAL,
      cellId: "cell_sc1",
      timeoutMs: TIMEOUT_MS,
      ...more,
    };
    return new Canary(cfg, () => this.at, () => `nonce-${++this.n}`);
  }

  /// `[method, path and query]` of each request the cell got since `from`.
  cellCalls(from = 0): string[][] {
    return this.cell.seen.slice(from).map((s) => [s.method, `${s.path}${s.query}`]);
  }
}

const OK: CanaryStep = { ok: true, status: 200, errorCode: null, origin: true };
const failed = (status: number | null, errorCode: string | null, origin = true): CanaryStep =>
  ({ ok: false, status, errorCode, origin });
const exchanged = (endpoint: string): CanaryResult["exchange"] =>
  ({ ok: true, status: 200, errorCode: null, origin: false, endpoint });
const READ_FROM = (cursor: string) => `${RECORDS_PATH}?cursor=${cursor}&maxBytes=${PAGE_BYTES}`;

describe("a round of the canary", () => {
  test("exchanges the credential, finds the stream missing and creates it, anchors at its tail, appends one record and reads it back", async () => {
    const r = new Rig();
    expect(await r.canary().run()).toEqual({ exchange: exchanged(r.cell.origin), append: OK, read: OK });
    expect(r.issuer.seen.map((s) => [s.method, s.path, s.authorization, s.body])).toEqual([
      ["POST", EXCHANGE_PATH, `StreamsCredential ${CANARY_CREDENTIAL}`, ""],
    ]);
    expect(r.cellCalls()).toEqual([
      ["GET", READ_FROM("now")],
      ["PUT", STREAM_PATH],
      ["GET", READ_FROM("now")],
      ["POST", RECORDS_PATH],
      ["GET", READ_FROM("c0")],
    ]);
    expect(r.cell.seen.map((s) => [s.authorization, s.encryptionKey])).toEqual(Array(5).fill([`Bearer ${CANARY_TOKEN}`, KEY]));
    expect(r.cell.seen.map((s) => s.contentType)).toEqual([null, "application/json", null, "application/json", null]);
    expect(JSON.parse(r.cell.seen[1]!.body)).toEqual({ format: { kind: "json" }, expiry: { idle: "1d" } });
    expect(r.cell.stream?.records).toEqual([{ canary: "nonce-1", at: T0 }]);
  });

  test("a later round appends one record and reads from where the last read ended, on one connection", async () => {
    const r = new Rig();
    const c = r.canary();
    await c.run();
    r.at += 60_000;
    expect(await c.run()).toEqual({ exchange: null, append: OK, read: OK });
    expect(r.cellCalls(5)).toEqual([["POST", RECORDS_PATH], ["GET", READ_FROM("c1")]]);
    expect(r.cell.stream?.records).toEqual([{ canary: "nonce-1", at: T0 }, { canary: "nonce-2", at: T0 + 60_000 }]);
    expect(new Set(r.cell.seen.map((s) => s.port)).size).toBe(1);
  });

  test("keeps its token until two minutes before it expires, then exchanges again", async () => {
    const r = new Rig();
    const c = r.canary();
    const exchanges: (string | null)[] = [];
    // A token of 3,600 s is renewed at 3,480 s.
    for (const secs of [0, 60, 3_420, 3_479, 3_480, 3_540]) {
      r.at = T0 + secs * 1_000;
      exchanges.push((await c.run()).exchange === null ? null : `${secs}`);
    }
    expect([RENEW_BEFORE_MS, exchanges]).toEqual([120_000, ["0", null, null, null, "3480", null]]);
    expect(r.issuer.seen).toHaveLength(2);
  });

  test("a new watch finds the stream there: no create, the same key, and its record after those already there", async () => {
    const r = new Rig();
    await r.canary().run();
    const second = r.canary();
    expect(await second.run()).toEqual({ exchange: exchanged(r.cell.origin), append: OK, read: OK });
    expect(r.cellCalls(5)).toEqual([["GET", READ_FROM("now")], ["POST", RECORDS_PATH], ["GET", READ_FROM("c1")]]);
    expect(r.cell.seen.slice(5).map((s) => s.encryptionKey)).toEqual(Array(3).fill(KEY));
  });
});

describe("the exchange", () => {
  test("refused: its status and code, nothing sent to the cell, and the next round exchanges again", async () => {
    const r = new Rig();
    const c = r.canary();
    r.issuer.exchange = { status: 503, headers: { "retry-after": "5" }, body: '{"error":{"code":"issuer_unavailable","message":"x"}}' };
    const refused = { ok: false, status: 503, errorCode: "issuer_unavailable", origin: false, endpoint: null };
    expect(await c.run()).toEqual({ exchange: refused, append: null, read: null });
    expect(r.cell.seen).toEqual([]);
    r.issuer.exchange = null;
    r.at += 60_000;
    expect(await c.run()).toEqual({ exchange: exchanged(r.cell.origin), append: OK, read: OK });
    expect(r.issuer.seen).toHaveLength(2);
  });

  test("after an exchange that is not good it exchanges every round until four in a row are, then keeps its token", async () => {
    const r = new Rig();
    const c = r.canary();
    r.issuer.exchange = { status: 503, body: '{"error":{"code":"issuer_unavailable","message":"x"}}' };
    const exchanges: (number | null)[] = [];
    for (let round = 0; round < 7; round++) {
      if (round === 1) r.issuer.exchange = null;
      exchanges.push((await c.run()).exchange?.status ?? null);
      r.at += 60_000;
    }
    expect([RESOLVE_AFTER, exchanges]).toEqual([4, [503, 200, 200, 200, 200, null, null]]);
  });

  test("an endpoint other than the cell's is not a good exchange: each round exchanges again while it lasts", async () => {
    const r = new Rig();
    const c = r.canary();
    r.issuer.endpoint = "https://elsewhere.example.test";
    const got: CanaryResult[] = [];
    for (let round = 0; round < 3; round++) got.push(await c.run());
    expect(got).toEqual(Array(3).fill({ exchange: exchanged("https://elsewhere.example.test"), append: OK, read: OK }));
    // The cell's origin with a trailing slash is the cell's.
    r.issuer.endpoint = `${r.cell.origin}/`;
    for (let round = 0; round < 5; round++) await c.run();
    expect(r.issuer.seen).toHaveLength(7);
  });

  test("a failed exchange leaves a usable token in use; once it is two minutes from expiring, the round ends at the issuer", async () => {
    const r = new Rig();
    const c = r.canary();
    r.issuer.endpoint = "https://elsewhere.example.test";
    await c.run();
    r.issuer.exchange = { status: 503, body: '{"error":{"code":"issuer_unavailable","message":"x"}}' };
    const refused = { ok: false, status: 503, errorCode: "issuer_unavailable", origin: false, endpoint: null };
    r.at = T0 + 3_479_000;
    expect(await c.run()).toEqual({ exchange: refused, append: OK, read: OK });
    const before = r.cell.seen.length;
    r.at = T0 + 3_480_000;
    expect(await c.run()).toEqual({ exchange: refused, append: null, read: null });
    expect(r.cell.seen).toHaveLength(before);
  });

  test("a wrong credential is the issuer's 401 invalid_credential", async () => {
    const r = new Rig();
    const wrong = `prisma_streams_strcred_${"0".repeat(32)}.${"w".repeat(43)}`;
    const got = await r.canary({ credential: wrong }).run();
    expect(got).toEqual({ exchange: { ok: false, status: 401, errorCode: "invalid_credential", origin: false, endpoint: null }, append: null, read: null });
    expect(r.issuer.seen.map((s) => s.authorization)).toEqual([`StreamsCredential ${wrong}`]);
  });

  test("a 200 without a token, or with a lifetime that is not a positive number, is a failed exchange", async () => {
    const r = new Rig();
    const c = r.canary();
    const bodies = [
      "not json",
      JSON.stringify({ tokenType: "Bearer", expiresIn: 3600, endpoint: r.cell.origin }),
      JSON.stringify({ accessToken: "", expiresIn: 3600, endpoint: r.cell.origin }),
      JSON.stringify({ accessToken: CANARY_TOKEN, expiresIn: 0, endpoint: r.cell.origin }),
      JSON.stringify({ accessToken: CANARY_TOKEN, expiresIn: "3600", endpoint: r.cell.origin }),
    ];
    for (const body of bodies) {
      r.issuer.exchange = { status: 200, body };
      expect((await c.run()).exchange).toEqual({ ok: false, status: 200, errorCode: null, origin: false, endpoint: null });
    }
    // Another 2xx is not the exchange's answer, whatever it carries.
    r.issuer.exchange = { status: 201, body: JSON.stringify({ accessToken: CANARY_TOKEN, expiresIn: 3600, endpoint: r.cell.origin }) };
    expect((await c.run()).exchange).toEqual({ ok: false, status: 201, errorCode: null, origin: false, endpoint: null });
    expect(r.cell.seen).toEqual([]);
  });

  test("the token's lifetime counts from when the exchange was sent, not from when it was answered", async () => {
    const r = new Rig();
    const c = r.canary();
    const body = JSON.stringify({ accessToken: CANARY_TOKEN, expiresIn: 3600, endpoint: r.cell.origin });
    r.issuer.exchange = { status: 200, body, delayMs: 200 };
    setTimeout(() => {
      r.at += 100_000;
    }, 50);
    await c.run();
    r.issuer.exchange = null;
    r.at = T0 + 3_480_000;
    expect((await c.run()).exchange).toEqual(exchanged(r.cell.origin));
  });

  test("a timeout and a refused connection have no status", async () => {
    const r = new Rig();
    r.issuer.exchange = { status: 200, body: "{}", delayMs: 400 };
    const slow = await r.canary({ timeoutMs: 100 }).run();
    const gone = new FakeServer({});
    const origin = gone.origin;
    gone.stop();
    const refused = await r.canary({ issuerOrigin: origin }).run();
    const none = { ok: false, status: null, errorCode: null, origin: false, endpoint: null };
    expect([slow, refused]).toEqual([{ exchange: none, append: null, read: null }, { exchange: none, append: null, read: null }]);
  });

  test("an endpoint other than the cell's is reported, and the cell at STREAMS_CELL_ORIGIN is still the one probed", async () => {
    const r = new Rig();
    const elsewhere = new FakeServer({});
    running.push(elsewhere);
    r.issuer.endpoint = elsewhere.origin;
    expect(await r.canary().run()).toEqual({ exchange: exchanged(elsewhere.origin), append: OK, read: OK });
    r.issuer.endpoint = null;
    expect((await r.canary().run()).exchange).toEqual({ ok: true, status: 200, errorCode: null, origin: false, endpoint: null });
    expect(elsewhere.seen).toEqual([]);
    expect(r.cell.seen.filter((s) => s.method === "POST")).toHaveLength(2);
  });
});

describe("the cell's answers", () => {
  test("a refused append: its status, code and the server's origin, nothing read, and the next round appends again", async () => {
    const r = new Rig();
    const c = r.canary();
    await c.run();
    r.cell.canary.append = { status: 503, headers: { "prisma-streams-origin": "s", "retry-after": "1" }, body: '{"error":{"code":"temporarily_unavailable","message":"x","retryable":true}}' };
    r.at += 60_000;
    expect(await c.run()).toEqual({ exchange: null, append: failed(503, "temporarily_unavailable"), read: null });
    delete r.cell.canary.append;
    r.at += 60_000;
    expect(await c.run()).toEqual({ exchange: null, append: OK, read: OK });
    expect(r.cellCalls(5)).toEqual([["POST", RECORDS_PATH], ["POST", RECORDS_PATH], ["GET", READ_FROM("c1")]]);
  });

  test("the wrapper's 500 binary_exited is read from its flat error, with no origin header", async () => {
    const r = new Rig();
    const c = r.canary();
    await c.run();
    r.cell.canary.append = { status: 500, headers: { "content-type": "application/json" }, body: '{"error":"binary_exited","message":"the server exited"}' };
    expect((await c.run()).append).toEqual(failed(500, "binary_exited", false));
  });

  test("a cell's 401 drops the token: the next round exchanges again", async () => {
    const r = new Rig();
    const c = r.canary();
    await c.run();
    r.cell.canary.append = { status: 401, headers: { "prisma-streams-origin": "s" }, body: '{"error":{"code":"kid_unknown","message":"x"}}' };
    r.at += 60_000;
    expect(await c.run()).toEqual({ exchange: null, append: failed(401, "kid_unknown"), read: null });
    delete r.cell.canary.append;
    r.at += 60_000;
    expect(await c.run()).toEqual({ exchange: exchanged(r.cell.origin), append: OK, read: OK });
    expect(r.issuer.seen).toHaveLength(2);
  });

  test("a read that holds another writer's record but not this one's fails the read", async () => {
    const r = new Rig();
    const c = r.canary();
    await c.run();
    r.cell.stream?.records.push({ canary: "another watch's", at: T0 });
    r.cell.once.append = { status: 200, headers: { "prisma-streams-origin": "s" }, body: '{"cursor":"c2","count":1}' };
    r.at += 60_000;
    expect(await c.run()).toEqual({ exchange: null, append: OK, read: failed(200, null) });
  });

  test("only a 200 acknowledges an append (G1), and only a 200 read can hold the record", async () => {
    const r = new Rig();
    const c = r.canary();
    await c.run();
    r.cell.once.append = { status: 202, headers: { "prisma-streams-origin": "s" }, body: '{"cursor":"c2","count":1}' };
    expect(await c.run()).toEqual({ exchange: null, append: failed(202, null), read: null });
    r.cell.once.read = { status: 203, headers: { "prisma-streams-origin": "s" }, body: JSON.stringify([{ canary: "nonce-3", at: T0 }]) };
    expect(await c.run()).toEqual({ exchange: null, append: OK, read: failed(203, null) });
  });

  test("a read that does not hold the record fails the read with status 200, and the next round anchors at the tail again", async () => {
    const r = new Rig();
    const c = r.canary();
    r.cell.losesAppends = true;
    expect(await c.run()).toEqual({ exchange: exchanged(r.cell.origin), append: OK, read: failed(200, null) });
    r.cell.losesAppends = false;
    r.at += 60_000;
    expect(await c.run()).toEqual({ exchange: null, append: OK, read: OK });
    expect(r.cellCalls(5)).toEqual([["GET", READ_FROM("now")], ["POST", RECORDS_PATH], ["GET", READ_FROM("c0")]]);
  });

  test("a refused anchor is a read failure before any append or create, and an anchor without a cursor is none", async () => {
    const r = new Rig();
    const c = r.canary();
    r.cell.canary.read = {
      status: 503,
      headers: { "prisma-streams-origin": "s", "prisma-next-cursor": "c0" },
      body: '{"error":{"code":"read_memory_busy","message":"x"}}',
    };
    expect(await c.run()).toEqual({ exchange: exchanged(r.cell.origin), append: null, read: failed(503, "read_memory_busy") });
    r.cell.canary.read = { status: 200, headers: { "prisma-streams-origin": "s" }, body: "[]" };
    expect((await c.run()).read).toEqual(failed(200, null));
    expect([r.cell.stream, r.cellCalls()]).toEqual([null, [["GET", READ_FROM("now")], ["GET", READ_FROM("now")]]]);
    delete r.cell.canary.read;
    expect(await c.run()).toEqual({ exchange: null, append: OK, read: OK });
    expect(r.cell.stream?.records).toHaveLength(1);
  });

  test("a refused read back is a read failure after the append, and the next round anchors at the tail again", async () => {
    const r = new Rig();
    const c = r.canary();
    await c.run();
    r.cell.canary.read = { status: 503, headers: { "prisma-streams-origin": "s" }, body: '{"error":{"code":"read_memory_busy","message":"x"}}' };
    expect(await c.run()).toEqual({ exchange: null, append: OK, read: failed(503, "read_memory_busy") });
    delete r.cell.canary.read;
    expect(await c.run()).toEqual({ exchange: null, append: OK, read: OK });
    expect(r.cellCalls(5)).toEqual([
      ["POST", RECORDS_PATH], ["GET", READ_FROM("c1")], ["GET", READ_FROM("now")], ["POST", RECORDS_PATH], ["GET", READ_FROM("c2")],
    ]);
  });

  test("a stream that is gone (404) is created again in the next round", async () => {
    const r = new Rig();
    const c = r.canary();
    await c.run();
    r.cell.stream = null;
    r.at += 60_000;
    expect(await c.run()).toEqual({ exchange: null, append: failed(404, "not_found"), read: null });
    r.at += 60_000;
    expect(await c.run()).toEqual({ exchange: null, append: OK, read: OK });
    expect(r.cellCalls(5)).toEqual([
      ["POST", RECORDS_PATH],
      ["GET", READ_FROM("now")],
      ["PUT", STREAM_PATH],
      ["GET", READ_FROM("now")],
      ["POST", RECORDS_PATH],
      ["GET", READ_FROM("c0")],
    ]);
  });

  test("a create answered 200, the stream made by another watch after this one found it missing, is the stream", async () => {
    const r = new Rig();
    await r.canary().run();
    const second = r.canary();
    r.cell.once.read = { status: 404, headers: { "prisma-streams-origin": "s" }, body: '{"error":{"code":"not_found","message":"x"}}' };
    expect(await second.run()).toEqual({ exchange: exchanged(r.cell.origin), append: OK, read: OK });
    expect(r.cellCalls(5)).toEqual([
      ["GET", READ_FROM("now")], ["PUT", STREAM_PATH], ["GET", READ_FROM("now")], ["POST", RECORDS_PATH], ["GET", READ_FROM("c1")],
    ]);
    expect(r.cell.stream?.records).toHaveLength(2);
  });

  test("a refused create is the append step's failure, and the next round tries the create again", async () => {
    const r = new Rig();
    const c = r.canary();
    r.cell.canary.create = { status: 429, headers: { "prisma-streams-origin": "s" }, body: '{"error":{"code":"stream_limit","message":"x"}}' };
    expect(await c.run()).toEqual({ exchange: exchanged(r.cell.origin), append: failed(429, "stream_limit"), read: null });
    delete r.cell.canary.create;
    expect((await c.run()).append).toEqual(OK);
    expect(r.cellCalls().map(([m, p]) => `${m} ${p}`)).toEqual([
      `GET ${READ_FROM("now")}`, `PUT ${STREAM_PATH}`,
      `GET ${READ_FROM("now")}`, `PUT ${STREAM_PATH}`, `GET ${READ_FROM("now")}`, `POST ${RECORDS_PATH}`, `GET ${READ_FROM("c0")}`,
    ]);
  });

  test("a redirect is not followed, and an answer slower than the timeout has no status", async () => {
    const r = new Rig();
    const elsewhere = new FakeServer({ "/x": () => ({ status: 200, body: "{}" }) });
    running.push(elsewhere);
    const c = r.canary({ timeoutMs: 200 });
    await c.run();
    r.cell.canary.append = { status: 307, headers: { location: `${elsewhere.origin}/x` }, body: "" };
    expect((await c.run()).append).toEqual(failed(307, null, false));
    r.cell.canary.append = { status: 200, body: "{}", delayMs: 400 };
    expect((await c.run()).append).toEqual(failed(null, null, false));
    expect(elsewhere.seen).toEqual([]);
  });
});

describe("the canary's stream and secrets", () => {
  test("its stream key is the cell's, derived from its id: 32 bytes, the same for every watch and credential of the cell", () => {
    const want = createHash("sha256").update("prisma-streams-cell-watch-canary:cell_sc1").digest("base64url");
    expect([KEY, Buffer.from(KEY, "base64url").length]).toEqual([want, 32]);
    expect(canaryStreamKey("cell_sc2")).not.toBe(KEY);
    expect(STREAM_CONFIG).toEqual({ format: { kind: "json" }, expiry: { idle: "1d" } });
  });

  test("the credential went only to the issuer's exchange, the token only to the canary's stream at the cell", () => {
    expect(everySeen.length).toBeGreaterThan(40);
    const issuer = everySeen.filter((e) => e.server === "issuer").map((e) => e.seen);
    const cell = everySeen.filter((e) => e.server === "cell").map((e) => e.seen);
    expect(issuer.filter((s) => s.method !== "POST" || s.path !== EXCHANGE_PATH)).toEqual([]);
    expect(cell.filter((s) => ![STREAM_PATH, RECORDS_PATH].includes(s.path) || s.path.startsWith("/v1/debug"))).toEqual([]);
    const carried = (from: Seen[], secret: string) => from.filter((s) => s.authorization?.includes(secret) || s.body.includes(secret));
    expect(carried(cell, CANARY_CREDENTIAL)).toEqual([]);
    expect(carried(issuer, CANARY_TOKEN)).toEqual([]);
    expect(everySeen.filter((e) => e.server === "other")).toEqual([]);
  });
});
