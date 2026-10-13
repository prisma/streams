// The cell watch's poller (public-beta alerting triage B3; D6) against
// local servers: what one cycle reads and how it fills rules.ts's Poll,
// the cadence (billing and the issuer's metrics every fourth cycle), the
// connections (/health fresh each cycle, the bearer GETs on one kept-alive
// connection), timeouts, refusals, redirects, and the paths it may read.
// Run with `bun test ./deploy/cell-watch`.
import { afterEach, describe, expect, test } from "bun:test";
import { type Answer, FakeCell, FakeIssuer, FakeServer, ISSUER_METRICS, METRICS_TOKEN, OPERATOR_TOKEN, type Seen } from "./fake-cell";
import { BODY_PREFIX_CHARS, PATHS, type PollConfig, Poller, SLOW_EVERY, TIMEOUT_MS } from "./poll";
import { BILLING, BOOT, DATA, GIT } from "./rig";

const running: { stop(): void; seen: Seen[] }[] = [];
const everySeen: Seen[] = [];
afterEach(() => {
  for (const s of running.splice(0)) {
    everySeen.push(...s.seen);
    s.stop();
  }
});

function cell(): FakeCell {
  const c = new FakeCell();
  running.push(c);
  return c;
}

function issuer(): FakeIssuer {
  const i = new FakeIssuer();
  running.push(i);
  return i;
}

function poller(c: { origin: string }, more: Partial<PollConfig> = {}, now = () => 1_000): Poller {
  return new Poller({ cellOrigin: c.origin, operatorToken: OPERATOR_TOKEN, issuerOrigin: null, issuerMetricsToken: null, ...more }, now);
}

/// A port nothing listens on: a server started and stopped.
function closedOrigin(): string {
  const s = new FakeServer({});
  const origin = s.origin;
  s.stop();
  return origin;
}

const NO_ANSWER = { status: null, timedOut: false, origin: false, git: null, bootId: null, bodyPrefix: "" };

describe("one cycle", () => {
  test("fills the poll from the cell's answers: /health's status, headers and body, both documents, no issuer", async () => {
    const c = cell();
    expect(await poller(c, {}, () => 1_234).poll()).toEqual({
      at: 1_234,
      health: { status: 200, timedOut: false, origin: true, git: GIT, bootId: BOOT, bodyPrefix: "ok" },
      data: DATA,
      billing: BILLING,
      issuerHealth: null,
      issuerMetrics: null,
      canary: null,
    });
    expect(c.server.paths().sort()).toEqual([PATHS.health, PATHS.billing, PATHS.data]);
  });

  test("takes the timeout as 5 s and billing and the issuer's metrics every fourth 15 s cycle", () => {
    expect([TIMEOUT_MS, SLOW_EVERY, BODY_PREFIX_CHARS]).toEqual([5_000, 4, 256]);
  });

  test("reads billing on the first cycle and every fourth after it, and null in between", async () => {
    const c = cell();
    const p = poller(c);
    const billing: boolean[] = [];
    for (let i = 0; i < 9; i++) billing.push((await p.poll()).billing !== null);
    expect(billing).toEqual([true, false, false, false, true, false, false, false, true]);
    expect(c.seen.filter((s) => s.path === PATHS.data)).toHaveLength(9);
    expect(c.seen.filter((s) => s.path === PATHS.billing)).toHaveLength(3);
  });
});

describe("connections (D6)", () => {
  test("/health goes on a fresh connection each cycle with Connection: close and no bearer; data and billing share one kept-alive connection", async () => {
    const c = cell();
    const p = poller(c);
    for (let i = 0; i < 6; i++) await p.poll();
    const health = c.seen.filter((s) => s.path === PATHS.health);
    const bearer = c.seen.filter((s) => s.path !== PATHS.health);
    expect(health.map((s) => [s.connection, s.authorization])).toEqual(Array(6).fill(["close", null]));
    expect(new Set(health.map((s) => s.port)).size).toBe(6);
    expect(bearer).toHaveLength(8);
    expect(new Set(bearer.map((s) => s.port)).size).toBe(1);
    expect(health.map((s) => s.port)).not.toContain(bearer[0]?.port);
    expect(new Set(bearer.map((s) => [s.connection, s.authorization].join(" ")))).toEqual(
      new Set([`keep-alive Bearer ${OPERATOR_TOKEN}`]),
    );
  });
});

describe("answers that are not a healthy 200", () => {
  test("a server's 503 keeps its status, origin header and body prefix; it carries no identity headers", async () => {
    const c = cell();
    c.health = { status: 503, headers: { "prisma-streams-origin": "streams/test" }, body: "shard storage unavailable: s3 timed out" };
    expect((await poller(c).poll()).health).toEqual({
      status: 503, timedOut: false, origin: true, git: null, bootId: null, bodyPrefix: "shard storage unavailable: s3 timed out",
    });
  });

  test("the wrapper's 500 binary_exited has no origin header, and a long body is cut to its first 256 characters", async () => {
    const c = cell();
    const body = JSON.stringify({ error: "binary_exited", message: "x".repeat(400) });
    c.health = { status: 500, headers: { "content-type": "application/json" }, body };
    const h = (await poller(c).poll()).health;
    expect(h).toEqual({ status: 500, timedOut: false, origin: false, git: null, bootId: null, bodyPrefix: body.slice(0, 256) });
    expect(h.bodyPrefix.startsWith('{"error":"binary_exited"')).toBe(true);
  });

  test("an answer slower than the timeout is a timeout: /health flags it, a document is null", async () => {
    const c = cell();
    c.health = { ...c.health, delayMs: 400 };
    c.data = { ...c.data, delayMs: 400 };
    const poll = await poller(c, { timeoutMs: 100 }).poll();
    expect(poll.health).toEqual({ ...NO_ANSWER, timedOut: true });
    expect([poll.data, poll.billing]).toEqual([null, BILLING]);
  });

  test("a refused connection is no answer, not a timeout, and both documents are null", async () => {
    const poll = await poller({ origin: closedOrigin() }).poll();
    expect([poll.health, poll.data, poll.billing]).toEqual([NO_ANSWER, null, null]);
  });

  test("a document that is not a 200 with JSON is null: a wrong bearer's 401, a 503, a body that is not JSON", async () => {
    const wrong = cell();
    const p = poller(wrong, { operatorToken: "not-the-bearer" });
    expect([(await p.poll()).data, (await p.poll()).billing]).toEqual([null, null]);
    const c = cell();
    c.data = { status: 200, body: "<html>edge</html>" };
    c.billing = { status: 503, body: JSON.stringify(BILLING) };
    const poll = await poller(c).poll();
    expect([poll.data, poll.billing]).toEqual([null, null]);
  });

  test("a redirect is not followed: /health reads it as its status, a document as null, and the target sees nothing", async () => {
    const elsewhere = new FakeServer({ "/x": () => ({ status: 200, body: "{}" }) });
    running.push(elsewhere);
    const away: Answer = { status: 302, headers: { location: `${elsewhere.origin}/x` }, body: "" };
    const c = cell();
    c.health = away;
    c.data = away;
    const poll = await poller(c).poll();
    expect([poll.health.status, poll.health.origin, poll.data]).toEqual([302, false, null]);
    expect(elsewhere.seen).toEqual([]);
  });
});

describe("the issuer (ISSUER_ORIGIN set)", () => {
  test("/healthz every cycle, /v1/admin/metrics every fourth with its own bearer, both on one connection", async () => {
    const c = cell();
    const i = issuer();
    const p = poller(c, { issuerOrigin: i.origin, issuerMetricsToken: METRICS_TOKEN });
    const got: unknown[] = [];
    for (let n = 0; n < 5; n++) {
      const poll = await p.poll();
      got.push([poll.issuerHealth, poll.issuerMetrics]);
    }
    const ok = { status: 200, timedOut: false };
    expect(got).toEqual([[ok, ISSUER_METRICS], [ok, null], [ok, null], [ok, null], [ok, ISSUER_METRICS]]);
    expect(i.server.paths()).toEqual([
      PATHS.issuerHealth, PATHS.issuerMetrics, PATHS.issuerHealth, PATHS.issuerHealth, PATHS.issuerHealth,
      PATHS.issuerHealth, PATHS.issuerMetrics,
    ]);
    expect(i.seen.map((s) => s.authorization)).toEqual([null, `Bearer ${METRICS_TOKEN}`, null, null, null, null, `Bearer ${METRICS_TOKEN}`]);
    expect(new Set(i.seen.map((s) => s.port)).size).toBe(1);
    expect(c.seen.map((s) => s.authorization)).not.toContain(`Bearer ${METRICS_TOKEN}`);
  });

  test("without the metrics token only /healthz is read; a 503, a timeout and a refusal keep their answer", async () => {
    const c = cell();
    const i = issuer();
    const p = poller(c, { issuerOrigin: i.origin, timeoutMs: 100 });
    const first = await p.poll();
    expect([first.issuerHealth, first.issuerMetrics]).toEqual([{ status: 200, timedOut: false }, null]);
    i.health = { status: 503, body: '{"status":"unavailable","reason":"state_stale"}' };
    expect((await p.poll()).issuerHealth).toEqual({ status: 503, timedOut: false });
    i.health = { status: 200, body: "{}", delayMs: 400 };
    expect((await p.poll()).issuerHealth).toEqual({ status: null, timedOut: true });
    expect(i.server.paths()).toEqual([PATHS.issuerHealth, PATHS.issuerHealth, PATHS.issuerHealth]);
    const gone = await poller(c, { issuerOrigin: closedOrigin() }).poll();
    expect(gone.issuerHealth).toEqual({ status: null, timedOut: false });
  });

  test("no ISSUER_ORIGIN, no issuer GET: both issuer fields stay null", async () => {
    const poll = await poller(cell(), { issuerMetricsToken: METRICS_TOKEN }).poll();
    expect([poll.issuerHealth, poll.issuerMetrics]).toEqual([null, null]);
  });
});

describe("what the watch may read", () => {
  test("its paths are /health, the two operator documents and the issuer's two, never /v1/debug/*", () => {
    expect(Object.values(PATHS).sort()).toEqual(
      ["/health", "/healthz", "/operator/billing.json", "/operator/data.json", "/v1/admin/metrics"],
    );
  });

  test("every request this suite's servers saw was a GET of one of those paths", () => {
    expect(everySeen.length).toBeGreaterThan(50);
    const allowed: string[] = Object.values(PATHS);
    expect(everySeen.filter((s) => s.method !== "GET" || !allowed.includes(s.path) || s.body !== "")).toEqual([]);
  });
});
