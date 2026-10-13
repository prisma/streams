// The paging tool's local stand-in (pager.ts, public-beta alerting triage
// B8), on short windows: Events v2 incidents by dedup key, the watch's own
// deliveries taken as the real service takes them, the heartbeat monitor
// and the uptime check. Run with `bun test ./deploy/cell-watch`.
import { afterEach, describe, expect, test } from "bun:test";
import { FakeServer } from "./fake-cell";
import { HEARTBEAT_KEY, Pager, type PagerOptions, UPTIME_KEY } from "./pager";
import { event, initialState, RULES, rulesConfig } from "./rules";
import { Outbox } from "./sinks";

const KEY = "routing-key-for-the-pager-tests";
const open: { stop(): void }[] = [];
afterEach(() => {
  for (const s of open.splice(0)) s.stop();
});

function pager(more: Partial<PagerOptions> = {}): Pager {
  const p = new Pager({ routingKey: KEY, heartbeatToken: "beat-token", uptimeOrigin: null, ...more });
  open.push(p);
  return p;
}

const post = (p: Pager, body: unknown) =>
  fetch(p.eventsUrl, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });

const trigger = (key: string, severity: string, more: Record<string, unknown> = {}) => ({
  routing_key: KEY,
  event_action: "trigger",
  dedup_key: key,
  payload: { summary: `[x] ${key}`, source: "cell_sc1", severity, timestamp: new Date(0).toISOString() },
  ...more,
});

async function until(what: string, ms: number, cond: () => boolean): Promise<void> {
  for (const end = Date.now() + ms; Date.now() < end; await Bun.sleep(10)) if (cond()) return;
  throw new Error(`timed out waiting for ${what}`);
}

describe("the pager's Events v2 endpoint", () => {
  test("opens an incident on a trigger's dedup key, a page when critical, folds a second trigger and closes it on resolve", async () => {
    const p = pager();
    const answers = [
      await post(p, trigger("cell_sc1/r06_feed_delivery/page", "critical")),
      await post(p, trigger("cell_sc1/r06_feed_delivery/page", "critical")),
      await post(p, trigger("cell_sc1/r17_unowned_usage/notify", "warning")),
      await post(p, { routing_key: KEY, event_action: "resolve", dedup_key: "cell_sc1/r06_feed_delivery/page" }),
      await post(p, { routing_key: KEY, event_action: "resolve", dedup_key: "cell_sc1/never/page" }),
    ];
    expect(answers.map((r) => r.status)).toEqual([202, 202, 202, 202, 202]);
    expect(await answers[0]!.json()).toEqual({ status: "success", message: "Event processed", dedup_key: "cell_sc1/r06_feed_delivery/page" });
    expect(p.since(0)).toEqual([
      ["open", "cell_sc1/r06_feed_delivery/page", true],
      ["open", "cell_sc1/r17_unowned_usage/notify", false],
      ["resolve", "cell_sc1/r06_feed_delivery/page", true],
    ]);
    expect(p.changes.map((c) => c.summary)).toEqual(["[x] cell_sc1/r06_feed_delivery/page", "[x] cell_sc1/r17_unowned_usage/notify", ""]);
  });

  test("refuses another routing key, or an event without a field the API requires, with 400 and no change", async () => {
    const p = pager();
    const refused = [
      trigger("k", "critical", { routing_key: "another-key" }),
      trigger("k", "critical", { event_action: "page" }),
      trigger("k", "critical", { payload: undefined }),
      trigger("k", "critical", { payload: { source: "cell_sc1", severity: "critical" } }),
      trigger("k", "critical", { payload: { summary: "x".repeat(1025), source: "cell_sc1", severity: "critical" } }),
      trigger("k", "critical", { payload: { summary: "s", severity: "critical" } }),
      trigger("k", "loud"),
      trigger("k", "critical", { payload: { summary: "s", source: "c", severity: "critical", timestamp: "then" } }),
      trigger("x".repeat(256), "critical"),
      { routing_key: KEY, event_action: "resolve" },
    ];
    const answers = await Promise.all(refused.map((b) => post(p, b)));
    const bodies = await Promise.all(answers.map((r) => r.json() as Promise<{ status: string; errors: string[] }>));
    expect(answers.map((r) => r.status)).toEqual(refused.map(() => 400));
    expect(bodies.map((b) => [b.status, b.errors])).toEqual([
      ["routing_key"], ["event_action"], ["payload.summary"], ["payload.summary"], ["payload.summary"], ["payload.source"],
      ["payload.severity"], ["payload.timestamp"], ["dedup_key"], ["dedup_key"],
    ].map((e) => ["invalid event", e]));
    const notJson = await fetch(p.eventsUrl, { method: "POST", body: "{" });
    const notAnObject = await fetch(p.eventsUrl, { method: "POST", body: "null" });
    expect([notJson.status, notAnObject.status, (await fetch(p.eventsUrl)).status, p.changes]).toEqual([400, 400, 404, []]);
  });

  test("takes the watch's own deliveries: a NOTIFY to the channel, its rise to PAGE as a page, the resolve closing both", async () => {
    const p = pager();
    const cfg = rulesConfig({ cellId: "cell_sc1", cellOrigin: "https://cell.example.com" });
    const rule = RULES.find((r) => r.id === "r04_restarts")!;
    const outbox = new Outbox({ kind: "events-v2", url: p.eventsUrl, key: KEY }, () => {});
    outbox.add([
      event(cfg, rule, "trigger", "NOTIFY", 1_000, { boot_ids_30m: 2 }, {}),
      event(cfg, rule, "trigger", "PAGE", 2_000, { boot_ids_30m: 3 }, {}),
      event(cfg, rule, "resolve", "PAGE", 3_000, { open_secs: 2 }, {}),
    ]);
    const state = await outbox.flush(initialState());
    expect([outbox.size, state.failedTriggers]).toEqual([0, 0]);
    expect(p.since(0)).toEqual([
      ["open", "cell_sc1/r04_restarts/notify", false],
      ["open", "cell_sc1/r04_restarts/page", true],
      ["resolve", "cell_sc1/r04_restarts/notify", false],
      ["resolve", "cell_sc1/r04_restarts/page", true],
    ]);
  });
});

describe("the pager's heartbeat monitor", () => {
  test("waits for the first beat, pages once no beat came for its window, and closes the page at the next beat", async () => {
    // No uptime origin: no check runs, however often one would.
    const p = pager({ heartbeatWindowMs: 1_000, uptimeEveryMs: 100 });
    await Bun.sleep(1_500);
    expect(p.changes).toEqual([]);
    const beat = async () => {
      const r = await fetch(p.heartbeatUrl);
      return [r.status, await r.text()];
    };
    expect(await beat()).toEqual([200, "OK"]);
    for (let i = 0; i < 4; i++) {
      await Bun.sleep(250);
      await beat();
    }
    expect(p.changes).toEqual([]);
    await until("the heartbeat page", 3_000, () => p.changes.length > 0);
    const late = Date.now() - p.beats[p.beats.length - 1]!;
    expect([p.since(0), p.changes[0]!.summary, late >= 1_000]).toEqual([[["open", HEARTBEAT_KEY, true]], "no heartbeat for 1 s", true]);
    await beat();
    expect(p.since(0)).toEqual([["open", HEARTBEAT_KEY, true], ["resolve", HEARTBEAT_KEY, true]]);
    expect([(await fetch(`${p.heartbeatUrl}x`)).status, p.beats.length]).toEqual([404, 6]);
  });
});

describe("the pager's uptime check", () => {
  test("pages at the third failed check in a row, counts an answer slower than its timeout, and closes at a 2xx", async () => {
    let health: () => { status: number; body: string; delayMs?: number } = () => ({ status: 200, body: "ok" });
    const cell = new FakeServer({ "/health": () => health() });
    open.push(cell);
    const p = pager({ uptimeOrigin: cell.origin, uptimeEveryMs: 200, uptimeTimeoutMs: 500 });
    await until("two checks", 2_000, () => cell.seen.length >= 2);
    // Two failures, then a pass: no page; the count starts again.
    let n = 0;
    health = () => (++n <= 2 ? { status: 503, body: "" } : { status: 200, body: "ok" });
    await until("the pass after two failures", 2_000, () => n >= 3);
    expect(p.changes).toEqual([]);
    // A 503, a 500, then one slower than the timeout: the third pages.
    const failing = [{ status: 503, body: "" }, { status: 500, body: "" }, { status: 200, body: "ok", delayMs: 1_000 }];
    let m = 0;
    health = () => failing[m++] ?? { status: 200, body: "ok", delayMs: 1_000 };
    await until("the uptime page", 3_000, () => p.changes.length > 0);
    expect([p.since(0), p.changes[0]!.summary, m >= 3]).toEqual([[["open", UPTIME_KEY, true]], "3 failed checks of /health in a row", true]);
    health = () => ({ status: 200, body: "ok" });
    await until("its resolve", 3_000, () => p.changes.length > 1);
    // The slow checks still in flight fail after the pass, fewer than three: no new page.
    await Bun.sleep(1_200);
    expect(p.since(0)).toEqual([["open", UPTIME_KEY, true], ["resolve", UPTIME_KEY, true]]);
    expect(cell.seen.every((s) => s.method === "GET" && s.path === "/health")).toBe(true);
  });
});
