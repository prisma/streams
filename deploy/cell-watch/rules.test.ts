// The watch's rule machinery (public-beta alerting triage B2): the table,
// hysteresis, NOTIFY_ONLY, baselines, purity, the withheld heartbeat (D7)
// and the payload allowlist. Run with `bun test ./deploy/cell-watch`.
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  delivered, ERROR_CODES, event, heartbeatDue, initialState, type Json, type Poll, RESOLVE_AFTER, RULES, step,
  type WatchEvent, WITHHOLD_AFTER,
} from "./rules";
import {
  ALL_ROWS, billingWithStored, BOOT, CFG, DATA, healthyPoll, idOf, kinds, ofRow, patch, POLL_MS, T0, Watch,
} from "./rig";

/// GitHub's anchor of a Markdown heading.
const slug = (heading: string) =>
  heading.trim().toLowerCase().replace(/[^\p{L}\p{N}\s_-]/gu, "").replace(/\s/g, "-");

describe("the table", () => {
  test("holds rows 3 to 28 once each, every anchor a RUNBOOK heading, the level rules 8, 10 and 12 to 16", () => {
    expect(RULES.map((r) => r.row)).toEqual(Array.from({ length: 26 }, (_, i) => i + 3));
    expect(new Set(RULES.map((r) => r.id)).size).toBe(26);
    for (const r of RULES) expect(r.id).toMatch(new RegExp(`^r${String(r.row).padStart(2, "0")}_[a-z0-9_]+$`));
    const runbook = readFileSync(join(import.meta.dir, "..", "..", "RUNBOOK.md"), "utf8");
    const anchors = new Set(
      runbook.split("\n").filter((l) => /^#{1,6} /.test(l)).map((l) => `RUNBOOK.md#${slug(l.replace(/^#+ /, ""))}`),
    );
    for (const r of RULES) expect([r.id, anchors.has(r.anchor)]).toEqual([r.id, true]);
    expect(RULES.filter((r) => r.level).map((r) => r.row)).toEqual([8, 10, 12, 13, 14, 15, 16]);
  });

  test("the issuer's rows run only with ISSUER_ORIGIN, row 5 only with EXPECTED_GIT", () => {
    const on = (cfg: typeof CFG) => RULES.filter((r) => r.enabled(cfg)).map((r) => r.row);
    expect(on(CFG)).toEqual([3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 23, 24, 25, 26]);
    expect(on(ALL_ROWS)).toEqual(RULES.map((r) => r.row));
  });
});

describe("the captured healthy cell", () => {
  test("raises nothing over two hours with every row on", () => {
    const w = new Watch(ALL_ROWS);
    for (let i = 0; i < 480; i++) {
      const minute = i % 4 === 0;
      w.poll({
        billing: minute ? billingWithStored(1_000_000, { "rollup.lastApplyAgeSecs": 5 }) : null,
        issuerHealth: { status: 200, timedOut: false },
        issuerMetrics: minute ? { cache_age_secs: 12, gate_open: true, profile_ok: true } : null,
        canary: {
          exchange: minute ? { ok: true, status: 200, errorCode: null, origin: false, endpoint: ALL_ROWS.cellOrigin } : null,
          append: { ok: true, status: 200, errorCode: null, origin: true },
          read: { ok: true, status: 200, errorCode: null, origin: true },
        },
      });
    }
    expect(w.events).toEqual([]);
  });
});

describe("hysteresis", () => {
  const dropped = { data: patch(DATA, { "feeds.policies.reservedDropped": 1 }) };

  test("a rule opens once, stays open without a second trigger, and resolves after four consecutive clean polls", () => {
    const w = new Watch();
    expect(kinds(w.poll(dropped))).toEqual([["trigger", "PAGE"]]);
    expect(w.polls(10, dropped)).toEqual([]);
    expect(w.polls(RESOLVE_AFTER - 1)).toEqual([]);
    expect(w.poll(dropped)).toEqual([]);
    expect(w.polls(RESOLVE_AFTER - 1)).toEqual([]);
    const resolved = w.poll();
    expect(resolved).toEqual([{
      kind: "resolve", dedupKey: `cell_sc1/${idOf(7)}`, class: "PAGE", rule: idOf(7), values: { open_secs: 270 },
      labels: {}, at: T0 + 18 * POLL_MS, runbookAnchor: "RUNBOOK.md#shared-cells-single-server-design-partners",
    }]);
    expect(kinds(w.poll(dropped))).toEqual([["trigger", "PAGE"]]);
  });

  test("a poll that lacks the rule's source neither opens it nor counts as clean", () => {
    const w = new Watch();
    expect(w.poll({ data: null })).toEqual([]);
    expect(kinds(w.poll(dropped))).toEqual([["trigger", "PAGE"]]);
    expect(w.polls(RESOLVE_AFTER - 1)).toEqual([]);
    expect(w.polls(20, { data: null })).toEqual([]);
    expect(kinds(w.poll())).toEqual([["resolve", "PAGE"]]);
  });

  test("NOTIFY rises to PAGE once while open, never falls back, and resolves at the class it reached", () => {
    const w = new Watch();
    const shed = (n: number) => ({ data: patch(DATA, { "local.admit_shed_rss": n }) });
    w.poll(shed(0));
    expect(kinds(w.poll(shed(1)))).toEqual([["trigger", "NOTIFY"]]);
    expect(kinds(w.poll(shed(2)))).toEqual([["trigger", "PAGE"]]);
    expect(w.poll(shed(3))).toEqual([]);
    expect(w.poll(shed(3))).toEqual([]);
    expect(w.poll(shed(4))).toEqual([]);
    expect(w.polls(RESOLVE_AFTER - 1, shed(4))).toEqual([]);
    const resolved = w.poll(shed(4));
    expect(resolved.map((e) => [e.kind, e.class, e.values])).toEqual([["resolve", "PAGE", { open_secs: 120 }]]);
  });

  test("NOTIFY_ONLY demotes the level rules' PAGE to NOTIFY; the binary rows page from the first deploy (B6)", () => {
    const w = new Watch({ ...CFG, notifyOnly: true });
    const all = (n: number) => ({
      data: patch(DATA, {
        "feeds.policies.reservedDropped": 1,
        "local.admit_shed_rss": n,
        "local.maintenance_backpressure.engaged": true,
      }),
    });
    expect(w.poll(all(0)).map((e) => [e.rule, e.kind, e.class])).toEqual([
      [idOf(7), "trigger", "PAGE"], [idOf(12), "trigger", "NOTIFY"],
    ]);
    const rising = [1, 2, 3, 4, 5, 6].flatMap((n) => w.poll(all(n)));
    expect(rising.map((e) => [e.rule, e.kind, e.class])).toEqual([[idOf(9), "trigger", "NOTIFY"], [idOf(9), "trigger", "PAGE"]]);
    const settled = w.polls(RESOLVE_AFTER);
    expect(settled.map((e) => [e.rule, e.kind, e.class]).sort()).toEqual([
      [idOf(7), "resolve", "PAGE"], [idOf(9), "resolve", "PAGE"], [idOf(12), "resolve", "NOTIFY"],
    ]);
    // Without it the level rule pages too.
    const calibrated = new Watch(CFG);
    expect(calibrated.poll(all(0)).map((e) => [e.rule, e.class])).toEqual([[idOf(7), "PAGE"], [idOf(12), "PAGE"]]);
  });

  test("step leaves the state it was given as it was", () => {
    const w = new Watch();
    w.poll(dropped);
    const before = structuredClone(w.state);
    const kept = w.state;
    step(CFG, kept, { ...healthyPoll(w.at), health: { ...healthyPoll(w.at).health, status: 503, bootId: null } });
    step(CFG, kept, healthyPoll(w.at));
    expect(kept).toEqual(before);
  });
});

describe("counter baselines", () => {
  const shed = (n: number) => patch(DATA, { "local.admit_shed_rss": n });
  const bootPoll = (id: string, n: number): Partial<Poll> => ({
    health: { ...healthyPoll(0).health, bootId: id },
    data: shed(n),
  });

  test("the watch's first poll is a baseline, not growth", () => {
    const w = new Watch();
    expect(w.poll({ data: shed(100) })).toEqual([]);
    expect(w.poll({ data: shed(100) })).toEqual([]);
  });

  test("a new boot id resets them, so a restarted counter above the old one is not growth", () => {
    const w = new Watch();
    w.poll(bootPoll(BOOT, 100));
    w.poll(bootPoll(BOOT, 100));
    const restart = w.poll(bootPoll("boot-2", 150));
    expect(restart.map((e) => e.rule)).toEqual([idOf(4)]);
    expect(kinds(w.poll(bootPoll("boot-2", 151)))).toEqual([["trigger", "NOTIFY"]]);
    expect(ofRow(w.events, 9).length).toBe(1);
  });

  test("a counter that went down without a boot id seen re-baselines instead of growing", () => {
    const w = new Watch();
    w.poll({ data: shed(100) });
    w.poll({ data: shed(3), health: { ...healthyPoll(0).health, status: 503, bootId: null, git: null } });
    expect(ofRow(w.events, 9)).toEqual([]);
    expect(kinds(w.poll({ data: shed(4) }))).toEqual([["trigger", "NOTIFY"]]);
  });
});

describe("the heartbeat (D7)", () => {
  const trigger: WatchEvent = {
    kind: "trigger", dedupKey: "c/r", class: "PAGE", rule: "r", values: {}, labels: {}, at: 0, runbookAnchor: "",
  };
  const resolve: WatchEvent = { ...trigger, kind: "resolve" };

  test("is withheld after three consecutive failed trigger deliveries and sent again once one is delivered", () => {
    expect(WITHHOLD_AFTER).toBe(3);
    let s = initialState();
    expect(heartbeatDue(s)).toBe(true);
    s = delivered(s, trigger, false);
    expect(heartbeatDue(s)).toBe(true);
    s = delivered(s, trigger, false);
    expect(heartbeatDue(s)).toBe(true);
    s = delivered(s, resolve, false);
    s = delivered(s, resolve, true);
    expect(heartbeatDue(s)).toBe(true);
    const before = s;
    s = delivered(s, trigger, false);
    expect(heartbeatDue(s)).toBe(false);
    expect(heartbeatDue(before)).toBe(true);
    s = delivered(s, trigger, false);
    expect(heartbeatDue(s)).toBe(false);
    s = delivered(s, trigger, true);
    expect(heartbeatDue(s)).toBe(true);
    expect(s.failedTriggers).toBe(0);
  });
});

// ---- the payload allowlist ---------------------------------------------------

const PROJECT = "proj_7f3c9a";
const WORKSPACE = "wksp_42d1e0";
const STREAM_HASH = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
const OBJECT_PATH = "pilot12/shards/root-3/wal/00000042.sst";
const TOKEN = "eyJhbGciOiJFZERTQSIsImtpZCI6ImsxIn0.eyJzdWIiOiJwcm9qXzdmM2M5YSJ9.c2lnbmF0dXJlLXNlY3JldA";
const URL_WITH_CREDENTIALS = `https://operator:${TOKEN}@evil.example.test/v1`;
const LACES = [PROJECT, WORKSPACE, STREAM_HASH, OBJECT_PATH, TOKEN, URL_WITH_CREDENTIALS, "evil.example", "eyJ"];
const laced = `${PROJECT} ${WORKSPACE} ${STREAM_HASH} ${OBJECT_PATH} ${TOKEN} ${URL_WITH_CREDENTIALS}`;

/// The cell's documents with every rule's condition met and identifiers
/// laced through the fields the rules read past.
function lacedData(i: number): Json {
  return patch(DATA, {
    "feeds.jwks.ageSecs": 120,
    "feeds.policies.reservedDropped": 2,
    "feeds.policies.stale": true,
    "feeds.policies.projects_named": [PROJECT, WORKSPACE],
    "local.instance": laced,
    "local.rss_mb": 520.5,
    "local.admit_shed_rss": i,
    "local.admit_shed_inflight": i,
    "local.wedge_shed": i,
    "local.maintenance_backpressure.engaged": true,
    "local.maintenance_backpressure.cause": laced,
    "local.read_memory_bytes": 130_000_000,
    "local.parked": 1200,
    "local.store.ops.put:wal": { n: 10, err: 5, p50_ms: 2500, p90_ms: 2500, p99_ms: 2500, max_ms: 2500 },
    "local.store.slow": [{ ago_s: 1, op: "put", class: "wal", ms: 2500, ok: false, path: OBJECT_PATH }],
    "local.store.totals.ops.get:other": { ok: i * 1_000_000, not_found: 0, unbilled: 0, err: 0 },
    "local.usage_overflow_admits": 3,
    "ops.cell": laced,
    "ops.counters.unowned_meter_events_total": i,
    "ops.counters.ingest_bytes_total": i * 1000,
    "ops.gauges.history_flush_wait_ms_max": 45_000,
    [`ops.gauges.${STREAM_HASH}`]: 1,
    ts_ms: 1791830622870 + 600_000,
  });
}

function lacedPoll(at: number, i: number, bootId: string): Poll {
  const failing = { ok: false, status: 401, errorCode: PROJECT, origin: true };
  return {
    at,
    health: { status: 200, timedOut: false, origin: true, git: "0000000", bootId, bodyPrefix: laced },
    data: lacedData(i),
    billing: billingWithStored(i * 2 ** 40, {
      openAlerts: [{ rule: laced, project: PROJECT, detail: OBJECT_PATH }],
      ready: false,
      "rollup.lastApplyAgeSecs": 9999,
      "rollup.stored.largest": [{ project: PROJECT, storedBytes: 1, writtenBytes: 1 }],
    }),
    issuerHealth: { status: 503, timedOut: false },
    issuerMetrics: { cache_age_secs: 999, gate_open: false, kid: TOKEN, cell_id: laced },
    canary: {
      exchange: { ok: true, status: 200, errorCode: TOKEN, origin: false, endpoint: URL_WITH_CREDENTIALS },
      append: failing,
      read: null,
    },
  };
}

describe("the payload allowlist", () => {
  test("an event keeps only finite numbers and the labels its rule declares, with a value of their enum", () => {
    const r19 = RULES.find((r) => r.row === 19);
    if (r19 === undefined) throw new Error("row 19");
    const e = event(CFG, r19, "trigger", "PAGE", T0, { status: 401, nan: Number.NaN, inf: Infinity, none: null }, {
      step: "append", origin: "elsewhere", error_code: "kid_unknown", project: "proj_7f3c9a",
    });
    expect(e).toEqual({
      kind: "trigger", dedupKey: `cell_sc1/${r19.id}`, class: "PAGE", rule: r19.id, values: { status: 401 },
      labels: { step: "append", error_code: "kid_unknown" }, at: T0, runbookAnchor: r19.anchor,
    });
  });

  test("a run that fires every rule over laced documents carries none of the identifiers, only allowlisted fields", () => {
    const events: WatchEvent[] = [];
    let state = initialState();
    let at = T0;
    const run = (p: Poll) => {
      const r = step(ALL_ROWS, state, p);
      state = r.state;
      events.push(...r.events);
      at += POLL_MS;
    };
    for (let i = 0; i < 160; i++) run(lacedPoll(at, i, i < 3 ? `boot-${i}` : "boot-2"));
    for (let i = 0; i < 12; i++) {
      run({ ...lacedPoll(at, 0, "boot-2"), health: { status: 503, timedOut: false, origin: true, git: null, bootId: null, bodyPrefix: `critical task terminated: ${laced}` } });
    }
    for (let i = 0; i < 12; i++) {
      run({ ...lacedPoll(at, 0, "boot-2"), data: patch(lacedData(0), { ops: null }) });
    }

    const triggered = new Set(events.filter((e) => e.kind === "trigger").map((e) => e.rule));
    expect([...RULES.map((r) => r.id)].filter((id) => !triggered.has(id))).toEqual([]);
    const text = JSON.stringify(events);
    for (const lace of LACES) expect([lace, text.includes(lace)]).toEqual([lace, false]);
    for (const e of events) {
      const r = RULES.find((x) => x.id === e.rule);
      expect(r).toBeDefined();
      expect(Object.keys(e).sort()).toEqual(["at", "class", "dedupKey", "kind", "labels", "rule", "runbookAnchor", "values"]);
      expect(e.dedupKey).toBe(`cell_sc1/${e.rule}`);
      expect(e.runbookAnchor).toBe(r?.anchor ?? "");
      for (const v of Object.values(e.values)) expect(Number.isFinite(v)).toBe(true);
      for (const [k, v] of Object.entries(e.labels)) expect([k, r?.labels[k]?.includes(v)]).toEqual([k, true]);
    }
    expect(ofRow(events, 19)[0]?.labels).toEqual({ step: "append", origin: "server", error_code: "other" });
    expect(ofRow(events, 22)[0]?.labels).toEqual({ failure: "endpoint", answer: "status", error_code: "other" });
    expect(ofRow(events, 3)[0]?.labels).toEqual({ origin: "server", answer: "status", reason: "tasks" });
    expect(ERROR_CODES).not.toContain(PROJECT);
  });
});
