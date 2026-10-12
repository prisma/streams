// The watch's rules on billing, the canary and the issuer (public-beta
// alerting triage B2, rows 18, 19, 21-25, 27 and 28). Run with
// `bun test ./deploy/cell-watch`.
import { describe, expect, test } from "bun:test";
import { BUDGET_DEFAULT, type CanaryResult, type CanaryStep, DEFAULT_PRICES, type Json, type Poll, STORED_GROWTH_DEFAULT } from "./rules";
import { ALL_ROWS, BILLING, billingWithStored, CFG, DATA, healthyPoll, kinds, ofRow, ORIGIN, patch, Watch } from "./rig";

const GIB = 2 ** 30;
const row = (w: Watch, n: number) => kinds(ofRow(w.events, n));
const fired = (w: Watch, n: number) => ofRow(w.events, n).filter((e) => e.kind === "trigger");
const ok: CanaryStep = { ok: true, status: 200, errorCode: null, origin: true };
const bad = (status: number | null, errorCode: string | null, origin = true): CanaryStep =>
  ({ ok: false, status, errorCode, origin });
const canary = (c: Partial<CanaryResult>): Partial<Poll> => ({ canary: { exchange: null, append: ok, read: ok, ...c } });
const exchanged = (x: Partial<CanaryStep & { endpoint: string | null }>) =>
  ({ ok: true, status: 200, errorCode: null, origin: false, endpoint: ORIGIN, ...x });

describe("row 18: the billing pipeline", () => {
  test("notifies on an open alert, ready false or a drain older than 300 s, read every 60 s", () => {
    const cases: [Record<string, Json>, Record<string, number> | undefined][] = [
      [{ openAlerts: [{ rule: "usage_outbox_lag" }] }, { open_alerts: 1, ready: 1, drain_last_ok_age_secs: 2 }],
      [{ ready: false }, { open_alerts: 0, ready: 0, drain_last_ok_age_secs: 2 }],
      [{ "drain.lastOkAgeSecs": 301 }, { open_alerts: 0, ready: 1, drain_last_ok_age_secs: 301 }],
      [{ "drain.lastOkAgeSecs": 300 }, undefined],
      [{ "drain.lastOkAgeSecs": -1 }, undefined],
    ];
    for (const [edits, want] of cases) {
      const w = new Watch();
      w.poll({ billing: patch(BILLING, edits) });
      expect([edits, fired(w, 18)[0]?.values]).toEqual([edits, want]);
    }
    const w = new Watch();
    w.poll({ billing: patch(BILLING, { ready: false }) });
    // Polls between the 60 s reads neither hold nor clean it.
    for (let i = 0; i < 12; i++) w.poll({ billing: i % 4 === 3 ? BILLING : null });
    expect(row(w, 18)).toEqual([["trigger", "NOTIFY"]]);
    for (let i = 0; i < 4; i++) w.poll({ billing: i % 4 === 3 ? BILLING : null });
    expect(row(w, 18)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
  });
});

describe("row 19: the canary's append and read", () => {
  test("pages after three consecutive failures with the step, the origin class and the error code only", () => {
    const w = new Watch();
    w.polls(2, canary({ append: bad(401, "kid_unknown") }));
    w.poll(canary({}));
    w.polls(2, canary({ read: bad(200, null) }));
    expect(row(w, 19)).toEqual([]);
    w.poll(canary({ read: bad(200, null) }));
    expect(fired(w, 19).map((e) => [e.class, e.values, e.labels])).toEqual([
      ["PAGE", { status: 200, consecutive_failures: 3 }, { step: "read", origin: "server", error_code: "none" }],
    ]);
    w.polls(4, canary({}));
    expect(row(w, 19)).toEqual([["trigger", "PAGE"], ["resolve", "PAGE"]]);
  });

  test("classifies the platform and the wrapper, names a known code and calls any other code other", () => {
    for (const [step, labels] of [
      [bad(401, "kid_unknown"), { step: "append", origin: "server", error_code: "kid_unknown" }],
      [bad(null, null, false), { step: "append", origin: "platform", error_code: "none" }],
      [bad(500, "binary_exited", false), { step: "append", origin: "wrapper", error_code: "binary_exited" }],
      [bad(403, "sp_canary_cell_sc1 is suspended"), { step: "append", origin: "server", error_code: "other" }],
    ] as [CanaryStep, Record<string, string>][]) {
      const w = new Watch();
      w.polls(3, canary({ append: step, read: null }));
      expect(fired(w, 19).map((e) => e.labels)).toEqual([labels]);
    }
  });

  test("a cycle whose exchange failed, so nothing was appended, is not the cell's", () => {
    const w = new Watch();
    w.polls(2, canary({ append: bad(503, "temporarily_unavailable") }));
    w.polls(5, canary({ exchange: exchanged({ ok: false, status: 503 }), append: null, read: null }));
    w.poll(canary({ append: bad(503, "temporarily_unavailable") }));
    expect(fired(w, 19).map((e) => e.values)).toEqual([{ status: 503, consecutive_failures: 3 }]);
    expect(row(new Watch(), 19)).toEqual([]);
  });
});

describe("row 20 and the canary rows stay off until their sources exist", () => {
  test("no canary and no issuer: rows 19, 21, 22, 27 and 28 never fire", () => {
    const w = new Watch(CFG);
    w.polls(40, {
      issuerHealth: { status: 503, timedOut: false },
      issuerMetrics: { cache_age_secs: 999, gate_open: false },
    });
    for (const n of [19, 21, 22, 27, 28]) expect([n, row(w, n)]).toEqual([n, []]);
  });
});

describe("row 21: the issuer down", () => {
  test("pages after two minutes of non-200 or timeout", () => {
    const w = new Watch(ALL_ROWS);
    w.polls(8, { issuerHealth: { status: null, timedOut: true } });
    expect(row(w, 21)).toEqual([]);
    w.poll({ issuerHealth: { status: 503, timedOut: false } });
    expect(fired(w, 21).map((e) => [e.class, e.values, e.labels])).toEqual([
      ["PAGE", { status: 503, down_secs: 120 }, { answer: "status" }],
    ]);
    w.polls(3, { issuerHealth: { status: 200, timedOut: false } });
    w.polls(5, { issuerHealth: null });
    w.poll({ issuerHealth: { status: 200, timedOut: false } });
    expect(row(w, 21)).toEqual([["trigger", "PAGE"], ["resolve", "PAGE"]]);
  });
});

describe("row 22: the canary's exchange", () => {
  test("pages after three failed exchanges; cycles on the cached token do not break the streak", () => {
    const w = new Watch(ALL_ROWS);
    w.poll(canary({ exchange: exchanged({ ok: false, status: 503, errorCode: "issuer_unavailable" }), append: null, read: null }));
    w.poll(canary({}));
    w.poll(canary({ exchange: exchanged({ ok: false, status: 401, errorCode: "invalid_credential" }), append: null, read: null }));
    expect(row(w, 22)).toEqual([]);
    w.poll(canary({ exchange: exchanged({ ok: false, status: null }), append: null, read: null }));
    expect(fired(w, 22).map((e) => [e.class, e.values, e.labels])).toEqual([
      ["PAGE", { status: 0, consecutive_failures: 3 }, { failure: "exchange", answer: "no_answer", error_code: "none" }],
    ]);
    w.polls(4, canary({ exchange: exchanged({}) }));
    expect(row(w, 22)).toEqual([["trigger", "PAGE"], ["resolve", "PAGE"]]);
    expect(row(w, 19)).toEqual([]);
  });

  test("a 200 whose endpoint is not STREAMS_CELL_ORIGIN is an exchange failure", () => {
    const w = new Watch(ALL_ROWS);
    w.polls(3, canary({ exchange: exchanged({ endpoint: `${ORIGIN}/` }) }));
    expect(row(w, 22)).toEqual([]);
    for (const endpoint of ["https://other.example.test", null, "not a url"]) {
      w.poll(canary({ exchange: exchanged({ endpoint }) }));
    }
    expect(fired(w, 22).map((e) => [e.values, e.labels])).toEqual([
      [{ status: 200, consecutive_failures: 3 }, { failure: "endpoint", answer: "status", error_code: "none" }],
    ]);
  });
});

describe("rows 23 to 25 read stored-cap's rollup.stored", () => {
  test("no row evaluates without it", () => {
    const w = new Watch();
    for (let i = 0; i < 400; i++) {
      w.poll({
        billing: i % 4 === 0 ? patch(BILLING, { "rollup.lastApplyAgeSecs": 9999 }) : null,
        data: patch(DATA, { "ops.counters.ingest_bytes_total": i, "local.store.totals.ops.get:other.ok": i * 1e7 }),
      });
    }
    for (const n of [23, 24, 25]) expect([n, row(w, n)]).toEqual([n, []]);
  });

  test("row 23: notifies when written bytes grow by more than G within 24 h, not when spread over more", () => {
    const g = STORED_GROWTH_DEFAULT;
    const minute = (w: Watch, written: number) => {
      w.poll({ billing: billingWithStored(written) });
      w.polls(3);
    };
    const slow = new Watch();
    // 26 h at a rate that adds G in 24.5 h: never more than G within 24 h.
    for (let m = 0; m <= 26 * 60; m++) minute(slow, Math.floor((g * m) / (24.5 * 60)));
    expect(row(slow, 23)).toEqual([]);
    const fast = new Watch();
    for (let m = 0; m <= 23 * 60; m++) minute(fast, Math.floor((g * m) / (23 * 60)));
    minute(fast, g + 1);
    expect(fired(fast, 23).map((e) => [e.class, e.values])).toEqual([
      ["NOTIFY", { written_growth_bytes: g + 1, window_secs: 23 * 3600 + 60, g_bytes: g }],
    ]);
    // Flat from then on: it resolves once the growth leaves the 24 h window.
    for (let m = 0; m < 50; m++) minute(fast, g + 1);
    expect(row(fast, 23)).toEqual([["trigger", "NOTIFY"]]);
    for (let m = 0; m < 24 * 60; m++) minute(fast, g + 1);
    expect(row(fast, 23)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
  });

  test("row 23: the 24 h window slides, and a counter that went down restarts it", () => {
    const g = STORED_GROWTH_DEFAULT;
    const minute = (w: Watch, written: number) => {
      w.poll({ billing: billingWithStored(written) });
      w.polls(3);
    };
    // Flat for 20 h, then G + 1 over the next 5 h: more than G within the
    // last 24 h once it passes G, past the first sample's 24 h.
    const sliding = new Watch();
    for (let m = 0; m <= 20 * 60; m++) minute(sliding, 0);
    let notified = -1;
    for (let m = 1; m <= 5 * 60 && notified < 0; m++) {
      minute(sliding, Math.floor(((g + 1) * m) / (5 * 60)));
      if (row(sliding, 23).length > 0) notified = m;
    }
    expect(notified).toBe(5 * 60);
    // 100 GiB, then 0 (the rollup's table was rebuilt), then 60 GiB: 60 GiB
    // grown since the restart.
    const reset = new Watch();
    minute(reset, 100 * GIB);
    minute(reset, 0);
    minute(reset, 60 * GIB);
    expect(fired(reset, 23).map((e) => e.values.written_growth_bytes)).toEqual([60 * GIB]);
  });

  test("row 24: notifies when storage, a day of the hour's billed requests and the server pass the budget", () => {
    expect(BUDGET_DEFAULT).toBe(10);
    // Storage alone: (written / GiB) x 1.2 x $0.015 / 30 > $10 - $0.144.
    const w = new Watch();
    w.poll({ billing: billingWithStored(16_000 * GIB) });
    w.poll({ billing: billingWithStored(17_000 * GIB) });
    expect(fired(w, 24).map((e) => [e.class, e.values])).toEqual([
      ["NOTIFY", {
        estimate_per_day: 10.34, requests_per_day: 0, storage_per_day: 10.2, server_per_day: DEFAULT_PRICES.serverPerDay,
        budget_per_day: 10, window_secs: 15,
      }],
    ]);
    for (let i = 0; i < 3; i++) w.poll({ billing: billingWithStored(16_000 * GIB) });
    expect(row(w, 24)).toEqual([["trigger", "NOTIFY"]]);
    w.poll({ billing: billingWithStored(16_000 * GIB) });
    expect(row(w, 24)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
  });

  test("row 24: bills 2xx and 404 GETs, HEADs, PUTs, LISTs and COPYs from the totals, and only after an hour", () => {
    const w = new Watch();
    const minute = (m: number) => {
      w.poll({
        billing: billingWithStored(GIB),
        data: patch(DATA, {
          "local.store.totals.ops.get:other": { ok: m * 20_000, not_found: 0, unbilled: m * 1e9, err: m * 1e9 },
          "local.store.totals.ops.head:wal": { ok: 0, not_found: m * 900, unbilled: 0, err: 0 },
          "local.store.totals.ops.put:wal": { ok: m * 4, not_found: 0, unbilled: m * 1e9, err: 0 },
          "local.store.totals.ops.list:other": { ok: m * 4, not_found: 0, unbilled: 0, err: 0 },
          "local.store.totals.ops.copy:other": { ok: m * 2, not_found: 0, unbilled: 0, err: 0 },
          "local.store.totals.ops.delete:other": { ok: m * 1e9, not_found: 0, unbilled: 0, err: 0 },
        }),
      });
      w.polls(3);
    };
    for (let m = 0; m < 60; m++) minute(m);
    expect(row(w, 24)).toEqual([]);
    minute(60);
    // An hour: 1,200,000 GETs and 54,000 HEAD 404s at $5e-7, 600 PUTs,
    // LISTs and COPYs at $5e-6: $0.63, $15.12 a day; deletes, 304s, 412s
    // and failures are free.
    expect(fired(w, 24).map((e) => [e.class, e.values])).toEqual([
      ["NOTIFY", {
        estimate_per_day: 15.26, requests_per_day: 15.12, storage_per_day: 0, server_per_day: 0.144,
        budget_per_day: 10, window_secs: 3600,
      }],
    ]);
  });

  test("row 24: a restart begins a new hour of requests", () => {
    const w = new Watch();
    const gets = (n: number, bootId = "boot-1"): Partial<Poll> => ({
      health: { ...healthyPoll(0).health, bootId },
      billing: billingWithStored(GIB),
      data: patch(DATA, { "local.store.totals.ops.get:other": { ok: n, not_found: 0, unbilled: 0, err: 0 } }),
    });
    for (let m = 0; m <= 60; m++) {
      w.poll(gets(m * 20_000));
      w.polls(3, { health: { ...healthyPoll(0).health, bootId: "boot-1" } });
    }
    expect(row(w, 24)).toEqual([["trigger", "NOTIFY"]]);
    expect(fired(w, 24)[0]?.values.requests_per_day).toBe(14.4);
    const after = new Watch();
    for (let m = 0; m <= 30; m++) {
      after.poll(gets(m * 20_000));
      after.polls(3, { health: { ...healthyPoll(0).health, bootId: "boot-1" } });
    }
    for (let m = 0; m <= 45; m++) {
      after.poll(gets(600_000 + m * 20_000, "boot-2"));
      after.polls(3, { health: { ...healthyPoll(0).health, bootId: "boot-2" } });
    }
    expect(row(after, 24)).toEqual([]);
  });

  test("row 25: notifies when the rollup has not applied for 600 s while ingest grew in the last 10 min", () => {
    const w = new Watch();
    const at = (ingest: number, age: number | null): Partial<Poll> => ({
      data: patch(DATA, { "ops.counters.ingest_bytes_total": ingest }),
      billing: age === null ? null : billingWithStored(GIB, { "rollup.lastApplyAgeSecs": age }),
    });
    w.poll(at(100, 900));
    w.poll(at(100, 900));
    expect(row(w, 25)).toEqual([]);
    w.poll(at(200, null));
    w.poll(at(200, 600));
    expect(row(w, 25)).toEqual([]);
    w.poll(at(200, 601));
    expect(fired(w, 25).map((e) => [e.class, e.values])).toEqual([["NOTIFY", { last_apply_age_secs: 601 }]]);
    // No growth since: 10 min later it is quiet, then four clean reads resolve it.
    for (let i = 0; i < 40; i++) w.poll(at(200, i % 4 === 0 ? 900 : null));
    for (let i = 0; i < 16; i++) w.poll(at(200, i % 4 === 0 ? 900 : null));
    expect(row(w, 25)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
  });
});

describe("rows 27 and 28: the issuer's metrics", () => {
  test("row 27 notifies on a state copy older than 150 s; row 28 on a closed admission gate", () => {
    const w = new Watch(ALL_ROWS);
    w.poll({ issuerMetrics: { cache_age_secs: 150, gate_open: true } });
    expect([row(w, 27), row(w, 28)]).toEqual([[], []]);
    w.poll({ issuerMetrics: { cache_age_secs: 151, gate_open: false } });
    expect(fired(w, 27).map((e) => [e.class, e.values])).toEqual([["NOTIFY", { cache_age_secs: 151 }]]);
    expect(fired(w, 28).map((e) => [e.class, e.values])).toEqual([["NOTIFY", { gate_open: 0 }]]);
    // The cycles between the 60 s reads neither hold nor clean them.
    w.polls(3, { issuerMetrics: { cache_age_secs: 3, gate_open: true } });
    w.polls(8, { issuerMetrics: null });
    expect([row(w, 27), row(w, 28)]).toEqual([[["trigger", "NOTIFY"]], [["trigger", "NOTIFY"]]]);
    w.poll({ issuerMetrics: { cache_age_secs: 3, gate_open: true } });
    expect([row(w, 27), row(w, 28)]).toEqual([
      [["trigger", "NOTIFY"], ["resolve", "NOTIFY"]], [["trigger", "NOTIFY"], ["resolve", "NOTIFY"]],
    ]);
  });
});
