// The watch's rules on the cell's own surfaces (public-beta alerting
// triage B2, rows 3-17, 20 and 26): each opens as its row states, holds,
// and resolves after four clean polls. Run with `bun test ./deploy/cell-watch`.
import { describe, expect, test } from "bun:test";
import type { Class, HealthPoll, Json, Poll } from "./rules";
import { CFG, DATA, GIT, healthyPoll, kinds, ofRow, patch, Watch } from "./rig";

const MIN_POLLS = 4;
const data = (edits: Record<string, Json>): Partial<Poll> => ({ data: patch(DATA, edits) });
const health = (h: Partial<HealthPoll>): Partial<Poll> => ({ health: { ...healthyPoll(0).health, ...h } });
/// The row's events as `[kind, class]`, and its triggers' values and labels.
const row = (w: Watch, n: number) => kinds(ofRow(w.events, n));
const fired = (w: Watch, n: number) => ofRow(w.events, n).filter((e) => e.kind === "trigger");

describe("row 3: no 2xx from inside", () => {
  const down = (h: Partial<HealthPoll>) => health({ git: null, bootId: null, ...h });

  test("pages after two minutes of non-200, not before, and resolves after four 200s", () => {
    const w = new Watch();
    w.polls(8, down({ status: 503, bodyPrefix: "runtime draining" }));
    expect(row(w, 3)).toEqual([]);
    w.poll(down({ status: 503, bodyPrefix: "runtime draining" }));
    expect(row(w, 3)).toEqual([["trigger", "PAGE"]]);
    expect(fired(w, 3)[0]?.values).toEqual({ status: 503, down_secs: 120 });
    w.polls(3);
    w.poll(down({ status: 503, bodyPrefix: "runtime draining" }));
    w.polls(3);
    expect(row(w, 3)).toEqual([["trigger", "PAGE"]]);
    w.poll();
    expect(row(w, 3)).toEqual([["trigger", "PAGE"], ["resolve", "PAGE"]]);
  });

  test("names the server and its reason from the 503 body, the wrapper, or the platform, never the text", () => {
    const cases: [Partial<HealthPoll>, Record<string, string>, number][] = [
      [{ status: 503, origin: true, bodyPrefix: "runtime supervisor unavailable" }, { origin: "server", reason: "tasks", answer: "status" }, 503],
      [{ status: 503, origin: true, bodyPrefix: "critical task terminated: absorber" }, { origin: "server", reason: "tasks", answer: "status" }, 503],
      [{ status: 503, origin: true, bodyPrefix: "auth feeds not yet published: jwks" }, { origin: "server", reason: "feeds_unpublished", answer: "status" }, 503],
      [{ status: 503, origin: true, bodyPrefix: "shard storage unavailable: open failed" }, { origin: "server", reason: "shard_storage", answer: "status" }, 503],
      [{ status: 503, origin: true, bodyPrefix: "shard storage unavailable: superseded by a newer boot" }, { origin: "server", reason: "superseded", answer: "status" }, 503],
      [{ status: 503, origin: true, bodyPrefix: "billing not ready (spool=false, rollup=true)" }, { origin: "server", reason: "billing_not_ready", answer: "status" }, 503],
      [{ status: 503, origin: true, bodyPrefix: "something new" }, { origin: "server", reason: "other", answer: "status" }, 503],
      [{ status: 500, origin: true, bodyPrefix: "runtime stopped" }, { origin: "server", reason: "other", answer: "status" }, 500],
      [{ status: 500, origin: false, bodyPrefix: '{"error":"binary_exited","message":"the service is not running"}' }, { origin: "wrapper", answer: "status" }, 500],
      [{ status: 500, origin: false, bodyPrefix: "Internal Server Error" }, { origin: "platform", answer: "status" }, 500],
      [{ status: 503, origin: false, bodyPrefix: "upstream connect error" }, { origin: "platform", answer: "status" }, 503],
      [{ status: 404, origin: false, bodyPrefix: "" }, { origin: "platform", answer: "status" }, 404],
      [{ status: null, timedOut: true, origin: false, bodyPrefix: "" }, { origin: "platform", answer: "timeout" }, 0],
      [{ status: null, timedOut: false, origin: false, bodyPrefix: "" }, { origin: "platform", answer: "no_answer" }, 0],
    ];
    for (const [h, labels, status] of cases) {
      const w = new Watch();
      w.polls(9, down(h));
      expect([h.bodyPrefix, fired(w, 3).map((e) => [e.labels, e.values.status])]).toEqual([h.bodyPrefix, [[labels, status]]]);
    }
  });
});

describe("row 4: restarts", () => {
  test("one boot id change notifies; three boot ids within 30 min page; older ones age out", () => {
    const w = new Watch();
    w.polls(2);
    w.poll(health({ bootId: "boot-2" }));
    expect(row(w, 4)).toEqual([["trigger", "NOTIFY"]]);
    expect(fired(w, 4)[0]?.values).toEqual({ boot_ids_30m: 2 });
    w.polls(MIN_POLLS, health({ bootId: "boot-2" }));
    expect(row(w, 4)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
    w.polls(20, health({ bootId: "boot-2" }));
    w.poll(health({ bootId: "boot-3" }));
    expect(row(w, 4).slice(2)).toEqual([["trigger", "PAGE"]]);
    expect(fired(w, 4)[1]?.values).toEqual({ boot_ids_30m: 3 });
    // 30 min after the first boot id was last seen, two remain: clean.
    w.polls(4 * 30, health({ bootId: "boot-3" }));
    expect(row(w, 4).slice(2)).toEqual([["trigger", "PAGE"], ["resolve", "PAGE"]]);
    w.poll(health({ bootId: "boot-4" }));
    expect(row(w, 4).slice(4)).toEqual([["trigger", "NOTIFY"]]);
  });

  // The field sequence of 2026-10-17 (FIELD-REPORT Round 4, finding 2): a
  // deploy, then a restart 6 min later. The server's operator data names, in
  // `boot`, the boot it replaced and whether that boot learned of it (it
  // wrote its mark when it saw a newer boot registered): a planned
  // replacement. A boot that went before its successor registered has no mark.
  type Marked = boolean | null;
  /// A poll served by boot `id`, which replaced `predecessor` (`marked`: that boot's mark).
  const served = (id: string, predecessor: string | null, marked: Marked): Partial<Poll> => ({
    health: { ...healthyPoll(0).health, bootId: id },
    data: patch(DATA, { boot: { id, predecessor, predecessorSuperseded: predecessor === null ? null : marked } }),
  });
  /// The cell is replaced by `to`: its first poll before the old boot's mark, then marked.
  const replace = (w: Watch, to: string, from: string) => {
    w.poll(served(to, from, null));
    w.polls(3, served(to, from, true));
  };
  const pages = (w: Watch) => row(w, 4).filter(([, cls]) => cls === "PAGE");

  test("a deploy and a restart 6 min later, each a replacement that learned of its successor, notify twice and never page", () => {
    const w = new Watch();
    w.polls(8, served("boot-1", null, null));
    replace(w, "boot-2", "boot-1");
    w.polls(4 * 6, served("boot-2", "boot-1", true));
    replace(w, "boot-3", "boot-2");
    w.polls(4 * 30, served("boot-3", "boot-2", true));
    expect(row(w, 4)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"], ["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
    expect(fired(w, 4).map((e) => e.values)).toEqual([{ boot_ids_30m: 2 }, { boot_ids_30m: 2 }]);
  });

  test("any number of replacements in 30 min stay below the page", () => {
    const w = new Watch();
    w.polls(2, served("boot-1", null, null));
    let from = "boot-1";
    for (const id of ["boot-2", "boot-3", "boot-4", "boot-5", "boot-6"]) {
      replace(w, id, from);
      from = id;
    }
    expect(pages(w)).toEqual([]);
  });

  test("boots that went without a successor's link still page at three boot ids in 30 min, whatever the data says", () => {
    const sequences: [string, (id: string, from: string) => Partial<Poll>][] = [
      ["no mark", (id, from) => served(id, from, false)],
      ["undecided", (id, from) => served(id, from, null)],
      ["no predecessor", (id) => served(id, null, null)],
      ["an operator payload without a boot section", (id) => ({ health: { ...healthyPoll(0).health, bootId: id } })],
      ["no operator payload", (id) => ({ health: { ...healthyPoll(0).health, bootId: id }, data: null, dataStatus: null })],
    ];
    for (const [label, next] of sequences) {
      const w = new Watch();
      w.polls(2, next("boot-1", ""));
      w.polls(2, next("boot-2", "boot-1"));
      expect([label, row(w, 4)]).toEqual([label, [["trigger", "NOTIFY"]]]);
      w.polls(2, next("boot-3", "boot-2"));
      expect([label, row(w, 4)]).toEqual([label, [["trigger", "NOTIFY"], ["trigger", "PAGE"]]]);
      expect([label, fired(w, 4)[1]?.values]).toEqual([label, { boot_ids_30m: 3 }]);
    }
  });

  test("a replacement does not hide the crash loop after it: the replaced boot is the only one left out", () => {
    const w = new Watch();
    w.polls(2, served("boot-1", null, null));
    replace(w, "boot-2", "boot-1");
    w.polls(2, served("boot-3", "boot-2", false));
    expect(pages(w)).toEqual([]);
    w.polls(2, served("boot-4", "boot-3", false));
    expect(pages(w)).toEqual([["trigger", "PAGE"]]);
    expect(fired(w, 4).at(-1)?.values).toEqual({ boot_ids_30m: 3 });
  });

  test("the replaced boot is told by the serving boot's own record: a boot never replaces itself, and a mark ages out with its id", () => {
    const w = new Watch();
    w.polls(2, served("boot-1", null, null));
    w.polls(2, served("boot-2", "boot-2", true));
    w.polls(2, served("boot-3", "boot-3", true));
    expect(fired(w, 4).at(-1)?.class).toBe("PAGE");
    const aged = new Watch();
    aged.polls(2, served("boot-1", null, null));
    replace(aged, "boot-2", "boot-1");
    aged.polls(4 * 31, served("boot-2", "boot-1", true));
    expect(aged.state.replaced).toEqual({});
    expect(Object.keys(aged.state.bootIds)).toEqual(["boot-2"]);
  });

  test("a boot id is read from a 200 only", () => {
    const w = new Watch();
    w.polls(2);
    w.polls(3, health({ status: 503, bootId: "boot-2" }));
    w.polls(2);
    expect(row(w, 4)).toEqual([]);
  });

  test("the first boot id the watch sees is not a change", () => {
    const w = new Watch();
    w.polls(3, health({ status: 503, bootId: null, git: null }));
    w.polls(3);
    expect(row(w, 4)).toEqual([]);
  });
});

describe("row 5: wrong build", () => {
  test("notifies after 10 min of a build other than EXPECTED_GIT; a prefix matches; a 503 does not count", () => {
    const w = new Watch({ ...CFG, expectedGit: GIT.slice(0, 12) });
    w.polls(4 * 20);
    expect(row(w, 5)).toEqual([]);
    const wrong = health({ git: "0123456789abcdef" });
    w.polls(4 * 10, wrong);
    w.polls(4, health({ status: 503, git: null, bootId: null }));
    expect(row(w, 5)).toEqual([]);
    w.poll(wrong);
    expect(row(w, 5)).toEqual([["trigger", "NOTIFY"]]);
    expect(fired(w, 5)[0]?.values).toEqual({ wrong_build_secs: 660 });
    w.polls(MIN_POLLS);
    expect(row(w, 5)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
  });

  test("is off without EXPECTED_GIT", () => {
    const w = new Watch();
    w.polls(4 * 15, health({ git: "0123456789abcdef" }));
    expect(row(w, 5)).toEqual([]);
  });
});

describe("row 6: feed delivery", () => {
  test("pages on an age above 75 s or a stale feed; 75 s and a never-published feed do not", () => {
    for (const [edits, want] of [
      [{ "feeds.jwks.ageSecs": 76 }, { jwks_age_secs: 76, policies_age_secs: 0, grants_age_secs: 0, policies_stale: 0, grants_stale: 0 }],
      [{ "feeds.grants.ageSecs": 200 }, { jwks_age_secs: 0, policies_age_secs: 0, grants_age_secs: 200, policies_stale: 0, grants_stale: 0 }],
      [{ "feeds.policies.stale": true }, { jwks_age_secs: 0, policies_age_secs: 0, grants_age_secs: 0, policies_stale: 1, grants_stale: 0 }],
      [{ "feeds.grants.stale": true }, { jwks_age_secs: 0, policies_age_secs: 0, grants_age_secs: 0, policies_stale: 0, grants_stale: 1 }],
      [{ "feeds.policies.ageSecs": 75 }, undefined],
      [{ "feeds.jwks.ageSecs": null, "feeds.policies.ageSecs": null }, undefined],
    ] as [Record<string, Json>, Record<string, number> | undefined][]) {
      const w = new Watch();
      w.poll(data(edits));
      expect([edits, fired(w, 6)[0]?.values]).toEqual([edits, want]);
      expect(row(w, 6)).toEqual(want === undefined ? [] : [["trigger", "PAGE"]]);
    }
    const w = new Watch();
    w.polls(3, data({ "feeds.jwks.ageSecs": 90 }));
    w.polls(MIN_POLLS);
    expect(row(w, 6)).toEqual([["trigger", "PAGE"], ["resolve", "PAGE"]]);
  });
});

describe("row 7: a reserved policy dropped", () => {
  test("pages on any drop", () => {
    const w = new Watch();
    w.poll(data({ "feeds.policies.reservedDropped": 3 }));
    expect(fired(w, 7).map((e) => [e.class, e.values])).toEqual([["PAGE", { reserved_dropped: 3 }]]);
  });
});

describe("row 8: RSS at the shed line", () => {
  test("pages after 10 min at or above the line", () => {
    const w = new Watch();
    const at = data({ "local.rss_mb": 500 });
    w.polls(40, at);
    expect(row(w, 8)).toEqual([]);
    w.poll(at);
    expect(fired(w, 8).map((e) => [e.class, e.values])).toEqual([["PAGE", { pressure_mb: 500, rss_shed_mb: 500, held_secs: 600 }]]);
    w.polls(MIN_POLLS);
    expect(row(w, 8)).toEqual([["trigger", "PAGE"], ["resolve", "PAGE"]]);
  });

  test("notifies after 30 min at 90 % of the line; a line of 0 (no shed) is not evaluated", () => {
    const w = new Watch();
    w.polls(120, data({ "local.rss_mb": 450 }));
    expect(row(w, 8)).toEqual([]);
    w.poll(data({ "local.rss_mb": 450 }));
    expect(fired(w, 8).map((e) => [e.class, e.values])).toEqual([["NOTIFY", { pressure_mb: 450, rss_shed_mb: 500, held_secs: 1800 }]]);
    const below = new Watch();
    below.polls(200, data({ "local.rss_mb": 449.9 }));
    expect(row(below, 8)).toEqual([]);
    const off = new Watch();
    off.polls(200, data({ "local.rss_mb": 900, "local.rss_shed_mb": 0 }));
    expect(row(off, 8)).toEqual([]);
  });
});

describe("rows 9 to 11: shed counters", () => {
  const grow = (w: Watch, key: string, from: number, polls: number) => {
    for (let i = 0; i < polls; i++) w.poll(data({ [key]: from + i }));
  };

  test("row 9: one poll of RSS-shed growth notifies, two consecutive page", () => {
    const w = new Watch();
    grow(w, "local.admit_shed_rss", 10, 2);
    w.poll(data({ "local.admit_shed_rss": 11 }));
    w.polls(MIN_POLLS, data({ "local.admit_shed_rss": 11 }));
    expect(row(w, 9)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
    grow(w, "local.admit_shed_rss", 12, 2);
    expect(row(w, 9).slice(2)).toEqual([["trigger", "NOTIFY"], ["trigger", "PAGE"]]);
    expect(fired(w, 9)[2]?.values).toEqual({ admit_shed_rss: 13, growth: 1, polls: 2 });
  });

  test("row 10: inflight and survival sheds together; three consecutive growths page", () => {
    const w = new Watch();
    w.poll(data({ "local.admit_shed_inflight": 5, "local.admit_shed_survival": 5 }));
    w.poll(data({ "local.admit_shed_inflight": 5, "local.admit_shed_survival": 6 }));
    w.poll(data({ "local.admit_shed_inflight": 7, "local.admit_shed_survival": 6 }));
    expect(row(w, 10)).toEqual([["trigger", "NOTIFY"]]);
    w.poll(data({ "local.admit_shed_inflight": 8, "local.admit_shed_survival": 6 }));
    expect(row(w, 10)).toEqual([["trigger", "NOTIFY"], ["trigger", "PAGE"]]);
    expect(fired(w, 10)[1]?.values).toEqual({ admit_shed_inflight: 8, admit_shed_survival: 6, growth: 1, polls: 3 });
    w.polls(MIN_POLLS, data({ "local.admit_shed_inflight": 8, "local.admit_shed_survival": 6 }));
    expect(row(w, 10)).toEqual([["trigger", "NOTIFY"], ["trigger", "PAGE"], ["resolve", "PAGE"]]);
  });

  test("row 11: any wedge-shed growth pages", () => {
    const w = new Watch();
    w.poll(data({ "local.wedge_shed": 4 }));
    w.poll(data({ "local.wedge_shed": 4 }));
    expect(row(w, 11)).toEqual([]);
    w.poll(data({ "local.wedge_shed": 5 }));
    w.polls(MIN_POLLS, data({ "local.wedge_shed": 5 }));
    expect(row(w, 11)).toEqual([["trigger", "PAGE"], ["resolve", "PAGE"]]);
    expect(fired(w, 11)[0]?.values).toEqual({ wedge_shed: 5, growth: 1 });
  });
});

describe("row 12: the backlog latch", () => {
  const latch = (edits: Record<string, Json>) =>
    data(Object.fromEntries(Object.entries(edits).map(([k, v]) => [`local.maintenance_backpressure.${k}`, v])));

  test("pages while engaged or on appends shed, notifies on lag above 450 s or 256 MiB unabsorbed", () => {
    const cases: [Record<string, Json>, Class | undefined][] = [
      [{ engaged: true }, "PAGE"],
      [{ absorb_lag_secs: 451 }, "NOTIFY"],
      [{ unabsorbed_bytes: 256 * 2 ** 20 + 1 }, "NOTIFY"],
      [{ absorb_lag_secs: 450, unabsorbed_bytes: 256 * 2 ** 20 }, undefined],
    ];
    for (const [edits, want] of cases) {
      const w = new Watch();
      w.poll(latch(edits));
      expect([edits, fired(w, 12).map((e) => e.class)]).toEqual([edits, want === undefined ? [] : [want]]);
    }
    const w = new Watch();
    w.poll(latch({ appends_shed: 7 }));
    w.poll(latch({ appends_shed: 9 }));
    expect(fired(w, 12).map((e) => [e.class, e.values])).toEqual([
      ["PAGE", { engaged: 0, appends_shed_growth: 2, absorb_lag_secs: 1, unabsorbed_bytes: 2075 }],
    ]);
    w.polls(3, latch({ engaged: true, appends_shed: 9 }));
    w.polls(MIN_POLLS, latch({ appends_shed: 9 }));
    expect(row(w, 12)).toEqual([["trigger", "PAGE"], ["resolve", "PAGE"]]);
    // A counter that went down is no growth, never a negative one.
    w.poll(latch({ engaged: true, appends_shed: 2 }));
    expect(fired(w, 12)[1]?.values.appends_shed_growth).toBe(0);
  });
});

describe("row 14: read memory", () => {
  test("pages after 5 min above 80 %", () => {
    const w = new Watch();
    const high = data({ "local.read_memory_bytes": 104_857_601 });
    w.polls(20, high);
    expect(row(w, 14)).toEqual([]);
    w.poll(high);
    expect(fired(w, 14).map((e) => [e.class, e.values])).toEqual([
      ["PAGE", { read_memory_ratio: 0.8, read_memory_capacity_bytes: 131_072_000, held_secs: 300 }],
    ]);
    w.polls(MIN_POLLS, data({ "local.read_memory_bytes": 104_857_600 }));
    expect(row(w, 14)).toEqual([["trigger", "PAGE"], ["resolve", "PAGE"]]);
  });
});

describe("row 15: the live pool", () => {
  test("notifies after 1 min at 90 %, pages after 1 min full, counting parked waits", () => {
    const below = new Watch();
    below.polls(20, data({ "local.sse_connections": 1000, "local.parked": 79 }));
    expect(row(below, 15)).toEqual([]);
    const w = new Watch();
    w.polls(4, data({ "local.sse_connections": 1000, "local.parked": 80 }));
    expect(row(w, 15)).toEqual([]);
    w.poll(data({ "local.sse_connections": 1000, "local.parked": 80 }));
    expect(row(w, 15)).toEqual([["trigger", "NOTIFY"]]);
    w.polls(4, data({ "local.sse_connections": 1000, "local.parked": 200 }));
    expect(row(w, 15)).toEqual([["trigger", "NOTIFY"]]);
    w.poll(data({ "local.sse_connections": 1000, "local.parked": 200 }));
    expect(fired(w, 15).map((e) => [e.class, e.values])).toEqual([
      ["NOTIFY", { live_pool_ratio: 0.9, sse_connections: 1000, parked: 80, sse_max_connections: 1200 }],
      ["PAGE", { live_pool_ratio: 1, sse_connections: 1000, parked: 200, sse_max_connections: 1200 }],
    ]);
    w.polls(MIN_POLLS);
    expect(row(w, 15)).toEqual([["trigger", "NOTIFY"], ["trigger", "PAGE"], ["resolve", "PAGE"]]);
  });
});

describe("row 16: the object store", () => {
  const cell = (n: number, err: number, p99 = 5) => ({ n, err, p50_ms: 4, p90_ms: 4, p99_ms: p99, max_ms: p99 });

  /// `local.store.totals` with the cumulative put:wal and put:manifest counts by outcome.
  const SINCE = 1791830620434;
  const totals = (wal: [number, number], manifest: [number, number, number] = [10, 0, 0], since = SINCE) =>
    data({
      "local.store.totals.since_ms": since,
      "local.store.totals.ops.put:wal": { ok: wal[0], not_found: 0, unbilled: 0, err: wal[1] },
      "local.store.totals.ops.put:manifest": { ok: manifest[0], not_found: 0, unbilled: manifest[2], err: manifest[1] },
    });

  test("pages on failed put:wal at 5 % or more of the trailing minute's puts on two consecutive polls", () => {
    const w = new Watch();
    w.poll(totals([1000, 0]));
    w.poll(totals([1100, 5])); // 5 of 105
    w.poll(totals([1200, 5])); // 5 of 205
    w.poll(totals([1300, 20])); // 20 of 320: one poll at 5 % or more
    expect(row(w, 16)).toEqual([]);
    w.poll(totals([1400, 25])); // 25 of 425 over the trailing minute
    expect(fired(w, 16).map((e) => [e.class, e.values])).toEqual([
      ["PAGE", { put_wal_n: 425, put_wal_err: 25, put_manifest_n: 0, put_manifest_err: 0, put_wal_p99_ms: 8 }],
    ]);
    // No new failure: the trailing minute drains of them, then four clean polls.
    w.polls(5 + MIN_POLLS, totals([1400, 25]));
    expect(row(w, 16)).toEqual([["trigger", "PAGE"], ["resolve", "PAGE"]]);
  });

  test("pages on an idle cell's failed manifest puts", () => {
    const w = new Watch();
    w.poll(totals([24, 0], [10, 0, 0]));
    w.poll(totals([24, 0], [12, 1, 0]));
    expect(row(w, 16)).toEqual([]);
    w.poll(totals([24, 0], [14, 2, 0]));
    expect(fired(w, 16).map((e) => [e.class, e.values])).toEqual([
      ["PAGE", { put_wal_n: 0, put_wal_err: 0, put_manifest_n: 6, put_manifest_err: 2, put_wal_p99_ms: 8 }],
    ]);
  });

  test("a put the store refused (a 412 manifest CAS conflict) is no failure: an idle cell opens nothing", () => {
    // The field's idle scratch cell: the latency window's `err` counts every
    // answer that is not a 2xx or a 304 (1-3 refused manifest puts of 9 a
    // minute), while `totals` counts them `unbilled` and fails none.
    const w = new Watch();
    for (let i = 0; i < 40; i++) {
      w.poll({
        data: patch(DATA, {
          "local.store.ops.put:manifest": cell(9, 1 + (i % 3)),
          "local.store.totals.ops.put:manifest": { ok: 10 + 7 * i, not_found: 0, unbilled: 2 * i, err: 0 },
        }),
      });
    }
    expect(row(w, 16)).toEqual([]);
  });

  test("a restart starts a new baseline: the counters a new process restarts are no failures", () => {
    const w = new Watch();
    w.polls(2, totals([1000, 900]));
    const reboot = (wal: [number, number]) => ({ ...totals(wal, [3, 0, 0], SINCE + 600_000), ...health({ bootId: "boot-2" }) });
    w.polls(2, reboot([500, 50]));
    expect(row(w, 16)).toEqual([]);
    w.poll(reboot([520, 60]));
    w.poll(reboot([540, 70]));
    expect(fired(w, 16).map((e) => [e.class, e.values])).toEqual([
      ["PAGE", { put_wal_n: 60, put_wal_err: 20, put_manifest_n: 0, put_manifest_err: 0, put_wal_p99_ms: 8 }],
    ]);
  });

  test("notifies after 5 min of put:wal p99 above 2 s; a window with no puts is clean", () => {
    const w = new Watch();
    w.polls(20, data({ "local.store.ops.put:wal": cell(50, 0, 2001) }));
    expect(row(w, 16)).toEqual([]);
    w.poll(data({ "local.store.ops.put:wal": cell(50, 0, 2001) }));
    expect(row(w, 16)).toEqual([["trigger", "NOTIFY"]]);
    w.polls(MIN_POLLS, data({ "local.store.ops": {} }));
    expect(row(w, 16)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
  });
});

describe("row 17: unowned usage", () => {
  test("notifies on growth and holds 15 min after the last growth", () => {
    const w = new Watch();
    w.poll(data({ "ops.counters.unowned_meter_events_total": 2 }));
    w.poll(data({ "ops.counters.unowned_meter_events_total": 3 }));
    expect(fired(w, 17).map((e) => [e.class, e.values])).toEqual([
      ["NOTIFY", { unowned_meter_events_total: 3, since_growth_secs: 0 }],
    ]);
    const flat = data({ "ops.counters.unowned_meter_events_total": 3 });
    w.polls(60 + MIN_POLLS - 1, flat);
    expect(row(w, 17)).toEqual([["trigger", "NOTIFY"]]);
    w.poll(flat);
    expect(row(w, 17)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
  });
});

describe("row 20: the snapshot cache", () => {
  test("notifies when ops is null more than 60 s after the boot id was first seen", () => {
    const w = new Watch();
    w.polls(5, data({ ops: null }));
    expect(row(w, 20)).toEqual([]);
    w.poll(data({ ops: null }));
    expect(fired(w, 20).map((e) => [e.class, e.values])).toEqual([["NOTIFY", { ops_null_secs: 75 }]]);
    w.polls(MIN_POLLS);
    expect(row(w, 20)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
    w.poll({ ...health({ bootId: "boot-2" }), ...data({ ops: null }) });
    expect(row(w, 20)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
  });

  test("notifies when the snapshot is older than 60 s by the cell's clock", () => {
    const w = new Watch();
    const opsAt = (DATA as { ops: { ts_ms: number } }).ops.ts_ms;
    w.poll(data({ ts_ms: opsAt + 60_000 }));
    expect(row(w, 20)).toEqual([]);
    w.poll(data({ ts_ms: opsAt + 61_000 }));
    expect(fired(w, 20).map((e) => [e.class, e.values])).toEqual([["NOTIFY", { ops_age_secs: 61 }]]);
  });
});

describe("row 20: the operator document unread", () => {
  const refused = { data: null, dataStatus: 401 };

  test("notifies when /health is 200 but /operator/data.json has not been a 200 JSON document for 60 s, with its last status", () => {
    const w = new Watch();
    w.polls(4, refused);
    expect(row(w, 20)).toEqual([]);
    w.poll(refused);
    expect(fired(w, 20).map((e) => [e.class, e.values])).toEqual([["NOTIFY", { data_status: 401, data_unread_secs: 60 }]]);
    // The bearer put right: readable again, it resolves after four clean polls.
    w.polls(MIN_POLLS);
    expect(row(w, 20)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
  });

  test("no answer is status 0; an unready /health stops the clock, so a cell that is down is row 3's", () => {
    const w = new Watch();
    w.polls(3, { data: null, dataStatus: null });
    w.polls(8, { ...health({ status: 503, git: null, bootId: null }), data: null, dataStatus: null });
    w.polls(4, { data: null, dataStatus: null });
    expect(row(w, 20)).toEqual([]);
    w.poll({ data: null, dataStatus: null });
    expect(fired(w, 20).map((e) => e.values)).toEqual([{ data_status: 0, data_unread_secs: 60 }]);
    // Open, it is not cleaned by polls that read nothing while the cell is down.
    w.polls(MIN_POLLS, { ...health({ status: 503, git: null, bootId: null }), data: null, dataStatus: null });
    expect(row(w, 20)).toEqual([["trigger", "NOTIFY"]]);
  });
});

describe("row 20 across a restart", () => {
  test("the restart's unready polls, before the new boot id is seen, are not judged against the old boot", () => {
    const w = new Watch();
    w.polls(20);
    w.polls(3, { ...health({ status: 503, git: null, bootId: null, bodyPrefix: "auth feeds not yet published: jwks" }), ...data({ ops: null }) });
    w.polls(2, { ...health({ bootId: "boot-2" }), ...data({ ops: null }) });
    w.polls(4, health({ bootId: "boot-2" }));
    expect(row(w, 20)).toEqual([]);
  });
});

describe("row 20 before any boot id is seen", () => {
  test("a null snapshot is not judged: it neither holds the rule nor cleans it", () => {
    const w = new Watch();
    const unready = health({ status: 503, git: null, bootId: null });
    const opsAt = (DATA as { ops: { ts_ms: number } }).ops.ts_ms;
    w.poll({ ...unready, ...data({ ts_ms: opsAt + 90_000 }) });
    expect(row(w, 20)).toEqual([["trigger", "NOTIFY"]]);
    w.polls(2, unready);
    w.polls(6, { ...unready, ...data({ ops: null }) });
    w.polls(1, unready);
    expect(row(w, 20)).toEqual([["trigger", "NOTIFY"]]);
    w.poll(unready);
    expect(row(w, 20)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
  });
});

describe("row 30: sealed index corrupt", () => {
  test("pages while any seal-index row is skipped, and resolves after a restart that finds none", () => {
    const w = new Watch();
    w.polls(MIN_POLLS, data({ "local.sealed_index_corrupt_rows": 0 }));
    expect(fired(w, 30)).toEqual([]);
    w.poll(data({ "local.sealed_index_corrupt_rows": 2 }));
    expect(fired(w, 30).map((e) => [e.class, e.values])).toEqual([["PAGE", { sealed_index_corrupt_rows: 2 }]]);
    w.polls(MIN_POLLS, data({ "local.sealed_index_corrupt_rows": 0 }));
    expect(row(w, 30)).toEqual([["trigger", "PAGE"], ["resolve", "PAGE"]]);
  });
});

describe("row 31: gc inexact", () => {
  test("pages on any refused decrement, and resolves after a restart that finds none", () => {
    const w = new Watch();
    w.polls(MIN_POLLS, data({ "local.gc_inexact_decrements": 0 }));
    expect(fired(w, 31)).toEqual([]);
    w.poll(data({ "local.gc_inexact_decrements": 1 }));
    expect(fired(w, 31).map((e) => [e.class, e.values])).toEqual([["PAGE", { gc_inexact_decrements: 1 }]]);
    w.polls(MIN_POLLS, data({ "local.gc_inexact_decrements": 0 }));
    expect(row(w, 31)).toEqual([["trigger", "PAGE"], ["resolve", "PAGE"]]);
  });
});

describe("row 32: gc reclaim lag", () => {
  test("notifies once a dead incarnation waits over an hour past its due instant", () => {
    const w = new Watch();
    w.polls(MIN_POLLS, data({ "local.gc_oldest_unreclaimed_late_secs": 3600 }));
    expect(fired(w, 32)).toEqual([]);
    w.poll(data({ "local.gc_oldest_unreclaimed_late_secs": 3601 }));
    expect(fired(w, 32).map((e) => [e.class, e.values])).toEqual([["NOTIFY", { gc_oldest_unreclaimed_late_secs: 3601 }]]);
    w.polls(MIN_POLLS, data({ "local.gc_oldest_unreclaimed_late_secs": 0 }));
    expect(row(w, 32)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
  });
});

describe("row 26: usage overflow", () => {
  test("notifies on any overflow admit", () => {
    const w = new Watch();
    w.poll(data({ "local.usage_overflow_admits": 1 }));
    expect(fired(w, 26).map((e) => [e.class, e.values])).toEqual([["NOTIFY", { usage_overflow_admits: 1 }]]);
    // The counter is the process's: it stays open until a restart zeroes it.
    w.polls(20, data({ "local.usage_overflow_admits": 1 }));
    w.polls(MIN_POLLS, health({ bootId: "boot-2" }));
    expect(row(w, 26)).toEqual([["trigger", "NOTIFY"], ["resolve", "NOTIFY"]]);
  });
});
