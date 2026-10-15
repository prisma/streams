// tenants.ts against the product-surface fake (fake-cell.ts): a manifest of
// a compliant, a hostile and an idle class runs k2gen's modes per project,
// and every number the verdict reads is checked against what the fake
// stored and served: per-project ledgers, per-class windows, typed
// refusals, the token map's rotation, the usage read and the audit. Run
// with `bun test ./bench/k2cost`.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DEFAULT_PROFILES } from "../../deploy/cell-admin/commands.ts";
import { type Ceilings, cellSizing, readProfiles } from "../../deploy/cell-admin/profile.ts";
import { FREE_TIER_REQUESTS_PER_SEC } from "../../deploy/issuer/projects.ts";
import { Opts } from "./common.ts";
import { PayloadGen } from "./corpus.ts";
import { FakeCell } from "./fake-cell.ts";
import { ownerOf } from "./oracle.ts";
import { type ClassSpec, type Manifest, checkManifest, expand, phaseFlags, phaseStart, readManifest } from "./tenant-plan.ts";
import { TokenMap, auditAll, run, usage } from "./tenants.ts";

let cell: FakeCell;
const dir = mkdtempSync(join(tmpdir(), "k2-tenants-"));
beforeAll(() => {
  cell = new FakeCell();
});
afterAll(() => {
  cell.stop();
  rmSync(dir, { recursive: true, force: true });
});

const QH = { requests_per_sec: 20, max_streams: 50 };
const manifest = (): Manifest => ({
  name: "unit", seed: 3, duration: 4,
  periods: { early: [0, 2], late: [2, 4] },
  quota_sets: { "q-hostile": QH },
  classes: [
    { class: "app", count: 2, role: "compliant", streams: 2, phases: [
      { mode: "setup", at: 0 },
      { mode: "produce", at: 0.3, secs: 3, args: { rate: 40, "record-bytes": 256 } },
      { mode: "walk", at: 0.3, secs: 3.4, args: { from: "lag:0.5" } },
      { mode: "tail", at: 0.3, secs: 3.4, args: { via: "sse", readers: 1 } },
    ] },
    { class: "flood", count: 1, role: "hostile", quotas: "q-hostile", streams: 1, phases: [
      { mode: "setup", at: 0 },
      { mode: "produce", at: 0.3, secs: 3, args: { rate: 30, "record-bytes": 256 } },
    ] },
    { class: "cold", count: 3, role: "idle", streams: 2, phases: [{ mode: "setup", at: 0, spread: 1 }] },
  ],
});

const json = (f: string) => JSON.parse(readFileSync(f, "utf8"));
const lines = (f: string) => readFileSync(f, "utf8").trim().split("\n").map((l) => JSON.parse(l));

test("a manifest expands into projects with their own ids, seeds, keys and quotas, and refuses what cannot be audited", () => {
  const plan = expand(manifest(), "sha", 0.5);
  expect(plan.projects.map((p) => p.id)).toEqual([
    "proj_unit_app_0000", "proj_unit_app_0001", "proj_unit_flood_0000", "proj_unit_cold_0000", "proj_unit_cold_0001", "proj_unit_cold_0002",
  ]);
  expect(plan.projects.map((p) => p.workspace)[2]).toBe("wksp_unit_flood_0000");
  expect(new Set(plan.projects.map((p) => p.seed)).size).toBe(6);
  expect(new Set(plan.projects.map((p) => p.recordKey)).size).toBe(6);
  expect(plan.projects[2]!.quotaFlags).toEqual(["requests_per_sec=20", "max_streams=50"]);
  expect(plan.projects[0]!.quotaFlags).toEqual([]);
  const cold = plan.projects.slice(3);
  expect(cold.map((p) => phaseStart(plan, p, p.cls.phases[0]!))).toEqual([0, 1 / 6, 2 / 6]);
  expect(Object.fromEntries(phaseFlags(plan.projects[0]!, plan.projects[0]!.cls.phases[1]!))).toEqual({
    streams: "2", prefix: "t/", rate: "40", "record-bytes": "256", payload: "oracle",
  });
  const bad = (edit: (m: Manifest) => void) => {
    const m = manifest();
    edit(m);
    try {
      checkManifest(m);
      return "accepted";
    } catch (e) {
      return (e as Error).message;
    }
  };
  expect(bad((m) => (m.classes[0]!.phases[1]!.args!.payload = "corpus"))).toContain("a tenant writes --payload oracle");
  expect(bad((m) => (m.classes[0]!.phases[2]!.args!.rate = 5))).toContain("embeds no producer");
  expect(bad((m) => (m.classes[0]!.phases[1]!.repeat = true))).toContain("produce never repeats");
  expect(bad((m) => (m.classes[0]!.phases[1]!.args!.base = "x"))).toContain("--base is set by the runner");
  expect(bad((m) => (m.classes[0]!.phases[1]!.args!.bogus = 1))).toContain("produce takes no --bogus");
  expect(bad((m) => (m.classes[1]!.quotas = "q-none"))).toContain("no quota set q-none");
  expect(bad((m) => (m.classes[0]!.phases[1]!.at = 4))).toContain("starts at or after the run's duration");
  expect(bad((m) => (m.classes[0]!.phases[1]!.mode = "audit"))).toContain("unknown mode");
  for (const f of ["layer-b", "layer-b-tiny"]) {
    const { manifest: m } = readManifest(join(import.meta.dir, "tenants", `${f}.json`));
    expect([f, expand(m, "").projects.length]).toEqual([f, f === "layer-b" ? 1000 : 28]);
  }
});

/** A project's nominal requests a second with every phase of its class
 * running at once (setup is checked to end before the others start), from
 * what k2gen's modes send: produce its rate; a periodic walk (lag:SECS, no
 * embedded producer) a page per stream every SECS; a consumer group a pull
 * per stream and consumer every wait-ms while idle, and a pull and a
 * settle for each append it is fed; an SSE tail nothing once connected;
 * subs its connect rate; churn a create, its appends and a read-back page
 * per lifecycle (and a delete unless it ends by TTL). Throws on a shape it
 * does not model. */
function nominalRate(c: ClassSpec): number {
  const num = (v: string | number | undefined, d: number) => (v === undefined ? d : Number(v));
  const produce = c.phases.filter((ph) => ph.mode === "produce").map((ph) => ph.args ?? {});
  let rate = 0;
  for (const ph of c.phases) {
    const a = ph.args ?? {};
    const streams = num(a.streams, c.streams ?? 1);
    const lag = /^lag:([0-9.]+)$/.exec(String(a.from ?? ""));
    if (ph.mode === "setup") continue;
    else if (ph.mode === "produce" && a.rate !== undefined && a.mbps === undefined) rate += Number(a.rate);
    else if (ph.mode === "walk" && lag && a.rate === undefined) rate += streams / Number(lag[1]);
    else if (ph.mode === "tail" && a.via === "sse") continue;
    else if (ph.mode === "subs" && a.via === "sse") rate += num(a["connect-rate"], 20);
    else if (ph.mode === "group" && a.rate === undefined) {
      const pull = num(a.pull, 10);
      rate += (streams * num(a.consumers, 1) * 1000) / num(a["wait-ms"], 1000);
      for (const p of produce) rate += Number(p.rate) * Math.ceil(num(p.records, 1) / pull) * 2;
    } else if (ph.mode === "churn" && a.rate !== undefined) {
      const recordBytes = num(a["record-bytes"], 1024);
      const perLife = Math.max(1, Math.round((num(a["size-kib"], 10) * 1024) / recordBytes));
      const perReq = Math.max(1, Math.min(num(a.records, 100), Math.floor((1 << 20) / recordBytes)));
      const del = String(a.end ?? "delete").startsWith("ttl:") ? 0 : 1;
      rate += Number(a.rate) * (1 + Math.ceil(perLife / perReq) + 1 + del);
    } else throw new Error(`class ${c.class}: no request model for its ${ph.mode} phase ${JSON.stringify(a)}`);
  }
  return rate;
}

test("Layer B runs both quota postures: the ceilings classes as the stress, and free-tier twins shaped under the bucket", () => {
  // Lead decision deploy-q5 #6 (1), 2026-10-13. A free-tier project's
  // request bucket and store budget each hold one second of its rate
  // (src/quota/bucket.rs, src/quota/parked.rs).
  const twins = ["s0", "s1", "s2", "s3s", "n0", "victim"];
  for (const f of ["layer-b", "layer-b-tiny"]) {
    const { manifest: m } = readManifest(join(import.meta.dir, "tenants", `${f}.json`));
    expect([f, m.quota_sets?.["free-tier"]]).toEqual([f, { requests_per_sec: FREE_TIER_REQUESTS_PER_SEC }]);
    const free = m.classes.filter((c) => c.quotas === "free-tier");
    expect([f, free.map((c) => c.class)]).toEqual([f, twins.map((t) => `${t}-free`)]);
    for (const cls of [...twins, "h11t-target", "warm", "cold", "cold-none"]) {
      expect([f, cls, m.classes.find((c) => c.class === cls)?.quotas ?? "ceilings"]).toEqual([f, cls, "ceilings"]);
    }
    for (const c of free) {
      const twin = m.classes.find((o) => o.class === c.class.replace(/-free$/, ""))!;
      // As many projects as the original, except S3s's twin in Layer B:
      // one, for the live pool (the next test; lead decision 2026-10-14 night).
      const count = f === "layer-b" && c.class === "s3s-free" ? 1 : twin.count;
      expect([f, c.class, c.role, c.count, c.streams]).toEqual([f, c.class, twin.role, count, twin.streams]);
      // k2gen's appends are Poisson: at a mean of 5 a second or less none
      // is refused by a bucket of 10, at 8 about 0.3% are.
      const rate = nominalRate(c);
      expect([f, c.class, rate, rate <= FREE_TIER_REQUESTS_PER_SEC / 2]).toEqual([f, c.class, rate, true]);
      // Creates a second apart, all before any other phase starts; no
      // delete at all. A create weighs 11 of the store budget's 10 a second
      // and a project's first since its cell booted 21, so on the real
      // binary each twin's second create is refused once (README, "Known
      // red"; the owner's call).
      const [setup, ...rest] = c.phases;
      const stagger = Number(setup?.args?.stagger ?? 0);
      expect([f, c.class, setup?.mode, stagger >= 1]).toEqual([f, c.class, "setup", true]);
      const setupEnd = setup!.at + (setup!.spread ?? 0) + ((c.streams ?? 1) - 1) * stagger + 1;
      for (const ph of rest) expect([f, c.class, ph.mode, ph.at >= setupEnd]).toEqual([f, c.class, ph.mode, true]);
      for (const ph of rest.filter((p) => p.mode === "churn")) {
        expect([f, c.class, String(ph.args?.end).startsWith("ttl:"), Number(ph.args?.rate) <= 1]).toEqual([f, c.class, true, true]);
      }
      // A consumer group's appends each bring a pull's return and a settle
      // within milliseconds, so its Poisson clusters come three times over:
      // its consumers start a second apart and it appends at most 0.1 a
      // second (simulated, N0's 0.41 a second draws about one refusal an
      // hour, S1's 41 records a second as one :batch a second about 20).
      const groups = rest.filter((p) => p.mode === "group");
      const appends = rest.filter((p) => p.mode === "produce").reduce((a, p) => a + Number(p.args?.rate), 0);
      for (const g of groups) {
        expect([f, c.class, Number(g.args?.stagger) >= 1, appends <= 0.1]).toEqual([f, c.class, true, true]);
      }
    }
  }
});

/** A manifest's hold on the instance's live pool in its noisy period
 * (src/admission.rs `park`, src/quota/parked.rs): each project's SSE
 * connections (an SSE tail's readers on each stream, an SSE subs phase's
 * count), at most its max_live_subscriptions; then its waits (a consumer
 * group's pulls, one per stream and consumer, a long-poll tail's or subs
 * phase's readers), at most its in-flight quota and what its share has
 * left after its SSE. A project's quotas are its class's set, the cell's
 * ceilings on every axis the set leaves out. Every phase that overlaps the
 * period counts at once, so it is an upper bound. */
function livePool(m: Manifest, ceilings: Ceilings): { sse: number; waits: number } {
  const [from, to] = m.periods!.noisy!;
  let sse = 0;
  let waits = 0;
  for (const c of m.classes) {
    const q = { ...ceilings, ...(c.quotas ? m.quota_sets![c.quotas] : {}) };
    let wantSse = 0;
    let wantWaits = 0;
    for (const ph of c.phases) {
      if (ph.at >= to || ph.at + (ph.spread ?? 0) + (ph.secs ?? m.duration) <= from) continue;
      const a = ph.args ?? {};
      const streams = Number(a.streams ?? c.streams ?? 1);
      if (ph.mode === "group") wantWaits += streams * Number(a.consumers ?? 1);
      if (ph.mode !== "tail" && ph.mode !== "subs") continue;
      const n = ph.mode === "tail" ? streams * Number(a.readers ?? 1) : Number(a.count ?? 100);
      if ((a.via ?? (ph.mode === "subs" ? "sse" : "long-poll")) === "sse") wantSse += n;
      else wantWaits += n;
    }
    const held = Math.min(wantSse, q.max_live_subscriptions!);
    sse += c.count * held;
    waits += c.count * Math.min(wantWaits, q.max_inflight_requests!, q.max_live_subscriptions! - held);
  }
  return { sse, waits };
}

test("Layer B's noisy period fits the cell's live pool: every SSE connection held and every wait parked", () => {
  // Lead decision 2026-10-14 night: s3s-free at count 1, not the twins at
  // the originals' counts (1,130 SSE connections of 1,200, and about 114
  // waits left active against the write gate). A wait that finds the pool
  // full stays an active request (src/admission.rs `park`).
  const { bounds, ceilings } = cellSizing(readProfiles(DEFAULT_PROFILES));
  expect([bounds.max_live_subscriptions, ceilings.max_live_subscriptions, ceilings.max_inflight_requests]).toEqual([1200, 150, 64]);
  const want: Record<string, { sse: number; waits: number }> = { "layer-b": { sse: 830, waits: 184 }, "layer-b-tiny": { sse: 40, waits: 22 } };
  for (const f of ["layer-b", "layer-b-tiny"]) {
    const { manifest: m } = readManifest(join(import.meta.dir, "tenants", `${f}.json`));
    const live = livePool(m, ceilings);
    expect([f, live, live.sse + live.waits <= bounds.max_live_subscriptions]).toEqual([f, want[f], true]);
  }
});

test("the token map is re-read on its period, and soon (not at once) after a 401", async () => {
  const f = join(dir, "map-unit.json");
  writeFileSync(f, JSON.stringify([{ project: "p", token: "one" }]));
  const m = new TokenMap(f, 300);
  writeFileSync(f, JSON.stringify([{ project: "p", token: "two" }]));
  expect(m.token("p")).toBe("one");
  await Bun.sleep(320);
  expect(m.token("p")).toBe("two");
  writeFileSync(f, JSON.stringify([{ project: "p", token: "three" }]));
  m.reloadSoon();
  expect(m.token("p")).toBe("two");
  expect(m.reads).toBe(2);
});

test("a run writes exact per-project ledgers, per-class windows and typed refusals; usage and the audit agree with them", async () => {
  const m = manifest();
  const plan = expand(m, "sha");
  const mapFile = join(dir, "tokens.json");
  const tokens = plan.projects.map((p) => ({ project: p.id, token: `tok-${p.id}` }));
  writeFileSync(mapFile, JSON.stringify(tokens));
  for (const t of tokens) cell.tokens.set(t.token, t.project);
  const [app0, app1, flood] = plan.projects;
  // Once set up, the hostile project's next 6 requests are refused with a
  // code, and later 2 without one; app1's token is rotated at 1.3 s.
  setTimeout(() => cell.refuse(flood!.id, 6, 429, "rate_limited"), 500);
  setTimeout(() => {
    cell.refuse(flood!.id, 2, 503, null);
    cell.tokens.delete(`tok-${app1!.id}`);
    cell.tokens.set(`tok2-${app1!.id}`, app1!.id);
    writeFileSync(mapFile, JSON.stringify(tokens.map((t) => (t.project === app1!.id ? { ...t, token: `tok2-${app1!.id}` } : t))));
  }, 1300);
  const out = join(dir, "run");
  const summary = await run(plan, cell.base, mapFile, out, 1);
  expect(summary.phases_failed).toEqual([]);
  expect((summary.token_map as { reads: number }).reads).toBeGreaterThanOrEqual(2);

  const ledgers = Object.fromEntries(plan.projects.map((p) => [p.id, json(join(out, "projects", p.id, "ledger.json"))]));
  for (const p of plan.projects) {
    const t = ledgers[p.id].totals;
    const served = cell.meters.get(p.id) ?? { ingestRecords: 0, readPayloadBytes: 0 };
    const stored = [...cell.ns(p.id).values()].reduce((a, r) => a + r.length, 0);
    expect([p.id, t.acked_records, t.delivered_payload_bytes]).toEqual([p.id, served.ingestRecords, served.readPayloadBytes]);
    expect([p.id, stored]).toEqual([p.id, t.acked_records]);
    expect(t.oracle_foreign ?? 0).toBe(0);
  }
  expect(ledgers[app0!.id].totals.acked_records).toBeGreaterThan(60);
  expect(ledgers[app0!.id].totals.oracle_ok).toBe(ledgers[app0!.id].totals.delivered_records);
  expect(ledgers[app0!.id].refusals).toEqual({ typed: {}, untyped: {}, transport: 0 });
  const rotated = ledgers[app1!.id];
  expect(Object.keys(rotated.refusals.typed)).toEqual(["401:unauthorized"]);
  expect(rotated.phases.find((ph: { mode: string }) => ph.mode === "produce").acked_records).toBeGreaterThan(60);
  expect(ledgers[flood!.id].refusals).toEqual({ typed: { "429:rate_limited": 6 }, untyped: { "503": 2 }, transport: 0 });
  expect(ledgers[plan.projects[3]!.id].phases).toHaveLength(1);
  expect(readdirSync(join(out, "projects", app0!.id)).sort()).toEqual([
    "ledger.json", "phase-0.ledger.json", "phase-1.ledger.json", "phase-2.ledger.json", "phase-3.ledger.json",
  ]);

  // The driver's own event-loop lag, per window and over the run and each period.
  const lag = lines(join(out, "loop.jsonl"));
  expect(lag.length).toBeGreaterThanOrEqual(4);
  const runDoc = json(join(out, "run.json"));
  expect(Object.keys(runDoc.loop_lag).sort()).toEqual(["early", "late", "run"]);
  expect(runDoc.loop_lag.run.ticks).toBe(lag.reduce((a, l) => a + l.ticks, 0));
  expect(runDoc.loop_lag.run.ticks).toBeGreaterThan(40);
  expect(runDoc.loop_lag.run.lag_p99_ms).toBeGreaterThanOrEqual(0);
  // A window counts in the periods holding its midpoint; the last, after 4 s, in the run's only.
  expect(runDoc.loop_lag.early.ticks).toBeGreaterThan(0);
  expect(runDoc.loop_lag.early.ticks + runDoc.loop_lag.late.ticks).toBeLessThanOrEqual(runDoc.loop_lag.run.ticks);

  // The windows add up to the ledgers, class by class.
  const wins = lines(join(out, "windows.jsonl"));
  for (const cls of ["app", "flood", "cold"]) {
    const mine = wins.filter((w) => w.class === cls);
    const sum = (k: string) => mine.reduce((a, w) => a + (w[k] ?? 0), 0);
    const want = (k: string) => plan.projects.filter((p) => p.cls.class === cls).reduce((a, p) => a + (ledgers[p.id].totals[k] ?? 0), 0);
    for (const k of ["acked_records", "delivered_payload_bytes", "requests"]) expect([cls, k, sum(k)]).toEqual([cls, k, want(k)]);
  }
  const floodCodes = wins.filter((w) => w.class === "flood").reduce((a, w) => {
    for (const [k, v] of Object.entries(w.error_codes as Record<string, number>)) a[k] = (a[k] ?? 0) + v;
    return a;
  }, {} as Record<string, number>);
  expect(floodCodes).toEqual({ "429:rate_limited": 6, "503:-": 2 });
  const classes = json(join(out, "classes.json"));
  expect(classes.flood.refusals).toEqual({ typed: { "429:rate_limited": 6 }, untyped: { "503": 2 }, transport: 0 });
  expect(classes.app.periods.run.append_samples).toBe(ledgers[app0!.id].totals.acked_requests + ledgers[app1!.id].totals.acked_requests);
  expect(classes.app.periods.early.append_samples + classes.app.periods.late.append_samples).toBe(classes.app.periods.run.append_samples);
  expect(classes.app.periods.early.append_samples).toBeGreaterThan(0);

  // The meters settle at the ledgers; the audit reads every produced
  // record back, and then flags the one foreign record planted.
  const u = await usage(plan, cell.base, mapFile, out, new Opts(new Map([["poll-secs", "0.05"], ["max-wait-secs", "5"]])));
  for (const p of plan.projects) {
    const e = (u.projects as Record<string, any>)[p.id];
    expect([p.id, e.status, e.settled, e.meters.ingestRecords, e.meters.readPayloadBytes])
      .toEqual([p.id, 200, true, ledgers[p.id].totals.acked_records, ledgers[p.id].totals.delivered_payload_bytes]);
  }
  const a1 = (await auditAll(plan, cell.base, mapFile, out, new Opts(new Map()))).projects as Record<string, any>;
  expect(Object.keys(a1).sort()).toEqual([app0!.id, app1!.id, flood!.id].sort());
  for (const id of Object.keys(a1)) expect([id, a1[id].prefixes["t/"].ok, a1[id].prefixes["t/"].oracle.foreign]).toEqual([id, true, 0]);
  cell.plant(app0!.id, "t/00001", new PayloadGen("oracle", 256, 5, 3, ownerOf("unit:3", flood!.id)).json());
  const a2 = (await auditAll(plan, cell.base, mapFile, out, new Opts(new Map()))).projects as Record<string, any>;
  expect([a2[app0!.id].prefixes["t/"].ok, a2[app0!.id].prefixes["t/"].unexpected_records, a2[app0!.id].prefixes["t/"].oracle.foreign])
    .toEqual([false, 1, 1]);
  expect(json(join(out, "audit.json")).projects[app1!.id].prefixes["t/"].ok).toBe(true);
}, 30_000);

test("a subscription refused at connect counts its status and code among the project's typed refusals", async () => {
  const m: Manifest = {
    name: "sse", seed: 9, duration: 2.5, classes: [
      { class: "storm", count: 1, role: "hostile", streams: 2, phases: [
        { mode: "setup", at: 0 },
        { mode: "subs", at: 0.5, secs: 1.8, args: { count: 3, via: "sse", "connect-rate": 100 } },
      ] },
    ],
  };
  const plan = expand(m, "sha");
  const p = plan.projects[0]!;
  const mapFile = join(dir, "tokens-sse.json");
  writeFileSync(mapFile, JSON.stringify([{ project: p.id, token: `tok-${p.id}` }]));
  cell.tokens.set(`tok-${p.id}`, p.id);
  setTimeout(() => cell.refuse(p.id, 3, 429, "live_subscription_limit"), 300);
  const out = join(dir, "run-sse");
  expect((await run(plan, cell.base, mapFile, out, 1)).phases_failed).toEqual([]);
  const ledger = json(join(out, "projects", p.id, "ledger.json"));
  expect(ledger.refusals).toEqual({ typed: { "429:live_subscription_limit": 3 }, untyped: {}, transport: 0 });
  expect(ledger.totals.status_counts["429"]).toBe(3);
}, 20_000);

