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
import { Opts } from "./common.ts";
import { PayloadGen } from "./corpus.ts";
import { FakeCell } from "./fake-cell.ts";
import { ownerOf } from "./oracle.ts";
import { type Manifest, checkManifest, expand, phaseFlags, phaseStart, readManifest } from "./tenant-plan.ts";
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
    expect([f, expand(m, "").projects.length]).toEqual([f, f === "layer-b" ? 1000 : 18]);
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
