// The hostile legs (hostile.ts) through tenants.ts's runner against the
// product-surface fake: H9's bursts and its read-debt wait, H9b's stalled
// readers, the cost-hostile mix, H11t's timed pairs on the capability path
// and the cross-project probes, each checked for exact counts, and each
// leg shown to catch the defect it exists for (a quiet name answered
// without the store fetch a never-held name takes, an existence oracle on
// reads), H11t also shown to refuse a quiet name nobody created. Run with
// `bun test ./bench/k2cost`.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { HeaderSource, Http, Meter, Opts } from "./common.ts";
import { FakeCell } from "./fake-cell.ts";
import { bodyOf, h9, h9b, ks } from "./hostile.ts";
import { ownerOf } from "./oracle.ts";
import { type Manifest, checkManifest, expand } from "./tenant-plan.ts";
import { run } from "./tenants.ts";

let cell: FakeCell;
const dir = mkdtempSync(join(tmpdir(), "k2-hostile-"));
beforeAll(() => {
  cell = new FakeCell();
  // A descriptor slot is fresh for 100 ms here (the server's 5 s): a name
  // created at a run's start is quiet a few hundred ms later.
  cell.slotTtlMs = 100;
});
afterAll(() => {
  cell.stop();
  rmSync(dir, { recursive: true, force: true });
});
const json = (f: string) => JSON.parse(readFileSync(f, "utf8"));

async function runManifest(m: Manifest, tag: string, during?: (ids: Record<string, string>) => void) {
  const plan = expand(m, "sha");
  const map = join(dir, `tokens-${tag}.json`);
  writeFileSync(map, JSON.stringify(plan.projects.map((p) => ({ project: p.id, token: `${tag}-${p.id}` }))));
  for (const p of plan.projects) cell.tokens.set(`${tag}-${p.id}`, p.id);
  const ids = Object.fromEntries(plan.projects.map((p) => [`${p.cls.class}${p.index}`, p.id]));
  during?.(ids);
  const out = join(dir, tag);
  const summary = await run(plan, cell.base, map, out, 1);
  return { out, ids, summary };
}

test("a raw response's body is whole only when its framing says so; KS tells a shifted sample from its own", () => {
  const head = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n";
  expect(bodyOf(Buffer.from(`${head}content-length: 5\r\n\r\n[1,2]`))).toEqual({ status: 200, body: Buffer.from("[1,2]") });
  expect(bodyOf(Buffer.from(`${head}content-length: 9\r\n\r\n[1,2]`)).body).toBeNull();
  expect(bodyOf(Buffer.from(`${head}transfer-encoding: chunked\r\n\r\n3\r\n[1,\r\n2\r\n2]\r\n0\r\n\r\n`)).body?.toString()).toBe("[1,2]");
  expect(bodyOf(Buffer.from(`${head}transfer-encoding: chunked\r\n\r\n3\r\n[1,\r\n2\r\n2]`)).body).toBeNull();
  const a = Array.from({ length: 400 }, (_, i) => (i % 97) / 10);
  expect(ks(a, a)).toEqual({ d: 0, p: 1 });
  const shifted = ks(a, a.map((x) => x + 3));
  expect(shifted.d).toBe(132 / 400);
  expect(shifted.p).toBeLessThan(1e-10);
});

test("H9 bursts wait out the read debt, H9b's stalled readers finish whole, the cost mix is answered per kind, and H11t and the probes see no difference", async () => {
  cell.watches = [];
  const m: Manifest = {
    name: "hostile", seed: 5, duration: 3.6,
    classes: [
      { class: "reader", count: 1, role: "hostile", streams: 2, phases: [
        { mode: "setup", at: 0 },
        { mode: "produce", at: 0.1, secs: 0.6, args: { rate: 200, "record-bytes": 512 } },
        { mode: "h9", at: 1.0, secs: 0.8, args: { readers: 4, "max-bytes": 8192, "probe-ms": 20 } },
        { mode: "h9b", at: 2.0, secs: 1.4, args: { readers: 3, "max-bytes": 8192, "hold-secs": 0.3 } },
      ] },
      { class: "coster", count: 1, role: "hostile", phases: [{ mode: "cost", at: 0.2, secs: 1, args: { rate: 40 } }] },
      { class: "target", count: 2, role: "compliant", streams: 4, prefix: "v/", phases: [
        { mode: "setup", at: 0 }, { mode: "produce", at: 0.1, secs: 3.4, args: { rate: 20, "record-bytes": 256 } },
      ] },
      { class: "ghost", count: 3, role: "idle", phases: [] },
      { class: "quiet", count: 2, role: "idle", streams: 3, prefix: "q/", phases: [{ mode: "setup", at: 0 }] },
    ],
    legs: [
      { leg: "h11t", at: 0.5, args: { pairs: 150, rate: 1000, target: "quiet" } },
      { leg: "probes", at: 0.5, args: { pairs: 30, rate: 1000, from: "ghost", to: "target" } },
    ],
  };
  cell.trace = [];
  const { out, ids } = await runManifest(m, "h", (ids) => setTimeout(() => cell.refuse(ids.reader0!, 6, 429, "read_rate_limited"), 950));
  const trace = cell.trace;
  cell.trace = null;
  const reader = ids.reader0!;
  const ph = (i: number) => json(join(out, "projects", reader, `phase-${i}.ledger.json`));
  const bursts = ph(2).h9.bursts as Array<Record<string, any>>;
  expect(bursts.length).toBeGreaterThan(1);
  // The first burst's four reads and the first two debt probes are refused.
  expect([bursts[0]!.answers, bursts[0]!.debt_probes]).toEqual([{ "429:read_rate_limited": 4 }, 3]);
  for (const b of bursts.slice(1)) expect(b.answers).toEqual({ "200:-": 4 });
  const stalled = ph(3).h9b;
  expect([stalled.statuses, stalled.cut_short, stalled.reset_during_hold]).toEqual([{ "200": 3 }, 0, 0]);
  expect(stalled.held_ms).toBeGreaterThanOrEqual(300);
  const t = json(join(out, "projects", reader, "ledger.json")).totals;
  expect([t.delivered_payload_bytes, t.oracle_ok, t.oracle_foreign ?? 0])
    .toEqual([cell.meters.get(reader)!.readPayloadBytes, t.delivered_records, 0]);
  expect(ph(3).delivered_records).toBeGreaterThanOrEqual(3 * 15);

  const answers = json(join(out, "projects", ids.coster0!, "phase-0.ledger.json")).cost.answers;
  expect(Object.keys(answers.catalog)).toEqual(["200:-"]);
  expect(Object.keys(answers.create)).toEqual(["201:-"]);
  expect(Object.keys(answers.missing)).toEqual(["404:stream_not_found"]);
  expect(Object.keys(answers.refused)).toEqual(["403:forbidden"]);
  const sent = Object.values(answers).flatMap((a: any) => Object.values(a) as number[]).reduce((x, y) => x + y, 0);
  expect(sent).toBeGreaterThanOrEqual(38);

  // H11t's quiet names are the ones the quiet projects' setups recorded,
  // each past its slot's TTL, so the cell fetched from the store for every
  // quiet name exactly as for every never-held name.
  const quiet = [ids.quiet0!, ids.quiet1!];
  const recorded = quiet.map((q) => json(join(out, "projects", q, "phase-0.ledger.json")).setup.names);
  expect(recorded).toEqual([["q/00000", "q/00001", "q/00002"], ["q/00000", "q/00001", "q/00002"]]);
  const h = json(join(out, "legs", "h11t-0.json"));
  expect([h.pairs, h.differences, h.answers, h.names]).toEqual([150, 0,
    { quiet: { "403:watch_unauthorized": 150 }, never: { "403:watch_unauthorized": 150 } }, { projects: 2, names: 6 }]);
  expect([h.quiet_ms.length, h.never_ms.length]).toEqual([150, 150]);
  const timed = cell.watches.filter((w) => quiet.includes(w.project));
  const paths: Record<string, number> = {};
  for (const w of timed) {
    const k = `${w.held ? "held" : "never"} ${w.store ? "store" : "free"}`;
    paths[k] = (paths[k] ?? 0) + 1;
  }
  expect([paths, cell.watches.length, new Set(timed.filter((w) => w.held).map((w) => `${w.project} ${w.name}`)).size])
    .toEqual([{ "held store": 150, "never store": 150 }, 300, 6]);
  // Every (ghost, target name) once, each a first contact: 3 ghosts x the
  // targets' 4 names (v/00000..v/00003), fewer than the 30 pairs asked.
  const p = json(join(out, "legs", "probes-1.json"));
  expect([p.pairs, p.first_contacts, p.available, p.probers, p.names, p.differences, p.put, p.delete, p.usage_answers])
    .toEqual([12, 12, 12, 3, 4, { name: 0, usage: 0 }, { "201": 12 }, { "204": 12 }, { "403:forbidden": 12 }]);
  // No prober ever reads a name after it created that name in its own namespace.
  const ghosts = new Set([ids.ghost0!, ids.ghost1!, ids.ghost2!]);
  const created = new Map<string, Set<string>>();
  const late: string[] = [];
  for (const [project, method, path] of trace!.filter(([q]) => ghosts.has(q))) {
    const name = /^\/v1\/streams\/(.+?)(\/records)?$/.exec(path)?.[1];
    if (!name) continue;
    if (method === "PUT") created.set(project, new Set([...(created.get(project) ?? []), name]));
    else if (method === "GET" && created.get(project)?.has(name)) late.push(`${project} ${name}`);
  }
  expect([created.size, late]).toEqual([3, []]);
}, 30_000);

test("H11t's timing and the probes' answers each catch the defect they look for", async () => {
  // A cell that answers an expired slot without the store fetch an absent
  // name takes: a quiet name answers about 4 ms sooner than a never-held one.
  cell.storeMs = 4;
  cell.expiredAnsweredFree = true;
  cell.existenceLeak = true;
  try {
    const m: Manifest = {
      name: "defects", seed: 6, duration: 2,
      classes: [
        { class: "target", count: 1, role: "compliant", streams: 3, prefix: "v/", phases: [{ mode: "setup", at: 0 }] },
        { class: "ghost", count: 2, role: "idle", phases: [] },
      ],
      legs: [
        { leg: "h11t", at: 0.2, args: { pairs: 120, rate: 1000, target: "target" } },
        { leg: "probes", at: 0.2, args: { pairs: 20, rate: 1000, from: "ghost", to: "target" } },
      ],
    };
    const { out } = await runManifest(m, "d");
    const h = json(join(out, "legs", "h11t-0.json"));
    expect([h.pairs, h.differences, h.ks.p < 1e-6, h.quiet_p50_ms < h.never_p50_ms]).toEqual([120, 0, true, true]);
    const p = json(join(out, "legs", "probes-1.json"));
    // 2 ghosts x the target's 3 names: 6 first contacts, every one a difference.
    expect([p.first_contacts, p.differences]).toEqual([6, { name: 6, usage: 0 }]);
    expect(p.examples[0]).toContain("403|forbidden");
  } finally {
    cell.storeMs = 0;
    cell.expiredAnsweredFree = false;
    cell.existenceLeak = false;
  }
}, 30_000);

test("H11t fails rather than time a quiet name nobody created: its names come only from its target's setup record", async () => {
  cell.watches = [];
  const m: Manifest = {
    name: "vacuous", seed: 8, duration: 1.5,
    classes: [{ class: "quiet", count: 1, role: "idle", streams: 3, prefix: "q/", phases: [{ mode: "setup", at: 0 }] }],
    legs: [{ leg: "h11t", at: 0.3, args: { pairs: 50, rate: 1000, target: "quiet" } }],
  };
  // The cell refuses the quiet project's three creates, so its names exist
  // nowhere: timed against never-held names they would match by construction.
  const { out, ids } = await runManifest(m, "v", (ids) => cell.refuse(ids.quiet0!, 3, 403, "forbidden"));
  const h = json(join(out, "legs", "h11t-0.json"));
  expect([h.error ?? null, h.pairs ?? null, cell.watches.length]).toEqual([
    "h11t: the setup of class quiet recorded no stream it created (1 project), so there is no quiet name to time", null, 0,
  ]);
  const setup = json(join(out, "projects", ids.quiet0!, "phase-0.ledger.json")).setup;
  expect([setup.created, setup.failed, setup.names]).toEqual([0, 3, []]);
  // A target whose names are touched after its setup, or which has no
  // setup to record them, is refused before the run.
  const refused = (edit: (x: Manifest) => void) => {
    const x = structuredClone(m);
    edit(x);
    try {
      checkManifest(x);
      return "accepted";
    } catch (e) {
      return (e as Error).message;
    }
  };
  const why = "manifest: leg 0: h11t's target class quiet must run setup phases and nothing else, so its streams stay quiet";
  expect(refused((x) => x.classes[0]!.phases.push({ mode: "produce", at: 0.5, secs: 0.5, args: { rate: 1 } }))).toBe(why);
  expect(refused((x) => (x.classes[0]!.phases = []))).toBe(why);
  expect(refused(() => {})).toBe("accepted");
}, 30_000);

test("H11t's pairs start at least 1000/rate ms apart and never catch up after a stall", async () => {
  cell.watches = [];
  // The first request of pair 10 is answered 400 ms late.
  cell.watchStall = { index: 20, ms: 400 };
  try {
    const m: Manifest = {
      name: "pacing", seed: 7, duration: 3,
      classes: [{ class: "target", count: 1, role: "idle", streams: 2, prefix: "v/", phases: [{ mode: "setup", at: 0 }] }],
      legs: [{ leg: "h11t", at: 0.2, args: { pairs: 30, rate: 25, target: "target" } }],
    };
    const { out } = await runManifest(m, "pace");
    // Each pair sends its two requests one after the other: a pair starts
    // with the cell's request 2k. One gap holds the stall; after it the
    // pairs keep the 40 ms beat, none sent in a burst to catch up.
    const starts = cell.watches.filter((_, k) => k % 2 === 0).map((w) => w.t);
    const gaps = starts.slice(1).map((t, k) => t - starts[k]!);
    expect([starts.length, gaps.filter((g) => g >= 400).length, gaps.filter((g) => g < 10).map((g) => Math.round(g))]).toEqual([30, 1, []]);
    const h = json(join(out, "legs", "h11t-0.json"));
    expect([h.pairs, h.rate, h.gap_min_ms >= 40]).toEqual([30, 25, true]);
  } finally {
    cell.watchStall = null;
  }
}, 30_000);

test("H9b reports a stalled reader whose body the server cut short, and counts none of it as delivered", async () => {
  const srv = createServer((s) => s.once("data", () => s.end("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 4096\r\n\r\n[{\"o\":1}")));
  await new Promise<void>((r) => srv.listen(0, "127.0.0.1", () => r()));
  const port = (srv.address() as { port: number }).port;
  const meter = new Meter();
  const ctx = {
    http: new Http(`http://127.0.0.1:${port}`, new HeaderSource(undefined, { authorization: "Bearer t" }), meter), meter,
    o: new Opts(new Map([["readers", "2"], ["hold-secs", "0.1"]])), prefix: "t/", streams: 1, seed: 1, duration: 1,
    owner: ownerOf("x", "proj_x"),
  };
  const r = (await h9b(ctx, AbortSignal.timeout(5000))).h9b as Record<string, unknown>;
  srv.close();
  expect([r.statuses, r.cut_short, meter.tot.get("delivered_records")]).toEqual([{ "200": 2 }, 2, 0]);
});

test("an H9 read whose page does not parse counts as a corrupt delivery on the oracle", async () => {
  const srv = Bun.serve({
    port: 0, hostname: "127.0.0.1",
    fetch: (req) => new Response(new URL(req.url).searchParams.get("cursor") === "now" ? "[]" : '[{"o":"proj_x","id":',
      { headers: { "content-type": "application/json" } }),
  });
  const meter = new Meter();
  const ctx = {
    http: new Http(`http://127.0.0.1:${srv.port}`, new HeaderSource(undefined, { authorization: "Bearer t" }), meter), meter,
    o: new Opts(new Map([["readers", "2"], ["probe-ms", "10"]])), prefix: "t/", streams: 1, seed: 1, duration: 1,
    owner: ownerOf("x", "proj_x"),
  };
  const bursts = ((await h9(ctx, AbortSignal.timeout(300))).h9 as { bursts: unknown[] }).bursts;
  srv.stop(true);
  expect(bursts.length).toBeGreaterThan(0);
  expect([meter.tot.get("oracle_unmarked"), meter.tot.get("bad_json"), meter.tot.get("delivered_records")])
    .toEqual([2 * bursts.length, 2 * bursts.length, 0]);
});
