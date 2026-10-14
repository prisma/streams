// The tiny Layer B manifest (tenants/layer-b-tiny.json) on the real binary,
// booted on the shared-cell profile exactly as layered for Compute
// (compute-1g.env, then shared-cell.env) over s3lite, its projects
// admitted by tenants-admit.ts through cell-admin. Opt-in, because it needs
// built binaries and takes about two minutes:
//
//   cargo build --locked --release --bin streams-slate --bin s3lite
//   TENANTS_E2E_BIN_DIR=target/release bun test ./bench/k2cost/tenants.e2e.test.ts
//
// It proves what tenants.test.ts's fake assumes: the oracle reads clean on
// every path of the real server, every project's meters settle at its
// ledger, the audit passes every producing project, compliant tenants are
// never refused, and a hostile tenant at Q-hostile is refused with a code.
// The hostile legs run too (H9, H9b, the cost mix, H11t, h11t-fresh, the
// probes): their answers are checked, and the KS results are printed, not
// judged (H11t's bar is shared_verdict.py's; h11t-fresh is a fact). H11t
// times the cold class's quiet names (created at setup, untouched since, so
// past the cell's 5 s descriptor TTL) against never-held names, as the
// owner re-specified it (round-3 call 9, (a), 2026-10-13): on the server
// both take the same budgeted store fetch. h11t-fresh times names the
// target uses every few seconds, answered from their fresh slots.
import { afterAll, expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { DEFAULT_PROFILES } from "../../deploy/cell-admin/commands.ts";
import { readProfiles } from "../../deploy/cell-admin/profile.ts";
import { cleanup, keyFile, readBundle, scratchDir } from "../../deploy/cell-admin/test-rig.ts";
import { Opts } from "./common.ts";
import { expand, readManifest } from "./tenant-plan.ts";
import { main as admitMain } from "./tenants-admit.ts";
import { auditAll, run, usage } from "./tenants.ts";

const BIN_DIR = process.env.TENANTS_E2E_BIN_DIR;
const maybe = BIN_DIR ? test : test.skip;
const children: ReturnType<typeof Bun.spawn>[] = [];
afterAll(() => {
  for (const c of children) c.kill();
  cleanup();
});

function freePort(): number {
  const s = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response() });
  const port = s.port;
  s.stop(true);
  return port;
}

async function until<T>(what: string, secs: number, probe: () => Promise<T | undefined>): Promise<T> {
  for (let i = 0; i < secs * 4; i++) {
    const v = await probe().catch(() => undefined);
    if (v !== undefined) return v;
    await Bun.sleep(250);
  }
  throw new Error(`timed out waiting for ${what}`);
}


maybe("the tiny Layer B manifest runs clean on the real binary: oracle, meters, audit and refusals", async () => {
  const root = scratchDir();
  const { manifest, sha256 } = readManifest(join(import.meta.dir, "tenants", "layer-b-tiny.json"));
  const mfile = join(import.meta.dir, "tenants", "layer-b-tiny.json");
  const key = keyFile(root);
  const state = join(root, "state");
  const say = (l: string) => console.log(`[admit] ${l}`);
  expect(await admitMain(["admit", "--manifest", mfile, "--state", state, "--key-file", key, "--cell-id", "cell_e2e",
    "--deployment-project", "proj_e2e_deploy", "--account", "acct_e2e_sink"], say)).toBe(0);
  const map = join(root, "tokens.json");
  expect(await admitMain(["tokens", "--manifest", mfile, "--state", state, "--key-file", key, "--out", map], say)).toBe(0);
  const feeds = join(root, "feeds");
  mkdirSync(feeds);
  const b = readBundle(state);
  for (const [name, doc] of [["keys", b.keys], ["policies", b.policies], ["grants", b.grants]] as const) {
    writeFileSync(join(feeds, `${name}.json.tmp`), JSON.stringify(doc));
    renameSync(join(feeds, `${name}.json.tmp`), join(feeds, `${name}.json`));
  }
  const [s3Port, port] = [freePort(), freePort()];
  const bin = resolve(BIN_DIR ?? "");
  children.push(Bun.spawn([join(bin, "s3lite"), "--listen", `127.0.0.1:${s3Port}`, "--latency-ms", "5"], { stdout: "ignore", stderr: "ignore" }));
  const operator = randomBytes(24).toString("hex");
  const env: Record<string, string> = {
    PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "",
    ...Object.fromEntries(readProfiles(DEFAULT_PROFILES)),
    STREAMS_AUTH_KEYS_FILE: join(feeds, "keys.json"), STREAMS_AUTH_POLICY_FILE: join(feeds, "policies.json"),
    STREAMS_AUTH_GRANTS_FILE: join(feeds, "grants.json"),
    CELL_ID: "cell_e2e", PROJECT_ID: "proj_e2e_deploy", ACCOUNT_ID: "acct_e2e_sink",
    USAGE_STREAM_KEY: randomBytes(32).toString("base64url"), AUTH_TOKEN: operator,
  };
  children.push(Bun.spawn([join(bin, "streams-slate"), "--listen", `127.0.0.1:${port}`, "--s3-endpoint", `http://127.0.0.1:${s3Port}`,
    "--bucket", `tenants-e2e-${Date.now()}`], { env, stdout: Bun.file(join(root, "server.log")), stderr: Bun.file(join(root, "server.log")) }));
  const cell = `http://127.0.0.1:${port}`;
  await until("the cell to be ready", 90, async () => ((await fetch(`${cell}/health`)).ok ? true : undefined));

  const plan = expand(manifest, sha256, 0.5);
  const out = join(root, "run");
  const summary = await run(plan, cell, map, out, 5);
  expect(summary.phases_failed).toEqual([]);
  const u = (await usage(plan, cell, map, out, new Opts(new Map([["poll-secs", "2"], ["max-wait-secs", "360"]])))).projects as Record<string, any>;
  const a = (await auditAll(plan, cell, map, out, new Opts(new Map()))).projects as Record<string, any>;
  const classes = JSON.parse(readFileSync(join(out, "classes.json"), "utf8"));
  console.log(JSON.stringify(Object.fromEntries(Object.entries(classes).map(([k, v]: [string, any]) => [k, { refusals: v.refusals, acked: v.totals.acked_records, delivered: v.totals.delivered_records, oracle_ok: v.totals.oracle_ok ?? 0 }]))));
  for (const [cls, c] of Object.entries(classes) as Array<[string, any]>) {
    expect([cls, c.totals.oracle_foreign ?? 0, c.totals.oracle_bad_mac ?? 0, c.totals.oracle_unmarked ?? 0]).toEqual([cls, 0, 0, 0]);
    if (c.role === "hostile") continue;
    // 0 refusals, every code counted (shared_verdict.py's compliant bar).
    expect([cls, c.refusals.typed, c.refusals.untyped, c.refusals.transport]).toEqual([cls, {}, {}, 0]);
  }
  // Every hostile class at Q-hostile is refused, each refusal typed.
  for (const h of ["h1", "h2", "h4", "h6"]) {
    expect([h, Object.keys(classes[h].refusals.typed).some((k) => k.startsWith("429:")), classes[h].refusals.untyped])
      .toEqual([h, true, {}]);
  }
  // The hostile legs ran and saw what they report.
  const leg = (name: string) => JSON.parse(readFileSync(join(out, "legs", name), "utf8"));
  const h11t = leg("h11t-0.json");
  const probes = leg("probes-1.json");
  const fresh = leg("h11t-fresh-2.json");
  console.log(JSON.stringify({ h11t: { pairs: h11t.pairs, names: h11t.names, answers: h11t.answers, ks: h11t.ks, quiet_p50_ms: h11t.quiet_p50_ms, never_p50_ms: h11t.never_p50_ms },
    h11t_fresh: { pairs: fresh.pairs, answers: fresh.answers, ks: fresh.ks, fresh_p50_ms: fresh.fresh_p50_ms, never_p50_ms: fresh.never_p50_ms },
    probes: { pairs: probes.pairs, first_contacts: probes.first_contacts, differences: probes.differences, usage: probes.usage_answers, put: probes.put, examples: probes.examples } }));
  // 300 pairs over the 3 cold projects' 9 quiet names.
  expect([h11t.pairs, h11t.differences, h11t.names]).toEqual([300, 0, { projects: 3, names: 9 }]);
  // 100 pairs over the h11t-target's 8 names.
  expect([fresh.pairs, fresh.names]).toEqual([100, { projects: 1, names: 8 }]);
  // Every (prober, target name) once: 2 probers x 13 names in the tiny manifest.
  expect([probes.pairs, probes.first_contacts, probes.differences, probes.put]).toEqual([26, 26, { name: 0, usage: 0 }, { "201": 26 }]);
  const phase = (cls: string, i: number) => JSON.parse(readFileSync(join(out, "projects", plan.projects.find((p) => p.cls.class === cls)!.id, `phase-${i}.ledger.json`), "utf8"));
  const h9 = phase("h9", 2).h9;
  const h9b = phase("h9b", 2).h9b;
  const costAnswers = phase("cost", 0).cost.answers;
  console.log(JSON.stringify({ h9: h9.bursts.map((b: any) => [b.answers, b.payload_bytes, b.p99_ms, b.debt_clear_ms]), h9b, cost: costAnswers }));
  expect(h9.bursts.length).toBeGreaterThan(0);
  expect([h9b.statuses, h9b.cut_short]).toEqual([{ "200": 8 }, 0]);
  // Every kind of the cost mix was answered, every error answer with a code.
  for (const kind of ["catalog", "create", "missing", "refused"]) {
    const keys = Object.keys(costAnswers[kind]);
    expect([kind, keys.length > 0, keys.filter((k) => Number(k.split(":")[0]) >= 400 && k.endsWith(":-"))]).toEqual([kind, true, []]);
  }
  for (const p of plan.projects) {
    const e = u[p.id];
    const t = JSON.parse(readFileSync(join(out, "projects", p.id, "ledger.json"), "utf8")).totals;
    expect([p.id, e.settled, e.meters.ingestRecords, e.meters.readPayloadBytes, e.meters.readRecords])
      .toEqual([p.id, true, t.acked_records ?? 0, t.delivered_payload_bytes ?? 0, t.delivered_records ?? 0]);
  }
  for (const [id, x] of Object.entries(a)) {
    for (const [prefix, c] of Object.entries(x.prefixes) as Array<[string, any]>) {
      expect([id, prefix, c.ok, c.oracle?.foreign, c.oracle?.bad_mac]).toEqual([id, prefix, true, 0, 0]);
    }
  }
}, 600_000);
