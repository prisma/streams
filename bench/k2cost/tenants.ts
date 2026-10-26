#!/usr/bin/env bun
// tenants: the shared-cell harness's multi-project driver (deploy-q5
// design §3.2; shared-cells PLAN §4.1, validation.md §4.2-4.3). A tenants
// manifest (tenant-plan.ts) names tenant classes, their counts, quotas and
// phases; every project of a class runs its class's phases with k2gen's own
// modes (produce, walk, tail, group, churn, subs, setup), under its own
// token and record key, writing oracle records (oracle.ts).
//
//   bun bench/k2cost/tenants.ts plan  --manifest M [--time-scale F]
//   bun bench/k2cost/tenants.ts run   --manifest M --base URL --token-map F --out DIR
//                                     [--time-scale F] [--window 10]
//   bun bench/k2cost/tenants.ts usage --manifest M --base URL --token-map F --out DIR
//                                     [--max-wait-secs 330] [--poll-secs 5] [--parallel 16]
//   bun bench/k2cost/tenants.ts audit --manifest M --base URL --token-map F --out DIR [--parallel 8]
//
// The token map is a JSON list [{"project", "token"}] (tenants-admit.ts
// writes it; bench/soak/mtgen.mjs's shape). It is re-read every 10 min and
// soon after a 401, so a refresher can rotate the tokens under a run.
//
// `run` writes, under DIR:
//   run.json                        the manifest, its sha256, the window, counts
//   windows.jsonl                   one line per class per 10 s window: the
//                                   class's summed counters, latencies, error
//                                   codes (typed refusals) and connections
//   classes.json                    per class: totals, refusals, latency over
//                                   the run and over each manifest period
//   projects/<id>/phase-<i>.ledger.json   k2gen's ledger for each phase
//   projects/<id>/ledger.json       the project's totals and refusals
// `usage` reads every project's meters (GET /v1/projects/{p}/usage) after
// the drain until they settle and writes usage.json; `audit` reads every
// project's produced streams back against its produce ledgers and writes
// audit.json. Run usage before audit: the audit's reads are metered too.

import { mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  Counters, HeaderSource, Hist, Http, Meter, Opts, READ_BACK, type ReadBack, Reporter, ensureHttpHeadroom, installSignals, parseFlags, pool, round3,
  sleep, stop,
} from "./common.ts";
import { AuditReadError, audit } from "./audit.ts";
import { Pools } from "./conn-pool.ts";
import { churn } from "./churn.ts";
import { mux } from "./mux.ts";
import { group, subs, tail, walk } from "./consume.ts";
import { HOSTILE_LEGS, HOSTILE_MODES } from "./hostile.ts";
import { produce, setup, type Ctx } from "./produce.ts";
import { resolve } from "./resolve.ts";
import {
  type LegSpec, type PhaseSpec, type Plan, type Project, expand, phaseEnd, phaseFlags, phaseStart,
  readManifest,
} from "./tenant-plan.ts";

export const TOKEN_REREAD_MS = 600_000;
const TOKEN_RELOAD_MIN_MS = 2_000;
const REPEAT_PAUSE_MS = 1_000;

// ---------------------------------------------------------------- tokens

/** The token map, re-read every TOKEN_REREAD_MS and soon after a 401. */
export class TokenMap {
  private tokens = new Map<string, string>();
  private readAt = 0;
  private soon = false;
  reads = 0;
  failedReads = 0;
  constructor(readonly file: string, private readonly everyMs = TOKEN_REREAD_MS) {
    this.read(true);
  }
  private read(first: boolean): void {
    this.readAt = Date.now();
    this.soon = false;
    try {
      const list = JSON.parse(readFileSync(this.file, "utf8")) as Array<{ project: string; token: string }>;
      this.tokens = new Map(list.map((e) => [e.project, e.token]));
      this.reads++;
    } catch (e) {
      if (first) throw new Error(`--token-map ${this.file}: ${(e as Error).message}`);
      this.failedReads++;
    }
  }
  token(project: string): string | undefined {
    const age = Date.now() - this.readAt;
    if (age >= this.everyMs || (this.soon && age >= TOKEN_RELOAD_MIN_MS)) this.read(false);
    return this.tokens.get(project);
  }
  reloadSoon(): void {
    this.soon = true;
  }
}

/** One project's headers: its token from the map and its record key. */
export class ProjectHeaders extends HeaderSource {
  constructor(private readonly tokenMap: TokenMap, private readonly project: string, private readonly recordKey: string) {
    super(undefined, {});
  }
  override get(): Record<string, string> {
    const t = this.tokenMap.token(this.project);
    return { "streams-encryption-key": this.recordKey, ...(t ? { authorization: `Bearer ${t}` } : {}) };
  }
  override reloadSoon(): void {
    this.tokenMap.reloadSoon();
  }
  override key(): string {
    return this.recordKey;
  }
}

// ---------------------------------------------------------------- windows

export function mergeCounters(into: Counters, from: Counters): void {
  for (const [k, v] of from.f) into.add(k, v);
  for (const [k, v] of Object.entries(from.status)) into.status[k] = (into.status[k] ?? 0) + v;
  for (const [a, b] of [[into.append, from.append], [into.read, from.read]] as const) {
    for (let i = 0; i < b.counts.length; i++) a.counts[i] += b.counts[i]!;
    a.n += b.n;
    a.maxUs = Math.max(a.maxUs, b.maxUs);
  }
}

function addCodes(into: Record<string, number>, codes: Record<string, number>, minus: Record<string, number> = {}): void {
  for (const [k, v] of Object.entries(codes)) {
    const d = v - (minus[k] ?? 0);
    if (d) into[k] = (into[k] ?? 0) + d;
  }
}

/** Per-class 10 s windows over the phases' meters, and per-class latency
 * over each manifest period (a window counts in the period holding its
 * midpoint). */
export class ClassWindows {
  private live = new Map<string, Set<Meter>>();
  private seen = new Map<Meter, Record<string, number>>();
  private left = new Map<string, { c: Counters; codes: Record<string, number> }>();
  private start: number;
  readonly periods = new Map<string, Map<string, Counters>>();
  constructor(private readonly out: string, private readonly t0: number, private readonly spans: Array<[string, number, number]>) {
    this.start = Date.now();
    writeFileSync(out, "");
  }
  private leftover(cls: string) {
    let l = this.left.get(cls);
    if (!l) this.left.set(cls, (l = { c: new Counters(), codes: {} }));
    return l;
  }
  register(cls: string, m: Meter): void {
    let s = this.live.get(cls);
    if (!s) this.live.set(cls, (s = new Set()));
    s.add(m);
    this.seen.set(m, {});
  }
  retire(cls: string, m: Meter): void {
    const l = this.leftover(cls);
    mergeCounters(l.c, m.win);
    m.win = new Counters();
    addCodes(l.codes, m.errorCodes, this.seen.get(m));
    this.live.get(cls)?.delete(m);
    this.seen.delete(m);
  }
  flush(): void {
    const now = Date.now();
    const from = this.start;
    this.start = now;
    const mid = (from + now) / 2 - this.t0;
    const lines: string[] = [];
    for (const cls of [...new Set([...this.live.keys(), ...this.left.keys()])].sort()) {
      const acc = new Counters();
      const codes: Record<string, number> = {};
      let connected = 0, inflight = 0;
      const live = this.live.get(cls) ?? new Set<Meter>();
      for (const m of live) {
        mergeCounters(acc, m.win);
        m.win = new Counters();
        addCodes(codes, m.errorCodes, this.seen.get(m));
        this.seen.set(m, { ...m.errorCodes });
        connected += m.connected;
        inflight += m.inflight;
      }
      const l = this.left.get(cls);
      if (l) {
        mergeCounters(acc, l.c);
        addCodes(codes, l.codes);
        this.left.delete(cls);
      }
      for (const [name, a, b] of this.spans) {
        if (mid < a || mid >= b) continue;
        let byName = this.periods.get(cls);
        if (!byName) this.periods.set(cls, (byName = new Map()));
        let p = byName.get(name);
        if (!p) byName.set(name, (p = new Counters()));
        mergeCounters(p, acc);
      }
      lines.push(JSON.stringify({
        t: round3(now / 1000), elapsed_s: round3((now - this.t0) / 1000), window_s: round3((now - from) / 1000), class: cls,
        phases: live.size, ...acc.render(), error_codes: codes, connected, inflight,
      }));
    }
    if (lines.length) writeFileSync(this.out, lines.join("\n") + "\n", { flag: "a" });
  }
}

// ---------------------------------------------------------------- loop lag

/** How late the driver's own event loop runs: a timer due every
 * LOOP_TICK_MS records how far past its due time it fired. One process
 * drives every project, so a busy loop adds its lag to every latency the
 * run measures, the victim's p99 included: loop.jsonl has one line per
 * window and run.json the run's and each manifest period's percentiles,
 * which shared_verdict.py reads beside the victim's bar. */
export const LOOP_TICK_MS = 50;
export class LoopLag {
  private win = new Hist();
  private start = Date.now();
  private timer: ReturnType<typeof setTimeout> | undefined;
  readonly periods = new Map<string, Hist>();
  constructor(private readonly out: string, private readonly t0: number, private readonly spans: Array<[string, number, number]>) {
    writeFileSync(out, "");
  }
  run(): void {
    let due = performance.now() + LOOP_TICK_MS;
    const tick = () => {
      const now = performance.now();
      this.win.record(Math.max(0, now - due));
      due = now + LOOP_TICK_MS;
      this.timer = setTimeout(tick, LOOP_TICK_MS);
    };
    this.timer = setTimeout(tick, LOOP_TICK_MS);
  }
  stop(): void {
    clearTimeout(this.timer);
  }
  static render(h: Hist): Record<string, unknown> {
    return { lag_p50_ms: h.pct(0.5), lag_p99_ms: h.pct(0.99), lag_max_ms: h.n ? round3(h.maxUs / 1000) : null, ticks: h.n };
  }
  /** One window's line; the window counts in the periods holding its midpoint. */
  flush(): void {
    const now = Date.now();
    const [from, w] = [this.start, this.win];
    this.start = now;
    this.win = new Hist();
    const mid = (from + now) / 2 - this.t0;
    for (const [name, a, b] of this.spans) {
      if (mid < a || mid >= b) continue;
      let p = this.periods.get(name);
      if (!p) this.periods.set(name, (p = new Hist()));
      for (let i = 0; i < w.counts.length; i++) p.counts[i] += w.counts[i]!;
      p.n += w.n;
      p.maxUs = Math.max(p.maxUs, w.maxUs);
    }
    const line = { t: round3(now / 1000), elapsed_s: round3((now - this.t0) / 1000), window_s: round3((now - from) / 1000), ...LoopLag.render(w) };
    writeFileSync(this.out, JSON.stringify(line) + "\n", { flag: "a" });
  }
  summary(): Record<string, unknown> {
    return Object.fromEntries([...this.periods].map(([name, h]) => [name, LoopLag.render(h)]));
  }
}

/** Typed refusals ("status:code" with a code), untyped error answers and
 * transport failures. */
export function refusals(errorCodes: Record<string, number>, status: Record<string, number>) {
  const typed: Record<string, number> = {};
  const untyped: Record<string, number> = {};
  for (const [k, v] of Object.entries(errorCodes)) {
    const [st, code] = [k.slice(0, k.indexOf(":")), k.slice(k.indexOf(":") + 1)];
    if (code && code !== "-") typed[k] = v;
    else untyped[st] = (untyped[st] ?? 0) + v;
  }
  const transport = (status.err ?? 0) + (status.timeout ?? 0);
  return { typed, untyped, transport };
}

// ---------------------------------------------------------------- modes

export type Mode = (ctx: Ctx, signal: AbortSignal) => Promise<Record<string, unknown>>;
export const MODES: Record<string, Mode> = {
  setup: async (ctx) => ({ setup: await setup(ctx) }),
  produce,
  walk,
  tail,
  group,
  churn,
  subs,
  mux,
  idle: async (_ctx, signal) => {
    await sleep(7 * 86_400_000, signal);
    return {};
  },
  ...HOSTILE_MODES,
};

/** A run-level leg (hostile.ts): it gets the run's environment, and its
 * result is written to legs/<leg>-<i>.json. */
export type Leg = (env: RunEnv, leg: LegSpec, signal: AbortSignal) => Promise<Record<string, unknown>>;
export const LEGS: Record<string, Leg> = { ...HOSTILE_LEGS };

/** Sockets of each class's keep-alive pool (conn-pool.ts), shared by the
 * class's projects; each run-level leg has a pool of its own. Twice the
 * cell's per-project in-flight ceiling (ADMIT_MAX_INFLIGHT 512 / k 8 = 64),
 * so a class at its ceilings can still be refused for in-flight, and the
 * manifest's widest burst (h9's 64 readers) never waits; 37 classes at
 * 128 hold at most 4,736 sockets, a third of macOS's 16,383 ports. A
 * class's manifest `pool` replaces it for that class: a packing class of
 * hundreds of projects holds a long-poll (and pulls) per project at once. */
export const CLASS_POOL_SOCKETS = 128;

export interface RunEnv {
  plan: Plan;
  base: string;
  out: string;
  t0: number;
  tokens: TokenMap;
  headers: Map<string, ProjectHeaders>;
  windows: ClassWindows;
  /** Keep-alive pools by class, and by "leg:<leg>" for run-level legs. */
  pools: Pools;
}

// ---------------------------------------------------------------- run

interface PhaseRun {
  i: number;
  meter: Meter;
  ledger: Record<string, unknown>;
}

async function runPhase(env: RunEnv, p: Project, i: number, ph: PhaseSpec): Promise<PhaseRun | null> {
  const startMs = env.t0 + phaseStart(env.plan, p, ph) * 1000;
  const endMs = env.t0 + phaseEnd(env.plan, p, ph) * 1000;
  await sleep(startMs - Date.now(), stop.signal);
  if (stop.signal.aborted) return null;
  const meter = new Meter();
  const flags = phaseFlags(p, ph);
  const o = new Opts(flags);
  const ctx: Ctx = {
    http: new Http(env.base, env.headers.get(p.id)!, meter, env.pools.get(p.cls.class, p.cls.pool)), meter, o, prefix: flags.get("prefix")!,
    streams: Number(flags.get("streams")), seed: p.seed + i * 7919, duration: (endMs - startMs) / 1000, owner: p.owner,
  };
  const rep = new Reporter(ph.mode, meter, undefined, undefined, 10, o.asObject(), undefined);
  const signal = AbortSignal.any([stop.signal, AbortSignal.timeout(Math.max(1, endMs - Date.now()))]);
  env.windows.register(p.cls.class, meter);
  let result: Record<string, unknown> = {};
  let runs = 0;
  let error: string | null = null;
  try {
    do {
      if (runs > 0) await sleep(REPEAT_PAUSE_MS, signal);
      result = await MODES[ph.mode]!(ctx, signal);
      runs++;
    } while (ph.repeat && !signal.aborted);
  } catch (e) {
    error = (e as Error).message;
  }
  env.windows.retire(p.cls.class, meter);
  const ledger = {
    ...rep.totals(true), base: env.base, prefix: ctx.prefix, streams: ctx.streams, seed: ctx.seed,
    project: p.id, class: p.cls.class, phase: i, runs, error, stopped_by_signal: stop.signal.aborted, ...result,
  };
  writeFileSync(join(env.out, "projects", p.id, `phase-${i}.ledger.json`), JSON.stringify(ledger, null, 1) + "\n");
  return { i, meter, ledger };
}

export function projectLedger(p: Project, runs: PhaseRun[]): Record<string, unknown> {
  const tot = new Counters();
  const codes: Record<string, number> = {};
  for (const r of runs) {
    mergeCounters(tot, r.meter.tot);
    addCodes(codes, r.meter.errorCodes);
  }
  const totals = tot.render();
  return {
    project: p.id, workspace: p.workspace, class: p.cls.class, role: p.cls.role, quotas: p.cls.quotas ?? "ceilings",
    phases: runs.map((r) => ({
      i: r.i, mode: r.ledger.mode, start_ms: r.ledger.start_ms, end_ms: r.ledger.end_ms, runs: r.ledger.runs,
      error: r.ledger.error, acked_records: r.ledger.acked_records, delivered_payload_bytes: r.ledger.delivered_payload_bytes,
      ...(r.ledger.verify ? { verify: r.ledger.verify } : {}),
    })),
    totals, error_codes: codes, refusals: refusals(codes, totals.status_counts as Record<string, number>),
  };
}

async function runProject(env: RunEnv, p: Project): Promise<Record<string, unknown>> {
  mkdirSync(join(env.out, "projects", p.id), { recursive: true });
  const runs = (await Promise.all(p.cls.phases.map((ph, i) => runPhase(env, p, i, ph)))).filter((r): r is PhaseRun => r !== null);
  const ledger = projectLedger(p, runs);
  writeFileSync(join(env.out, "projects", p.id, "ledger.json"), JSON.stringify(ledger, null, 1) + "\n");
  return ledger;
}

async function runLeg(env: RunEnv, leg: LegSpec, i: number): Promise<void> {
  const startMs = env.t0 + leg.at * env.plan.scale * 1000;
  const endMs = env.t0 + Math.min(env.plan.manifest.duration, leg.at + (leg.secs ?? env.plan.manifest.duration)) * env.plan.scale * 1000;
  await sleep(startMs - Date.now(), stop.signal);
  if (stop.signal.aborted) return;
  const signal = AbortSignal.any([stop.signal, AbortSignal.timeout(Math.max(1, endMs - Date.now()))]);
  let result: Record<string, unknown>;
  try {
    result = await LEGS[leg.leg]!(env, leg, signal);
  } catch (e) {
    result = { error: (e as Error).message };
  }
  mkdirSync(join(env.out, "legs"), { recursive: true });
  writeFileSync(join(env.out, "legs", `${leg.leg}-${i}.json`), JSON.stringify({ leg: leg.leg, at: leg.at, args: leg.args ?? {}, ...result }, null, 1) + "\n");
}

export function classSummary(plan: Plan, ledgers: Array<Record<string, unknown>>, windows: ClassWindows) {
  const out: Record<string, unknown> = {};
  for (const cls of plan.manifest.classes) {
    const tot = new Counters();
    const codes: Record<string, number> = {};
    for (const l of ledgers.filter((x) => x.class === cls.class)) {
      const t = l.totals as Record<string, unknown>;
      for (const k of Object.keys(t)) if (typeof t[k] === "number" && !k.endsWith("_ms")) tot.add(k, t[k] as number);
      for (const [k, v] of Object.entries(t.status_counts as Record<string, number>)) tot.status[k] = (tot.status[k] ?? 0) + v;
      addCodes(codes, l.error_codes as Record<string, number>);
    }
    const totals = tot.render();
    for (const k of ["append_p50_ms", "append_p99_ms", "read_p50_ms", "read_p99_ms"]) delete totals[k];
    const periods: Record<string, unknown> = {};
    for (const [name, c] of windows.periods.get(cls.class) ?? []) {
      periods[name] = {
        append_p50_ms: c.append.pct(0.5), append_p99_ms: c.append.pct(0.99), append_samples: c.append.n,
        read_p50_ms: c.read.pct(0.5), read_p99_ms: c.read.pct(0.99), read_samples: c.read.n,
      };
    }
    out[cls.class] = {
      role: cls.role, count: cls.count, quotas: cls.quotas ?? "ceilings", totals, error_codes: codes,
      refusals: refusals(codes, totals.status_counts as Record<string, number>), periods,
    };
  }
  return out;
}

export async function run(plan: Plan, base: string, tokenMap: string, out: string, windowSecs = 10): Promise<Record<string, unknown>> {
  for (const leg of plan.manifest.legs ?? []) if (!LEGS[leg.leg]) throw new Error(`manifest: unknown leg ${leg.leg}`);
  mkdirSync(join(out, "projects"), { recursive: true });
  const tokens = new TokenMap(tokenMap);
  const t0 = Date.now();
  // Every class's latency over the whole run, and over each manifest period.
  const spans: Array<[string, number, number]> = [["run", 0, Infinity]];
  for (const [n, [a, b]] of Object.entries(plan.manifest.periods ?? {})) spans.push([n, a * plan.scale * 1000, b * plan.scale * 1000]);
  const env: RunEnv = {
    plan, base, out, t0, tokens, windows: new ClassWindows(join(out, "windows.jsonl"), t0, spans),
    headers: new Map(plan.projects.map((p) => [p.id, new ProjectHeaders(tokens, p.id, p.recordKey)])),
    pools: new Pools(CLASS_POOL_SOCKETS),
  };
  const head = {
    manifest: plan.manifest.name, manifest_sha256: plan.sha256, base, time_scale: plan.scale, started_ms: t0,
    projects: plan.projects.length, duration_s: plan.manifest.duration * plan.scale, pool_sockets: CLASS_POOL_SOCKETS,
    classes: Object.fromEntries(plan.manifest.classes.map((c) => [c.class, { count: c.count, role: c.role, quotas: c.quotas ?? "ceilings" }])),
  };
  writeFileSync(join(out, "run.json"), JSON.stringify(head, null, 1) + "\n");
  const loop = new LoopLag(join(out, "loop.jsonl"), t0, spans);
  loop.run();
  const timer = setInterval(() => (env.windows.flush(), loop.flush()), windowSecs * 1000);
  const [ledgers] = await Promise.all([
    Promise.all(plan.projects.map((p) => runProject(env, p))),
    Promise.all((plan.manifest.legs ?? []).map((leg, i) => runLeg(env, leg, i))),
  ]);
  clearInterval(timer);
  loop.stop();
  env.windows.flush();
  loop.flush();
  writeFileSync(join(out, "classes.json"), JSON.stringify(classSummary(plan, ledgers, env.windows), null, 1) + "\n");
  const summary = {
    ...head, finished_ms: Date.now(), stopped_by_signal: stop.signal.aborted, loop_lag: loop.summary(),
    pools: env.pools.summary(),
    token_map: { file: tokenMap, reads: tokens.reads, failed_reads: tokens.failedReads },
    phases_failed: ledgers.flatMap((l) => (l.phases as Array<Record<string, unknown>>).filter((ph) => ph.error).map((ph) => `${l.project}#${ph.i}: ${ph.error}`)),
  };
  writeFileSync(join(out, "run.json"), JSON.stringify(summary, null, 1) + "\n");
  return summary;
}

// ---------------------------------------------------------------- usage

const METERS = ["ingestRecords", "ingestPayloadBytes", "readRecords", "readPayloadBytes"] as const;

function readLedger(out: string, p: Project): Record<string, unknown> | null {
  try {
    return JSON.parse(readFileSync(join(out, "projects", p.id, "ledger.json"), "utf8"));
  } catch {
    return null;
  }
}

/** Every project's meters, polled until they reach its ledger and two
 * polls agree, or until the wait runs out. */
export async function usage(plan: Plan, base: string, tokenMap: string, out: string, o: Opts): Promise<Record<string, unknown>> {
  const tokens = new TokenMap(tokenMap);
  const maxWaitMs = o.num("max-wait-secs", 330) * 1000;
  const pollMs = o.num("poll-secs", 5) * 1000;
  const projects: Record<string, unknown> = {};
  const deadline = Date.now() + maxWaitMs;
  await pool(plan.projects.length, o.int("parallel", 16), async (i) => {
    const p = plan.projects[i]!;
    const t = (readLedger(out, p)?.totals ?? {}) as Record<string, number>;
    const expected = { ingestRecords: t.acked_records ?? 0, readPayloadBytes: t.delivered_payload_bytes ?? 0, readRecords: t.delivered_records ?? 0 };
    const http = new Http(base, new ProjectHeaders(tokens, p.id, p.recordKey), new Meter());
    let prev = "";
    let polls = 0;
    let settled = false;
    let last: { status: number; body: Record<string, unknown> | null } = { status: 0, body: null };
    for (;;) {
      polls++;
      const r = await http.call("GET", `/v1/projects/${encodeURIComponent(p.id)}/usage`);
      let body: Record<string, unknown> | null = null;
      try {
        body = JSON.parse(new TextDecoder().decode(r.body));
      } catch {
        body = null;
      }
      last = { status: r.status, body };
      if (r.status === 200 && body) {
        const eff = ((body.effective as Record<string, unknown>) ?? body) as Record<string, number>;
        const cur = JSON.stringify(METERS.map((k) => eff[k] ?? 0));
        if (expected.ingestRecords <= (eff.ingestRecords ?? 0) && expected.readPayloadBytes <= (eff.readPayloadBytes ?? 0) && cur === prev) {
          settled = true;
          break;
        }
        prev = cur;
      }
      if (Date.now() + pollMs > deadline || stop.signal.aborted) break;
      await sleep(pollMs, stop.signal);
    }
    const eff = ((last.body?.effective as Record<string, unknown>) ?? last.body ?? {}) as Record<string, number>;
    projects[p.id] = {
      class: p.cls.class, status: last.status, settled, polls, expected,
      meters: Object.fromEntries(METERS.map((k) => [k, eff[k] ?? null])),
    };
  }, stop.signal);
  const doc = { t_ms: Date.now(), manifest: plan.manifest.name, projects };
  writeFileSync(join(out, "usage.json"), JSON.stringify(doc, null, 1) + "\n");
  return doc;
}

// ---------------------------------------------------------------- audit

/** The names a project's setup phases on `prefix` recorded as created
 * (setup's `names`), or undefined when no setup phase writes that prefix
 * (the audit then reads every name). A setup ledger that is missing
 * records none. */
export function heldNames(out: string, p: Project, prefix: string): Set<string> | undefined {
  let held: Set<string> | undefined;
  p.cls.phases.forEach((ph, j) => {
    if (ph.mode !== "setup" || phaseFlags(p, ph).get("prefix") !== prefix) return;
    held ??= new Set();
    try {
      const ledger = JSON.parse(readFileSync(join(out, "projects", p.id, `phase-${j}.ledger.json`), "utf8"));
      for (const n of (ledger.setup?.names ?? []) as string[]) held.add(n);
    } catch {
      // never ran or never ended: it recorded nothing
    }
  });
  return held;
}

/** Every project's produced streams read back against its produce
 * ledgers (audit.ts), one audit per stream prefix, reading the names its
 * setup created; and its outcome-unknown appends resolved by read-back
 * (resolve.ts), as `resolution`. */
export async function auditAll(plan: Plan, base: string, tokenMap: string, out: string, o: Opts, rb: ReadBack = READ_BACK): Promise<Record<string, unknown>> {
  const tokens = new TokenMap(tokenMap);
  const projects: Record<string, unknown> = {};
  const audited = new Map<string, { ledgers: Array<Record<string, unknown>>; read: Map<string, number> }>();
  await pool(plan.projects.length, o.int("parallel", 8), async (i) => {
    const p = plan.projects[i]!;
    const byPrefix = new Map<string, { files: string[]; streams: number }>();
    p.cls.phases.forEach((ph, j) => {
      if (ph.mode !== "produce") return;
      const file = join(out, "projects", p.id, `phase-${j}.ledger.json`);
      try {
        statSync(file);
      } catch {
        return;
      }
      const f = phaseFlags(p, ph);
      const e = byPrefix.get(f.get("prefix")!) ?? { files: [], streams: 0 };
      e.files.push(file);
      e.streams = Math.max(e.streams, Number(f.get("streams")));
      byPrefix.set(f.get("prefix")!, e);
    });
    const checks: Record<string, unknown> = {};
    const read = new Map<string, number>();
    for (const [prefix, e] of byPrefix) {
      const meter = new Meter();
      const ctx: Ctx = {
        http: new Http(base, new ProjectHeaders(tokens, p.id, p.recordKey), meter), meter,
        o: new Opts(new Map([["expect", e.files.join(",")], ["parallel", "2"]])), prefix, streams: e.streams,
        seed: p.seed, duration: 0, owner: p.owner,
      };
      try {
        const v = (await audit(ctx, stop.signal, heldNames(out, p, prefix), rb)).verify as Record<string, unknown>;
        const { per_stream: ps, ...rest } = v;
        for (const c of (ps ?? []) as Array<{ name: string; pages: number; read_records: number }>) if (c.pages > 0) read.set(c.name, c.read_records);
        checks[prefix] = { ...rest, oracle: Object.fromEntries(["ok", "foreign", "bad_mac", "unmarked"].map((k) => [k, meter.tot.get(`oracle_${k}`)])) };
      } catch (err) {
        const cause = err instanceof AuditReadError ? `read_failed ${err.cls}` : "error";
        checks[prefix] = { ok: false, error: (err as Error).message, causes: [{ cause, detail: (err as Error).message }] };
      }
    }
    // The ledgers of the appends answered with an unknown outcome, settled
    // by read-back (resolve.ts) for the billing bar once every project is
    // audited.
    const ledgers = p.cls.phases.flatMap((_, j) => {
      try {
        return [JSON.parse(readFileSync(join(out, "projects", p.id, `phase-${j}.ledger.json`), "utf8")) as Record<string, unknown>];
      } catch {
        return [];
      }
    });
    audited.set(p.id, { ledgers, read });
    if (byPrefix.size) projects[p.id] = { class: p.cls.class, prefixes: checks };
  }, stop.signal);
  // The read-back runs after every project's audit reads, when the cell is
  // no longer read by them, and asks again for as long as the cell refuses
  // it (resolve.ts READ_BACK).
  await pool(plan.projects.length, o.int("resolve-parallel", 4), async (i) => {
    const p = plan.projects[i]!;
    const a = audited.get(p.id);
    if (!a) return;
    const resolution = await resolve(new Http(base, new ProjectHeaders(tokens, p.id, p.recordKey), new Meter()), a.ledgers, a.read, rb);
    if (resolution) projects[p.id] = { ...(projects[p.id] ?? { class: p.cls.class, prefixes: {} }), resolution };
  }, stop.signal);
  const doc = { t_ms: Date.now(), manifest: plan.manifest.name, projects };
  writeFileSync(join(out, "audit.json"), JSON.stringify(doc, null, 1) + "\n");
  return doc;
}

// ---------------------------------------------------------------- plan

export function describe(plan: Plan): Record<string, unknown> {
  return {
    manifest: plan.manifest.name, sha256: plan.sha256, time_scale: plan.scale, projects: plan.projects.length,
    duration_s: plan.manifest.duration * plan.scale,
    classes: plan.manifest.classes.map((c) => {
      const ps = plan.projects.filter((p) => p.cls === c);
      return {
        class: c.class, role: c.role, count: c.count, quotas: c.quotas ?? "ceilings", first: ps[0]!.id, last: ps[ps.length - 1]!.id,
        phases: c.phases.map((ph) => ({ mode: ph.mode, start_s: phaseStart(plan, ps[0]!, ph), end_s: phaseEnd(plan, ps[0]!, ph), args: Object.fromEntries(phaseFlags(ps[0]!, ph)) })),
      };
    }),
    legs: plan.manifest.legs ?? [],
  };
}

// ---------------------------------------------------------------- main

const USAGE = `usage: tenants.ts <plan|run|usage|audit> --manifest M [flags]
  plan   [--time-scale F]
  run    --base URL --token-map F --out DIR [--time-scale F] [--window 10]
  usage  --base URL --token-map F --out DIR [--max-wait-secs 330] [--poll-secs 5] [--parallel 16]
  audit  --base URL --token-map F --out DIR [--parallel 8]
`;

export async function main(argv: string[]): Promise<number> {
  const { positional, flags } = parseFlags(argv);
  const cmd = positional[0] ?? "";
  if (!["plan", "run", "usage", "audit"].includes(cmd) || positional.length > 1 || !flags.has("manifest")) {
    process.stderr.write(USAGE);
    return 2;
  }
  const o = new Opts(flags);
  const { manifest, sha256 } = readManifest(o.str("manifest")!);
  const plan = expand(manifest, sha256, o.num("time-scale", 1));
  if (cmd === "plan") {
    process.stdout.write(JSON.stringify(describe(plan), null, 1) + "\n");
    return 0;
  }
  const need = (k: string) => o.str(k) ?? (() => { throw new Error(`${cmd} needs --${k}`); })();
  const base = need("base").replace(/\/+$/, "");
  const [tokenMap, out] = [need("token-map"), need("out")];
  mkdirSync(out, { recursive: true });
  if (cmd === "run") {
    const s = await run(plan, base, tokenMap, out, o.num("window", 10));
    process.stdout.write(JSON.stringify({ run: out, projects: s.projects, phases_failed: (s.phases_failed as string[]).length }) + "\n");
  } else if (cmd === "usage") {
    const d = await usage(plan, base, tokenMap, out, o);
    const ps = Object.values(d.projects as Record<string, { settled: boolean }>);
    process.stdout.write(JSON.stringify({ usage: join(out, "usage.json"), projects: ps.length, settled: ps.filter((x) => x.settled).length }) + "\n");
  } else {
    const d = await auditAll(plan, base, tokenMap, out, o);
    const ps = Object.values(d.projects as Record<string, { prefixes: Record<string, { ok: boolean }> }>);
    const bad = ps.filter((x) => Object.values(x.prefixes).some((c) => !c.ok)).length;
    process.stdout.write(JSON.stringify({ audit: join(out, "audit.json"), projects: ps.length, failed: bad }) + "\n");
  }
  return 0;
}

if (import.meta.main) {
  await ensureHttpHeadroom();
  installSignals();
  try {
    process.exit(await main(process.argv.slice(2)));
  } catch (e) {
    process.stderr.write(`tenants: ${(e as Error).message}\n`);
    process.exit(2);
  }
}

