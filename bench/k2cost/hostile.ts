// The hostile legs of the shared-cell harness (deploy-q5 design §3.2;
// shared-cells PLAN §4.1 "New hostile legs", review-isolation.md H11t),
// run by tenants.ts: three phase modes a class runs per project, and two
// run-level legs.
//
//   h9      64 concurrent 8 MiB reads from the beginning of the project's
//           streams (consume.ts's reader, so the oracle checks them), then
//           a cheap read every --probe-ms until the read debt clears
//           (no 429), then again, until the phase ends. Per burst: statuses
//           and codes, bytes, latency, and how long the debt took to clear.
//   h9b     readers that never drain: --readers raw HTTP/1.1 GETs of an
//           8 MiB page each, read to the end of their headers and then
//           paused for --hold-secs, so the server's response buffers stay
//           full; then resumed and read to the end. Per reader: status,
//           bytes before the stall, a reset during the hold, a body cut
//           short. Bodies read in full are counted as delivered (oracle
//           too), so the project's meter still equals its ledger.
//   cost    the cost-hostile mix at --rate requests/s: catalog reads,
//           creates of new names, reads of names that never existed, and
//           a usage read of a project not its own (a refused request).
//   leg h11t      >= --pairs timed pairs on the anonymous capability path
//                 (GET …/watches/w/keys/<16 hex>?cap=<B>.x): a quiet name
//                 of a --target project (created by its setup and touched
//                 by nothing since, so past the cell's 5 s descriptor TTL)
//                 against a name that never existed, in random order, at
//                 most --rate pairs a second with no catch-up after a
//                 stall. The quiet names are the ones the target's setup
//                 ledgers record as created; none fails the leg. Status
//                 and code must be identical; the time distributions are
//                 compared by a two-sample KS test (shared_verdict.py
//                 recomputes it).
//   leg probes    >= --pairs cross-project probes, each a first contact:
//                 every (prober A, target name) once, A from a class with
//                 no streams, the name one some B of the --to classes holds.
//                 A reads B's stream and a name nobody has (status, code,
//                 header names and body must match once the name is
//                 masked), reads B's usage and a never-placed project's
//                 (same), and then creates B's name in its own namespace
//                 (201) and deletes it; A never reads that name again, so
//                 no later probe meets a name A itself created.

import { readFileSync } from "node:fs";
import { connect as netConnect, type Socket } from "node:net";
import { join } from "node:path";
import { connect as tlsConnect } from "node:tls";
import { Http, Meter, Rng, type Resp, encName, errCode, round3, sleep, streamName, utf8 } from "./common.ts";
import { countOracle } from "./oracle.ts";
import type { Ctx } from "./produce.ts";
import type { LegSpec, Project } from "./tenant-plan.ts";
import type { RunEnv } from "./tenants.ts";

const MAX_PAGE = 8 << 20;

const pct = (xs: number[], p: number): number | null => {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  return Math.round(s[Math.min(s.length - 1, Math.max(0, Math.ceil(p * s.length) - 1))]! * 1000) / 1000;
};
const bump = (m: Record<string, number>, k: string) => void (m[k] = (m[k] ?? 0) + 1);
const answer = (r: Resp) => `${r.status}:${errCode(r) || "-"}`;

/** A page's records: JSON array elements and their payload bytes, counted
 * on the meter like consume.ts's reader does. */
function deliver(ctx: Ctx, stream: string, body: Uint8Array): { records: number; bytes: number } {
  let arr: unknown[] = [];
  try {
    arr = JSON.parse(utf8(body)) as unknown[];
  } catch {
    ctx.meter.add("bad_json");
    // Delivered as no record of this project's: one corrupt delivery.
    if (ctx.owner) ctx.meter.add("oracle_unmarked");
  }
  const bytes = arr.length ? body.byteLength - 2 - (arr.length - 1) : 0;
  if (ctx.owner) countOracle(ctx.meter, ctx.owner, arr);
  if (arr.length) {
    ctx.meter.add("delivered_records", arr.length);
    ctx.meter.add("delivered_payload_bytes", bytes);
    const t = ctx.meter.tally(stream);
    t.delivered_records += arr.length;
    t.delivered_bytes += bytes;
  }
  return { records: arr.length, bytes };
}

// ---------------------------------------------------------------- h9

export async function h9(ctx: Ctx, signal: AbortSignal): Promise<Record<string, unknown>> {
  const readers = ctx.o.int("readers", 64);
  const maxBytes = ctx.o.int("max-bytes", MAX_PAGE);
  const probeMs = ctx.o.int("probe-ms", 500);
  const names = Array.from({ length: ctx.streams }, (_, i) => streamName(ctx.prefix, i));
  const bursts: Array<Record<string, unknown>> = [];
  while (!signal.aborted) {
    const t0 = Date.now();
    const answers: Record<string, number> = {};
    const ms: number[] = [];
    let bytes = 0;
    await Promise.all(Array.from({ length: readers }, async (_, i) => {
      const name = names[i % names.length]!;
      // Not cancelled at the deadline: the server meters a page it served,
      // so a burst always finishes and its reads are always counted.
      const r = await ctx.http.call("GET", `/v1/streams/${encName(name)}/records?cursor=beginning&maxBytes=${maxBytes}`, { timeoutMs: 300_000 });
      bump(answers, answer(r));
      ms.push(r.ms);
      if (r.status === 200) {
        ctx.meter.readMs(r.ms);
        bytes += deliver(ctx, name, r.body).bytes;
      }
    }));
    // The debt clears when a cheap read is served again.
    const tDebt = Date.now();
    let probes = 0;
    while (!signal.aborted) {
      probes++;
      const r = await ctx.http.call("GET", `/v1/streams/${encName(names[0]!)}/records?cursor=now`, { signal });
      if (r.status !== 429) break;
      await sleep(probeMs, signal);
    }
    bursts.push({
      t_ms: t0, readers, answers, payload_bytes: bytes, p50_ms: pct(ms, 0.5), p99_ms: pct(ms, 0.99),
      max_ms: pct(ms, 1), debt_clear_ms: signal.aborted ? null : Date.now() - tDebt, debt_probes: probes,
    });
  }
  return { h9: { readers, max_bytes: maxBytes, bursts } };
}

// ---------------------------------------------------------------- h9b

interface Stalled {
  status: number;
  bytes_before_resume: number;
  bytes_total: number;
  /** The connection failed (a reset) while the reader was stalled. */
  reset_during_hold: boolean;
  /** The server had sent everything and closed before the stall ended:
   * the socket buffers took the whole response. */
  closed_during_hold: boolean;
  complete: boolean;
  records: number;
  error: string | null;
}

/** The body of a raw response: content-length or chunked framing; null
 * when the bytes stop before the framing says the body ends. */
export function bodyOf(raw: Buffer): { status: number; body: Buffer | null } {
  const end = raw.indexOf("\r\n\r\n");
  if (end < 0) return { status: 0, body: null };
  const head = raw.subarray(0, end).toString("latin1").split("\r\n");
  const status = Number(head[0]!.split(" ")[1] ?? 0);
  const hdr = new Map(head.slice(1).map((l) => [l.slice(0, l.indexOf(":")).trim().toLowerCase(), l.slice(l.indexOf(":") + 1).trim()]));
  let rest = raw.subarray(end + 4);
  if (hdr.get("transfer-encoding")?.toLowerCase().includes("chunked")) {
    const parts: Buffer[] = [];
    for (;;) {
      const nl = rest.indexOf("\r\n");
      if (nl < 0) return { status, body: null };
      const size = parseInt(rest.subarray(0, nl).toString("latin1"), 16);
      if (!Number.isFinite(size)) return { status, body: null };
      if (size === 0) return { status, body: Buffer.concat(parts) };
      if (rest.length < nl + 2 + size + 2) return { status, body: null };
      parts.push(rest.subarray(nl + 2, nl + 2 + size));
      rest = rest.subarray(nl + 2 + size + 2);
    }
  }
  const len = Number(hdr.get("content-length") ?? NaN);
  if (!Number.isFinite(len)) return { status, body: rest };
  return { status, body: rest.length >= len ? rest.subarray(0, len) : null };
}

function stalledRead(base: URL, path: string, headers: Record<string, string>, holdMs: number, signal: AbortSignal): Promise<{ s: Stalled; raw: Buffer }> {
  return new Promise((resolve) => {
    const tls = base.protocol === "https:";
    const port = Number(base.port || (tls ? 443 : 80));
    const sock: Socket = tls ? tlsConnect({ host: base.hostname, port, servername: base.hostname }) : netConnect({ host: base.hostname, port });
    const chunks: Buffer[] = [];
    let got = 0;
    let headDone = false;
    let resumed = false;
    let before = 0;
    let reset = false;
    let closed = false;
    let closedEarly = false;
    let error: string | null = null;
    let finished = false;
    const finish = () => {
      if (finished) return;
      finished = true;
      if (drainTimer) clearTimeout(drainTimer);
      const raw = Buffer.concat(chunks);
      const { status, body } = bodyOf(raw);
      resolve({
        s: { status, bytes_before_resume: before, bytes_total: got, reset_during_hold: reset, closed_during_hold: closedEarly, complete: body !== null, records: 0, error },
        raw,
      });
    };
    let drainTimer: ReturnType<typeof setTimeout> | undefined;
    const resume = () => {
      if (resumed) return;
      resumed = true;
      before = got;
      if (closed) return finish();
      drainTimer = setTimeout(() => (sock.destroy(), finish()), 300_000);
      sock.resume();
    };
    sock.on("data", (d: Buffer) => {
      chunks.push(d);
      got += d.length;
      if (!headDone && Buffer.concat(chunks).includes("\r\n\r\n")) {
        headDone = true;
        if (!resumed) sock.pause();
      }
    });
    sock.on("error", (e) => {
      error = String((e as { code?: string }).code ?? e);
      if (!resumed) reset = true;
    });
    sock.on("close", () => {
      closed = true;
      if (!resumed) closedEarly = true;
      else finish();
    });
    sock.once(tls ? "secureConnect" : "connect", () => {
      const lines = [`GET ${path} HTTP/1.1`, `host: ${base.host}`, "connection: close", ...Object.entries(headers).map(([k, v]) => `${k}: ${v}`)];
      sock.write(lines.join("\r\n") + "\r\n\r\n");
    });
    const timer = setTimeout(resume, holdMs);
    signal.addEventListener("abort", () => (clearTimeout(timer), resume()), { once: true });
  });
}

export async function h9b(ctx: Ctx, signal: AbortSignal): Promise<Record<string, unknown>> {
  const readers = ctx.o.int("readers", 64);
  const holdMs = ctx.o.num("hold-secs", 120) * 1000;
  const maxBytes = ctx.o.int("max-bytes", MAX_PAGE);
  const base = new URL(ctx.http.base);
  const names = Array.from({ length: ctx.streams }, (_, i) => streamName(ctx.prefix, i));
  const headers = ctx.http.hdrs.get();
  const t0 = Date.now();
  const results = await Promise.all(Array.from({ length: readers }, async (_, i) => {
    const name = names[i % names.length]!;
    ctx.meter.add("requests");
    const { s, raw } = await stalledRead(base, `/v1/streams/${encName(name)}/records?cursor=beginning&maxBytes=${maxBytes}`, headers, holdMs, signal);
    ctx.meter.status(s.status ? String(s.status) : "err");
    ctx.meter.add("wire_bytes_down", s.bytes_total);
    const { body } = bodyOf(raw);
    if (s.status >= 400 && body) ctx.meter.errorCode(`${s.status}:${errCode({ status: s.status, headers: null, body, ms: 0 }) || "-"}`);
    if (s.status === 200 && body) s.records = deliver(ctx, name, body).records;
    return s;
  }));
  const statuses: Record<string, number> = {};
  for (const r of results) bump(statuses, String(r.status));
  return {
    h9b: {
      readers, hold_ms: holdMs, held_ms: Date.now() - t0, statuses,
      reset_during_hold: results.filter((r) => r.reset_during_hold).length,
      closed_during_hold: results.filter((r) => r.closed_during_hold).length,
      cut_short: results.filter((r) => r.status === 200 && !r.complete).length,
      bytes_before_resume: results.reduce((a, r) => a + r.bytes_before_resume, 0),
      bytes_total: results.reduce((a, r) => a + r.bytes_total, 0),
      errors: results.filter((r) => r.error).map((r) => r.error).slice(0, 5),
    },
  };
}

// ---------------------------------------------------------------- cost

const COST_KINDS = ["catalog", "create", "missing", "refused"] as const;

export async function cost(ctx: Ctx, signal: AbortSignal): Promise<Record<string, unknown>> {
  const rate = ctx.o.num("rate", 10);
  const answers: Record<string, Record<string, number>> = Object.fromEntries(COST_KINDS.map((k) => [k, {}]));
  const t0 = performance.now();
  const inflight = new Set<Promise<void>>();
  for (let i = 0; !signal.aborted; i++) {
    const due = t0 + (i * 1000) / rate;
    await sleep(due - performance.now(), signal);
    if (signal.aborted) break;
    const kind = COST_KINDS[i % COST_KINDS.length]!;
    const send = async () => {
      let r: Resp;
      if (kind === "catalog") r = await ctx.http.call("GET", "/v1/streams");
      else if (kind === "create") {
        r = await ctx.http.call("PUT", `/v1/streams/${encName(`x/${ctx.seed}-${i}`)}`, { body: '{"format":{"kind":"json"}}', contentType: "application/json" });
      } else if (kind === "missing") r = await ctx.http.call("GET", `/v1/streams/${encName(`never/${ctx.seed}-${i}`)}/records?cursor=beginning`);
      else r = await ctx.http.call("GET", `/v1/projects/${encodeURIComponent(`${ctx.owner?.project ?? "p"}_other`)}/usage`);
      bump(answers[kind]!, answer(r));
    };
    const p = send().finally(() => inflight.delete(p));
    inflight.add(p);
  }
  await Promise.all(inflight);
  return { cost: { rate, answers } };
}

// ---------------------------------------------------------------- KS

/** Two-sample Kolmogorov-Smirnov: D and the asymptotic p-value
 * (Numerical Recipes' probks with Stephens' correction). */
export function ks(a: number[], b: number[]): { d: number; p: number } {
  const x = [...a].sort((p, q) => p - q);
  const y = [...b].sort((p, q) => p - q);
  let i = 0, j = 0, d = 0;
  while (i < x.length && j < y.length) {
    const v = Math.min(x[i]!, y[j]!);
    while (i < x.length && x[i]! <= v) i++;
    while (j < y.length && y[j]! <= v) j++;
    d = Math.max(d, Math.abs(i / x.length - j / y.length));
  }
  const en = Math.sqrt((x.length * y.length) / (x.length + y.length));
  const lam = (en + 0.12 + 0.11 / en) * d;
  let p = 0, sign = 1, prev = 0;
  for (let k = 1; k <= 100; k++) {
    const term = sign * 2 * Math.exp(-2 * k * k * lam * lam);
    p += term;
    if (Math.abs(term) <= 0.001 * prev || Math.abs(term) <= 1e-8 * p) return { d, p: Math.min(1, Math.max(0, p)) };
    sign = -sign;
    prev = Math.abs(term);
  }
  return { d, p: 1 };
}

// ---------------------------------------------------------------- legs

const classProjects = (env: RunEnv, cls: string): Project[] => {
  const ps = env.plan.projects.filter((p) => p.cls.class === cls);
  if (!ps.length) throw new Error(`no class ${cls} in the manifest`);
  return ps;
};
const arg = (leg: LegSpec, k: string, d: string): string => String(leg.args?.[k] ?? d);

/** The names a project's setup phases recorded as held (setup's `names`),
 * from the phase ledgers tenants.ts writes as each phase ends: none for a
 * setup that has not ended, never ran, or created nothing. */
function setupNames(env: RunEnv, p: Project): string[] {
  return p.cls.phases.flatMap((ph, i) => {
    if (ph.mode !== "setup") return [];
    try {
      const ledger = JSON.parse(readFileSync(join(env.out, "projects", p.id, `phase-${i}.ledger.json`), "utf8"));
      return (ledger.setup?.names ?? []) as string[];
    } catch {
      return [];
    }
  });
}

export async function h11t(env: RunEnv, leg: LegSpec, signal: AbortSignal): Promise<Record<string, unknown>> {
  const pairs = Number(arg(leg, "pairs", "2000"));
  const rate = Number(arg(leg, "rate", "8"));
  const cls = arg(leg, "target", "cold");
  const targets = classProjects(env, cls);
  const rng = new Rng(Number(arg(leg, "seed", String(env.plan.manifest.seed))), 11);
  // The quiet names are what the target's setup recorded as created, never
  // names derived from the manifest: a setup that failed or wrote other
  // names would leave those uncreated, and an uncreated name timed against
  // a never-held one matches by construction. Pairs take them in a seeded
  // shuffle, each timed once before any is timed twice.
  const quiet = targets.flatMap((p) => setupNames(env, p).map((n): [Project, string] => [p, n]));
  if (!quiet.length) {
    const n = `${targets.length} project${targets.length === 1 ? "" : "s"}`;
    throw new Error(`h11t: the setup of class ${cls} recorded no stream it created (${n}), so there is no quiet name to time`);
  }
  for (let i = quiet.length - 1; i > 0; i--) {
    const j = rng.int(i + 1);
    [quiet[i], quiet[j]] = [quiet[j]!, quiet[i]!];
  }
  const anon = async (path: string): Promise<{ ms: number; ans: string }> => {
    const t0 = performance.now();
    let ans = "0:transport";
    try {
      const res = await fetch(env.base + path, { signal });
      const body = new Uint8Array(await res.arrayBuffer());
      ans = `${res.status}:${errCode({ status: res.status, headers: res.headers, body, ms: 0 }) || "-"}`;
    } catch {
      // counted as a transport answer
    }
    return { ms: performance.now() - t0, ans };
  };
  const quietMs: number[] = [];
  const neverMs: number[] = [];
  const answers = { quiet: {} as Record<string, number>, never: {} as Record<string, number> };
  let differences = 0;
  const examples: string[] = [];
  // At most `rate` pairs a second: a pair starts no sooner than 1000/rate
  // ms after the one before it started, and a late pair never makes the
  // next one early, so after a stall the leg does not burst to catch up
  // (each request may spend one of the cell's lookups).
  let due = performance.now();
  let last: number | null = null;
  let gapMin = Infinity;
  for (let i = 0; i < pairs && !signal.aborted; i++) {
    while (performance.now() < due && !signal.aborted) await sleep(due - performance.now(), signal);
    if (signal.aborted) break;
    const start = performance.now();
    if (last !== null) gapMin = Math.min(gapMin, start - last);
    last = start;
    due = start + 1000 / rate;
    const [b, name] = quiet[i % quiet.length]!;
    const path = (n: string) => `/v1/streams/${encName(n)}/watches/w/keys/0000000000000000?cap=${encodeURIComponent(b.id)}.x&timeoutMs=0`;
    const first = rng.next() < 0.5;
    const one = first ? await anon(path(name)) : null;
    const two = await anon(path(`never/${env.plan.manifest.name}-${i}`));
    const q = one ?? (await anon(path(name)));
    quietMs.push(q.ms);
    neverMs.push(two.ms);
    bump(answers.quiet, q.ans);
    bump(answers.never, two.ans);
    if (q.ans !== two.ans) {
      differences++;
      if (examples.length < 5) examples.push(`pair ${i}: quiet ${q.ans}, never ${two.ans}`);
    }
  }
  return {
    pairs: quietMs.length, rate, gap_min_ms: Number.isFinite(gapMin) ? round3(gapMin) : null, target: cls,
    names: { projects: new Set(quiet.map(([p]) => p.id)).size, names: quiet.length },
    answers, differences, examples, ks: ks(quietMs, neverMs), quiet_p50_ms: pct(quietMs, 0.5), never_p50_ms: pct(neverMs, 0.5),
    quiet_ms: quietMs.map(round3), never_ms: neverMs.map(round3),
  };
}

const VOLATILE = /^(date|content-length|x-streams-.*|.*request-id.*|traceparent|server-timing)$/;
function shape(r: Resp, masks: string[]): string {
  const names = r.headers ? [...r.headers.keys()].filter((h) => !VOLATILE.test(h)).sort().join(",") : "";
  let body = utf8(r.body);
  for (const m of masks) body = body.split(m).join("<masked>");
  return `${r.status}|${errCode(r)}|${names}|${body}`;
}

export async function probes(env: RunEnv, leg: LegSpec, signal: AbortSignal): Promise<Record<string, unknown>> {
  const pairs = Number(arg(leg, "pairs", "2000"));
  const rate = Number(arg(leg, "rate", "10"));
  const from = classProjects(env, arg(leg, "from", "cold-none"));
  const to = arg(leg, "to", "warm").split(",").flatMap((c) => classProjects(env, c));
  if (from.some((p) => p.cls.phases.length)) throw new Error(`probes: --from class ${from[0]!.cls.class} must hold no streams (no phases)`);
  const rng = new Rng(Number(arg(leg, "seed", String(env.plan.manifest.seed))), 13);
  // Every name a target holds, with its holders; every (prober, name) once,
  // in seeded random order: each pair is A's first contact with that name.
  const holders = new Map<string, Project[]>();
  for (const b of to) {
    for (let k = 0; k < (b.cls.streams ?? 1); k++) {
      const n = streamName(b.cls.prefix ?? "t/", k);
      holders.set(n, [...(holders.get(n) ?? []), b]);
    }
  }
  const combos: Array<[Project, string]> = from.flatMap((a) => [...holders.keys()].sort().map((n) => [a, n] as [Project, string]));
  for (let i = combos.length - 1; i > 0; i--) {
    const j = rng.int(i + 1);
    [combos[i], combos[j]] = [combos[j]!, combos[i]!];
  }
  const todo = combos.slice(0, pairs);
  const meter = new Meter();
  const differences = { name: 0, usage: 0 };
  const examples: string[] = [];
  const usageAnswers: Record<string, number> = {};
  const put: Record<string, number> = {};
  const del: Record<string, number> = {};
  const t0 = performance.now();
  let done = 0;
  const seen = new Set<string>();
  for (let i = 0; i < todo.length && !signal.aborted; i++) {
    await sleep(t0 + (i * 1000) / rate - performance.now(), signal);
    const [a, name] = todo[i]!;
    const bs = holders.get(name)!;
    const b = bs[rng.int(bs.length)]!;
    seen.add(`${a.id} ${name}`);
    const http = new Http(env.base, env.headers.get(a.id)!, meter);
    const never = `never/${env.plan.manifest.name}-p${i}`;
    const r1 = await http.call("GET", `/v1/streams/${encName(name)}/records?cursor=beginning`);
    const r2 = await http.call("GET", `/v1/streams/${encName(never)}/records?cursor=beginning`);
    if (shape(r1, [name, encName(name)]) !== shape(r2, [never, encName(never)])) {
      differences.name++;
      if (examples.length < 5) examples.push(`pair ${i} ${a.id} -> ${b.id} ${name}: ${shape(r1, [name])} vs ${shape(r2, [never])}`.slice(0, 400));
    }
    const ghost = `proj_never_${env.plan.manifest.name.replaceAll("-", "_")}_${i}`;
    const u1 = await http.call("GET", `/v1/projects/${encodeURIComponent(b.id)}/usage`);
    const u2 = await http.call("GET", `/v1/projects/${encodeURIComponent(ghost)}/usage`);
    bump(usageAnswers, answer(u1));
    if (shape(u1, [b.id]) !== shape(u2, [ghost])) {
      differences.usage++;
      if (examples.length < 5) examples.push(`pair ${i} usage ${a.id} -> ${b.id}: ${shape(u1, [b.id])} vs ${shape(u2, [ghost])}`.slice(0, 400));
    }
    const p = await http.call("PUT", `/v1/streams/${encName(name)}`, { body: '{"format":{"kind":"json"}}', contentType: "application/json" });
    bump(put, String(p.status));
    if (p.status === 201 || p.status === 200) bump(del, String((await http.call("DELETE", `/v1/streams/${encName(name)}`)).status));
    done++;
  }
  return {
    // first_contacts: distinct (prober, name) pairs read, the bar's value;
    // available: every such pair the classes allow (probers x names).
    pairs: done, first_contacts: seen.size, available: combos.length, probers: from.length, names: holders.size,
    rate, from: from[0]!.cls.class, to: arg(leg, "to", "warm"), differences, examples,
    usage_answers: usageAnswers, put, delete: del, requests: meter.tot.get("requests"), error_codes: meter.errorCodes,
  };
}

export const HOSTILE_MODES = { h9, h9b, cost };
export const HOSTILE_LEGS = { h11t, probes };
