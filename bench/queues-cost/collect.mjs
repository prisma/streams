#!/usr/bin/env node
// The queue cost benchmark's instruments and pricing (docs/QUEUES-PRICING-
// BENCHMARK.md §6). run-ab.sh drives it; each subcommand also runs alone.
//
//   collect.mjs mark   --dir ARM --tag TAG --pid PID [--s3 URL] [--server URL --auth-file F]
//       snapshots s3lite's /_s3lite/stats2 and /_s3lite/stats (and the
//       server's /v1/debug/store) into ARM/snaps/TAG-*.json and appends
//       {tag, t, cpu_s, rss_kb} for the server process to ARM/marks.jsonl
//   collect.mjs sample --dir ARM --pid PID [--s3 URL] [--interval-s 10] [--live-every 6]
//       until SIGTERM (or the process exits): the server's CPU-seconds and
//       RSS every interval, and the bucket's live bytes every live-every
//       samples, to ARM/samples.jsonl
//   Field mode (a Compute instance): `--cpu-log FILE --server URL --auth-file F`
//       in place of --pid. CPU comes from the wrapper's `cpu sample:` lines
//       in FILE (cpuFromLog), RSS from /v1/debug/load `local.rss_mb`; the
//       s3lite ledger is read only when --s3 is given. A mark refuses its
//       CPU (cpu_s null) when the newest line is older than
//       --cpu-max-age-s [60] or the log lost a boot an earlier mark saw.
//   collect.mjs price  --a ARM --b ARM [--sa ARM --sb ARM] --scale S --out costs.json
//       [--scenario P] [--cell-vcpus 1] [--cell-mem-gib 1] [--utilization 0.6] [--floor-usd 95.3]
//       prices the A/B pair (and the standalone pair: an empty cell, then
//       the scenario alone) and writes costs.json for cf_bill.py --measured
//   collect.mjs idle   --a ARM --b ARM [--tolerance 0] --out idle.json
//       the idle run's verdict: object requests and bytes per hour, B - A
//
// Windows come from marks: `start` .. `end`, split at `burst_end` when
// present (the burst window counted once, the rest of the month at the
// steady window's rate).
import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import * as P from './prices.mjs';

const HERE = new URL('.', import.meta.url);

export function parseFlags(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) throw new Error(`unexpected argument ${a}`);
    const k = a.slice(2);
    if (i + 1 >= argv.length || argv[i + 1].startsWith('--')) out[k] = true;
    else out[k] = argv[++i];
  }
  return out;
}

let sysconf = null;
const getconf = (name, fallback) => {
  try {
    return Number(execFileSync('getconf', [name]).toString().trim()) || fallback;
  } catch {
    return fallback;
  }
};
/** Cumulative CPU-seconds and RSS (KiB) of a process, or null if gone. */
export function procSample(pid) {
  const stat = `/proc/${pid}/stat`;
  if (existsSync(stat)) {
    try {
      // Fields after the parenthesised command: utime is 14th, stime 15th,
      // rss (pages) 24th of the whole line.
      const text = readFileSync(stat, 'utf8');
      const f = text.slice(text.lastIndexOf(')') + 2).split(' ');
      sysconf ??= { tck: getconf('CLK_TCK', 100), page: getconf('PAGESIZE', 4096) };
      return { cpu_s: (Number(f[11]) + Number(f[12])) / sysconf.tck, rss_kb: (Number(f[21]) * sysconf.page) / 1024 };
    } catch {
      return null;
    }
  }
  try {
    const out = execFileSync('ps', ['-o', 'time=,rss=', '-p', String(pid)], { stdio: ['ignore', 'pipe', 'ignore'] })
      .toString().trim().split(/\s+/);
    if (out.length < 2) return null;
    return { cpu_s: P.parseCpuTime(out[0]), rss_kb: Number(out[1]) };
  } catch {
    return null;
  }
}

const CPU_LINE = /cpu sample: (\{.*\})/;
/**
 * The instance's cumulative CPU-seconds from the Compute wrapper's `cpu
 * sample:` lines (deploy/app-server/index.ts, CPU_LOG_SECS). proc_cpu_s is
 * cumulative per boot (boot_ms), so the instance's CPU is the sum over boots
 * of each boot's latest line: a reboot adds a boot instead of resetting the
 * figure. Lines repeated by overlapping tails, cut off or of other kinds are
 * skipped. Returns { cpu_s, boots: { boot_ms: proc_cpu_s }, t_ms (newest
 * line) }, or null without a line. One instance per log.
 */
export function cpuFromLog(text) {
  const latest = new Map();
  for (const line of text.split('\n')) {
    const m = CPU_LINE.exec(line);
    if (!m) continue;
    let s;
    try {
      s = JSON.parse(m[1]);
    } catch {
      continue;
    }
    if (![s.boot_ms, s.t_ms, s.proc_cpu_s].every(Number.isFinite)) continue;
    const b = latest.get(s.boot_ms);
    if (!b || s.t_ms > b.t_ms) latest.set(s.boot_ms, s);
  }
  if (!latest.size) return null;
  const boots = {};
  let cpu = 0;
  let t = 0;
  for (const [k, s] of [...latest].sort((x, y) => x[0] - y[0])) {
    boots[k] = s.proc_cpu_s;
    cpu += s.proc_cpu_s;
    t = Math.max(t, s.t_ms);
  }
  return { cpu_s: Math.round(cpu * 1000) / 1000, boots, t_ms: t };
}

/**
 * Why a mark's CPU cannot follow an earlier mark's boots, or null: a boot
 * that mark saw is gone from the log, or its CPU went back (the log was cut
 * or replaced). Its window would read short or negative CPU.
 */
export function cpuRegression(prevBoots, boots) {
  for (const [k, v] of Object.entries(prevBoots ?? {})) {
    if (!(k in boots)) return `boot ${k} of the previous mark is missing from the CPU log`;
    if (boots[k] < v) return `boot ${k} went back from ${v} to ${boots[k]} CPU-seconds`;
  }
  return null;
}

async function getJson(url, headers = {}) {
  try {
    const r = await fetch(url, { headers, signal: AbortSignal.timeout(30_000) });
    if (!r.ok) return null;
    return await r.json();
  } catch {
    return null;
  }
}

const bearer = (f) => ({ authorization: `Bearer ${readFileSync(f['auth-file'], 'utf8').trim()}` });

/** The field source: --cpu-log with --server and --auth-file; else --pid. */
function source(f, cmd) {
  const field = f['cpu-log'] !== undefined;
  if (field ? !(f.server && f['auth-file']) : !f.pid) {
    throw new Error(`${cmd} needs --dir${cmd === 'mark' ? ' --tag' : ''} and --pid, or --cpu-log with --server --auth-file`);
  }
  return field;
}

/**
 * Field mode's reading: the CPU log's figure and the server's RSS and boot
 * id. Fields that cannot be read are null.
 */
async function fieldSample(f) {
  let text = '';
  try {
    text = readFileSync(f['cpu-log'], 'utf8');
  } catch {
    // not written yet: no CPU
  }
  const cpu = cpuFromLog(text);
  const load = await getJson(`${f.server}/v1/debug/load`, bearer(f));
  const rssMb = load?.local?.rss_mb;
  return {
    cpu_s: cpu?.cpu_s ?? null, cpu_boots: cpu?.boots ?? null, cpu_t: cpu?.t_ms ?? null,
    rss_kb: Number.isFinite(rssMb) ? rssMb * 1024 : null, boot_id: load?.boot?.id ?? null,
  };
}

/**
 * A field mark's CPU fields. The CPU is refused (null, with cpu_error) when
 * the newest line is older than --cpu-max-age-s, or when the log lost a
 * boot or CPU the last mark with boots recorded.
 */
function fieldMarkCpu(f, dir, t, s) {
  const maxAge = Number(f['cpu-max-age-s'] ?? 60);
  const prev = readJsonl(join(dir, 'marks.jsonl')).filter((m) => m.cpu_boots).at(-1);
  let error = null;
  if (s.cpu_s === null) error = `no cpu sample in ${f['cpu-log']}`;
  else if (t - s.cpu_t > maxAge * 1000) error = `newest cpu sample is ${Math.round((t - s.cpu_t) / 1000)} s old (--cpu-max-age-s ${maxAge})`;
  else error = cpuRegression(prev?.cpu_boots, s.cpu_boots);
  if (error) return { cpu_s: null, cpu_source: 'log', cpu_t: s.cpu_t, cpu_boots: null, cpu_error: error };
  return { cpu_s: s.cpu_s, cpu_source: 'log', cpu_t: s.cpu_t, cpu_boots: s.cpu_boots };
}

async function mark(f) {
  const dir = f.dir;
  const tag = f.tag;
  if (!dir || !tag) throw new Error('mark needs --dir --tag and --pid, or --cpu-log with --server --auth-file');
  const field = source(f, 'mark');
  // In the field there is no s3lite unless --s3 names one.
  const s3 = f.s3 ?? (field ? null : 'http://127.0.0.1:9500');
  mkdirSync(join(dir, 'snaps'), { recursive: true });
  const t = Date.now();
  const [stats2, stats] = s3 ? await Promise.all([getJson(`${s3}/_s3lite/stats2`), getJson(`${s3}/_s3lite/stats`)]) : [null, null];
  let line;
  if (field) {
    const s = await fieldSample(f);
    line = { tag, t, ...fieldMarkCpu(f, dir, t, s), rss_kb: s.rss_kb, boot_id: s.boot_id };
  } else {
    const proc = procSample(f.pid);
    line = { tag, t, cpu_s: proc?.cpu_s ?? null, rss_kb: proc?.rss_kb ?? null };
  }
  line.ledger = stats2 !== null && stats !== null;
  if (s3) {
    writeFileSync(join(dir, 'snaps', `${tag}-stats2.json`), `${JSON.stringify(stats2)}\n`);
    writeFileSync(join(dir, 'snaps', `${tag}-stats.json`), `${JSON.stringify(stats)}\n`);
  }
  if (f.server && f['auth-file']) {
    const store = await getJson(`${f.server}/v1/debug/store`, bearer(f));
    writeFileSync(join(dir, 'snaps', `${tag}-store.json`), `${JSON.stringify(store)}\n`);
  }
  appendFileSync(join(dir, 'marks.jsonl'), `${JSON.stringify(line)}\n`);
  const boots = line.cpu_boots ? ` boots=${Object.keys(line.cpu_boots).length}` : '';
  const err = line.cpu_error ? ` cpu_error="${line.cpu_error}"` : '';
  process.stdout.write(`mark ${tag}: cpu_s=${line.cpu_s}${boots} rss_kb=${line.rss_kb} class_a=${stats2?.total?.class_a ?? '?'}${err}\n`);
}

async function sample(f) {
  if (!f.dir) throw new Error('sample needs --dir and --pid, or --cpu-log with --server --auth-file');
  const field = source(f, 'sample');
  const s3 = f.s3 ?? (field ? null : 'http://127.0.0.1:9500');
  const every = Number(f['interval-s'] ?? 10) * 1000;
  const liveEvery = Number(f['live-every'] ?? 6);
  let stop = false;
  process.on('SIGTERM', () => { stop = true; });
  process.on('SIGINT', () => { stop = true; });
  for (let i = 0; !stop; i++) {
    const t = Date.now();
    let line;
    if (field) {
      // A field sample never stops on a failed read: unread fields are left out.
      const s = await fieldSample(f);
      line = { t };
      if (s.cpu_s !== null) line.cpu_s = s.cpu_s;
      if (s.rss_kb !== null) line.rss_kb = s.rss_kb;
    } else {
      const proc = procSample(f.pid);
      if (!proc) break;
      line = { t, cpu_s: proc.cpu_s, rss_kb: proc.rss_kb };
    }
    if (s3 && i % liveEvery === 0) {
      const s2 = await getJson(`${s3}/_s3lite/stats2`);
      const live = s2?.live_bytes?.total?.bytes;
      if (Number.isFinite(live)) line.live_bytes = live;
    }
    appendFileSync(join(f.dir, 'samples.jsonl'), `${JSON.stringify(line)}\n`);
    const wait = every - (Date.now() - t);
    for (let w = 0; w < wait && !stop; w += 200) await new Promise((r) => setTimeout(r, Math.min(200, wait - w)));
  }
}

const readJsonl = (p) => (existsSync(p)
  ? readFileSync(p, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l))
  : []);
const readJson = (p) => (existsSync(p) ? JSON.parse(readFileSync(p, 'utf8')) : null);

/** An arm directory: marks with their snapshots, samples, the replay's files. */
export function loadArm(dir) {
  if (!existsSync(join(dir, 'marks.jsonl'))) throw new Error(`${dir}: no marks.jsonl`);
  const marks = new Map();
  for (const m of readJsonl(join(dir, 'marks.jsonl'))) {
    marks.set(m.tag, {
      ...m,
      stats2: readJson(join(dir, 'snaps', `${m.tag}-stats2.json`)),
      stats: readJson(join(dir, 'snaps', `${m.tag}-stats.json`)),
    });
  }
  return {
    dir, marks,
    samples: readJsonl(join(dir, 'samples.jsonl')),
    timeline: readJsonl(join(dir, 'replay-timeline.jsonl')),
    summary: readJson(join(dir, 'replay-summary.json')),
  };
}

/** The arm's measured windows: [start..end], or [start..burst_end, burst_end..end]. */
export function armWindows(arm, cell) {
  const need = (tag) => {
    const m = arm.marks.get(tag);
    if (!m) throw new Error(`${arm.dir}: no ${tag} mark`);
    if (!m.ledger || !m.stats2 || !m.stats) throw new Error(`${arm.dir}: ${tag} mark has no s3lite ledger`);
    if (!Number.isFinite(m.cpu_s)) throw new Error(`${arm.dir}: ${tag} mark has no CPU sample`);
    return m;
  };
  const start = need('start');
  const end = need('end');
  const w = (a, b) => P.windowTerms(a, b, arm.samples, arm.timeline, cell);
  return arm.marks.has('burst_end') ? [w(start, need('burst_end')), w(need('burst_end'), end)] : [w(start, end)];
}

function sameShape(a, b, label, warnings) {
  if (a.length !== b.length) throw new Error(`${label}: arms have different windows`);
  if ([...a, ...b].some((w) => w.live_samples === 0)) {
    warnings.push(`${label}: storage unmeasured, s3lite has no live_bytes census (use target/release/s3lite)`);
  }
  a.forEach((wa, i) => {
    const d = Math.abs(wa.dur_s - b[i].dur_s) / Math.max(wa.dur_s, b[i].dur_s);
    if (d > 0.05) warnings.push(`${label}: window ${i} lasts ${wa.dur_s}s in A and ${b[i].dur_s}s in B`);
  });
}

/** run-ab.sh's QUEUE_SHARDS for an arm (its arm.json), or null when unrecorded. */
const armShards = (dir) => {
  const n = readJson(join(dir, 'arm.json'))?.shards;
  return Number.isInteger(n) ? n : null;
};

/**
 * The shard count the priced figure ran on. B - A across two counts is not a
 * marginal cost (void); a standalone pair on another count is reported apart
 * (warned); an arm without the record is unknown (warned).
 */
function shardCount(f, reasons, warnings) {
  const by = { a: armShards(f.a), b: armShards(f.b), sa: f.sa ? armShards(f.sa) : null, sb: f.sb ? armShards(f.sb) : null };
  const pair = (x, y, label) => {
    if (x === null || y === null) {
      warnings.push(`${label}: shard count unknown (no arm.json shards)`);
      return null;
    }
    if (x !== y) {
      reasons.push(`${label}: arm A ran on ${x} shards, arm B on ${y}`);
      return null;
    }
    return x;
  };
  const shards = pair(by.a, by.b, 'A/B');
  if (f.sa && f.sb) {
    const sa = pair(by.sa, by.sb, 'standalone');
    if (sa !== null && shards !== null && sa !== shards) warnings.push(`standalone: ran on ${sa} shards, the A/B pair on ${shards}`);
  }
  return { shards, shards_by_arm: by };
}

function replayValidity(arm, label, reasons) {
  const s = arm.summary;
  if (!s) reasons.push(`${label}: no replay-summary.json`);
  else if (!s.ok) reasons.push(...s.void_reasons.map((r) => `${label}: ${r}`));
}

export function price(f) {
  const scenario = JSON.parse(readFileSync(f.scenario ?? new URL('scenario.json', HERE).pathname, 'utf8'));
  const scale = Number(f.scale ?? 1);
  const cell = {
    vcpus: Number(f['cell-vcpus'] ?? P.DEFAULT_CELL.vcpus),
    mem_bytes: Number(f['cell-mem-gib'] ?? 1) * P.GIB,
    utilization: Number(f.utilization ?? P.DEFAULT_CELL.utilization),
  };
  const floorUsd = Number(f['floor-usd'] ?? P.CELL_FLOOR_USD);
  const reasons = [];
  const warnings = [];
  const a = loadArm(f.a);
  const b = loadArm(f.b);
  replayValidity(b, 'arm B', reasons);
  const wa = armWindows(a, cell);
  const wb = armWindows(b, cell);
  sameShape(wa, wb, 'A/B', warnings);
  let standalone = null;
  if (f.sa && f.sb) {
    const sa = loadArm(f.sa);
    const sb = loadArm(f.sb);
    replayValidity(sb, 'standalone', reasons);
    const wsa = armWindows(sa, cell);
    const wsb = armWindows(sb, cell);
    sameShape(wsa, wsb, 'standalone', warnings);
    standalone = { a: P.toMonth(wsa), b: P.toMonth(wsb) };
  }
  const costs = P.buildCosts({
    abMonths: { a: P.toMonth(wa), b: P.toMonth(wb) }, standalone, scale, scenario, cell, floorUsd,
    meta: {
      generated_at: new Date().toISOString(),
      replay: b.summary ? { options: b.summary.options, totals: b.summary.totals, latency_ms: b.summary.latency_ms } : null,
      windows: { a: wa, b: wb },
      arms: { a: resolve(f.a), b: resolve(f.b), sa: f.sa ? resolve(f.sa) : null, sb: f.sb ? resolve(f.sb) : null },
    },
  });
  Object.assign(costs, shardCount(f, reasons, warnings));
  costs.void = reasons.length > 0;
  costs.void_reasons = reasons;
  costs.warnings = warnings;
  return costs;
}

export function idle(f) {
  const tol = Number(f.tolerance ?? 0);
  const a = loadArm(f.a);
  const b = loadArm(f.b);
  const reasons = [];
  replayValidity(b, 'idle arm', reasons);
  const wsa = armWindows(a, P.DEFAULT_CELL);
  const wsb = armWindows(b, P.DEFAULT_CELL);
  if (wsa.length !== 1 || wsb.length !== 1) throw new Error('idle: each arm must have one window (no burst_end mark)');
  const [wa] = wsa;
  const [wb] = wsb;
  const perH = (w, k) => (w[k] * 3600) / w.dur_s;
  const rows = {};
  for (const k of ['class_a', 'class_b', 'put_bytes', 'cpu_s']) {
    rows[k] = { a_per_hour: perH(wa, k), b_per_hour: perH(wb, k), delta_per_hour: perH(wb, k) - perH(wa, k) };
  }
  const tiers = {};
  for (const t of new Set([...Object.keys(wa.by_tier), ...Object.keys(wb.by_tier)])) {
    tiers[t] = {
      class_a_delta_per_hour: ((wb.by_tier[t]?.class_a ?? 0) * 3600) / wb.dur_s - ((wa.by_tier[t]?.class_a ?? 0) * 3600) / wa.dur_s,
      class_b_delta_per_hour: ((wb.by_tier[t]?.class_b ?? 0) * 3600) / wb.dur_s - ((wa.by_tier[t]?.class_b ?? 0) * 3600) / wa.dur_s,
    };
  }
  for (const k of ['class_a', 'class_b', 'put_bytes']) {
    if (rows[k].delta_per_hour > tol * rows[k].a_per_hour) reasons.push(`${k} above the baseline by ${rows[k].delta_per_hour.toFixed(1)}/h`);
  }
  const claims = b.summary?.totals?.claims ?? 0;
  return {
    verdict: reasons.length ? 'IDLE_NOT_CLEAN' : 'IDLE_CLEAN',
    reasons, tolerance: tol, window_s: { a: wa.dur_s, b: wb.dur_s }, per_hour: rows, by_tier: tiers,
    claims, empty_claims: b.summary?.totals?.empty_claims ?? 0,
    claims_per_hour: (claims * 3600) / wb.dur_s,
  };
}

function printCosts(c) {
  const usd = (x) => (x === null ? 'n/a' : `$${x.toFixed(2)}`);
  const lines = [
    `costs (30-day month, scale ${c.scale}, ${c.shards ?? 'unknown'} shards${c.void ? ', VOID' : ''})`,
    `  fully loaded ${usd(c.month_usd.fully_loaded)}  marginal ${usd(c.month_usd.marginal)}  standalone ${usd(c.month_usd.standalone)}`,
    `  at Tigris list: fully loaded ${usd(c.month_usd_list.fully_loaded)}  marginal ${usd(c.month_usd_list.marginal)}`,
    `  per million messages (fully loaded): ${usd(c.per_million_messages_usd.fully_loaded)} over the scenario's month; ` +
      `${usd(c.per_million_replayed_messages_usd.fully_loaded)} over the messages this run sent`,
    '  marginal terms:',
  ];
  for (const [k, t] of Object.entries(c.terms.marginal)) lines.push(`    ${k.padEnd(12)} ${usd(t.usd).padStart(10)}  ${Math.round(t.quantity).toLocaleString('en-US')} ${t.unit}`);
  const fs = c.terms.floor_share;
  lines.push(`    floor share  ${usd(fs.usd).padStart(10)}  max(cpu ${(fs.cpu_share * 100).toFixed(3)}%, mem ${(fs.mem_share * 100).toFixed(3)}%) of $${fs.floor_usd}`);
  const ratio = c.replayed_to_scenario;
  if (ratio !== null && (ratio < 0.9 || ratio > 1.1)) {
    lines.push(`  NOTE: the run sent ${ratio.toFixed(2)}x the scenario's average rate (a busiest-hour or quiet-hour shape): read per million on the messages it sent; its month is a rate-scaled ${ratio > 1 ? 'peak' : 'quiet'} month, not the scenario's`);
  }
  for (const r of c.void_reasons) lines.push(`  VOID: ${r}`);
  for (const w of c.warnings) lines.push(`  warning: ${w}`);
  process.stdout.write(`${lines.join('\n')}\n`);
}

async function main() {
  const [cmd, ...rest] = process.argv.slice(2);
  const f = parseFlags(rest);
  if (cmd === 'mark') return mark(f);
  if (cmd === 'sample') return sample(f);
  if (cmd === 'price') {
    if (!f.a || !f.b || !f.out) throw new Error('price needs --a --b --out');
    const c = price(f);
    writeFileSync(f.out, `${JSON.stringify(c, null, 2)}\n`);
    printCosts(c);
    return undefined;
  }
  if (cmd === 'idle') {
    if (!f.a || !f.b || !f.out) throw new Error('idle needs --a --b --out');
    const r = idle(f);
    writeFileSync(f.out, `${JSON.stringify(r, null, 2)}\n`);
    process.stdout.write(`${r.verdict}: class A ${r.per_hour.class_a.delta_per_hour.toFixed(1)}/h, ` +
      `class B ${r.per_hour.class_b.delta_per_hour.toFixed(1)}/h, put ${Math.round(r.per_hour.put_bytes.delta_per_hour)} B/h ` +
      `over the baseline; ${r.claims} empty claims (${Math.round(r.claims_per_hour)}/h)\n`);
    for (const x of r.reasons) process.stdout.write(`  ${x}\n`);
    process.exitCode = r.verdict === 'IDLE_CLEAN' ? 0 : 1;
    return undefined;
  }
  throw new Error('usage: collect.mjs mark|sample|price|idle [flags] (see the header)');
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  main().catch((e) => {
    process.stderr.write(`collect: ${e.message}\n`);
    process.exit(2);
  });
}
