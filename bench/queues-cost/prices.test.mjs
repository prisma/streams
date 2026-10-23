// node --test bench/queues-cost/  (the collector's pricing math, and the
// price/idle subcommands over synthetic arm directories)
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';
import * as P from './prices.mjs';
import { idle, price, procSample } from './collect.mjs';

const scenario = JSON.parse(readFileSync(new URL('./scenario.json', import.meta.url), 'utf8'));
const close = (a, b, tol, msg) => assert.ok(Math.abs(a - b) <= tol, `${msg}: ${a} vs ${b}`);

test('ps CPU times parse in every format', () => {
  assert.equal(P.parseCpuTime('0:01.50'), 1.5);
  assert.equal(P.parseCpuTime('12:03.25'), 723.25);
  assert.equal(P.parseCpuTime('01:02:03'), 3723);
  assert.equal(P.parseCpuTime('2-01:00:00'), 2 * 86_400 + 3600);
  assert.equal(P.parseCpuTime('42'), 42);
  assert.ok(Number.isNaN(P.parseCpuTime('')));
  assert.ok(Number.isNaN(P.parseCpuTime('x:1')));
});

test('procSample reads this process', () => {
  const s = procSample(process.pid);
  assert.ok(s && s.cpu_s >= 0 && s.rss_kb > 1000, JSON.stringify(s));
  assert.equal(procSample(2 ** 22 + 12_345), null);
});

test('cumulative series interpolate and gauges average by time', () => {
  const tl = [{ t: 0, x: 0 }, { t: 10, x: 100 }, { t: 20, x: 100 }];
  assert.equal(P.cumAt(tl, 5, 'x'), 50);
  assert.equal(P.cumAt(tl, 15, 'x'), 100);
  assert.equal(P.cumAt(tl, 99, 'x'), 100);
  assert.equal(P.cumAt([], 5, 'x'), 0);
  // A gauge at 0 then 100 from t=10 (linear between samples).
  const g = [{ t: 0, v: 0 }, { t: 10, v: 100 }, { t: 20, v: 100 }];
  assert.equal(P.timeAverage(g, 0, 20, 'v'), 75);
  assert.equal(P.timeAverage(g, 10, 20, 'v'), 100);
  assert.equal(P.timeAverage([{ t: 5, v: 7 }], 0, 20, 'v'), 7);
});

const mk = (t, cpu, a, b, put, live, tiers = {}) => ({
  t, cpu_s: cpu,
  stats2: { total: { class_a: a, class_b: b, free: 0 }, by_tier: tiers, live_bytes: { total: { bytes: live } } },
  stats: { put_bytes: put },
});

test('a window measures deltas, caps CPU at the cell and reads egress from the timeline', () => {
  const from = mk(1_000, 10, 100, 1000, 5e6, 1e9, { shard: { class_a: 100, class_b: 1000 } });
  const to = mk(3_601_000, 4000, 1100, 3000, 6e6, 1e9, { shard: { class_a: 900, class_b: 3000 }, hist: { class_a: 200, class_b: 0 } });
  const samples = [{ t: 1_000, rss_kb: 100_000 }, { t: 3_601_000, rss_kb: 300_000 }];
  const timeline = [{ t: 0, delivered_bytes: 0, sent: 0 }, { t: 3_601_000, delivered_bytes: 3_601_000, sent: 36_010 }];
  const w = P.windowTerms(from, to, samples, timeline, { vcpus: 1, mem_bytes: P.GIB, utilization: 0.6 });
  assert.equal(w.dur_s, 3600);
  assert.equal(w.class_a, 1000);
  assert.equal(w.class_b, 2000);
  assert.deepEqual(w.by_tier, { shard: { class_a: 800, class_b: 2000 }, hist: { class_a: 200, class_b: 0 } });
  assert.equal(w.put_bytes, 1e6);
  assert.equal(w.cpu_s_uncapped, 3990);
  assert.equal(w.cpu_s, 3600);
  assert.equal(w.egress_bytes, 3_600_000);
  assert.equal(w.messages, 36_000);
  assert.equal(w.rss_avg_bytes, 200_000 * 1024);
  assert.equal(w.live_avg_bytes, 1e9);
});

const win = (dur_s, x) => ({
  dur_s, class_a: x, class_b: 10 * x, put_bytes: 1000 * x, cpu_s: x / 100, egress_bytes: 500 * x, messages: x,
  rss_avg_bytes: 1e8, live_avg_bytes: 2e8, by_tier: { shard: { class_a: x, class_b: 10 * x } },
});

test('a single window scales to the month by rate', () => {
  const m = P.toMonth([win(3600, 100)]);
  assert.equal(m.basis, 'rate');
  assert.equal(m.class_a, 100 * 720);
  assert.equal(m.by_tier.shard.class_b, 1000 * 720);
  assert.equal(m.rss_avg_bytes, 1e8);
});

test('a burst window counts once and the steady rate fills the month', () => {
  const day = 86_400;
  const m = P.toMonth([win(day, 5000), win(day, 1000)]);
  assert.equal(m.basis, 'burst_once');
  // 5000 once + 1000 a day for the other 29 days.
  assert.equal(m.class_a, 5000 + 29 * 1000);
  assert.equal(m.messages, 34_000);
  const avg = P.toMonth([{ ...win(day, 1), rss_avg_bytes: 3e8 }, win(day, 1)]);
  close(avg.rss_avg_bytes, (3e8 + 29 * 1e8) / 30, 1, 'weighted average');
  assert.throws(() => P.toMonth([win(1, 1), win(1, 1), win(1, 1)]), /one window/);
});

test('owner and list prices price each term', () => {
  const m = {
    class_a: 1e6, class_b: 1e6, put_bytes: 1e9, live_avg_bytes: P.GIB, cpu_s: 3600,
    rss_avg_bytes: 1e9, egress_bytes: 1e9,
  };
  const o = P.priceMonth(m, P.OWNER_PRICES);
  assert.equal(o.terms.class_a.usd, 3.75);
  assert.equal(o.terms.class_b.usd, 0.375);
  assert.equal(o.terms.transfer_in.usd, 0.01);
  assert.equal(o.terms.storage.usd, 0.015);
  assert.equal(o.terms.cpu.usd, 0.064);
  close(o.terms.memory.usd, 720 * 0.006, 1e-12, 'memory');
  assert.equal(o.terms.egress.usd, 0.005);
  close(o.usd, 3.75 + 0.375 + 0.01 + 0.015 + 0.064 + 4.32 + 0.005, 1e-9, 'total');
  const l = P.priceMonth(m, P.LIST_PRICES);
  assert.equal(l.terms.class_a.usd, 5);
  assert.equal(l.terms.class_b.usd, 0.5);
  assert.equal(l.terms.storage.usd, 0.02);
  assert.equal(l.terms.cpu.usd, 0.064);
});

test('the floor share is the larger of CPU and memory share at planned utilization', () => {
  const cell = { vcpus: 1, mem_bytes: P.GIB, utilization: 0.6 };
  // 1% of a vCPU-month at 60% utilization: 1/60 share.
  const cpuHeavy = P.floorShare({ cpu_s: 0.006 * P.MONTH_S, rss_avg_bytes: 0 }, cell);
  close(cpuHeavy.share, 0.01, 1e-12, 'cpu share');
  close(cpuHeavy.usd, 0.953, 1e-9, 'cpu usd');
  const memHeavy = P.floorShare({ cpu_s: 0, rss_avg_bytes: 0.06 * P.GIB }, cell);
  close(memHeavy.share, 0.1, 1e-12, 'mem share');
  assert.equal(P.floorShare({ cpu_s: -5, rss_avg_bytes: -5 }, cell).share, 0);
  assert.equal(P.floorShare({ cpu_s: 1e12, rss_avg_bytes: 0 }, cell).share, 1);
});

test('costs.json: fully loaded is marginal plus the floor share; standalone carries the whole floor', () => {
  const a = P.toMonth([win(3600, 100)]);
  const b = P.toMonth([win(3600, 300)]);
  const c = P.buildCosts({ abMonths: { a, b }, standalone: { a, b }, scale: 1, scenario });
  const marginal = P.priceMonth(P.monthDelta(a, b));
  close(c.month_usd.marginal, marginal.usd, 1e-4, 'marginal');
  close(c.month_usd.fully_loaded, marginal.usd + c.terms.floor_share.usd, 1e-4, 'fully loaded');
  close(c.month_usd.standalone, marginal.usd + 95.3, 1e-4, 'standalone');
  assert.equal(c.messages_month, Math.round(22_747_641.43));
  close(c.per_million_messages_usd.fully_loaded, (c.month_usd.fully_loaded / 22_747_641.43) * 1e6, 1e-4, 'per million');
  assert.ok(c.month_usd_list.marginal > c.month_usd.marginal);
  assert.equal(P.buildCosts({ abMonths: { a, b }, scale: 1, scenario }).month_usd.standalone, null);
  // The marginal class A is B - A: 200 an hour, 144,000 a month.
  assert.equal(c.terms.marginal.class_a.quantity, 144_000);
});

// ---- price and idle over synthetic arm directories --------------------------

function writeArm(dir, { classA, cpuPerH, rssKb, liveBytes, burst = false, summary = null, deliveredPerS = 0 }) {
  mkdirSync(join(dir, 'snaps'), { recursive: true });
  const tags = burst ? ['t0', 'start', 'burst_end', 'end'] : ['t0', 'start', 'end'];
  const step = 3_600_000;
  const lines = [];
  tags.forEach((tag, i) => {
    const t = 1_000_000 + i * step;
    lines.push(JSON.stringify({ tag, t, cpu_s: i * cpuPerH, rss_kb: rssKb, ledger: true }));
    // Burst window does 3x the steady rate of class A.
    const a = tags.slice(1, i + 1).reduce((s, x) => s + (x === 'burst_end' ? 3 * classA : classA), 0);
    writeFileSync(join(dir, 'snaps', `${tag}-stats2.json`), JSON.stringify({
      total: { class_a: a, class_b: 10 * a, free: 0 }, by_tier: { shard: { class_a: a, class_b: 10 * a } },
      live_bytes: { total: { objects: 1, bytes: liveBytes } },
    }));
    writeFileSync(join(dir, 'snaps', `${tag}-stats.json`), JSON.stringify({ put_bytes: 1000 * a }));
  });
  writeFileSync(join(dir, 'marks.jsonl'), `${lines.join('\n')}\n`);
  const samples = [];
  for (let t = 1_000_000; t <= 1_000_000 + tags.length * step; t += 600_000) samples.push(JSON.stringify({ t, cpu_s: 0, rss_kb: rssKb, live_bytes: liveBytes }));
  writeFileSync(join(dir, 'samples.jsonl'), `${samples.join('\n')}\n`);
  if (summary) {
    writeFileSync(join(dir, 'replay-summary.json'), JSON.stringify(summary));
    const end = 1_000_000 + tags.length * step;
    writeFileSync(join(dir, 'replay-timeline.jsonl'), [
      JSON.stringify({ t: 1_000_000, sent: 0, delivered_bytes: 0 }),
      JSON.stringify({ t: end, sent: (end - 1_000_000) / 100, delivered_bytes: ((end - 1_000_000) / 1000) * deliveredPerS }),
    ].join('\n'));
  }
}

const okSummary = { ok: true, void_reasons: [], options: {}, totals: { claims: 360, empty_claims: 360 }, latency_ms: {} };

test('price: an A/B pair becomes costs.json that cf_bill.py reads', () => {
  const root = mkdtempSync(join(tmpdir(), 'qcost-'));
  writeArm(join(root, 'A'), { classA: 1000, cpuPerH: 360, rssKb: 400_000, liveBytes: 1e9 });
  writeArm(join(root, 'B'), { classA: 1100, cpuPerH: 396, rssKb: 410_000, liveBytes: 1.1e9, summary: okSummary, deliveredPerS: 5000 });
  const c = price({ a: join(root, 'A'), b: join(root, 'B'), scale: '1' });
  assert.equal(c.void, false);
  // 100 more class A an hour: 72,000 a month, $0.27 at $3.75 a million.
  close(c.terms.marginal.class_a.quantity, 72_000, 1e-6, 'class A');
  close(c.terms.marginal.class_a.usd, 0.27, 1e-9, 'class A usd');
  close(c.terms.marginal.cpu.quantity, 36 * 720, 1e-6, 'cpu seconds');
  close(c.terms.marginal.storage.quantity, 1e8, 1e-3, 'stored bytes');
  close(c.terms.marginal.egress.quantity, 5000 * 3600 * 720, 1, 'egress');
  // 1% of a vCPU at 60% planned: 1/60; memory 10,000 KiB of 0.6 GiB: 1.6%.
  close(c.terms.floor_share.cpu_share, 0.01 / 0.6, 1e-9, 'cpu share');
  close(c.terms.floor_share.mem_share, (10_000 * 1024) / (0.6 * P.GIB), 1e-9, 'mem share');
  writeFileSync(join(root, 'costs.json'), JSON.stringify(c));
  let out;
  try {
    out = execFileSync('python3', [new URL('./cf_bill.py', import.meta.url).pathname, '--measured', join(root, 'costs.json')]).toString();
  } catch (e) {
    if (e.code === 'ENOENT') return; // no python3 on this host
    throw e;
  }
  assert.match(out, /standalone n\/a\); margin .*: goal (MET|NOT MET)/);
});

test('price: a burst pair counts the burst once; a void replay voids the verdict', () => {
  const root = mkdtempSync(join(tmpdir(), 'qcost-'));
  writeArm(join(root, 'A'), { classA: 1000, cpuPerH: 360, rssKb: 400_000, liveBytes: 1e9, burst: true });
  const bad = { ...okSummary, ok: false, void_reasons: ['qb-x: none_lost'] };
  writeArm(join(root, 'B'), { classA: 1100, cpuPerH: 360, rssKb: 400_000, liveBytes: 1e9, burst: true, summary: bad });
  const c = price({ a: join(root, 'A'), b: join(root, 'B'), scale: '1' });
  // Burst window: 300 more, once; steady: 100 more an hour for 719 hours.
  close(c.terms.marginal.class_a.quantity, 300 + 100 * 719, 1e-6, 'burst once');
  assert.equal(c.months.b.basis, 'burst_once');
  assert.equal(c.void, true);
  assert.deepEqual(c.void_reasons, ['arm B: qb-x: none_lost']);
  writeFileSync(join(root, 'costs.json'), JSON.stringify(c));
  try {
    const out = execFileSync('python3', [new URL('./cf_bill.py', import.meta.url).pathname, '--measured', join(root, 'costs.json')]).toString();
    assert.match(out, /goal VOID \(arm B: qb-x: none_lost\)/);
  } catch (e) {
    if (e.code !== 'ENOENT') throw e;
  }
});

test('idle: requests over the baseline fail the idle verdict', () => {
  const root = mkdtempSync(join(tmpdir(), 'qidle-'));
  writeArm(join(root, 'A'), { classA: 1000, cpuPerH: 36, rssKb: 400_000, liveBytes: 1e9 });
  writeArm(join(root, 'B'), { classA: 1000, cpuPerH: 40, rssKb: 400_000, liveBytes: 1e9, summary: okSummary });
  const clean = idle({ a: join(root, 'A'), b: join(root, 'B') });
  assert.equal(clean.verdict, 'IDLE_CLEAN');
  assert.equal(clean.claims_per_hour, 360);
  writeArm(join(root, 'C'), { classA: 1001, cpuPerH: 36, rssKb: 400_000, liveBytes: 1e9, summary: okSummary });
  const dirty = idle({ a: join(root, 'A'), b: join(root, 'C') });
  assert.equal(dirty.verdict, 'IDLE_NOT_CLEAN');
  assert.ok(dirty.reasons.some((r) => r.startsWith('class_a above the baseline')));
  assert.equal(idle({ a: join(root, 'A'), b: join(root, 'C'), tolerance: '0.01' }).verdict, 'IDLE_CLEAN');
});
