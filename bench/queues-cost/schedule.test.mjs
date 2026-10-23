// node --test bench/queues-cost/  (the schedule, rate and fate math)
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import * as S from './schedule.mjs';

const scenario = JSON.parse(readFileSync(new URL('./scenario.json', import.meta.url), 'utf8'));
const byName = (plan, name) => plan.queues.find((q) => q.name === name);
const sumOver = (plan, ms, step) => S.expectedSends(plan, ms, step);
const close = (a, b, tol, msg) => assert.ok(Math.abs(a - b) <= tol, `${msg}: ${a} vs ${b} (tol ${tol})`);

test('a virtual day at 1x without the invoice burst sends each queue its day volume', () => {
  const plan = S.buildPlan(scenario, { scale: 1, compress: 1, invoiceBurst: false });
  const sent = sumOver(plan, S.DAY_MS, 60_000);
  for (const q of plan.queues) {
    if (q.name === 'mkp-vercel-submit-invoice') {
      // Only the 18% non-burst share: (1 - 0.82) * 7 / 6 of D a day.
      close(sent[q.index], ((1 - 0.82) * 7) / 6 * q.D, 1, q.name);
    } else {
      close(sent[q.index], q.D, 1, q.name);
    }
  }
  const total = sent.reduce((a, b) => a + b, 0);
  // Week writes / 7 less the invoice burst share: about 752 k messages.
  close(total, 5_307_783 / 7 - (34_800 / 7) * (1 - 0.21), 25, 'total');
});

test('compression keeps rates per real second and shrinks cron spacing', () => {
  for (const K of [1, 10, 24, 1440]) {
    const plan = S.buildPlan(scenario, { scale: 1, compress: K, invoiceBurst: false });
    const realDay = S.DAY_MS / K; // one virtual day
    const sent = sumOver(plan, realDay, Math.min(60_000, realDay / 24));
    const stripe = byName(plan, 'cp-stripe-usage-records-sync');
    // A virtual day carries D / K messages: the same rate per real second.
    close(sent[stripe.index], stripe.D / K, 1, `stripe K=${K}`);
    const perRealS = sent[stripe.index] / (realDay / 1000);
    close(perRealS, stripe.D / 86_400, 0.02 * (stripe.D / 86_400) + 1 / (realDay / 1000), `stripe rate K=${K}`);
  }
  // Cron spacing: 14 firings a virtual day, one every 102.86 / K minutes.
  const plan = S.buildPlan(scenario, { compress: 10, invoiceBurst: false, only: ['cp-stripe-usage-records-sync'] });
  const firings = [];
  for (let r = 0; r < S.DAY_MS / 10; r += 1000) {
    if (S.eventsBetween(plan, r, r + 1000).length) firings.push(r);
  }
  assert.equal(firings.length, 14);
  close(firings[1] - firings[0], S.DAY_MS / 14 / 10, 1000, 'spacing');
});

test('scale multiplies every rate', () => {
  const one = sumOver(S.buildPlan(scenario, { scale: 1, invoiceBurst: false }), S.DAY_MS, 600_000);
  const hundred = sumOver(S.buildPlan(scenario, { scale: 100, invoiceBurst: false }), S.DAY_MS, 600_000);
  one.forEach((n, i) => close(hundred[i], 100 * n, 100, `queue ${i}`));
});

test('cron sweeps fire whole batches at their phase and minute', () => {
  const plan = S.buildPlan(scenario, { only: ['cp-spend-limit-enforce'] });
  const q = byName(plan, 'cp-spend-limit-enforce');
  // Hourly at :15; 67,840 / 7 / 24 = 403.8 per sweep.
  assert.deepEqual(S.eventsBetween(plan, 0, 15 * 60_000), []);
  const ev = S.eventsBetween(plan, 15 * 60_000, 15 * 60_000 + 1);
  assert.equal(ev.length, 1);
  assert.ok(ev[0].count === 403 || ev[0].count === 404);
  close(sumOver(plan, 24 * S.HOUR_MS, S.HOUR_MS)[q.index], 67_840 / 7, 1, 'day');
  assert.deepEqual(S.batches(404, 100), [100, 100, 100, 100, 4]);
});

test('notification bursts put ~90% of the day in the 10:00 hour', () => {
  const plan = S.buildPlan(scenario, { only: ['cp-notification-controller'] });
  const q = byName(plan, 'cp-notification-controller');
  let hour10 = 0;
  for (const e of S.eventsBetween(plan, 10 * S.HOUR_MS, 11 * S.HOUR_MS)) hour10 += e.count;
  close(hour10 / q.D, 0.9 + 0.05 / 24, 0.002, 'share');
  close(hour10, 17_618, 120, 'peak hour vs measured');
});

test('the month-start invoice burst is 82% of the week, once, at the peak-hour rate', () => {
  const plan = S.buildPlan(scenario, { only: ['mkp-vercel-submit-invoice'], burstAtS: 300 });
  const q = byName(plan, 'mkp-vercel-submit-invoice');
  const day0 = sumOver(plan, S.DAY_MS, 60_000)[q.index];
  close(day0, 0.82 * 34_800, 2, 'burst day');
  const day1 = sumOver(plan, 2 * S.DAY_MS, 60_000)[q.index] - day0;
  close(day1, (0.18 * 34_800) / 6, 2, 'ordinary day');
  // The burst starts at 300 s and runs at 6,162 an hour.
  const burst = plan.queues[q.index].components.find((c) => c.kind === 'monthStart');
  const first = S.eventsBetween({ ...plan, queues: [{ ...q, components: [burst] }] }, 0, 300_000);
  assert.deepEqual(first, []);
  let hour = 0;
  for (const e of S.eventsBetween({ ...plan, queues: [{ ...q, components: [burst] }] }, 300_000, 300_000 + S.HOUR_MS)) hour += e.count;
  close(hour, 6_162, 1, 'burst rate');
  // Compressed 10x: a tenth of the volume at the same rate.
  const c10 = S.buildPlan(scenario, { only: ['mkp-vercel-submit-invoice'], compress: 10 });
  const b10 = c10.queues[q.index].components.find((c) => c.kind === 'monthStart');
  close(b10.total, burst.total / 10, 1e-6, 'compressed burst');
});

test('the peak profile runs at the busiest hour, shared by per-queue peaks', () => {
  const plan = S.buildPlan(scenario, { profile: 'peak', scale: 10 });
  const hour = sumOver(plan, S.HOUR_MS, 60_000).reduce((a, b) => a + b, 0);
  close(hour, 629_040, 20, 'busiest hour x10');
  const stripe = byName(plan, 'cp-stripe-usage-records-sync');
  assert.equal(stripe.components[0].batch, 100);
  assert.equal(stripe.components[0].spreadMs, 80 * 60_000);
});

test('fates reproduce the measured deliveries and dead-letter rates', () => {
  const inv = S.fateModel(scenario.queues.find((q) => q.name === 'mkp-vercel-submit-invoice'), scenario);
  close(inv.dlq, 12_714 / 34_800, 1e-9, 'invoice dlq');
  assert.equal(inv.pRetry, 0);
  const rest = S.fateModel(scenario.queues[0], scenario);
  close(rest.deliveries, 1.0032, 0.0001, 'rest ratio');
  for (const [m, n] of [[inv, 100_000], [rest, 200_000]]) {
    let deliveries = 0;
    let fails = 0;
    for (let s = 0; s < n; s++) {
      const f = S.fateOf(s, m);
      deliveries += S.fateDeliveries(f, m.maxAttempts);
      if (f === S.FATE.FAIL) fails++;
    }
    close(deliveries / n, m.deliveries, 0.0005, 'deliveries per message');
    close(fails / n, m.dlq, 0.0002, 'dead-letter rate');
  }
});

test('settle ops walk a failing message through every attempt', () => {
  const m = { maxAttempts: 6, retryDelayMs: 10_000 };
  for (let a = 1; a < 6; a++) assert.deepEqual(S.settleOp(S.FATE.FAIL, a, m), { op: 'retry', delayMs: 10_000, final: false });
  assert.deepEqual(S.settleOp(S.FATE.FAIL, 6, m), { op: 'retry', delayMs: 10_000, final: true, terminal: 'failed' });
  assert.equal(S.settleOp(S.FATE.FAIL, 6, m, 'fail').op, 'fail');
  assert.equal(S.settleOp(S.FATE.RETRY_ONCE, 1, m).op, 'retry');
  assert.deepEqual(S.settleOp(S.FATE.RETRY_ONCE, 2, m), { op: 'ack', final: true, terminal: 'acked' });
  assert.equal(S.settleOp(S.FATE.ACK, 1, m, 'retry', 0.001).op, 'ack');
  assert.equal(S.settleOp(S.FATE.FAIL, 1, m, 'retry', 0.001).delayMs, 10);
});

test('bodies serialize to the scenario average size', () => {
  for (const n of [43, 68, 161, 423]) {
    assert.equal(JSON.stringify(S.makeBody(17, 123_456, 2, n)).length, n);
  }
  // Too small to pad: the minimal body.
  assert.equal(S.makeBody(1, 1, 0, 0).p, '');
});

test('placement: spread compresses, fixed delays do not', () => {
  const stripe = S.SHAPES['cp-stripe-usage-records-sync'][0];
  const vals = [];
  for (let s = 0; s < 1000; s++) vals.push(S.placement(stripe, s, 1_000, 10).deliverAt - 1_000);
  assert.ok(Math.min(...vals) >= 0 && Math.max(...vals) < 8 * 60_000);
  close(vals.reduce((a, b) => a + b, 0) / vals.length, 4 * 60_000, 6_000, 'mean spread');
  assert.deepEqual(S.placement(S.SHAPES['cp-vercel-usage-sync'][0], 3, 0, 10), { delayMs: 120_000 });
  const inv = S.SHAPES['mkp-vercel-submit-invoice'][0];
  assert.deepEqual([0, 1, 2].map((s) => S.placement(inv, s, 0, 1).delayMs), [5_000, 60_000, 5_000]);
});

test('consumer loops cover twice the peak rate, within the cap', () => {
  const q = (name) => scenario.queues.find((x) => x.name === name);
  assert.equal(S.consumerLoops(q('cp-stripe-usage-records-sync'), 1), 15);
  assert.equal(S.consumerLoops(q('cp-ppg-operations'), 1), 2);
  assert.equal(S.consumerLoops(q('cp-stripe-usage-records-sync'), 100), 64);
  assert.equal(S.consumerLoops(q('cp-stripe-usage-records-sync'), 10), 15);
});

test('queue config mirrors the scenario and sizes budgets to the backlog', () => {
  const stripe = scenario.queues.find((x) => x.name === 'cp-stripe-usage-records-sync');
  const c1 = S.queueConfig(stripe, 1);
  assert.equal(c1.group.maxAttempts, 11);
  assert.deepEqual(c1.group.backoff, { initialMs: 30_000, maxMs: 30_000, multiplier: 1 });
  assert.equal(c1.maxQueuedMessages, 1_000_000);
  const c100 = S.queueConfig(stripe, 100);
  // 2 x 937.6/s x 80 min of spread = 9.0 M messages.
  close(c100.maxQueuedMessages, 2 * (33_752 * 100 / 3600) * 4800, 2, 'backlog at 100x');
  const enforce = scenario.queues.find((x) => x.name === 'cp-spend-limit-enforce');
  assert.equal(S.queueConfig(enforce, 1).group.backoff.initialMs, 1000);
});

test('the PRNG is deterministic and handler latency stays in 50-500 ms', () => {
  const a = S.rng(7);
  const b = S.rng(7);
  for (let i = 0; i < 1000; i++) {
    const x = S.handlerLatency(a);
    assert.equal(x, S.handlerLatency(b));
    assert.ok(x >= 50 && x <= 500);
  }
});
