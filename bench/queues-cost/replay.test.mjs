// node --test bench/queues-cost/  (the replay end to end against the
// in-memory fake of the /v1/queues routes; the real server is not needed)
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startFakeQueues } from './fake-server.mjs';
import { runReplay, parseArgs, Hist } from './replay.mjs';

const quiet = () => {};

function opts(fake, extra = []) {
  const dir = mkdtempSync(join(tmpdir(), 'qreplay-'));
  const o = parseArgs([
    '--base', fake.base, '--compress', '1440', '--start-hour', '9.9', '--duration-s', '4',
    '--drain-s', '30', '--delay-scale', '0.001', '--handler-ms', '1,5', '--wait-ms', '200',
    '--progress-s', '0', '--out', join(dir, 'summary.json'), '--timeline', join(dir, 'timeline.jsonl'), ...extra,
  ]);
  o.token = fake.token;
  o.key = fake.key;
  return o;
}

test('a compressed day slice replays, settles every fate and verifies clean', async () => {
  const fake = await startFakeQueues();
  try {
    const o = opts(fake);
    const s = await runReplay(o, quiet);
    assert.deepEqual(s.void_reasons, []);
    assert.equal(s.ok, true);
    assert.ok(s.totals.sent > 20, `sent ${s.totals.sent}`);
    assert.equal(s.errors.count, 0);
    assert.equal(s.totals.lost, 0);
    const inv = s.queues.find((q) => q.name === 'qb-mkp-vercel-submit-invoice');
    // The month-start burst at 1,440x: 1.7 a second for 4 s, 36.5% failing
    // after all six attempts, the rest acked at once.
    assert.ok(inv.sent >= 5, `invoice sent ${inv.sent}`);
    assert.ok(inv.failed >= 1, `invoice failed ${inv.failed}`);
    assert.equal(inv.server.failed, inv.failed);
    assert.equal(inv.delivered, inv.acked + 6 * inv.failed);
    // The 10:00 notification burst fell inside the slice (9:54-11:30 virtual).
    const notif = s.queues.find((q) => q.name === 'qb-cp-notification-controller');
    assert.ok(notif.sent > 0);
    for (const q of s.queues) {
      assert.equal(q.server.counters.sent, q.sent, q.name);
      assert.equal(q.server.counters.acked, q.acked, q.name);
      assert.equal(q.pending, q.server.due + q.server.scheduled + q.server.leased + q.server.retrying, q.name);
    }
    assert.ok(s.latency_ms.send.n > 0 && s.latency_ms.settle.n > 0);
    // The timeline starts at zero and ends at the summary's totals.
    const tl = readFileSync(o.timeline, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
    assert.ok(tl.length >= 2);
    assert.equal(tl[0].sent, 0);
    assert.equal(tl.at(-1).sent, s.totals.sent);
    assert.equal(tl.at(-1).delivered_bytes, s.totals.delivered_bytes);
  } finally {
    await fake.close();
  }
});

test('a message the server drops is reported lost and voids the run', async () => {
  const fake = await startFakeQueues({ dropSeq: { queue: 'qb-mkp-vercel-send-billing-data', seq: 3 } });
  try {
    const s = await runReplay(opts(fake, ['--only', 'mkp-vercel-send-billing-data']), quiet);
    assert.equal(s.ok, false);
    const q = s.queues.find((x) => x.name === 'qb-mkp-vercel-send-billing-data');
    assert.equal(q.lost, 1);
    assert.equal(s.totals.lost, 1);
    assert.ok(s.void_reasons.includes('qb-mkp-vercel-send-billing-data: none_lost'));
    assert.ok(s.void_reasons.includes('qb-mkp-vercel-send-billing-data: sent_match'));
  } finally {
    await fake.close();
  }
});

test('the idle run sends nothing and only claims empty', async () => {
  const fake = await startFakeQueues();
  try {
    const s = await runReplay(opts(fake, ['--idle', '--duration-s', '1']), quiet);
    assert.equal(s.ok, true);
    assert.equal(s.mode, 'idle');
    assert.equal(s.totals.sent, 0);
    assert.equal(s.totals.delivered, 0);
    assert.ok(s.totals.claims > 0);
    assert.equal(s.totals.claims, s.totals.empty_claims);
  } finally {
    await fake.close();
  }
});

test('the explicit fail op dead-letters at the last attempt too', async () => {
  const fake = await startFakeQueues();
  try {
    const s = await runReplay(opts(fake, ['--only', 'mkp-vercel-submit-invoice', '--final-op', 'fail']), quiet);
    assert.equal(s.ok, true, s.void_reasons.join('; '));
    const inv = s.queues.find((q) => q.name === 'qb-mkp-vercel-submit-invoice');
    assert.ok(inv.failed >= 1);
    assert.equal(inv.server.failed, inv.failed);
  } finally {
    await fake.close();
  }
});

test('the histogram is exact below 100 ms and within 1% above', () => {
  const h = new Hist();
  for (let i = 1; i <= 100; i++) h.add(i);
  assert.equal(h.q(0.5), 50);
  const g = new Hist();
  for (let i = 0; i < 1000; i++) g.add(1000 + i);
  const p99 = g.q(0.99);
  assert.ok(Math.abs(p99 - 1990) / 1990 < 0.011, `p99 ${p99}`);
  assert.equal(new Hist().q(0.5), null);
});

test('arguments parse and refuse nonsense', () => {
  assert.equal(parseArgs(['--scale', '10']).scale, 10);
  assert.deepEqual(parseArgs(['--handler-ms', '50,500']).handlerMs, [50, 500]);
  assert.throws(() => parseArgs(['--profile', 'week']), /day or peak/);
  assert.throws(() => parseArgs(['--scale']), /needs a value/);
  assert.throws(() => parseArgs(['--bogus']), /unknown argument/);
});
