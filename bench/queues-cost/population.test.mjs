// node --test bench/queues-cost/  (the population driver against the
// in-memory fake of the /v1/queues routes)
import test from 'node:test';
import assert from 'node:assert/strict';
import { startFakeQueues } from './fake-server.mjs';
import { parseArgs, runPopulation, walkStep } from './population.mjs';

const quiet = () => {};

test('the walk touches every queue once before any twice', () => {
  for (const n of [1, 2, 7, 10, 64, 1000, 100_000]) {
    const step = walkStep(n);
    const seen = new Set();
    let at = 0;
    for (let k = 0; k < n; k++) {
      seen.add(at);
      at = (at + step) % n;
    }
    assert.equal(seen.size, n, `n=${n} step=${step}`);
    assert.equal(at, 0, `n=${n}: the walk is back at the start after n touches`);
  }
});

test('queues are created, then each touch sends, claims one and acks it', async () => {
  const fake = await startFakeQueues();
  try {
    const o = parseArgs(['--base', fake.base, '--queues', '20', '--rate', '40', '--duration-s', '1',
      '--progress-s', '0', '--out', '']);
    o.token = fake.token;
    o.key = fake.key;
    const s = await runPopulation(o, quiet);
    assert.equal(s.ok, true, JSON.stringify(s.errors));
    assert.deepEqual(s.created, { ok: 20, failed: 0, ms: s.created.ms });
    assert.equal(s.counts.touches, 40);
    assert.deepEqual([s.counts.failed, s.counts.skipped, s.counts.empty], [0, 0, 0]);
    assert.equal(s.counts.leased, 40, 'each touch leases the message it sent');
    assert.equal(s.latency_ms.settle.n, 40);
  } finally {
    await fake.close();
  }
});

test('arguments parse and refuse nonsense', () => {
  assert.equal(parseArgs(['--queues', '5']).queues, 5);
  assert.equal(parseArgs(['--skip-create']).skipCreate, true);
  assert.throws(() => parseArgs(['--queues', '0']), /positive integer/);
  assert.throws(() => parseArgs(['--rate', '0']), /positive/);
  assert.throws(() => parseArgs(['--nope']), /unknown argument/);
});
