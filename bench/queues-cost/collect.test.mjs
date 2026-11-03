// node --test bench/queues-cost/  (field mode: `mark` and `sample` read the
// Compute wrapper's `cpu sample:` log and the server's /v1/debug/load)
import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import * as P from './prices.mjs';
import { cpuFromLog, cpuRegression } from './collect.mjs';

// fixtures/cpu-log.txt: `compute logs` output of one instance, as
// deploy/app-server/index.ts prints it with CPU_LOG_SECS=15. Boot 1762160000000
// logs 1.5, 4.25, 9.5 and 12 CPU-seconds, with one line repeated by an
// overlapping tail and one line cut off; the instance reboots
// (1762160071000) and logs 0.75 and 3.5. Lines 1-6 are the log as a mark
// taken at 09:00:50 saw it.
const FIXTURE = new URL('./fixtures/cpu-log.txt', import.meta.url);
const fixture = readFileSync(FIXTURE, 'utf8');
const firstLines = (n) => `${fixture.split('\n').slice(0, n).join('\n')}\n`;
const BOOT1 = '1762160000000';
const BOOT2 = '1762160071000';

test('the CPU log sums each boot\'s latest cumulative line', () => {
  assert.deepEqual(cpuFromLog(fixture), { cpu_s: 15.5, boots: { [BOOT1]: 12, [BOOT2]: 3.5 }, t_ms: 1762160101000 });
  assert.deepEqual(cpuFromLog(firstLines(6)), { cpu_s: 9.5, boots: { [BOOT1]: 9.5 }, t_ms: 1762160045000 });
  assert.equal(cpuFromLog('instance shape: {}\ncpu sample: {"boot_ms":1\n'), null);
  assert.equal(cpuFromLog(''), null);
});

test('a reboot inside a window adds both boots\' CPU, never a negative delta', () => {
  const start = cpuFromLog(firstLines(6));
  const end = cpuFromLog(fixture);
  assert.equal(cpuRegression(start.boots, end.boots), null);
  // The window's CPU: the rest of boot 1 (12 - 9.5) plus boot 2 (3.5). The
  // latest line alone would read 3.5 - 9.5 = -6.
  const w = P.windowTerms({ t: 0, cpu_s: start.cpu_s }, { t: 3_600_000, cpu_s: end.cpu_s }, [], []);
  assert.equal(w.cpu_s_uncapped, 6);
  // A log that lost a boot the earlier mark saw (cut or replaced) is refused.
  const cut = cpuFromLog(fixture.split('\n').slice(10).join('\n'));
  assert.deepEqual(cut.boots, { [BOOT2]: 3.5 });
  assert.equal(cpuRegression(end.boots, cut.boots), `boot ${BOOT1} of the previous mark is missing from the CPU log`);
  assert.equal(cpuRegression({ [BOOT2]: 4 }, cut.boots), `boot ${BOOT2} went back from 4 to 3.5 CPU-seconds`);
  assert.equal(cpuRegression(null, cut.boots), null);
});

const run = promisify(execFile);
const COLLECT = new URL('./collect.mjs', import.meta.url).pathname;

/** A /v1/debug/load that answers only the token, and counts its requests. */
async function loadServer(token, rssMb) {
  const seen = { load: 0, unauthorized: 0 };
  const srv = createServer((req, res) => {
    if (req.headers.authorization !== `Bearer ${token}`) {
      seen.unauthorized++;
      res.writeHead(401).end();
      return;
    }
    if (req.url === '/v1/debug/load') {
      seen.load++;
      res.writeHead(200, { 'content-type': 'application/json' })
        .end(JSON.stringify({ boot: { id: 'boot-b' }, local: { rss_mb: rssMb } }));
      return;
    }
    res.writeHead(200, { 'content-type': 'application/json' }).end('{}');
  });
  await new Promise((r) => srv.listen(0, '127.0.0.1', r));
  return { url: `http://127.0.0.1:${srv.address().port}`, seen, close: () => new Promise((r) => srv.close(r)) };
}

test('field mark: CPU from the log, RSS from /v1/debug/load, the token from its file', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'qfield-'));
  const token = 'field-token-not-on-argv';
  writeFileSync(join(dir, 'auth'), `${token}\n`);
  const srv = await loadServer(token, 312.5);
  try {
    const log = join(dir, 'cpu.log');
    const mark = (tag, extra = []) => run('node', [COLLECT, 'mark', '--dir', dir, '--tag', tag, '--cpu-log', log,
      '--server', srv.url, '--auth-file', join(dir, 'auth'), ...extra]);
    const old = ['--cpu-max-age-s', '1000000000'];
    writeFileSync(log, firstLines(6));
    await mark('start', old);
    writeFileSync(log, fixture);
    await mark('end', old);
    // The log went stale (its newest line is from 2025): refused.
    await mark('stale');
    writeFileSync(log, fixture.split('\n').slice(10).join('\n'));
    await mark('cut', old);
    const marks = readFileSync(join(dir, 'marks.jsonl'), 'utf8').trim().split('\n').map((l) => JSON.parse(l));
    const pick = (m) => [m.tag, m.cpu_s, m.rss_kb, m.ledger, m.cpu_source, m.cpu_boots, m.boot_id, m.cpu_error ?? null];
    assert.deepEqual(pick(marks[0]), ['start', 9.5, 320_000, false, 'log', { [BOOT1]: 9.5 }, 'boot-b', null]);
    assert.deepEqual(pick(marks[1]), ['end', 15.5, 320_000, false, 'log', { [BOOT1]: 12, [BOOT2]: 3.5 }, 'boot-b', null]);
    assert.equal(marks[1].cpu_t, 1762160101000);
    assert.equal(marks[2].cpu_s, null);
    assert.match(marks[2].cpu_error, /^newest cpu sample is \d+ s old \(--cpu-max-age-s 60\)$/);
    // The stale mark recorded no boots, so the cut log is checked against `end`.
    assert.equal(marks[3].cpu_s, null);
    assert.equal(marks[3].cpu_error, `boot ${BOOT1} of the previous mark is missing from the CPU log`);
    assert.equal(srv.seen.unauthorized, 0);
    assert.ok(srv.seen.load >= 4);
  } finally {
    await srv.close();
  }
});

test('field sample: CPU and RSS lines until SIGTERM; mark refuses a half field source', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'qfield-'));
  writeFileSync(join(dir, 'auth'), 'tok\n');
  writeFileSync(join(dir, 'cpu.log'), fixture);
  const srv = await loadServer('tok', 100);
  try {
    const child = execFile('node', [COLLECT, 'sample', '--dir', dir, '--cpu-log', join(dir, 'cpu.log'),
      '--server', srv.url, '--auth-file', join(dir, 'auth'), '--interval-s', '0.2']);
    const exited = new Promise((r) => child.on('exit', r));
    await new Promise((r) => setTimeout(r, 900));
    child.kill('SIGTERM');
    assert.equal(await exited, 0);
    const lines = readFileSync(join(dir, 'samples.jsonl'), 'utf8').trim().split('\n').map((l) => JSON.parse(l));
    assert.ok(lines.length >= 2, `${lines.length} samples`);
    for (const l of lines) assert.deepEqual([l.cpu_s, l.rss_kb, 'live_bytes' in l], [15.5, 102_400, false]);
    // --cpu-log without --server and --auth-file is no field source.
    await assert.rejects(run('node', [COLLECT, 'mark', '--dir', dir, '--tag', 'x', '--cpu-log', join(dir, 'cpu.log')]),
      /mark needs --dir --tag and --pid, or --cpu-log with --server --auth-file/);
  } finally {
    await srv.close();
  }
});
