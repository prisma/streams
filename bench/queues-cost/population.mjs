#!/usr/bin/env node
// Many tiny queues, each touched rarely (habanero's `--mode population`,
// kind queue; tuning plan 2026-11-03, item 4 and section 3 rung 4): it
// creates QUEUES queues, then touches them at RATE touches a second, each
// touch on the next queue of a fixed-step walk so every queue is touched
// once before any twice. A touch is one send, a claim of at most one
// message without waiting, and the ack of what it leased. With QUEUES /
// RATE seconds between touches of one queue, every touch is cold once the
// walk is longer than the server keeps a queue's rows hot.
//
//   node bench/queues-cost/population.mjs --base http://127.0.0.1:8090 \
//     --auth-file <file> --key-file <file> --queues 100000 --rate 100 \
//     --duration-s 600 --out pop.json
//
// The summary: touches done and failed, the latency of each call and of
// the whole touch, and the touch rate held (a rate below RATE means the
// server did not keep up). Exit 0 when no touch failed, 1 otherwise.
import { readFileSync, writeFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { Hist } from './replay.mjs';

const USAGE = `usage: population.mjs --auth-file F --key-file F [options]
  --base URL             server origin (http://127.0.0.1:8090)
  --prefix P             queue name prefix (pop)
  --queues N             queues to create and touch (1000)
  --create-concurrency N creates in flight (64)
  --skip-create          the queues exist: touch only
  --rate R               touches a second (10)
  --workers N            touches in flight at most (256)
  --duration-s N         seconds of touching (60)
  --payload-bytes N      body size of each message (400)
  --out PATH             summary JSON (population-summary.json)
  --progress-s N         progress line every N s on stderr (10)`;

export function parseArgs(argv) {
  const o = {
    base: 'http://127.0.0.1:8090', prefix: 'pop', queues: 1000, createConcurrency: 64, skipCreate: false,
    rate: 10, workers: 256, durationS: 60, payloadBytes: 400, out: 'population-summary.json', progressS: 10,
    authFile: process.env.QUEUES_AUTH_FILE, keyFile: process.env.QUEUES_KEY_FILE,
  };
  const num = (v, k) => {
    const n = Number(v);
    if (!Number.isFinite(n) || n < 0) throw new Error(`${k} needs a non-negative number, got ${v}`);
    return n;
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const v = () => {
      if (i + 1 >= argv.length) throw new Error(`${a} needs a value`);
      return argv[++i];
    };
    switch (a) {
      case '--base': o.base = v(); break;
      case '--prefix': o.prefix = v(); break;
      case '--queues': o.queues = num(v(), a); break;
      case '--create-concurrency': o.createConcurrency = num(v(), a); break;
      case '--skip-create': o.skipCreate = true; break;
      case '--rate': o.rate = num(v(), a); break;
      case '--workers': o.workers = num(v(), a); break;
      case '--duration-s': o.durationS = num(v(), a); break;
      case '--payload-bytes': o.payloadBytes = num(v(), a); break;
      case '--out': o.out = v(); break;
      case '--progress-s': o.progressS = num(v(), a); break;
      case '--auth-file': o.authFile = v(); break;
      case '--key-file': o.keyFile = v(); break;
      case '-h': case '--help': throw new Error(USAGE);
      default: throw new Error(`unknown argument ${a}\n${USAGE}`);
    }
  }
  if (!Number.isInteger(o.queues) || o.queues < 1) throw new Error('--queues needs a positive integer');
  if (o.rate <= 0) throw new Error('--rate must be positive');
  return o;
}

/** A step coprime with `n`, near n times the golden ratio, so consecutive
 * touches land far apart and every queue comes once per n touches. */
export function walkStep(n) {
  const gcd = (a, b) => (b === 0 ? a : gcd(b, a % b));
  let step = Math.max(1, Math.round(n * 0.6180339887)) % n || 1;
  while (gcd(step, n) !== 1) step = (step + 1) % n || 1;
  return step;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export async function runPopulation(o, log = (s) => process.stderr.write(`${s}\n`)) {
  const token = (o.token ?? readFileSync(o.authFile, 'utf8')).trim();
  const key = (o.key ?? readFileSync(o.keyFile, 'utf8')).trim();
  const headers = {
    authorization: `Bearer ${token}`, 'content-type': 'application/json', 'streams-encryption-key': key,
  };
  const path = (i) => `${o.base}/v1/queues/${encodeURIComponent(`${o.prefix}-${i}`)}`;
  async function call(method, url, body) {
    const t0 = performance.now();
    const res = await fetch(url, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
    const text = await res.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = null;
    }
    return { status: res.status, json, ms: performance.now() - t0 };
  }
  const errors = { count: 0, by_kind: {}, samples: [] };
  const error = (kind, detail) => {
    errors.count++;
    errors.by_kind[kind] = (errors.by_kind[kind] ?? 0) + 1;
    if (errors.samples.length < 20) errors.samples.push({ t: Date.now(), kind, detail: String(detail).slice(0, 300) });
  };

  // ---- create ------------------------------------------------------------
  const created = { ok: 0, failed: 0, ms: 0 };
  if (!o.skipCreate) {
    const t0 = performance.now();
    let next = 0;
    const creator = async () => {
      while (next < o.queues) {
        const i = next++;
        for (let tries = 0; ; tries++) {
          let r;
          try {
            r = await call('PUT', path(i), {});
          } catch (e) {
            r = { status: 0, json: { error: { code: String(e) } } };
          }
          if (r.status === 200 || r.status === 201) {
            created.ok++;
            break;
          }
          if (tries < 5 && (r.status === 0 || r.status === 429 || r.status === 503)) {
            await sleep(200 * (tries + 1));
            continue;
          }
          created.failed++;
          error(`create_${r.status}_${r.json?.error?.code ?? ''}`, JSON.stringify(r.json));
          break;
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(o.createConcurrency, o.queues) }, creator));
    created.ms = performance.now() - t0;
    log(`population: created ${created.ok} of ${o.queues} in ${(created.ms / 1000).toFixed(1)} s (${created.failed} failed)`);
  }

  // ---- touch -------------------------------------------------------------
  const hist = { touch: new Hist(), send: new Hist(), claim: new Hist(), settle: new Hist() };
  const counts = { touches: 0, failed: 0, leased: 0, empty: 0, skipped: 0 };
  const payload = 'x'.repeat(o.payloadBytes);
  const step = walkStep(o.queues);
  let at = 0;
  let inFlight = 0;
  async function touch(i) {
    const t0 = performance.now();
    const fail = (kind, r) => {
      counts.failed++;
      error(`${kind}_${r.status}_${r.json?.error?.code ?? ''}`, JSON.stringify(r.json));
    };
    try {
      const s = await call('POST', `${path(i)}/messages`, { body: { p: payload } });
      if (s.status !== 200 && s.status !== 201) return fail('send', s);
      hist.send.add(s.ms);
      const c = await call('POST', `${path(i)}/groups/default:claim`, { max: 1, waitMs: 0 });
      if (c.status !== 200) return fail('claim', c);
      hist.claim.add(c.ms);
      const ops = (c.json?.messages ?? []).map((m) => ({ token: m.token, op: 'ack' }));
      if (ops.length === 0) {
        counts.empty++;
      } else {
        counts.leased += ops.length;
        const a = await call('POST', `${path(i)}/groups/default:settle`, { ops });
        if (a.status !== 200) return fail('settle', a);
        hist.settle.add(a.ms);
      }
      counts.touches++;
      hist.touch.add(performance.now() - t0);
    } catch (e) {
      counts.failed++;
      error('network', e);
    }
  }
  const pending = new Set();
  const start = performance.now();
  const end = start + o.durationS * 1000;
  let lastLog = start;
  for (let k = 0; ; k++) {
    const due = start + (k * 1000) / o.rate;
    if (due >= end) break;
    const wait = due - performance.now();
    if (wait > 0) await sleep(wait);
    if (inFlight >= o.workers) {
      counts.skipped++;
      continue;
    }
    const i = at;
    at = (at + step) % o.queues;
    inFlight++;
    const p = touch(i).finally(() => {
      inFlight--;
      pending.delete(p);
    });
    pending.add(p);
    const now = performance.now();
    if (o.progressS > 0 && now - lastLog >= o.progressS * 1000) {
      lastLog = now;
      log(`population: t=${((now - start) / 1000).toFixed(0)}s touches=${counts.touches} failed=${counts.failed} ` +
        `in_flight=${inFlight} skipped=${counts.skipped} touch_p99=${hist.touch.summary().p99}ms`);
    }
  }
  await Promise.all(pending);
  const secs = (performance.now() - start) / 1000;
  const summary = {
    ok: counts.failed === 0 && counts.skipped === 0 && created.failed === 0,
    options: { queues: o.queues, rate: o.rate, workers: o.workers, duration_s: o.durationS, payload_bytes: o.payloadBytes, step },
    created,
    counts,
    touch_rate: counts.touches / secs,
    latency_ms: Object.fromEntries(Object.entries(hist).map(([k, h]) => [k, h.summary()])),
    errors,
  };
  if (o.out) writeFileSync(o.out, `${JSON.stringify(summary, null, 2)}\n`);
  log(`population: ${summary.ok ? 'OK' : 'FAILED'} touches=${counts.touches} failed=${counts.failed} ` +
    `skipped=${counts.skipped} rate=${summary.touch_rate.toFixed(1)}/s touch_p99=${summary.latency_ms.touch.p99}ms`);
  return summary;
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  let o;
  try {
    o = parseArgs(process.argv.slice(2));
  } catch (e) {
    process.stderr.write(`${e.message}\n`);
    process.exit(2);
  }
  runPopulation(o).then((s) => process.exit(s.ok ? 0 : 1), (e) => {
    process.stderr.write(`population: ${e.stack ?? e}\n`);
    process.exit(1);
  });
}
