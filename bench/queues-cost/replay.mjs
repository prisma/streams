#!/usr/bin/env node
// Replays the queue scenario (docs/QUEUES-PRICING-BENCHMARK.md §3,
// bench/queues-cost/scenario.json) against a server's /v1/queues routes
// (the contract in ~/.streams-k2/analysis/queues/API.md), then checks that
// every message reached a terminal outcome or is legitimately still pending,
// and writes a JSON summary. Node 20+, no dependencies.
//
//   node bench/queues-cost/replay.mjs --base http://127.0.0.1:8090 \
//     --auth-file <file> --key-file <file> --scale 1 --duration-s 3600 \
//     --out summary.json
//
// Producers follow schedule.mjs (cron sweeps, bursts, delays, producer
// batches); per queue, consumer loops claim up to 10 (leaseMs 30000,
// waitMs 20000), sleep a handler latency of 50-500 ms per batch, and settle
// each message by its fate (ack, retry once, or retry to exhaustion), which
// the body carries so any consumer can settle any message.
//
// Exit status: 0 when the run is valid (no lost message, no unexpected
// error, server counters equal ours), 1 when it is void, 2 on usage errors.
import { appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import * as S from './schedule.mjs';

const HERE = new URL('.', import.meta.url);

const USAGE = `usage: replay.mjs --auth-file F --key-file F [options]
  --base URL            server origin (http://127.0.0.1:8090)
  --scenario PATH       scenario.json (beside this script)
  --scale S             rate multiplier (1)
  --compress K          run the day shape K times faster, same rates (1)
  --profile day|peak    day shape, or every queue at its busiest-hour rate (day)
  --start-hour H        virtual time of day at the start (0)
  --burst-at-s N        month-start invoice burst starts N s in (0)
  --no-invoice-burst    leave the month-start burst out
  --duration-s N        seconds of production (60)
  --drain-s N           at most N s to drain after production (900)
  --idle                consumers only, no sends (the idle run)
  --only a,b            replay these queues only (all queues are created)
  --prefix P            server queue name prefix (qb-)
  --final-op retry|fail last attempt of a failing message (retry)
  --send-concurrency N  producer requests in flight (32)
  --max-consumers N     consumer loops per queue at most (64)
  --claim-max N         messages per claim (10)
  --lease-ms N          (30000)    --wait-ms N  long-poll wait (20000)
  --handler-ms LO,HI    handler latency per batch (50,500)
  --delay-scale X       scale fixed and retry delays; smoke tests only (1)
  --seed N              handler-latency PRNG seed (1)
  --progress-s N        progress line every N s on stderr (60)
  --out PATH            summary JSON (replay-summary.json)
  --timeline PATH       cumulative counters every 10 s, JSONL (none)
  --dry-run             print the plan and expected sends; no requests`;

export function parseArgs(argv) {
  const o = {
    base: 'http://127.0.0.1:8090', scenario: new URL('scenario.json', HERE).pathname,
    scale: 1, compress: 1, profile: 'day', startHour: 0, burstAtS: 0, invoiceBurst: undefined,
    durationS: 60, drainS: 900, idle: false, only: null, prefix: 'qb-', finalOp: 'retry',
    sendConcurrency: 32, maxConsumers: 64, claimMax: 10, leaseMs: 30_000, waitMs: 20_000,
    handlerMs: [50, 500], delayScale: 1, seed: 1, progressS: 60, out: 'replay-summary.json',
    timeline: null, dryRun: false, authFile: process.env.QUEUES_AUTH_FILE, keyFile: process.env.QUEUES_KEY_FILE,
  };
  const num = (v, k) => {
    const n = Number(v);
    if (!Number.isFinite(n)) throw new Error(`${k} needs a number, got ${v}`);
    return n;
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const v = () => {
      if (i + 1 >= argv.length) throw new Error(`${a} needs a value`);
      return argv[++i];
    };
    switch (a) {
      case '--base': o.base = v().replace(/\/$/, ''); break;
      case '--scenario': o.scenario = v(); break;
      case '--auth-file': o.authFile = v(); break;
      case '--key-file': o.keyFile = v(); break;
      case '--scale': o.scale = num(v(), a); break;
      case '--compress': o.compress = num(v(), a); break;
      case '--profile': o.profile = v(); break;
      case '--start-hour': o.startHour = num(v(), a); break;
      case '--burst-at-s': o.burstAtS = num(v(), a); break;
      case '--no-invoice-burst': o.invoiceBurst = false; break;
      case '--duration-s': o.durationS = num(v(), a); break;
      case '--drain-s': o.drainS = num(v(), a); break;
      case '--idle': o.idle = true; break;
      case '--only': o.only = v().split(',').filter(Boolean); break;
      case '--prefix': o.prefix = v(); break;
      case '--final-op': o.finalOp = v(); break;
      case '--send-concurrency': o.sendConcurrency = num(v(), a); break;
      case '--max-consumers': o.maxConsumers = num(v(), a); break;
      case '--claim-max': o.claimMax = num(v(), a); break;
      case '--lease-ms': o.leaseMs = num(v(), a); break;
      case '--wait-ms': o.waitMs = num(v(), a); break;
      case '--handler-ms': o.handlerMs = v().split(',').map((x) => num(x, a)); break;
      case '--delay-scale': o.delayScale = num(v(), a); break;
      case '--seed': o.seed = num(v(), a); break;
      case '--progress-s': o.progressS = num(v(), a); break;
      case '--out': o.out = v(); break;
      case '--timeline': o.timeline = v(); break;
      case '--dry-run': o.dryRun = true; break;
      case '-h': case '--help': throw new Error('help');
      default: throw new Error(`unknown argument ${a}`);
    }
  }
  if (!['day', 'peak'].includes(o.profile)) throw new Error('--profile is day or peak');
  if (!['retry', 'fail'].includes(o.finalOp)) throw new Error('--final-op is retry or fail');
  if (o.handlerMs.length !== 2 || o.handlerMs[0] > o.handlerMs[1]) throw new Error('--handler-ms LO,HI');
  return o;
}

/** Latency histogram: exact to 1 ms below 100 ms, 1% buckets above. */
export class Hist {
  constructor() {
    this.b = new Uint32Array(1400);
    this.n = 0;
    this.max = 0;
  }
  static idx(v) {
    if (v < 100) return Math.max(0, Math.round(v));
    return Math.min(1399, 100 + Math.ceil(Math.log(v / 100) / Math.log(1.01)));
  }
  static val(i) {
    return i <= 100 ? i : 100 * 1.01 ** (i - 100);
  }
  add(v) {
    this.b[Hist.idx(v)]++;
    this.n++;
    if (v > this.max) this.max = v;
  }
  q(p) {
    if (!this.n) return null;
    const want = Math.ceil(p * this.n);
    let seen = 0;
    for (let i = 0; i < this.b.length; i++) {
      seen += this.b[i];
      if (seen >= want) return Math.round(Hist.val(i) * 10) / 10;
    }
    return this.max;
  }
  summary() {
    return { n: this.n, p50: this.q(0.5), p99: this.q(0.99), max: Math.round(this.max) };
  }
}

// Message states per queue and sequence number.
const ST = { NONE: 0, SENT: 1, ACKED: 2, FAILED: 3, AMBIGUOUS: 4 };

class Tracker {
  constructor() {
    this.a = new Uint8Array(1024);
  }
  set(seq, st) {
    if (seq >= this.a.length) {
      const b = new Uint8Array(Math.max(this.a.length * 2, seq + 1));
      b.set(this.a);
      this.a = b;
    }
    this.a[seq] = st;
  }
  get(seq) {
    return seq < this.a.length ? this.a[seq] : ST.NONE;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function newCounts() {
  return {
    sent: 0, send_requests: 0, delivered: 0, delivered_bytes: 0, acked: 0, retried: 0, failed: 0,
    claims: 0, empty_claims: 0, stale: 0, expired: 0, throttled: 0, ambiguous: 0, refused: 0,
    redelivered_after_terminal: 0, unknown_delivered: 0,
  };
}

export async function runReplay(o, log = (s) => process.stderr.write(`${s}\n`)) {
  const scenario = JSON.parse(readFileSync(o.scenario, 'utf8'));
  const plan = S.buildPlan(scenario, {
    scale: o.scale, compress: o.compress, profile: o.profile, startHour: o.startHour,
    invoiceBurst: o.invoiceBurst, burstAtS: o.burstAtS, only: o.only,
  });
  const durationMs = o.durationS * 1000;
  if (o.dryRun) return dryRun(plan, o, durationMs);
  const token = (o.token ?? readFileSync(o.authFile, 'utf8')).trim();
  const key = (o.key ?? readFileSync(o.keyFile, 'utf8')).trim();
  const errors = { count: 0, by_kind: {}, samples: [] };
  const error = (kind, detail) => {
    errors.count++;
    errors.by_kind[kind] = (errors.by_kind[kind] ?? 0) + 1;
    if (errors.samples.length < 50) errors.samples.push({ t: Date.now(), kind, detail: String(detail).slice(0, 300) });
  };
  const hist = { send: new Hist(), settle: new Hist(), lag: new Hist(), send_lag: new Hist() };
  const random = S.rng(o.seed);

  async function call(method, path, body, { withKey = false, signal } = {}) {
    const headers = { authorization: `Bearer ${token}` };
    if (body !== undefined) headers['content-type'] = 'application/json';
    if (withKey) headers['streams-encryption-key'] = key;
    const t0 = performance.now();
    const res = await fetch(o.base + path, {
      method, headers, body: body === undefined ? undefined : JSON.stringify(body), signal,
    });
    const text = await res.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = null;
    }
    return { status: res.status, json, bytes: Buffer.byteLength(text), ms: performance.now() - t0 };
  }

  const qs = plan.queues.map((p) => ({
    plan: p, name: o.prefix + p.name, path: `/v1/queues/${encodeURIComponent(o.prefix + p.name)}`,
    model: p.fate, salt: p.index * 0.1, nextSeq: 0, track: new Tracker(), c: newCounts(),
    loops: S.consumerLoops(p.config, o.scale, { max: o.maxConsumers, claimMax: o.claimMax }),
  }));

  // ---- create ------------------------------------------------------------
  for (const q of qs) {
    const config = S.queueConfig(q.plan.config, o.scale, { leaseMs: o.leaseMs });
    const r = await call('PUT', q.path, config, { withKey: true });
    if (r.status !== 200 && r.status !== 201) {
      throw new Error(`create ${q.name}: HTTP ${r.status} ${JSON.stringify(r.json)}`);
    }
  }
  log(`replay: ${qs.length} queues created; ${qs.reduce((a, q) => a + q.loops, 0)} consumer loops; ` +
    `profile ${o.profile} scale ${o.scale} compress ${o.compress}${o.idle ? ' IDLE' : ''}`);

  // ---- consumers ---------------------------------------------------------
  let stopConsumers = false;
  const claimAbort = new AbortController();
  let settlesInFlight = 0;

  async function consume(q) {
    while (!stopConsumers) {
      let r;
      try {
        const signal = AbortSignal.any([claimAbort.signal, AbortSignal.timeout(o.waitMs + 30_000)]);
        r = await call('POST', `${q.path}/groups/default:claim`,
          { max: o.claimMax, leaseMs: o.leaseMs, waitMs: o.waitMs }, { withKey: true, signal });
      } catch (e) {
        if (stopConsumers) break;
        error('claim_network', e);
        await sleep(1000);
        continue;
      }
      if (r.status !== 200) {
        error(`claim_${r.status}_${r.json?.error?.code ?? ''}`, JSON.stringify(r.json));
        await sleep(1000);
        continue;
      }
      q.c.claims++;
      const msgs = r.json?.messages ?? [];
      if (!msgs.length) {
        q.c.empty_claims++;
        continue;
      }
      settlesInFlight++;
      try {
        await handleBatch(q, msgs, r.bytes);
      } finally {
        settlesInFlight--;
      }
    }
  }

  async function handleBatch(q, msgs, bytes) {
    const now = Date.now();
    q.c.delivered += msgs.length;
    q.c.delivered_bytes += bytes;
    if (o.idle) error('idle_delivery', `${q.name}: ${msgs.length} messages`);
    const plans = msgs.map((m) => {
      hist.lag.add(Math.max(0, now - (m.availableAt ?? now)));
      const seq = m.body?.s;
      if (m.body?.i !== q.plan.index || !Number.isInteger(seq)) {
        error('foreign_body', `${q.name}: ${JSON.stringify(m.body).slice(0, 100)}`);
        return { token: m.token, op: 'ack', final: true, seq: null };
      }
      const st = q.track.get(seq);
      if (st === ST.ACKED || st === ST.FAILED) {
        q.c.redelivered_after_terminal++;
        error('redelivered_after_terminal', `${q.name} seq ${seq}`);
      } else if (seq >= q.nextSeq) {
        q.c.unknown_delivered++;
        error('unknown_delivered', `${q.name} seq ${seq}`);
      }
      const s = S.settleOp(m.body.f, m.attempt, q.model, o.finalOp, o.delayScale);
      return { ...s, token: m.token, seq };
    });
    await sleep(S.handlerLatency(random, o.handlerMs[0], o.handlerMs[1]));
    const ops = plans.map((p) => (p.op === 'ack'
      ? { token: p.token, op: 'ack' }
      : p.op === 'fail'
        ? { token: p.token, op: 'fail', code: p.code }
        : { token: p.token, op: 'retry', delayMs: p.delayMs, code: 'replay_retry' }));
    for (let tries = 0; ; tries++) {
      let r;
      try {
        r = await call('POST', `${q.path}/groups/default:settle`, { ops });
      } catch (e) {
        error('settle_network', e);
        if (tries < 3) {
          await sleep(500);
          continue;
        }
        return;
      }
      hist.settle.add(r.ms);
      if (r.status !== 200) {
        error(`settle_${r.status}_${r.json?.error?.code ?? ''}`, JSON.stringify(r.json));
        if (r.json?.error?.retryable && tries < 3) {
          await sleep(500);
          continue;
        }
        return;
      }
      const results = r.json?.results ?? [];
      plans.forEach((p, i) => {
        const outcome = results[i]?.outcome;
        if (outcome !== 'ok') {
          if (outcome === 'stale') q.c.stale++;
          else if (outcome === 'expired') q.c.expired++;
          error(`settle_outcome_${outcome}`, `${q.name} seq ${p.seq}`);
          return;
        }
        if (p.seq === null) return;
        if (!p.final) q.c.retried++;
        else if (p.terminal === 'acked') {
          q.c.acked++;
          q.track.set(p.seq, ST.ACKED);
        } else {
          q.c.failed++;
          q.track.set(p.seq, ST.FAILED);
        }
      });
      return;
    }
  }

  const consumers = [];
  for (const q of qs) for (let i = 0; i < q.loops; i++) consumers.push(consume(q));

  // ---- producers ---------------------------------------------------------
  const jobs = [];
  let jobHead = 0;
  let sendsInFlight = 0;
  const t0 = Date.now();

  // Cumulative counters for collect.mjs, which interpolates them at its
  // window marks (delivered bytes are the delivery-egress term).
  const TIMELINE_KEYS = ['sent', 'delivered', 'delivered_bytes', 'acked', 'retried', 'failed', 'claims', 'empty_claims'];
  const timelinePoint = () => {
    if (!o.timeline) return;
    const p = { t: Date.now() };
    for (const k of TIMELINE_KEYS) p[k] = qs.reduce((a, q) => a + q.c[k], 0);
    p.errors = errors.count;
    appendFileSync(o.timeline, `${JSON.stringify(p)}\n`);
  };
  timelinePoint();
  const timelineTimer = setInterval(timelinePoint, 10_000);

  async function send(job) {
    const q = qs[job.queue];
    const comp = q.plan.components[job.component];
    const first = q.nextSeq;
    q.nextSeq += job.n;
    const now = Date.now();
    hist.send_lag.add(now - job.at);
    const msgs = [];
    for (let k = 0; k < job.n; k++) {
      const seq = first + k;
      const fate = S.fateOf(seq, q.model, q.salt);
      msgs.push({
        body: S.makeBody(q.plan.index, seq, fate, q.plan.avgBytes),
        ...S.placement(comp, seq, now, o.compress, o.delayScale),
      });
    }
    const single = job.n === 1;
    for (let tries = 0; ; tries++) {
      let r;
      try {
        r = single
          ? await call('POST', `${q.path}/messages`, msgs[0], { withKey: true })
          : await call('POST', `${q.path}/messages:batch`, { messages: msgs }, { withKey: true });
      } catch (e) {
        // Outcome unknown: the server may hold these messages.
        for (let k = 0; k < job.n; k++) q.track.set(first + k, ST.AMBIGUOUS);
        q.c.ambiguous += job.n;
        error('send_network', e);
        return;
      }
      q.c.send_requests++;
      hist.send.add(r.ms);
      if (r.status === 200 || r.status === 201) {
        for (let k = 0; k < job.n; k++) q.track.set(first + k, ST.SENT);
        q.c.sent += job.n;
        const dups = single ? (r.json?.duplicate ? 1 : 0) : (r.json?.results ?? []).filter((x) => x.duplicate).length;
        if (dups) error('send_duplicate', `${q.name}: ${dups}`);
        return;
      }
      const retryable = r.json?.error?.retryable || r.status === 503;
      if (retryable && tries < 30) {
        q.c.throttled++;
        await sleep(Math.min(5000, 100 * 2 ** Math.min(tries, 6)));
        continue;
      }
      q.c.refused += job.n;
      error(`send_${r.status}_${r.json?.error?.code ?? ''}`, JSON.stringify(r.json));
      return;
    }
  }

  async function pump() {
    while (jobHead < jobs.length && sendsInFlight < o.sendConcurrency) {
      const job = jobs[jobHead++];
      sendsInFlight++;
      send(job).finally(() => {
        sendsInFlight--;
        pump();
      });
    }
    if (jobHead > 100_000) {
      jobs.splice(0, jobHead);
      jobHead = 0;
    }
  }

  let stopProducing = false;
  const onSignal = () => {
    log('replay: signal, stopping production');
    stopProducing = true;
  };
  process.once('SIGINT', onSignal);
  process.once('SIGTERM', onSignal);

  let lastProgress = Date.now();
  const progress = (phase) => {
    const t = qs.reduce((a, q) => {
      for (const k of Object.keys(a)) a[k] += q.c[k];
      return a;
    }, { sent: 0, delivered: 0, acked: 0, failed: 0, claims: 0 });
    log(`replay ${phase} t=${Math.round((Date.now() - t0) / 1000)}s sent=${t.sent} delivered=${t.delivered} ` +
      `acked=${t.acked} failed=${t.failed} claims=${t.claims} backlog_jobs=${jobs.length - jobHead} errors=${errors.count}`);
  };

  if (!o.idle) {
    let rPrev = 0;
    while (!stopProducing) {
      const r = Math.min(durationMs, Date.now() - t0);
      for (const e of S.eventsBetween(plan, rPrev, r)) {
        const comp = plan.queues[e.queue].components[e.component];
        for (const n of S.batches(e.count, comp.batch)) jobs.push({ queue: e.queue, component: e.component, n, at: t0 + r });
      }
      rPrev = r;
      pump();
      if (o.progressS > 0 && Date.now() - lastProgress >= o.progressS * 1000) {
        progress('produce');
        lastProgress = Date.now();
      }
      if (r >= durationMs) break;
      await sleep(100);
    }
    while (jobHead < jobs.length || sendsInFlight > 0) {
      pump();
      await sleep(50);
    }
  } else {
    while (!stopProducing && Date.now() - t0 < durationMs) {
      await sleep(Math.min(1000, durationMs - (Date.now() - t0)));
      if (o.progressS > 0 && Date.now() - lastProgress >= o.progressS * 1000) {
        progress('idle');
        lastProgress = Date.now();
      }
    }
  }
  const produceEnd = Date.now();

  // ---- drain ---------------------------------------------------------------
  async function serverStats() {
    const out = [];
    for (const q of qs) {
      try {
        const r = await call('GET', q.path);
        out.push(r.status === 200 ? r.json?.stats ?? null : null);
      } catch {
        out.push(null);
      }
    }
    return out;
  }
  let drain = { state: 'skipped', s: 0 };
  if (!o.idle) {
    drain = { state: 'timeout', s: 0 };
    const deadline = Date.now() + o.drainS * 1000;
    while (Date.now() < deadline && !stopProducing) {
      const st = await serverStats();
      const quiet = st.every((s) => s && s.due === 0 && s.leased === 0 && (s.retrying ?? 0) === 0);
      if (quiet && settlesInFlight === 0) {
        drain.state = 'quiet';
        break;
      }
      if (o.progressS > 0 && Date.now() - lastProgress >= o.progressS * 1000) {
        progress('drain');
        lastProgress = Date.now();
      }
      await sleep(1000);
    }
    drain.s = Math.round((Date.now() - produceEnd) / 1000);
    if (drain.state === 'timeout') {
      const st = await serverStats();
      drain.due_or_leased = st.every((s) => s && s.due === 0 && s.leased === 0) ? 'none' : 'some';
    }
  }

  // ---- stop and verify -----------------------------------------------------
  stopConsumers = true;
  claimAbort.abort();
  await Promise.allSettled(consumers);
  clearInterval(timelineTimer);
  timelinePoint();
  process.removeListener('SIGINT', onSignal);
  process.removeListener('SIGTERM', onSignal);
  await sleep(200);
  const finalStats = await serverStats();
  const summary = verify(qs, finalStats, { o, errors, hist, drain, t0, produceEnd, plan });
  writeFileSync(o.out, `${JSON.stringify(summary, null, 2)}\n`);
  log(`replay: ${summary.ok ? 'VALID' : 'VOID'} sent=${summary.totals.sent} acked=${summary.totals.acked} ` +
    `failed=${summary.totals.failed} pending=${summary.totals.pending} lost=${summary.totals.lost} ` +
    `errors=${errors.count}${summary.void_reasons.length ? ` reasons=${summary.void_reasons.join('; ')}` : ''}`);
  return summary;
}

/** Compare our ledger with the server's per queue; void reasons if any. */
export function verify(qs, finalStats, ctx) {
  const reasons = [];
  const totals = { ...newCounts(), pending: 0, server_nonterminal: 0, lost: 0 };
  const queues = qs.map((q, i) => {
    const s = finalStats[i];
    const c = q.c;
    const pending = c.sent - c.acked - c.failed;
    const row = { name: q.name, loops: q.loops, ...c, pending, server: s };
    for (const k of Object.keys(c)) totals[k] += c[k];
    totals.pending += pending;
    const checks = {};
    if (!s || typeof s.due !== 'number') {
      checks.server_stats = false;
      reasons.push(`${q.name}: no server stats`);
    } else {
      const nonterminal = s.due + s.scheduled + s.leased + (s.retrying ?? 0);
      totals.server_nonterminal += nonterminal;
      // Messages whose send outcome is unknown may or may not be held.
      const lost = pending - nonterminal;
      row.lost = lost;
      totals.lost += Math.max(0, lost);
      checks.none_lost = lost <= 0 && lost >= -c.ambiguous;
      checks.failed_match = s.failed === c.failed;
      if (s.counters) {
        checks.sent_match = s.counters.sent === c.sent || (c.ambiguous > 0 && s.counters.sent <= c.sent + c.ambiguous);
        checks.acked_match = s.counters.acked === c.acked;
      }
      for (const [k, v] of Object.entries(checks)) if (!v) reasons.push(`${q.name}: ${k}`);
    }
    // Scenario rates over terminal messages, gated once there are enough.
    const terminal = c.acked + c.failed;
    if (terminal > 0) {
      row.observed = {
        dlq_rate: c.failed / terminal,
        deliveries_per_terminal: c.delivered / terminal,
      };
      row.expected = { dlq_rate: q.model.dlq, deliveries_per_message: q.model.deliveries };
      if (terminal >= 1000 && ctx.drain.state === 'quiet') {
        checks.dlq_rate = Math.abs(row.observed.dlq_rate - q.model.dlq) <= 0.02;
        if (!checks.dlq_rate) reasons.push(`${q.name}: dlq rate ${row.observed.dlq_rate.toFixed(4)}`);
      }
    }
    row.checks = checks;
    return row;
  });
  if (ctx.errors.count > 0) reasons.push(`${ctx.errors.count} unexpected errors`);
  if (ctx.drain.state === 'timeout' && ctx.drain.due_or_leased !== 'none') reasons.push('drain timed out with due or leased messages');
  const o = ctx.o;
  return {
    ok: reasons.length === 0,
    void_reasons: reasons,
    mode: o.idle ? 'idle' : 'replay',
    options: {
      base: o.base, scale: o.scale, compress: o.compress, profile: o.profile, start_hour: o.startHour,
      burst_at_s: o.burstAtS, invoice_burst: ctx.plan.queues.some((q) => q.components.some((c) => c.kind === 'monthStart')),
      duration_s: o.durationS, drain_s: o.drainS, final_op: o.finalOp, claim_max: o.claimMax,
      lease_ms: o.leaseMs, wait_ms: o.waitMs, handler_ms: o.handlerMs, delay_scale: o.delayScale,
      seed: o.seed, only: o.only, prefix: o.prefix,
    },
    started_at: new Date(ctx.t0).toISOString(),
    produce_end_at: new Date(ctx.produceEnd).toISOString(),
    ended_at: new Date().toISOString(),
    drain: ctx.drain,
    totals,
    latency_ms: Object.fromEntries(Object.entries(ctx.hist).map(([k, h]) => [k, h.summary()])),
    errors: ctx.errors,
    queues,
  };
}

function dryRun(plan, o, durationMs) {
  const sends = S.expectedSends(plan, durationMs, Math.min(60_000, Math.max(1000, durationMs / 1000)));
  const rows = plan.queues.map((q) => ({
    name: q.name, components: q.components.map((c) => c.kind).join('+') || '-',
    expected_sends: sends[q.index], loops: S.consumerLoops(q.config, o.scale, { max: o.maxConsumers, claimMax: o.claimMax }),
    dlq: q.fate.dlq, deliveries: q.fate.deliveries, p_retry: q.fate.pRetry,
  }));
  const total = sends.reduce((a, b) => a + b, 0);
  return { dry_run: true, profile: o.profile, scale: o.scale, compress: o.compress, duration_s: o.durationS,
    expected_sends: total, per_second: total / o.durationS, queues: rows };
}

async function main() {
  let o;
  try {
    o = parseArgs(process.argv.slice(2));
    if (!o.dryRun && (!o.authFile || !o.keyFile)) throw new Error('--auth-file and --key-file are required');
  } catch (e) {
    process.stderr.write(`${e.message === 'help' ? '' : `replay: ${e.message}\n`}${USAGE}\n`);
    process.exit(2);
  }
  const summary = await runReplay(o);
  if (o.dryRun) {
    process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
    return;
  }
  process.exitCode = summary.ok ? 0 : 1;
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  main().catch((e) => {
    process.stderr.write(`replay: ${e.stack ?? e}\n`);
    process.exit(1);
  });
}
