// The replay's schedule, rate and fate math (docs/QUEUES-PRICING-BENCHMARK.md
// §3), kept free of I/O so `node --test bench/queues-cost/` pins it.
//
// Time. The replay runs on a virtual clock v = v0 + K * r, where r is real
// milliseconds since the replay started, K the compression (--compress) and
// v0 the virtual time of day at the start (--start-hour). Everything that is
// a position in the day (cron firings, notification bursts, the deliverAt
// spread of a pass, the month-start invoice burst's length) happens K times
// sooner, and every event carries 1/K of its real-day volume, so the rate
// per real second stays the scenario's rate times the scale S. Compression
// changes cron spacing, never rates. Fixed per-message delays (10 s, 2 min,
// 5 s/60 s) and retry delays are per-message semantics and stay real.
//
// Volumes. A queue's day volume is D = writes_7d / 7 messages. At scale S
// and compression K a virtual day lasts 86,400 / K real seconds and carries
// D * S / K messages, so the rate per real second is D * S / 86,400 whatever
// K is. Event sizes are made exact over time with cumulative floors:
// count(k) = floor((k + 1) n) - floor(k n), so fractions never accumulate.

export const DAY_MS = 86_400_000;
export const HOUR_MS = 3_600_000;
export const MONTH_S = 30 * 86_400;
/** The scenario week's busiest hour, all queues (benchmark §3). */
export const BUSIEST_HOUR = 62_904;
/** Golden-ratio and silver-ratio Weyl steps: low-discrepancy, decorrelated. */
const PHI = 0.6180339887498949;
const SILVER = 0.41421356237309515;

const frac = (x) => x - Math.floor(x);

// Shapes per queue (benchmark §3 table and scenario.json `delay`). Values
// marked ASSUMPTION are not measured; the README lists them.
//   cron:       fires perDay times a day at phaseMin + i * day/perDay,
//               sending share * D per firing (batch: 'producer' = the
//               scenario's producer_batch)
//   steady:     share * D spread evenly over the day
//   monthStart: the invoice burst, once, at the queue's peak-hour rate
// Optional per component: delayMs (fixed), spreadMs (deliverAt uniform in
// [0, spreadMs), a position in the pass, compressed by K), delayCycle (the
// fixed delay alternates by sequence number).
export const SHAPES = {
  // 14 sweeps a day, 80-minute deliverAt spread, producer batches of 100.
  'cp-stripe-usage-records-sync': [
    { kind: 'cron', share: 1, perDay: 14, phaseMin: 0, batch: 'producer', spreadMs: 80 * 60_000 },
  ],
  // Hourly sweeps. ASSUMPTION: phases staggered 5-20 min so sweeps do not
  // coincide (the review records hourly, not the minute).
  'cp-spend-limit-usage-records-sync': [
    { kind: 'cron', share: 1, perDay: 24, phaseMin: 5, batch: 'producer' },
  ],
  // Hourly sweeps plus domain events, 10 s delay. ASSUMPTION: half each.
  'cp-offer-limit-enforce': [
    { kind: 'cron', share: 0.5, perDay: 24, phaseMin: 10, batch: 'producer', delayMs: 10_000 },
    { kind: 'steady', share: 0.5, batch: 1, delayMs: 10_000 },
  ],
  'cp-spend-limit-enforce': [
    { kind: 'cron', share: 1, perDay: 24, phaseMin: 15, batch: 'producer' },
  ],
  // 12 runs a day, 2-minute delay.
  'cp-vercel-usage-sync': [
    { kind: 'cron', share: 1, perDay: 12, phaseMin: 20, batch: 'producer', delayMs: 120_000 },
  ],
  // Campaign bursts at 10:00, 10:30 and 12:00; ASSUMPTION: weights
  // 0.45/0.45/0.05 and 5% steady, which puts 90% of the day in the 10:00
  // hour (measured: 17,618 of 19,661 a day, 89.6%).
  'cp-notification-controller': [
    { kind: 'cron', share: 0.45, perDay: 1, phaseMin: 600, batch: 'producer' },
    { kind: 'cron', share: 0.45, perDay: 1, phaseMin: 630, batch: 'producer' },
    { kind: 'cron', share: 0.05, perDay: 1, phaseMin: 720, batch: 'producer' },
    { kind: 'steady', share: 0.05, batch: 1 },
  ],
  // Month-start burst: 82% of the week's writes on one day, at the peak-hour
  // rate; the other 18% spread over the remaining six days. ASSUMPTION: the
  // "5 s and 60 s continuations" alternate by sequence number.
  'mkp-vercel-submit-invoice': [
    { kind: 'monthStart', burstFraction: 0.82, batch: 1, delayCycle: [5_000, 60_000] },
    { kind: 'steady', share: ((1 - 0.82) * 7) / 6, batch: 1, delayCycle: [5_000, 60_000] },
  ],
};
const DEFAULT_SHAPE = [{ kind: 'steady', share: 1, batch: 'producer' }];

export function shapeOf(queue) {
  return SHAPES[queue.name] ?? DEFAULT_SHAPE;
}

/** Messages a real day at scale 1 (the week's average day). */
export function dayVolume(queue, scenario) {
  return queue.writes_7d / scenario.measured_days;
}

/** Deliveries per message for queues without measured reads (§3: 1.0032). */
export function restReadRatio(scenario) {
  const known = scenario.queues.filter((q) => 'reads_7d' in q);
  const kr = known.reduce((a, q) => a + q.reads_7d, 0);
  const kw = known.reduce((a, q) => a + q.writes_7d, 0);
  return (scenario.week_totals.reads - kr) / (scenario.week_totals.writes - kw);
}

/**
 * A queue's fate model: the dead-letter rate, the deliveries per message the
 * scenario measured, and the retry-once probability that reproduces them
 * given that a dead-lettered message uses every attempt:
 *   d = dlq * maxAttempts + (1 - dlq) * (1 + pRetry).
 * For the invoice queue (36.5% dead-lettered, 6 attempts, 2.826 deliveries)
 * this gives pRetry = 0: failing messages take all six attempts, the rest
 * succeed at once, exactly the measured mean.
 */
export function fateModel(queue, scenario) {
  const w = queue.writes_7d;
  const dlq = w > 0 ? queue.dlq_writes_7d / w : 0;
  const deliveries = w > 0 && 'reads_7d' in queue ? queue.reads_7d / w : restReadRatio(scenario);
  const maxA = queue.max_attempts;
  const pRetry = maxA >= 2 && dlq < 1
    ? Math.min(1, Math.max(0, (deliveries - dlq * maxA) / (1 - dlq) - 1))
    : 0;
  return { dlq, deliveries, pRetry, maxAttempts: maxA, retryDelayMs: queue.retry_delay_s * 1000 };
}

export const FATE = { ACK: 0, RETRY_ONCE: 1, FAIL: 2 };

/** The fate of a queue's message number seq (0-based), low-discrepancy. */
export function fateOf(seq, model, salt = 0) {
  const u = frac((seq + 1) * PHI + salt);
  if (u < model.dlq) return FATE.FAIL;
  if (u < model.dlq + (1 - model.dlq) * model.pRetry) return FATE.RETRY_ONCE;
  return FATE.ACK;
}

/** Deliveries a fate implies, and its expected terminal state. */
export function fateDeliveries(fate, maxAttempts) {
  if (fate === FATE.FAIL) return maxAttempts;
  if (fate === FATE.RETRY_ONCE) return maxAttempts >= 2 ? 2 : 1;
  return 1;
}

/**
 * The settle op for a delivered message. finalOp is what a consumer does at
 * the last attempt of a failing message: 'retry' (as a Cloudflare consumer
 * does; the server exhausts the message into failed) or 'fail' (explicit).
 */
export function settleOp(fate, attempt, model, finalOp = 'retry', delayScale = 1) {
  const delayMs = Math.round(model.retryDelayMs * delayScale);
  if (fate === FATE.RETRY_ONCE && attempt < 2 && model.maxAttempts >= 2) {
    return { op: 'retry', delayMs, final: false };
  }
  if (fate === FATE.FAIL) {
    if (attempt < model.maxAttempts) return { op: 'retry', delayMs, final: false };
    return finalOp === 'fail'
      ? { op: 'fail', code: 'replay_dead_letter', final: true, terminal: 'failed' }
      : { op: 'retry', delayMs, final: true, terminal: 'failed' };
  }
  return { op: 'ack', final: true, terminal: 'acked' };
}

/** A JSON body of (at least) avgBytes serialized bytes naming its message. */
export function makeBody(queueIndex, seq, fate, avgBytes) {
  const body = { i: queueIndex, s: seq, f: fate, p: '' };
  const base = JSON.stringify(body).length;
  if (avgBytes > base) body.p = 'x'.repeat(avgBytes - base);
  return body;
}

/** The fixed delay or deliverAt of a message, or none. */
export function placement(component, seq, nowMs, compress, delayScale = 1) {
  if (component.spreadMs) {
    const spread = component.spreadMs / compress;
    return { deliverAt: Math.floor(nowMs + frac((seq + 1) * SILVER) * spread) };
  }
  if (component.delayCycle) {
    return { delayMs: Math.round(component.delayCycle[seq % component.delayCycle.length] * delayScale) };
  }
  if (component.delayMs) return { delayMs: Math.round(component.delayMs * delayScale) };
  return {};
}

const cumFloor = (n, k) => Math.floor((k + 1) * n + 1e-9) - Math.floor(k * n + 1e-9);

/**
 * The replay plan: per queue, its components with rates resolved for the
 * profile. profile 'day' replays the day shape; 'peak' runs every queue at
 * its share of the scenario's busiest hour (per-queue peaks scaled so they
 * sum to BUSIEST_HOUR), steadily, keeping batches and delays.
 */
export function buildPlan(scenario, opts = {}) {
  const scale = opts.scale ?? 1;
  const compress = opts.compress ?? 1;
  const profile = opts.profile ?? 'day';
  const startMs = (opts.startHour ?? 0) * HOUR_MS;
  const invoiceBurst = opts.invoiceBurst ?? profile === 'day';
  const burstAtMs = (opts.burstAtS ?? 0) * 1000;
  const only = opts.only ? new Set(opts.only) : null;
  if (!(compress >= 1)) throw new Error('compress must be >= 1');
  if (!(scale > 0)) throw new Error('scale must be > 0');
  const peakSum = scenario.queues.reduce((a, q) => a + q.peak_hour, 0);
  const busiest = opts.busiestHour ?? BUSIEST_HOUR;
  const queues = scenario.queues.map((q, index) => {
    const D = dayVolume(q, scenario);
    const producer = q.producer_batch ?? 1;
    const shape = shapeOf(q);
    const batchOf = (c) => (c.batch === 'producer' ? producer : c.batch ?? 1);
    let components;
    if (only && !only.has(q.name)) {
      components = [];
    } else if (profile === 'peak') {
      const perS = (q.peak_hour * (busiest / peakSum) * scale) / 3600;
      const lead = shape.find((c) => c.spreadMs || c.delayMs || c.delayCycle) ?? {};
      components = perS > 0 ? [{
        kind: 'steady', perMs: perS / 1000, batch: producer,
        spreadMs: lead.spreadMs, delayMs: lead.delayMs, delayCycle: lead.delayCycle,
      }] : [];
    } else if (profile === 'day') {
      components = [];
      for (const c of shape) {
        const base = { batch: batchOf(c), spreadMs: c.spreadMs, delayMs: c.delayMs, delayCycle: c.delayCycle };
        if (c.kind === 'steady') {
          components.push({ ...base, kind: 'steady', perMs: (c.share * D * scale) / DAY_MS });
        } else if (c.kind === 'cron') {
          components.push({
            ...base, kind: 'cron', periodMs: DAY_MS / c.perDay, phaseMs: c.phaseMin * 60_000,
            perFiring: (c.share * D * scale) / compress / c.perDay,
          });
        } else if (c.kind === 'monthStart' && invoiceBurst) {
          const rest = ((1 - c.burstFraction) * scenario.measured_days) / (scenario.measured_days - 1);
          const total = (c.burstFraction * scenario.measured_days - rest) * D * scale / compress;
          components.push({
            ...base, kind: 'monthStart', total, atMs: burstAtMs, perMs: (q.peak_hour * scale) / HOUR_MS,
          });
        }
      }
    } else {
      throw new Error(`unknown profile ${profile}`);
    }
    return { index, name: q.name, avgBytes: q.avg_bytes, D, components, fate: fateModel(q, scenario), config: q };
  });
  return { scale, compress, profile, startMs, queues };
}

/**
 * Sends due in the real interval [r0, r1) ms since the replay started: one
 * entry per component with messages, { queue, component, count }.
 */
export function eventsBetween(plan, r0, r1) {
  const out = [];
  const K = plan.compress;
  for (const q of plan.queues) {
    q.components.forEach((c, ci) => {
      let count = 0;
      if (c.kind === 'steady') {
        count = Math.floor(c.perMs * r1 + 1e-9) - Math.floor(c.perMs * r0 + 1e-9);
      } else if (c.kind === 'cron') {
        // Firing j at virtual phase + j * period; k = j - j0 counts firings
        // from the replay's start so the cumulative floor starts there.
        const v0 = plan.startMs;
        const j0 = Math.ceil((v0 - c.phaseMs) / c.periodMs - 1e-9);
        const ja = Math.ceil((v0 + K * r0 - c.phaseMs) / c.periodMs - 1e-9);
        const jb = Math.ceil((v0 + K * r1 - c.phaseMs) / c.periodMs - 1e-9);
        for (let j = ja; j < jb; j++) count += cumFloor(c.perFiring, j - j0);
      } else if (c.kind === 'monthStart') {
        const total = Math.round(c.total);
        const upto = (r) => Math.min(total, Math.max(0, Math.floor(c.perMs * (r - c.atMs) + 1e-9)));
        count = upto(r1) - upto(r0);
      }
      if (count > 0) out.push({ queue: q.index, component: ci, count });
    });
  }
  return out;
}

/** Expected sends over [0, durationMs) per queue, summed from the plan. */
export function expectedSends(plan, durationMs, stepMs = 60_000) {
  const per = new Array(plan.queues.length).fill(0);
  for (let r = 0; r < durationMs; r += stepMs) {
    for (const e of eventsBetween(plan, r, Math.min(durationMs, r + stepMs))) per[e.queue] += e.count;
  }
  return per;
}

/** Split count messages into producer requests of at most batch. */
export function batches(count, batch) {
  const out = [];
  for (let left = count; left > 0; left -= batch) out.push(Math.min(batch, left));
  return out;
}

/**
 * Consumer loops per queue: at least the scenario's concurrency (or
 * `fallback`), and enough that loops * max / batchSeconds covers twice the
 * queue's peak-hour rate at this scale.
 */
export function consumerLoops(queue, scale, opts = {}) {
  const fallback = opts.fallback ?? 2;
  const cap = opts.max ?? 64;
  const batchSeconds = opts.batchSeconds ?? 0.35;
  const claimMax = opts.claimMax ?? 10;
  const peakPerS = (queue.peak_hour * scale) / 3600;
  const needed = Math.ceil((2 * peakPerS * batchSeconds) / claimMax);
  return Math.max(1, Math.min(cap, Math.max(queue.concurrency ?? fallback, needed)));
}

/**
 * The queue's create body (API.md, PUT /v1/queues/{name}) from the scenario:
 * maxAttempts and a constant retry delay as the backoff; budgets sized to
 * twice the backlog the shape can hold (peak rate x the longest of the
 * spread, the delays and the retry chain), never below the defaults.
 */
export function queueConfig(queue, scale, opts = {}) {
  const shape = shapeOf(queue);
  const retryMs = queue.retry_delay_s * 1000;
  const holdMs = Math.max(
    60_000,
    ...shape.map((c) => Math.max(c.spreadMs ?? 0, c.delayMs ?? 0, ...(c.delayCycle ?? [0]))),
    retryMs * Math.max(0, queue.max_attempts - 1),
  );
  const peakPerMs = (queue.peak_hour * scale) / HOUR_MS;
  const backlog = Math.ceil(2 * peakPerMs * holdMs);
  const perMessage = (queue.avg_bytes || 64) + 512;
  const initialMs = Math.max(retryMs, opts.minBackoffMs ?? 1000);
  return {
    maxQueuedMessages: Math.max(1_000_000, backlog),
    maxQueuedBytes: Math.max(268_435_456, backlog * perMessage),
    group: {
      maxAttempts: queue.max_attempts,
      backoff: { initialMs, maxMs: initialMs, multiplier: 1 },
      leaseMs: opts.leaseMs ?? 30_000,
      maxInFlight: opts.maxInFlight ?? 1000,
      maxBatch: opts.maxBatch ?? 100,
    },
  };
}

/** Deterministic PRNG (mulberry32) for handler latencies. */
export function rng(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Handler latency in ms, uniform in [lo, hi]. */
export function handlerLatency(random, lo = 50, hi = 500) {
  return lo + random() * (hi - lo);
}
