// Pricing math for the queue cost benchmark (docs/QUEUES-PRICING-BENCHMARK.md
// §6), free of I/O so `node --test bench/queues-cost/` pins it. collect.mjs
// reads the instruments and calls these.
//
// A measured window is turned into a 30-day month: additive quantities
// (requests, bytes, CPU-seconds) by rate, averages (RSS, stored bytes) by
// time weight. With a burst window (the 1x run's first 24 h, holding the
// month-start invoice burst) the burst is counted once and the steady
// window's rate fills the rest of the month (§6 "Runs").

export const MONTH_S = 30 * 86_400;
export const MONTH_H = 720;
export const GIB = 2 ** 30;
export const GB = 1e9;

/** The owner's prices of 2026-10-07 (§6). */
export const OWNER_PRICES = Object.freeze({
  class_a_per_million: 3.75,
  class_b_per_million: 0.375,
  storage_per_gib_month: 0.015,
  transfer_in_per_gb: 0.01,
  memory_per_gb_hour: 0.006,
  cpu_per_vcpu_hour: 0.064,
  egress_per_gb: 0.005,
});

/** Tigris list prices for the sensitivity (§6); Compute and egress as owner. */
export const LIST_PRICES = Object.freeze({
  ...OWNER_PRICES,
  class_a_per_million: 5,
  class_b_per_million: 0.5,
  storage_per_gib_month: 0.02,
});

/** The shared cell's fixed floor a month at the owner's prices (§6, F1). */
export const CELL_FLOOR_USD = 95.3;

/** The reference cell the floor share divides by (README: assumptions). */
export const DEFAULT_CELL = Object.freeze({ vcpus: 1, mem_bytes: GIB, utilization: 0.6 });

const ADDITIVE = ['class_a', 'class_b', 'put_bytes', 'cpu_s', 'egress_bytes', 'messages'];
const AVERAGES = ['rss_avg_bytes', 'live_avg_bytes'];

/**
 * ps cumulative CPU time to seconds: "MM:SS.ss" (macOS), "HH:MM:SS",
 * "D-HH:MM:SS" (Linux procps), or plain seconds.
 */
export function parseCpuTime(s) {
  const t = String(s).trim();
  if (!t) return NaN;
  let days = 0;
  let rest = t;
  const dash = t.indexOf('-');
  if (dash > 0) {
    days = Number(t.slice(0, dash));
    rest = t.slice(dash + 1);
  }
  const parts = rest.split(':').map(Number);
  if (parts.some((x) => !Number.isFinite(x))) return NaN;
  let secs = 0;
  for (const p of parts) secs = secs * 60 + p;
  return days * 86_400 + secs;
}

/** Linear interpolation of a cumulative series [{t, [key]}] at time t. */
export function cumAt(series, t, key) {
  const pts = series.filter((p) => Number.isFinite(p[key]));
  if (!pts.length) return 0;
  if (t <= pts[0].t) return pts[0].t === t ? pts[0][key] : 0;
  for (let i = 1; i < pts.length; i++) {
    if (t <= pts[i].t) {
      const a = pts[i - 1];
      const b = pts[i];
      return a[key] + ((b[key] - a[key]) * (t - a.t)) / (b.t - a.t);
    }
  }
  return pts[pts.length - 1][key];
}

/** Time-weighted mean of a sampled gauge over [t0, t1] (trapezoids). */
export function timeAverage(samples, t0, t1, key) {
  const pts = samples.filter((p) => Number.isFinite(p[key])).sort((a, b) => a.t - b.t);
  if (!pts.length || t1 <= t0) return 0;
  const at = (t) => {
    if (t <= pts[0].t) return pts[0][key];
    for (let i = 1; i < pts.length; i++) {
      if (t <= pts[i].t) {
        const a = pts[i - 1];
        const b = pts[i];
        return a[key] + ((b[key] - a[key]) * (t - a.t)) / (b.t - a.t);
      }
    }
    return pts[pts.length - 1][key];
  };
  const cuts = [t0, ...pts.map((p) => p.t).filter((t) => t > t0 && t < t1), t1];
  let area = 0;
  for (let i = 1; i < cuts.length; i++) area += ((at(cuts[i - 1]) + at(cuts[i])) / 2) * (cuts[i] - cuts[i - 1]);
  return area / (t1 - t0);
}

/** Per-tier class counts b - a from two stats2 `by_tier` maps. */
export function tierDelta(a = {}, b = {}) {
  const out = {};
  for (const tier of new Set([...Object.keys(a), ...Object.keys(b)])) {
    out[tier] = {
      class_a: (b[tier]?.class_a ?? 0) - (a[tier]?.class_a ?? 0),
      class_b: (b[tier]?.class_b ?? 0) - (a[tier]?.class_b ?? 0),
    };
  }
  return out;
}

/**
 * One measured window of one arm. from/to are marks: { t, cpu_s, stats2,
 * stats }; samples: [{ t, rss_kb, live_bytes }]; timeline: the replay's
 * cumulative [{ t, delivered_bytes, sent }] (empty for a baseline arm).
 * CPU is capped at vcpus x the window (Compute bills at most that).
 */
export function windowTerms(from, to, samples, timeline, cell = DEFAULT_CELL) {
  const dur_s = (to.t - from.t) / 1000;
  if (!(dur_s > 0)) throw new Error(`empty window ${from.tag ?? ''}..${to.tag ?? ''}`);
  const tot = (m, k) => m.stats2?.total?.[k] ?? 0;
  const cpu = Math.max(0, to.cpu_s - from.cpu_s);
  const live = samples.map((s) => ({ t: s.t, v: s.live_bytes }));
  for (const m of [from, to]) {
    const v = m.stats2?.live_bytes?.total?.bytes;
    if (Number.isFinite(v)) live.push({ t: m.t, v });
  }
  return {
    dur_s,
    class_a: tot(to, 'class_a') - tot(from, 'class_a'),
    class_b: tot(to, 'class_b') - tot(from, 'class_b'),
    by_tier: tierDelta(from.stats2?.by_tier, to.stats2?.by_tier),
    put_bytes: (to.stats?.put_bytes ?? 0) - (from.stats?.put_bytes ?? 0),
    cpu_s: Math.min(cpu, dur_s * cell.vcpus),
    cpu_s_uncapped: cpu,
    egress_bytes: cumAt(timeline, to.t, 'delivered_bytes') - cumAt(timeline, from.t, 'delivered_bytes'),
    messages: cumAt(timeline, to.t, 'sent') - cumAt(timeline, from.t, 'sent'),
    rss_avg_bytes: timeAverage(samples.map((s) => ({ t: s.t, v: s.rss_kb * 1024 })), from.t, to.t, 'v'),
    live_avg_bytes: timeAverage(live, from.t, to.t, 'v'),
    // Zero when s3lite has no live-bytes census (an old build): storage
    // is then unmeasured, which collect.mjs warns about.
    live_samples: live.filter((p) => Number.isFinite(p.v)).length,
  };
}

/**
 * A month from one window (scaled by rate) or from a burst window plus a
 * steady window (burst once, the steady rate for the rest of the month).
 */
export function toMonth(windows) {
  if (windows.length === 1) {
    const [w] = windows;
    const out = { basis: 'rate', window_s: w.dur_s };
    for (const k of ADDITIVE) out[k] = (w[k] * MONTH_S) / w.dur_s;
    for (const k of AVERAGES) out[k] = w[k];
    out.by_tier = scaleTiers(w.by_tier, MONTH_S / w.dur_s);
    return out;
  }
  if (windows.length !== 2) throw new Error('toMonth takes one window, or a burst and a steady window');
  const [b, s] = windows;
  if (b.dur_s >= MONTH_S) throw new Error('burst window longer than a month');
  const rest = MONTH_S - b.dur_s;
  const out = { basis: 'burst_once', burst_s: b.dur_s, steady_s: s.dur_s };
  for (const k of ADDITIVE) out[k] = b[k] + (s[k] * rest) / s.dur_s;
  for (const k of AVERAGES) out[k] = (b[k] * b.dur_s + s[k] * rest) / MONTH_S;
  out.by_tier = addTiers(b.by_tier, scaleTiers(s.by_tier, rest / s.dur_s));
  return out;
}

function scaleTiers(t = {}, f) {
  return Object.fromEntries(Object.entries(t).map(([k, v]) => [k, { class_a: v.class_a * f, class_b: v.class_b * f }]));
}

function addTiers(a = {}, b = {}) {
  const out = {};
  for (const k of new Set([...Object.keys(a), ...Object.keys(b)])) {
    out[k] = {
      class_a: (a[k]?.class_a ?? 0) + (b[k]?.class_a ?? 0),
      class_b: (a[k]?.class_b ?? 0) + (b[k]?.class_b ?? 0),
    };
  }
  return out;
}

/** b - a for every monthly quantity (the marginal month). */
export function monthDelta(a, b) {
  const out = { basis: b.basis };
  for (const k of [...ADDITIVE, ...AVERAGES]) out[k] = b[k] - a[k];
  out.by_tier = addTiers(b.by_tier, scaleTiers(a.by_tier, -1));
  return out;
}

/** Monthly quantities priced, term by term (USD a month). */
export function priceMonth(m, prices = OWNER_PRICES) {
  const terms = {
    class_a: { quantity: m.class_a, unit: 'requests', usd: (m.class_a * prices.class_a_per_million) / 1e6 },
    class_b: { quantity: m.class_b, unit: 'requests', usd: (m.class_b * prices.class_b_per_million) / 1e6 },
    transfer_in: { quantity: m.put_bytes, unit: 'bytes', usd: (m.put_bytes / GB) * prices.transfer_in_per_gb },
    storage: { quantity: m.live_avg_bytes, unit: 'bytes held (month average)', usd: (m.live_avg_bytes / GIB) * prices.storage_per_gib_month },
    cpu: { quantity: m.cpu_s, unit: 'vCPU-seconds', usd: (m.cpu_s / 3600) * prices.cpu_per_vcpu_hour },
    memory: { quantity: m.rss_avg_bytes, unit: 'bytes resident (month average)', usd: (m.rss_avg_bytes / GB) * MONTH_H * prices.memory_per_gb_hour },
    egress: { quantity: m.egress_bytes, unit: 'bytes delivered', usd: (m.egress_bytes / GB) * prices.egress_per_gb },
  };
  const usd = Object.values(terms).reduce((a, t) => a + t.usd, 0);
  return { terms, usd };
}

/**
 * The queue's share of the cell floor: the larger of its CPU share and its
 * memory share of the cell at planned utilization, within [0, 1].
 */
export function floorShare(marginal, cell = DEFAULT_CELL, floorUsd = CELL_FLOOR_USD) {
  const cpu_share = marginal.cpu_s / (MONTH_S * cell.vcpus * cell.utilization);
  const mem_share = marginal.rss_avg_bytes / (cell.mem_bytes * cell.utilization);
  const share = Math.min(1, Math.max(0, cpu_share, mem_share));
  return { cpu_share, mem_share, share, floor_usd: floorUsd, usd: share * floorUsd };
}

/** Messages a month the scenario carries at a scale (cf_bill's writes). */
export function scenarioMessagesMonth(scenario, scale) {
  const week = scenario.queues.reduce((a, q) => a + q.writes_7d, 0);
  return (week * scenario.month_days * scale) / scenario.measured_days;
}

/**
 * costs.json: the three monthly costs and every term behind them, at the
 * owner's prices (`month_usd`, what cf_bill.py --measured reads) and at
 * Tigris list prices (`month_usd_list`). abMonths: { a, b } month
 * quantities of the A/B arms; standalone: { a, b } of the standalone pair
 * (an empty cell, then the scenario alone on it) or null.
 */
export function buildCosts({ abMonths, standalone = null, scale, scenario, cell = DEFAULT_CELL, floorUsd = CELL_FLOOR_USD, meta = {} }) {
  const marginal = monthDelta(abMonths.a, abMonths.b);
  const share = floorShare(marginal, cell, floorUsd);
  const sa = standalone ? monthDelta(standalone.a, standalone.b) : null;
  const priced = (prices) => {
    const m = priceMonth(marginal, prices);
    const s = sa ? priceMonth(sa, prices) : null;
    return {
      marginal: m,
      fully_loaded_usd: m.usd + share.usd,
      standalone: s,
      standalone_usd: s ? s.usd + floorUsd : null,
    };
  };
  const owner = priced(OWNER_PRICES);
  const list = priced(LIST_PRICES);
  const messages = scenarioMessagesMonth(scenario, scale);
  const perM = (usd) => (usd === null ? null : (usd / messages) * 1e6);
  // A busiest-hour run sends about twice the scenario's average rate: its
  // cost per message is its month divided by the messages it sent, not by
  // the scenario's month (which would charge every message twice).
  const replayed = abMonths.b.messages;
  const perReplayed = (usd) => (usd === null || !(replayed > 0) ? null : (usd / replayed) * 1e6);
  const round = (x) => (x === null ? null : Math.round(x * 10_000) / 10_000);
  return {
    benchmark: 'docs/QUEUES-PRICING-BENCHMARK.md §6',
    scale,
    month_days: 30,
    ...meta,
    month_usd: {
      fully_loaded: round(owner.fully_loaded_usd),
      marginal: round(owner.marginal.usd),
      standalone: round(owner.standalone_usd),
    },
    month_usd_list: {
      fully_loaded: round(list.fully_loaded_usd),
      marginal: round(list.marginal.usd),
      standalone: round(list.standalone_usd),
    },
    messages_month: Math.round(messages),
    messages_month_replayed: Math.round(abMonths.b.messages),
    replayed_to_scenario: replayed > 0 ? round(replayed / messages) : null,
    per_million_messages_usd: {
      fully_loaded: round(perM(owner.fully_loaded_usd)),
      marginal: round(perM(owner.marginal.usd)),
      standalone: round(perM(owner.standalone_usd)),
    },
    per_million_replayed_messages_usd: {
      fully_loaded: round(perReplayed(owner.fully_loaded_usd)),
      marginal: round(perReplayed(owner.marginal.usd)),
      standalone: round(perReplayed(owner.standalone_usd)),
    },
    terms: {
      marginal: owner.marginal.terms,
      marginal_list: list.marginal.terms,
      floor_share: share,
      standalone: owner.standalone?.terms ?? null,
      standalone_list: list.standalone?.terms ?? null,
    },
    months: { a: abMonths.a, b: abMonths.b, marginal, standalone: sa },
    prices: { owner: OWNER_PRICES, list: LIST_PRICES },
    cell,
  };
}
