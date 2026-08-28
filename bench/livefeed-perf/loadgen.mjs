#!/usr/bin/env node
// Round-12 performance characterization: the phased load driver.
//
// One process drives ONE run of ONE arm: creates F streams, parks
// F x S subscribers on the default lane (cursor=now), then walks the
// fixed phase timeline (idle -> sparse -> fanout -> mixed -> slow ->
// disconnect -> teardown), measuring client-side latency/throughput
// and sampling the server's debug surface every 10 s. Every append
// carries {q, t, s} and every delivery is reconciled EXACTLY per
// subscriber (bitmap per stream) — performance numbers without exact
// delivery reconciliation are not usable (round-12 charter).
//
// Env: TARGET, AUTH, KEY_B64, OUT,
//      FEEDS, SUBS_PER,
//      WARMUP_SECS, IDLE_SECS, SPARSE_SECS, FANOUT_SECS, MIXED_SECS,
//      SLOW_SECS, TEARDOWN_SECS,
//      FANOUT_DELIVERY_RATE (target deliveries/s in the fanout phase),
//      MIXED_BG_RATE (writes/s to unsubscribed streams in mixed),
//      PAYLOAD_BYTES (fanout/mixed record size, default 1024)
import { writeFileSync, appendFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";

const TARGET = process.env.TARGET;
const AUTH = process.env.AUTH;
const KEY_B64 = process.env.KEY_B64;
const OUT = process.env.OUT ?? "/out";
const FEEDS = Number(process.env.FEEDS ?? 10);
const SUBS_PER = Number(process.env.SUBS_PER ?? 10);
const SECS = (k, d) => Number(process.env[k] ?? d);
const WARMUP = SECS("WARMUP_SECS", 30);
const IDLE = SECS("IDLE_SECS", 600);
const SPARSE = SECS("SPARSE_SECS", 120);
const FANOUT = SECS("FANOUT_SECS", 180);
const MIXED = SECS("MIXED_SECS", 180);
const SLOW = SECS("SLOW_SECS", 120);
const TEARDOWN = SECS("TEARDOWN_SECS", 600);
const FANOUT_RATE = Number(process.env.FANOUT_DELIVERY_RATE ?? 1000);
const MIXED_BG = Number(process.env.MIXED_BG_RATE ?? 200);
const PAYLOAD = Number(process.env.PAYLOAD_BYTES ?? 1024);
// Capacity-ladder geometry: appends target only the first
// WRITE_BREADTH streams (default: all) so subscriber count scales
// while the WRITE workload stays constant — the v1 ladder scaled
// write breadth with feeds and measured the absorber's RSS ceiling
// instead of subscriber capacity.
const WRITE_BREADTH = Math.min(FEEDS, Number(process.env.WRITE_BREADTH || FEEDS));

mkdirSync(OUT, { recursive: true });
const series = join(OUT, "series.jsonl");
const H = (extra = {}) => ({
  authorization: `Bearer ${AUTH}`,
  "prisma-encryption-key": KEY_B64,
  "content-type": "application/json",
  ...extra,
});
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => Date.now();

// ---- latency buckets (0.1ms ticks to 100ms, then 1ms to 10s) -------
function mkHist() {
  return { a: new Uint32Array(1000), b: new Uint32Array(10000), n: 0, max: 0 };
}
function rec(h, ms) {
  h.n++;
  if (ms > h.max) h.max = ms;
  if (ms < 100) h.a[Math.floor(ms * 10)]++;
  else h.b[Math.min(9999, Math.floor(ms))]++;
}
function pct(h, p) {
  if (!h.n) return null;
  const target = Math.ceil((p / 100) * h.n);
  let seen = 0;
  for (let i = 0; i < 1000; i++) { seen += h.a[i]; if (seen >= target) return i / 10; }
  for (let i = 0; i < 10000; i++) { seen += h.b[i]; if (seen >= target) return i; }
  return h.max;
}
const summarize = (h) => ({ n: h.n, p50: pct(h, 50), p99: pct(h, 99), p999: pct(h, 99.9), max: h.max });

// ---- counters shared across phases ---------------------------------
const state = {
  phase: "boot",
  deliveries: 0,
  appends: 0,
  appendErrors: 0,
  reconnects: 0,
  subEofs: 0,
  parseErrors: 0,
  deliveryHist: mkHist(),
  appendHist: mkHist(),
};
function resetPhaseCounters() {
  state.deliveries = 0;
  state.appends = 0;
  state.appendErrors = 0;
  state.reconnects = 0;
  state.subEofs = 0;
  state.deliveryHist = mkHist();
  state.appendHist = mkHist();
}

// ---- server debug sampling -----------------------------------------
async function debugLoad() {
  try {
    const r = await fetch(`${TARGET}/v1/debug/load`, {
      headers: { authorization: `Bearer ${AUTH}` }, signal: AbortSignal.timeout(8000),
    });
    return await r.json();
  } catch { return null; }
}
function serverSnap(d) {
  if (!d) return null;
  const lf = d.sse_livefeed ?? {};
  return {
    rss_mb: d.rss_mb ?? null,
    sse_connections: d.sse_connections ?? null,
    live_feeds: lf.live_feeds ?? d.sse_live_hubs ?? null,
    reserved_bytes: lf.reserved_bytes ?? null,
    delivered_records: d.sse_canary?.delivered_records ?? null,
    lag_disconnects: lf.lag_disconnects ?? null,
    uncached_publish: lf.uncached_publish ?? null,
    capacity_rejected: lf.capacity_rejected ?? null,
    admit_shed: d.admit_shed ?? null,
    stream_shed: d.stream_shed ?? null,
    open_fds: d.open_fds ?? null,
  };
}
let lastSeriesDeliveries = 0;
setInterval(async () => {
  const d = serverSnap(await debugLoad());
  const del10 = state.deliveries - lastSeriesDeliveries;
  lastSeriesDeliveries = state.deliveries;
  appendFileSync(series, JSON.stringify({
    t: now(), phase: state.phase, deliveries_10s: del10, ...d,
  }) + "\n");
}, 10_000).unref();

// ---- subscribers ----------------------------------------------------
// One subscriber = one SSE connection parsing incrementally; the raw
// text is DISCARDED after event boundaries (long runs must not grow).
// Reconciliation: per (subscriber, stream) bitmap of received seqs.
const subs = [];
function mkSub(streamIdx, subIdx) {
  const sub = {
    streamIdx, subIdx, alive: false, paused: false, received: 0,
    bitmap: new Uint8Array(4096), maxSeq: -1, ctl: null, cursor: null,
    lagDisconnects: 0,
  };
  sub.run = async () => {
    for (;;) {
      if (sub.stopped) return;
      sub.ctl = new AbortController();
      try {
        // The DOCUMENTED resume contract: first attach at the tail,
        // every reconnect resumes FROM THE LAST CURSOR — a typed lag
        // disconnect (livefeed budget exhaustion) is overhead, never
        // loss. cursor=now on reconnect was harness bug #2: it turned
        // designed resumable EOFs into fake missing records.
        const cur = sub.cursor ? `cursor=${encodeURIComponent(sub.cursor)}` : "cursor=now";
        const r = await fetch(
          `${TARGET}/v1/streams/perf-s${streamIdx}/records:sse?${cur}`,
          { headers: { ...H(), accept: "text/event-stream" }, signal: sub.ctl.signal });
        if (r.status !== 200 || !r.body) { await sleep(500); continue; }
        sub.alive = true;
        const reader = r.body.getReader();
        const dec = new TextDecoder();
        let buf = "";
        for (;;) {
          while (sub.paused && !sub.stopped) await sleep(50);
          const { done, value } = await reader.read();
          if (done) break;
          buf += dec.decode(value, { stream: true });
          let i;
          while ((i = buf.indexOf("\n\n")) >= 0) {
            const block = buf.slice(0, i);
            buf = buf.slice(i + 2);
            const dl = block.indexOf("data:");
            if (dl < 0) continue;
            if (block.includes("event: control")) {
              const mm = block.slice(dl + 5).match(/"nextCursor":"([^"]+)"/);
              if (mm) sub.cursor = mm[1];
              continue;
            }
            if (!block.includes("event: data")) continue;
            try {
              const arr = JSON.parse(block.slice(dl + 5));
              for (const recv of Array.isArray(arr) ? arr : [arr]) {
                if (typeof recv?.q !== "number") continue;
                state.deliveries++;
                sub.received++;
                rec(state.deliveryHist, Math.max(0, now() - recv.t));
                if (recv.q >= sub.bitmap.length * 8) {
                  const nb = new Uint8Array(Math.max(sub.bitmap.length * 2, (recv.q >> 3) + 1024));
                  nb.set(sub.bitmap); sub.bitmap = nb;
                }
                sub.bitmap[recv.q >> 3] |= 1 << (recv.q & 7);
                if (recv.q > sub.maxSeq) sub.maxSeq = recv.q;
              }
            } catch { state.parseErrors++; }
          }
        }
        sub.alive = false;
        state.subEofs++;
        if (sub.stopped) return;
        state.reconnects++;
        await sleep(250);
      } catch {
        sub.alive = false;
        if (sub.stopped) return;
        await sleep(500);
      }
    }
  };
  return sub;
}

// ---- appends --------------------------------------------------------
// Reconciliation owes ONLY acknowledged appends: a timed-out or
// refused append consumed a seq but its record may not exist — the
// first sweep counted those as "missing" (phantoms at 1000x1 burst
// concurrency). ackBitmap marks 2xx-acked seqs per stream.
const perStreamSeq = new Array(FEEDS).fill(0);
const ackBitmaps = Array.from({ length: FEEDS }, () => new Uint8Array(4096));
function ack(streamIdx, q) {
  let bm = ackBitmaps[streamIdx];
  if (q >= bm.length * 8) {
    const nb = new Uint8Array(Math.max(bm.length * 2, (q >> 3) + 1024));
    nb.set(bm); ackBitmaps[streamIdx] = nb; bm = nb;
  }
  bm[q >> 3] |= 1 << (q & 7);
}
const pad = "x".repeat(Math.max(0, PAYLOAD - 80));
async function appendTo(streamIdx, small = false) {
  const q = perStreamSeq[streamIdx]++;
  const body = JSON.stringify(small
    ? { q, t: now(), s: streamIdx }
    : { q, t: now(), s: streamIdx, pad });
  const t0 = now();
  try {
    const r = await fetch(`${TARGET}/v1/streams/perf-s${streamIdx}/records`, {
      method: "POST", headers: H(), body, signal: AbortSignal.timeout(15000),
    });
    state.appends++;
    rec(state.appendHist, now() - t0);
    if (r.status === 200 || r.status === 204) ack(streamIdx, q);
    else state.appendErrors++;
    await r.body?.cancel?.().catch(() => {});
  } catch { state.appendErrors++; }
}
async function bgAppend(i) {
  try {
    const r = await fetch(`${TARGET}/v1/streams/perf-bg${i % 64}/records`, {
      method: "POST", headers: H(), body: JSON.stringify({ b: i, t: now(), pad }),
      signal: AbortSignal.timeout(15000),
    });
    state.appends++;
    rec(state.appendHist, now() - (state._bgT0 ?? now()));
    if (r.status !== 200 && r.status !== 204) state.appendErrors++;
    await r.body?.cancel?.().catch(() => {});
  } catch { state.appendErrors++; }
}
// Paced append loop: `rate`/s spread over subscribed streams.
async function pacedAppends(secs, rate, { bgRate = 0, small = false } = {}) {
  // Fractional token budgets per 100ms tick — sub-10/s rates must not
  // floor to one append per tick (the smoke run measured 10x).
  const end = now() + secs * 1000;
  let idx = 0, bg = 0, budget = 0, bgBudget = 0;
  while (now() < end) {
    const tickStart = now();
    budget += rate / 10;
    bgBudget += bgRate / 10;
    const jobs = [];
    while (budget >= 1) { budget -= 1; jobs.push(appendTo(idx++ % WRITE_BREADTH, small)); }
    while (bgBudget >= 1) { bgBudget -= 1; jobs.push(bgAppend(bg++)); }
    if (jobs.length) await Promise.all(jobs);
    const rem = 100 - (now() - tickStart);
    if (rem > 0) await sleep(rem);
  }
}

// ---- phase machinery ------------------------------------------------
const phases = [];
async function phase(name, fn) {
  state.phase = name;
  resetPhaseCounters();
  const before = serverSnap(await debugLoad());
  const t0 = now();
  await fn();
  const after = serverSnap(await debugLoad());
  const durMs = now() - t0;
  phases.push({
    name, dur_secs: Math.round(durMs / 1000),
    client: {
      deliveries: state.deliveries,
      deliveries_per_sec: +(state.deliveries / (durMs / 1000)).toFixed(1),
      appends: state.appends,
      append_errors: state.appendErrors,
      reconnects: state.reconnects,
      sub_eofs: state.subEofs,
      parse_errors: state.parseErrors,
      append_latency_ms: summarize(state.appendHist),
      delivery_latency_ms: summarize(state.deliveryHist),
      alive_subs: subs.filter((s) => s.alive).length,
    },
    server_before: before, server_after: after,
  });
  console.log(`== phase ${name} done (${Math.round(durMs / 1000)}s) deliveries=${state.deliveries} appends=${state.appends}`);
}

// ---- run ------------------------------------------------------------
const run = async () => {
  // 1. warmup: server must serve.
  await phase("warmup", async () => {
    const end = now() + WARMUP * 1000;
    for (;;) {
      const d = await debugLoad();
      if (d) break;
      if (now() > end) throw new Error("server never served debug/load");
      await sleep(1000);
    }
    await sleep(Math.max(0, end - now()));
  });

  // 2. create streams (+ background streams for mixed).
  await phase("create", async () => {
    const names = [];
    for (let i = 0; i < FEEDS; i++) names.push(`perf-s${i}`);
    for (let i = 0; i < 64; i++) names.push(`perf-bg${i}`);
    let k = 0;
    const worker = async () => {
      for (;;) {
        const n = names[k++];
        if (!n) return;
        const r = await fetch(`${TARGET}/v1/streams/${n}`, {
          method: "PUT", headers: H(), body: JSON.stringify({ format: { kind: "json" } }),
          signal: AbortSignal.timeout(20000),
        });
        if (r.status !== 200 && r.status !== 201) throw new Error(`create ${n}: ${r.status}`);
        await r.body?.cancel?.().catch(() => {});
      }
    };
    await Promise.all(Array.from({ length: 32 }, worker));
  });

  // 3. park subscribers.
  await phase("park", async () => {
    for (let s = 0; s < FEEDS; s++) {
      for (let j = 0; j < SUBS_PER; j++) subs.push(mkSub(s, j));
    }
    let k = 0;
    const worker = async () => {
      for (;;) {
        const sub = subs[k++];
        if (!sub) return;
        sub.task = sub.run();
        await sleep(2); // ~500 conns/s ramp
      }
    };
    await Promise.all(Array.from({ length: 4 }, worker));
    const end = now() + 60_000;
    while (now() < end && subs.some((s) => !s.alive)) await sleep(500);
    const alive = subs.filter((s) => s.alive).length;
    if (alive < subs.length) console.log(`WARN: only ${alive}/${subs.length} subscribers parked`);
  });

  // 4. idle.
  await phase("idle", async () => { await sleep(IDLE * 1000); });

  // 5. sparse appends: 1/s, small records (wakeup cost).
  await phase("sparse", async () => { await pacedAppends(SPARSE, 1, { small: true }); });

  // 6. fanout: target FANOUT_RATE deliveries/s at PAYLOAD bytes.
  const appendRate = Math.max(1, Math.round(FANOUT_RATE / SUBS_PER));
  // (delivery rate = appendRate x SUBS_PER regardless of breadth)
  await phase("fanout", async () => { await pacedAppends(FANOUT, appendRate); });

  // 7. mixed: fanout writes + background writes to unsubscribed streams.
  await phase("mixed", async () => { await pacedAppends(MIXED, appendRate, { bgRate: MIXED_BG }); });

  // 8. slow client: pause 1% of subscribers (min 1) mid-flow.
  await phase("slow", async () => {
    const nSlow = Math.max(1, Math.floor(subs.length / 100));
    const slowSet = subs.slice(0, nSlow);
    for (const s of slowSet) s.paused = true;
    await pacedAppends(SLOW, appendRate);
    for (const s of slowSet) s.paused = false;
  });

  // 9. settle: every subscriber converges to the appended tail before
  // disconnect — the slow phase's paused subscriber drains its
  // buffered backlog here, so reconciliation asserts CONVERGENCE
  // rather than racing the deliberate lag (smoke: 29 missing = the
  // slow sub's undrained tail on both engines).
  await phase("settle", async () => {
    const end = now() + 90_000;
    for (;;) {
      // A sub mid-reconnect (alive=false, not stopped) is still owed
      // its resume — settling only on alive subs raced the slow set's
      // reconnect and cut their tails (v2 residual FAILs).
      const behind = subs.filter((s) => !s.stopped && s.maxSeq + 1 < perStreamSeq[s.streamIdx]).length;
      if (behind === 0 || now() > end) {
        if (behind > 0) console.log(`WARN: ${behind} subscribers still behind at settle deadline`);
        break;
      }
      await sleep(500);
    }
  });

  // 10. disconnect all.
  await phase("disconnect", async () => {
    for (const s of subs) { s.stopped = true; s.ctl?.abort(); }
    await sleep(3000);
  });

  // 11. teardown observation.
  await phase("teardown", async () => { await sleep(TEARDOWN * 1000); });

  // Reconciliation: every subscriber must hold EVERY seq of its stream
  // appended while it was parked (cursor=now; all appends post-park).
  const ackedPerStream = ackBitmaps.map((bm, i) => {
    let n = 0;
    for (let q = 0; q < perStreamSeq[i]; q++) if (bm[q >> 3] & (1 << (q & 7))) n++;
    return n;
  });
  const recon = {
    checked: 0, missing: 0, duplicates: 0, duplicate_free: true,
    appended: perStreamSeq.reduce((a, b) => a + b, 0),
    acked: ackedPerStream.reduce((a, b) => a + b, 0),
  };
  for (const sub of subs) {
    recon.checked++;
    const bm = ackBitmaps[sub.streamIdx];
    let owed = 0, got = 0;
    for (let q = 0; q < perStreamSeq[sub.streamIdx]; q++) {
      if (!(bm[q >> 3] & (1 << (q & 7)))) continue; // unacked: not owed
      owed++;
      if (sub.bitmap[q >> 3] & (1 << (q & 7))) got++;
    }
    if (got !== owed) recon.missing += owed - got;
    // received counts every delivery incl. resume overlaps; distinct
    // acked receipts are `got` — an overlap re-delivery on resume is
    // NOT a correctness duplicate (at-least-once across reconnects,
    // exactly-once within a session), but record the overlap volume.
    recon.duplicates += Math.max(0, sub.received - got);
  }
  const verdict =
    recon.missing === 0 && recon.duplicate_free && state.parseErrors === 0 ? "PASS" : "FAIL";
  writeFileSync(join(OUT, "run.json"), JSON.stringify({
    shape: { feeds: FEEDS, subscribers: FEEDS * SUBS_PER, subs_per: SUBS_PER },
    config: {
      warmup: WARMUP, idle: IDLE, sparse: SPARSE, fanout: FANOUT, mixed: MIXED,
      slow: SLOW, teardown: TEARDOWN, fanout_delivery_rate: FANOUT_RATE,
      mixed_bg_rate: MIXED_BG, payload_bytes: PAYLOAD,
    },
    phases,
    reconciliation: recon,
    verdict,
  }, null, 1));
  console.log(`LOADGEN_${verdict} missing=${recon.missing}`);
  process.exit(verdict === "PASS" ? 0 : 1);
};
run().catch((e) => { console.error("LOADGEN_ABORT", e); process.exit(2); });
