// setup and produce. The producer is also embedded by the consume
// modes (walk/tail/group) when they are given --rate/--mbps or
// --concurrency, so a reader and its load run in one process.

import {
  Http, Meter, Opts, Rng, Zipf, backoffMs, encName, parseJson, pool, sdkBackoffMs, sdkRetryable, sleep, stop, streamName, utf8,
} from "./common.ts";
import { PayloadGen, payloadKind, type PayloadKind } from "./corpus.ts";

export interface Ctx {
  http: Http;
  meter: Meter;
  o: Opts;
  prefix: string;
  streams: number;
  seed: number;
  duration: number;
}

export const CAP = 4096;
const LATE_MS = 100;
const SDK_RETRIES = 3;

// ---------------------------------------------------------------- setup

/** PUT /v1/streams/{name} for every stream (idempotent: 201 new, 200 same). */
export async function setup(ctx: Ctx): Promise<Record<string, unknown>> {
  const { o, http, meter } = ctx;
  const format = o.str("format", o.str("payload", "corpus") === "bytes" ? "bytes" : "json");
  if (format !== "json" && format !== "bytes") throw new Error("--format must be json|bytes");
  const ttl = o.int("ttl", 0);
  const parallel = o.int("parallel", 8);
  const doc: Record<string, unknown> = { format: { kind: format } };
  if (ttl > 0) doc.expiry = { idle: `${ttl}s` };
  const body = JSON.stringify(doc);
  let created = 0, existing = 0, failed = 0;
  await pool(ctx.streams, parallel, async (i) => {
    const name = streamName(ctx.prefix, i);
    for (let attempt = 0; attempt < 6; attempt++) {
      const r = await http.call("PUT", `/v1/streams/${encName(name)}`, { body, contentType: "application/json" });
      if (r.status === 201) return void created++;
      if (r.status === 200) return void existing++;
      if (r.status === 429 || r.status === 503 || r.status === 0) {
        await sleep(backoffMs(r), stop.signal);
        continue;
      }
      process.stderr.write(`[k2gen setup] ${name}: ${r.status} ${utf8(r.body).slice(0, 200)}\n`);
      break;
    }
    failed++;
  }, stop.signal);
  meter.add("streams_created", created);
  meter.add("streams_existing", existing);
  meter.add("streams_failed", failed);
  return { format, ttl_secs: ttl || null, created, existing, failed };
}

// ---------------------------------------------------------------- spec

export interface ProduceSpec {
  rate: number | null; // open loop, req/s
  concurrency: number | null; // closed loop
  records: number;
  recordBytes: number;
  payload: PayloadKind;
  zipf: number;
  routingKeys: number;
  pool: number;
  timeoutMs: number;
  drainSecs: number;
}

/** The produce flags; null when the mode was given neither a rate nor a
 * concurrency (a consume mode without an embedded producer). */
export function produceSpec(o: Opts, required: boolean): ProduceSpec | null {
  const records = o.int("records", 1);
  const recordBytes = o.int("record-bytes", 1024);
  const payload = payloadKind(o.str("payload", "corpus"));
  const mbps = o.num("mbps", 0);
  let rate: number | null = o.has("rate") ? o.num("rate", 0) : null;
  if (mbps > 0) rate = (mbps * 1e6) / (records * recordBytes);
  const concurrency = o.has("concurrency") ? o.int("concurrency", 1) : null;
  const spec: ProduceSpec = {
    rate,
    concurrency,
    records,
    recordBytes,
    payload,
    zipf: o.num("zipf", 0),
    routingKeys: o.int("routing-keys", 0),
    pool: o.int("pool", 0),
    timeoutMs: o.int("timeout-ms", 60_000),
    drainSecs: o.num("drain-secs", 60),
  };
  if (rate === null && concurrency === null) {
    if (required) throw new Error("produce needs --rate R (open loop), --mbps X or --concurrency C (closed loop)");
    return null;
  }
  if (rate !== null && concurrency !== null) throw new Error("--rate/--mbps and --concurrency are exclusive");
  if (rate !== null && !(rate > 0)) throw new Error("--rate must be > 0");
  if (records < 1 || records > 10_000) throw new Error("--records must be 1..10000");
  if (payload === "bytes" && records !== 1) throw new Error("--payload bytes takes singles only (:batch is JSON only)");
  if (payload === "corpus" && recordBytes < 128) throw new Error("--payload corpus needs --record-bytes >= 128");
  return spec;
}

// ---------------------------------------------------------------- producer

/** Cumulative acked records per target over time, for lag readers. */
class Timeline {
  t: number[] = [];
  cum: number[] = [];
  head = 0;
  push(tMs: number, cum: number, keepMs: number): void {
    this.t.push(tMs);
    this.cum.push(cum);
    // Keep one entry at or before the oldest instant still asked for.
    const cutoff = tMs - keepMs;
    while (this.head + 1 < this.t.length && this.t[this.head + 1]! <= cutoff) this.head++;
    if (this.head > 4096) {
      this.t = this.t.slice(this.head);
      this.cum = this.cum.slice(this.head);
      this.head = 0;
    }
  }
  at(tMs: number): number {
    let lo = this.head, hi = this.t.length - 1, ans = 0;
    while (lo <= hi) {
      const mid = (lo + hi) >>> 1;
      if (this.t[mid]! <= tMs) {
        ans = this.cum[mid]!;
        lo = mid + 1;
      } else hi = mid - 1;
    }
    return ans;
  }
}

/** A target is a stream, or a stream and routing key (`name#key`). */
export function targetKey(name: string, key: string): string {
  return key ? `${name}#${key}` : name;
}

export class Producer {
  readonly names: string[];
  private zipf: Zipf;
  private rngStream: Rng;
  private gen: PayloadGen;
  private poolRecs: Array<string | Uint8Array> = [];
  private poolNext = 0;
  private timelines = new Map<string, Timeline>();
  private acked = new Map<string, number>();
  timelineKeepMs = 0; // > 0 when a lag reader asks
  late = 0;
  capHits = 0;
  unsent = 0;
  ambiguous = 0;
  constructor(private readonly ctx: Ctx, readonly spec: ProduceSpec) {
    this.names = Array.from({ length: ctx.streams }, (_, i) => streamName(ctx.prefix, i));
    this.zipf = new Zipf(ctx.streams, spec.zipf);
    this.rngStream = new Rng(ctx.seed, 2);
    this.gen = new PayloadGen(spec.payload, spec.recordBytes, ctx.seed, 3);
    for (let i = 0; i < spec.pool; i++) this.poolRecs.push(this.gen.isJson ? this.gen.json() : this.gen.raw());
  }
  private record(): string | Uint8Array {
    if (this.poolRecs.length) {
      const r = this.poolRecs[this.poolNext]!;
      this.poolNext = (this.poolNext + 1) % this.poolRecs.length;
      return r;
    }
    return this.gen.isJson ? this.gen.json() : this.gen.raw();
  }
  /** Cumulative acked records of a target at a wall time (ms). */
  ackedAt(target: string, tMs: number): number {
    return this.timelines.get(target)?.at(tMs) ?? 0;
  }
  /** Build one request; returns a thunk that sends it. Building happens
   * in dispatch order, so record content is a function of the seed. */
  private build(): () => Promise<{ ok: boolean; ms: number }> {
    const { spec } = this;
    const { http, meter } = this.ctx;
    const name = this.names[this.zipf.sample(this.rngStream)]!;
    const key = spec.routingKeys > 0 ? `k${this.rngStream.int(spec.routingKeys)}` : "";
    let body: string | Uint8Array;
    let pbytes = 0;
    let path = "/records";
    let ct = "application/json";
    if (spec.payload === "bytes") {
      body = this.record() as Uint8Array;
      pbytes = body.byteLength;
      ct = "application/octet-stream";
    } else if (spec.records === 1) {
      body = this.record() as string;
      pbytes = body.length; // ASCII
    } else {
      const rs: string[] = [];
      for (let i = 0; i < spec.records; i++) {
        const r = this.record() as string;
        pbytes += r.length;
        rs.push(r);
      }
      body = `[${rs.join(",")}]`;
      path = "/records:batch";
    }
    const target = targetKey(name, key);
    const extra = key ? { "prisma-routing-key": key } : undefined;
    const url = `/v1/streams/${encName(name)}${path}`;
    return async () => {
      meter.add("sent_requests");
      meter.add("sent_records", spec.records);
      meter.add("sent_payload_bytes", pbytes);
      meter.flight(1);
      let r = await http.call("POST", url, { body, contentType: ct, timeoutMs: spec.timeoutMs, extra });
      // As the SDK does: up to 3 retries of a retryable 429/503 (a cold
      // shard's first touch answers 503 temporarily_unavailable), after
      // retry-after. Each attempt is a real request and stays in the
      // status counts; latency runs from the first attempt's schedule.
      for (let a = 0; a < SDK_RETRIES && sdkRetryable(r) && !stop.signal.aborted; a++) {
        meter.add("retries");
        await sleep(sdkBackoffMs(r), stop.signal);
        r = await http.call("POST", url, { body, contentType: ct, timeoutMs: spec.timeoutMs, extra });
      }
      meter.flight(-1);
      if (r.status !== 200) {
        meter.add("failed_requests");
        if (r.status === 0) this.ambiguous++;
        return { ok: false, ms: r.ms };
      }
      let count = spec.records;
      const j = parseJson<{ count?: number }>(r, meter, "append");
      if (typeof j?.count === "number") count = j.count;
      if (count !== spec.records) meter.add("ack_count_mismatch");
      meter.add("acked_requests");
      meter.add("acked_records", count);
      meter.add("payload_bytes", pbytes);
      const t = meter.tally(target);
      t.acked_records += count;
      t.acked_bytes += pbytes;
      const cum = (this.acked.get(target) ?? 0) + count;
      this.acked.set(target, cum);
      if (this.timelineKeepMs > 0) {
        let tl = this.timelines.get(target);
        if (!tl) this.timelines.set(target, (tl = new Timeline()));
        tl.push(Date.now(), cum, this.timelineKeepMs);
      }
      return { ok: true, ms: r.ms };
    };
  }

  /** Runs until `signal` fires, then drains in-flight requests. */
  async run(signal: AbortSignal): Promise<Record<string, unknown>> {
    if (this.spec.rate !== null) await this.openLoop(this.spec.rate, signal);
    else await this.closedLoop(this.spec.concurrency!, signal);
    const t0 = Date.now();
    while (this.ctx.meter.inflight > 0 && Date.now() - t0 < this.spec.drainSecs * 1000) await sleep(20);
    return this.summary();
  }

  summary(): Record<string, unknown> {
    const s = this.spec;
    return {
      producer: {
        loop: s.rate !== null ? "open" : "closed",
        rate_req_s: s.rate,
        concurrency: s.concurrency,
        records_per_request: s.records,
        record_bytes: s.recordBytes,
        payload: s.payload,
        zipf: s.zipf,
        routing_keys: s.routingKeys,
        pool: s.pool,
        late_sends: this.late,
        late_threshold_ms: LATE_MS,
        cap: CAP,
        cap_hits: this.capHits,
        unsent: this.unsent,
        ambiguous_timeouts: this.ambiguous,
        inflight_at_exit: this.ctx.meter.inflight,
      },
    };
  }

  /** Open loop: Poisson arrivals from a precomputed clock (a seeded
   * stream of exponential gaps, filled 8,192 at a time). A slow answer
   * never delays the next send; in flight is unbounded up to CAP, and an
   * arrival that finds CAP requests in flight waits (counted in
   * cap_hits; late if dispatched > 100 ms after its time). Latency is
   * measured from the scheduled arrival, so dispatch lag counts. */
  private async openLoop(rate: number, signal: AbortSignal): Promise<void> {
    const meter = this.ctx.meter;
    const rng = new Rng(this.ctx.seed, 1);
    const meanMs = 1000 / rate;
    const clock = new Float64Array(8192);
    let ci = clock.length;
    let at = 0;
    const nextArrival = (): number => {
      if (ci === clock.length) {
        for (let i = 0; i < clock.length; i++) clock[i] = at += rng.exp(meanMs);
        ci = 0;
      }
      return clock[ci++]!;
    };
    const t0 = performance.now();
    let next = nextArrival();
    const queue: number[] = [];
    let qh = 0;
    while (!signal.aborted) {
      const now = performance.now() - t0;
      while (next <= now) {
        if (meter.inflight + (queue.length - qh) >= CAP) {
          this.capHits++;
          meter.add("cap_hits");
        }
        queue.push(next);
        next = nextArrival();
      }
      while (qh < queue.length && meter.inflight < CAP) {
        const sched = queue[qh++]!;
        const send = this.build();
        const lag = performance.now() - t0 - sched;
        if (lag > LATE_MS) {
          this.late++;
          meter.add("late_sends");
        }
        const schedAbs = t0 + sched;
        void send().then((r) => {
          if (r.ok) meter.appendMs(performance.now() - schedAbs);
        });
      }
      if (qh > 65536) {
        queue.splice(0, qh);
        qh = 0;
      }
      const wait = next - (performance.now() - t0);
      if (wait > 1) await sleep(Math.min(wait, 50), signal);
      else await new Promise((r) => setImmediate(r));
    }
    this.unsent = queue.length - qh;
    meter.add("unsent", this.unsent);
  }

  private async closedLoop(c: number, signal: AbortSignal): Promise<void> {
    const meter = this.ctx.meter;
    const worker = async () => {
      while (!signal.aborted) {
        const r = await this.build()();
        if (r.ok) meter.appendMs(r.ms);
        else await sleep(200, signal);
      }
    };
    await Promise.all(Array.from({ length: c }, worker));
  }
}

/** produce mode. */
export async function produce(ctx: Ctx, signal: AbortSignal): Promise<Record<string, unknown>> {
  const spec = produceSpec(ctx.o, true)!;
  const p = new Producer(ctx, spec);
  return p.run(signal);
}
