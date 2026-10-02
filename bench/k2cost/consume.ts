// walk, tail, group and subs: the read side of the product surface.
//
// Every delivered record and its payload bytes are counted where the
// client sees them: a GET page (JSON array, or the concatenated body of
// a bytes stream), an SSE data event (one record each), or a consumer
// pull message. Each consume mode embeds a producer when given
// --rate/--mbps/--concurrency, so a lagged reader, a live tail or a
// consumer group can run against live load in one process.

import { readFileSync } from "node:fs";
import {
  Http, Meter, Rng, b64Len, backoffMs, encName, errCode, parseJson, pool, sleep, stop, streamName,
  type Resp, type StreamTally,
} from "./common.ts";
import { Producer, produceSpec, targetKey, type Ctx } from "./produce.ts";

interface Target {
  name: string;
  key: string;
  id: string; // targetKey
}

function targets(ctx: Ctx): Target[] {
  const keys = ctx.o.int("routing-keys", 0);
  const out: Target[] = [];
  for (let i = 0; i < ctx.streams; i++) {
    const name = streamName(ctx.prefix, i);
    if (keys > 0) for (let k = 0; k < keys; k++) out.push({ name, key: `k${k}`, id: targetKey(name, `k${k}`) });
    else out.push({ name, key: "", id: name });
  }
  return out;
}

const MAX_PAGE = 8 << 20;

// ---------------------------------------------------------------- pages

interface Page {
  r: Resp;
  ok: boolean; // 200 or 204
  records: number;
  bytes: number;
  next: string | null;
  upToDate: boolean;
  sealed: boolean;
  estimated: boolean; // a bytes stream: records = bytes / --record-bytes
}

class Reader {
  estimated = false;
  constructor(
    private readonly http: Http,
    private readonly meter: Meter,
    private readonly recordBytes: number,
  ) {}
  /** One GET of records (or a long-poll) from `cursor`, counted. */
  async page(t: Target, cursor: string, opt: { live?: boolean; waitMs?: number; maxBytes?: number; signal?: AbortSignal }): Promise<Page> {
    const q = new URLSearchParams();
    q.set("cursor", cursor);
    if (t.key) q.set("routingKey", t.key);
    if (opt.maxBytes) q.set("maxBytes", String(Math.min(MAX_PAGE, Math.max(4096, Math.round(opt.maxBytes)))));
    if (opt.live) q.set("waitMs", String(opt.waitMs ?? 25_000));
    const path = `/v1/streams/${encName(t.name)}/records${opt.live ? ":long-poll" : ""}?${q}`;
    const r = await this.http.call("GET", path, {
      timeoutMs: opt.live ? (opt.waitMs ?? 25_000) + 30_000 : 120_000,
      signal: opt.signal,
    });
    const pg: Page = { r, ok: r.status === 200 || r.status === 204, records: 0, bytes: 0, next: null, upToDate: false, sealed: false, estimated: false };
    if (!pg.ok) return pg;
    this.meter.readMs(r.ms);
    pg.next = r.headers?.get("prisma-next-cursor") ?? null;
    pg.upToDate = r.headers?.get("prisma-up-to-date") === "true";
    pg.sealed = r.headers?.get("prisma-sealed") === "true";
    if (r.status === 200 && r.body.byteLength > 0) {
      const ct = r.headers?.get("content-type") ?? "";
      if (ct.startsWith("application/json")) {
        const arr = parseJson<unknown[]>(r, this.meter, "read") ?? [];
        pg.records = arr.length;
        // read_payload: "[" + records joined by "," + "]"
        pg.bytes = arr.length ? r.body.byteLength - 2 - (arr.length - 1) : 0;
      } else {
        pg.bytes = r.body.byteLength;
        pg.records = Math.round(pg.bytes / this.recordBytes);
        pg.estimated = this.estimated = true;
      }
    }
    if (pg.records === 0) this.meter.add("empty_polls");
    this.deliver(t, pg.records, pg.bytes);
    return pg;
  }
  deliver(t: Target, records: number, bytes: number): void {
    if (records === 0 && bytes === 0) return;
    this.meter.add("delivered_records", records);
    this.meter.add("delivered_payload_bytes", bytes);
    const tl = this.meter.tally(t.id);
    tl.delivered_records += records;
    tl.delivered_bytes += bytes;
  }

  /** One SSE subscription, resumed from its last control cursor after a
   * cutoff, until `signal`. Counts each data event as one record. */
  async sse(t: Target, cursor: string, signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      const q = new URLSearchParams();
      q.set("cursor", cursor);
      if (t.key) q.set("routingKey", t.key);
      const res = await this.http.open(`/v1/streams/${encName(t.name)}/records:sse?${q}`, signal);
      if (!(res instanceof Response)) {
        await sleep(1000, signal);
        continue;
      }
      if (res.status !== 200) {
        const body = new Uint8Array(await res.arrayBuffer().catch(() => new ArrayBuffer(0)));
        this.meter.add("wire_bytes_down", body.byteLength);
        await sleep(res.status === 429 || res.status === 503 ? 2000 : 1000, signal);
        continue;
      }
      const b64 = res.headers.get("stream-sse-data-encoding") === "base64";
      this.meter.connect(1);
      try {
        cursor = await this.sseBody(t, res, b64, cursor, signal);
      } catch {
        // cut off mid-stream: resume from the last cursor
      } finally {
        this.meter.connect(-1);
      }
      if (!signal.aborted) {
        this.meter.add("sse_reconnects");
        await sleep(250, signal);
      }
    }
  }
  private async sseBody(t: Target, res: Response, b64: boolean, cursor: string, signal: AbortSignal): Promise<string> {
    const rd = res.body!.getReader();
    const dec = new TextDecoder();
    let buf = "";
    const onAbort = () => void rd.cancel().catch(() => {});
    signal.addEventListener("abort", onAbort, { once: true });
    try {
      for (;;) {
        const { value, done } = await rd.read();
        if (done) return cursor;
        this.meter.add("wire_bytes_down", value.byteLength);
        buf += dec.decode(value, { stream: true });
        let i: number;
        while ((i = buf.indexOf("\n\n")) >= 0) {
          const ev = buf.slice(0, i);
          buf = buf.slice(i + 2);
          let kind = "message";
          const data: string[] = [];
          for (const line of ev.split("\n")) {
            if (line.startsWith("event:")) kind = line.slice(6).trim();
            else if (line.startsWith("data:")) data.push(line.slice(5));
          }
          const d = data.join("\n");
          if (kind === "data") {
            // JSON: data:[<record>]; binary: base64 of the record.
            const bytes = b64 ? b64Len(d) : Buffer.byteLength(d) - 2;
            this.deliver(t, 1, bytes);
          } else if (kind === "control") {
            try {
              const c = JSON.parse(d) as { nextCursor?: string };
              if (c.nextCursor) cursor = c.nextCursor;
            } catch {
              this.meter.add("bad_control");
            }
          }
        }
      }
    } finally {
      signal.removeEventListener("abort", onAbort);
    }
  }

  /** A long-poll subscription loop from `cursor` until `signal`. */
  async longPoll(t: Target, cursor: string, waitMs: number, signal: AbortSignal): Promise<void> {
    this.meter.connect(1);
    try {
      while (!signal.aborted) {
        const pg = await this.page(t, cursor, { live: true, waitMs, signal });
        if (signal.aborted) break;
        if (!pg.ok) {
          await sleep(backoffMs(pg.r), signal);
          continue;
        }
        if (pg.next) cursor = pg.next;
      }
    } finally {
      this.meter.connect(-1);
    }
  }
}

function startProducer(ctx: Ctx, signal: AbortSignal): { p: Producer; done: Promise<Record<string, unknown>> } | null {
  const spec = produceSpec(ctx.o, false);
  if (!spec) return null;
  const p = new Producer(ctx, spec);
  return { p, done: p.run(signal) };
}

/** The tail cursor of a target right now (an empty read from "now"). */
async function headCursor(rd: Reader, t: Target): Promise<string> {
  for (let i = 0; i < 10; i++) {
    const pg = await rd.page(t, "now", {});
    if (pg.ok && pg.next) return pg.next;
    await sleep(backoffMs(pg.r), stop.signal);
  }
  return "now";
}

// ---------------------------------------------------------------- walk

/** --expect ledger files: per target acked records/bytes, summed. */
function expected(files: string): Map<string, StreamTally> {
  const m = new Map<string, StreamTally>();
  for (const f of files.split(",").filter(Boolean)) {
    const j = JSON.parse(readFileSync(f, "utf8")) as { per_stream?: Record<string, StreamTally> };
    for (const [k, v] of Object.entries(j.per_stream ?? {})) {
      const e = m.get(k) ?? { acked_records: 0, acked_bytes: 0, delivered_records: 0, delivered_bytes: 0 };
      e.acked_records += v.acked_records;
      e.acked_bytes += v.acked_bytes;
      m.set(k, e);
    }
  }
  return m;
}

export async function walk(ctx: Ctx, signal: AbortSignal): Promise<Record<string, unknown>> {
  const { o, http, meter } = ctx;
  const from = o.str("from", "earliest");
  const readers = o.int("readers", 1);
  const stagger = o.num("stagger", 0);
  const parallel = o.int("parallel", 4);
  const maxBytes = o.int("max-bytes", 0) || undefined;
  const expect = o.str("expect");
  const rd = new Reader(http, meter, o.int("record-bytes", 1024));
  const ts = targets(ctx);
  const reader0 = new Map<string, Got>();
  const done: number[] = [];
  let lagSecs = -1;
  if (from.startsWith("lag:")) {
    lagSecs = Number(from.slice(4));
    if (!(lagSecs >= 0)) throw new Error("--from lag:SECS needs SECS >= 0");
  } else if (from !== "earliest") throw new Error("--from must be earliest or lag:SECS");
  // An earliest walk ends when every reader has reached every head, and
  // takes its embedded producer down with it.
  const local = new AbortController();
  const sig = AbortSignal.any([signal, local.signal]);

  // A lag reader starts at the head as it was when the mode started.
  const starts = new Map<string, string>();
  if (lagSecs >= 0) await pool(ts.length, 8, async (i) => void starts.set(ts[i]!.id, await headCursor(rd, ts[i]!)));
  const prod = startProducer(ctx, sig);
  if (prod && lagSecs >= 0) prod.p.timelineKeepMs = lagSecs * 1000 + 60_000;
  const perRecord = prod ? prod.p.spec.recordBytes : o.int("record-bytes", 1024);

  const walkOne = async (r: number, t: Target): Promise<void> => {
    let cursor = "beginning";
    while (!sig.aborted) {
      const pg = await rd.page(t, cursor, { maxBytes, signal: sig });
      if (!pg.ok) {
        await sleep(backoffMs(pg.r), sig);
        continue;
      }
      if (r === 0) {
        const e = reader0.get(t.id) ?? { records: 0, bytes: 0, estimated: false };
        e.records += pg.records;
        e.bytes += pg.bytes;
        e.estimated ||= pg.estimated;
        reader0.set(t.id, e);
      }
      if (pg.upToDate || pg.r.status === 204) return;
      if (pg.records === 0 && (pg.next === null || pg.next === cursor)) return;
      if (pg.next) cursor = pg.next;
    }
  };

  const lagOne = async (t: Target): Promise<void> => {
    let cursor = starts.get(t.id) ?? "now";
    let delivered = 0;
    while (!sig.aborted) {
      if (!prod) {
        // No producer in this process: a periodic catch-up every SECS.
        await sleep(lagSecs * 1000, sig);
        while (!sig.aborted) {
          const pg = await rd.page(t, cursor, { maxBytes, signal: sig });
          if (!pg.ok) {
            await sleep(backoffMs(pg.r), sig);
            continue;
          }
          if (pg.next) cursor = pg.next;
          if (pg.upToDate || pg.records === 0) break;
        }
        continue;
      }
      // Trail the producer by SECS: read what it had acked SECS ago
      // (a page is sized to that count, so it overshoots by < 4 KiB).
      const want = prod.p.ackedAt(t.id, Date.now() - lagSecs * 1000) - delivered;
      if (want <= 0) {
        await sleep(200, sig);
        continue;
      }
      const pg = await rd.page(t, cursor, { maxBytes: Math.min(maxBytes ?? MAX_PAGE, want * perRecord), signal: sig });
      if (!pg.ok) {
        await sleep(backoffMs(pg.r), sig);
        continue;
      }
      if (pg.next) cursor = pg.next;
      delivered += pg.records;
      if (pg.records === 0) await sleep(200, sig);
    }
  };

  const t0 = Date.now();
  const runReader = async (r: number): Promise<void> => {
    await sleep(r * stagger * 1000, sig);
    if (sig.aborted) return;
    if (lagSecs >= 0) await Promise.all(ts.map((t) => lagOne(t)));
    else await pool(ts.length, parallel, (i) => walkOne(r, ts[i]!), sig);
    if (!sig.aborted) done.push(Math.round((Date.now() - t0) / 100) / 10);
  };
  await Promise.all(Array.from({ length: readers }, (_, r) => runReader(r)));
  local.abort();
  const producer = prod ? await prod.done : {};
  const out: Record<string, unknown> = {
    walk: {
      from, readers, stagger_secs: stagger, targets: ts.length, readers_done_s: done,
      lag_mode: lagSecs < 0 ? null : prod ? "trailing" : "periodic",
      delivered_records_estimated: rd.estimated,
    },
    ...producer,
  };
  if (expect) out.verify = verify(expected(expect), reader0);
  return out;
}

interface Got {
  records: number;
  bytes: number;
  estimated: boolean;
}

/** Reader 0's per-target reads against the acked counts. A bytes
 * stream's record count is an estimate, so it is checked by bytes. */
function verify(exp: Map<string, StreamTally>, got: Map<string, Got>): Record<string, unknown> {
  let er = 0, eb = 0, dr = 0, db = 0;
  const bad: unknown[] = [];
  const keys = new Set([...exp.keys(), ...got.keys()]);
  for (const k of [...keys].sort()) {
    const e = exp.get(k) ?? { acked_records: 0, acked_bytes: 0, delivered_records: 0, delivered_bytes: 0 };
    const g = got.get(k) ?? { records: 0, bytes: 0, estimated: false };
    er += e.acked_records;
    eb += e.acked_bytes;
    dr += g.records;
    db += g.bytes;
    if (e.acked_bytes !== g.bytes || (!g.estimated && e.acked_records !== g.records)) {
      bad.push({ target: k, acked_records: e.acked_records, read_records: g.records, acked_bytes: e.acked_bytes, read_bytes: g.bytes });
    }
  }
  return {
    targets: keys.size,
    acked_records: er,
    read_records: dr,
    acked_bytes: eb,
    read_bytes: db,
    mismatched_targets: bad.length,
    examples: bad.slice(0, 10),
    ok: bad.length === 0,
  };
}

// ---------------------------------------------------------------- tail

export async function tail(ctx: Ctx, signal: AbortSignal): Promise<Record<string, unknown>> {
  const { o, http, meter } = ctx;
  const via = o.str("via", "long-poll");
  const per = o.int("readers", 1);
  const waitMs = o.int("wait-ms", 25_000);
  const from = o.str("from", "now");
  if (from !== "now" && from !== "beginning") throw new Error("tail --from must be now|beginning");
  const rd = new Reader(http, meter, o.int("record-bytes", 1024));
  const ts = targets(ctx);
  // Pin each subscription's start before the producer runs, so it sees
  // every record acked from here on.
  const starts = new Map<string, string>();
  if (from === "now") await pool(ts.length, 8, async (i) => void starts.set(ts[i]!.id, await headCursor(rd, ts[i]!)));
  const prod = startProducer(ctx, signal);
  const loops: Promise<void>[] = [];
  for (const t of ts) {
    for (let k = 0; k < per; k++) {
      const c = from === "now" ? starts.get(t.id) ?? "now" : "beginning";
      loops.push(via === "sse" ? rd.sse(t, c, signal) : rd.longPoll(t, c, waitMs, signal));
    }
  }
  await Promise.all(loops);
  const producer = prod ? await prod.done : {};
  return { tail: { via, readers_per_target: per, targets: ts.length, wait_ms: waitMs, delivered_records_estimated: rd.estimated }, ...producer };
}

// ---------------------------------------------------------------- subs

export async function subs(ctx: Ctx, signal: AbortSignal): Promise<Record<string, unknown>> {
  const { o, http, meter } = ctx;
  const count = o.int("count", 100);
  const via = o.str("via", "sse");
  const rate = o.num("connect-rate", 20);
  const waitMs = o.int("wait-ms", 25_000);
  const rd = new Reader(http, meter, o.int("record-bytes", 1024));
  const ts = targets(ctx);
  const prod = startProducer(ctx, signal);
  const loops: Promise<void>[] = [];
  for (let i = 0; i < count && !signal.aborted; i++) {
    const t = ts[i % ts.length]!;
    loops.push(via === "sse" ? rd.sse(t, "now", signal) : rd.longPoll(t, "now", waitMs, signal));
    meter.add("subscribers_started");
    await sleep(1000 / rate, signal);
  }
  await Promise.all(loops);
  const producer = prod ? await prod.done : {};
  return { subs: { via, count, connect_rate: rate, targets: ts.length }, ...producer };
}

// ---------------------------------------------------------------- group

interface PullMsg {
  leaseToken: string;
  attempts: number;
  value: unknown;
}

export async function group(ctx: Ctx, signal: AbortSignal): Promise<Record<string, unknown>> {
  const { o, http, meter } = ctx;
  const pull = o.int("pull", 10);
  if (pull < 1 || pull > 1000) throw new Error("--pull must be 1..1000 (the server clamps maxBatchRecords to 1..1000)");
  const consumers = o.int("consumers", 1);
  const name = o.str("group", `k2-g${pull}`);
  const settleMode = o.str("settle", "every");
  if (settleMode !== "every" && settleMode !== "none") throw new Error("--settle must be every|none");
  const waitMs = o.int("wait-ms", 1000);
  const visibilityMs = o.int("visibility-ms", 30_000);
  const retryFrac = o.num("retry-frac", 0);
  const extendFrac = o.num("extend-frac", 0);
  const untilEmpty = o.int("until-empty", 0) === 1;
  const dlq = o.str("dlq");
  const maxAttempts = o.has("max-attempts") ? o.int("max-attempts", 5) : undefined;
  const rng = new Rng(ctx.seed, 5);
  const ts: Target[] = Array.from({ length: ctx.streams }, (_, i) => {
    const n = streamName(ctx.prefix, i);
    return { name: n, key: "", id: n };
  });
  const bytesStream = new Map<string, boolean>();
  const cfg = JSON.stringify({ maxBatchRecords: pull, visibilityTimeoutMs: visibilityMs, maxAttempts, deadLetterStream: dlq });
  let conflicts = 0;
  await pool(ts.length, 8, async (i) => {
    const t = ts[i]!;
    const md = await http.call("GET", `/v1/streams/${encName(t.name)}`);
    if (md.status === 200) {
      const j = parseJson<{ contentType?: string }>(md, meter, "metadata");
      bytesStream.set(t.name, !(j?.contentType ?? "application/json").startsWith("application/json"));
    }
    for (let a = 0; a < 6; a++) {
      const r = await http.call("PUT", `/v1/streams/${encName(t.name)}/consumers/${encodeURIComponent(name)}`, { body: cfg, contentType: "application/json" });
      if (r.status === 200 || r.status === 201) return;
      if (r.status === 409 && errCode(r) === "consumer_config_conflict") {
        conflicts++;
        process.stderr.write(`[k2gen group] ${t.name}/${name}: existing config differs; pulls clamp to it\n`);
        return;
      }
      await sleep(backoffMs(r), stop.signal);
    }
  });
  const prod = startProducer(ctx, signal);
  const base = (t: Target) => `/v1/streams/${encName(t.name)}/consumers/${encodeURIComponent(name)}`;
  const loop = async (t: Target): Promise<void> => {
    const isBytes = bytesStream.get(t.name) ?? false;
    const body = JSON.stringify({ max: pull, waitMs });
    while (!signal.aborted) {
      // Not cancelled at the deadline: a pull that leased messages is
      // always read and settled, so the run's delivery count is exact.
      const r = await http.call("POST", `${base(t)}:pull`, { body, contentType: "application/json", timeoutMs: waitMs + 30_000 });
      if (r.status !== 200) {
        if (signal.aborted) return;
        await sleep(backoffMs(r), signal);
        continue;
      }
      meter.readMs(r.ms);
      meter.add("pulls");
      const j = parseJson<{ messages: PullMsg[]; backlog: number }>(r, meter, "pull");
      if (!j) {
        await sleep(1000, signal);
        continue;
      }
      const msgs = j.messages ?? [];
      if (msgs.length === 0) {
        meter.add("empty_polls");
        if (untilEmpty && j.backlog === 0 && !prod) return;
        continue;
      }
      let bytes = 0;
      for (const m of msgs) {
        bytes += isBytes && typeof m.value === "string" ? b64Len(m.value) : Buffer.byteLength(JSON.stringify(m.value));
        if (m.attempts > 1) meter.add("redelivered_records");
      }
      const tl = meter.tally(t.id);
      tl.delivered_records += msgs.length;
      tl.delivered_bytes += bytes;
      meter.add("delivered_records", msgs.length);
      meter.add("delivered_payload_bytes", bytes);
      if (settleMode !== "every") continue;
      const acks: object[] = [], retries: object[] = [], extendsList: object[] = [];
      for (const m of msgs) {
        const u = rng.next();
        if (u < retryFrac) retries.push({ leaseToken: m.leaseToken, delayMs: 1000 });
        else if (u < retryFrac + extendFrac) extendsList.push({ leaseToken: m.leaseToken, visibilityMs });
        else acks.push({ leaseToken: m.leaseToken });
      }
      const sb = JSON.stringify({ acks, retries, extends: extendsList });
      const s = await http.call("POST", `${base(t)}:settle`, { body: sb, contentType: "application/json" });
      meter.add("settles");
      if (s.status === 200) {
        const sj = parseJson<Record<string, number>>(s, meter, "settle") ?? {};
        for (const k of ["acked", "retried", "extended", "dlq", "stale"]) meter.add(`settle_${k}`, sj[k] ?? 0);
      }
    }
  };
  const loops: Promise<void>[] = [];
  for (const t of ts) for (let k = 0; k < consumers; k++) loops.push(loop(t));
  await Promise.all(loops);
  const producer = prod ? await prod.done : {};
  return {
    group: { name, pull, consumers_per_stream: consumers, settle: settleMode, wait_ms: waitMs, visibility_ms: visibilityMs, retry_frac: retryFrac, extend_frac: extendFrac, dlq: dlq ?? null, max_attempts: maxAttempts ?? null, until_empty: untilEmpty, config_conflicts: conflicts, streams: ts.length },
    ...producer,
  };
}
