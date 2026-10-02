// k2gen shared plumbing: flags, PRNG, Zipf, the header file, the HTTP
// client with wire-byte accounting, the log-bucket latency histogram,
// the 10 s window reporter and the exit ledger.
//
// Every number k2gen reports is counted here, so the window lines and
// the ledger always agree: each `Meter.add` updates the open window and
// the run totals together.

import { appendFileSync, mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

// ---------------------------------------------------------------- flags

export type Flags = Map<string, string>;

/** `--name value` and `--name=value`; a bare `--name` is "1". */
export function parseFlags(argv: string[]): { positional: string[]; flags: Flags } {
  const flags: Flags = new Map();
  const positional: string[] = [];
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    if (!a.startsWith("--")) {
      positional.push(a);
      continue;
    }
    const eq = a.indexOf("=");
    if (eq > 0) {
      flags.set(a.slice(2, eq), a.slice(eq + 1));
    } else if (i + 1 < argv.length && !argv[i + 1]!.startsWith("--")) {
      flags.set(a.slice(2), argv[++i]!);
    } else {
      flags.set(a.slice(2), "1");
    }
  }
  return { positional, flags };
}

export class Opts {
  constructor(readonly flags: Flags) {}
  has(k: string): boolean {
    return this.flags.has(k);
  }
  str(k: string, d: string): string;
  str(k: string, d?: string): string | undefined;
  str(k: string, d?: string): string | undefined {
    return this.flags.get(k) ?? d;
  }
  num(k: string, d: number): number {
    const v = this.flags.get(k);
    if (v === undefined) return d;
    const n = Number(v);
    if (!Number.isFinite(n)) throw new Error(`--${k} must be a number, got ${JSON.stringify(v)}`);
    return n;
  }
  int(k: string, d: number): number {
    const n = this.num(k, d);
    if (!Number.isInteger(n)) throw new Error(`--${k} must be an integer`);
    return n;
  }
  asObject(): Record<string, string> {
    return Object.fromEntries(this.flags);
  }
}

// ---------------------------------------------------------------- PRNG

/** sfc32 seeded through splitmix32: fast, 128-bit state, deterministic. */
export class Rng {
  private a: number;
  private b: number;
  private c: number;
  private d: number;
  constructor(seed: number, tag = 0) {
    let s = (seed ^ Math.imul(tag + 1, 0x9e3779b9)) >>> 0;
    const mix = () => {
      s = (s + 0x9e3779b9) >>> 0;
      let z = s;
      z = Math.imul(z ^ (z >>> 16), 0x85ebca6b);
      z = Math.imul(z ^ (z >>> 13), 0xc2b2ae35);
      return (z ^ (z >>> 16)) >>> 0;
    };
    this.a = mix();
    this.b = mix();
    this.c = mix();
    this.d = mix();
    for (let i = 0; i < 12; i++) this.u32();
  }
  u32(): number {
    const t = (((this.a + this.b) | 0) + this.d) | 0;
    this.d = (this.d + 1) | 0;
    this.a = this.b ^ (this.b >>> 9);
    this.b = (this.c + (this.c << 3)) | 0;
    this.c = (this.c << 21) | (this.c >>> 11);
    this.c = (this.c + t) | 0;
    return t >>> 0;
  }
  /** Uniform in [0, 1). */
  next(): number {
    return this.u32() / 4294967296;
  }
  int(n: number): number {
    return Math.floor(this.next() * n);
  }
  pick<T>(xs: readonly T[]): T {
    return xs[this.int(xs.length)]!;
  }
  /** Exponential with the given mean. */
  exp(mean: number): number {
    return -Math.log(1 - this.next()) * mean;
  }
  /** Log-normal with the given median and sigma. */
  lognormal(median: number, sigma: number): number {
    const u = 1 - this.next();
    const v = this.next();
    const z = Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
    return median * Math.exp(sigma * z);
  }
  fill(buf: Uint8Array): Uint8Array {
    const words = Math.floor(buf.byteLength / 4);
    const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    for (let i = 0; i < words; i++) view.setUint32(i * 4, this.u32(), true);
    for (let i = words * 4; i < buf.byteLength; i++) buf[i] = this.u32() & 0xff;
    return buf;
  }
}

/** Zipf over ranks 0..n-1 with exponent s (s = 0 is uniform). */
export class Zipf {
  private cdf: Float64Array | null;
  constructor(readonly n: number, readonly s: number) {
    if (n < 1) throw new Error("Zipf needs n >= 1");
    if (s === 0) {
      this.cdf = null;
      return;
    }
    this.cdf = new Float64Array(n);
    let acc = 0;
    for (let k = 0; k < n; k++) {
      acc += 1 / Math.pow(k + 1, s);
      this.cdf[k] = acc;
    }
  }
  sample(rng: Rng): number {
    if (!this.cdf) return rng.int(this.n);
    const u = rng.next() * this.cdf[this.n - 1]!;
    let lo = 0;
    let hi = this.n - 1;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.cdf[mid]! < u) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }
}

// ---------------------------------------------------------------- names

export const DEFAULT_KEY_B64 = Buffer.from(new Uint8Array(32).fill(7)).toString("base64");

/** Stream i of a prefix. Fixed width, so phases with different
 * --streams counts address the same names. */
export function streamName(prefix: string, i: number): string {
  return `${prefix}${String(i).padStart(5, "0")}`;
}

export function encName(name: string): string {
  return name.split("/").map(encodeURIComponent).join("/");
}

// ---------------------------------------------------------------- headers

/** The rig's header file: raw "Name: value" lines sent on every request.
 * Re-read when its mtime changes (checked every 2 s) and after a 401, so
 * the rig can rotate the bearer token under a running generator. */
export class HeaderSource {
  private map: Record<string, string> = {};
  private mtime = -1;
  private checked = 0;
  constructor(private readonly file: string | undefined, private readonly base: Record<string, string>) {
    this.load(true);
  }
  private load(force: boolean): void {
    if (!this.file) return;
    let m: number;
    try {
      m = statSync(this.file).mtimeMs;
    } catch (e) {
      if (force) throw new Error(`--headers-file ${this.file}: ${(e as Error).message}`);
      return;
    }
    if (!force && m === this.mtime) return;
    const map: Record<string, string> = {};
    for (const raw of readFileSync(this.file, "utf8").split(/\r?\n/)) {
      const line = raw.trim();
      if (!line || line.startsWith("#")) continue;
      const i = line.indexOf(":");
      if (i <= 0) continue;
      map[line.slice(0, i).trim().toLowerCase()] = line.slice(i + 1).trim();
    }
    this.map = map;
    this.mtime = m;
  }
  get(): Record<string, string> {
    const now = Date.now();
    if (this.file && now - this.checked > 2000) {
      this.checked = now;
      this.load(false);
    }
    return { ...this.base, ...this.map };
  }
  reloadSoon(): void {
    this.checked = 0;
  }
  /** The record key in force (the header file may carry its own). */
  key(): string {
    return this.get()["prisma-encryption-key"] ?? "";
  }
}

// ---------------------------------------------------------------- histogram

const SUB = 64;
const HBUCKETS = 128 + 26 * SUB;

/** HDR-like log-bucket histogram over microseconds: exact below 128 us,
 * then 64 sub-buckets per octave (relative error < 1.6%). */
export class Hist {
  counts = new Float64Array(HBUCKETS);
  n = 0;
  maxUs = 0;
  private static idx(us: number): number {
    if (us < 128) return us;
    const msb = 31 - Math.clz32(us);
    const shift = msb - 6;
    return 128 + (shift - 1) * SUB + ((us >>> shift) - SUB);
  }
  private static upper(idx: number): number {
    if (idx < 128) return idx;
    const k = idx - 128;
    const shift = Math.floor(k / SUB) + 1;
    return ((k % SUB) + SUB + 1) * 2 ** shift - 1;
  }
  record(ms: number): void {
    const us = Math.min(0x7fffffff, Math.max(0, Math.round(ms * 1000)));
    this.counts[Hist.idx(us)]!++;
    this.n++;
    if (us > this.maxUs) this.maxUs = us;
  }
  /** Highest value equivalent to the p-quantile, in ms (null when empty). */
  pct(p: number): number | null {
    if (this.n === 0) return null;
    const target = Math.max(1, Math.ceil(p * this.n));
    let acc = 0;
    for (let i = 0; i < HBUCKETS; i++) {
      acc += this.counts[i]!;
      if (acc >= target) return round3(Math.min(Hist.upper(i), this.maxUs) / 1000);
    }
    return round3(this.maxUs / 1000);
  }
  reset(): void {
    this.counts.fill(0);
    this.n = 0;
    this.maxUs = 0;
  }
}

export function round3(x: number): number {
  return Math.round(x * 1000) / 1000;
}

// ---------------------------------------------------------------- meter

export const BASE_FIELDS = [
  "acked_requests",
  "acked_records",
  "payload_bytes",
  "wire_bytes_up",
  "wire_bytes_down",
  "delivered_records",
  "delivered_payload_bytes",
] as const;

class Counters {
  f = new Map<string, number>();
  status: Record<string, number> = {};
  append = new Hist();
  read = new Hist();
  add(k: string, n: number): void {
    this.f.set(k, (this.f.get(k) ?? 0) + n);
  }
  get(k: string): number {
    return this.f.get(k) ?? 0;
  }
  render(): Record<string, unknown> {
    const o: Record<string, unknown> = {};
    for (const k of BASE_FIELDS) o[k] = this.get(k);
    o.status_counts = { ...this.status };
    o.append_p50_ms = this.append.pct(0.5);
    o.append_p99_ms = this.append.pct(0.99);
    o.read_p50_ms = this.read.pct(0.5);
    o.read_p99_ms = this.read.pct(0.99);
    for (const [k, v] of [...this.f.entries()].sort()) if (!(k in o)) o[k] = v;
    return o;
  }
}

export interface StreamTally {
  acked_records: number;
  acked_bytes: number;
  delivered_records: number;
  delivered_bytes: number;
}

export class Meter {
  win = new Counters();
  tot = new Counters();
  connected = 0;
  peakConnected = 0;
  inflight = 0;
  peakInflight = 0;
  perStream = new Map<string, StreamTally>();
  add(k: string, n = 1): void {
    if (n === 0) return;
    this.win.add(k, n);
    this.tot.add(k, n);
  }
  status(code: string): void {
    this.win.status[code] = (this.win.status[code] ?? 0) + 1;
    this.tot.status[code] = (this.tot.status[code] ?? 0) + 1;
  }
  /** Run totals of error answers by "status:error.code" (ledger only). */
  errorCodes: Record<string, number> = {};
  errorCode(key: string): void {
    this.errorCodes[key] = (this.errorCodes[key] ?? 0) + 1;
  }
  appendMs(ms: number): void {
    this.win.append.record(ms);
    this.tot.append.record(ms);
  }
  readMs(ms: number): void {
    this.win.read.record(ms);
    this.tot.read.record(ms);
  }
  tally(stream: string): StreamTally {
    let t = this.perStream.get(stream);
    if (!t) {
      t = { acked_records: 0, acked_bytes: 0, delivered_records: 0, delivered_bytes: 0 };
      this.perStream.set(stream, t);
    }
    return t;
  }
  connect(delta: number): void {
    this.connected += delta;
    if (this.connected > this.peakConnected) this.peakConnected = this.connected;
  }
  flight(delta: number): void {
    this.inflight += delta;
    if (this.inflight > this.peakInflight) this.peakInflight = this.inflight;
  }
}

// ---------------------------------------------------------------- reporter

export class Reporter {
  readonly startMs = Date.now();
  private winStart = performance.now();
  private timer: ReturnType<typeof setInterval> | undefined;
  private latest: Record<string, unknown> | null = null;
  private server: ReturnType<typeof Bun.serve> | undefined;
  constructor(
    readonly mode: string,
    readonly meter: Meter,
    private readonly out: string | undefined,
    private readonly ledger: string | undefined,
    private readonly windowSecs: number,
    private readonly args: Record<string, string>,
    port: number | undefined,
  ) {
    for (const f of [out, ledger]) if (f) mkdirSync(dirname(f), { recursive: true });
    if (out) writeFileSync(out, "");
    if (port) {
      this.server = Bun.serve({
        port,
        hostname: "0.0.0.0",
        fetch: () => Response.json({ mode, latest: this.latest, totals: this.totals(false) }),
      });
    }
  }
  start(): void {
    this.timer = setInterval(() => this.flush(false), this.windowSecs * 1000);
  }
  private flush(partial: boolean): void {
    const now = performance.now();
    const windowS = (now - this.winStart) / 1000;
    this.winStart = now;
    const m = this.meter;
    const line: Record<string, unknown> = {
      t: round3(Date.now() / 1000),
      elapsed_s: round3((Date.now() - this.startMs) / 1000),
      window_s: round3(windowS),
      mode: this.mode,
      ...m.win.render(),
      connected: m.connected,
      inflight: m.inflight,
    };
    if (partial) line.partial = true;
    this.latest = line;
    m.win = new Counters();
    if (this.out) appendFileSync(this.out, JSON.stringify(line) + "\n");
    const st = Object.entries(line.status_counts as Record<string, number>)
      .map(([k, v]) => `${k}:${v}`)
      .join(" ");
    process.stderr.write(
      `[k2gen ${this.mode}] +${line.elapsed_s}s acked=${line.acked_requests}/${line.acked_records}rec ` +
        `deliv=${line.delivered_records}rec p99=${line.append_p99_ms ?? "-"}ms conn=${m.connected} ` +
        `inflight=${m.inflight} [${st}]\n`,
    );
  }
  totals(final: boolean): Record<string, unknown> {
    const m = this.meter;
    const end = Date.now();
    const t: Record<string, unknown> = {
      mode: this.mode,
      start_ms: this.startMs,
      end_ms: end,
      start_wall: new Date(this.startMs).toISOString(),
      end_wall: new Date(end).toISOString(),
      duration_s: round3((end - this.startMs) / 1000),
      ...m.tot.render(),
      append_p90_ms: m.tot.append.pct(0.9),
      append_p999_ms: m.tot.append.pct(0.999),
      append_max_ms: m.tot.append.n ? round3(m.tot.append.maxUs / 1000) : null,
      append_samples: m.tot.append.n,
      read_p999_ms: m.tot.read.pct(0.999),
      read_samples: m.tot.read.n,
      connected: m.connected,
      peak_connected: m.peakConnected,
      peak_inflight: m.peakInflight,
      error_codes: { ...m.errorCodes },
      args: this.args,
    };
    if (final) {
      const ps: Record<string, StreamTally> = {};
      for (const [k, v] of [...m.perStream.entries()].sort()) ps[k] = v;
      t.per_stream = ps;
    }
    return t;
  }
  async finish(more: Record<string, unknown> = {}): Promise<Record<string, unknown>> {
    if (this.timer) clearInterval(this.timer);
    this.flush(true);
    const t = { ...this.totals(true), ...more };
    if (this.ledger) writeFileSync(this.ledger, JSON.stringify(t, null, 2) + "\n");
    this.server?.stop(true);
    return t;
  }
}

// ---------------------------------------------------------------- stop

/** Aborted by SIGINT/SIGTERM; a further signal more than 2 s after the
 * first exits at once (a group-delivered signal plus the re-exec
 * parent's forwarded copy arrive together and count once). */
export const stop = new AbortController();
export function installSignals(): void {
  let first = 0;
  for (const sig of ["SIGINT", "SIGTERM"] as const) {
    process.on(sig, () => {
      if (first && Date.now() - first > 2000) process.exit(130);
      if (first) return;
      first = Date.now();
      process.stderr.write(`[k2gen] ${sig}: stopping, writing the ledger\n`);
      stop.abort();
    });
  }
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (ms <= 0) return Promise.resolve();
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const t = setTimeout(done, ms);
    function done() {
      clearTimeout(t);
      signal?.removeEventListener("abort", done);
      resolve();
    }
    signal?.addEventListener("abort", done, { once: true });
  });
}

/** An AbortSignal that fires at the deadline or on stop. */
export function deadlineSignal(durationSecs: number): AbortSignal {
  return AbortSignal.any([stop.signal, AbortSignal.timeout(Math.max(1, durationSecs * 1000))]);
}

// ---------------------------------------------------------------- http

export interface Resp {
  status: number; // 0 = no response
  headers: Headers | null;
  body: Uint8Array;
  ms: number;
  error?: string;
}

const dec = new TextDecoder();
export const utf8 = (b: Uint8Array): string => dec.decode(b);

/** The product client. Counts request bodies sent (wire_bytes_up) and
 * response bodies received (wire_bytes_down), every status, and
 * transport failures as "err"/"timeout" (an abort we caused is
 * "cancelled"). Headers are not counted. */
export class Http {
  constructor(
    readonly base: string,
    readonly hdrs: HeaderSource,
    readonly meter: Meter,
  ) {}
  async call(
    method: string,
    path: string,
    opt: { body?: Uint8Array | string; contentType?: string; timeoutMs?: number; signal?: AbortSignal; extra?: Record<string, string> } = {},
  ): Promise<Resp> {
    const h = this.hdrs.get();
    if (opt.contentType) h["content-type"] = opt.contentType;
    if (opt.extra) Object.assign(h, opt.extra);
    const up = opt.body === undefined ? 0 : typeof opt.body === "string" ? Buffer.byteLength(opt.body) : opt.body.byteLength;
    const timeout = AbortSignal.timeout(opt.timeoutMs ?? 60_000);
    const signal = opt.signal ? AbortSignal.any([timeout, opt.signal]) : timeout;
    const t0 = performance.now();
    this.meter.add("requests");
    this.meter.add("wire_bytes_up", up);
    try {
      const res = await fetch(this.base + path, { method, headers: h, body: opt.body, signal });
      const body = new Uint8Array(await res.arrayBuffer());
      this.meter.add("wire_bytes_down", body.byteLength);
      this.meter.status(String(res.status));
      if (res.status === 401) this.hdrs.reloadSoon();
      const r: Resp = { status: res.status, headers: res.headers, body, ms: performance.now() - t0 };
      if (res.status >= 400) this.meter.errorCode(`${res.status}:${errCode(r) || "-"}`);
      return r;
    } catch (e) {
      const code = opt.signal?.aborted ? "cancelled" : timeout.aborted ? "timeout" : "err";
      this.meter.status(code);
      if (code !== "cancelled") this.meter.add("transport_errors");
      return { status: 0, headers: null, body: new Uint8Array(0), ms: performance.now() - t0, error: `${code}: ${(e as Error).message}` };
    }
  }
  /** A streaming GET (SSE). Returns the open Response or a failure. */
  async open(path: string, signal: AbortSignal): Promise<Response | Resp> {
    const t0 = performance.now();
    this.meter.add("requests");
    try {
      const res = await fetch(this.base + path, { headers: this.hdrs.get(), signal });
      this.meter.status(String(res.status));
      if (res.status === 401) this.hdrs.reloadSoon();
      return res;
    } catch (e) {
      const code = signal.aborted ? "cancelled" : "err";
      this.meter.status(code);
      if (code !== "cancelled") this.meter.add("transport_errors");
      return { status: 0, headers: null, body: new Uint8Array(0), ms: performance.now() - t0, error: `${code}: ${(e as Error).message}` };
    }
  }
}

/** The SDK's retry rule (sdk/src/index.ts `retryableRequestError`): a
 * 429 or 503 whose error body does not say `retryable: false`. A
 * transport failure is ambiguous for an append and is never retried. */
export function sdkRetryable(r: Resp): boolean {
  if (r.status !== 429 && r.status !== 503) return false;
  try {
    const j = JSON.parse(utf8(r.body)) as { error?: { retryable?: boolean } };
    return j.error?.retryable !== false;
  } catch {
    return true;
  }
}

/** The SDK's backoff: retry-after seconds clamped to 0..5 (default 1). */
export function sdkBackoffMs(r: Resp): number {
  const ra = Number(r.headers?.get("retry-after") ?? "1");
  return (Number.isFinite(ra) ? Math.max(0, Math.min(ra, 5)) : 1) * 1000;
}

/** Seconds to wait after a 429/503, from retry-after (1..5 s). */
export function backoffMs(r: Resp): number {
  const ra = Number(r.headers?.get("retry-after") ?? "1");
  return (Number.isFinite(ra) ? Math.max(0.2, Math.min(ra, 5)) : 1) * 1000;
}

/** Bodies that should have been JSON and were not (first 5 kept for
 * the ledger's `bad_json`). */
export const badJson: Array<Record<string, unknown>> = [];

/** Parse a JSON answer; a body that does not parse is counted
 * (`bad_json`) and kept as an example, never a crash. */
export function parseJson<T>(r: Resp, meter: Meter, what: string): T | null {
  try {
    return JSON.parse(utf8(r.body)) as T;
  } catch (e) {
    meter.add("bad_json");
    if (badJson.length < 5) {
      badJson.push({ what, status: r.status, bytes: r.body.byteLength, ms: Math.round(r.ms), error: (e as Error).message, head: utf8(r.body.slice(0, 160)) });
    }
    return null;
  }
}

export function errCode(r: Resp): string {
  try {
    const j = JSON.parse(utf8(r.body)) as { error?: { code?: string } };
    return j.error?.code ?? "";
  } catch {
    return "";
  }
}

/** Decoded byte length of a base64 string. */
export function b64Len(s: string): number {
  let pad = 0;
  if (s.endsWith("==")) pad = 2;
  else if (s.endsWith("=")) pad = 1;
  return Math.floor((s.length * 3) / 4) - pad;
}

/** Run `n` jobs with at most `c` in flight. */
export async function pool(n: number, c: number, job: (i: number) => Promise<void>, signal?: AbortSignal): Promise<void> {
  let next = 0;
  const worker = async () => {
    while (next < n && !signal?.aborted) await job(next++);
  };
  await Promise.all(Array.from({ length: Math.max(1, Math.min(c, n)) }, worker));
}
