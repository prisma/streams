#!/usr/bin/env bun
// edgeprobe: the edge arm of the shared-cell certification (public-beta
// deploy-q5 design §3.5, triage item B6). Facts about the Compute edge in
// front of a single-server cell, for the owner and the Compute team, then
// one posture check. Every mode writes JSON lines (--out FILE, else stdout),
// each record carrying its vantage (--vantage, default the hostname) and its
// wall-clock time; `report` merges the records of several vantages.
//
//   bun bench/edgeprobe/edgeprobe.ts <mode> [flags]       (bun edgeprobe.ts help)
//
// Connections are raw HTTP/1.1 over node:net/node:tls, one socket each, so a
// count is a count of sockets the edge accepted (fetch pools and queues).
// Run it from K >= 3 source IPs (the laptop plus app-gen in two other
// regions, as bench/soak/wc-ladder.sh does) and one in-region app-gen, the
// Compute customer's vantage. Raise the descriptor limit first for the wall
// (`ulimit -n 8192`).

import { appendFileSync, readFileSync } from "node:fs";
import { connect as netConnect, type Socket } from "node:net";
import { hostname } from "node:os";
import { join } from "node:path";
import { connect as tlsConnect } from "node:tls";
import { DEFAULT_KEY_B64, HeaderSource, Opts, encName, parseFlags, streamName } from "../k2cost/common.ts";

const USAGE = `edgeprobe <mode> [flags]          (records: JSON lines to --out FILE, else stdout)

  idle      --target URL [--gaps 30,60,90,110,125,180,300] [--path /health]
            one keep-alive connection per gap: a request, the gap idle, the same request again
  park      --target URL --headers-file F (--stream NAME | --streams N --prefix P) --count N --rate R
            [--via sse|long-poll|mix] [--wait-ms 25000] [--hold SECS] [--settle-ms 250]
            ramp parked subscriptions, one socket each; record how many the edge holds
  fresh     --target URL [--every SECS] [--duration SECS] [--path /health]
            a new connection each time: does a client on this IP still get in, and how does it fail
  write     --target URL --headers-file F (--stream | --streams --prefix) --rate R [--conns 4] --duration SECS
            appends on persistent connections: do writers starve at the wall
  tls-idle  --target URL --notice FILE --measured-wall N [--count N] [--rate R] [--hold SECS]
            handshaken connections that never send a request (only behind a dated Compute notice
            naming the service and this vantage, with Compute's reply and go-ahead; at most the
            measured wall + 10%)
  sse-first --target URL --headers-file F --stream NAME [--n 10] [--settle 1] [--timeout 70]
            time from an append to its event on a raw SSE subscription
  deadline  --target URL --headers-file F --stream NAME [--waits 25,55,65] [--repeat 3]
            long-polls on a quiet stream: who answers each wait and after how long (the origin caps waits at 25 s)
  report    FILE.jsonl... [--wall N]           facts from the records of every vantage
  posture   --wall N --long-polls N --writer-headroom N [--sse-max N]
            PASS when SSE_MAX_CONNECTIONS (the layered profiles) + long-polls + headroom < wall

Common: --vantage NAME (default the hostname), --out FILE
`;

type Rec = Record<string, unknown>;

class ProbeError extends Error {
  constructor(readonly kind: string) {
    super(kind);
  }
}

/// What a socket failure was, in the words the facts use.
function kindOf(e: unknown): string {
  const code = String((e as { code?: string })?.code ?? "");
  const msg = String((e as Error)?.message ?? e);
  if (code === "ECONNREFUSED") return "refused";
  if (code === "ECONNRESET" || code === "EPIPE") return "reset";
  if (code === "ETIMEDOUT") return "timeout";
  if (code === "ENOTFOUND" || code === "EAI_AGAIN") return "dns";
  if (code.startsWith("ERR_TLS") || /tls|certificate|handshake/i.test(msg)) return "tls";
  return code || "error";
}

type Head = { status: number; headers: Record<string, string> };

/// One raw HTTP/1.1 connection.
class Conn {
  private buf = Buffer.alloc(0);
  private wake: (() => void) | null = null;
  closed = false;
  closedAt = 0;
  err: string | null = null;

  private constructor(readonly sock: Socket, readonly url: URL, readonly connectMs: number) {
    sock.on("data", (d: Buffer) => {
      this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
      this.poke();
    });
    sock.on("close", () => {
      this.closed = true;
      this.closedAt = Date.now();
      this.poke();
    });
    sock.on("error", (e) => (this.err ??= kindOf(e)));
  }

  static open(url: URL, timeoutMs = 10_000): Promise<Conn> {
    const t0 = Date.now();
    const tls = url.protocol === "https:";
    const host = url.hostname;
    const port = Number(url.port || (tls ? 443 : 80));
    return new Promise((resolve, reject) => {
      const sock = tls ? tlsConnect({ host, port, servername: host, ALPNProtocols: ["http/1.1"] }) : netConnect({ host, port });
      const timer = setTimeout(() => (sock.destroy(), reject(new ProbeError("connect_timeout"))), timeoutMs);
      sock.once(tls ? "secureConnect" : "connect", () => (clearTimeout(timer), resolve(new Conn(sock, url, Date.now() - t0))));
      sock.once("error", (e) => (clearTimeout(timer), reject(new ProbeError(kindOf(e)))));
    });
  }

  private poke(): void {
    const w = this.wake;
    this.wake = null;
    w?.();
  }

  /// Waits for more bytes or the close; rejects once closed or past `deadline`.
  private more(deadline: number): Promise<void> {
    if (this.closed) return Promise.reject(new ProbeError(this.err ?? "reset"));
    const left = deadline - Date.now();
    if (left <= 0) return Promise.reject(new ProbeError("timeout"));
    return new Promise((resolve) => {
      const t = setTimeout(() => ((this.wake = null), resolve()), Math.min(left, 2 ** 31 - 1));
      this.wake = () => (clearTimeout(t), resolve());
    });
  }

  send(method: string, path: string, headers: Record<string, string> = {}, body?: string): void {
    const lines = [`${method} ${path} HTTP/1.1`, `host: ${this.url.host}`, "user-agent: streams-edgeprobe", "connection: keep-alive"];
    for (const [k, v] of Object.entries(headers)) lines.push(`${k}: ${v}`);
    if (body !== undefined) lines.push(`content-length: ${Buffer.byteLength(body)}`);
    this.sock.write(`${lines.join("\r\n")}\r\n\r\n${body ?? ""}`);
  }

  async head(timeoutMs: number): Promise<Head> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const end = this.buf.indexOf("\r\n\r\n");
      if (end >= 0) {
        const [status, ...rest] = this.buf.subarray(0, end).toString("latin1").split("\r\n");
        this.buf = this.buf.subarray(end + 4);
        const headers: Record<string, string> = {};
        for (const line of rest) {
          const i = line.indexOf(":");
          if (i > 0) headers[line.slice(0, i).trim().toLowerCase()] = line.slice(i + 1).trim();
        }
        return { status: Number(status!.split(" ")[1]), headers };
      }
      await this.more(deadline);
    }
  }

  /// The body by its framing (none, content-length, chunked, or until the
  /// close); `onChunk` sees each piece as it arrives. Keeps at most 4 KiB.
  async body(h: Head, timeoutMs: number, onChunk?: (b: Buffer) => void): Promise<string> {
    const deadline = Date.now() + timeoutMs;
    let kept = Buffer.alloc(0);
    const take = (n: number) => {
      const piece = this.buf.subarray(0, n);
      this.buf = this.buf.subarray(n);
      if (kept.length < 4096) kept = Buffer.concat([kept, piece.subarray(0, 4096 - kept.length)]);
      onChunk?.(piece);
    };
    if (h.status === 204 || h.status === 304 || h.status < 200) return "";
    if ((h.headers["transfer-encoding"] ?? "").includes("chunked")) {
      for (;;) {
        let eol = this.buf.indexOf("\r\n");
        while (eol < 0) (await this.more(deadline), (eol = this.buf.indexOf("\r\n")));
        const size = parseInt(this.buf.subarray(0, eol).toString(), 16);
        while (this.buf.length < eol + 2 + size + 2) await this.more(deadline);
        this.buf = this.buf.subarray(eol + 2);
        if (size === 0) {
          this.buf = this.buf.subarray(2);
          return kept.toString();
        }
        take(size);
        this.buf = this.buf.subarray(2);
      }
    }
    const len = h.headers["content-length"];
    if (len !== undefined) {
      while (this.buf.length < Number(len)) await this.more(deadline);
      take(Number(len));
      return kept.toString();
    }
    for (;;) {
      if (this.buf.length) take(this.buf.length);
      try {
        await this.more(deadline);
      } catch (e) {
        if (this.closed) return kept.toString();
        throw e;
      }
    }
  }

  /// Resolves when the peer closes, or after `ms`.
  idle(ms: number): Promise<void> {
    if (this.closed) return Promise.resolve();
    return new Promise((r) => {
      const t = setTimeout(r, ms);
      this.sock.once("close", () => (clearTimeout(t), r()));
    });
  }

  destroy(): void {
    this.sock.destroy();
  }
}

/// One request on a fresh connection: status, whether the origin answered,
/// the body (kept short), timings, or the failure.
async function once(url: URL, method: string, path: string, headers: Record<string, string>, timeoutMs: number, body?: string) {
  const t0 = Date.now();
  let c: Conn | undefined;
  try {
    c = await Conn.open(url, Math.min(timeoutMs, 10_000));
    c.send(method, path, headers, body);
    const h = await c.head(timeoutMs);
    const text = await c.body(h, timeoutMs);
    return { ok: true, status: h.status, origin: isOrigin(h), body: text, connect_ms: c.connectMs, ms: Date.now() - t0 };
  } catch (e) {
    return { ok: false, status: 0, origin: false, body: "", error: e instanceof ProbeError ? e.kind : kindOf(e), ms: Date.now() - t0 };
  } finally {
    c?.destroy();
  }
}

/// Who answered. The cell sets x-streams-boot-id on /health only
/// (src/http.rs), and its product CORS header on every /v1/streams and
/// usage answer, successes, errors, 204s, long-polls and SSE alike:
/// access-control-expose-headers naming prisma-next-cursor (src/product.rs
/// with_product_cors). An answer carrying neither is the edge's.
const isOrigin = (h: Head) =>
  "x-streams-boot-id" in h.headers || /\bprisma-next-cursor\b/.test(h.headers["access-control-expose-headers"] ?? "");
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));
const secs = (o: Opts, k: string, d: number) => o.num(k, d) * 1000;

class Out {
  constructor(readonly file: string | undefined, readonly vantage: string) {}
  write(rec: Rec): void {
    const line = `${JSON.stringify({ t: Date.now(), vantage: this.vantage, ...rec })}\n`;
    if (this.file) appendFileSync(this.file, line);
    else process.stdout.write(line);
  }
}

function streamsOf(o: Opts): string[] {
  const one = o.str("stream");
  if (one) return one.split(",");
  const n = o.int("streams", 1);
  return Array.from({ length: n }, (_, i) => streamName(o.str("prefix", "k2/s"), i));
}

function product(o: Opts): () => Record<string, string> {
  const src = new HeaderSource(o.str("headers-file"), { "streams-encryption-key": DEFAULT_KEY_B64 });
  return () => src.get();
}

const sse = (name: string) => `/v1/streams/${encName(name)}/records:sse?cursor=now`;
const longPoll = (name: string, waitMs: number) => `/v1/streams/${encName(name)}/records:long-poll?cursor=now&waitMs=${waitMs}`;

// ---------------------------------------------------------------- idle

async function idle(o: Opts, out: Out, url: URL): Promise<void> {
  const path = o.str("path", "/health");
  const gaps = o.str("gaps", "30,60,90,110,125,180,300").split(",").map(Number);
  await Promise.all(gaps.map(async (gap) => {
    const rec: Rec = { kind: "idle", gap_s: gap };
    let c: Conn | undefined;
    try {
      c = await Conn.open(url);
      c.send("GET", path);
      const h1 = await c.head(15_000);
      await c.body(h1, 15_000);
      rec.first_status = h1.status;
      const t0 = Date.now();
      await c.idle(gap * 1000);
      if (c.closed) {
        Object.assign(rec, { outcome: "closed", closed_after_s: Math.round((c.closedAt - t0) / 10) / 100 });
      } else {
        c.send("GET", path);
        try {
          const h2 = await c.head(15_000);
          await c.body(h2, 15_000);
          Object.assign(rec, { outcome: h2.status < 300 ? "reused" : "status", status: h2.status, origin: isOrigin(h2) });
        } catch (e) {
          rec.outcome = e instanceof ProbeError ? e.kind : kindOf(e);
        }
      }
    } catch (e) {
      rec.outcome = `first request: ${e instanceof ProbeError ? e.kind : kindOf(e)}`;
    } finally {
      c?.destroy();
    }
    out.write(rec);
  }));
}

// ---------------------------------------------------------------- park

async function park(o: Opts, out: Out, url: URL): Promise<void> {
  const streams = streamsOf(o);
  const hdrs = product(o);
  const count = o.int("count", 100);
  const rate = o.num("rate", 20);
  const via = o.str("via", "sse");
  const waitMs = o.int("wait-ms", 25_000);
  const settle = o.int("settle-ms", 250);
  const conns = new Set<Conn>();
  const held = { sse: new Set<number>(), lp: new Map<number, number>() };
  let attempts = 0, failed = 0, dropped = 0, stopping = false, lastWrite = 0;
  const count_ = () => {
    const now = Date.now();
    let lp = 0;
    for (const since of held.lp.values()) if (now - since >= settle) lp++;
    return { held_sse: held.sse.size, held_lp: lp };
  };
  const record = (force = false) => {
    if (!force && Date.now() - lastWrite < 100) return;
    lastWrite = Date.now();
    out.write({ kind: "park", ...count_(), attempts, failed, dropped });
  };
  const ticker = setInterval(() => record(), 100);
  const fail = (i: number, what: string, error: string, h?: Head, body = "") => {
    failed++;
    out.write({ kind: "park_fail", slot: i, via: what, error, status: h?.status ?? 0, origin: h ? isOrigin(h) : false,
                body: body.slice(0, 160), ...count_() });
  };
  const slot = async (i: number) => {
    const what = via === "mix" ? (i % 2 ? "long-poll" : "sse") : via;
    const name = streams[i % streams.length]!;
    let c: Conn;
    try {
      c = await Conn.open(url);
    } catch (e) {
      return fail(i, what, (e as ProbeError).kind ?? kindOf(e));
    }
    conns.add(c);
    try {
      if (what === "sse") {
        c.send("GET", sse(name), hdrs());
        const h = await c.head(20_000);
        if (h.status !== 200) return fail(i, what, "status", h, await c.body(h, 5_000).catch(() => ""));
        held.sse.add(i);
        await c.body(h, 1e12).catch(() => {});
        held.sse.delete(i);
      } else {
        for (;;) {
          c.send("GET", longPoll(name, waitMs), hdrs());
          held.lp.set(i, held.lp.has(i) ? 0 : Date.now());
          const h = await c.head(waitMs + 30_000);
          await c.body(h, 30_000);
          if (h.status !== 200 && h.status !== 204) {
            held.lp.delete(i);
            return fail(i, what, "status", h);
          }
          if (stopping || h.headers.connection === "close") break;
        }
        held.lp.delete(i);
      }
      if (!stopping) (dropped++, out.write({ kind: "park_drop", slot: i, via: what, error: c.err ?? "closed", ...count_() }));
    } catch (e) {
      const known = held.sse.delete(i) || held.lp.delete(i);
      if (!stopping) {
        if (known) (dropped++, out.write({ kind: "park_drop", slot: i, via: what, error: (e as ProbeError).kind, ...count_() }));
        else fail(i, what, (e as ProbeError).kind ?? kindOf(e));
      }
    }
  };
  const running: Promise<void>[] = [];
  const t0 = Date.now();
  for (; attempts < count; attempts++) {
    running.push(slot(attempts));
    const due = t0 + ((attempts + 1) * 1000) / rate;
    if (due > Date.now()) await sleep(due - Date.now());
  }
  await sleep(secs(o, "hold", 60));
  record(true);
  stopping = true;
  clearInterval(ticker);
  for (const c of conns) c.destroy();
  await Promise.allSettled(running);
}

// ---------------------------------------------------------------- fresh

async function fresh(o: Opts, out: Out, url: URL): Promise<void> {
  const path = o.str("path", "/health");
  const every = secs(o, "every", 2);
  const end = Date.now() + secs(o, "duration", 600);
  while (Date.now() < end) {
    const t0 = Date.now();
    const r = await once(url, "GET", path, {}, 10_000);
    const theirs = r.ok && !r.origin ? { body: r.body.slice(0, 160) } : {};
    out.write({ kind: "fresh", t: t0, ...r, ok: r.ok && r.status === 200 && r.origin, body: undefined, ...theirs });
    await sleep(Math.max(0, t0 + every - Date.now()));
  }
}

// ---------------------------------------------------------------- write

async function write(o: Opts, out: Out, url: URL): Promise<void> {
  const streams = streamsOf(o);
  const hdrs = product(o);
  const rate = o.num("rate", 10);
  const k = o.int("conns", 4);
  const t0 = Date.now();
  const end = t0 + secs(o, "duration", 600);
  let win = { ok: 0, failed: 0, statuses: {} as Record<string, number> };
  const flush = () => {
    out.write({ kind: "write", conns: k, ...win });
    win = { ok: 0, failed: 0, statuses: {} };
  };
  const ticker = setInterval(flush, 1000);
  let seq = 0;
  await Promise.all(Array.from({ length: k }, async (_, j) => {
    let c: Conn | undefined;
    for (let n = 0; ; n++) {
      const due = t0 + ((n * k + j) * 1000) / rate;
      if (due >= end) break;
      if (due > Date.now()) await sleep(due - Date.now());
      let status = "0";
      try {
        if (!c || c.closed) c = await Conn.open(url);
        c.send("POST", `/v1/streams/${encName(streams[seq % streams.length]!)}/records`,
               { ...hdrs(), "content-type": "application/json" }, JSON.stringify({ edgeprobe: seq++ }));
        const h = await c.head(30_000);
        await c.body(h, 30_000);
        status = String(h.status);
      } catch (e) {
        status = e instanceof ProbeError ? e.kind : kindOf(e);
        c?.destroy();
        c = undefined;
      }
      win.statuses[status] = (win.statuses[status] ?? 0) + 1;
      status === "200" ? win.ok++ : win.failed++;
    }
    c?.destroy();
  }));
  clearInterval(ticker);
  flush();
}

// ---------------------------------------------------------------- tls-idle

/// Unauthenticated exhaustion runs only behind a dated Compute notice that
/// names the service and this vantage and carries Compute's reply with an
/// explicit go-ahead (triage §4 (13) asks them whether to run it at all),
/// and never past the measured wall + 10% (triage D11). Returns the
/// refusal, or the cap, the count and the notice.
function noticeRefusal(o: Opts, url: URL, vantage: string): { refusal: string } | { cap: number; count: number; notice: Rec } {
  const file = o.str("notice");
  if (!file) return { refusal: "tls-idle needs --notice FILE: the dated notice to Compute naming the service" };
  let notice: Rec;
  try {
    notice = JSON.parse(readFileSync(file, "utf8"));
  } catch (e) {
    return { refusal: `--notice ${file}: ${(e as Error).message}` };
  }
  const date = String(notice.notified ?? "");
  const today = new Date().toISOString().slice(0, 10);
  if (!/^\d{4}-\d\d-\d\d$/.test(date)) return { refusal: `the notice has no "notified": "YYYY-MM-DD" date` };
  if (date > today) return { refusal: `the notice is dated ${date}, after today (${today})` };
  if (notice.service !== url.hostname) return { refusal: `the notice names service ${String(notice.service)}, not ${url.hostname}` };
  if (!Array.isArray(notice.vantages) || !notice.vantages.includes(vantage)) {
    return { refusal: `vantage ${vantage} is not one the notice lists (${JSON.stringify(notice.vantages)})` };
  }
  if (typeof notice.reply !== "string" || !notice.reply.trim()) return { refusal: `the notice has no "reply": Compute's answer` };
  if (notice.go_ahead !== true) return { refusal: `the notice has no "go_ahead": true: Compute has not agreed to the run` };
  if (!o.has("measured-wall")) return { refusal: "tls-idle needs --measured-wall N: the wall leg's count" };
  // Integer arithmetic, rounded down: the cap never exceeds the wall + 10%.
  const cap = Math.floor((o.int("measured-wall", 0) * 11) / 10);
  const count = o.int("count", cap);
  if (count > cap) return { refusal: `--count ${count} is over the cap ${cap} (the measured wall + 10%)` };
  return { cap, count, notice };
}

async function tlsIdle(o: Opts, out: Out, url: URL): Promise<number> {
  const ok = noticeRefusal(o, url, out.vantage);
  if ("refusal" in ok) {
    process.stderr.write(`edgeprobe tls-idle: ${ok.refusal}\n`);
    return 2;
  }
  out.write({ kind: "tls_idle_start", cap: ok.cap, count: ok.count, notice: ok.notice });
  const rate = o.num("rate", 20);
  const conns: Conn[] = [];
  let failed = 0;
  const record = () => out.write({ kind: "tls_idle", held: conns.filter((c) => !c.closed).length, failed });
  const ticker = setInterval(record, 1000);
  const t0 = Date.now();
  const opening: Promise<void>[] = [];
  for (let i = 0; i < ok.count; i++) {
    opening.push(Conn.open(url).then((c) => void conns.push(c), (e) => {
      failed++;
      out.write({ kind: "tls_idle_fail", error: (e as ProbeError).kind, held: conns.filter((c) => !c.closed).length });
    }));
    const due = t0 + ((i + 1) * 1000) / rate;
    if (due > Date.now()) await sleep(due - Date.now());
  }
  await Promise.allSettled(opening);
  await sleep(secs(o, "hold", 60));
  clearInterval(ticker);
  record();
  for (const c of conns) c.destroy();
  return 0;
}

// ---------------------------------------------------------------- sse-first, deadline

async function sseFirst(o: Opts, out: Out, url: URL): Promise<void> {
  const name = o.str("stream", "");
  const hdrs = product(o);
  const timeout = secs(o, "timeout", 70);
  for (let i = 0; i < o.int("n", 10); i++) {
    const rec: Rec = { kind: "sse_first", n: i + 1 };
    const t0 = Date.now();
    let c: Conn | undefined;
    try {
      c = await Conn.open(url);
      c.send("GET", sse(name), hdrs());
      const h = await c.head(timeout);
      Object.assign(rec, { status: h.status, origin: isOrigin(h), headers_ms: Date.now() - t0 });
      if (h.status === 200) {
        let event: number | null = null;
        let text = "";
        const reading = c.body(h, timeout, (b) => {
          text += b.toString();
          if (event === null && /(^|\n)event: data\n/.test(text)) event = Date.now();
        }).catch(() => {});
        await sleep(secs(o, "settle", 1));
        const sent = Date.now();
        const ack = await once(url, "POST", `/v1/streams/${encName(name)}/records`, { ...hdrs(), "content-type": "application/json" },
                               30_000, JSON.stringify({ edgeprobe: "sse-first", n: i }));
        rec.ack_status = ack.status;
        rec.ack_ms = ack.ms;
        while (event === null && Date.now() - sent < timeout && !c.closed) await sleep(10);
        rec.event_ms = event === null ? null : event - sent;
        if (event === null) rec.error = c.closed ? "closed" : "timeout";
        c.destroy();
        await reading;
      }
    } catch (e) {
      rec.error = e instanceof ProbeError ? e.kind : kindOf(e);
    } finally {
      c?.destroy();
    }
    out.write(rec);
  }
}

async function deadline(o: Opts, out: Out, url: URL): Promise<void> {
  const name = o.str("stream", "");
  const hdrs = product(o);
  const waits = o.str("waits", "25,55,65").split(",").map(Number);
  const runs = Array.from({ length: o.int("repeat", 3) }, () => waits).flat();
  await Promise.all(runs.map(async (w) => {
    const r = await once(url, "GET", longPoll(name, Math.round(w * 1000)), hdrs(), w * 1000 + 30_000);
    out.write({ kind: "deadline", wait_s: w, status: r.status, origin: r.origin, elapsed_ms: r.ms, error: r.error,
                body: r.origin ? undefined : r.body.slice(0, 160) });
  }));
}

// ---------------------------------------------------------------- report

const q = (xs: number[], p: number) => {
  const s = [...xs].sort((a, b) => a - b);
  return s.length ? s[Math.min(s.length - 1, Math.floor(p * s.length))]! : NaN;
};

/// Records of every vantage, merged into facts.
export function report(recs: Rec[], wall?: number): string[] {
  const lines: string[] = [];
  const of = (k: string) => recs.filter((r) => r.kind === k).sort((a, b) => (a.t as number) - (b.t as number));
  for (const r of of("idle").sort((a, b) => `${a.vantage}`.localeCompare(`${b.vantage}`) || (a.gap_s as number) - (b.gap_s as number))) {
    const at = `idle: ${r.vantage} ${r.gap_s} s:`;
    if (r.outcome === "reused") lines.push(`FACT ${at} reused, ${r.status}`);
    else if (r.outcome === "closed") lines.push(`FACT ${at} the peer closed the idle connection after ${r.closed_after_s} s`);
    else if (r.outcome === "status" && !r.origin) {
      lines.push(`FINDING ${at} reuse answered ${r.status} by the edge, not the origin: the edge's upstream idle timeout outlives the origin's ` +
                 `(the binary's 120 s h1_header_timeout); set it explicitly (a config change and an edge record)`);
    } else if (r.outcome === "status") lines.push(`FACT ${at} reuse answered ${r.status} by the origin`);
    else lines.push(`FACT ${at} reuse failed: ${r.outcome}`);
  }
  // Held at a moment: each parking vantage's largest count from 1 s before
  // to 0.5 s after it (a long-poll counts once settled; records every 0.1 s).
  const holders = [...of("park"), ...of("tls_idle")];
  const heldAt = (t: number) => {
    const per = new Map<string, number>();
    for (const r of holders) {
      if ((r.t as number) < t - 1000 || (r.t as number) > t + 500) continue;
      const n = r.kind === "park" ? (r.held_sse as number) + (r.held_lp as number) : (r.held as number);
      per.set(`${r.vantage}`, Math.max(per.get(`${r.vantage}`) ?? 0, n));
    }
    return per;
  };
  const failure = of("fresh").find((r) => !r.ok);
  if (failure && holders.length) {
    const per = heldAt(failure.t as number);
    const sum = [...per.values()].reduce((a, b) => a + b, 0);
    const how = failure.error ?? `status ${failure.status}${failure.body ? ` ${JSON.stringify(failure.body)}` : ""}`;
    lines.push(`FACT wall: a fresh client on ${failure.vantage} first failed (${how}) with ${sum} connections parked ` +
               `(${[...per].map(([v, n]) => `${v} ${n}`).join(", ")})`);
    for (const [v, ws] of Object.entries(Object.groupBy(of("write"), (r) => `${r.vantage}`))) {
      const split = (after: boolean) => ws!.filter((r) => ((r.t as number) >= (failure.t as number)) === after)
        .reduce((a, r) => [a[0]! + (r.ok as number), a[1]! + (r.failed as number)], [0, 0]);
      const [b, a] = [split(false), split(true)];
      lines.push(`FACT wall: writers on ${v}: ${b[0]} ok / ${b[1]} failed before the first fresh failure, ${a[0]} ok / ${a[1]} failed after`);
    }
  } else if (of("fresh").length && holders.length) {
    lines.push(`FACT wall: a fresh client never failed; peak parked ${Math.max(...holders.map((r) => r.kind === "park" ? (r.held_sse as number) + (r.held_lp as number) : (r.held as number)))}`);
  }
  for (const r of of("tls_idle_start")) {
    const n = r.notice as Rec;
    lines.push(`FACT tls-idle: ${r.vantage} ran under the notice of ${n.notified} for ${n.service} (cap ${r.cap}); Compute's reply: ${JSON.stringify(n.reply ?? null)}`);
  }
  for (const [v, rs] of Object.entries(Object.groupBy(of("sse_first"), (r) => `${r.vantage}`))) {
    const got = rs!.filter((r) => typeof r.event_ms === "number").map((r) => r.event_ms as number);
    lines.push(`FACT sse-first: ${v}: ${rs!.length} subscription(s), first event p50 ${q(got, 0.5)} ms p90 ${q(got, 0.9)} ms; ` +
               `${rs!.length - got.length} without an event (${[...new Set(rs!.filter((r) => r.error).map((r) => r.error))].join(", ") || "none"})`);
  }
  // The deadline: what matters is how long the origin held a long-poll's
  // first byte, not the wait asked. The binary caps every long-poll's wait
  // at 25 s in all, a split's refresh included (src/application/
  // read_request.rs wait_tail; MAX_LONG_POLL, src/http.rs; consumer pulls
  // likewise), so it answers a longer wait with 204 at its cap: no
  // long-poll waits more than 25 s in total, and the edge's first-byte
  // deadline is out of its reach, whatever the wait asked.
  const s1 = (ms: number) => Math.round(ms / 100) / 10;
  for (const [v, rs] of Object.entries(Object.groupBy(of("deadline"), (r) => `${r.vantage}`))) {
    const waits = [...new Set(rs!.map((r) => r.wait_s as number))].sort((a, b) => a - b);
    const origin = rs!.filter((r) => r.origin && (r.status === 200 || r.status === 204));
    const held = origin.length ? s1(Math.max(...origin.map((r) => r.elapsed_ms as number))) : null;
    lines.push(`FACT deadline: ${v}: the origin held a long-poll's first byte at most ${held ?? "none"} s (waits asked: ${waits.join(", ")} s)`);
    const capped = origin.filter((r) => r.status === 204 && (r.elapsed_ms as number) < 0.9 * (r.wait_s as number) * 1000);
    if (capped.length) {
      const asked = [...new Set(capped.map((r) => r.wait_s as number))].sort((a, b) => a - b);
      lines.push(`FACT deadline: ${v}: the origin answered waits of ${asked.join(", ")} s with 204 by ` +
                 `${s1(Math.max(...capped.map((r) => r.elapsed_ms as number)))} s: it caps a long-poll's wait, so no ` +
                 "long-poll holds its first byte to the edge's deadline, which is unreachable through one");
    }
    for (const r of rs!.filter((x) => !(x.origin && (x.status === 200 || x.status === 204))).sort((a, b) => (a.wait_s as number) - (b.wait_s as number))) {
      lines.push(`FACT deadline: ${v}: a wait of ${r.wait_s} s was answered by the ${r.origin ? "origin" : "edge"}, ` +
                 `${r.status || r.error}, after ${s1(r.elapsed_ms as number)} s`);
    }
  }
  if (wall !== undefined && holders.length) {
    const peak = Math.max(...[...new Set(holders.map((r) => r.t as number))].map((t) => [...heldAt(t).values()].reduce((a, b) => a + b, 0)));
    const writers = Object.values(Object.groupBy(of("write"), (r) => `${r.vantage}`)).reduce((a, ws) => a + ((ws![0]!.conns as number) ?? 0), 0);
    lines.push(`FACT margin: peak ${peak} parked + ${writers} writer connections = ${peak + writers}; the wall ${wall} leaves ${wall - peak - writers}`);
  }
  return lines;
}

// ---------------------------------------------------------------- posture

function profileValue(name: string): number | undefined {
  let value: string | undefined;
  for (const f of ["compute-1g.env", "shared-cell.env"]) {
    for (const line of readFileSync(join(import.meta.dir, "..", "..", "deploy", "profiles", f), "utf8").split("\n")) {
      const m = /^([A-Z0-9_]+)=(\S*)$/.exec(line.trim());
      if (m && m[1] === name) value = m[2];
    }
  }
  return value === undefined ? undefined : Number(value);
}

function posture(o: Opts): number {
  if (!o.has("wall") || !o.has("long-polls") || !o.has("writer-headroom")) {
    process.stderr.write("edgeprobe posture needs --wall, --long-polls and --writer-headroom\n");
    return 2;
  }
  const from = o.has("sse-max") ? "--sse-max" : "deploy/profiles";
  const sseMax = o.has("sse-max") ? o.int("sse-max", 0) : profileValue("SSE_MAX_CONNECTIONS");
  if (sseMax === undefined || !Number.isInteger(sseMax)) {
    process.stderr.write("edgeprobe posture: no SSE_MAX_CONNECTIONS in the profiles; pass --sse-max\n");
    return 2;
  }
  const [wall, lp, head] = [o.int("wall", 0), o.int("long-polls", 0), o.int("writer-headroom", 0)];
  const sum = sseMax + lp + head;
  const ok = sum < wall;
  process.stdout.write(`${ok ? "PASS" : "FAIL"} posture: SSE_MAX_CONNECTIONS ${sseMax} (${from}) + long-polls ${lp} + ` +
                       `writer headroom ${head} = ${sum} ${ok ? "<" : ">="} the wall ${wall}\n`);
  return ok ? 0 : 1;
}

// ---------------------------------------------------------------- main

const KNOWN: Record<string, string[]> = {
  idle: ["target", "gaps", "path"],
  park: ["target", "headers-file", "stream", "streams", "prefix", "count", "rate", "via", "wait-ms", "hold", "settle-ms"],
  fresh: ["target", "every", "duration", "path"],
  write: ["target", "headers-file", "stream", "streams", "prefix", "rate", "conns", "duration"],
  "tls-idle": ["target", "notice", "measured-wall", "count", "rate", "hold"],
  "sse-first": ["target", "headers-file", "stream", "n", "settle", "timeout"],
  deadline: ["target", "headers-file", "stream", "waits", "repeat"],
  report: ["wall"],
  posture: ["wall", "long-polls", "writer-headroom", "sse-max"],
};

async function main(argv: string[]): Promise<number> {
  const { positional, flags } = parseFlags(argv);
  const mode = positional[0] ?? "";
  if (!(mode in KNOWN)) {
    process.stderr.write(USAGE);
    return mode === "help" ? 0 : 2;
  }
  const unknown = [...flags.keys()].filter((k) => !KNOWN[mode]!.includes(k) && k !== "vantage" && k !== "out");
  if (unknown.length || (mode !== "report" && positional.length > 1)) {
    process.stderr.write(`edgeprobe ${mode}: unknown flag or argument: ${[...unknown.map((k) => `--${k}`), ...positional.slice(1)].join(" ")}\n`);
    return 2;
  }
  const o = new Opts(flags);
  if (mode === "posture") return posture(o);
  if (mode === "report") {
    const recs = positional.slice(1).flatMap((f) => readFileSync(f, "utf8").split("\n").filter(Boolean).map((l) => JSON.parse(l)));
    process.stdout.write(`${report(recs, o.has("wall") ? o.int("wall", 0) : undefined).join("\n")}\n`);
    return 0;
  }
  const target = o.str("target");
  if (!target) {
    process.stderr.write(`edgeprobe ${mode}: --target URL is required\n`);
    return 2;
  }
  const url = new URL(target);
  const out = new Out(o.str("out"), o.str("vantage", hostname()));
  switch (mode) {
    case "idle": await idle(o, out, url); break;
    case "park": await park(o, out, url); break;
    case "fresh": await fresh(o, out, url); break;
    case "write": await write(o, out, url); break;
    case "tls-idle": return tlsIdle(o, out, url);
    case "sse-first": await sseFirst(o, out, url); break;
    case "deadline": await deadline(o, out, url); break;
  }
  return 0;
}

if (import.meta.main) process.exit(await main(process.argv.slice(2)));
