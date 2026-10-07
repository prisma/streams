// audit: read a stream set back in full and check it record for record
// against the produce runs that wrote it (their k2gen ledgers).
//
//   audit --streams N --prefix P --expect a.ledger.json[,b...] [--parallel 4] [--max-bytes B]
//
// The producer's records are a pure function of its flags and seed
// (corpus.ts), and the stream each request goes to is drawn from its own
// seeded generator (produce.ts Producer.build). The audit replays that
// sequence for the ledger's `sent_requests` requests, keeping each
// record's SHA-256 (first 64 bits) and length, grouped per stream as the
// request blocks the producer sent. It then reads every stream from the
// beginning on the product surface and checks:
//   - every record read back equals a record the producer sent, and the
//     records of one request are adjacent and in request order (a batch
//     is atomic and ordered);
//   - every acked request is read back exactly once (a ledger with failed
//     requests allows up to that many blocks to be missing; a request
//     that failed never shows up as a partial block);
//   - offsets are contiguous: each page's Prisma-Next-Cursor names the
//     offset of the records read so far (key cursor payload: kind,
//     epoch[16], key hash[16], segment u32 LE, offset u64 LE).
// The replay checks itself against the ledger's per-stream acked records
// and bytes before any read. JSON streams only, unkeyed, no --pool.
// Reads are counted like walk's (delivered records and payload bytes,
// read latency per page), so the rig's meter checks apply unchanged.

import { readFileSync } from "node:fs";
import { Rng, Zipf, backoffMs, encName, pool, sleep, streamName, type StreamTally } from "./common.ts";
import { PayloadGen, payloadKind } from "./corpus.ts";
import type { Ctx } from "./produce.ts";

const MAX_PAGE = 8 << 20;
const MAX_FAILURES = 50;

/** One record's identity: the first 64 bits of its SHA-256 and its length. */
function digest(rec: Uint8Array | string): string {
  const h = new Bun.CryptoHasher("sha256");
  h.update(rec);
  const len = typeof rec === "string" ? Buffer.byteLength(rec) : rec.byteLength;
  return `${h.digest("hex").slice(0, 16)}:${len}`;
}

export interface StreamPlan {
  digests: string[]; // every record the producer sent to the stream, in send order
  blocks: Array<{ at: number; n: number; seq: number }>; // request blocks into digests
  first: Map<string, number[]>; // first record digest -> block indices
  bytes: number;
}

interface LedgerSpec {
  file: string;
  seed: number;
  streams: number;
  prefix: string;
  sent: number;
  failed: number;
  records: number;
  recordBytes: number;
  payload: string;
  zipf: number;
  perStream: Record<string, StreamTally>;
}

export function ledgerSpec(file: string): LedgerSpec {
  const j = JSON.parse(readFileSync(file, "utf8")) as Record<string, unknown>;
  const args = (j.args ?? {}) as Record<string, string>;
  const num = (k: string, d: number) => (k in args ? Number(args[k]) : d);
  if (j.mode !== "produce") throw new Error(`${file}: not a produce ledger (mode ${String(j.mode)})`);
  if (num("routing-keys", 0) > 0) throw new Error(`${file}: keyed producers are not audited`);
  if (num("pool", 0) > 0) throw new Error(`${file}: a --pool producer repeats records; not audited`);
  const payload = payloadKind(args.payload ?? "corpus");
  if (payload === "bytes") throw new Error(`${file}: bytes streams are not audited (JSON only)`);
  return {
    file,
    seed: Number(j.seed),
    streams: Number(j.streams),
    prefix: String(j.prefix),
    sent: Number(j.sent_requests ?? 0),
    failed: Number(j.failed_requests ?? 0),
    records: num("records", 1),
    recordBytes: num("record-bytes", 1024),
    payload,
    zipf: num("zipf", 0),
    perStream: (j.per_stream ?? {}) as Record<string, StreamTally>,
  };
}

/** Replays Producer.build for `sent` requests (produce.ts). */
export function replay(s: LedgerSpec, plans: Map<string, StreamPlan>, seqBase: number): { failed: number; replayErrors: string[] } {
  const names = Array.from({ length: s.streams }, (_, i) => streamName(s.prefix, i));
  const zipf = new Zipf(s.streams, s.zipf);
  const rngStream = new Rng(s.seed, 2);
  const gen = new PayloadGen(payloadKind(s.payload), s.recordBytes, s.seed, 3);
  const got = new Map<string, { records: number; bytes: number }>();
  for (let i = 0; i < s.sent; i++) {
    const name = names[zipf.sample(rngStream)]!;
    let plan = plans.get(name);
    if (!plan) plans.set(name, (plan = { digests: [], blocks: [], first: new Map(), bytes: 0 }));
    const at = plan.digests.length;
    let bytes = 0;
    for (let r = 0; r < s.records; r++) {
      const rec = gen.json();
      bytes += rec.length; // ASCII, as produce.ts counts
      plan.digests.push(digest(rec));
    }
    const b = plan.blocks.length;
    plan.blocks.push({ at, n: s.records, seq: seqBase + i });
    const f = plan.digests[at]!;
    const list = plan.first.get(f);
    if (list) list.push(b);
    else plan.first.set(f, [b]);
    plan.bytes += bytes;
    const g = got.get(name) ?? { records: 0, bytes: 0 };
    g.records += s.records;
    g.bytes += bytes;
    got.set(name, g);
  }
  // With no failed request, the replay must equal the acked tallies.
  const replayErrors: string[] = [];
  if (s.failed === 0) {
    for (const name of new Set([...got.keys(), ...Object.keys(s.perStream)])) {
      const g = got.get(name) ?? { records: 0, bytes: 0 };
      const e = s.perStream[name] ?? { acked_records: 0, acked_bytes: 0 };
      if (g.records !== e.acked_records || g.bytes !== e.acked_bytes) {
        replayErrors.push(`${s.file}: ${name} replayed ${g.records}/${g.bytes} B, acked ${e.acked_records}/${e.acked_bytes} B`);
      }
    }
  }
  return { failed: s.failed, replayErrors };
}

/** The raw text of each element of a JSON array body, split at its
 * top-level commas (strings and nesting tracked byte by byte). */
export function splitArray(body: Uint8Array): Uint8Array[] {
  const out: Uint8Array[] = [];
  let i = 0;
  while (i < body.length && body[i] !== 0x5b) i++; // [
  let depth = 0, inStr = false, esc = false, start = i + 1;
  for (i = i + 1; i < body.length; i++) {
    const c = body[i]!;
    if (inStr) {
      if (esc) esc = false;
      else if (c === 0x5c) esc = true;
      else if (c === 0x22) inStr = false;
      continue;
    }
    if (c === 0x22) inStr = true;
    else if (c === 0x7b || c === 0x5b) depth++;
    else if (c === 0x7d || c === 0x5d) {
      if (depth === 0) {
        if (i > start) out.push(body.subarray(start, i));
        break;
      }
      depth--;
    } else if (c === 0x2c && depth === 0) {
      out.push(body.subarray(start, i));
      start = i + 1;
    }
  }
  return out;
}

/** The offset a key cursor names (null when the token is not one). */
export function cursorOffset(token: string): { kind: number; segment: number; offset: bigint } | null {
  const b = Buffer.from(token, "base64url");
  if (b.length < 45 || (b[0] !== 0x12 && b[0] !== 0x13)) return null;
  return { kind: b[0]!, segment: b.readUInt32LE(33), offset: b.readBigUInt64LE(37) };
}

interface StreamCheck {
  name: string;
  read_records: number;
  read_bytes: number;
  pages: number;
  blocks_found: number;
  blocks_missing: number;
  unexpected_records: number;
  mismatched_records: number;
  incomplete_blocks: number;
  offset_errors: number;
  inversions: number;
  segments: number[];
  examples: string[];
}

class Matcher {
  used: Uint8Array;
  cur = -1; // block being matched
  pos = 0;
  lastSeq = -1;
  constructor(readonly plan: StreamPlan, readonly c: StreamCheck) {
    this.used = new Uint8Array(plan.blocks.length);
  }
  private bad(kind: "unexpected_records" | "mismatched_records", msg: string): void {
    this.c[kind]++;
    if (this.c.examples.length < 5) this.c.examples.push(msg);
  }
  feed(d: string, index: number): void {
    const p = this.plan;
    if (this.cur >= 0) {
      const blk = p.blocks[this.cur]!;
      if (p.digests[blk.at + this.pos] === d) {
        if (++this.pos === blk.n) this.cur = -1;
        return;
      }
      this.c.incomplete_blocks++;
      this.bad("mismatched_records", `record ${index}: expected record ${this.pos} of request ${blk.seq}, got ${d}`);
      this.cur = -1;
    }
    const cands = p.first.get(d) ?? [];
    const b = cands.find((x) => !this.used[x]);
    if (b === undefined) {
      this.bad("unexpected_records", `record ${index}: ${d} starts no unread request`);
      return;
    }
    this.used[b] = 1;
    this.c.blocks_found++;
    const blk = p.blocks[b]!;
    if (blk.seq < this.lastSeq) this.c.inversions++;
    this.lastSeq = blk.seq;
    if (blk.n > 1) {
      this.cur = b;
      this.pos = 1;
    }
  }
  finish(): void {
    if (this.cur >= 0) this.c.incomplete_blocks++;
    this.c.blocks_missing = this.used.length - this.used.reduce((a, x) => a + x, 0);
  }
}

async function auditStream(ctx: Ctx, name: string, plan: StreamPlan, maxBytes: number | undefined): Promise<StreamCheck> {
  const { http, meter } = ctx;
  const c: StreamCheck = {
    name, read_records: 0, read_bytes: 0, pages: 0, blocks_found: 0, blocks_missing: 0, unexpected_records: 0,
    mismatched_records: 0, incomplete_blocks: 0, offset_errors: 0, inversions: 0, segments: [], examples: [],
  };
  const m = new Matcher(plan, c);
  let cursor = "beginning";
  let failures = 0;
  for (;;) {
    const q = new URLSearchParams({ cursor });
    if (maxBytes) q.set("maxBytes", String(Math.min(MAX_PAGE, Math.max(4096, maxBytes))));
    const r = await http.call("GET", `/v1/streams/${encName(name)}/records?${q}`, { timeoutMs: 120_000 });
    if (r.status !== 200 && r.status !== 204) {
      if (++failures > MAX_FAILURES) throw new Error(`audit ${name}: ${failures} failed reads, last ${r.status}`);
      await sleep(backoffMs(r));
      continue;
    }
    meter.readMs(r.ms);
    c.pages++;
    const recs = r.status === 200 ? splitArray(r.body) : [];
    let bytes = 0;
    for (const rec of recs) {
      bytes += rec.byteLength;
      m.feed(digest(rec), c.read_records++);
    }
    c.read_bytes += bytes;
    if (recs.length) {
      meter.add("delivered_records", recs.length);
      meter.add("delivered_payload_bytes", bytes);
      const t = meter.tally(name);
      t.delivered_records += recs.length;
      t.delivered_bytes += bytes;
    } else meter.add("empty_polls");
    const next = r.headers?.get("prisma-next-cursor") ?? null;
    const pos = next ? cursorOffset(next) : null;
    if (!pos || pos.offset !== BigInt(c.read_records)) {
      c.offset_errors++;
      if (c.examples.length < 5) c.examples.push(`page ${c.pages}: cursor offset ${pos?.offset ?? "none"} after ${c.read_records} records`);
    }
    if (pos && !c.segments.includes(pos.segment)) c.segments.push(pos.segment);
    if (r.headers?.get("prisma-up-to-date") === "true" || r.status === 204) break;
    if (!next || (recs.length === 0 && next === cursor)) break;
    cursor = next;
  }
  m.finish();
  return c;
}

/** audit mode. */
export async function audit(ctx: Ctx, signal: AbortSignal): Promise<Record<string, unknown>> {
  const files = (ctx.o.str("expect") ?? "").split(",").filter(Boolean);
  if (!files.length) throw new Error("audit needs --expect a.ledger.json[,b...]");
  const plans = new Map<string, StreamPlan>();
  let failed = 0, sent = 0;
  const replayErrors: string[] = [];
  const t0 = performance.now();
  for (const f of files) {
    const s = ledgerSpec(f);
    const r = replay(s, plans, sent);
    sent += s.sent;
    failed += r.failed;
    replayErrors.push(...r.replayErrors);
  }
  const replayS = (performance.now() - t0) / 1000;
  if (replayErrors.length) {
    return { verify: { ok: false, stage: "replay", replay_errors: replayErrors.slice(0, 10) } };
  }
  const names = Array.from({ length: ctx.streams }, (_, i) => streamName(ctx.prefix, i));
  for (const n of plans.keys()) if (!names.includes(n)) throw new Error(`audit: ${n} was produced but is not in --streams/--prefix`);
  const maxBytes = ctx.o.int("max-bytes", 0) || undefined;
  const checks: StreamCheck[] = new Array(names.length);
  const empty: StreamPlan = { digests: [], blocks: [], first: new Map(), bytes: 0 };
  await pool(names.length, ctx.o.int("parallel", 4), async (i) => {
    checks[i] = await auditStream(ctx, names[i]!, plans.get(names[i]!) ?? empty, maxBytes);
  }, signal);
  const sum = (k: keyof StreamCheck) => checks.reduce((a, c) => a + (c[k] as number), 0);
  const expected = [...plans.values()].reduce((a, p) => a + p.digests.length, 0);
  const blocks = [...plans.values()].reduce((a, p) => a + p.blocks.length, 0);
  const missing = sum("blocks_missing");
  const ok = !signal.aborted && checks.every(Boolean) && sum("unexpected_records") === 0 && sum("mismatched_records") === 0 &&
    sum("incomplete_blocks") === 0 && sum("offset_errors") === 0 && missing <= failed &&
    sum("read_records") + missingRecords(plans, checks) === expected;
  return {
    verify: {
      ok,
      ledgers: files,
      sent_requests: sent,
      failed_requests: failed,
      replay_s: Math.round(replayS * 10) / 10,
      streams: names.length,
      expected_records: expected,
      read_records: sum("read_records"),
      read_bytes: sum("read_bytes"),
      pages: sum("pages"),
      blocks_expected: blocks,
      blocks_found: sum("blocks_found"),
      blocks_missing: missing,
      unexpected_records: sum("unexpected_records"),
      mismatched_records: sum("mismatched_records"),
      incomplete_blocks: sum("incomplete_blocks"),
      offset_errors: sum("offset_errors"),
      order_inversions: sum("inversions"),
      per_stream: checks.filter(Boolean).map((c) => ({ ...c, examples: c.examples.slice(0, 3) })),
    },
  };
}

/** Records in the blocks a stream never returned (failed requests). */
function missingRecords(plans: Map<string, StreamPlan>, checks: StreamCheck[]): number {
  let n = 0;
  for (const c of checks) {
    if (!c || c.blocks_missing === 0) continue;
    const p = plans.get(c.name);
    if (p) n += c.blocks_missing * (p.blocks[0]?.n ?? 0);
  }
  return n;
}
