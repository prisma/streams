// resolve: the read-back that settles appends answered with an unknown
// outcome (common.ts appendOutcomeUnknown: a 408, a transport failure, a
// 5xx). The server may have written such an append and metered it; the
// generators' ledgers count only acked records. tenants.ts runs this after
// usage, inside its audit: for every stream a project's phase ledgers sent
// outcome-unknown records to, it counts the records the stream holds (the
// audit's own read when it read the stream, else a read from the
// beginning), and the records past the acked ones are the ones written.
// A stream resolves only when that count is between 0 and the unknown
// records sent there; a stream that is gone (deleted, expired), a bytes
// stream (its record count is an estimate) or a count out of bounds is
// reported unresolved, never forgiven (shared_verdict.py leg_billing).

import { splitArray } from "./audit.ts";
import { type Http, READ_BACK, type ReadBack, type StreamTally, encName, failureClass, productHeader, readRefused, sleep } from "./common.ts";

export { READ_BACK, type ReadBack };

export interface Resolution {
  /** Streams that were sent outcome-unknown records. */
  streams: number;
  unknown_records: number;
  /** Records the read-back found written past the acked ones. */
  committed_records: number;
  unresolved: Array<{ stream: string; reason: string }>;
  /** The unresolved streams counted per reason class ("429
   * project_rate_limit", "404 stream_not_found", "bytes stream", ...). */
  reasons: Record<string, number>;
  /** The reads the cell refused or failed and the read-back asked again,
   * per class: what the resolutions that succeeded had to outlast. */
  retried: Record<string, number>;
}

/** A stream the read-back could not count, and the class of why. */
export interface Unreadable {
  reason: string;
  cls: string;
}

interface Owed {
  acked: number;
  unknown: number;
  bytes: boolean;
}

/** Per stream (a `name#key` target counts as its stream), the records the
 * phase ledgers acked and sent with an unknown outcome, and whether a
 * bytes writer wrote it. */
export function owed(ledgers: Array<Record<string, unknown>>): Map<string, Owed> {
  const m = new Map<string, Owed>();
  for (const lg of ledgers) {
    const bytes = ((lg.args ?? {}) as Record<string, unknown>).payload === "bytes";
    for (const [k, t] of Object.entries((lg.per_stream ?? {}) as Record<string, StreamTally>)) {
      const n = k.split("#", 1)[0]!;
      const o = m.get(n) ?? { acked: 0, unknown: 0, bytes: false };
      o.acked += t.acked_records;
      o.unknown += t.unknown_records ?? 0;
      o.bytes ||= bytes && t.acked_records + (t.unknown_records ?? 0) > 0;
      m.set(n, o);
    }
  }
  return m;
}

/** The records a JSON stream holds, read from the beginning in the cell's
 * pages (the cursor each page returns), or why they could not be counted.
 * A refused or failed read (408, 429, 5xx, a transport failure, a page
 * that moved nothing) is asked again until `rb.budgetMs` has passed with
 * no good page; a 404 and the other 4xx are the cell's answer and end the
 * read at once. `retried` counts the asks again per class. */
export async function storedRecords(http: Http, stream: string, rb: ReadBack = READ_BACK, retried: Map<string, number> = new Map()): Promise<number | Unreadable> {
  let cursor = "beginning";
  let n = 0;
  let failed = 0;
  let since = 0;
  for (;;) {
    const r = await http.call("GET", `/v1/streams/${encName(stream)}/records?${new URLSearchParams({ cursor })}`, { timeoutMs: 120_000 });
    if (r.status === 404) {
      const cls = failureClass(r);
      return { reason: `${cls}: the stream is gone (deleted or expired)`, cls };
    }
    const served = r.status === 200 || r.status === 204;
    const recs = r.status === 200 ? splitArray(r.body).length : 0;
    const next = productHeader(r.headers, "next-cursor");
    const upToDate = productHeader(r.headers, "up-to-date") === "true";
    const stalled = r.status === 200 && recs === 0 && !upToDate && next === cursor;
    if (served && !stalled) {
      failed = 0;
      n += recs;
      if (upToDate || r.status === 204 || !next) return n;
      cursor = next;
      continue;
    }
    const cls = stalled ? "stalled page" : failureClass(r);
    if (!stalled && readRefused(r)) return { reason: `${cls}: refused, not retryable`, cls };
    if (++failed === 1) since = performance.now();
    const spent = performance.now() - since;
    if (spent >= rb.budgetMs) return { reason: `${failed} failed reads over ${(spent / 1000).toFixed(1)}s, last ${cls}`, cls };
    retried.set(cls, (retried.get(cls) ?? 0) + 1);
    await sleep(rb.delayMs(r, failed));
  }
}

/** Resolves a project's outcome-unknown appends; null when it sent none.
 * `read` holds the record counts of streams the audit already read whole. */
export async function resolve(http: Http, ledgers: Array<Record<string, unknown>>, read: Map<string, number>, rb: ReadBack = READ_BACK): Promise<Resolution | null> {
  const due = [...owed(ledgers)].filter(([, o]) => o.unknown > 0).sort(([a], [b]) => (a < b ? -1 : 1));
  if (!due.length) return null;
  const res: Resolution = { streams: due.length, unknown_records: 0, committed_records: 0, unresolved: [], reasons: {}, retried: {} };
  const retried = new Map<string, number>();
  const fail = (stream: string, reason: string, cls: string) => {
    res.unresolved.push({ stream, reason });
    res.reasons[cls] = (res.reasons[cls] ?? 0) + 1;
  };
  for (const [stream, o] of due) {
    res.unknown_records += o.unknown;
    if (o.bytes) {
      fail(stream, "a bytes stream's records cannot be counted back exactly", "bytes stream");
      continue;
    }
    const got = read.get(stream) ?? (await storedRecords(http, stream, rb, retried));
    if (typeof got === "object") {
      fail(stream, got.reason, got.cls);
      continue;
    }
    const committed = got - o.acked;
    if (committed < 0 || committed > o.unknown) {
      fail(stream, `holds ${got} records, which ${o.acked} acked and ${o.unknown} outcome-unknown cannot account for`, "count out of bounds");
      continue;
    }
    res.committed_records += committed;
  }
  res.retried = Object.fromEntries([...retried].sort(([a], [b]) => (a < b ? -1 : 1)));
  return res;
}
