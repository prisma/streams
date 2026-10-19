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
import { type Http, type StreamTally, backoffMs, encName, errCode, productHeader, sleep } from "./common.ts";

const MAX_FAILURES = 50;

export interface Resolution {
  /** Streams that were sent outcome-unknown records. */
  streams: number;
  unknown_records: number;
  /** Records the read-back found written past the acked ones. */
  committed_records: number;
  unresolved: Array<{ stream: string; reason: string }>;
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

/** The records a JSON stream holds, read from the beginning, or why they
 * could not be counted. */
export async function storedRecords(http: Http, stream: string): Promise<number | string> {
  let cursor = "beginning";
  let n = 0;
  let failures = 0;
  for (;;) {
    const r = await http.call("GET", `/v1/streams/${encName(stream)}/records?${new URLSearchParams({ cursor })}`, { timeoutMs: 120_000 });
    if (r.status === 404) return `404 ${errCode(r) || "-"}: the stream is gone (deleted or expired)`;
    if (r.status !== 200 && r.status !== 204) {
      if (++failures > MAX_FAILURES) return `${failures} failed reads, last ${r.status} ${errCode(r) || "-"}`;
      await sleep(backoffMs(r));
      continue;
    }
    const recs = r.status === 200 ? splitArray(r.body).length : 0;
    n += recs;
    const next = productHeader(r.headers, "next-cursor");
    if (productHeader(r.headers, "up-to-date") === "true" || r.status === 204 || !next || (recs === 0 && next === cursor)) return n;
    cursor = next;
  }
}

/** Resolves a project's outcome-unknown appends; null when it sent none.
 * `read` holds the record counts of streams the audit already read whole. */
export async function resolve(http: Http, ledgers: Array<Record<string, unknown>>, read: Map<string, number>): Promise<Resolution | null> {
  const due = [...owed(ledgers)].filter(([, o]) => o.unknown > 0).sort(([a], [b]) => (a < b ? -1 : 1));
  if (!due.length) return null;
  const res: Resolution = { streams: due.length, unknown_records: 0, committed_records: 0, unresolved: [] };
  for (const [stream, o] of due) {
    res.unknown_records += o.unknown;
    if (o.bytes) {
      res.unresolved.push({ stream, reason: "a bytes stream's records cannot be counted back exactly" });
      continue;
    }
    const got = read.get(stream) ?? (await storedRecords(http, stream));
    if (typeof got === "string") {
      res.unresolved.push({ stream, reason: got });
      continue;
    }
    const committed = got - o.acked;
    if (committed < 0 || committed > o.unknown) {
      res.unresolved.push({ stream, reason: `holds ${got} records, which ${o.acked} acked and ${o.unknown} outcome-unknown cannot account for` });
      continue;
    }
    res.committed_records += committed;
  }
  return res;
}
