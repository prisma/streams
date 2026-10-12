// The leak oracle of the shared-cell harness (shared-cells PLAN §4.1,
// validation.md §4.2 "The oracle and the probes").
//
// A tenant's `--payload oracle` record names its project and carries a MAC
// under that project's harness key, which the server never sees:
//
//   {"o":"<project>","id":<seq>,"ts":"<iso>","m":"<mac>","data":"<base64>"}
//
// m = HMAC-SHA256(K_p, o "\n" id "\n" ts "\n" data), first 16 bytes in hex,
// with K_p = HMAC-SHA256(SHA-256("k2-oracle-v1:" secret), project). Every
// reader that knows its owner (consume.ts, churn.ts) checks each record it
// is delivered, on every read path (pages, long-polls, SSE data events,
// consumer pulls), and counts the verdict on its meter:
//   oracle_ok        the reader's own record, intact;
//   oracle_foreign   a record naming another project: a leak;
//   oracle_bad_mac   its own project's name over bytes that do not verify:
//                    corruption (or a forged owner field);
//   oracle_unmarked  a record without the oracle's fields: data no tenant of
//                    the harness wrote, so foreign as well.
// The fields are checked on the parsed record, so a server that re-serializes
// JSON does not fail it. Records are a pure function of (seed, tag, call
// order) and the owner, so audit.ts replays them byte for byte.

import { createHash, createHmac } from "node:crypto";
import type { Meter } from "./common.ts";

export interface Owner {
  project: string;
  key: Buffer;
}

export type OracleVerdict = "ok" | "foreign" | "bad_mac" | "unmarked";

/** The project's harness key, derived from the run's secret. */
export function ownerOf(secret: string, project: string): Owner {
  const root = createHash("sha256").update(`k2-oracle-v1:${secret}`).digest();
  return { project, key: createHmac("sha256", root).update(project).digest() };
}

export function oracleMac(owner: Owner, id: number, ts: string, data: string): string {
  return createHmac("sha256", owner.key).update(`${owner.project}\n${id}\n${ts}\n${data}`).digest("hex").slice(0, 32);
}

/** The JSON text of one oracle record whose data field is `raw` in base64. */
export function oracleRecord(owner: Owner, id: number, ts: string, raw: Uint8Array): string {
  const data = Buffer.from(raw).toString("base64");
  return `{"o":"${owner.project}","id":${id},"ts":"${ts}","m":"${oracleMac(owner, id, ts, data)}","data":"${data}"}`;
}

/** Bytes of an oracle record around its base64 data. */
export function oracleOverhead(owner: Owner, id: number, ts: string): number {
  return `{"o":"${owner.project}","id":${id},"ts":"${ts}","m":"","data":""}`.length + 32;
}

export function verifyOracle(owner: Owner, value: unknown): OracleVerdict {
  if (!value || typeof value !== "object" || Array.isArray(value)) return "unmarked";
  const r = value as Record<string, unknown>;
  if (typeof r.o !== "string" || typeof r.m !== "string" || typeof r.id !== "number" ||
    typeof r.ts !== "string" || typeof r.data !== "string") return "unmarked";
  if (r.o !== owner.project) return "foreign";
  return r.m === oracleMac(owner, r.id, r.ts, r.data) ? "ok" : "bad_mac";
}

/** Count each delivered record's verdict on the reader's meter. A reader
 * passes [null] for a page or pull whose body does not parse: one
 * unmarked (corrupt) delivery. */
export function countOracle(meter: Meter, owner: Owner, values: readonly unknown[]): void {
  for (const v of values) meter.add(`oracle_${verifyOracle(owner, v)}`);
}

/** An SSE data event's payload (`[<record>]` on a JSON stream). */
export function countOracleEvent(meter: Meter, owner: Owner, data: string): void {
  let parsed: unknown;
  try {
    parsed = JSON.parse(data);
  } catch {
    meter.add("oracle_unmarked");
    return;
  }
  countOracle(meter, owner, Array.isArray(parsed) ? parsed : [parsed]);
}
