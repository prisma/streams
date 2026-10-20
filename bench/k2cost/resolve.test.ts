// The outcome-unknown rule the read-back resolves (common.ts
// appendOutcomeUnknown, the SDK's AppendOutcomeUnknownError) and the
// per-stream sums it resolves against (resolve.ts owed).
import { afterEach, expect, test } from "bun:test";
import { HeaderSource, Http, Meter, type Resp, appendOutcomeUnknown } from "./common.ts";
import { type ReadBack, owed, resolve } from "./resolve.ts";

const resp = (status: number, body: unknown = null, error?: string): Resp => ({
  status, headers: null, ms: 0, error, body: new TextEncoder().encode(body === null ? "" : JSON.stringify(body)),
});

test("an append's outcome is unknown after a 408, a sent transport failure, a 5xx, or a 503 not stated retryable", () => {
  const cases: Array<[string, Resp, boolean]> = [
    ["408 append_failed", resp(408, { error: { code: "append_failed" } }), true],
    ["timeout", resp(0, null, "timeout: aborted"), true],
    ["transport error", resp(0, null, "err: socket closed"), true],
    ["cancelled after sending", resp(0, null, "cancelled: aborted"), true],
    ["never sent", resp(0, null, "unsent: cancelled while waiting for a pooled connection"), false],
    ["500", resp(500, { error: { code: "internal" } }), true],
    ["502 without a body", resp(502), true],
    ["503 outcome_unknown", resp(503, { error: { code: "outcome_unknown", retryable: false } }), true],
    ["503 outcome_unknown said retryable", resp(503, { error: { code: "outcome_unknown", retryable: true } }), true],
    ["503 with no retryable", resp(503, { error: { code: "temporarily_unavailable" } }), true],
    ["503 temporarily_unavailable retryable", resp(503, { error: { code: "temporarily_unavailable", retryable: true } }), false],
    ["429", resp(429, { error: { code: "rate_limited", retryable: true } }), false],
    ["404", resp(404, { error: { code: "stream_not_found" } }), false],
    ["200", resp(200, { count: 1 }), false],
  ];
  expect(cases.map(([n, r]) => [n, appendOutcomeUnknown(r)])).toEqual(cases.map(([n, , w]) => [n, w]));
});

test("the read-back owes each stream its acked and outcome-unknown records over every phase, a keyed target counted as its stream", () => {
  const t = (acked: number, unknown?: number) => ({ acked_records: acked, acked_bytes: 0, delivered_records: 0, delivered_bytes: 0, unknown_records: unknown });
  const o = owed([
    { args: { payload: "oracle" }, per_stream: { "t/00000": t(10, 4), "t/00001": t(5), "k/00000#k1": t(2, 1), "k/00000#k2": t(3, 2) } },
    { args: { payload: "oracle" }, per_stream: { "t/00000": t(1) } },
    { args: { payload: "bytes" }, per_stream: { "b/00000": t(1, 1), "t/00001": t(0) } },
  ]);
  expect(Object.fromEntries(o)).toEqual({
    "t/00000": { acked: 11, unknown: 4, bytes: false },
    "t/00001": { acked: 5, unknown: 0, bytes: false },
    "k/00000": { acked: 5, unknown: 3, bytes: false },
    "b/00000": { acked: 1, unknown: 1, bytes: true },
  });
});

// A cell that answers GET /records from a scripted list of refusals first
// (each [status, code]; retry-after 0), then pages one long stream the way
// the shared cell does: 1 MiB pages unless maxBytes says otherwise.
const servers: Array<ReturnType<typeof Bun.serve>> = [];
afterEach(() => {
  for (const s of servers.splice(0)) s.stop(true);
});
function cell(records: number, refusals: Array<[number, string]>, midway: Array<[number, string]> = []) {
  const recs = Array.from({ length: records }, (_, i) => JSON.stringify({ i, pad: "x".repeat(1000) }));
  const seen = { reads: 0, pages: 0, cursors: [] as string[] };
  const server = Bun.serve({
    port: 0, hostname: "127.0.0.1",
    fetch(req) {
      const url = new URL(req.url);
      seen.reads++;
      const bad = refusals.shift() ?? (url.searchParams.get("cursor") !== "beginning" ? midway.shift() : undefined);
      if (bad) {
        return new Response(JSON.stringify({ error: { code: bad[1], retryable: true } }), { status: bad[0], headers: { "retry-after": "0" } });
      }
      const cursor = url.searchParams.get("cursor") ?? "beginning";
      seen.cursors.push(cursor);
      const from = cursor === "beginning" ? 0 : Number(cursor);
      const max = Number(url.searchParams.get("maxBytes") ?? 1 << 20);
      let bytes = 0;
      const out: string[] = [];
      for (let i = from; i < recs.length && (!out.length || bytes + recs[i]!.length <= max); i++) {
        out.push(recs[i]!);
        bytes += recs[i]!.length;
      }
      seen.pages++;
      const next = from + out.length;
      return new Response(`[${out.join(",")}]`, { headers: { "content-type": "application/json", "streams-next-cursor": String(next), "streams-up-to-date": String(next === recs.length) } });
    },
  });
  servers.push(server);
  return { http: new Http(`http://127.0.0.1:${server.port}`, new HeaderSource(undefined, { authorization: "Bearer t" }), new Meter()), seen };
}
const ledger = (acked: number, unknown: number) => [{ args: {}, per_stream: { "s/0": { acked_records: acked, acked_bytes: 0, delivered_records: 0, delivered_bytes: 0, unknown_records: unknown } } }];
const FAST = (budgetMs: number): ReadBack => ({ budgetMs, delayMs: () => 1 });

test("the read-back outlasts a cell that refuses it for load, and pages a stream of thousands of records through the cell's default 1 MiB pages", async () => {
  // 60 refusals up front (past the old 50-failure cap), one more between pages.
  const refusals: Array<[number, string]> = Array.from({ length: 60 }, (_, i) => (i % 2 ? [503, "read_memory_busy"] : [429, "project_rate_limit"]));
  const { http, seen } = cell(3000, refusals, [[503, "read_memory_busy"]]);
  const r = await resolve(http, ledger(2900, 100), new Map(), FAST(30_000));
  expect(r).toEqual({
    streams: 1, unknown_records: 100, committed_records: 100, unresolved: [], reasons: {},
    retried: { "429 project_rate_limit": 30, "503 read_memory_busy": 31 },
  });
  // ~3 MB of records is 3 default pages (a cursor each); 61 refusals were asked again.
  expect([seen.reads, seen.pages, seen.cursors.length]).toEqual([64, 3, 3]);
  expect(seen.cursors[0]).toBe("beginning");
}, 30_000);

test("a stream refused past the budget is unresolved, with its reason class counted; one that is truly gone or forbidden is not retried", async () => {
  const always: Array<[number, string]> = Array.from({ length: 10_000 }, () => [429, "project_rate_limit"]);
  const a = cell(10, always);
  const ra = await resolve(a.http, ledger(5, 5), new Map(), FAST(150));
  expect(ra!.unresolved).toHaveLength(1);
  expect(ra!.unresolved[0]!.reason).toMatch(/^\d+ failed reads over 0\.\ds, last 429 project_rate_limit$/);
  expect([ra!.committed_records, ra!.reasons]).toEqual([0, { "429 project_rate_limit": 1 }]);
  expect(a.seen.reads).toBeGreaterThan(5);
  const f = cell(10, [[403, "forbidden"], [403, "forbidden"]]);
  const rf = await resolve(f.http, ledger(5, 5), new Map(), FAST(60_000));
  expect([f.seen.reads, rf!.unresolved, rf!.reasons]).toEqual([1, [{ stream: "s/0", reason: "403 forbidden: refused, not retryable" }], { "403 forbidden": 1 }]);
  const g = cell(10, [[404, "stream_not_found"]]);
  const rg = await resolve(g.http, ledger(5, 5), new Map(), FAST(60_000));
  expect([g.seen.reads, rg!.reasons]).toEqual([1, { "404 stream_not_found": 1 }]);
}, 30_000);
