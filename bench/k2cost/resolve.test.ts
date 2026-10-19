// The outcome-unknown rule the read-back resolves (common.ts
// appendOutcomeUnknown, the SDK's AppendOutcomeUnknownError) and the
// per-stream sums it resolves against (resolve.ts owed).
import { expect, test } from "bun:test";
import { type Resp, appendOutcomeUnknown } from "./common.ts";
import { owed } from "./resolve.ts";

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
