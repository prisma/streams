import { Result } from "better-result";

export function parseTimestampMsResult(input: string): Result<bigint, { kind: "empty_timestamp" | "invalid_timestamp"; message: string }> {
  const s = input.trim();
  if (s === "") return Result.err({ kind: "empty_timestamp", message: "empty timestamp" });
  // unix nanos
  if (/^[0-9]+$/.test(s)) {
    return Result.ok(BigInt(s) / 1_000_000n);
  }
  const d = new Date(s);
  const ms = d.getTime();
  if (Number.isNaN(ms)) return Result.err({ kind: "invalid_timestamp", message: `invalid timestamp: ${input}` });
  return Result.ok(BigInt(ms));
}
