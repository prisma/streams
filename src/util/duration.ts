import { Result } from "better-result";

/** Parse a duration like 15s, 30m, 24h into milliseconds. */
export function parseDurationMsResult(s: string): Result<number, { kind: "invalid_duration"; message: string }> {
  const m = /^([0-9]+)(ms|s|m|h|d)$/.exec(s.trim());
  if (!m) return Result.err({ kind: "invalid_duration", message: `invalid duration: ${s}` });
  const n = Number(m[1]);
  const unit = m[2];
  switch (unit) {
    case "ms": return Result.ok(n);
    case "s": return Result.ok(n * 1000);
    case "m": return Result.ok(n * 60 * 1000);
    case "h": return Result.ok(n * 60 * 60 * 1000);
    case "d": return Result.ok(n * 24 * 60 * 60 * 1000);
    default: return Result.err({ kind: "invalid_duration", message: `invalid unit: ${unit}` });
  }
}
