import type { StreamInterpreterConfig } from "../../src/touch/spec";
import { dsError } from "../../src/util/ds_error.ts";

export const DEFAULT_BASE_URL = "http://127.0.0.1:8080";
export const DEFAULT_STREAM = "demo.wal";

export const DEMO_SCHEMA = {
  type: "object",
  additionalProperties: true,
};

export const DEMO_ENTITY = "public.todos";

export const DEMO_INTERPRETER: StreamInterpreterConfig = {
  apiVersion: "durable.streams/stream-interpreter/v1",
  format: "durable.streams/state-protocol/v1",
  touch: {
    enabled: true,
  },
};

export function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms));
}

export function argValue(args: string[], flag: string): string | null {
  const idx = args.indexOf(flag);
  if (idx === -1) return null;
  return args[idx + 1] ?? null;
}

export function hasFlag(args: string[], flag: string): boolean {
  return args.includes(flag);
}

export function parseIntArg(args: string[], flag: string, fallback: number): number {
  const raw = argValue(args, flag);
  if (raw == null) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n < 0) {
    throw dsError(`invalid ${flag}: ${raw}`);
  }
  return n;
}

export function parseStringArg(args: string[], flag: string, fallback: string): string {
  const raw = argValue(args, flag);
  return raw == null ? fallback : raw;
}
