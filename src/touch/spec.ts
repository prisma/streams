import { Result } from "better-result";
import { parseDurationMsResult } from "../util/duration.ts";
import { dsError } from "../util/ds_error.ts";

export type StreamInterpreterConfig = {
  apiVersion: "durable.streams/stream-interpreter/v1";
  format?: "durable.streams/state-protocol/v1";
  touch?: TouchConfig;
};

export type StreamInterpreterConfigValidationError = {
  kind: "invalid_interpreter";
  message: string;
};

export type TouchConfig = {
  enabled: boolean;
  /**
   * Touch storage backend.
   *
   * - memory: in-process, lossy (false positives allowed), epoch-scoped journal.
   * - sqlite: derived companion stream in SQLite WAL (legacy).
   *
   * Default: memory.
   */
  storage?: "memory" | "sqlite";
  /**
   * Advanced override. When unset, the server exposes touches as a companion of
   * the source stream at `/v1/stream/<name>/touch` and uses an internal derived
   * stream name.
   */
  derivedStream?: string;
  retention?: { maxAgeMs: number };
  /**
   * Coarse invalidation interval. The server emits at most one table-touch per
   * entity per interval.
   *
   * Default: 100ms.
   */
  coarseIntervalMs?: number;
  /**
   * Fine-touch coalescing window for watch keys.
   *
   * Default: 100ms.
   */
  touchCoalesceWindowMs?: number;
  /**
   * Policy when an update event is missing `oldValue` (before image).
   *
   * - coarse: emit coarse table touches only (safe default)
   * - skipBefore: compute fine touches from `value` only
   * - error: interpreter errors (useful for strict debugging)
   */
  onMissingBefore?: "coarse" | "skipBefore" | "error";
  /**
   * Optional guardrail: when the interpreter backlog (source offsets behind the tail)
   * exceeds this threshold, the interpreter will emit coarse table touches only
   * (fine/template touches are suppressed) to preserve timeliness under overload.
   *
   * Default: 5000.
   */
  lagDegradeFineTouchesAtSourceOffsets?: number;
  /**
   * Hysteresis recovery threshold for lag-based degradation.
   *
   * When fine touches are currently suppressed due to lag, they are re-enabled
   * only after lag falls to this threshold (or lower).
   *
   * Default: 1000.
   */
  lagRecoverFineTouchesAtSourceOffsets?: number;
  /**
   * Optional guardrail: cap fine/template touches emitted per interpreter batch.
   * Table touches are always emitted for correctness.
   *
   * Default: 2000.
   */
  fineTouchBudgetPerBatch?: number;
  /**
   * Fine-touch token bucket refill rate (tokens/sec).
   *
   * Default: 200000.
   */
  fineTokensPerSecond?: number;
  /**
   * Fine-touch token bucket burst capacity (tokens).
   *
   * Default: 400000.
   */
  fineBurstTokens?: number;
  /**
   * When lag guardrails are active, reserve a small fine-touch budget per batch
   * for currently hot keys/templates (premium lane). Set 0 to disable.
   *
   * Default: 200.
   */
  lagReservedFineTouchBudgetPerBatch?: number;
  /**
   * Memory-only touch journal parameters. Only used when storage="memory".
   */
  memory?: {
    /**
     * Bucket duration. Cursor generations advance only on bucket flush.
     *
     * Default: 100ms.
     */
    bucketMs?: number;
    /**
     * Bloom filter size as a power of two (positions). Memory use per stream is
     * `4 * 2^filterPow2` bytes.
     *
     * Default: 22 (16MiB).
     */
    filterPow2?: number;
    /**
     * Hash positions per key.
     *
     * Default: 4.
     */
    k?: number;
    /**
     * Hard cap on unique keys tracked per bucket. If exceeded, the bucket is
     * treated as a broadcast invalidation (wake all waiters) to avoid false negatives.
     *
     * Default: 100000.
     */
    pendingMaxKeys?: number;
    /**
     * Maximum keys per /touch/wait to index per-key. Larger keysets are treated
     * as "broad" and are scanned on each bucket flush.
     *
     * Default: 32.
     */
    keyIndexMaxKeys?: number;
    /**
     * Sliding TTL for "hot" fine keys observed from /touch/wait.
     *
     * Default: 10000ms.
     */
    hotKeyTtlMs?: number;
    /**
     * Sliding TTL for "hot" templates observed from templateIdsUsed.
     *
     * Default: 10000ms.
     */
    hotTemplateTtlMs?: number;
    /**
     * Upper bound for hot fine key tracking per stream.
     *
     * Default: 1000000.
     */
    hotMaxKeys?: number;
    /**
     * Upper bound for hot template tracking per stream.
     *
     * Default: 4096.
     */
    hotMaxTemplates?: number;
  };
  templates?: {
    /**
     * Sliding inactivity TTL for templates, measured since last use.
     * Individual activations may override this TTL.
     *
     * Default: 1 hour.
     */
    defaultInactivityTtlMs?: number;
    /**
     * Persist last-seen timestamps at most once per interval per template.
     *
     * Default: 5 minutes.
     */
    lastSeenPersistIntervalMs?: number;
    /**
     * Template GC interval.
     *
     * Default: 1 minute.
     */
    gcIntervalMs?: number;
    maxActiveTemplatesPerEntity?: number;
    maxActiveTemplatesPerStream?: number;
    activationRateLimitPerMinute?: number;
  };
};

function invalidInterpreter<T = never>(message: string): Result<T, StreamInterpreterConfigValidationError> {
  return Result.err({ kind: "invalid_interpreter", message });
}

function parseNumberField(
  value: any,
  defaultValue: number,
  message: string,
  predicate: (n: number) => boolean
): Result<number, StreamInterpreterConfigValidationError> {
  const n = value === undefined ? defaultValue : Number(value);
  if (!Number.isFinite(n) || !predicate(n)) return invalidInterpreter(message);
  return Result.ok(n);
}

function parseIntegerField(
  value: any,
  defaultValue: number,
  message: string,
  predicate: (n: number) => boolean
): Result<number, StreamInterpreterConfigValidationError> {
  const n = value === undefined ? defaultValue : Number(value);
  if (!Number.isFinite(n) || !Number.isInteger(n) || !predicate(n)) return invalidInterpreter(message);
  return Result.ok(n);
}

function validateRetentionResult(
  raw: any
): Result<{ maxAgeMs: number } | undefined, StreamInterpreterConfigValidationError> {
  if (raw == null) return Result.ok(undefined);
  if (!raw || typeof raw !== "object") return invalidInterpreter("interpreter.touch.retention must be an object");
  const maxAge = raw.maxAge;
  const maxAgeMsRaw = raw.maxAgeMs;
  if (maxAge !== undefined && maxAgeMsRaw !== undefined) {
    return invalidInterpreter("interpreter.touch.retention must specify only one of maxAge|maxAgeMs");
  }
  let ms: number | null = null;
  if (maxAgeMsRaw !== undefined) {
    if (typeof maxAgeMsRaw !== "number" || !Number.isFinite(maxAgeMsRaw) || maxAgeMsRaw < 0) {
      return invalidInterpreter("interpreter.touch.retention.maxAgeMs must be a non-negative number");
    }
    ms = maxAgeMsRaw;
  } else if (maxAge !== undefined) {
    if (typeof maxAge !== "string" || maxAge.trim() === "") {
      return invalidInterpreter("interpreter.touch.retention.maxAge must be a non-empty duration string");
    }
    const durationRes = parseDurationMsResult(maxAge);
    if (Result.isError(durationRes)) {
      return invalidInterpreter(`interpreter.touch.retention.maxAge ${durationRes.error.message}`);
    }
    ms = durationRes.value;
    if (ms < 0) ms = 0;
  } else {
    return invalidInterpreter("interpreter.touch.retention must include maxAge or maxAgeMs");
  }
  return Result.ok({ maxAgeMs: ms });
}

function validateTouchConfigResult(raw: any): Result<TouchConfig, StreamInterpreterConfigValidationError> {
  if (!raw || typeof raw !== "object") return invalidInterpreter("interpreter.touch must be an object");
  const enabled = !!raw.enabled;
  if (!enabled) {
    return Result.ok({
      enabled: false,
      storage: undefined,
      derivedStream: undefined,
      retention: undefined,
    });
  }

  const storageRaw = raw.storage === undefined ? "memory" : String(raw.storage).trim();
  if (storageRaw !== "memory" && storageRaw !== "sqlite") {
    return invalidInterpreter("interpreter.touch.storage must be memory|sqlite");
  }
  const storage = storageRaw as "memory" | "sqlite";

  const derivedStream =
    raw.derivedStream === undefined
      ? undefined
      : typeof raw.derivedStream === "string" && raw.derivedStream.trim() !== ""
        ? raw.derivedStream
        : null;
  if (derivedStream === null) {
    return invalidInterpreter("interpreter.touch.derivedStream must be a non-empty string when provided");
  }

  // Touch companions are intended as short-retention invalidation journals.
  // Default to 24h to prevent unbounded growth if retention is omitted.
  const retentionRaw = raw.retention;
  const retentionRes = retentionRaw === undefined ? Result.ok({ maxAgeMs: 24 * 60 * 60 * 1000 }) : validateRetentionResult(retentionRaw);
  if (Result.isError(retentionRes)) return invalidInterpreter(retentionRes.error.message);
  const retention = retentionRes.value;

  const coarseIntervalMsRes = parseNumberField(
    raw.coarseIntervalMs,
    100,
    "interpreter.touch.coarseIntervalMs must be > 0",
    (n) => n > 0
  );
  if (Result.isError(coarseIntervalMsRes)) return coarseIntervalMsRes;
  const touchCoalesceWindowMsRes = parseNumberField(
    raw.touchCoalesceWindowMs,
    100,
    "interpreter.touch.touchCoalesceWindowMs must be > 0",
    (n) => n > 0
  );
  if (Result.isError(touchCoalesceWindowMsRes)) return touchCoalesceWindowMsRes;

  const onMissingBefore = raw.onMissingBefore === undefined ? "coarse" : raw.onMissingBefore;
  if (onMissingBefore !== "coarse" && onMissingBefore !== "skipBefore" && onMissingBefore !== "error") {
    return invalidInterpreter("interpreter.touch.onMissingBefore must be coarse|skipBefore|error");
  }

  const templates = raw.templates && typeof raw.templates === "object" ? raw.templates : {};
  const defaultInactivityTtlMsRes = parseNumberField(
    templates.defaultInactivityTtlMs,
    60 * 60 * 1000,
    "interpreter.touch.templates.defaultInactivityTtlMs must be >= 0",
    (n) => n >= 0
  );
  if (Result.isError(defaultInactivityTtlMsRes)) return defaultInactivityTtlMsRes;
  const lastSeenPersistIntervalMsRes = parseNumberField(
    templates.lastSeenPersistIntervalMs,
    5 * 60 * 1000,
    "interpreter.touch.templates.lastSeenPersistIntervalMs must be > 0",
    (n) => n > 0
  );
  if (Result.isError(lastSeenPersistIntervalMsRes)) return lastSeenPersistIntervalMsRes;
  const gcIntervalMsRes = parseNumberField(
    templates.gcIntervalMs,
    60_000,
    "interpreter.touch.templates.gcIntervalMs must be > 0",
    (n) => n > 0
  );
  if (Result.isError(gcIntervalMsRes)) return gcIntervalMsRes;
  const maxActiveTemplatesPerEntityRes = parseNumberField(
    templates.maxActiveTemplatesPerEntity,
    256,
    "interpreter.touch.templates.maxActiveTemplatesPerEntity must be > 0",
    (n) => n > 0
  );
  if (Result.isError(maxActiveTemplatesPerEntityRes)) return maxActiveTemplatesPerEntityRes;
  const maxActiveTemplatesPerStreamRes = parseNumberField(
    templates.maxActiveTemplatesPerStream,
    2048,
    "interpreter.touch.templates.maxActiveTemplatesPerStream must be > 0",
    (n) => n > 0
  );
  if (Result.isError(maxActiveTemplatesPerStreamRes)) return maxActiveTemplatesPerStreamRes;
  const activationRateLimitPerMinuteRes = parseNumberField(
    templates.activationRateLimitPerMinute,
    100,
    "interpreter.touch.templates.activationRateLimitPerMinute must be >= 0",
    (n) => n >= 0
  );
  if (Result.isError(activationRateLimitPerMinuteRes)) return activationRateLimitPerMinuteRes;

  if (raw.metrics !== undefined) {
    return invalidInterpreter("interpreter.touch.metrics is not supported; live metrics are a global server feature");
  }

  const memoryRaw = raw.memory && typeof raw.memory === "object" ? raw.memory : {};
  const bucketMsRes = parseIntegerField(
    memoryRaw.bucketMs,
    100,
    "interpreter.touch.memory.bucketMs must be an integer > 0",
    (n) => n > 0
  );
  if (Result.isError(bucketMsRes)) return bucketMsRes;
  const filterPow2Res = parseIntegerField(
    memoryRaw.filterPow2,
    22,
    "interpreter.touch.memory.filterPow2 must be an integer in [10,30]",
    (n) => n >= 10 && n <= 30
  );
  if (Result.isError(filterPow2Res)) return filterPow2Res;
  const kRes = parseIntegerField(
    memoryRaw.k,
    4,
    "interpreter.touch.memory.k must be an integer in [1,8]",
    (n) => n >= 1 && n <= 8
  );
  if (Result.isError(kRes)) return kRes;
  const pendingMaxKeysRes = parseIntegerField(
    memoryRaw.pendingMaxKeys,
    100_000,
    "interpreter.touch.memory.pendingMaxKeys must be an integer > 0",
    (n) => n > 0
  );
  if (Result.isError(pendingMaxKeysRes)) return pendingMaxKeysRes;
  const keyIndexMaxKeysRes = parseIntegerField(
    memoryRaw.keyIndexMaxKeys,
    32,
    "interpreter.touch.memory.keyIndexMaxKeys must be an integer in [1,1024]",
    (n) => n >= 1 && n <= 1024
  );
  if (Result.isError(keyIndexMaxKeysRes)) return keyIndexMaxKeysRes;
  const hotKeyTtlMsRes = parseIntegerField(
    memoryRaw.hotKeyTtlMs,
    10_000,
    "interpreter.touch.memory.hotKeyTtlMs must be an integer > 0",
    (n) => n > 0
  );
  if (Result.isError(hotKeyTtlMsRes)) return hotKeyTtlMsRes;
  const hotTemplateTtlMsRes = parseIntegerField(
    memoryRaw.hotTemplateTtlMs,
    10_000,
    "interpreter.touch.memory.hotTemplateTtlMs must be an integer > 0",
    (n) => n > 0
  );
  if (Result.isError(hotTemplateTtlMsRes)) return hotTemplateTtlMsRes;
  const hotMaxKeysRes = parseIntegerField(
    memoryRaw.hotMaxKeys,
    1_000_000,
    "interpreter.touch.memory.hotMaxKeys must be an integer > 0",
    (n) => n > 0
  );
  if (Result.isError(hotMaxKeysRes)) return hotMaxKeysRes;
  const hotMaxTemplatesRes = parseIntegerField(
    memoryRaw.hotMaxTemplates,
    4096,
    "interpreter.touch.memory.hotMaxTemplates must be an integer > 0",
    (n) => n > 0
  );
  if (Result.isError(hotMaxTemplatesRes)) return hotMaxTemplatesRes;

  const lagDegradeFineTouchesAtSourceOffsetsRes = parseIntegerField(
    raw.lagDegradeFineTouchesAtSourceOffsets,
    5000,
    "interpreter.touch.lagDegradeFineTouchesAtSourceOffsets must be an integer >= 0",
    (n) => n >= 0
  );
  if (Result.isError(lagDegradeFineTouchesAtSourceOffsetsRes)) return lagDegradeFineTouchesAtSourceOffsetsRes;
  const lagRecoverFineTouchesAtSourceOffsetsRes = parseIntegerField(
    raw.lagRecoverFineTouchesAtSourceOffsets,
    1000,
    "interpreter.touch.lagRecoverFineTouchesAtSourceOffsets must be an integer >= 0",
    (n) => n >= 0
  );
  if (Result.isError(lagRecoverFineTouchesAtSourceOffsetsRes)) return lagRecoverFineTouchesAtSourceOffsetsRes;
  const fineTouchBudgetPerBatchRes = parseIntegerField(
    raw.fineTouchBudgetPerBatch,
    2000,
    "interpreter.touch.fineTouchBudgetPerBatch must be an integer >= 0",
    (n) => n >= 0
  );
  if (Result.isError(fineTouchBudgetPerBatchRes)) return fineTouchBudgetPerBatchRes;
  const fineTokensPerSecondRes = parseIntegerField(
    raw.fineTokensPerSecond,
    200_000,
    "interpreter.touch.fineTokensPerSecond must be an integer >= 0",
    (n) => n >= 0
  );
  if (Result.isError(fineTokensPerSecondRes)) return fineTokensPerSecondRes;
  const fineBurstTokensRes = parseIntegerField(
    raw.fineBurstTokens,
    400_000,
    "interpreter.touch.fineBurstTokens must be an integer >= 0",
    (n) => n >= 0
  );
  if (Result.isError(fineBurstTokensRes)) return fineBurstTokensRes;
  const lagReservedFineTouchBudgetPerBatchRes = parseIntegerField(
    raw.lagReservedFineTouchBudgetPerBatch,
    200,
    "interpreter.touch.lagReservedFineTouchBudgetPerBatch must be an integer >= 0",
    (n) => n >= 0
  );
  if (Result.isError(lagReservedFineTouchBudgetPerBatchRes)) return lagReservedFineTouchBudgetPerBatchRes;

  return Result.ok({
    enabled: true,
    storage,
    derivedStream,
    retention,
    coarseIntervalMs: coarseIntervalMsRes.value,
    touchCoalesceWindowMs: touchCoalesceWindowMsRes.value,
    onMissingBefore,
    lagDegradeFineTouchesAtSourceOffsets: lagDegradeFineTouchesAtSourceOffsetsRes.value,
    lagRecoverFineTouchesAtSourceOffsets: lagRecoverFineTouchesAtSourceOffsetsRes.value,
    fineTouchBudgetPerBatch: fineTouchBudgetPerBatchRes.value,
    fineTokensPerSecond: fineTokensPerSecondRes.value,
    fineBurstTokens: fineBurstTokensRes.value,
    lagReservedFineTouchBudgetPerBatch: lagReservedFineTouchBudgetPerBatchRes.value,
    memory: {
      bucketMs: bucketMsRes.value,
      filterPow2: filterPow2Res.value,
      k: kRes.value,
      pendingMaxKeys: pendingMaxKeysRes.value,
      keyIndexMaxKeys: keyIndexMaxKeysRes.value,
      hotKeyTtlMs: hotKeyTtlMsRes.value,
      hotTemplateTtlMs: hotTemplateTtlMsRes.value,
      hotMaxKeys: hotMaxKeysRes.value,
      hotMaxTemplates: hotMaxTemplatesRes.value,
    },
    templates: {
      defaultInactivityTtlMs: defaultInactivityTtlMsRes.value,
      lastSeenPersistIntervalMs: lastSeenPersistIntervalMsRes.value,
      gcIntervalMs: gcIntervalMsRes.value,
      maxActiveTemplatesPerEntity: maxActiveTemplatesPerEntityRes.value,
      maxActiveTemplatesPerStream: maxActiveTemplatesPerStreamRes.value,
      activationRateLimitPerMinute: activationRateLimitPerMinuteRes.value,
    },
  });
}

export function validateStreamInterpreterConfigResult(
  raw: any
): Result<StreamInterpreterConfig, StreamInterpreterConfigValidationError> {
  if (!raw || typeof raw !== "object") return invalidInterpreter("interpreter must be an object");
  if (raw.apiVersion !== "durable.streams/stream-interpreter/v1") {
    return invalidInterpreter("invalid interpreter apiVersion");
  }
  const formatRaw = raw.format === undefined ? undefined : raw.format;
  if (formatRaw !== undefined && formatRaw !== "durable.streams/state-protocol/v1") {
    return invalidInterpreter("interpreter.format must be durable.streams/state-protocol/v1");
  }
  if (raw.variants !== undefined) {
    return invalidInterpreter("interpreter.variants is not supported (State Protocol is the only supported format)");
  }
  let touch: TouchConfig | undefined;
  if (raw.touch !== undefined) {
    const touchRes = validateTouchConfigResult(raw.touch);
    if (Result.isError(touchRes)) return invalidInterpreter(touchRes.error.message);
    touch = touchRes.value;
  }
  return Result.ok({
    apiVersion: "durable.streams/stream-interpreter/v1",
    format: "durable.streams/state-protocol/v1",
    touch,
  });
}

export function validateStreamInterpreterConfig(raw: any): StreamInterpreterConfig {
  const res = validateStreamInterpreterConfigResult(raw);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function isTouchEnabled(cfg: StreamInterpreterConfig | undefined): cfg is StreamInterpreterConfig & { touch: TouchConfig } {
  return !!cfg?.touch?.enabled;
}
