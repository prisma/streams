import { Result } from "better-result";
import { dsError } from "../util/ds_error.ts";

export type TouchConfigValidationError = {
  kind: "invalid_touch";
  message: string;
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function rejectUnknownKeysResult(
  obj: Record<string, unknown>,
  allowed: readonly string[],
  path: string
): Result<void, TouchConfigValidationError> {
  const allowedSet = new Set(allowed);
  for (const key of Object.keys(obj)) {
    if (!allowedSet.has(key)) return invalidTouch(`${path}.${key} is not supported`);
  }
  return Result.ok(undefined);
}

export type TouchConfig = {
  enabled: boolean;
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
   * Policy when an update event is missing `old_value` (before image).
   *
   * - coarse: emit coarse table touches only (safe default)
   * - skipBefore: compute fine touches from `value` only
   * - error: processing errors (useful for strict debugging)
   */
  onMissingBefore?: "coarse" | "skipBefore" | "error";
  /**
   * Optional guardrail: when the touch-processing backlog (source offsets behind the tail)
   * exceeds this threshold, the processor will emit coarse table touches only
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
   * Optional guardrail: cap fine/template touches emitted per processing batch.
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
   * In-memory touch journal parameters.
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

function invalidTouch<T = never>(message: string): Result<T, TouchConfigValidationError> {
  return Result.err({ kind: "invalid_touch", message });
}

function parseNumberField(
  value: any,
  defaultValue: number,
  message: string,
  predicate: (n: number) => boolean
): Result<number, TouchConfigValidationError> {
  const n = value === undefined ? defaultValue : Number(value);
  if (!Number.isFinite(n) || !predicate(n)) return invalidTouch(message);
  return Result.ok(n);
}

function parseIntegerField(
  value: any,
  defaultValue: number,
  message: string,
  predicate: (n: number) => boolean
): Result<number, TouchConfigValidationError> {
  const n = value === undefined ? defaultValue : Number(value);
  if (!Number.isFinite(n) || !Number.isInteger(n) || !predicate(n)) return invalidTouch(message);
  return Result.ok(n);
}

export function validateTouchConfigResult(raw: any, fieldPath = "touch"): Result<TouchConfig, TouchConfigValidationError> {
  if (!isPlainObject(raw)) return invalidTouch(`${fieldPath} must be an object`);
  const topLevelCheck = rejectUnknownKeysResult(
    raw,
    [
      "enabled",
      "coarseIntervalMs",
      "touchCoalesceWindowMs",
      "onMissingBefore",
      "lagDegradeFineTouchesAtSourceOffsets",
      "lagRecoverFineTouchesAtSourceOffsets",
      "fineTouchBudgetPerBatch",
      "fineTokensPerSecond",
      "fineBurstTokens",
      "lagReservedFineTouchBudgetPerBatch",
      "memory",
      "templates",
    ],
    fieldPath
  );
  if (Result.isError(topLevelCheck)) return topLevelCheck;

  const enabled = !!raw.enabled;
  if (!enabled) {
    return Result.ok({ enabled: false });
  }

  const coarseIntervalMsRes = parseNumberField(
    raw.coarseIntervalMs,
    100,
    `${fieldPath}.coarseIntervalMs must be > 0`,
    (n) => n > 0
  );
  if (Result.isError(coarseIntervalMsRes)) return coarseIntervalMsRes;
  const touchCoalesceWindowMsRes = parseNumberField(
    raw.touchCoalesceWindowMs,
    100,
    `${fieldPath}.touchCoalesceWindowMs must be > 0`,
    (n) => n > 0
  );
  if (Result.isError(touchCoalesceWindowMsRes)) return touchCoalesceWindowMsRes;

  const onMissingBefore = raw.onMissingBefore === undefined ? "coarse" : raw.onMissingBefore;
  if (onMissingBefore !== "coarse" && onMissingBefore !== "skipBefore" && onMissingBefore !== "error") {
    return invalidTouch(`${fieldPath}.onMissingBefore must be coarse|skipBefore|error`);
  }

  const templates = raw.templates === undefined ? {} : isPlainObject(raw.templates) ? raw.templates : null;
  if (templates == null) return invalidTouch(`${fieldPath}.templates must be an object`);
  const templatesCheck = rejectUnknownKeysResult(
    templates,
    ["defaultInactivityTtlMs", "lastSeenPersistIntervalMs", "gcIntervalMs", "maxActiveTemplatesPerEntity", "maxActiveTemplatesPerStream", "activationRateLimitPerMinute"],
    `${fieldPath}.templates`
  );
  if (Result.isError(templatesCheck)) return templatesCheck;
  const defaultInactivityTtlMsRes = parseNumberField(
    templates.defaultInactivityTtlMs,
    60 * 60 * 1000,
    `${fieldPath}.templates.defaultInactivityTtlMs must be >= 0`,
    (n) => n >= 0
  );
  if (Result.isError(defaultInactivityTtlMsRes)) return defaultInactivityTtlMsRes;
  const lastSeenPersistIntervalMsRes = parseNumberField(
    templates.lastSeenPersistIntervalMs,
    5 * 60 * 1000,
    `${fieldPath}.templates.lastSeenPersistIntervalMs must be > 0`,
    (n) => n > 0
  );
  if (Result.isError(lastSeenPersistIntervalMsRes)) return lastSeenPersistIntervalMsRes;
  const gcIntervalMsRes = parseNumberField(
    templates.gcIntervalMs,
    60_000,
    `${fieldPath}.templates.gcIntervalMs must be > 0`,
    (n) => n > 0
  );
  if (Result.isError(gcIntervalMsRes)) return gcIntervalMsRes;
  const maxActiveTemplatesPerEntityRes = parseNumberField(
    templates.maxActiveTemplatesPerEntity,
    256,
    `${fieldPath}.templates.maxActiveTemplatesPerEntity must be > 0`,
    (n) => n > 0
  );
  if (Result.isError(maxActiveTemplatesPerEntityRes)) return maxActiveTemplatesPerEntityRes;
  const maxActiveTemplatesPerStreamRes = parseNumberField(
    templates.maxActiveTemplatesPerStream,
    2048,
    `${fieldPath}.templates.maxActiveTemplatesPerStream must be > 0`,
    (n) => n > 0
  );
  if (Result.isError(maxActiveTemplatesPerStreamRes)) return maxActiveTemplatesPerStreamRes;
  const activationRateLimitPerMinuteRes = parseNumberField(
    templates.activationRateLimitPerMinute,
    100,
    `${fieldPath}.templates.activationRateLimitPerMinute must be >= 0`,
    (n) => n >= 0
  );
  if (Result.isError(activationRateLimitPerMinuteRes)) return activationRateLimitPerMinuteRes;

  const memoryRaw = raw.memory === undefined ? {} : isPlainObject(raw.memory) ? raw.memory : null;
  if (memoryRaw == null) return invalidTouch(`${fieldPath}.memory must be an object`);
  const memoryCheck = rejectUnknownKeysResult(
    memoryRaw,
    ["bucketMs", "filterPow2", "k", "pendingMaxKeys", "keyIndexMaxKeys", "hotKeyTtlMs", "hotTemplateTtlMs", "hotMaxKeys", "hotMaxTemplates"],
    `${fieldPath}.memory`
  );
  if (Result.isError(memoryCheck)) return memoryCheck;
  const bucketMsRes = parseIntegerField(
    memoryRaw.bucketMs,
    100,
    `${fieldPath}.memory.bucketMs must be an integer > 0`,
    (n) => n > 0
  );
  if (Result.isError(bucketMsRes)) return bucketMsRes;
  const filterPow2Res = parseIntegerField(
    memoryRaw.filterPow2,
    22,
    `${fieldPath}.memory.filterPow2 must be an integer in [10,30]`,
    (n) => n >= 10 && n <= 30
  );
  if (Result.isError(filterPow2Res)) return filterPow2Res;
  const kRes = parseIntegerField(
    memoryRaw.k,
    4,
    `${fieldPath}.memory.k must be an integer in [1,8]`,
    (n) => n >= 1 && n <= 8
  );
  if (Result.isError(kRes)) return kRes;
  const pendingMaxKeysRes = parseIntegerField(
    memoryRaw.pendingMaxKeys,
    100_000,
    `${fieldPath}.memory.pendingMaxKeys must be an integer > 0`,
    (n) => n > 0
  );
  if (Result.isError(pendingMaxKeysRes)) return pendingMaxKeysRes;
  const keyIndexMaxKeysRes = parseIntegerField(
    memoryRaw.keyIndexMaxKeys,
    32,
    `${fieldPath}.memory.keyIndexMaxKeys must be an integer in [1,1024]`,
    (n) => n >= 1 && n <= 1024
  );
  if (Result.isError(keyIndexMaxKeysRes)) return keyIndexMaxKeysRes;
  const hotKeyTtlMsRes = parseIntegerField(
    memoryRaw.hotKeyTtlMs,
    10_000,
    `${fieldPath}.memory.hotKeyTtlMs must be an integer > 0`,
    (n) => n > 0
  );
  if (Result.isError(hotKeyTtlMsRes)) return hotKeyTtlMsRes;
  const hotTemplateTtlMsRes = parseIntegerField(
    memoryRaw.hotTemplateTtlMs,
    10_000,
    `${fieldPath}.memory.hotTemplateTtlMs must be an integer > 0`,
    (n) => n > 0
  );
  if (Result.isError(hotTemplateTtlMsRes)) return hotTemplateTtlMsRes;
  const hotMaxKeysRes = parseIntegerField(
    memoryRaw.hotMaxKeys,
    1_000_000,
    `${fieldPath}.memory.hotMaxKeys must be an integer > 0`,
    (n) => n > 0
  );
  if (Result.isError(hotMaxKeysRes)) return hotMaxKeysRes;
  const hotMaxTemplatesRes = parseIntegerField(
    memoryRaw.hotMaxTemplates,
    4096,
    `${fieldPath}.memory.hotMaxTemplates must be an integer > 0`,
    (n) => n > 0
  );
  if (Result.isError(hotMaxTemplatesRes)) return hotMaxTemplatesRes;

  const lagDegradeFineTouchesAtSourceOffsetsRes = parseIntegerField(
    raw.lagDegradeFineTouchesAtSourceOffsets,
    5000,
    `${fieldPath}.lagDegradeFineTouchesAtSourceOffsets must be an integer >= 0`,
    (n) => n >= 0
  );
  if (Result.isError(lagDegradeFineTouchesAtSourceOffsetsRes)) return lagDegradeFineTouchesAtSourceOffsetsRes;
  const lagRecoverFineTouchesAtSourceOffsetsRes = parseIntegerField(
    raw.lagRecoverFineTouchesAtSourceOffsets,
    1000,
    `${fieldPath}.lagRecoverFineTouchesAtSourceOffsets must be an integer >= 0`,
    (n) => n >= 0
  );
  if (Result.isError(lagRecoverFineTouchesAtSourceOffsetsRes)) return lagRecoverFineTouchesAtSourceOffsetsRes;
  const fineTouchBudgetPerBatchRes = parseIntegerField(
    raw.fineTouchBudgetPerBatch,
    2000,
    `${fieldPath}.fineTouchBudgetPerBatch must be an integer >= 0`,
    (n) => n >= 0
  );
  if (Result.isError(fineTouchBudgetPerBatchRes)) return fineTouchBudgetPerBatchRes;
  const fineTokensPerSecondRes = parseIntegerField(
    raw.fineTokensPerSecond,
    200_000,
    `${fieldPath}.fineTokensPerSecond must be an integer >= 0`,
    (n) => n >= 0
  );
  if (Result.isError(fineTokensPerSecondRes)) return fineTokensPerSecondRes;
  const fineBurstTokensRes = parseIntegerField(
    raw.fineBurstTokens,
    400_000,
    `${fieldPath}.fineBurstTokens must be an integer >= 0`,
    (n) => n >= 0
  );
  if (Result.isError(fineBurstTokensRes)) return fineBurstTokensRes;
  const lagReservedFineTouchBudgetPerBatchRes = parseIntegerField(
    raw.lagReservedFineTouchBudgetPerBatch,
    200,
    `${fieldPath}.lagReservedFineTouchBudgetPerBatch must be an integer >= 0`,
    (n) => n >= 0
  );
  if (Result.isError(lagReservedFineTouchBudgetPerBatchRes)) return lagReservedFineTouchBudgetPerBatchRes;

  return Result.ok({
    enabled: true,
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

export function validateTouchConfig(raw: any, fieldPath = "touch"): TouchConfig {
  const res = validateTouchConfigResult(raw, fieldPath);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function isTouchEnabled(cfg: TouchConfig | undefined): cfg is TouchConfig & { enabled: true } {
  return !!cfg?.enabled;
}
