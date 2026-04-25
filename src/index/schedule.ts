import type { Config } from "../config";

const LOW_MEMORY_INDEX_WAKE_LIMIT_BYTES = 1024 * 1024 * 1024;
const DEFERRED_INDEX_INTERVAL_MS = 60_000;
export const LOW_MEMORY_INDEX_ENQUEUE_QUIET_MS = 30_000;
export const LOW_MEMORY_INDEX_ENQUEUE_MAX_DEFER_MS = LOW_MEMORY_INDEX_ENQUEUE_QUIET_MS * 2;

export function shouldDeferEnqueuedIndexWork(
  cfg: Pick<Config, "indexCheckIntervalMs" | "memoryLimitBytes">
): boolean {
  return (
    cfg.memoryLimitBytes > 0 &&
    cfg.memoryLimitBytes <= LOW_MEMORY_INDEX_WAKE_LIMIT_BYTES &&
    cfg.indexCheckIntervalMs >= DEFERRED_INDEX_INTERVAL_MS
  );
}

export function shouldWaitForLowMemoryIndexQuiet(
  cfg: Pick<Config, "indexCheckIntervalMs" | "memoryLimitBytes">,
  firstQueuedAtMs: number | null,
  recentForegroundActivity: boolean,
  nowMs = Date.now()
): boolean {
  if (!shouldDeferEnqueuedIndexWork(cfg)) return false;
  if (!recentForegroundActivity) return false;
  if (firstQueuedAtMs != null && nowMs - firstQueuedAtMs >= LOW_MEMORY_INDEX_ENQUEUE_MAX_DEFER_MS) return false;
  return true;
}
