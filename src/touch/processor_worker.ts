import { parentPort, workerData } from "node:worker_threads";
import { Result } from "better-result";
import type { Config } from "../config.ts";
import { SqliteDurableStore } from "../db/db.ts";
import { resolveEnabledTouchCapability } from "../profiles/index.ts";
import type { HostRuntime } from "../runtime/host_runtime.ts";
import { setSqliteRuntimeOverride } from "../sqlite/adapter.ts";
import { initConsoleLogging } from "../util/log.ts";
import type { ProcessRequest } from "./worker_protocol.ts";
import {
  encodeTemplateArg,
  membershipKeyFor,
  membershipKeyIdFor,
  projectedFieldKeyFor,
  projectedFieldKeyIdFor,
  tableKeyIdFor,
  templateKeyIdFor,
  watchKeyFor,
  watchKeyIdFor,
  type TemplateEncoding,
} from "./live_keys.ts";

initConsoleLogging();

const data = workerData as { config: Config; hostRuntime?: HostRuntime };
const cfg = data.config;
// Bun worker_threads can miss the Bun globals that the main thread sees.
// Use the parent host runtime hint before the worker opens SQLite.
setSqliteRuntimeOverride(data.hostRuntime ?? null);
// The main server process initializes/migrates schema; workers should avoid
// concurrent migrations on the same sqlite file.
const db = new SqliteDurableStore(cfg.dbPath, { cacheBytes: cfg.workerSqliteCacheBytes, skipMigrations: true });

const decoder = new TextDecoder();

type ActiveTemplate = {
  templateId: string;
  entity: string;
  fields: string[];
  encodings: TemplateEncoding[];
  activeFromSourceOffset: bigint;
};

type TouchProcessorWorkerError = { kind: "missing_old_value"; message: string };

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function isProjectedFieldValue(value: unknown): boolean {
  return (
    value === undefined ||
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    typeof value === "bigint" ||
    (typeof value === "number" && Number.isFinite(value))
  );
}

function projectedFieldValueEquals(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  return typeof a === "number" && typeof b === "number" && Number.isNaN(a) && Number.isNaN(b);
}

function changedProjectedFieldNames(args: {
  before: Record<string, unknown>;
  after: Record<string, unknown>;
  excluded: ReadonlySet<string>;
}): string[] {
  const names = new Set<string>([...Object.keys(args.before), ...Object.keys(args.after)]);
  const out: string[] = [];
  for (const name of names) {
    if (args.excluded.has(name)) continue;
    const beforeValue = Object.prototype.hasOwnProperty.call(args.before, name) ? args.before[name] : undefined;
    const afterValue = Object.prototype.hasOwnProperty.call(args.after, name) ? args.after[name] : undefined;
    if (!isProjectedFieldValue(beforeValue) || !isProjectedFieldValue(afterValue)) continue;
    if (!projectedFieldValueEquals(beforeValue, afterValue)) out.push(name);
  }
  return out;
}

function projectedFieldNamesFromAfter(args: { after: Record<string, unknown>; excluded: ReadonlySet<string> }): string[] {
  const out: string[] = [];
  for (const name of Object.keys(args.after)) {
    if (args.excluded.has(name)) continue;
    if (!isProjectedFieldValue(args.after[name])) continue;
    out.push(name);
  }
  return out;
}

async function handleProcess(msg: ProcessRequest): Promise<void> {
  const { stream, fromOffset, toOffset, profile, maxRows, maxBytes } = msg;
  const failProcess = (message: string): void => {
    const err = Result.err<never, TouchProcessorWorkerError>({ kind: "missing_old_value", message });
    parentPort?.postMessage({
      type: "error",
      id: msg.id,
      stream,
      message: err.error.message,
    });
  };
  const enabledTouch = resolveEnabledTouchCapability(profile);
  if (!enabledTouch) {
    parentPort?.postMessage({
      type: "error",
      id: msg.id,
      stream,
      message: "touch not enabled for profile",
    });
    return;
  }
  const { capability: touchCapability, touchCfg: touch } = enabledTouch;

  const fineBudgetRaw = msg.fineTouchBudget ?? touch.fineTouchBudgetPerBatch;
  const fineBudget = fineBudgetRaw == null ? null : Math.max(0, Math.floor(fineBudgetRaw));
  const fineGranularity = msg.fineGranularity === "template" ? "template" : "key";
  const processingMode = msg.processingMode === "hotTemplatesOnly" ? "hotTemplatesOnly" : "full";
  const hotTemplatesOnly = fineGranularity === "template" && processingMode === "hotTemplatesOnly";

  const emitFineTouches = msg.emitFineTouches !== false && fineBudget !== 0;
  let fineBudgetExhausted = fineBudget != null && fineBudget <= 0;
  let fineKeysBudgetRemaining = fineBudget;
  let fineTouchesSuppressedDueToBudget = false;
  const filterHotTemplates = msg.filterHotTemplates === true;
  const hotTemplateIdsRaw = filterHotTemplates ? msg.hotTemplateIds ?? [] : [];
  const hotTemplateIds = filterHotTemplates ? new Set(hotTemplateIdsRaw.filter((x): x is string => typeof x === "string" && /^[0-9a-f]{16}$/.test(x))) : null;

  const coarseIntervalMs = Math.max(1, Math.floor(touch.coarseIntervalMs ?? 100));
  const coalesceWindowMs = Math.max(1, Math.floor(touch.touchCoalesceWindowMs ?? 100));
  const onMissingBefore = touch.onMissingBefore ?? "coarse";

  const templatesByEntity = new Map<string, ActiveTemplate[]>();
  const coldTemplateCountByEntity = new Map<string, number>();
  if (emitFineTouches) {
    try {
      const rows = db.db
        .query(
          `SELECT template_id, entity, fields_json, encodings_json, active_from_source_offset
           FROM live_templates
           WHERE stream=? AND state='active';`
        )
        .all(stream) as any[];
      for (const row of rows) {
        const templateId = String(row.template_id ?? "");
        if (!/^[0-9a-f]{16}$/.test(templateId)) continue;
        const entity = String(row.entity ?? "");
        if (entity.trim() === "") continue;
        let fields: any;
        let encodings: any;
        try {
          fields = JSON.parse(String(row.fields_json ?? "[]"));
          encodings = JSON.parse(String(row.encodings_json ?? "[]"));
        } catch {
          continue;
        }
        if (!Array.isArray(fields) || !Array.isArray(encodings) || fields.length !== encodings.length) continue;
        const f = fields.map(String);
        const e = encodings.map(String) as TemplateEncoding[];
        if (f.length === 0 || f.length > 3) continue;
        if (!e.every((x) => x === "string" || x === "int64" || x === "bool" || x === "datetime" || x === "bytes")) continue;
        if (hotTemplateIds && !hotTemplateIds.has(templateId)) {
          coldTemplateCountByEntity.set(entity, (coldTemplateCountByEntity.get(entity) ?? 0) + 1);
          continue;
        }
        const activeFromSourceOffset = typeof row.active_from_source_offset === "bigint" ? row.active_from_source_offset : BigInt(row.active_from_source_offset ?? 0);
        const tpl: ActiveTemplate = { templateId, entity, fields: f, encodings: e, activeFromSourceOffset };
        const arr = templatesByEntity.get(entity) ?? [];
        arr.push(tpl);
        templatesByEntity.set(entity, arr);
      }
    } catch {
      // If the live_templates table isn't available yet (old DB), treat as no templates.
    }
  }

  let rowsRead = 0;
  let bytesRead = 0;
  let changes = 0;
  let maxSourceTsMs = 0;

  let processedThrough = fromOffset - 1n;

  type PendingTouch = {
    keyId: number;
    routingKey?: string;
    windowStartMs: number;
    watermark: string;
    entity: string;
    kind: "table" | "template";
    templateId?: string;
  };
  type EntityTemplateOnlyTouch = { offset: bigint; tsMs: number; watermark: string };

  const pending = new Map<string, PendingTouch>();
  const templateOnlyEntityTouch = new Map<string, EntityTemplateOnlyTouch>();
  const touches: Array<{ keyId: number; routingKey?: string; watermark: string; entity: string; kind: "table" | "template"; templateId?: string }> = [];
  let fineTouchesDroppedDueToBudget = 0;
  let fineTouchesSkippedColdTemplate = 0;

  const flush = (_mapKey: string, p: PendingTouch) => {
    touches.push({
      keyId: p.keyId >>> 0,
      routingKey: p.routingKey,
      watermark: p.watermark,
      entity: p.entity,
      kind: p.kind,
      templateId: p.templateId,
    });
  };

  const queueTouch = (args: {
    keyId: number;
    routingKey?: string;
    tsMs: number;
    watermark: string;
    entity: string;
    kind: "table" | "template";
    templateId?: string;
    windowMs: number;
  }) => {
    const mapKey = args.routingKey ? `r:${args.routingKey}` : `i:${args.keyId >>> 0}`;
    const prev = pending.get(mapKey);

    // Guardrail: cap fine/template touches (key cardinality) per batch.
    // Table touches are always emitted for correctness.
    if (args.kind !== "table" && fineBudget != null && !fineBudgetExhausted && !prev) {
      const remaining = fineKeysBudgetRemaining ?? 0;
      if (remaining <= 0) {
        fineBudgetExhausted = true;
        fineTouchesSuppressedDueToBudget = true;
        fineTouchesDroppedDueToBudget += 1;
        return;
      }
      fineKeysBudgetRemaining = remaining - 1;
    } else if (args.kind !== "table" && fineBudget != null && !prev && fineBudgetExhausted) {
      fineTouchesSuppressedDueToBudget = true;
      fineTouchesDroppedDueToBudget += 1;
      return;
    }

    if (!prev) {
      pending.set(mapKey, {
        keyId: args.keyId >>> 0,
        routingKey: args.routingKey,
        windowStartMs: args.tsMs,
        watermark: args.watermark,
        entity: args.entity,
        kind: args.kind,
        templateId: args.templateId,
      });
      return;
    }
    if (args.tsMs - prev.windowStartMs < args.windowMs) {
      // Coalesce within the window; keep the latest watermark for debugging.
      prev.watermark = args.watermark;
      return;
    }
    flush(mapKey, prev);
    pending.set(mapKey, {
      keyId: args.keyId >>> 0,
      routingKey: args.routingKey,
      windowStartMs: args.tsMs,
      watermark: args.watermark,
      entity: args.entity,
      kind: args.kind,
      templateId: args.templateId,
    });
  };

  for (const row of db.iterWalRange(stream, fromOffset, toOffset)) {
    const payload = row.payload as Uint8Array;
    const payloadLen = payload.byteLength;
    if (rowsRead > 0 && (rowsRead >= maxRows || bytesRead + payloadLen > maxBytes)) break;

    rowsRead++;
    bytesRead += payloadLen;
    const offset = typeof row.offset === "bigint" ? (row.offset as bigint) : BigInt(row.offset);
    processedThrough = offset;
    const tsMsRaw = row.ts_ms;
    const tsMs = typeof tsMsRaw === "bigint" ? Number(tsMsRaw) : Number(tsMsRaw);
    if (!Number.isFinite(tsMs)) continue;
    if (tsMs > maxSourceTsMs) maxSourceTsMs = tsMs;

    let value: any;
    try {
      value = JSON.parse(decoder.decode(payload));
    } catch {
      // Treat invalid JSON as "no changes".
      continue;
    }

    const canonical = touchCapability.deriveCanonicalChanges(value, profile);
    changes += canonical.length;
    if (canonical.length === 0) continue;
    const watermark = offset.toString();

    for (const ch of canonical) {
      const entity = ch.entity;

      // Always emit coarse table touches for correctness.
      const coarseKeyId = tableKeyIdFor(entity);
      queueTouch({
        keyId: coarseKeyId,
        tsMs,
        watermark,
        entity,
        kind: "table",
        windowMs: coarseIntervalMs,
      });

      if (!emitFineTouches) continue;
      if (fineBudgetExhausted) continue;

      const tpls = templatesByEntity.get(entity);
      if (filterHotTemplates) {
        fineTouchesSkippedColdTemplate += coldTemplateCountByEntity.get(entity) ?? 0;
      }
      if (!tpls || tpls.length === 0) continue;

      if (hotTemplatesOnly) {
        const prev = templateOnlyEntityTouch.get(entity);
        if (!prev || offset > prev.offset) templateOnlyEntityTouch.set(entity, { offset, tsMs, watermark });
        continue;
      }

      for (const tpl of tpls) {
        if (fineBudgetExhausted) break;
        if (offset < tpl.activeFromSourceOffset) continue;

        if (fineGranularity === "template") {
          queueTouch({
            keyId: templateKeyIdFor(tpl.templateId) >>> 0,
            tsMs,
            watermark,
            entity,
            kind: "template",
            templateId: tpl.templateId,
            windowMs: coalesceWindowMs,
          });
          if (fineBudgetExhausted) break;
          continue;
        }

        const afterObj = ch.after;
        const beforeObj = ch.before;

        const watchKeys = new Map<number, string>();
        const membershipKeys = new Map<number, string>();
        const projectedFieldKeys = new Map<number, string>();

        const computeArgs = (obj: unknown): string[] | null => {
          if (!obj || typeof obj !== "object" || Array.isArray(obj)) return null;
          const args: string[] = [];
          for (let i = 0; i < tpl.fields.length; i++) {
            const name = tpl.fields[i];
            const enc = tpl.encodings[i];
            const v = (obj as any)[name];
            const encoded = encodeTemplateArg(v, enc);
            if (encoded == null) return null;
            args.push(encoded);
          }
          return args;
        };
        const computeWatch = (args: string[]): { keyId: number; routingKey: string } => {
          const routingKey = watchKeyFor(tpl.templateId, args);
          return { keyId: watchKeyIdFor(tpl.templateId, args) >>> 0, routingKey };
        };
        const computeMembership = (args: string[]): { keyId: number; routingKey: string } => {
          const routingKey = membershipKeyFor(tpl.templateId, args);
          return { keyId: membershipKeyIdFor(tpl.templateId, args) >>> 0, routingKey };
        };
        const computeProjectedField = (fieldName: string, args: string[]): { keyId: number; routingKey: string } => {
          const routingKey = projectedFieldKeyFor(tpl.templateId, fieldName, args);
          return { keyId: projectedFieldKeyIdFor(tpl.templateId, fieldName, args) >>> 0, routingKey };
        };
        const afterArgs = computeArgs(afterObj);
        const beforeArgs = computeArgs(beforeObj);
        const watchAfter = afterArgs != null ? computeWatch(afterArgs) : null;
        const watchBefore = beforeArgs != null ? computeWatch(beforeArgs) : null;
        const membershipAfter = afterArgs != null ? computeMembership(afterArgs) : null;
        const membershipBefore = beforeArgs != null ? computeMembership(beforeArgs) : null;
        const sameTuple = watchBefore != null && watchAfter != null && watchBefore.routingKey === watchAfter.routingKey;
        const excludedProjectedFields = new Set(tpl.fields);

        if (ch.op === "insert") {
          if (watchAfter != null) watchKeys.set(watchAfter.keyId >>> 0, watchAfter.routingKey);
          if (membershipAfter != null) membershipKeys.set(membershipAfter.keyId >>> 0, membershipAfter.routingKey);
        } else if (ch.op === "delete") {
          if (watchBefore != null) watchKeys.set(watchBefore.keyId >>> 0, watchBefore.routingKey);
          if (membershipBefore != null) membershipKeys.set(membershipBefore.keyId >>> 0, membershipBefore.routingKey);
        } else {
          // update: compute touches from both before and after (when possible).
          // Policy for missing/insufficient before image:
          // - coarse: emit no fine touches (table touch already guarantees correctness)
          // - skipBefore: emit after-only touch
          // - error: fail the processing batch
          if (watchBefore != null) {
            watchKeys.set(watchBefore.keyId >>> 0, watchBefore.routingKey);
            if (watchAfter != null) watchKeys.set(watchAfter.keyId >>> 0, watchAfter.routingKey);
            if (membershipBefore != null && membershipAfter != null) {
              if (membershipBefore.routingKey !== membershipAfter.routingKey) {
                membershipKeys.set(membershipBefore.keyId >>> 0, membershipBefore.routingKey);
                membershipKeys.set(membershipAfter.keyId >>> 0, membershipAfter.routingKey);
              } else if (sameTuple && isPlainObject(beforeObj) && isPlainObject(afterObj) && afterArgs != null) {
                for (const fieldName of changedProjectedFieldNames({
                  before: beforeObj,
                  after: afterObj,
                  excluded: excludedProjectedFields,
                })) {
                  const projected = computeProjectedField(fieldName, afterArgs);
                  projectedFieldKeys.set(projected.keyId >>> 0, projected.routingKey);
                }
              }
            } else {
              if (membershipBefore != null) membershipKeys.set(membershipBefore.keyId >>> 0, membershipBefore.routingKey);
              if (membershipAfter != null) membershipKeys.set(membershipAfter.keyId >>> 0, membershipAfter.routingKey);
            }
          } else {
            if (beforeObj === undefined) {
              if (onMissingBefore === "error") {
                failProcess(`missing old_value for update (entity=${entity}, templateId=${tpl.templateId})`);
                return;
              }
            } else {
              // old_value exists but missing fields / unsupported types.
              if (onMissingBefore === "error") {
                failProcess(`old_value missing required fields for update (entity=${entity}, templateId=${tpl.templateId})`);
                return;
              }
            }

            if (onMissingBefore === "skipBefore") {
              if (watchAfter != null) watchKeys.set(watchAfter.keyId >>> 0, watchAfter.routingKey);
              if (membershipAfter != null) membershipKeys.set(membershipAfter.keyId >>> 0, membershipAfter.routingKey);
              if (afterArgs != null && isPlainObject(afterObj)) {
                for (const fieldName of projectedFieldNamesFromAfter({
                  after: afterObj,
                  excluded: excludedProjectedFields,
                })) {
                  const projected = computeProjectedField(fieldName, afterArgs);
                  projectedFieldKeys.set(projected.keyId >>> 0, projected.routingKey);
                }
              }
            } else {
              // coarse: no fine touches
            }
          }
        }

        for (const [watchKeyId, routingKey] of watchKeys) {
          queueTouch({
            keyId: watchKeyId >>> 0,
            routingKey,
            tsMs,
            watermark,
            entity,
            kind: "template",
            templateId: tpl.templateId,
            windowMs: coalesceWindowMs,
          });
          if (fineBudgetExhausted) break;
        }
        for (const [membershipKeyId, routingKey] of membershipKeys) {
          queueTouch({
            keyId: membershipKeyId >>> 0,
            routingKey,
            tsMs,
            watermark,
            entity,
            kind: "template",
            templateId: tpl.templateId,
            windowMs: coalesceWindowMs,
          });
          if (fineBudgetExhausted) break;
        }
        for (const [projectedFieldKeyId, routingKey] of projectedFieldKeys) {
          queueTouch({
            keyId: projectedFieldKeyId >>> 0,
            routingKey,
            tsMs,
            watermark,
            entity,
            kind: "template",
            templateId: tpl.templateId,
            windowMs: coalesceWindowMs,
          });
          if (fineBudgetExhausted) break;
        }
      }
    }
  }

  if (emitFineTouches && hotTemplatesOnly && !fineBudgetExhausted && templateOnlyEntityTouch.size > 0) {
    for (const [entity, agg] of templateOnlyEntityTouch.entries()) {
      if (fineBudgetExhausted) break;
      const tpls = templatesByEntity.get(entity);
      if (!tpls || tpls.length === 0) continue;
      for (const tpl of tpls) {
        if (fineBudgetExhausted) break;
        if (agg.offset < tpl.activeFromSourceOffset) continue;
        queueTouch({
          keyId: templateKeyIdFor(tpl.templateId) >>> 0,
          tsMs: agg.tsMs,
          watermark: agg.watermark,
          entity,
          kind: "template",
          templateId: tpl.templateId,
          windowMs: coalesceWindowMs,
        });
      }
    }
  }

  for (const [key, p] of pending.entries()) {
    flush(key, p);
  }

  touches.sort((a, b) => {
    const ak = a.keyId >>> 0;
    const bk = b.keyId >>> 0;
    if (ak < bk) return -1;
    if (ak > bk) return 1;
    const ar = a.routingKey ?? "";
    const br = b.routingKey ?? "";
    if (ar < br) return -1;
    if (ar > br) return 1;
    const aw = BigInt(a.watermark);
    const bw = BigInt(b.watermark);
    if (aw < bw) return -1;
    if (aw > bw) return 1;
    return 0;
  });

  let tableTouchesEmitted = 0;
  let templateTouchesEmitted = 0;
  for (const t of touches) {
    if (t.kind === "table") tableTouchesEmitted++;
    else templateTouchesEmitted++;
  }

  parentPort?.postMessage({
    type: "result",
    id: msg.id,
    stream,
    processedThrough,
    touches,
    stats: {
      rowsRead,
      bytesRead,
      changes,
      touchesEmitted: touches.length,
      tableTouchesEmitted,
      templateTouchesEmitted,
      maxSourceTsMs,
      fineTouchesDroppedDueToBudget,
      fineTouchesSuppressedDueToBudget,
      fineTouchesSkippedColdTemplate,
    },
  });
}

parentPort?.on("message", (msg: any) => {
  if (!msg || typeof msg !== "object") return;
  if (msg.type === "stop") {
    try {
      db.close();
    } catch {
      // ignore
    }
    try {
      parentPort?.postMessage({ type: "stopped" });
    } catch {
      // ignore
    }
    return;
  }
  if (msg.type === "process") {
    void handleProcess(msg as ProcessRequest).catch((e: any) => {
      try {
        parentPort?.postMessage({
          type: "error",
          id: (msg as any).id,
          stream: (msg as any).stream,
          message: String(e?.message ?? e),
          stack: e?.stack ? String(e.stack) : undefined,
        });
      } catch {
        // ignore
      }
    });
  }
});
