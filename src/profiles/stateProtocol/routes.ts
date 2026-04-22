import { Result } from "better-result";
import { encodeOffset } from "../../offset";
import { parseTouchCursor } from "../../touch/touch_journal";
import { touchKeyIdFromRoutingKeyResult } from "../../touch/touch_key_id";
import { tableKeyIdFor, templateKeyIdFor } from "../../touch/live_keys";
import type { TemplateDecl } from "../../touch/live_templates";
import type { TouchConfig } from "../../touch/spec";
import type { StreamTouchRouteArgs } from "../profile";
import { getStateProtocolTouchConfig } from "./validation";

const EXACT_FINE_WAIT_MAX_KEYS = 16;

function countActiveTemplates(stream: string, db: StreamTouchRouteArgs["db"]): number {
  try {
    const row = db.db.query(`SELECT COUNT(*) as cnt FROM live_templates WHERE stream=? AND state='active';`).get(stream) as any;
    return Number(row?.cnt ?? 0);
  } catch {
    return 0;
  }
}

function parseInactivityTtlResult(
  raw: unknown,
  defaultValue: number,
  fieldPath: string
): Result<number, { message: string }> {
  if (raw === undefined) return Result.ok(defaultValue);
  if (typeof raw === "number" && Number.isFinite(raw) && raw >= 0) {
    return Result.ok(Math.floor(raw));
  }
  return Result.err({ message: `${fieldPath} must be a non-negative number (ms)` });
}

function parseTemplateDeclsResult(raw: unknown, fieldPath: string): Result<TemplateDecl[], { message: string }> {
  if (!Array.isArray(raw) || raw.length === 0) {
    return Result.err({ message: `${fieldPath} must be a non-empty array` });
  }
  if (raw.length > 256) return Result.err({ message: `${fieldPath} too large (max 256)` });

  const templates: TemplateDecl[] = [];
  for (const t of raw) {
    const entity = typeof t?.entity === "string" ? t.entity.trim() : "";
    const fieldsRaw = t?.fields;
    if (entity === "" || !Array.isArray(fieldsRaw) || fieldsRaw.length === 0 || fieldsRaw.length > 3) {
      return Result.err({ message: `${fieldPath} contains invalid template definitions` });
    }

    const fields: TemplateDecl["fields"] = [];
    for (const f of fieldsRaw) {
      const name = typeof f?.name === "string" ? f.name.trim() : "";
      const encoding = f?.encoding;
      if (name === "") {
        return Result.err({ message: `${fieldPath} contains invalid template definitions` });
      }
      fields.push({ name, encoding });
    }
    if (fields.length !== fieldsRaw.length) {
      return Result.err({ message: `${fieldPath} contains invalid template definitions` });
    }
    templates.push({ entity, fields });
  }
  return Result.ok(templates);
}

function parseWaitTimeoutMsQueryResult(raw: string | null, defaultValue: number, fieldPath: string): Result<number, { message: string }> {
  if (raw == null || raw.trim() === "") return Result.ok(defaultValue);
  const n = Number(raw);
  if (!Number.isFinite(n)) return Result.err({ message: `${fieldPath} must be a number (ms)` });
  return Result.ok(Math.max(0, Math.min(120_000, Math.floor(n))));
}

function normalizeExactFineWaitKeys(keys: string[]): string[] {
  if (keys.length === 0 || keys.length > EXACT_FINE_WAIT_MAX_KEYS) return [];
  const normalized = Array.from(new Set(keys.map((key) => key.trim().toLowerCase())));
  if (!normalized.every((key) => /^[0-9a-f]{16}$/.test(key))) return [];
  return normalized;
}

async function handleTemplatesActivateRoute(args: StreamTouchRouteArgs, touchCfg: TouchConfig): Promise<Response> {
  const { req, stream, streamRow, touchManager, respond } = args;
  if (req.method !== "POST") return respond.badRequest("unsupported method");

  let body: any;
  try {
    body = await req.json();
  } catch {
    return respond.badRequest("activate body must be valid JSON");
  }

  const templatesRes = parseTemplateDeclsResult(body?.templates, "activate.templates");
  if (Result.isError(templatesRes)) return respond.badRequest(templatesRes.error.message);

  const inactivityTtlRes = parseInactivityTtlResult(
    body?.inactivityTtlMs,
    touchCfg.templates?.defaultInactivityTtlMs ?? 60 * 60 * 1000,
    "activate.inactivityTtlMs"
  );
  if (Result.isError(inactivityTtlRes)) return respond.badRequest(inactivityTtlRes.error.message);

  const limits = {
    maxActiveTemplatesPerStream: touchCfg.templates?.maxActiveTemplatesPerStream ?? 2048,
    maxActiveTemplatesPerEntity: touchCfg.templates?.maxActiveTemplatesPerEntity ?? 256,
  };
  const activeFromTouchOffset = touchManager.getOrCreateJournal(stream, touchCfg).getCursor();
  const res = touchManager.activateTemplates({
    stream,
    touchCfg,
    baseStreamNextOffset: streamRow.next_offset,
    activeFromTouchOffset,
    templates: templatesRes.value,
    inactivityTtlMs: inactivityTtlRes.value,
  });

  return respond.json(200, { activated: res.activated, denied: res.denied, limits });
}

function buildMetaRoutePayload(args: StreamTouchRouteArgs, touchCfg: TouchConfig) {
  const { stream, streamRow, db, touchManager } = args;
  const meta = touchManager.getOrCreateJournal(stream, touchCfg).getMeta();
  const runtime = touchManager.getTouchRuntimeSnapshot({ stream, touchCfg });
  const touchState = db.getStreamTouchState(stream);
  return {
    ...meta,
    settled: meta.pendingKeys === 0 && runtime.lagSourceOffsets === 0,
    coarseIntervalMs: touchCfg.coarseIntervalMs ?? 100,
    touchCoalesceWindowMs: touchCfg.touchCoalesceWindowMs ?? 100,
    activeTemplates: countActiveTemplates(stream, db),
    lagSourceOffsets: runtime.lagSourceOffsets,
    touchMode: runtime.touchMode,
    walScannedThrough: touchState ? encodeOffset(streamRow.epoch, touchState.processed_through) : null,
    bucketMaxSourceOffsetSeq: meta.bucketMaxSourceOffsetSeq,
    hotFineKeys: runtime.hotFineKeys,
    hotTemplates: runtime.hotTemplates,
    hotFineKeysActive: runtime.hotFineKeysActive,
    hotFineKeysGrace: runtime.hotFineKeysGrace,
    hotTemplatesActive: runtime.hotTemplatesActive,
    hotTemplatesGrace: runtime.hotTemplatesGrace,
    fineWaitersActive: runtime.fineWaitersActive,
    coarseWaitersActive: runtime.coarseWaitersActive,
    broadFineWaitersActive: runtime.broadFineWaitersActive,
    hotKeyFilteringEnabled: runtime.hotKeyFilteringEnabled,
    hotTemplateFilteringEnabled: runtime.hotTemplateFilteringEnabled,
    scanRowsTotal: runtime.scanRowsTotal,
    scanBatchesTotal: runtime.scanBatchesTotal,
    scannedButEmitted0BatchesTotal: runtime.scannedButEmitted0BatchesTotal,
    processedThroughDeltaTotal: runtime.processedThroughDeltaTotal,
    touchesEmittedTotal: runtime.touchesEmittedTotal,
    touchesTableTotal: runtime.touchesTableTotal,
    touchesTemplateTotal: runtime.touchesTemplateTotal,
    fineTouchesDroppedDueToBudgetTotal: runtime.fineTouchesDroppedDueToBudgetTotal,
    fineTouchesSkippedColdTemplateTotal: runtime.fineTouchesSkippedColdTemplateTotal,
    fineTouchesSkippedColdKeyTotal: runtime.fineTouchesSkippedColdKeyTotal,
    fineTouchesSkippedTemplateBucketTotal: runtime.fineTouchesSkippedTemplateBucketTotal,
    waitTouchedTotal: runtime.waitTouchedTotal,
    waitTimeoutTotal: runtime.waitTimeoutTotal,
    waitStaleTotal: runtime.waitStaleTotal,
    journalFlushesTotal: runtime.journalFlushesTotal,
    journalNotifyWakeupsTotal: runtime.journalNotifyWakeupsTotal,
    journalNotifyWakeMsTotal: runtime.journalNotifyWakeMsTotal,
    journalNotifyWakeMsMax: runtime.journalNotifyWakeMsMax,
    journalTimeoutsFiredTotal: runtime.journalTimeoutsFiredTotal,
    journalTimeoutSweepMsTotal: runtime.journalTimeoutSweepMsTotal,
  };
}

async function handleMetaRoute(args: StreamTouchRouteArgs, touchCfg: TouchConfig): Promise<Response> {
  const { req, respond } = args;
  if (req.method !== "GET") return respond.badRequest("unsupported method");

  const url = new URL(req.url);
  const settleRaw = url.searchParams.get("settle");
  if (settleRaw !== null && settleRaw !== "flush") {
    return respond.badRequest("meta.settle must be 'flush' when provided");
  }

  const timeoutMsRes = parseWaitTimeoutMsQueryResult(url.searchParams.get("timeoutMs"), 30_000, "meta.timeoutMs");
  if (Result.isError(timeoutMsRes)) return respond.badRequest(timeoutMsRes.error.message);

  if (settleRaw !== "flush") {
    return respond.json(200, buildMetaRoutePayload(args, touchCfg));
  }

  const deadlineMs = Date.now() + timeoutMsRes.value;
  for (;;) {
    const payload = buildMetaRoutePayload(args, touchCfg);
    if (payload.settled || Date.now() >= deadlineMs) {
      return respond.json(200, payload);
    }
    if (req.signal.aborted) return new Response(null, { status: 204 });
    const remainingMs = Math.max(1, deadlineMs - Date.now());
    await new Promise<void>((resolve) => {
      const waitMs = Math.min(25, remainingMs);
      const timer = setTimeout(() => {
        req.signal.removeEventListener("abort", onAbort);
        resolve();
      }, waitMs);
      const onAbort = () => {
        clearTimeout(timer);
        req.signal.removeEventListener("abort", onAbort);
        resolve();
      };
      req.signal.addEventListener("abort", onAbort, { once: true });
    });
  }
}

async function handleWaitRoute(args: StreamTouchRouteArgs, touchCfg: TouchConfig): Promise<Response> {
  const { req, stream, streamRow, touchManager, respond } = args;
  if (req.method !== "POST") return respond.badRequest("unsupported method");

  const waitStartMs = Date.now();
  let body: any;
  try {
    body = await req.json();
  } catch {
    return respond.badRequest("wait body must be valid JSON");
  }

  const keysRaw = body?.keys;
  if (keysRaw !== undefined && (!Array.isArray(keysRaw) || !keysRaw.every((k: any) => typeof k === "string" && k.trim() !== ""))) {
    return respond.badRequest("wait.keys must be a non-empty string array when provided");
  }
  const keys = Array.isArray(keysRaw) ? Array.from(new Set(keysRaw.map((k: string) => k.trim()))) : [];
  if (keys.length > 1024) return respond.badRequest("wait.keys too large (max 1024)");

  const keyIdsRaw = body?.keyIds;
  const keyIds =
    Array.isArray(keyIdsRaw) && keyIdsRaw.length > 0
      ? Array.from(
          new Set(
            keyIdsRaw
              .map((x: any) => Number(x))
              .filter((n: number) => Number.isFinite(n) && Number.isInteger(n) && n >= 0 && n <= 0xffffffff)
          )
        ).map((n) => n >>> 0)
      : [];
  if (Array.isArray(keyIdsRaw) && keyIds.length !== keyIdsRaw.length) {
    return respond.badRequest("wait.keyIds must be a non-empty uint32 array when provided");
  }
  if (keys.length === 0 && keyIds.length === 0) return respond.badRequest("wait requires keys or keyIds");
  if (keyIds.length > 1024) return respond.badRequest("wait.keyIds too large (max 1024)");

  const exactRaw = body?.exact;
  if (exactRaw !== undefined && typeof exactRaw !== "boolean") return respond.badRequest("wait.exact must be a boolean when provided");
  const exactRequested = exactRaw === true;

  const cursorRaw = body?.cursor;
  if (typeof cursorRaw !== "string" || cursorRaw.trim() === "") return respond.badRequest("wait.cursor must be a non-empty string");
  const cursor = cursorRaw.trim();

  const timeoutMsRaw = body?.timeoutMs;
  const timeoutMs =
    timeoutMsRaw === undefined
      ? 30_000
      : typeof timeoutMsRaw === "number" && Number.isFinite(timeoutMsRaw)
        ? Math.max(0, Math.min(120_000, timeoutMsRaw))
        : null;
  if (timeoutMs == null) return respond.badRequest("wait.timeoutMs must be a number (ms)");

  const templateIdsUsedRaw = body?.templateIdsUsed;
  if (Array.isArray(templateIdsUsedRaw) && !templateIdsUsedRaw.every((x: any) => typeof x === "string" && x.trim() !== "")) {
    return respond.badRequest("wait.templateIdsUsed must be a string array");
  }
  const templateIdsUsed =
    Array.isArray(templateIdsUsedRaw) && templateIdsUsedRaw.length > 0
      ? Array.from(new Set(templateIdsUsedRaw.map((s: any) => (typeof s === "string" ? s.trim() : "")).filter((s: string) => s !== "")))
      : [];

  const interestModeRaw = body?.interestMode;
  if (interestModeRaw !== undefined && interestModeRaw !== "fine" && interestModeRaw !== "coarse") {
    return respond.badRequest("wait.interestMode must be 'fine' or 'coarse'");
  }
  const interestMode: "fine" | "coarse" = interestModeRaw === "coarse" ? "coarse" : "fine";

  if (interestMode === "fine" && templateIdsUsed.length > 0) {
    touchManager.heartbeatTemplates({ stream, touchCfg, templateIdsUsed });
  }

  const declareTemplatesRaw = body?.declareTemplates;
  if (Array.isArray(declareTemplatesRaw) && declareTemplatesRaw.length > 0) {
    const templatesRes = parseTemplateDeclsResult(declareTemplatesRaw, "wait.declareTemplates");
    if (Result.isError(templatesRes)) return respond.badRequest(templatesRes.error.message);

    const inactivityTtlRes = parseInactivityTtlResult(
      body?.inactivityTtlMs,
      touchCfg.templates?.defaultInactivityTtlMs ?? 60 * 60 * 1000,
      "wait.inactivityTtlMs"
    );
    if (Result.isError(inactivityTtlRes)) return respond.badRequest(inactivityTtlRes.error.message);

    const activeFromTouchOffset = touchManager.getOrCreateJournal(stream, touchCfg).getCursor();
    touchManager.activateTemplates({
      stream,
      touchCfg,
      baseStreamNextOffset: streamRow.next_offset,
      activeFromTouchOffset,
      templates: templatesRes.value,
      inactivityTtlMs: inactivityTtlRes.value,
    });
  }

  const journal = touchManager.getOrCreateJournal(stream, touchCfg);
  const runtime = touchManager.getTouchRuntimeSnapshot({ stream, touchCfg });

  let rawFineKeyIds = keyIds;
  if (keyIds.length === 0) {
    const parsedKeyIds: number[] = [];
    for (const key of keys) {
      const keyIdRes = touchKeyIdFromRoutingKeyResult(key);
      if (Result.isError(keyIdRes)) return respond.internalError();
      parsedKeyIds.push(keyIdRes.value);
    }
    rawFineKeyIds = parsedKeyIds;
  }

  const templateWaitKeyIds = templateIdsUsed.length > 0 ? Array.from(new Set(templateIdsUsed.map((templateId) => templateKeyIdFor(templateId) >>> 0))) : [];
  let waitKeyIds = rawFineKeyIds;
  let effectiveWaitKind: "fineKey" | "templateKey" | "tableKey" = "fineKey";

  if (interestMode === "coarse") {
    effectiveWaitKind = "tableKey";
  } else if (runtime.touchMode === "restricted" && templateIdsUsed.length > 0) {
    effectiveWaitKind = "templateKey";
  } else if (runtime.touchMode === "coarseOnly" && templateIdsUsed.length > 0) {
    effectiveWaitKind = "tableKey";
  }

  if (effectiveWaitKind === "templateKey") {
    waitKeyIds = templateWaitKeyIds;
  } else if (effectiveWaitKind === "tableKey" && templateIdsUsed.length > 0) {
    const entities = touchManager.resolveTemplateEntitiesForWait({ stream, templateIdsUsed });
    waitKeyIds = Array.from(new Set(entities.map((entity) => tableKeyIdFor(entity) >>> 0)));
  }

  if (exactRequested && (interestMode !== "fine" || effectiveWaitKind !== "fineKey")) {
    return respond.badRequest("wait.exact requires fine interest while runtime is in fine-key mode");
  }

  const exactFineRoutingKeys =
    exactRequested && interestMode === "fine" && effectiveWaitKind === "fineKey" ? normalizeExactFineWaitKeys(keys) : [];
  if (exactRequested && exactFineRoutingKeys.length === 0) {
    return respond.badRequest("wait.exact requires 1 to 16 literal 64-bit routing keys");
  }
  const useExactFineKeyMatch = exactFineRoutingKeys.length > 0;
  const exactFallbackKeyIds =
    interestMode === "fine" && effectiveWaitKind === "fineKey" && templateWaitKeyIds.length > 0 ? templateWaitKeyIds : [];

  if (interestMode === "fine" && effectiveWaitKind === "fineKey" && templateWaitKeyIds.length > 0 && !useExactFineKeyMatch) {
    const merged = new Set<number>();
    for (const keyId of waitKeyIds) merged.add(keyId >>> 0);
    for (const keyId of templateWaitKeyIds) merged.add(keyId >>> 0);
    waitKeyIds = Array.from(merged);
  }

  if (waitKeyIds.length === 0) {
    waitKeyIds = rawFineKeyIds;
    effectiveWaitKind = "fineKey";
  }

  const hotInterestKeyIds = interestMode === "fine" ? rawFineKeyIds : waitKeyIds;
  const releaseHotInterest = touchManager.beginHotWaitInterest({
    stream,
    touchCfg,
    keyIds: hotInterestKeyIds,
    templateIdsUsed,
    interestMode,
  });

  try {
    let sinceGen: number;
    if (cursor === "now") {
      sinceGen = journal.getGeneration();
    } else {
      const parsed = parseTouchCursor(cursor);
      if (!parsed) return respond.badRequest("wait.cursor must be in the form <epochHex>:<generation> or 'now'");
      if (parsed.epoch !== journal.getEpoch()) {
        const latencyMs = Date.now() - waitStartMs;
        touchManager.recordWaitMetrics({ stream, touchCfg, keysCount: waitKeyIds.length, outcome: "stale", latencyMs });
        return respond.json(200, {
          stale: true,
          cursor: journal.getCursor(),
          epoch: journal.getEpoch(),
          generation: journal.getGeneration(),
          effectiveWaitKind,
          bucketMaxSourceOffsetSeq: journal.getLastFlushedSourceOffsetSeq().toString(),
          flushAtMs: journal.getLastFlushAtMs(),
          bucketStartMs: journal.getLastBucketStartMs(),
          error: { code: "stale", message: "cursor epoch mismatch; rerun/re-subscribe and start from cursor" },
        });
      }
      sinceGen = parsed.generation;
    }

    const nowGen = journal.getGeneration();
    if (sinceGen > nowGen) sinceGen = nowGen;

    if (useExactFineKeyMatch) {
      const exactTouched = journal.exactTouchedSinceAny(exactFineRoutingKeys, sinceGen);
      if (exactTouched === true) {
        const latencyMs = Date.now() - waitStartMs;
        touchManager.recordWaitMetrics({ stream, touchCfg, keysCount: waitKeyIds.length, outcome: "touched", latencyMs });
        return respond.json(200, {
          touched: true,
          cursor: journal.getCursor(),
          effectiveWaitKind,
          bucketMaxSourceOffsetSeq: journal.getLastFlushedSourceOffsetSeq().toString(),
          flushAtMs: journal.getLastFlushAtMs(),
          bucketStartMs: journal.getLastBucketStartMs(),
        });
      }
      const fallbackTouched =
        exactFallbackKeyIds.length > 0 ? journal.maybeTouchedSinceAny(exactFallbackKeyIds, sinceGen) : false;
      if (fallbackTouched) {
        const latencyMs = Date.now() - waitStartMs;
        touchManager.recordWaitMetrics({ stream, touchCfg, keysCount: waitKeyIds.length, outcome: "touched", latencyMs });
        return respond.json(200, {
          touched: true,
          cursor: journal.getCursor(),
          effectiveWaitKind,
          bucketMaxSourceOffsetSeq: journal.getLastFlushedSourceOffsetSeq().toString(),
          flushAtMs: journal.getLastFlushAtMs(),
          bucketStartMs: journal.getLastBucketStartMs(),
        });
      }
      if (exactTouched === false) {
        // Exact recent history covers this cursor range and saw none of the watched keys.
      } else if (journal.maybeTouchedSinceAny(waitKeyIds, sinceGen)) {
        const latencyMs = Date.now() - waitStartMs;
        touchManager.recordWaitMetrics({ stream, touchCfg, keysCount: waitKeyIds.length, outcome: "touched", latencyMs });
        return respond.json(200, {
          touched: true,
          cursor: journal.getCursor(),
          effectiveWaitKind,
          bucketMaxSourceOffsetSeq: journal.getLastFlushedSourceOffsetSeq().toString(),
          flushAtMs: journal.getLastFlushAtMs(),
          bucketStartMs: journal.getLastBucketStartMs(),
        });
      }
    } else if (journal.maybeTouchedSinceAny(waitKeyIds, sinceGen)) {
      const latencyMs = Date.now() - waitStartMs;
      touchManager.recordWaitMetrics({ stream, touchCfg, keysCount: waitKeyIds.length, outcome: "touched", latencyMs });
      return respond.json(200, {
        touched: true,
        cursor: journal.getCursor(),
        effectiveWaitKind,
        bucketMaxSourceOffsetSeq: journal.getLastFlushedSourceOffsetSeq().toString(),
        flushAtMs: journal.getLastFlushAtMs(),
        bucketStartMs: journal.getLastBucketStartMs(),
      });
    }

    const deadline = Date.now() + timeoutMs;
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      const latencyMs = Date.now() - waitStartMs;
      touchManager.recordWaitMetrics({ stream, touchCfg, keysCount: waitKeyIds.length, outcome: "timeout", latencyMs });
      return respond.json(200, {
        touched: false,
        cursor: journal.getCursor(),
        effectiveWaitKind,
        bucketMaxSourceOffsetSeq: journal.getLastFlushedSourceOffsetSeq().toString(),
        flushAtMs: journal.getLastFlushAtMs(),
        bucketStartMs: journal.getLastBucketStartMs(),
      });
    }

    const afterGen = journal.getGeneration();
    const hit = await journal.waitForAny({
      keys: useExactFineKeyMatch ? exactFallbackKeyIds : waitKeyIds,
      exactKeys: useExactFineKeyMatch ? exactFineRoutingKeys : null,
      afterGeneration: afterGen,
      timeoutMs: remaining,
      signal: req.signal,
    });
    if (req.signal.aborted) return new Response(null, { status: 204 });

    if (hit == null) {
      const latencyMs = Date.now() - waitStartMs;
      touchManager.recordWaitMetrics({ stream, touchCfg, keysCount: waitKeyIds.length, outcome: "timeout", latencyMs });
      return respond.json(200, {
        touched: false,
        cursor: journal.getCursor(),
        effectiveWaitKind,
        bucketMaxSourceOffsetSeq: journal.getLastFlushedSourceOffsetSeq().toString(),
        flushAtMs: journal.getLastFlushAtMs(),
        bucketStartMs: journal.getLastBucketStartMs(),
      });
    }

    const latencyMs = Date.now() - waitStartMs;
    touchManager.recordWaitMetrics({ stream, touchCfg, keysCount: waitKeyIds.length, outcome: "touched", latencyMs });
    return respond.json(200, {
      touched: true,
      cursor: journal.getCursor(),
      effectiveWaitKind,
      bucketMaxSourceOffsetSeq: hit.bucketMaxSourceOffsetSeq.toString(),
      flushAtMs: hit.flushAtMs,
      bucketStartMs: hit.bucketStartMs,
    });
  } finally {
    releaseHotInterest();
  }
}

export async function handleStateProtocolTouchRoute(args: StreamTouchRouteArgs): Promise<Response> {
  const { route, profile, respond } = args;
  const touchCfg = getStateProtocolTouchConfig(profile);
  if (!touchCfg) return respond.notFound("touch not enabled");
  if (route.kind === "templates_activate") return handleTemplatesActivateRoute(args, touchCfg);
  if (route.kind === "meta") return handleMetaRoute(args, touchCfg);
  return handleWaitRoute(args, touchCfg);
}
