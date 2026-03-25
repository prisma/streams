/**
 * Live load test (Test 4): selective fine shedding under high write load.
 *
 * Roles:
 * - setup: create stream, configure state-protocol profile, activate templates, write config JSON
 * - writer: generate high-rate noise writes + targeted writes and marker events
 * - appkit: emulate AppKit Policy B admission (fine/coarse mode switching)
 * - metrics: poll /touch/meta monotonic counters and evaluate phase deltas
 * - all: run setup then writer+appkit+metrics together in one process
 */

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { offsetToSeqOrNeg1, parseOffsetResult } from "../../../src/offset";
import { tableKeyFor, templateIdFor, templateKeyFor, watchKeyFor } from "../../../src/touch/live_keys";
import { touchKeyIdFromRoutingKey } from "../../../src/touch/touch_key_id";
import { Result } from "better-result";
import { dsError } from "../../../src/util/ds_error.ts";
import {
  activateTemplatesChunked,
  deleteStream,
  ensureSchemaAndProfile,
  ensureStream,
  fetchJson,
  getTouchMeta,
  hasFlag,
  intArg,
  isMemoryTouchMeta,
  parseArgs,
  sleep,
  stringArg,
  waitForTouchReady,
  type ParsedArgs,
  type TemplateDecl,
} from "./common";

const ARGS = parseArgs(process.argv.slice(2));

type Role = "setup" | "writer" | "appkit" | "metrics" | "all";

type TemplateSpec = {
  index: number;
  templateId: string;
  fields: [string, string, string];
  templateKey: string;
  templateKeyId: number;
  hot: boolean;
};

type TargetedKeySpec = {
  templateId: string;
  templateIndex: number;
  fields: [string, string, string];
  args: [number, number, number];
  key: string;
  keyId: number;
};

type Test4Config = {
  stream: string;
  entity: string;
  dimensions: string[];
  templates: TemplateSpec[];
  hotTemplateIds: string[];
  coldTemplateIds: string[];
  tableKey: string;
  tableKeyId: number;
  targeted: TargetedKeySpec[];
  createdAtIso: string;
};

type MetaSample = {
  tsMs: number;
  lagSourceOffsets: number;
  hotTemplates: number;
  hotFineKeys: number;
  touchMode: string;
  processedThroughDeltaTotal: number;
  touchesEmittedTotal: number;
  touchesTemplateTotal: number;
  journalNotifyWakeupsTotal: number;
  journalNotifyWakeMsTotal: number;
  journalNotifyWakeMsMax: number;
  journalFlushesTotal: number;
  lastFlushAtMs: number;
  flushIntervalMsMaxLast10s: number;
  flushIntervalMsP95Last10s: number;
};

type AppkitSummary = {
  durationMs: number;
  subscriptionsTotal: number;
  subscriptionsFine: number;
  subscriptionsCoarse: number;
  touched: number;
  timeout: number;
  stale: number;
  errors: number;
  transitionsFineToCoarse: number;
  transitionsCoarseToFine: number;
  markersReceived: number;
  markersIgnoredUnwatched: number;
  markersMatched: number;
  markersDroppedExpired: number;
  markersOutstandingEnd: number;
  markerSuccessWithin1s: number;
  targetedLatencyCount: number;
  targetedLatencyP50Ms: number;
  targetedLatencyP95Ms: number;
  targetedLatencyP99Ms: number;
  markerToFlushLatencyP50Ms: number;
  markerToFlushLatencyP95Ms: number;
  flushToClientLatencyP50Ms: number;
  flushToClientLatencyP95Ms: number;
  targetedLatencyCountA: number;
  targetedLatencyCountB: number;
  targetedLatencyCountC: number;
  targetedLatencyP95AMs: number;
  targetedLatencyP95BMs: number;
  targetedLatencyP95CMs: number;
  markerToFlushLatencyP95BMs: number;
  flushToClientLatencyP95BMs: number;
};

type WriterSummary = {
  durationMs: number;
  sentEvents: number;
  sentNoiseEvents: number;
  sentTargetedEvents: number;
  requests: number;
  httpErrors: number;
  markerPostErrors: number;
};

type MetricsSummary = {
  durationMs: number;
  metaSamples: number;
  tickSamples: number;
  lagMax: number;
  lagEnd: number;
  hotTemplatesEnd: number;
  hotFineKeysEnd: number;
  hotTemplatesBMin: number;
  hotTemplatesBMax: number;
  fineRateA: number;
  fineRateB: number;
  fineRateC: number;
  processedDeltaRateA: number;
  processedDeltaRateB: number;
  processedDeltaRateC: number;
  touchesDeltaRateA: number;
  touchesDeltaRateB: number;
  touchesDeltaRateC: number;
  notifyWakeupsRateA: number;
  notifyWakeupsRateB: number;
  notifyWakeupsRateC: number;
  flushesRateA: number;
  flushesRateB: number;
  flushesRateC: number;
  flushIntervalP95A: number;
  flushIntervalP95B: number;
  flushIntervalP95C: number;
  notifyWakeMsMaxA: number;
  notifyWakeMsMaxB: number;
  notifyWakeMsMaxC: number;
  lagBStart: number;
  lagBEnd: number;
  lagBSlopePerSec: number;
};

type AllSummary = {
  config: Test4Config;
  writer: WriterSummary;
  appkit: AppkitSummary;
  metrics: MetricsSummary;
  pass: {
    lagBoundedInB: boolean;
    hotTemplatesCollapsed: boolean;
    fineRateDropped: boolean;
    targetedLatencyRecovered: boolean;
    noErrors: boolean;
  };
};

function usage(exitCode = 0): never {
  // eslint-disable-next-line no-console
  console.log(`
Selective Fine Shedding Load Test (Test 4)

Usage:
  bun run experiments/loadtests/live/selective_shedding.ts --role <setup|writer|appkit|metrics|all> [options]

Shared options:
  --url <baseUrl>                        (default: http://127.0.0.1:8080)
  --stream <name>                        (default: load.live.shedding)
  --config <path>                        (default: ./tmp/test4_config.json)
  --duration-seconds <n>                 (default: 110)

Setup options:
  --templates <n>                        (default: 128)
  --hot-templates <n>                    (default: 16)
  --ttl-ms <n>                           (default: 3600000)
  --reset                                Delete stream before setup
  --lag-degrade-offsets <n>              (default: 5000)
  --lag-recover-offsets <n>              (default: 1000)
  --fine-budget-per-batch <n>            (default: 2000)
  --fine-tokens-per-second <n>           (default: 200000)
  --fine-burst <n>                       (default: 400000)
  --coarse-interval-ms <n>               (default: 100)
  --coalesce-window-ms <n>               (default: 100)
  --hot-key-ttl-ms <n>                   (default: 2000)
  --hot-template-ttl-ms <n>              (default: 2000)

Writer options:
  --noise-rate <events/sec>              (default: 10000)
  --targeted-rate <events/sec>           (default: 40)
  --targeted-per-template <n>            (default: 8)
  --writer-tick-ms <n>                   (default: 100)
  --batch-events <n>                     (default: 250)
  --targeted-listen-port <n>             (default: 9091)
  --start-barrier-fine-waiters <n>       (default: 1)
  --start-barrier-hot-templates <n>      (default: 1)
  --start-barrier-timeout-ms <n>         (default: 30000)

AppKit emulator options:
  --subscriptions-hot-per-template <n>   (default: 64)
  --subscriptions-cold-per-template <n>  (default: 1)
  --wait-timeout-ms <n>                  (default: 30000)
  --controller-interval-ms <n>           (default: 250)
  --phase-a-seconds <n>                  (default: 30)
  --phase-b-seconds <n>                  (default: 60)
  --phase-c-seconds <n>                  (default: 20)
  --admit-a <n>                          (default: 128)
  --admit-b <n>                          (default: 16)
  --admit-c <n>                          (default: 128)
  --targeted-listen-port <n>             (default: 9091)
  --marker-expiry-ms <n>                 (default: 5000)

Metrics options:
  --poll-ms <n>                          (default: 1000)

Notes:
  - Use --role all for a single-process deterministic run.
  - For split-mode, run setup once, then writer/appkit/metrics concurrently.
`);
  process.exit(exitCode);
}

function parseRole(args: ParsedArgs): Role {
  const raw = stringArg(args, "role", "all").trim();
  if (raw === "setup" || raw === "writer" || raw === "appkit" || raw === "metrics" || raw === "all") return raw;
  throw dsError(`invalid --role: ${raw}`);
}

function clamp(n: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, n));
}

function percentile(values: number[], q: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.max(0, Math.floor((sorted.length - 1) * q)));
  return sorted[idx] ?? 0;
}

function parseConfigPath(args: ParsedArgs): string {
  return stringArg(args, "config", "./tmp/test4_config.json");
}

function writeJson(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

function readConfig(path: string): Test4Config {
  const raw = readFileSync(path, "utf8");
  return JSON.parse(raw) as Test4Config;
}

function combos3(items: string[]): [string, string, string][] {
  const out: [string, string, string][] = [];
  for (let i = 0; i < items.length; i++) {
    for (let j = i + 1; j < items.length; j++) {
      for (let k = j + 1; k < items.length; k++) {
        out.push([items[i]!, items[j]!, items[k]!]);
      }
    }
  }
  return out;
}

function argsFor(templateIndex: number, subIndex: number): [number, number, number] {
  const seed = templateIndex * 131 + subIndex * 17 + 7;
  return [
    (seed * 3 + 11) % 10_000,
    (seed * 7 + 23) % 10_000,
    (seed * 13 + 37) % 10_000,
  ];
}

function makeTemplateDecl(entity: string, fields: [string, string, string]): TemplateDecl {
  return {
    entity,
    fields: fields.map((name) => ({ name, encoding: "int64" as const })),
  };
}

function makeFineKey(templateId: string, args: [number, number, number]): string {
  return watchKeyFor(templateId, args.map((x) => String(x)));
}

function makeDimsRandom(rng: () => number, dims: string[]): Record<string, number> {
  const out: Record<string, number> = {};
  for (const d of dims) out[d] = Math.floor(rng() * 1_000_000);
  return out;
}

function makeEvent(args: {
  entity: string;
  id: number;
  op: "insert" | "update" | "delete";
  valueDims: Record<string, number>;
  oldDims: Record<string, number>;
  txid: bigint;
}): any {
  const value = { id: String(args.id), ...args.valueDims };
  const oldValue = { id: String(args.id), ...args.oldDims };
  if (args.op === "insert") {
    return {
      type: args.entity,
      key: String(args.id),
      value,
      headers: {
        operation: "insert",
        txid: args.txid.toString(),
        timestamp: new Date().toISOString(),
      },
    };
  }
  if (args.op === "delete") {
    return {
      type: args.entity,
      key: String(args.id),
      oldValue,
      headers: {
        operation: "delete",
        txid: args.txid.toString(),
        timestamp: new Date().toISOString(),
      },
    };
  }
  return {
    type: args.entity,
    key: String(args.id),
    value,
    oldValue,
    headers: {
      operation: "update",
      txid: args.txid.toString(),
      timestamp: new Date().toISOString(),
    },
  };
}

function seqOfOffset(offset: string): bigint | null {
  const parsed = parseOffsetResult(offset);
  if (Result.isError(parsed)) return null;
  return offsetToSeqOrNeg1(parsed.value);
}

async function postEvents(args: { baseUrl: string; stream: string; events: any[] }): Promise<Result<{ lastOffset: string | null }, { kind: "http_error" }>> {
  if (args.events.length === 0) return Result.ok({ lastOffset: null });
  const r = await fetch(`${args.baseUrl}/v1/stream/${encodeURIComponent(args.stream)}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(args.events),
  });
  if (r.ok) {
    return Result.ok({ lastOffset: r.headers.get("stream-next-offset") });
  }
  return Result.err({ kind: "http_error" });
}

async function runSetup(args: ParsedArgs): Promise<Test4Config> {
  const baseUrl = stringArg(args, "url", "http://127.0.0.1:8080");
  const stream = stringArg(args, "stream", "load.live.shedding");
  const configPath = parseConfigPath(args);
  const templatesN = clamp(intArg(args, "templates", 128), 1, 560);
  const hotTemplatesN = clamp(intArg(args, "hot-templates", 16), 0, templatesN);
  const targetedPerTemplate = clamp(intArg(args, "targeted-per-template", 8), 1, 128);
  const ttlMs = intArg(args, "ttl-ms", 60 * 60 * 1000);

  const dims = Array.from({ length: 16 }, (_, i) => `d${i}`);
  const entity = "public.items";

  if (hasFlag(args, "reset")) {
    await deleteStream(baseUrl, stream).catch(() => {});
  }

  await ensureStream(baseUrl, stream, "application/json");

  const profile = {
    kind: "state-protocol",
    touch: {
      enabled: true,
      coarseIntervalMs: intArg(args, "coarse-interval-ms", 100),
      touchCoalesceWindowMs: intArg(args, "coalesce-window-ms", 100),
      lagDegradeFineTouchesAtSourceOffsets: intArg(args, "lag-degrade-offsets", 5000),
      lagRecoverFineTouchesAtSourceOffsets: intArg(args, "lag-recover-offsets", 1000),
      fineTouchBudgetPerBatch: intArg(args, "fine-budget-per-batch", 2000),
      fineTokensPerSecond: intArg(args, "fine-tokens-per-second", 200000),
      fineBurstTokens: intArg(args, "fine-burst", 400000),
      onMissingBefore: "coarse",
      memory: {
        bucketMs: 100,
        filterPow2: 22,
        k: 4,
        pendingMaxKeys: 100_000,
        keyIndexMaxKeys: 32,
        hotKeyTtlMs: intArg(args, "hot-key-ttl-ms", 2000),
        hotTemplateTtlMs: intArg(args, "hot-template-ttl-ms", 2000),
        hotMaxKeys: 1_000_000,
        hotMaxTemplates: 4096,
      },
      templates: {
        defaultInactivityTtlMs: ttlMs,
        lastSeenPersistIntervalMs: 30_000,
        gcIntervalMs: 10_000,
        maxActiveTemplatesPerEntity: 10_000,
        maxActiveTemplatesPerStream: 10_000,
        activationRateLimitPerMinute: 0,
      },
    },
  };

  await ensureSchemaAndProfile(baseUrl, stream, profile, { type: "object", additionalProperties: true });
  await waitForTouchReady(baseUrl, stream, 30_000);

  const allTriplets = combos3(dims);
  const selectedTriplets = allTriplets.slice(0, templatesN);
  const templateDecls = selectedTriplets.map((fields) => makeTemplateDecl(entity, fields));

  const activation = await activateTemplatesChunked({
    baseUrl,
    stream,
    templates: templateDecls,
    inactivityTtlMs: ttlMs,
    chunkSize: 128,
  });

  const templateSpecs: TemplateSpec[] = selectedTriplets.map((fields, idx) => {
    const templateId = templateIdFor(entity, [...fields]);
    const templateKey = templateKeyFor(templateId);
    return {
      index: idx,
      templateId,
      fields,
      templateKey,
      templateKeyId: touchKeyIdFromRoutingKey(templateKey),
      hot: idx < hotTemplatesN,
    };
  });

  const hotTemplateIds = templateSpecs.filter((t) => t.hot).map((t) => t.templateId);
  const coldTemplateIds = templateSpecs.filter((t) => !t.hot).map((t) => t.templateId);

  const targeted: TargetedKeySpec[] = [];
  const targetedTemplateCount = Math.min(4, hotTemplatesN);
  for (let ti = 0; ti < targetedTemplateCount; ti++) {
    const tpl = templateSpecs[ti]!;
    for (let si = 0; si < targetedPerTemplate; si++) {
      const args3 = argsFor(tpl.index, si);
      const key = makeFineKey(tpl.templateId, args3);
      targeted.push({
        templateId: tpl.templateId,
        templateIndex: tpl.index,
        fields: tpl.fields,
        args: args3,
        key,
        keyId: touchKeyIdFromRoutingKey(key),
      });
    }
  }

  const tableKey = tableKeyFor(entity);
  const out: Test4Config = {
    stream,
    entity,
    dimensions: dims,
    templates: templateSpecs,
    hotTemplateIds,
    coldTemplateIds,
    tableKey,
    tableKeyId: touchKeyIdFromRoutingKey(tableKey),
    targeted,
    createdAtIso: new Date().toISOString(),
  };

  writeJson(configPath, out);

  // eslint-disable-next-line no-console
  console.log(
    `[test4][setup] stream=${stream} templates=${templatesN} hotTemplates=${hotTemplatesN} activated=${activation.activated.length} denied=${activation.denied.length} config=${configPath}`
  );

  return out;
}

async function runWriter(args: ParsedArgs, cfg: Test4Config): Promise<WriterSummary> {
  const baseUrl = stringArg(args, "url", "http://127.0.0.1:8080");
  const durationSeconds = intArg(args, "duration-seconds", 110);
  const noiseRate = Math.max(0, intArg(args, "noise-rate", 10_000));
  const targetedRate = Math.max(0, intArg(args, "targeted-rate", 40));
  const tickMs = Math.max(10, intArg(args, "writer-tick-ms", 100));
  const batchEvents = Math.max(1, intArg(args, "batch-events", 250));
  const targetedPort = intArg(args, "targeted-listen-port", 9091);
  const startBarrierFineWaiters = Math.max(0, intArg(args, "start-barrier-fine-waiters", 1));
  const startBarrierHotTemplates = Math.max(0, intArg(args, "start-barrier-hot-templates", 1));
  const startBarrierTimeoutMs = Math.max(1000, intArg(args, "start-barrier-timeout-ms", 30_000));

  const tplById = new Map(cfg.templates.map((t) => [t.templateId, t]));

  if (startBarrierFineWaiters > 0 || startBarrierHotTemplates > 0) {
    const barrierStartMs = Date.now();
    for (;;) {
      try {
        const meta = await getTouchMeta(baseUrl, cfg.stream);
        if (isMemoryTouchMeta(meta)) {
          const fineWaiters = Number(meta.fineWaitersActive ?? 0);
          const hotTemplates = Number(meta.hotTemplatesActive ?? meta.hotTemplates ?? 0);
          if (fineWaiters >= startBarrierFineWaiters && hotTemplates >= startBarrierHotTemplates) break;
        }
      } catch {
        // keep polling
      }
      if (Date.now() - barrierStartMs >= startBarrierTimeoutMs) break;
      await sleep(100);
    }
  }

  const startedAt = Date.now();
  const deadline = startedAt + durationSeconds * 1000;
  let sentEvents = 0;
  let sentNoiseEvents = 0;
  let sentTargetedEvents = 0;
  let requests = 0;
  let httpErrors = 0;
  let markerPostErrors = 0;

  let nextTxid = 1n;
  let nextItemId = 1;

  let noiseCarry = 0;
  let nextTargetAt = startedAt;
  let targetIdx = 0;

  const rand = Math.random;

  while (Date.now() < deadline) {
    const tickStart = Date.now();
    const events: any[] = [];

    noiseCarry += (noiseRate * tickMs) / 1000;
    const noiseN = Math.floor(noiseCarry);
    noiseCarry -= noiseN;

    for (let i = 0; i < noiseN; i++) {
      const valueDims = makeDimsRandom(rand, cfg.dimensions);
      const oldDims = makeDimsRandom(rand, cfg.dimensions);
      const p = rand();
      const op: "insert" | "update" | "delete" = p < 0.1 ? "insert" : p < 0.9 ? "update" : "delete";
      events.push(
        makeEvent({
          entity: cfg.entity,
          id: nextItemId++,
          op,
          valueDims,
          oldDims,
          txid: nextTxid++,
        })
      );
      sentNoiseEvents++;
    }

    while (targetedRate > 0 && nextTargetAt <= tickStart) {
      const target = cfg.targeted[targetIdx % cfg.targeted.length]!;
      targetIdx++;
      nextTargetAt += Math.floor(1000 / targetedRate);

      const valueDims = makeDimsRandom(rand, cfg.dimensions);
      const oldDims = makeDimsRandom(rand, cfg.dimensions);
      const tpl = tplById.get(target.templateId);
      if (tpl) {
        valueDims[tpl.fields[0]] = target.args[0];
        valueDims[tpl.fields[1]] = target.args[1];
        valueDims[tpl.fields[2]] = target.args[2];
        oldDims[tpl.fields[0]] = target.args[0];
        oldDims[tpl.fields[1]] = target.args[1];
        oldDims[tpl.fields[2]] = target.args[2];
      }

      const event = makeEvent({
        entity: cfg.entity,
        id: nextItemId++,
        op: "update",
        valueDims,
        oldDims,
        txid: nextTxid++,
      });
      const postRes = await postEvents({ baseUrl, stream: cfg.stream, events: [event] });
      requests++;
      if (Result.isError(postRes)) {
        httpErrors++;
      } else {
        sentEvents += 1;
        sentTargetedEvents += 1;
        const sourceOffset = postRes.value.lastOffset;
        const sourceOffsetSeq = sourceOffset ? seqOfOffset(sourceOffset) : null;
        const markerBody = JSON.stringify({
          fineKey: target.key,
          keyId: target.keyId,
          templateId: target.templateId,
          sentAtMs: Date.now(),
          seq: targetIdx,
          ...(sourceOffset ? { sourceOffset } : {}),
          ...(sourceOffsetSeq != null ? { sourceOffsetSeq: sourceOffsetSeq.toString() } : {}),
        });
        void fetch(`http://127.0.0.1:${targetedPort}/targeted`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: markerBody,
        }).catch(() => {
          markerPostErrors++;
        });
      }
    }

    for (let i = 0; i < events.length; i += batchEvents) {
      const chunk = events.slice(i, i + batchEvents);
      if (chunk.length === 0) continue;
      const res = await postEvents({ baseUrl, stream: cfg.stream, events: chunk });
      requests++;
      if (Result.isError(res)) {
        httpErrors++;
      } else {
        sentEvents += chunk.length;
      }
    }

    const sleepMs = tickMs - (Date.now() - tickStart);
    if (sleepMs > 0) await sleep(sleepMs);
  }

  const summary: WriterSummary = {
    durationMs: Date.now() - startedAt,
    sentEvents,
    sentNoiseEvents,
    sentTargetedEvents,
    requests,
    httpErrors,
    markerPostErrors,
  };

  // eslint-disable-next-line no-console
  console.log(
    `[test4][writer] done sent=${summary.sentEvents} noise=${summary.sentNoiseEvents} targeted=${summary.sentTargetedEvents} req=${summary.requests} httpErrors=${summary.httpErrors} markerPostErrors=${summary.markerPostErrors}`
  );

  return summary;
}

type Subscription = {
  id: string;
  templateId: string;
  templateIndex: number;
  fineKey: string;
  fineKeyId: number;
  mode: "fine" | "coarse";
  cursor: string;
  inFlightAbort: AbortController | null;
};

async function runAppkit(args: ParsedArgs, cfg: Test4Config): Promise<AppkitSummary> {
  const baseUrl = stringArg(args, "url", "http://127.0.0.1:8080");
  const durationSeconds = intArg(args, "duration-seconds", 110);
  const waitTimeoutMs = intArg(args, "wait-timeout-ms", 30_000);
  const controllerIntervalMs = intArg(args, "controller-interval-ms", 250);
  const phaseASeconds = intArg(args, "phase-a-seconds", 30);
  const phaseBSeconds = intArg(args, "phase-b-seconds", 60);
  const phaseCSeconds = intArg(args, "phase-c-seconds", 20);
  const admitA = Math.max(0, intArg(args, "admit-a", 128));
  const admitB = Math.max(0, intArg(args, "admit-b", 16));
  const admitC = Math.max(0, intArg(args, "admit-c", 128));
  const hotPerTemplate = Math.max(1, intArg(args, "subscriptions-hot-per-template", 64));
  const coldPerTemplate = Math.max(1, intArg(args, "subscriptions-cold-per-template", 1));
  const targetedPort = intArg(args, "targeted-listen-port", 9091);
  const markerExpiryMs = Math.max(250, intArg(args, "marker-expiry-ms", 5000));


  const subs: Subscription[] = [];

  for (const t of cfg.templates) {
    const perTemplate = t.hot ? hotPerTemplate : coldPerTemplate;
    for (let i = 0; i < perTemplate; i++) {
      const a = argsFor(t.index, i);
      const fineKey = makeFineKey(t.templateId, a);
      const fineKeyId = touchKeyIdFromRoutingKey(fineKey);
      subs.push({
        id: `${t.templateId}:${i}`,
        templateId: t.templateId,
        templateIndex: t.index,
        fineKey,
        fineKeyId,
        mode: "fine",
        cursor: "now",
        inFlightAbort: null,
      });
    }
  }

  let transitionsFineToCoarse = 0;
  let transitionsCoarseToFine = 0;
  let touched = 0;
  let timeout = 0;
  let stale = 0;
  let errors = 0;
  let markersReceived = 0;
  let markersIgnoredUnwatched = 0;
  let markersMatched = 0;
  let markersDroppedExpired = 0;
  let markerSuccessWithin1s = 0;
  const targetedLatencies: number[] = [];
  const targetedLatenciesA: number[] = [];
  const targetedLatenciesB: number[] = [];
  const targetedLatenciesC: number[] = [];
  const markerToFlushLatencies: number[] = [];
  const markerToFlushLatenciesB: number[] = [];
  const flushToClientLatencies: number[] = [];
  const flushToClientLatenciesB: number[] = [];
  const watchedFineKeys = new Set(subs.map((s) => s.fineKey));
  const markersByFineKey = new Map<string, Array<{ sentAtMs: number; sourceSeq: bigint | null }>>();
  const pruneMarkers = (nowMs: number) => {
    for (const [fineKey, q] of markersByFineKey.entries()) {
      while (q.length > 0 && nowMs - q[0]!.sentAtMs > markerExpiryMs) {
        q.shift();
        markersDroppedExpired++;
      }
      if (q.length === 0) markersByFineKey.delete(fineKey);
    }
  };
  const markersOutstanding = () => {
    let total = 0;
    for (const q of markersByFineKey.values()) total += q.length;
    return total;
  };

  const markerServer = Bun.serve({
    port: targetedPort,
    fetch: async (req) => {
      const url = new URL(req.url);
      if (req.method !== "POST" || url.pathname !== "/targeted") return new Response("not found", { status: 404 });
      try {
        const body = (await req.json()) as { fineKey?: string; templateId?: string; sentAtMs?: number; sourceOffset?: string; sourceOffsetSeq?: string };
        const fineKey = typeof body?.fineKey === "string" ? body.fineKey.trim().toLowerCase() : "";
        const templateId = typeof body?.templateId === "string" ? body.templateId : "";
        const sentAtMs = Number(body?.sentAtMs);
        if (/^[0-9a-f]{16}$/.test(templateId) && /^[0-9a-f]{16}$/.test(fineKey) && Number.isFinite(sentAtMs)) {
          if (!watchedFineKeys.has(fineKey)) {
            markersIgnoredUnwatched++;
            return new Response("ignored", { status: 200 });
          }
          let sourceSeq: bigint | null = null;
          if (typeof body?.sourceOffsetSeq === "string" && body.sourceOffsetSeq.trim() !== "") {
            try {
              sourceSeq = BigInt(body.sourceOffsetSeq.trim());
            } catch {
              sourceSeq = null;
            }
          } else if (typeof body?.sourceOffset === "string" && body.sourceOffset.trim() !== "") {
            sourceSeq = seqOfOffset(body.sourceOffset.trim());
          }
          pruneMarkers(Date.now());
          const q = markersByFineKey.get(fineKey) ?? [];
          q.push({ sentAtMs, sourceSeq });
          if (q.length > 10_000) q.splice(0, q.length - 10_000);
          markersByFineKey.set(fineKey, q);
          markersReceived++;
        }
        return new Response("ok", { status: 200 });
      } catch {
        return new Response("bad", { status: 400 });
      }
    },
  });

  const allTemplateIdsSorted = [...cfg.templates].sort((a, b) => a.index - b.index).map((t) => t.templateId);

  const startedAt = Date.now();
  const deadline = startedAt + durationSeconds * 1000;
  const phaseAEndMs = startedAt + phaseASeconds * 1000;
  const phaseBEndMs = phaseAEndMs + phaseBSeconds * 1000;

  let admitted = new Set<string>();
  const updateAdmission = () => {
    const now = Date.now();
    const elapsedSec = (now - startedAt) / 1000;
    let k = admitA;
    if (elapsedSec >= phaseASeconds + phaseBSeconds) k = admitC;
    else if (elapsedSec >= phaseASeconds) k = admitB;
    const next = new Set<string>(allTemplateIdsSorted.slice(0, Math.min(k, allTemplateIdsSorted.length)));
    admitted = next;
    pruneMarkers(now);

    for (const s of subs) {
      const desired: "fine" | "coarse" = admitted.has(s.templateId) ? "fine" : "coarse";
      if (s.mode !== desired) {
        if (s.mode === "fine" && desired === "coarse") transitionsFineToCoarse++;
        if (s.mode === "coarse" && desired === "fine") transitionsCoarseToFine++;
        s.mode = desired;
        if (s.inFlightAbort) s.inFlightAbort.abort();
      }
    }
  };

  updateAdmission();
  const controllerTimer = setInterval(updateAdmission, controllerIntervalMs);

  const runOne = async (s: Subscription): Promise<void> => {
    const issueWait = () => {
      const modeAtRequest = s.mode;
      const keyId = modeAtRequest === "fine" ? s.fineKeyId : cfg.tableKeyId;
      const key = modeAtRequest === "fine" ? s.fineKey : cfg.tableKey;
      const interestMode: "fine" | "coarse" = modeAtRequest === "fine" ? "fine" : "coarse";
      const templateIdsUsed = interestMode === "fine" ? [s.templateId] : [];
      const abort = new AbortController();
      s.inFlightAbort = abort;
      const promise = fetchJson(`${baseUrl}/v1/stream/${encodeURIComponent(cfg.stream)}/touch/wait`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          signal: abort.signal,
          body: JSON.stringify({
            cursor: s.cursor,
            timeoutMs: waitTimeoutMs,
            keyIds: [keyId],
            keys: [key],
            interestMode,
            ...(templateIdsUsed.length > 0 ? { templateIdsUsed } : {}),
          }),
        }).finally(() => {
          if (s.inFlightAbort === abort) s.inFlightAbort = null;
        });
      return { modeAtRequest, interestMode, abort, promise };
    };

    let current = issueWait();
    while (Date.now() < deadline) {
      let res: any;
      try {
        res = await current.promise;
      } catch (err) {
        if (current.abort.signal.aborted || (err && typeof err === "object" && (err as any).name === "AbortError")) {
          if (Date.now() >= deadline) break;
          current = issueWait();
          continue;
        }
        errors++;
        await sleep(50);
        if (Date.now() >= deadline) break;
        current = issueWait();
        continue;
      }

      const modeAtRequest = current.modeAtRequest;
      const interestMode = current.interestMode;

      if (res?.stale === true) {
        stale++;
        s.cursor = typeof res?.cursor === "string" ? res.cursor : "now";
        if (Date.now() >= deadline) break;
        current = issueWait();
        continue;
      }
      if (typeof res?.cursor === "string" && res.cursor.trim() !== "") s.cursor = res.cursor;

      // Keep wait coverage continuous: queue the next wait before processing markers.
      if (Date.now() >= deadline) {
        // no-op
      } else {
        current = issueWait();
      }

      if (res?.touched === true) touched++;
      else {
        timeout++;
      }

      if (res?.touched === true && modeAtRequest === "fine" && interestMode === "fine" && res?.effectiveWaitKind === "fineKey") {
        const q = markersByFineKey.get(s.fineKey);
        if (q && q.length > 0) {
          const nowMs = Date.now();
          while (q.length > 0 && nowMs - q[0]!.sentAtMs > markerExpiryMs) {
            q.shift();
            markersDroppedExpired++;
          }
          if (q.length > 0) {
            let bucketSeq: bigint | null = null;
            if (typeof res?.bucketMaxSourceOffsetSeq === "string" && res.bucketMaxSourceOffsetSeq.trim() !== "") {
              try {
                bucketSeq = BigInt(res.bucketMaxSourceOffsetSeq.trim());
              } catch {
                bucketSeq = null;
              }
            }
            while (q.length > 0) {
              const head = q[0]!;
              if (bucketSeq == null || head.sourceSeq == null || head.sourceSeq <= bucketSeq) {
                q.shift();
                const flushAtRaw = Number(res?.flushAtMs);
                const flushAtMs = Number.isFinite(flushAtRaw) && flushAtRaw > 0 ? flushAtRaw : nowMs;
                const latency = Math.max(0, nowMs - head.sentAtMs);
                const markerToFlushLatency = Math.max(0, flushAtMs - head.sentAtMs);
                const flushToClientLatency = Math.max(0, nowMs - flushAtMs);
                targetedLatencies.push(latency);
                markerToFlushLatencies.push(markerToFlushLatency);
                flushToClientLatencies.push(flushToClientLatency);
                if (nowMs < phaseAEndMs) targetedLatenciesA.push(latency);
                else if (nowMs < phaseBEndMs) {
                  targetedLatenciesB.push(latency);
                  markerToFlushLatenciesB.push(markerToFlushLatency);
                  flushToClientLatenciesB.push(flushToClientLatency);
                } else targetedLatenciesC.push(latency);
                markersMatched++;
                if (latency <= 1000) markerSuccessWithin1s++;
                continue;
              }
              break;
            }
          }
          if (q.length === 0) markersByFineKey.delete(s.fineKey);
        }
      }

      if (Date.now() >= deadline) break;
    }
  };

  const workers = subs.map((s) => runOne(s));
  await Promise.all(workers);

  clearInterval(controllerTimer);
  for (const s of subs) {
    if (s.inFlightAbort) s.inFlightAbort.abort();
  }
  pruneMarkers(Date.now());
  markerServer.stop();

  const summary: AppkitSummary = {
    durationMs: Date.now() - startedAt,
    subscriptionsTotal: subs.length,
    subscriptionsFine: subs.filter((s) => s.mode === "fine").length,
    subscriptionsCoarse: subs.filter((s) => s.mode === "coarse").length,
    touched,
    timeout,
    stale,
    errors,
    transitionsFineToCoarse,
    transitionsCoarseToFine,
    markersReceived,
    markersIgnoredUnwatched,
    markersMatched,
    markersDroppedExpired,
    markersOutstandingEnd: markersOutstanding(),
    markerSuccessWithin1s,
    targetedLatencyCount: targetedLatencies.length,
    targetedLatencyP50Ms: percentile(targetedLatencies, 0.5),
    targetedLatencyP95Ms: percentile(targetedLatencies, 0.95),
    targetedLatencyP99Ms: percentile(targetedLatencies, 0.99),
    markerToFlushLatencyP50Ms: percentile(markerToFlushLatencies, 0.5),
    markerToFlushLatencyP95Ms: percentile(markerToFlushLatencies, 0.95),
    flushToClientLatencyP50Ms: percentile(flushToClientLatencies, 0.5),
    flushToClientLatencyP95Ms: percentile(flushToClientLatencies, 0.95),
    targetedLatencyCountA: targetedLatenciesA.length,
    targetedLatencyCountB: targetedLatenciesB.length,
    targetedLatencyCountC: targetedLatenciesC.length,
    targetedLatencyP95AMs: percentile(targetedLatenciesA, 0.95),
    targetedLatencyP95BMs: percentile(targetedLatenciesB, 0.95),
    targetedLatencyP95CMs: percentile(targetedLatenciesC, 0.95),
    markerToFlushLatencyP95BMs: percentile(markerToFlushLatenciesB, 0.95),
    flushToClientLatencyP95BMs: percentile(flushToClientLatenciesB, 0.95),
  };

  // eslint-disable-next-line no-console
  console.log(
    `[test4][appkit] done subs=${summary.subscriptionsTotal} touched=${summary.touched} timeout=${summary.timeout} stale=${summary.stale} errors=${summary.errors} p95=${summary.targetedLatencyP95Ms.toFixed(
      1
    )}ms p95B=${summary.targetedLatencyP95BMs.toFixed(1)}ms markerToFlush.p95B=${summary.markerToFlushLatencyP95BMs.toFixed(
      1
    )}ms flushToClient.p95B=${summary.flushToClientLatencyP95BMs.toFixed(1)}ms matched=${summary.markersMatched}/${summary.markersReceived} ignored=${summary.markersIgnoredUnwatched} dropped=${summary.markersDroppedExpired} outstanding=${summary.markersOutstandingEnd}`
  );

  return summary;
}

function slopePerSecond(samples: Array<{ tMs: number; v: number }>): number {
  if (samples.length < 2) return 0;
  const first = samples[0]!;
  const last = samples[samples.length - 1]!;
  const dtSec = Math.max(1e-6, (last.tMs - first.tMs) / 1000);
  return (last.v - first.v) / dtSec;
}

async function runMetrics(args: ParsedArgs, cfg: Test4Config): Promise<MetricsSummary> {
  const baseUrl = stringArg(args, "url", "http://127.0.0.1:8080");
  const durationSeconds = intArg(args, "duration-seconds", 110);
  const pollMs = Math.max(200, intArg(args, "poll-ms", 1000));
  const phaseASeconds = intArg(args, "phase-a-seconds", 30);
  const phaseBSeconds = intArg(args, "phase-b-seconds", 60);
  const phaseCSeconds = intArg(args, "phase-c-seconds", 20);

  const startedAt = Date.now();
  const deadline = startedAt + durationSeconds * 1000;

  const metaSamples: MetaSample[] = [];

  while (Date.now() < deadline) {
    try {
      const meta = await getTouchMeta(baseUrl, cfg.stream);
      if (isMemoryTouchMeta(meta)) {
        const sample: MetaSample = {
          tsMs: Date.now(),
          lagSourceOffsets: Number(meta.lagSourceOffsets ?? 0),
          hotTemplates: Number(meta.hotTemplates ?? 0),
          hotFineKeys: Number(meta.hotFineKeys ?? 0),
          touchMode: String(meta.touchMode ?? "unknown"),
          processedThroughDeltaTotal: Number(meta.processedThroughDeltaTotal ?? 0),
          touchesEmittedTotal: Number(meta.touchesEmittedTotal ?? 0),
          touchesTemplateTotal: Number(meta.touchesTemplateTotal ?? 0),
          journalNotifyWakeupsTotal: Number(meta.journalNotifyWakeupsTotal ?? 0),
          journalNotifyWakeMsTotal: Number(meta.journalNotifyWakeMsTotal ?? 0),
          journalNotifyWakeMsMax: Number(meta.journalNotifyWakeMsMax ?? 0),
          journalFlushesTotal: Number(meta.journalFlushesTotal ?? 0),
          lastFlushAtMs: Number(meta.lastFlushAtMs ?? 0),
          flushIntervalMsMaxLast10s: Number(meta.flushIntervalMsMaxLast10s ?? 0),
          flushIntervalMsP95Last10s: Number(meta.flushIntervalMsP95Last10s ?? 0),
        };
        metaSamples.push(sample);
        // eslint-disable-next-line no-console
        console.log(
          `[test4][metrics] lag=${sample.lagSourceOffsets} hotTemplates=${sample.hotTemplates} hotFineKeys=${sample.hotFineKeys} mode=${sample.touchMode}`
        );
      }
    } catch {
      // ignore
    }
    await sleep(pollMs);
  }

  const tA0 = startedAt;
  const tA1 = startedAt + phaseASeconds * 1000;
  const tB0 = tA1;
  const tB1 = tB0 + phaseBSeconds * 1000;
  const tC0 = tB1;
  const tC1 = tC0 + phaseCSeconds * 1000;

  const valuesIn = (fromMs: number, toMs: number, selector: (s: MetaSample) => number, lastNSeconds = 0): number[] => {
    let list = metaSamples.filter((s) => s.tsMs >= fromMs && s.tsMs <= toMs);
    if (lastNSeconds > 0) {
      const floor = toMs - lastNSeconds * 1000;
      list = list.filter((s) => s.tsMs >= floor);
    }
    return list.map(selector);
  };

  const lagIn = (fromMs: number, toMs: number, lastNSeconds = 0): Array<{ tMs: number; v: number }> => {
    let list = metaSamples.filter((s) => s.tsMs >= fromMs && s.tsMs <= toMs);
    if (lastNSeconds > 0) {
      const floor = toMs - lastNSeconds * 1000;
      list = list.filter((s) => s.tsMs >= floor);
    }
    return list.map((s) => ({ tMs: s.tsMs, v: s.lagSourceOffsets }));
  };

  const deltaRate = (fromMs: number, toMs: number, selector: (s: MetaSample) => number, lastNSeconds = 0): number => {
    let list = metaSamples.filter((s) => s.tsMs >= fromMs && s.tsMs <= toMs);
    if (lastNSeconds > 0) {
      const floor = toMs - lastNSeconds * 1000;
      list = list.filter((s) => s.tsMs >= floor);
    }
    if (list.length < 2) return 0;
    const first = list[0]!;
    const last = list[list.length - 1]!;
    const dt = Math.max(1e-6, (last.tsMs - first.tsMs) / 1000);
    const delta = selector(last) - selector(first);
    return delta <= 0 ? 0 : delta / dt;
  };

  const lagBSeries = lagIn(tB0, tB1, 30);
  const summary: MetricsSummary = {
    durationMs: Date.now() - startedAt,
    metaSamples: metaSamples.length,
    tickSamples: metaSamples.length,
    lagMax: metaSamples.length > 0 ? Math.max(...metaSamples.map((s) => s.lagSourceOffsets)) : 0,
    lagEnd: metaSamples.length > 0 ? metaSamples[metaSamples.length - 1]!.lagSourceOffsets : 0,
    hotTemplatesEnd: metaSamples.length > 0 ? metaSamples[metaSamples.length - 1]!.hotTemplates : 0,
    hotFineKeysEnd: metaSamples.length > 0 ? metaSamples[metaSamples.length - 1]!.hotFineKeys : 0,
    hotTemplatesBMin: (() => {
      const list = metaSamples.filter((s) => s.tsMs >= tB0 && s.tsMs <= tB1).map((s) => s.hotTemplates);
      if (list.length === 0) return 0;
      return Math.min(...list);
    })(),
    hotTemplatesBMax: (() => {
      const list = metaSamples.filter((s) => s.tsMs >= tB0 && s.tsMs <= tB1).map((s) => s.hotTemplates);
      if (list.length === 0) return 0;
      return Math.max(...list);
    })(),
    fineRateA: deltaRate(tA0, tA1, (s) => s.touchesTemplateTotal, 10),
    fineRateB: deltaRate(tB0, tB1, (s) => s.touchesTemplateTotal, 30),
    fineRateC: deltaRate(tC0, tC1, (s) => s.touchesTemplateTotal, 10),
    processedDeltaRateA: deltaRate(tA0, tA1, (s) => s.processedThroughDeltaTotal, 10),
    processedDeltaRateB: deltaRate(tB0, tB1, (s) => s.processedThroughDeltaTotal, 30),
    processedDeltaRateC: deltaRate(tC0, tC1, (s) => s.processedThroughDeltaTotal, 10),
    touchesDeltaRateA: deltaRate(tA0, tA1, (s) => s.touchesEmittedTotal, 10),
    touchesDeltaRateB: deltaRate(tB0, tB1, (s) => s.touchesEmittedTotal, 30),
    touchesDeltaRateC: deltaRate(tC0, tC1, (s) => s.touchesEmittedTotal, 10),
    notifyWakeupsRateA: deltaRate(tA0, tA1, (s) => s.journalNotifyWakeupsTotal, 10),
    notifyWakeupsRateB: deltaRate(tB0, tB1, (s) => s.journalNotifyWakeupsTotal, 30),
    notifyWakeupsRateC: deltaRate(tC0, tC1, (s) => s.journalNotifyWakeupsTotal, 10),
    flushesRateA: deltaRate(tA0, tA1, (s) => s.journalFlushesTotal, 10),
    flushesRateB: deltaRate(tB0, tB1, (s) => s.journalFlushesTotal, 30),
    flushesRateC: deltaRate(tC0, tC1, (s) => s.journalFlushesTotal, 10),
    flushIntervalP95A: percentile(valuesIn(tA0, tA1, (s) => s.flushIntervalMsP95Last10s, 10), 0.95),
    flushIntervalP95B: percentile(valuesIn(tB0, tB1, (s) => s.flushIntervalMsP95Last10s, 30), 0.95),
    flushIntervalP95C: percentile(valuesIn(tC0, tC1, (s) => s.flushIntervalMsP95Last10s, 10), 0.95),
    notifyWakeMsMaxA: Math.max(0, ...valuesIn(tA0, tA1, (s) => s.journalNotifyWakeMsMax, 10)),
    notifyWakeMsMaxB: Math.max(0, ...valuesIn(tB0, tB1, (s) => s.journalNotifyWakeMsMax, 30)),
    notifyWakeMsMaxC: Math.max(0, ...valuesIn(tC0, tC1, (s) => s.journalNotifyWakeMsMax, 10)),
    lagBStart: lagBSeries.length > 0 ? lagBSeries[0]!.v : 0,
    lagBEnd: lagBSeries.length > 0 ? lagBSeries[lagBSeries.length - 1]!.v : 0,
    lagBSlopePerSec: slopePerSecond(lagBSeries),
  };

  // eslint-disable-next-line no-console
  console.log(
    `[test4][metrics] done lagMax=${summary.lagMax} lagEnd=${summary.lagEnd} fineRateA=${summary.fineRateA.toFixed(1)} fineRateB=${summary.fineRateB.toFixed(
      1
    )} fineRateC=${summary.fineRateC.toFixed(1)} processedRateB=${summary.processedDeltaRateB.toFixed(1)} touchDeltaB=${summary.touchesDeltaRateB.toFixed(
      1
    )} flushRateB=${summary.flushesRateB.toFixed(1)} flushP95B=${summary.flushIntervalP95B.toFixed(1)} notifyWakeMsMaxB=${summary.notifyWakeMsMaxB.toFixed(1)}`
  );

  return summary;
}

function evaluatePass(args: {
  metrics: MetricsSummary;
  appkit: AppkitSummary;
}): AllSummary["pass"] {
  const fineDrop = args.metrics.fineRateA > 0 ? args.metrics.fineRateB <= args.metrics.fineRateA / 4 : false;
  const lagSlopeMaxPerSec = 200;
  const lagBounded = args.metrics.lagBEnd <= args.metrics.lagBStart + 10_000 && args.metrics.lagBSlopePerSec <= lagSlopeMaxPerSec;
  const hotCollapsed = args.metrics.hotTemplatesBMin <= 20;
  const targetedP95BOk = args.appkit.targetedLatencyCountB > 0 && args.appkit.targetedLatencyP95BMs <= 250;
  const targetedP95CRecoverOk = args.appkit.targetedLatencyCountC === 0 || args.appkit.targetedLatencyP95CMs <= 1000;
  const targetedSuccessRatio = args.appkit.markersReceived > 0 ? args.appkit.markerSuccessWithin1s / args.appkit.markersReceived : 0;
  const targetedOk = targetedP95BOk && targetedP95CRecoverOk && targetedSuccessRatio >= 0.99;
  const noErrors = args.appkit.errors === 0;
  return {
    lagBoundedInB: lagBounded,
    hotTemplatesCollapsed: hotCollapsed,
    fineRateDropped: fineDrop,
    targetedLatencyRecovered: targetedOk,
    noErrors,
  };
}

async function runAll(args: ParsedArgs): Promise<AllSummary> {
  const cfg = await runSetup(args);

  const [writer, appkit, metrics] = await Promise.all([
    runWriter(args, cfg),
    runAppkit(args, cfg),
    runMetrics(args, cfg),
  ]);

  const pass = evaluatePass({ metrics, appkit });
  const out: AllSummary = { config: cfg, writer, appkit, metrics, pass };

  // eslint-disable-next-line no-console
  console.log(`[test4][all] pass=${JSON.stringify(pass)}`);
  return out;
}

async function main(): Promise<void> {
  if (hasFlag(ARGS, "help") || hasFlag(ARGS, "h")) usage(0);

  const role = parseRole(ARGS);
  const configPath = parseConfigPath(ARGS);

  if (role === "setup") {
    await runSetup(ARGS);
    return;
  }

  if (role === "all") {
    const summary = await runAll(ARGS);
    const outPath = stringArg(ARGS, "out", "./tmp/test4_summary.json");
    writeJson(outPath, summary);
    // eslint-disable-next-line no-console
    console.log(`[test4] summary written to ${outPath}`);
    return;
  }

  const cfg = readConfig(configPath);

  if (role === "writer") {
    const summary = await runWriter(ARGS, cfg);
    const outPath = stringArg(ARGS, "out", "./tmp/test4_writer_summary.json");
    writeJson(outPath, summary);
    return;
  }

  if (role === "appkit") {
    const summary = await runAppkit(ARGS, cfg);
    const outPath = stringArg(ARGS, "out", "./tmp/test4_appkit_summary.json");
    writeJson(outPath, summary);
    return;
  }

  if (role === "metrics") {
    const summary = await runMetrics(ARGS, cfg);
    const outPath = stringArg(ARGS, "out", "./tmp/test4_metrics_summary.json");
    writeJson(outPath, summary);
    return;
  }

  throw dsError(`unsupported role: ${role}`);
}

await main();
