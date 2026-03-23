/**
 * Live load test (read path): /touch/wait fanout, keyset size, and template churn.
 *
 * This is a black-box HTTP load generator intended to run against a Durable Streams
 * server process (this repo) with touch enabled on the target stream.
 *
 * Docs: docs/live-load-tests.md
 */

import { tableKeyFor, templateIdFor, watchKeyFor } from "../../../src/touch/live_keys";
import { touchKeyIdFromRoutingKey } from "../../../src/touch/touch_key_id";
import { dsError } from "../../../src/util/ds_error.ts";
import {
  activateTemplatesChunked,
  csvArg,
  deleteStream,
  ensureSchemaAndInterpreter,
  ensureStream,
  getTouchMeta,
  hasFlag,
  intArg,
  parseArgs,
  sleep,
  startTailJsonStream,
  stringArg,
  waitForTouchReady,
  type ParsedArgs,
  type TemplateDecl,
} from "./common";

const ARGS = parseArgs(process.argv.slice(2));

type Scenario = "small" | "huge" | "churn";
type Role = "all" | "waiters" | "writer" | "metrics";

function usage(exitCode = 0): never {
  // eslint-disable-next-line no-console
  console.log(`
Live Read-Path Load Test (/touch/wait scalability)

Usage:
  bun run experiments/loadtests/live/read_path.ts [options]

Options:
  --url <baseUrl>                     (default: http://127.0.0.1:8080)
  --stream <name>                     (default: load.live.read)
  --scenario small|huge|churn          (default: small)
  --role all|waiters|writer|metrics    (default: all) Run only part of the test (useful to split writer/metrics from waiters)
  --duration-seconds <n>              (default: 120)
  --concurrency <n>                   (default: 1000)
  --waiter-processes <n>              (default: 1) Shard waiters across N child processes (helps exceed per-process HTTP connection limits)
  --waiter-seed <n>                   (default: 0x1337babe) RNG seed for waiter key generation (use different seeds across shards)
  --waiter-shard-gap-ms <n>           (default: 0) Delay between spawning waiter shards (helps avoid connection storms)
  --waiter-start-jitter-ms <n>        (default: 0) Random delay per waiter before its first /touch/wait call (helps avoid connection storms)
  --keys-per-wait <n>                 (default: 3)
  --wait-timeout-ms <n>               (default: 30000)
  --graceful-stop                     Do not abort in-flight wait calls at the duration boundary; allow them to complete (adds up to waitTimeoutMs wall time)
  --lag-degrade-offsets <n>           (default: 5000) Switch to coarse-only when lag >= N source offsets
  --lag-recover-offsets <n>           (default: 1000) Re-enable fine touches when lag <= N source offsets
  --fine-budget-per-batch <n>         (default: 2000) Hard cap for fine/template touches per interpreter batch (0 = coarse-only)
  --fine-tokens-per-second <n>        (default: 200000) Fine-touch token bucket refill rate
  --fine-burst <n>                    (default: 400000) Fine-touch token bucket burst capacity
  --setup                             Ensure stream + interpreter config (default: on)
  --no-setup                          Do not modify stream schema/interpreter
  --activate-templates                Activate templates pre-step (default: on)
  --no-activate-templates             Skip template activation pre-step
  --metrics                           Tail live.metrics and print summaries (default: on)
  --no-metrics                        Disable live.metrics tailing
  --metrics-stream <name>             (default: live.metrics)
  --reset                             Delete <stream> first

Touch driver (writer):
  --writer-rate <events/sec>          (default: 200)
  --writer-tick-ms <n>                (default: 100)
  --touch-fraction <0..1>             (default: 0.01) Fraction of the waiter key-domain to touch (fanout control)

Small scenario key domains (for watch keys):
  --user-domain <n>                   (default: 10000)
  --org-domain <n>                    (default: 1000)
  --status-domain <n>                 (default: 5)
  --table-key-entity <entity>         (default: public.backstop) Entity used for the coarse tableKey included in waits.
                                      Use public.posts to model a true coarse backstop (broadcast-y).

Huge scenario key shapes:
  --hot-keys <n>                      (default: 5) Hot watch keys present in every wait set

Template churn:
  --template-churn                    Enable activation + heartbeat churn (default: on for scenario=churn)
  --ttl-ms <n>                        (default: 3600000) Template inactivity TTL (use 30000 for accelerated churn)
  --churn-entities <csv>              (default: public.posts,public.posts2,...)
  --churn-every-ms <n>                (default: 1000)
  --churn-activate <n>                (default: 5)
  --churn-drop <n>                    (default: 5)
  --churn-heartbeat-templates <n>     (default: 50) How many templateIds to heartbeat at a time

Notes:
  - /touch/wait supports up to 1024 keys per call. For huge keysets, keep
    --keys-per-wait <= 1024.
  - Bun's fetch() typically has a per-process concurrent connection cap per host
    (often ~256). Use --waiter-processes to reach higher true concurrency.
  - For scenario=small, the default tableKey entity is *not* written by the
    touch driver, so fine touches drive wakeups (more controllable fanout).
`);
  process.exit(exitCode);
}

function parseScenario(args: ParsedArgs): Scenario {
  const raw = stringArg(args, "scenario", "small").trim();
  if (raw === "small" || raw === "huge" || raw === "churn") return raw;
  throw dsError(`invalid --scenario: ${raw}`);
}

function parseRole(args: ParsedArgs): Role {
  const raw = stringArg(args, "role", "all").trim();
  if (raw === "all" || raw === "waiters" || raw === "writer" || raw === "metrics") return raw;
  throw dsError(`invalid --role: ${raw}`);
}

type XorShift32 = {
  nextU32: () => number;
  int: (maxExclusive: number) => number;
};

function makeRng(seed: number): XorShift32 {
  let x = seed | 0;
  if (x === 0) x = 0x12345678;
  const nextU32 = () => {
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    return x >>> 0;
  };
  const int = (maxExclusive: number) => {
    if (!Number.isFinite(maxExclusive) || maxExclusive <= 0) return 0;
    return nextU32() % maxExclusive;
  };
  return { nextU32, int };
}

function combos(fields: string[], maxArity: 1 | 2 | 3): string[][] {
  const sorted = [...fields].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  const out: string[][] = [];
  for (let i = 0; i < sorted.length; i++) out.push([sorted[i]]);
  if (maxArity >= 2) {
    for (let i = 0; i < sorted.length; i++) {
      for (let j = i + 1; j < sorted.length; j++) out.push([sorted[i], sorted[j]]);
    }
  }
  if (maxArity >= 3) {
    for (let i = 0; i < sorted.length; i++) {
      for (let j = i + 1; j < sorted.length; j++) {
        for (let k = j + 1; k < sorted.length; k++) out.push([sorted[i], sorted[j], sorted[k]]);
      }
    }
  }
  return out;
}

const POSTS_FIELDS = [
  "userId",
  "orgId",
  "status",
  "projectId",
  "authorId",
  "channelId",
  "kind",
  "region",
  "teamId",
  "tagId",
  "visibility",
  "priority",
];

function makeModerateTemplateSet(): TemplateDecl[] {
  const out: TemplateDecl[] = [];

  // 200 templates on posts, include 1..3-arity combos in canonical order.
  const postCombos = combos(POSTS_FIELDS, 3);
  for (const fieldsSorted of postCombos.slice(0, 200)) {
    out.push({ entity: "public.posts", fields: fieldsSorted.map((name) => ({ name, encoding: "string" })) });
  }

  // 50 templates across a few other entities.
  const extraEntities: Array<{ entity: string; fields: string[] }> = [
    { entity: "public.comments", fields: ["orgId", "userId", "status", "region", "kind", "priority"] },
    { entity: "public.users", fields: ["orgId", "region", "teamId", "status", "visibility", "priority"] },
    { entity: "public.projects", fields: ["projectId", "orgId", "status", "region", "teamId", "visibility"] },
  ];
  const extras: TemplateDecl[] = [];
  for (const e of extraEntities) {
    const c = combos(e.fields, 2);
    for (const fieldsSorted of c) {
      extras.push({ entity: e.entity, fields: fieldsSorted.map((name) => ({ name, encoding: "string" })) });
    }
  }
  out.push(...extras.slice(0, 50));

  // Ensure key templates used by this load test are present.
  const ensure = (entity: string, fieldsSorted: string[]) => {
    const id = templateIdFor(entity, fieldsSorted);
    const exists = out.some((t) => t.entity === entity && templateIdFor(entity, t.fields.map((f) => f.name).sort()) === id);
    if (!exists) {
      out.push({ entity, fields: fieldsSorted.map((name) => ({ name, encoding: "string" })) });
    }
  };
  ensure("public.posts", ["userId"]);
  ensure("public.posts", ["orgId", "status"].sort());

  return out;
}

function makeNearCapTemplateSet(entities: string[]): TemplateDecl[] {
  const out: TemplateDecl[] = [];
  for (const entity of entities) {
    for (const fieldsSorted of combos(POSTS_FIELDS, 3)) {
      out.push({ entity, fields: fieldsSorted.map((name) => ({ name, encoding: "string" })) });
    }
  }
  return out;
}

const STATUS_VALUES = ["open", "done", "pending", "archived", "blocked"];

function encodeUser(idx: number): string {
  return `u${idx}`;
}
function encodeOrg(idx: number): string {
  return `o${idx}`;
}
function encodeStatus(idx: number): string {
  return STATUS_VALUES[idx % STATUS_VALUES.length] ?? "open";
}

function randomHex16(rng: XorShift32): string {
  // 64-bit value as 16 lowercase hex chars.
  const hi = rng.nextU32();
  const lo = rng.nextU32();
  return hi.toString(16).padStart(8, "0") + lo.toString(16).padStart(8, "0");
}

type LatencyHistogram = {
  bounds: number[];
  counts: number[];
  record: (ms: number) => void;
  mergeFrom: (other: LatencyHistogram) => void;
  p95: () => number;
};

function makeLatencyHistogram(): LatencyHistogram {
  const bounds = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10_000, 30_000, 120_000];
  const counts = new Array(bounds.length + 1).fill(0);
  const record = (ms: number) => {
    const x = Math.max(0, Math.floor(ms));
    let i = 0;
    while (i < bounds.length && x > bounds[i]!) i++;
    counts[i] += 1;
  };
  const mergeFrom = (other: LatencyHistogram) => {
    // Assume compatible bounds (same factory).
    for (let i = 0; i < counts.length; i++) counts[i] += other.counts[i] ?? 0;
  };
  const p95 = () => {
    const total = counts.reduce((a, b) => a + b, 0);
    if (total === 0) return 0;
    const target = Math.ceil(total * 0.95);
    let acc = 0;
    for (let i = 0; i < counts.length; i++) {
      acc += counts[i]!;
      if (acc >= target) return i < bounds.length ? bounds[i]! : bounds[bounds.length - 1]!;
    }
    return bounds[bounds.length - 1]!;
  };
  return { bounds, counts, record, mergeFrom, p95 };
}

type WaiterStats = {
  calls: number;
  touched: number;
  timeout: number;
  stale: number;
  errors: number;
  aborted: number;
  touchedLatency: LatencyHistogram;
  lastError: string | null;
};

type WaiterSpec = {
  keys: string[];
  keyIds?: number[];
  templateIdsUsed: string[];
};

async function waitLoop(args: {
  baseUrl: string;
  stream: string;
  since: string;
  timeoutMs: number;
  spec: WaiterSpec;
  startDelayMs?: number;
  stopAtMs: number;
  signal: AbortSignal;
  stats: WaiterStats;
}): Promise<void> {
  const url = `${args.baseUrl}/v1/stream/${encodeURIComponent(args.stream)}/touch/wait`;
  let cursor = args.since;

  if (args.startDelayMs && args.startDelayMs > 0) {
    await sleep(args.startDelayMs);
  }

  while (!args.signal.aborted && Date.now() < args.stopAtMs) {
    const t0 = Date.now();
    let res: any;
    try {
      const r = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          cursor,
          timeoutMs: args.timeoutMs,
          keys: args.spec.keys,
          ...(args.spec.keyIds && args.spec.keyIds.length > 0 ? { keyIds: args.spec.keyIds } : {}),
          templateIdsUsed: args.spec.templateIdsUsed,
        }),
        signal: args.signal,
      });
      const text = await r.text();
      if (!r.ok) throw dsError(`HTTP ${r.status}: ${text}`);
      res = text ? JSON.parse(text) : null;
    } catch (e: any) {
      if (args.signal.aborted) {
        args.stats.aborted += 1;
        break;
      }
      args.stats.errors += 1;
      args.stats.lastError = String(e?.message ?? e);
      await sleep(50);
      continue;
    } finally {
      args.stats.calls += 1;
    }
    const dt = Date.now() - t0;

    if (res?.stale) {
      args.stats.stale += 1;
      cursor = res.cursor ?? cursor;
      continue;
    }
    if (res?.touched) {
      args.stats.touched += 1;
      args.stats.touchedLatency.record(dt);
      cursor = res.cursor ?? cursor;
      continue;
    }
    args.stats.timeout += 1;
    cursor = res?.cursor ?? cursor;
  }
}

async function runTouchDriver(args: {
  baseUrl: string;
  stream: string;
  stopAtMs: number;
  rate: number;
  tickMs: number;
  touchFraction: number;
  userDomain: number;
  orgDomain: number;
  statusDomain: number;
  signal: AbortSignal;
}): Promise<void> {
  const url = `${args.baseUrl}/v1/stream/${encodeURIComponent(args.stream)}`;
  const eventsPerTick = Math.max(1, Math.floor((args.rate * args.tickMs) / 1000));
  const touchUserCount = Math.max(1, Math.floor(args.userDomain * args.touchFraction));
  const touchOrgCount = Math.max(1, Math.floor(args.orgDomain * args.touchFraction));
  const touchStatusCount = Math.max(1, Math.floor(args.statusDomain * args.touchFraction));

  const rng = makeRng(0xfeedcafe);

  while (!args.signal.aborted && Date.now() < args.stopAtMs) {
    const tickStart = Date.now();
    const batch: string[] = [];
    for (let i = 0; i < eventsPerTick; i++) {
      const userIdx = rng.int(touchUserCount);
      const orgIdx = rng.int(touchOrgCount);
      const statusIdx = rng.int(touchStatusCount);
      const valueJson = `{\"id\":\"${rng.nextU32()}\",\"userId\":${JSON.stringify(encodeUser(userIdx))},\"orgId\":${JSON.stringify(
        encodeOrg(orgIdx)
      )},\"status\":${JSON.stringify(encodeStatus(statusIdx))}}`;
      const evt = `{\"type\":\"public.posts\",\"key\":\"${rng.nextU32()}\",\"value\":${valueJson},\"oldValue\":${valueJson},\"headers\":{\"operation\":\"update\",\"txid\":\"${rng.nextU32()}\",\"timestamp\":\"${new Date().toISOString()}\"}}`;
      batch.push(evt);
    }
    const body = `[${batch.join(",")}]`;
    try {
      const r = await fetch(url, { method: "POST", headers: { "content-type": "application/json" }, body, signal: args.signal });
      if (!r.ok && r.status !== 204 && r.status !== 200) {
        const t = await r.text().catch(() => "");
        // eslint-disable-next-line no-console
        console.error(`[read-path][writer] HTTP ${r.status}: ${t.slice(0, 200)}`);
      }
    } catch (e) {
      if (args.signal.aborted) break;
      // eslint-disable-next-line no-console
      console.error(`[read-path][writer] error: ${String((e as any)?.message ?? e)}`);
    }

    const elapsed = Date.now() - tickStart;
    const sleepMs = Math.max(0, args.tickMs - elapsed);
    if (sleepMs > 0) await sleep(sleepMs);
  }
}

type TickAgg = {
  ticks: number;
  waitCalls: number;
  waitAvgLatencyMs: number;
  waitP95LatencyMs: number;
  waitKeysWatchedTotal: number;
  waitActiveWaiters: number;
  waitTimeoutsFired: number;
  waitTimeoutSweepMsMax: number;
  loopLagMsMax: number;
  staleResponses: number;
  templateActivated: number;
  templateRetired: number;
  templateEvicted: number;
  templateDenied: number;
  lastTick: any | null;
};

function newTickAgg(): TickAgg {
  return {
    ticks: 0,
    waitCalls: 0,
    waitAvgLatencyMs: 0,
    waitP95LatencyMs: 0,
    waitKeysWatchedTotal: 0,
    waitActiveWaiters: 0,
    waitTimeoutsFired: 0,
    waitTimeoutSweepMsMax: 0,
    loopLagMsMax: 0,
    staleResponses: 0,
    templateActivated: 0,
    templateRetired: 0,
    templateEvicted: 0,
    templateDenied: 0,
    lastTick: null,
  };
}

function applyTick(agg: TickAgg, tick: any): void {
  agg.ticks += 1;
  agg.lastTick = tick;
  agg.waitCalls += Number(tick?.wait?.calls ?? 0);
  agg.waitKeysWatchedTotal += Number(tick?.wait?.keysWatchedTotal ?? 0);
  agg.waitAvgLatencyMs = Number(tick?.wait?.avgLatencyMs ?? agg.waitAvgLatencyMs);
  agg.waitP95LatencyMs = Number(tick?.wait?.p95LatencyMs ?? agg.waitP95LatencyMs);
  agg.waitActiveWaiters = Number(tick?.wait?.activeWaiters ?? agg.waitActiveWaiters);
  agg.waitTimeoutsFired += Number(tick?.wait?.timeoutsFired ?? 0);
  agg.waitTimeoutSweepMsMax = Math.max(agg.waitTimeoutSweepMsMax, Number(tick?.wait?.timeoutSweepMsMax ?? 0));
  agg.loopLagMsMax = Math.max(agg.loopLagMsMax, Number(tick?.process?.eventLoopLagMsMax ?? 0));
  agg.staleResponses += Number(tick?.touch?.staleResponses ?? 0);
  agg.templateActivated += Number(tick?.templates?.activated ?? 0);
  agg.templateRetired += Number(tick?.templates?.retired ?? 0);
  agg.templateEvicted += Number(tick?.templates?.evicted ?? 0);
  agg.templateDenied += Number(tick?.templates?.activationDenied ?? 0);
}

function fmtInt(n: number): string {
  if (!Number.isFinite(n)) return "0";
  return Math.floor(n).toLocaleString("en-US");
}

async function warmupTouchStream(baseUrl: string, stream: string): Promise<void> {
  const meta0 = await getTouchMeta(baseUrl, stream);
  const before = meta0.cursor;

  const ts = new Date().toISOString();
  const evt = `{\"type\":\"public.__warmup\",\"key\":\"warmup\",\"value\":{\"id\":\"warmup\"},\"headers\":{\"operation\":\"insert\",\"txid\":\"warmup\",\"timestamp\":${JSON.stringify(ts)}}}`;
  const body = `[${evt}]`;
  const r = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
  });
  if (!r.ok && r.status !== 200 && r.status !== 204) return;

  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    const meta = await getTouchMeta(baseUrl, stream);
    const cur = meta.cursor;
    if (cur !== before) return;
    await sleep(200);
  }
}

async function setupStream(baseUrl: string, stream: string, args: ParsedArgs): Promise<void> {
  const lagDegradeOffsets = Math.max(0, intArg(args, "lag-degrade-offsets", 5000));
  const lagRecoverOffsets = Math.max(0, intArg(args, "lag-recover-offsets", 1000));
  const fineBudgetPerBatch = Math.max(0, intArg(args, "fine-budget-per-batch", 2000));
  const fineTokensPerSecond = Math.max(0, intArg(args, "fine-tokens-per-second", 200_000));
  const fineBurst = Math.max(0, intArg(args, "fine-burst", 400_000));
  const interpreter = {
    apiVersion: "durable.streams/stream-interpreter/v1",
    format: "durable.streams/state-protocol/v1",
    touch: {
      enabled: true,
      lagDegradeFineTouchesAtSourceOffsets: lagDegradeOffsets,
      lagRecoverFineTouchesAtSourceOffsets: lagRecoverOffsets,
      fineTouchBudgetPerBatch: fineBudgetPerBatch,
      fineTokensPerSecond,
      fineBurstTokens: fineBurst,
      // Read-path load tests don't want interpreter batches to wedge just because
      // some active templates can't be evaluated from partial row images.
      onMissingBefore: "skipBefore",
      templates: {
        maxActiveTemplatesPerStream: 4096,
        maxActiveTemplatesPerEntity: 1024,
        activationRateLimitPerMinute: 0,
      },
    },
  };

  await ensureStream(baseUrl, stream, "application/json");
  await ensureSchemaAndInterpreter(baseUrl, stream, interpreter);
  await waitForTouchReady(baseUrl, stream);
}

async function main(): Promise<void> {
  if (hasFlag(ARGS, "help") || hasFlag(ARGS, "h")) usage(0);

  const baseUrl = stringArg(ARGS, "url", "http://127.0.0.1:8080");
  const stream = stringArg(ARGS, "stream", "load.live.read");
  const scenario = parseScenario(ARGS);
  const role = parseRole(ARGS);
  const durationSeconds = Math.max(1, intArg(ARGS, "duration-seconds", 120));
  const concurrency = Math.max(1, intArg(ARGS, "concurrency", 1000));
  const waiterProcesses = Math.max(1, intArg(ARGS, "waiter-processes", 1));
  const waiterSeed = intArg(ARGS, "waiter-seed", 0x1337babe);
  const waiterShardGapMs = Math.max(0, intArg(ARGS, "waiter-shard-gap-ms", 0));
  const waiterStartJitterMs = Math.max(0, intArg(ARGS, "waiter-start-jitter-ms", 0));
  const keysPerWait = Math.max(1, intArg(ARGS, "keys-per-wait", scenario === "huge" ? 700 : 3));
  const waitTimeoutMs = Math.max(0, intArg(ARGS, "wait-timeout-ms", 30_000));
  const gracefulStop = hasFlag(ARGS, "graceful-stop");

  const doReset = hasFlag(ARGS, "reset");
  const doSetup = !hasFlag(ARGS, "no-setup");
  const doActivateTemplates = !hasFlag(ARGS, "no-activate-templates");
  const doMetrics = !hasFlag(ARGS, "no-metrics");
  const metricsStream = stringArg(ARGS, "metrics-stream", "live.metrics");

  const runWriter = role === "all" || role === "writer";
  const runWaiters = role === "all" || role === "waiters";
  const runMetrics = role === "all" || role === "writer" || role === "waiters" || role === "metrics";

  const writerRateArg = Math.max(0, intArg(ARGS, "writer-rate", 200));
  const writerRate = runWriter ? writerRateArg : 0;
  const writerTickMs = Math.max(10, intArg(ARGS, "writer-tick-ms", 100));
  const touchFraction = Math.max(0, Math.min(1, Number(stringArg(ARGS, "touch-fraction", "0.01"))));

  const userDomain = Math.max(1, intArg(ARGS, "user-domain", 10_000));
  const orgDomain = Math.max(1, intArg(ARGS, "org-domain", 1000));
  const statusDomain = Math.max(1, intArg(ARGS, "status-domain", 5));
  const tableKeyEntity = stringArg(ARGS, "table-key-entity", "public.backstop").trim() || "public.backstop";

  const ttlMs = Math.max(0, intArg(ARGS, "ttl-ms", 60 * 60 * 1000));
  const churnEveryMs = Math.max(100, intArg(ARGS, "churn-every-ms", 1000));
  const churnActivate = Math.max(1, intArg(ARGS, "churn-activate", 5));
  const churnDrop = Math.max(0, intArg(ARGS, "churn-drop", 5));
  const churnHeartbeatTemplates = Math.max(1, intArg(ARGS, "churn-heartbeat-templates", 50));
  const doChurn = hasFlag(ARGS, "template-churn") || scenario === "churn";
  const hotKeysCount = Math.max(1, intArg(ARGS, "hot-keys", 5));

  if (keysPerWait > 1024) throw dsError(`--keys-per-wait too large: ${keysPerWait} (max 1024)`);

  if (doReset) {
    // eslint-disable-next-line no-console
    console.log(`[read-path] reset: deleting ${stream}`);
    await deleteStream(baseUrl, stream);
  }

  if (doSetup) {
    // eslint-disable-next-line no-console
    console.log(`[read-path] setup: ensuring stream + interpreter config (touch enabled)`);
    await setupStream(baseUrl, stream, ARGS);
  } else {
    // eslint-disable-next-line no-console
    console.log(`[read-path] setup: skipped (--no-setup)`);
    await waitForTouchReady(baseUrl, stream);
  }

  // Template activation pre-step.
  const templates =
    scenario === "churn"
      ? makeNearCapTemplateSet(
          csvArg(ARGS, "churn-entities", [
            "public.posts",
            "public.posts2",
            "public.posts3",
            "public.posts4",
            "public.posts5",
            "public.posts6",
            "public.posts7",
            "public.posts8",
          ])
        )
      : makeModerateTemplateSet();

  if (doActivateTemplates) {
    // eslint-disable-next-line no-console
    console.log(`[read-path] template pre-step: activating ${templates.length} templates (ttlMs=${ttlMs})`);
    const act = await activateTemplatesChunked({ baseUrl, stream, templates, inactivityTtlMs: ttlMs });
    // eslint-disable-next-line no-console
    console.log(`[read-path] activate: activated=${act.activated.length} denied=${act.denied.length}`);
  } else {
    // eslint-disable-next-line no-console
    console.log(`[read-path] template pre-step: skipped (--no-activate-templates)`);
  }

  // Warmup: ensure interpreter/derived stream are actually advancing before we start measuring wait behavior.
  if (role !== "waiters") {
    await warmupTouchStream(baseUrl, stream);
  }

  const meta = await getTouchMeta(baseUrl, stream);
  // eslint-disable-next-line no-console
  console.log(`[read-path] touch/meta: cursor=${meta.cursor} activeTemplates=${meta.activeTemplates}`);

  if (role === "waiters" && waiterProcesses > 1) {
    if (scenario === "churn") throw dsError(`--waiter-processes is not supported for scenario=churn (use role=all or run separate churn controller)`);
    // eslint-disable-next-line no-console
    console.log(`[read-path] waiters: sharding across ${waiterProcesses} processes (total concurrency=${concurrency})`);

    const per = Math.floor(concurrency / waiterProcesses);
    const rem = concurrency % waiterProcesses;

    const parseIntCommas = (s: string) => Number(String(s).replace(/,/g, ""));
    const parseDone = (text: string) => {
      const line = text
        .split(/\r?\n/)
        .find((l) => l.startsWith("[read-path] done: calls="));
      if (!line) return null;
      const m = line.match(
        /calls=([0-9,]+) completed=([0-9,]+) touched=([0-9,]+) timeout=([0-9,]+) stale=([0-9,]+) errors=([0-9,]+) aborted=([0-9,]+)/
      );
      if (!m) return null;
      return {
        calls: parseIntCommas(m[1]!),
        completed: parseIntCommas(m[2]!),
        touched: parseIntCommas(m[3]!),
        timeout: parseIntCommas(m[4]!),
        stale: parseIntCommas(m[5]!),
        errors: parseIntCommas(m[6]!),
        aborted: parseIntCommas(m[7]!),
      };
    };

    const shardTasks: Array<Promise<{ shard: number; out: string; err: string; code: number; stats: any | null }>> = [];
    for (let i = 0; i < waiterProcesses; i++) {
      const c = per + (i < rem ? 1 : 0);
      if (c <= 0) continue;
      const shardSeed = (waiterSeed ^ ((i + 1) * 0x9e3779b9)) | 0;
      const cmd = [
        "bun",
        "run",
        "experiments/loadtests/live/read_path.ts",
        "--url",
        baseUrl,
        "--stream",
        stream,
        "--scenario",
        scenario,
        "--role",
        "waiters",
        "--duration-seconds",
        String(durationSeconds),
        "--concurrency",
        String(c),
        "--waiter-processes",
        "1",
        "--waiter-seed",
        `0x${(shardSeed >>> 0).toString(16)}`,
        "--waiter-start-jitter-ms",
        String(waiterStartJitterMs),
        "--keys-per-wait",
        String(keysPerWait),
        "--wait-timeout-ms",
        String(waitTimeoutMs),
        "--user-domain",
        String(userDomain),
        "--org-domain",
        String(orgDomain),
        "--status-domain",
        String(statusDomain),
        "--table-key-entity",
        tableKeyEntity,
        "--hot-keys",
        String(hotKeysCount),
        "--writer-rate",
        "0",
        "--no-setup",
        "--no-activate-templates",
        "--no-metrics",
        ...(gracefulStop ? ["--graceful-stop"] : []),
      ];

      // eslint-disable-next-line no-console
      console.log(`[read-path] waiters shard ${i + 1}/${waiterProcesses}: concurrency=${c} seed=0x${(shardSeed >>> 0).toString(16)}`);

      shardTasks.push(
        (async () => {
          if (waiterShardGapMs > 0) await sleep(i * waiterShardGapMs);
          const proc = Bun.spawn({ cmd, cwd: process.cwd(), stdout: "pipe", stderr: "pipe", env: process.env });
          const outP = proc.stdout ? new Response(proc.stdout).text() : Promise.resolve("");
          const errP = proc.stderr ? new Response(proc.stderr).text() : Promise.resolve("");
          const code = await proc.exited;
          const [out, err] = await Promise.all([outP, errP]);
          const stats = parseDone(out) ?? parseDone(err);
          return { shard: i + 1, out, err, code, stats };
        })()
      );
    }

    const results = await Promise.all(shardTasks);
    for (const r of results) {
      if (r.code !== 0) {
        // eslint-disable-next-line no-console
        console.error(`[read-path] waiter shard ${r.shard} exited with code ${r.code}`);
      }
      if (!r.stats) {
        // eslint-disable-next-line no-console
        console.error(`[read-path] waiter shard ${r.shard} missing summary line (outLen=${r.out.length} errLen=${r.err.length})`);
        const combined = `${r.out}\n${r.err}`.trim();
        if (combined) {
          const tail = combined.split(/\r?\n/).slice(-15).join("\n");
          // eslint-disable-next-line no-console
          console.error(`[read-path] waiter shard ${r.shard} output tail:\n${tail}`);
        }
      }
    }

    const totals = results.reduce(
      (acc, r) => {
        if (!r.stats) return acc;
        acc.calls += r.stats.calls;
        acc.completed += r.stats.completed;
        acc.touched += r.stats.touched;
        acc.timeout += r.stats.timeout;
        acc.stale += r.stats.stale;
        acc.errors += r.stats.errors;
        acc.aborted += r.stats.aborted;
        return acc;
      },
      { calls: 0, completed: 0, touched: 0, timeout: 0, stale: 0, errors: 0, aborted: 0 }
    );

    // eslint-disable-next-line no-console
    console.log(
      `[read-path] done (sharded): calls=${fmtInt(totals.calls)} completed=${fmtInt(totals.completed)} touched=${fmtInt(totals.touched)} timeout=${fmtInt(
        totals.timeout
      )} stale=${fmtInt(totals.stale)} errors=${fmtInt(totals.errors)} aborted=${fmtInt(totals.aborted)}`
    );
    return;
  }

  // Precompute the key templates for the read-path test.
  const tplUser = templateIdFor("public.posts", ["userId"]);
  const tplOrgStatus = templateIdFor("public.posts", ["orgId", "status"].sort());
  const tableKey = tableKeyFor(tableKeyEntity);

  // Build waiter specs.
  const rng = makeRng(waiterSeed);
  const startRng = makeRng(waiterSeed ^ 0x7f4a7c15);
  const waiters: WaiterSpec[] = [];
  if (scenario === "small" || scenario === "churn") {
    for (let i = 0; i < concurrency; i++) {
      const u = rng.int(userDomain);
      const o = rng.int(orgDomain);
      const s = rng.int(statusDomain);
      const keyUser = watchKeyFor(tplUser, [encodeUser(u)]);
      const keyOrgStatus = watchKeyFor(tplOrgStatus, [encodeOrg(o), encodeStatus(s)]);
      const keys = [keyUser, keyOrgStatus, tableKey].slice(0, keysPerWait);
      waiters.push({
        keys,
        keyIds: keys.map((k) => touchKeyIdFromRoutingKey(k)),
        templateIdsUsed: [tplUser, tplOrgStatus],
      });
    }
  } else {
    // huge
    const hot: string[] = [];
    for (let i = 0; i < hotKeysCount; i++) {
      hot.push(watchKeyFor(tplUser, [encodeUser(i)]));
    }
    for (let i = 0; i < concurrency; i++) {
      const keys = new Set<string>();
      keys.add(tableKey);
      for (const k of hot) keys.add(k);
      while (keys.size < keysPerWait) keys.add(randomHex16(rng));
      const keyList = Array.from(keys);
      waiters.push({ keys: keyList, keyIds: keyList.map((k) => touchKeyIdFromRoutingKey(k)), templateIdsUsed: [tplUser] });
    }
  }

  const controller = new AbortController();
  const stopAtMs = Date.now() + durationSeconds * 1000;

  // live.metrics tailer (optional)
  let tickAgg = newTickAgg();
  let lastPrintMs = 0;
  let tailStop: (() => void) | null = null;
  let tailTask: Promise<void> | null = null;
  if (doMetrics && runMetrics) {
    // eslint-disable-next-line no-console
    console.log(`[read-path] metrics: tailing ${metricsStream} (filter stream=${stream})`);
    const tail = startTailJsonStream({
      baseUrl,
      stream: metricsStream,
      offset: "now",
      timeoutMs: 3000,
      onEvents: (events) => {
        for (const e of events) {
          if (!e || typeof e !== "object") continue;
          if (e.type !== "live.tick") continue;
          if (e.stream !== stream) continue;
          applyTick(tickAgg, e);
          const now = Date.now();
          if (now - lastPrintMs >= 5000) {
            lastPrintMs = now;
            // eslint-disable-next-line no-console
            console.log(
              `[read-path][metrics] wait.calls=${fmtInt(e.wait.calls)} avgLatencyMs=${Number(e.wait.avgLatencyMs ?? 0).toFixed(1)} p95LatencyMs=${fmtInt(
                e.wait.p95LatencyMs
              )} keysWatchedTotal=${fmtInt(e.wait.keysWatchedTotal)} activeWaiters=${fmtInt(e.wait.activeWaiters ?? 0)} timeoutsFired=${fmtInt(
                e.wait.timeoutsFired ?? 0
              )} timeoutSweepMsMax=${fmtInt(e.wait.timeoutSweepMsMax ?? 0)} loopLagMsMax=${fmtInt(e.process?.eventLoopLagMsMax ?? 0)} templates.active=${fmtInt(
                e.templates.active
              )} templates.activated=${fmtInt(
                e.templates.activated
              )} retired=${fmtInt(e.templates.retired)} evicted=${fmtInt(e.templates.evicted)} denied=${fmtInt(e.templates.activationDenied)}`
            );
          }
        }
      },
      onError: (err) => {
        // eslint-disable-next-line no-console
        console.error(`[read-path][metrics] error: ${String((err as any)?.message ?? err)}`);
      },
    });
    tailStop = tail.stop;
    tailTask = tail.task;
  } else {
    // eslint-disable-next-line no-console
    console.log(`[read-path] metrics: disabled (--no-metrics)`);
  }

  if (role === "metrics") {
    // eslint-disable-next-line no-console
    console.log(`[read-path] role=metrics: tailing only (durationSeconds=${durationSeconds})`);
    await sleep(durationSeconds * 1000);
    controller.abort();
    if (tailStop) {
      tailStop();
      void tailTask?.catch(() => {});
    }
    // eslint-disable-next-line no-console
    console.log(
      `[read-path][metrics totals] ticks=${fmtInt(tickAgg.ticks)} wait.calls=${fmtInt(tickAgg.waitCalls)} staleResponses=${fmtInt(
        tickAgg.staleResponses
      )} activeWaiters=${fmtInt(tickAgg.waitActiveWaiters)} wait.timeoutsFired=${fmtInt(tickAgg.waitTimeoutsFired)} timeoutSweepMsMax=${fmtInt(
        tickAgg.waitTimeoutSweepMsMax
      )} loopLagMsMax=${fmtInt(tickAgg.loopLagMsMax)} templates.activated=${fmtInt(tickAgg.templateActivated)} retired=${fmtInt(
        tickAgg.templateRetired
      )} evicted=${fmtInt(tickAgg.templateEvicted)} denied=${fmtInt(tickAgg.templateDenied)}`
    );
    return;
  }

  // Touch driver.
  const writerTask =
    writerRate > 0
      ? runTouchDriver({
          baseUrl,
          stream,
          stopAtMs,
          rate: writerRate,
          tickMs: writerTickMs,
          touchFraction: scenario === "huge" ? 1 : touchFraction,
          userDomain: scenario === "huge" ? hotKeysCount : userDomain,
          orgDomain: scenario === "huge" ? 1 : orgDomain,
          statusDomain: scenario === "huge" ? 1 : statusDomain,
          signal: controller.signal,
        })
      : Promise.resolve();

  // Waiters.
  // eslint-disable-next-line no-console
  console.log(
    `[read-path] run: role=${role} scenario=${scenario} durationSeconds=${durationSeconds} concurrency=${concurrency} keysPerWait=${keysPerWait} waitTimeoutMs=${waitTimeoutMs} writerRate=${writerRate} touchFraction=${touchFraction}`
  );

  const waiterStats: WaiterStats[] = [];
  const waiterTasks: Promise<void>[] = [];
  const since = meta.cursor;
  if (runWaiters) {
    for (let i = 0; i < concurrency; i++) {
      const st: WaiterStats = {
        calls: 0,
        touched: 0,
        timeout: 0,
        stale: 0,
        errors: 0,
        aborted: 0,
        touchedLatency: makeLatencyHistogram(),
        lastError: null,
      };
      waiterStats.push(st);
      waiterTasks.push(
        waitLoop({
          baseUrl,
          stream,
          since,
          timeoutMs: waitTimeoutMs,
          spec: waiters[i]!,
          startDelayMs: waiterStartJitterMs > 0 ? startRng.int(waiterStartJitterMs + 1) : 0,
          stopAtMs,
          signal: controller.signal,
          stats: st,
        })
      );
    }
  }

  // Template churn controller (optional): activate + heartbeat a moving window of templates.
  const churnTask = doChurn
    ? (async () => {
        // Candidate templates to cycle through.
        const candidates = templates.slice();
        let cursor = 0;

        let used: string[] = [];
        // Start with a window of templateIds.
        for (let i = 0; i < Math.min(churnHeartbeatTemplates, candidates.length); i++) {
          const t = candidates[i]!;
          const id = templateIdFor(t.entity, t.fields.map((f) => f.name).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)));
          used.push(id);
        }
        used = Array.from(new Set(used)).slice(0, churnHeartbeatTemplates);

        const heartbeatUrl = `${baseUrl}/v1/stream/${encodeURIComponent(stream)}/touch/wait`;
        const heartbeatKeys = [tableKey]; // any non-empty key set; we aren't trying to wake ourselves
        const heartbeatKeyIds = heartbeatKeys.map((k) => touchKeyIdFromRoutingKey(k));

        const heartbeatLoop = async () => {
          let cursorOffset = since;
          while (!controller.signal.aborted && Date.now() < stopAtMs) {
            try {
              const r = await fetch(heartbeatUrl, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                  cursor: cursorOffset,
                  timeoutMs: 1000,
                  keys: heartbeatKeys,
                  keyIds: heartbeatKeyIds,
                  templateIdsUsed: used,
                }),
                signal: controller.signal,
              });
              const text = await r.text();
              if (r.ok && text) {
                const res = JSON.parse(text);
                cursorOffset = res.cursor ?? cursorOffset;
              }
            } catch {
              // ignore
            }
            await sleep(200);
          }
        };

        void heartbeatLoop();

        while (!controller.signal.aborted && Date.now() < stopAtMs) {
          // Activate N templates.
          const activateBatch: TemplateDecl[] = [];
          for (let i = 0; i < churnActivate; i++) {
            const t = candidates[cursor % candidates.length]!;
            activateBatch.push(t);
            cursor++;
          }
          try {
            await activateTemplatesChunked({ baseUrl, stream, templates: activateBatch, inactivityTtlMs: ttlMs });
          } catch {
            // ignore
          }

          // Rotate used templateIds (drop M, add M).
          const addIds: string[] = [];
          for (const t of activateBatch) {
            const id = templateIdFor(t.entity, t.fields.map((f) => f.name).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)));
            addIds.push(id);
          }
          if (churnDrop > 0) used = used.slice(Math.min(churnDrop, used.length));
          used.push(...addIds);
          used = Array.from(new Set(used)).slice(0, churnHeartbeatTemplates);

          await sleep(churnEveryMs);
        }
      })()
    : Promise.resolve();

  // Let the scenario run.
  await sleep(durationSeconds * 1000);

  const settle = Promise.allSettled([writerTask, churnTask, ...waiterTasks]);
  if (!gracefulStop) {
    controller.abort();
    await settle;
  } else {
    // Allow in-flight long-polls to complete naturally, but don't hang forever.
    const graceMs = Math.max(0, waitTimeoutMs + 5000);
    const finished = await Promise.race([settle.then(() => true), sleep(graceMs).then(() => false)]);
    if (!finished) {
      controller.abort();
      await settle;
    }
  }

  if (tailStop) {
    tailStop();
    void tailTask?.catch(() => {});
  }

  // Summarize client-side wait results.
  const globalTouchedLatency = makeLatencyHistogram();
  const totals = waiterStats.reduce(
    (acc, s) => {
      acc.calls += s.calls;
      acc.touched += s.touched;
      acc.timeout += s.timeout;
      acc.stale += s.stale;
      acc.errors += s.errors;
      acc.aborted += s.aborted;
      globalTouchedLatency.mergeFrom(s.touchedLatency);
      if (!acc.errorSample && s.lastError) acc.errorSample = s.lastError;
      return acc;
    },
    { calls: 0, touched: 0, timeout: 0, stale: 0, errors: 0, aborted: 0, errorSample: "" as string }
  );
  const completed = totals.touched + totals.timeout + totals.stale + totals.errors;
  const p95Touched = globalTouchedLatency.p95();

  // eslint-disable-next-line no-console
  console.log(
    `[read-path] done: calls=${fmtInt(totals.calls)} completed=${fmtInt(completed)} touched=${fmtInt(totals.touched)} timeout=${fmtInt(
      totals.timeout
    )} stale=${fmtInt(totals.stale)} errors=${fmtInt(totals.errors)} aborted=${fmtInt(totals.aborted)} touched.p95LatencyMs=${fmtInt(p95Touched)}`
  );
  if (totals.errorSample) {
    // eslint-disable-next-line no-console
    console.log(`[read-path] error sample: ${totals.errorSample}`);
  }
  if (doMetrics) {
    // eslint-disable-next-line no-console
    console.log(
      `[read-path][metrics totals] ticks=${fmtInt(tickAgg.ticks)} wait.calls=${fmtInt(tickAgg.waitCalls)} staleResponses=${fmtInt(
        tickAgg.staleResponses
      )} templates.activated=${fmtInt(tickAgg.templateActivated)} retired=${fmtInt(tickAgg.templateRetired)} evicted=${fmtInt(tickAgg.templateEvicted)} denied=${fmtInt(
        tickAgg.templateDenied
      )}`
    );
  }
}

await main().catch((e) => {
  // eslint-disable-next-line no-console
  console.error(`[read-path] fatal: ${String((e as any)?.message ?? e)}`);
  process.exit(1);
});
