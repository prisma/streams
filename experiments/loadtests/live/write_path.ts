/**
 * Live load test (write path): touch generation throughput vs key cardinality.
 *
 * This is a black-box HTTP load generator intended to run against a Durable Streams
 * server process (this repo) with touch enabled on the target stream.
 *
 * Docs: docs/live-load-tests.md
 */

import { templateIdFor } from "../../../src/touch/live_keys";
import { dsError } from "../../../src/util/ds_error.ts";
import {
  activateTemplatesChunked,
  csvIntArg,
  deleteStream,
  ensureSchemaAndProfile,
  ensureStream,
  fetchJson,
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

type Mode = "high-cardinality" | "low-cardinality";

const ARGS = parseArgs(process.argv.slice(2));

function usage(exitCode = 0): never {
  // eslint-disable-next-line no-console
  console.log(`
Live Write-Path Load Test (touch generation throughput)

Usage:
  bun run experiments/loadtests/live/write_path.ts [options]

Options:
  --url <baseUrl>                     (default: http://127.0.0.1:8080)
  --stream <name>                     (default: load.live.write)
  --mode high-cardinality|low-cardinality  (default: high-cardinality)
  --lag-degrade-offsets <n>           (default: 5000) Switch to coarse-only when lag >= N source offsets
  --lag-recover-offsets <n>           (default: 1000) Re-enable fine touches when lag <= N source offsets
  --fine-budget-per-batch <n>         (default: 2000) Hard cap for fine/template touches per processing batch (0 = coarse-only)
  --fine-tokens-per-second <n>        (default: 200000) Fine-touch token bucket refill rate
  --fine-burst <n>                    (default: 400000) Fine-touch token bucket burst capacity
  --steps <csv events/sec>            (default: 1000,5000,10000,20000)
  --step-seconds <n>                  (default: 60)
  --producers <n>                     (default: 4)
  --batch-events <n>                  (default: 200)
  --columns <n>                       (default: 128)  Value/old_value columns per row object
  --row-space <n>                     (default: 100000) Rows per entity per producer
  --ttl-ms <n>                        (default: 3600000) Template inactivity TTL for activations
  --coarse-interval-ms <n>            (default: 100)
  --coalesce-window-ms <n>            (default: 100)
  --setup                             Ensure stream + state-protocol profile (default: on)
  --no-setup                          Do not modify stream schema/profile
  --activate-templates                Activate templates pre-step (default: on)
  --no-activate-templates             Skip template activation pre-step
  --reset                             Delete <stream> first
  --metrics                           Tail live.metrics and print summaries (default: on)
  --no-metrics                        Disable live.metrics tailing
  --metrics-stream <name>             (default: live.metrics)

Notes:
  - This script batches append requests; adjust --batch-events and --producers
    to avoid client-side bottlenecks.
  - Template activation requires caps/rate-limits that allow >298 templates per
    entity; --setup configures a permissive state-protocol touch.templates policy.
`);
  process.exit(exitCode);
}

function parseMode(args: ParsedArgs): Mode {
  const raw = stringArg(args, "mode", "high-cardinality").trim();
  if (raw === "high-cardinality" || raw === "low-cardinality") return raw;
  throw dsError(`invalid --mode: ${raw}`);
}

type XorShift32 = {
  nextU32: () => number;
  int: (maxExclusive: number) => number;
};

function makeRng(seed: number): XorShift32 {
  let x = seed | 0;
  if (x === 0) x = 0x12345678;
  const nextU32 = () => {
    // xorshift32
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    // Force uint32
    return x >>> 0;
  };
  const int = (maxExclusive: number) => {
    if (!Number.isFinite(maxExclusive) || maxExclusive <= 0) return 0;
    return nextU32() % maxExclusive;
  };
  return { nextU32, int };
}

type EntitySpec = {
  entity: string;
  fields: string[]; // template field pool for this entity
  templateMaxArity: 1 | 2 | 3;
  weight: number; // event mix weight
};

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

const ENTITY_SPECS: EntitySpec[] = [
  { entity: "public.posts", fields: POSTS_FIELDS, templateMaxArity: 3, weight: 0.7 },
  { entity: "public.comments", fields: ["orgId", "userId", "status", "region", "kind", "priority"], templateMaxArity: 2, weight: 0.1 },
  { entity: "public.users", fields: ["orgId", "region", "teamId", "status", "visibility", "priority"], templateMaxArity: 2, weight: 0.1 },
  { entity: "public.orgs", fields: ["orgId", "region", "status", "visibility", "priority", "teamId"], templateMaxArity: 2, weight: 0.05 },
  { entity: "public.projects", fields: ["projectId", "orgId", "status", "region", "teamId", "visibility"], templateMaxArity: 2, weight: 0.05 },
];

function normalizeWeights(specs: EntitySpec[]): Array<EntitySpec & { p: number }> {
  const sum = specs.reduce((acc, s) => acc + Math.max(0, s.weight), 0);
  if (sum <= 0) {
    const p = 1 / specs.length;
    return specs.map((s) => ({ ...s, p }));
  }
  return specs.map((s) => ({ ...s, p: Math.max(0, s.weight) / sum }));
}

function chooseEntity(rng: XorShift32, weighted: Array<EntitySpec & { p: number }>): EntitySpec {
  const x = rng.nextU32() / 0xffffffff;
  let acc = 0;
  for (const s of weighted) {
    acc += s.p;
    if (x <= acc) return s;
  }
  return weighted[weighted.length - 1]!;
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

function makeTemplates(specs: EntitySpec[]): TemplateDecl[] {
  const out: TemplateDecl[] = [];
  for (const s of specs) {
    const c = combos(s.fields, s.templateMaxArity);
    for (const fieldsSorted of c) {
      out.push({
        entity: s.entity,
        fields: fieldsSorted.map((name) => ({ name, encoding: "string" })),
      });
    }
  }
  return out;
}

const LOW_ENUMS = {
  status: ["open", "done", "pending", "archived", "blocked"],
  visibility: ["public", "org", "team", "private", "unlisted"],
  region: ["us-east", "us-west", "eu", "apac", "sa"],
  kind: ["a", "b", "c", "d", "e"],
};

function lowDomainSize(field: string): number {
  if (field in LOW_ENUMS) return (LOW_ENUMS as any)[field].length;
  if (field === "userId" || field === "authorId") return 100;
  if (field === "orgId") return 10;
  if (field === "projectId") return 50;
  if (field === "channelId") return 20;
  if (field === "teamId") return 20;
  if (field === "tagId") return 200;
  if (field === "priority") return 10;
  return 100;
}

function highDomainSize(_field: string): number {
  return 1_000_000;
}

function prefixForField(field: string): string {
  switch (field) {
    case "userId":
      return "u";
    case "orgId":
      return "o";
    case "status":
      return "s";
    case "projectId":
      return "p";
    case "authorId":
      return "a";
    case "channelId":
      return "c";
    case "kind":
      return "k";
    case "region":
      return "r";
    case "teamId":
      return "t";
    case "tagId":
      return "g";
    case "visibility":
      return "v";
    case "priority":
      return "pr";
    default:
      return field.slice(0, 2) || "x";
  }
}

function encodeField(field: string, idx: number, mode: Mode): string {
  if (mode === "low-cardinality") {
    const enums = (LOW_ENUMS as any)[field];
    if (Array.isArray(enums)) return String(enums[idx % enums.length]);
  }
  return `${prefixForField(field)}${idx}`;
}

function makeFillerSegment(keysCount: number): string {
  if (keysCount <= 0) return "";
  const parts: string[] = [];
  for (let i = 0; i < keysCount; i++) {
    parts.push(`\"c${i}\":${i}`);
  }
  return parts.join(",");
}

type RowPool = {
  active: number[];
  free: number[];
  fields: string[];
  fieldIdx: Uint32Array[];
};

function makeRowPool(rowSpace: number, fields: string[], rng: XorShift32, mode: Mode): RowPool {
  const free: number[] = [];
  for (let i = rowSpace - 1; i >= 0; i--) free.push(i);
  const active: number[] = [];
  const fieldIdx = fields.map(() => new Uint32Array(rowSpace));

  // Seed half full so updates/deletes are valid immediately.
  const seedN = Math.floor(rowSpace / 2);
  for (let n = 0; n < seedN; n++) {
    const id = free.pop();
    if (id == null) break;
    active.push(id);
    for (let f = 0; f < fields.length; f++) {
      const domain = mode === "high-cardinality" ? highDomainSize(fields[f]) : lowDomainSize(fields[f]);
      fieldIdx[f]![id] = rng.int(domain);
    }
  }

  return { active, free, fields, fieldIdx };
}

type EventFactory = {
  entity: string;
  fields: string[];
  rowSpace: number;
  mode: Mode;
  fillerSegment: string;
  pool: RowPool;
  rng: XorShift32;
  nextTxid: bigint;
  makeOne: (op: "insert" | "update" | "delete") => string;
};

function makeEventFactory(args: { entity: string; fields: string[]; columns: number; rowSpace: number; mode: Mode; seed: number }): EventFactory {
  const rng = makeRng(args.seed);
  const pool = makeRowPool(args.rowSpace, args.fields, rng, args.mode);
  const reservedKeys = 1 + args.fields.length; // id + template fields
  const fillerCount = Math.max(0, args.columns - reservedKeys);
  const fillerSegment = makeFillerSegment(fillerCount);

  const makeRowJson = (id: number, indices: number[]): string => {
    const parts: string[] = [];
    const idStr = String(id);
    parts.push(`\"id\":${JSON.stringify(idStr)}`);
    for (let i = 0; i < args.fields.length; i++) {
      const name = args.fields[i]!;
      parts.push(`\"${name}\":${JSON.stringify(encodeField(name, indices[i]!, args.mode))}`);
    }
    if (fillerSegment) parts.push(fillerSegment);
    return `{${parts.join(",")}}`;
  };

  const pickActive = (): { id: number; pos: number } | null => {
    if (pool.active.length === 0) return null;
    const pos = rng.int(pool.active.length);
    const id = pool.active[pos]!;
    return { id, pos };
  };

  const doInsert = (): { id: number; after: number[]; before: null } => {
    const id = pool.free.pop();
    if (id == null) {
      // No free ids; degrade into update semantics.
      const picked = pickActive();
      if (!picked) return { id: 0, after: new Array(args.fields.length).fill(0), before: null };
      const after = new Array(args.fields.length);
      for (let f = 0; f < args.fields.length; f++) {
        const domain = args.mode === "high-cardinality" ? highDomainSize(args.fields[f]!) : lowDomainSize(args.fields[f]!);
        const v = rng.int(domain);
        pool.fieldIdx[f]![picked.id] = v;
        after[f] = v;
      }
      return { id: picked.id, after, before: null };
    }
    pool.active.push(id);
    const after = new Array(args.fields.length);
    for (let f = 0; f < args.fields.length; f++) {
      const domain = args.mode === "high-cardinality" ? highDomainSize(args.fields[f]!) : lowDomainSize(args.fields[f]!);
      const v = rng.int(domain);
      pool.fieldIdx[f]![id] = v;
      after[f] = v;
    }
    return { id, after, before: null };
  };

  const doUpdate = (): { id: number; after: number[]; before: number[] } => {
    const picked = pickActive() ?? doInsert();
    const id = picked.id;
    const before = new Array(args.fields.length);
    const after = new Array(args.fields.length);
    for (let f = 0; f < args.fields.length; f++) {
      const old = pool.fieldIdx[f]![id]!;
      before[f] = old;
      if (args.mode === "low-cardinality") {
        // Best-case for coalescing: keep template fields stable; update changes elsewhere.
        after[f] = old;
      } else {
        const domain = highDomainSize(args.fields[f]!);
        const v = rng.int(domain);
        pool.fieldIdx[f]![id] = v;
        after[f] = v;
      }
    }
    return { id, after, before };
  };

  const doDelete = (): { id: number; before: number[]; after: null } => {
    const picked = pickActive();
    if (!picked) {
      const ins = doInsert();
      return { id: ins.id, before: ins.after, after: null };
    }
    const { id, pos } = picked;
    const before = new Array(args.fields.length);
    for (let f = 0; f < args.fields.length; f++) before[f] = pool.fieldIdx[f]![id]!;
    // Remove from active in O(1)
    const last = pool.active.pop()!;
    if (pos < pool.active.length) pool.active[pos] = last;
    pool.free.push(id);
    return { id, before, after: null };
  };

  let nextTxid = 1n;

  const makeOne = (op: "insert" | "update" | "delete"): string => {
    const txid = nextTxid++;
    const timestamp = new Date().toISOString();

    if (op === "insert") {
      const { id, after } = doInsert();
      const key = String(id);
      const valueJson = makeRowJson(id, after);
      return `{\"type\":${JSON.stringify(args.entity)},\"key\":${JSON.stringify(key)},\"value\":${valueJson},\"headers\":{\"operation\":\"insert\",\"txid\":${JSON.stringify(
        String(txid)
      )},\"timestamp\":${JSON.stringify(timestamp)}}}`;
    }
    if (op === "delete") {
      const { id, before } = doDelete();
      const key = String(id);
      const oldJson = makeRowJson(id, before);
      return `{\"type\":${JSON.stringify(args.entity)},\"key\":${JSON.stringify(key)},\"value\":null,\"old_value\":${oldJson},\"headers\":{\"operation\":\"delete\",\"txid\":${JSON.stringify(
        String(txid)
      )},\"timestamp\":${JSON.stringify(timestamp)}}}`;
    }
    // update
    const { id, before, after } = doUpdate();
    const key = String(id);
    const valueJson = makeRowJson(id, after);
    const oldJson = makeRowJson(id, before);
    return `{\"type\":${JSON.stringify(args.entity)},\"key\":${JSON.stringify(key)},\"value\":${valueJson},\"old_value\":${oldJson},\"headers\":{\"operation\":\"update\",\"txid\":${JSON.stringify(
      String(txid)
    )},\"timestamp\":${JSON.stringify(timestamp)}}}`;
  };

  return {
    entity: args.entity,
    fields: args.fields,
    rowSpace: args.rowSpace,
    mode: args.mode,
    fillerSegment,
    pool,
    rng,
    nextTxid,
    makeOne,
  };
}

type ProducerStats = {
  requests: number;
  events: number;
  httpErrors: number;
  lastError: string | null;
};

async function runProducer(args: {
  id: number;
  baseUrl: string;
  stream: string;
  stepRate: number;
  stepDeadlineMs: number;
  batchEvents: number;
  entityWeighted: Array<EntitySpec & { p: number }>;
  columns: number;
  rowSpace: number;
  mode: Mode;
  signal: AbortSignal;
  stats: ProducerStats;
}): Promise<void> {
  const rng = makeRng(0xabc00000 + args.id * 101);

  // One event factory per entity keeps per-entity row pools isolated and avoids
  // shared-state synchronization between entities.
  const factories = new Map<string, EventFactory>();
  const getFactory = (entity: string): EventFactory => {
    const existing = factories.get(entity);
    if (existing) return existing;
    const spec = args.entityWeighted.find((s) => s.entity === entity);
    if (!spec) throw dsError(`unknown entity: ${entity}`);
    const f = makeEventFactory({
      entity,
      fields: spec.fields,
      columns: args.columns,
      rowSpace: args.rowSpace,
      mode: args.mode,
      seed: 0xdecafbad + args.id * 1009 + entity.length * 7,
    });
    factories.set(entity, f);
    return f;
  };

  const perProducerRate = Math.max(0, args.stepRate);
  const batchSize = Math.max(1, args.batchEvents);
  const targetBatchIntervalMs = perProducerRate > 0 ? (batchSize / perProducerRate) * 1000 : 1000;

  const streamUrl = `${args.baseUrl}/v1/stream/${encodeURIComponent(args.stream)}`;

  while (!args.signal.aborted && Date.now() < args.stepDeadlineMs) {
    const batchStartMs = Date.now();

    // Build request body as a JSON array string to reduce client-side overhead.
    const events: string[] = [];
    for (let i = 0; i < batchSize; i++) {
      const spec = chooseEntity(rng, args.entityWeighted);
      const factory = getFactory(spec.entity);
      // 10% insert, 80% update, 10% delete
      const x = rng.int(100);
      const op = x < 10 ? "insert" : x < 90 ? "update" : "delete";
      events.push(factory.makeOne(op));
    }
    const body = `[${events.join(",")}]`;

    try {
      const r = await fetch(streamUrl, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body,
        signal: args.signal,
      });
      args.stats.requests += 1;
      if (!r.ok && r.status !== 204 && r.status !== 200) {
        args.stats.httpErrors += 1;
        const t = await r.text().catch(() => "");
        args.stats.lastError = `HTTP ${r.status}: ${t.slice(0, 200)}`;
      } else {
        args.stats.events += batchSize;
      }
    } catch (e: any) {
      if (args.signal.aborted) break;
      args.stats.httpErrors += 1;
      args.stats.lastError = String(e?.message ?? e);
    }

    const elapsed = Date.now() - batchStartMs;
    const sleepMs = Math.max(0, Math.floor(targetBatchIntervalMs - elapsed));
    if (sleepMs > 0) await sleep(sleepMs);
  }
}

type LiveTick = {
  type: "live.tick";
  ts: string;
  stream: string;
  touch: {
    touchesEmitted: number;
    uniqueKeysTouched: number;
    tableTouchesEmitted: number;
    templateTouchesEmitted: number;
    fineTouchesDroppedDueToBudget?: number;
    fineTouchesSuppressedBatchesDueToLag?: number;
    fineTouchesSuppressedBatchesDueToBudget?: number;
  };
  processor: {
    errors: number;
    lagSourceOffsets: number;
  };
};

function isLiveTick(x: any): x is LiveTick {
  return !!x && typeof x === "object" && x.type === "live.tick" && typeof x.stream === "string" && x.touch && x.processor;
}

type TickAgg = {
  ticks: number;
  touchesEmitted: number;
  uniqueKeysTouched: number;
  tableTouchesEmitted: number;
  templateTouchesEmitted: number;
  fineTouchesDroppedDueToBudget: number;
  fineTouchesSuppressedBatchesDueToLag: number;
  fineTouchesSuppressedBatchesDueToBudget: number;
  processorErrors: number;
  maxLagSourceOffsets: number;
  lastTick: LiveTick | null;
};

function newTickAgg(): TickAgg {
  return {
    ticks: 0,
    touchesEmitted: 0,
    uniqueKeysTouched: 0,
    tableTouchesEmitted: 0,
    templateTouchesEmitted: 0,
    fineTouchesDroppedDueToBudget: 0,
    fineTouchesSuppressedBatchesDueToLag: 0,
    fineTouchesSuppressedBatchesDueToBudget: 0,
    processorErrors: 0,
    maxLagSourceOffsets: 0,
    lastTick: null,
  };
}

function applyTick(agg: TickAgg, tick: LiveTick): void {
  agg.ticks += 1;
  agg.touchesEmitted += Number(tick.touch.touchesEmitted ?? 0);
  agg.uniqueKeysTouched += Number(tick.touch.uniqueKeysTouched ?? 0);
  agg.tableTouchesEmitted += Number(tick.touch.tableTouchesEmitted ?? 0);
  agg.templateTouchesEmitted += Number(tick.touch.templateTouchesEmitted ?? 0);
  agg.fineTouchesDroppedDueToBudget += Number(tick.touch.fineTouchesDroppedDueToBudget ?? 0);
  agg.fineTouchesSuppressedBatchesDueToLag += Number(tick.touch.fineTouchesSuppressedBatchesDueToLag ?? 0);
  agg.fineTouchesSuppressedBatchesDueToBudget += Number(tick.touch.fineTouchesSuppressedBatchesDueToBudget ?? 0);
  agg.processorErrors += Number(tick.processor.errors ?? 0);
  agg.maxLagSourceOffsets = Math.max(agg.maxLagSourceOffsets, Number(tick.processor.lagSourceOffsets ?? 0));
  agg.lastTick = tick;
}

function fmtInt(n: number): string {
  if (!Number.isFinite(n)) return "0";
  return Math.floor(n).toLocaleString("en-US");
}

async function warmupTouchStream(baseUrl: string, stream: string): Promise<void> {
  const meta0 = await getTouchMeta(baseUrl, stream);
  const before = meta0.cursor;

  // Append one small change and wait for the touch cursor to advance.
  const ts = new Date().toISOString();
  const evt = `{\"type\":\"public.__warmup\",\"key\":\"warmup\",\"value\":{\"id\":\"warmup\"},\"headers\":{\"operation\":\"insert\",\"txid\":\"warmup\",\"timestamp\":${JSON.stringify(ts)}}}`;
  const body = `[${evt}]`;
  const r = await fetch(`${baseUrl}/v1/stream/${encodeURIComponent(stream)}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body,
  });
  if (!r.ok && r.status !== 200 && r.status !== 204) {
    const t = await r.text().catch(() => "");
    // eslint-disable-next-line no-console
    console.log(`[write-path] warmup: append failed (HTTP ${r.status}): ${t.slice(0, 200)}`);
    return;
  }

  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    const meta = await getTouchMeta(baseUrl, stream);
    const cur = meta.cursor;
    if (cur !== before) {
      // eslint-disable-next-line no-console
      console.log(`[write-path] warmup: cursor advanced: ${before} -> ${cur}`);
      return;
    }
    await sleep(200);
  }
  // eslint-disable-next-line no-console
  console.log(`[write-path] warmup: cursor did not advance within 10s (continuing)`);
}

async function setupStream(baseUrl: string, stream: string, args: ParsedArgs): Promise<void> {
  const coarseIntervalMs = intArg(args, "coarse-interval-ms", 100);
  const coalesceWindowMs = intArg(args, "coalesce-window-ms", 100);
  const lagDegradeOffsets = Math.max(0, intArg(args, "lag-degrade-offsets", 5000));
  const lagRecoverOffsets = Math.max(0, intArg(args, "lag-recover-offsets", 1000));
  const fineBudgetPerBatch = Math.max(0, intArg(args, "fine-budget-per-batch", 2000));
  const fineTokensPerSecond = Math.max(0, intArg(args, "fine-tokens-per-second", 200_000));
  const fineBurst = Math.max(0, intArg(args, "fine-burst", 400_000));

  const profile = {
    kind: "state-protocol",
    touch: {
      enabled: true,
      coarseIntervalMs,
      touchCoalesceWindowMs: coalesceWindowMs,
      lagDegradeFineTouchesAtSourceOffsets: lagDegradeOffsets,
      lagRecoverFineTouchesAtSourceOffsets: lagRecoverOffsets,
      fineTouchBudgetPerBatch: fineBudgetPerBatch,
      fineTokensPerSecond,
      fineBurstTokens: fineBurst,
      onMissingBefore: "error",
      templates: {
        // Defaults are too low for this load test; make the stream permissive.
        maxActiveTemplatesPerStream: 4096,
        maxActiveTemplatesPerEntity: 1024,
        activationRateLimitPerMinute: 0,
      },
    },
  };

  await ensureStream(baseUrl, stream, "application/json");
  await ensureSchemaAndProfile(baseUrl, stream, profile);
  await waitForTouchReady(baseUrl, stream);
}

async function main(): Promise<void> {
  if (hasFlag(ARGS, "help") || hasFlag(ARGS, "h")) usage(0);

  const baseUrl = stringArg(ARGS, "url", "http://127.0.0.1:8080");
  const stream = stringArg(ARGS, "stream", "load.live.write");
  const mode = parseMode(ARGS);
  const steps = csvIntArg(ARGS, "steps", [1000, 5000, 10_000, 20_000]);
  const stepSeconds = intArg(ARGS, "step-seconds", 60);
  const producers = Math.max(1, intArg(ARGS, "producers", 4));
  const batchEvents = Math.max(1, intArg(ARGS, "batch-events", 200));
  const columns = Math.max(8, intArg(ARGS, "columns", 128));
  const rowSpace = Math.max(1000, intArg(ARGS, "row-space", 100_000));
  const ttlMs = Math.max(0, intArg(ARGS, "ttl-ms", 60 * 60 * 1000));

  const doSetup = !hasFlag(ARGS, "no-setup");
  const doActivateTemplates = !hasFlag(ARGS, "no-activate-templates");
  const doReset = hasFlag(ARGS, "reset");
  const doMetrics = !hasFlag(ARGS, "no-metrics");
  const metricsStream = stringArg(ARGS, "metrics-stream", "live.metrics");

  if (doReset) {
    // eslint-disable-next-line no-console
    console.log(`[write-path] reset: deleting ${stream}`);
    await deleteStream(baseUrl, stream);
  }

  if (doSetup) {
    // eslint-disable-next-line no-console
    console.log(`[write-path] setup: ensuring stream + state-protocol profile (touch enabled)`);
    await setupStream(baseUrl, stream, ARGS);
  } else {
    // eslint-disable-next-line no-console
    console.log(`[write-path] setup: skipped (--no-setup)`);
    await waitForTouchReady(baseUrl, stream);
  }

  if (doActivateTemplates) {
    // eslint-disable-next-line no-console
    console.log(`[write-path] template pre-step: activating templates`);

    const templates = makeTemplates(ENTITY_SPECS);
    // eslint-disable-next-line no-console
    console.log(`[write-path] templates to activate: ${templates.length} (ttlMs=${ttlMs})`);

    const res = await activateTemplatesChunked({ baseUrl, stream, templates, inactivityTtlMs: ttlMs });
    // eslint-disable-next-line no-console
    console.log(`[write-path] activate: activated=${res.activated.length} denied=${res.denied.length}`);
    if (res.denied.length > 0) {
      const byReason = new Map<string, number>();
      for (const d of res.denied) {
        const reason = String(d?.reason ?? "unknown");
        byReason.set(reason, (byReason.get(reason) ?? 0) + 1);
      }
      // eslint-disable-next-line no-console
      console.log(`[write-path] denied by reason: ${JSON.stringify(Object.fromEntries(byReason.entries()))}`);
    }

    const meta = await getTouchMeta(baseUrl, stream);
    // eslint-disable-next-line no-console
    console.log(
      `[write-path] touch/meta: mode=${meta.mode} activeTemplates=${meta.activeTemplates} coarseIntervalMs=${meta.coarseIntervalMs} coalesceWindowMs=${meta.touchCoalesceWindowMs}`
    );

    // Ensure templateId computation matches spec (debug aid).
    const sampleTpl = { entity: "public.posts", fieldsSorted: ["orgId", "status"] };
    // eslint-disable-next-line no-console
    console.log(`[write-path] sample templateId(public.posts,[orgId,status])=${templateIdFor(sampleTpl.entity, sampleTpl.fieldsSorted)}`);
  } else {
    // eslint-disable-next-line no-console
    console.log(`[write-path] template pre-step: skipped (--no-activate-templates)`);
  }

  const entityWeighted = normalizeWeights(ENTITY_SPECS);

  // Warmup: avoid false "0 touches" steps caused by touch processing startup latency.
  await warmupTouchStream(baseUrl, stream);

  // live.metrics tailer (optional)
  let tickAgg = newTickAgg();
  let lastPrintMs = 0;
  let tailStop: (() => void) | null = null;
  let tailTask: Promise<void> | null = null;
  if (doMetrics) {
    // eslint-disable-next-line no-console
    console.log(`[write-path] metrics: tailing ${metricsStream} (filter stream=${stream})`);
    const tail = startTailJsonStream({
      baseUrl,
      stream: metricsStream,
      offset: "now",
      timeoutMs: 3000,
      onEvents: (events) => {
        for (const e of events) {
          if (!isLiveTick(e)) continue;
          if (e.stream !== stream) continue;
          applyTick(tickAgg, e);
          const now = Date.now();
          if (now - lastPrintMs >= 5000) {
            lastPrintMs = now;
            const t = tickAgg.lastTick;
            if (t) {
              // eslint-disable-next-line no-console
              console.log(
                `[write-path][metrics] touchesEmitted=${fmtInt(t.touch.touchesEmitted)} templateTouches=${fmtInt(t.touch.templateTouchesEmitted)} uniqueKeys=${fmtInt(
                  t.touch.uniqueKeysTouched
                )} fineDropped=${fmtInt(t.touch.fineTouchesDroppedDueToBudget ?? 0)} fineSuppressedBatches=${fmtInt(
                  t.touch.fineTouchesSuppressedBatchesDueToLag ?? 0
                )} fineSuppressedBudgetBatches=${fmtInt(
                  t.touch.fineTouchesSuppressedBatchesDueToBudget ?? 0
                )} lagSourceOffsets=${fmtInt(t.processor.lagSourceOffsets)} errors=${fmtInt(t.processor.errors)}`
              );
            }
          }
        }
      },
      onError: (err) => {
        // eslint-disable-next-line no-console
        console.error(`[write-path][metrics] error: ${String((err as any)?.message ?? err)}`);
      },
    });
    tailStop = tail.stop;
    tailTask = tail.task;
  } else {
    // eslint-disable-next-line no-console
    console.log(`[write-path] metrics: disabled (--no-metrics)`);
  }

  // Run ramp.
  // eslint-disable-next-line no-console
  console.log(`[write-path] ramp: mode=${mode} steps=${steps.join(",")} stepSeconds=${stepSeconds} producers=${producers} batchEvents=${batchEvents} columns=${columns} rowSpace=${rowSpace}`);

  for (const stepRate of steps) {
    // Reset per-step aggregator (keep tailer running).
    tickAgg = newTickAgg();
    lastPrintMs = 0;

    const meta0 = await getTouchMeta(baseUrl, stream);
    // eslint-disable-next-line no-console
    console.log(
      `[write-path] step start: rate=${fmtInt(stepRate)} events/sec (mode=${mode}) cursor=${meta0.cursor} activeTemplates=${meta0.activeTemplates}`
    );

    const controller = new AbortController();
    const deadlineMs = Date.now() + stepSeconds * 1000;

    const perProducerRateBase = Math.floor(stepRate / producers);
    const remainder = stepRate % producers;

    const stats: ProducerStats[] = [];
    const tasks: Promise<void>[] = [];
    for (let i = 0; i < producers; i++) {
      const rate = perProducerRateBase + (i < remainder ? 1 : 0);
      const st: ProducerStats = { requests: 0, events: 0, httpErrors: 0, lastError: null };
      stats.push(st);
      tasks.push(
        runProducer({
          id: i,
          baseUrl,
          stream,
          stepRate: rate,
          stepDeadlineMs: deadlineMs,
          batchEvents,
          entityWeighted,
          columns,
          rowSpace,
          mode,
          signal: controller.signal,
          stats: st,
        })
      );
    }

    // Wait for duration then stop.
    await sleep(stepSeconds * 1000);
    controller.abort();
    await Promise.all(tasks);

    const meta1 = await getTouchMeta(baseUrl, stream);
    const totalEvents = stats.reduce((acc, s) => acc + s.events, 0);
    const totalReq = stats.reduce((acc, s) => acc + s.requests, 0);
    const totalErr = stats.reduce((acc, s) => acc + s.httpErrors, 0);
    const errSample = stats.map((s) => s.lastError).find((x) => x != null) ?? null;
    const effRate = stepSeconds > 0 ? Math.floor(totalEvents / stepSeconds) : 0;

    const endCursor = meta1.cursor;
    // eslint-disable-next-line no-console
    console.log(
      `[write-path] step end: rate=${fmtInt(stepRate)} events/sec effective=${fmtInt(effRate)} events/sec sent=${fmtInt(
        totalEvents
      )} requests=${fmtInt(totalReq)} httpErrors=${fmtInt(totalErr)} cursor=${endCursor}`
    );
    if (errSample) {
      // eslint-disable-next-line no-console
      console.log(`[write-path] http error sample: ${errSample}`);
    }

    if (doMetrics) {
      // eslint-disable-next-line no-console
      console.log(
        `[write-path][step metrics] ticks=${fmtInt(tickAgg.ticks)} touchesEmitted=${fmtInt(tickAgg.touchesEmitted)} templateTouches=${fmtInt(
          tickAgg.templateTouchesEmitted
        )} fineDropped=${fmtInt(tickAgg.fineTouchesDroppedDueToBudget)} fineSuppressedBatches=${fmtInt(
          tickAgg.fineTouchesSuppressedBatchesDueToLag
        )} fineSuppressedBudgetBatches=${fmtInt(
          tickAgg.fineTouchesSuppressedBatchesDueToBudget
        )} uniqueKeysTouched=${fmtInt(tickAgg.uniqueKeysTouched)} maxLagSourceOffsets=${fmtInt(tickAgg.maxLagSourceOffsets)} processorErrors=${fmtInt(
          tickAgg.processorErrors
        )}`
      );
    }
  }

  if (tailStop) {
    tailStop();
    // Don't hang shutdown on metrics tailing.
    void tailTask?.catch(() => {});
  }

  // eslint-disable-next-line no-console
  console.log(`[write-path] done`);
}

await main().catch((e) => {
  // eslint-disable-next-line no-console
  console.error(`[write-path] fatal: ${String((e as any)?.message ?? e)}`);
  process.exit(1);
});
