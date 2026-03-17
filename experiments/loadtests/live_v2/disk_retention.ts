/**
 * Live Query V2 load test (disk/retention/GC correctness).
 *
 * Test 3A:
 *   - Touch retention rollover (oldestAvailableTouchOffset advances)
 *   - Stale cursor correctness (/touch/wait returns stale=true deterministically)
 *   - Recovery behavior (client resets cursor and continues)
 *
 * Test 3B:
 *   - Base WAL GC gating correctness under interpreter lag
 *   - "Catch up after lag" GC progress (requires periodic GC; see touch/manager.ts)
 *
 * Docs: experiments/loadtests/live_v2/README.md
 */

import { offsetToSeqOrNeg1, parseOffset } from "../../../src/offset";
import { tableKeyFor, templateIdFor, watchKeyFor } from "../../../src/touch/live_keys";
import { defaultTouchStreamName } from "../../../src/touch/naming";
import { dsError } from "../../../src/util/ds_error.ts";
import {
  activateTemplatesChunked,
  deleteStream,
  ensureSchemaAndInterpreter,
  ensureStream,
  getTouchMeta,
  hasFlag,
  intArg,
  isMemoryTouchMeta,
  optionalStringArg,
  parseArgs,
  sleep,
  startTailJsonStream,
  stringArg,
  waitForTouchReady,
  type ParsedArgs,
  type TemplateDecl,
} from "./common";

const ARGS = parseArgs(process.argv.slice(2));

type Scenario = "3a" | "3b-lag" | "3b-catchup";
type Mode = "high-cardinality" | "low-cardinality";

function usage(exitCode = 0): never {
  // eslint-disable-next-line no-console
  console.log(`
Live V2 Disk/Retention/GC Load Test (Test 3)

Usage:
  bun run experiments/loadtests/live_v2/disk_retention.ts [options]

Options:
  --url <baseUrl>                       (default: http://127.0.0.1:8080)
  --stream <name>                       (default: load.live.disk)
  --scenario 3a|3b-lag|3b-catchup        (default: 3a)

General knobs:
  --mode high-cardinality|low-cardinality (default: high-cardinality)
  --duration-seconds <n>                (default: 60)
  --writer-rate <events/sec>            (default: 500 for 3a, 1000 for 3b-lag, 0 for 3b-catchup)
  --producers <n>                       (default: 2)
  --batch-events <n>                    (default: 200)
  --columns <n>                         (default: 64)
  --row-space <n>                       (default: 100000)

Touch config (applied when --setup is enabled):
  --touch-retention-ms <n>              (default: 10000 for 3a, 86400000 otherwise)
  --coarse-interval-ms <n>              (default: 100)
  --coalesce-window-ms <n>              (default: 100)
  --lag-degrade-offsets <n>             (default: 5000) Switch to coarse-only when lag >= N source offsets
  --lag-recover-offsets <n>             (default: 1000) Re-enable fine touches when lag <= N source offsets
  --fine-budget-per-batch <n>           (default: 2000) Hard cap for fine/template touches per interpreter batch
  --fine-tokens-per-second <n>          (default: 200000) Fine-touch token bucket refill rate
  --fine-burst <n>                      (default: 400000) Fine-touch token bucket burst capacity

Scenario 3a (stale subscriber):
  --behind-ms <n>                       (default: 30000) Subscriber B sleep to fall behind retention
  --restart-pause-ms <n>                (default: 0) In memory mode, pause here so you can restart DS to test epoch-stale behavior
  --wait-timeout-ms <n>                 (default: 5000)
  --meta-every-ms <n>                   (default: 2000)
  --ttl-ms <n>                          (default: 3600000) Template inactivity TTL for activation

Scenario 3b-catchup:
  --max-wait-seconds <n>                (default: 120) How long to wait for catch-up/GC progress

Instrumentation:
  --ds-root <path>                      (optional) If provided, uses 'du -sk' to sample on-disk DS_ROOT size.
  --metrics                             Tail live.metrics and print summaries (default: on)
  --no-metrics                          Disable live.metrics tailing
  --metrics-stream <name>               (default: live.metrics)

Setup/reset:
  --setup                               Ensure stream + interpreter config (default: on)
  --no-setup                            Do not modify stream schema/interpreter
  --activate-templates                  Activate templates pre-step (default: on for 3a)
  --no-activate-templates               Skip template activation pre-step
  --reset                               Delete <stream> and its default touch companion (<stream>.__touch) first
`);
  process.exit(exitCode);
}

function parseScenario(args: ParsedArgs): Scenario {
  const raw = stringArg(args, "scenario", "3a").trim();
  if (raw === "3a" || raw === "3b-lag" || raw === "3b-catchup") return raw;
  throw dsError(`invalid --scenario: ${raw}`);
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
  for (let i = 0; i < keysCount; i++) parts.push(`\"c${i}\":${i}`);
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
      const domain = mode === "high-cardinality" ? highDomainSize(fields[f]!) : lowDomainSize(fields[f]!);
      fieldIdx[f]![id] = rng.int(domain);
    }
  }

  return { active, free, fields, fieldIdx };
}

type EventFactory = {
  entity: string;
  fields: string[];
  pool: RowPool;
  rng: XorShift32;
  fillerSegment: string;
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
        // Keep template fields stable; update changes elsewhere.
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
      return `{\"type\":${JSON.stringify(args.entity)},\"key\":${JSON.stringify(key)},\"value\":null,\"oldValue\":${oldJson},\"headers\":{\"operation\":\"delete\",\"txid\":${JSON.stringify(
        String(txid)
      )},\"timestamp\":${JSON.stringify(timestamp)}}}`;
    }
    // update
    const { id, before, after } = doUpdate();
    const key = String(id);
    const valueJson = makeRowJson(id, after);
    const oldJson = makeRowJson(id, before);
    return `{\"type\":${JSON.stringify(args.entity)},\"key\":${JSON.stringify(key)},\"value\":${valueJson},\"oldValue\":${oldJson},\"headers\":{\"operation\":\"update\",\"txid\":${JSON.stringify(
      String(txid)
    )},\"timestamp\":${JSON.stringify(timestamp)}}}`;
  };

  return { entity: args.entity, fields: args.fields, pool, rng, fillerSegment, nextTxid, makeOne };
}

type ProducerStats = {
  requests: number;
  events: number;
  httpErrors: number;
  lastError: string | null;
};

async function runProducers(args: {
  baseUrl: string;
  stream: string;
  rate: number;
  durationSeconds: number;
  producers: number;
  batchEvents: number;
  columns: number;
  rowSpace: number;
  mode: Mode;
  signal: AbortSignal;
}): Promise<ProducerStats[]> {
  const stats: ProducerStats[] = [];
  const tasks: Promise<void>[] = [];
  const streamUrl = `${args.baseUrl}/v1/stream/${encodeURIComponent(args.stream)}`;
  const deadlineMs = Date.now() + Math.max(0, args.durationSeconds) * 1000;

  for (let i = 0; i < args.producers; i++) {
    const st: ProducerStats = { requests: 0, events: 0, httpErrors: 0, lastError: null };
    stats.push(st);
    const perProducerRate = args.rate > 0 ? args.rate / args.producers : 0;
    const batchSize = Math.max(1, args.batchEvents);
    const targetBatchIntervalMs = perProducerRate > 0 ? (batchSize / perProducerRate) * 1000 : 1000;

    const factory = makeEventFactory({
      entity: "public.posts",
      fields: POSTS_FIELDS,
      columns: args.columns,
      rowSpace: args.rowSpace,
      mode: args.mode,
      seed: 0xdecafbad + i * 101,
    });

    const task = (async () => {
      while (!args.signal.aborted && Date.now() < deadlineMs) {
        if (perProducerRate <= 0) {
          await sleep(50);
          continue;
        }

        const batchStartMs = Date.now();
        const events: string[] = [];
        for (let j = 0; j < batchSize; j++) {
          // 10% insert, 80% update, 10% delete
          const x = factory.rng.int(100);
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
          st.requests += 1;
          if (!r.ok && r.status !== 204 && r.status !== 200) {
            st.httpErrors += 1;
            const t = await r.text().catch(() => "");
            st.lastError = `HTTP ${r.status}: ${t.slice(0, 200)}`;
          } else {
            st.events += batchSize;
          }
        } catch (e: any) {
          if (args.signal.aborted) break;
          st.httpErrors += 1;
          st.lastError = String(e?.message ?? e);
        }

        const elapsed = Date.now() - batchStartMs;
        const sleepMs = Math.max(0, Math.floor(targetBatchIntervalMs - elapsed));
        if (sleepMs > 0) await sleep(sleepMs);
      }
    })();
    tasks.push(task);
  }

  await Promise.all(tasks);
  return stats;
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

function makeModerateTemplateSet(count: number): TemplateDecl[] {
  const out: TemplateDecl[] = [];
  const c = combos(POSTS_FIELDS, 3);
  for (const fieldsSorted of c.slice(0, Math.max(0, count))) {
    out.push({ entity: "public.posts", fields: fieldsSorted.map((name) => ({ name, encoding: "string" })) });
  }
  // Ensure templates used by this load test are present.
  const ensure = (entity: string, fieldsSorted: string[]) => {
    const id = templateIdFor(entity, fieldsSorted);
    const exists = out.some((t) => t.entity === entity && templateIdFor(entity, t.fields.map((f) => f.name).sort()) === id);
    if (!exists) out.push({ entity, fields: fieldsSorted.map((name) => ({ name, encoding: "string" })) });
  };
  ensure("public.posts", ["userId"]);
  ensure("public.posts", ["orgId", "status"].sort());
  return out;
}

async function setupStream(baseUrl: string, stream: string, args: ParsedArgs, scenario: Scenario): Promise<void> {
  const coarseIntervalMs = intArg(args, "coarse-interval-ms", 100);
  const coalesceWindowMs = intArg(args, "coalesce-window-ms", 100);
  const lagDegradeOffsets = Math.max(0, intArg(args, "lag-degrade-offsets", 5000));
  const lagRecoverOffsets = Math.max(0, intArg(args, "lag-recover-offsets", 1000));
  const fineBudgetPerBatch = Math.max(0, intArg(args, "fine-budget-per-batch", 2000));
  const fineTokensPerSecond = Math.max(0, intArg(args, "fine-tokens-per-second", 200_000));
  const fineBurst = Math.max(0, intArg(args, "fine-burst", 400_000));
  const retentionDefault = scenario === "3a" ? 10_000 : 24 * 60 * 60 * 1000;
  const retentionMs = Math.max(0, intArg(args, "touch-retention-ms", retentionDefault));

  const interpreter = {
    apiVersion: "durable.streams/stream-interpreter/v1",
    format: "durable.streams/state-protocol/v1",
    touch: {
      enabled: true,
      storage: "memory",
      coarseIntervalMs,
      touchCoalesceWindowMs: coalesceWindowMs,
      lagDegradeFineTouchesAtSourceOffsets: lagDegradeOffsets,
      lagRecoverFineTouchesAtSourceOffsets: lagRecoverOffsets,
      fineTouchBudgetPerBatch: fineBudgetPerBatch,
      fineTokensPerSecond,
      fineBurstTokens: fineBurst,
      retention: { maxAgeMs: retentionMs },
      // Load tests are intentionally sloppy; avoid wedging when before images
      // are partial/missing (coarse touches still preserve correctness).
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

function seqOfOffset(offset: string): bigint {
  const p = parseOffset(offset);
  return offsetToSeqOrNeg1(p);
}

function fmtInt(n: number): string {
  if (!Number.isFinite(n)) return "0";
  return Math.floor(n).toLocaleString("en-US");
}

function fmtBytes(n: number): string {
  if (!Number.isFinite(n) || n <= 0) return "0B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let x = n;
  let i = 0;
  while (x >= 1024 && i < units.length - 1) {
    x /= 1024;
    i++;
  }
  return `${x.toFixed(i === 0 ? 0 : 1)}${units[i]}`;
}

function duSizeBytes(path: string): number | null {
  try {
    const proc = Bun.spawnSync({ cmd: ["du", "-sk", path] });
    if (proc.exitCode !== 0) return null;
    const out = new TextDecoder().decode(proc.stdout).trim();
    const kb = Number(out.split(/\s+/)[0] ?? "");
    if (!Number.isFinite(kb) || kb < 0) return null;
    return kb * 1024;
  } catch {
    return null;
  }
}

type LiveTick = {
  type: "live.tick";
  ts: string;
  stream: string;
  base?: {
    tailOffset?: string;
    uploadedThrough?: string;
    interpretedThrough?: string;
    gcThrough?: string;
    walOldestOffset?: string | null;
    backlogSourceOffsets?: number;
  };
  process?: {
    eventLoopLagMsMax?: number;
    eventLoopLagMsAvg?: number;
  };
};

function isLiveTick(x: any): x is LiveTick {
  return !!x && typeof x === "object" && x.type === "live.tick" && typeof x.stream === "string";
}

async function postTouchWait(args: {
  baseUrl: string;
  stream: string;
  cursor: string;
  useCursor: boolean;
  timeoutMs: number;
  keys: string[];
  signal?: AbortSignal;
  templateIdsUsed?: string[];
}): Promise<any> {
  const body: any = {
    ...(args.useCursor ? { cursor: args.cursor } : { sinceTouchOffset: args.cursor }),
    timeoutMs: args.timeoutMs,
    keys: args.keys,
  };
  if (args.templateIdsUsed && args.templateIdsUsed.length > 0) body.templateIdsUsed = args.templateIdsUsed;
  const r = await fetch(`${args.baseUrl}/v1/stream/${encodeURIComponent(args.stream)}/touch/wait`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
    signal: args.signal,
  });
  const text = await r.text().catch(() => "");
  if (!r.ok) throw dsError(`touch/wait failed: HTTP ${r.status}: ${text.slice(0, 200)}`);
  return text ? JSON.parse(text) : null;
}

async function runScenario3a(opts: {
  baseUrl: string;
  stream: string;
  mode: Mode;
  durationSeconds: number;
  writerRate: number;
  producers: number;
  batchEvents: number;
  columns: number;
  rowSpace: number;
  behindMs: number;
  restartPauseMs: number;
  waitTimeoutMs: number;
  metaEveryMs: number;
  ttlMs: number;
  dsRoot: string | null;
  doMetrics: boolean;
  metricsStream: string;
  doActivateTemplates: boolean;
  signal: AbortSignal;
}): Promise<void> {
  const metaStart = await getTouchMeta(opts.baseUrl, opts.stream);
  const useCursor = isMemoryTouchMeta(metaStart);
  // eslint-disable-next-line no-console
  console.log(
    `[disk] run: scenario=3a mode=${useCursor ? "memory" : "sqlite"} durationSeconds=${opts.durationSeconds} writerRate=${opts.writerRate} writerMode=${opts.mode}${
      useCursor ? "" : ` retentionMs=${(metaStart as any).touchRetentionMs ?? "unknown"}`
    }`
  );

  const keys = [
    // Use non-hot keys so A mostly times out (avoid adding meaningful read load).
    tableKeyFor("public.backstop"),
    watchKeyFor(templateIdFor("public.posts", ["userId"]), ["u0"]),
    watchKeyFor(templateIdFor("public.posts", ["orgId", "status"].sort()), ["o0", "open"]),
  ];

  const templates = makeModerateTemplateSet(50);
  if (opts.doActivateTemplates) {
    // eslint-disable-next-line no-console
    console.log(`[disk] template pre-step: activating ${templates.length} templates (ttlMs=${opts.ttlMs})`);
    const res = await activateTemplatesChunked({ baseUrl: opts.baseUrl, stream: opts.stream, templates, inactivityTtlMs: opts.ttlMs });
    // eslint-disable-next-line no-console
    console.log(`[disk] activate: activated=${res.activated.length} denied=${res.denied.length}`);
  }

  const controller = new AbortController();
  const onAbort = () => controller.abort();
  opts.signal.addEventListener("abort", onAbort, { once: true });

  const meta0 = await getTouchMeta(opts.baseUrl, opts.stream);
  const bSince = useCursor ? (meta0 as any).cursor : (meta0 as any).currentTouchOffset;

  // Subscriber A (healthy): loops and should never see stale=true.
  const aStats = { calls: 0, touched: 0, timeout: 0, stale: 0, errors: 0, lastErr: null as string | null };
  const subATask = (async () => {
    let since = bSince;
    while (!controller.signal.aborted) {
      try {
        const res = await postTouchWait({
          baseUrl: opts.baseUrl,
          stream: opts.stream,
          cursor: since,
          useCursor,
          timeoutMs: opts.waitTimeoutMs,
          keys,
          signal: controller.signal,
        });
        aStats.calls += 1;
        if (res?.stale) {
          aStats.stale += 1;
          break;
        }
        if (res?.touched) {
          aStats.touched += 1;
          if (useCursor) {
            if (typeof res?.cursor === "string") since = res.cursor;
          } else {
            if (typeof res?.touchOffset === "string") since = res.touchOffset;
            else if (typeof res?.currentTouchOffset === "string") since = res.currentTouchOffset;
          }
        } else {
          aStats.timeout += 1;
          if (useCursor) {
            if (typeof res?.cursor === "string") since = res.cursor;
          } else {
            if (typeof res?.currentTouchOffset === "string") since = res.currentTouchOffset;
          }
        }
      } catch (e: any) {
        if (controller.signal.aborted) break;
        aStats.errors += 1;
        aStats.lastErr = String(e?.message ?? e);
        await sleep(50);
      }
    }
  })();

  // Subscriber B: fall behind and assert stale response + recovery.
  const bStats = { stale: 0, recovered: 0, lastErr: null as string | null };
  const subBTask = (async () => {
    if (useCursor) {
      // eslint-disable-next-line no-console
      console.log(`[disk] subscriber B: captured cursor=${bSince} (sleeping ${opts.behindMs}ms; memory mode does not retention-stale)`);
      await sleep(opts.behindMs);
      if (controller.signal.aborted) return;

      if (opts.restartPauseMs > 0) {
        // eslint-disable-next-line no-console
        console.log(`[disk] subscriber B: restart pause ${opts.restartPauseMs}ms (restart DS now to test epoch-stale)`);
        await sleep(opts.restartPauseMs);
      }

      const res = await postTouchWait({
        baseUrl: opts.baseUrl,
        stream: opts.stream,
        cursor: bSince,
        useCursor,
        timeoutMs: Math.min(opts.waitTimeoutMs, 10_000),
        keys,
        signal: controller.signal,
      });
      if (res?.stale) {
        bStats.stale += 1;
        // eslint-disable-next-line no-console
        console.log(`[disk] subscriber B: stale=true (epoch mismatch) cursor=${String(res?.cursor ?? "")}`);
      } else if (opts.restartPauseMs > 0) {
        // eslint-disable-next-line no-console
        console.log(`[disk] subscriber B: expected stale=true after restart, got: ${JSON.stringify(res).slice(0, 200)}`);
      } else {
        // eslint-disable-next-line no-console
        console.log(`[disk] subscriber B: (no restart) stale=false as expected`);
      }

      // Recovery: restart from "now" (current cursor).
      let since = (await getTouchMeta(opts.baseUrl, opts.stream) as any).cursor;
      for (let i = 0; i < 3; i++) {
        const r2 = await postTouchWait({
          baseUrl: opts.baseUrl,
          stream: opts.stream,
          cursor: since,
          useCursor,
          timeoutMs: opts.waitTimeoutMs,
          keys,
          signal: controller.signal,
        });
        if (r2?.stale) {
          bStats.lastErr = `recovery returned stale=true (unexpected): ${JSON.stringify(r2).slice(0, 200)}`;
          break;
        }
        bStats.recovered += 1;
        if (typeof r2?.cursor === "string") since = r2.cursor;
      }
      return;
    }

    // sqlite mode: fall behind retention and assert stale response + recovery.
    // eslint-disable-next-line no-console
    console.log(`[disk] subscriber B: captured sinceTouchOffset=${bSince} (sleeping ${opts.behindMs}ms to fall behind retention)`);
    await sleep(opts.behindMs);
    if (controller.signal.aborted) return;

    // Wait until the cursor is definitely stale (robust vs trim throttling).
    const deadline = Date.now() + Math.max(10_000, opts.behindMs);
    while (!controller.signal.aborted) {
      const meta = await getTouchMeta(opts.baseUrl, opts.stream);
      if (seqOfOffset(bSince) < seqOfOffset((meta as any).oldestAvailableTouchOffset)) break;
      if (Date.now() > deadline) break;
      await sleep(1000);
    }

    const res = await postTouchWait({
      baseUrl: opts.baseUrl,
      stream: opts.stream,
      cursor: bSince,
      useCursor,
      timeoutMs: Math.min(opts.waitTimeoutMs, 10_000),
      keys,
      signal: controller.signal,
    });
    if (res?.stale) {
      bStats.stale += 1;
      // eslint-disable-next-line no-console
      console.log(
        `[disk] subscriber B: stale=true oldest=${String(res?.oldestAvailableTouchOffset ?? "")} current=${String(res?.currentTouchOffset ?? "")}`
      );
    } else {
      // eslint-disable-next-line no-console
      console.log(`[disk] subscriber B: expected stale=true, got: ${JSON.stringify(res).slice(0, 200)}`);
    }

    // Recovery: restart from "now".
    let since = (await getTouchMeta(opts.baseUrl, opts.stream) as any).currentTouchOffset;
    for (let i = 0; i < 3; i++) {
      const r2 = await postTouchWait({
        baseUrl: opts.baseUrl,
        stream: opts.stream,
        cursor: since,
        useCursor,
        timeoutMs: opts.waitTimeoutMs,
        keys,
        signal: controller.signal,
      });
      if (r2?.stale) {
        bStats.lastErr = `recovery returned stale=true (unexpected): ${JSON.stringify(r2).slice(0, 200)}`;
        break;
      }
      bStats.recovered += 1;
      if (r2?.touched && typeof r2?.touchOffset === "string") since = r2.touchOffset;
      else if (typeof r2?.currentTouchOffset === "string") since = r2.currentTouchOffset;
    }
  })().catch((e: any) => {
    bStats.lastErr = String(e?.message ?? e);
  });

  // Touch meta sampler: show rollover and optional disk size.
  let metaSamples = 0;
  let metaOldestAdvanced = false;
  const metaTask = (async () => {
    const startOldestSeq = useCursor ? 0n : seqOfOffset((meta0 as any).oldestAvailableTouchOffset);
    const startGen = useCursor ? Number((meta0 as any).generation ?? 0) : 0;
    while (!controller.signal.aborted) {
      const meta = await getTouchMeta(opts.baseUrl, opts.stream);
      metaSamples += 1;
      if (useCursor) {
        const gen = Number((meta as any).generation ?? 0);
        if (gen > startGen) metaOldestAdvanced = true;
      } else {
        const oldestSeq = seqOfOffset((meta as any).oldestAvailableTouchOffset);
        if (oldestSeq > startOldestSeq) metaOldestAdvanced = true;
      }

      let diskPart = "";
      if (opts.dsRoot) {
        const bytes = duSizeBytes(opts.dsRoot);
        if (bytes != null) diskPart = ` disk=${fmtBytes(bytes)}`;
      }

      // eslint-disable-next-line no-console
      if (useCursor) {
        console.log(
          `[disk][meta] cursor=${String((meta as any).cursor ?? "")} gen=${fmtInt(Number((meta as any).generation ?? 0))} pendingKeys=${fmtInt(
            Number((meta as any).pendingKeys ?? 0)
          )} overflowBuckets=${fmtInt(Number((meta as any).overflowBuckets ?? 0))} activeTemplates=${fmtInt(Number((meta as any).activeTemplates ?? 0))} filterSize=${fmtInt(
            Number((meta as any).filterSize ?? 0)
          )}${diskPart}`
        );
      } else {
        console.log(
          `[disk][meta] current=${String((meta as any).currentTouchOffset ?? "")} oldest=${String(
            (meta as any).oldestAvailableTouchOffset ?? ""
          )} retentionMs=${(meta as any).touchRetentionMs ?? "?"} activeTemplates=${fmtInt(
            Number((meta as any).activeTemplates ?? 0)
          )} touchWalRows=${fmtInt(Number((meta as any).touchWalRetainedRows ?? 0))} touchWalBytes=${fmtBytes(
            Number((meta as any).touchWalRetainedBytes ?? 0)
          )}${diskPart}`
        );
      }
      await sleep(Math.max(250, opts.metaEveryMs));
    }
  })().catch(() => {
    // ignore
  });

  // Metrics tailer (optional): show base GC state for this stream as context.
  let metricsTail: { stop: () => void; task: Promise<void> } | null = null;
  if (opts.doMetrics) {
    let lastPrintMs = 0;
    metricsTail = startTailJsonStream({
      baseUrl: opts.baseUrl,
      stream: opts.metricsStream,
      offset: "now",
      timeoutMs: 10_000,
      onEvents: async (events) => {
        for (const e of events) {
          if (!isLiveTick(e)) continue;
          if (e.stream !== opts.stream) continue;
          const now = Date.now();
          if (now - lastPrintMs < 5000) continue;
          lastPrintMs = now;
          // eslint-disable-next-line no-console
          console.log(
            `[disk][metrics] backlog=${fmtInt(Number(e.base?.backlogSourceOffsets ?? 0))} walOldest=${String(
              e.base?.walOldestOffset ?? ""
            )} walRows=${fmtInt(Number((e.base as any)?.walRetainedRows ?? 0))} walBytes=${fmtBytes(
              Number((e.base as any)?.walRetainedBytes ?? 0)
            )} gc.delRows=${fmtInt(Number((e.base as any)?.gc?.deletedRows ?? 0))} gc.msMax=${fmtInt(
              Number((e.base as any)?.gc?.msMax ?? 0)
            )} touchWalRows=${fmtInt(Number((e.touch as any)?.walRetainedRows ?? 0))} touchWalBytes=${fmtBytes(
              Number((e.touch as any)?.walRetainedBytes ?? 0)
            )} loopLagMax=${fmtInt(Number(e.process?.eventLoopLagMsMax ?? 0))}ms`
          );
        }
      },
    });
  }

  // Writer (touch traffic).
  const writerTask =
    opts.writerRate > 0
      ? runProducers({
          baseUrl: opts.baseUrl,
          stream: opts.stream,
          rate: opts.writerRate,
          durationSeconds: opts.durationSeconds,
          producers: opts.producers,
          batchEvents: opts.batchEvents,
          columns: opts.columns,
          rowSpace: opts.rowSpace,
          mode: opts.mode,
          signal: controller.signal,
        })
      : Promise.resolve([]);

  // Allow the stale subscriber flow to complete even if the writer stops first.
  const [producerStats] = await Promise.all([writerTask, subBTask]);

  controller.abort();
  metricsTail?.stop();
  opts.signal.removeEventListener("abort", onAbort);
  await Promise.allSettled([subATask, metaTask, metricsTail?.task].filter(Boolean) as any);

  const sent = producerStats.reduce((acc, s) => acc + s.events, 0);
  const reqs = producerStats.reduce((acc, s) => acc + s.requests, 0);
  const httpErrors = producerStats.reduce((acc, s) => acc + s.httpErrors, 0);
  const metaEnd = await getTouchMeta(opts.baseUrl, opts.stream);

  // eslint-disable-next-line no-console
  console.log(
    `[disk] done: writer.sent=${fmtInt(sent)} writer.requests=${fmtInt(reqs)} writer.httpErrors=${fmtInt(httpErrors)} metaSamples=${fmtInt(metaSamples)} oldestAdvanced=${
      metaOldestAdvanced ? "yes" : "no"
    }`
  );
  // eslint-disable-next-line no-console
  console.log(
    `[disk] subscriberA: calls=${fmtInt(aStats.calls)} touched=${fmtInt(aStats.touched)} timeout=${fmtInt(aStats.timeout)} stale=${fmtInt(aStats.stale)} errors=${fmtInt(
      aStats.errors
    )}${aStats.lastErr ? ` lastErr=${aStats.lastErr}` : ""}`
  );
  // eslint-disable-next-line no-console
  console.log(
    `[disk] subscriberB: stale=${fmtInt(bStats.stale)} recoveredWaits=${fmtInt(bStats.recovered)}${bStats.lastErr ? ` lastErr=${bStats.lastErr}` : ""}`
  );
  // eslint-disable-next-line no-console
  if (useCursor) {
    console.log(`[disk] meta end: cursor=${String((metaEnd as any).cursor ?? "")} gen=${fmtInt(Number((metaEnd as any).generation ?? 0))}`);
  } else {
    console.log(`[disk] meta end: current=${String((metaEnd as any).currentTouchOffset ?? "")} oldest=${String((metaEnd as any).oldestAvailableTouchOffset ?? "")}`);
  }
}

async function runScenario3bLag(opts: {
  baseUrl: string;
  stream: string;
  mode: Mode;
  durationSeconds: number;
  writerRate: number;
  producers: number;
  batchEvents: number;
  columns: number;
  rowSpace: number;
  dsRoot: string | null;
  doMetrics: boolean;
  metricsStream: string;
  signal: AbortSignal;
}): Promise<void> {
  // eslint-disable-next-line no-console
  console.log(`[disk] run: scenario=3b-lag durationSeconds=${opts.durationSeconds} writerRate=${opts.writerRate} (expects interpreter to be disabled/throttled)`);

  const controller = new AbortController();
  const onAbort = () => controller.abort();
  opts.signal.addEventListener("abort", onAbort, { once: true });

  let metricsTail: { stop: () => void; task: Promise<void> } | null = null;
  if (opts.doMetrics) {
    let lastPrintMs = 0;
    metricsTail = startTailJsonStream({
      baseUrl: opts.baseUrl,
      stream: opts.metricsStream,
      offset: "now",
      timeoutMs: 10_000,
      onEvents: async (events) => {
        for (const e of events) {
          if (!isLiveTick(e) || e.stream !== opts.stream) continue;
          const now = Date.now();
          if (now - lastPrintMs < 2000) continue;
          lastPrintMs = now;
          let diskPart = "";
          if (opts.dsRoot) {
            const bytes = duSizeBytes(opts.dsRoot);
            if (bytes != null) diskPart = ` disk=${fmtBytes(bytes)}`;
          }
          // eslint-disable-next-line no-console
          console.log(
            `[disk][metrics] tail=${String(e.base?.tailOffset ?? "")} uploaded=${String(e.base?.uploadedThrough ?? "")} interpreted=${String(
              e.base?.interpretedThrough ?? ""
            )} gcThrough=${String(e.base?.gcThrough ?? "")} walOldest=${String(e.base?.walOldestOffset ?? "")} backlog=${fmtInt(
              Number(e.base?.backlogSourceOffsets ?? 0)
            )} walRows=${fmtInt(Number((e.base as any)?.walRetainedRows ?? 0))} walBytes=${fmtBytes(
              Number((e.base as any)?.walRetainedBytes ?? 0)
            )} gc.delRows=${fmtInt(Number((e.base as any)?.gc?.deletedRows ?? 0))} gc.msMax=${fmtInt(
              Number((e.base as any)?.gc?.msMax ?? 0)
            )}${diskPart}`
          );
        }
      },
    });
  }

  const producerStats =
    opts.writerRate > 0
      ? await runProducers({
          baseUrl: opts.baseUrl,
          stream: opts.stream,
          rate: opts.writerRate,
          durationSeconds: opts.durationSeconds,
          producers: opts.producers,
          batchEvents: opts.batchEvents,
          columns: opts.columns,
          rowSpace: opts.rowSpace,
          mode: opts.mode,
          signal: controller.signal,
        })
      : [];

  controller.abort();
  metricsTail?.stop();
  opts.signal.removeEventListener("abort", onAbort);
  await Promise.allSettled([metricsTail?.task].filter(Boolean) as any);

  const sent = producerStats.reduce((acc, s) => acc + s.events, 0);
  const reqs = producerStats.reduce((acc, s) => acc + s.requests, 0);
  const httpErrors = producerStats.reduce((acc, s) => acc + s.httpErrors, 0);
  // eslint-disable-next-line no-console
  console.log(`[disk] done: writer.sent=${fmtInt(sent)} writer.requests=${fmtInt(reqs)} writer.httpErrors=${fmtInt(httpErrors)}`);
}

async function runScenario3bCatchup(opts: {
  baseUrl: string;
  stream: string;
  dsRoot: string | null;
  doMetrics: boolean;
  metricsStream: string;
  maxWaitSeconds: number;
  signal: AbortSignal;
}): Promise<void> {
  // eslint-disable-next-line no-console
  console.log(`[disk] run: scenario=3b-catchup (expects interpreter enabled; waiting for backlog to fall and WAL GC to advance)`);

  const controller = new AbortController();
  const onAbort = () => controller.abort();
  opts.signal.addEventListener("abort", onAbort, { once: true });

  const deadlineMs = Date.now() + Math.max(1, opts.maxWaitSeconds) * 1000;
  let firstWalOldest: string | null = null;
  let firstWalRows: number | null = null;
  let firstGcThrough: string | null = null;
  let firstBacklog: number | null = null;

  let lastWalOldest: string | null = null;
  let lastWalRows: number | null = null;
  let lastGcThrough: string | null = null;
  let lastBacklog: number | null = null;

  let walOldestAdvanced = false;
  let alreadyAdvancedAtStart = false;
  let walRowsDecreased = false;
  let gcThroughAdvanced = false;
  let backlogReachedZero = false;
  let stopReason: string | null = null;

  let metricsTail: { stop: () => void; task: Promise<void> } | null = null;
  if (opts.doMetrics) {
    let lastPrintMs = 0;
    metricsTail = startTailJsonStream({
      baseUrl: opts.baseUrl,
      stream: opts.metricsStream,
      offset: "now",
      timeoutMs: 10_000,
      onEvents: async (events) => {
        for (const e of events) {
          if (!isLiveTick(e) || e.stream !== opts.stream) continue;
          const walOldest = (e.base?.walOldestOffset ?? null) as string | null;
          const walRows = Number((e.base as any)?.walRetainedRows ?? 0);
          const gcThrough = (e.base?.gcThrough ?? null) as string | null;
          const backlog = Number(e.base?.backlogSourceOffsets ?? 0);

          if (firstWalOldest === null) {
            firstWalOldest = walOldest;
            if (walOldest && seqOfOffset(walOldest) > 0n) alreadyAdvancedAtStart = true;
          }
          if (firstWalRows === null) firstWalRows = walRows;
          if (firstGcThrough === null) firstGcThrough = gcThrough;
          if (firstBacklog === null) firstBacklog = backlog;

          lastWalOldest = walOldest;
          lastWalRows = walRows;
          lastGcThrough = gcThrough;
          lastBacklog = backlog;

          if (backlog === 0) backlogReachedZero = true;
          if (walOldest && firstWalOldest && seqOfOffset(walOldest) > seqOfOffset(firstWalOldest)) walOldestAdvanced = true;
          if (Number.isFinite(walRows) && firstWalRows != null && walRows < firstWalRows) walRowsDecreased = true;
          if (gcThrough && firstGcThrough && seqOfOffset(gcThrough) > seqOfOffset(firstGcThrough)) gcThroughAdvanced = true;

          const now = Date.now();
          if (now - lastPrintMs < 2000) continue;
          lastPrintMs = now;

          let diskPart = "";
          if (opts.dsRoot) {
            const bytes = duSizeBytes(opts.dsRoot);
            if (bytes != null) diskPart = ` disk=${fmtBytes(bytes)}`;
          }
          // eslint-disable-next-line no-console
          console.log(
            `[disk][metrics] uploaded=${String(e.base?.uploadedThrough ?? "")} interpreted=${String(e.base?.interpretedThrough ?? "")} gcThrough=${String(
              gcThrough ?? ""
            )} walOldest=${String(walOldest ?? "")} walRows=${fmtInt(walRows)} walBytes=${fmtBytes(
              Number((e.base as any)?.walRetainedBytes ?? 0)
            )} gc.delRows=${fmtInt(Number((e.base as any)?.gc?.deletedRows ?? 0))} gc.msMax=${fmtInt(
              Number((e.base as any)?.gc?.msMax ?? 0)
            )} backlog=${fmtInt(backlog)}${diskPart}`
          );
        }
      },
    });
  }

  while (!controller.signal.aborted && Date.now() < deadlineMs) {
    await sleep(250);
    if (!opts.doMetrics) continue;
    if (firstWalOldest === null) continue; // no tick yet

    const gcResumed = walOldestAdvanced || alreadyAdvancedAtStart || walRowsDecreased || gcThroughAdvanced;
    if (backlogReachedZero && gcResumed) {
      stopReason = `backlog=0 and gcResumed (walOldestAdvanced=${walOldestAdvanced ? "yes" : "no"} alreadyAdvancedAtStart=${
        alreadyAdvancedAtStart ? "yes" : "no"
      } walRowsDecreased=${walRowsDecreased ? "yes" : "no"} gcThroughAdvanced=${gcThroughAdvanced ? "yes" : "no"})`;
      break;
    }
  }

  controller.abort();
  metricsTail?.stop();
  opts.signal.removeEventListener("abort", onAbort);
  await Promise.allSettled([metricsTail?.task].filter(Boolean) as any);

  // eslint-disable-next-line no-console
  console.log(
    `[disk] done: backlogZero=${backlogReachedZero ? "yes" : "no"} walOldestAdvanced=${walOldestAdvanced ? "yes" : "no"} alreadyAdvancedAtStart=${
      alreadyAdvancedAtStart ? "yes" : "no"
    } walRowsDecreased=${walRowsDecreased ? "yes" : "no"} gcThroughAdvanced=${gcThroughAdvanced ? "yes" : "no"}${
      stopReason ? ` stopReason=${stopReason}` : ""
    }`
  );
  // eslint-disable-next-line no-console
  console.log(
    `[disk] first: walOldest=${firstWalOldest ?? "null"} walRows=${firstWalRows ?? "null"} gcThrough=${firstGcThrough ?? "null"} backlog=${
      firstBacklog ?? "null"
    }`
  );
  // eslint-disable-next-line no-console
  console.log(
    `[disk] last: walOldest=${lastWalOldest ?? "null"} walRows=${lastWalRows ?? "null"} gcThrough=${lastGcThrough ?? "null"} backlog=${
      lastBacklog ?? "null"
    }`
  );
}

async function main(): Promise<void> {
  if (hasFlag(ARGS, "help") || hasFlag(ARGS, "h")) usage(0);

  const baseUrl = stringArg(ARGS, "url", "http://127.0.0.1:8080");
  const scenario = parseScenario(ARGS);
  const stream = stringArg(ARGS, "stream", "load.live.disk");
  const mode = parseMode(ARGS);
  const durationSeconds = intArg(ARGS, "duration-seconds", scenario === "3b-catchup" ? 0 : 60);
  const writerRateDefault = scenario === "3a" ? 500 : scenario === "3b-lag" ? 1000 : 0;
  const writerRate = Math.max(0, intArg(ARGS, "writer-rate", writerRateDefault));
  const producers = Math.max(1, intArg(ARGS, "producers", 2));
  const batchEvents = Math.max(1, intArg(ARGS, "batch-events", 200));
  const columns = Math.max(8, intArg(ARGS, "columns", 64));
  const rowSpace = Math.max(1000, intArg(ARGS, "row-space", 100_000));

  const behindMs = Math.max(0, intArg(ARGS, "behind-ms", 30_000));
  const restartPauseMs = Math.max(0, intArg(ARGS, "restart-pause-ms", 0));
  const waitTimeoutMs = Math.max(0, intArg(ARGS, "wait-timeout-ms", 5000));
  const metaEveryMs = Math.max(250, intArg(ARGS, "meta-every-ms", 2000));
  const ttlMs = Math.max(0, intArg(ARGS, "ttl-ms", 60 * 60 * 1000));
  const maxWaitSeconds = Math.max(1, intArg(ARGS, "max-wait-seconds", 120));

  const doSetup = !hasFlag(ARGS, "no-setup");
  const doReset = hasFlag(ARGS, "reset");
  const doMetrics = !hasFlag(ARGS, "no-metrics");
  const metricsStream = stringArg(ARGS, "metrics-stream", "live.metrics");
  const dsRoot = optionalStringArg(ARGS, "ds-root");
  const doActivateTemplates = scenario === "3a" ? !hasFlag(ARGS, "no-activate-templates") : !hasFlag(ARGS, "no-activate-templates") && hasFlag(ARGS, "activate-templates");

  const globalController = new AbortController();
  const onSigInt = () => globalController.abort();
  process.on("SIGINT", onSigInt);
  process.on("SIGTERM", onSigInt);

  if (doReset) {
    const derived = defaultTouchStreamName(stream);
    // eslint-disable-next-line no-console
    console.log(`[disk] reset: deleting ${stream} and ${derived}`);
    await deleteStream(baseUrl, stream);
    await deleteStream(baseUrl, derived);
  }

  if (doSetup) {
    // eslint-disable-next-line no-console
    console.log(`[disk] setup: ensuring stream + interpreter config (touch enabled)`);
    await setupStream(baseUrl, stream, ARGS, scenario);
  } else {
    // eslint-disable-next-line no-console
    console.log(`[disk] setup: skipped (--no-setup)`);
    await waitForTouchReady(baseUrl, stream);
  }

  if (scenario === "3a") {
    await runScenario3a({
      baseUrl,
      stream,
      mode,
      durationSeconds,
      writerRate,
      producers,
      batchEvents,
      columns,
      rowSpace,
      behindMs,
      restartPauseMs,
      waitTimeoutMs,
      metaEveryMs,
      ttlMs,
      dsRoot,
      doMetrics,
      metricsStream,
      doActivateTemplates,
      signal: globalController.signal,
    });
  } else if (scenario === "3b-lag") {
    await runScenario3bLag({
      baseUrl,
      stream,
      mode,
      durationSeconds,
      writerRate,
      producers,
      batchEvents,
      columns,
      rowSpace,
      dsRoot,
      doMetrics,
      metricsStream,
      signal: globalController.signal,
    });
  } else {
    await runScenario3bCatchup({
      baseUrl,
      stream,
      dsRoot,
      doMetrics,
      metricsStream,
      maxWaitSeconds,
      signal: globalController.signal,
    });
  }
}

main().catch((e) => {
  // eslint-disable-next-line no-console
  console.error(`[disk] fatal:`, e);
  process.exit(1);
});
