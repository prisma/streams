import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../../src/app";
import { loadConfig, type Config } from "../../src/config";
import { MockR2Store } from "../../src/objectstore/mock_r2";
import { buildGhArchiveSchemaUpdate, type GhArchiveDemoEvent } from "../demo/gharchive_demo.ts";
import { dsError } from "../../src/util/ds_error.ts";

const STREAM = "segment-companion-memory";
const TARGET_SEGMENT_BYTES = argValue("target-segment-bytes", 16 * 1024 * 1024);
const APPEND_TARGET_BYTES = argValue("append-target-bytes", Math.floor(TARGET_SEGMENT_BYTES * 1.08));
const BATCH_ROWS = argValue("batch-rows", 200);
const ROW_TOKEN_COUNT = argValue("row-token-count", 96);
const BODY_TOKEN_COUNT = argValue("body-token-count", 192);

function argValue(name: string, def: number): number {
  const prefix = `--${name}=`;
  for (const arg of Bun.argv.slice(2)) {
    if (!arg.startsWith(prefix)) continue;
    const value = Number(arg.slice(prefix.length));
    if (Number.isFinite(value) && value > 0) return Math.floor(value);
    throw dsError(`invalid --${name}: ${arg.slice(prefix.length)}`);
  }
  return def;
}

function makeConfig(rootDir: string, overrides: Partial<Config>): Config {
  const base = loadConfig();
  return {
    ...base,
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    ...overrides,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function createStreamAndSchema(app: ReturnType<typeof createApp>): Promise<void> {
  let res = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
    })
  );
  if (![200, 201].includes(res.status)) {
    throw dsError(`stream create failed: ${res.status} ${await res.text()}`);
  }

  res = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_profile`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        apiVersion: "durable.streams/profile/v1",
        profile: { kind: "generic" },
      }),
    })
  );
  if (res.status !== 200) {
    throw dsError(`profile install failed: ${res.status} ${await res.text()}`);
  }

  res = await app.fetch(
    new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}/_schema`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(buildGhArchiveSchemaUpdate()),
    })
  );
  if (res.status !== 200) {
    throw dsError(`schema install failed: ${res.status} ${await res.text()}`);
  }
}

function makeTokenSeries(prefix: string, index: number, count: number): string {
  const parts: string[] = [];
  for (let i = 0; i < count; i++) {
    const bucket = (index * 131 + i * 17) % 8192;
    parts.push(`${prefix}-${bucket.toString(36)}-${index.toString(36)}-${i.toString(36)}`);
  }
  return parts.join(" ");
}

function makeSyntheticEvent(index: number): GhArchiveDemoEvent {
  const owner = `owner-${(index % 2048).toString(36)}`;
  const repoName = `${owner}/repo-${(index % 16384).toString(36)}`;
  const action = index % 5 === 0 ? "opened" : index % 5 === 1 ? "created" : index % 5 === 2 ? "edited" : index % 5 === 3 ? "synchronize" : "closed";
  const refType = index % 3 === 0 ? "branch" : index % 3 === 1 ? "tag" : "repository";
  const title = `Synthetic GH title ${index} ${makeTokenSeries("ttl", index, Math.max(8, Math.floor(ROW_TOKEN_COUNT / 6)))}`;
  const message = `Synthetic GH message ${index} ${owner} ${repoName} ${makeTokenSeries("msg", index, ROW_TOKEN_COUNT)}`;
  const body = `Synthetic GH body ${index} ${makeTokenSeries("body", index, BODY_TOKEN_COUNT)}`;
  const eventTime = new Date(Date.UTC(2026, 0, 1, 0, 0, 0, 0) + index * 1000).toISOString();
  const payloadBytes = title.length + message.length + body.length;
  return {
    action,
    actorLogin: `actor-${(index % 4096).toString(36)}`,
    archiveHour: "2026-01-01-00",
    body,
    commitCount: (index % 12) + 1,
    eventTime,
    eventType: index % 6 === 0 ? "IssuesEvent" : "PushEvent",
    ghArchiveId: `gh-${index}`,
    isBot: index % 17 === 0,
    message,
    orgLogin: index % 4 === 0 ? `org-${(index % 256).toString(36)}` : null,
    payloadBytes,
    payloadKb: Number((payloadBytes / 1024).toFixed(3)),
    public: index % 9 !== 0,
    refType,
    repoName,
    repoOwner: owner,
    title,
  };
}

async function appendUntilTarget(app: ReturnType<typeof createApp>, targetBytes: number): Promise<{ rows: number; payloadBytes: number }> {
  let totalRows = 0;
  let totalBytes = 0;
  const encoder = new TextEncoder();
  while (totalBytes < targetBytes) {
    const rows: string[] = [];
    let batchBytes = 2;
    while (rows.length < BATCH_ROWS && totalBytes < targetBytes) {
      const event = makeSyntheticEvent(totalRows);
      const serialized = JSON.stringify(event);
      const serializedBytes = encoder.encode(serialized).byteLength;
      if (rows.length > 0 && batchBytes + serializedBytes + 1 > 8 * 1024 * 1024) break;
      rows.push(serialized);
      batchBytes += serializedBytes + 1;
      totalRows += 1;
      totalBytes += serializedBytes;
    }
    const res = await app.fetch(
      new Request(`http://local/v1/stream/${encodeURIComponent(STREAM)}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: `[${rows.join(",")}]`,
      })
    );
    if (res.status !== 204) {
      throw dsError(`append failed: ${res.status} ${await res.text()}`);
    }
  }
  return { rows: totalRows, payloadBytes: totalBytes };
}

async function waitForCompanion(app: ReturnType<typeof createApp>, timeoutMs = 300_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const stream = app.deps.db.getStream(STREAM);
    if (
      stream &&
      stream.uploaded_segment_count >= 1 &&
      app.deps.db.listSearchSegmentCompanions(STREAM).length >= 1
    ) {
      return;
    }
    app.deps.indexer?.enqueue(STREAM);
    await sleep(25);
  }
  const stream = app.deps.db.getStream(STREAM);
  throw dsError(
    `timeout waiting for first companion (uploaded_segments=${stream?.uploaded_segment_count ?? 0} segment_count=${stream?.segment_count ?? 0} logical_size_bytes=${stream?.logical_size_bytes?.toString() ?? "0"})`
  );
}

type SamplerSummary = {
  peakRss: number;
  peakHeap: number;
  peakCompanionRss: number;
  phasePeaks: Record<string, number>;
  lastProgress: Record<string, unknown> | null;
};

function summarizeSampler(path: string): SamplerSummary {
  const summary: SamplerSummary = {
    peakRss: 0,
    peakHeap: 0,
    peakCompanionRss: 0,
    phasePeaks: Object.create(null) as Record<string, number>,
    lastProgress: null,
  };
  if (!existsSync(path)) return summary;
  const lines = readFileSync(path, "utf8").trim().split("\n").filter((line) => line.length > 0);
  for (const line of lines) {
    const parsed = JSON.parse(line) as Record<string, any>;
    if (parsed.kind === "sample") {
      const rss = Number(parsed.process_memory_usage?.rss ?? 0);
      const heap = Number(parsed.process_memory_usage?.heapUsed ?? 0);
      summary.peakRss = Math.max(summary.peakRss, rss);
      summary.peakHeap = Math.max(summary.peakHeap, heap);
      const activeLabels = Array.isArray(parsed.active_phases)
        ? parsed.active_phases
            .map((phase: Record<string, unknown>) => String(phase.label ?? ""))
            .filter((label: string) => label.length > 0)
        : [];
      if (activeLabels.includes("companion")) summary.peakCompanionRss = Math.max(summary.peakCompanionRss, rss);
      for (const label of activeLabels) {
        summary.phasePeaks[label] = Math.max(summary.phasePeaks[label] ?? 0, rss);
      }
    } else if (parsed.kind === "marker" && parsed.reason === "companion_progress") {
      summary.lastProgress = parsed.data ?? null;
    }
  }
  return summary;
}

async function main(): Promise<void> {
  const root = mkdtempSync(join(tmpdir(), "ds-segment-companion-memory-"));
  const samplerPath = `${root}/sampler.jsonl`;
  const store = new MockR2Store({
    maxInMemoryBytes: 0,
    spillDir: `${root}/mock-r2`,
  });
  const cfg = makeConfig(root, {
    segmentMaxBytes: TARGET_SEGMENT_BYTES,
    segmentTargetRows: 250_000,
    segmentMaxIntervalMs: 0,
    segmentCheckIntervalMs: 10,
    uploadIntervalMs: 10,
    uploadConcurrency: 1,
    indexCheckIntervalMs: 10,
    indexL0SpanSegments: 16,
    indexBuildConcurrency: 1,
    searchCompanionBuildBatchSegments: 1,
    searchCompanionYieldBlocks: 1,
    segmentCacheMaxBytes: 0,
    segmentFooterCacheEntries: 0,
    memorySamplerPath: samplerPath,
    memorySamplerIntervalMs: 100,
    segmenterWorkers: 0,
  });

  const app = createApp(cfg, store);
  try {
    await createStreamAndSchema(app);
    const appendSummary = await appendUntilTarget(app, APPEND_TARGET_BYTES);
    await waitForCompanion(app);

    const stream = app.deps.db.getStream(STREAM);
    const segment = app.deps.db.getSegmentByIndex(STREAM, 0);
    const companion = app.deps.db.getSearchSegmentCompanion(STREAM, 0);
    if (!stream || !segment || !companion) throw dsError("missing stream state after companion build");
    app.close();

    const sampler = summarizeSampler(samplerPath);
    const output = {
      targetSegmentBytes: TARGET_SEGMENT_BYTES,
      appendTargetBytes: APPEND_TARGET_BYTES,
      rowsAppended: appendSummary.rows,
      appendedPayloadBytes: appendSummary.payloadBytes,
      logicalSizeBytes: stream.logical_size_bytes.toString(),
      uploadedSegmentCount: stream.uploaded_segment_count,
      firstSegmentBytes: segment.size_bytes,
      firstSegmentRows: (segment.end_offset - segment.start_offset + 1n).toString(),
      companionBytes: companion.size_bytes,
      peakRss: sampler.peakRss,
      peakHeap: sampler.peakHeap,
      peakCompanionRss: sampler.peakCompanionRss,
      phasePeaks: sampler.phasePeaks,
      lastProgress: sampler.lastProgress,
      objectStoreMemoryBytes: store.memoryBytes(),
      samplerPath,
    };
    console.log(JSON.stringify(output, null, 2));
  } finally {
    app.close();
    rmSync(root, { recursive: true, force: true });
  }
}

try {
  await main();
  process.exit(0);
} catch (error: unknown) {
  console.error(error);
  process.exit(1);
}
