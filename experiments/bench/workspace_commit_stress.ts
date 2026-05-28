/**
 * workspace-fs commit stress benchmark.
 *
 * Default workload:
 *   - 1000 individual workspace-fs commits
 *   - 100000 file edits total
 *   - 1 GiB target written to the object store
 *   - MockR2 PUT delay of 50ms
 *
 * Usage:
 *   bun run experiments/bench/workspace_commit_stress.ts
 *   bun run experiments/bench/workspace_commit_stress.ts --commits 10 --files 1000 --target-object-store-bytes 64mb
 */

import { Buffer } from "node:buffer";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../../src/app";
import { loadConfig } from "../../src/config";
import { formatBytes } from "../../src/memory";
import { MockR2Store } from "../../src/objectstore/mock_r2";
import { openWorkspaceFsRepo, WorkspaceFsClientError, type WorkspaceFsFetch } from "../../src/workspace_fs";
import type { WorkspaceFsWorkspaceOpInput } from "../../src/workspace_fs/types";
import { dsError } from "../../src/util/ds_error.ts";

type BenchOptions = {
  commits: number;
  files: number;
  targetObjectStoreBytes: number;
  mockPutDelayMs: number;
  progressEvery: number;
  persistenceTimeoutMs: number;
  filesPerDir: number;
  segmentMaxBytes: number;
  uploadConcurrency: number;
  backgroundIntervalMs: number;
  mockMaxInMemoryBytes: number;
  retainMockGitBlobBodies: boolean;
  keepRoot: boolean;
};

type BenchStats = ReturnType<MockR2Store["stats"]>;

const ARGS = process.argv.slice(2);
const DEFAULT_TARGET_BYTES = 1024 ** 3;

function argValue(flag: string): string | null {
  const idx = ARGS.indexOf(flag);
  if (idx === -1) return null;
  return ARGS[idx + 1] ?? null;
}

function hasFlag(flag: string): boolean {
  return ARGS.includes(flag);
}

function parseNumberArg(flag: string, def: number): number {
  const raw = argValue(flag);
  if (raw == null) return def;
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) throw dsError(`invalid ${flag}: ${raw}`);
  return Math.floor(value);
}

function parseBytesLiteral(raw: string): number | null {
  const trimmed = raw.trim();
  const plain = Number(trimmed);
  if (Number.isFinite(plain) && plain >= 0) return Math.floor(plain);
  const match = trimmed.match(/^([0-9]+(?:\.[0-9]+)?)\s*(b|kb|kib|mb|mib|gb|gib)$/i);
  if (!match) return null;
  const value = Number(match[1]);
  if (!Number.isFinite(value) || value < 0) return null;
  const unit = match[2].toLowerCase();
  const mult = unit === "b"
    ? 1
    : unit === "kb" || unit === "kib"
      ? 1024
      : unit === "mb" || unit === "mib"
        ? 1024 ** 2
        : 1024 ** 3;
  return Math.floor(value * mult);
}

function parseBytesArg(flag: string, def: number): number {
  const raw = argValue(flag);
  if (raw == null) return def;
  const parsed = parseBytesLiteral(raw);
  if (parsed == null || parsed <= 0) throw dsError(`invalid ${flag}: ${raw}`);
  return parsed;
}

function parseOptions(): BenchOptions {
  const commits = parseNumberArg("--commits", 1000);
  const files = parseNumberArg("--files", 100000);
  return {
    commits,
    files,
    targetObjectStoreBytes: parseBytesArg("--target-object-store-bytes", DEFAULT_TARGET_BYTES),
    mockPutDelayMs: parseNumberArg("--mock-put-delay-ms", 50),
    progressEvery: parseNumberArg("--progress-every", 50),
    persistenceTimeoutMs: parseNumberArg("--persistence-timeout-ms", 15 * 60_000),
    filesPerDir: parseNumberArg("--files-per-dir", Math.max(1, Math.ceil(files / commits))),
    segmentMaxBytes: parseBytesArg("--segment-max-bytes", 16 * 1024 * 1024),
    uploadConcurrency: parseNumberArg("--upload-concurrency", 4),
    backgroundIntervalMs: parseNumberArg("--background-interval-ms", 60_000),
    mockMaxInMemoryBytes: parseBytesArg("--mock-max-in-memory-bytes", 64 * 1024),
    retainMockGitBlobBodies: hasFlag("--retain-mock-git-blob-bodies"),
    keepRoot: hasFlag("--keep-root"),
  };
}

function elapsedMs(start: bigint): number {
  return Number(process.hrtime.bigint() - start) / 1_000_000;
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms.toFixed(0)}ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(2)}s`;
  const minutes = Math.floor(seconds / 60);
  const rest = seconds - minutes * 60;
  return `${minutes}m ${rest.toFixed(1)}s`;
}

function fmtRate(count: number, ms: number, unit: string): string {
  if (ms <= 0) return `0 ${unit}/s`;
  return `${(count / (ms / 1000)).toFixed(2)} ${unit}/s`;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function appFetch(app: ReturnType<typeof createApp>): WorkspaceFsFetch {
  return (input, init) => app.fetch(new Request(input, init));
}

async function fetchJson(app: ReturnType<typeof createApp>, url: string, init: RequestInit): Promise<unknown> {
  const res = await app.fetch(new Request(url, init));
  const text = await res.text();
  if (!res.ok) throw dsError(`${init.method ?? "GET"} ${url} failed: ${res.status} ${text}`);
  return text === "" ? null : JSON.parse(text);
}

async function installGitRepoProfile(app: ReturnType<typeof createApp>, stream: string): Promise<void> {
  const base = `http://local/v1/stream/${encodeURIComponent(stream)}`;
  const create = await app.fetch(new Request(base, {
    method: "PUT",
    headers: { "content-type": "application/json" },
  }));
  if (!create.ok) throw dsError(`git-repo stream create failed: ${create.status} ${await create.text()}`);
  await fetchJson(app, `${base}/_profile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      apiVersion: "durable.streams/profile/v1",
      profile: { kind: "git-repo", version: 1, objectFormat: "sha1", defaultBranch: "main" },
    }),
  });
}

function fillPayload(fileIndex: number, size: number): Buffer {
  const out = Buffer.allocUnsafe(size);
  let state = (0x9e3779b9 ^ fileIndex) >>> 0;
  for (let i = 0; i < out.length; i++) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    out[i] = state & 0xff;
  }
  return out;
}

function stressPath(fileIndex: number, filesPerDir: number): string {
  const dirIndex = Math.floor(fileIndex / filesPerDir);
  return `/bucket-${String(dirIndex).padStart(6, "0")}/file-${String(fileIndex).padStart(6, "0")}.bin`;
}

function fileRangeForCommit(commitIndex: number, commits: number, files: number): { start: number; end: number } {
  const start = Math.floor((commitIndex * files) / commits);
  const end = Math.floor(((commitIndex + 1) * files) / commits);
  return { start, end };
}

function buildOps(start: number, end: number, payloadBytesPerFile: number, filesPerDir: number): WorkspaceFsWorkspaceOpInput[] {
  const ops: WorkspaceFsWorkspaceOpInput[] = [];
  for (let fileIndex = start; fileIndex < end; fileIndex++) {
    ops.push({
      kind: "put-file",
      path: stressPath(fileIndex, filesPerDir),
      contentBase64: fillPayload(fileIndex, payloadBytesPerFile).toString("base64"),
      contentType: "application/octet-stream",
    });
  }
  return ops;
}

async function waitForObjectStoreTarget(
  store: MockR2Store,
  targetBytes: number,
  timeoutMs: number
): Promise<BenchStats> {
  const deadline = Date.now() + timeoutMs;
  let stats = store.stats();
  while (stats.putBytes < targetBytes && Date.now() < deadline) {
    await sleep(250);
    stats = store.stats();
  }
  return stats;
}

function logProgress(
  label: string,
  start: bigint,
  commits: number,
  files: number,
  logicalBytes: number,
  stats: BenchStats
): void {
  const ms = elapsedMs(start);
  console.log(
    [
      label,
      `elapsed=${formatDuration(ms)}`,
      `commits=${commits}`,
      `files=${files}`,
      `logical=${formatBytes(logicalBytes)}`,
      `objectStore=${formatBytes(stats.putBytes)}`,
      `puts=${stats.puts}`,
      `commitRate=${fmtRate(commits, ms, "commits")}`,
    ].join(" ")
  );
}

async function run(): Promise<void> {
  const opts = parseOptions();
  const root = mkdtempSync(join(tmpdir(), "ds-workspace-commit-stress-"));
  const payloadBytesPerFile = Math.max(1, Math.ceil(opts.targetObjectStoreBytes / opts.files));
  const store = new MockR2Store({
    faults: { putDelayMs: opts.mockPutDelayMs },
    maxInMemoryBytes: opts.mockMaxInMemoryBytes,
    spillDir: join(root, "mock-r2"),
    discardGitBlobBodies: !opts.retainMockGitBlobBodies,
  });
  const cfg = {
    ...loadConfig(),
    rootDir: root,
    dbPath: join(root, "wal.sqlite"),
    segmentMaxBytes: opts.segmentMaxBytes,
    blockMaxBytes: Math.min(opts.segmentMaxBytes, 256 * 1024),
    segmentCheckIntervalMs: opts.backgroundIntervalMs,
    segmentMaxIntervalMs: opts.backgroundIntervalMs,
    uploadIntervalMs: opts.backgroundIntervalMs,
    uploadConcurrency: opts.uploadConcurrency,
    metricsFlushIntervalMs: 0,
  };
  const app = createApp(cfg, store);
  const suffix = `stress-${Date.now()}`;
  const gitStream = `git/bench/${suffix}`;
  const workspaceStream = `workspace/bench/${suffix}/control`;
  const repo = openWorkspaceFsRepo({
    streamsUrl: "http://local",
    stream: workspaceStream,
    fetch: appFetch(app),
  });
  const started = process.hrtime.bigint();
  let committedFiles = 0;
  let logicalBytes = 0;
  let head: string | null = null;
  let activeCommit = 0;
  let activeStage = "setup";
  let checkoutMs = 0;
  let appendOpsMs = 0;
  let commitMs = 0;
  let commitOpsMs = 0;

  console.log("workspace-fs commit stress benchmark");
  console.log(`root=${root}`);
  console.log(`gitStream=${gitStream}`);
  console.log(`workspaceStream=${workspaceStream}`);
  console.log(`commits=${opts.commits}`);
  console.log(`files=${opts.files}`);
  console.log(`payloadBytesPerFile=${payloadBytesPerFile}`);
  console.log(`targetObjectStoreBytes=${formatBytes(opts.targetObjectStoreBytes)}`);
  console.log(`mockPutDelayMs=${opts.mockPutDelayMs}`);
  console.log(`mockMaxInMemoryBytes=${formatBytes(opts.mockMaxInMemoryBytes)}`);
  console.log(`retainMockGitBlobBodies=${opts.retainMockGitBlobBodies}`);
  console.log(`segmentMaxBytes=${formatBytes(opts.segmentMaxBytes)}`);
  console.log(`uploadConcurrency=${opts.uploadConcurrency}`);
  console.log(`backgroundIntervalMs=${opts.backgroundIntervalMs}`);

  try {
    await installGitRepoProfile(app, gitStream);
    await repo.ensure({ gitRepoStream: gitStream });
    for (let commitIndex = 0; commitIndex < opts.commits; commitIndex++) {
      activeCommit = commitIndex + 1;
      const { start, end } = fileRangeForCommit(commitIndex, opts.commits, opts.files);
      const workspaceId = `stress-${commitIndex + 1}`;
      const ops = buildOps(start, end, payloadBytesPerFile, opts.filesPerDir);
      activeStage = "commit-ops";
      const commitOpsStart = process.hrtime.bigint();
      const result = await repo.commitOps(ops, {
        workspaceId,
        ref: "main",
        expectedHead: head,
        message: `stress commit ${commitIndex + 1}`,
        author: { id: "workspace-stress" },
      });
      commitOpsMs += elapsedMs(commitOpsStart);
      head = result.newCommitId;
      committedFiles += ops.length;
      logicalBytes += ops.length * payloadBytesPerFile;

      const completed = commitIndex + 1;
      if (completed === 1 || completed % opts.progressEvery === 0 || completed === opts.commits) {
        logProgress(`progress=${completed}/${opts.commits}`, started, completed, committedFiles, logicalBytes, store.stats());
      }
    }

    const afterCommitsMs = elapsedMs(started);
    activeStage = "wait-object-store-target";
    const finalStats = await waitForObjectStoreTarget(store, opts.targetObjectStoreBytes, opts.persistenceTimeoutMs);
    const totalMs = elapsedMs(started);
    if (finalStats.putBytes < opts.targetObjectStoreBytes) {
      throw dsError(
        `object store target not reached: wrote ${formatBytes(finalStats.putBytes)} of ${formatBytes(opts.targetObjectStoreBytes)} after ${formatDuration(totalMs)}`
      );
    }

    console.log("summary");
    console.log(`commitPhase=${formatDuration(afterCommitsMs)}`);
    console.log(`totalUntilTarget=${formatDuration(totalMs)}`);
    console.log(`commits=${opts.commits}`);
    console.log(`files=${committedFiles}`);
    console.log(`logicalFileBytes=${formatBytes(logicalBytes)}`);
    console.log(`objectStorePutBytes=${formatBytes(finalStats.putBytes)}`);
    console.log(`objectStorePuts=${finalStats.puts}`);
    console.log(`objectStoreMaxConcurrentPuts=${finalStats.maxConcurrentPuts}`);
    console.log(`objectStoreMemoryBytes=${formatBytes(finalStats.memoryBytes)}`);
    console.log(`checkoutTotal=${formatDuration(checkoutMs)}`);
    console.log(`appendOpsTotal=${formatDuration(appendOpsMs)}`);
    console.log(`commitTotal=${formatDuration(commitMs)}`);
    console.log(`commitOpsTotal=${formatDuration(commitOpsMs)}`);
    console.log(`checkoutAvg=${formatDuration(checkoutMs / opts.commits)}`);
    console.log(`appendOpsAvg=${formatDuration(appendOpsMs / opts.commits)}`);
    console.log(`commitAvg=${formatDuration(commitMs / opts.commits)}`);
    console.log(`commitOpsAvg=${formatDuration(commitOpsMs / opts.commits)}`);
    console.log(`commitRate=${fmtRate(opts.commits, afterCommitsMs, "commits")}`);
    console.log(`objectStoreWriteRate=${fmtRate(finalStats.putBytes, totalMs, "bytes")}`);
  } catch (error) {
    const stats = store.stats();
    console.error("failure");
    console.error(`failedAtCommit=${activeCommit}/${opts.commits}`);
    console.error(`failedStage=${activeStage}`);
    console.error(`elapsed=${formatDuration(elapsedMs(started))}`);
    console.error(`committedFiles=${committedFiles}`);
    console.error(`logicalFileBytes=${formatBytes(logicalBytes)}`);
    console.error(`objectStorePutBytes=${formatBytes(stats.putBytes)}`);
    console.error(`objectStorePuts=${stats.puts}`);
    console.error(`objectStoreMaxConcurrentPuts=${stats.maxConcurrentPuts}`);
    console.error(`checkoutTotal=${formatDuration(checkoutMs)}`);
    console.error(`appendOpsTotal=${formatDuration(appendOpsMs)}`);
    console.error(`commitTotal=${formatDuration(commitMs)}`);
    console.error(`commitOpsTotal=${formatDuration(commitOpsMs)}`);
    if (error instanceof WorkspaceFsClientError) {
      console.error(`workspaceStatus=${error.status}`);
      console.error(`workspaceMessage=${error.message}`);
    }
    throw error;
  } finally {
    app.close();
    if (!opts.keepRoot) rmSync(root, { recursive: true, force: true });
  }
}

await run();
