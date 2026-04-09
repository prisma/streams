import { randomBytes } from "node:crypto";
import { mkdtempSync, rmSync, statSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { zstdCompressSync, zstdDecompressSync } from "node:zlib";

export const BUN_NATIVE_API_RSS_SCENARIOS = [
  "zstd_compress_sync",
  "zstd_decompress_sync",
  "bun_file_bytes",
  "bun_file_arraybuffer",
  "bun_write_from_bun_file",
  "bun_mmap_shared",
  "s3_put_fetch_blob_buffer",
  "s3_put_native_buffer",
  "s3_put_fetch_bun_file",
  "s3_put_native_bun_file",
  "s3_get_arraybuffer",
  "s3_get_bun_write",
] as const;

export type BunNativeApiRssScenario = (typeof BUN_NATIVE_API_RSS_SCENARIOS)[number];

export type BunNativeApiRssMeasurement = {
  scenario: BunNativeApiRssScenario;
  productionUse: string;
  iterations: number;
  payloadBytes: number;
  baselineRssBytes: number;
  peakRssBytes: number;
  settledRssBytes: number;
  peakContributedRssBytes: number;
  settledContributedRssBytes: number;
  baselineHeapUsedBytes: number;
  peakHeapUsedBytes: number;
  settledHeapUsedBytes: number;
  baselineExternalBytes: number;
  peakExternalBytes: number;
  settledExternalBytes: number;
  baselineArrayBuffersBytes: number;
  peakArrayBuffersBytes: number;
  settledArrayBuffersBytes: number;
  peakAccountedBytes: number;
  settledAccountedBytes: number;
  peakUnaccountedBytes: number;
  settledUnaccountedBytes: number;
  durationMs: number;
};

type ScenarioConfig = {
  productionUse: string;
  iterations: number;
  payloadBytes: number;
  requiresS3: boolean;
};

type MemorySnapshot = ReturnType<typeof process.memoryUsage>;

const CONTENT_TYPE = "application/octet-stream";

const SCENARIO_CONFIG: Record<BunNativeApiRssScenario, ScenarioConfig> = {
  zstd_compress_sync: {
    productionUse: "node:zlib zstdCompressSync in segment/format.ts and metrics block encoding",
    iterations: 48,
    payloadBytes: 8 * 1024 * 1024,
    requiresS3: false,
  },
  zstd_decompress_sync: {
    productionUse: "node:zlib zstdDecompressSync in segment/format.ts and bootstrap.ts",
    iterations: 48,
    payloadBytes: 8 * 1024 * 1024,
    requiresS3: false,
  },
  bun_file_bytes: {
    productionUse: "Bun.file(path).bytes() fallback in run_payload_upload.ts",
    iterations: 48,
    payloadBytes: 8 * 1024 * 1024,
    requiresS3: false,
  },
  bun_file_arraybuffer: {
    productionUse: "Bun file/body arrayBuffer paths mirrored by R2 get()",
    iterations: 48,
    payloadBytes: 8 * 1024 * 1024,
    requiresS3: false,
  },
  bun_write_from_bun_file: {
    productionUse: "Bun.write(path, Bun.file(...)) analogous to local getFile/get-by-stream paths",
    iterations: 32,
    payloadBytes: 8 * 1024 * 1024,
    requiresS3: false,
  },
  bun_mmap_shared: {
    productionUse: "Bun.mmap shared segment cache paths in segment/cache.ts and cached_segment.ts",
    iterations: 64,
    payloadBytes: 8 * 1024 * 1024,
    requiresS3: false,
  },
  s3_put_fetch_blob_buffer: {
    productionUse: "R2 putWithFetch(new Blob([Buffer.from(data)])) path in r2.ts put()",
    iterations: 24,
    payloadBytes: 8 * 1024 * 1024,
    requiresS3: true,
  },
  s3_put_native_buffer: {
    productionUse: "R2 putNoEtag(Buffer.from(data)) path in r2.ts",
    iterations: 24,
    payloadBytes: 8 * 1024 * 1024,
    requiresS3: true,
  },
  s3_put_fetch_bun_file: {
    productionUse: "R2 putFile via presigned fetch with Bun.file(path) body",
    iterations: 24,
    payloadBytes: 8 * 1024 * 1024,
    requiresS3: true,
  },
  s3_put_native_bun_file: {
    productionUse: "R2 putFileNoEtag via file.write(Bun.file(path))",
    iterations: 24,
    payloadBytes: 8 * 1024 * 1024,
    requiresS3: true,
  },
  s3_get_arraybuffer: {
    productionUse: "R2 get() path using file.arrayBuffer() then Uint8Array copy",
    iterations: 24,
    payloadBytes: 8 * 1024 * 1024,
    requiresS3: true,
  },
  s3_get_bun_write: {
    productionUse: "R2 getFile() path using Bun.write(path, body)",
    iterations: 24,
    payloadBytes: 8 * 1024 * 1024,
    requiresS3: true,
  },
};

function captureMemorySnapshot(): MemorySnapshot {
  return process.memoryUsage();
}

function maybeForceGc(): void {
  const maybeGc = (Bun as unknown as { gc?: (full?: boolean) => void }).gc;
  if (typeof maybeGc !== "function") return;
  try {
    maybeGc(true);
  } catch {
    // best effort only
  }
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function buildPayloadBytes(size: number): Uint8Array {
  const bytes = new Uint8Array(size);
  const seed = randomBytes(256);
  for (let i = 0; i < bytes.byteLength; i += 1) bytes[i] = seed[i % seed.byteLength]!;
  return bytes;
}

function requireS3Env(): {
  endpoint: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
} {
  const endpoint = process.env.S3_TEST_ENDPOINT;
  const bucket = process.env.S3_TEST_BUCKET;
  const accessKeyId = process.env.S3_TEST_ACCESS_KEY_ID;
  const secretAccessKey = process.env.S3_TEST_SECRET_ACCESS_KEY;
  if (!endpoint || !bucket || !accessKeyId || !secretAccessKey) {
    throw new Error("missing S3_TEST_* environment for bun native api rss scenarios");
  }
  return { endpoint, bucket, accessKeyId, secretAccessKey };
}

function isRetryableS3MicrobenchError(error: unknown): boolean {
  const message = String((error as { message?: unknown } | null | undefined)?.message ?? error ?? "");
  const code = String((error as { code?: unknown } | null | undefined)?.code ?? "");
  return code === "EAGAIN" || message.includes("EAGAIN") || message.includes("socket") || message.includes("timed out");
}

async function withS3Retries<T>(work: () => Promise<T>): Promise<T> {
  let lastError: unknown = null;
  for (let attempt = 0; attempt < 5; attempt += 1) {
    try {
      return await work();
    } catch (error) {
      lastError = error;
      if (!isRetryableS3MicrobenchError(error) || attempt === 4) break;
      await sleep(50);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError ?? "S3 micro-benchmark failed"));
}

function makeS3Client(): Bun.S3Client {
  const env = requireS3Env();
  return new Bun.S3Client({
    bucket: env.bucket,
    accessKeyId: env.accessKeyId,
    secretAccessKey: env.secretAccessKey,
    endpoint: env.endpoint,
    region: "us-east-1",
  });
}

function createTempRoot(prefix: string): string {
  return mkdtempSync(join(tmpdir(), prefix));
}

async function measureScenario(
  scenario: BunNativeApiRssScenario,
  run: () => void | Promise<void>
): Promise<BunNativeApiRssMeasurement> {
  const cfg = SCENARIO_CONFIG[scenario];
  maybeForceGc();
  await sleep(25);
  const baseline = captureMemorySnapshot();
  const samplerRoot = createTempRoot(`ds-bun-native-rss-sampler-${scenario}-`);
  const stopPath = join(samplerRoot, "stop");
  const outputPath = join(samplerRoot, "result.json");
  const env: Record<string, string> = {};
  for (const [name, value] of Object.entries(process.env)) {
    if (value != null) env[name] = value;
  }
  const sampler = Bun.spawn({
    cmd: ["bun", "run", "test/helpers/process_tree_rss_sampler.ts", String(process.pid), stopPath, outputPath],
    cwd: process.cwd(),
    env,
    stdout: "ignore",
    stderr: "pipe",
  });
  const startedAt = performance.now();
  try {
    await run();
  } finally {
    writeFileSync(stopPath, "");
  }
  const durationMs = performance.now() - startedAt;
  const samplerExitCode = await sampler.exited;
  if (samplerExitCode !== 0) {
    const stderr = sampler.stderr ? await new Response(sampler.stderr).text() : "";
    throw new Error(`bun native rss sampler failed scenario=${scenario} exit=${samplerExitCode}\n${stderr}`);
  }
  const peakRssBytes =
    (JSON.parse(await Bun.file(outputPath).text()) as { peakRssBytes: number | null }).peakRssBytes ?? baseline.rss;
  const afterRun = captureMemorySnapshot();
  maybeForceGc();
  await sleep(250);
  const settled = captureMemorySnapshot();
  rmSync(samplerRoot, { recursive: true, force: true });

  const peakMeasuredRss = Math.max(peakRssBytes, afterRun.rss, settled.rss);
  const peakContributedRssBytes = Math.max(0, peakMeasuredRss - baseline.rss);
  const settledContributedRssBytes = Math.max(0, settled.rss - baseline.rss);
  const peakAccountedBytes =
    Math.max(0, afterRun.heapUsed - baseline.heapUsed) + Math.max(0, afterRun.external - baseline.external);
  const settledAccountedBytes =
    Math.max(0, settled.heapUsed - baseline.heapUsed) + Math.max(0, settled.external - baseline.external);

  return {
    scenario,
    productionUse: cfg.productionUse,
    iterations: cfg.iterations,
    payloadBytes: cfg.payloadBytes,
    baselineRssBytes: baseline.rss,
    peakRssBytes: peakMeasuredRss,
    settledRssBytes: settled.rss,
    peakContributedRssBytes,
    settledContributedRssBytes,
    baselineHeapUsedBytes: baseline.heapUsed,
    peakHeapUsedBytes: afterRun.heapUsed,
    settledHeapUsedBytes: settled.heapUsed,
    baselineExternalBytes: baseline.external,
    peakExternalBytes: afterRun.external,
    settledExternalBytes: settled.external,
    baselineArrayBuffersBytes: baseline.arrayBuffers,
    peakArrayBuffersBytes: afterRun.arrayBuffers,
    settledArrayBuffersBytes: settled.arrayBuffers,
    peakAccountedBytes,
    settledAccountedBytes,
    peakUnaccountedBytes: Math.max(0, peakContributedRssBytes - peakAccountedBytes),
    settledUnaccountedBytes: Math.max(0, settledContributedRssBytes - settledAccountedBytes),
    durationMs,
  };
}

async function runLocalFileScenario(
  scenario: BunNativeApiRssScenario,
  fn: (root: string, payloadPath: string, payload: Uint8Array) => Promise<void>
): Promise<BunNativeApiRssMeasurement> {
  const cfg = SCENARIO_CONFIG[scenario];
  return await measureScenario(scenario, async () => {
    const root = createTempRoot(`ds-bun-native-${scenario}-`);
    try {
      const payload = buildPayloadBytes(cfg.payloadBytes);
      const payloadPath = join(root, "payload.bin");
      writeFileSync(payloadPath, payload);
      await fn(root, payloadPath, payload);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
}

async function runS3Scenario(
  scenario: BunNativeApiRssScenario,
  fn: (ctx: { root: string; payload: Uint8Array; payloadPath: string; client: Bun.S3Client }) => Promise<void>
): Promise<BunNativeApiRssMeasurement> {
  const cfg = SCENARIO_CONFIG[scenario];
  if (!cfg.requiresS3) throw new Error(`scenario does not require S3: ${scenario}`);
  requireS3Env();
  return await measureScenario(scenario, async () => {
    const root = createTempRoot(`ds-bun-native-${scenario}-`);
    try {
      const payload = buildPayloadBytes(cfg.payloadBytes);
      const payloadPath = join(root, "payload.bin");
      writeFileSync(payloadPath, payload);
      const client = makeS3Client();
      await fn({ root, payload, payloadPath, client });
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
}

export async function runBunNativeApiRssScenario(
  scenario: BunNativeApiRssScenario
): Promise<BunNativeApiRssMeasurement> {
  const cfg = SCENARIO_CONFIG[scenario];
  switch (scenario) {
    case "zstd_compress_sync":
      return await runLocalFileScenario(scenario, async (_root, _payloadPath, payload) => {
        let last = new Uint8Array(0);
        for (let i = 0; i < cfg.iterations; i += 1) last = new Uint8Array(zstdCompressSync(payload));
        if (last.byteLength === 0) throw new Error("zstd compress produced empty output");
      });
    case "zstd_decompress_sync":
      return await runLocalFileScenario(scenario, async (_root, _payloadPath, payload) => {
        const compressed = new Uint8Array(zstdCompressSync(payload));
        let last = new Uint8Array(0);
        for (let i = 0; i < cfg.iterations; i += 1) last = new Uint8Array(zstdDecompressSync(compressed));
        if (last.byteLength !== payload.byteLength) throw new Error("zstd decompress roundtrip mismatch");
      });
    case "bun_file_bytes":
      return await runLocalFileScenario(scenario, async (_root, payloadPath) => {
        let last = new Uint8Array(0);
        for (let i = 0; i < cfg.iterations; i += 1) last = await Bun.file(payloadPath).bytes();
        if (last.byteLength !== cfg.payloadBytes) throw new Error("Bun.file().bytes() size mismatch");
      });
    case "bun_file_arraybuffer":
      return await runLocalFileScenario(scenario, async (_root, payloadPath) => {
        let last = 0;
        for (let i = 0; i < cfg.iterations; i += 1) {
          const bytes = new Uint8Array(await Bun.file(payloadPath).arrayBuffer());
          last = bytes.byteLength;
        }
        if (last !== cfg.payloadBytes) throw new Error("Bun.file().arrayBuffer() size mismatch");
      });
    case "bun_write_from_bun_file":
      return await runLocalFileScenario(scenario, async (root, payloadPath) => {
        const destPath = join(root, "copy.bin");
        for (let i = 0; i < cfg.iterations; i += 1) {
          const written = await Bun.write(destPath, Bun.file(payloadPath));
          if (written !== cfg.payloadBytes) throw new Error(`Bun.write mismatch: ${written}`);
          unlinkSync(destPath);
        }
      });
    case "bun_mmap_shared":
      return await runLocalFileScenario(scenario, async (_root, payloadPath) => {
        let last = 0;
        for (let i = 0; i < cfg.iterations; i += 1) {
          const bytes = (Bun as any).mmap(payloadPath, { shared: true }) as Uint8Array;
          last = bytes[bytes.byteLength - 1] ?? 0;
        }
        if (!Number.isFinite(last)) throw new Error("Bun.mmap result invalid");
      });
    case "s3_put_fetch_blob_buffer":
      return await runS3Scenario(scenario, async ({ payload, client }) => {
        for (let i = 0; i < cfg.iterations; i += 1) {
          const file = client.file(`bun-native-rss/${scenario}/${i}.bin`);
          const url = file.presign({ method: "PUT", type: CONTENT_TYPE });
          const res = await withS3Retries(() =>
            fetch(url, {
              method: "PUT",
              body: new Blob([Buffer.from(payload)]),
              headers: {
                "content-type": CONTENT_TYPE,
                "content-length": String(payload.byteLength),
              },
            })
          );
          if (!res.ok) throw new Error(`presign blob upload failed: HTTP ${res.status}`);
        }
      });
    case "s3_put_native_buffer":
      return await runS3Scenario(scenario, async ({ payload, client }) => {
        for (let i = 0; i < cfg.iterations; i += 1) {
          const file = client.file(`bun-native-rss/${scenario}/${i}.bin`);
          await withS3Retries(() => file.write(Buffer.from(payload), { type: CONTENT_TYPE }));
        }
      });
    case "s3_put_fetch_bun_file":
      return await runS3Scenario(scenario, async ({ payloadPath, client }) => {
        for (let i = 0; i < cfg.iterations; i += 1) {
          const file = client.file(`bun-native-rss/${scenario}/${i}.bin`);
          const url = file.presign({ method: "PUT", type: CONTENT_TYPE });
          const res = await withS3Retries(() =>
            fetch(url, {
              method: "PUT",
              body: Bun.file(payloadPath),
              headers: {
                "content-type": CONTENT_TYPE,
                "content-length": String(cfg.payloadBytes),
              },
            })
          );
          if (!res.ok) throw new Error(`presign file upload failed: HTTP ${res.status}`);
        }
      });
    case "s3_put_native_bun_file":
      return await runS3Scenario(scenario, async ({ payloadPath, client }) => {
        for (let i = 0; i < cfg.iterations; i += 1) {
          const file = client.file(`bun-native-rss/${scenario}/${i}.bin`);
          await withS3Retries(() => file.write(Bun.file(payloadPath), { type: CONTENT_TYPE }));
        }
      });
    case "s3_get_arraybuffer":
      return await runS3Scenario(scenario, async ({ payloadPath, client }) => {
        const file = client.file(`bun-native-rss/${scenario}/seed.bin`);
        await withS3Retries(() => file.write(Bun.file(payloadPath), { type: CONTENT_TYPE }));
        let last = 0;
        for (let i = 0; i < cfg.iterations; i += 1) {
          last = (await withS3Retries(() => file.arrayBuffer())).byteLength;
        }
        if (last !== cfg.payloadBytes) throw new Error("S3 arrayBuffer size mismatch");
      });
    case "s3_get_bun_write":
      return await runS3Scenario(scenario, async ({ root, payloadPath, client }) => {
        const file = client.file(`bun-native-rss/${scenario}/seed.bin`);
        await withS3Retries(() => file.write(Bun.file(payloadPath), { type: CONTENT_TYPE }));
        const destPath = join(root, "download.bin");
        for (let i = 0; i < cfg.iterations; i += 1) {
          await withS3Retries(() => Bun.write(destPath, file));
          const written = statSync(destPath).size;
          if (written !== cfg.payloadBytes) throw new Error(`S3 Bun.write mismatch: ${written}`);
          unlinkSync(destPath);
        }
      });
  }
}
