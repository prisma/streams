import { readFileSync } from "node:fs";
import { EOL } from "node:os";

function envInt(name: string, fallback: number): number {
  const value = process.env[name];
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function envRequired(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`missing required env ${name}`);
  return value;
}

function envBool(name: string, fallback: boolean): boolean {
  const value = process.env[name];
  if (value == null) return fallback;
  if (value === "1" || value.toLowerCase() === "true" || value.toLowerCase() === "yes") return true;
  if (value === "0" || value.toLowerCase() === "false" || value.toLowerCase() === "no") return false;
  return fallback;
}

function forceGc(): void {
  try {
    Bun.gc(true);
  } catch {
    // ignore
  }
}

function readOptionalText(path: string): string | null {
  try {
    return readFileSync(path, "utf8").trim();
  } catch {
    return null;
  }
}

function cgroupMemoryLimitBytes(): number | null {
  const raw = readOptionalText("/sys/fs/cgroup/memory.max");
  if (!raw || raw === "max") return null;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function cgroupMemoryCurrentBytes(): number | null {
  const raw = readOptionalText("/sys/fs/cgroup/memory.current");
  if (!raw) return null;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function printJsonLine(kind: string, payload: Record<string, unknown>): void {
  process.stdout.write(`${kind} ${JSON.stringify(payload)}${EOL}`);
}

const endpoint = envRequired("S3_TEST_ENDPOINT");
const bucket = envRequired("S3_TEST_BUCKET");
const accessKeyId = envRequired("S3_TEST_ACCESS_KEY_ID");
const secretAccessKey = envRequired("S3_TEST_SECRET_ACCESS_KEY");

const PAYLOAD_MIB = envInt("PAYLOAD_MIB", 8);
const MAX_ITERATIONS = envInt("MAX_ITERATIONS", 256);
const REPORT_EVERY = envInt("REPORT_EVERY", 8);
const TARGET_RSS_MIB = envInt("TARGET_RSS_MIB", 900);
const EXIT_ON_THRESHOLD = envBool("EXIT_ON_THRESHOLD", true);
const PAYLOAD_BYTES = PAYLOAD_MIB * 1024 * 1024;
const TARGET_RSS_BYTES = TARGET_RSS_MIB * 1024 * 1024;
const CONTENT_TYPE = "application/octet-stream";

const client = new Bun.S3Client({
  bucket,
  accessKeyId,
  secretAccessKey,
  endpoint,
  region: "us-east-1",
});

const seedKey = `bun-oom-repro/s3-arraybuffer-${Date.now()}.bin`;
const file = client.file(seedKey);
const payload = new Uint8Array(PAYLOAD_BYTES);
for (let i = 0; i < payload.byteLength; i += 1) payload[i] = i & 0xff;

printJsonLine("REPRO_START", {
  scenario: "bun_s3_arraybuffer_oom",
  bun_version: Bun.version,
  platform: process.platform,
  arch: process.arch,
  pid: process.pid,
  endpoint,
  bucket,
  seed_key: seedKey,
  payload_mib: PAYLOAD_MIB,
  payload_bytes: PAYLOAD_BYTES,
  max_iterations: MAX_ITERATIONS,
  report_every: REPORT_EVERY,
  target_rss_mib: TARGET_RSS_MIB,
  target_rss_bytes: TARGET_RSS_BYTES,
  exit_on_threshold: EXIT_ON_THRESHOLD,
  cgroup_memory_limit_bytes: cgroupMemoryLimitBytes(),
  cgroup_memory_current_bytes: cgroupMemoryCurrentBytes(),
});

await file.write(payload, { type: CONTENT_TYPE });
forceGc();
const baseline = process.memoryUsage();

printJsonLine("REPRO_BASELINE", {
  rss_bytes: baseline.rss,
  heap_total_bytes: baseline.heapTotal,
  heap_used_bytes: baseline.heapUsed,
  external_bytes: baseline.external,
  array_buffers_bytes: baseline.arrayBuffers,
  cgroup_memory_current_bytes: cgroupMemoryCurrentBytes(),
});

for (let iteration = 1; iteration <= MAX_ITERATIONS; iteration += 1) {
  const bytes = new Uint8Array(await file.arrayBuffer());
  if (bytes.byteLength !== PAYLOAD_BYTES) throw new Error(`size mismatch: ${bytes.byteLength}`);
  forceGc();
  const current = process.memoryUsage();
  if (iteration % REPORT_EVERY === 0) {
    printJsonLine("REPRO_PROGRESS", {
      iteration,
      rss_bytes: current.rss,
      heap_total_bytes: current.heapTotal,
      heap_used_bytes: current.heapUsed,
      external_bytes: current.external,
      array_buffers_bytes: current.arrayBuffers,
      cgroup_memory_current_bytes: cgroupMemoryCurrentBytes(),
    });
  }
  if (current.rss >= TARGET_RSS_BYTES) {
    printJsonLine("REPRO_THRESHOLD_REACHED", {
      iteration,
      baseline_rss_bytes: baseline.rss,
      rss_bytes: current.rss,
      heap_total_bytes: current.heapTotal,
      heap_used_bytes: current.heapUsed,
      external_bytes: current.external,
      array_buffers_bytes: current.arrayBuffers,
      target_rss_bytes: TARGET_RSS_BYTES,
      cgroup_memory_current_bytes: cgroupMemoryCurrentBytes(),
    });
    if (EXIT_ON_THRESHOLD) process.exit(0);
  }
}

printJsonLine("REPRO_THRESHOLD_NOT_REACHED", {
  max_iterations: MAX_ITERATIONS,
  target_rss_bytes: TARGET_RSS_BYTES,
});
process.exit(1);
