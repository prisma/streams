import { randomBytes } from "node:crypto";
import { zstdCompressSync, zstdDecompressSync } from "node:zlib";

function envInt(name: string, fallback: number): number {
  const value = process.env[name];
  if (!value) return fallback;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function forceGc(): void {
  try {
    Bun.gc(true);
  } catch {
    // ignore
  }
}

const PAYLOAD_MIB = envInt("PAYLOAD_MIB", 8);
const MAX_ITERATIONS = envInt("MAX_ITERATIONS", 512);
const REPORT_EVERY = envInt("REPORT_EVERY", 16);
const TARGET_RSS_MIB = envInt("TARGET_RSS_MIB", 900);
const PAYLOAD_BYTES = PAYLOAD_MIB * 1024 * 1024;
const TARGET_RSS_BYTES = TARGET_RSS_MIB * 1024 * 1024;

const payload = randomBytes(PAYLOAD_BYTES);
const compressed = new Uint8Array(zstdCompressSync(payload));

forceGc();
const baseline = process.memoryUsage();

for (let iteration = 1; iteration <= MAX_ITERATIONS; iteration += 1) {
  const bytes = new Uint8Array(zstdDecompressSync(compressed));
  if (bytes.byteLength !== PAYLOAD_BYTES) throw new Error(`size mismatch: ${bytes.byteLength}`);
  forceGc();
  const current = process.memoryUsage();
  if (iteration % REPORT_EVERY === 0) {
    // eslint-disable-next-line no-console
    console.log(
      JSON.stringify({
        scenario: "bun_zstd_decompress_oom",
        iteration,
        rss_bytes: current.rss,
        heap_used_bytes: current.heapUsed,
        external_bytes: current.external,
        array_buffers_bytes: current.arrayBuffers,
      })
    );
  }
  if (current.rss >= TARGET_RSS_BYTES) {
    // eslint-disable-next-line no-console
    console.log(
      `REPRO_THRESHOLD_REACHED ${JSON.stringify({
        scenario: "bun_zstd_decompress_oom",
        iteration,
        baseline_rss_bytes: baseline.rss,
        rss_bytes: current.rss,
        target_rss_bytes: TARGET_RSS_BYTES,
      })}`
    );
    process.exit(0);
  }
}

throw new Error(`threshold not reached after ${MAX_ITERATIONS} iterations`);
