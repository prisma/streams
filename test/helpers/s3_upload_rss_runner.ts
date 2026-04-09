import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomBytes } from "node:crypto";

type Mode = "presign-fetch" | "native-write";

type UploadRssSummary = {
  mode: Mode;
  iterations: number;
  payloadBytes: number;
  elapsedMs: number;
  baselineRssBytes: number;
  peakRssBytes: number;
  settledRssBytes: number;
  peakContributedRssBytes: number;
  settledContributedRssBytes: number;
};

const mode = (process.argv[2] ?? "presign-fetch") as Mode;
const endpoint = process.env.S3_TEST_ENDPOINT;
const bucket = process.env.S3_TEST_BUCKET;
const accessKeyId = process.env.S3_TEST_ACCESS_KEY_ID;
const secretAccessKey = process.env.S3_TEST_SECRET_ACCESS_KEY;

if (!endpoint || !bucket || !accessKeyId || !secretAccessKey) {
  throw new Error("missing S3_TEST_* environment for s3 upload RSS runner");
}

const PAYLOAD_BYTES =
  Number.parseInt(process.env.S3_UPLOAD_PAYLOAD_BYTES ?? "", 10) ||
  Number.parseInt(process.env.S3_UPLOAD_PAYLOAD_KIB ?? "", 10) * 1024 ||
  8 * 1024 * 1024;
const ITERATIONS = Number.parseInt(process.env.S3_UPLOAD_ITERATIONS ?? "", 10) || 24;
const CONTENT_TYPE = "application/octet-stream";

function forceGc(): void {
  try {
    Bun.gc(true);
  } catch {
    // ignore when GC is unavailable
  }
}

function rssBytes(): number {
  return process.memoryUsage().rss;
}

const client = new Bun.S3Client({
  bucket,
  accessKeyId,
  secretAccessKey,
  endpoint,
  region: "us-east-1",
});

async function uploadOnce(path: string, iteration: number): Promise<void> {
  const file = client.file(`rss-upload-${mode}-${iteration}.bin`);
  if (mode === "presign-fetch") {
    let lastError: unknown = null;
    for (let attempt = 0; attempt < 5; attempt += 1) {
      try {
        const url = file.presign({ method: "PUT", type: CONTENT_TYPE });
        const res = await fetch(url, {
          method: "PUT",
          body: Bun.file(path),
          headers: {
            "content-type": CONTENT_TYPE,
            "content-length": String(PAYLOAD_BYTES),
          },
        });
        if (!res.ok) throw new Error(`presign-fetch upload failed: HTTP ${res.status}`);
        return;
      } catch (error: unknown) {
        lastError = error;
        if (attempt === 4) break;
        await Bun.sleep(50);
      }
    }
    throw lastError instanceof Error ? lastError : new Error(String(lastError ?? "presign-fetch upload failed"));
    return;
  }

  await file.write(Bun.file(path), { type: CONTENT_TYPE });
}

const root = mkdtempSync(join(tmpdir(), "ds-s3-upload-rss-"));
const payloadPath = join(root, "payload.bin");
writeFileSync(payloadPath, randomBytes(PAYLOAD_BYTES));

forceGc();
const baselineRssBytes = rssBytes();
let peakRssBytes = baselineRssBytes;
const startTime = Date.now();

for (let i = 0; i < ITERATIONS; i += 1) {
  await uploadOnce(payloadPath, i);
  peakRssBytes = Math.max(peakRssBytes, rssBytes());
}
const elapsedMs = Date.now() - startTime;

forceGc();
const settledRssBytes = rssBytes();

const summary: UploadRssSummary = {
  mode,
  iterations: ITERATIONS,
  payloadBytes: PAYLOAD_BYTES,
  elapsedMs,
  baselineRssBytes,
  peakRssBytes,
  settledRssBytes,
  peakContributedRssBytes: peakRssBytes - baselineRssBytes,
  settledContributedRssBytes: settledRssBytes - baselineRssBytes,
};

console.log(`S3_UPLOAD_RSS_SUMMARY ${JSON.stringify(summary)}`);

rmSync(root, { recursive: true, force: true });
