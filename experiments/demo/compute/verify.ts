/**
 * Demo: append a large binary stream and wait until all sealed segments are
 * uploaded. The payload is deliberately mixed-entropy so DS_SEGMENT_MAX_BYTES
 * remains the active seal threshold without needing a tiny DS_SEGMENT_TARGET_ROWS.
 *
 * Usage:
 *   bun run experiments/demo/compute/verify.ts --url https://service.example
 *   bun run experiments/demo/compute/verify.ts --url https://service.example --total-bytes 134217728
 *   bun run experiments/demo/compute/verify.ts --url https://service.example --stream compute-demo-fixed
 */

import { randomUUID } from "node:crypto";
import { DEFAULT_BASE_URL, parseIntArg, parseStringArg, sleep } from "../common";
import { buildComputeVerifyPayload } from "./verify_payload";
import { streamHash16Hex } from "../../../src/util/stream_paths";
import { dsError } from "../../../src/util/ds_error.ts";

const ARGS = process.argv.slice(2);
const baseUrl = parseStringArg(ARGS, "--url", DEFAULT_BASE_URL).replace(/\/+$/, "");
const streamPrefix = parseStringArg(ARGS, "--stream-prefix", "compute-demo");
const explicitStream = parseStringArg(ARGS, "--stream", "");
const totalBytes = parseIntArg(ARGS, "--total-bytes", 1024 * 1024 * 1024);
const chunkBytes = parseIntArg(ARGS, "--chunk-bytes", 1024 * 1024);
const retryMax = parseIntArg(ARGS, "--retry-max", 20);
const retryBaseMs = parseIntArg(ARGS, "--retry-base-ms", 250);
const pollMs = parseIntArg(ARGS, "--poll-ms", 5000);

if (chunkBytes <= 0) throw dsError("--chunk-bytes must be > 0");
if (totalBytes <= 0) throw dsError("--total-bytes must be > 0");
if (totalBytes % chunkBytes !== 0) throw dsError("--total-bytes must be an exact multiple of --chunk-bytes");

const totalChunks = totalBytes / chunkBytes;
const stream =
  explicitStream.trim() !== ""
    ? explicitStream.trim()
    : `${streamPrefix}-${new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 12)}`;
const producerId = `producer-${randomUUID()}`;

async function request(path: string, init: RequestInit): Promise<{ status: number; text: string }> {
  const res = await fetch(`${baseUrl}${path}`, init);
  return { status: res.status, text: await res.text() };
}

async function ensureStream(): Promise<void> {
  const res = await request(`/v1/stream/${encodeURIComponent(stream)}`, {
    method: "PUT",
    headers: { "content-type": "application/octet-stream" },
  });
  if (res.status !== 200 && res.status !== 201) throw dsError(`create failed: ${res.status} ${res.text}`);
}

async function appendChunk(seq: number): Promise<void> {
  const payload = buildComputeVerifyPayload(chunkBytes, seq);
  for (let attempt = 1; attempt <= retryMax; attempt++) {
    const res = await request(`/v1/stream/${encodeURIComponent(stream)}`, {
      method: "POST",
      headers: {
        "content-type": "application/octet-stream",
        "producer-id": producerId,
        "producer-epoch": "0",
        "producer-seq": String(seq),
      },
      body: payload,
    });
    if (res.status === 200 || res.status === 204) return;
    if (![408, 429, 500, 502, 503, 504].includes(res.status) || attempt === retryMax) {
      throw dsError(`append seq=${seq} failed: ${res.status} ${res.text}`);
    }
    const waitMs = Math.min(5000, retryBaseMs * attempt);
    console.log(`retry seq=${seq} attempt=${attempt} status=${res.status} wait_ms=${waitMs}`);
    await sleep(waitMs);
  }
}

async function closeStream(): Promise<void> {
  const res = await request(`/v1/stream/${encodeURIComponent(stream)}`, {
    method: "POST",
    headers: { "stream-closed": "true" },
  });
  if (res.status !== 200 && res.status !== 204) throw dsError(`close failed: ${res.status} ${res.text}`);
}

type Details = {
  stream?: {
    next_offset: string;
    sealed_through: string;
    uploaded_through: string;
    segment_count: number;
    uploaded_segment_count: number;
    pending_rows: string;
    pending_bytes: string;
    total_size_bytes: string;
    closed: boolean;
  };
  storage?: {
    object_storage?: {
      total_bytes: string;
      segments_bytes: string;
      indexes_bytes: string;
      manifest_and_meta_bytes: string;
      manifest_bytes: string;
      schema_registry_bytes: string;
      segment_object_count: number;
      routing_index_object_count: number;
      routing_lexicon_object_count: number;
      exact_index_object_count: number;
      bundled_companion_object_count: number;
    };
  };
  object_store_requests?: unknown;
};

async function fetchDetails(): Promise<Details> {
  const res = await request(`/v1/stream/${encodeURIComponent(stream)}/_details`, { method: "GET" });
  if (res.status !== 200) throw dsError(`details failed: ${res.status} ${res.text}`);
  return JSON.parse(res.text);
}

async function waitForUploaded(): Promise<Details> {
  for (;;) {
    const details = await fetchDetails();
    const row = details.stream;
    if (!row) throw dsError("details response missing stream");
    const nextOffset = Number(row.next_offset);
    const uploadedThrough = Number(row.uploaded_through);
    if (
      nextOffset === totalChunks &&
      uploadedThrough === totalChunks - 1 &&
      row.pending_rows === "0" &&
      row.pending_bytes === "0"
    ) {
      return details;
    }
    console.log(
      "wait_upload",
      JSON.stringify({
        next_offset: row.next_offset,
        sealed_through: row.sealed_through,
        uploaded_through: row.uploaded_through,
        pending_rows: row.pending_rows,
        pending_bytes: row.pending_bytes,
      })
    );
    await sleep(pollMs);
  }
}

async function main(): Promise<void> {
  await ensureStream();
  console.log(`created ${stream}`);
  const startMs = Date.now();
  for (let seq = 0; seq < totalChunks; seq++) {
    await appendChunk(seq);
    if ((seq + 1) % 64 === 0 || seq + 1 === totalChunks) {
      const mib = ((seq + 1) * chunkBytes) / (1024 * 1024);
      const rate = mib / ((Date.now() - startMs) / 1000);
      console.log(`chunks=${seq + 1}/${totalChunks} mib=${mib} rate_mib_s=${rate.toFixed(2)}`);
    }
  }
  await closeStream();
  console.log("closed stream");
  const details = await waitForUploaded();
  console.log(
    "upload_complete",
    JSON.stringify({
      stream,
      streamHash: streamHash16Hex(stream),
      producerId,
      next_offset: details.stream?.next_offset,
      sealed_through: details.stream?.sealed_through,
      uploaded_through: details.stream?.uploaded_through,
      segment_count: details.stream?.segment_count,
      uploaded_segment_count: details.stream?.uploaded_segment_count,
      pending_rows: details.stream?.pending_rows,
      pending_bytes: details.stream?.pending_bytes,
      total_size_bytes: details.stream?.total_size_bytes,
      storage: details.storage?.object_storage,
      object_store_requests: details.object_store_requests,
    })
  );
}

await main();
