import { describe, expect, test } from "bun:test";
import { encodeBlock } from "../../src/segment/format";
import { readU32BE } from "../../src/util/endian";
import { buildComputeVerifyPayload } from "../../experiments/demo/compute/verify_payload";

function compressionRatio(records: Uint8Array[]): number {
  const block = encodeBlock(
    records.map((payload, idx) => ({
      appendNs: BigInt(idx + 1),
      routingKey: new Uint8Array(0),
      payload,
    }))
  );
  const compressedLen = readU32BE(block, 8);
  const payloadBytes = records.reduce((sum, payload) => sum + payload.byteLength, 0);
  return compressedLen / payloadBytes;
}

describe("compute verify payload", () => {
  test("stays above the 2:1 compression threshold while remaining compressible", () => {
    const records = Array.from({ length: 8 }, (_, seq) => buildComputeVerifyPayload(256 * 1024, seq));
    const ratio = compressionRatio(records);
    expect(ratio).toBeGreaterThanOrEqual(0.5);
    expect(ratio).toBeLessThan(0.8);
  });

  test("varies by sequence so producer retries can resend the same payload deterministically", () => {
    const first = buildComputeVerifyPayload(1024, 1);
    const second = buildComputeVerifyPayload(1024, 2);
    const retry = buildComputeVerifyPayload(1024, 1);
    expect(Array.from(first)).not.toEqual(Array.from(second));
    expect(Array.from(first)).toEqual(Array.from(retry));
  });
});
