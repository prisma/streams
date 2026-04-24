const PATTERN = new TextEncoder().encode("prisma-streams-compute-demo:");

function mix32(seed: number): number {
  let x = seed | 0;
  x ^= x >>> 16;
  x = Math.imul(x, 0x7feb352d);
  x ^= x >>> 15;
  x = Math.imul(x, 0x846ca68b);
  x ^= x >>> 16;
  return x >>> 0;
}

function fillPattern(view: Uint8Array, seq: number): void {
  const seqTag = `${seq.toString(16).padStart(8, "0")}:`;
  const tagBytes = new TextEncoder().encode(seqTag);
  let off = 0;
  while (off < view.byteLength) {
    for (let i = 0; i < PATTERN.byteLength && off < view.byteLength; i++) view[off++] = PATTERN[i]!;
    for (let i = 0; i < tagBytes.byteLength && off < view.byteLength; i++) view[off++] = tagBytes[i]!;
  }
}

function fillDeterministicNoise(view: Uint8Array, seq: number): void {
  let state = mix32(seq + 1);
  for (let i = 0; i < view.byteLength; i++) {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    view[i] = state & 0xff;
  }
}

export function buildComputeVerifyPayload(sizeBytes: number, seq: number): Uint8Array {
  const out = new Uint8Array(sizeBytes);
  const split = Math.floor(sizeBytes / 2);
  fillPattern(out.subarray(0, split), seq);
  fillDeterministicNoise(out.subarray(split), seq);
  return out;
}
