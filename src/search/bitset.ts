export function createBitset(size: number): Uint8Array {
  return new Uint8Array(Math.ceil(size / 8));
}

export function bitsetSet(bitset: Uint8Array, index: number): void {
  bitset[index >> 3] |= 1 << (index & 7);
}

export function bitsetGet(bitset: Uint8Array, index: number): boolean {
  return (bitset[index >> 3] & (1 << (index & 7))) !== 0;
}

export function listFromBitset(bitset: Uint8Array, size: number): number[] {
  const out: number[] = [];
  for (let i = 0; i < size; i++) {
    if (bitsetGet(bitset, i)) out.push(i);
  }
  return out;
}
