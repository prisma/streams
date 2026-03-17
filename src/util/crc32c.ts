/**
 * CRC32C (Castagnoli) implementation in pure TypeScript.
 *
 * Used for DSB3 block headers.
 */

const POLY = 0x82f63b78;
let TABLE: Uint32Array | null = null;

function makeTable(): Uint32Array {
  const t = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let k = 0; k < 8; k++) {
      c = (c & 1) ? (POLY ^ (c >>> 1)) : (c >>> 1);
    }
    t[i] = c >>> 0;
  }
  return t;
}

export function crc32c(buf: Uint8Array): number {
  if (!TABLE) TABLE = makeTable();
  let c = 0xffffffff;
  for (const b of buf) {
    c = TABLE[(c ^ b) & 0xff] ^ (c >>> 8);
  }
  return (c ^ 0xffffffff) >>> 0;
}
