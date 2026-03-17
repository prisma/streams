/**
 * Endian helpers.
 *
 * The TieredStore spec uses big-endian for on-disk encodings.
 */

export function writeU32BE(dst: Uint8Array, offset: number, value: number): void {
  const dv = new DataView(dst.buffer, dst.byteOffset, dst.byteLength);
  dv.setUint32(offset, value >>> 0, false);
}

export function writeU16BE(dst: Uint8Array, offset: number, value: number): void {
  const dv = new DataView(dst.buffer, dst.byteOffset, dst.byteLength);
  dv.setUint16(offset, value & 0xffff, false);
}

export function writeU64BE(dst: Uint8Array, offset: number, value: bigint): void {
  const dv = new DataView(dst.buffer, dst.byteOffset, dst.byteLength);
  dv.setBigUint64(offset, value, false);
}

export function readU32BE(src: Uint8Array, offset: number): number {
  const dv = new DataView(src.buffer, src.byteOffset, src.byteLength);
  return dv.getUint32(offset, false);
}

export function readU16BE(src: Uint8Array, offset: number): number {
  const dv = new DataView(src.buffer, src.byteOffset, src.byteLength);
  return dv.getUint16(offset, false);
}

export function readU64BE(src: Uint8Array, offset: number): bigint {
  const dv = new DataView(src.buffer, src.byteOffset, src.byteLength);
  return dv.getBigUint64(offset, false);
}

// Little-endian helpers for metadata arrays (not on-disk segment format).
export function readU64LE(src: Uint8Array, offset: number): bigint {
  const dv = new DataView(src.buffer, src.byteOffset, src.byteLength);
  return dv.getBigUint64(offset, true);
}

export function concatBytes(parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const p of parts) total += p.byteLength;
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.byteLength;
  }
  return out;
}
