const INT64_SIGN = 0x8000_0000_0000_0000n;
const UINT64_MASK = 0xffff_ffff_ffff_ffffn;

export function encodeSortableInt64(value: bigint): Uint8Array {
  const signed = BigInt.asIntN(64, value);
  const sortable = BigInt.asUintN(64, signed ^ INT64_SIGN);
  const out = new Uint8Array(8);
  const dv = new DataView(out.buffer, out.byteOffset, out.byteLength);
  dv.setBigUint64(0, sortable, false);
  return out;
}

export function decodeSortableInt64(bytes: Uint8Array, offset = 0): bigint {
  const dv = new DataView(bytes.buffer, bytes.byteOffset + offset, 8);
  const sortable = dv.getBigUint64(0, false);
  return BigInt.asIntN(64, sortable ^ INT64_SIGN);
}

export function encodeSortableFloat64(value: number): Uint8Array {
  const out = new Uint8Array(8);
  const dv = new DataView(out.buffer, out.byteOffset, out.byteLength);
  dv.setFloat64(0, value, false);
  const raw = dv.getBigUint64(0, false);
  const sortable = (raw & INT64_SIGN) !== 0n ? (~raw) & UINT64_MASK : raw ^ INT64_SIGN;
  dv.setBigUint64(0, sortable, false);
  return out;
}

export function decodeSortableFloat64(bytes: Uint8Array, offset = 0): number {
  const dv = new DataView(bytes.buffer, bytes.byteOffset + offset, 8);
  const sortable = dv.getBigUint64(0, false);
  const raw = (sortable & INT64_SIGN) !== 0n ? sortable ^ INT64_SIGN : (~sortable) & UINT64_MASK;
  dv.setBigUint64(0, raw, false);
  return dv.getFloat64(0, false);
}

export function encodeSortableBool(value: boolean): Uint8Array {
  return new Uint8Array([value ? 1 : 0]);
}

export function decodeSortableBool(bytes: Uint8Array, offset = 0): boolean {
  return bytes[offset] === 1;
}
