import { BinaryCursor, BinaryPayloadError, BinaryWriter } from "./codec";

export function writeUVarint(writer: BinaryWriter, value: number | bigint): void {
  let remaining = typeof value === "bigint" ? value : BigInt(value >>> 0 === value ? value : Math.trunc(value));
  if (remaining < 0n) throw new BinaryPayloadError("uvarint cannot encode negative values");
  while (remaining >= 0x80n) {
    writer.writeU8(Number((remaining & 0x7fn) | 0x80n));
    remaining >>= 7n;
  }
  writer.writeU8(Number(remaining));
}

export function readUVarint(cursor: BinaryCursor): bigint {
  let shift = 0n;
  let value = 0n;
  for (let i = 0; i < 10; i++) {
    const next = BigInt(cursor.readU8());
    value |= (next & 0x7fn) << shift;
    if ((next & 0x80n) === 0n) return value;
    shift += 7n;
  }
  throw new BinaryPayloadError("invalid uvarint");
}

export function writeZigZagVarint(writer: BinaryWriter, value: number | bigint): void {
  const bigintValue = typeof value === "bigint" ? value : BigInt(Math.trunc(value));
  const zigzag = bigintValue >= 0n ? bigintValue << 1n : ((-bigintValue) << 1n) - 1n;
  writeUVarint(writer, zigzag);
}

export function readZigZagVarint(cursor: BinaryCursor): bigint {
  const raw = readUVarint(cursor);
  return (raw & 1n) === 0n ? raw >> 1n : -((raw + 1n) >> 1n);
}
