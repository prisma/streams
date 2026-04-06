export class BinaryPayloadError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BinaryPayloadError";
  }
}

export class BinaryWriter {
  private readonly parts: Uint8Array[] = [];
  private lengthBytes = 0;

  get length(): number {
    return this.lengthBytes;
  }

  writeU8(value: number): void {
    const out = new Uint8Array(1);
    out[0] = value & 0xff;
    this.push(out);
  }

  writeU16(value: number): void {
    const out = new Uint8Array(2);
    new DataView(out.buffer).setUint16(0, value, true);
    this.push(out);
  }

  writeU32(value: number): void {
    const out = new Uint8Array(4);
    new DataView(out.buffer).setUint32(0, value, true);
    this.push(out);
  }

  writeU64(value: bigint): void {
    const out = new Uint8Array(8);
    new DataView(out.buffer).setBigUint64(0, BigInt.asUintN(64, value), true);
    this.push(out);
  }

  writeI64(value: bigint): void {
    const out = new Uint8Array(8);
    new DataView(out.buffer).setBigInt64(0, BigInt.asIntN(64, value), true);
    this.push(out);
  }

  writeF64(value: number): void {
    const out = new Uint8Array(8);
    new DataView(out.buffer).setFloat64(0, value, true);
    this.push(out);
  }

  writeBytes(value: Uint8Array): void {
    this.push(value);
  }

  finish(): Uint8Array {
    return concatBytes(this.parts);
  }

  private push(value: Uint8Array): void {
    this.parts.push(value);
    this.lengthBytes += value.byteLength;
  }
}

export class BinaryCursor {
  private offsetBytes = 0;

  constructor(private readonly bytes: Uint8Array) {}

  get offset(): number {
    return this.offsetBytes;
  }

  remaining(): number {
    return this.bytes.byteLength - this.offsetBytes;
  }

  readU8(): number {
    this.ensureAvailable(1);
    return this.bytes[this.offsetBytes++]!;
  }

  readU16(): number {
    const value = readU16(this.bytes, this.offsetBytes);
    this.offsetBytes += 2;
    return value;
  }

  readU32(): number {
    const value = readU32(this.bytes, this.offsetBytes);
    this.offsetBytes += 4;
    return value;
  }

  readU64(): bigint {
    const value = readU64(this.bytes, this.offsetBytes);
    this.offsetBytes += 8;
    return value;
  }

  readI64(): bigint {
    const value = readI64(this.bytes, this.offsetBytes);
    this.offsetBytes += 8;
    return value;
  }

  readF64(): number {
    const value = readF64(this.bytes, this.offsetBytes);
    this.offsetBytes += 8;
    return value;
  }

  readBytes(length: number): Uint8Array {
    this.ensureAvailable(length);
    const out = this.bytes.subarray(this.offsetBytes, this.offsetBytes + length);
    this.offsetBytes += length;
    return out;
  }

  skip(length: number): void {
    this.ensureAvailable(length);
    this.offsetBytes += length;
  }

  private ensureAvailable(length: number): void {
    if (length < 0 || this.offsetBytes + length > this.bytes.byteLength) {
      throw new BinaryPayloadError("unexpected end of binary payload");
    }
  }
}

export function concatBytes(parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((sum, part) => sum + part.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.byteLength;
  }
  return out;
}

export function readU16(bytes: Uint8Array, offset: number): number {
  return new DataView(bytes.buffer, bytes.byteOffset + offset, 2).getUint16(0, true);
}

export function readU32(bytes: Uint8Array, offset: number): number {
  return new DataView(bytes.buffer, bytes.byteOffset + offset, 4).getUint32(0, true);
}

export function readU64(bytes: Uint8Array, offset: number): bigint {
  return new DataView(bytes.buffer, bytes.byteOffset + offset, 8).getBigUint64(0, true);
}

export function readI64(bytes: Uint8Array, offset: number): bigint {
  return new DataView(bytes.buffer, bytes.byteOffset + offset, 8).getBigInt64(0, true);
}

export function readF64(bytes: Uint8Array, offset: number): number {
  return new DataView(bytes.buffer, bytes.byteOffset + offset, 8).getFloat64(0, true);
}
