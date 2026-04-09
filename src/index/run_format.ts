import { Result } from "better-result";
import { concatBytes, readU16BE, readU32BE, readU64BE, writeU32BE, writeU64BE } from "../util/endian";
import { decodeBinaryFuseResult, encodeBinaryFuse, type BinaryFuseFilter } from "./binary_fuse";
import { dsError } from "../util/ds_error.ts";

export const INDEX_RUN_MAGIC = "IRN1";
export const INDEX_RUN_VERSION = 1;
export const RUN_TYPE_MASK16 = 0;
export const RUN_TYPE_POSTINGS = 1;

export type IndexRunMeta = {
  runId: string;
  level: number;
  startSegment: number;
  endSegment: number;
  objectKey: string;
  filterLen: number;
  recordCount: number;
};

export type IndexRun = {
  meta: IndexRunMeta;
  runType: number;
  filterBytes: Uint8Array;
  filter?: BinaryFuseFilter | null;
  fingerprints: bigint[];
  masks?: number[];
  postings?: number[][];
};

export type IndexRunFormatError = {
  kind: "invalid_index_run";
  message: string;
};

function invalidRun<T = never>(message: string): Result<T, IndexRunFormatError> {
  return Result.err({ kind: "invalid_index_run", message });
}

export function encodeIndexRun(run: IndexRun): Uint8Array {
  const res = encodeIndexRunResult(run);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function encodeIndexRunResult(run: IndexRun): Result<Uint8Array, IndexRunFormatError> {
  const filterBytes =
    run.filterBytes && run.filterBytes.byteLength > 0
      ? run.filterBytes
      : run.filter
        ? encodeBinaryFuse(run.filter)
        : new Uint8Array(0);
  const recordCount = run.fingerprints.length;
  let dataBytes: Uint8Array;
  if (run.runType === RUN_TYPE_MASK16) {
    if (!run.masks || run.masks.length !== recordCount) return invalidRun("mask run missing masks");
    dataBytes = new Uint8Array(recordCount * 10);
    const dataView = new DataView(dataBytes.buffer, dataBytes.byteOffset, dataBytes.byteLength);
    for (let i = 0; i < recordCount; i++) {
      const off = i * 10;
      dataView.setBigUint64(off, run.fingerprints[i]!, false);
      dataView.setUint16(off + 8, run.masks[i]! & 0xffff, false);
    }
  } else if (run.runType === RUN_TYPE_POSTINGS) {
    if (!run.postings || run.postings.length !== recordCount) return invalidRun("postings run missing postings");
    const chunks: Uint8Array[] = [];
    for (let i = 0; i < recordCount; i++) {
      const fp = run.fingerprints[i];
      const hdr = new Uint8Array(8);
      writeU64BE(hdr, 0, fp);
      chunks.push(hdr);
      const ps = run.postings[i];
      chunks.push(encodeUVarint(ps.length));
      for (const p of ps) chunks.push(encodeUVarint(p));
    }
    dataBytes = concatBytes(chunks);
  } else {
    return invalidRun(`unknown run type ${run.runType}`);
  }

  const header = new Uint8Array(36);
  const headerView = new DataView(header.buffer, header.byteOffset, header.byteLength);
  header[0] = INDEX_RUN_MAGIC.charCodeAt(0);
  header[1] = INDEX_RUN_MAGIC.charCodeAt(1);
  header[2] = INDEX_RUN_MAGIC.charCodeAt(2);
  header[3] = INDEX_RUN_MAGIC.charCodeAt(3);
  header[4] = INDEX_RUN_VERSION;
  header[5] = run.runType & 0xff;
  header[6] = run.meta.level & 0xff;
  header[7] = 0;
  headerView.setBigUint64(8, BigInt(run.meta.startSegment), false);
  headerView.setBigUint64(16, BigInt(run.meta.endSegment), false);
  writeU32BE(header, 24, recordCount);
  writeU32BE(header, 28, filterBytes.byteLength);
  writeU32BE(header, 32, dataBytes.byteLength);

  return Result.ok(concatBytes([header, filterBytes, dataBytes]));
}

export function decodeIndexRun(data: Uint8Array): IndexRun {
  const res = decodeIndexRunResult(data);
  if (Result.isError(res)) throw dsError(res.error.message);
  return res.value;
}

export function decodeIndexRunResult(data: Uint8Array): Result<IndexRun, IndexRunFormatError> {
  if (data.byteLength < 36) return invalidRun("run too short");
  const magic = String.fromCharCode(data[0], data[1], data[2], data[3]);
  if (magic !== INDEX_RUN_MAGIC) return invalidRun("invalid run magic");
  if (data[4] !== INDEX_RUN_VERSION) return invalidRun("unsupported run version");
  const runType = data[5];
  const level = data[6];
  const startSegment = Number(readU64BE(data, 8));
  const endSegment = Number(readU64BE(data, 16));
  const recordCount = readU32BE(data, 24);
  const filterLen = readU32BE(data, 28);
  const dataLen = readU32BE(data, 32);
  const totalLen = 36 + filterLen + dataLen;
  if (totalLen > data.byteLength) return invalidRun("run data truncated");
  const filterBytes = data.slice(36, 36 + filterLen);
  const filterRes = decodeBinaryFuseResult(filterBytes);
  if (Result.isError(filterRes)) return invalidRun(filterRes.error.message);
  const filter = filterRes.value;
  const body = data.slice(36 + filterLen, totalLen);

  const meta: IndexRunMeta = {
    runId: "",
    level,
    startSegment,
    endSegment,
    objectKey: "",
    filterLen,
    recordCount,
  };

  const run: IndexRun = {
    meta,
    runType,
    filterBytes,
    filter,
    fingerprints: [],
  };

  if (runType === RUN_TYPE_MASK16) {
    const needed = recordCount * 10;
    if (body.byteLength < needed) return invalidRun("mask run truncated");
    const fps: bigint[] = new Array(recordCount);
    const masks: number[] = new Array(recordCount);
    for (let i = 0; i < recordCount; i++) {
      const off = i * 10;
      fps[i] = readU64BE(body, off);
      masks[i] = readU16BE(body, off + 8);
    }
    run.fingerprints = fps;
    run.masks = masks;
    return Result.ok(run);
  }

  if (runType === RUN_TYPE_POSTINGS) {
    const fps: bigint[] = [];
    const postings: number[][] = [];
    let pos = 0;
    while (fps.length < recordCount && pos < body.byteLength) {
      if (pos + 8 > body.byteLength) return invalidRun("postings run truncated (fp)");
      const fp = readU64BE(body, pos);
      pos += 8;
      const countRes = decodeUVarint(body, pos);
      if (!countRes) return invalidRun("postings run truncated (count)");
      pos = countRes.next;
      const count = countRes.value;
      const ps: number[] = [];
      for (let i = 0; i < count; i++) {
        const vRes = decodeUVarint(body, pos);
        if (!vRes) return invalidRun("postings run truncated (val)");
        pos = vRes.next;
        ps.push(vRes.value);
      }
      fps.push(fp);
      postings.push(ps);
    }
    run.fingerprints = fps;
    run.postings = postings;
    return Result.ok(run);
  }

  return invalidRun(`unknown run type ${runType}`);
}

type VarintResult = { value: number; next: number } | null;

function decodeUVarint(bytes: Uint8Array, offset: number): VarintResult {
  let x = 0;
  let s = 0;
  for (let i = 0; i < 10 && offset + i < bytes.byteLength; i++) {
    const b = bytes[offset + i];
    if (b < 0x80) {
      if (i === 9 && b > 1) return null;
      return { value: x | (b << s), next: offset + i + 1 };
    }
    x |= (b & 0x7f) << s;
    s += 7;
  }
  return null;
}

function encodeUVarint(value: number): Uint8Array {
  let v = value >>> 0;
  const out: number[] = [];
  while (v >= 0x80) {
    out.push((v & 0x7f) | 0x80);
    v >>>= 7;
  }
  out.push(v);
  return new Uint8Array(out);
}
