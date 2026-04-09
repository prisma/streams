import { BinaryCursor, BinaryPayloadError, BinaryWriter } from "./codec";
import { createBitset, bitsetGet, bitsetSet } from "../bitset";
import { readUVarint, writeUVarint } from "./varint";

export const DOCSET_CODEC_ALL = 0;
export const DOCSET_CODEC_BITSET = 1;
export const DOCSET_CODEC_DELTA_DOCIDS = 2;

export type EncodedDocSet = {
  codec: number;
  payload: Uint8Array;
  docIds: number[];
};

export function encodeAllDocSet(): EncodedDocSet {
  return {
    codec: DOCSET_CODEC_ALL,
    payload: new Uint8Array(),
    docIds: [],
  };
}

export function encodeDocSet(docCount: number, docIds: number[]): EncodedDocSet {
  const sortedDocIds = [...docIds].sort((a, b) => a - b);
  if (sortedDocIds.length === docCount) {
    return { codec: DOCSET_CODEC_ALL, payload: new Uint8Array(), docIds: sortedDocIds };
  }
  const bitset = createBitset(docCount);
  for (const docId of sortedDocIds) bitsetSet(bitset, docId);
  const deltaWriter = new BinaryWriter();
  let previous = 0;
  for (let i = 0; i < sortedDocIds.length; i++) {
    const docId = sortedDocIds[i]!;
    writeUVarint(deltaWriter, i === 0 ? docId : docId - previous);
    previous = docId;
  }
  const deltaPayload = deltaWriter.finish();
  const bitsetPayload = new Uint8Array(bitset);
  const useDelta = sortedDocIds.length > 0 && deltaPayload.byteLength < bitsetPayload.byteLength;
  return {
    codec: useDelta ? DOCSET_CODEC_DELTA_DOCIDS : DOCSET_CODEC_BITSET,
    payload: useDelta ? deltaPayload : bitsetPayload,
    docIds: sortedDocIds,
  };
}

export function decodeDocIds(docCount: number, codec: number, payload: Uint8Array): number[] {
  if (codec === DOCSET_CODEC_ALL) {
    return Array.from({ length: docCount }, (_, index) => index);
  }
  if (codec === DOCSET_CODEC_BITSET) {
    const out: number[] = [];
    for (let docId = 0; docId < docCount; docId++) {
      if (bitsetGet(payload, docId)) out.push(docId);
    }
    return out;
  }
  if (codec === DOCSET_CODEC_DELTA_DOCIDS) {
    const cursor = new BinaryCursor(payload);
    const out: number[] = [];
    let previous = 0;
    while (cursor.remaining() > 0) {
      const delta = Number(readUVarint(cursor));
      const next = out.length === 0 ? delta : previous + delta;
      out.push(next);
      previous = next;
    }
    return out;
  }
  throw new BinaryPayloadError(`unknown docset codec ${codec}`);
}

export function iterateDocIds(docCount: number, codec: number, payload: Uint8Array): Iterable<number> {
  return decodeDocIds(docCount, codec, payload);
}
