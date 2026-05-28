import { Buffer } from "node:buffer";
import { createHash, randomUUID } from "node:crypto";
import { Result } from "better-result";
import type { ObjectStore } from "../objectstore/interface";
import { encodeOffset } from "../offset";
import type { StreamReader } from "../reader";
import type { StreamProfileAppendJsonRecord, StreamProfileAppendJsonResult, StreamProfileAppendJsonError } from "../profiles/profile";
import {
  type GitLooseObjectResponse,
  type GitObjectFormat,
  type GitObjectType,
  type GitRefCheckpoint,
  type GitRefTransactionCommittedRecord,
  type GitRefTransactionRequest,
  type GitRepoObjectSet,
  type GitRepoRecord,
  type GitTransactionKeyIndexedRecord,
} from "./types";
import { hashGitObject, type GitObject } from "./objects";
import { gitLatestRefCheckpointKey, gitLooseObjectKey, gitObjectArtifactPrefix, gitRefCheckpointKey } from "./artifacts";
import { applyRefUpdatesResult, buildRefs, routingKeyForGitIdempotencyKey, routingKeyForGitTxn, validateRefTransactionRequestResult } from "./refs";
import { parseGitCommitBodyResult, parseGitTreeBodyResult } from "./tree";

export type GitRepoServiceError = {
  status: 400 | 404 | 409 | 500;
  message: string;
};

export type GitRepoAppendJsonRecords = (args: {
  stream: string;
  records: StreamProfileAppendJsonRecord[];
  ttlSeconds?: number | null;
  expectedNextOffset?: bigint | null;
}) => Promise<Result<StreamProfileAppendJsonResult, StreamProfileAppendJsonError>>;

export type GitRepoServiceArgs = {
  stream: string;
  reader: StreamReader;
  objectStore: ObjectStore;
  appendJsonRecords: GitRepoAppendJsonRecords;
};

export type GitRefTransactionCommitResult = {
  transaction: GitRefTransactionCommittedRecord;
  refs: Record<string, string | null>;
  idempotent: boolean;
};

export type GitRefTransactionVerificationMode = "full" | "changed-objects";

const TEXT_DECODER = new TextDecoder();
const GIT_OBJECT_HEADER_READ_BYTES = 512;
const GIT_OBJECT_SET_HEAD_CONCURRENCY = 128;

async function mapConcurrentGitResult<T, U>(
  items: T[],
  concurrency: number,
  fn: (item: T, index: number) => Promise<Result<U, GitRepoServiceError>>
): Promise<Result<U[], GitRepoServiceError>> {
  if (items.length === 0) return Result.ok([]);
  const results = new Array<U>(items.length);
  let next = 0;
  let firstError: GitRepoServiceError | null = null;
  const workerCount = Math.max(1, Math.min(concurrency, items.length));
  const workers = Array.from({ length: workerCount }, async () => {
    for (;;) {
      if (firstError) return;
      const index = next;
      next += 1;
      if (index >= items.length) return;
      const res = await fn(items[index]!, index);
      if (Result.isError(res)) {
        firstError = res.error;
        return;
      }
      results[index] = res.value;
    }
  });
  await Promise.all(workers);
  if (firstError) return Result.err(firstError);
  return Result.ok(results);
}

export type GitLooseObjectHeader = {
  type: GitObjectType;
  size: number;
  headerBytes: number;
};

export type GitRepoRecordSnapshot = {
  records: GitRepoRecord[];
  entries: Array<{ offset: bigint; record: GitRepoRecord }>;
  nextOffset: bigint;
};

type GitRepoTailSnapshot = GitRepoRecordSnapshot;

type GitRefsSnapshotCacheEntry = {
  refs: Record<string, string | null>;
  checkpoint: GitRefCheckpoint | null;
  nextOffset: bigint;
};

const gitRefsSnapshotCache = new Map<string, GitRefsSnapshotCacheEntry>();

export async function readGitRecordSnapshotResult(
  args: Pick<GitRepoServiceArgs, "stream" | "reader">
): Promise<Result<GitRepoRecordSnapshot, GitRepoServiceError>> {
  const values: GitRepoRecord[] = [];
  const entries: Array<{ offset: bigint; record: GitRepoRecord }> = [];
  let offset = "-1";
  for (;;) {
    const batchRes = await args.reader.readResult({ stream: args.stream, offset, key: null, format: "raw", filter: null });
    if (Result.isError(batchRes)) {
      if (batchRes.error.kind === "not_found" || batchRes.error.kind === "gone") return Result.ok({ records: values, entries, nextOffset: 0n });
      return Result.err({ status: 500, message: batchRes.error.message });
    }
    for (const record of batchRes.value.records) {
      try {
        const parsed = JSON.parse(TEXT_DECODER.decode(record.payload)) as Partial<GitRepoRecord>;
        if (typeof parsed.type === "string") {
          const value = parsed as GitRepoRecord;
          values.push(value);
          entries.push({ offset: record.offset, record: value });
        }
      } catch {
        return Result.err({ status: 500, message: `invalid JSON record in ${args.stream}` });
      }
    }
    if (batchRes.value.nextOffsetSeq >= batchRes.value.endOffsetSeq) {
      return Result.ok({ records: values, entries, nextOffset: batchRes.value.endOffsetSeq + 1n });
    }
    offset = batchRes.value.nextOffset;
  }
}

export async function readGitRecordsResult(args: Pick<GitRepoServiceArgs, "stream" | "reader">): Promise<Result<GitRepoRecord[], GitRepoServiceError>> {
  const snapshotRes = await readGitRecordSnapshotResult(args);
  if (Result.isError(snapshotRes)) return snapshotRes;
  return Result.ok(snapshotRes.value.records);
}

async function readGitRecordTailAfterOffsetResult(
  args: Pick<GitRepoServiceArgs, "stream" | "reader">,
  offset: number
): Promise<Result<GitRepoTailSnapshot, GitRepoServiceError>> {
  const values: GitRepoRecord[] = [];
  const entries: Array<{ offset: bigint; record: GitRepoRecord }> = [];
  let cursor = offset < 0 ? "-1" : encodeOffset(0, BigInt(offset), 0);
  for (;;) {
    const batchRes = await args.reader.readResult({ stream: args.stream, offset: cursor, key: null, format: "raw", filter: null });
    if (Result.isError(batchRes)) {
      if (batchRes.error.kind === "not_found" || batchRes.error.kind === "gone") return Result.ok({ records: values, entries, nextOffset: 0n });
      return Result.err({ status: 500, message: batchRes.error.message });
    }
    for (const record of batchRes.value.records) {
      try {
        const parsed = JSON.parse(TEXT_DECODER.decode(record.payload)) as Partial<GitRepoRecord>;
        if (typeof parsed.type === "string") {
          const value = parsed as GitRepoRecord;
          values.push(value);
          entries.push({ offset: record.offset, record: value });
        }
      } catch {
        return Result.err({ status: 500, message: `invalid JSON record in ${args.stream}` });
      }
    }
    if (batchRes.value.nextOffsetSeq >= batchRes.value.endOffsetSeq) {
      return Result.ok({ records: values, entries, nextOffset: batchRes.value.endOffsetSeq + 1n });
    }
    cursor = batchRes.value.nextOffset;
  }
}

async function readGitRecordsAfterOffsetResult(
  args: Pick<GitRepoServiceArgs, "stream" | "reader">,
  offset: number
): Promise<Result<GitRepoRecord[], GitRepoServiceError>> {
  const tailRes = await readGitRecordTailAfterOffsetResult(args, offset);
  if (Result.isError(tailRes)) return tailRes;
  return Result.ok(tailRes.value.records);
}

function applyRecordsToRefs(refs: Record<string, string | null>, records: GitRepoRecord[]): Record<string, string | null> {
  const next = { ...refs };
  for (const record of records) {
    if (record.type !== "ref-transaction-committed") continue;
    for (const update of record.refUpdates) next[update.ref] = update.newOid ?? null;
  }
  return next;
}

function cacheGitRefsSnapshot(
  stream: string,
  refs: Record<string, string | null>,
  nextOffset: bigint,
  checkpoint: GitRefCheckpoint | null = null
): void {
  gitRefsSnapshotCache.set(stream, { refs: { ...refs }, checkpoint, nextOffset });
}

async function readCachedGitRefsSnapshotResult(args: Pick<GitRepoServiceArgs, "stream" | "reader">): Promise<Result<{
  refs: Record<string, string | null>;
  checkpoint: GitRefCheckpoint | null;
  nextOffset: bigint;
} | null, GitRepoServiceError>> {
  const cached = gitRefsSnapshotCache.get(args.stream);
  if (!cached) return Result.ok(null);
  const lastCachedOffset = cached.nextOffset <= 0n ? -1 : Number(cached.nextOffset - 1n);
  const tailRes = await readGitRecordTailAfterOffsetResult(args, lastCachedOffset);
  if (Result.isError(tailRes)) return tailRes;
  if (tailRes.value.nextOffset < cached.nextOffset) {
    gitRefsSnapshotCache.delete(args.stream);
    return Result.ok(null);
  }
  const refs = applyRecordsToRefs(cached.refs, tailRes.value.records);
  cacheGitRefsSnapshot(args.stream, refs, tailRes.value.nextOffset, cached.checkpoint);
  return Result.ok({ refs, checkpoint: cached.checkpoint, nextOffset: tailRes.value.nextOffset });
}

function parseRefCheckpointBytesResult(bytes: Uint8Array, stream: string): Result<GitRefCheckpoint, GitRepoServiceError> {
  try {
    const parsed = JSON.parse(TEXT_DECODER.decode(bytes)) as Partial<GitRefCheckpoint>;
    if (parsed.repoId !== stream) return Result.err({ status: 500, message: "git ref checkpoint belongs to another repo" });
    if (!Number.isSafeInteger(parsed.streamOffset)) return Result.err({ status: 500, message: "git ref checkpoint streamOffset is invalid" });
    if (!parsed.refs || typeof parsed.refs !== "object" || Array.isArray(parsed.refs)) {
      return Result.err({ status: 500, message: "git ref checkpoint refs are invalid" });
    }
    if (!parsed.head || typeof parsed.head.symbolicRef !== "string") {
      return Result.err({ status: 500, message: "git ref checkpoint head is invalid" });
    }
    return Result.ok(parsed as GitRefCheckpoint);
  } catch {
    return Result.err({ status: 500, message: "git ref checkpoint JSON is invalid" });
  }
}

export async function writeGitRefCheckpointArtifactsResult(args: GitRepoServiceArgs & {
  format: GitObjectFormat;
  checkpoint: GitRefCheckpoint;
}): Promise<Result<{ refCheckpointUri: string; latestRefCheckpointUri: string }, GitRepoServiceError>> {
  const bytes = new TextEncoder().encode(JSON.stringify(args.checkpoint));
  const refCheckpointUri = gitRefCheckpointKey(args.stream, args.format, args.checkpoint.generation);
  const latestRefCheckpointUri = gitLatestRefCheckpointKey(args.stream, args.format);
  const putCheckpointRes = await putObjectStoreResult(args.objectStore, refCheckpointUri, bytes, "application/json");
  if (Result.isError(putCheckpointRes)) return putCheckpointRes;
  const putLatestRes = await putObjectStoreResult(args.objectStore, latestRefCheckpointUri, bytes, "application/json");
  if (Result.isError(putLatestRes)) return putLatestRes;
  cacheGitRefsSnapshot(args.stream, args.checkpoint.refs, BigInt(args.checkpoint.streamOffset) + 1n, args.checkpoint);
  return Result.ok({ refCheckpointUri, latestRefCheckpointUri });
}

export async function readGitRefsSnapshotResult(args: GitRepoServiceArgs & {
  format: GitObjectFormat;
}): Promise<Result<{ refs: Record<string, string | null>; checkpoint: GitRefCheckpoint | null; nextOffset: bigint }, GitRepoServiceError>> {
  const cachedRes = await readCachedGitRefsSnapshotResult(args);
  if (Result.isError(cachedRes)) return cachedRes;
  if (cachedRes.value) {
    return Result.ok({ refs: cachedRes.value.refs, checkpoint: cachedRes.value.checkpoint, nextOffset: cachedRes.value.nextOffset });
  }

  const latestKey = gitLatestRefCheckpointKey(args.stream, args.format);
  const checkpointBytesRes = await getObjectStoreBytesResult(args.objectStore, latestKey);
  if (Result.isError(checkpointBytesRes)) return checkpointBytesRes;
  if (checkpointBytesRes.value) {
    const checkpointRes = parseRefCheckpointBytesResult(checkpointBytesRes.value, args.stream);
    if (Result.isError(checkpointRes)) return checkpointRes;
    const tailRes = await readGitRecordTailAfterOffsetResult(args, checkpointRes.value.streamOffset);
    if (Result.isError(tailRes)) return tailRes;
    const refs = applyRecordsToRefs(checkpointRes.value.refs, tailRes.value.records);
    cacheGitRefsSnapshot(args.stream, refs, tailRes.value.nextOffset, checkpointRes.value);
    return Result.ok({
      refs,
      checkpoint: checkpointRes.value,
      nextOffset: tailRes.value.nextOffset,
    });
  }

  const snapshotRes = await readGitRecordSnapshotResult(args);
  if (Result.isError(snapshotRes)) return snapshotRes;
  const refs = buildRefs(snapshotRes.value.records);
  cacheGitRefsSnapshot(args.stream, refs, snapshotRes.value.nextOffset);
  return Result.ok({ refs, checkpoint: null, nextOffset: snapshotRes.value.nextOffset });
}

export async function appendGitRecordResult(
  args: Pick<GitRepoServiceArgs, "stream" | "appendJsonRecords">,
  record: GitRepoRecord,
  routingKey: string | null,
  expectedNextOffset?: bigint | null
): Promise<Result<void, GitRepoServiceError>> {
  return appendGitRecordsResult(args, [{ value: record, routingKey }], expectedNextOffset);
}

async function appendGitRecordsResult(
  args: Pick<GitRepoServiceArgs, "stream" | "appendJsonRecords">,
  records: Array<{ value: GitRepoRecord; routingKey: string | null }>,
  expectedNextOffset?: bigint | null
): Promise<Result<void, GitRepoServiceError>> {
  const appendRes = await args.appendJsonRecords({
    stream: args.stream,
    records,
    expectedNextOffset,
  });
  if (Result.isError(appendRes)) {
    if (appendRes.error.kind === "overloaded") return Result.err({ status: 500, message: "git-repo append overloaded" });
    if (appendRes.error.kind === "not_found" || appendRes.error.kind === "gone") return Result.err({ status: 404, message: "stream not found" });
    if (appendRes.error.kind === "offset_mismatch") return Result.err({ status: 409, message: "git-repo stream changed" });
    if (appendRes.error.kind === "content_type_mismatch") return Result.err({ status: 409, message: "content-type mismatch" });
    return Result.err({ status: 500, message: appendRes.error.message });
  }
  return Result.ok(undefined);
}

export async function headObjectStoreResult(
  objectStore: ObjectStore,
  key: string
): Promise<Result<{ etag: string; size: number } | null, GitRepoServiceError>> {
  try {
    return Result.ok(await objectStore.head(key));
  } catch {
    return Result.err({ status: 500, message: "git object-store HEAD failed" });
  }
}

export async function putObjectStoreResult(
  objectStore: ObjectStore,
  key: string,
  bytes: Uint8Array,
  contentType: string
): Promise<Result<{ etag: string }, GitRepoServiceError>> {
  try {
    return Result.ok(await objectStore.put(key, bytes, { contentType, contentLength: bytes.byteLength }));
  } catch {
    return Result.err({ status: 500, message: "git object-store PUT failed" });
  }
}

export async function getObjectStoreRangeResult(
  objectStore: ObjectStore,
  key: string,
  start: number,
  end: number
): Promise<Result<Uint8Array | null, GitRepoServiceError>> {
  try {
    return Result.ok(await objectStore.get(key, { range: { start, end } }));
  } catch {
    return Result.err({ status: 500, message: "git object-store GET failed" });
  }
}

export async function getObjectStoreBytesResult(
  objectStore: ObjectStore,
  key: string
): Promise<Result<Uint8Array | null, GitRepoServiceError>> {
  try {
    return Result.ok(await objectStore.get(key));
  } catch {
    return Result.err({ status: 500, message: "git object-store GET failed" });
  }
}

export async function readLooseGitObjectHeaderResult(args: {
  repoStream: string;
  objectStore: ObjectStore;
  format: GitObjectFormat;
  oid: string;
}): Promise<Result<GitLooseObjectHeader, GitRepoServiceError>> {
  const objectKey = gitLooseObjectKey(args.repoStream, args.format, args.oid);
  const prefixRes = await getObjectStoreRangeResult(args.objectStore, objectKey, 0, GIT_OBJECT_HEADER_READ_BYTES - 1);
  if (Result.isError(prefixRes)) return prefixRes;
  if (!prefixRes.value) return Result.err({ status: 404, message: "git object not found" });
  const nul = prefixRes.value.indexOf(0);
  if (nul < 0) return Result.err({ status: 500, message: "git object header is too large or invalid" });
  const header = TEXT_DECODER.decode(prefixRes.value.slice(0, nul));
  const match = header.match(/^(blob|tree|commit|tag) ([0-9]+)$/);
  if (!match) return Result.err({ status: 500, message: "git object header is invalid" });
  const size = Number(match[2]);
  if (!Number.isSafeInteger(size) || size < 0) return Result.err({ status: 500, message: "git object size is invalid" });
  return Result.ok({ type: match[1] as GitObjectType, size, headerBytes: nul + 1 });
}

export async function readLooseGitObjectBodyResult(args: {
  repoStream: string;
  objectStore: ObjectStore;
  format: GitObjectFormat;
  oid: string;
  expectedType?: GitObjectType;
  range?: { start: number; end: number } | null;
}): Promise<Result<{ header: GitLooseObjectHeader; body: Uint8Array }, GitRepoServiceError>> {
  const headerRes = await readLooseGitObjectHeaderResult(args);
  if (Result.isError(headerRes)) return headerRes;
  const header = headerRes.value;
  if (args.expectedType && header.type !== args.expectedType) {
    return Result.err({ status: 409, message: `git object is not ${args.expectedType}` });
  }
  const bodyRange = args.range ?? (header.size === 0 ? null : { start: 0, end: header.size - 1 });
  const expectedLength = bodyRange ? bodyRange.end - bodyRange.start + 1 : 0;
  const objectKey = gitLooseObjectKey(args.repoStream, args.format, args.oid);
  const bytesRes = bodyRange
    ? await getObjectStoreRangeResult(args.objectStore, objectKey, header.headerBytes + bodyRange.start, header.headerBytes + bodyRange.end)
    : Result.ok(new Uint8Array());
  if (Result.isError(bytesRes)) return bytesRes;
  if (!bytesRes.value) return Result.err({ status: 404, message: "git object not found" });
  if (bytesRes.value.byteLength !== expectedLength) {
    return Result.err({ status: 500, message: "git object range read returned partial data" });
  }
  return Result.ok({ header, body: bytesRes.value });
}

export async function writeLooseGitObjectResult(args: {
  repoStream: string;
  objectStore: ObjectStore;
  format: GitObjectFormat;
  object: GitObject;
}): Promise<Result<GitLooseObjectResponse, GitRepoServiceError>> {
  const objectKey = gitLooseObjectKey(args.repoStream, args.format, args.object.oid);
  const existingRes = await headObjectStoreResult(args.objectStore, objectKey);
  if (Result.isError(existingRes)) return existingRes;
  if (existingRes.value) {
    if (existingRes.value.size !== args.object.framed.byteLength) {
      return Result.err({ status: 409, message: "git object artifact key collision" });
    }
    return Result.ok({
      oid: args.object.oid,
      type: args.object.type,
      format: args.format,
      size: args.object.size,
      framedSize: args.object.framed.byteLength,
      objectKey,
      etag: existingRes.value.etag,
      deduplicated: true,
    });
  }

  const putRes = await putObjectStoreResult(args.objectStore, objectKey, args.object.framed, "application/x-git-loose-object");
  if (Result.isError(putRes)) return putRes;
  return Result.ok({
    oid: args.object.oid,
    type: args.object.type,
    format: args.format,
    size: args.object.size,
    framedSize: args.object.framed.byteLength,
    objectKey,
    etag: putRes.value.etag,
    deduplicated: false,
  });
}

export function parseBase64Result(value: unknown, label: string): Result<Uint8Array, GitRepoServiceError> {
  if (typeof value !== "string") return Result.err({ status: 400, message: `${label} must be base64` });
  return Result.ok(new Uint8Array(Buffer.from(value, "base64")));
}

export async function validateObjectSetResult(args: {
  repoStream: string;
  objectStore: ObjectStore;
  format: GitObjectFormat;
  objectSet: unknown;
}): Promise<Result<void, GitRepoServiceError>> {
  if (args.objectSet === undefined) return Result.ok(undefined);
  if (!args.objectSet || typeof args.objectSet !== "object" || Array.isArray(args.objectSet)) {
    return Result.err({ status: 400, message: "objects must be an object" });
  }
  const objects = args.objectSet as GitRepoObjectSet;
  if (!Number.isSafeInteger(objects.objectCount) || objects.objectCount < 0) {
    return Result.err({ status: 400, message: "objects.objectCount must be a non-negative integer" });
  }
  if (!Number.isSafeInteger(objects.bytes) || objects.bytes < 0) {
    return Result.err({ status: 400, message: "objects.bytes must be a non-negative integer" });
  }

  const prefix = `${gitObjectArtifactPrefix(args.repoStream, args.format)}/`;
  const uris: string[] = [];
  if (objects.packUri !== undefined) uris.push(objects.packUri);
  if (objects.idxUri !== undefined) uris.push(objects.idxUri);
  if (objects.looseObjectUris !== undefined) {
    if (!Array.isArray(objects.looseObjectUris)) return Result.err({ status: 400, message: "objects.looseObjectUris must be an array" });
    uris.push(...objects.looseObjectUris);
  }

  const headRes = await mapConcurrentGitResult(uris, GIT_OBJECT_SET_HEAD_CONCURRENCY, async (uri) => {
    if (typeof uri !== "string" || !uri.startsWith(prefix)) {
      return Result.err({ status: 400, message: "object artifact URI is outside this git-repo stream" });
    }
    const itemHeadRes = await headObjectStoreResult(args.objectStore, uri);
    if (Result.isError(itemHeadRes)) return itemHeadRes;
    if (!itemHeadRes.value) return Result.err({ status: 409, message: `object artifact missing: ${uri}` });
    return Result.ok(undefined);
  });
  if (Result.isError(headRes)) return headRes;
  return Result.ok(undefined);
}

function oidPattern(format: GitObjectFormat): RegExp {
  return format === "sha256" ? /^[0-9a-f]{64}$/ : /^[0-9a-f]{40}$/;
}

function gitError(status: GitRepoServiceError["status"], message: string): GitRepoServiceError {
  return { status, message };
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, item]) => item !== undefined)
    .sort(([a], [b]) => a.localeCompare(b));
  return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`).join(",")}}`;
}

function sortedObjectSet(objects: GitRepoObjectSet | undefined): GitRepoObjectSet | undefined {
  if (!objects) return undefined;
  return {
    ...objects,
    looseObjectUris: objects.looseObjectUris ? objects.looseObjectUris.slice().sort((a, b) => a.localeCompare(b)) : undefined,
  };
}

function requestHashForRefTransaction(request: GitRefTransactionRequest): string {
  const refUpdates = request.refUpdates
    .slice()
    .sort((a, b) => a.ref.localeCompare(b.ref))
    .map((update) => ({ ref: update.ref, oldOid: update.oldOid ?? null, newOid: update.newOid ?? null }));
  return createHash("sha256").update(canonicalJson({ refUpdates, objects: sortedObjectSet(request.objects) })).digest("hex");
}

async function readGitTransactionByTxnIdResult(
  args: Pick<GitRepoServiceArgs, "stream" | "reader">,
  txnId: string
): Promise<Result<GitRefTransactionCommittedRecord | null, GitRepoServiceError>> {
  let cursor = "-1";
  const key = routingKeyForGitTxn(txnId);
  for (;;) {
    const batchRes = await args.reader.readResult({ stream: args.stream, offset: cursor, key, format: "raw", filter: null });
    if (Result.isError(batchRes)) {
      if (batchRes.error.kind === "not_found" || batchRes.error.kind === "gone") return Result.ok(null);
      return Result.err({ status: 500, message: batchRes.error.message });
    }
    for (const record of batchRes.value.records) {
      try {
        const parsed = JSON.parse(TEXT_DECODER.decode(record.payload)) as Partial<GitRepoRecord>;
        if (parsed.type === "ref-transaction-committed" && parsed.txnId === txnId) {
          return Result.ok(parsed as GitRefTransactionCommittedRecord);
        }
      } catch {
        return Result.err({ status: 500, message: `invalid JSON record in ${args.stream}` });
      }
    }
    if (batchRes.value.nextOffsetSeq >= batchRes.value.endOffsetSeq) return Result.ok(null);
    cursor = batchRes.value.nextOffset;
  }
}

async function readGitTransactionByIdempotencyKeyResult(
  args: Pick<GitRepoServiceArgs, "stream" | "reader">,
  idempotencyKey: string
): Promise<Result<GitRefTransactionCommittedRecord | null, GitRepoServiceError>> {
  let cursor = "-1";
  const key = routingKeyForGitIdempotencyKey(idempotencyKey);
  for (;;) {
    const batchRes = await args.reader.readResult({ stream: args.stream, offset: cursor, key, format: "raw", filter: null });
    if (Result.isError(batchRes)) {
      if (batchRes.error.kind === "not_found" || batchRes.error.kind === "gone") return Result.ok(null);
      return Result.err({ status: 500, message: batchRes.error.message });
    }
    for (const record of batchRes.value.records) {
      try {
        const parsed = JSON.parse(TEXT_DECODER.decode(record.payload)) as Partial<GitRepoRecord>;
        if (
          parsed.type === "transaction-key-indexed" &&
          parsed.keyType === "idempotency-key" &&
          parsed.key === idempotencyKey &&
          typeof parsed.txnId === "string"
        ) {
          const txnRes = await readGitTransactionByTxnIdResult(args, parsed.txnId);
          if (Result.isError(txnRes)) return txnRes;
          if (!txnRes.value) return Result.err({ status: 500, message: "git idempotency index points at a missing transaction" });
          if (txnRes.value.requestHash !== parsed.requestHash) {
            return Result.err({ status: 500, message: "git idempotency index request hash mismatch" });
          }
          return Result.ok(txnRes.value);
        }
      } catch {
        return Result.err({ status: 500, message: `invalid JSON record in ${args.stream}` });
      }
    }
    if (batchRes.value.nextOffsetSeq >= batchRes.value.endOffsetSeq) return Result.ok(null);
    cursor = batchRes.value.nextOffset;
  }
}

async function readExistingTransactionResult(
  args: Pick<GitRepoServiceArgs, "stream" | "reader">,
  request: GitRefTransactionRequest
): Promise<Result<GitRefTransactionCommittedRecord | null, GitRepoServiceError>> {
  const txnRes = request.txnId ? await readGitTransactionByTxnIdResult(args, request.txnId) : Result.ok(null);
  if (Result.isError(txnRes)) return txnRes;
  const idempotencyRes = request.idempotencyKey
    ? await readGitTransactionByIdempotencyKeyResult(args, request.idempotencyKey)
    : Result.ok(null);
  if (Result.isError(idempotencyRes)) return idempotencyRes;
  if (txnRes.value && idempotencyRes.value && txnRes.value.txnId !== idempotencyRes.value.txnId) {
    return Result.err({ status: 409, message: "transaction id and idempotency key refer to different transactions" });
  }
  return Result.ok(txnRes.value ?? idempotencyRes.value);
}

function parseTagTarget(body: Uint8Array, format: GitObjectFormat): string | null {
  const text = TEXT_DECODER.decode(body);
  const objectLine = text.split("\n").find((line) => line.startsWith("object "));
  if (!objectLine) return null;
  const oid = objectLine.slice("object ".length).trim();
  return oidPattern(format).test(oid) ? oid : null;
}

async function readVerifiedObjectResult(args: {
  repoStream: string;
  objectStore: ObjectStore;
  format: GitObjectFormat;
  oid: string;
}): Promise<Result<{ header: GitLooseObjectHeader; body: Uint8Array }, GitRepoServiceError>> {
  const objectRes = await readLooseGitObjectBodyResult({
    repoStream: args.repoStream,
    objectStore: args.objectStore,
    format: args.format,
    oid: args.oid,
  });
  if (Result.isError(objectRes)) {
    if (objectRes.error.status === 404) return Result.err(gitError(409, `referenced git object is missing: ${args.oid}`));
    return objectRes;
  }
  const actualOid = hashGitObject(objectRes.value.header.type, objectRes.value.body, args.format);
  if (actualOid !== args.oid) return Result.err(gitError(409, `git object hash mismatch for ${args.oid}`));
  return objectRes;
}

function expectedChildType(entryType: "file" | "dir" | "symlink"): GitObjectType {
  return entryType === "dir" ? "tree" : "blob";
}

async function validateReachableGitObjectGraphResult(args: {
  repoStream: string;
  objectStore: ObjectStore;
  format: GitObjectFormat;
  ref: string;
  rootOid: string;
}): Promise<Result<void, GitRepoServiceError>> {
  const visited = new Set<string>();
  const visiting = new Set<string>();

  const visit = async (oid: string, expectedType?: GitObjectType): Promise<Result<void, GitRepoServiceError>> => {
    if (visited.has(oid)) return Result.ok(undefined);
    if (visiting.has(oid)) return Result.ok(undefined);
    if (!oidPattern(args.format).test(oid)) return Result.err(gitError(400, `invalid git object id: ${oid}`));
    visiting.add(oid);
    const objectRes = await readVerifiedObjectResult({
      repoStream: args.repoStream,
      objectStore: args.objectStore,
      format: args.format,
      oid,
    });
    if (Result.isError(objectRes)) return objectRes;
    const objectType = objectRes.value.header.type;
    if (expectedType && objectType !== expectedType) {
      return Result.err(gitError(409, `git object ${oid} is ${objectType}, expected ${expectedType}`));
    }
    if (oid === args.rootOid && args.ref.startsWith("refs/heads/") && objectType !== "commit") {
      return Result.err(gitError(409, `branch ref ${args.ref} must point at a commit`));
    }
    if (oid === args.rootOid && objectType !== "commit" && objectType !== "tag") {
      return Result.err(gitError(409, `ref ${args.ref} must point at a commit or tag`));
    }

    if (objectType === "commit") {
      const commitRes = parseGitCommitBodyResult(objectRes.value.body, args.format);
      if (Result.isError(commitRes)) return Result.err(gitError(409, commitRes.error.message));
      const treeRes = await visit(commitRes.value.tree, "tree");
      if (Result.isError(treeRes)) return treeRes;
      for (const parent of commitRes.value.parents) {
        const parentRes = await visit(parent, "commit");
        if (Result.isError(parentRes)) return parentRes;
      }
    } else if (objectType === "tree") {
      const treeRes = parseGitTreeBodyResult(objectRes.value.body, args.format);
      if (Result.isError(treeRes)) return Result.err(gitError(409, treeRes.error.message));
      for (const entry of treeRes.value) {
        const childRes = await visit(entry.oid, expectedChildType(entry.type));
        if (Result.isError(childRes)) return childRes;
      }
    } else if (objectType === "tag") {
      const target = parseTagTarget(objectRes.value.body, args.format);
      if (!target) return Result.err(gitError(409, `git tag ${oid} is missing an object target`));
      const targetRes = await visit(target);
      if (Result.isError(targetRes)) return targetRes;
    }

    visiting.delete(oid);
    visited.add(oid);
    return Result.ok(undefined);
  };

  return visit(args.rootOid);
}

async function validateRefTransactionObjectsResult(args: {
  repoStream: string;
  objectStore: ObjectStore;
  format: GitObjectFormat;
  request: GitRefTransactionRequest;
}): Promise<Result<void, GitRepoServiceError>> {
  for (const update of args.request.refUpdates) {
    if (!update.newOid) continue;
    const graphRes = await validateReachableGitObjectGraphResult({
      repoStream: args.repoStream,
      objectStore: args.objectStore,
      format: args.format,
      ref: update.ref,
      rootOid: update.newOid,
    });
    if (Result.isError(graphRes)) return graphRes;
  }
  return Result.ok(undefined);
}

async function validateChangedRefTransactionObjectsResult(args: {
  repoStream: string;
  objectStore: ObjectStore;
  format: GitObjectFormat;
  request: GitRefTransactionRequest;
}): Promise<Result<void, GitRepoServiceError>> {
  for (const update of args.request.refUpdates) {
    if (!update.newOid) continue;
    const objectRes = await readVerifiedObjectResult({
      repoStream: args.repoStream,
      objectStore: args.objectStore,
      format: args.format,
      oid: update.newOid,
    });
    if (Result.isError(objectRes)) return objectRes;
    const objectType = objectRes.value.header.type;
    if (update.ref.startsWith("refs/heads/") && objectType !== "commit") {
      return Result.err(gitError(409, `branch ref ${update.ref} must point at a commit`));
    }
    if (objectType !== "commit" && objectType !== "tag") {
      return Result.err(gitError(409, `ref ${update.ref} must point at a commit or tag`));
    }
  }
  return Result.ok(undefined);
}

export async function verifyGitRefTransactionRecordResult(args: {
  repoStream: string;
  objectStore: ObjectStore;
  format: GitObjectFormat;
  transaction: GitRefTransactionCommittedRecord;
}): Promise<Result<void, GitRepoServiceError>> {
  const oidShapeRes = validateRefTransactionOidShapesResult(args.format, args.transaction);
  if (Result.isError(oidShapeRes)) return oidShapeRes;
  const objectSetRes = await validateObjectSetResult({
    repoStream: args.repoStream,
    objectStore: args.objectStore,
    format: args.format,
    objectSet: args.transaction.objects,
  });
  if (Result.isError(objectSetRes)) return objectSetRes;
  return validateRefTransactionObjectsResult({
    repoStream: args.repoStream,
    objectStore: args.objectStore,
    format: args.format,
    request: args.transaction,
  });
}

function validateRefTransactionOidShapesResult(format: GitObjectFormat, request: GitRefTransactionRequest): Result<void, GitRepoServiceError> {
  const oidRe = oidPattern(format);
  for (const update of request.refUpdates) {
    if (update.oldOid !== null && !oidRe.test(update.oldOid)) return Result.err(gitError(400, `invalid oldOid for ${update.ref}`));
    if (update.newOid !== null && !oidRe.test(update.newOid)) return Result.err(gitError(400, `invalid newOid for ${update.ref}`));
  }
  return Result.ok(undefined);
}

export async function commitGitRefTransactionResult(args: GitRepoServiceArgs & {
  format: GitObjectFormat;
  request: GitRefTransactionRequest;
  verificationMode?: GitRefTransactionVerificationMode;
}): Promise<Result<GitRefTransactionCommitResult, GitRepoServiceError>> {
  const requestRes = validateRefTransactionRequestResult(args.request);
  if (Result.isError(requestRes)) return Result.err({ status: 400, message: requestRes.error.message });
  const request = requestRes.value;
  const requestHash = requestHashForRefTransaction(request);
  const oidShapeRes = validateRefTransactionOidShapesResult(args.format, request);
  if (Result.isError(oidShapeRes)) return oidShapeRes;
  const verificationMode = args.verificationMode ?? "full";
  let requestObjectsValidated = false;

  for (let attempt = 0; attempt < 5; attempt++) {
    const refsSnapshotRes = await readGitRefsSnapshotResult(args);
    if (Result.isError(refsSnapshotRes)) return refsSnapshotRes;
    const currentRefs = refsSnapshotRes.value.refs;
    const expectedNextOffset = refsSnapshotRes.value.nextOffset;
    const existingRes = await readExistingTransactionResult(args, request);
    if (Result.isError(existingRes)) return existingRes;
    const existing = existingRes.value;
    if (existing) {
      if (existing.requestHash !== requestHash) {
        return Result.err({ status: 409, message: "idempotency key or transaction id was already used for a different request" });
      }
      return Result.ok({
        transaction: existing,
        refs: currentRefs,
        idempotent: true,
      });
    }

    const nextRefsRes = applyRefUpdatesResult(currentRefs, request.refUpdates);
    if (Result.isError(nextRefsRes)) return Result.err({ status: 409, message: nextRefsRes.error.message });

    if (!requestObjectsValidated) {
      const objectSetRes = await validateObjectSetResult({
        repoStream: args.stream,
        objectStore: args.objectStore,
        format: args.format,
        objectSet: request.objects,
      });
      if (Result.isError(objectSetRes)) return objectSetRes;
      const graphRes = verificationMode === "full"
        ? await validateRefTransactionObjectsResult({
            repoStream: args.stream,
            objectStore: args.objectStore,
            format: args.format,
            request,
          })
        : await validateChangedRefTransactionObjectsResult({
            repoStream: args.stream,
            objectStore: args.objectStore,
            format: args.format,
            request,
          });
      if (Result.isError(graphRes)) return graphRes;
      requestObjectsValidated = true;
    }

    const transaction: GitRefTransactionCommittedRecord = {
      type: "ref-transaction-committed",
      repoId: args.stream,
      txnId: request.txnId ?? randomUUID(),
      idempotencyKey: request.idempotencyKey,
      requestHash,
      actor: request.actor,
      createdAt: new Date().toISOString(),
      refUpdates: request.refUpdates,
      objects: request.objects,
    };
    const records: Array<{ value: GitRepoRecord; routingKey: string | null }> = [
      { value: transaction, routingKey: routingKeyForGitTxn(transaction.txnId) },
    ];
    if (request.idempotencyKey) {
      const idempotencyIndex: GitTransactionKeyIndexedRecord = {
        type: "transaction-key-indexed",
        repoId: args.stream,
        keyType: "idempotency-key",
        key: request.idempotencyKey,
        txnId: transaction.txnId,
        requestHash,
        createdAt: transaction.createdAt,
      };
      records.push({ value: idempotencyIndex, routingKey: routingKeyForGitIdempotencyKey(request.idempotencyKey) });
    }
    const appendRes = await appendGitRecordsResult(args, records, expectedNextOffset);
    if (Result.isError(appendRes)) {
      if (appendRes.error.status === 409 && appendRes.error.message === "git-repo stream changed") continue;
      return appendRes;
    }

    return Result.ok({
      transaction,
      refs: nextRefsRes.value,
      idempotent: false,
    });
  }

  return Result.err({ status: 409, message: "git-repo stream changed" });
}
