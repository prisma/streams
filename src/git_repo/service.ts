import { Buffer } from "node:buffer";
import { randomUUID } from "node:crypto";
import { Result } from "better-result";
import type { ObjectStore } from "../objectstore/interface";
import type { StreamReader } from "../reader";
import type { StreamProfileAppendJsonRecord, StreamProfileAppendJsonResult, StreamProfileAppendJsonError } from "../profiles/profile";
import {
  type GitLooseObjectResponse,
  type GitObjectFormat,
  type GitObjectType,
  type GitRefTransactionCommittedRecord,
  type GitRefTransactionRequest,
  type GitRepoObjectSet,
  type GitRepoRecord,
} from "./types";
import type { GitObject } from "./objects";
import { gitLooseObjectKey, gitObjectArtifactPrefix } from "./artifacts";
import { applyRefUpdatesResult, buildRefs, findTransaction, routingKeyForGitTxn } from "./refs";

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

const TEXT_DECODER = new TextDecoder();
const GIT_OBJECT_HEADER_READ_BYTES = 512;

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

export async function appendGitRecordResult(
  args: Pick<GitRepoServiceArgs, "stream" | "appendJsonRecords">,
  record: GitRepoRecord,
  routingKey: string | null,
  expectedNextOffset?: bigint | null
): Promise<Result<void, GitRepoServiceError>> {
  const appendRes = await args.appendJsonRecords({
    stream: args.stream,
    records: [{ value: record, routingKey }],
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

  for (const uri of uris) {
    if (typeof uri !== "string" || !uri.startsWith(prefix)) {
      return Result.err({ status: 400, message: "object artifact URI is outside this git-repo stream" });
    }
    const headRes = await headObjectStoreResult(args.objectStore, uri);
    if (Result.isError(headRes)) return headRes;
    if (!headRes.value) return Result.err({ status: 409, message: `object artifact missing: ${uri}` });
  }
  return Result.ok(undefined);
}

export async function commitGitRefTransactionResult(args: GitRepoServiceArgs & {
  format: GitObjectFormat;
  request: GitRefTransactionRequest;
}): Promise<Result<GitRefTransactionCommitResult, GitRepoServiceError>> {
  const objectSetRes = await validateObjectSetResult({
    repoStream: args.stream,
    objectStore: args.objectStore,
    format: args.format,
    objectSet: args.request.objects,
  });
  if (Result.isError(objectSetRes)) return objectSetRes;

  for (let attempt = 0; attempt < 5; attempt++) {
    const snapshotRes = await readGitRecordSnapshotResult(args);
    if (Result.isError(snapshotRes)) return snapshotRes;
    const records = snapshotRes.value.records;

    const existing = findTransaction(records, {
      txnId: args.request.txnId,
      idempotencyKey: args.request.idempotencyKey,
    });
    if (existing) {
      return Result.ok({
        transaction: existing,
        refs: buildRefs(records),
        idempotent: true,
      });
    }

    const currentRefs = buildRefs(records);
    const nextRefsRes = applyRefUpdatesResult(currentRefs, args.request.refUpdates);
    if (Result.isError(nextRefsRes)) return Result.err({ status: 409, message: nextRefsRes.error.message });

    const transaction: GitRefTransactionCommittedRecord = {
      type: "ref-transaction-committed",
      repoId: args.stream,
      txnId: args.request.txnId ?? randomUUID(),
      idempotencyKey: args.request.idempotencyKey,
      actor: args.request.actor,
      createdAt: new Date().toISOString(),
      refUpdates: args.request.refUpdates,
      objects: args.request.objects,
    };
    const appendRes = await appendGitRecordResult(args, transaction, routingKeyForGitTxn(transaction.txnId), snapshotRes.value.nextOffset);
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
