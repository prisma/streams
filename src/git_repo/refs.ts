import { Result } from "better-result";
import type {
  GitOid,
  GitRefTransactionCommittedRecord,
  GitRefTransactionRequest,
  GitRefUpdate,
  GitRepoRecord,
} from "./types";

export type GitRefError = {
  message: string;
};

export function normalizeGitRef(ref: string): string {
  const raw = ref.trim();
  if (raw === "HEAD") return raw;
  if (raw.startsWith("refs/")) return raw;
  return `refs/heads/${raw}`;
}

export function routingKeyForGitRef(ref: string): string {
  return `git-ref:${normalizeGitRef(ref)}`;
}

export function routingKeyForGitTxn(txnId: string): string {
  return `git-txn:${txnId}`;
}

export function isGitRefTransactionRecord(record: GitRepoRecord): record is GitRefTransactionCommittedRecord {
  return record.type === "ref-transaction-committed";
}

export function validateRefUpdateResult(update: GitRefUpdate): Result<GitRefUpdate, GitRefError> {
  const ref = normalizeGitRef(update.ref);
  if (!/^refs\/[A-Za-z0-9._/\-]+$/.test(ref) && ref !== "HEAD") {
    return Result.err({ message: `invalid git ref: ${update.ref}` });
  }
  return Result.ok({
    ref,
    oldOid: update.oldOid ?? null,
    newOid: update.newOid ?? null,
  });
}

export function validateRefTransactionRequestResult(body: GitRefTransactionRequest): Result<GitRefTransactionRequest, GitRefError> {
  if (!body || typeof body !== "object") return Result.err({ message: "ref transaction request must be an object" });
  if (!Array.isArray(body.refUpdates) || body.refUpdates.length === 0) {
    return Result.err({ message: "refUpdates must be a non-empty array" });
  }
  const refUpdates: GitRefUpdate[] = [];
  const seenRefs = new Set<string>();
  for (const raw of body.refUpdates.slice(0, 1000)) {
    const updateRes = validateRefUpdateResult(raw);
    if (Result.isError(updateRes)) return updateRes;
    if (seenRefs.has(updateRes.value.ref)) return Result.err({ message: `duplicate ref update: ${updateRes.value.ref}` });
    seenRefs.add(updateRes.value.ref);
    refUpdates.push(updateRes.value);
  }
  return Result.ok({
    txnId: typeof body.txnId === "string" && body.txnId.trim() !== "" ? body.txnId : undefined,
    idempotencyKey: typeof body.idempotencyKey === "string" && body.idempotencyKey.trim() !== "" ? body.idempotencyKey : undefined,
    actor: typeof body.actor === "string" && body.actor.trim() !== "" ? body.actor : undefined,
    refUpdates,
    objects: body.objects,
  });
}

export function buildRefs(records: GitRepoRecord[]): Record<string, GitOid | null> {
  const refs: Record<string, GitOid | null> = {};
  for (const record of records) {
    if (!isGitRefTransactionRecord(record)) continue;
    for (const update of record.refUpdates) {
      refs[normalizeGitRef(update.ref)] = update.newOid ?? null;
    }
  }
  return refs;
}

export function findTransaction(
  records: GitRepoRecord[],
  args: { txnId?: string; idempotencyKey?: string }
): GitRefTransactionCommittedRecord | null {
  for (let i = records.length - 1; i >= 0; i--) {
    const record = records[i]!;
    if (!isGitRefTransactionRecord(record)) continue;
    if (args.txnId && record.txnId === args.txnId) return record;
    if (args.idempotencyKey && record.idempotencyKey === args.idempotencyKey) return record;
  }
  return null;
}

export function applyRefUpdatesResult(
  refs: Record<string, GitOid | null>,
  updates: GitRefUpdate[]
): Result<Record<string, GitOid | null>, GitRefError> {
  const next = { ...refs };
  for (const update of updates) {
    const ref = normalizeGitRef(update.ref);
    const current = Object.prototype.hasOwnProperty.call(next, ref) ? next[ref] ?? null : null;
    if (current !== (update.oldOid ?? null)) {
      return Result.err({ message: `ref head changed for ${ref}` });
    }
    next[ref] = update.newOid ?? null;
  }
  return Result.ok(next);
}
