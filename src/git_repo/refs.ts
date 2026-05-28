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

export const MAX_GIT_REF_UPDATES = 1000;

export function normalizeGitRef(ref: string): string {
  const raw = ref.trim();
  if (raw === "HEAD") return raw;
  if (raw.startsWith("refs/")) return raw;
  return `refs/heads/${raw}`;
}

export function validateGitRefNameResult(ref: string, opts: { allowHead?: boolean } = {}): Result<string, GitRefError> {
  const normalized = normalizeGitRef(ref);
  if (normalized === "HEAD") {
    return opts.allowHead === true ? Result.ok(normalized) : Result.err({ message: "HEAD is symbolic and cannot be updated as a ref" });
  }
  if (!normalized.startsWith("refs/")) return Result.err({ message: `invalid git ref: ${ref}` });
  if (normalized.length > 1024) return Result.err({ message: `invalid git ref: ${ref}` });
  if (normalized.startsWith("/") || normalized.endsWith("/") || normalized.includes("//")) {
    return Result.err({ message: `invalid git ref: ${ref}` });
  }
  if (normalized.endsWith(".") || normalized.includes("..") || normalized.includes("@{") || normalized === "@") {
    return Result.err({ message: `invalid git ref: ${ref}` });
  }
  if (/[\x00-\x20\x7f~^:?*[\\]/.test(normalized)) {
    return Result.err({ message: `invalid git ref: ${ref}` });
  }
  const parts = normalized.split("/");
  if (parts.length < 3) return Result.err({ message: `invalid git ref: ${ref}` });
  for (const part of parts) {
    if (part === "" || part.startsWith(".") || part.endsWith(".lock")) {
      return Result.err({ message: `invalid git ref: ${ref}` });
    }
  }
  return Result.ok(normalized);
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
  if (!update || typeof update !== "object") return Result.err({ message: "ref update must be an object" });
  if (typeof update.ref !== "string" || update.ref.trim() === "") return Result.err({ message: "ref update ref must be a non-empty string" });
  const refRes = validateGitRefNameResult(update.ref, { allowHead: false });
  if (Result.isError(refRes)) return refRes;
  if (update.oldOid !== null && update.oldOid !== undefined && typeof update.oldOid !== "string") {
    return Result.err({ message: `oldOid for ${update.ref} must be a string or null` });
  }
  if (update.newOid !== null && update.newOid !== undefined && typeof update.newOid !== "string") {
    return Result.err({ message: `newOid for ${update.ref} must be a string or null` });
  }
  return Result.ok({
    ref: refRes.value,
    oldOid: update.oldOid ?? null,
    newOid: update.newOid ?? null,
  });
}

export function validateRefTransactionRequestResult(body: GitRefTransactionRequest): Result<GitRefTransactionRequest, GitRefError> {
  if (!body || typeof body !== "object") return Result.err({ message: "ref transaction request must be an object" });
  if (!Array.isArray(body.refUpdates) || body.refUpdates.length === 0) {
    return Result.err({ message: "refUpdates must be a non-empty array" });
  }
  if (body.refUpdates.length > MAX_GIT_REF_UPDATES) {
    return Result.err({ message: `refUpdates must contain at most ${MAX_GIT_REF_UPDATES} entries` });
  }
  const refUpdates: GitRefUpdate[] = [];
  const seenRefs = new Set<string>();
  for (const raw of body.refUpdates) {
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
