import { Buffer } from "node:buffer";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { deflateSync } from "node:zlib";
import { createHash, randomUUID } from "node:crypto";
import { Result } from "better-result";
import {
  buildGitObject,
  frameGitObject,
  type GitObject,
} from "./objects";
import {
  commitGitRefTransactionResult,
  readGitRecordsResult,
  readLooseGitObjectBodyResult,
  appendGitRecordResult,
  putObjectStoreResult,
  writeLooseGitObjectResult,
  type GitRepoServiceArgs,
  type GitRepoServiceError,
} from "./service";
import { buildRefCheckpoint } from "./maintenance";
import { gitObjectArtifactPrefix, gitPackIndexPath, gitPackPath } from "./artifacts";
import { buildRefs, normalizeGitRef } from "./refs";
import { parseGitCommitBodyResult, parseGitTreeBodyResult } from "./tree";
import type {
  GitImportRequest,
  GitImportResponse,
  GitLooseObjectResponse,
  GitMaintenancePublishedRecord,
  GitObjectFormat,
  GitObjectType,
  GitOid,
  GitRefUpdate,
  GitRepoProfileConfig,
} from "./types";

type GitImportExportArgs = GitRepoServiceArgs & {
  config: GitRepoProfileConfig;
  gitProtocol?: string | null;
};

type GitCliObject = {
  oid: GitOid;
  type: GitObjectType;
  body: Uint8Array;
};

type ExportObject = GitCliObject;

const TEXT_DECODER = new TextDecoder();

function gitError(status: GitRepoServiceError["status"], message: string): GitRepoServiceError {
  return { status, message };
}

function gitBinary(config: GitRepoProfileConfig): string {
  const configured = config.importExport?.gitBinary;
  return configured && configured.trim() !== "" ? configured.trim() : "git";
}

function importExportEnabled(config: GitRepoProfileConfig): boolean {
  return config.importExport?.enabled !== false;
}

function maxImportExportBytes(config: GitRepoProfileConfig): number {
  const value = config.importExport?.maxBytes;
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0 ? value : 512 * 1024 * 1024;
}

function maxPushBytes(config: GitRepoProfileConfig): number {
  const value = config.push?.maxPackBytes;
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0 ? value : maxImportExportBytes(config);
}

function runGitResult(
  config: GitRepoProfileConfig,
  args: string[],
  opts: { input?: Uint8Array; cwd?: string; maxBuffer?: number; status?: GitRepoServiceError["status"]; env?: Record<string, string> } = {}
): Result<Buffer, GitRepoServiceError> {
  const proc = spawnSync(gitBinary(config), args, {
    cwd: opts.cwd,
    env: opts.env ? { ...process.env, ...opts.env } : undefined,
    input: opts.input ? Buffer.from(opts.input.buffer, opts.input.byteOffset, opts.input.byteLength) : undefined,
    maxBuffer: opts.maxBuffer ?? maxImportExportBytes(config),
  });
  if (proc.error) return Result.err(gitError(500, proc.error.message));
  if (proc.status !== 0) {
    const stderr = TEXT_DECODER.decode(proc.stderr).trim();
    return Result.err(gitError(opts.status ?? 409, stderr || `git ${args[0] ?? ""} failed`));
  }
  return Result.ok(proc.stdout);
}

function isGitObjectType(value: string): value is GitObjectType {
  return value === "blob" || value === "tree" || value === "commit" || value === "tag";
}

function parseRefsMap(value: unknown): Result<Record<string, GitOid>, GitRepoServiceError> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return Result.err(gitError(400, "refs must be an object"));
  const refs: Record<string, GitOid> = {};
  for (const [rawRef, rawOid] of Object.entries(value as Record<string, unknown>)) {
    if (typeof rawOid !== "string" || !/^[0-9a-f]{40}$/.test(rawOid)) return Result.err(gitError(400, `invalid oid for ref ${rawRef}`));
    refs[normalizeGitRef(rawRef)] = rawOid;
  }
  return Result.ok(refs);
}

function parseImportRequestResult(raw: unknown): Result<GitImportRequest, GitRepoServiceError> {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return Result.err(gitError(400, "git import request must be an object"));
  const body = raw as Record<string, unknown>;
  const common = {
    txnId: typeof body.txnId === "string" && body.txnId.trim() !== "" ? body.txnId.trim() : undefined,
    idempotencyKey: typeof body.idempotencyKey === "string" && body.idempotencyKey.trim() !== "" ? body.idempotencyKey.trim() : undefined,
    actor: typeof body.actor === "string" && body.actor.trim() !== "" ? body.actor.trim() : undefined,
  };
  if (body.format === "bundle") {
    if (typeof body.bundleBase64 !== "string") return Result.err(gitError(400, "bundleBase64 must be base64"));
    return Result.ok({ format: "bundle", bundleBase64: body.bundleBase64, ...common });
  }
  if (body.format === "pack") {
    if (typeof body.packBase64 !== "string") return Result.err(gitError(400, "packBase64 must be base64"));
    const refsRes = parseRefsMap(body.refs);
    if (Result.isError(refsRes)) return refsRes;
    return Result.ok({ format: "pack", packBase64: body.packBase64, refs: refsRes.value, ...common });
  }
  if (body.format === "local-bare-repo") {
    if (typeof body.path !== "string" || body.path.trim() === "") return Result.err(gitError(400, "path is required"));
    const refs = Array.isArray(body.refs) ? body.refs.filter((ref): ref is string => typeof ref === "string") : undefined;
    return Result.ok({ format: "local-bare-repo", path: body.path.trim(), refs, ...common });
  }
  return Result.err(gitError(400, "format must be bundle, pack, or local-bare-repo"));
}

function decodeBase64Result(value: string, label: string, maxBytes: number): Result<Uint8Array, GitRepoServiceError> {
  try {
    const bytes = new Uint8Array(Buffer.from(value, "base64"));
    if (bytes.byteLength > maxBytes) return Result.err(gitError(400, `${label} exceeds maxBytes`));
    return Result.ok(bytes);
  } catch {
    return Result.err(gitError(400, `${label} must be base64`));
  }
}

function listRefsFromGitDirResult(
  config: GitRepoProfileConfig,
  gitDir: string,
  requestedRefs?: string[]
): Result<Record<string, GitOid>, GitRepoServiceError> {
  const args = ["--git-dir", gitDir, "for-each-ref", "--format=%(objectname) %(refname)", "refs/heads", "refs/tags"];
  const refsRes = runGitResult(config, args);
  if (Result.isError(refsRes)) return refsRes;
  const refs: Record<string, GitOid> = {};
  const requested = requestedRefs ? new Set(requestedRefs.map(normalizeGitRef)) : null;
  for (const line of TEXT_DECODER.decode(refsRes.value).split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const [oid, ref] = trimmed.split(/\s+/, 2);
    if (!oid || !ref) continue;
    const normalized = normalizeGitRef(ref);
    if (requested && !requested.has(normalized)) continue;
    refs[normalized] = oid;
  }
  return Result.ok(refs);
}

function listObjectIdsFromGitDirResult(
  config: GitRepoProfileConfig,
  gitDir: string,
  refs: Record<string, GitOid>
): Result<GitOid[], GitRepoServiceError> {
  const roots = Object.values(refs);
  if (roots.length === 0) return Result.err(gitError(400, "no refs to import"));
  const revListRes = runGitResult(config, ["--git-dir", gitDir, "rev-list", "--objects", "--no-object-names", ...roots]);
  if (Result.isError(revListRes)) return revListRes;
  const objectIds = new Set<GitOid>(roots);
  for (const line of TEXT_DECODER.decode(revListRes.value).split("\n")) {
    const oid = line.trim();
    if (/^[0-9a-f]{40}$/.test(oid)) objectIds.add(oid);
  }
  return Result.ok(Array.from(objectIds).sort());
}

function readObjectFromGitDirResult(
  config: GitRepoProfileConfig,
  gitDir: string,
  oid: GitOid
): Result<GitCliObject, GitRepoServiceError> {
  const typeRes = runGitResult(config, ["--git-dir", gitDir, "cat-file", "-t", oid]);
  if (Result.isError(typeRes)) return typeRes;
  const type = TEXT_DECODER.decode(typeRes.value).trim();
  if (!isGitObjectType(type)) return Result.err(gitError(409, `unsupported git object type: ${type}`));
  const bodyRes = runGitResult(config, ["--git-dir", gitDir, "cat-file", type, oid]);
  if (Result.isError(bodyRes)) return bodyRes;
  return Result.ok({ oid, type, body: new Uint8Array(bodyRes.value) });
}

async function importObjectsFromGitDirResult(
  args: GitImportExportArgs,
  gitDir: string,
  refs: Record<string, GitOid>,
): Promise<Result<{ artifacts: GitLooseObjectResponse[]; objectCount: number; bytes: number }, GitRepoServiceError>> {
  if (args.config.objectFormat !== "sha1") return Result.err(gitError(400, "Git CLI import currently requires sha1 objectFormat"));
  const objectIdsRes = listObjectIdsFromGitDirResult(args.config, gitDir, refs);
  if (Result.isError(objectIdsRes)) return objectIdsRes;
  const artifacts: GitLooseObjectResponse[] = [];
  let bytes = 0;
  for (const oid of objectIdsRes.value) {
    const objectRes = readObjectFromGitDirResult(args.config, gitDir, oid);
    if (Result.isError(objectRes)) return objectRes;
    const object = buildGitObject(objectRes.value.type, objectRes.value.body, args.config.objectFormat);
    if (object.oid !== oid) return Result.err(gitError(409, `git object hash mismatch for ${oid}`));
    const writeRes = await writeLooseGitObjectResult({
      repoStream: args.stream,
      objectStore: args.objectStore,
      format: args.config.objectFormat,
      object,
    });
    if (Result.isError(writeRes)) return writeRes;
    artifacts.push(writeRes.value);
    bytes += writeRes.value.framedSize;
  }
  return Result.ok({ artifacts, objectCount: objectIdsRes.value.length, bytes });
}

async function commitImportedRefsResult(
  args: GitImportExportArgs,
  request: Pick<GitImportRequest, "txnId" | "idempotencyKey" | "actor">,
  refs: Record<string, GitOid>,
  artifacts: GitLooseObjectResponse[],
  bytes: number,
): Promise<Result<GitImportResponse, GitRepoServiceError>> {
  const recordsRes = await readGitRecordsResult(args);
  if (Result.isError(recordsRes)) return recordsRes;
  const currentRefs = buildRefs(recordsRes.value);
  const refUpdates = Object.entries(refs)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([ref, oid]) => ({
      ref,
      oldOid: Object.prototype.hasOwnProperty.call(currentRefs, ref) ? currentRefs[ref] ?? null : null,
      newOid: oid,
    }));
  if (refUpdates.length === 0) return Result.err(gitError(400, "no refs to import"));

  const txnRes = await commitGitRefTransactionResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    format: args.config.objectFormat,
    request: {
      txnId: request.txnId ?? `git-import:${randomUUID()}`,
      idempotencyKey: request.idempotencyKey,
      actor: request.actor,
      refUpdates,
      objects: {
        looseObjectUris: artifacts.map((artifact) => artifact.objectKey),
        objectCount: artifacts.length,
        bytes,
      },
    },
  });
  if (Result.isError(txnRes)) return txnRes;
  return Result.ok({
    imported: {
      refs: refUpdates.length,
      objects: artifacts.length,
      bytes,
    },
    transaction: txnRes.value.transaction,
    refs: txnRes.value.refs,
  });
}

async function importLocalBareRepoResult(
  args: GitImportExportArgs,
  request: Extract<GitImportRequest, { format: "local-bare-repo" }>
): Promise<Result<GitImportResponse, GitRepoServiceError>> {
  if (args.config.importExport?.allowLocalPathImport !== true) {
    return Result.err(gitError(409, "local path import is disabled for this git-repo profile"));
  }
  if (!existsSync(request.path)) return Result.err(gitError(404, "local git path not found"));
  const refsRes = listRefsFromGitDirResult(args.config, request.path, request.refs);
  if (Result.isError(refsRes)) return refsRes;
  const objectsRes = await importObjectsFromGitDirResult(args, request.path, refsRes.value);
  if (Result.isError(objectsRes)) return objectsRes;
  return commitImportedRefsResult(args, request, refsRes.value, objectsRes.value.artifacts, objectsRes.value.bytes);
}

function createBareRepoResult(config: GitRepoProfileConfig, root: string): Result<string, GitRepoServiceError> {
  const gitDir = join(root, "repo.git");
  const initRes = runGitResult(config, ["init", "--bare", gitDir]);
  if (Result.isError(initRes)) return initRes;
  return Result.ok(gitDir);
}

function writeBundleToBareRepoResult(
  config: GitRepoProfileConfig,
  bundleBytes: Uint8Array,
  root: string
): Result<string, GitRepoServiceError> {
  const bundlePath = join(root, "repo.bundle");
  const gitDir = join(root, "bundle.git");
  writeFileSync(bundlePath, Buffer.from(bundleBytes.buffer, bundleBytes.byteOffset, bundleBytes.byteLength));
  const cloneRes = runGitResult(config, ["clone", "--bare", bundlePath, gitDir]);
  if (Result.isError(cloneRes)) return cloneRes;
  return Result.ok(gitDir);
}

function writePackToBareRepoResult(
  config: GitRepoProfileConfig,
  packBytes: Uint8Array,
  refs: Record<string, GitOid>,
  root: string
): Result<string, GitRepoServiceError> {
  const gitDirRes = createBareRepoResult(config, root);
  if (Result.isError(gitDirRes)) return gitDirRes;
  const unpackRes = runGitResult(config, ["--git-dir", gitDirRes.value, "unpack-objects"], { input: packBytes });
  if (Result.isError(unpackRes)) return unpackRes;
  for (const [ref, oid] of Object.entries(refs)) {
    const updateRes = runGitResult(config, ["--git-dir", gitDirRes.value, "update-ref", normalizeGitRef(ref), oid]);
    if (Result.isError(updateRes)) return updateRes;
  }
  return Result.ok(gitDirRes.value);
}

export async function importGitRepoResult(
  args: GitImportExportArgs,
  rawRequest: unknown
): Promise<Result<GitImportResponse, GitRepoServiceError>> {
  if (!importExportEnabled(args.config)) return Result.err(gitError(404, "git import/export is disabled for this git-repo profile"));
  const requestRes = parseImportRequestResult(rawRequest);
  if (Result.isError(requestRes)) return requestRes;
  const request = requestRes.value;
  const root = mkdtempSync(join(tmpdir(), "streams-git-import-"));
  try {
    if (request.format === "local-bare-repo") return await importLocalBareRepoResult(args, request);
    if (request.format === "bundle") {
      const bytesRes = decodeBase64Result(request.bundleBase64, "bundleBase64", maxImportExportBytes(args.config));
      if (Result.isError(bytesRes)) return bytesRes;
      const gitDirRes = writeBundleToBareRepoResult(args.config, bytesRes.value, root);
      if (Result.isError(gitDirRes)) return gitDirRes;
      const refsRes = listRefsFromGitDirResult(args.config, gitDirRes.value);
      if (Result.isError(refsRes)) return refsRes;
      const objectsRes = await importObjectsFromGitDirResult(args, gitDirRes.value, refsRes.value);
      if (Result.isError(objectsRes)) return objectsRes;
      return commitImportedRefsResult(args, request, refsRes.value, objectsRes.value.artifacts, objectsRes.value.bytes);
    }
    const bytesRes = decodeBase64Result(request.packBase64, "packBase64", maxImportExportBytes(args.config));
    if (Result.isError(bytesRes)) return bytesRes;
    const gitDirRes = writePackToBareRepoResult(args.config, bytesRes.value, request.refs, root);
    if (Result.isError(gitDirRes)) return gitDirRes;
    const objectsRes = await importObjectsFromGitDirResult(args, gitDirRes.value, request.refs);
    if (Result.isError(objectsRes)) return objectsRes;
    return commitImportedRefsResult(args, request, request.refs, objectsRes.value.artifacts, objectsRes.value.bytes);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

function parseTagTarget(body: Uint8Array): string | null {
  const firstLine = TEXT_DECODER.decode(body).split("\n", 1)[0] ?? "";
  const match = firstLine.match(/^object ([0-9a-f]{40})$/);
  return match ? match[1] : null;
}

async function collectReachableObjectsResult(
  args: GitImportExportArgs,
  roots: GitOid[]
): Promise<Result<Map<GitOid, ExportObject>, GitRepoServiceError>> {
  const objects = new Map<GitOid, ExportObject>();
  const visiting = new Set<GitOid>();

  const visit = async (oid: GitOid): Promise<Result<void, GitRepoServiceError>> => {
    if (objects.has(oid) || visiting.has(oid)) return Result.ok(undefined);
    visiting.add(oid);
    const objectRes = await readLooseGitObjectBodyResult({
      repoStream: args.stream,
      objectStore: args.objectStore,
      format: args.config.objectFormat,
      oid,
    });
    if (Result.isError(objectRes)) return objectRes;
    const object: ExportObject = { oid, type: objectRes.value.header.type, body: objectRes.value.body };
    objects.set(oid, object);

    if (object.type === "commit") {
      const commitRes = parseGitCommitBodyResult(object.body, args.config.objectFormat);
      if (Result.isError(commitRes)) return Result.err(gitError(500, commitRes.error.message));
      const treeRes = await visit(commitRes.value.tree);
      if (Result.isError(treeRes)) return treeRes;
      for (const parent of commitRes.value.parents) {
        const parentRes = await visit(parent);
        if (Result.isError(parentRes)) return parentRes;
      }
    } else if (object.type === "tree") {
      const treeRes = parseGitTreeBodyResult(object.body, args.config.objectFormat);
      if (Result.isError(treeRes)) return Result.err(gitError(500, treeRes.error.message));
      for (const entry of treeRes.value) {
        const childRes = await visit(entry.oid);
        if (Result.isError(childRes)) return childRes;
      }
    } else if (object.type === "tag") {
      const target = parseTagTarget(object.body);
      if (target) {
        const targetRes = await visit(target);
        if (Result.isError(targetRes)) return targetRes;
      }
    }
    visiting.delete(oid);
    return Result.ok(undefined);
  };

  for (const root of roots) {
    const visitRes = await visit(root);
    if (Result.isError(visitRes)) return visitRes;
  }
  return Result.ok(objects);
}

function writeLooseObjectToGitDir(gitDir: string, object: GitObject): void {
  const dir = join(gitDir, "objects", object.oid.slice(0, 2));
  const file = join(dir, object.oid.slice(2));
  mkdirSync(dirname(file), { recursive: true });
  writeFileSync(file, deflateSync(Buffer.from(object.framed.buffer, object.framed.byteOffset, object.framed.byteLength)));
}

async function materializeBareRepoResult(
  args: GitImportExportArgs,
  root: string,
  opts: { allowEmpty?: boolean } = {}
): Promise<Result<{ gitDir: string; refs: Record<string, GitOid>; objectCount: number }, GitRepoServiceError>> {
  if (args.config.objectFormat !== "sha1") return Result.err(gitError(400, "Git CLI export currently requires sha1 objectFormat"));
  const recordsRes = await readGitRecordsResult(args);
  if (Result.isError(recordsRes)) return recordsRes;
  const refs = Object.fromEntries(Object.entries(buildRefs(recordsRes.value)).filter((entry): entry is [string, GitOid] => typeof entry[1] === "string"));
  const roots = Object.values(refs);
  let objectCount = 0;
  let objects: Map<GitOid, ExportObject> | null = null;
  if (roots.length === 0) {
    if (!opts.allowEmpty) return Result.err(gitError(404, "git repository has no refs to export"));
  } else {
    const objectsRes = await collectReachableObjectsResult(args, roots);
    if (Result.isError(objectsRes)) return objectsRes;
    objects = objectsRes.value;
    objectCount = objects.size;
  }
  const gitDirRes = createBareRepoResult(args.config, root);
  if (Result.isError(gitDirRes)) return gitDirRes;
  for (const object of objects?.values() ?? []) {
    writeLooseObjectToGitDir(gitDirRes.value, {
      ...object,
      size: object.body.byteLength,
      framed: frameGitObject(object.type, object.body),
    });
  }
  for (const [ref, oid] of Object.entries(refs)) {
    const updateRes = runGitResult(args.config, ["--git-dir", gitDirRes.value, "update-ref", ref, oid]);
    if (Result.isError(updateRes)) return updateRes;
  }
  const headRef = normalizeGitRef(args.config.defaultBranch);
  const headRes = runGitResult(args.config, ["--git-dir", gitDirRes.value, "symbolic-ref", "HEAD", headRef]);
  if (Result.isError(headRes)) return headRes;
  return Result.ok({ gitDir: gitDirRes.value, refs, objectCount });
}

export async function exportGitBundleResult(args: GitImportExportArgs): Promise<Result<Uint8Array, GitRepoServiceError>> {
  if (!importExportEnabled(args.config)) return Result.err(gitError(404, "git import/export is disabled for this git-repo profile"));
  const root = mkdtempSync(join(tmpdir(), "streams-git-export-"));
  try {
    const materializedRes = await materializeBareRepoResult(args, root);
    if (Result.isError(materializedRes)) return materializedRes;
    const bundlePath = join(root, "repo.bundle");
    const bundleRes = runGitResult(args.config, ["--git-dir", materializedRes.value.gitDir, "bundle", "create", bundlePath, "--all"]);
    if (Result.isError(bundleRes)) return bundleRes;
    const bytes = readFileSync(bundlePath);
    if (bytes.byteLength > maxImportExportBytes(args.config)) return Result.err(gitError(409, "exported bundle exceeds maxBytes"));
    return Result.ok(new Uint8Array(bytes));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

export async function exportGitPackResult(args: GitImportExportArgs): Promise<Result<Uint8Array, GitRepoServiceError>> {
  if (!importExportEnabled(args.config)) return Result.err(gitError(404, "git import/export is disabled for this git-repo profile"));
  const root = mkdtempSync(join(tmpdir(), "streams-git-export-"));
  try {
    const materializedRes = await materializeBareRepoResult(args, root);
    if (Result.isError(materializedRes)) return materializedRes;
    const packRes = runGitResult(args.config, ["--git-dir", materializedRes.value.gitDir, "pack-objects", "--stdout", "--all"], {
      maxBuffer: maxImportExportBytes(args.config),
    });
    if (Result.isError(packRes)) return packRes;
    return Result.ok(new Uint8Array(packRes.value));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

export type GitPackMaintenanceResponse = {
  packUri: string;
  idxUri: string;
  packBytes: number;
  idxBytes: number;
  objectCount: number;
  record: GitMaintenancePublishedRecord;
};

export type GitReachabilityVerificationResponse = {
  status: "verified";
  refs: Record<string, GitOid>;
  objectCount: number;
};

export type GitSmartHttpResponse = {
  contentType: string;
  body: Uint8Array;
};

function smartHttpFetchEnabled(config: GitRepoProfileConfig): boolean {
  return config.http?.enabled === true && config.http.allowFetch === true;
}

function smartHttpPushEnabled(config: GitRepoProfileConfig): boolean {
  return config.http?.enabled === true && config.http.allowPush === true;
}

function pktLine(text: string): Buffer {
  const body = Buffer.from(text);
  const length = (body.byteLength + 4).toString(16).padStart(4, "0");
  return Buffer.concat([Buffer.from(length), body]);
}

function bytesHashHex(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function stableRefsDigest(refs: Record<string, GitOid>): string {
  const canonical = Object.entries(refs).sort(([a], [b]) => a.localeCompare(b));
  return bytesHashHex(Buffer.from(JSON.stringify(canonical)));
}

function changedRefUpdates(before: Record<string, GitOid>, after: Record<string, GitOid>): GitRefUpdate[] {
  const refs = new Set([...Object.keys(before), ...Object.keys(after)]);
  return Array.from(refs).sort().flatMap((ref) => {
    const oldOid = before[ref] ?? null;
    const newOid = after[ref] ?? null;
    return oldOid === newOid ? [] : [{ ref, oldOid, newOid }];
  });
}

function configureReceivePackRepoResult(config: GitRepoProfileConfig, gitDir: string): Result<void, GitRepoServiceError> {
  const deleteRes = runGitResult(config, [
    "--git-dir",
    gitDir,
    "config",
    "receive.denyDeletes",
    config.push?.allowDeleteRefs === true ? "false" : "true",
  ]);
  if (Result.isError(deleteRes)) return deleteRes;
  const atomicRes = runGitResult(config, [
    "--git-dir",
    gitDir,
    "config",
    "receive.advertiseAtomic",
    config.push?.allowAtomic === false ? "false" : "true",
  ]);
  if (Result.isError(atomicRes)) return atomicRes;
  return Result.ok(undefined);
}

function configureUploadPackRepoResult(config: GitRepoProfileConfig, gitDir: string): Result<void, GitRepoServiceError> {
  const entries = [
    ["uploadpack.allowFilter", config.fetch?.allowFilter === true ? "true" : "false"],
    ["uploadpack.allowReachableSHA1InWant", "true"],
    ["uploadpack.allowTipSHA1InWant", "true"],
    ["uploadpack.allowRefInWant", "true"],
  ];
  for (const [key, value] of entries) {
    const configRes = runGitResult(config, ["--git-dir", gitDir, "config", key, value]);
    if (Result.isError(configRes)) return configRes;
  }
  return Result.ok(undefined);
}

function gitProtocolEnv(value: string | null | undefined): Record<string, string> | undefined {
  const protocol = value?.trim();
  if (!protocol || protocol.length > 1024 || /[\0\r\n]/.test(protocol)) return undefined;
  return { GIT_PROTOCOL: protocol };
}

function decodePktLinePayloads(bytes: Uint8Array): string[] | null {
  const payloads: string[] = [];
  let offset = 0;
  while (offset < bytes.byteLength) {
    if (offset + 4 > bytes.byteLength) return null;
    const header = TEXT_DECODER.decode(bytes.slice(offset, offset + 4));
    if (!/^[0-9a-fA-F]{4}$/.test(header)) return null;
    const length = Number.parseInt(header, 16);
    offset += 4;
    if (length === 0 || length === 1 || length === 2) continue;
    if (length < 4 || offset + length - 4 > bytes.byteLength) return null;
    payloads.push(TEXT_DECODER.decode(bytes.slice(offset, offset + length - 4)));
    offset += length - 4;
  }
  return payloads;
}

function validateUploadPackRequestResult(config: GitRepoProfileConfig, requestBody: Uint8Array): Result<void, GitRepoServiceError> {
  const payloads = decodePktLinePayloads(requestBody);
  if (!payloads) return Result.ok(undefined);
  if (config.fetch?.allowFilter !== true && payloads.some((payload) => payload.startsWith("filter ") || payload.includes(" filter "))) {
    return Result.err(gitError(400, "git upload-pack filter fetches are disabled for this git-repo profile"));
  }
  if (config.fetch?.allowDepth === false && payloads.some((payload) => payload.startsWith("deepen ") || payload.startsWith("deepen-since ") || payload.startsWith("deepen-not "))) {
    return Result.err(gitError(400, "git upload-pack shallow fetches are disabled for this git-repo profile"));
  }
  return Result.ok(undefined);
}

async function importObjectsFromMaybeEmptyGitDirResult(
  args: GitImportExportArgs,
  gitDir: string,
  refs: Record<string, GitOid>,
): Promise<Result<{ artifacts: GitLooseObjectResponse[]; objectCount: number; bytes: number }, GitRepoServiceError>> {
  if (Object.keys(refs).length === 0) return Result.ok({ artifacts: [], objectCount: 0, bytes: 0 });
  return importObjectsFromGitDirResult(args, gitDir, refs);
}

export async function publishGitPackArtifactsResult(
  args: GitImportExportArgs
): Promise<Result<GitPackMaintenanceResponse, GitRepoServiceError>> {
  if (!importExportEnabled(args.config)) return Result.err(gitError(404, "git import/export is disabled for this git-repo profile"));
  const root = mkdtempSync(join(tmpdir(), "streams-git-pack-"));
  try {
    const materializedRes = await materializeBareRepoResult(args, root);
    if (Result.isError(materializedRes)) return materializedRes;
    const packPath = join(root, "repo.pack");
    const idxPath = join(root, "repo.idx");
    const packRes = runGitResult(args.config, ["--git-dir", materializedRes.value.gitDir, "pack-objects", "--stdout", "--all"], {
      maxBuffer: maxImportExportBytes(args.config),
    });
    if (Result.isError(packRes)) return packRes;
    writeFileSync(packPath, packRes.value);
    const indexRes = runGitResult(args.config, ["--git-dir", materializedRes.value.gitDir, "index-pack", "-o", idxPath, packPath]);
    if (Result.isError(indexRes)) return indexRes;

    const packBytes = new Uint8Array(readFileSync(packPath));
    const idxBytes = new Uint8Array(readFileSync(idxPath));
    const packId = bytesHashHex(packBytes).slice(0, 32);
    const prefix = gitObjectArtifactPrefix(args.stream, args.config.objectFormat);
    const packUri = `${prefix}/${gitPackPath(packId)}`;
    const idxUri = `${prefix}/${gitPackIndexPath(packId)}`;
    const packPutRes = await putObjectStoreResult(args.objectStore, packUri, packBytes, "application/x-git-packed-objects");
    if (Result.isError(packPutRes)) return packPutRes;
    const idxPutRes = await putObjectStoreResult(args.objectStore, idxUri, idxBytes, "application/x-git-packed-objects-index");
    if (Result.isError(idxPutRes)) return idxPutRes;

    const recordsRes = await readGitRecordsResult(args);
    if (Result.isError(recordsRes)) return recordsRes;
    const record: GitMaintenancePublishedRecord = {
      type: "maintenance-published",
      repoId: args.stream,
      createdAt: new Date().toISOString(),
      refCheckpoint: buildRefCheckpoint({
        repoId: args.stream,
        records: recordsRes.value,
        streamOffset: recordsRes.value.length,
        generation: recordsRes.value.filter((candidate) => candidate.type === "maintenance-published").length + 1,
        defaultBranch: args.config.defaultBranch,
      }),
      packIndexManifestUri: idxUri,
      preferredClonePackUris: [packUri],
    };
    const appendRes = await appendGitRecordResult(args, record, "git-maintenance:pack");
    if (Result.isError(appendRes)) return appendRes;
    return Result.ok({
      packUri,
      idxUri,
      packBytes: packBytes.byteLength,
      idxBytes: idxBytes.byteLength,
      objectCount: materializedRes.value.objectCount,
      record,
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

export async function verifyGitReachabilityResult(
  args: GitImportExportArgs
): Promise<Result<GitReachabilityVerificationResponse, GitRepoServiceError>> {
  const recordsRes = await readGitRecordsResult(args);
  if (Result.isError(recordsRes)) return recordsRes;
  const refs = Object.fromEntries(Object.entries(buildRefs(recordsRes.value)).filter((entry): entry is [string, GitOid] => typeof entry[1] === "string"));
  const roots = Object.values(refs);
  if (roots.length === 0) return Result.ok({ status: "verified", refs, objectCount: 0 });
  const objectsRes = await collectReachableObjectsResult(args, roots);
  if (Result.isError(objectsRes)) {
    const status = objectsRes.error.status === 404 ? 409 : objectsRes.error.status;
    return Result.err(gitError(status, `git reachability verification failed: ${objectsRes.error.message}`));
  }
  return Result.ok({
    status: "verified",
    refs,
    objectCount: objectsRes.value.size,
  });
}

export async function gitUploadPackAdvertiseRefsResult(
  args: GitImportExportArgs
): Promise<Result<GitSmartHttpResponse, GitRepoServiceError>> {
  if (!smartHttpFetchEnabled(args.config)) return Result.err(gitError(404, "git upload-pack is disabled for this git-repo profile"));
  const root = mkdtempSync(join(tmpdir(), "streams-git-upload-pack-"));
  try {
    const materializedRes = await materializeBareRepoResult(args, root);
    if (Result.isError(materializedRes)) return materializedRes;
    const configRes = configureUploadPackRepoResult(args.config, materializedRes.value.gitDir);
    if (Result.isError(configRes)) return configRes;
    const refsRes = runGitResult(args.config, ["upload-pack", "--stateless-rpc", "--advertise-refs", materializedRes.value.gitDir], {
      maxBuffer: maxImportExportBytes(args.config),
      env: gitProtocolEnv(args.gitProtocol),
    });
    if (Result.isError(refsRes)) return refsRes;
    return Result.ok({
      contentType: "application/x-git-upload-pack-advertisement",
      body: new Uint8Array(Buffer.concat([pktLine("# service=git-upload-pack\n"), Buffer.from("0000"), refsRes.value])),
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

export async function gitUploadPackRpcResult(
  args: GitImportExportArgs,
  requestBody: Uint8Array
): Promise<Result<GitSmartHttpResponse, GitRepoServiceError>> {
  if (!smartHttpFetchEnabled(args.config)) return Result.err(gitError(404, "git upload-pack is disabled for this git-repo profile"));
  if (requestBody.byteLength > maxImportExportBytes(args.config)) return Result.err(gitError(400, "git-upload-pack request exceeds maxBytes"));
  const requestRes = validateUploadPackRequestResult(args.config, requestBody);
  if (Result.isError(requestRes)) return requestRes;
  const root = mkdtempSync(join(tmpdir(), "streams-git-upload-pack-"));
  try {
    const materializedRes = await materializeBareRepoResult(args, root);
    if (Result.isError(materializedRes)) return materializedRes;
    const configRes = configureUploadPackRepoResult(args.config, materializedRes.value.gitDir);
    if (Result.isError(configRes)) return configRes;
    const packRes = runGitResult(args.config, ["upload-pack", "--stateless-rpc", materializedRes.value.gitDir], {
      input: requestBody,
      maxBuffer: maxImportExportBytes(args.config),
      env: gitProtocolEnv(args.gitProtocol),
    });
    if (Result.isError(packRes)) return packRes;
    return Result.ok({
      contentType: "application/x-git-upload-pack-result",
      body: new Uint8Array(packRes.value),
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

export async function gitReceivePackAdvertiseRefsResult(
  args: GitImportExportArgs
): Promise<Result<GitSmartHttpResponse, GitRepoServiceError>> {
  if (!smartHttpPushEnabled(args.config)) return Result.err(gitError(404, "git receive-pack is disabled for this git-repo profile"));
  const root = mkdtempSync(join(tmpdir(), "streams-git-receive-pack-"));
  try {
    const materializedRes = await materializeBareRepoResult(args, root, { allowEmpty: true });
    if (Result.isError(materializedRes)) return materializedRes;
    const configRes = configureReceivePackRepoResult(args.config, materializedRes.value.gitDir);
    if (Result.isError(configRes)) return configRes;
    const refsRes = runGitResult(args.config, ["receive-pack", "--stateless-rpc", "--advertise-refs", materializedRes.value.gitDir], {
      maxBuffer: maxPushBytes(args.config),
      env: gitProtocolEnv(args.gitProtocol),
    });
    if (Result.isError(refsRes)) return refsRes;
    return Result.ok({
      contentType: "application/x-git-receive-pack-advertisement",
      body: new Uint8Array(Buffer.concat([pktLine("# service=git-receive-pack\n"), Buffer.from("0000"), refsRes.value])),
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

export async function gitReceivePackRpcResult(
  args: GitImportExportArgs,
  requestBody: Uint8Array
): Promise<Result<GitSmartHttpResponse, GitRepoServiceError>> {
  if (!smartHttpPushEnabled(args.config)) return Result.err(gitError(404, "git receive-pack is disabled for this git-repo profile"));
  if (requestBody.byteLength > maxPushBytes(args.config)) return Result.err(gitError(400, "git-receive-pack request exceeds maxPackBytes"));
  const root = mkdtempSync(join(tmpdir(), "streams-git-receive-pack-"));
  try {
    const materializedRes = await materializeBareRepoResult(args, root, { allowEmpty: true });
    if (Result.isError(materializedRes)) return materializedRes;
    const configRes = configureReceivePackRepoResult(args.config, materializedRes.value.gitDir);
    if (Result.isError(configRes)) return configRes;

    const receiveRes = runGitResult(args.config, ["receive-pack", "--stateless-rpc", materializedRes.value.gitDir], {
      input: requestBody,
      maxBuffer: maxPushBytes(args.config),
      env: gitProtocolEnv(args.gitProtocol),
    });
    if (Result.isError(receiveRes)) return receiveRes;

    const afterRefsRes = listRefsFromGitDirResult(args.config, materializedRes.value.gitDir);
    if (Result.isError(afterRefsRes)) return afterRefsRes;
    const refUpdates = changedRefUpdates(materializedRes.value.refs, afterRefsRes.value);
    if (refUpdates.length > 0) {
      if (args.config.push?.allowDeleteRefs !== true && refUpdates.some((update) => update.newOid === null)) {
        return Result.err(gitError(409, "git receive-pack delete refs are disabled for this git-repo profile"));
      }
      const objectsRes = await importObjectsFromMaybeEmptyGitDirResult(args, materializedRes.value.gitDir, afterRefsRes.value);
      if (Result.isError(objectsRes)) return objectsRes;
      const txnRes = await commitGitRefTransactionResult({
        stream: args.stream,
        reader: args.reader,
        objectStore: args.objectStore,
        appendJsonRecords: args.appendJsonRecords,
        format: args.config.objectFormat,
        request: {
          txnId: `git-receive-pack:${randomUUID()}`,
          idempotencyKey: `git-receive-pack:${bytesHashHex(requestBody)}:${stableRefsDigest(materializedRes.value.refs)}`,
          actor: "git-receive-pack",
          refUpdates,
          objects: {
            looseObjectUris: objectsRes.value.artifacts.map((artifact) => artifact.objectKey),
            objectCount: objectsRes.value.objectCount,
            bytes: objectsRes.value.bytes,
          },
        },
      });
      if (Result.isError(txnRes)) return txnRes;
    }

    return Result.ok({
      contentType: "application/x-git-receive-pack-result",
      body: new Uint8Array(receiveRes.value),
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}
