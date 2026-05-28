import { Buffer } from "node:buffer";
import { Result } from "better-result";
import { parseStoredProfileJsonResult, type StreamProfileVfsRouteArgs } from "../profiles/profile";
import type {
  VfsBatchReadBlobsRequest,
  VfsBatchReadMetadataRequest,
  VfsBatchStatRequest,
  VfsBlobManifest,
  VfsChunkObject,
  VfsCommit,
  VfsCommitRequest,
  VfsCommitResponse,
  VfsCanonicalGitCommit,
  VfsNodeStat,
  VfsRefUpdate,
  VfsStoredObject,
  VfsTreeEntry,
  VfsTreePage,
  VfsWorkspaceConflictsResponse,
  VfsWorkspaceOp,
  VfsWorkspaceOpInput,
  VfsWorkspaceRebaseResponse,
  VfsWorkspaceRecord,
} from "./types";
import {
  VFS_DEFAULT_REF,
  VFS_WORKSPACE_TTL_SECONDS,
  applyWorkspaceOp,
  basename,
  bytesFromText,
  canonicalizeVfsPath,
  controlRefKey,
  decodeStoredObject,
  directChildren,
  emptyMutableTree,
  ensureParentDirectories,
  fromBase64,
  isWorkspaceClosed,
  makeBlobObjects,
  makeCommitId,
  makeTreePage,
  newWorkspaceId,
  nodeStat,
  normalizeRef,
  objectKey,
  objectsStreamName,
  parentPath,
  toBase64,
  workspaceOpKey,
  workspaceOpsFromRecords,
  workspaceStreamName,
  type MutableVfsNode,
  type MutableVfsTree,
} from "./model";
import {
  GIT_REPO_PROFILE_KIND,
  buildRefs as buildGitRefs,
  commitGitRefTransactionResult,
  defaultGitRepoProfileConfig,
  normalizeGitRef,
  readGitRecordSnapshotResult,
  readGitRecordsResult,
  verifyGitRefTransactionRecordResult,
  writeGitBlob,
  writeGitCommitResult,
  writeGitTreeResult,
  writeLooseGitObjectResult,
  type GitLooseObjectResponse,
  type GitObject,
  type GitObjectFormat,
  type GitPerson,
  type GitRepoProfileConfig,
  type GitTransactionStatus,
  type GitTreeEntry,
  type GitTreeEntryInput,
  parseGitCommitBodyResult,
  parseGitTreeBodyResult,
  readLooseGitObjectBodyResult,
  readLooseGitObjectHeaderResult,
} from "../git_repo";

type VfsServerError = {
  status: 400 | 404 | 409 | 500;
  message: string;
};

type WorkspaceCheckoutRecord = {
  kind: "workspace-checkout" | "workspace-rebased";
  ref: string;
  baseCommitId: string | null;
  rootTreeId: string | null;
  createdAt: string;
};

type LoadedWorkspace = {
  records: VfsWorkspaceRecord[];
  checkout: WorkspaceCheckoutRecord | null;
  ops: VfsWorkspaceOp[];
  state: ReturnType<typeof isWorkspaceClosed>;
};

type LoadedTree = {
  tree: MutableVfsTree;
  treeIdsByPath: Map<string, string | null>;
};

function normalizeCommitDurabilityResult(body: VfsCommitRequest): Result<{ durability: Exclude<GitTransactionStatus, "accepted"> | "accepted"; timeoutMs: number }, VfsServerError> {
  const durability = body.durability ?? "accepted";
  if (durability !== "accepted" && durability !== "published" && durability !== "verified") {
    return Result.err({ status: 400, message: "durability must be accepted, published, or verified" });
  }
  const timeout = body.durabilityTimeoutMs;
  if (timeout !== undefined && (!Number.isSafeInteger(timeout) || timeout < 0)) {
    return Result.err({ status: 400, message: "durabilityTimeoutMs must be a non-negative integer" });
  }
  return Result.ok({ durability, timeoutMs: timeout ?? 30_000 });
}

type PreparedWorkspaceOp = {
  op: VfsWorkspaceOp;
  objects: VfsStoredObject[];
};

type WorkspaceOverlayIndex = {
  latestByPath: Map<string, VfsWorkspaceOp>;
  childrenByDir: Map<string, Set<string>>;
  deletedPaths: Set<string>;
};

const TEXT_DECODER = new TextDecoder();
const REPO_LOCKS = new Map<string, Promise<unknown>>();
const VFS_REF_CACHE = new Map<string, VfsRefUpdate | null>();
const VFS_METADATA_OBJECT_CACHE_MAX = 20_000;
const VFS_METADATA_OBJECT_CACHE = new Map<string, VfsStoredObject>();

function isCacheableVfsObject(object: VfsStoredObject): boolean {
  return object.kind === "tree-page" || object.kind === "tree-index-page" || object.kind === "commit";
}

function cacheVfsObject(object: VfsStoredObject): void {
  if (!isCacheableVfsObject(object)) return;
  VFS_METADATA_OBJECT_CACHE.delete(object.id);
  VFS_METADATA_OBJECT_CACHE.set(object.id, object);
  while (VFS_METADATA_OBJECT_CACHE.size > VFS_METADATA_OBJECT_CACHE_MAX) {
    const oldest = VFS_METADATA_OBJECT_CACHE.keys().next().value;
    if (oldest === undefined) break;
    VFS_METADATA_OBJECT_CACHE.delete(oldest);
  }
}

function cachedVfsObject(id: string): VfsStoredObject | null {
  const object = VFS_METADATA_OBJECT_CACHE.get(id);
  if (!object) return null;
  VFS_METADATA_OBJECT_CACHE.delete(id);
  VFS_METADATA_OBJECT_CACHE.set(id, object);
  return object;
}

function refCacheKey(repo: string, ref: string): string {
  return `${repo}\0${ref}`;
}

function responseForError(args: StreamProfileVfsRouteArgs, err: VfsServerError): Response {
  if (err.status === 400) return args.respond.badRequest(err.message);
  if (err.status === 404) return args.respond.notFound(err.message);
  if (err.status === 409) return args.respond.conflict(err.message);
  return args.respond.internalError(err.message);
}

async function withRepoLock<T>(repo: string, fn: () => Promise<T>): Promise<T> {
  const previous = REPO_LOCKS.get(repo) ?? Promise.resolve();
  const run = previous.catch(() => undefined).then(fn);
  REPO_LOCKS.set(repo, run);
  try {
    return await run;
  } finally {
    if (REPO_LOCKS.get(repo) === run) REPO_LOCKS.delete(repo);
  }
}

function parseJsonObjectBodyResult(body: unknown, label: string): Result<Record<string, unknown>, VfsServerError> {
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    return Result.err({ status: 400, message: `${label} must be a JSON object` });
  }
  return Result.ok(body as Record<string, unknown>);
}

async function readRequestJsonResult(req: Request, label: string): Promise<Result<unknown, VfsServerError>> {
  try {
    return Result.ok(await req.json());
  } catch {
    return Result.err({ status: 400, message: `${label} must be valid JSON` });
  }
}

async function readJsonRecordsResult(
  args: StreamProfileVfsRouteArgs,
  stream: string,
  key: string | null = null
): Promise<Result<unknown[], VfsServerError>> {
  const srow = args.db.getStream(stream);
  if (!srow || args.db.isDeleted(srow)) return Result.ok([]);
  if (srow.expires_at_ms != null && args.db.nowMs() > srow.expires_at_ms) return Result.ok([]);

  const values: unknown[] = [];
  let offset = "-1";
  for (;;) {
    const batchRes = await args.reader.readResult({ stream, offset, key, format: "raw", filter: null });
    if (Result.isError(batchRes)) {
      if (batchRes.error.kind === "not_found" || batchRes.error.kind === "gone") return Result.ok(values);
      return Result.err({ status: 500, message: batchRes.error.message });
    }
    for (const record of batchRes.value.records) {
      try {
        values.push(JSON.parse(TEXT_DECODER.decode(record.payload)));
      } catch {
        return Result.err({ status: 500, message: `invalid JSON record in ${stream}` });
      }
    }
    if (batchRes.value.nextOffsetSeq >= batchRes.value.endOffsetSeq) return Result.ok(values);
    offset = batchRes.value.nextOffset;
  }
}

async function appendJsonResult(
  args: StreamProfileVfsRouteArgs,
  stream: string,
  records: Array<{ value: unknown; routingKey?: string | null }>,
  ttlSeconds: number | null = null
): Promise<Result<void, VfsServerError>> {
  const appendRes = await args.appendJsonRecords({ stream, records, ttlSeconds });
  if (Result.isError(appendRes)) {
    if (appendRes.error.kind === "overloaded") return Result.err({ status: 500, message: "VFS append overloaded" });
    if (appendRes.error.kind === "not_found" || appendRes.error.kind === "gone") return Result.err({ status: 404, message: "stream not found" });
    if (appendRes.error.kind === "offset_mismatch") return Result.err({ status: 409, message: "stream changed" });
    if (appendRes.error.kind === "content_type_mismatch") return Result.err({ status: 409, message: "content-type mismatch" });
    return Result.err({ status: 500, message: appendRes.error.message });
  }
  return Result.ok(undefined);
}

async function writeObjectsResult(args: StreamProfileVfsRouteArgs, objects: VfsStoredObject[]): Promise<Result<void, VfsServerError>> {
  if (objects.length === 0) return Result.ok(undefined);
  const uniqueObjects = uniqueVfsObjects(objects);
  const stream = objectsStreamName(args.stream);
  const appendRes = await appendJsonResult(
    args,
    stream,
    uniqueObjects.map((value) => ({ value, routingKey: objectKey(value.id) }))
  );
  if (Result.isError(appendRes)) return appendRes;
  for (const object of uniqueObjects) cacheVfsObject(object);
  return Result.ok(undefined);
}

function uniqueVfsObjects(objects: VfsStoredObject[]): VfsStoredObject[] {
  const seen = new Set<string>();
  const unique: VfsStoredObject[] = [];
  for (const object of objects) {
    if (seen.has(object.id)) continue;
    seen.add(object.id);
    unique.push(object);
  }
  return unique;
}

async function loadObjectResult(
  args: StreamProfileVfsRouteArgs,
  id: string
): Promise<Result<VfsStoredObject | null, VfsServerError>> {
  const cached = cachedVfsObject(id);
  if (cached) return Result.ok(cached);
  const recordsRes = await readJsonRecordsResult(args, objectsStreamName(args.stream), objectKey(id));
  if (Result.isError(recordsRes)) return recordsRes;
  if (recordsRes.value.length === 0) return Result.ok(null);
  const latest = recordsRes.value[recordsRes.value.length - 1];
  const objectRes = decodeStoredObject(latest);
  if (Result.isError(objectRes)) return Result.err({ status: 500, message: objectRes.error.message });
  cacheVfsObject(objectRes.value);
  return Result.ok(objectRes.value);
}

async function loadRequiredObjectResult<T extends VfsStoredObject>(
  args: StreamProfileVfsRouteArgs,
  id: string,
  kind: T["kind"]
): Promise<Result<T, VfsServerError>> {
  const objectRes = await loadObjectResult(args, id);
  if (Result.isError(objectRes)) return objectRes;
  if (!objectRes.value) return Result.err({ status: 404, message: `VFS object not found: ${id}` });
  if (objectRes.value.kind !== kind) return Result.err({ status: 500, message: `VFS object ${id} is not ${kind}` });
  return Result.ok(objectRes.value as T);
}

async function readBlobBytesResult(args: StreamProfileVfsRouteArgs, blobId: string): Promise<Result<Uint8Array, VfsServerError>> {
  const manifestRes = await loadRequiredObjectResult<VfsBlobManifest>(args, blobId, "blob");
  if (Result.isError(manifestRes)) return manifestRes;
  const manifest = manifestRes.value;
  if (manifest.inlineBase64 !== undefined) {
    const bytesRes = fromBase64(manifest.inlineBase64);
    if (Result.isError(bytesRes)) return Result.err({ status: 500, message: bytesRes.error.message });
    return Result.ok(bytesRes.value);
  }
  const chunks = manifest.chunks ?? [];
  const out = new Uint8Array(manifest.size);
  for (const chunkRef of chunks) {
    const chunkRes = await loadRequiredObjectResult<VfsChunkObject>(args, chunkRef.id, "chunk");
    if (Result.isError(chunkRes)) return chunkRes;
    const bytesRes = fromBase64(chunkRes.value.dataBase64);
    if (Result.isError(bytesRes)) return Result.err({ status: 500, message: bytesRes.error.message });
    out.set(bytesRes.value, chunkRef.offset);
  }
  return Result.ok(out);
}

function gitRepoStreamForVfsProfile(args: StreamProfileVfsRouteArgs): string | null {
  const gitRepo = (args.profile as { gitRepo?: { stream?: unknown } }).gitRepo;
  return typeof gitRepo?.stream === "string" && gitRepo.stream.trim() !== "" ? gitRepo.stream.trim() : null;
}

function auditStreamForWorkspaceProfile(args: StreamProfileVfsRouteArgs): string | null {
  const audit = (args.profile as { audit?: { stream?: unknown } }).audit;
  return typeof audit?.stream === "string" && audit.stream.trim() !== "" ? audit.stream.trim() : null;
}

async function appendWorkspaceAuditEventResult(
  args: StreamProfileVfsRouteArgs,
  event: {
    type: string;
    workspaceId?: string;
    ref?: string;
    actorId?: string;
    status?: "started" | "succeeded" | "failed" | "conflict";
    context?: Record<string, unknown>;
  }
): Promise<Result<void, VfsServerError>> {
  const auditStream = auditStreamForWorkspaceProfile(args);
  if (!auditStream) return Result.ok(undefined);
  const row = args.db.getStream(auditStream);
  if (!row || args.db.isDeleted(row)) return Result.err({ status: 404, message: "workspace audit stream not found" });
  if (row.profile !== "evlog") return Result.err({ status: 409, message: "workspace audit stream must use the evlog profile" });
  const appendRes = await args.appendJsonRecords({
    stream: auditStream,
    records: [{
      routingKey: event.workspaceId ?? args.stream,
      value: {
        timestamp: new Date().toISOString(),
        service: "workspace-fs",
        level: event.status === "failed" || event.status === "conflict" ? "warn" : "info",
        message: event.type,
        context: {
          event: event.type,
          workspaceStream: args.stream,
          workspaceId: event.workspaceId,
          ref: event.ref,
          actorId: event.actorId,
          status: event.status,
          ...event.context,
        },
      },
    }],
  });
  if (Result.isError(appendRes)) {
    if (appendRes.error.kind === "not_found" || appendRes.error.kind === "gone") {
      return Result.err({ status: 404, message: "workspace audit stream not found" });
    }
    if (appendRes.error.kind === "content_type_mismatch") {
      return Result.err({ status: 409, message: "workspace audit stream content-type mismatch" });
    }
    if (appendRes.error.kind === "offset_mismatch") {
      return Result.err({ status: 409, message: "workspace audit stream changed" });
    }
    return Result.err({ status: 500, message: appendRes.error.message });
  }
  return Result.ok(undefined);
}

function gitRepoProfileConfigResult(args: StreamProfileVfsRouteArgs, stream: string): Result<GitRepoProfileConfig, VfsServerError> {
  const srow = args.db.getStream(stream);
  if (!srow || args.db.isDeleted(srow)) return Result.err({ status: 404, message: "git-repo stream not found" });
  if (srow.expires_at_ms != null && args.db.nowMs() > srow.expires_at_ms) return Result.err({ status: 404, message: "git-repo stream expired" });
  if (srow.profile !== GIT_REPO_PROFILE_KIND) return Result.err({ status: 409, message: "configured canonical stream is not git-repo" });

  const row = args.db.getStreamProfile(stream);
  if (!row) return Result.ok(defaultGitRepoProfileConfig());
  const parsedRes = parseStoredProfileJsonResult(row.profile_json);
  if (Result.isError(parsedRes)) return Result.err({ status: 500, message: parsedRes.error.message });
  const raw = parsedRes.value as Partial<GitRepoProfileConfig>;
  if (raw.kind !== GIT_REPO_PROFILE_KIND) return Result.err({ status: 500, message: "invalid git-repo profile state" });
  return Result.ok({
    ...defaultGitRepoProfileConfig(),
    ...raw,
    objectFormat: raw.objectFormat === "sha256" ? "sha256" : "sha1",
    defaultBranch: typeof raw.defaultBranch === "string" ? normalizeRef(raw.defaultBranch) : "refs/heads/main",
  });
}

function isWorkspaceFsProfile(args: StreamProfileVfsRouteArgs): boolean {
  return args.profile.kind === "workspace-fs";
}

function oidPattern(format: GitObjectFormat): RegExp {
  return format === "sha256" ? /^[0-9a-f]{64}$/ : /^[0-9a-f]{40}$/;
}

function encodeGitBlobId(repoStream: string, format: GitObjectFormat, oid: string): string {
  return `git-loose:${format}:${Buffer.from(repoStream).toString("base64url")}:${oid}`;
}

function decodeGitBlobId(blobId: string): { repoStream: string; format: GitObjectFormat; oid: string } | null {
  const [prefix, format, encodedRepo, oid] = blobId.split(":");
  if (prefix !== "git-loose" || (format !== "sha1" && format !== "sha256") || !encodedRepo || !oid) return null;
  if (!oidPattern(format).test(oid)) return null;
  try {
    return {
      repoStream: Buffer.from(encodedRepo, "base64url").toString("utf8"),
      format,
      oid,
    };
  } catch {
    return null;
  }
}

function gitModeToVfsMode(mode: string): number {
  if (mode === "40000") return 0o040755;
  if (mode === "120000") return 0o120777;
  if (mode === "100755") return 0o100755;
  return 0o100644;
}

function gitEntryToVfsStat(args: {
  path: string;
  entry: GitTreeEntry;
  repoStream: string;
  format: GitObjectFormat;
  size: number;
  symlinkTarget?: string;
}): VfsNodeStat {
  if (args.entry.type === "dir") {
    return {
      path: args.path,
      type: "dir",
      mode: gitModeToVfsMode(args.entry.mode),
      size: 0,
      treeId: args.entry.oid,
    };
  }
  if (args.entry.type === "symlink") {
    return {
      path: args.path,
      type: "symlink",
      mode: gitModeToVfsMode(args.entry.mode),
      size: args.size,
      symlinkTarget: args.symlinkTarget ?? "",
    };
  }
  return {
    path: args.path,
    type: "file",
    mode: gitModeToVfsMode(args.entry.mode),
    size: args.size,
    blobId: encodeGitBlobId(args.repoStream, args.format, args.entry.oid),
  };
}

function objectFromOverlay<T extends VfsStoredObject>(
  overlay: Map<string, VfsStoredObject>,
  id: string,
  kind: T["kind"]
): Result<T | null, VfsServerError> {
  const object = overlay.get(id);
  if (!object) return Result.ok(null);
  if (object.kind !== kind) return Result.err({ status: 500, message: `VFS object ${id} is not ${kind}` });
  return Result.ok(object as T);
}

async function loadRequiredObjectWithOverlayResult<T extends VfsStoredObject>(
  args: StreamProfileVfsRouteArgs,
  overlay: Map<string, VfsStoredObject>,
  id: string,
  kind: T["kind"]
): Promise<Result<T, VfsServerError>> {
  const overlayRes = objectFromOverlay<T>(overlay, id, kind);
  if (Result.isError(overlayRes)) return overlayRes;
  if (overlayRes.value) return Result.ok(overlayRes.value);
  return loadRequiredObjectResult<T>(args, id, kind);
}

async function readBlobBytesWithOverlayResult(
  args: StreamProfileVfsRouteArgs,
  overlay: Map<string, VfsStoredObject>,
  blobId: string
): Promise<Result<Uint8Array, VfsServerError>> {
  const manifestRes = await loadRequiredObjectWithOverlayResult<VfsBlobManifest>(args, overlay, blobId, "blob");
  if (Result.isError(manifestRes)) return manifestRes;
  const manifest = manifestRes.value;
  if (manifest.inlineBase64 !== undefined) {
    const bytesRes = fromBase64(manifest.inlineBase64);
    if (Result.isError(bytesRes)) return Result.err({ status: 500, message: bytesRes.error.message });
    return Result.ok(bytesRes.value);
  }
  const chunks = manifest.chunks ?? [];
  const out = new Uint8Array(manifest.size);
  for (const chunkRef of chunks) {
    const chunkRes = await loadRequiredObjectWithOverlayResult<VfsChunkObject>(args, overlay, chunkRef.id, "chunk");
    if (Result.isError(chunkRes)) return chunkRes;
    const bytesRes = fromBase64(chunkRes.value.dataBase64);
    if (Result.isError(bytesRes)) return Result.err({ status: 500, message: bytesRes.error.message });
    out.set(bytesRes.value, chunkRef.offset);
  }
  return Result.ok(out);
}

async function loadTreePageEntriesWithOverlayResult(
  args: StreamProfileVfsRouteArgs,
  overlay: Map<string, VfsStoredObject>,
  treeId: string
): Promise<Result<VfsTreeEntry[], VfsServerError>> {
  const entries: VfsTreeEntry[] = [];
  let currentId: string | undefined = treeId;
  while (currentId) {
    const pageRes: Result<VfsTreePage, VfsServerError> = await loadRequiredObjectWithOverlayResult<VfsTreePage>(args, overlay, currentId, "tree-page");
    if (Result.isError(pageRes)) return pageRes;
    entries.push(...pageRes.value.entries);
    currentId = pageRes.value.nextPageId;
  }
  return Result.ok(entries);
}

function safeGitPerson(author: { id: string; name?: string }, createdAt: string): GitPerson {
  const name = (author.name ?? author.id).replace(/[<>\r\n]/g, " ").trim() || "Agent";
  const local = author.id.replace(/[^A-Za-z0-9._+-]/g, "-").replace(/^-+|-+$/g, "") || "agent";
  const timestampMs = Date.parse(createdAt);
  return {
    name,
    email: `${local}@agents.prisma-streams.local`,
    timestampSeconds: Number.isFinite(timestampMs) ? Math.floor(timestampMs / 1000) : Math.floor(Date.now() / 1000),
    timezone: "+0000",
  };
}

function gitModeForVfsEntry(entry: VfsTreeEntry): GitTreeEntryInput["mode"] {
  if (entry.type === "dir") return "40000";
  if (entry.type === "symlink") return "120000";
  return entry.mode === 0o100755 ? "100755" : "100644";
}

async function writeGitObjectOnceResult(args: {
  route: StreamProfileVfsRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  object: GitObject;
  written: Map<string, GitLooseObjectResponse>;
}): Promise<Result<GitLooseObjectResponse, VfsServerError>> {
  const existing = args.written.get(args.object.oid);
  if (existing) return Result.ok(existing);
  const writeRes = await writeLooseGitObjectResult({
    repoStream: args.gitRepoStream,
    objectStore: args.route.objectStore,
    format: args.format,
    object: args.object,
  });
  if (Result.isError(writeRes)) return Result.err(writeRes.error);
  args.written.set(args.object.oid, writeRes.value);
  return Result.ok(writeRes.value);
}

async function materializeGitTreeResult(args: {
  route: StreamProfileVfsRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  overlay: Map<string, VfsStoredObject>;
  treeId: string;
  written: Map<string, GitLooseObjectResponse>;
}): Promise<Result<string, VfsServerError>> {
  const entriesRes = await loadTreePageEntriesWithOverlayResult(args.route, args.overlay, args.treeId);
  if (Result.isError(entriesRes)) return entriesRes;
  const gitEntries: GitTreeEntryInput[] = [];
  for (const entry of entriesRes.value) {
    if (entry.type === "dir") {
      if (!entry.treeId) return Result.err({ status: 500, message: `directory entry ${entry.name} is missing tree id` });
      const treeOidRes = await materializeGitTreeResult({ ...args, treeId: entry.treeId });
      if (Result.isError(treeOidRes)) return treeOidRes;
      gitEntries.push({ mode: gitModeForVfsEntry(entry), name: entry.name, oid: treeOidRes.value });
    } else if (entry.type === "symlink") {
      const object = writeGitBlob(entry.symlinkTarget ?? "", args.format);
      const writeRes = await writeGitObjectOnceResult({ ...args, object });
      if (Result.isError(writeRes)) return writeRes;
      gitEntries.push({ mode: gitModeForVfsEntry(entry), name: entry.name, oid: object.oid });
    } else {
      if (!entry.blobId) return Result.err({ status: 500, message: `file entry ${entry.name} is missing blob id` });
      const bytesRes = await readBlobBytesWithOverlayResult(args.route, args.overlay, entry.blobId);
      if (Result.isError(bytesRes)) return bytesRes;
      const object = writeGitBlob(bytesRes.value, args.format);
      const writeRes = await writeGitObjectOnceResult({ ...args, object });
      if (Result.isError(writeRes)) return writeRes;
      gitEntries.push({ mode: gitModeForVfsEntry(entry), name: entry.name, oid: object.oid });
    }
  }
  const treeRes = writeGitTreeResult(gitEntries, args.format);
  if (Result.isError(treeRes)) return Result.err({ status: 500, message: treeRes.error.message });
  const writeTreeRes = await writeGitObjectOnceResult({ ...args, object: treeRes.value });
  if (Result.isError(writeTreeRes)) return writeTreeRes;
  return Result.ok(treeRes.value.oid);
}

function gitTreeInputMode(entry: GitTreeEntry): GitTreeEntryInput["mode"] {
  if (entry.type === "dir") return "40000";
  if (entry.type === "symlink") return "120000";
  return entry.mode === "100755" ? "100755" : "100644";
}

async function baseGitRootForCommitResult(args: {
  route: StreamProfileVfsRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  commitOid: string | null;
}): Promise<Result<string | null, VfsServerError>> {
  if (!args.commitOid) return Result.ok(null);
  return rootGitTreeOidForCommitResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    format: args.format,
    commitOid: args.commitOid,
  });
}

async function buildGitTreeFromWorkspaceOpsResult(args: {
  route: StreamProfileVfsRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  baseCommitOid: string | null;
  ops: VfsWorkspaceOp[];
}): Promise<Result<{
  rootTreeOid: string;
  written: Map<string, GitLooseObjectResponse>;
  changeSummary: { added: number; modified: number; deleted: number; renamed: number };
}, VfsServerError>> {
  const rootTreeRes = await baseGitRootForCommitResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    format: args.format,
    commitOid: args.baseCommitOid,
  });
  if (Result.isError(rootTreeRes)) return rootTreeRes;
  const entriesByDir = new Map<string, GitTreeEntryInput[]>();
  const treeOidsByDir = new Map<string, string | null>([["/", rootTreeRes.value]]);
  const dirtyDirs = new Set<string>();
  const written = new Map<string, GitLooseObjectResponse>();
  let added = 0;
  let modified = 0;
  let deleted = 0;
  let renamed = 0;

  function findEntry(entries: GitTreeEntryInput[], name: string): GitTreeEntryInput | undefined {
    return entries.find((entry) => entry.name === name);
  }

  function sameGitEntry(a: GitTreeEntryInput | undefined, b: GitTreeEntryInput): boolean {
    return !!a && a.mode === b.mode && a.oid === b.oid;
  }

  function upsertEntry(entries: GitTreeEntryInput[], entry: GitTreeEntryInput): boolean {
    const idx = entries.findIndex((candidate) => candidate.name === entry.name);
    if (idx >= 0) {
      const changed = !sameGitEntry(entries[idx], entry);
      entries[idx] = entry;
      return changed;
    }
    entries.push(entry);
    return true;
  }

  function removeEntry(entries: GitTreeEntryInput[], name: string): GitTreeEntryInput | null {
    const idx = entries.findIndex((candidate) => candidate.name === name);
    if (idx < 0) return null;
    const [entry] = entries.splice(idx, 1);
    return entry ?? null;
  }

  function markDirty(dir: string): void {
    let current = dir;
    for (;;) {
      dirtyDirs.add(current);
      if (current === "/") break;
      current = parentPath(current);
    }
  }

  async function loadDir(dir: string): Promise<Result<GitTreeEntryInput[], VfsServerError>> {
    const cached = entriesByDir.get(dir);
    if (cached) return Result.ok(cached);
    if (!treeOidsByDir.has(dir) && dir !== "/") {
      const parentRes = await loadDir(parentPath(dir));
      if (Result.isError(parentRes)) return parentRes;
      const entry = findEntry(parentRes.value, basename(dir));
      if (!entry || entry.mode !== "40000") return Result.err({ status: 404, message: `directory not found: ${dir}` });
      treeOidsByDir.set(dir, entry.oid);
    }
    const treeOid = treeOidsByDir.get(dir) ?? null;
    if (!treeOid) {
      const empty: GitTreeEntryInput[] = [];
      entriesByDir.set(dir, empty);
      return Result.ok(empty);
    }
    const entriesRes = await loadGitTreeEntriesResult({
      route: args.route,
      gitRepoStream: args.gitRepoStream,
      format: args.format,
      treeOid,
    });
    if (Result.isError(entriesRes)) return entriesRes;
    const entries = entriesRes.value.map((entry): GitTreeEntryInput => ({
      mode: gitTreeInputMode(entry),
      name: entry.name,
      oid: entry.oid,
    }));
    entriesByDir.set(dir, entries);
    for (const entry of entries) {
      if (entry.mode === "40000") treeOidsByDir.set(childPath(dir, entry.name), entry.oid);
    }
    return Result.ok(entries);
  }

  async function ensureDirectoryResult(dir: string): Promise<Result<void, VfsServerError>> {
    if (dir === "/") {
      const rootRes = await loadDir("/");
      if (Result.isError(rootRes)) return rootRes;
      return Result.ok(undefined);
    }
    let current = "/";
    for (const part of pathParts(dir)) {
      const entriesRes = await loadDir(current);
      if (Result.isError(entriesRes)) return entriesRes;
      const existing = findEntry(entriesRes.value, part);
      const next = childPath(current, part);
      if (!existing) {
        upsertEntry(entriesRes.value, { mode: "40000", name: part, oid: "" });
        entriesByDir.set(next, []);
        treeOidsByDir.set(next, null);
        added += 1;
        markDirty(current);
        markDirty(next);
      } else if (existing.mode !== "40000") {
        return Result.err({ status: 409, message: `${next} is not a directory` });
      }
      current = next;
    }
    return Result.ok(undefined);
  }

  async function upsertPathEntryResult(path: string, entry: GitTreeEntryInput): Promise<Result<void, VfsServerError>> {
    const parent = parentPath(path);
    const ensureRes = await ensureDirectoryResult(parent);
    if (Result.isError(ensureRes)) return ensureRes;
    const entriesRes = await loadDir(parent);
    if (Result.isError(entriesRes)) return entriesRes;
    const before = findEntry(entriesRes.value, entry.name);
    const changed = upsertEntry(entriesRes.value, entry);
    if (!before) added += 1;
    else if (changed) modified += 1;
    markDirty(parent);
    return Result.ok(undefined);
  }

  for (const op of args.ops) {
    if (op.kind === "put-file") {
      const bytesRes = await readBlobBytesResult(args.route, op.blobId);
      if (Result.isError(bytesRes)) return bytesRes;
      const object = writeGitBlob(bytesRes.value, args.format);
      const writeRes = await writeGitObjectOnceResult({
        route: args.route,
        gitRepoStream: args.gitRepoStream,
        format: args.format,
        object,
        written,
      });
      if (Result.isError(writeRes)) return writeRes;
      const applyRes = await upsertPathEntryResult(op.path, {
        mode: op.executable === true ? "100755" : "100644",
        name: basename(op.path),
        oid: object.oid,
      });
      if (Result.isError(applyRes)) return applyRes;
    } else if (op.kind === "symlink") {
      const object = writeGitBlob(op.target, args.format);
      const writeRes = await writeGitObjectOnceResult({
        route: args.route,
        gitRepoStream: args.gitRepoStream,
        format: args.format,
        object,
        written,
      });
      if (Result.isError(writeRes)) return writeRes;
      const applyRes = await upsertPathEntryResult(op.path, {
        mode: "120000",
        name: basename(op.path),
        oid: object.oid,
      });
      if (Result.isError(applyRes)) return applyRes;
    } else if (op.kind === "mkdir") {
      const ensureRes = await ensureDirectoryResult(op.path);
      if (Result.isError(ensureRes)) return ensureRes;
    } else if (op.kind === "delete") {
      if (op.path === "/") return Result.err({ status: 400, message: "cannot delete workspace root" });
      const parent = parentPath(op.path);
      const entriesRes = await loadDir(parent);
      if (Result.isError(entriesRes)) {
        if (op.force && entriesRes.error.status === 404) continue;
        return entriesRes;
      }
      const removed = removeEntry(entriesRes.value, basename(op.path));
      if (!removed) {
        if (op.force) continue;
        return Result.err({ status: 404, message: `path not found: ${op.path}` });
      }
      deleted += 1;
      markDirty(parent);
    } else if (op.kind === "rename") {
      if (op.from === "/" || op.to === "/") return Result.err({ status: 400, message: "cannot rename workspace root" });
      const fromParent = parentPath(op.from);
      const fromEntriesRes = await loadDir(fromParent);
      if (Result.isError(fromEntriesRes)) return fromEntriesRes;
      const removed = removeEntry(fromEntriesRes.value, basename(op.from));
      if (!removed) return Result.err({ status: 404, message: `path not found: ${op.from}` });
      const ensureRes = await ensureDirectoryResult(parentPath(op.to));
      if (Result.isError(ensureRes)) return ensureRes;
      const toEntriesRes = await loadDir(parentPath(op.to));
      if (Result.isError(toEntriesRes)) return toEntriesRes;
      upsertEntry(toEntriesRes.value, { ...removed, name: basename(op.to) });
      renamed += 1;
      markDirty(fromParent);
      markDirty(parentPath(op.to));
    }
  }

  if (dirtyDirs.size === 0 && !rootTreeRes.value) dirtyDirs.add("/");
  if (dirtyDirs.size === 0 && rootTreeRes.value) {
    return Result.ok({
      rootTreeOid: rootTreeRes.value,
      written,
      changeSummary: { added, modified, deleted, renamed },
    });
  }

  const newTreeOids = new Map<string, string>();
  const dirty = Array.from(dirtyDirs).sort((a, b) => pathParts(b).length - pathParts(a).length);
  for (const dir of dirty) {
    const entriesRes = await loadDir(dir);
    if (Result.isError(entriesRes)) return entriesRes;
    const entries = entriesRes.value.map((entry) => {
      if (entry.mode !== "40000") return entry;
      const nextOid = newTreeOids.get(childPath(dir, entry.name)) ?? entry.oid;
      return { ...entry, oid: nextOid };
    });
    const treeRes = writeGitTreeResult(entries, args.format);
    if (Result.isError(treeRes)) return Result.err({ status: 500, message: treeRes.error.message });
    const writeRes = await writeGitObjectOnceResult({
      route: args.route,
      gitRepoStream: args.gitRepoStream,
      format: args.format,
      object: treeRes.value,
      written,
    });
    if (Result.isError(writeRes)) return writeRes;
    newTreeOids.set(dir, treeRes.value.oid);
  }

  const rootTreeOid = newTreeOids.get("/") ?? rootTreeRes.value;
  if (!rootTreeOid) return Result.err({ status: 500, message: "git tree build did not produce a root tree" });
  return Result.ok({
    rootTreeOid,
    written,
    changeSummary: { added, modified, deleted, renamed },
  });
}

async function currentGitHeadResult(
  args: StreamProfileVfsRouteArgs,
  gitRepoStream: string,
  ref: string
): Promise<Result<string | null, VfsServerError>> {
  const recordsRes = await readGitRecordsResult({ stream: gitRepoStream, reader: args.reader });
  if (Result.isError(recordsRes)) return Result.err(recordsRes.error);
  const refs = buildGitRefs(recordsRes.value);
  const normalized = normalizeGitRef(ref);
  return Result.ok(Object.prototype.hasOwnProperty.call(refs, normalized) ? refs[normalized] ?? null : null);
}

function latestUploadedThroughForStream(args: StreamProfileVfsRouteArgs, stream: string): bigint {
  return args.db.getStream(stream)?.uploaded_through ?? -1n;
}

async function waitGitTransactionDurabilityResult(args: {
  route: StreamProfileVfsRouteArgs;
  gitRepoStream: string;
  gitConfig: GitRepoProfileConfig;
  txnId: string;
  durability: "accepted" | "published" | "verified";
  timeoutMs: number;
}): Promise<Result<GitTransactionStatus, VfsServerError>> {
  if (args.durability === "accepted") return Result.ok("accepted");
  const deadline = Date.now() + args.timeoutMs;
  for (;;) {
    const snapshotRes = await readGitRecordSnapshotResult({ stream: args.gitRepoStream, reader: args.route.reader });
    if (Result.isError(snapshotRes)) return Result.err(snapshotRes.error);
    const entry = snapshotRes.value.entries.find(
      (candidate) => candidate.record.type === "ref-transaction-committed" && candidate.record.txnId === args.txnId
    );
    if (!entry || entry.record.type !== "ref-transaction-committed") {
      return Result.err({ status: 404, message: "git transaction not found" });
    }
    if (entry.offset <= latestUploadedThroughForStream(args.route, args.gitRepoStream)) {
      if (args.durability === "published") return Result.ok("published");
      const verificationRes = await verifyGitRefTransactionRecordResult({
        repoStream: args.gitRepoStream,
        objectStore: args.route.objectStore,
        format: args.gitConfig.objectFormat,
        transaction: entry.record,
      });
      if (Result.isError(verificationRes)) return Result.err(verificationRes.error);
      return Result.ok("verified");
    }
    if (Date.now() >= deadline) {
      return Result.err({ status: 409, message: `git transaction did not reach ${args.durability} durability before timeout` });
    }
    await new Promise((resolve) => setTimeout(resolve, Math.min(50, deadline - Date.now())));
  }
}

async function rootGitTreeOidForCommitResult(args: {
  route: StreamProfileVfsRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  commitOid: string;
}): Promise<Result<string, VfsServerError>> {
  const commitRes = await readLooseGitObjectBodyResult({
    repoStream: args.gitRepoStream,
    objectStore: args.route.objectStore,
    format: args.format,
    oid: args.commitOid,
    expectedType: "commit",
  });
  if (Result.isError(commitRes)) return Result.err(commitRes.error);
  const parsedRes = parseGitCommitBodyResult(commitRes.value.body, args.format);
  if (Result.isError(parsedRes)) return Result.err({ status: 500, message: parsedRes.error.message });
  return Result.ok(parsedRes.value.tree);
}

async function loadGitTreeEntriesResult(args: {
  route: StreamProfileVfsRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  treeOid: string;
}): Promise<Result<GitTreeEntry[], VfsServerError>> {
  const treeRes = await readLooseGitObjectBodyResult({
    repoStream: args.gitRepoStream,
    objectStore: args.route.objectStore,
    format: args.format,
    oid: args.treeOid,
    expectedType: "tree",
  });
  if (Result.isError(treeRes)) return Result.err(treeRes.error);
  const parsedRes = parseGitTreeBodyResult(treeRes.value.body, args.format);
  if (Result.isError(parsedRes)) return Result.err({ status: 500, message: parsedRes.error.message });
  return Result.ok(parsedRes.value);
}

async function statGitEntryResult(args: {
  route: StreamProfileVfsRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  path: string;
  entry: GitTreeEntry;
}): Promise<Result<VfsNodeStat, VfsServerError>> {
  if (args.entry.type === "dir") {
    return Result.ok(gitEntryToVfsStat({ ...args, repoStream: args.gitRepoStream, size: 0 }));
  }
  if (args.entry.type === "file") {
    const sizeRes = await gitBlobSizeResult({
      route: args.route,
      gitRepoStream: args.gitRepoStream,
      format: args.format,
      oid: args.entry.oid,
    });
    if (Result.isError(sizeRes)) return sizeRes;
    return Result.ok(gitEntryToVfsStat({
      path: args.path,
      entry: args.entry,
      repoStream: args.gitRepoStream,
      format: args.format,
      size: sizeRes.value,
    }));
  }
  const bodyRes = await readLooseGitObjectBodyResult({
    repoStream: args.gitRepoStream,
    objectStore: args.route.objectStore,
    format: args.format,
    oid: args.entry.oid,
    expectedType: "blob",
  });
  if (Result.isError(bodyRes)) return Result.err(bodyRes.error);
  const symlinkTarget = new TextDecoder().decode(bodyRes.value.body);
  return Result.ok(gitEntryToVfsStat({
    path: args.path,
    entry: args.entry,
    repoStream: args.gitRepoStream,
    format: args.format,
    size: bodyRes.value.header.size,
    symlinkTarget,
  }));
}

async function gitBlobSizeResult(args: {
  route: StreamProfileVfsRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  oid: string;
}): Promise<Result<number, VfsServerError>> {
  const headerRes = await readLooseGitObjectHeaderResult({
    repoStream: args.gitRepoStream,
    objectStore: args.route.objectStore,
    format: args.format,
    oid: args.oid,
  });
  if (Result.isError(headerRes)) return Result.err(headerRes.error);
  if (headerRes.value.type !== "blob") return Result.err({ status: 500, message: "git tree file entry does not point at a blob" });
  return Result.ok(headerRes.value.size);
}

async function statGitEntryLazyResult(args: {
  route: StreamProfileVfsRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  path: string;
  entry: GitTreeEntry;
}): Promise<Result<VfsNodeStat, VfsServerError>> {
  if (args.entry.type === "dir") {
    return Result.ok(gitEntryToVfsStat({ ...args, repoStream: args.gitRepoStream, size: 0 }));
  }
  if (args.entry.type === "symlink") return statGitEntryResult(args);
  const sizeRes = await gitBlobSizeResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    format: args.format,
    oid: args.entry.oid,
  });
  if (Result.isError(sizeRes)) return sizeRes;
  return Result.ok(gitEntryToVfsStat({
    path: args.path,
    entry: args.entry,
    repoStream: args.gitRepoStream,
    format: args.format,
    size: sizeRes.value,
  }));
}

async function resolveGitPathForWorkspaceResult(args: {
  route: StreamProfileVfsRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  commitOid: string;
  path: string;
}): Promise<Result<{ node: VfsNodeStat; treeOid: string }, VfsServerError>> {
  const rootRes = await rootGitTreeOidForCommitResult(args);
  if (Result.isError(rootRes)) return rootRes;
  if (args.path === "/") {
    return Result.ok({
      treeOid: rootRes.value,
      node: { path: "/", type: "dir", mode: 0o040755, size: 0, treeId: rootRes.value },
    });
  }

  let treeOid = rootRes.value;
  let currentPath = "/";
  for (const part of pathParts(args.path)) {
    const entriesRes = await loadGitTreeEntriesResult({ ...args, treeOid });
    if (Result.isError(entriesRes)) return entriesRes;
    const entry = entriesRes.value.find((candidate) => candidate.name === part);
    if (!entry) return Result.err({ status: 404, message: "path not found" });
    currentPath = childPath(currentPath, entry.name);
    if (currentPath === args.path) {
      const statRes = await statGitEntryLazyResult({ ...args, path: currentPath, entry });
      if (Result.isError(statRes)) return statRes;
      return Result.ok({ treeOid: entry.type === "dir" ? entry.oid : treeOid, node: statRes.value });
    }
    if (entry.type !== "dir") return Result.err({ status: 404, message: "path not found" });
    treeOid = entry.oid;
  }
  return Result.err({ status: 404, message: "path not found" });
}

async function commitWorkspaceToGitRepoResult(args: {
  route: StreamProfileVfsRouteArgs;
  gitRepoStream: string;
  gitConfig: GitRepoProfileConfig;
  ref: string;
  workspaceId: string;
  rootTreeId: string;
  newObjects: VfsStoredObject[];
  message: string;
  author: { id: string; name?: string };
  createdAt: string;
  vfsCommitId: string;
  durability: "accepted" | "published" | "verified";
  durabilityTimeoutMs: number;
}): Promise<Result<VfsCanonicalGitCommit, VfsServerError>> {
  const overlay = new Map<string, VfsStoredObject>();
  for (const object of args.newObjects) overlay.set(object.id, object);
  const written = new Map<string, GitLooseObjectResponse>();
  const rootOidRes = await materializeGitTreeResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    format: args.gitConfig.objectFormat,
    overlay,
    treeId: args.rootTreeId,
    written,
  });
  if (Result.isError(rootOidRes)) return rootOidRes;

  const oldHeadRes = await currentGitHeadResult(args.route, args.gitRepoStream, args.ref);
  if (Result.isError(oldHeadRes)) return oldHeadRes;
  const person = safeGitPerson(args.author, args.createdAt);
  const commitRes = writeGitCommitResult({
    tree: rootOidRes.value,
    parents: oldHeadRes.value ? [oldHeadRes.value] : [],
    author: person,
    committer: person,
    message: args.message,
  }, args.gitConfig.objectFormat);
  if (Result.isError(commitRes)) return Result.err({ status: 500, message: commitRes.error.message });
  const writeCommitRes = await writeGitObjectOnceResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    format: args.gitConfig.objectFormat,
    object: commitRes.value,
    written,
  });
  if (Result.isError(writeCommitRes)) return writeCommitRes;

  const artifacts = Array.from(written.values());
  const txnId = `workspace:${args.workspaceId}:${args.vfsCommitId}`;
  const txnRes = await commitGitRefTransactionResult({
    stream: args.gitRepoStream,
    reader: args.route.reader,
    objectStore: args.route.objectStore,
    appendJsonRecords: args.route.appendJsonRecords,
    format: args.gitConfig.objectFormat,
    request: {
      txnId,
      idempotencyKey: txnId,
      actor: args.author.id,
      refUpdates: [{
        ref: normalizeGitRef(args.ref),
        oldOid: oldHeadRes.value,
        newOid: commitRes.value.oid,
      }],
      objects: {
        looseObjectUris: artifacts.map((artifact) => artifact.objectKey),
        objectCount: artifacts.length,
        bytes: artifacts.reduce((sum, artifact) => sum + artifact.framedSize, 0),
      },
    },
  });
  if (Result.isError(txnRes)) return Result.err(txnRes.error);
  const durabilityRes = await waitGitTransactionDurabilityResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    gitConfig: args.gitConfig,
    txnId: txnRes.value.transaction.txnId,
    durability: args.durability,
    timeoutMs: args.durabilityTimeoutMs,
  });
  if (Result.isError(durabilityRes)) return durabilityRes;
  const objectBytes = artifacts.reduce((sum, artifact) => sum + artifact.framedSize, 0);
  return Result.ok({
    repoStream: args.gitRepoStream,
    ref: normalizeGitRef(args.ref),
    oldOid: oldHeadRes.value,
    newOid: commitRes.value.oid,
    txnId: txnRes.value.transaction.txnId,
    objectCount: artifacts.length,
    bytes: objectBytes,
    durability: durabilityRes.value,
  });
}

async function latestRefResult(args: StreamProfileVfsRouteArgs, ref: string): Promise<Result<VfsRefUpdate | null, VfsServerError>> {
  const cacheKey = refCacheKey(args.stream, ref);
  if (VFS_REF_CACHE.has(cacheKey)) return Result.ok(VFS_REF_CACHE.get(cacheKey) ?? null);
  const recordsRes = await readJsonRecordsResult(args, args.stream, controlRefKey(ref));
  if (Result.isError(recordsRes)) return recordsRes;
  for (let i = recordsRes.value.length - 1; i >= 0; i--) {
    const record = recordsRes.value[i] as Partial<VfsRefUpdate>;
    if (record.kind === "ref-update" && record.ref === ref && typeof record.newCommitId === "string") {
      VFS_REF_CACHE.set(cacheKey, record as VfsRefUpdate);
      return Result.ok(record as VfsRefUpdate);
    }
  }
  VFS_REF_CACHE.set(cacheKey, null);
  return Result.ok(null);
}

function workspaceCheckout(records: VfsWorkspaceRecord[]): WorkspaceCheckoutRecord | null {
  for (let i = records.length - 1; i >= 0; i--) {
    const record = records[i];
    if (record?.kind === "workspace-checkout") return record;
    if (record?.kind === "workspace-rebased") {
      return {
        kind: "workspace-rebased",
        ref: record.ref,
        baseCommitId: record.baseCommitId,
        rootTreeId: record.rootTreeId,
        createdAt: record.createdAt,
      };
    }
  }
  return null;
}

async function loadWorkspaceResult(args: StreamProfileVfsRouteArgs, workspaceId: string): Promise<Result<LoadedWorkspace, VfsServerError>> {
  const recordsRes = await readJsonRecordsResult(args, workspaceStreamName(args.stream, workspaceId));
  if (Result.isError(recordsRes)) return recordsRes;
  const records = recordsRes.value as VfsWorkspaceRecord[];
  return Result.ok({
    records,
    checkout: workspaceCheckout(records),
    ops: workspaceOpsFromRecords(records),
    state: isWorkspaceClosed(records),
  });
}

function overlayStatFromOp(op: VfsWorkspaceOp): VfsNodeStat | null {
  if (op.kind === "put-file") {
    return {
      path: op.path,
      type: "file",
      mode: op.executable === true ? 0o100755 : 0o100644,
      size: op.size,
      blobId: op.blobId,
      mtime: op.createdAt,
    };
  }
  if (op.kind === "mkdir") {
    return {
      path: op.path,
      type: "dir",
      mode: 0o040755,
      size: 0,
      mtime: op.createdAt,
    };
  }
  if (op.kind === "symlink") {
    return {
      path: op.path,
      type: "symlink",
      mode: 0o120777,
      size: bytesFromText(op.target).byteLength,
      symlinkTarget: op.target,
      mtime: op.createdAt,
    };
  }
  return null;
}

function deleteOverlayPath(index: WorkspaceOverlayIndex, path: string): void {
  index.latestByPath.delete(path);
  index.childrenByDir.get(parentPath(path))?.delete(basename(path));
  for (const existing of Array.from(index.latestByPath.keys())) {
    if (existing !== path && existing.startsWith(`${path}/`)) {
      index.latestByPath.delete(existing);
      index.childrenByDir.get(parentPath(existing))?.delete(basename(existing));
    }
  }
}

function addOverlayPath(index: WorkspaceOverlayIndex, path: string, op: VfsWorkspaceOp): void {
  index.latestByPath.set(path, op);
  let current = parentPath(path);
  while (current !== "/") {
    const parent = parentPath(current);
    let siblings = index.childrenByDir.get(parent);
    if (!siblings) {
      siblings = new Set();
      index.childrenByDir.set(parent, siblings);
    }
    siblings.add(basename(current));
    current = parent;
  }
  const parent = parentPath(path);
  let children = index.childrenByDir.get(parent);
  if (!children) {
    children = new Set();
    index.childrenByDir.set(parent, children);
  }
  children.add(basename(path));
}

function buildWorkspaceOverlayIndex(ops: VfsWorkspaceOp[]): WorkspaceOverlayIndex {
  const index: WorkspaceOverlayIndex = {
    latestByPath: new Map(),
    childrenByDir: new Map(),
    deletedPaths: new Set(),
  };

  for (const op of ops) {
    if (op.kind === "delete") {
      deleteOverlayPath(index, op.path);
      index.deletedPaths.add(op.path);
      continue;
    }
    if (op.kind === "rename") {
      const moved: Array<{ from: string; to: string; op: VfsWorkspaceOp }> = [];
      for (const [path, existing] of index.latestByPath.entries()) {
        if (path === op.from || path.startsWith(`${op.from}/`)) {
          const suffix = path === op.from ? "" : path.slice(op.from.length);
          moved.push({ from: path, to: `${op.to}${suffix}`, op: { ...existing, path: `${op.to}${suffix}` } as VfsWorkspaceOp });
        }
      }
      deleteOverlayPath(index, op.from);
      index.deletedPaths.add(op.from);
      for (const move of moved) addOverlayPath(index, move.to, move.op);
      continue;
    }
    deleteOverlayPath(index, op.path);
    index.deletedPaths.delete(op.path);
    addOverlayPath(index, op.path, op);
  }

  return index;
}

function pathHiddenByOverlay(index: WorkspaceOverlayIndex, path: string): boolean {
  for (const deleted of index.deletedPaths) {
    if (path === deleted || path.startsWith(`${deleted}/`)) return true;
  }
  return false;
}

function overlayHasChild(index: WorkspaceOverlayIndex, dir: string): boolean {
  const children = index.childrenByDir.get(dir);
  return children !== undefined && children.size > 0;
}

async function loadTreePageEntriesResult(
  args: StreamProfileVfsRouteArgs,
  treeId: string
): Promise<Result<VfsTreeEntry[], VfsServerError>> {
  const entries: VfsTreeEntry[] = [];
  let currentId: string | undefined = treeId;
  while (currentId) {
    const pageRes: Result<VfsTreePage, VfsServerError> = await loadRequiredObjectResult<VfsTreePage>(args, currentId, "tree-page");
    if (Result.isError(pageRes)) return pageRes;
    entries.push(...pageRes.value.entries);
    currentId = pageRes.value.nextPageId;
  }
  return Result.ok(entries);
}

async function loadTreeAtResult(
  args: StreamProfileVfsRouteArgs,
  tree: MutableVfsTree,
  treeIdsByPath: Map<string, string | null>,
  dirPath: string,
  treeId: string | null
): Promise<Result<void, VfsServerError>> {
  tree.set(dirPath, { type: "dir", mode: 0o040755 });
  treeIdsByPath.set(dirPath, treeId);
  if (!treeId) return Result.ok(undefined);
  const entriesRes = await loadTreePageEntriesResult(args, treeId);
  if (Result.isError(entriesRes)) return entriesRes;
  for (const entry of entriesRes.value) {
    const childPath = dirPath === "/" ? `/${entry.name}` : `${dirPath}/${entry.name}`;
    if (entry.type === "dir") {
      const childRes = await loadTreeAtResult(args, tree, treeIdsByPath, childPath, entry.treeId ?? null);
      if (Result.isError(childRes)) return childRes;
    } else if (entry.type === "symlink") {
      tree.set(childPath, {
        type: "symlink",
        mode: entry.mode,
        size: entry.size ?? bytesFromText(entry.symlinkTarget ?? "").byteLength,
        symlinkTarget: entry.symlinkTarget ?? "",
        mtime: entry.mtime,
      });
    } else {
      tree.set(childPath, {
        type: "file",
        mode: entry.mode,
        size: entry.size ?? 0,
        blobId: entry.blobId ?? "",
        mtime: entry.mtime,
      });
    }
  }
  return Result.ok(undefined);
}

async function loadTreeForCommitResult(
  args: StreamProfileVfsRouteArgs,
  commitId: string | null
): Promise<Result<LoadedTree, VfsServerError>> {
  const tree = emptyMutableTree();
  const treeIdsByPath = new Map<string, string | null>([["/", null]]);
  if (!commitId) return Result.ok({ tree, treeIdsByPath });
  const commitRes = await loadRequiredObjectResult<VfsCommit>(args, commitId, "commit");
  if (Result.isError(commitRes)) return commitRes;
  const loadedRes = await loadTreeAtResult(args, tree, treeIdsByPath, "/", commitRes.value.rootTreeId);
  if (Result.isError(loadedRes)) return loadedRes;
  return Result.ok({ tree, treeIdsByPath });
}

async function loadOverlayTreeResult(
  args: StreamProfileVfsRouteArgs,
  commitId: string | null,
  workspaceId: string | null
): Promise<Result<LoadedTree, VfsServerError>> {
  let baseCommitId = commitId;
  let workspace: LoadedWorkspace | null = null;
  if (workspaceId) {
    const workspaceRes = await loadWorkspaceResult(args, workspaceId);
    if (Result.isError(workspaceRes)) return workspaceRes;
    workspace = workspaceRes.value;
    if (!baseCommitId) baseCommitId = workspace.checkout?.baseCommitId ?? null;
  }
  const loadedRes = await loadTreeForCommitResult(args, baseCommitId);
  if (Result.isError(loadedRes)) return loadedRes;
  if (workspace) {
    for (const op of workspace.ops) applyWorkspaceOp(loadedRes.value.tree, op);
  }
  return loadedRes;
}

function parseCommitParam(url: URL): string | null {
  const commit = url.searchParams.get("commit");
  return commit && commit.trim() !== "" ? commit : null;
}

function parseWorkspaceParam(url: URL): string | null {
  const workspaceId = url.searchParams.get("workspaceId") ?? url.searchParams.get("workspace");
  return workspaceId && workspaceId.trim() !== "" ? workspaceId : null;
}

function resolveNodeResult(tree: MutableVfsTree, path: string): Result<MutableVfsNode, VfsServerError> {
  const node = tree.get(path);
  if (!node) return Result.err({ status: 404, message: "path not found" });
  return Result.ok(node);
}

function listDirectoryStats(
  loaded: LoadedTree,
  dir: string,
  cursor: string | null,
  limit: number
): Result<{ entries: VfsNodeStat[]; nextCursor: string | null }, VfsServerError> {
  const dirNode = loaded.tree.get(dir);
  if (!dirNode) return Result.err({ status: 404, message: "path not found" });
  if (dirNode.type !== "dir") return Result.err({ status: 400, message: "path is not a directory" });
  const children = directChildren(loaded.tree, dir);
  const start = cursor ? children.findIndex((child) => basename(child.path) > cursor) : 0;
  const safeStart = start < 0 ? children.length : start;
  const page = children.slice(safeStart, safeStart + limit);
  const entries = page.map((child) => nodeStat(child.path, child.node, loaded.treeIdsByPath.get(child.path) ?? null));
  const next = safeStart + limit < children.length ? basename(children[safeStart + limit - 1]!.path) : null;
  return Result.ok({ entries, nextCursor: next });
}

async function gitWorkspaceBaseResult(args: StreamProfileVfsRouteArgs, workspaceId: string): Promise<Result<{
  workspace: LoadedWorkspace;
  gitRepoStream: string;
  gitConfig: GitRepoProfileConfig;
  baseCommitOid: string | null;
}, VfsServerError>> {
  const gitRepoStream = gitRepoStreamForVfsProfile(args);
  if (!gitRepoStream || !isWorkspaceFsProfile(args)) return Result.err({ status: 404, message: "workspace-fs git backing is not enabled" });
  const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStream);
  if (Result.isError(gitConfigRes)) return gitConfigRes;
  const workspaceRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(workspaceRes)) return workspaceRes;
  if (!workspaceRes.value.checkout) return Result.err({ status: 404, message: "workspace not found" });
  return Result.ok({
    workspace: workspaceRes.value,
    gitRepoStream,
    gitConfig: gitConfigRes.value,
    baseCommitOid: workspaceRes.value.checkout.baseCommitId,
  });
}

async function statGitBackedWorkspaceResult(
  args: StreamProfileVfsRouteArgs,
  path: string,
  workspaceId: string
): Promise<Result<VfsNodeStat, VfsServerError>> {
  const baseRes = await gitWorkspaceBaseResult(args, workspaceId);
  if (Result.isError(baseRes)) return baseRes;
  const overlay = buildWorkspaceOverlayIndex(baseRes.value.workspace.ops);
  if (pathHiddenByOverlay(overlay, path)) return Result.err({ status: 404, message: "path not found" });
  const overlayNode = overlay.latestByPath.get(path);
  if (overlayNode) {
    const stat = overlayStatFromOp(overlayNode);
    if (stat) return Result.ok(stat);
  }
  if (overlayHasChild(overlay, path)) {
    return Result.ok({ path, type: "dir", mode: 0o040755, size: 0 });
  }
  if (!baseRes.value.baseCommitOid) return Result.err({ status: 404, message: "path not found" });
  const resolvedRes = await resolveGitPathForWorkspaceResult({
    route: args,
    gitRepoStream: baseRes.value.gitRepoStream,
    format: baseRes.value.gitConfig.objectFormat,
    commitOid: baseRes.value.baseCommitOid,
    path,
  });
  if (Result.isError(resolvedRes)) return resolvedRes;
  return Result.ok(resolvedRes.value.node);
}

async function readdirGitBackedWorkspaceResult(
  args: StreamProfileVfsRouteArgs,
  dir: string,
  workspaceId: string,
  cursor: string | null,
  limit: number
): Promise<Result<{ entries: VfsNodeStat[]; nextCursor: string | null }, VfsServerError>> {
  const baseRes = await gitWorkspaceBaseResult(args, workspaceId);
  if (Result.isError(baseRes)) return baseRes;
  const overlay = buildWorkspaceOverlayIndex(baseRes.value.workspace.ops);
  if (pathHiddenByOverlay(overlay, dir)) return Result.err({ status: 404, message: "path not found" });

  const byName = new Map<string, VfsNodeStat>();
  let baseDirExists = false;
  let baseTreeOid: string | null = null;
  if (baseRes.value.baseCommitOid) {
    const resolvedRes = await resolveGitPathForWorkspaceResult({
      route: args,
      gitRepoStream: baseRes.value.gitRepoStream,
      format: baseRes.value.gitConfig.objectFormat,
      commitOid: baseRes.value.baseCommitOid,
      path: dir,
    });
    if (Result.isError(resolvedRes)) {
      if (resolvedRes.error.status !== 404) return resolvedRes;
    } else {
      if (resolvedRes.value.node.type !== "dir") return Result.err({ status: 400, message: "path is not a directory" });
      baseDirExists = true;
      baseTreeOid = resolvedRes.value.node.treeId ?? resolvedRes.value.node.blobId ?? null;
    }
  }

  if (baseTreeOid) {
    const entriesRes = await loadGitTreeEntriesResult({
      route: args,
      gitRepoStream: baseRes.value.gitRepoStream,
      format: baseRes.value.gitConfig.objectFormat,
      treeOid: baseTreeOid,
    });
    if (Result.isError(entriesRes)) return entriesRes;
    for (const entry of entriesRes.value) {
      const path = childPath(dir, entry.name);
      if (pathHiddenByOverlay(overlay, path) || overlay.latestByPath.has(path)) continue;
      const statRes = await statGitEntryLazyResult({
        route: args,
        gitRepoStream: baseRes.value.gitRepoStream,
        format: baseRes.value.gitConfig.objectFormat,
        path,
        entry,
      });
      if (Result.isError(statRes)) return statRes;
      byName.set(entry.name, statRes.value);
    }
  }

  const overlayChildren = overlay.childrenByDir.get(dir);
  if (overlayChildren) {
    for (const name of overlayChildren) {
      const path = childPath(dir, name);
      if (pathHiddenByOverlay(overlay, path)) continue;
      const op = overlay.latestByPath.get(path);
      const stat = op ? overlayStatFromOp(op) : { path, type: "dir" as const, mode: 0o040755, size: 0 };
      if (stat) byName.set(name, stat);
    }
  }

  if (!baseDirExists && byName.size === 0 && dir !== "/") return Result.err({ status: 404, message: "path not found" });
  const sorted = Array.from(byName.entries()).sort((a, b) => a[0].localeCompare(b[0]));
  const start = cursor ? sorted.findIndex(([name]) => name > cursor) : 0;
  const safeStart = start < 0 ? sorted.length : start;
  const page = sorted.slice(safeStart, safeStart + limit).map(([, stat]) => stat);
  const nextCursor = safeStart + limit < sorted.length ? sorted[safeStart + limit - 1]![0] : null;
  return Result.ok({ entries: page, nextCursor });
}

async function checkout(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "checkout request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const bodyObjRes = parseJsonObjectBodyResult(bodyRes.value, "checkout request");
  if (Result.isError(bodyObjRes)) return responseForError(args, bodyObjRes.error);
  const ref = normalizeRef(typeof bodyObjRes.value.ref === "string" ? bodyObjRes.value.ref : VFS_DEFAULT_REF);
  const workspaceId = typeof bodyObjRes.value.workspaceId === "string" && bodyObjRes.value.workspaceId.trim() !== "" ? bodyObjRes.value.workspaceId : newWorkspaceId();
  const ttlSeconds = typeof bodyObjRes.value.ttlSeconds === "number" && Number.isFinite(bodyObjRes.value.ttlSeconds)
    ? Math.max(1, Math.floor(bodyObjRes.value.ttlSeconds))
    : VFS_WORKSPACE_TTL_SECONDS;

  let baseCommitId: string | null = null;
  let rootTreeId: string | null = null;
  const gitRepoStream = gitRepoStreamForVfsProfile(args);
  if (gitRepoStream && isWorkspaceFsProfile(args)) {
    const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStream);
    if (Result.isError(gitConfigRes)) return responseForError(args, gitConfigRes.error);
    const gitHeadRes = await currentGitHeadResult(args, gitRepoStream, ref);
    if (Result.isError(gitHeadRes)) return responseForError(args, gitHeadRes.error);
    baseCommitId = gitHeadRes.value;
    if (baseCommitId) {
      const rootRes = await rootGitTreeOidForCommitResult({
        route: args,
        gitRepoStream,
        format: gitConfigRes.value.objectFormat,
        commitOid: baseCommitId,
      });
      if (Result.isError(rootRes)) return responseForError(args, rootRes.error);
      rootTreeId = rootRes.value;
    }
  } else {
    const refRes = await latestRefResult(args, ref);
    if (Result.isError(refRes)) return responseForError(args, refRes.error);
    baseCommitId = refRes.value?.newCommitId ?? null;
    if (baseCommitId) {
      const commitRes = await loadRequiredObjectResult<VfsCommit>(args, baseCommitId, "commit");
      if (Result.isError(commitRes)) return responseForError(args, commitRes.error);
      rootTreeId = commitRes.value.rootTreeId;
    }
  }

  const workspaceRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(workspaceRes)) return responseForError(args, workspaceRes.error);
  let createdWorkspace = false;
  if (!workspaceRes.value.checkout) {
    const appendRes = await appendJsonResult(
      args,
      workspaceStreamName(args.stream, workspaceId),
      [
        {
          value: {
            kind: "workspace-checkout",
            ref,
            baseCommitId,
            rootTreeId,
            createdAt: new Date().toISOString(),
          },
          routingKey: "workspace",
        },
      ],
      ttlSeconds
    );
    if (Result.isError(appendRes)) return responseForError(args, appendRes.error);
    createdWorkspace = true;
  }
  const auditRes = await appendWorkspaceAuditEventResult(args, {
    type: "workspace_checked_out",
    workspaceId,
    ref,
    status: "succeeded",
    context: { baseCommitId, rootTreeId, createdWorkspace },
  });
  if (Result.isError(auditRes)) return responseForError(args, auditRes.error);

  return args.respond.json(200, {
    repo: args.stream,
    ref,
    workspaceId,
    baseCommitId,
    rootTreeId,
  });
}

async function refGet(args: StreamProfileVfsRouteArgs, refSegments: string[]): Promise<Response> {
  const ref = normalizeRef(decodeURIComponent(refSegments.join("/")));
  const gitRepoStream = gitRepoStreamForVfsProfile(args);
  if (gitRepoStream && isWorkspaceFsProfile(args)) {
    const gitHeadRes = await currentGitHeadResult(args, gitRepoStream, ref);
    if (Result.isError(gitHeadRes)) return responseForError(args, gitHeadRes.error);
    return args.respond.json(200, {
      ref,
      commitId: gitHeadRes.value,
      update: null,
    });
  }
  const refRes = await latestRefResult(args, ref);
  if (Result.isError(refRes)) return responseForError(args, refRes.error);
  return args.respond.json(200, {
    ref,
    commitId: refRes.value?.newCommitId ?? null,
    update: refRes.value,
  });
}

async function stat(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const pathRes = canonicalizeVfsPath(args.url.searchParams.get("path") ?? "/");
  if (Result.isError(pathRes)) return args.respond.badRequest(pathRes.error.message);
  const workspaceId = parseWorkspaceParam(args.url);
  if (workspaceId && gitRepoStreamForVfsProfile(args) && isWorkspaceFsProfile(args)) {
    const statRes = await statGitBackedWorkspaceResult(args, pathRes.value, workspaceId);
    if (Result.isError(statRes)) return responseForError(args, statRes.error);
    return args.respond.json(200, { node: statRes.value });
  }
  const gitCommitId = parseCommitParam(args.url);
  const gitRepoStream = gitRepoStreamForVfsProfile(args);
  if (gitCommitId && gitRepoStream && isWorkspaceFsProfile(args)) {
    const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStream);
    if (Result.isError(gitConfigRes)) return responseForError(args, gitConfigRes.error);
    if (!oidPattern(gitConfigRes.value.objectFormat).test(gitCommitId)) return args.respond.badRequest("invalid git commit id");
    const resolvedRes = await resolveGitPathForWorkspaceResult({
      route: args,
      gitRepoStream,
      format: gitConfigRes.value.objectFormat,
      commitOid: gitCommitId,
      path: pathRes.value,
    });
    if (Result.isError(resolvedRes)) return responseForError(args, resolvedRes.error);
    return args.respond.json(200, { node: resolvedRes.value.node });
  }
  const treeRes = await loadOverlayTreeResult(args, parseCommitParam(args.url), parseWorkspaceParam(args.url));
  if (Result.isError(treeRes)) return responseForError(args, treeRes.error);
  const nodeRes = resolveNodeResult(treeRes.value.tree, pathRes.value);
  if (Result.isError(nodeRes)) return responseForError(args, nodeRes.error);
  return args.respond.json(200, {
    node: nodeStat(pathRes.value, nodeRes.value, treeRes.value.treeIdsByPath.get(pathRes.value) ?? null),
  });
}

async function readdir(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const pathRes = canonicalizeVfsPath(args.url.searchParams.get("path") ?? "/");
  if (Result.isError(pathRes)) return args.respond.badRequest(pathRes.error.message);
  const rawLimit = Number(args.url.searchParams.get("limit") ?? "500");
  const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(1000, Math.floor(rawLimit))) : 500;
  const workspaceId = parseWorkspaceParam(args.url);
  if (workspaceId && gitRepoStreamForVfsProfile(args) && isWorkspaceFsProfile(args)) {
    const entriesRes = await readdirGitBackedWorkspaceResult(args, pathRes.value, workspaceId, args.url.searchParams.get("cursor"), limit);
    if (Result.isError(entriesRes)) return responseForError(args, entriesRes.error);
    return args.respond.json(200, {
      path: pathRes.value,
      entries: entriesRes.value.entries,
      nextCursor: entriesRes.value.nextCursor,
    });
  }
  const gitCommitId = parseCommitParam(args.url);
  const gitRepoStream = gitRepoStreamForVfsProfile(args);
  if (gitCommitId && gitRepoStream && isWorkspaceFsProfile(args)) {
    const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStream);
    if (Result.isError(gitConfigRes)) return responseForError(args, gitConfigRes.error);
    if (!oidPattern(gitConfigRes.value.objectFormat).test(gitCommitId)) return args.respond.badRequest("invalid git commit id");
    const resolvedRes = await resolveGitPathForWorkspaceResult({
      route: args,
      gitRepoStream,
      format: gitConfigRes.value.objectFormat,
      commitOid: gitCommitId,
      path: pathRes.value,
    });
    if (Result.isError(resolvedRes)) return responseForError(args, resolvedRes.error);
    if (resolvedRes.value.node.type !== "dir") return args.respond.badRequest("path is not a directory");
    const entriesRes = await loadGitTreeEntriesResult({
      route: args,
      gitRepoStream,
      format: gitConfigRes.value.objectFormat,
      treeOid: resolvedRes.value.node.treeId ?? "",
    });
    if (Result.isError(entriesRes)) return responseForError(args, entriesRes.error);
    const stats: VfsNodeStat[] = [];
    for (const entry of entriesRes.value) {
      const statRes = await statGitEntryLazyResult({
        route: args,
        gitRepoStream,
        format: gitConfigRes.value.objectFormat,
        path: childPath(pathRes.value, entry.name),
        entry,
      });
      if (Result.isError(statRes)) return responseForError(args, statRes.error);
      stats.push(statRes.value);
    }
    const sorted = stats.sort((a, b) => basename(a.path).localeCompare(basename(b.path)));
    const cursor = args.url.searchParams.get("cursor");
    const start = cursor ? sorted.findIndex((entry) => basename(entry.path) > cursor) : 0;
    const safeStart = start < 0 ? sorted.length : start;
    const page = sorted.slice(safeStart, safeStart + limit);
    const nextCursor = safeStart + limit < sorted.length ? basename(sorted[safeStart + limit - 1]!.path) : null;
    return args.respond.json(200, { path: pathRes.value, entries: page, nextCursor });
  }
  const treeRes = await loadOverlayTreeResult(args, parseCommitParam(args.url), parseWorkspaceParam(args.url));
  if (Result.isError(treeRes)) return responseForError(args, treeRes.error);
  const entriesRes = listDirectoryStats(treeRes.value, pathRes.value, args.url.searchParams.get("cursor"), limit);
  if (Result.isError(entriesRes)) return responseForError(args, entriesRes.error);
  return args.respond.json(200, {
    path: pathRes.value,
    entries: entriesRes.value.entries,
    nextCursor: entriesRes.value.nextCursor,
  });
}

function parseRange(value: string | null, size: number): { start: number; end: number } | null {
  if (!value) return null;
  const raw = value.startsWith("bytes=") ? value.slice("bytes=".length) : value;
  const match = raw.match(/^([0-9]+)-([0-9]*)$/);
  if (!match) return null;
  const start = Number(match[1]);
  const end = match[2] === "" ? size - 1 : Number(match[2]);
  if (!Number.isFinite(start) || !Number.isFinite(end) || start < 0 || end < start) return null;
  return { start, end: Math.min(end, size - 1) };
}

async function blob(args: StreamProfileVfsRouteArgs, blobId: string): Promise<Response> {
  const decodedBlobId = decodeURIComponent(blobId);
  const gitBlob = decodeGitBlobId(decodedBlobId);
  if (gitBlob) {
    const headerRes = await readLooseGitObjectHeaderResult({
      repoStream: gitBlob.repoStream,
      objectStore: args.objectStore,
      format: gitBlob.format,
      oid: gitBlob.oid,
    });
    if (Result.isError(headerRes)) return responseForError(args, headerRes.error);
    if (headerRes.value.type !== "blob") return responseForError(args, { status: 500, message: "git blob id does not point at a blob" });
    const range = parseRange(args.url.searchParams.get("range") ?? args.req.headers.get("range"), headerRes.value.size);
    const bytesRes = await readLooseGitObjectBodyResult({
      repoStream: gitBlob.repoStream,
      objectStore: args.objectStore,
      format: gitBlob.format,
      oid: gitBlob.oid,
      expectedType: "blob",
      range,
    });
    if (Result.isError(bytesRes)) return responseForError(args, bytesRes.error);
    const headers: Record<string, string> = {
      "content-type": "application/octet-stream",
      "cache-control": "immutable, max-age=31536000",
      "content-length": String(bytesRes.value.body.byteLength),
      "x-git-blob-oid": gitBlob.oid,
    };
    if (range) headers["content-range"] = `bytes ${range.start}-${range.end}/${headerRes.value.size}`;
    const body = bytesRes.value.body.buffer.slice(bytesRes.value.body.byteOffset, bytesRes.value.body.byteOffset + bytesRes.value.body.byteLength) as ArrayBuffer;
    return new Response(body, { status: range ? 206 : 200, headers });
  }
  const bytesRes = await readBlobBytesResult(args, decodedBlobId);
  if (Result.isError(bytesRes)) return responseForError(args, bytesRes.error);
  const range = parseRange(args.url.searchParams.get("range") ?? args.req.headers.get("range"), bytesRes.value.byteLength);
  const bytes = range ? bytesRes.value.slice(range.start, range.end + 1) : bytesRes.value;
  const headers: Record<string, string> = {
    "content-type": "application/octet-stream",
    "cache-control": "immutable, max-age=31536000",
    "content-length": String(bytes.byteLength),
  };
  if (range) headers["content-range"] = `bytes ${range.start}-${range.end}/${bytesRes.value.byteLength}`;
  const body = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
  return new Response(body, { status: range ? 206 : 200, headers });
}

async function prepareWorkspaceOpResult(
  args: StreamProfileVfsRouteArgs,
  op: VfsWorkspaceOpInput
): Promise<Result<PreparedWorkspaceOp, VfsServerError>> {
  const createdAt = new Date().toISOString();
  if (op.kind === "mkdir") {
    const pathRes = canonicalizeVfsPath(op.path);
    if (Result.isError(pathRes)) return Result.err({ status: 400, message: pathRes.error.message });
    return Result.ok({ op: { kind: "mkdir", path: pathRes.value, createdAt }, objects: [] });
  }
  if (op.kind === "delete") {
    const pathRes = canonicalizeVfsPath(op.path);
    if (Result.isError(pathRes)) return Result.err({ status: 400, message: pathRes.error.message });
    if (pathRes.value === "/") return Result.err({ status: 400, message: "cannot delete workspace root" });
    return Result.ok({ op: { kind: "delete", path: pathRes.value, recursive: op.recursive, force: op.force, createdAt }, objects: [] });
  }
  if (op.kind === "rename") {
    const fromRes = canonicalizeVfsPath(op.from);
    const toRes = canonicalizeVfsPath(op.to);
    if (Result.isError(fromRes)) return Result.err({ status: 400, message: fromRes.error.message });
    if (Result.isError(toRes)) return Result.err({ status: 400, message: toRes.error.message });
    if (fromRes.value === "/" || toRes.value === "/") return Result.err({ status: 400, message: "cannot rename workspace root" });
    return Result.ok({ op: { kind: "rename", from: fromRes.value, to: toRes.value, createdAt }, objects: [] });
  }
  if (op.kind === "symlink") {
    const pathRes = canonicalizeVfsPath(op.path);
    if (Result.isError(pathRes)) return Result.err({ status: 400, message: pathRes.error.message });
    return Result.ok({ op: { kind: "symlink", path: pathRes.value, target: op.target, createdAt }, objects: [] });
  }
  if (op.kind === "put-file") {
    const pathRes = canonicalizeVfsPath(op.path);
    if (Result.isError(pathRes)) return Result.err({ status: 400, message: pathRes.error.message });
    if (pathRes.value === "/") return Result.err({ status: 400, message: "cannot write workspace root" });
    let blobId = op.blobId;
    let size = 0;
    let objects: VfsStoredObject[] = [];
    if (!blobId) {
      const bytesRes = op.contentBase64 !== undefined ? fromBase64(op.contentBase64) : Result.ok(bytesFromText(op.text ?? ""));
      if (Result.isError(bytesRes)) return Result.err({ status: 400, message: bytesRes.error.message });
      const blob = makeBlobObjects(bytesRes.value, { executable: op.executable, contentType: op.contentType });
      objects = [...blob.chunks, blob.manifest];
      blobId = blob.manifest.id;
      size = blob.manifest.size;
    } else {
      const blobRes = await loadRequiredObjectResult<VfsBlobManifest>(args, blobId, "blob");
      if (Result.isError(blobRes)) return blobRes;
      size = blobRes.value.size;
    }
    return Result.ok({
      op: {
        kind: "put-file",
        path: pathRes.value,
        blobId,
        size,
        executable: op.executable,
        contentType: op.contentType,
        createdAt,
      },
      objects,
    });
  }
  return Result.err({ status: 400, message: "unsupported workspace op" });
}

async function workspaceOps(args: StreamProfileVfsRouteArgs, workspaceId: string): Promise<Response> {
  const loadedRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(loadedRes)) return responseForError(args, loadedRes.error);
  if (loadedRes.value.state.state !== "open") return args.respond.conflict(`workspace is ${loadedRes.value.state.state}`);

  const bodyRes = await readRequestJsonResult(args.req, "workspace ops request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const bodyObjRes = parseJsonObjectBodyResult(bodyRes.value, "workspace ops request");
  if (Result.isError(bodyObjRes)) return responseForError(args, bodyObjRes.error);
  const rawOps = bodyObjRes.value.ops;
  if (!Array.isArray(rawOps) || rawOps.length === 0) return args.respond.badRequest("ops must be a non-empty array");
  if (rawOps.length > 1000) return args.respond.badRequest("too many ops");

  const ops: VfsWorkspaceOp[] = [];
  const objects: VfsStoredObject[] = [];
  for (const rawOp of rawOps) {
    const opRes = await prepareWorkspaceOpResult(args, rawOp as VfsWorkspaceOpInput);
    if (Result.isError(opRes)) return responseForError(args, opRes.error);
    ops.push(opRes.value.op);
    objects.push(...opRes.value.objects);
  }

  const writeObjectsRes = await writeObjectsResult(args, objects);
  if (Result.isError(writeObjectsRes)) return responseForError(args, writeObjectsRes.error);

  const appendRes = await appendJsonResult(
    args,
    workspaceStreamName(args.stream, workspaceId),
    ops.map((value) => ({
      value,
      routingKey: value.kind === "rename" ? workspaceOpKey(value.from) : workspaceOpKey(value.path),
    })),
    VFS_WORKSPACE_TTL_SECONDS
  );
  if (Result.isError(appendRes)) return responseForError(args, appendRes.error);
  const auditRes = await appendWorkspaceAuditEventResult(args, {
    type: "workspace_ops_appended",
    workspaceId,
    status: "succeeded",
    context: {
      opCount: ops.length,
      changedPaths: changedPaths(ops),
      ops: ops.map((op) => op.kind === "rename"
        ? { kind: op.kind, from: op.from, to: op.to }
        : { kind: op.kind, path: op.path }),
    },
  });
  if (Result.isError(auditRes)) return responseForError(args, auditRes.error);
  return args.respond.json(200, { workspaceId, appended: ops.length, ops });
}

function changedPaths(ops: VfsWorkspaceOp[]): string[] {
  const paths = new Set<string>();
  for (const op of ops) {
    if (op.kind === "rename") {
      paths.add(op.from);
      paths.add(op.to);
    } else {
      paths.add(op.path);
    }
  }
  return Array.from(paths).sort((a, b) => a.localeCompare(b));
}

async function gitPathDescriptorsForCommitResult(args: {
  route: StreamProfileVfsRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  commitOid: string | null;
}): Promise<Result<Map<string, string>, VfsServerError>> {
  const paths = new Map<string, string>();
  if (!args.commitOid) return Result.ok(paths);
  const rootRes = await rootGitTreeOidForCommitResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    format: args.format,
    commitOid: args.commitOid,
  });
  if (Result.isError(rootRes)) return rootRes;

  async function walk(dir: string, treeOid: string): Promise<Result<void, VfsServerError>> {
    const entriesRes = await loadGitTreeEntriesResult({
      route: args.route,
      gitRepoStream: args.gitRepoStream,
      format: args.format,
      treeOid,
    });
    if (Result.isError(entriesRes)) return entriesRes;
    for (const entry of entriesRes.value) {
      const path = childPath(dir, entry.name);
      if (entry.type === "dir") {
        paths.set(path, `${gitTreeInputMode(entry)}:dir`);
        const childRes = await walk(path, entry.oid);
        if (Result.isError(childRes)) return childRes;
      } else {
        paths.set(path, `${gitTreeInputMode(entry)}:${entry.type}:${entry.oid}`);
      }
    }
    return Result.ok(undefined);
  }

  const walkRes = await walk("/", rootRes.value);
  if (Result.isError(walkRes)) return walkRes;
  return Result.ok(paths);
}

async function changedGitPathsBetweenCommitsResult(args: {
  route: StreamProfileVfsRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  baseCommitOid: string | null;
  headCommitOid: string | null;
}): Promise<Result<string[], VfsServerError>> {
  if (args.baseCommitOid === args.headCommitOid) return Result.ok([]);
  const baseRes = await gitPathDescriptorsForCommitResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    format: args.format,
    commitOid: args.baseCommitOid,
  });
  if (Result.isError(baseRes)) return baseRes;
  const headRes = await gitPathDescriptorsForCommitResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    format: args.format,
    commitOid: args.headCommitOid,
  });
  if (Result.isError(headRes)) return headRes;

  const changed = new Set<string>();
  for (const path of baseRes.value.keys()) {
    if (baseRes.value.get(path) !== headRes.value.get(path)) changed.add(path);
  }
  for (const path of headRes.value.keys()) {
    if (!baseRes.value.has(path)) changed.add(path);
  }
  return Result.ok(Array.from(changed).sort((a, b) => a.localeCompare(b)));
}

function workspacePathsOverlap(a: string, b: string): boolean {
  return a === b || a.startsWith(`${b}/`) || b.startsWith(`${a}/`);
}

function workspaceConflictPaths(workspaceChangedPaths: string[], upstreamChangedPaths: string[]): string[] {
  const conflicts = new Set<string>();
  for (const path of workspaceChangedPaths) {
    if (upstreamChangedPaths.some((upstreamPath) => workspacePathsOverlap(path, upstreamPath))) conflicts.add(path);
  }
  return Array.from(conflicts).sort((a, b) => a.localeCompare(b));
}

async function workspaceConflictSnapshotResult(
  args: StreamProfileVfsRouteArgs,
  workspaceId: string
): Promise<Result<VfsWorkspaceConflictsResponse, VfsServerError>> {
  const workspaceRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(workspaceRes)) return workspaceRes;
  const workspace = workspaceRes.value;
  if (!workspace.checkout) return Result.err({ status: 404, message: "workspace not found" });
  if (workspace.state.state !== "open") return Result.err({ status: 409, message: `workspace is ${workspace.state.state}` });

  const gitRepoStream = gitRepoStreamForVfsProfile(args);
  if (!gitRepoStream || !isWorkspaceFsProfile(args)) {
    return Result.err({ status: 400, message: "workspace rebase requires git-repo backing" });
  }
  const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStream);
  if (Result.isError(gitConfigRes)) return gitConfigRes;
  const currentHeadRes = await currentGitHeadResult(args, gitRepoStream, workspace.checkout.ref);
  if (Result.isError(currentHeadRes)) return currentHeadRes;
  const workspaceChangedPaths = changedPaths(workspace.ops);
  const upstreamChangedRes = await changedGitPathsBetweenCommitsResult({
    route: args,
    gitRepoStream,
    format: gitConfigRes.value.objectFormat,
    baseCommitOid: workspace.checkout.baseCommitId,
    headCommitOid: currentHeadRes.value,
  });
  if (Result.isError(upstreamChangedRes)) return upstreamChangedRes;
  const conflictPaths = workspaceConflictPaths(workspaceChangedPaths, upstreamChangedRes.value);
  return Result.ok({
    workspaceId,
    ref: normalizeGitRef(workspace.checkout.ref),
    baseCommitId: workspace.checkout.baseCommitId,
    currentHead: currentHeadRes.value,
    changedPaths: workspaceChangedPaths,
    upstreamChangedPaths: upstreamChangedRes.value,
    conflictPaths,
    canRebase: conflictPaths.length === 0,
  });
}

async function workspaceStatus(args: StreamProfileVfsRouteArgs, workspaceId: string): Promise<Response> {
  const workspaceRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(workspaceRes)) return responseForError(args, workspaceRes.error);
  return args.respond.json(200, {
    workspaceId,
    state: workspaceRes.value.state.state,
    baseCommitId: workspaceRes.value.checkout?.baseCommitId ?? null,
    opCount: workspaceRes.value.ops.length,
    changedPaths: changedPaths(workspaceRes.value.ops),
    lastCommitId: workspaceRes.value.state.commitId,
  });
}

function overlayIndexResponse(workspaceId: string, workspace: LoadedWorkspace, path: string | null = null) {
  const index = buildWorkspaceOverlayIndex(workspace.ops);
  const childDirs = Array.from(index.childrenByDir.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([dir, names]) => ({ dir, names: Array.from(names).sort((a, b) => a.localeCompare(b)) }));
  const latestPaths = Array.from(index.latestByPath.keys()).sort((a, b) => a.localeCompare(b));
  const deletedPaths = Array.from(index.deletedPaths).sort((a, b) => a.localeCompare(b));
  const normalizedPath = path ?? null;
  return {
    workspaceId,
    baseCommitId: workspace.checkout?.baseCommitId ?? null,
    generation: workspace.ops.length,
    opCount: workspace.ops.length,
    path: normalizedPath,
    latest: normalizedPath ? index.latestByPath.get(normalizedPath) ?? null : null,
    deleted: normalizedPath ? pathHiddenByOverlay(index, normalizedPath) : false,
    children: normalizedPath ? Array.from(index.childrenByDir.get(normalizedPath) ?? []).sort((a, b) => a.localeCompare(b)) : undefined,
    latestPaths,
    childDirs,
    deletedPaths,
  };
}

async function workspaceIndex(args: StreamProfileVfsRouteArgs, workspaceId: string): Promise<Response> {
  const workspaceRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(workspaceRes)) return responseForError(args, workspaceRes.error);
  const rawPath = args.url.searchParams.get("path");
  let path: string | null = null;
  if (rawPath != null) {
    const pathRes = canonicalizeVfsPath(rawPath);
    if (Result.isError(pathRes)) return args.respond.badRequest(pathRes.error.message);
    path = pathRes.value;
  }
  return args.respond.json(200, overlayIndexResponse(workspaceId, workspaceRes.value, path));
}

async function workspaceChanges(args: StreamProfileVfsRouteArgs, workspaceId: string): Promise<Response> {
  const workspaceRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(workspaceRes)) return responseForError(args, workspaceRes.error);
  const prefixRes = canonicalizeVfsPath(args.url.searchParams.get("prefix") ?? "/");
  if (Result.isError(prefixRes)) return args.respond.badRequest(prefixRes.error.message);
  const prefix = prefixRes.value;
  const paths = changedPaths(workspaceRes.value.ops).filter((path) => prefix === "/" || path === prefix || path.startsWith(`${prefix}/`));
  return args.respond.json(200, {
    workspaceId,
    baseCommitId: workspaceRes.value.checkout?.baseCommitId ?? null,
    generation: workspaceRes.value.ops.length,
    prefix,
    paths,
  });
}

async function workspaceConflicts(args: StreamProfileVfsRouteArgs, workspaceId: string): Promise<Response> {
  const snapshotRes = await workspaceConflictSnapshotResult(args, workspaceId);
  if (Result.isError(snapshotRes)) return responseForError(args, snapshotRes.error);
  return args.respond.json(200, snapshotRes.value);
}

async function rebaseWorkspace(args: StreamProfileVfsRouteArgs, workspaceId: string): Promise<Response> {
  return withRepoLock(args.stream, async () => {
    const snapshotRes = await workspaceConflictSnapshotResult(args, workspaceId);
    if (Result.isError(snapshotRes)) return responseForError(args, snapshotRes.error);
    const snapshot = snapshotRes.value;
    if (snapshot.conflictPaths.length > 0) {
      const auditRes = await appendWorkspaceAuditEventResult(args, {
        type: "workspace_rebase_failed",
        workspaceId,
        ref: snapshot.ref,
        status: "conflict",
        context: {
          baseCommitId: snapshot.baseCommitId,
          currentHead: snapshot.currentHead,
          conflictPaths: snapshot.conflictPaths,
        },
      });
      if (Result.isError(auditRes)) return responseForError(args, auditRes.error);
      return args.respond.json(409, {
        error: {
          code: "workspace_conflict",
          message: "workspace has path conflicts",
        },
        ...snapshot,
      });
    }

    const gitRepoStream = gitRepoStreamForVfsProfile(args);
    if (!gitRepoStream || !isWorkspaceFsProfile(args)) return args.respond.badRequest("workspace rebase requires git-repo backing");
    const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStream);
    if (Result.isError(gitConfigRes)) return responseForError(args, gitConfigRes.error);
    let rootTreeId: string | null = null;
    if (snapshot.currentHead) {
      const rootRes = await rootGitTreeOidForCommitResult({
        route: args,
        gitRepoStream,
        format: gitConfigRes.value.objectFormat,
        commitOid: snapshot.currentHead,
      });
      if (Result.isError(rootRes)) return responseForError(args, rootRes.error);
      rootTreeId = rootRes.value;
    }

    const rebased = snapshot.baseCommitId !== snapshot.currentHead;
    if (rebased) {
      const appendRes = await appendJsonResult(args, workspaceStreamName(args.stream, workspaceId), [
        {
          value: {
            kind: "workspace-rebased",
            ref: snapshot.ref,
            oldBaseCommitId: snapshot.baseCommitId,
            baseCommitId: snapshot.currentHead,
            rootTreeId,
            createdAt: new Date().toISOString(),
          },
          routingKey: "workspace",
        },
      ], VFS_WORKSPACE_TTL_SECONDS);
      if (Result.isError(appendRes)) return responseForError(args, appendRes.error);
    }

    const response: VfsWorkspaceRebaseResponse = {
      ...snapshot,
      baseCommitId: snapshot.currentHead,
      rebased,
      oldBaseCommitId: snapshot.baseCommitId,
      newBaseCommitId: snapshot.currentHead,
      rootTreeId,
    };
    const auditRes = await appendWorkspaceAuditEventResult(args, {
      type: "workspace_rebased",
      workspaceId,
      ref: snapshot.ref,
      status: "succeeded",
      context: {
        rebased,
        oldBaseCommitId: snapshot.baseCommitId,
        newBaseCommitId: snapshot.currentHead,
        rootTreeId,
      },
    });
    if (Result.isError(auditRes)) return responseForError(args, auditRes.error);
    return args.respond.json(200, response);
  });
}

async function compactWorkspace(args: StreamProfileVfsRouteArgs, workspaceId: string): Promise<Response> {
  const workspaceRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(workspaceRes)) return responseForError(args, workspaceRes.error);
  if (!workspaceRes.value.checkout) return args.respond.notFound("workspace not found");
  const snapshot = overlayIndexResponse(workspaceId, workspaceRes.value);
  const appendRes = await appendJsonResult(args, workspaceStreamName(args.stream, workspaceId), [
    {
      value: {
        kind: "workspace-overlay-index",
        workspaceId,
        baseCommitId: workspaceRes.value.checkout.baseCommitId,
        generation: workspaceRes.value.ops.length,
        opCount: workspaceRes.value.ops.length,
        latestPaths: snapshot.latestPaths,
        childDirs: snapshot.childDirs,
        deletedPaths: snapshot.deletedPaths,
        createdAt: new Date().toISOString(),
      },
      routingKey: "workspace-index",
    },
  ], VFS_WORKSPACE_TTL_SECONDS);
  if (Result.isError(appendRes)) return responseForError(args, appendRes.error);
  return args.respond.json(200, snapshot);
}

async function buildTreeObjectsResult(
  args: StreamProfileVfsRouteArgs,
  tree: MutableVfsTree,
  dir: string = "/",
  objects: VfsStoredObject[] = []
): Promise<Result<{ treeId: string; objects: VfsStoredObject[] }, VfsServerError>> {
  const entries: VfsTreeEntry[] = [];
  for (const child of directChildren(tree, dir)) {
    const name = basename(child.path);
    if (child.node.type === "dir") {
      const childRes = await buildTreeObjectsResult(args, tree, child.path, objects);
      if (Result.isError(childRes)) return childRes;
      entries.push({
        name,
        type: "dir",
        mode: child.node.mode,
        treeId: childRes.value.treeId,
        mtime: child.node.mtime,
      });
    } else if (child.node.type === "symlink") {
      entries.push({
        name,
        type: "symlink",
        mode: child.node.mode,
        size: child.node.size,
        symlinkTarget: child.node.symlinkTarget,
        mtime: child.node.mtime,
      });
    } else {
      entries.push({
        name,
        type: "file",
        mode: child.node.mode,
        size: child.node.size,
        blobId: child.node.blobId,
        mtime: child.node.mtime,
      });
    }
  }
  const page = makeTreePage(dir, entries);
  objects.push(page);
  return Result.ok({ treeId: page.id, objects });
}

function summarizeChanges(base: MutableVfsTree, next: MutableVfsTree): { added: number; modified: number; deleted: number; renamed: number } {
  let added = 0;
  let modified = 0;
  let deleted = 0;
  const paths = new Set([...base.keys(), ...next.keys()]);
  for (const path of paths) {
    if (path === "/") continue;
    const before = base.get(path);
    const after = next.get(path);
    if (!before && after) added += 1;
    else if (before && !after) deleted += 1;
    else if (before && after && JSON.stringify(before) !== JSON.stringify(after)) modified += 1;
  }
  return { added, modified, deleted, renamed: 0 };
}

function canBuildIncrementalTree(ops: VfsWorkspaceOp[]): boolean {
  return ops.every((op) => op.kind === "put-file" || op.kind === "mkdir" || op.kind === "symlink");
}

function pathParts(path: string): string[] {
  return path.split("/").filter((part) => part.length > 0);
}

function childPath(dir: string, name: string): string {
  return dir === "/" ? `/${name}` : `${dir}/${name}`;
}

function sameEntryForSummary(before: VfsTreeEntry, after: VfsTreeEntry): boolean {
  if (before.type !== after.type || before.mode !== after.mode || before.mtime !== after.mtime) return false;
  if (before.type === "dir") return true;
  if (before.type === "symlink") return before.size === after.size && before.symlinkTarget === after.symlinkTarget;
  return before.size === after.size && before.blobId === after.blobId;
}

async function rootTreeIdForCommitResult(
  args: StreamProfileVfsRouteArgs,
  commitId: string | null
): Promise<Result<string | null, VfsServerError>> {
  if (!commitId) return Result.ok(null);
  const commitRes = await loadRequiredObjectResult<VfsCommit>(args, commitId, "commit");
  if (Result.isError(commitRes)) return commitRes;
  return Result.ok(commitRes.value.rootTreeId);
}

async function buildIncrementalTreeObjectsResult(
  args: StreamProfileVfsRouteArgs,
  rootTreeId: string | null,
  ops: VfsWorkspaceOp[]
): Promise<Result<{ rootTreeId: string; objects: VfsStoredObject[]; changeSummary: { added: number; modified: number; deleted: number; renamed: number } }, VfsServerError>> {
  const entriesByPath = new Map<string, VfsTreeEntry[]>();
  const treeIdsByPath = new Map<string, string | null>([["/", rootTreeId]]);
  const dirtyDirs = new Set<string>();
  let added = 0;
  let modified = 0;

  function findEntry(entries: VfsTreeEntry[], name: string): VfsTreeEntry | undefined {
    return entries.find((entry) => entry.name === name);
  }

  function upsertEntry(entries: VfsTreeEntry[], entry: VfsTreeEntry): void {
    const idx = entries.findIndex((candidate) => candidate.name === entry.name);
    if (idx >= 0) entries[idx] = entry;
    else entries.push(entry);
  }

  function markDirty(dir: string): void {
    let current = dir;
    for (;;) {
      dirtyDirs.add(current);
      if (current === "/") break;
      current = parentPath(current);
    }
  }

  async function loadDir(dir: string): Promise<Result<VfsTreeEntry[], VfsServerError>> {
    const existing = entriesByPath.get(dir);
    if (existing) return Result.ok(existing);
    if (!treeIdsByPath.has(dir) && dir !== "/") {
      const parentRes = await loadDir(parentPath(dir));
      if (Result.isError(parentRes)) return parentRes;
      const parentEntry = findEntry(parentRes.value, basename(dir));
      if (!parentEntry || parentEntry.type !== "dir") {
        return Result.err({ status: 404, message: `directory not found: ${dir}` });
      }
      treeIdsByPath.set(dir, parentEntry.treeId ?? null);
    }

    const treeId = treeIdsByPath.get(dir) ?? null;
    if (!treeId) {
      const entries: VfsTreeEntry[] = [];
      entriesByPath.set(dir, entries);
      return Result.ok(entries);
    }
    const entriesRes = await loadTreePageEntriesResult(args, treeId);
    if (Result.isError(entriesRes)) return entriesRes;
    const entries = entriesRes.value.map((entry) => ({ ...entry }));
    entriesByPath.set(dir, entries);
    for (const entry of entries) {
      if (entry.type === "dir") treeIdsByPath.set(childPath(dir, entry.name), entry.treeId ?? null);
    }
    return Result.ok(entries);
  }

  async function ensureDirectoryResult(dir: string, mtime: string): Promise<Result<void, VfsServerError>> {
    if (dir === "/") {
      const rootRes = await loadDir("/");
      if (Result.isError(rootRes)) return rootRes;
      return Result.ok(undefined);
    }

    let current = "/";
    for (const part of pathParts(dir)) {
      const entriesRes = await loadDir(current);
      if (Result.isError(entriesRes)) return entriesRes;
      const existing = findEntry(entriesRes.value, part);
      const next = childPath(current, part);
      if (!existing) {
        upsertEntry(entriesRes.value, {
          name: part,
          type: "dir",
          mode: 0o040755,
          treeId: undefined,
          mtime,
        });
        treeIdsByPath.set(next, null);
        entriesByPath.set(next, []);
        added += 1;
        markDirty(current);
        markDirty(next);
      } else if (existing.type !== "dir") {
        return Result.err({ status: 409, message: `${next} is not a directory` });
      }
      current = next;
    }
    return Result.ok(undefined);
  }

  async function applyDirEntryResult(path: string, entry: VfsTreeEntry, createdAt: string): Promise<Result<void, VfsServerError>> {
    const parent = parentPath(path);
    const parentRes = await ensureDirectoryResult(parent, createdAt);
    if (Result.isError(parentRes)) return parentRes;
    const entriesRes = await loadDir(parent);
    if (Result.isError(entriesRes)) return entriesRes;
    const before = findEntry(entriesRes.value, entry.name);
    if (!before) added += 1;
    else if (!sameEntryForSummary(before, entry)) modified += 1;
    upsertEntry(entriesRes.value, entry);
    if (entry.type === "dir") treeIdsByPath.set(path, entry.treeId ?? null);
    markDirty(parent);
    return Result.ok(undefined);
  }

  for (const op of ops) {
    if (op.kind === "put-file") {
      const applyRes = await applyDirEntryResult(op.path, {
        name: basename(op.path),
        type: "file",
        mode: op.executable === true ? 0o100755 : 0o100644,
        size: op.size,
        blobId: op.blobId,
        mtime: op.createdAt,
      }, op.createdAt);
      if (Result.isError(applyRes)) return applyRes;
    } else if (op.kind === "symlink") {
      const applyRes = await applyDirEntryResult(op.path, {
        name: basename(op.path),
        type: "symlink",
        mode: 0o120777,
        size: bytesFromText(op.target).byteLength,
        symlinkTarget: op.target,
        mtime: op.createdAt,
      }, op.createdAt);
      if (Result.isError(applyRes)) return applyRes;
    } else if (op.kind === "mkdir") {
      if (op.path === "/") {
        const rootRes = await loadDir("/");
        if (Result.isError(rootRes)) return rootRes;
        markDirty("/");
      } else {
        const parent = parentPath(op.path);
        const parentRes = await ensureDirectoryResult(parent, op.createdAt);
        if (Result.isError(parentRes)) return parentRes;
        const entriesRes = await loadDir(parent);
        if (Result.isError(entriesRes)) return entriesRes;
        const before = findEntry(entriesRes.value, basename(op.path));
        const existingTreeId = before?.type === "dir" ? before.treeId ?? null : null;
        const entry: VfsTreeEntry = {
          name: basename(op.path),
          type: "dir",
          mode: 0o040755,
          treeId: existingTreeId ?? undefined,
          mtime: op.createdAt,
        };
        if (!before) {
          added += 1;
          entriesByPath.set(op.path, []);
        } else if (!sameEntryForSummary(before, entry)) {
          modified += 1;
        }
        upsertEntry(entriesRes.value, entry);
        treeIdsByPath.set(op.path, existingTreeId);
        markDirty(parent);
      }
    }
  }

  if (dirtyDirs.size === 0) {
    const rootRes = await loadDir("/");
    if (Result.isError(rootRes)) return rootRes;
    markDirty("/");
  }

  const objects: VfsStoredObject[] = [];
  const newTreeIds = new Map<string, string>();
  const dirty = Array.from(dirtyDirs).sort((a, b) => pathParts(b).length - pathParts(a).length);
  for (const dir of dirty) {
    const entriesRes = await loadDir(dir);
    if (Result.isError(entriesRes)) return entriesRes;
    const entries = entriesRes.value.map((entry) => {
      if (entry.type !== "dir") return entry;
      const nextId = newTreeIds.get(childPath(dir, entry.name)) ?? entry.treeId ?? null;
      return { ...entry, treeId: nextId ?? undefined };
    });
    const page = makeTreePage(dir, entries);
    objects.push(page);
    newTreeIds.set(dir, page.id);
  }

  const nextRootTreeId = newTreeIds.get("/");
  if (!nextRootTreeId) return Result.err({ status: 500, message: "incremental tree build did not produce a root tree" });
  return Result.ok({
    rootTreeId: nextRootTreeId,
    objects,
    changeSummary: { added, modified, deleted: 0, renamed: 0 },
  });
}

async function commitGitBackedWorkspaceResult(args: {
  route: StreamProfileVfsRouteArgs;
  workspace: LoadedWorkspace;
  workspaceId: string;
  gitRepoStream: string;
  gitConfig: GitRepoProfileConfig;
  ref: string;
  expectedHead: string | null | undefined;
  message: string;
  author: { id: string; name?: string };
  durability: "accepted" | "published" | "verified";
  durabilityTimeoutMs: number;
}): Promise<Result<VfsCommitResponse, VfsServerError>> {
  const currentHeadRes = await currentGitHeadResult(args.route, args.gitRepoStream, args.ref);
  if (Result.isError(currentHeadRes)) return currentHeadRes;
  const currentHead = currentHeadRes.value;
  const expectedHead = args.expectedHead === undefined ? args.workspace.checkout?.baseCommitId ?? null : args.expectedHead;
  if (currentHead !== expectedHead) return Result.err({ status: 409, message: "ref head changed" });

  const treeRes = await buildGitTreeFromWorkspaceOpsResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    format: args.gitConfig.objectFormat,
    baseCommitOid: currentHead,
    ops: args.workspace.ops,
  });
  if (Result.isError(treeRes)) return treeRes;

  const createdAt = new Date().toISOString();
  const person = safeGitPerson(args.author, createdAt);
  const commitObjectRes = writeGitCommitResult({
    tree: treeRes.value.rootTreeOid,
    parents: currentHead ? [currentHead] : [],
    author: person,
    committer: person,
    message: args.message,
  }, args.gitConfig.objectFormat);
  if (Result.isError(commitObjectRes)) return Result.err({ status: 500, message: commitObjectRes.error.message });
  const writeCommitRes = await writeGitObjectOnceResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    format: args.gitConfig.objectFormat,
    object: commitObjectRes.value,
    written: treeRes.value.written,
  });
  if (Result.isError(writeCommitRes)) return writeCommitRes;

  const artifacts = Array.from(treeRes.value.written.values());
  const txnId = `workspace:${args.workspaceId}:${commitObjectRes.value.oid}`;
  const txnRes = await commitGitRefTransactionResult({
    stream: args.gitRepoStream,
    reader: args.route.reader,
    objectStore: args.route.objectStore,
    appendJsonRecords: args.route.appendJsonRecords,
    format: args.gitConfig.objectFormat,
    request: {
      txnId,
      idempotencyKey: txnId,
      actor: args.author.id,
      refUpdates: [{
        ref: normalizeGitRef(args.ref),
        oldOid: currentHead,
        newOid: commitObjectRes.value.oid,
      }],
      objects: {
        looseObjectUris: artifacts.map((artifact) => artifact.objectKey),
        objectCount: artifacts.length,
        bytes: artifacts.reduce((sum, artifact) => sum + artifact.framedSize, 0),
      },
    },
  });
  if (Result.isError(txnRes)) return Result.err(txnRes.error);
  const durabilityRes = await waitGitTransactionDurabilityResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    gitConfig: args.gitConfig,
    txnId: txnRes.value.transaction.txnId,
    durability: args.durability,
    timeoutMs: args.durabilityTimeoutMs,
  });
  if (Result.isError(durabilityRes)) return durabilityRes;

  const gitCommit: VfsCanonicalGitCommit = {
    repoStream: args.gitRepoStream,
    ref: normalizeGitRef(args.ref),
    oldOid: currentHead,
    newOid: commitObjectRes.value.oid,
    txnId: txnRes.value.transaction.txnId,
    objectCount: artifacts.length,
    bytes: artifacts.reduce((sum, artifact) => sum + artifact.framedSize, 0),
    durability: durabilityRes.value,
  };
  const commit: VfsCommit = {
    kind: "commit",
    id: commitObjectRes.value.oid,
    parents: currentHead ? [currentHead] : [],
    rootTreeId: treeRes.value.rootTreeOid,
    author: args.author,
    message: args.message,
    createdAt,
    workspaceId: args.workspaceId,
    changeSummary: treeRes.value.changeSummary,
    git: gitCommit,
  };

  const writeCompatCommitRes = await writeObjectsResult(args.route, [commit]);
  if (Result.isError(writeCompatCommitRes)) return writeCompatCommitRes;
  const markerAppendRes = await appendJsonResult(args.route, workspaceStreamName(args.route.stream, args.workspaceId), [
    {
      value: { kind: "workspace-committed", commitId: commit.id, git: gitCommit, createdAt },
      routingKey: "workspace",
    },
  ], VFS_WORKSPACE_TTL_SECONDS);
  if (Result.isError(markerAppendRes)) return markerAppendRes;

  return Result.ok({
    ref: normalizeGitRef(args.ref),
    oldCommitId: currentHead,
    newCommitId: commitObjectRes.value.oid,
    commit,
    git: gitCommit,
  });
}

async function commitWorkspace(args: StreamProfileVfsRouteArgs, workspaceId: string): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "commit request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const bodyObjRes = parseJsonObjectBodyResult(bodyRes.value, "commit request");
  if (Result.isError(bodyObjRes)) return responseForError(args, bodyObjRes.error);
  const body = bodyObjRes.value as VfsCommitRequest;
  if (typeof body.message !== "string" || body.message.trim() === "") return args.respond.badRequest("message is required");
  if (!body.author || typeof body.author !== "object" || typeof body.author.id !== "string" || body.author.id.trim() === "") {
    return args.respond.badRequest("author.id is required");
  }
  const ref = normalizeRef(body.ref);
  const durabilityRes = normalizeCommitDurabilityResult(body);
  if (Result.isError(durabilityRes)) return responseForError(args, durabilityRes.error);
  const durability = durabilityRes.value;
  const startAuditRes = await appendWorkspaceAuditEventResult(args, {
    type: "workspace_commit_started",
    workspaceId,
    ref,
    actorId: body.author.id,
    status: "started",
    context: { durability: durability.durability },
  });
  if (Result.isError(startAuditRes)) return responseForError(args, startAuditRes.error);

  return withRepoLock(args.stream, async () => {
    const workspaceRes = await loadWorkspaceResult(args, workspaceId);
    if (Result.isError(workspaceRes)) return responseForError(args, workspaceRes.error);
    if (!workspaceRes.value.checkout) return args.respond.notFound("workspace not found");
    if (workspaceRes.value.state.state !== "open") return args.respond.conflict(`workspace is ${workspaceRes.value.state.state}`);

    const gitRepoStreamForWorkspace = gitRepoStreamForVfsProfile(args);
    if (gitRepoStreamForWorkspace && isWorkspaceFsProfile(args)) {
      const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStreamForWorkspace);
      if (Result.isError(gitConfigRes)) return responseForError(args, gitConfigRes.error);
      const commitRes = await commitGitBackedWorkspaceResult({
        route: args,
        workspace: workspaceRes.value,
        workspaceId,
        gitRepoStream: gitRepoStreamForWorkspace,
        gitConfig: gitConfigRes.value,
        ref,
        expectedHead: body.expectedHead,
        message: body.message,
        author: body.author,
        durability: durability.durability,
        durabilityTimeoutMs: durability.timeoutMs,
      });
      if (Result.isError(commitRes)) {
        const auditRes = await appendWorkspaceAuditEventResult(args, {
          type: "workspace_commit_failed",
          workspaceId,
          ref,
          actorId: body.author.id,
          status: commitRes.error.status === 409 ? "conflict" : "failed",
          context: { message: commitRes.error.message },
        });
        if (Result.isError(auditRes)) return responseForError(args, auditRes.error);
        return responseForError(args, commitRes.error);
      }
      const auditRes = await appendWorkspaceAuditEventResult(args, {
        type: "workspace_commit_succeeded",
        workspaceId,
        ref,
        actorId: body.author.id,
        status: "succeeded",
        context: {
          oldCommitId: commitRes.value.oldCommitId,
          newCommitId: commitRes.value.newCommitId,
          git: commitRes.value.git,
        },
      });
      if (Result.isError(auditRes)) return responseForError(args, auditRes.error);
      return args.respond.json(200, commitRes.value);
    }

    const refRes = await latestRefResult(args, ref);
    if (Result.isError(refRes)) return responseForError(args, refRes.error);
    const currentHead = refRes.value?.newCommitId ?? null;
    const expectedHead = body.expectedHead === undefined ? workspaceRes.value.checkout.baseCommitId : body.expectedHead;
    if (currentHead !== expectedHead) {
      const auditRes = await appendWorkspaceAuditEventResult(args, {
        type: "workspace_commit_failed",
        workspaceId,
        ref,
        actorId: body.author.id,
        status: "conflict",
        context: { message: "ref head changed", expectedHead, currentHead },
      });
      if (Result.isError(auditRes)) return responseForError(args, auditRes.error);
      return args.respond.conflict("ref head changed");
    }

    const treePlanRes = canBuildIncrementalTree(workspaceRes.value.ops)
      ? await rootTreeIdForCommitResult(args, currentHead)
        .then(async (rootRes) => {
          if (Result.isError(rootRes)) return rootRes;
          return buildIncrementalTreeObjectsResult(args, rootRes.value, workspaceRes.value.ops);
        })
      : await loadTreeForCommitResult(args, currentHead)
        .then(async (baseTreeRes) => {
          if (Result.isError(baseTreeRes)) return baseTreeRes;
          const nextTree = new Map(baseTreeRes.value.tree);
          for (const op of workspaceRes.value.ops) applyWorkspaceOp(nextTree, op);
          const treeObjectsRes = await buildTreeObjectsResult(args, nextTree);
          if (Result.isError(treeObjectsRes)) return treeObjectsRes;
          return Result.ok({
            rootTreeId: treeObjectsRes.value.treeId,
            objects: treeObjectsRes.value.objects,
            changeSummary: summarizeChanges(baseTreeRes.value.tree, nextTree),
          });
        });
    if (Result.isError(treePlanRes)) return responseForError(args, treePlanRes.error);
    const commitWithoutId = {
      kind: "commit" as const,
      parents: currentHead ? [currentHead] : [],
      rootTreeId: treePlanRes.value.rootTreeId,
      author: body.author,
      message: body.message,
      createdAt: new Date().toISOString(),
      workspaceId,
      changeSummary: treePlanRes.value.changeSummary,
    };
    const commit: VfsCommit = {
      ...commitWithoutId,
      id: makeCommitId(commitWithoutId),
    };
    let gitCommit: VfsCanonicalGitCommit | undefined;
    const gitRepoStream = gitRepoStreamForVfsProfile(args);
    if (!gitRepoStream && durability.durability !== "accepted") {
      return args.respond.badRequest("published and verified durability require git-repo backing");
    }
    if (gitRepoStream) {
      const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStream);
      if (Result.isError(gitConfigRes)) return responseForError(args, gitConfigRes.error);
      const gitCommitRes = await commitWorkspaceToGitRepoResult({
        route: args,
        gitRepoStream,
        gitConfig: gitConfigRes.value,
        ref,
        workspaceId,
        rootTreeId: treePlanRes.value.rootTreeId,
        newObjects: treePlanRes.value.objects,
        message: body.message,
        author: body.author,
        createdAt: commit.createdAt,
        vfsCommitId: commit.id,
        durability: durability.durability,
        durabilityTimeoutMs: durability.timeoutMs,
      });
      if (Result.isError(gitCommitRes)) return responseForError(args, gitCommitRes.error);
      gitCommit = gitCommitRes.value;
      commit.git = gitCommit;
    }
    const refUpdate: VfsRefUpdate = {
      kind: "ref-update",
      ref,
      oldCommitId: currentHead,
      newCommitId: commit.id,
      actorId: body.author.id,
      createdAt: commit.createdAt,
    };

    const writeCommitRes = await writeObjectsResult(args, [...treePlanRes.value.objects, commit]);
    if (Result.isError(writeCommitRes)) return responseForError(args, writeCommitRes.error);
    const refAppendRes = await appendJsonResult(args, args.stream, [{ value: refUpdate, routingKey: controlRefKey(ref) }]);
    if (Result.isError(refAppendRes)) return responseForError(args, refAppendRes.error);
    VFS_REF_CACHE.set(refCacheKey(args.stream, ref), refUpdate);
    const markerAppendRes = await appendJsonResult(args, workspaceStreamName(args.stream, workspaceId), [
      {
        value: { kind: "workspace-committed", commitId: commit.id, git: gitCommit, createdAt: commit.createdAt },
        routingKey: "workspace",
      },
    ], VFS_WORKSPACE_TTL_SECONDS);
    if (Result.isError(markerAppendRes)) return responseForError(args, markerAppendRes.error);
    const auditRes = await appendWorkspaceAuditEventResult(args, {
      type: "workspace_commit_succeeded",
      workspaceId,
      ref,
      actorId: body.author.id,
      status: "succeeded",
      context: {
        oldCommitId: currentHead,
        newCommitId: commit.id,
        git: gitCommit,
      },
    });
    if (Result.isError(auditRes)) return responseForError(args, auditRes.error);

    return args.respond.json(200, {
      ref,
      oldCommitId: currentHead,
      newCommitId: commit.id,
      commit,
      git: gitCommit,
    });
  });
}

async function discardWorkspace(args: StreamProfileVfsRouteArgs, workspaceId: string): Promise<Response> {
  const markerAppendRes = await appendJsonResult(args, workspaceStreamName(args.stream, workspaceId), [
    {
      value: { kind: "workspace-discarded", createdAt: new Date().toISOString() },
      routingKey: "workspace",
    },
  ], VFS_WORKSPACE_TTL_SECONDS);
  if (Result.isError(markerAppendRes)) return responseForError(args, markerAppendRes.error);
  const auditRes = await appendWorkspaceAuditEventResult(args, {
    type: "workspace_discarded",
    workspaceId,
    status: "succeeded",
  });
  if (Result.isError(auditRes)) return responseForError(args, auditRes.error);
  return args.respond.json(200, { workspaceId, state: "discarded" });
}

async function loadGitCommitAsVfsResult(args: {
  route: StreamProfileVfsRouteArgs;
  gitRepoStream: string;
  gitConfig: GitRepoProfileConfig;
  commitOid: string;
}): Promise<Result<VfsCommit, VfsServerError>> {
  const commitRes = await readLooseGitObjectBodyResult({
    repoStream: args.gitRepoStream,
    objectStore: args.route.objectStore,
    format: args.gitConfig.objectFormat,
    oid: args.commitOid,
    expectedType: "commit",
  });
  if (Result.isError(commitRes)) return Result.err(commitRes.error);
  const parsedRes = parseGitCommitBodyResult(commitRes.value.body, args.gitConfig.objectFormat);
  if (Result.isError(parsedRes)) return Result.err({ status: 500, message: parsedRes.error.message });
  return Result.ok({
    kind: "commit",
    id: args.commitOid,
    parents: parsedRes.value.parents,
    rootTreeId: parsedRes.value.tree,
    author: { id: "git" },
    message: parsedRes.value.message,
    createdAt: "",
    git: {
      repoStream: args.gitRepoStream,
      ref: "",
      oldOid: parsedRes.value.parents[0] ?? null,
      newOid: args.commitOid,
      txnId: "",
      objectCount: 0,
      bytes: 0,
    },
  });
}

async function log(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const ref = normalizeRef(args.url.searchParams.get("ref"));
  const limitRaw = Number(args.url.searchParams.get("limit") ?? "20");
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(100, Math.floor(limitRaw))) : 20;
  const gitRepoStream = gitRepoStreamForVfsProfile(args);
  if (gitRepoStream && isWorkspaceFsProfile(args)) {
    const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStream);
    if (Result.isError(gitConfigRes)) return responseForError(args, gitConfigRes.error);
    const headRes = await currentGitHeadResult(args, gitRepoStream, ref);
    if (Result.isError(headRes)) return responseForError(args, headRes.error);
    const commits: VfsCommit[] = [];
    let commitId = headRes.value;
    while (commitId && commits.length < limit) {
      const commitRes = await loadGitCommitAsVfsResult({
        route: args,
        gitRepoStream,
        gitConfig: gitConfigRes.value,
        commitOid: commitId,
      });
      if (Result.isError(commitRes)) return responseForError(args, commitRes.error);
      commits.push(commitRes.value);
      commitId = commitRes.value.parents[0] ?? null;
    }
    return args.respond.json(200, { commits });
  }
  const refRes = await latestRefResult(args, ref);
  if (Result.isError(refRes)) return responseForError(args, refRes.error);
  const commits: VfsCommit[] = [];
  let commitId = refRes.value?.newCommitId ?? null;
  while (commitId && commits.length < limit) {
    const commitRes = await loadRequiredObjectResult<VfsCommit>(args, commitId, "commit");
    if (Result.isError(commitRes)) return responseForError(args, commitRes.error);
    commits.push(commitRes.value);
    commitId = commitRes.value.parents[0] ?? null;
  }
  return args.respond.json(200, { commits });
}

async function show(args: StreamProfileVfsRouteArgs, commitId: string): Promise<Response> {
  const gitRepoStream = gitRepoStreamForVfsProfile(args);
  if (gitRepoStream && isWorkspaceFsProfile(args)) {
    const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStream);
    if (Result.isError(gitConfigRes)) return responseForError(args, gitConfigRes.error);
    const decoded = decodeURIComponent(commitId);
    if (!oidPattern(gitConfigRes.value.objectFormat).test(decoded)) return args.respond.badRequest("invalid git commit id");
    const commitRes = await loadGitCommitAsVfsResult({
      route: args,
      gitRepoStream,
      gitConfig: gitConfigRes.value,
      commitOid: decoded,
    });
    if (Result.isError(commitRes)) return responseForError(args, commitRes.error);
    return args.respond.json(200, { commit: commitRes.value });
  }
  const commitRes = await loadRequiredObjectResult<VfsCommit>(args, decodeURIComponent(commitId), "commit");
  if (Result.isError(commitRes)) return responseForError(args, commitRes.error);
  return args.respond.json(200, { commit: commitRes.value });
}

async function batchStat(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "batch stat request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const body = bodyRes.value as VfsBatchStatRequest;
  if (!Array.isArray(body.paths)) return args.respond.badRequest("paths must be an array");
  if (body.workspaceId && gitRepoStreamForVfsProfile(args) && isWorkspaceFsProfile(args)) {
    const stats = [];
    for (const rawPath of body.paths.slice(0, 1000)) {
      const pathRes = canonicalizeVfsPath(rawPath);
      if (Result.isError(pathRes)) {
        stats.push({ path: rawPath, node: null, error: pathRes.error.message });
        continue;
      }
      const statRes = await statGitBackedWorkspaceResult(args, pathRes.value, body.workspaceId);
      stats.push({
        path: pathRes.value,
        node: Result.isError(statRes) ? null : statRes.value,
        error: Result.isError(statRes) ? statRes.error.message : undefined,
      });
    }
    return args.respond.json(200, { stats });
  }
  const treeRes = await loadOverlayTreeResult(args, body.commit ?? null, body.workspaceId ?? null);
  if (Result.isError(treeRes)) return responseForError(args, treeRes.error);
  const stats = [];
  for (const rawPath of body.paths.slice(0, 1000)) {
    const pathRes = canonicalizeVfsPath(rawPath);
    if (Result.isError(pathRes)) {
      stats.push({ path: rawPath, node: null, error: pathRes.error.message });
      continue;
    }
    const node = treeRes.value.tree.get(pathRes.value);
    stats.push({
      path: pathRes.value,
      node: node ? nodeStat(pathRes.value, node, treeRes.value.treeIdsByPath.get(pathRes.value) ?? null) : null,
      error: node ? undefined : "path not found",
    });
  }
  return args.respond.json(200, { stats });
}

async function batchReadMetadata(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "batch metadata request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const body = bodyRes.value as VfsBatchReadMetadataRequest;
  if (!Array.isArray(body.ids)) return args.respond.badRequest("ids must be an array");
  const objects = [];
  for (const id of body.ids.slice(0, 1000)) {
    const objectRes = await loadObjectResult(args, id);
    if (Result.isError(objectRes)) return responseForError(args, objectRes.error);
    objects.push({ id, object: objectRes.value });
  }
  return args.respond.json(200, { objects });
}

async function batchReadBlobs(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "batch blob request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const body = bodyRes.value as VfsBatchReadBlobsRequest;
  if (!Array.isArray(body.blobIds)) return args.respond.badRequest("blobIds must be an array");
  const blobs = [];
  for (const blobId of body.blobIds.slice(0, 100)) {
    const bytesRes = await readBlobBytesResult(args, blobId);
    if (Result.isError(bytesRes)) blobs.push({ blobId, contentBase64: null, error: bytesRes.error.message });
    else blobs.push({ blobId, contentBase64: toBase64(bytesRes.value) });
  }
  return args.respond.json(200, { blobs });
}

export async function handleWorkspaceFsRoute(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const [first, second, ...rest] = args.segments;
  if (!first) return args.respond.notFound("missing VFS route");

  if (args.req.method === "POST" && first === "checkout") return checkout(args);
  if (args.req.method === "GET" && first === "ref") return refGet(args, [second ?? "", ...rest].filter(Boolean));
  if (args.req.method === "GET" && first === "stat") return stat(args);
  if (args.req.method === "GET" && first === "readdir") return readdir(args);
  if (args.req.method === "GET" && first === "blob" && second) return blob(args, [second, ...rest].join("/"));
  if (args.req.method === "GET" && first === "log") return log(args);
  if (args.req.method === "GET" && first === "show" && second) return show(args, [second, ...rest].join("/"));

  if (first === "workspace" && second) {
    const workspaceId = decodeURIComponent(second);
    const [workspaceRoute] = rest;
    if (args.req.method === "POST" && workspaceRoute === "ops") return workspaceOps(args, workspaceId);
    if (args.req.method === "GET" && workspaceRoute === "status") return workspaceStatus(args, workspaceId);
    if (args.req.method === "GET" && workspaceRoute === "index") return workspaceIndex(args, workspaceId);
    if (args.req.method === "GET" && workspaceRoute === "changes") return workspaceChanges(args, workspaceId);
    if (args.req.method === "GET" && workspaceRoute === "conflicts") return workspaceConflicts(args, workspaceId);
    if (args.req.method === "POST" && workspaceRoute === "rebase") return rebaseWorkspace(args, workspaceId);
    if (args.req.method === "POST" && workspaceRoute === "compact") return compactWorkspace(args, workspaceId);
    if (args.req.method === "POST" && workspaceRoute === "commit") return commitWorkspace(args, workspaceId);
    if (args.req.method === "POST" && workspaceRoute === "discard") return discardWorkspace(args, workspaceId);
  }

  if (first === "batch") {
    if (args.req.method === "POST" && second === "stat") return batchStat(args);
    if (args.req.method === "POST" && second === "read-metadata") return batchReadMetadata(args);
    if (args.req.method === "POST" && second === "read-blobs") return batchReadBlobs(args);
  }

  return args.respond.notFound("unknown workspace-fs route");
}
