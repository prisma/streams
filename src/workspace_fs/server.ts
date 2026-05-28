import { Buffer } from "node:buffer";
import { Result } from "better-result";
import { parseStoredProfileJsonResult, type StreamProfileRouteArgs } from "../profiles/profile";
import type {
  WorkspaceFsBatchReadBlobsRequest,
  WorkspaceFsBatchReadMetadataRequest,
  WorkspaceFsBatchStatRequest,
  WorkspaceFsBlobManifest,
  WorkspaceFsChunkObject,
  WorkspaceFsCommit,
  WorkspaceFsCommitRequest,
  WorkspaceFsCommitResponse,
  WorkspaceFsCanonicalGitCommit,
  WorkspaceFsGitBlobArtifact,
  WorkspaceFsNodeStat,
  WorkspaceFsStoredObject,
  WorkspaceFsWorkspaceConflictsResponse,
  WorkspaceFsWorkspaceOp,
  WorkspaceFsWorkspaceOpInput,
  WorkspaceFsWorkspaceRebaseResponse,
  WorkspaceFsWorkspaceRecord,
} from "./types";
import {
  WORKSPACE_FS_DEFAULT_REF,
  WORKSPACE_FS_WORKSPACE_TTL_SECONDS,
  basename,
  bytesFromText,
  canonicalizeWorkspaceFsPath,
  decodeStoredObject,
  fromBase64,
  isWorkspaceClosed,
  makeBlobObjects,
  newWorkspaceId,
  normalizeRef,
  objectKey,
  objectsStreamName,
  parentPath,
  toBase64,
  workspaceOpKey,
  workspaceOpsFromRecords,
  workspaceStreamName,
} from "./model";
import {
  GIT_REPO_PROFILE_KIND,
  commitGitRefTransactionResult,
  defaultGitRepoProfileConfig,
  gitLooseObjectKey,
  headObjectStoreResult,
  normalizeGitRef,
  putObjectStoreResult,
  readGitRecordSnapshotResult,
  readGitRefsSnapshotResult,
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

type WorkspaceFsServerError = {
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
  records: WorkspaceFsWorkspaceRecord[];
  checkout: WorkspaceCheckoutRecord | null;
  ops: WorkspaceFsWorkspaceOp[];
  state: ReturnType<typeof isWorkspaceClosed>;
};

function normalizeCommitDurabilityResult(body: WorkspaceFsCommitRequest): Result<{ durability: Exclude<GitTransactionStatus, "accepted"> | "accepted"; timeoutMs: number }, WorkspaceFsServerError> {
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
  op: WorkspaceFsWorkspaceOp;
  objects: WorkspaceFsStoredObject[];
};

type PreparedWorkspaceCommitOp = PreparedWorkspaceOp & {
  gitObjects: GitObject[];
};

type AppendedWorkspaceOps = {
  ops: WorkspaceFsWorkspaceOp[];
};

type WorkspaceOverlayIndex = {
  latestByPath: Map<string, WorkspaceFsWorkspaceOp>;
  childrenByDir: Map<string, Set<string>>;
  deletedPaths: Set<string>;
};

const TEXT_DECODER = new TextDecoder();
const WORKSPACE_FS_GIT_OBJECT_WRITE_CONCURRENCY = 128;

async function mapConcurrentWorkspaceResult<T, U>(
  items: T[],
  concurrency: number,
  fn: (item: T, index: number) => Promise<Result<U, WorkspaceFsServerError>>
): Promise<Result<U[], WorkspaceFsServerError>> {
  if (items.length === 0) return Result.ok([]);
  const results = new Array<U>(items.length);
  let next = 0;
  let firstError: WorkspaceFsServerError | null = null;
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

function responseForError(args: StreamProfileRouteArgs, err: WorkspaceFsServerError): Response {
  if (err.status === 400) return args.respond.badRequest(err.message);
  if (err.status === 404) return args.respond.notFound(err.message);
  if (err.status === 409) return args.respond.conflict(err.message);
  return args.respond.internalError(err.message);
}

function parseJsonObjectBodyResult(body: unknown, label: string): Result<Record<string, unknown>, WorkspaceFsServerError> {
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    return Result.err({ status: 400, message: `${label} must be a JSON object` });
  }
  return Result.ok(body as Record<string, unknown>);
}

async function readRequestJsonResult(req: Request, label: string): Promise<Result<unknown, WorkspaceFsServerError>> {
  try {
    return Result.ok(await req.json());
  } catch {
    return Result.err({ status: 400, message: `${label} must be valid JSON` });
  }
}

async function readJsonRecordsResult(
  args: StreamProfileRouteArgs,
  stream: string,
  key: string | null = null
): Promise<Result<unknown[], WorkspaceFsServerError>> {
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
  args: StreamProfileRouteArgs,
  stream: string,
  records: Array<{ value: unknown; routingKey?: string | null }>,
  ttlSeconds: number | null = null
): Promise<Result<void, WorkspaceFsServerError>> {
  const appendRes = await args.appendJsonRecords({ stream, records, ttlSeconds });
  if (Result.isError(appendRes)) {
    if (appendRes.error.kind === "overloaded") return Result.err({ status: 500, message: "workspace-fs append overloaded" });
    if (appendRes.error.kind === "not_found" || appendRes.error.kind === "gone") return Result.err({ status: 404, message: "stream not found" });
    if (appendRes.error.kind === "offset_mismatch") return Result.err({ status: 409, message: "stream changed" });
    if (appendRes.error.kind === "content_type_mismatch") return Result.err({ status: 409, message: "content-type mismatch" });
    return Result.err({ status: 500, message: appendRes.error.message });
  }
  return Result.ok(undefined);
}

async function writeObjectsResult(args: StreamProfileRouteArgs, objects: WorkspaceFsStoredObject[]): Promise<Result<void, WorkspaceFsServerError>> {
  if (objects.length === 0) return Result.ok(undefined);
  const uniqueObjects = uniqueWorkspaceFsObjects(objects);
  const stream = objectsStreamName(args.stream);
  const appendRes = await appendJsonResult(
    args,
    stream,
    uniqueObjects.map((value) => ({ value, routingKey: objectKey(value.id) }))
  );
  if (Result.isError(appendRes)) return appendRes;
  return Result.ok(undefined);
}

function uniqueWorkspaceFsObjects(objects: WorkspaceFsStoredObject[]): WorkspaceFsStoredObject[] {
  const seen = new Set<string>();
  const unique: WorkspaceFsStoredObject[] = [];
  for (const object of objects) {
    if (seen.has(object.id)) continue;
    seen.add(object.id);
    unique.push(object);
  }
  return unique;
}

function uniqueGitObjects(objects: GitObject[]): GitObject[] {
  const seen = new Set<string>();
  const unique: GitObject[] = [];
  for (const object of objects) {
    if (seen.has(object.oid)) continue;
    seen.add(object.oid);
    unique.push(object);
  }
  return unique;
}

async function loadObjectResult(
  args: StreamProfileRouteArgs,
  id: string
): Promise<Result<WorkspaceFsStoredObject | null, WorkspaceFsServerError>> {
  const recordsRes = await readJsonRecordsResult(args, objectsStreamName(args.stream), objectKey(id));
  if (Result.isError(recordsRes)) return recordsRes;
  if (recordsRes.value.length === 0) return Result.ok(null);
  const latest = recordsRes.value[recordsRes.value.length - 1];
  const objectRes = decodeStoredObject(latest);
  if (Result.isError(objectRes)) return Result.err({ status: 500, message: objectRes.error.message });
  return Result.ok(objectRes.value);
}

async function loadRequiredObjectResult<T extends WorkspaceFsStoredObject>(
  args: StreamProfileRouteArgs,
  id: string,
  kind: T["kind"]
): Promise<Result<T, WorkspaceFsServerError>> {
  const objectRes = await loadObjectResult(args, id);
  if (Result.isError(objectRes)) return objectRes;
  if (!objectRes.value) return Result.err({ status: 404, message: `workspace object not found: ${id}` });
  if (objectRes.value.kind !== kind) return Result.err({ status: 500, message: `workspace object ${id} is not ${kind}` });
  return Result.ok(objectRes.value as T);
}

async function readBlobBytesResult(args: StreamProfileRouteArgs, blobId: string): Promise<Result<Uint8Array, WorkspaceFsServerError>> {
  const gitBlob = decodeGitBlobId(blobId);
  if (gitBlob) {
    const bodyRes = await readLooseGitObjectBodyResult({
      repoStream: gitBlob.repoStream,
      objectStore: args.objectStore,
      format: gitBlob.format,
      oid: gitBlob.oid,
      expectedType: "blob",
    });
    if (Result.isError(bodyRes)) return Result.err(bodyRes.error);
    return Result.ok(bodyRes.value.body);
  }
  const manifestRes = await loadRequiredObjectResult<WorkspaceFsBlobManifest>(args, blobId, "blob");
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
    const chunkRes = await loadRequiredObjectResult<WorkspaceFsChunkObject>(args, chunkRef.id, "chunk");
    if (Result.isError(chunkRes)) return chunkRes;
    const bytesRes = fromBase64(chunkRes.value.dataBase64);
    if (Result.isError(bytesRes)) return Result.err({ status: 500, message: bytesRes.error.message });
    out.set(bytesRes.value, chunkRef.offset);
  }
  return Result.ok(out);
}

async function gitBlobArtifactResult(args: {
  route: StreamProfileRouteArgs;
  repoStream: string;
  format: GitObjectFormat;
  oid: string;
}): Promise<Result<GitLooseObjectResponse, WorkspaceFsServerError>> {
  const headerRes = await readLooseGitObjectHeaderResult({
    repoStream: args.repoStream,
    objectStore: args.route.objectStore,
    format: args.format,
    oid: args.oid,
  });
  if (Result.isError(headerRes)) return Result.err(headerRes.error);
  if (headerRes.value.type !== "blob") return Result.err({ status: 409, message: "git blob id does not point at a blob" });
  const objectKey = gitLooseObjectKey(args.repoStream, args.format, args.oid);
  const headRes = await headObjectStoreResult(args.route.objectStore, objectKey);
  if (Result.isError(headRes)) return Result.err(headRes.error);
  if (!headRes.value) return Result.err({ status: 404, message: "git blob artifact not found" });
  return Result.ok({
    oid: args.oid,
    type: "blob",
    format: args.format,
    size: headerRes.value.size,
    framedSize: headRes.value.size,
    objectKey,
    etag: headRes.value.etag,
    deduplicated: true,
  });
}

function workspaceGitBlobArtifactFromWrite(args: {
  repoStream: string;
  format: GitObjectFormat;
  artifact: GitLooseObjectResponse;
}): WorkspaceFsGitBlobArtifact {
  return {
    repoStream: args.repoStream,
    format: args.format,
    oid: args.artifact.oid,
    objectKey: args.artifact.objectKey,
    size: args.artifact.size,
    framedSize: args.artifact.framedSize,
    etag: args.artifact.etag,
  };
}

function gitLooseObjectResponseFromWorkspaceArtifact(artifact: WorkspaceFsGitBlobArtifact): GitLooseObjectResponse {
  return {
    oid: artifact.oid,
    type: "blob",
    format: artifact.format,
    size: artifact.size,
    framedSize: artifact.framedSize,
    objectKey: artifact.objectKey,
    etag: artifact.etag ?? "",
    deduplicated: true,
  };
}

function validatedWorkspaceGitBlobArtifact(args: {
  op: Extract<WorkspaceFsWorkspaceOp, { kind: "put-file" }>;
  gitRepoStream: string;
  format: GitObjectFormat;
}): WorkspaceFsGitBlobArtifact | null {
  if (!args.op.git) return null;
  const gitBlob = decodeGitBlobId(args.op.blobId);
  if (!gitBlob) return null;
  if (gitBlob.repoStream !== args.gitRepoStream || gitBlob.format !== args.format) return null;
  if (args.op.git.repoStream !== args.gitRepoStream || args.op.git.format !== args.format || args.op.git.oid !== gitBlob.oid) return null;
  if (args.op.git.objectKey !== gitLooseObjectKey(args.gitRepoStream, args.format, gitBlob.oid)) return null;
  if (args.op.git.size !== args.op.size || !Number.isSafeInteger(args.op.git.framedSize) || args.op.git.framedSize <= 0) return null;
  return args.op.git;
}

function gitRepoStreamForWorkspaceProfile(args: StreamProfileRouteArgs): string | null {
  const gitRepo = (args.profile as { gitRepo?: { stream?: unknown } }).gitRepo;
  return typeof gitRepo?.stream === "string" && gitRepo.stream.trim() !== "" ? gitRepo.stream.trim() : null;
}

function auditStreamForWorkspaceProfile(args: StreamProfileRouteArgs): string | null {
  const audit = (args.profile as { audit?: { stream?: unknown } }).audit;
  return typeof audit?.stream === "string" && audit.stream.trim() !== "" ? audit.stream.trim() : null;
}

async function appendWorkspaceAuditEventResult(
  args: StreamProfileRouteArgs,
  event: {
    type: string;
    workspaceId?: string;
    ref?: string;
    actorId?: string;
    status?: "started" | "succeeded" | "failed" | "conflict";
    context?: Record<string, unknown>;
  }
): Promise<Result<void, WorkspaceFsServerError>> {
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

function gitRepoProfileConfigResult(args: StreamProfileRouteArgs, stream: string): Result<GitRepoProfileConfig, WorkspaceFsServerError> {
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

function isWorkspaceFsProfile(args: StreamProfileRouteArgs): boolean {
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

function gitModeToWorkspaceMode(mode: string): number {
  if (mode === "40000") return 0o040755;
  if (mode === "120000") return 0o120777;
  if (mode === "100755") return 0o100755;
  return 0o100644;
}

function gitEntryToWorkspaceStat(args: {
  path: string;
  entry: GitTreeEntry;
  repoStream: string;
  format: GitObjectFormat;
  size: number;
  symlinkTarget?: string;
}): WorkspaceFsNodeStat {
  if (args.entry.type === "dir") {
    return {
      path: args.path,
      type: "dir",
      mode: gitModeToWorkspaceMode(args.entry.mode),
      size: 0,
      treeId: args.entry.oid,
    };
  }
  if (args.entry.type === "symlink") {
    return {
      path: args.path,
      type: "symlink",
      mode: gitModeToWorkspaceMode(args.entry.mode),
      size: args.size,
      symlinkTarget: args.symlinkTarget ?? "",
    };
  }
  return {
    path: args.path,
    type: "file",
    mode: gitModeToWorkspaceMode(args.entry.mode),
    size: args.size,
    blobId: encodeGitBlobId(args.repoStream, args.format, args.entry.oid),
  };
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

async function writeGitObjectOnceResult(args: {
  route: StreamProfileRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  object: GitObject;
  written: Map<string, GitLooseObjectResponse>;
  assumeFresh?: boolean;
}): Promise<Result<GitLooseObjectResponse, WorkspaceFsServerError>> {
  const existing = args.written.get(args.object.oid);
  if (existing && existing.etag !== "") return Result.ok(existing);
  if (args.assumeFresh === true) {
    const objectKey = gitLooseObjectKey(args.gitRepoStream, args.format, args.object.oid);
    const putRes = await putObjectStoreResult(args.route.objectStore, objectKey, args.object.framed, "application/x-git-loose-object");
    if (Result.isError(putRes)) return Result.err(putRes.error);
    const artifact: GitLooseObjectResponse = {
      oid: args.object.oid,
      type: args.object.type,
      format: args.format,
      size: args.object.size,
      framedSize: args.object.framed.byteLength,
      objectKey,
      etag: putRes.value.etag,
      deduplicated: false,
    };
    args.written.set(args.object.oid, artifact);
    return Result.ok(artifact);
  }
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

function gitTreeInputMode(entry: GitTreeEntry): GitTreeEntryInput["mode"] {
  if (entry.type === "dir") return "40000";
  if (entry.type === "symlink") return "120000";
  return entry.mode === "100755" ? "100755" : "100644";
}

async function baseGitRootForCommitResult(args: {
  route: StreamProfileRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  commitOid: string | null;
}): Promise<Result<string | null, WorkspaceFsServerError>> {
  if (!args.commitOid) return Result.ok(null);
  return rootGitTreeOidForCommitResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    format: args.format,
    commitOid: args.commitOid,
  });
}

async function buildGitTreeFromWorkspaceOpsResult(args: {
  route: StreamProfileRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  baseCommitOid: string | null;
  ops: WorkspaceFsWorkspaceOp[];
}): Promise<Result<{
  rootTreeOid: string;
  written: Map<string, GitLooseObjectResponse>;
  treeObjects: GitObject[];
  changeSummary: { added: number; modified: number; deleted: number; renamed: number };
}, WorkspaceFsServerError>> {
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

  async function loadDir(dir: string): Promise<Result<GitTreeEntryInput[], WorkspaceFsServerError>> {
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

  async function ensureDirectoryResult(dir: string): Promise<Result<void, WorkspaceFsServerError>> {
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

  async function upsertPathEntryResult(path: string, entry: GitTreeEntryInput): Promise<Result<void, WorkspaceFsServerError>> {
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

  const gitWriteTargets = args.ops
    .map((op, opIndex) => ({ op, opIndex }))
    .filter((target): target is { op: Extract<WorkspaceFsWorkspaceOp, { kind: "put-file" | "symlink" }>; opIndex: number } =>
      target.op.kind === "put-file" || target.op.kind === "symlink");
  type PreparedGitWrite = { opIndex: number; entry: GitTreeEntryInput };
  const preparedGitWritesRes = await mapConcurrentWorkspaceResult<typeof gitWriteTargets[number], PreparedGitWrite>(
    gitWriteTargets,
    WORKSPACE_FS_GIT_OBJECT_WRITE_CONCURRENCY,
    async (target) => {
      if (target.op.kind === "put-file") {
        const cachedArtifact = validatedWorkspaceGitBlobArtifact({
          op: target.op,
          gitRepoStream: args.gitRepoStream,
          format: args.format,
        });
        if (cachedArtifact) {
          written.set(cachedArtifact.oid, gitLooseObjectResponseFromWorkspaceArtifact(cachedArtifact));
          return Result.ok({
            opIndex: target.opIndex,
            entry: {
              mode: target.op.executable === true ? "100755" : "100644",
              name: basename(target.op.path),
              oid: cachedArtifact.oid,
            } satisfies GitTreeEntryInput,
          });
        }
        const gitBlob = decodeGitBlobId(target.op.blobId);
        if (gitBlob && (gitBlob.repoStream !== args.gitRepoStream || gitBlob.format !== args.format)) {
          return Result.err({ status: 409, message: "workspace git blob belongs to another repo" });
        }
        const bytesRes = await readBlobBytesResult(args.route, target.op.blobId);
        if (Result.isError(bytesRes)) return Result.err(bytesRes.error);
        const object = writeGitBlob(bytesRes.value, args.format);
        const writeRes = await writeGitObjectOnceResult({
          route: args.route,
          gitRepoStream: args.gitRepoStream,
          format: args.format,
          object,
          written,
        });
        if (Result.isError(writeRes)) return Result.err(writeRes.error);
        return Result.ok({
          opIndex: target.opIndex,
          entry: {
            mode: target.op.executable === true ? "100755" : "100644",
            name: basename(target.op.path),
            oid: object.oid,
          } satisfies GitTreeEntryInput,
        });
      }
      const object = writeGitBlob(target.op.target, args.format);
      const writeRes = await writeGitObjectOnceResult({
        route: args.route,
        gitRepoStream: args.gitRepoStream,
        format: args.format,
        object,
        written,
      });
      if (Result.isError(writeRes)) return Result.err(writeRes.error);
      return Result.ok({
        opIndex: target.opIndex,
        entry: {
          mode: "120000",
          name: basename(target.op.path),
          oid: object.oid,
        } satisfies GitTreeEntryInput,
      });
    }
  );
  if (Result.isError(preparedGitWritesRes)) return preparedGitWritesRes;
  const preparedGitWrites = new Map(preparedGitWritesRes.value.map((prepared) => [prepared.opIndex, prepared.entry]));

  for (const [opIndex, op] of args.ops.entries()) {
    if (op.kind === "put-file") {
      const entry = preparedGitWrites.get(opIndex);
      if (!entry) return Result.err({ status: 500, message: "prepared git blob entry missing" });
      const applyRes = await upsertPathEntryResult(op.path, entry);
      if (Result.isError(applyRes)) return applyRes;
    } else if (op.kind === "symlink") {
      const entry = preparedGitWrites.get(opIndex);
      if (!entry) return Result.err({ status: 500, message: "prepared git symlink entry missing" });
      const applyRes = await upsertPathEntryResult(op.path, entry);
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
      treeObjects: [],
      changeSummary: { added, modified, deleted, renamed },
    });
  }

  const newTreeOids = new Map<string, string>();
  const treeObjects: GitObject[] = [];
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
    treeObjects.push(treeRes.value);
    newTreeOids.set(dir, treeRes.value.oid);
  }

  const rootTreeOid = newTreeOids.get("/") ?? rootTreeRes.value;
  if (!rootTreeOid) return Result.err({ status: 500, message: "git tree build did not produce a root tree" });
  return Result.ok({
    rootTreeOid,
    written,
    treeObjects,
    changeSummary: { added, modified, deleted, renamed },
  });
}

async function currentGitHeadResult(
  args: StreamProfileRouteArgs,
  gitRepoStream: string,
  ref: string
): Promise<Result<string | null, WorkspaceFsServerError>> {
  const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStream);
  if (Result.isError(gitConfigRes)) return gitConfigRes;
  const refsRes = await readGitRefsSnapshotResult({
    stream: gitRepoStream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    format: gitConfigRes.value.objectFormat,
  });
  if (Result.isError(refsRes)) return Result.err(refsRes.error);
  const refs = refsRes.value.refs;
  const normalized = normalizeGitRef(ref);
  return Result.ok(Object.prototype.hasOwnProperty.call(refs, normalized) ? refs[normalized] ?? null : null);
}

function latestUploadedThroughForStream(args: StreamProfileRouteArgs, stream: string): bigint {
  return args.db.getStream(stream)?.uploaded_through ?? -1n;
}

async function waitGitTransactionDurabilityResult(args: {
  route: StreamProfileRouteArgs;
  gitRepoStream: string;
  gitConfig: GitRepoProfileConfig;
  txnId: string;
  durability: "accepted" | "published" | "verified";
  timeoutMs: number;
}): Promise<Result<GitTransactionStatus, WorkspaceFsServerError>> {
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
  route: StreamProfileRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  commitOid: string;
}): Promise<Result<string, WorkspaceFsServerError>> {
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
  route: StreamProfileRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  treeOid: string;
}): Promise<Result<GitTreeEntry[], WorkspaceFsServerError>> {
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
  route: StreamProfileRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  path: string;
  entry: GitTreeEntry;
}): Promise<Result<WorkspaceFsNodeStat, WorkspaceFsServerError>> {
  if (args.entry.type === "dir") {
    return Result.ok(gitEntryToWorkspaceStat({ ...args, repoStream: args.gitRepoStream, size: 0 }));
  }
  if (args.entry.type === "file") {
    const sizeRes = await gitBlobSizeResult({
      route: args.route,
      gitRepoStream: args.gitRepoStream,
      format: args.format,
      oid: args.entry.oid,
    });
    if (Result.isError(sizeRes)) return sizeRes;
    return Result.ok(gitEntryToWorkspaceStat({
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
  return Result.ok(gitEntryToWorkspaceStat({
    path: args.path,
    entry: args.entry,
    repoStream: args.gitRepoStream,
    format: args.format,
    size: bodyRes.value.header.size,
    symlinkTarget,
  }));
}

async function gitBlobSizeResult(args: {
  route: StreamProfileRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  oid: string;
}): Promise<Result<number, WorkspaceFsServerError>> {
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
  route: StreamProfileRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  path: string;
  entry: GitTreeEntry;
}): Promise<Result<WorkspaceFsNodeStat, WorkspaceFsServerError>> {
  if (args.entry.type === "dir") {
    return Result.ok(gitEntryToWorkspaceStat({ ...args, repoStream: args.gitRepoStream, size: 0 }));
  }
  if (args.entry.type === "symlink") return statGitEntryResult(args);
  const sizeRes = await gitBlobSizeResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    format: args.format,
    oid: args.entry.oid,
  });
  if (Result.isError(sizeRes)) return sizeRes;
  return Result.ok(gitEntryToWorkspaceStat({
    path: args.path,
    entry: args.entry,
    repoStream: args.gitRepoStream,
    format: args.format,
    size: sizeRes.value,
  }));
}

async function resolveGitPathForWorkspaceResult(args: {
  route: StreamProfileRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  commitOid: string;
  path: string;
}): Promise<Result<{ node: WorkspaceFsNodeStat; treeOid: string }, WorkspaceFsServerError>> {
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

function workspaceCheckout(records: WorkspaceFsWorkspaceRecord[]): WorkspaceCheckoutRecord | null {
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

async function loadWorkspaceResult(args: StreamProfileRouteArgs, workspaceId: string): Promise<Result<LoadedWorkspace, WorkspaceFsServerError>> {
  const recordsRes = await readJsonRecordsResult(args, workspaceStreamName(args.stream, workspaceId));
  if (Result.isError(recordsRes)) return recordsRes;
  const records = recordsRes.value as WorkspaceFsWorkspaceRecord[];
  return Result.ok({
    records,
    checkout: workspaceCheckout(records),
    ops: workspaceOpsFromRecords(records),
    state: isWorkspaceClosed(records),
  });
}

function overlayStatFromOp(op: WorkspaceFsWorkspaceOp): WorkspaceFsNodeStat | null {
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

function addOverlayPath(index: WorkspaceOverlayIndex, path: string, op: WorkspaceFsWorkspaceOp): void {
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

function buildWorkspaceOverlayIndex(ops: WorkspaceFsWorkspaceOp[]): WorkspaceOverlayIndex {
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
      const moved: Array<{ from: string; to: string; op: WorkspaceFsWorkspaceOp }> = [];
      for (const [path, existing] of index.latestByPath.entries()) {
        if (path === op.from || path.startsWith(`${op.from}/`)) {
          const suffix = path === op.from ? "" : path.slice(op.from.length);
          moved.push({ from: path, to: `${op.to}${suffix}`, op: { ...existing, path: `${op.to}${suffix}` } as WorkspaceFsWorkspaceOp });
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

function parseCommitParam(url: URL): string | null {
  const commit = url.searchParams.get("commit");
  return commit && commit.trim() !== "" ? commit : null;
}

function parseWorkspaceParam(url: URL): string | null {
  const workspaceId = url.searchParams.get("workspaceId") ?? url.searchParams.get("workspace");
  return workspaceId && workspaceId.trim() !== "" ? workspaceId : null;
}

async function gitWorkspaceBaseResult(args: StreamProfileRouteArgs, workspaceId: string): Promise<Result<{
  workspace: LoadedWorkspace;
  gitRepoStream: string;
  gitConfig: GitRepoProfileConfig;
  baseCommitOid: string | null;
}, WorkspaceFsServerError>> {
  const gitRepoStream = gitRepoStreamForWorkspaceProfile(args);
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
  args: StreamProfileRouteArgs,
  path: string,
  workspaceId: string
): Promise<Result<WorkspaceFsNodeStat, WorkspaceFsServerError>> {
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
  args: StreamProfileRouteArgs,
  dir: string,
  workspaceId: string,
  cursor: string | null,
  limit: number
): Promise<Result<{ entries: WorkspaceFsNodeStat[]; nextCursor: string | null }, WorkspaceFsServerError>> {
  const baseRes = await gitWorkspaceBaseResult(args, workspaceId);
  if (Result.isError(baseRes)) return baseRes;
  const overlay = buildWorkspaceOverlayIndex(baseRes.value.workspace.ops);
  if (pathHiddenByOverlay(overlay, dir)) return Result.err({ status: 404, message: "path not found" });

  const byName = new Map<string, WorkspaceFsNodeStat>();
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

async function checkout(args: StreamProfileRouteArgs): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "checkout request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const bodyObjRes = parseJsonObjectBodyResult(bodyRes.value, "checkout request");
  if (Result.isError(bodyObjRes)) return responseForError(args, bodyObjRes.error);
  const ref = normalizeRef(typeof bodyObjRes.value.ref === "string" ? bodyObjRes.value.ref : WORKSPACE_FS_DEFAULT_REF);
  const workspaceId = typeof bodyObjRes.value.workspaceId === "string" && bodyObjRes.value.workspaceId.trim() !== "" ? bodyObjRes.value.workspaceId : newWorkspaceId();
  const ttlSeconds = typeof bodyObjRes.value.ttlSeconds === "number" && Number.isFinite(bodyObjRes.value.ttlSeconds)
    ? Math.max(1, Math.floor(bodyObjRes.value.ttlSeconds))
    : WORKSPACE_FS_WORKSPACE_TTL_SECONDS;

  const gitRepoStream = gitRepoStreamForWorkspaceProfile(args);
  if (!gitRepoStream || !isWorkspaceFsProfile(args)) return args.respond.badRequest("workspace checkout requires git-repo backing");
  const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStream);
  if (Result.isError(gitConfigRes)) return responseForError(args, gitConfigRes.error);
  const gitHeadRes = await currentGitHeadResult(args, gitRepoStream, ref);
  if (Result.isError(gitHeadRes)) return responseForError(args, gitHeadRes.error);
  const baseCommitId = gitHeadRes.value;
  let rootTreeId: string | null = null;
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

async function refGet(args: StreamProfileRouteArgs, refSegments: string[]): Promise<Response> {
  const ref = normalizeRef(decodeURIComponent(refSegments.join("/")));
  const gitRepoStream = gitRepoStreamForWorkspaceProfile(args);
  if (!gitRepoStream || !isWorkspaceFsProfile(args)) return args.respond.badRequest("workspace ref lookup requires git-repo backing");
  const gitHeadRes = await currentGitHeadResult(args, gitRepoStream, ref);
  if (Result.isError(gitHeadRes)) return responseForError(args, gitHeadRes.error);
  return args.respond.json(200, {
    ref,
    commitId: gitHeadRes.value,
  });
}

async function stat(args: StreamProfileRouteArgs): Promise<Response> {
  const pathRes = canonicalizeWorkspaceFsPath(args.url.searchParams.get("path") ?? "/");
  if (Result.isError(pathRes)) return args.respond.badRequest(pathRes.error.message);
  const gitRepoStream = gitRepoStreamForWorkspaceProfile(args);
  if (!gitRepoStream || !isWorkspaceFsProfile(args)) return args.respond.badRequest("workspace stat requires git-repo backing");
  const workspaceId = parseWorkspaceParam(args.url);
  if (workspaceId) {
    const statRes = await statGitBackedWorkspaceResult(args, pathRes.value, workspaceId);
    if (Result.isError(statRes)) return responseForError(args, statRes.error);
    return args.respond.json(200, { node: statRes.value });
  }
  const gitCommitId = parseCommitParam(args.url);
  if (gitCommitId) {
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
  return args.respond.badRequest("workspace stat requires workspaceId or commit");
}

async function readdir(args: StreamProfileRouteArgs): Promise<Response> {
  const pathRes = canonicalizeWorkspaceFsPath(args.url.searchParams.get("path") ?? "/");
  if (Result.isError(pathRes)) return args.respond.badRequest(pathRes.error.message);
  const rawLimit = Number(args.url.searchParams.get("limit") ?? "500");
  const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(1000, Math.floor(rawLimit))) : 500;
  const gitRepoStream = gitRepoStreamForWorkspaceProfile(args);
  if (!gitRepoStream || !isWorkspaceFsProfile(args)) return args.respond.badRequest("workspace readdir requires git-repo backing");
  const workspaceId = parseWorkspaceParam(args.url);
  if (workspaceId) {
    const entriesRes = await readdirGitBackedWorkspaceResult(args, pathRes.value, workspaceId, args.url.searchParams.get("cursor"), limit);
    if (Result.isError(entriesRes)) return responseForError(args, entriesRes.error);
    return args.respond.json(200, {
      path: pathRes.value,
      entries: entriesRes.value.entries,
      nextCursor: entriesRes.value.nextCursor,
    });
  }
  const gitCommitId = parseCommitParam(args.url);
  if (gitCommitId) {
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
    const stats: WorkspaceFsNodeStat[] = [];
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
  return args.respond.badRequest("workspace readdir requires workspaceId or commit");
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

async function auditBlobReadResult(
  args: StreamProfileRouteArgs,
  blobId: string,
  range: { start: number; end: number } | null,
  size: number
): Promise<Result<void, WorkspaceFsServerError>> {
  const workspaceId = args.url.searchParams.get("workspaceId");
  const rawPath = args.url.searchParams.get("path");
  if (!workspaceId && !rawPath) return Result.ok(undefined);
  if (!workspaceId || !rawPath) return Result.err({ status: 400, message: "workspaceId and path are required for audited blob reads" });
  const pathRes = canonicalizeWorkspaceFsPath(rawPath);
  if (Result.isError(pathRes)) return Result.err({ status: 400, message: pathRes.error.message });
  return appendWorkspaceAuditEventResult(args, {
    type: "workspace_file_read",
    workspaceId,
    status: "succeeded",
    context: {
      path: pathRes.value,
      blobId,
      size,
      range: range ? { start: range.start, end: range.end } : null,
    },
  });
}

async function blob(args: StreamProfileRouteArgs, blobId: string): Promise<Response> {
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
    const auditRes = await auditBlobReadResult(args, decodedBlobId, range, headerRes.value.size);
    if (Result.isError(auditRes)) return responseForError(args, auditRes.error);
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
  const auditRes = await auditBlobReadResult(args, decodedBlobId, range, bytesRes.value.byteLength);
  if (Result.isError(auditRes)) return responseForError(args, auditRes.error);
  const body = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
  return new Response(body, { status: range ? 206 : 200, headers });
}

async function prepareWorkspaceOpResult(
  args: StreamProfileRouteArgs,
  op: WorkspaceFsWorkspaceOpInput
): Promise<Result<PreparedWorkspaceOp, WorkspaceFsServerError>> {
  const createdAt = new Date().toISOString();
  if (op.kind === "mkdir") {
    const pathRes = canonicalizeWorkspaceFsPath(op.path);
    if (Result.isError(pathRes)) return Result.err({ status: 400, message: pathRes.error.message });
    return Result.ok({ op: { kind: "mkdir", path: pathRes.value, createdAt }, objects: [] });
  }
  if (op.kind === "delete") {
    const pathRes = canonicalizeWorkspaceFsPath(op.path);
    if (Result.isError(pathRes)) return Result.err({ status: 400, message: pathRes.error.message });
    if (pathRes.value === "/") return Result.err({ status: 400, message: "cannot delete workspace root" });
    return Result.ok({ op: { kind: "delete", path: pathRes.value, recursive: op.recursive, force: op.force, createdAt }, objects: [] });
  }
  if (op.kind === "rename") {
    const fromRes = canonicalizeWorkspaceFsPath(op.from);
    const toRes = canonicalizeWorkspaceFsPath(op.to);
    if (Result.isError(fromRes)) return Result.err({ status: 400, message: fromRes.error.message });
    if (Result.isError(toRes)) return Result.err({ status: 400, message: toRes.error.message });
    if (fromRes.value === "/" || toRes.value === "/") return Result.err({ status: 400, message: "cannot rename workspace root" });
    return Result.ok({ op: { kind: "rename", from: fromRes.value, to: toRes.value, createdAt }, objects: [] });
  }
  if (op.kind === "symlink") {
    const pathRes = canonicalizeWorkspaceFsPath(op.path);
    if (Result.isError(pathRes)) return Result.err({ status: 400, message: pathRes.error.message });
    return Result.ok({ op: { kind: "symlink", path: pathRes.value, target: op.target, createdAt }, objects: [] });
  }
  if (op.kind === "put-file") {
    const pathRes = canonicalizeWorkspaceFsPath(op.path);
    if (Result.isError(pathRes)) return Result.err({ status: 400, message: pathRes.error.message });
    if (pathRes.value === "/") return Result.err({ status: 400, message: "cannot write workspace root" });
    let blobId = op.blobId;
    let size = 0;
    let git: WorkspaceFsGitBlobArtifact | undefined;
    let objects: WorkspaceFsStoredObject[] = [];
    if (!blobId) {
      const bytesRes = op.contentBase64 !== undefined ? fromBase64(op.contentBase64) : Result.ok(bytesFromText(op.text ?? ""));
      if (Result.isError(bytesRes)) return Result.err({ status: 400, message: bytesRes.error.message });
      const gitRepoStream = gitRepoStreamForWorkspaceProfile(args);
      if (gitRepoStream && isWorkspaceFsProfile(args)) {
        const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStream);
        if (Result.isError(gitConfigRes)) return gitConfigRes;
        const object = writeGitBlob(bytesRes.value, gitConfigRes.value.objectFormat);
        const writeRes = await writeLooseGitObjectResult({
          repoStream: gitRepoStream,
          objectStore: args.objectStore,
          format: gitConfigRes.value.objectFormat,
          object,
        });
        if (Result.isError(writeRes)) return Result.err(writeRes.error);
        blobId = encodeGitBlobId(gitRepoStream, gitConfigRes.value.objectFormat, object.oid);
        size = object.size;
        git = workspaceGitBlobArtifactFromWrite({
          repoStream: gitRepoStream,
          format: gitConfigRes.value.objectFormat,
          artifact: writeRes.value,
        });
      } else {
        const blob = makeBlobObjects(bytesRes.value, { executable: op.executable, contentType: op.contentType });
        objects = [...blob.chunks, blob.manifest];
        blobId = blob.manifest.id;
        size = blob.manifest.size;
      }
    } else {
      const gitBlob = decodeGitBlobId(blobId);
      if (gitBlob) {
        const gitRepoStream = gitRepoStreamForWorkspaceProfile(args);
        if (gitRepoStream && gitBlob.repoStream !== gitRepoStream) {
          return Result.err({ status: 409, message: "workspace git blob belongs to another repo" });
        }
        const artifactRes = await gitBlobArtifactResult({
          route: args,
          repoStream: gitBlob.repoStream,
          format: gitBlob.format,
          oid: gitBlob.oid,
        });
        if (Result.isError(artifactRes)) return artifactRes;
        size = artifactRes.value.size;
        git = workspaceGitBlobArtifactFromWrite({
          repoStream: gitBlob.repoStream,
          format: gitBlob.format,
          artifact: artifactRes.value,
        });
      } else {
        const blobRes = await loadRequiredObjectResult<WorkspaceFsBlobManifest>(args, blobId, "blob");
        if (Result.isError(blobRes)) return blobRes;
        size = blobRes.value.size;
      }
    }
    return Result.ok({
      op: {
        kind: "put-file",
        path: pathRes.value,
        blobId,
        size,
        git,
        executable: op.executable,
        contentType: op.contentType,
        createdAt,
      },
      objects,
    });
  }
  return Result.err({ status: 400, message: "unsupported workspace op" });
}

async function prepareWorkspaceCommitOpResult(args: {
  route: StreamProfileRouteArgs;
  op: WorkspaceFsWorkspaceOpInput;
  gitRepoStream: string;
  format: GitObjectFormat;
}): Promise<Result<PreparedWorkspaceCommitOp, WorkspaceFsServerError>> {
  const op = args.op;
  if (op.kind !== "put-file" || op.blobId !== undefined) {
    const preparedRes = await prepareWorkspaceOpResult(args.route, op);
    if (Result.isError(preparedRes)) return preparedRes;
    return Result.ok({ ...preparedRes.value, gitObjects: [] });
  }

  const createdAt = new Date().toISOString();
  const pathRes = canonicalizeWorkspaceFsPath(op.path);
  if (Result.isError(pathRes)) return Result.err({ status: 400, message: pathRes.error.message });
  if (pathRes.value === "/") return Result.err({ status: 400, message: "cannot write workspace root" });
  const bytesRes = op.contentBase64 !== undefined ? fromBase64(op.contentBase64) : Result.ok(bytesFromText(op.text ?? ""));
  if (Result.isError(bytesRes)) return Result.err({ status: 400, message: bytesRes.error.message });

  const object = writeGitBlob(bytesRes.value, args.format);
  const blobId = encodeGitBlobId(args.gitRepoStream, args.format, object.oid);
  const git: WorkspaceFsGitBlobArtifact = {
    repoStream: args.gitRepoStream,
    format: args.format,
    oid: object.oid,
    objectKey: gitLooseObjectKey(args.gitRepoStream, args.format, object.oid),
    size: object.size,
    framedSize: object.framed.byteLength,
  };

  return Result.ok({
    op: {
      kind: "put-file",
      path: pathRes.value,
      blobId,
      size: object.size,
      git,
      executable: op.executable,
      contentType: op.contentType,
      createdAt,
    },
    objects: [],
    gitObjects: [object],
  });
}

async function appendPreparedWorkspaceOpsResult(
  args: StreamProfileRouteArgs,
  workspaceId: string,
  ops: WorkspaceFsWorkspaceOp[],
  objects: WorkspaceFsStoredObject[]
): Promise<Result<void, WorkspaceFsServerError>> {
  const writeObjectsRes = await writeObjectsResult(args, objects);
  if (Result.isError(writeObjectsRes)) return writeObjectsRes;

  return appendWorkspaceCheckoutAndOpsResult(args, workspaceId, null, ops);
}

async function appendWorkspaceCheckoutAndOpsResult(
  args: StreamProfileRouteArgs,
  workspaceId: string,
  checkout: WorkspaceCheckoutRecord | null,
  ops: WorkspaceFsWorkspaceOp[]
): Promise<Result<void, WorkspaceFsServerError>> {
  const records: Array<{ value: WorkspaceFsWorkspaceRecord; routingKey?: string | null }> = [];
  if (checkout) records.push({ value: checkout as WorkspaceFsWorkspaceRecord, routingKey: "workspace" });
  records.push(...ops.map((value) => ({
    value,
    routingKey: value.kind === "rename" ? workspaceOpKey(value.from) : workspaceOpKey(value.path),
  })));
  const appendRes = await appendJsonResult(
    args,
    workspaceStreamName(args.stream, workspaceId),
    records,
    WORKSPACE_FS_WORKSPACE_TTL_SECONDS
  );
  if (Result.isError(appendRes)) return appendRes;
  if (checkout) {
    const checkoutAuditRes = await appendWorkspaceAuditEventResult(args, {
      type: "workspace_checked_out",
      workspaceId,
      ref: checkout.ref,
      status: "succeeded",
      context: { baseCommitId: checkout.baseCommitId, rootTreeId: checkout.rootTreeId, createdWorkspace: true },
    });
    if (Result.isError(checkoutAuditRes)) return checkoutAuditRes;
  }
  if (ops.length === 0) return Result.ok(undefined);
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
  if (Result.isError(auditRes)) return auditRes;
  return Result.ok(undefined);
}

async function workspaceOps(args: StreamProfileRouteArgs, workspaceId: string): Promise<Response> {
  const loadedRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(loadedRes)) return responseForError(args, loadedRes.error);
  if (loadedRes.value.state.state !== "open") return args.respond.conflict(`workspace is ${loadedRes.value.state.state}`);

  const bodyRes = await readRequestJsonResult(args.req, "workspace ops request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const bodyObjRes = parseJsonObjectBodyResult(bodyRes.value, "workspace ops request");
  if (Result.isError(bodyObjRes)) return responseForError(args, bodyObjRes.error);
  const appendRes = await appendWorkspaceOpsResult(args, workspaceId, bodyObjRes.value.ops);
  if (Result.isError(appendRes)) return responseForError(args, appendRes.error);
  return args.respond.json(200, { workspaceId, appended: appendRes.value.ops.length, ops: appendRes.value.ops });
}

async function appendWorkspaceOpsResult(
  args: StreamProfileRouteArgs,
  workspaceId: string,
  rawOps: unknown
): Promise<Result<AppendedWorkspaceOps, WorkspaceFsServerError>> {
  if (!Array.isArray(rawOps) || rawOps.length === 0) return Result.err({ status: 400, message: "ops must be a non-empty array" });
  if (rawOps.length > 1000) return Result.err({ status: 400, message: "too many ops" });

  const preparedOpsRes = await mapConcurrentWorkspaceResult(
    rawOps as WorkspaceFsWorkspaceOpInput[],
    WORKSPACE_FS_GIT_OBJECT_WRITE_CONCURRENCY,
    (rawOp) => prepareWorkspaceOpResult(args, rawOp)
  );
  if (Result.isError(preparedOpsRes)) return preparedOpsRes;
  const ops: WorkspaceFsWorkspaceOp[] = [];
  const objects: WorkspaceFsStoredObject[] = [];
  for (const prepared of preparedOpsRes.value) {
    ops.push(prepared.op);
    objects.push(...prepared.objects);
  }

  const appendRes = await appendPreparedWorkspaceOpsResult(args, workspaceId, ops, objects);
  if (Result.isError(appendRes)) return appendRes;
  return Result.ok({ ops });
}

async function prepareGitBackedCommitOpsResult(args: {
  route: StreamProfileRouteArgs;
  rawOps: unknown;
  gitRepoStream: string;
  format: GitObjectFormat;
}): Promise<Result<{
  ops: WorkspaceFsWorkspaceOp[];
  workspaceObjects: WorkspaceFsStoredObject[];
  gitObjects: GitObject[];
}, WorkspaceFsServerError>> {
  if (!Array.isArray(args.rawOps) || args.rawOps.length === 0) return Result.err({ status: 400, message: "ops must be a non-empty array" });
  if (args.rawOps.length > 1000) return Result.err({ status: 400, message: "too many ops" });
  const preparedOpsRes = await mapConcurrentWorkspaceResult(
    args.rawOps as WorkspaceFsWorkspaceOpInput[],
    WORKSPACE_FS_GIT_OBJECT_WRITE_CONCURRENCY,
    (op) => prepareWorkspaceCommitOpResult({
      route: args.route,
      op,
      gitRepoStream: args.gitRepoStream,
      format: args.format,
    })
  );
  if (Result.isError(preparedOpsRes)) return preparedOpsRes;
  const ops: WorkspaceFsWorkspaceOp[] = [];
  const workspaceObjects: WorkspaceFsStoredObject[] = [];
  const gitObjects: GitObject[] = [];
  for (const prepared of preparedOpsRes.value) {
    ops.push(prepared.op);
    workspaceObjects.push(...prepared.objects);
    gitObjects.push(...prepared.gitObjects);
  }
  return Result.ok({ ops, workspaceObjects, gitObjects });
}

function changedPaths(ops: WorkspaceFsWorkspaceOp[]): string[] {
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
  route: StreamProfileRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  commitOid: string | null;
}): Promise<Result<Map<string, string>, WorkspaceFsServerError>> {
  const paths = new Map<string, string>();
  if (!args.commitOid) return Result.ok(paths);
  const rootRes = await rootGitTreeOidForCommitResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    format: args.format,
    commitOid: args.commitOid,
  });
  if (Result.isError(rootRes)) return rootRes;

  async function walk(dir: string, treeOid: string): Promise<Result<void, WorkspaceFsServerError>> {
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
  route: StreamProfileRouteArgs;
  gitRepoStream: string;
  format: GitObjectFormat;
  baseCommitOid: string | null;
  headCommitOid: string | null;
}): Promise<Result<string[], WorkspaceFsServerError>> {
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
  args: StreamProfileRouteArgs,
  workspaceId: string
): Promise<Result<WorkspaceFsWorkspaceConflictsResponse, WorkspaceFsServerError>> {
  const workspaceRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(workspaceRes)) return workspaceRes;
  const workspace = workspaceRes.value;
  if (!workspace.checkout) return Result.err({ status: 404, message: "workspace not found" });
  if (workspace.state.state !== "open") return Result.err({ status: 409, message: `workspace is ${workspace.state.state}` });

  const gitRepoStream = gitRepoStreamForWorkspaceProfile(args);
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

async function workspaceStatus(args: StreamProfileRouteArgs, workspaceId: string): Promise<Response> {
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

async function workspaceIndex(args: StreamProfileRouteArgs, workspaceId: string): Promise<Response> {
  const workspaceRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(workspaceRes)) return responseForError(args, workspaceRes.error);
  const rawPath = args.url.searchParams.get("path");
  let path: string | null = null;
  if (rawPath != null) {
    const pathRes = canonicalizeWorkspaceFsPath(rawPath);
    if (Result.isError(pathRes)) return args.respond.badRequest(pathRes.error.message);
    path = pathRes.value;
  }
  return args.respond.json(200, overlayIndexResponse(workspaceId, workspaceRes.value, path));
}

async function workspaceChanges(args: StreamProfileRouteArgs, workspaceId: string): Promise<Response> {
  const workspaceRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(workspaceRes)) return responseForError(args, workspaceRes.error);
  const prefixRes = canonicalizeWorkspaceFsPath(args.url.searchParams.get("prefix") ?? "/");
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

async function workspaceConflicts(args: StreamProfileRouteArgs, workspaceId: string): Promise<Response> {
  const snapshotRes = await workspaceConflictSnapshotResult(args, workspaceId);
  if (Result.isError(snapshotRes)) return responseForError(args, snapshotRes.error);
  return args.respond.json(200, snapshotRes.value);
}

async function rebaseWorkspace(args: StreamProfileRouteArgs, workspaceId: string): Promise<Response> {
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

  const gitRepoStream = gitRepoStreamForWorkspaceProfile(args);
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
    ], WORKSPACE_FS_WORKSPACE_TTL_SECONDS);
    if (Result.isError(appendRes)) return responseForError(args, appendRes.error);
  }

  const response: WorkspaceFsWorkspaceRebaseResponse = {
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
}

async function compactWorkspace(args: StreamProfileRouteArgs, workspaceId: string): Promise<Response> {
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
  ], WORKSPACE_FS_WORKSPACE_TTL_SECONDS);
  if (Result.isError(appendRes)) return responseForError(args, appendRes.error);
  return args.respond.json(200, snapshot);
}

function pathParts(path: string): string[] {
  return path.split("/").filter((part) => part.length > 0);
}

function childPath(dir: string, name: string): string {
  return dir === "/" ? `/${name}` : `${dir}/${name}`;
}

async function commitGitBackedWorkspaceResult(args: {
  route: StreamProfileRouteArgs;
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
  pendingCheckout?: WorkspaceCheckoutRecord | null;
  pendingOps?: WorkspaceFsWorkspaceOp[];
  precomputedGitObjects?: GitObject[];
}): Promise<Result<WorkspaceFsCommitResponse, WorkspaceFsServerError>> {
  const expectedHead = args.expectedHead === undefined ? args.workspace.checkout?.baseCommitId ?? null : args.expectedHead;
  const baseHead = expectedHead;

  const treeRes = await buildGitTreeFromWorkspaceOpsResult({
    route: args.route,
    gitRepoStream: args.gitRepoStream,
    format: args.gitConfig.objectFormat,
    baseCommitOid: baseHead,
    ops: args.workspace.ops,
  });
  if (Result.isError(treeRes)) return treeRes;

  const createdAt = new Date().toISOString();
  const person = safeGitPerson(args.author, createdAt);
  const commitObjectRes = writeGitCommitResult({
    tree: treeRes.value.rootTreeOid,
    parents: baseHead ? [baseHead] : [],
    author: person,
    committer: person,
    message: args.message,
  }, args.gitConfig.objectFormat);
  if (Result.isError(commitObjectRes)) return Result.err({ status: 500, message: commitObjectRes.error.message });
  const objectsToWrite = uniqueGitObjects([...(args.precomputedGitObjects ?? []), ...treeRes.value.treeObjects, commitObjectRes.value]);
  const objectWritesRes = await mapConcurrentWorkspaceResult(
    objectsToWrite,
    WORKSPACE_FS_GIT_OBJECT_WRITE_CONCURRENCY,
    async (object) => {
      const writeRes = await writeGitObjectOnceResult({
        route: args.route,
        gitRepoStream: args.gitRepoStream,
        format: args.gitConfig.objectFormat,
        object,
        written: treeRes.value.written,
        assumeFresh: true,
      });
      if (Result.isError(writeRes)) return Result.err(writeRes.error);
      return Result.ok(writeRes.value);
    }
  );
  if (Result.isError(objectWritesRes)) return objectWritesRes;

  if (args.pendingCheckout || (args.pendingOps && args.pendingOps.length > 0)) {
    const appendOpsRes = await appendWorkspaceCheckoutAndOpsResult(args.route, args.workspaceId, args.pendingCheckout ?? null, args.pendingOps ?? []);
    if (Result.isError(appendOpsRes)) return appendOpsRes;
  }

  const artifacts = Array.from(treeRes.value.written.values());
  const txnId = `workspace:${args.workspaceId}:${commitObjectRes.value.oid}`;
  const txnRes = await commitGitRefTransactionResult({
    stream: args.gitRepoStream,
    reader: args.route.reader,
    objectStore: args.route.objectStore,
    appendJsonRecords: args.route.appendJsonRecords,
    format: args.gitConfig.objectFormat,
    verificationMode: "changed-objects",
    request: {
      txnId,
      actor: args.author.id,
      refUpdates: [{
        ref: normalizeGitRef(args.ref),
        oldOid: baseHead,
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

  const gitCommit: WorkspaceFsCanonicalGitCommit = {
    repoStream: args.gitRepoStream,
    ref: normalizeGitRef(args.ref),
    oldOid: baseHead,
    newOid: commitObjectRes.value.oid,
    txnId: txnRes.value.transaction.txnId,
    objectCount: artifacts.length,
    bytes: artifacts.reduce((sum, artifact) => sum + artifact.framedSize, 0),
    durability: durabilityRes.value,
  };
  const commit: WorkspaceFsCommit = {
    kind: "commit",
    id: commitObjectRes.value.oid,
    parents: baseHead ? [baseHead] : [],
    rootTreeId: treeRes.value.rootTreeOid,
    author: args.author,
    message: args.message,
    createdAt,
    workspaceId: args.workspaceId,
    changeSummary: treeRes.value.changeSummary,
    git: gitCommit,
  };

  const markerAppendRes = await appendJsonResult(args.route, workspaceStreamName(args.route.stream, args.workspaceId), [
    {
      value: { kind: "workspace-committed", commitId: commit.id, git: gitCommit, createdAt },
      routingKey: "workspace",
    },
  ], WORKSPACE_FS_WORKSPACE_TTL_SECONDS);
  if (Result.isError(markerAppendRes)) return markerAppendRes;

  return Result.ok({
    ref: normalizeGitRef(args.ref),
    oldCommitId: baseHead,
    newCommitId: commitObjectRes.value.oid,
    commit,
    git: gitCommit,
  });
}

async function commitLoadedWorkspaceResponse(args: {
  route: StreamProfileRouteArgs;
  workspaceId: string;
  workspace: LoadedWorkspace;
  body: WorkspaceFsCommitRequest;
  ref: string;
  durability: { durability: Exclude<GitTransactionStatus, "accepted"> | "accepted"; timeoutMs: number };
  pendingCheckout?: WorkspaceCheckoutRecord | null;
  pendingOps?: WorkspaceFsWorkspaceOp[];
  precomputedGitObjects?: GitObject[];
}): Promise<Response> {
  const startAuditRes = await appendWorkspaceAuditEventResult(args.route, {
    type: "workspace_commit_started",
    workspaceId: args.workspaceId,
    ref: args.ref,
    actorId: args.body.author.id,
    status: "started",
    context: { durability: args.durability.durability },
  });
  if (Result.isError(startAuditRes)) return responseForError(args.route, startAuditRes.error);

  const gitRepoStreamForWorkspace = gitRepoStreamForWorkspaceProfile(args.route);
  if (!gitRepoStreamForWorkspace || !isWorkspaceFsProfile(args.route)) return args.route.respond.badRequest("workspace commit requires git-repo backing");
  const gitConfigRes = gitRepoProfileConfigResult(args.route, gitRepoStreamForWorkspace);
  if (Result.isError(gitConfigRes)) return responseForError(args.route, gitConfigRes.error);
  const commitRes = await commitGitBackedWorkspaceResult({
    route: args.route,
    workspace: args.workspace,
    workspaceId: args.workspaceId,
    gitRepoStream: gitRepoStreamForWorkspace,
    gitConfig: gitConfigRes.value,
    ref: args.ref,
    expectedHead: args.body.expectedHead,
    message: args.body.message,
    author: args.body.author,
    durability: args.durability.durability,
    durabilityTimeoutMs: args.durability.timeoutMs,
    pendingCheckout: args.pendingCheckout,
    pendingOps: args.pendingOps,
    precomputedGitObjects: args.precomputedGitObjects,
  });
  if (Result.isError(commitRes)) {
    const auditRes = await appendWorkspaceAuditEventResult(args.route, {
      type: "workspace_commit_failed",
      workspaceId: args.workspaceId,
      ref: args.ref,
      actorId: args.body.author.id,
      status: commitRes.error.status === 409 ? "conflict" : "failed",
      context: { message: commitRes.error.message },
    });
    if (Result.isError(auditRes)) return responseForError(args.route, auditRes.error);
    return responseForError(args.route, commitRes.error);
  }
  const auditRes = await appendWorkspaceAuditEventResult(args.route, {
    type: "workspace_commit_succeeded",
    workspaceId: args.workspaceId,
    ref: args.ref,
    actorId: args.body.author.id,
    status: "succeeded",
    context: {
      oldCommitId: commitRes.value.oldCommitId,
      newCommitId: commitRes.value.newCommitId,
      git: commitRes.value.git,
    },
  });
  if (Result.isError(auditRes)) return responseForError(args.route, auditRes.error);
  return args.route.respond.json(200, commitRes.value);
}

async function commitWorkspace(args: StreamProfileRouteArgs, workspaceId: string): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "commit request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const bodyObjRes = parseJsonObjectBodyResult(bodyRes.value, "commit request");
  if (Result.isError(bodyObjRes)) return responseForError(args, bodyObjRes.error);
  const body = bodyObjRes.value as WorkspaceFsCommitRequest;
  if (typeof body.message !== "string" || body.message.trim() === "") return args.respond.badRequest("message is required");
  if (!body.author || typeof body.author !== "object" || typeof body.author.id !== "string" || body.author.id.trim() === "") {
    return args.respond.badRequest("author.id is required");
  }
  const ref = normalizeRef(body.ref);
  const durabilityRes = normalizeCommitDurabilityResult(body);
  if (Result.isError(durabilityRes)) return responseForError(args, durabilityRes.error);
  const workspaceRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(workspaceRes)) return responseForError(args, workspaceRes.error);
  if (!workspaceRes.value.checkout) return args.respond.notFound("workspace not found");
  if (workspaceRes.value.state.state !== "open") return args.respond.conflict(`workspace is ${workspaceRes.value.state.state}`);
  return commitLoadedWorkspaceResponse({
    route: args,
    workspaceId,
    workspace: workspaceRes.value,
    body,
    ref,
    durability: durabilityRes.value,
  });
}

async function commitWorkspaceOps(args: StreamProfileRouteArgs, workspaceId: string): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "commit ops request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const bodyObjRes = parseJsonObjectBodyResult(bodyRes.value, "commit ops request");
  if (Result.isError(bodyObjRes)) return responseForError(args, bodyObjRes.error);
  const body = bodyObjRes.value as WorkspaceFsCommitRequest & { ops?: unknown };
  if (typeof body.message !== "string" || body.message.trim() === "") return args.respond.badRequest("message is required");
  if (!body.author || typeof body.author !== "object" || typeof body.author.id !== "string" || body.author.id.trim() === "") {
    return args.respond.badRequest("author.id is required");
  }
  const ref = normalizeRef(body.ref);
  const durabilityRes = normalizeCommitDurabilityResult(body);
  if (Result.isError(durabilityRes)) return responseForError(args, durabilityRes.error);

  const workspaceRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(workspaceRes)) return responseForError(args, workspaceRes.error);
  if (!workspaceRes.value.checkout) return args.respond.notFound("workspace not found");
  if (workspaceRes.value.state.state !== "open") return args.respond.conflict(`workspace is ${workspaceRes.value.state.state}`);

  const gitRepoStreamForWorkspace = gitRepoStreamForWorkspaceProfile(args);
  if (!gitRepoStreamForWorkspace || !isWorkspaceFsProfile(args)) return args.respond.badRequest("workspace commit requires git-repo backing");
  const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStreamForWorkspace);
  if (Result.isError(gitConfigRes)) return responseForError(args, gitConfigRes.error);

  const preparedOpsRes = await prepareGitBackedCommitOpsResult({
    route: args,
    rawOps: body.ops,
    gitRepoStream: gitRepoStreamForWorkspace,
    format: gitConfigRes.value.objectFormat,
  });
  if (Result.isError(preparedOpsRes)) return responseForError(args, preparedOpsRes.error);
  if (preparedOpsRes.value.workspaceObjects.length > 0) {
    const writeObjectsRes = await writeObjectsResult(args, preparedOpsRes.value.workspaceObjects);
    if (Result.isError(writeObjectsRes)) return responseForError(args, writeObjectsRes.error);
  }
  const workspace: LoadedWorkspace = {
    ...workspaceRes.value,
    records: [...workspaceRes.value.records, ...preparedOpsRes.value.ops],
    ops: [...workspaceRes.value.ops, ...preparedOpsRes.value.ops],
  };
  return commitLoadedWorkspaceResponse({
    route: args,
    workspaceId,
    workspace,
    body,
    ref,
    durability: durabilityRes.value,
    pendingOps: preparedOpsRes.value.ops,
    precomputedGitObjects: preparedOpsRes.value.gitObjects,
  });
}

async function commitNewWorkspaceOps(args: StreamProfileRouteArgs): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "commit ops request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const bodyObjRes = parseJsonObjectBodyResult(bodyRes.value, "commit ops request");
  if (Result.isError(bodyObjRes)) return responseForError(args, bodyObjRes.error);
  const body = bodyObjRes.value as WorkspaceFsCommitRequest & { ops?: unknown; workspaceId?: unknown };
  if (typeof body.message !== "string" || body.message.trim() === "") return args.respond.badRequest("message is required");
  if (!body.author || typeof body.author !== "object" || typeof body.author.id !== "string" || body.author.id.trim() === "") {
    return args.respond.badRequest("author.id is required");
  }
  const workspaceId = typeof body.workspaceId === "string" && body.workspaceId.trim() !== "" ? body.workspaceId.trim() : newWorkspaceId();
  const existingWorkspaceRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(existingWorkspaceRes)) return responseForError(args, existingWorkspaceRes.error);
  if (existingWorkspaceRes.value.records.length > 0) return args.respond.conflict("workspace already exists");

  const ref = normalizeRef(body.ref);
  const durabilityRes = normalizeCommitDurabilityResult(body);
  if (Result.isError(durabilityRes)) return responseForError(args, durabilityRes.error);
  const gitRepoStreamForWorkspace = gitRepoStreamForWorkspaceProfile(args);
  if (!gitRepoStreamForWorkspace || !isWorkspaceFsProfile(args)) return args.respond.badRequest("workspace commit requires git-repo backing");
  const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStreamForWorkspace);
  if (Result.isError(gitConfigRes)) return responseForError(args, gitConfigRes.error);
  const baseCommitId = body.expectedHead === undefined
    ? await currentGitHeadResult(args, gitRepoStreamForWorkspace, ref)
    : Result.ok(body.expectedHead);
  if (Result.isError(baseCommitId)) return responseForError(args, baseCommitId.error);

  const preparedOpsRes = await prepareGitBackedCommitOpsResult({
    route: args,
    rawOps: body.ops,
    gitRepoStream: gitRepoStreamForWorkspace,
    format: gitConfigRes.value.objectFormat,
  });
  if (Result.isError(preparedOpsRes)) return responseForError(args, preparedOpsRes.error);
  if (preparedOpsRes.value.workspaceObjects.length > 0) {
    const writeObjectsRes = await writeObjectsResult(args, preparedOpsRes.value.workspaceObjects);
    if (Result.isError(writeObjectsRes)) return responseForError(args, writeObjectsRes.error);
  }

  const checkout: WorkspaceCheckoutRecord = {
    kind: "workspace-checkout",
    ref,
    baseCommitId: baseCommitId.value,
    rootTreeId: null,
    createdAt: new Date().toISOString(),
  };
  const records: WorkspaceFsWorkspaceRecord[] = [checkout as WorkspaceFsWorkspaceRecord, ...preparedOpsRes.value.ops];
  const workspace: LoadedWorkspace = {
    records,
    checkout,
    ops: preparedOpsRes.value.ops,
    state: isWorkspaceClosed(records),
  };
  return commitLoadedWorkspaceResponse({
    route: args,
    workspaceId,
    workspace,
    body: { ...body, expectedHead: baseCommitId.value },
    ref,
    durability: durabilityRes.value,
    pendingCheckout: checkout,
    pendingOps: preparedOpsRes.value.ops,
    precomputedGitObjects: preparedOpsRes.value.gitObjects,
  });
}

async function discardWorkspace(args: StreamProfileRouteArgs, workspaceId: string): Promise<Response> {
  const markerAppendRes = await appendJsonResult(args, workspaceStreamName(args.stream, workspaceId), [
    {
      value: { kind: "workspace-discarded", createdAt: new Date().toISOString() },
      routingKey: "workspace",
    },
  ], WORKSPACE_FS_WORKSPACE_TTL_SECONDS);
  if (Result.isError(markerAppendRes)) return responseForError(args, markerAppendRes.error);
  const auditRes = await appendWorkspaceAuditEventResult(args, {
    type: "workspace_discarded",
    workspaceId,
    status: "succeeded",
  });
  if (Result.isError(auditRes)) return responseForError(args, auditRes.error);
  return args.respond.json(200, { workspaceId, state: "discarded" });
}

async function loadGitCommitAsWorkspaceResult(args: {
  route: StreamProfileRouteArgs;
  gitRepoStream: string;
  gitConfig: GitRepoProfileConfig;
  commitOid: string;
}): Promise<Result<WorkspaceFsCommit, WorkspaceFsServerError>> {
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

async function log(args: StreamProfileRouteArgs): Promise<Response> {
  const ref = normalizeRef(args.url.searchParams.get("ref"));
  const limitRaw = Number(args.url.searchParams.get("limit") ?? "20");
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(100, Math.floor(limitRaw))) : 20;
  const gitRepoStream = gitRepoStreamForWorkspaceProfile(args);
  if (!gitRepoStream || !isWorkspaceFsProfile(args)) return args.respond.badRequest("workspace log requires git-repo backing");
  const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStream);
  if (Result.isError(gitConfigRes)) return responseForError(args, gitConfigRes.error);
  const headRes = await currentGitHeadResult(args, gitRepoStream, ref);
  if (Result.isError(headRes)) return responseForError(args, headRes.error);
  const commits: WorkspaceFsCommit[] = [];
  let commitId = headRes.value;
  while (commitId && commits.length < limit) {
    const commitRes = await loadGitCommitAsWorkspaceResult({
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

async function show(args: StreamProfileRouteArgs, commitId: string): Promise<Response> {
  const gitRepoStream = gitRepoStreamForWorkspaceProfile(args);
  if (!gitRepoStream || !isWorkspaceFsProfile(args)) return args.respond.badRequest("workspace show requires git-repo backing");
  const gitConfigRes = gitRepoProfileConfigResult(args, gitRepoStream);
  if (Result.isError(gitConfigRes)) return responseForError(args, gitConfigRes.error);
  const decoded = decodeURIComponent(commitId);
  if (!oidPattern(gitConfigRes.value.objectFormat).test(decoded)) return args.respond.badRequest("invalid git commit id");
  const commitRes = await loadGitCommitAsWorkspaceResult({
    route: args,
    gitRepoStream,
    gitConfig: gitConfigRes.value,
    commitOid: decoded,
  });
  if (Result.isError(commitRes)) return responseForError(args, commitRes.error);
  return args.respond.json(200, { commit: commitRes.value });
}

async function batchStat(args: StreamProfileRouteArgs): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "batch stat request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const body = bodyRes.value as WorkspaceFsBatchStatRequest;
  if (!Array.isArray(body.paths)) return args.respond.badRequest("paths must be an array");
  if (body.paths.length > 1000) return args.respond.badRequest("paths exceeds maximum of 1000");
  const gitRepoStream = gitRepoStreamForWorkspaceProfile(args);
  if (!gitRepoStream || !isWorkspaceFsProfile(args)) return args.respond.badRequest("workspace batch stat requires git-repo backing");
  if (!body.workspaceId && !body.commit) return args.respond.badRequest("batch stat requires workspaceId or commit");
  const gitConfigRes = body.commit ? gitRepoProfileConfigResult(args, gitRepoStream) : null;
  if (gitConfigRes && Result.isError(gitConfigRes)) return responseForError(args, gitConfigRes.error);
  if (body.commit && gitConfigRes && !oidPattern(gitConfigRes.value.objectFormat).test(body.commit)) return args.respond.badRequest("invalid git commit id");
  const stats = [];
  for (const rawPath of body.paths) {
    const pathRes = canonicalizeWorkspaceFsPath(rawPath);
    if (Result.isError(pathRes)) {
      stats.push({ path: rawPath, node: null, error: pathRes.error.message });
      continue;
    }
    if (body.workspaceId) {
      const statRes = await statGitBackedWorkspaceResult(args, pathRes.value, body.workspaceId);
      stats.push({
        path: pathRes.value,
        node: Result.isError(statRes) ? null : statRes.value,
        error: Result.isError(statRes) ? statRes.error.message : undefined,
      });
      continue;
    }
    const statRes = await resolveGitPathForWorkspaceResult({
        route: args,
        gitRepoStream,
        format: gitConfigRes!.value.objectFormat,
        commitOid: body.commit!,
        path: pathRes.value,
    });
    stats.push({
      path: pathRes.value,
      node: Result.isError(statRes) ? null : statRes.value.node,
      error: Result.isError(statRes) ? statRes.error.message : undefined,
    });
  }
  return args.respond.json(200, { stats });
}

async function batchReadMetadata(args: StreamProfileRouteArgs): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "batch metadata request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const body = bodyRes.value as WorkspaceFsBatchReadMetadataRequest;
  if (!Array.isArray(body.ids)) return args.respond.badRequest("ids must be an array");
  const objects = [];
  for (const id of body.ids.slice(0, 1000)) {
    const objectRes = await loadObjectResult(args, id);
    if (Result.isError(objectRes)) return responseForError(args, objectRes.error);
    objects.push({ id, object: objectRes.value });
  }
  return args.respond.json(200, { objects });
}

async function batchReadBlobs(args: StreamProfileRouteArgs): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "batch blob request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const body = bodyRes.value as WorkspaceFsBatchReadBlobsRequest;
  if (!Array.isArray(body.blobIds)) return args.respond.badRequest("blobIds must be an array");
  const blobs = [];
  for (const blobId of body.blobIds.slice(0, 100)) {
    const bytesRes = await readBlobBytesResult(args, blobId);
    if (Result.isError(bytesRes)) blobs.push({ blobId, contentBase64: null, error: bytesRes.error.message });
    else blobs.push({ blobId, contentBase64: toBase64(bytesRes.value) });
  }
  return args.respond.json(200, { blobs });
}

export async function handleWorkspaceFsRoute(args: StreamProfileRouteArgs): Promise<Response> {
  if (args.namespace !== "_workspace") return args.respond.notFound("missing workspace-fs route");
  if (args.profile.kind !== "workspace-fs") return args.respond.notFound("workspace-fs profile not enabled");
  const [first, second, ...rest] = args.segments;
  if (!first) return args.respond.notFound("missing workspace-fs route");

  if (args.req.method === "POST" && first === "checkout") return checkout(args);
  if (args.req.method === "GET" && first === "ref") return refGet(args, [second ?? "", ...rest].filter(Boolean));
  if (args.req.method === "GET" && first === "stat") return stat(args);
  if (args.req.method === "GET" && first === "readdir") return readdir(args);
  if (args.req.method === "GET" && first === "blob" && second) return blob(args, [second, ...rest].join("/"));
  if (args.req.method === "GET" && first === "log") return log(args);
  if (args.req.method === "GET" && first === "show" && second) return show(args, [second, ...rest].join("/"));
  if (args.req.method === "POST" && first === "commit-ops") return commitNewWorkspaceOps(args);

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
    if (args.req.method === "POST" && workspaceRoute === "commit-ops") return commitWorkspaceOps(args, workspaceId);
    if (args.req.method === "POST" && workspaceRoute === "discard") return discardWorkspace(args, workspaceId);
  }

  if (first === "batch") {
    if (args.req.method === "POST" && second === "stat") return batchStat(args);
    if (args.req.method === "POST" && second === "read-metadata") return batchReadMetadata(args);
    if (args.req.method === "POST" && second === "read-blobs") return batchReadBlobs(args);
  }

  return args.respond.notFound("unknown workspace-fs route");
}
