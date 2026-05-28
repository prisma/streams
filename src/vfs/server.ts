import { Result } from "better-result";
import type { StreamProfileVfsRouteArgs } from "../profiles/profile";
import type {
  VfsBatchReadBlobsRequest,
  VfsBatchReadMetadataRequest,
  VfsBatchStatRequest,
  VfsBlobManifest,
  VfsChunkObject,
  VfsCommit,
  VfsCommitRequest,
  VfsNodeStat,
  VfsRefUpdate,
  VfsStoredObject,
  VfsTreeEntry,
  VfsTreePage,
  VfsWorkspaceOp,
  VfsWorkspaceOpInput,
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

type VfsServerError = {
  status: 400 | 404 | 409 | 500;
  message: string;
};

type WorkspaceCheckoutRecord = Extract<VfsWorkspaceRecord, { kind: "workspace-checkout" }>;

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

type PreparedWorkspaceOp = {
  op: VfsWorkspaceOp;
  objects: VfsStoredObject[];
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
  for (const record of records) {
    if (record.kind === "workspace-checkout") return record;
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

  const refRes = await latestRefResult(args, ref);
  if (Result.isError(refRes)) return responseForError(args, refRes.error);
  const baseCommitId = refRes.value?.newCommitId ?? null;
  let rootTreeId: string | null = null;
  if (baseCommitId) {
    const commitRes = await loadRequiredObjectResult<VfsCommit>(args, baseCommitId, "commit");
    if (Result.isError(commitRes)) return responseForError(args, commitRes.error);
    rootTreeId = commitRes.value.rootTreeId;
  }

  const workspaceRes = await loadWorkspaceResult(args, workspaceId);
  if (Result.isError(workspaceRes)) return responseForError(args, workspaceRes.error);
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
  }

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
  const bytesRes = await readBlobBytesResult(args, decodeURIComponent(blobId));
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

  return withRepoLock(args.stream, async () => {
    const workspaceRes = await loadWorkspaceResult(args, workspaceId);
    if (Result.isError(workspaceRes)) return responseForError(args, workspaceRes.error);
    if (!workspaceRes.value.checkout) return args.respond.notFound("workspace not found");
    if (workspaceRes.value.state.state !== "open") return args.respond.conflict(`workspace is ${workspaceRes.value.state.state}`);

    const refRes = await latestRefResult(args, ref);
    if (Result.isError(refRes)) return responseForError(args, refRes.error);
    const currentHead = refRes.value?.newCommitId ?? null;
    const expectedHead = body.expectedHead === undefined ? workspaceRes.value.checkout.baseCommitId : body.expectedHead;
    if (currentHead !== expectedHead) return args.respond.conflict("ref head changed");

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
        value: { kind: "workspace-committed", commitId: commit.id, createdAt: commit.createdAt },
        routingKey: "workspace",
      },
    ], VFS_WORKSPACE_TTL_SECONDS);
    if (Result.isError(markerAppendRes)) return responseForError(args, markerAppendRes.error);

    return args.respond.json(200, {
      ref,
      oldCommitId: currentHead,
      newCommitId: commit.id,
      commit,
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
  return args.respond.json(200, { workspaceId, state: "discarded" });
}

async function log(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const ref = normalizeRef(args.url.searchParams.get("ref"));
  const limitRaw = Number(args.url.searchParams.get("limit") ?? "20");
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(100, Math.floor(limitRaw))) : 20;
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
  const commitRes = await loadRequiredObjectResult<VfsCommit>(args, decodeURIComponent(commitId), "commit");
  if (Result.isError(commitRes)) return responseForError(args, commitRes.error);
  return args.respond.json(200, { commit: commitRes.value });
}

async function batchStat(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "batch stat request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const body = bodyRes.value as VfsBatchStatRequest;
  if (!Array.isArray(body.paths)) return args.respond.badRequest("paths must be an array");
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

export async function handleVfsRepoRoute(args: StreamProfileVfsRouteArgs): Promise<Response> {
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
    if (args.req.method === "POST" && workspaceRoute === "commit") return commitWorkspace(args, workspaceId);
    if (args.req.method === "POST" && workspaceRoute === "discard") return discardWorkspace(args, workspaceId);
  }

  if (first === "batch") {
    if (args.req.method === "POST" && second === "stat") return batchStat(args);
    if (args.req.method === "POST" && second === "read-metadata") return batchReadMetadata(args);
    if (args.req.method === "POST" && second === "read-blobs") return batchReadBlobs(args);
  }

  return args.respond.notFound("unknown VFS route");
}
