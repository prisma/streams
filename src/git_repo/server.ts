import { Result } from "better-result";
import type { StreamProfileVfsRouteArgs } from "../profiles/profile";
import {
  GIT_REPO_PROFILE_KIND,
  type GitCheckoutResponse,
  type GitImportRequest,
  type GitMaintenancePublishedRecord,
  type GitNodeStat,
  type GitRefTransactionRequest,
  type GitReaddirResponse,
  type GitRepoProfileConfig,
  type GitStatResponse,
  type GitTransactionStatusResponse,
  type GitWriteObjectRequest,
} from "./types";
import { buildGitObject } from "./objects";
import {
  exportGitBundleResult,
  exportGitPackResult,
  gitReceivePackAdvertiseRefsResult,
  gitReceivePackRpcResult,
  gitUploadPackAdvertiseRefsResult,
  gitUploadPackRpcResult,
  importGitRepoResult,
  publishGitPackArtifactsResult,
  readGitPackfileArtifactResult,
  verifyGitReachabilityResult,
} from "./import_export";
import { buildRefs, validateGitRefNameResult, validateRefTransactionRequestResult } from "./refs";
import { buildRefCheckpoint, latestInlineRefCheckpoint } from "./maintenance";
import {
  appendGitRecordResult,
  commitGitRefTransactionResult,
  parseBase64Result,
  readGitRecordSnapshotResult,
  readGitRecordsResult,
  readGitRefsSnapshotResult,
  readLooseGitObjectBodyResult,
  readLooseGitObjectHeaderResult,
  writeGitRefCheckpointArtifactsResult,
  writeLooseGitObjectResult,
  type GitRepoServiceError,
} from "./service";
import { parseGitCommitBodyResult, parseGitTreeBodyResult, type GitTreeEntry } from "./tree";

type GitServerError = GitRepoServiceError;

function responseForError(args: StreamProfileVfsRouteArgs, err: GitServerError): Response {
  if (err.status === 400) return args.respond.badRequest(err.message);
  if (err.status === 404) return args.respond.notFound(err.message);
  if (err.status === 409) return args.respond.conflict(err.message);
  return args.respond.internalError(err.message);
}

function profileConfig(args: StreamProfileVfsRouteArgs): GitRepoProfileConfig {
  return args.profile as GitRepoProfileConfig;
}

async function readRequestJsonResult(req: Request, label: string): Promise<Result<unknown, GitServerError>> {
  try {
    return Result.ok(await req.json());
  } catch {
    return Result.err({ status: 400, message: `${label} must be valid JSON` });
  }
}

function isGitObjectType(value: unknown): value is GitWriteObjectRequest["type"] {
  return value === "blob" || value === "tree" || value === "commit" || value === "tag";
}

function oidPattern(format: GitRepoProfileConfig["objectFormat"]): RegExp {
  return format === "sha256" ? /^[0-9a-f]{64}$/ : /^[0-9a-f]{40}$/;
}

async function writeLooseObject(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "git object request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const body = bodyRes.value as Partial<GitWriteObjectRequest>;
  if (!body || typeof body !== "object" || Array.isArray(body)) return args.respond.badRequest("git object request must be an object");
  if (!isGitObjectType(body.type)) return args.respond.badRequest("type must be blob, tree, commit, or tag");
  const bytesRes = parseBase64Result(body.bodyBase64, "bodyBase64");
  if (Result.isError(bytesRes)) return responseForError(args, bytesRes.error);

  const config = profileConfig(args);
  const object = buildGitObject(body.type, bytesRes.value, config.objectFormat);
  if (body.expectedOid !== undefined && body.expectedOid !== object.oid) {
    return args.respond.conflict("expectedOid does not match encoded Git object");
  }
  const writeRes = await writeLooseGitObjectResult({
    repoStream: args.stream,
    objectStore: args.objectStore,
    format: config.objectFormat,
    object,
  });
  if (Result.isError(writeRes)) return responseForError(args, writeRes.error);
  return args.respond.json(200, writeRes.value);
}

async function importRepo(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "git import request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const importRes = await importGitRepoResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    config: profileConfig(args),
  }, bodyRes.value as GitImportRequest);
  if (Result.isError(importRes)) return responseForError(args, importRes.error);
  return args.respond.json(200, importRes.value);
}

async function exportBundle(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const exportRes = await exportGitBundleResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    config: profileConfig(args),
  });
  if (Result.isError(exportRes)) return responseForError(args, exportRes.error);
  const body = exportRes.value.buffer.slice(exportRes.value.byteOffset, exportRes.value.byteOffset + exportRes.value.byteLength) as ArrayBuffer;
  return new Response(body, {
    status: 200,
    headers: {
      "content-type": "application/x-git-bundle",
      "content-length": String(exportRes.value.byteLength),
      "cache-control": "no-store",
    },
  });
}

async function exportPack(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const exportRes = await exportGitPackResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    config: profileConfig(args),
  });
  if (Result.isError(exportRes)) return responseForError(args, exportRes.error);
  const body = exportRes.value.buffer.slice(exportRes.value.byteOffset, exportRes.value.byteOffset + exportRes.value.byteLength) as ArrayBuffer;
  return new Response(body, {
    status: 200,
    headers: {
      "content-type": "application/x-git-packed-objects",
      "content-length": String(exportRes.value.byteLength),
      "cache-control": "no-store",
    },
  });
}

function parseBodyRangeResult(value: string | null, size: number): Result<{ start: number; end: number } | null, GitServerError> {
  if (!value) return Result.ok(null);
  const raw = value.startsWith("bytes=") ? value.slice("bytes=".length) : value;
  const match = raw.match(/^([0-9]+)-([0-9]*)$/);
  if (!match) return Result.err({ status: 400, message: "invalid range" });
  const start = Number(match[1]);
  const end = match[2] === "" ? size - 1 : Number(match[2]);
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || end < start || start >= size) {
    return Result.err({ status: 400, message: "invalid range" });
  }
  return Result.ok({ start, end: Math.min(end, size - 1) });
}

async function readLooseObject(args: StreamProfileVfsRouteArgs, oid: string): Promise<Response> {
  const config = profileConfig(args);
  const objectId = decodeURIComponent(oid);
  if (!oidPattern(config.objectFormat).test(objectId)) return args.respond.badRequest("invalid git object id");
  const headerRes = await readLooseGitObjectHeaderResult({
    repoStream: args.stream,
    objectStore: args.objectStore,
    format: config.objectFormat,
    oid: objectId,
  });
  if (Result.isError(headerRes)) return responseForError(args, headerRes.error);
  const header = headerRes.value;
  const rangeRes = parseBodyRangeResult(args.url.searchParams.get("range") ?? args.req.headers.get("range"), header.size);
  if (Result.isError(rangeRes)) return responseForError(args, rangeRes.error);
  const bytesRes = await readLooseGitObjectBodyResult({
    repoStream: args.stream,
    objectStore: args.objectStore,
    format: config.objectFormat,
    oid: objectId,
    range: rangeRes.value,
  });
  if (Result.isError(bytesRes)) return responseForError(args, bytesRes.error);

  const headers: Record<string, string> = {
    "content-type": "application/octet-stream",
    "cache-control": "immutable, max-age=31536000",
    "content-length": String(bytesRes.value.body.byteLength),
    "x-git-object-oid": objectId,
    "x-git-object-type": header.type,
    "x-git-object-size": String(header.size),
  };
  if (rangeRes.value) headers["content-range"] = `bytes ${rangeRes.value.start}-${rangeRes.value.end}/${header.size}`;
  const bytes = bytesRes.value.body;
  const body = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
  return new Response(body, { status: rangeRes.value ? 206 : 200, headers });
}

function canonicalGitPathResult(path: string | null): Result<string, GitServerError> {
  const raw = path ?? "/";
  if (raw.includes("\0")) return Result.err({ status: 400, message: "path must not contain null bytes" });
  const parts: string[] = [];
  for (const part of raw.replace(/\\/g, "/").split("/")) {
    if (part === "" || part === ".") continue;
    if (part === "..") {
      parts.pop();
      continue;
    }
    parts.push(part);
  }
  return Result.ok(`/${parts.join("/")}`);
}

function basename(path: string): string {
  if (path === "/") return "";
  const idx = path.lastIndexOf("/");
  return idx < 0 ? path : path.slice(idx + 1);
}

function childPath(dir: string, name: string): string {
  return dir === "/" ? `/${name}` : `${dir}/${name}`;
}

async function commitOidFromRequestResult(args: StreamProfileVfsRouteArgs): Promise<Result<string, GitServerError>> {
  const config = profileConfig(args);
  const explicit = args.url.searchParams.get("commit");
  if (explicit && explicit.trim() !== "") {
    const oid = explicit.trim();
    if (!oidPattern(config.objectFormat).test(oid)) return Result.err({ status: 400, message: "invalid git commit id" });
    return Result.ok(oid);
  }

  const refRes = validateGitRefNameResult(args.url.searchParams.get("ref") ?? config.defaultBranch, { allowHead: false });
  if (Result.isError(refRes)) return Result.err({ status: 400, message: refRes.error.message });
  const ref = refRes.value;
  const refsRes = await readGitRefsSnapshotResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    format: config.objectFormat,
  });
  if (Result.isError(refsRes)) return refsRes;
  const refsByName = refsRes.value.refs;
  const oid = Object.prototype.hasOwnProperty.call(refsByName, ref) ? refsByName[ref] ?? null : null;
  if (!oid) return Result.err({ status: 404, message: "git ref not found" });
  return Result.ok(oid);
}

async function rootTreeOidForCommitResult(
  args: StreamProfileVfsRouteArgs,
  commitOid: string
): Promise<Result<string, GitServerError>> {
  const config = profileConfig(args);
  const commitRes = await readLooseGitObjectBodyResult({
    repoStream: args.stream,
    objectStore: args.objectStore,
    format: config.objectFormat,
    oid: commitOid,
    expectedType: "commit",
  });
  if (Result.isError(commitRes)) return commitRes;
  const parsedRes = parseGitCommitBodyResult(commitRes.value.body, config.objectFormat);
  if (Result.isError(parsedRes)) return Result.err({ status: 500, message: parsedRes.error.message });
  return Result.ok(parsedRes.value.tree);
}

async function loadTreeEntriesResult(
  args: StreamProfileVfsRouteArgs,
  treeOid: string
): Promise<Result<GitTreeEntry[], GitServerError>> {
  const config = profileConfig(args);
  const treeRes = await readLooseGitObjectBodyResult({
    repoStream: args.stream,
    objectStore: args.objectStore,
    format: config.objectFormat,
    oid: treeOid,
    expectedType: "tree",
  });
  if (Result.isError(treeRes)) return treeRes;
  const parsedRes = parseGitTreeBodyResult(treeRes.value.body, config.objectFormat);
  if (Result.isError(parsedRes)) return Result.err({ status: 500, message: parsedRes.error.message });
  return Result.ok(parsedRes.value);
}

async function statForEntryResult(
  args: StreamProfileVfsRouteArgs,
  path: string,
  entry: GitTreeEntry
): Promise<Result<GitNodeStat, GitServerError>> {
  if (entry.type === "dir") {
    return Result.ok({ path, type: "dir", mode: entry.mode, oid: entry.oid, size: 0 });
  }
  const headerRes = await readLooseGitObjectHeaderResult({
    repoStream: args.stream,
    objectStore: args.objectStore,
    format: profileConfig(args).objectFormat,
    oid: entry.oid,
  });
  if (Result.isError(headerRes)) return headerRes;
  if (headerRes.value.type !== "blob") return Result.err({ status: 500, message: "git tree file entry does not point at a blob" });
  return Result.ok({ path, type: entry.type, mode: entry.mode, oid: entry.oid, size: headerRes.value.size });
}

type ResolvedGitPath = {
  commitOid: string;
  rootTreeOid: string;
  node: GitNodeStat;
};

async function resolveGitPathResult(
  args: StreamProfileVfsRouteArgs,
  commitOid: string,
  path: string
): Promise<Result<ResolvedGitPath, GitServerError>> {
  const rootTreeRes = await rootTreeOidForCommitResult(args, commitOid);
  if (Result.isError(rootTreeRes)) return rootTreeRes;
  if (path === "/") {
    return Result.ok({
      commitOid,
      rootTreeOid: rootTreeRes.value,
      node: { path: "/", type: "dir", mode: "40000", oid: rootTreeRes.value, size: 0 },
    });
  }

  let treeOid = rootTreeRes.value;
  let currentPath = "/";
  const parts = path.split("/").filter(Boolean);
  for (let i = 0; i < parts.length; i++) {
    const entriesRes = await loadTreeEntriesResult(args, treeOid);
    if (Result.isError(entriesRes)) return entriesRes;
    const entry = entriesRes.value.find((candidate) => candidate.name === parts[i]);
    if (!entry) return Result.err({ status: 404, message: "path not found" });
    currentPath = childPath(currentPath, entry.name);
    if (i === parts.length - 1) {
      const statRes = await statForEntryResult(args, currentPath, entry);
      if (Result.isError(statRes)) return statRes;
      return Result.ok({ commitOid, rootTreeOid: rootTreeRes.value, node: statRes.value });
    }
    if (entry.type !== "dir") return Result.err({ status: 404, message: "path not found" });
    treeOid = entry.oid;
  }
  return Result.err({ status: 404, message: "path not found" });
}

async function checkout(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const config = profileConfig(args);
  const refRes = validateGitRefNameResult(args.url.searchParams.get("ref") ?? config.defaultBranch, { allowHead: false });
  if (Result.isError(refRes)) return args.respond.badRequest(refRes.error.message);
  const ref = refRes.value;
  const refsRes = await readGitRefsSnapshotResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    format: config.objectFormat,
  });
  if (Result.isError(refsRes)) return responseForError(args, refsRes.error);
  const commitOid = Object.prototype.hasOwnProperty.call(refsRes.value.refs, ref) ? refsRes.value.refs[ref] ?? null : null;
  let rootTreeOid: string | null = null;
  if (commitOid) {
    const rootRes = await rootTreeOidForCommitResult(args, commitOid);
    if (Result.isError(rootRes)) return responseForError(args, rootRes.error);
    rootTreeOid = rootRes.value;
  }
  return args.respond.json(200, {
    repo: args.stream,
    ref,
    commitOid,
    rootTreeOid,
  } satisfies GitCheckoutResponse);
}

async function stat(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const pathRes = canonicalGitPathResult(args.url.searchParams.get("path"));
  if (Result.isError(pathRes)) return responseForError(args, pathRes.error);
  const commitRes = await commitOidFromRequestResult(args);
  if (Result.isError(commitRes)) return responseForError(args, commitRes.error);
  const resolvedRes = await resolveGitPathResult(args, commitRes.value, pathRes.value);
  if (Result.isError(resolvedRes)) return responseForError(args, resolvedRes.error);
  return args.respond.json(200, {
    commitOid: resolvedRes.value.commitOid,
    node: resolvedRes.value.node,
  } satisfies GitStatResponse);
}

async function readdir(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const pathRes = canonicalGitPathResult(args.url.searchParams.get("path"));
  if (Result.isError(pathRes)) return responseForError(args, pathRes.error);
  const rawLimit = Number(args.url.searchParams.get("limit") ?? "500");
  const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(1000, Math.floor(rawLimit))) : 500;
  const commitRes = await commitOidFromRequestResult(args);
  if (Result.isError(commitRes)) return responseForError(args, commitRes.error);
  const resolvedRes = await resolveGitPathResult(args, commitRes.value, pathRes.value);
  if (Result.isError(resolvedRes)) return responseForError(args, resolvedRes.error);
  if (resolvedRes.value.node.type !== "dir") return args.respond.badRequest("path is not a directory");
  const entriesRes = await loadTreeEntriesResult(args, resolvedRes.value.node.oid);
  if (Result.isError(entriesRes)) return responseForError(args, entriesRes.error);
  const sorted = entriesRes.value.slice().sort((a, b) => a.name.localeCompare(b.name));
  const cursor = args.url.searchParams.get("cursor");
  const start = cursor ? sorted.findIndex((entry) => entry.name > cursor) : 0;
  const safeStart = start < 0 ? sorted.length : start;
  const page = sorted.slice(safeStart, safeStart + limit);
  const stats: GitNodeStat[] = [];
  for (const entry of page) {
    const statRes = await statForEntryResult(args, childPath(pathRes.value, entry.name), entry);
    if (Result.isError(statRes)) return responseForError(args, statRes.error);
    stats.push(statRes.value);
  }
  const nextCursor = safeStart + limit < sorted.length ? sorted[safeStart + limit - 1]!.name : null;
  return args.respond.json(200, {
    commitOid: commitRes.value,
    path: pathRes.value,
    entries: stats,
    nextCursor,
  } satisfies GitReaddirResponse);
}

async function blobByPath(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const pathRes = canonicalGitPathResult(args.url.searchParams.get("path"));
  if (Result.isError(pathRes)) return responseForError(args, pathRes.error);
  const commitRes = await commitOidFromRequestResult(args);
  if (Result.isError(commitRes)) return responseForError(args, commitRes.error);
  const resolvedRes = await resolveGitPathResult(args, commitRes.value, pathRes.value);
  if (Result.isError(resolvedRes)) return responseForError(args, resolvedRes.error);
  if (resolvedRes.value.node.type === "dir") return args.respond.badRequest("path is a directory");
  const rangeRes = parseBodyRangeResult(args.url.searchParams.get("range") ?? args.req.headers.get("range"), resolvedRes.value.node.size);
  if (Result.isError(rangeRes)) return responseForError(args, rangeRes.error);
  const bytesRes = await readLooseGitObjectBodyResult({
    repoStream: args.stream,
    objectStore: args.objectStore,
    format: profileConfig(args).objectFormat,
    oid: resolvedRes.value.node.oid,
    expectedType: "blob",
    range: rangeRes.value,
  });
  if (Result.isError(bytesRes)) return responseForError(args, bytesRes.error);
  const headers: Record<string, string> = {
    "content-type": "application/octet-stream",
    "cache-control": "immutable, max-age=31536000",
    "content-length": String(bytesRes.value.body.byteLength),
    "x-git-commit-oid": commitRes.value,
    "x-git-blob-oid": resolvedRes.value.node.oid,
  };
  if (rangeRes.value) headers["content-range"] = `bytes ${rangeRes.value.start}-${rangeRes.value.end}/${resolvedRes.value.node.size}`;
  const body = bytesRes.value.body.buffer.slice(bytesRes.value.body.byteOffset, bytesRes.value.body.byteOffset + bytesRes.value.body.byteLength) as ArrayBuffer;
  return new Response(body, { status: rangeRes.value ? 206 : 200, headers });
}

function latestUploadedThrough(args: StreamProfileVfsRouteArgs): bigint {
  const row = args.db.getStream(args.stream);
  return row?.uploaded_through ?? args.streamRow.uploaded_through;
}

async function transactionStatusBodyResult(
  args: StreamProfileVfsRouteArgs,
  txnId: string
): Promise<Result<GitTransactionStatusResponse, GitServerError>> {
  const snapshotRes = await readGitRecordSnapshotResult(args);
  if (Result.isError(snapshotRes)) return snapshotRes;
  const entry = snapshotRes.value.entries.find(
    (candidate) => candidate.record.type === "ref-transaction-committed" && candidate.record.txnId === txnId
  );
  if (!entry || entry.record.type !== "ref-transaction-committed") return Result.err({ status: 404, message: "git transaction not found" });
  const publishedThrough = latestUploadedThrough(args);
  return Result.ok({
    txnId,
    status: entry.offset <= publishedThrough ? "published" : "accepted",
    transaction: entry.record,
    offset: entry.offset.toString(),
    publishedThrough: publishedThrough.toString(),
  });
}

async function transactionStatus(args: StreamProfileVfsRouteArgs, txnSegments: string[]): Promise<Response> {
  const txnId = decodeURIComponent(txnSegments.join("/"));
  if (txnId.trim() === "") return args.respond.badRequest("transaction id is required");
  const statusRes = await transactionStatusBodyResult(args, txnId);
  if (Result.isError(statusRes)) return responseForError(args, statusRes.error);
  return args.respond.json(200, statusRes.value);
}

async function waitPublished(args: StreamProfileVfsRouteArgs, txnSegments: string[]): Promise<Response> {
  const txnId = decodeURIComponent(txnSegments.join("/"));
  if (txnId.trim() === "") return args.respond.badRequest("transaction id is required");
  const rawTimeout = Number(args.url.searchParams.get("timeout_ms") ?? "0");
  const timeoutMs = Number.isFinite(rawTimeout) ? Math.max(0, Math.min(30_000, Math.floor(rawTimeout))) : 0;
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const statusRes = await transactionStatusBodyResult(args, txnId);
    if (Result.isError(statusRes)) return responseForError(args, statusRes.error);
    if (statusRes.value.status === "published") return args.respond.json(200, statusRes.value);
    if (Date.now() >= deadline) return args.respond.json(202, statusRes.value);
    await new Promise((resolve) => setTimeout(resolve, Math.min(50, deadline - Date.now())));
  }
}

async function status(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const refsRes = await readGitRefsSnapshotResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    format: profileConfig(args).objectFormat,
  });
  if (Result.isError(refsRes)) return responseForError(args, refsRes.error);
  return args.respond.json(200, {
    repo: args.stream,
    profile: profileConfig(args),
    refs: refsRes.value.refs,
  });
}

async function refs(args: StreamProfileVfsRouteArgs): Promise<Response> {
  if (args.url.searchParams.get("publishedOnly") === "true") {
    const snapshotRes = await readGitRecordSnapshotResult(args);
    if (Result.isError(snapshotRes)) return responseForError(args, snapshotRes.error);
    const uploadedThrough = latestUploadedThrough(args);
    const records = snapshotRes.value.entries
      .filter((entry) => entry.offset <= uploadedThrough)
      .map((entry) => entry.record);
    return args.respond.json(200, {
      refs: buildRefs(records),
      checkpoint: latestInlineRefCheckpoint(records),
    });
  }
  const refsRes = await readGitRefsSnapshotResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    format: profileConfig(args).objectFormat,
  });
  if (Result.isError(refsRes)) return responseForError(args, refsRes.error);
  return args.respond.json(200, {
    refs: refsRes.value.refs,
    checkpoint: refsRes.value.checkpoint,
  });
}

async function refGet(args: StreamProfileVfsRouteArgs, refSegments: string[]): Promise<Response> {
  const refRes = validateGitRefNameResult(decodeURIComponent(refSegments.join("/")), { allowHead: false });
  if (Result.isError(refRes)) return args.respond.badRequest(refRes.error.message);
  const ref = refRes.value;
  const refsRes = await readGitRefsSnapshotResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    format: profileConfig(args).objectFormat,
  });
  if (Result.isError(refsRes)) return responseForError(args, refsRes.error);
  return args.respond.json(200, {
    ref,
    oid: Object.prototype.hasOwnProperty.call(refsRes.value.refs, ref) ? refsRes.value.refs[ref] ?? null : null,
  });
}

async function refTransaction(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const bodyRes = await readRequestJsonResult(args.req, "ref transaction request");
  if (Result.isError(bodyRes)) return responseForError(args, bodyRes.error);
  const requestRes = validateRefTransactionRequestResult(bodyRes.value as GitRefTransactionRequest);
  if (Result.isError(requestRes)) return args.respond.badRequest(requestRes.error.message);
  const request = requestRes.value;
  const commitRes = await commitGitRefTransactionResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    format: profileConfig(args).objectFormat,
    request,
  });
  if (Result.isError(commitRes)) return responseForError(args, commitRes.error);
  return args.respond.json(200, commitRes.value);
}

async function publishRefCheckpoint(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const snapshotRes = await readGitRecordSnapshotResult(args);
  if (Result.isError(snapshotRes)) return responseForError(args, snapshotRes.error);
  const checkpoint = buildRefCheckpoint({
    repoId: args.stream,
    records: snapshotRes.value.records,
    streamOffset: Number(snapshotRes.value.nextOffset - 1n),
    generation: snapshotRes.value.records.filter((record) => record.type === "maintenance-published").length + 1,
    defaultBranch: profileConfig(args).defaultBranch,
  });
  const artifactRes = await writeGitRefCheckpointArtifactsResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    format: profileConfig(args).objectFormat,
    checkpoint,
  });
  if (Result.isError(artifactRes)) return responseForError(args, artifactRes.error);
  const record: GitMaintenancePublishedRecord = {
    type: "maintenance-published",
    repoId: args.stream,
    createdAt: checkpoint.createdAt,
    refCheckpointUri: artifactRes.value.refCheckpointUri,
    refCheckpoint: checkpoint,
  };
  const appendRes = await appendGitRecordResult(args, record, "git-maintenance:ref-checkpoint");
  if (Result.isError(appendRes)) return responseForError(args, appendRes.error);
  return args.respond.json(200, { checkpoint, record });
}

async function publishPack(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const packRes = await publishGitPackArtifactsResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    config: profileConfig(args),
  });
  if (Result.isError(packRes)) return responseForError(args, packRes.error);
  return args.respond.json(200, packRes.value);
}

async function verifyReachability(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const verifyRes = await verifyGitReachabilityResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    config: profileConfig(args),
  });
  if (Result.isError(verifyRes)) return responseForError(args, verifyRes.error);
  return args.respond.json(200, verifyRes.value);
}

async function readPackfile(args: StreamProfileVfsRouteArgs, packHash: string): Promise<Response> {
  const packRes = await readGitPackfileArtifactResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    config: profileConfig(args),
  }, decodeURIComponent(packHash).replace(/\.pack$/, ""));
  if (Result.isError(packRes)) return responseForError(args, packRes.error);
  const body = packRes.value.bytes.buffer.slice(packRes.value.bytes.byteOffset, packRes.value.bytes.byteOffset + packRes.value.bytes.byteLength) as ArrayBuffer;
  return new Response(body, {
    status: 200,
    headers: {
      "content-type": "application/x-git-packed-objects",
      "content-length": String(packRes.value.bytes.byteLength),
      "cache-control": "immutable, max-age=31536000",
      "x-git-pack-hash": packRes.value.pack.packHash,
    },
  });
}

function binaryResponse(value: { contentType: string; body: Uint8Array }): Response {
  const body = value.body.buffer.slice(value.body.byteOffset, value.body.byteOffset + value.body.byteLength) as ArrayBuffer;
  return new Response(body, {
    status: 200,
    headers: {
      "content-type": value.contentType,
      "content-length": String(value.body.byteLength),
      "cache-control": "no-cache",
    },
  });
}

async function smartInfoRefs(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const service = args.url.searchParams.get("service");
  const requestArgs = {
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    config: profileConfig(args),
    gitProtocol: args.req.headers.get("git-protocol"),
    requestUrl: args.url,
  };
  const refsRes = service === "git-upload-pack"
    ? await gitUploadPackAdvertiseRefsResult(requestArgs)
    : service === "git-receive-pack"
      ? await gitReceivePackAdvertiseRefsResult(requestArgs)
      : Result.err({ status: 400 as const, message: "service must be git-upload-pack or git-receive-pack" });
  if (Result.isError(refsRes)) return responseForError(args, refsRes.error);
  return binaryResponse(refsRes.value);
}

async function uploadPackRpc(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const requestBody = new Uint8Array(await args.req.arrayBuffer());
  const rpcRes = await gitUploadPackRpcResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    config: profileConfig(args),
    gitProtocol: args.req.headers.get("git-protocol"),
    requestUrl: args.url,
  }, requestBody);
  if (Result.isError(rpcRes)) return responseForError(args, rpcRes.error);
  return binaryResponse(rpcRes.value);
}

async function receivePackRpc(args: StreamProfileVfsRouteArgs): Promise<Response> {
  const requestBody = new Uint8Array(await args.req.arrayBuffer());
  const rpcRes = await gitReceivePackRpcResult({
    stream: args.stream,
    reader: args.reader,
    objectStore: args.objectStore,
    appendJsonRecords: args.appendJsonRecords,
    config: profileConfig(args),
    gitProtocol: args.req.headers.get("git-protocol"),
  }, requestBody);
  if (Result.isError(rpcRes)) return responseForError(args, rpcRes.error);
  return binaryResponse(rpcRes.value);
}

export async function handleGitRepoRoute(args: StreamProfileVfsRouteArgs): Promise<Response> {
  if (args.namespace !== "_git") return args.respond.notFound("missing git route");
  if (args.profile.kind !== GIT_REPO_PROFILE_KIND) return args.respond.notFound("git-repo profile not enabled");
  const [first, second, third, ...rest] = args.segments;
  if (!first) return args.respond.notFound("missing git route");

  if (args.req.method === "GET" && first === "status") return status(args);
  if (args.req.method === "GET" && first === "checkout") return checkout(args);
  if (args.req.method === "GET" && first === "refs") return refs(args);
  if (args.req.method === "GET" && first === "ref") return refGet(args, [second ?? "", third ?? "", ...rest].filter(Boolean));
  if (args.req.method === "GET" && first === "stat") return stat(args);
  if (args.req.method === "GET" && first === "readdir") return readdir(args);
  if (args.req.method === "GET" && first === "blob") return blobByPath(args);
  if (args.req.method === "POST" && first === "import") return importRepo(args);
  if (args.req.method === "GET" && first === "export.bundle") return exportBundle(args);
  if (args.req.method === "GET" && first === "export.pack") return exportPack(args);
  if (args.req.method === "GET" && first === "packfile" && second) return readPackfile(args, second);
  if (args.req.method === "POST" && first === "objects") return writeLooseObject(args);
  if (args.req.method === "GET" && first === "object" && second) return readLooseObject(args, second);
  if (args.req.method === "POST" && first === "transactions" && second === "ref") return refTransaction(args);
  if (first === "transactions" && second && second !== "ref") {
    const txnSegments = [second, third ?? "", ...rest].filter(Boolean);
    if (args.req.method === "GET") return transactionStatus(args, txnSegments);
    if (args.req.method === "POST" && txnSegments[txnSegments.length - 1] === "wait-published") {
      return waitPublished(args, txnSegments.slice(0, -1));
    }
  }
  if (args.req.method === "POST" && first === "maintenance" && second === "publish-ref-checkpoint") return publishRefCheckpoint(args);
  if (args.req.method === "POST" && first === "maintenance" && second === "publish-pack") return publishPack(args);
  if (args.req.method === "POST" && first === "maintenance" && second === "verify-reachability") return verifyReachability(args);
  if (args.req.method === "GET" && first === "smart" && second === "info" && third === "refs") return smartInfoRefs(args);
  if (args.req.method === "POST" && first === "smart" && second === "git-upload-pack") return uploadPackRpc(args);
  if (args.req.method === "POST" && first === "smart" && second === "git-receive-pack") return receivePackRpc(args);

  return args.respond.notFound("unknown git-repo route");
}
