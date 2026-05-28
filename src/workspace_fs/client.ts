import { Buffer } from "node:buffer";
import type {
  VfsBatchReadBlobsResponse,
  VfsBatchReadMetadataResponse,
  VfsBatchStatResponse,
  VfsCheckoutRequest,
  VfsCheckoutResponse,
  VfsCommit,
  VfsCommitRequest,
  VfsCommitResponse,
  VfsLogResponse,
  VfsNodeStat,
  VfsReaddirResponse,
  VfsRefName,
  VfsRefResponse,
  VfsShowResponse,
  VfsStoredObject,
  VfsWorkspaceOpInput,
  VfsWorkspaceChangesResponse,
  VfsWorkspaceConflictsResponse,
  VfsWorkspaceIndexResponse,
  VfsWorkspaceOpsResponse,
  VfsWorkspaceRebaseResponse,
  VfsWorkspaceStatusResponse,
} from "./types";
import { bytesFromContent, canonicalizeVfsPath, fromBase64, normalizeRef, toBase64 } from "./model";
import { Result } from "better-result";

export type WorkspaceFsFetch = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

export type OpenWorkspaceFsRepoOptions = {
  streamsUrl: string;
  stream?: string;
  tenantId?: string;
  repoId?: string;
  authToken?: string;
  fetch?: WorkspaceFsFetch;
};

export type WorkspaceFsEnsureOptions = {
  gitRepoStream: string;
  auditStream?: string;
};

export type WorkspaceCheckoutOptions = Omit<VfsCheckoutRequest, "ref"> & {
  ref?: VfsRefName;
};

export type WorkspaceCommitOptions = Omit<VfsCommitRequest, "expectedHead"> & {
  expectedHead?: string | null;
};

export class WorkspaceFsClientError extends Error {
  readonly status: number;
  readonly body: unknown;

  constructor(message: string, status: number, body: unknown = null) {
    super(message);
    this.name = "WorkspaceFsClientError";
    this.status = status;
    this.body = body;
  }
}

export function workspaceFsStreamName(tenantId: string, repoId: string): string {
  return `workspace/${tenantId}/${repoId}/control`;
}

function joinUrl(base: string, path: string): string {
  return `${base.replace(/\/+$/, "")}${path}`;
}

function encodeStreamPath(stream: string): string {
  return `/v1/stream/${encodeURIComponent(stream)}`;
}

function normalizeBaseUrl(url: string): string {
  return url.replace(/\/+$/, "");
}

function decodeJsonMaybe(text: string): unknown {
  if (text === "") return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function errorMessage(body: unknown, fallback: string): string {
  if (body && typeof body === "object") {
    const err = (body as { error?: { message?: unknown } }).error;
    if (err && typeof err.message === "string") return err.message;
  }
  return fallback;
}

export class WorkspaceFsClient {
  readonly streamsUrl: string;
  readonly stream: string;
  private readonly fetchImpl: WorkspaceFsFetch;
  private readonly authToken?: string;

  constructor(options: OpenWorkspaceFsRepoOptions) {
    const stream = options.stream ?? (options.tenantId && options.repoId ? workspaceFsStreamName(options.tenantId, options.repoId) : null);
    if (!stream) {
      throw new WorkspaceFsClientError("stream or tenantId/repoId is required", 0);
    }
    this.streamsUrl = normalizeBaseUrl(options.streamsUrl);
    this.stream = stream;
    this.fetchImpl = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.authToken = options.authToken;
  }

  private headers(extra: HeadersInit = {}): Headers {
    const headers = new Headers(extra);
    if (this.authToken) headers.set("authorization", `Bearer ${this.authToken}`);
    return headers;
  }

  private url(path: string, params?: Record<string, string | number | boolean | null | undefined>): string {
    const url = new URL(joinUrl(this.streamsUrl, `${encodeStreamPath(this.stream)}${path}`));
    for (const [key, value] of Object.entries(params ?? {})) {
      if (value !== undefined && value !== null) url.searchParams.set(key, String(value));
    }
    return url.toString();
  }

  private async requestJson<T>(path: string, init: RequestInit = {}, params?: Record<string, string | number | boolean | null | undefined>): Promise<T> {
    const headers = this.headers(init.headers);
    const res = await this.fetchImpl(this.url(path, params), { ...init, headers });
    const text = await res.text();
    const body = decodeJsonMaybe(text);
    if (!res.ok) throw new WorkspaceFsClientError(errorMessage(body, `workspace-fs request failed with ${res.status}`), res.status, body);
    return body as T;
  }

  private async requestBytes(path: string, init: RequestInit = {}, params?: Record<string, string | number | boolean | null | undefined>): Promise<Uint8Array> {
    const headers = this.headers(init.headers);
    const res = await this.fetchImpl(this.url(path, params), { ...init, headers });
    if (!res.ok) {
      const text = await res.text();
      const body = decodeJsonMaybe(text);
      throw new WorkspaceFsClientError(errorMessage(body, `workspace-fs request failed with ${res.status}`), res.status, body);
    }
    return new Uint8Array(await res.arrayBuffer());
  }

  async ensure(options: WorkspaceFsEnsureOptions): Promise<void> {
    const res = await this.fetchImpl(joinUrl(this.streamsUrl, encodeStreamPath(this.stream)), {
      method: "PUT",
      headers: this.headers({ "content-type": "application/json" }),
    });
    if (!res.ok) {
      const text = await res.text();
      const body = decodeJsonMaybe(text);
      throw new WorkspaceFsClientError(errorMessage(body, `workspace-fs stream create failed with ${res.status}`), res.status, body);
    }
    await this.requestJson("/_profile", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        apiVersion: "durable.streams/profile/v1",
        profile: {
          kind: "workspace-fs",
          version: 1,
          gitRepo: { stream: options.gitRepoStream },
          audit: options.auditStream ? { stream: options.auditStream } : undefined,
        },
      }),
    });
  }

  async checkout(options: WorkspaceCheckoutOptions = {}): Promise<WorkspaceFsWorkspace> {
    const ref = normalizeRef(options.ref);
    const res = await this.requestJson<VfsCheckoutResponse>("/_vfs/checkout", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ...options, ref }),
    });
    return new WorkspaceFsWorkspace(this, res);
  }

  async getRef(ref: string): Promise<VfsRefResponse> {
    return this.requestJson<VfsRefResponse>(`/_vfs/ref/${encodeURIComponent(normalizeRef(ref))}`);
  }

  async stat(path: string, opts: { commit?: string | null; workspaceId?: string | null } = {}): Promise<VfsNodeStat> {
    const res = await this.requestJson<{ node: VfsNodeStat }>("/_vfs/stat", { method: "GET" }, {
      path,
      commit: opts.commit ?? undefined,
      workspaceId: opts.workspaceId ?? undefined,
    });
    return res.node;
  }

  async maybeStat(path: string, opts: { commit?: string | null; workspaceId?: string | null } = {}): Promise<VfsNodeStat | null> {
    try {
      return await this.stat(path, opts);
    } catch (error) {
      if (error instanceof WorkspaceFsClientError && error.status === 404) return null;
      throw error;
    }
  }

  async readdir(path: string, opts: { commit?: string | null; workspaceId?: string | null; cursor?: string | null; limit?: number } = {}): Promise<VfsReaddirResponse> {
    return this.requestJson<VfsReaddirResponse>("/_vfs/readdir", { method: "GET" }, {
      path,
      commit: opts.commit ?? undefined,
      workspaceId: opts.workspaceId ?? undefined,
      cursor: opts.cursor ?? undefined,
      limit: opts.limit,
    });
  }

  async readBlob(blobId: string, opts: { range?: string; workspaceId?: string; path?: string } = {}): Promise<Uint8Array> {
    return this.requestBytes(`/_vfs/blob/${encodeURIComponent(blobId)}`, { method: "GET" }, {
      range: opts.range,
      workspaceId: opts.workspaceId,
      path: opts.path,
    });
  }

  async log(ref: string = "main", limit = 20): Promise<VfsCommit[]> {
    const res = await this.requestJson<VfsLogResponse>("/_vfs/log", { method: "GET" }, { ref: normalizeRef(ref), limit });
    return res.commits;
  }

  async show(commitId: string): Promise<VfsCommit> {
    const res = await this.requestJson<VfsShowResponse>(`/_vfs/show/${encodeURIComponent(commitId)}`, { method: "GET" });
    return res.commit;
  }

  async batchStat(paths: string[], opts: { commit?: string | null; workspaceId?: string | null } = {}): Promise<VfsBatchStatResponse> {
    return this.requestJson<VfsBatchStatResponse>("/_vfs/batch/stat", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ paths, commit: opts.commit ?? null, workspaceId: opts.workspaceId ?? undefined }),
    });
  }

  async batchReadMetadata(ids: string[]): Promise<Array<{ id: string; object: VfsStoredObject | null }>> {
    const res = await this.requestJson<VfsBatchReadMetadataResponse>("/_vfs/batch/read-metadata", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ids }),
    });
    return res.objects;
  }

  async batchReadBlobs(blobIds: string[]): Promise<VfsBatchReadBlobsResponse["blobs"]> {
    const res = await this.requestJson<VfsBatchReadBlobsResponse>("/_vfs/batch/read-blobs", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ blobIds }),
    });
    return res.blobs;
  }

  async appendWorkspaceOps(workspaceId: string, ops: VfsWorkspaceOpInput[]): Promise<VfsWorkspaceOpsResponse> {
    return this.requestJson<VfsWorkspaceOpsResponse>(`/_vfs/workspace/${encodeURIComponent(workspaceId)}/ops`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ ops }),
    });
  }

  async workspaceStatus(workspaceId: string): Promise<VfsWorkspaceStatusResponse> {
    return this.requestJson<VfsWorkspaceStatusResponse>(`/_vfs/workspace/${encodeURIComponent(workspaceId)}/status`);
  }

  async workspaceIndex(workspaceId: string, opts: { path?: string | null } = {}): Promise<VfsWorkspaceIndexResponse> {
    return this.requestJson<VfsWorkspaceIndexResponse>(`/_vfs/workspace/${encodeURIComponent(workspaceId)}/index`, { method: "GET" }, {
      path: opts.path ?? undefined,
    });
  }

  async workspaceChanges(workspaceId: string, opts: { prefix?: string | null } = {}): Promise<VfsWorkspaceChangesResponse> {
    return this.requestJson<VfsWorkspaceChangesResponse>(`/_vfs/workspace/${encodeURIComponent(workspaceId)}/changes`, { method: "GET" }, {
      prefix: opts.prefix ?? undefined,
    });
  }

  async workspaceConflicts(workspaceId: string): Promise<VfsWorkspaceConflictsResponse> {
    return this.requestJson<VfsWorkspaceConflictsResponse>(`/_vfs/workspace/${encodeURIComponent(workspaceId)}/conflicts`, { method: "GET" });
  }

  async rebaseWorkspace(workspaceId: string): Promise<VfsWorkspaceRebaseResponse> {
    return this.requestJson<VfsWorkspaceRebaseResponse>(`/_vfs/workspace/${encodeURIComponent(workspaceId)}/rebase`, { method: "POST" });
  }

  async compactWorkspace(workspaceId: string): Promise<VfsWorkspaceIndexResponse> {
    return this.requestJson<VfsWorkspaceIndexResponse>(`/_vfs/workspace/${encodeURIComponent(workspaceId)}/compact`, { method: "POST" });
  }

  async commitWorkspace(workspaceId: string, options: WorkspaceCommitOptions): Promise<VfsCommitResponse> {
    return this.requestJson<VfsCommitResponse>(`/_vfs/workspace/${encodeURIComponent(workspaceId)}/commit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(options),
    });
  }

  async discardWorkspace(workspaceId: string): Promise<{ workspaceId: string; state: "discarded" }> {
    return this.requestJson<{ workspaceId: string; state: "discarded" }>(`/_vfs/workspace/${encodeURIComponent(workspaceId)}/discard`, {
      method: "POST",
    });
  }
}

export class WorkspaceFsWorkspace {
  readonly repo: WorkspaceFsClient;
  readonly ref: string;
  readonly workspaceId: string;
  baseCommitId: string | null;
  rootTreeId: string | null;

  constructor(repo: WorkspaceFsClient, checkout: VfsCheckoutResponse) {
    this.repo = repo;
    this.ref = checkout.ref;
    this.workspaceId = checkout.workspaceId;
    this.baseCommitId = checkout.baseCommitId;
    this.rootTreeId = checkout.rootTreeId;
  }

  async stat(path: string): Promise<VfsNodeStat> {
    return this.repo.stat(path, { workspaceId: this.workspaceId });
  }

  async maybeStat(path: string): Promise<VfsNodeStat | null> {
    return this.repo.maybeStat(path, { workspaceId: this.workspaceId });
  }

  async readdir(path: string, opts: { cursor?: string | null; limit?: number } = {}): Promise<VfsReaddirResponse> {
    return this.repo.readdir(path, { workspaceId: this.workspaceId, cursor: opts.cursor, limit: opts.limit });
  }

  async readFileBuffer(path: string): Promise<Uint8Array> {
    const node = await this.stat(path);
    if (node.type === "symlink") {
      return this.readFileBuffer(node.symlinkTarget ?? "");
    }
    if (node.type !== "file" || !node.blobId) {
      throw new WorkspaceFsClientError(`${path} is not a file`, 400);
    }
    return this.repo.readBlob(node.blobId, { workspaceId: this.workspaceId, path });
  }

  async readFile(path: string, encoding: BufferEncoding = "utf8"): Promise<string> {
    const bytes = await this.readFileBuffer(path);
    return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString(encoding);
  }

  async writeFile(path: string, content: string | Uint8Array, opts: { executable?: boolean; contentType?: string } = {}): Promise<void> {
    const normalizedRes = canonicalizeVfsPath(path);
    if (Result.isError(normalizedRes)) throw new WorkspaceFsClientError(normalizedRes.error.message, 400);
    const bytes = bytesFromContent(content);
    await this.repo.appendWorkspaceOps(this.workspaceId, [
      {
        kind: "put-file",
        path: normalizedRes.value,
        contentBase64: toBase64(bytes),
        executable: opts.executable,
        contentType: opts.contentType,
      },
    ]);
  }

  async appendFile(path: string, content: string | Uint8Array): Promise<void> {
    const existing = (await this.maybeStat(path)) ? await this.readFileBuffer(path) : new Uint8Array();
    const extra = bytesFromContent(content);
    const merged = new Uint8Array(existing.byteLength + extra.byteLength);
    merged.set(existing, 0);
    merged.set(extra, existing.byteLength);
    await this.writeFile(path, merged);
  }

  async mkdir(path: string): Promise<void> {
    await this.repo.appendWorkspaceOps(this.workspaceId, [{ kind: "mkdir", path }]);
  }

  async remove(path: string, opts: { recursive?: boolean; force?: boolean } = {}): Promise<void> {
    await this.repo.appendWorkspaceOps(this.workspaceId, [{ kind: "delete", path, recursive: opts.recursive, force: opts.force }]);
  }

  async rename(from: string, to: string): Promise<void> {
    await this.repo.appendWorkspaceOps(this.workspaceId, [{ kind: "rename", from, to }]);
  }

  async symlink(target: string, path: string): Promise<void> {
    await this.repo.appendWorkspaceOps(this.workspaceId, [{ kind: "symlink", target, path }]);
  }

  async status(): Promise<VfsWorkspaceStatusResponse> {
    return this.repo.workspaceStatus(this.workspaceId);
  }

  async index(path?: string): Promise<VfsWorkspaceIndexResponse> {
    return this.repo.workspaceIndex(this.workspaceId, { path });
  }

  async changes(prefix?: string): Promise<VfsWorkspaceChangesResponse> {
    return this.repo.workspaceChanges(this.workspaceId, { prefix });
  }

  async conflicts(): Promise<VfsWorkspaceConflictsResponse> {
    return this.repo.workspaceConflicts(this.workspaceId);
  }

  async rebase(): Promise<VfsWorkspaceRebaseResponse> {
    const res = await this.repo.rebaseWorkspace(this.workspaceId);
    this.baseCommitId = res.newBaseCommitId;
    this.rootTreeId = res.rootTreeId;
    return res;
  }

  async compact(): Promise<VfsWorkspaceIndexResponse> {
    return this.repo.compactWorkspace(this.workspaceId);
  }

  async commit(options: WorkspaceCommitOptions): Promise<VfsCommitResponse> {
    const res = await this.repo.commitWorkspace(this.workspaceId, {
      ...options,
      ref: normalizeRef(options.ref ?? this.ref),
      expectedHead: options.expectedHead === undefined ? this.baseCommitId : options.expectedHead,
    });
    this.baseCommitId = res.newCommitId;
    this.rootTreeId = res.commit.rootTreeId;
    return res;
  }

  async discard(): Promise<void> {
    await this.repo.discardWorkspace(this.workspaceId);
  }

  async diff(): Promise<string> {
    const status = await this.status();
    const chunks: string[] = [];
    for (const path of status.changedPaths) {
      const before = await this.repo.maybeStat(path, { commit: this.baseCommitId });
      const after = await this.maybeStat(path);
      chunks.push(await renderPathDiff(this, path, before, after));
    }
    return chunks.filter(Boolean).join("");
  }
}

async function renderPathDiff(
  workspace: WorkspaceFsWorkspace,
  path: string,
  before: VfsNodeStat | null,
  after: VfsNodeStat | null
): Promise<string> {
  if (!before && !after) return "";
  if (before?.type === "dir" || after?.type === "dir") {
    if (!before) return `diff -- workspace-fs ${path}\nnew directory\n`;
    if (!after) return `diff -- workspace-fs ${path}\ndeleted directory\n`;
    return "";
  }
  let beforeText = "";
  let afterText = "";
  if (before?.type === "file" && before.blobId) {
    const bytes = await workspace.repo.readBlob(before.blobId);
    const decoded = fromBase64(toBase64(bytes));
    if (Result.isError(decoded)) beforeText = "";
    else beforeText = Buffer.from(decoded.value).toString("utf8");
  }
  if (after?.type === "file") afterText = await workspace.readFile(path);
  if (beforeText === afterText && before?.type === after?.type) return "";
  const beforeLines = beforeText === "" ? [] : beforeText.replace(/\n$/, "").split("\n");
  const afterLines = afterText === "" ? [] : afterText.replace(/\n$/, "").split("\n");
  const out = [`diff -- workspace-fs ${path}\n`, `--- a${path}\n`, `+++ b${path}\n`, "@@\n"];
  for (const line of beforeLines) out.push(`-${line}\n`);
  for (const line of afterLines) out.push(`+${line}\n`);
  return out.join("");
}

export function openWorkspaceFsRepo(options: OpenWorkspaceFsRepoOptions): WorkspaceFsClient {
  return new WorkspaceFsClient(options);
}
