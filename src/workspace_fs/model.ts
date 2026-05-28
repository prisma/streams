import { createHash, randomUUID } from "node:crypto";
import { Buffer } from "node:buffer";
import { Result } from "better-result";
import type {
  VfsBlobManifest,
  VfsChunkObject,
  VfsCommit,
  VfsCommitId,
  VfsNodeStat,
  VfsObjectId,
  VfsStoredObject,
  VfsTreeEntry,
  VfsTreeId,
  VfsTreePage,
  VfsWorkspaceOp,
  VfsWorkspaceRecord,
} from "./types";

export const VFS_DEFAULT_REF = "refs/heads/main";
export const VFS_WORKSPACE_TTL_SECONDS = 24 * 60 * 60;
export const VFS_INLINE_BLOB_MAX_BYTES = 4 * 1024;
export const VFS_CHUNK_SIZE_BYTES = 64 * 1024;

export type VfsModelError = {
  message: string;
};

export type MutableVfsNode =
  | {
      type: "dir";
      mode: number;
      mtime?: string;
    }
  | {
      type: "file";
      mode: number;
      size: number;
      blobId: string;
      mtime?: string;
    }
  | {
      type: "symlink";
      mode: number;
      size: number;
      symlinkTarget: string;
      mtime?: string;
    };

export type MutableVfsTree = Map<string, MutableVfsNode>;

const TEXT_ENCODER = new TextEncoder();
const TEXT_DECODER = new TextDecoder();

export function newWorkspaceId(): string {
  return randomUUID();
}

export function toBase64(bytes: Uint8Array): string {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString("base64");
}

export function fromBase64(value: string): Result<Uint8Array, VfsModelError> {
  try {
    return Result.ok(new Uint8Array(Buffer.from(value, "base64")));
  } catch {
    return Result.err({ message: "invalid base64" });
  }
}

export function bytesFromText(value: string): Uint8Array {
  return TEXT_ENCODER.encode(value);
}

export function textFromBytes(bytes: Uint8Array): string {
  return TEXT_DECODER.decode(bytes);
}

export function bytesFromContent(content: string | Uint8Array): Uint8Array {
  return typeof content === "string" ? bytesFromText(content) : content;
}

export function normalizeRef(ref: string | null | undefined): string {
  const raw = ref?.trim();
  if (!raw) return VFS_DEFAULT_REF;
  return raw.startsWith("refs/") ? raw : `refs/heads/${raw}`;
}

export function controlRefKey(ref: string): string {
  return `ref:${ref}`;
}

export function objectKey(id: string): string {
  return `object:${id}`;
}

export function workspaceOpKey(path: string): string {
  return `path:${hashHex(bytesFromText(path)).slice(0, 32)}`;
}

export function objectsStreamName(repoStream: string): string {
  return `${repoStream}/_vfs/objects`;
}

export function workspaceStreamName(repoStream: string, workspaceId: string): string {
  return `${repoStream}/_vfs/workspaces/${encodeURIComponent(workspaceId)}`;
}

export function canonicalizeVfsPath(path: string): Result<string, VfsModelError> {
  if (typeof path !== "string" || path.trim() === "") return Result.err({ message: "path must be a non-empty string" });
  if (path.includes("\0")) return Result.err({ message: "path must not contain null bytes" });
  const rawParts = path.replace(/\\/g, "/").split("/");
  const parts: string[] = [];
  for (const part of rawParts) {
    if (part === "" || part === ".") continue;
    if (part === "..") {
      parts.pop();
      continue;
    }
    parts.push(part);
  }
  return Result.ok(`/${parts.join("/")}`);
}

export function parentPath(path: string): string {
  if (path === "/") return "/";
  const idx = path.lastIndexOf("/");
  return idx <= 0 ? "/" : path.slice(0, idx);
}

export function basename(path: string): string {
  if (path === "/") return "";
  const idx = path.lastIndexOf("/");
  return idx < 0 ? path : path.slice(idx + 1);
}

export function isPathInside(path: string, candidateParent: string): boolean {
  if (candidateParent === "/") return path !== "/";
  return path.startsWith(`${candidateParent}/`);
}

export function stableStringify(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  const obj = value as Record<string, unknown>;
  const entries = Object.keys(obj)
    .filter((key) => obj[key] !== undefined)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableStringify(obj[key])}`);
  return `{${entries.join(",")}}`;
}

export function hashHex(bytesOrText: Uint8Array | string): string {
  const hash = createHash("sha256");
  if (typeof bytesOrText === "string") hash.update(bytesOrText);
  else hash.update(bytesOrText);
  return hash.digest("hex");
}

export function objectId(kind: string, value: unknown): VfsObjectId {
  return `sha256:${hashHex(`${kind}\0${stableStringify(value)}`)}`;
}

export function makeBlobObjects(bytes: Uint8Array, opts: { executable?: boolean; contentType?: string } = {}): {
  manifest: VfsBlobManifest;
  chunks: VfsChunkObject[];
} {
  if (bytes.byteLength <= VFS_INLINE_BLOB_MAX_BYTES) {
    const body = {
      kind: "blob" as const,
      size: bytes.byteLength,
      executable: opts.executable === true ? true : undefined,
      contentType: opts.contentType,
      inlineBase64: toBase64(bytes),
    };
    const id = objectId("blob", body);
    return {
      manifest: { ...body, id },
      chunks: [],
    };
  }

  const chunks: VfsChunkObject[] = [];
  const chunkRefs = [];
  for (let offset = 0; offset < bytes.byteLength; offset += VFS_CHUNK_SIZE_BYTES) {
    const chunk = bytes.slice(offset, Math.min(bytes.byteLength, offset + VFS_CHUNK_SIZE_BYTES));
    const id = `sha256:${hashHex(chunk)}`;
    chunks.push({
      kind: "chunk",
      id,
      size: chunk.byteLength,
      dataBase64: toBase64(chunk),
    });
    chunkRefs.push({
      id,
      offset,
      size: chunk.byteLength,
      compression: "none" as const,
    });
  }

  const body = {
    kind: "blob" as const,
    size: bytes.byteLength,
    executable: opts.executable === true ? true : undefined,
    contentType: opts.contentType,
    chunks: chunkRefs,
  };
  const id = objectId("blob", body);
  return {
    manifest: { ...body, id },
    chunks,
  };
}

export function makeCommitId(commitWithoutId: Omit<VfsCommit, "id">): VfsCommitId {
  return objectId("commit", commitWithoutId);
}

export function makeTreePage(pathHint: string, entries: VfsTreeEntry[], nextPageId?: VfsTreeId): VfsTreePage {
  const body = {
    kind: "tree-page" as const,
    pathHint,
    entries: entries.slice().sort((a, b) => a.name.localeCompare(b.name)),
    nextPageId,
  };
  const id = objectId("tree-page", body);
  return { ...body, id };
}

export function emptyMutableTree(): MutableVfsTree {
  return new Map([["/", { type: "dir", mode: 0o040755 }]]);
}

export function nodeStat(path: string, node: MutableVfsNode, treeId: string | null = null): VfsNodeStat {
  if (node.type === "dir") {
    return {
      path,
      type: "dir",
      mode: node.mode,
      size: 0,
      mtime: node.mtime,
      treeId,
    };
  }
  if (node.type === "symlink") {
    return {
      path,
      type: "symlink",
      mode: node.mode,
      size: node.size,
      mtime: node.mtime,
      symlinkTarget: node.symlinkTarget,
    };
  }
  return {
    path,
    type: "file",
    mode: node.mode,
    size: node.size,
    mtime: node.mtime,
    blobId: node.blobId,
  };
}

export function ensureParentDirectories(tree: MutableVfsTree, path: string, mtime?: string): void {
  let current = parentPath(path);
  const stack: string[] = [];
  while (current !== "/" && !tree.has(current)) {
    stack.push(current);
    current = parentPath(current);
  }
  if (!tree.has("/")) tree.set("/", { type: "dir", mode: 0o040755, mtime });
  for (const dir of stack.reverse()) {
    tree.set(dir, { type: "dir", mode: 0o040755, mtime });
  }
}

export function directChildren(tree: MutableVfsTree, dir: string): Array<{ path: string; node: MutableVfsNode }> {
  const prefix = dir === "/" ? "/" : `${dir}/`;
  const out: Array<{ path: string; node: MutableVfsNode }> = [];
  for (const [path, node] of tree.entries()) {
    if (path === dir || !path.startsWith(prefix)) continue;
    const rest = path.slice(prefix.length);
    if (rest.includes("/")) continue;
    out.push({ path, node });
  }
  out.sort((a, b) => basename(a.path).localeCompare(basename(b.path)));
  return out;
}

export function applyWorkspaceOp(tree: MutableVfsTree, op: VfsWorkspaceOp): void {
  if (op.kind === "mkdir") {
    ensureParentDirectories(tree, op.path, op.createdAt);
    tree.set(op.path, { type: "dir", mode: 0o040755, mtime: op.createdAt });
    return;
  }

  if (op.kind === "put-file") {
    ensureParentDirectories(tree, op.path, op.createdAt);
    tree.set(op.path, {
      type: "file",
      mode: op.executable === true ? 0o100755 : 0o100644,
      size: op.size,
      blobId: op.blobId,
      mtime: op.createdAt,
    });
    return;
  }

  if (op.kind === "symlink") {
    ensureParentDirectories(tree, op.path, op.createdAt);
    tree.set(op.path, {
      type: "symlink",
      mode: 0o120777,
      size: bytesFromText(op.target).byteLength,
      symlinkTarget: op.target,
      mtime: op.createdAt,
    });
    return;
  }

  if (op.kind === "delete") {
    tree.delete(op.path);
    for (const path of Array.from(tree.keys())) {
      if (isPathInside(path, op.path)) tree.delete(path);
    }
    return;
  }

  if (op.kind === "rename") {
    const moves: Array<{ from: string; to: string; node: MutableVfsNode }> = [];
    for (const [path, node] of tree.entries()) {
      if (path === op.from || isPathInside(path, op.from)) {
        const suffix = path === op.from ? "" : path.slice(op.from.length);
        moves.push({ from: path, to: `${op.to}${suffix}`, node });
      }
    }
    moves.sort((a, b) => b.from.length - a.from.length);
    for (const move of moves) tree.delete(move.from);
    ensureParentDirectories(tree, op.to, op.createdAt);
    for (const move of moves.reverse()) tree.set(move.to, { ...move.node, mtime: op.createdAt });
  }
}

export function workspaceOpsFromRecords(records: VfsWorkspaceRecord[]): VfsWorkspaceOp[] {
  return records.filter((record): record is VfsWorkspaceOp => {
    return (
      record.kind === "put-file" ||
      record.kind === "delete" ||
      record.kind === "mkdir" ||
      record.kind === "rename" ||
      record.kind === "symlink"
    );
  });
}

export function isWorkspaceClosed(records: VfsWorkspaceRecord[]): { state: "open" | "committed" | "discarded"; commitId?: string } {
  for (let i = records.length - 1; i >= 0; i--) {
    const record = records[i]!;
    if (record.kind === "workspace-committed") return { state: "committed", commitId: record.commitId };
    if (record.kind === "workspace-discarded") return { state: "discarded" };
  }
  return { state: "open" };
}

export function decodeStoredObject(raw: unknown): Result<VfsStoredObject, VfsModelError> {
  if (!raw || typeof raw !== "object") return Result.err({ message: "stored object must be an object" });
  const kind = (raw as { kind?: unknown }).kind;
  if (kind !== "blob" && kind !== "chunk" && kind !== "tree-page" && kind !== "tree-index-page" && kind !== "commit") {
    return Result.err({ message: "unsupported stored object kind" });
  }
  return Result.ok(raw as VfsStoredObject);
}
