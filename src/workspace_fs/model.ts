import { createHash, randomUUID } from "node:crypto";
import { Buffer } from "node:buffer";
import { Result } from "better-result";
import type {
  VfsBlobManifest,
  VfsChunkObject,
  VfsObjectId,
  VfsStoredObject,
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

export function objectKey(id: string): string {
  return `object:${id}`;
}

export function workspaceOpKey(path: string): string {
  return `path:${hashHex(bytesFromText(path)).slice(0, 32)}`;
}

export function objectsStreamName(repoStream: string): string {
  return `${repoStream}/_workspace/objects`;
}

export function workspaceStreamName(repoStream: string, workspaceId: string): string {
  return `${repoStream}/_workspace/workspaces/${encodeURIComponent(workspaceId)}`;
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
  if (kind !== "blob" && kind !== "chunk") {
    return Result.err({ message: "unsupported stored object kind" });
  }
  return Result.ok(raw as VfsStoredObject);
}
