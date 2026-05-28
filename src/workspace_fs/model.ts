import { createHash, randomUUID } from "node:crypto";
import { Buffer } from "node:buffer";
import { Result } from "better-result";
import type {
  WorkspaceFsBlobManifest,
  WorkspaceFsChunkObject,
  WorkspaceFsObjectId,
  WorkspaceFsStoredObject,
  WorkspaceFsWorkspaceOp,
  WorkspaceFsWorkspaceRecord,
} from "./types";

export const WORKSPACE_FS_DEFAULT_REF = "refs/heads/main";
export const WORKSPACE_FS_WORKSPACE_TTL_SECONDS = 24 * 60 * 60;
export const WORKSPACE_FS_INLINE_BLOB_MAX_BYTES = 4 * 1024;
export const WORKSPACE_FS_CHUNK_SIZE_BYTES = 64 * 1024;

export type WorkspaceFsModelError = {
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

export function fromBase64(value: string): Result<Uint8Array, WorkspaceFsModelError> {
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
  if (!raw) return WORKSPACE_FS_DEFAULT_REF;
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

export function canonicalizeWorkspaceFsPath(path: string): Result<string, WorkspaceFsModelError> {
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

export function objectId(kind: string, value: unknown): WorkspaceFsObjectId {
  return `sha256:${hashHex(`${kind}\0${stableStringify(value)}`)}`;
}

export function makeBlobObjects(bytes: Uint8Array, opts: { executable?: boolean; contentType?: string } = {}): {
  manifest: WorkspaceFsBlobManifest;
  chunks: WorkspaceFsChunkObject[];
} {
  if (bytes.byteLength <= WORKSPACE_FS_INLINE_BLOB_MAX_BYTES) {
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

  const chunks: WorkspaceFsChunkObject[] = [];
  const chunkRefs = [];
  for (let offset = 0; offset < bytes.byteLength; offset += WORKSPACE_FS_CHUNK_SIZE_BYTES) {
    const chunk = bytes.slice(offset, Math.min(bytes.byteLength, offset + WORKSPACE_FS_CHUNK_SIZE_BYTES));
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

export function workspaceOpsFromRecords(records: WorkspaceFsWorkspaceRecord[]): WorkspaceFsWorkspaceOp[] {
  return records.filter((record): record is WorkspaceFsWorkspaceOp => {
    return (
      record.kind === "put-file" ||
      record.kind === "delete" ||
      record.kind === "mkdir" ||
      record.kind === "rename" ||
      record.kind === "symlink"
    );
  });
}

export function isWorkspaceClosed(records: WorkspaceFsWorkspaceRecord[]): { state: "open" | "committed" | "discarded"; commitId?: string } {
  for (let i = records.length - 1; i >= 0; i--) {
    const record = records[i]!;
    if (record.kind === "workspace-committed") return { state: "committed", commitId: record.commitId };
    if (record.kind === "workspace-discarded") return { state: "discarded" };
  }
  return { state: "open" };
}

export function decodeStoredObject(raw: unknown): Result<WorkspaceFsStoredObject, WorkspaceFsModelError> {
  if (!raw || typeof raw !== "object") return Result.err({ message: "stored object must be an object" });
  const kind = (raw as { kind?: unknown }).kind;
  if (kind !== "blob" && kind !== "chunk") {
    return Result.err({ message: "unsupported stored object kind" });
  }
  return Result.ok(raw as WorkspaceFsStoredObject);
}
