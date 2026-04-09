import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import type { SqliteDurableStore } from "../db/db";
import type {
  GetFileResult,
  GetOptions,
  ObjectStore,
  PutFileNoEtagOptions,
  PutFileOptions,
  PutNoEtagOptions,
  PutOptions,
  PutResult,
} from "./interface";

type ClassifiedRequest = {
  streamHash: string;
  artifact: string;
};

function classifyKey(key: string): ClassifiedRequest | null {
  const match = /^streams\/([0-9a-f]{32})\/(.+)$/.exec(key);
  if (!match) return null;
  const [, streamHash, rest] = match;
  if (rest === "manifest.json") return { streamHash, artifact: "manifest" };
  if (rest === "schema-registry.json") return { streamHash, artifact: "schema_registry" };
  if (rest.startsWith("index/")) return { streamHash, artifact: "routing_index" };
  if (rest.startsWith("lexicon/")) return { streamHash, artifact: "routing_key_lexicon" };
  if (rest.startsWith("secondary-index/")) return { streamHash, artifact: "exact_index" };
  if (rest.startsWith("segments/") && rest.endsWith(".bin")) return { streamHash, artifact: "segment" };
  if (rest.startsWith("segments/") && rest.endsWith(".cix")) return { streamHash, artifact: "bundled_companion" };
  return { streamHash, artifact: "meta" };
}

function classifyListPrefix(prefix: string): ClassifiedRequest | null {
  const exact = classifyKey(prefix.replace(/\/+$/, ""));
  if (exact) return exact;
  const match = /^streams\/([0-9a-f]{32})(?:\/(.+))?\/?$/.exec(prefix);
  if (!match) return null;
  const [, streamHash, rest = ""] = match;
  if (rest === "" || rest === "segments") return { streamHash, artifact: "segment" };
  if (rest === "index") return { streamHash, artifact: "routing_index" };
  if (rest.startsWith("lexicon")) return { streamHash, artifact: "routing_key_lexicon" };
  if (rest.startsWith("secondary-index")) return { streamHash, artifact: "exact_index" };
  return { streamHash, artifact: "meta" };
}

export class AccountingObjectStore implements ObjectStore {
  constructor(
    private readonly inner: ObjectStore,
    private readonly db: SqliteDurableStore
  ) {}

  async put(key: string, data: Uint8Array, opts?: PutOptions): Promise<PutResult> {
    const res = await this.inner.put(key, data, opts);
    const classified = classifyKey(key);
    if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "put", data.byteLength);
    return res;
  }

  async putFile(key: string, path: string, size: number, opts?: PutFileOptions): Promise<PutResult> {
    if (!this.inner.putFile) {
      const bytes = await Bun.file(path).bytes();
      const res = await this.inner.put(key, bytes, {
        contentType: opts?.contentType,
        contentLength: size,
      });
      const classified = classifyKey(key);
      if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "put", size);
      return res;
    }
    const res = await this.inner.putFile(key, path, size, opts);
    const classified = classifyKey(key);
    if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "put", size);
    return res;
  }

  async putNoEtag(key: string, data: Uint8Array, opts?: PutNoEtagOptions): Promise<number> {
    if (!this.inner.putNoEtag) {
      await this.inner.put(key, data, { contentType: opts?.contentType, contentLength: opts?.contentLength });
      const classified = classifyKey(key);
      if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "put", data.byteLength);
      return data.byteLength;
    }
    const size = await this.inner.putNoEtag(key, data, opts);
    const classified = classifyKey(key);
    if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "put", size);
    return size;
  }

  async putFileNoEtag(key: string, path: string, size: number, opts?: PutFileNoEtagOptions): Promise<number> {
    if (!this.inner.putFileNoEtag) {
      if (this.inner.putFile) {
        await this.inner.putFile(key, path, size, { contentType: opts?.contentType });
      } else {
        const bytes = await Bun.file(path).bytes();
        await this.inner.put(key, bytes, { contentType: opts?.contentType, contentLength: size });
      }
      const classified = classifyKey(key);
      if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "put", size);
      return size;
    }
    const written = await this.inner.putFileNoEtag(key, path, size, opts);
    const classified = classifyKey(key);
    if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "put", written);
    return written;
  }

  async get(key: string, opts?: GetOptions): Promise<Uint8Array | null> {
    const res = await this.inner.get(key, opts);
    const classified = classifyKey(key);
    if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "get", res?.byteLength ?? 0);
    return res;
  }

  async getFile(key: string, path: string, opts?: GetOptions): Promise<GetFileResult | null> {
    if (!this.inner.getFile) {
      const bytes = await this.inner.get(key, opts);
      if (!bytes) return null;
      mkdirSync(dirname(path), { recursive: true });
      await Bun.write(path, bytes);
      const classified = classifyKey(key);
      if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "get", bytes.byteLength);
      return { size: bytes.byteLength };
    }
    const res = await this.inner.getFile(key, path, opts);
    const classified = classifyKey(key);
    if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "get", res?.size ?? 0);
    return res;
  }

  async head(key: string): Promise<{ etag: string; size: number } | null> {
    const res = await this.inner.head(key);
    const classified = classifyKey(key);
    if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "head", res?.size ?? 0);
    return res;
  }

  async delete(key: string): Promise<void> {
    await this.inner.delete(key);
    const classified = classifyKey(key);
    if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "delete", 0);
  }

  async list(prefix: string): Promise<string[]> {
    const res = await this.inner.list(prefix);
    const classified = classifyListPrefix(prefix);
    if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "list", 0);
    return res;
  }
}
