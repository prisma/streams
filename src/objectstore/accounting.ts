import type { SqliteDurableStore } from "../db/db";
import type { Metrics } from "../metrics";
import type { GetOptions, ObjectStore, PutResult } from "./interface";

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
    private readonly db: SqliteDurableStore,
    private readonly metrics?: Metrics
  ) {}

  private recordLatency(op: "put" | "get" | "head" | "delete" | "list", artifact: string, startedNs: bigint, outcome: "ok" | "miss" | "error"): void {
    if (!this.metrics) return;
    const elapsedNs = Number(process.hrtime.bigint() - startedNs);
    this.metrics.record(`tieredstore.objectstore.${op}.latency`, elapsedNs, "ns", {
      artifact,
      outcome,
    });
  }

  async put(key: string, data: Uint8Array, opts?: { contentType?: string; contentLength?: number }): Promise<PutResult> {
    const startedNs = process.hrtime.bigint();
    const classified = classifyKey(key);
    const artifact = classified?.artifact ?? "unknown";
    try {
      const res = await this.inner.put(key, data, opts);
      if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "put", data.byteLength);
      this.recordLatency("put", artifact, startedNs, "ok");
      return res;
    } catch (error) {
      this.recordLatency("put", artifact, startedNs, "error");
      throw error;
    }
  }

  async putFile(key: string, path: string, size: number, opts?: { contentType?: string }): Promise<PutResult> {
    const startedNs = process.hrtime.bigint();
    const classified = classifyKey(key);
    const artifact = classified?.artifact ?? "unknown";
    try {
      if (!this.inner.putFile) {
        const bytes = await Bun.file(path).bytes();
        const res = await this.inner.put(key, bytes, {
          contentType: opts?.contentType,
          contentLength: size,
        });
        if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "put", size);
        this.recordLatency("put", artifact, startedNs, "ok");
        return res;
      }
      const res = await this.inner.putFile(key, path, size, opts);
      if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "put", size);
      this.recordLatency("put", artifact, startedNs, "ok");
      return res;
    } catch (error) {
      this.recordLatency("put", artifact, startedNs, "error");
      throw error;
    }
  }

  async get(key: string, opts?: GetOptions): Promise<Uint8Array | null> {
    const startedNs = process.hrtime.bigint();
    const classified = classifyKey(key);
    const artifact = classified?.artifact ?? "unknown";
    try {
      const res = await this.inner.get(key, opts);
      if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "get", res?.byteLength ?? 0);
      this.recordLatency("get", artifact, startedNs, res == null ? "miss" : "ok");
      return res;
    } catch (error) {
      this.recordLatency("get", artifact, startedNs, "error");
      throw error;
    }
  }

  async head(key: string): Promise<{ etag: string; size: number } | null> {
    const startedNs = process.hrtime.bigint();
    const classified = classifyKey(key);
    const artifact = classified?.artifact ?? "unknown";
    try {
      const res = await this.inner.head(key);
      if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "head", res?.size ?? 0);
      this.recordLatency("head", artifact, startedNs, res == null ? "miss" : "ok");
      return res;
    } catch (error) {
      this.recordLatency("head", artifact, startedNs, "error");
      throw error;
    }
  }

  async delete(key: string): Promise<void> {
    const startedNs = process.hrtime.bigint();
    const classified = classifyKey(key);
    const artifact = classified?.artifact ?? "unknown";
    try {
      await this.inner.delete(key);
      if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "delete", 0);
      this.recordLatency("delete", artifact, startedNs, "ok");
    } catch (error) {
      this.recordLatency("delete", artifact, startedNs, "error");
      throw error;
    }
  }

  async list(prefix: string): Promise<string[]> {
    const startedNs = process.hrtime.bigint();
    const classified = classifyListPrefix(prefix);
    const artifact = classified?.artifact ?? (prefix.replace(/\/+$/, "") === "streams" ? "stream_catalog" : "unknown");
    try {
      const res = await this.inner.list(prefix);
      if (classified) this.db.recordObjectStoreRequestByHash(classified.streamHash, classified.artifact, "list", 0);
      this.recordLatency("list", artifact, startedNs, "ok");
      return res;
    } catch (error) {
      this.recordLatency("list", artifact, startedNs, "error");
      throw error;
    }
  }
}
