import { createHash } from "node:crypto";
import { Buffer } from "node:buffer";
import { createReadStream, mkdirSync } from "node:fs";
import { dirname } from "node:path";
import type { GetFileResult, GetOptions, ObjectStore, PutFileOptions, PutOptions, PutResult } from "./interface";
import { dsError } from "../util/ds_error.ts";

export type R2Config = {
  accountId: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  region?: string;
};

function sha256Hex(data: Uint8Array | string): string {
  return createHash("sha256").update(data).digest("hex");
}

async function sha256FileHex(path: string): Promise<string> {
  const hash = createHash("sha256");
  await new Promise<void>((resolve, reject) => {
    const stream = createReadStream(path);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("error", (err) => reject(err));
    stream.on("end", () => resolve());
  });
  return hash.digest("hex");
}

function stripQuotes(value: string | null): string {
  if (!value) return "";
  return value.replace(/^\"|\"$/g, "");
}

function isMissingObjectError(err: unknown): boolean {
  const record = err as Record<string, unknown> | null | undefined;
  const status = record?.status;
  const statusCode = record?.statusCode;
  const code = String(record?.code ?? "");
  const message = String(record?.message ?? err ?? "").toLowerCase();
  if (status === 404 || statusCode === 404) return true;
  if (code === "NoSuchKey" || code === "NotFound") return true;
  return (
    message.includes("not found") ||
    message.includes("no such key") ||
    message.includes("does not exist") ||
    message === "missing"
  );
}

function isAbortError(err: unknown): boolean {
  const record = err as Record<string, unknown> | null | undefined;
  const name = String(record?.name ?? "");
  const code = String(record?.code ?? "");
  const message = String(record?.message ?? err ?? "").toLowerCase();
  return name === "AbortError" || code === "ABORT_ERR" || message.includes("aborted");
}

export class R2ObjectStore implements ObjectStore {
  private readonly client: Bun.S3Client;

  constructor(cfg: R2Config) {
    this.client = new Bun.S3Client({
      bucket: cfg.bucket,
      accessKeyId: cfg.accessKeyId,
      secretAccessKey: cfg.secretAccessKey,
      region: cfg.region ?? "auto",
      endpoint: `https://${cfg.accountId}.r2.cloudflarestorage.com`,
    });
  }

  private file(key: string): Bun.S3File {
    return this.client.file(key);
  }

  private wrapError(op: string, key: string, err: unknown): never {
    const message = String((err as any)?.message ?? err);
    throw dsError(`R2 ${op} failed for ${key}: ${message}`);
  }

  private async putWithFetch(key: string, body: Blob, opts: PutOptions = {}): Promise<PutResult> {
    const file = this.file(key);
    const url = file.presign({ method: "PUT", type: opts.contentType });
    const headers = new Headers();
    if (opts.contentType) headers.set("content-type", opts.contentType);
    try {
      const res = await fetch(url, {
        method: "PUT",
        body,
        headers,
        signal: opts.signal,
      });
      if (!res.ok) throw dsError(`HTTP ${res.status}`);
      return { etag: stripQuotes(res.headers.get("etag")) };
    } catch (err) {
      if (opts.signal?.aborted && isAbortError(err)) {
        throw (opts.signal.reason instanceof Error ? opts.signal.reason : dsError(String(opts.signal.reason ?? "aborted")));
      }
      if (isAbortError(err)) throw dsError("aborted");
      throw err;
    }
  }

  async put(key: string, data: Uint8Array, opts: PutOptions = {}): Promise<PutResult> {
    try {
      const res = await this.putWithFetch(key, new Blob([Buffer.from(data)]), opts);
      return { etag: res.etag || sha256Hex(data) };
    } catch (err) {
      this.wrapError("PUT", key, err);
    }
  }

  async putFile(key: string, path: string, _size: number, opts: PutFileOptions = {}): Promise<PutResult> {
    try {
      const res = await this.putWithFetch(key, Bun.file(path), opts);
      return { etag: res.etag || (await sha256FileHex(path)) };
    } catch (err) {
      this.wrapError("PUT", key, err);
    }
  }

  async get(key: string, opts: GetOptions = {}): Promise<Uint8Array | null> {
    try {
      const file = this.file(key);
      const body =
        opts.range == null
          ? file
          : file.slice(opts.range.start, opts.range.end == null ? undefined : opts.range.end + 1);
      return new Uint8Array(await body.arrayBuffer());
    } catch (err) {
      if (isMissingObjectError(err)) return null;
      this.wrapError("GET", key, err);
    }
  }

  async getFile(key: string, path: string, opts: GetOptions = {}): Promise<GetFileResult | null> {
    try {
      const file = this.file(key);
      const body =
        opts.range == null
          ? file
          : file.slice(opts.range.start, opts.range.end == null ? undefined : opts.range.end + 1);
      mkdirSync(dirname(path), { recursive: true });
      const size = await Bun.write(path, body);
      return { size };
    } catch (err) {
      if (isMissingObjectError(err)) return null;
      this.wrapError("GET", key, err);
    }
  }

  async head(key: string): Promise<{ etag: string; size: number } | null> {
    try {
      const file = this.file(key);
      if (!(await file.exists())) return null;
      const stat = await file.stat();
      return { etag: stripQuotes(stat.etag), size: stat.size };
    } catch (err) {
      this.wrapError("HEAD", key, err);
    }
  }

  async delete(key: string): Promise<void> {
    try {
      const file = this.file(key);
      if (!(await file.exists())) return;
      await file.delete();
    } catch (err) {
      this.wrapError("DELETE", key, err);
    }
  }

  async list(prefix: string): Promise<string[]> {
    try {
      const keys: string[] = [];
      let continuationToken: string | undefined;
      for (;;) {
        const res = await this.client.list({ prefix, continuationToken });
        for (const entry of res.contents ?? []) {
          keys.push(entry.key);
        }
        if (!res.isTruncated || !res.nextContinuationToken) break;
        continuationToken = res.nextContinuationToken;
      }
      return keys;
    } catch (err) {
      this.wrapError("LIST", prefix, err);
    }
  }
}
