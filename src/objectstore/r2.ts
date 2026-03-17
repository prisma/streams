import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import type { GetOptions, ObjectStore, PutResult } from "./interface";
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

  async put(key: string, data: Uint8Array, opts: { contentType?: string; contentLength?: number } = {}): Promise<PutResult> {
    try {
      await this.file(key).write(data, { type: opts.contentType });
      const stat = await this.file(key).stat();
      return { etag: stripQuotes(stat.etag) || sha256Hex(data) };
    } catch (err) {
      this.wrapError("PUT", key, err);
    }
  }

  async putFile(key: string, path: string, _size: number, opts: { contentType?: string } = {}): Promise<PutResult> {
    try {
      await this.file(key).write(Bun.file(path), { type: opts.contentType });
      const stat = await this.file(key).stat();
      return { etag: stripQuotes(stat.etag) || (await sha256FileHex(path)) };
    } catch (err) {
      this.wrapError("PUT", key, err);
    }
  }

  async get(key: string, opts: GetOptions = {}): Promise<Uint8Array | null> {
    try {
      const file = this.file(key);
      if (!(await file.exists())) return null;
      const body =
        opts.range == null
          ? file
          : file.slice(opts.range.start, opts.range.end == null ? undefined : opts.range.end + 1);
      return new Uint8Array(await body.arrayBuffer());
    } catch (err) {
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
