import { createHash, createHmac } from "node:crypto";
import { Buffer } from "node:buffer";
import { createReadStream, createWriteStream, mkdirSync, rmSync } from "node:fs";
import { dirname } from "node:path";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
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
import { dsError } from "../util/ds_error.ts";

export type R2Config = {
  accountId: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  region?: string;
};

const EMPTY_SHA256_HEX = createHash("sha256").update("").digest("hex");

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

function encodeRfc3986(value: string): string {
  return encodeURIComponent(value).replace(/[!'()*]/g, (char) => `%${char.charCodeAt(0).toString(16).toUpperCase()}`);
}

function encodePathSegments(value: string): string {
  return value
    .split("/")
    .map((segment) => encodeRfc3986(segment))
    .join("/");
}

function hmacSha256(key: string | Buffer, value: string): Buffer {
  return createHmac("sha256", key).update(value).digest();
}

function amzDateParts(now: Date): { dateStamp: string; dateTime: string } {
  const iso = now.toISOString().replace(/[:-]|\.\d{3}/g, "");
  return {
    dateStamp: iso.slice(0, 8),
    dateTime: iso,
  };
}

function normalizeHeaderValue(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function buildCanonicalQuery(entries: Array<[string, string]>): string {
  return entries
    .map(([key, value]) => [encodeRfc3986(key), encodeRfc3986(value)] as const)
    .sort(([leftKey, leftValue], [rightKey, rightValue]) => {
      if (leftKey === rightKey) return leftValue.localeCompare(rightValue);
      return leftKey.localeCompare(rightKey);
    })
    .map(([key, value]) => `${key}=${value}`)
    .join("&");
}

function deriveSigningKey(secretAccessKey: string, dateStamp: string, region: string, service: string): Buffer {
  const dateKey = hmacSha256(`AWS4${secretAccessKey}`, dateStamp);
  const regionKey = hmacSha256(dateKey, region);
  const serviceKey = hmacSha256(regionKey, service);
  return hmacSha256(serviceKey, "aws4_request");
}

async function readResponseBytes(response: Response): Promise<Uint8Array> {
  if (!response.body) return new Uint8Array(0);
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value || value.byteLength === 0) continue;
      chunks.push(value);
      total += value.byteLength;
    }
  } finally {
    reader.releaseLock();
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

async function writeResponseToFile(response: Response, path: string): Promise<number> {
  mkdirSync(dirname(path), { recursive: true });
  if (!response.body) {
    await Bun.write(path, new Uint8Array(0));
    return 0;
  }
  let size = 0;
  const counter = new Transform({
    transform(chunk, _encoding, callback) {
      size += Buffer.byteLength(chunk);
      callback(null, chunk);
    },
  });
  await pipeline(Readable.fromWeb(response.body as any), counter, createWriteStream(path));
  return size;
}

function listTag(xml: string, tag: string): string {
  const match = new RegExp(`<${tag}>([^<]*)</${tag}>`).exec(xml);
  return match?.[1]?.trim() ?? "";
}

function listKeys(xml: string): string[] {
  return [...xml.matchAll(/<Key>([^<]*)<\/Key>/g)].map((match) => decodeURIComponent(match[1] ?? ""));
}

export class R2ObjectStore implements ObjectStore {
  private readonly bucket: string;
  private readonly origin: string;
  private readonly host: string;
  private readonly accessKeyId: string;
  private readonly secretAccessKey: string;
  private readonly region: string;

  constructor(cfg: R2Config) {
    this.bucket = cfg.bucket;
    this.origin = `https://${cfg.accountId}.r2.cloudflarestorage.com`;
    this.host = new URL(this.origin).host;
    this.accessKeyId = cfg.accessKeyId;
    this.secretAccessKey = cfg.secretAccessKey;
    this.region = cfg.region ?? "auto";
  }

  private objectPath(key: string): string {
    return `/${encodeRfc3986(this.bucket)}/${encodePathSegments(key)}`;
  }

  private bucketPath(): string {
    return `/${encodeRfc3986(this.bucket)}`;
  }

  private wrapError(op: string, key: string, err: unknown): never {
    const message = String((err as any)?.message ?? err);
    throw dsError(`R2 ${op} failed for ${key}: ${message}`);
  }

  private async signedFetch(
    method: string,
    path: string,
    options: {
      query?: Array<[string, string]>;
      body?: BodyInit | null;
      payloadHash?: string;
      headers?: Record<string, string>;
      signal?: AbortSignal;
    } = {}
  ): Promise<Response> {
    const now = new Date();
    const { dateStamp, dateTime } = amzDateParts(now);
    const canonicalQuery = buildCanonicalQuery(options.query ?? []);
    const payloadHash = options.payloadHash ?? EMPTY_SHA256_HEX;
    const requestHeaders = new Headers(options.headers);
    requestHeaders.set("x-amz-content-sha256", payloadHash);
    requestHeaders.set("x-amz-date", dateTime);

    const canonicalHeaderEntries = new Map<string, string>();
    canonicalHeaderEntries.set("host", this.host);
    for (const [name, value] of requestHeaders.entries()) {
      canonicalHeaderEntries.set(name.toLowerCase(), normalizeHeaderValue(value));
    }
    const sortedHeaders = [...canonicalHeaderEntries.entries()].sort(([left], [right]) => left.localeCompare(right));
    const signedHeaders = sortedHeaders.map(([name]) => name).join(";");
    const canonicalHeaders = sortedHeaders.map(([name, value]) => `${name}:${value}\n`).join("");
    const canonicalRequest = [method, path, canonicalQuery, canonicalHeaders, signedHeaders, payloadHash].join("\n");
    const credentialScope = `${dateStamp}/${this.region}/s3/aws4_request`;
    const stringToSign = [
      "AWS4-HMAC-SHA256",
      dateTime,
      credentialScope,
      sha256Hex(canonicalRequest),
    ].join("\n");
    const signingKey = deriveSigningKey(this.secretAccessKey, dateStamp, this.region, "s3");
    const signature = createHmac("sha256", signingKey).update(stringToSign).digest("hex");
    requestHeaders.set(
      "authorization",
      `AWS4-HMAC-SHA256 Credential=${this.accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`
    );

    const url = `${this.origin}${path}${canonicalQuery.length > 0 ? `?${canonicalQuery}` : ""}`;
    return await fetch(url, {
      method,
      headers: requestHeaders,
      body: options.body ?? null,
      signal: options.signal,
    });
  }

  async put(key: string, data: Uint8Array, opts: PutOptions = {}): Promise<PutResult> {
    try {
      const payloadHash = sha256Hex(data);
      const headers: Record<string, string> = {};
      if (opts.contentType) headers["content-type"] = opts.contentType;
      if (opts.contentLength != null) headers["content-length"] = String(opts.contentLength);
      const res = await this.signedFetch("PUT", this.objectPath(key), {
        body: Buffer.from(data),
        payloadHash,
        headers,
        signal: opts.signal,
      });
      if (!res.ok) throw dsError(`HTTP ${res.status}: ${await res.text().catch(() => "")}`.trim());
      return { etag: stripQuotes(res.headers.get("etag")) || payloadHash };
    } catch (err) {
      this.wrapError("PUT", key, err);
    }
  }

  async putFile(key: string, path: string, _size: number, opts: PutFileOptions = {}): Promise<PutResult> {
    try {
      const payloadHash = await sha256FileHex(path);
      const headers: Record<string, string> = {};
      if (opts.contentType) headers["content-type"] = opts.contentType;
      const res = await this.signedFetch("PUT", this.objectPath(key), {
        body: Bun.file(path),
        payloadHash,
        headers,
        signal: opts.signal,
      });
      if (!res.ok) throw dsError(`HTTP ${res.status}: ${await res.text().catch(() => "")}`.trim());
      return { etag: stripQuotes(res.headers.get("etag")) || payloadHash };
    } catch (err) {
      this.wrapError("PUT", key, err);
    }
  }

  async putNoEtag(key: string, data: Uint8Array, opts: PutNoEtagOptions = {}): Promise<number> {
    try {
      const payloadHash = sha256Hex(data);
      const headers: Record<string, string> = {};
      if (opts.contentType) headers["content-type"] = opts.contentType;
      if (opts.contentLength != null) headers["content-length"] = String(opts.contentLength);
      const res = await this.signedFetch("PUT", this.objectPath(key), {
        body: Buffer.from(data),
        payloadHash,
        headers,
      });
      if (!res.ok) throw dsError(`HTTP ${res.status}: ${await res.text().catch(() => "")}`.trim());
      return data.byteLength;
    } catch (err) {
      this.wrapError("PUT", key, err);
    }
  }

  async putFileNoEtag(key: string, path: string, _size: number, opts: PutFileNoEtagOptions = {}): Promise<number> {
    try {
      const payloadHash = await sha256FileHex(path);
      const headers: Record<string, string> = {};
      if (opts.contentType) headers["content-type"] = opts.contentType;
      const res = await this.signedFetch("PUT", this.objectPath(key), {
        body: Bun.file(path),
        payloadHash,
        headers,
      });
      if (!res.ok) throw dsError(`HTTP ${res.status}: ${await res.text().catch(() => "")}`.trim());
      return _size;
    } catch (err) {
      this.wrapError("PUT", key, err);
    }
  }

  async get(key: string, opts: GetOptions = {}): Promise<Uint8Array | null> {
    try {
      const headers: Record<string, string> = {};
      if (opts.range) {
        headers.range = `bytes=${opts.range.start}-${opts.range.end == null ? "" : opts.range.end}`;
      }
      const res = await this.signedFetch("GET", this.objectPath(key), { headers });
      if (res.status === 404) return null;
      if (!res.ok) throw dsError(`HTTP ${res.status}: ${await res.text().catch(() => "")}`.trim());
      return await readResponseBytes(res);
    } catch (err) {
      this.wrapError("GET", key, err);
    }
  }

  async getFile(key: string, path: string, opts: GetOptions = {}): Promise<GetFileResult | null> {
    try {
      const headers: Record<string, string> = {};
      if (opts.range) {
        headers.range = `bytes=${opts.range.start}-${opts.range.end == null ? "" : opts.range.end}`;
      }
      const res = await this.signedFetch("GET", this.objectPath(key), { headers });
      if (res.status === 404) {
        rmSync(path, { force: true });
        return null;
      }
      if (!res.ok) throw dsError(`HTTP ${res.status}: ${await res.text().catch(() => "")}`.trim());
      const size = await writeResponseToFile(res, path);
      return { size };
    } catch (err) {
      rmSync(path, { force: true });
      this.wrapError("GET", key, err);
    }
  }

  async head(key: string): Promise<{ etag: string; size: number } | null> {
    try {
      const res = await this.signedFetch("HEAD", this.objectPath(key));
      if (res.status === 404) return null;
      if (!res.ok) throw dsError(`HTTP ${res.status}: ${await res.text().catch(() => "")}`.trim());
      return {
        etag: stripQuotes(res.headers.get("etag")),
        size: Number(res.headers.get("content-length") ?? "0"),
      };
    } catch (err) {
      this.wrapError("HEAD", key, err);
    }
  }

  async delete(key: string): Promise<void> {
    try {
      const res = await this.signedFetch("DELETE", this.objectPath(key));
      if (res.status === 404) return;
      if (!res.ok) throw dsError(`HTTP ${res.status}: ${await res.text().catch(() => "")}`.trim());
    } catch (err) {
      this.wrapError("DELETE", key, err);
    }
  }

  async list(prefix: string): Promise<string[]> {
    try {
      const keys: string[] = [];
      let continuationToken: string | undefined;
      for (;;) {
        const query: Array<[string, string]> = [["list-type", "2"], ["encoding-type", "url"], ["prefix", prefix]];
        if (continuationToken) query.push(["continuation-token", continuationToken]);
        const res = await this.signedFetch("GET", this.bucketPath(), { query });
        if (!res.ok) throw dsError(`HTTP ${res.status}: ${await res.text().catch(() => "")}`.trim());
        const text = await res.text();
        keys.push(...listKeys(text));
        const isTruncated = listTag(text, "IsTruncated") === "true";
        continuationToken = listTag(text, "NextContinuationToken") || undefined;
        if (!isTruncated || !continuationToken) break;
      }
      return keys;
    } catch (err) {
      this.wrapError("LIST", prefix, err);
    }
  }
}
