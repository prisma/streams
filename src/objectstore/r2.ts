import { createHash, createHmac } from "node:crypto";
import { createReadStream } from "node:fs";
import { Readable } from "node:stream";
import type { GetOptions, ObjectStore, PutResult } from "./interface";
import { dsError } from "../util/ds_error.ts";

export type R2Config = {
  accountId: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  region?: string;
  endpoint?: string;
};

const EMPTY_SHA256 = sha256Hex(new Uint8Array(0));
const XML_DECODER = new TextDecoder();

function sha256Hex(data: Uint8Array | string): string {
  return createHash("sha256").update(data).digest("hex");
}

async function readResponseBytes(res: Response): Promise<Uint8Array> {
  if (!res.body) return new Uint8Array(0);
  const reader = res.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      const chunk = next.value;
      chunks.push(chunk);
      total += chunk.byteLength;
    }
  } finally {
    reader.releaseLock();
  }
  if (chunks.length === 1) return chunks[0]!;
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

async function readResponseText(res: Response): Promise<string> {
  return XML_DECODER.decode(await readResponseBytes(res));
}

function fileStreamBody(path: string): BodyInit {
  return Readable.toWeb(createReadStream(path)) as unknown as BodyInit;
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

function encodePathPart(part: string): string {
  return encodeURIComponent(part).replace(/[!'()*]/g, (ch) => `%${ch.charCodeAt(0).toString(16).toUpperCase()}`);
}

function encodeKeyPath(key: string): string {
  return key.split("/").map(encodePathPart).join("/");
}

function encodeQueryPart(part: string): string {
  return encodePathPart(part).replace(/%7E/g, "~");
}

function hmac(key: string | Buffer, data: string): Buffer {
  return createHmac("sha256", key).update(data).digest();
}

function xmlDecode(value: string): string {
  return value
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, "\"")
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&");
}

function firstXmlTag(xml: string, tag: string): string | null {
  const match = xml.match(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`));
  return match ? xmlDecode(match[1] ?? "") : null;
}

function allXmlTags(xml: string, tag: string): string[] {
  const out: string[] = [];
  const re = new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, "g");
  for (;;) {
    const match = re.exec(xml);
    if (!match) break;
    out.push(xmlDecode(match[1] ?? ""));
  }
  return out;
}

export class R2ObjectStore implements ObjectStore {
  private readonly endpoint: URL;
  private readonly bucket: string;
  private readonly accessKeyId: string;
  private readonly secretAccessKey: string;
  private readonly region: string;

  constructor(cfg: R2Config) {
    this.endpoint = new URL(cfg.endpoint ?? `https://${cfg.accountId}.r2.cloudflarestorage.com`);
    this.bucket = cfg.bucket;
    this.accessKeyId = cfg.accessKeyId;
    this.secretAccessKey = cfg.secretAccessKey;
    this.region = cfg.region ?? "auto";
  }

  private objectPath(key: string): string {
    return `/${encodePathPart(this.bucket)}/${encodeKeyPath(key)}`;
  }

  private requestUrl(path: string, query: Array<[string, string]> = []): URL {
    const url = new URL(this.endpoint.href);
    const basePath = url.pathname.replace(/\/+$/, "");
    url.pathname = `${basePath}${path}`;
    url.search = "";
    for (const [key, value] of query) url.searchParams.append(key, value);
    return url;
  }

  private authorization(method: string, url: URL, headers: Headers, payloadHash: string, amzDate: string): string {
    const date = amzDate.slice(0, 8);
    const host = url.host;
    headers.set("host", host);
    headers.set("x-amz-content-sha256", payloadHash);
    headers.set("x-amz-date", amzDate);

    const signedHeaderNames = [...headers.keys()].map((h) => h.toLowerCase()).sort();
    const canonicalHeaders = signedHeaderNames
      .map((name) => `${name}:${headers.get(name)?.trim().replace(/\s+/g, " ") ?? ""}\n`)
      .join("");
    const signedHeaders = signedHeaderNames.join(";");
    const queryEntries = [...url.searchParams.entries()].sort(([ak, av], [bk, bv]) => {
      if (ak === bk) return av < bv ? -1 : av > bv ? 1 : 0;
      return ak < bk ? -1 : 1;
    });
    const canonicalQuery = queryEntries
      .map(([key, value]) => `${encodeQueryPart(key)}=${encodeQueryPart(value)}`)
      .join("&");
    const canonicalRequest = [
      method,
      url.pathname,
      canonicalQuery,
      canonicalHeaders,
      signedHeaders,
      payloadHash,
    ].join("\n");
    const scope = `${date}/${this.region}/s3/aws4_request`;
    const stringToSign = [
      "AWS4-HMAC-SHA256",
      amzDate,
      scope,
      sha256Hex(canonicalRequest),
    ].join("\n");
    const kDate = hmac(`AWS4${this.secretAccessKey}`, date);
    const kRegion = hmac(kDate, this.region);
    const kService = hmac(kRegion, "s3");
    const kSigning = hmac(kService, "aws4_request");
    const signature = createHmac("sha256", kSigning).update(stringToSign).digest("hex");
    return `AWS4-HMAC-SHA256 Credential=${this.accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
  }

  private async request(
    method: string,
    path: string,
    opts: {
      query?: Array<[string, string]>;
      headers?: HeadersInit;
      body?: BodyInit;
      payloadHash?: string;
    } = {}
  ): Promise<Response> {
    const url = this.requestUrl(path, opts.query ?? []);
    const headers = new Headers(opts.headers);
    const now = new Date();
    const amzDate = now.toISOString().replace(/[:-]|\.\d{3}/g, "");
    const payloadHash = opts.payloadHash ?? EMPTY_SHA256;
    headers.set("authorization", this.authorization(method, url, headers, payloadHash, amzDate));
    return fetch(url, {
      method,
      headers,
      body: opts.body,
    });
  }

  private wrapStatus(op: string, key: string, res: Response): never {
    throw dsError(`R2 ${op} failed for ${key}: HTTP ${res.status} ${res.statusText}`);
  }

  private wrapError(op: string, key: string, err: unknown): never {
    const message = String((err as any)?.message ?? err);
    throw dsError(`R2 ${op} failed for ${key}: ${message}`);
  }

  async put(key: string, data: Uint8Array, opts: { contentType?: string; contentLength?: number } = {}): Promise<PutResult> {
    const payloadHash = sha256Hex(data);
    try {
      const headers: Record<string, string> = {
        "content-length": String(opts.contentLength ?? data.byteLength),
      };
      if (opts.contentType) headers["content-type"] = opts.contentType;
      const res = await this.request("PUT", this.objectPath(key), {
        headers,
        body: data as unknown as BodyInit,
        payloadHash,
      });
      if (!res.ok) this.wrapStatus("PUT", key, res);
      return { etag: payloadHash };
    } catch (err) {
      this.wrapError("PUT", key, err);
    }
  }

  async putFile(key: string, path: string, size: number, opts: { contentType?: string } = {}): Promise<PutResult> {
    const payloadHash = await sha256FileHex(path);
    try {
      const headers: Record<string, string> = {
        "content-length": String(size),
      };
      if (opts.contentType) headers["content-type"] = opts.contentType;
      const res = await this.request("PUT", this.objectPath(key), {
        headers,
        body: fileStreamBody(path),
        payloadHash,
      });
      if (!res.ok) this.wrapStatus("PUT", key, res);
      return { etag: payloadHash };
    } catch (err) {
      this.wrapError("PUT", key, err);
    }
  }

  async get(key: string, opts: GetOptions = {}): Promise<Uint8Array | null> {
    try {
      const headers: Record<string, string> = {};
      if (opts.range) {
        const end = opts.range.end == null ? "" : String(opts.range.end);
        headers.range = `bytes=${opts.range.start}-${end}`;
      }
      const res = await this.request("GET", this.objectPath(key), { headers });
      if (res.status === 404) return null;
      if (!res.ok && res.status !== 206) this.wrapStatus("GET", key, res);
      return readResponseBytes(res);
    } catch (err) {
      this.wrapError("GET", key, err);
    }
  }

  async head(key: string): Promise<{ etag: string; size: number } | null> {
    try {
      const res = await this.request("HEAD", this.objectPath(key));
      if (res.status === 404) return null;
      if (!res.ok) this.wrapStatus("HEAD", key, res);
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
      const res = await this.request("DELETE", this.objectPath(key));
      if (res.status === 404) return;
      if (!res.ok && res.status !== 204) this.wrapStatus("DELETE", key, res);
    } catch (err) {
      this.wrapError("DELETE", key, err);
    }
  }

  async list(prefix: string): Promise<string[]> {
    try {
      const keys: string[] = [];
      let continuationToken: string | null = null;
      for (;;) {
        const query: Array<[string, string]> = [
          ["list-type", "2"],
          ["prefix", prefix],
        ];
        if (continuationToken) query.push(["continuation-token", continuationToken]);
        const res = await this.request("GET", `/${encodePathPart(this.bucket)}`, { query });
        if (!res.ok) this.wrapStatus("LIST", prefix, res);
        const xml = await readResponseText(res);
        keys.push(...allXmlTags(xml, "Key"));
        const truncated = firstXmlTag(xml, "IsTruncated") === "true";
        if (!truncated) break;
        continuationToken = firstXmlTag(xml, "NextContinuationToken");
        if (!continuationToken) break;
      }
      return keys;
    } catch (err) {
      this.wrapError("LIST", prefix, err);
    }
  }
}
