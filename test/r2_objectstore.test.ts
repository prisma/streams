import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { R2ObjectStore } from "../src/objectstore/r2";

type Entry = {
  data: Uint8Array;
  etag: string;
  type: string;
};

const originalS3Client = Bun.S3Client;
const originalFetch = globalThis.fetch;

let entries: Map<string, Entry>;
let fetchCalls = 0;
let observedRequests: Array<{ method: string; url: string; headers: Headers }>;

function keyFromUrl(url: URL): string {
  const parts = url.pathname.split("/").filter((part) => part.length > 0);
  if (parts[0] !== "bucket") throw new Error(`unexpected bucket path ${url.pathname}`);
  return parts
    .slice(1)
    .map((part) => decodeURIComponent(part))
    .join("/");
}

function encodeXml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

function rangeSlice(data: Uint8Array, rangeHeader: string | null): Uint8Array {
  if (!rangeHeader) return data;
  const match = /^bytes=(\d+)-(\d*)$/.exec(rangeHeader);
  if (!match) throw new Error(`unexpected range header ${rangeHeader}`);
  const start = Number(match[1]);
  const end = match[2].length > 0 ? Number(match[2]) : data.byteLength - 1;
  return data.slice(start, end + 1);
}

async function bodyBytes(body: BodyInit | null | undefined): Promise<Uint8Array> {
  if (!body) return new Uint8Array(0);
  if (body instanceof Uint8Array) return body;
  if (body instanceof ArrayBuffer) return new Uint8Array(body);
  if (typeof body === "string") return new TextEncoder().encode(body);
  return new Uint8Array(await new Response(body).arrayBuffer());
}

describe("R2ObjectStore", () => {
  beforeEach(() => {
    entries = new Map();
    fetchCalls = 0;
    observedRequests = [];
    (Bun as any).S3Client = class UnexpectedS3Client {
      constructor() {
        throw new Error("R2ObjectStore should not construct Bun.S3Client");
      }
    };
    globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
      fetchCalls += 1;
      const url = new URL(typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url);
      const method = init?.method ?? (input instanceof Request ? input.method : "GET");
      const headers = new Headers(init?.headers ?? (input instanceof Request ? input.headers : undefined));
      observedRequests.push({ method, url: url.toString(), headers: new Headers(headers) });

      if (url.pathname === "/bucket" || url.pathname === "/bucket/") {
        if (method !== "GET") return new Response("bad method", { status: 405 });
        const prefix = url.searchParams.get("prefix") ?? "";
        const token = Number(url.searchParams.get("continuation-token") ?? "0");
        const keys = [...entries.keys()].filter((key) => key.startsWith(prefix)).sort();
        const page = keys.slice(token, token + 2);
        const next = token + page.length;
        const xml = [
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
          "<ListBucketResult>",
          ...page.map((key) => `<Contents><Key>${encodeXml(encodeURIComponent(key))}</Key></Contents>`),
          `<IsTruncated>${next < keys.length ? "true" : "false"}</IsTruncated>`,
          next < keys.length ? `<NextContinuationToken>${next}</NextContinuationToken>` : "",
          "</ListBucketResult>",
        ].join("");
        return new Response(xml, { status: 200, headers: { "content-type": "application/xml" } });
      }

      const key = keyFromUrl(url);
      const existing = entries.get(key);
      if (method === "PUT") {
        const data = await bodyBytes(init?.body ?? null);
        const etag = `"etag-${key}"`;
        entries.set(key, {
          data,
          etag,
          type: headers.get("content-type") ?? "",
        });
        return new Response(null, { status: 200, headers: { etag } });
      }
      if (method === "GET") {
        if (!existing) return new Response("missing", { status: 404 });
        const data = rangeSlice(existing.data, headers.get("range"));
        return new Response(data, {
          status: 200,
          headers: {
            etag: existing.etag,
            "content-length": String(data.byteLength),
            "content-type": existing.type || "application/octet-stream",
          },
        });
      }
      if (method === "HEAD") {
        if (!existing) return new Response(null, { status: 404 });
        return new Response(null, {
          status: 200,
          headers: {
            etag: existing.etag,
            "content-length": String(existing.data.byteLength),
            "content-type": existing.type || "application/octet-stream",
          },
        });
      }
      if (method === "DELETE") {
        if (!existing) return new Response(null, { status: 404 });
        entries.delete(key);
        return new Response(null, { status: 204 });
      }
      return new Response("bad method", { status: 405 });
    }) as typeof globalThis.fetch;
  });

  afterEach(() => {
    (Bun as any).S3Client = originalS3Client;
    globalThis.fetch = originalFetch;
  });

  test("uses signed fetch for reads and writes without Bun.S3Client", async () => {
    const store = new R2ObjectStore({
      accountId: "acct",
      bucket: "bucket",
      accessKeyId: "key",
      secretAccessKey: "secret",
    });

    await store.put("streams/a", new Uint8Array([1, 2, 3]), { contentType: "application/octet-stream" });

    expect(await store.head("streams/a")).toEqual({
      etag: "etag-streams/a",
      size: 3,
    });
    expect(fetchCalls).toBeGreaterThanOrEqual(2);
    expect(await store.get("streams/a")).toEqual(new Uint8Array([1, 2, 3]));
    expect(await store.get("streams/a", { range: { start: 1, end: 2 } })).toEqual(new Uint8Array([2, 3]));
    expect(observedRequests.every((req) => req.headers.get("authorization")?.startsWith("AWS4-HMAC-SHA256"))).toBe(true);

    await store.delete("streams/a");
    expect(await store.head("streams/a")).toBeNull();
    expect(await store.get("streams/a")).toBeNull();
  });

  test("get handles missing objects from GET directly", async () => {
    const store = new R2ObjectStore({
      accountId: "acct",
      bucket: "bucket",
      accessKeyId: "key",
      secretAccessKey: "secret",
    });

    expect(await store.get("streams/missing")).toBeNull();
    expect(observedRequests).toHaveLength(1);
    expect(observedRequests[0]?.method).toBe("GET");
  });

  test("streams getFile responses to disk", async () => {
    const store = new R2ObjectStore({
      accountId: "acct",
      bucket: "bucket",
      accessKeyId: "key",
      secretAccessKey: "secret",
    });
    const tmpDir = mkdtempSync(join(tmpdir(), "r2-objectstore-"));
    const outPath = join(tmpDir, "payload.bin");
    entries.set("streams/payload", {
      data: new Uint8Array([9, 8, 7, 6]),
      etag: '"etag-streams/payload"',
      type: "application/octet-stream",
    });

    expect(await store.getFile?.("streams/payload", outPath)).toEqual({ size: 4 });
    expect(readFileSync(outPath)).toEqual(Buffer.from([9, 8, 7, 6]));

    rmSync(tmpDir, { recursive: true, force: true });
  });

  test("paginates list results", async () => {
    const store = new R2ObjectStore({
      accountId: "acct",
      bucket: "bucket",
      accessKeyId: "key",
      secretAccessKey: "secret",
      region: "auto",
    });

    entries.set("streams/a", { data: new Uint8Array([1]), etag: '"a"', type: "application/octet-stream" });
    entries.set("streams/b", { data: new Uint8Array([2]), etag: '"b"', type: "application/octet-stream" });
    entries.set("streams/c", { data: new Uint8Array([3]), etag: '"c"', type: "application/octet-stream" });
    entries.set("other/d", { data: new Uint8Array([4]), etag: '"d"', type: "application/octet-stream" });

    expect(await store.list("streams/")).toEqual(["streams/a", "streams/b", "streams/c"]);
  });

  test("supports fetch-only no-etag upload paths for bytes and files", async () => {
    const store = new R2ObjectStore({
      accountId: "acct",
      bucket: "bucket",
      accessKeyId: "key",
      secretAccessKey: "secret",
    });

    const tmpDir = mkdtempSync(join(tmpdir(), "r2-objectstore-"));
    const tmpPath = join(tmpDir, "payload.bin");
    await Bun.write(tmpPath, new Uint8Array([4, 5, 6]));

    expect(await store.putNoEtag?.("streams/no-etag-bytes", new Uint8Array([1, 2, 3]))).toBe(3);
    expect(await store.putFileNoEtag?.("streams/no-etag-file", tmpPath, 3)).toBe(3);
    expect(fetchCalls).toBe(2);

    expect(entries.get("streams/no-etag-bytes")?.data).toEqual(new Uint8Array([1, 2, 3]));
    expect(entries.get("streams/no-etag-file")?.data).toEqual(new Uint8Array([4, 5, 6]));
    const noEtagPuts = observedRequests.filter((req) => req.method === "PUT");
    expect(noEtagPuts).toHaveLength(2);
    expect(noEtagPuts.every((req) => req.headers.get("x-amz-content-sha256") === "UNSIGNED-PAYLOAD")).toBe(true);
    expect(noEtagPuts[0]?.headers.get("content-length")).toBeNull();
    expect(noEtagPuts[1]?.headers.get("content-length")).toBe("3");

    rmSync(tmpDir, { recursive: true, force: true });
  });
});
