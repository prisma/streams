import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { R2ObjectStore } from "../src/objectstore/r2";

type Entry = {
  data: Uint8Array;
  etag: string;
  type: string;
};

const TEXT_ENCODER = new TextEncoder();
const originalFetch = globalThis.fetch;
const entries = new Map<string, Entry>();
const requests: Request[] = [];

function xmlEscape(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function objectKeyFromUrl(url: URL): string {
  const parts = url.pathname.split("/").filter(Boolean);
  return parts.slice(1).map(decodeURIComponent).join("/");
}

function etagFor(key: string): string {
  return `"etag-${key}"`;
}

async function fakeFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  const req = new Request(input, init);
  requests.push(req);
  expect(req.headers.get("authorization")).toStartWith("AWS4-HMAC-SHA256 ");
  expect(req.headers.get("x-amz-date")).not.toBeNull();
  expect(req.headers.get("x-amz-content-sha256")).not.toBeNull();

  const url = new URL(req.url);
  const key = objectKeyFromUrl(url);
  if (req.method === "PUT") {
    const bytes = new Uint8Array(await req.arrayBuffer());
    entries.set(key, {
      data: bytes,
      etag: etagFor(key),
      type: req.headers.get("content-type") ?? "",
    });
    return new Response(null, { status: 200, headers: { etag: etagFor(key) } });
  }
  if (req.method === "HEAD") {
    const entry = entries.get(key);
    if (!entry) return new Response(null, { status: 404 });
    return new Response(null, {
      status: 200,
      headers: {
        etag: entry.etag,
        "content-length": String(entry.data.byteLength),
        "content-type": entry.type,
      },
    });
  }
  if (req.method === "GET" && url.searchParams.get("list-type") === "2") {
    const prefix = url.searchParams.get("prefix") ?? "";
    const keys = [...entries.keys()].filter((k) => k.startsWith(prefix)).sort();
    const body = [
      '<?xml version="1.0" encoding="UTF-8"?>',
      "<ListBucketResult>",
      "<IsTruncated>false</IsTruncated>",
      ...keys.map((k) => `<Contents><Key>${xmlEscape(k)}</Key></Contents>`),
      "</ListBucketResult>",
    ].join("");
    return new Response(TEXT_ENCODER.encode(body), { status: 200 });
  }
  if (req.method === "GET") {
    const entry = entries.get(key);
    if (!entry) return new Response(null, { status: 404 });
    const range = req.headers.get("range");
    if (range) {
      const match = range.match(/^bytes=(\d+)-(\d*)$/);
      if (!match) return new Response(null, { status: 416 });
      const start = Number(match[1]);
      const end = match[2] ? Number(match[2]) : entry.data.byteLength - 1;
      return new Response(entry.data.slice(start, Math.min(end + 1, entry.data.byteLength)), { status: 206 });
    }
    return new Response(entry.data.slice(), { status: 200 });
  }
  if (req.method === "DELETE") {
    entries.delete(key);
    return new Response(null, { status: 204 });
  }
  return new Response(null, { status: 405 });
}

describe("R2ObjectStore", () => {
  beforeEach(() => {
    entries.clear();
    requests.length = 0;
    globalThis.fetch = fakeFetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  test("uses signed fetch requests for R2 reads and writes", async () => {
    const store = new R2ObjectStore({
      accountId: "acct",
      bucket: "bucket",
      accessKeyId: "key",
      secretAccessKey: "secret",
    });

    await store.put("streams/a", new Uint8Array([1, 2, 3]), { contentType: "application/octet-stream" });
    expect(requests[0]?.url).toBe("https://acct.r2.cloudflarestorage.com/bucket/streams/a");

    expect(await store.head("streams/a")).toEqual({
      etag: "etag-streams/a",
      size: 3,
    });
    expect(await store.get("streams/a")).toEqual(new Uint8Array([1, 2, 3]));
    expect(await store.get("streams/a", { range: { start: 1, end: 2 } })).toEqual(new Uint8Array([2, 3]));

    await store.delete("streams/a");
    expect(await store.head("streams/a")).toBeNull();
    expect(await store.get("streams/a")).toBeNull();
  });

  test("supports custom S3-compatible endpoints for local R2-path stress tests", async () => {
    const store = new R2ObjectStore({
      accountId: "acct",
      bucket: "bucket",
      accessKeyId: "key",
      secretAccessKey: "secret",
      endpoint: "http://127.0.0.1:9000",
      region: "us-east-1",
    });

    await store.put("streams/a", new Uint8Array([1]));
    expect(requests[0]?.url).toBe("http://127.0.0.1:9000/bucket/streams/a");
  });

  test("get handles missing objects", async () => {
    const store = new R2ObjectStore({
      accountId: "acct",
      bucket: "bucket",
      accessKeyId: "key",
      secretAccessKey: "secret",
    });

    expect(await store.get("streams/missing")).toBeNull();
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
});
