import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { R2ObjectStore } from "../src/objectstore/r2";

type Entry = {
  data: Uint8Array;
  etag: string;
  type: string;
};

class FakeS3File {
  constructor(
    private readonly client: FakeS3Client,
    private readonly key: string,
    private readonly range?: { begin?: number; end?: number }
  ) {}

  async exists(): Promise<boolean> {
    this.client.existsCalls += 1;
    return this.client.entries.has(this.key);
  }

  slice(begin?: number, end?: number): FakeS3File {
    return new FakeS3File(this.client, this.key, { begin, end });
  }

  async arrayBuffer(): Promise<ArrayBuffer> {
    this.client.arrayBufferCalls += 1;
    const entry = this.client.entries.get(this.key);
    if (!entry) throw new Error("missing");
    const start = this.range?.begin ?? 0;
    const end = this.range?.end ?? entry.data.byteLength;
    const sliced = entry.data.slice(start, end);
    return sliced.buffer.slice(sliced.byteOffset, sliced.byteOffset + sliced.byteLength);
  }

  async write(data: Uint8Array | Blob, opts?: { type?: string }): Promise<number> {
    const bytes = data instanceof Uint8Array ? data : new Uint8Array(await data.arrayBuffer());
    this.client.entries.set(this.key, {
      data: bytes,
      etag: `"etag-${this.key}"`,
      type: opts?.type ?? "",
    });
    return bytes.byteLength;
  }

  presign(opts?: { method?: string; type?: string }): string {
    const url = `https://example.test/${encodeURIComponent(this.key)}`;
    FakeS3Client.presigned.set(url, {
      client: this.client,
      key: this.key,
      type: opts?.type ?? "",
      method: opts?.method ?? "GET",
    });
    return url;
  }

  async stat(): Promise<{ size: number; etag: string; type: string; lastModified: Date }> {
    const entry = this.client.entries.get(this.key);
    if (!entry) throw new Error("missing");
    return {
      size: entry.data.byteLength,
      etag: entry.etag,
      type: entry.type,
      lastModified: new Date(),
    };
  }

  async delete(): Promise<void> {
    this.client.entries.delete(this.key);
  }
}

class FakeS3Client {
  static instances: FakeS3Client[] = [];
  static presigned = new Map<string, { client: FakeS3Client; key: string; type: string; method: string }>();

  readonly entries = new Map<string, Entry>();
  readonly options: Record<string, unknown> | undefined;
  existsCalls = 0;
  arrayBufferCalls = 0;

  constructor(options?: Record<string, unknown>) {
    this.options = options;
    FakeS3Client.instances.push(this);
  }

  file(path: string): FakeS3File {
    return new FakeS3File(this, path);
  }

  async list(input?: { prefix?: string; continuationToken?: string }): Promise<{
    contents: Array<{ key: string }>;
    isTruncated: boolean;
    nextContinuationToken?: string;
  }> {
    const filtered = [...this.entries.keys()].filter((key) => (input?.prefix ? key.startsWith(input.prefix) : true)).sort();
    const start = input?.continuationToken ? Number(input.continuationToken) : 0;
    const page = filtered.slice(start, start + 2);
    const next = start + page.length;
    return {
      contents: page.map((key) => ({ key })),
      isTruncated: next < filtered.length,
      nextContinuationToken: next < filtered.length ? String(next) : undefined,
    };
  }
}

const originalS3Client = Bun.S3Client;
const originalFetch = globalThis.fetch;

describe("R2ObjectStore", () => {
  beforeEach(() => {
    FakeS3Client.instances = [];
    FakeS3Client.presigned = new Map();
    (Bun as any).S3Client = FakeS3Client;
    globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
      const presigned = FakeS3Client.presigned.get(url);
      if (!presigned) return new Response("missing presigned url", { status: 404 });
      if (presigned.method !== "PUT") return new Response("bad method", { status: 405 });
      const body = init?.body;
      let bytes = new Uint8Array(0);
      if (body instanceof Uint8Array) {
        bytes = body;
      } else if (body instanceof Blob) {
        bytes = new Uint8Array(await body.arrayBuffer());
      } else if (body instanceof ArrayBuffer) {
        bytes = new Uint8Array(body);
      } else if (typeof body === "string") {
        bytes = new TextEncoder().encode(body);
      }
      presigned.client.entries.set(presigned.key, {
        data: bytes,
        etag: `"etag-${presigned.key}"`,
        type: presigned.type,
      });
      return new Response(null, {
        status: 200,
        headers: { etag: `"etag-${presigned.key}"` },
      });
    }) as typeof globalThis.fetch;
  });

  afterEach(() => {
    (Bun as any).S3Client = originalS3Client;
    globalThis.fetch = originalFetch;
  });

  test("uses Bun.S3Client for R2 reads and writes", async () => {
    const store = new R2ObjectStore({
      accountId: "acct",
      bucket: "bucket",
      accessKeyId: "key",
      secretAccessKey: "secret",
    });

    const client = FakeS3Client.instances[0];
    expect(client?.options).toMatchObject({
      bucket: "bucket",
      accessKeyId: "key",
      secretAccessKey: "secret",
      region: "auto",
      endpoint: "https://acct.r2.cloudflarestorage.com",
    });

    await store.put("streams/a", new Uint8Array([1, 2, 3]), { contentType: "application/octet-stream" });

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

  test("get handles missing objects from the GET itself without an exists preflight", async () => {
    const store = new R2ObjectStore({
      accountId: "acct",
      bucket: "bucket",
      accessKeyId: "key",
      secretAccessKey: "secret",
    });

    const client = FakeS3Client.instances[0]!;
    expect(await store.get("streams/missing")).toBeNull();
    expect(client.existsCalls).toBe(0);
    expect(client.arrayBufferCalls).toBe(1);
  });

  test("paginates list results", async () => {
    const store = new R2ObjectStore({
      accountId: "acct",
      bucket: "bucket",
      accessKeyId: "key",
      secretAccessKey: "secret",
      region: "auto",
    });

    const client = FakeS3Client.instances[0]!;
    client.entries.set("streams/a", { data: new Uint8Array([1]), etag: '"a"', type: "application/octet-stream" });
    client.entries.set("streams/b", { data: new Uint8Array([2]), etag: '"b"', type: "application/octet-stream" });
    client.entries.set("streams/c", { data: new Uint8Array([3]), etag: '"c"', type: "application/octet-stream" });
    client.entries.set("other/d", { data: new Uint8Array([4]), etag: '"d"', type: "application/octet-stream" });

    expect(await store.list("streams/")).toEqual(["streams/a", "streams/b", "streams/c"]);
  });
});
