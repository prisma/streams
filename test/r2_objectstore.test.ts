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
    return this.client.entries.has(this.key);
  }

  slice(begin?: number, end?: number): FakeS3File {
    return new FakeS3File(this.client, this.key, { begin, end });
  }

  async arrayBuffer(): Promise<ArrayBuffer> {
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

  readonly entries = new Map<string, Entry>();
  readonly options: Record<string, unknown> | undefined;

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

describe("R2ObjectStore", () => {
  beforeEach(() => {
    FakeS3Client.instances = [];
    (Bun as any).S3Client = FakeS3Client;
  });

  afterEach(() => {
    (Bun as any).S3Client = originalS3Client;
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
