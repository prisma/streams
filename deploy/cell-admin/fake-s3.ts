// An in-process S3 for the S3 store's tests: path-style GET, PUT and
// DELETE through presigned URLs (signatures unchecked, as s3lite), the
// two write preconditions (`If-None-Match: *`, `If-Match`) and the
// conditional GET, the way src/bin/s3lite/emulator.rs answers them.
// e2e.test.ts runs the same store against s3lite itself.
import { createHash } from "node:crypto";

export interface FakeObject {
  body: string;
  etag: string;
}

export class FakeS3 {
  /// "bucket/key" -> object.
  objects = new Map<string, FakeObject>();
  /// false: answer every write 200, as a store that ignores preconditions.
  honourPreconditions = true;
  /// true: answer every write 403, as a bucket under a read-only key.
  denyWrites = false;
  /// true: answer every read 500, as a store in an outage.
  failReads = false;
  requests: { method: string; key: string; status: number }[] = [];
  /// Runs before a PUT of `key` is decided, e.g. to land a racing write.
  beforePut?: (key: string) => void;
  /// A PUT of a `key` it returns true for answers 409
  /// ConditionalRequestConflict without writing, as AWS answers a
  /// conditional write that races another operation (retry it).
  conflictOnce?: (key: string) => boolean;
  /// A PUT of a `key` it returns true for answers 500, as an outage would.
  failPut?: (key: string) => boolean;
  private n = 0;
  private server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: (req) => this.answer(req) });

  get endpoint(): string {
    return `http://127.0.0.1:${this.server.port}`;
  }

  stop(): void {
    this.server.stop(true);
  }

  text(bucket: string, key: string): string | undefined {
    return this.objects.get(`${bucket}/${key}`)?.body;
  }

  /// Write an object as another writer would, with a fresh ETag.
  put(bucket: string, key: string, body: string): void {
    this.n += 1;
    const etag = `"${this.n}-${createHash("md5").update(body).digest("hex")}"`;
    this.objects.set(`${bucket}/${key}`, { body, etag });
  }

  private error(status: number, code: string): Response {
    return new Response(`<?xml version="1.0"?><Error><Code>${code}</Code></Error>`, {
      status,
      headers: { "content-type": "application/xml" },
    });
  }

  private async answer(req: Request): Promise<Response> {
    const key = decodeURIComponent(new URL(req.url).pathname.slice(1));
    const res = await this.decide(req, key);
    this.requests.push({ method: req.method, key, status: res.status });
    return res;
  }

  private async decide(req: Request, key: string): Promise<Response> {
    const obj = this.objects.get(key);
    if (req.method === "GET") {
      if (this.failReads) return this.error(500, "InternalError");
      if (!obj) return this.error(404, "NoSuchKey");
      if (req.headers.get("if-none-match") === obj.etag) return new Response(null, { status: 304 });
      return new Response(obj.body, { headers: { etag: obj.etag } });
    }
    if (req.method === "DELETE") {
      this.objects.delete(key);
      return new Response(null, { status: 204 });
    }
    if (req.method !== "PUT") return this.error(405, "MethodNotAllowed");
    const body = await req.text();
    if (this.denyWrites) return this.error(403, "AccessDenied");
    if (this.failPut?.(key)) return this.error(500, "InternalError");
    if (this.conflictOnce?.(key)) return this.error(409, "ConditionalRequestConflict");
    this.beforePut?.(key);
    const current = this.objects.get(key);
    const ifNoneMatch = req.headers.get("if-none-match");
    const ifMatch = req.headers.get("if-match");
    if (this.honourPreconditions) {
      if (ifNoneMatch === "*" && current) return this.error(412, "PreconditionFailed");
      if (ifMatch !== null && current?.etag !== ifMatch) return this.error(412, "PreconditionFailed");
    }
    const [bucket, ...rest] = key.split("/");
    this.put(bucket, rest.join("/"), body);
    return new Response(null, { headers: { etag: this.objects.get(key)!.etag } });
  }
}
