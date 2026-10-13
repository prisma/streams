// A fake shared cell for the deploy tools' offline self-tests (cell-check,
// deploy-release.sh): the identity a running streams-slate shows (the
// /health headers, /v1/debug/load, /v1/debug/auth's feed versions, behind
// the deployment bearer) and the product requests cell-check's canary
// makes (create, append, read, project usage, delete), behind the canary
// token. src/http.rs and docs/refactor/WIRE-MATRIX.md §2 are the shapes.
// `POST /__deploy {commit, sha, nonce}` is the fake's own: a fake `prisma
// deploy` calls it to put a new version, a new instance with a boot id of
// its own, in place, unless commit, sha and nonce are the running ones (an
// unchanged input: Composer's no-op, D0 finding 8).
export interface Identity {
  commit: string;
  sha: string;
  policies: number;
  grants: number;
  /// The instance's x-streams-boot-id ("b" when not given).
  boot?: string;
}

export class FakeStreamsCell {
  identity: Identity;
  /// The CELL_DEPLOY_NONCE the running version was deployed with.
  nonce: string | null = null;
  /// Instances /__deploy has started.
  boots = 0;
  /// Served instead of `identity` until this instant (an old version
  /// still answering beside a new one).
  previous?: { identity: Identity; until: number };
  /// /health's status when not 200 (a booting or refused cell).
  healthStatus = 200;
  /// Project usage answers the figure before an append this many times
  /// after it (the rollup's lag).
  usageLag = 0;
  /// The canary project's max_streams: a create past it answers the
  /// product's `429 stream_limit`, which is not retryable.
  maxStreams = Number.POSITIVE_INFINITY;
  /// The next request of this "METHOD path" answers the product's
  /// retryable `429 project_rate_limit` with Retry-After 1, once.
  rateLimitOnce?: string;
  streams = new Map<string, { key: string; records: unknown[] }>();
  ingest = 0;
  requests: string[] = [];
  private lagLeft = 0;
  private server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: (req) => this.serve(req) });

  constructor(
    identity: Identity,
    readonly bearer: string,
    readonly canary: string,
    readonly project: string,
  ) {
    this.identity = identity;
  }

  get url(): string {
    return `http://127.0.0.1:${this.server.port}`;
  }

  stop(): void {
    this.server.stop(true);
  }

  private now(): Identity {
    return this.previous && Date.now() < this.previous.until ? this.previous.identity : this.identity;
  }

  private json(status: number, body: unknown, headers: Record<string, string> = {}): Response {
    return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });
  }

  private refuse(status: number, code: string): Response {
    return this.json(status, { error: { code, message: code, retryable: false } });
  }

  private async serve(req: Request): Promise<Response> {
    const url = new URL(req.url);
    this.requests.push(`${req.method} ${url.pathname}${url.search}`);
    if (url.pathname === "/__deploy" && req.method === "POST") {
      const next = await req.json();
      const id = this.identity;
      if (next.commit === id.commit && next.sha === id.sha && next.nonce === this.nonce) return new Response(null, { status: 204 });
      this.boots += 1;
      this.identity = { ...id, commit: next.commit, sha: next.sha, boot: `b${this.boots}` };
      this.nonce = next.nonce;
      return new Response(null, { status: 204 });
    }
    const id = this.now();
    const bearer = req.headers.get("authorization")?.replace(/^Bearer /, "");
    if (url.pathname === "/health") {
      return new Response(this.healthStatus === 200 ? "ok" : "booting", {
        status: this.healthStatus,
        headers: this.healthStatus === 200 ? { "x-streams-git": id.commit, "x-streams-build-unix": "1", "x-streams-boot-id": id.boot ?? "b" } : {},
      });
    }
    if (url.pathname.startsWith("/v1/debug/")) {
      if (bearer !== this.bearer) return this.refuse(401, "unauthorized");
      if (url.pathname === "/v1/debug/load") return this.json(200, { binary_sha256: id.sha, git_commit: id.commit, boot_id: id.boot ?? "b" });
      if (url.pathname === "/v1/debug/auth") {
        return this.json(200, { feeds: { policies: { feedVersion: id.policies }, grants: { feedVersion: id.grants } } });
      }
      return this.refuse(404, "not_found");
    }
    if (bearer !== this.canary) return this.refuse(401, "unauthorized");
    return this.product(req, url);
  }

  private async product(req: Request, url: URL): Promise<Response> {
    if (this.rateLimitOnce === `${req.method} ${url.pathname}`) {
      this.rateLimitOnce = undefined;
      return this.json(
        429,
        { error: { code: "project_rate_limit", message: "the project's rate quota is exhausted; retry", retryable: true } },
        { "retry-after": "1" },
      );
    }
    if (url.pathname === `/v1/projects/${this.project}/usage` && req.method === "GET") {
      const lagging = this.lagLeft > 0;
      if (lagging) this.lagLeft -= 1;
      return this.json(200, { accountId: "acct", projectId: this.project, ingestPayloadBytes: lagging ? 0 : this.ingest });
    }
    const m = url.pathname.match(/^\/v1\/streams\/([^/]+)(\/records)?$/);
    if (!m) return this.refuse(404, "not_found");
    const [name, records] = [decodeURIComponent(m[1]), m[2] !== undefined];
    const stream = this.streams.get(name);
    const key = req.headers.get("prisma-encryption-key") ?? "";
    if (!records && req.method === "DELETE") {
      if (!stream) return this.json(404, { error: "not_found" });
      this.streams.delete(name);
      return new Response(null, { status: 204 });
    }
    if (!records && req.method === "PUT") {
      if (stream) return this.json(200, { name });
      if (this.streams.size >= this.maxStreams) return this.refuse(429, "stream_limit");
      this.streams.set(name, { key, records: [] });
      return this.json(201, { name });
    }
    if (!stream) return this.refuse(404, "not_found");
    if (stream.key !== key) return this.refuse(403, "wrong_key");
    if (records && req.method === "POST") {
      const body = await req.text();
      stream.records.push(JSON.parse(body));
      this.ingest += body.length;
      this.lagLeft = this.usageLag;
      return this.json(200, { cursor: "c1", count: 1, duplicate: false, sealed: false });
    }
    if (records && req.method === "GET") return this.json(200, stream.records);
    return this.refuse(405, "method_not_allowed");
  }
}
