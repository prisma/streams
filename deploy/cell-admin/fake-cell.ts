// A fake cell for offboard.test.ts: it answers the product routes
// offboarding uses the way the server does (src/product.rs
// auth_failure_response and product_list, the verification order of
// src/auth.rs verify_customer). e2e.test.ts runs the same flows against
// the real binary.
import type { Bundle } from "./feeds";
import { verifiedClaims } from "./test-rig";

/// A fake cell: it serves the bundle the test publishes to it, and keeps
/// each project's streams. Hooks let a test add a stream between walks
/// or rate-limit a request once.
export class FakeCell {
  live?: Bundle;
  streams = new Map<string, Set<string>>();
  pageSize = 2;
  requests: { method: string; path: string; status: number }[] = [];
  afterWalk?: (walks: number) => void;
  rateLimitNext = 0;
  private walks = 0;
  private server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: (req) => this.answer(req) });

  get url(): string {
    return `http://127.0.0.1:${this.server.port}`;
  }

  stop(): void {
    this.server.stop(true);
  }

  publish(bundle: Bundle): void {
    this.live = structuredClone(bundle);
  }

  private reply(status: number, code?: string, body?: unknown, headers: Record<string, string> = {}): Response {
    const payload = body ?? (code ? { error: { code, message: code, retryable: false } } : undefined);
    return new Response(payload === undefined ? null : JSON.stringify(payload), {
      status,
      headers: { "content-type": "application/json", ...headers },
    });
  }

  /// verify_customer's order: signature, project placed, ownership,
  /// credential known, its project, its status, its grant_version.
  private authorize(req: Request): { project: string } | Response {
    const bearer = req.headers.get("authorization")?.replace(/^Bearer /, "") ?? "";
    const claims = this.live && verifiedClaims(this.live, bearer);
    if (!this.live || !claims) return this.reply(401, "bad_signature");
    if (claims.exp + 30 <= Date.now() / 1000) return this.reply(401, "expired");
    const policy = (this.live.policies.projects as any[]).find((p) => p.project_id === claims.project_id);
    if (!policy) return this.reply(421, "wrong_cell");
    if (claims.ownership_version !== policy.ownership_version) return this.reply(401, "ownership_version_mismatch");
    const cred = (this.live.grants.credentials as any[]).find((c) => c.credential_id === claims.credential_id);
    if (!cred) return this.reply(401, "credential_unknown");
    if (cred.project_id !== claims.project_id) return this.reply(401, "credential_project_mismatch");
    if (cred.status !== "active") return this.reply(403, "credential_not_active");
    if (cred.grant_version !== claims.grant_version) return this.reply(401, "grant_version_mismatch");
    return { project: claims.project_id };
  }

  private answer(req: Request): Response {
    const url = new URL(req.url);
    const r = this.route(req, url);
    this.requests.push({ method: req.method, path: url.pathname + url.search, status: r.status });
    return r;
  }

  private route(req: Request, url: URL): Response {
    const who = this.authorize(req);
    if (who instanceof Response) return who;
    if (this.rateLimitNext > 0) {
      this.rateLimitNext -= 1;
      return this.reply(429, "project_rate_limit", undefined, { "retry-after": "0" });
    }
    const mine = this.streams.get(who.project) ?? new Set<string>();
    this.streams.set(who.project, mine);
    if (req.method === "GET" && url.pathname === "/v1/streams") {
      // Offboarding's probes ask for one item, its walks for 1,000: a walk
      // ends at the first full-size request answered without a cursor.
      const limit = Number(url.searchParams.get("limit") ?? "100");
      const after = url.searchParams.get("cursor") ?? "";
      const names = [...mine].sort().filter((n) => n > after);
      const page = names.slice(0, Math.min(this.pageSize, limit));
      const more = names.length > page.length;
      const body = { streams: page.map((name) => ({ name, contentType: "application/json", sealed: false })) };
      const answer = this.reply(200, undefined, more ? { ...body, cursor: page[page.length - 1] } : body);
      if (!more && limit === 1000) {
        this.walks += 1;
        this.afterWalk?.(this.walks);
      }
      return answer;
    }
    if (req.method === "DELETE" && url.pathname.startsWith("/v1/streams/")) {
      const name = url.pathname.slice("/v1/streams/".length).split("/").map(decodeURIComponent).join("/");
      return mine.delete(name) ? new Response(null, { status: 204 }) : this.reply(404, "not_found");
    }
    return this.reply(404, "unknown_route");
  }
}
