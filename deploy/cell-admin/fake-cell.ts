// A fake cell for offboard.test.ts: it answers the product routes
// offboarding uses the way the server does (src/product.rs
// auth_failure_response and product_list, the verification order of
// src/auth.rs verify_customer). e2e.test.ts runs the same flows against
// the real binary.
import type { Bundle } from "./feeds";
import { verifiedClaims } from "./test-rig";

/// A fake cell: it serves the bundle the test publishes to it (or, with
/// `follow`, the one the store holds at each request), and keeps each
/// project's streams. Hooks let a test add a stream between walks or
/// rate-limit a request once.
export class FakeCell {
  live?: Bundle;
  streams = new Map<string, Set<string>>();
  pageSize = 2;
  /// Every request, with the credential of its token when its signature
  /// verified.
  requests: { method: string; path: string; status: number; credential?: string }[] = [];
  /// The claims of every token whose signature verified, in order.
  tokens: any[] = [];
  afterWalk?: (walks: number) => void;
  rateLimitNext = 0;
  /// The cell's wrapper and refresh without delay, when set: each request
  /// is answered on the bundle it returns at that moment.
  follow?: () => Bundle;
  /// The cell's clock in milliseconds (a test's clock): a token's and a
  /// credential's expiry and the store bucket's refill read it.
  now: () => number = () => Date.now();
  /// Phase B's store bucket (P1 (b′)), per project, when set: a listing
  /// of N streams weighs N + 10 and a delete 11, debited after it is
  /// served; both are refused 429 project_rate_limit while the level is
  /// below 0, with Retry-After from the debt. The level refills at
  /// `perSec` on the cell's clock and holds at most one second of it.
  storeOps?: { perSec: number };
  private levels = new Map<string, { level: number; at: number }>();
  private walks = 0;
  /// The credential of the request being answered, when its token verified.
  private credential?: string;
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

  /// verify_customer's order: signature, project placed, ownership, the
  /// project's status (before any credential, so a suspended project's
  /// every token is refused, an operator's too), credential known, its
  /// project, its status, its expiry, its grant_version; then the
  /// catalog's scope.
  private authorize(req: Request): { project: string } | Response {
    if (this.follow) this.publish(this.follow());
    const bearer = req.headers.get("authorization")?.replace(/^Bearer /, "") ?? "";
    const claims = this.live && verifiedClaims(this.live, bearer);
    if (!this.live || !claims) return this.reply(401, "bad_signature");
    this.tokens.push(claims);
    this.credential = claims.credential_id;
    if (claims.exp + 30 <= this.now() / 1000) return this.reply(401, "expired");
    const policy = (this.live.policies.projects as any[]).find((p) => p.project_id === claims.project_id);
    if (!policy) return this.reply(421, "wrong_cell");
    if (claims.ownership_version !== policy.ownership_version) return this.reply(401, "ownership_version_mismatch");
    if (policy.status !== "active") return this.reply(403, "project_not_active");
    const cred = (this.live.grants.credentials as any[]).find((c) => c.credential_id === claims.credential_id);
    if (!cred) return this.reply(401, "credential_unknown");
    if (cred.project_id !== claims.project_id) return this.reply(401, "credential_project_mismatch");
    if (cred.status !== "active") return this.reply(403, "credential_not_active");
    if (cred.expires_at !== null && cred.expires_at <= Math.floor(this.now() / 1000)) {
      return this.reply(401, "credential_expired");
    }
    if (cred.grant_version !== claims.grant_version) return this.reply(401, "grant_version_mismatch");
    // The catalog needs catalog reads, in the token and in the grant.
    const granted = (scopes: string) => scopes.split(" ").includes("streams.catalog.read");
    if (new URL(req.url).pathname === "/v1/streams" && !(granted(claims.scope) && granted(cred.scopes))) {
      return this.reply(403, "missing_scope");
    }
    return { project: claims.project_id };
  }

  private answer(req: Request): Response {
    const url = new URL(req.url);
    this.credential = undefined;
    const r = this.route(req, url);
    const credential = this.credential === undefined ? {} : { credential: this.credential };
    this.requests.push({ method: req.method, path: url.pathname + url.search, status: r.status, ...credential });
    return r;
  }

  /// The project's store bucket refilled to now, or undefined when off.
  private storeLevel(project: string): { level: number; at: number } | undefined {
    if (!this.storeOps) return undefined;
    const { perSec } = this.storeOps;
    const now = this.now();
    const b = this.levels.get(project) ?? { level: perSec, at: now };
    b.level = Math.min(perSec, b.level + ((now - b.at) / 1000) * perSec);
    b.at = now;
    this.levels.set(project, b);
    return b;
  }

  private route(req: Request, url: URL): Response {
    const who = this.authorize(req);
    if (who instanceof Response) return who;
    if (this.rateLimitNext > 0) {
      this.rateLimitNext -= 1;
      return this.reply(429, "project_rate_limit", undefined, { "retry-after": "0" });
    }
    const store = this.storeLevel(who.project);
    if (store && store.level < 0) {
      const wait = Math.max(1, Math.ceil(-store.level / this.storeOps!.perSec));
      return this.reply(429, "project_rate_limit", undefined, { "retry-after": String(wait) });
    }
    const { response, weight } = this.serve(req, url, who.project);
    if (store) store.level -= weight;
    return response;
  }

  /// The product route, and its store weight under P1 (b′).
  private serve(req: Request, url: URL, project: string): { response: Response; weight: number } {
    const mine = this.streams.get(project) ?? new Set<string>();
    this.streams.set(project, mine);
    if (req.method === "GET" && url.pathname === "/v1/streams") {
      // Offboarding's probes ask for one item, its walks for more: a walk
      // ends at the first walk request answered without a cursor.
      const limit = Number(url.searchParams.get("limit") ?? "100");
      const after = url.searchParams.get("cursor") ?? "";
      const names = [...mine].sort().filter((n) => n > after);
      const page = names.slice(0, Math.min(this.pageSize, limit));
      const more = names.length > page.length;
      const body = { streams: page.map((name) => ({ name, contentType: "application/json", sealed: false })) };
      const answer = this.reply(200, undefined, more ? { ...body, cursor: page[page.length - 1] } : body);
      if (!more && limit > 1) {
        this.walks += 1;
        this.afterWalk?.(this.walks);
      }
      return { response: answer, weight: page.length + 10 };
    }
    if (req.method === "DELETE" && url.pathname.startsWith("/v1/streams/")) {
      const name = url.pathname.slice("/v1/streams/".length).split("/").map(decodeURIComponent).join("/");
      return { response: mine.delete(name) ? new Response(null, { status: 204 }) : this.reply(404, "not_found"), weight: 11 };
    }
    return { response: this.reply(404, "unknown_route"), weight: 0 };
  }
}
