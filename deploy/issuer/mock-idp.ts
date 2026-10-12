// A mock of "Sign in with Prisma" for `prisma dev` and the issuer's
// tests (public-beta issuer triage B4.5): the three endpoints the issuer's
// sign-in calls (signin.ts), as @prisma/management-api-sdk 1.82.0 calls
// them and its OpenAPI typings describe them, on one origin. The issuer
// is pointed at it with STREAMS_ISSUER_OAUTH_AUTHORITY=<its origin>.
//
//   GET  /authorize  response_type=code, client_id, redirect_uri, scope,
//                    state, code_challenge, code_challenge_method=S256:
//                    approves at once as its principal and redirects to
//                    redirect_uri?code=…&state=… (or ?error=access_denied
//                    while it is set to deny). A client or redirect_uri it
//                    cannot take is answered 400 in place, as RFC 6749
//                    §4.1.2.1 says.
//   POST /token      grant_type=authorization_code, code, redirect_uri,
//                    client_id, code_verifier, and the client's secret
//                    by HTTP Basic when it has one: a code is good once,
//                    for 60 s, for the redirect_uri and client it was
//                    issued to and the verifier whose SHA-256 is its
//                    challenge. {access_token, token_type, expires_in,
//                    scope}; a refresh_token only for `offline_access`.
//   GET  /v1/me      Bearer <access token>: {data: {user, workspace,
//                    credential}}, the principal when the token was issued.
//
//   bun deploy/issuer/mock-idp.ts [--port 9600] [--client-id ID]
//     [--client-secret S] [--user-id ID] [--email E] [--workspace ID]
//     [--workspace-name N]
//
// Development only: it signs anyone in as its principal.
import { createHash, randomBytes, timingSafeEqual } from "node:crypto";

export interface MockPrincipal {
  user: { id: string; email: string; name: string | null } | null;
  workspace: { id: string; name: string } | null;
  credentialType: "oauth" | "service_token" | "management_token" | "agent_token";
}

export interface MockIdpOptions {
  /// The one client it serves; any client when unset.
  clientId?: string;
  /// The client's secret, asked for by HTTP Basic at /token when set.
  clientSecret?: string;
  principal?: MockPrincipal;
  port?: number;
  hostname?: string;
  now?: () => number;
}

const CODE_TTL_MS = 60_000;
const TOKEN_TTL_SECS = 3600;

interface Grant {
  clientId: string;
  redirectUri: string;
  challenge: string;
  scope: string;
  principal: MockPrincipal;
  at: number;
}

const json = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", "cache-control": "no-store" } });

const oauthError = (status: number, error: string, description: string) =>
  json(status, { error, error_description: description });

const same = (a: string, b: string) => {
  const [x, y] = [Buffer.from(a), Buffer.from(b)];
  return x.length === y.length && timingSafeEqual(x, y);
};

export class MockIdp {
  principal: MockPrincipal;
  /// "deny": every authorization is declined, as a user who says no.
  decision: "approve" | "deny" = "approve";
  /// true: /v1/me answers 500, as an API outage would.
  meFails = false;
  readonly calls = { authorize: 0, token: 0, me: 0 };
  /// The scope of every authorization asked, in order.
  readonly scopesAsked: string[] = [];
  private readonly codes = new Map<string, Grant>();
  private readonly tokens = new Map<string, MockPrincipal>();
  private readonly now: () => number;
  private readonly server: ReturnType<typeof Bun.serve>;

  constructor(private readonly opts: MockIdpOptions = {}) {
    this.principal = opts.principal ?? {
      user: { id: "usr_dev", email: "dev@example.com", name: "Dev User" },
      workspace: { id: "ws_dev", name: "Dev Workspace" },
      credentialType: "oauth",
    };
    this.now = opts.now ?? Date.now;
    this.server = Bun.serve({
      port: opts.port ?? 0,
      hostname: opts.hostname ?? "127.0.0.1",
      fetch: (req) => this.answer(req),
    });
  }

  get url(): string {
    return `http://${this.opts.hostname ?? "127.0.0.1"}:${this.server.port}`;
  }

  stop(): void {
    this.server.stop(true);
  }

  private async answer(req: Request): Promise<Response> {
    const url = new URL(req.url);
    if (url.pathname === "/authorize" && req.method === "GET") return this.authorize(url.searchParams);
    if (url.pathname === "/token" && req.method === "POST") return this.token(req);
    if (url.pathname === "/v1/me" && req.method === "GET") return this.me(req);
    return json(404, { error: { code: "not_found", message: `no route ${req.method} ${url.pathname}` } });
  }

  private authorize(q: URLSearchParams): Response {
    this.calls.authorize += 1;
    const clientId = q.get("client_id") ?? "";
    const redirectUri = q.get("redirect_uri") ?? "";
    let back: URL;
    try {
      back = new URL(redirectUri);
    } catch {
      return oauthError(400, "invalid_request", "redirect_uri is not a URL");
    }
    if (clientId === "" || (this.opts.clientId !== undefined && clientId !== this.opts.clientId)) {
      return oauthError(400, "unauthorized_client", "unknown client_id");
    }
    const scope = q.get("scope") ?? "";
    this.scopesAsked.push(scope);
    const state = q.get("state");
    if (state !== null) back.searchParams.set("state", state);
    const challenge = q.get("code_challenge") ?? "";
    const refusal =
      q.get("response_type") !== "code"
        ? "unsupported_response_type"
        : q.get("code_challenge_method") !== "S256" || !/^[A-Za-z0-9_-]{43}$/.test(challenge)
          ? "invalid_request"
          : this.decision === "deny"
            ? "access_denied"
            : null;
    if (refusal !== null) {
      back.searchParams.set("error", refusal);
    } else {
      const code = randomBytes(16).toString("base64url");
      this.codes.set(code, { clientId, redirectUri, challenge, scope, principal: structuredClone(this.principal), at: this.now() });
      back.searchParams.set("code", code);
    }
    return new Response(null, { status: 302, headers: { location: back.toString(), "cache-control": "no-store" } });
  }

  /// Whether the request authenticates `clientId` as RFC 6749 §2.3.1's
  /// HTTP Basic asks, when the client has a secret.
  private clientAuthenticated(req: Request, clientId: string): boolean {
    if (this.opts.clientId !== undefined && clientId !== this.opts.clientId) return false;
    if (this.opts.clientSecret === undefined) return true;
    const basic = /^Basic ([A-Za-z0-9+/=]+)$/.exec(req.headers.get("authorization") ?? "");
    if (!basic) return false;
    const [id, ...secret] = Buffer.from(basic[1], "base64").toString("utf8").split(":");
    return decodeURIComponent(id) === clientId && same(decodeURIComponent(secret.join(":")), this.opts.clientSecret);
  }

  private async token(req: Request): Promise<Response> {
    this.calls.token += 1;
    const form = new URLSearchParams(await req.text());
    if (form.get("grant_type") !== "authorization_code") {
      return oauthError(400, "unsupported_grant_type", "only authorization_code");
    }
    const clientId = form.get("client_id") ?? "";
    if (!this.clientAuthenticated(req, clientId)) return oauthError(401, "invalid_client", "client authentication failed");
    const code = form.get("code") ?? "";
    const grant = this.codes.get(code);
    this.codes.delete(code);
    const verifier = form.get("code_verifier") ?? "";
    if (
      grant === undefined ||
      this.now() - grant.at > CODE_TTL_MS ||
      grant.clientId !== clientId ||
      grant.redirectUri !== form.get("redirect_uri") ||
      !same(createHash("sha256").update(verifier).digest("base64url"), grant.challenge)
    ) {
      return oauthError(400, "invalid_grant", "unknown, used or expired code");
    }
    const accessToken = `mock_at_${randomBytes(24).toString("base64url")}`;
    this.tokens.set(accessToken, grant.principal);
    const offline = grant.scope.split(" ").includes("offline_access");
    return json(200, {
      access_token: accessToken,
      token_type: "Bearer",
      expires_in: TOKEN_TTL_SECS,
      scope: grant.scope,
      ...(offline ? { refresh_token: `mock_rt_${randomBytes(24).toString("base64url")}` } : {}),
    });
  }

  private me(req: Request): Response {
    this.calls.me += 1;
    const bearer = /^Bearer (\S+)$/.exec(req.headers.get("authorization") ?? "")?.[1];
    const p = bearer === undefined ? undefined : this.tokens.get(bearer);
    if (p === undefined) return json(401, { error: { code: "unauthorized", message: "missing or invalid authorization token" } });
    if (this.meFails) return json(500, { error: { code: "internal", message: "mock outage" } });
    return json(200, { data: { user: p.user, workspace: p.workspace, credential: { type: p.credentialType, id: null, name: null } } });
  }
}

/// The flags' values: `--name value` pairs.
function flags(argv: string[]): Map<string, string> {
  const out = new Map<string, string>();
  for (let i = 0; i < argv.length; i += 2) {
    if (!argv[i].startsWith("--") || argv[i + 1] === undefined) throw new Error(`expected --flag value, got ${argv[i]}`);
    out.set(argv[i].slice(2), argv[i + 1]);
  }
  return out;
}

if (import.meta.main) {
  const f = flags(Bun.argv.slice(2));
  const idp = new MockIdp({
    clientId: f.get("client-id"),
    clientSecret: f.get("client-secret"),
    port: Number(f.get("port") ?? 9600),
    principal: {
      user: { id: f.get("user-id") ?? "usr_dev", email: f.get("email") ?? "dev@example.com", name: "Dev User" },
      workspace: { id: f.get("workspace") ?? "ws_dev", name: f.get("workspace-name") ?? "Dev Workspace" },
      credentialType: "oauth",
    },
  });
  console.log(`mock-idp: signing everyone in as ${idp.principal.user?.email} to ${idp.principal.workspace?.id}; ` +
    `STREAMS_ISSUER_OAUTH_AUTHORITY=${idp.url}`);
}
