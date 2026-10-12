// "Sign in with Prisma" (public-beta issuer triage B4.1, B4.2; design §3.1
// option 1 and §3.3; owner question I3, answer (1), the recommended one):
// the OAuth 2.0 authorization code grant with PKCE (S256, RFC 7636)
// against Prisma's authorization server, then one GET /v1/me with the
// access token, which the issuer then drops: it is never stored, logged or
// sent to the browser. `offline_access` is never asked for, so no refresh
// token exists either.
//
// The wire shapes are @prisma/management-api-sdk 1.82.0's (getLoginUrl,
// handleCallback, getV1Me), written out here rather than imported: the
// SDK's handleCallback refuses a token answer without a refresh_token,
// which only offline_access brings, hands the tokens to a store, and reads
// the workspace out of the access token as a JWT, a format nothing
// documents (review R1.10a); it would also bring openapi-fetch into a
// bundle that has no npm dependency (B2.1).
//
// A sign-in's state and PKCE verifier ride in a cookie the session key
// MACs for this purpose only, for 10 minutes (`__Host-streams-signin`). A
// state is taken once per instance, so a callback replayed with its
// cookie never reaches the authorization server twice.
import { createHash, randomBytes, timingSafeEqual } from "node:crypto";
import type { IssuerConfig } from "./config";
import { cookieValue, seal, unseal, WORKSPACE_ID } from "./session";

/// Owner question I4, answer (a) (the build default): the consent screen
/// asks for workspace:admin, the only scope the Management API SDK
/// documents, until Prisma Auth names a narrower one that still answers
/// /v1/me (team ask C2).
export const OAUTH_SCOPE = "workspace:admin";
export const SIGNIN_COOKIE = "__Host-streams-signin";
export const SIGNIN_TTL_SECS = 600;
export const CALLBACK_PATH = "/auth/callback";
const OUTBOUND_TIMEOUT_MS = 10_000;
const MAX_USER_ID_CHARS = 256;
const MAX_EMAIL_CHARS = 254;

/// Who a finished sign-in proves manages a workspace.
export interface Manager {
  userId: string;
  email: string;
  workspaceId: string;
}

/// The issuer's identity layer, one interface (B4.1): the manager a
/// request proves. "Sign in with Prisma" proves one at its callback; a
/// Console-signed assertion (I3 (2)) would prove one per request.
export interface ManagerVerifier {
  verifyManager(req: Request): Promise<Manager>;
}

/// Why a sign-in is refused; the page words each.
export type SignInRefusal = "denied" | "expired" | "failed" | "not_oauth" | "no_user" | "no_workspace" | "invalid_workspace";

export class SignInRefused extends Error {
  constructor(
    readonly code: SignInRefusal,
    message: string,
  ) {
    super(message);
  }
}

/// What the sign-in's cookie carries from its start to its callback.
interface Pending {
  state: string;
  verifier: string;
  /// Epoch seconds.
  exp: number;
}

const isPending = (v: unknown): v is Pending => {
  const p = v as Pending;
  return typeof p === "object" && p !== null && typeof p.state === "string" && typeof p.verifier === "string" &&
    Number.isSafeInteger(p.exp);
};

const sameText = (a: string, b: string) => {
  const [x, y] = [Buffer.from(a), Buffer.from(b)];
  return x.length === y.length && timingSafeEqual(x, y);
};

/// RFC 6749 §2.3.1: HTTP Basic's user and password are form-encoded first.
const formEncoded = (v: string) => new URLSearchParams({ v }).toString().slice(2);

/// An OAuth error code as the authorization server sent it, or "".
const oauthCode = (v: unknown) => (typeof v === "string" && /^[a-z_]{1,64}$/.test(v) ? ` ${v}` : "");

const failed = (message: string) => new SignInRefused("failed", message);

export class PrismaSignIn implements ManagerVerifier {
  /// States taken, until their cookie expires (epoch ms).
  private readonly taken = new Map<string, number>();

  constructor(
    private readonly cfg: IssuerConfig,
    private readonly now: () => number,
  ) {}

  private get redirectUri(): string {
    return `${this.cfg.issuerOrigin}${CALLBACK_PATH}`;
  }

  /// Where a browser goes to sign in, and the cookie it carries back.
  begin(): { location: string; cookie: string } {
    const pending: Pending = {
      state: randomBytes(16).toString("base64url"),
      verifier: randomBytes(32).toString("base64url"),
      exp: Math.floor(this.now() / 1000) + SIGNIN_TTL_SECS,
    };
    const url = new URL(`${this.cfg.authOrigin}/authorize`);
    url.searchParams.set("response_type", "code");
    url.searchParams.set("client_id", this.cfg.oauthClientId);
    url.searchParams.set("redirect_uri", this.redirectUri);
    url.searchParams.set("scope", OAUTH_SCOPE);
    url.searchParams.set("state", pending.state);
    url.searchParams.set("code_challenge", createHash("sha256").update(pending.verifier).digest("base64url"));
    url.searchParams.set("code_challenge_method", "S256");
    const sealed = seal(this.cfg.sessionKey, "signin", pending);
    return {
      location: url.toString(),
      cookie: `${SIGNIN_COOKIE}=${sealed}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SIGNIN_TTL_SECS}`,
    };
  }

  /// The manager the callback `req` proves: its state is the one its
  /// cookie carries, the authorization server gives an access token for
  /// its code and verifier, and /v1/me names an OAuth user of a workspace
  /// whose id the cell takes. Else SignInRefused.
  async verifyManager(req: Request): Promise<Manager> {
    const q = new URL(req.url).searchParams;
    const pending = this.take(req, q.get("state"));
    const error = q.get("error");
    if (error !== null) {
      throw new SignInRefused(error === "access_denied" ? "denied" : "failed",
        `the authorization server answered${oauthCode(error) || " an error"}`);
    }
    const code = q.get("code");
    if (code === null || code === "") throw failed("the authorization server sent no code");
    return this.me(await this.token(code, pending.verifier));
  }

  /// The sign-in `req`'s cookie carries, when it is the issuer's, current,
  /// for `state` and not taken before; taken now. Else "expired".
  private take(req: Request, state: string | null): Pending {
    const p = unseal(this.cfg.sessionKey, "signin", cookieValue(req, SIGNIN_COOKIE));
    const now = this.now();
    if (!isPending(p) || now >= p.exp * 1000 || state === null || !sameText(state, p.state) || this.taken.has(p.state)) {
      throw new SignInRefused("expired", "no current sign-in for this state");
    }
    for (const [s, until] of this.taken) if (until <= now) this.taken.delete(s);
    this.taken.set(p.state, p.exp * 1000);
    return p;
  }

  private async token(code: string, verifier: string): Promise<string> {
    const headers: Record<string, string> = { "content-type": "application/x-www-form-urlencoded", accept: "application/json" };
    if (this.cfg.oauthClientSecret !== undefined) {
      const pair = `${formEncoded(this.cfg.oauthClientId)}:${formEncoded(this.cfg.oauthClientSecret)}`;
      headers.authorization = `Basic ${Buffer.from(pair).toString("base64")}`;
    }
    const body = new URLSearchParams({
      grant_type: "authorization_code",
      code,
      redirect_uri: this.redirectUri,
      client_id: this.cfg.oauthClientId,
      code_verifier: verifier,
    });
    let res: Response;
    try {
      res = await fetch(`${this.cfg.authOrigin}/token`, { method: "POST", headers, body, signal: AbortSignal.timeout(OUTBOUND_TIMEOUT_MS) });
    } catch (e) {
      throw failed(`the token endpoint could not be reached: ${(e as Error).message}`);
    }
    const data = (await res.json().catch(() => undefined)) as { access_token?: unknown; error?: unknown } | undefined;
    if (res.status !== 200 || typeof data?.access_token !== "string" || data.access_token === "") {
      throw failed(`the token endpoint answered ${res.status}${oauthCode(data?.error)}`);
    }
    return data.access_token;
  }

  /// /v1/me's principal for `accessToken`, as a manager.
  private async me(accessToken: string): Promise<Manager> {
    let res: Response;
    try {
      res = await fetch(`${this.cfg.apiOrigin}/v1/me`, {
        headers: { authorization: `Bearer ${accessToken}`, accept: "application/json" },
        signal: AbortSignal.timeout(OUTBOUND_TIMEOUT_MS),
      });
    } catch (e) {
      throw failed(`/v1/me could not be reached: ${(e as Error).message}`);
    }
    const body = (await res.json().catch(() => undefined)) as { data?: MeData } | undefined;
    if (res.status !== 200 || typeof body?.data !== "object" || body.data === null) throw failed(`/v1/me answered ${res.status}`);
    return managerOf(body.data);
  }
}

/// What /v1/me answers (the SDK's getV1Me), as far as the issuer reads it.
interface MeData {
  user?: { id?: unknown; email?: unknown } | null;
  workspace?: { id?: unknown } | null;
  credential?: { type?: unknown } | null;
}

/// The manager /v1/me's `d` names: a signed-in user's OAuth grant, a user
/// and a workspace (review R1.10b), the workspace's id in the cell's
/// grammar; else SignInRefused.
function managerOf(d: MeData): Manager {
  const type = d.credential?.type;
  if (type !== "oauth") {
    throw new SignInRefused("not_oauth", `/v1/me names a ${typeof type === "string" ? type : "missing"} credential, not a user's sign-in`);
  }
  const { user, workspace } = d;
  if (
    typeof user?.id !== "string" || user.id === "" || user.id.length > MAX_USER_ID_CHARS ||
    typeof user.email !== "string" || user.email.length > MAX_EMAIL_CHARS
  ) {
    throw new SignInRefused("no_user", "/v1/me names no user");
  }
  if (typeof workspace?.id !== "string") throw new SignInRefused("no_workspace", "/v1/me names no workspace");
  if (!WORKSPACE_ID.test(workspace.id)) {
    throw new SignInRefused("invalid_workspace",
      `workspace id ${JSON.stringify(workspace.id.slice(0, 140))} is outside the cell's grammar ${WORKSPACE_ID.source}`);
  }
  return { userId: user.id, email: user.email, workspaceId: workspace.id };
}
