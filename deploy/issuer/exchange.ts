// POST /v1/token/streams (public-beta issuer triage B2.6; design §6.2;
// contracts/streams-platform/v1/management.openapi.yaml): a customer's
// durable credential, `Authorization: StreamsCredential
// prisma_streams_<credential id>.<secret>`, for a signed access token of
// the credential's project at its cell. Optional JSON body
// `{"requestedScopes": [...], "ttlSecs": n}`; the SDK sends none.
//
// Checks, in order: the credential's form; the body; the secret against
// the stored hash (only a customer credential with a secret has one: an
// operator's or the canary's catalog-read credential answers as an
// unknown id); the credential active, not omitted, not expired; its
// project active and not suspended; the state copy fresh; the signing key
// still the cell's. The token is cell-admin's customerClaims at the
// credential's current grant_version, its scopes narrowed to those asked
// for, signed with the issuer's key. The cell checks every claim again
// (src/auth.rs, verify_customer), so the exchange can at most mint what
// the credential grants.
import { verifySecret } from "../cell-admin/secrets";
import type { CellState, CredentialRecord } from "../cell-admin/state";
import { customerClaims, signJwt } from "../cell-admin/tokens";
import { stateMismatch } from "./boot";
import type { StateCache } from "./cache";
import { type IssuerConfig, TOKEN_TTL_MIN_SECS } from "./config";
import { json, refuse } from "./respond";

/// The presented credential: a cell-admin credential id (`strcred_` and
/// 32 hex) and a 32-byte base64url secret.
const PRESENTED = /^StreamsCredential prisma_streams_(strcred_[0-9a-f]{32})\.([A-Za-z0-9_-]{43})$/;
const MAX_BODY_BYTES = 4096;
/// The issuer cannot mint now; a retry after this many seconds may.
export const UNAVAILABLE_RETRY_SECS = 5;

interface Ask {
  scopes?: string[];
  ttlSecs?: number;
}

const unauthorized = (message: string) =>
  refuse(401, "invalid_credential", message, { "www-authenticate": 'StreamsCredential realm="streams"' });

export const unavailable = (message: string) =>
  refuse(503, "issuer_unavailable", message, { "retry-after": String(UNAVAILABLE_RETRY_SECS) });

/// The request's body as what it asks for, or why it is refused.
async function readAsk(req: Request, maxTtlSecs: number): Promise<Ask | string> {
  if (Number(req.headers.get("content-length") ?? 0) > MAX_BODY_BYTES) {
    return `the body is larger than ${MAX_BODY_BYTES} bytes`;
  }
  const text = await req.text();
  if (text.length > MAX_BODY_BYTES) return `the body is larger than ${MAX_BODY_BYTES} bytes`;
  if (text.trim() === "") return {};
  let body: unknown;
  try {
    body = JSON.parse(text);
  } catch {
    return "the body is not JSON";
  }
  if (typeof body !== "object" || body === null || Array.isArray(body)) return "the body is not a JSON object";
  const { requestedScopes, ttlSecs, ...rest } = body as Record<string, unknown>;
  const [unknown] = Object.keys(rest);
  if (unknown !== undefined) return `unknown member ${JSON.stringify(unknown)}: the body takes requestedScopes and ttlSecs`;
  const ask: Ask = {};
  if (requestedScopes !== undefined) {
    if (!Array.isArray(requestedScopes) || requestedScopes.length === 0 || requestedScopes.some((s) => typeof s !== "string")) {
      return "requestedScopes is a non-empty array of scope names";
    }
    ask.scopes = requestedScopes as string[];
  }
  if (ttlSecs !== undefined) {
    if (typeof ttlSecs !== "number" || !Number.isSafeInteger(ttlSecs) || ttlSecs < TOKEN_TTL_MIN_SECS || ttlSecs > maxTtlSecs) {
      return `ttlSecs is a whole number of seconds in ${TOKEN_TTL_MIN_SECS}..${maxTtlSecs}`;
    }
    ask.ttlSecs = ttlSecs;
  }
  return ask;
}

/// The granted scopes the request asked for, in the grant's order; all of
/// them when it asked for none; null when it asked only for others.
function narrowed(granted: string, requested: string[] | undefined): string | null {
  if (requested === undefined) return granted;
  const want = new Set(requested);
  const kept = granted.split(" ").filter((s) => want.has(s));
  return kept.length > 0 ? kept.join(" ") : null;
}

/// Why `cred` mints nothing now, or null.
function inactive(cred: CredentialRecord, id: string, nowSecs: number): string | null {
  if (cred.status !== "active" || cred.omitted) return `credential ${id} is revoked`;
  if (cred.expires_at !== null && cred.expires_at <= nowSecs) return `credential ${id} has expired`;
  return null;
}

/// Why `cred`'s project takes no token now, or null.
function projectInactive(state: CellState, cred: CredentialRecord): string | null {
  const p = state.projects[cred.project_id];
  if (!p || p.phase !== "active") return `project ${cred.project_id} is being deleted`;
  if (p.status !== "active") return `project ${cred.project_id} is suspended`;
  return null;
}

export class Exchange {
  constructor(
    private readonly cfg: IssuerConfig,
    private readonly cache: StateCache,
    private readonly now: () => number = Date.now,
  ) {}

  async handle(req: Request): Promise<Response> {
    const presented = PRESENTED.exec(req.headers.get("authorization") ?? "");
    if (!presented) return unauthorized("send Authorization: StreamsCredential prisma_streams_<credential id>.<secret>");
    const [, id, secret] = presented;
    const ask = await readAsk(req, this.cfg.tokenTtlSecs);
    if (typeof ask === "string") return refuse(400, "invalid_request", ask);
    const state = this.cache.state;
    const cred = state.credentials[id];
    // The HMAC runs for an unknown id too: the answer's timing tells nothing.
    const pepper = verifySecret(this.cfg.peppers, cred, secret);
    if (pepper === null || cred === undefined) return unauthorized("unknown credential or wrong secret");
    const nowSecs = Math.floor(this.now() / 1000);
    const notActive = inactive(cred, id, nowSecs);
    if (notActive !== null) return refuse(403, "credential_not_active", notActive);
    const projectNot = projectInactive(state, cred);
    if (projectNot !== null) return refuse(403, "project_not_active", projectNot);
    if (!this.cache.fresh()) return unavailable("the issuer cannot see the cell's current state; retry shortly");
    if (stateMismatch(this.cfg, state) !== null) return unavailable("the issuer's signing key is not the cell's; retry shortly");
    const scope = narrowed(cred.scopes, ask.scopes);
    if (scope === null) return refuse(400, "invalid_request", "the credential grants none of requestedScopes");
    const claims: Record<string, unknown> = {
      ...customerClaims(state, id, ask.ttlSecs ?? this.cfg.tokenTtlSecs, `cred:${id}`, nowSecs),
      scope,
    };
    return json(200, {
      accessToken: signJwt(this.cfg.signingKey, claims),
      tokenType: "Bearer",
      expiresIn: Number(claims.exp) - nowSecs,
      projectId: cred.project_id,
      cellId: state.cell.cell_id,
      endpoint: this.cfg.cellOrigin,
    });
  }
}
