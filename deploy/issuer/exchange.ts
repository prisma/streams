// POST /v1/token/streams (public-beta issuer triage B2.6; design §6.2;
// contracts/streams-platform/v1/management.openapi.yaml): a customer's
// durable credential, `Authorization: StreamsCredential
// prisma_streams_<credential id>.<secret>`, for a signed access token of
// the credential's project at its cell. Optional JSON body
// `{"requestedScopes": [...], "ttlSecs": n}`; the SDK sends none.
//
// Checks, in order: the rate limits (limits.ts: the instance's exchanges,
// then, once K4 names the edge's client-IP header, the address's
// exchanges and failures, and after the form the credential's own
// exchanges), all before any secret work; the credential's form; the
// body; the secret against the stored hash (only a customer credential
// with a secret has one: an operator's or the canary's catalog-read
// credential answers as an unknown id; a match under an older pepper is
// noted for the re-hash sweep, rehash.ts); the credential active, not
// omitted, not expired; its project active and not suspended; the state
// copy fresh; the signing key still the cell's. The token is cell-admin's
// customerClaims at the credential's current grant_version, its scopes
// narrowed to those asked for, signed with the issuer's key. The cell
// checks every claim again (src/auth.rs, verify_customer), so the
// exchange can at most mint what the credential grants. Every exchange is
// counted for the audit (audit.ts) under its outcome.
import { verifySecret } from "../cell-admin/secrets";
import type { CellState, CredentialRecord } from "../cell-admin/state";
import { customerClaims, signJwt } from "../cell-admin/tokens";
import { type Audit, type ExchangeOutcome, UNKNOWN_CREDENTIAL } from "./audit";
import { stateMismatch } from "./boot";
import type { StateCache } from "./cache";
import { type IssuerConfig, TOKEN_TTL_MIN_SECS } from "./config";
import { clientIp, type RateLimits, TRUSTED_CLIENT_IP_HEADER } from "./limits";
import type { Rehasher } from "./rehash";
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

export const rateLimited = (seconds: number, message: string) =>
  refuse(429, "rate_limited", `${message}; retry after ${seconds} s`, { "retry-after": String(seconds) });

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

export interface ExchangeDeps {
  cache: StateCache;
  limits: RateLimits;
  rehasher: Rehasher;
  audit: Audit;
  now?: () => number;
  /// The edge's trusted client-IP header (K4); null until it is known.
  ipHeader?: string | null;
}

interface Result {
  outcome: ExchangeOutcome;
  response: Response;
}

/// What the audit counts an exchange under, learnt as it proceeds.
interface Seen {
  credential: string;
  projectId?: string;
  jti?: string;
}

const result = (outcome: ExchangeOutcome, response: Response): Result => ({ outcome, response });

export class Exchange {
  private readonly cache: StateCache;
  private readonly limits: RateLimits;
  private readonly rehasher: Rehasher;
  private readonly audit: Audit;
  private readonly now: () => number;
  private readonly ipHeader: string | null;

  constructor(
    private readonly cfg: IssuerConfig,
    deps: ExchangeDeps,
  ) {
    this.cache = deps.cache;
    this.limits = deps.limits;
    this.rehasher = deps.rehasher;
    this.audit = deps.audit;
    this.now = deps.now ?? Date.now;
    this.ipHeader = deps.ipHeader === undefined ? TRUSTED_CLIENT_IP_HEADER : deps.ipHeader;
  }

  /// The limits taken before the credential is even parsed: a 429, or null.
  private admit(ip: string | null): Response | null {
    const all = this.limits.take("exchange_global", "*");
    if (all > 0) return rateLimited(all, "the issuer is at its exchange rate");
    if (ip === null) return null;
    const locked = this.limits.locked("exchange_failed_ip", ip);
    if (locked > 0) return rateLimited(locked, "too many failed exchanges from this address");
    const mine = this.limits.take("exchange_ip", ip);
    return mine > 0 ? rateLimited(mine, "too many exchanges from this address") : null;
  }

  /// A refused credential, counted against its address's failure budget.
  private failed(ip: string | null, message: string): Result {
    if (ip !== null) this.limits.charge("exchange_failed_ip", ip);
    return result("invalid_credential", unauthorized(message));
  }

  /// Answer one exchange and count it for the audit.
  async handle(req: Request): Promise<Response> {
    const seen: Seen = { credential: UNKNOWN_CREDENTIAL };
    const { outcome, response } = await this.attempt(req, seen);
    this.audit.exchange(outcome, seen.credential, seen.projectId, seen.jti);
    return response;
  }

  private async attempt(req: Request, seen: Seen): Promise<Result> {
    const ip = clientIp(req, this.ipHeader);
    const limited = this.admit(ip);
    if (limited !== null) return result("rate_limited", limited);
    const presented = PRESENTED.exec(req.headers.get("authorization") ?? "");
    if (!presented) return this.failed(ip, "send Authorization: StreamsCredential prisma_streams_<credential id>.<secret>");
    const [, id, secret] = presented;
    const state = this.cache.state;
    const cred = state.credentials[id];
    if (cred !== undefined) Object.assign(seen, { credential: id, projectId: cred.project_id });
    const wait = this.limits.take("exchange_credential", id);
    if (wait > 0) return result("rate_limited", rateLimited(wait, `too many exchanges for credential ${id}`));
    const ask = await readAsk(req, this.cfg.tokenTtlSecs);
    if (typeof ask === "string") return result("invalid_request", refuse(400, "invalid_request", ask));
    // The HMAC runs for an unknown id too: the answer's timing tells nothing.
    const pepper = verifySecret(this.cfg.peppers, cred, secret);
    if (pepper === null || cred === undefined) return this.failed(ip, "unknown credential or wrong secret");
    this.rehasher.note(id, cred, pepper, secret);
    const nowSecs = Math.floor(this.now() / 1000);
    const notActive = inactive(cred, id, nowSecs);
    if (notActive !== null) return result("credential_not_active", refuse(403, "credential_not_active", notActive));
    const projectNot = projectInactive(state, cred);
    if (projectNot !== null) return result("project_not_active", refuse(403, "project_not_active", projectNot));
    if (!this.cache.fresh()) {
      return result("issuer_unavailable", unavailable("the issuer cannot see the cell's current state; retry shortly"));
    }
    if (stateMismatch(this.cfg, state) !== null) {
      return result("issuer_unavailable", unavailable("the issuer's signing key is not the cell's; retry shortly"));
    }
    const scope = narrowed(cred.scopes, ask.scopes);
    if (scope === null) {
      return result("invalid_request", refuse(400, "invalid_request", "the credential grants none of requestedScopes"));
    }
    const claims: Record<string, unknown> = {
      ...customerClaims(state, id, ask.ttlSecs ?? this.cfg.tokenTtlSecs, `cred:${id}`, nowSecs),
      scope,
    };
    seen.jti = String(claims.jti);
    return result("issued", json(200, {
      accessToken: signJwt(this.cfg.signingKey, claims),
      tokenType: "Bearer",
      expiresIn: Number(claims.exp) - nowSecs,
      projectId: cred.project_id,
      cellId: state.cell.cell_id,
      endpoint: this.cfg.cellOrigin,
    }));
  }
}
