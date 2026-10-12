// The interim issuer's inputs (public-beta issuer triage B2.2, and B4's
// STREAMS_ISSUER_ORIGIN and STREAMS_ISSUER_OAUTH_AUTHORITY). The entry
// reads exactly the names below from its environment and nothing else: no
// COMPOSER_*, no SLATE_*, no BIN_*. In one Compute project every service
// sees every variable (deploy/README.md: "Compute env vars are
// project-scoped and merged"), so reading only these names is how the
// issuer keeps out of what is not its own. A refusal names the input,
// never its value.
import { AdminError } from "../cell-admin/errors";
import { BUNDLE_IN_FEEDS_BUCKET, type S3Target } from "../cell-admin/s3store";
import { type Pepper, parsePeppers } from "../cell-admin/secrets";
import { loadSigningKeyFromPem, MAX_TOKEN_LIFETIME_SECS, type SigningKey } from "../cell-admin/tokens";

/// Owner question I5, answer (a) (the build default): an access token
/// lives 60 minutes unless STREAMS_ISSUER_TOKEN_TTL_SECS says otherwise.
export const TOKEN_TTL_DEFAULT_SECS = 3600;
/// The shortest token a request may ask for.
export const TOKEN_TTL_MIN_SECS = 300;
/// Where "Sign in with Prisma" authorizes and where its /v1/me is: the
/// Management API SDK's defaults (@prisma/management-api-sdk 1.82.0).
/// STREAMS_ISSUER_OAUTH_AUTHORITY names one origin for both instead (the
/// mock identity provider, mock-idp.ts, for prisma dev and the tests).
export const PRISMA_AUTH_ORIGIN = "https://auth.prisma.io";
export const PRISMA_API_ORIGIN = "https://api.prisma.io";
/// Owner question I10 (the build default: awake): the issuer holds
/// Compute's keep-awake guard unless KEEP_AWAKE=0.
export const KEEP_AWAKE_DEFAULT = "1";

const SESSION_KEY_BYTES = 32;
const METRICS_TOKEN_MIN_CHARS = 32;
const ID = /^[A-Za-z0-9_-]{1,128}$/;

const S3_FIELDS = {
  endpoint: "ENDPOINT",
  bucket: "BUCKET",
  region: "REGION",
  accessKeyId: "ACCESS_KEY_ID",
  secretAccessKey: "SECRET_ACCESS_KEY",
} as const satisfies Record<keyof S3Target, string>;

const s3Names = (prefix: string) => Object.values(S3_FIELDS).map((f) => `${prefix}_${f}`);

/// Every name the entry reads. Secrets among them are deleted from the
/// process environment once parsed (hygiene, not a control: the values
/// stay in /proc/self/environ and in a core dump).
export const INPUTS = [
  "STREAMS_CELL_ID",
  "STREAMS_ISSUER_ID",
  "STREAMS_CELL_ORIGIN",
  "STREAMS_ISSUER_ORIGIN",
  "STREAMS_ISSUER_SIGNING_KEY",
  "STREAMS_ISSUER_PEPPER",
  "STREAMS_ISSUER_SESSION_KEY",
  "STREAMS_ISSUER_OAUTH_CLIENT_ID",
  "STREAMS_ISSUER_OAUTH_CLIENT_SECRET",
  "STREAMS_ISSUER_OAUTH_AUTHORITY",
  "STREAMS_ISSUER_METRICS_TOKEN",
  "STREAMS_ISSUER_TOKEN_TTL_SECS",
  ...s3Names("STREAMS_CONTROL_S3"),
  // Owner question D3 (a): the bundle lives in its own feeds bucket.
  ...(BUNDLE_IN_FEEDS_BUCKET ? s3Names("STREAMS_FEEDS_S3") : []),
  // The platform's: the port Compute serves, and the keep-awake flag.
  "PORT",
  "KEEP_AWAKE",
] as const;

export const SECRET_INPUTS = [
  "STREAMS_ISSUER_SIGNING_KEY",
  "STREAMS_ISSUER_PEPPER",
  "STREAMS_ISSUER_SESSION_KEY",
  "STREAMS_ISSUER_OAUTH_CLIENT_SECRET",
  "STREAMS_ISSUER_METRICS_TOKEN",
  "STREAMS_CONTROL_S3_SECRET_ACCESS_KEY",
  "STREAMS_FEEDS_S3_SECRET_ACCESS_KEY",
] as const;

export interface IssuerConfig {
  cellId: string;
  issuerId: string;
  /// Where customers send their tokens: the exchange's `endpoint`.
  cellOrigin: string;
  /// Where the issuer is served; the sign-in's redirect_uri is
  /// `<issuerOrigin>/auth/callback`, as the OAuth client registers it.
  issuerOrigin: string;
  signingKey: SigningKey;
  /// Newest first; credentials are hashed under the first.
  peppers: Pepper[];
  sessionKey: Buffer;
  oauthClientId: string;
  oauthClientSecret?: string;
  /// The authorization server (/authorize, /token) and the Management
  /// API (/v1/me) the sign-in calls.
  authOrigin: string;
  apiOrigin: string;
  metricsToken: string;
  tokenTtlSecs: number;
  control: S3Target;
  feeds: S3Target;
  port: number;
  keepAwake: boolean;
}

export type Env = Readonly<Record<string, string | undefined>>;

function required(env: Env, name: string): string {
  const v = env[name];
  if (v === undefined || v === "") throw new AdminError(`${name} is required`);
  return v;
}

const optional = (env: Env, name: string) => (env[name] === "" ? undefined : env[name]);

function wholeNumber(env: Env, name: string, dflt: number, min: number, max: number): number {
  const raw = optional(env, name);
  if (raw === undefined) return dflt;
  const n = Number(raw);
  if (!/^[0-9]+$/.test(raw) || !Number.isSafeInteger(n) || n < min || n > max) {
    throw new AdminError(`${name} is not a whole number in ${min}..${max}`);
  }
  return n;
}

/// An origin: https, or http to this machine (development only), with no
/// path. The cell's (tokens are only ever sent to it), the issuer's own
/// (the sign-in's callback) and the authorization server's.
function origin(env: Env, name: string): string {
  return originOf(name, required(env, name));
}

function originOf(name: string, raw: string): string {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw new AdminError(`${name} is not a URL`);
  }
  const local = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
  if (url.protocol !== "https:" && !(url.protocol === "http:" && local)) {
    throw new AdminError(`${name} must be an https origin`);
  }
  if (url.pathname !== "/" || url.search !== "" || url.hash !== "") {
    throw new AdminError(`${name} must be an origin, without a path`);
  }
  return url.origin;
}

function sessionKey(env: Env): Buffer {
  const raw = required(env, "STREAMS_ISSUER_SESSION_KEY");
  const key = /^[A-Za-z0-9+/_-]+={0,2}$/.test(raw) ? Buffer.from(raw, "base64") : Buffer.alloc(0);
  if (key.length !== SESSION_KEY_BYTES) {
    throw new AdminError(`STREAMS_ISSUER_SESSION_KEY must be ${SESSION_KEY_BYTES} bytes in base64`);
  }
  return key;
}

function target(env: Env, prefix: string): S3Target {
  return Object.fromEntries(
    Object.entries(S3_FIELDS).map(([field, suffix]) => [field, required(env, `${prefix}_${suffix}`)]),
  ) as unknown as S3Target;
}

/// The issuer's configuration from `env`, or a refusal naming the input.
export function readConfig(env: Env): IssuerConfig {
  const cellId = required(env, "STREAMS_CELL_ID");
  if (!ID.test(cellId)) throw new AdminError(`STREAMS_CELL_ID is not a valid id (${ID.source})`);
  const metricsToken = required(env, "STREAMS_ISSUER_METRICS_TOKEN");
  if (metricsToken.length < METRICS_TOKEN_MIN_CHARS) {
    throw new AdminError(`STREAMS_ISSUER_METRICS_TOKEN is shorter than ${METRICS_TOKEN_MIN_CHARS} characters`);
  }
  const keepAwake = optional(env, "KEEP_AWAKE") ?? KEEP_AWAKE_DEFAULT;
  if (keepAwake !== "0" && keepAwake !== "1") throw new AdminError("KEEP_AWAKE is 0 or 1");
  const control = target(env, "STREAMS_CONTROL_S3");
  const oauthClientSecret = optional(env, "STREAMS_ISSUER_OAUTH_CLIENT_SECRET");
  const authority = optional(env, "STREAMS_ISSUER_OAUTH_AUTHORITY");
  const mock = authority === undefined ? undefined : originOf("STREAMS_ISSUER_OAUTH_AUTHORITY", authority);
  return {
    cellId,
    issuerId: required(env, "STREAMS_ISSUER_ID"),
    cellOrigin: origin(env, "STREAMS_CELL_ORIGIN"),
    issuerOrigin: origin(env, "STREAMS_ISSUER_ORIGIN"),
    signingKey: loadSigningKeyFromPem(required(env, "STREAMS_ISSUER_SIGNING_KEY"), "STREAMS_ISSUER_SIGNING_KEY"),
    peppers: parsePeppers(required(env, "STREAMS_ISSUER_PEPPER"), "STREAMS_ISSUER_PEPPER"),
    sessionKey: sessionKey(env),
    oauthClientId: required(env, "STREAMS_ISSUER_OAUTH_CLIENT_ID"),
    ...(oauthClientSecret !== undefined ? { oauthClientSecret } : {}),
    authOrigin: mock ?? PRISMA_AUTH_ORIGIN,
    apiOrigin: mock ?? PRISMA_API_ORIGIN,
    metricsToken,
    tokenTtlSecs: wholeNumber(env, "STREAMS_ISSUER_TOKEN_TTL_SECS", TOKEN_TTL_DEFAULT_SECS, TOKEN_TTL_MIN_SECS,
      MAX_TOKEN_LIFETIME_SECS),
    control,
    feeds: BUNDLE_IN_FEEDS_BUCKET ? target(env, "STREAMS_FEEDS_S3") : control,
    port: wholeNumber(env, "PORT", 8080, 0, 65535),
    keepAwake: keepAwake === "1",
  };
}
