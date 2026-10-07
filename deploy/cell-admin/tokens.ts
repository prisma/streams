// The cell's signing key and the tokens cell-admin mints with it.
//
// Key custody (PLAN section 5): the private key stays in ONE file the
// operator names on the command line. cell-admin never generates, copies,
// prints or logs it; it refuses a key file that group or others can read,
// and a message about a bad key file never quotes the file. Only the
// public half reaches the state and the keys feed, pinned to the customer
// audience (prisma-streams-data): a cell-admin cell has no key that can
// sign a fleet workload token (PLAN step 8, H6).
//
// Tokens live at most 24 h (MAX_TOKEN_LIFETIME_SECS, src/auth.rs): exp - iat
// never exceeds it. iat is backdated by 60 s so a cell whose clock trails
// the operator's by up to a minute does not refuse it as issued in the
// future; the usable lifetime is therefore ttl - 60 s.
import {
  createHash,
  createPrivateKey,
  createPublicKey,
  type KeyObject,
  randomBytes,
  sign,
} from "node:crypto";
import { readFileSync, statSync } from "node:fs";
import { AdminError } from "./errors";
import { validate } from "./feeds";
import type { CellState, KeyRecord } from "./state";

export const AUDIENCE = "prisma-streams-data";
export const MAX_TOKEN_LIFETIME_SECS = 24 * 3600;
export const MIN_TOKEN_TTL_SECS = 120;
const BACKDATE_SECS = 60;

export interface SigningKey {
  key: KeyObject;
  record: KeyRecord;
}

function checkCustody(path: string): void {
  let st: ReturnType<typeof statSync>;
  try {
    st = statSync(path);
  } catch {
    throw new AdminError(`key file ${path} cannot be read`);
  }
  if (!st.isFile()) throw new AdminError(`key file ${path} is not a regular file`);
  if ((st.mode & 0o077) !== 0) {
    const mode = (st.mode & 0o777).toString(8).padStart(4, "0");
    throw new AdminError(
      `key file ${path} has mode ${mode}: group or others can read the cell's signing key; chmod 600 it`,
    );
  }
  if (typeof process.getuid === "function" && st.uid !== process.getuid()) {
    throw new AdminError(`key file ${path} is not owned by the user running cell-admin`);
  }
}

/// The key id is the public key's fingerprint, so a kid can never name
/// two keys (the cell refuses a kid rebound to other material).
export function kidOf(alg: KeyRecord["alg"], publicKey: KeyObject): string {
  const der = publicKey.export({ type: "spki", format: "der" });
  const fp = createHash("sha256").update(der).digest("hex").slice(0, 16);
  return `streams-${alg === "RS256" ? "rs256" : "eddsa"}-${fp}`;
}

/// Load the signing key from `path` after the custody checks.
export function loadSigningKey(path: string): SigningKey {
  checkCustody(path);
  let key: KeyObject;
  try {
    key = createPrivateKey({ key: readFileSync(path), format: "pem" });
  } catch {
    throw new AdminError(`key file ${path} does not hold a PEM private key`);
  }
  let alg: KeyRecord["alg"];
  if (key.asymmetricKeyType === "rsa") {
    if ((key.asymmetricKeyDetails?.modulusLength ?? 0) < 2048) {
      throw new AdminError(`key file ${path} holds an RSA key shorter than 2048 bits`);
    }
    alg = "RS256";
  } else if (key.asymmetricKeyType === "ed25519") {
    alg = "EdDSA";
  } else {
    throw new AdminError(`key file ${path} holds a ${key.asymmetricKeyType} key; use RSA or Ed25519`);
  }
  const pub = createPublicKey(key);
  const pem = pub.export({ type: "spki", format: "pem" }).toString();
  return { key, record: { kid: kidOf(alg, pub), alg, aud: AUDIENCE, pem } };
}

/// The signing key, refused unless the cell's keys feed carries it.
export function cellSigningKey(state: CellState, path: string): SigningKey {
  const sk = loadSigningKey(path);
  if (!state.keys.some((k) => k.kid === sk.record.kid && k.pem === sk.record.pem)) {
    throw new AdminError(`key file ${path} is not a key of cell ${state.cell.cell_id}`);
  }
  return sk;
}

const b64u = (b: Buffer | string) => Buffer.from(b).toString("base64url");

export function signJwt(sk: SigningKey, claims: Record<string, unknown>): string {
  const header = b64u(JSON.stringify({ alg: sk.record.alg, typ: "JWT", kid: sk.record.kid }));
  const body = b64u(JSON.stringify(claims));
  const input = Buffer.from(`${header}.${body}`);
  const sig = sign(sk.record.alg === "RS256" ? "sha256" : null, input, sk.key);
  return `${header}.${body}.${b64u(sig)}`;
}

/// Customer-token claims for `credentialId` at its current grant and
/// ownership versions, schema-checked.
export function customerClaims(
  state: CellState,
  credentialId: string,
  ttlSecs: number,
  sub: string,
  nowSecs = Math.floor(Date.now() / 1000),
): Record<string, unknown> {
  if (!Number.isSafeInteger(ttlSecs) || ttlSecs < MIN_TOKEN_TTL_SECS || ttlSecs > MAX_TOKEN_LIFETIME_SECS) {
    throw new AdminError(
      `--ttl must be a whole number of seconds in ${MIN_TOKEN_TTL_SECS}..${MAX_TOKEN_LIFETIME_SECS} (24 h)`,
    );
  }
  const cred = state.credentials[credentialId];
  if (!cred) throw new AdminError(`credential ${credentialId} is not on this cell`);
  const project = state.projects[cred.project_id];
  const iat = nowSecs - BACKDATE_SECS;
  const claims = {
    iss: state.cell.issuer,
    aud: AUDIENCE,
    sub,
    credential_id: credentialId,
    project_id: cred.project_id,
    workspace_id: project.workspace_id,
    cell_id: state.cell.cell_id,
    ownership_version: project.ownership_version,
    grant_version: cred.grant_version,
    scope: cred.scopes,
    ...(cred.stream_prefixes ? { stream_prefixes: [...cred.stream_prefixes] } : {}),
    jti: randomBytes(16).toString("hex"),
    iat,
    nbf: iat,
    exp: iat + ttlSecs,
  };
  validate(claims, "customer-token-claims.schema.json", "token claims");
  return claims;
}
