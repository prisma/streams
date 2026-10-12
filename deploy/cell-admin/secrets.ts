// Customer credential secrets (public-beta issuer triage B2.5; design
// §6.1), shared by `cell-admin credential` and the interim issuer.
//
// A secret is 32 random bytes in base64url, presented as
// `prisma_streams_<credential id>.<secret>` (MULTITENANCY §3.2). The state
// stores HMAC-SHA-256(pepper, secret) in base64url with the pepper's
// version `hv`, and the secret's last 6 characters as a hint; never the
// secret. A 256-bit random secret cannot be guessed offline, so a keyed
// fast hash replaces a password-grade one (the owner's cryptographic
// acceptance, question I2): a slow hash would hand every caller of the
// exchange a CPU lever.
//
// Peppers are versioned, newest first: `v<N>:<base64>[,v<M>:<base64>]...`
// with N > M, each at least 32 bytes. A credential is hashed under the
// newest; an older one still verifies until its credentials are re-hashed.
import { createHmac, randomBytes } from "node:crypto";
import { readFileSync } from "node:fs";
import { AdminError } from "./errors";
import { checkCustody } from "./tokens";

export interface Pepper {
  version: number;
  key: Buffer;
}

export const SECRET_BYTES = 32;
const PEPPER_MIN_BYTES = 32;
const HINT_CHARS = 6;

/// The peppers of `text`, newest first; `what` names their source in a
/// refusal, which never quotes a pepper.
export function parsePeppers(text: string, what: string): Pepper[] {
  const peppers = text
    .trim()
    .split(",")
    .map((item, i) => {
      const m = /^v([1-9][0-9]{0,8}):([A-Za-z0-9+/_-]+={0,2})$/.exec(item.trim());
      if (!m) throw new AdminError(`${what}: entry ${i + 1} is not v<N>:<base64>`);
      const key = Buffer.from(m[2], "base64");
      if (key.length < PEPPER_MIN_BYTES) {
        throw new AdminError(`${what}: pepper v${m[1]} is shorter than ${PEPPER_MIN_BYTES} bytes`);
      }
      return { version: Number(m[1]), key };
    });
  for (let i = 1; i < peppers.length; i++) {
    if (peppers[i].version >= peppers[i - 1].version) {
      throw new AdminError(`${what}: versions must fall from the newest, first, to the oldest`);
    }
  }
  return peppers;
}

/// The peppers in a file only its owner can read.
export function readPepperFile(path: string): Pepper[] {
  checkCustody(path, "pepper file", "the pepper");
  return parsePeppers(readFileSync(path, "utf8"), `pepper file ${path}`);
}

/// A new secret: 32 random bytes, base64url (43 characters).
export const newSecret = () => randomBytes(SECRET_BYTES).toString("base64url");

/// What the customer holds and presents.
export const presentedSecret = (credentialId: string, secret: string) => `prisma_streams_${credentialId}.${secret}`;

/// The stored hash of `secret` under `pepper`, base64url.
export const hashSecret = (pepper: Pepper, secret: string) =>
  createHmac("sha256", pepper.key).update(secret).digest("base64url");

/// The fields a credential record stores for `secret`, under the newest
/// pepper.
export function secretFields(peppers: readonly Pepper[], secret: string): { secret_hash: string; hv: number; hint: string } {
  const [newest] = peppers;
  return { secret_hash: hashSecret(newest, secret), hv: newest.version, hint: secret.slice(-HINT_CHARS) };
}
