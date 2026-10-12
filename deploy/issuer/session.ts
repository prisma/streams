// The management API's browser sessions (public-beta issuer triage B4.3;
// design §3.3). The cookie is built here because every management route
// (B3) takes one; signing in, which makes it, is B4's.
//
// A session is `{sid, user_id, email, workspace_id, iat}` in one cookie,
// base64url JSON and its HMAC-SHA-256 under STREAMS_ISSUER_SESSION_KEY:
// `__Host-streams-session=<payload>.<mac>`, HttpOnly, Secure,
// SameSite=Lax, 8 hours. Nothing is stored server-side: the issuer reads
// who is calling from the cookie alone, so rotating the key signs every
// user out. A state-changing call also sends the session's CSRF value in
// X-CSRF-Token: a page of another origin can neither read it nor set the
// header. Each MAC names its purpose, so a value made for one use is never
// valid for another (the sign-in's state cookie, B4, takes its own).
import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";

/// The `__Host-` prefix: the browser takes it only Secure, from this
/// origin, for Path=/, so no sibling domain can plant one.
export const SESSION_COOKIE = "__Host-streams-session";
export const SESSION_TTL_SECS = 8 * 3600;
/// A session issued this far in the future is still taken (clock skew).
const ISSUE_SKEW_SECS = 60;

const SID = /^[0-9a-f]{32}$/;
/// The feed's id grammar (src/tenant.rs); B4 refuses any other workspace.
const WORKSPACE = /^[A-Za-z0-9_-]{1,128}$/;

export interface Session {
  /// 128 random bits; the CSRF value derives from it.
  sid: string;
  user_id: string;
  email: string;
  workspace_id: string;
  /// Issued at, epoch seconds.
  iat: number;
}

const mac = (key: Buffer, purpose: string, text: string) =>
  createHmac("sha256", key).update(`${purpose}\n${text}`).digest();

/// A new session for a user signed in to `workspace_id`.
export function newSession(who: Omit<Session, "sid" | "iat">, nowMs: number): Session {
  return { sid: randomBytes(16).toString("hex"), ...who, iat: Math.floor(nowMs / 1000) };
}

/// The Set-Cookie value that carries `s`.
export function sessionCookie(key: Buffer, s: Session): string {
  const payload = Buffer.from(JSON.stringify(s)).toString("base64url");
  const tag = mac(key, "session", payload).toString("base64url");
  return `${SESSION_COOKIE}=${payload}.${tag}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_TTL_SECS}`;
}

/// What a state-changing call must send in X-CSRF-Token: the key's MAC
/// of the session id. The issuer's page reads it from the sign-in (B4).
export const csrfToken = (key: Buffer, sid: string) => mac(key, "csrf", sid).toString("base64url");

/// Whether `presented` is `sid`'s CSRF value, compared in constant time.
export function csrfMatches(key: Buffer, sid: string, presented: string | null): boolean {
  const want = Buffer.from(csrfToken(key, sid));
  const got = Buffer.from(presented ?? "");
  return got.length === want.length && timingSafeEqual(got, want);
}

/// The first value of cookie `name` in the request, or null.
function cookieValue(req: Request, name: string): string | null {
  for (const part of (req.headers.get("cookie") ?? "").split(";")) {
    const eq = part.indexOf("=");
    if (eq > 0 && part.slice(0, eq).trim() === name) return part.slice(eq + 1).trim();
  }
  return null;
}

function wellFormed(v: unknown): v is Session {
  const s = v as Session;
  return (
    typeof s === "object" && s !== null &&
    typeof s.sid === "string" && SID.test(s.sid) &&
    typeof s.user_id === "string" && s.user_id !== "" &&
    typeof s.email === "string" &&
    typeof s.workspace_id === "string" && WORKSPACE.test(s.workspace_id) &&
    Number.isSafeInteger(s.iat)
  );
}

/// The request's session: its cookie present, made under `key`, every
/// field in form and issued less than 8 hours ago; else null.
export function readSession(key: Buffer, req: Request, nowMs: number): Session | null {
  const value = cookieValue(req, SESSION_COOKIE);
  const dot = value?.indexOf(".") ?? -1;
  if (value === null || dot <= 0) return null;
  const [payload, tag] = [value.slice(0, dot), value.slice(dot + 1)];
  // The tag's text, not its decoding: base64url decoding skips stray
  // characters, and a cookie has one spelling.
  const want = Buffer.from(mac(key, "session", payload).toString("base64url"));
  const got = Buffer.from(tag);
  if (got.length !== want.length || !timingSafeEqual(got, want)) return null;
  let s: unknown;
  try {
    s = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
  } catch {
    return null;
  }
  if (!wellFormed(s)) return null;
  const nowSecs = Math.floor(nowMs / 1000);
  if (s.iat > nowSecs + ISSUE_SKEW_SECS || nowSecs >= s.iat + SESSION_TTL_SECS) return null;
  const { sid, user_id, email, workspace_id, iat } = s;
  return { sid, user_id, email, workspace_id, iat };
}
