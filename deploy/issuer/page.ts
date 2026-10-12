// The issuer's one page (public-beta issuer triage B4.4, B4.6; design
// §3.3): page.html, a static file whose script and style are inline,
// compiled into the bundle as text. It signs a user in (signin.ts), shows
// the workspace's project, its endpoint, readiness and credentials,
// creates, rotates and revokes credentials (each secret shown once),
// deletes the project once its name is typed, gives the SDK snippet, and
// says who may manage (owner question I4 (a)); it has no admin control,
// and it says "the beta is at capacity" and "not on the allow-list" when
// the management API does.
//
// The browser runs only what the CSP names by hash: no other script or
// style, nothing from another origin, never framed, connections to this
// origin only, no form posts; and, where it enforces Trusted Types, no
// HTML written from a string (the script builds every element).
import { createHash } from "node:crypto";
import pageText from "./page.html" with { type: "text" };

// Bun's typings call every .html import an HTMLBundle; `with { type:
// "text" }` makes it the file's text.
const page = pageText as unknown as string;

/// The CSP source of the page's one inline `<tag>`.
function inlineHash(tag: string): string {
  const found = [...page.matchAll(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, "g"))];
  if (found.length !== 1) throw new Error(`page.html holds ${found.length} <${tag}> elements, not one`);
  return `'sha256-${createHash("sha256").update(found[0][1]).digest("base64")}'`;
}

export const PAGE_CSP = [
  "default-src 'none'",
  `script-src ${inlineHash("script")}`,
  `style-src ${inlineHash("style")}`,
  "connect-src 'self'",
  "base-uri 'none'",
  "form-action 'none'",
  "frame-ancestors 'none'",
  "require-trusted-types-for 'script'",
].join("; ");

/// GET /.
export function servePage(): Response {
  return new Response(page, {
    headers: {
      "content-type": "text/html; charset=utf-8",
      "content-security-policy": PAGE_CSP,
      "cache-control": "no-store",
      "x-content-type-options": "nosniff",
      "x-frame-options": "DENY",
      "referrer-policy": "no-referrer",
      "cross-origin-opener-policy": "same-origin",
    },
  });
}
