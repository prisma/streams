// The issuer's one page and what it reads of the session (public-beta
// issuer triage B4.3, B4.4, B4.6). Run with `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { cleanup, run } from "../cell-admin/test-rig";
import { issuerCell, managed } from "./rig";
import { csrfToken, readSession } from "./session";

afterEach(cleanup);

const hash = (text: string) => `'sha256-${createHash("sha256").update(text).digest("base64")}'`;

/// The page's inline elements `tag`: [attributes, text] each.
const inline = (html: string, tag: string) =>
  [...html.matchAll(new RegExp(`<${tag}\\b([^>]*)>([\\s\\S]*?)</${tag}>`, "g"))].map((m) => [m[1], m[2]]);

async function page() {
  const m = await managed(await issuerCell());
  const res = await m.get("/");
  return { m, res, html: await res.text() };
}

describe("the issuer's page", () => {
  test("is one static page whose only script and style the CSP names by hash, framed by no one, loading nothing from elsewhere", async () => {
    const { res, html } = await page();
    expect(res.status).toBe(200);
    expect(Object.fromEntries(["content-type", "cache-control", "x-content-type-options", "x-frame-options", "referrer-policy",
      "cross-origin-opener-policy"].map((h) => [h, res.headers.get(h)]))).toEqual({
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
      "x-content-type-options": "nosniff",
      "x-frame-options": "DENY",
      "referrer-policy": "no-referrer",
      "cross-origin-opener-policy": "same-origin",
    });
    const scripts = inline(html, "script");
    const styles = inline(html, "style");
    expect([scripts.length, styles.length, scripts[0][0], styles[0][0]]).toEqual([1, 1, "", ""]);
    expect(res.headers.get("content-security-policy")).toBe(
      `default-src 'none'; script-src ${hash(scripts[0][1])}; style-src ${hash(styles[0][1])}; connect-src 'self'; ` +
        "base-uri 'none'; form-action 'none'; frame-ancestors 'none'; require-trusted-types-for 'script'",
    );
    // Nothing from another origin, no element that loads or posts, no
    // inline handler, and no HTML written from strings.
    expect(html).not.toMatch(/https?:\/\//);
    expect(html).not.toMatch(/<(link|iframe|object|embed|img|form|base|meta http-equiv)\b|\s(src|srcset|action|on[a-z]+)=/i);
    expect(scripts[0][1]).not.toMatch(/innerHTML|outerHTML|insertAdjacentHTML|document\.write|\beval\(|new Function|setTimeout\(\s*["'`]/);
    // Its calls are the issuer's own routes.
    for (const route of ["/auth/session", "/auth/login", "/v1/workspaces/", "/streams/projects", "/v1/projects/", "/streams/credentials"]) {
      expect(scripts[0][1]).toContain(`"${route}`);
    }
  });

  test("words every sign-in refusal, the beta at capacity and a workspace off the allow-list, and says who may manage", async () => {
    const { html } = await page();
    const [[, script]] = inline(html, "script");
    for (const code of ["denied", "expired", "failed", "not_oauth", "no_user", "no_workspace", "invalid_workspace",
      "workspace_not_allowed", "unavailable"]) {
      expect(script).toMatch(new RegExp(`\\n\\s+${code}: "[^"]+",\\n`));
    }
    expect(script).toContain("is not on the Prisma Streams beta's allow-list");
    expect(script).toContain("The Prisma Streams beta is at capacity");
    expect(html).toContain("There is no role check during the beta");
    // The SDK snippet: the credential in the environment, this issuer's origin.
    expect(script).toContain("StreamsClient.connect({");
    expect(script).toContain("issuerUrl: '\" + location.origin + \"',");
    // A project is deleted only once its name is typed.
    expect(script).toContain("button.disabled = input.value !== p.displayName;");
  });

  test("answers another method 405", async () => {
    const { m } = await page();
    const res = await m.call("POST", "/");
    expect([res.status, res.headers.get("allow")]).toEqual([405, "GET"]);
  });
});

describe("GET /auth/session", () => {
  test("tells the page who is signed in and the CSRF value its changes send", async () => {
    const cell = await issuerCell();
    const m = await managed(cell);
    const cookie = m.signIn("ws_a");
    const res = await m.get("/auth/session", cookie);
    const sid = readSession(m.cfg.sessionKey, new Request("https://issuer.example.com/", { headers: { cookie } }), m.clock.t)!.sid;
    expect([res.status, res.headers.get("cache-control"), res.headers.get("x-content-type-options"), await res.json()]).toEqual([
      200, "no-store", "nosniff",
      { userId: "usr_ws_a", email: "owner@ws_a.example.com", workspaceId: "ws_a", csrfToken: csrfToken(m.cfg.sessionKey, sid) },
    ]);
    expect(res.headers.get("access-control-allow-origin")).toBeNull();
    // The value it gives is the one a change needs.
    expect((await run("allow", "--state", cell.state, "--workspace", "ws_a")).code).toBe(0);
    const { csrfToken: token } = await (await m.get("/auth/session", cookie)).json();
    const created = await m.call("POST", "/v1/workspaces/ws_a/streams/projects", cookie, {
      body: JSON.stringify({ displayName: "app" }),
      headers: { "content-type": "application/json", "x-csrf-token": token },
    });
    expect(created.status).toBe(201);
  });

  test("answers 401 without a session, and takes from the session's budget", async () => {
    const m = await managed(await issuerCell());
    const res = await m.get("/auth/session");
    expect([res.status, await res.json()]).toEqual([
      401, { error: { code: "unauthorized", message: "sign in with Prisma to manage Streams projects" } },
    ]);
    const cookie = m.signIn("ws_a");
    for (let i = 0; i < 60; i++) expect((await m.get("/auth/session", cookie)).status).toBe(200);
    expect((await m.get("/auth/session", cookie)).status).toBe(429);
    expect((await m.call("POST", "/auth/session", cookie)).status).toBe(405);
  });
});
