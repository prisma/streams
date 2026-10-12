// "Sign in with Prisma" (public-beta issuer triage B4.1-B4.3): OAuth 2.0
// with PKCE against the authorization server, one /v1/me, the allow-list,
// then the issuer's session. Against the mock identity provider
// (mock-idp.ts). Run with `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { AdminError } from "../cell-admin/errors";
import { cleanup, fakeS3, run } from "../cell-admin/test-rig";
import { readConfig } from "./config";
import { MockIdp, type MockPrincipal } from "./mock-idp";
import { envWith, ISSUER_ORIGIN, type IssuerCell, issuerCell, managed } from "./rig";
import { readSession } from "./session";

const idps: MockIdp[] = [];
afterEach(() => {
  for (const idp of idps.splice(0)) idp.stop();
  cleanup();
});

const CLIENT = "streams-beta-test";
const CALLBACK = `${ISSUER_ORIGIN}/auth/callback`;
const SIGNIN_COOKIE = /^__Host-streams-signin=[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}; Path=\/; HttpOnly; Secure; SameSite=Lax; Max-Age=600$/;
const CLEARED = "__Host-streams-signin=; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=0";

async function allow(cell: IssuerCell, workspace: string): Promise<void> {
  const r = await run("allow", "--state", cell.state, "--workspace", workspace);
  if (r.code !== 0) throw new Error(`allow: ${r.err}`);
}

/// An issuer signing in through a mock identity provider of its own.
async function withIdp(cell: IssuerCell, idpOpts: ConstructorParameters<typeof MockIdp>[0] = {}, env: Record<string, string> = {}) {
  const idp = new MockIdp({ clientId: CLIENT, ...idpOpts });
  idps.push(idp);
  const m = await managed(cell, envWith(cell, { STREAMS_ISSUER_OAUTH_AUTHORITY: idp.url, ...env }));
  return { idp, m };
}

type Managed = Awaited<ReturnType<typeof managed>>;

/// The browser's first step: /auth/login, which sends it to the
/// authorization server with a state cookie.
async function start(m: Managed) {
  const res = await m.get("/auth/login");
  expect(res.status).toBe(303);
  const [cookie] = res.headers.getSetCookie();
  return { res, cookie, authorize: new URL(res.headers.get("location")!) };
}

/// The authorization server's answer: where it sends the browser back.
async function authorized(authorize: URL): Promise<URL> {
  return new URL((await fetch(authorize, { redirect: "manual" })).headers.get("location")!);
}

/// The browser back at the issuer's callback, carrying `cookie`.
const callback = (m: Managed, back: URL, cookie?: string) => m.get(back.pathname + back.search, cookie?.split(";")[0]);

/// A whole sign-in; the callback's answer.
async function signIn(m: Managed): Promise<Response> {
  const s = await start(m);
  return callback(m, await authorized(s.authorize), s.cookie);
}

const cookiesOf = (res: Response) => res.headers.getSetCookie();

/// The callback's answer: a redirect to the page, with the sign-in's
/// state cookie cleared and, when it signed in, a session cookie.
function refusedWith(res: Response, code: string) {
  expect([res.status, res.headers.get("location"), cookiesOf(res)]).toEqual([303, `/?signin=${code}`, [CLEARED]]);
}

describe("signing in with Prisma", () => {
  test("sends the browser to the authorization server with PKCE, then holds a session of the user's allow-listed workspace", async () => {
    const cell = await issuerCell();
    await allow(cell, "ws_dev");
    const { idp, m } = await withIdp(cell);
    const s = await start(m);
    expect([s.res.headers.get("cache-control"), s.res.headers.get("referrer-policy")]).toEqual(["no-store", "no-referrer"]);
    expect(cookiesOf(s.res).length).toBe(1);
    expect(s.cookie).toMatch(SIGNIN_COOKIE);
    const q = Object.fromEntries(s.authorize.searchParams);
    expect([s.authorize.origin + s.authorize.pathname, Object.keys(q).sort()]).toEqual([
      `${idp.url}/authorize`,
      ["client_id", "code_challenge", "code_challenge_method", "redirect_uri", "response_type", "scope", "state"],
    ]);
    expect(q).toEqual({
      response_type: "code",
      client_id: CLIENT,
      redirect_uri: CALLBACK,
      scope: "workspace:admin",
      state: expect.stringMatching(/^[A-Za-z0-9_-]{22}$/),
      code_challenge: expect.stringMatching(/^[A-Za-z0-9_-]{43}$/),
      code_challenge_method: "S256",
    });
    const back = await authorized(s.authorize);
    const res = await callback(m, back, s.cookie);
    expect([res.status, res.headers.get("location"), res.headers.get("cache-control"), res.headers.get("referrer-policy")]).toEqual([
      303, "/", "no-store", "no-referrer",
    ]);
    expect(await res.text()).toBe("");
    const [session, cleared, ...rest] = cookiesOf(res);
    expect([cleared, rest]).toEqual([CLEARED, []]);
    expect(session).toMatch(/^__Host-streams-session=[^;]+; Path=\/; HttpOnly; Secure; SameSite=Lax; Max-Age=28800$/);
    const cookie = session.split(";")[0];
    expect(readSession(m.cfg.sessionKey, new Request(ISSUER_ORIGIN, { headers: { cookie } }), m.clock.t)).toEqual({
      sid: expect.stringMatching(/^[0-9a-f]{32}$/),
      user_id: "usr_dev",
      email: "dev@example.com",
      workspace_id: "ws_dev",
      iat: Math.floor(m.clock.t / 1000),
    });
    // The access token went no further than /v1/me.
    expect(cookiesOf(res).some((c) => c.includes("mock_at_"))).toBe(false);
    expect([idp.calls, idp.scopesAsked]).toEqual([{ authorize: 1, token: 1, me: 1 }, ["workspace:admin"]]);
    const listed = await m.get("/v1/workspaces/ws_dev/streams/projects", cookie);
    expect([listed.status, await listed.json()]).toEqual([200, { projects: [] }]);
  });

  test("refuses a callback whose state is missing, forged, another sign-in's, older than 10 minutes or used, without calling the authorization server", async () => {
    const cell = await issuerCell();
    await allow(cell, "ws_dev");
    const { idp, m } = await withIdp(cell);
    const a = await start(m);
    const aBack = await authorized(a.authorize);
    const b = await start(m);
    const [value, attrs] = [a.cookie.split(";")[0], a.cookie.slice(a.cookie.indexOf(";"))];
    const forged = `${value.slice(0, -1)}${value.endsWith("A") ? "B" : "A"}${attrs}`;
    for (const cookie of [undefined, forged, b.cookie]) refusedWith(await callback(m, aBack, cookie), "expired");
    // The code's verifier is in the state cookie: without it no code is spent.
    expect(idp.calls.token).toBe(0);
    expect((await callback(m, aBack, a.cookie)).headers.get("location")).toBe("/");
    // A state is good once.
    refusedWith(await callback(m, aBack, a.cookie), "expired");
    const c = await start(m);
    const cBack = await authorized(c.authorize);
    m.clock.t += 600_000;
    refusedWith(await callback(m, cBack, c.cookie), "expired");
    expect(idp.calls.token).toBe(1);
  });

  test("refuses an account that /v1/me does not show as an OAuth user of a workspace whose id the cell takes", async () => {
    const cell = await issuerCell();
    const { idp, m } = await withIdp(cell);
    const user = idp.principal.user;
    const cases: [Partial<MockPrincipal>, string][] = [
      [{ credentialType: "service_token", user: null }, "not_oauth"],
      [{ user: null }, "no_user"],
      [{ user: { id: "", email: "a@example.com", name: null } }, "no_user"],
      [{ workspace: null }, "no_workspace"],
      [{ workspace: { id: "ws dev", name: "Spaced" } }, "invalid_workspace"],
      [{ workspace: { id: "w".repeat(129), name: "Long" } }, "invalid_workspace"],
    ];
    for (const [patch, code] of cases) {
      idp.principal = { user, workspace: { id: "ws_dev", name: "Dev" }, credentialType: "oauth", ...patch };
      refusedWith(await signIn(m), code);
    }
    expect(idp.calls.me).toBe(cases.length);
  });

  test("refuses a workspace off the beta's allow-list, and answers unavailable while the allow-list cannot be read", async () => {
    const cell = await issuerCell();
    const { m } = await withIdp(cell);
    refusedWith(await signIn(m), "workspace_not_allowed");
    await allow(cell, "ws_dev");
    m.clock.t += 5_001;
    expect((await signIn(m)).headers.get("location")).toBe("/");
    m.clock.t += 5_001;
    fakeS3().failReads = true;
    refusedWith(await signIn(m), "unavailable");
  });

  test("answers a declined consent denied, and a refused code, an authorization error or a /v1/me outage failed", async () => {
    const cell = await issuerCell();
    await allow(cell, "ws_dev");
    const { idp, m } = await withIdp(cell);
    idp.decision = "deny";
    refusedWith(await signIn(m), "denied");
    idp.decision = "approve";
    const s = await start(m);
    const back = await authorized(s.authorize);
    back.searchParams.set("code", "not-the-code");
    refusedWith(await callback(m, back, s.cookie), "failed");
    const e = await start(m);
    const errBack = new URL(CALLBACK);
    errBack.searchParams.set("error", "server_error");
    errBack.searchParams.set("state", e.authorize.searchParams.get("state")!);
    refusedWith(await callback(m, errBack, e.cookie), "failed");
    idp.meFails = true;
    refusedWith(await signIn(m), "failed");
  });

  test("authenticates with the client secret by HTTP Basic when it has one", async () => {
    const cell = await issuerCell();
    await allow(cell, "ws_dev");
    const good = await withIdp(cell, { clientSecret: "s3cret:x" }, { STREAMS_ISSUER_OAUTH_CLIENT_SECRET: "s3cret:x" });
    expect((await signIn(good.m)).headers.get("location")).toBe("/");
    const bad = await withIdp(cell, { clientSecret: "s3cret:x" }, { STREAMS_ISSUER_OAUTH_CLIENT_SECRET: "other" });
    refusedWith(await signIn(bad.m), "failed");
    const none = await withIdp(cell, { clientSecret: "s3cret:x" });
    refusedWith(await signIn(none.m), "failed");
  });

  test("starts 30 sign-ins a minute, then answers 429 rate_limited with Retry-After", async () => {
    const cell = await issuerCell();
    const { m } = await withIdp(cell);
    for (let i = 0; i < 30; i++) expect((await m.get("/auth/login")).status).toBe(303);
    const res = await m.get("/auth/login");
    expect([res.status, res.headers.get("retry-after"), await res.json()]).toEqual([
      429, "2", { error: { code: "rate_limited", message: "too many sign-ins are starting; retry after 2 s" } },
    ]);
    expect(m.issuer.limits.refused.oauth_start).toBe(1);
    for (const path of ["/auth/login", "/auth/callback"]) {
      const res = await m.call("POST", path);
      expect([res.status, res.headers.get("allow")]).toEqual([405, "GET"]);
    }
  });
});

describe("the sign-in's inputs", () => {
  test("take the issuer's own origin for the callback, and Prisma's authorization server unless an authority is named", async () => {
    const cell = await issuerCell();
    const cfg = readConfig(cell.env);
    expect([cfg.issuerOrigin, cfg.authOrigin, cfg.apiOrigin]).toEqual([ISSUER_ORIGIN, "https://auth.prisma.io", "https://api.prisma.io"]);
    const local = readConfig(envWith(cell, { STREAMS_ISSUER_OAUTH_AUTHORITY: "http://localhost:9600", STREAMS_ISSUER_ORIGIN: "http://localhost:8080" }));
    expect([local.issuerOrigin, local.authOrigin, local.apiOrigin]).toEqual([
      "http://localhost:8080", "http://localhost:9600", "http://localhost:9600",
    ]);
    const refusals: [Record<string, string | undefined>, string][] = [
      [{ STREAMS_ISSUER_ORIGIN: undefined }, "STREAMS_ISSUER_ORIGIN is required"],
      [{ STREAMS_ISSUER_ORIGIN: "http://issuer.example.com" }, "STREAMS_ISSUER_ORIGIN must be an https origin"],
      [{ STREAMS_ISSUER_ORIGIN: `${ISSUER_ORIGIN}/beta` }, "STREAMS_ISSUER_ORIGIN must be an origin, without a path"],
      [{ STREAMS_ISSUER_OAUTH_AUTHORITY: "auth.prisma.io" }, "STREAMS_ISSUER_OAUTH_AUTHORITY is not a URL"],
    ];
    for (const [patch, message] of refusals) {
      expect(() => readConfig(envWith(cell, patch))).toThrow(new AdminError(message));
    }
  });
});
