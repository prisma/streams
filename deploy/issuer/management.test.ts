// The management API (public-beta issuer triage B3): a signed-in
// workspace's projects and credentials. Run with `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { cleanup, run } from "../cell-admin/test-rig";
import { CELL, CELL_ORIGIN, customerSecret, type IssuerCell, issuerCell, managed } from "./rig";
import { readSession, SESSION_COOKIE, SESSION_TTL_SECS, sessionCookie } from "./session";

afterEach(cleanup);

const answer = async (res: Response): Promise<[number, any]> => [res.status, await res.json()];
const refusal = (code: string, message: string) => ({ error: { code, message } });

async function allow(cell: IssuerCell, workspace: string, ...extra: string[]): Promise<void> {
  const r = await run("allow", "--state", cell.state, "--workspace", workspace, ...extra);
  if (r.code !== 0) throw new Error(`allow: ${r.err}`);
}

describe("the management API's sessions", () => {
  test("lists the signed-in workspace's projects, and none of another workspace's or the canary", async () => {
    const cell = await issuerCell();
    await customerSecret(cell, "sp_a");
    await customerSecret(cell, "sp_b");
    await allow(cell, "ws_sp_a");
    const m = await managed(cell);
    const res = await m.get("/v1/workspaces/ws_sp_a/streams/projects", m.signIn("ws_sp_a"));
    expect(res.headers.get("cache-control")).toBe("no-store");
    expect(res.headers.get("x-request-id")).toMatch(/^[0-9a-f]{16}$/);
    const state = m.cache.state;
    expect(await answer(res)).toEqual([200, {
      projects: [{
        id: "sp_a",
        workspaceId: "ws_sp_a",
        displayName: "sp_a",
        status: "active",
        createdAt: state.projects.sp_a.admitted_at,
        cellId: CELL,
        endpoint: CELL_ORIGIN,
        quotas: state.projects.sp_a.quotas,
      }],
    }]);
  });

  test("refuses a request without a valid session 401, whatever is wrong with the cookie", async () => {
    const cell = await issuerCell();
    await allow(cell, "ws_a");
    const m = await managed(cell);
    const good = m.signIn("ws_a");
    const value = good.split("=")[1];
    const [payload, tag] = value.split(".");
    const forged = Buffer.from(JSON.stringify({ ...JSON.parse(Buffer.from(payload, "base64url").toString()), workspace_id: "ws_b" }))
      .toString("base64url");
    const expired = m.signIn("ws_a", { iat: Math.floor(m.clock.t / 1000) - SESSION_TTL_SECS });
    const otherKey = sessionCookie(randomBytes(32), m.session("ws_a")).split(";")[0];
    const cases = [
      undefined,
      "",
      `${SESSION_COOKIE}=`,
      `${SESSION_COOKIE}=${payload}`,
      `${SESSION_COOKIE}=${forged}.${tag}`,
      `${SESSION_COOKIE}=${payload}.${tag.slice(1)}x`,
      `other=${value}`,
      expired,
      otherKey,
    ];
    for (const cookie of cases) {
      expect(await answer(await m.get("/v1/workspaces/ws_a/streams/projects", cookie))).toEqual([
        401, refusal("unauthorized", "sign in with Prisma to manage Streams projects"),
      ]);
    }
    expect((await m.get("/v1/workspaces/ws_a/streams/projects", `a=b; ${good}; c=d`)).status).toBe(200);
    // A session lives 8 hours from its issue.
    const old = m.signIn("ws_a", { iat: Math.floor(m.clock.t / 1000) - SESSION_TTL_SECS + 1 });
    expect((await m.get("/v1/workspaces/ws_a/streams/projects", old)).status).toBe(200);
  });

  test("reads a session only from a cookie the issuer's key made, with every field in form", () => {
    const key = randomBytes(32);
    const now = 1_800_000_000_000;
    const s = { sid: "0".repeat(32), user_id: "usr_1", email: "a@example.com", workspace_id: "ws_a", iat: now / 1000 };
    const req = (cookie: string) => new Request("https://issuer.example.com/", { headers: { cookie } });
    const cookie = sessionCookie(key, s);
    expect(cookie).toBe(
      `${cookie.split(";")[0]}; Path=/; HttpOnly; Secure; SameSite=Lax; Max-Age=${SESSION_TTL_SECS}`,
    );
    expect(readSession(key, req(cookie.split(";")[0]), now)).toEqual(s);
    for (const bad of [{ ...s, sid: "x" }, { ...s, workspace_id: "w s" }, { ...s, email: 5 }, { ...s, iat: now / 1000 + 120 }]) {
      expect(readSession(key, req(sessionCookie(key, bad as typeof s).split(";")[0]), now)).toBeNull();
    }
  });

  test("refuses another workspace 403 and a workspace off the allow-list 403, and re-reads the allow-list within 5 s", async () => {
    const cell = await issuerCell();
    await allow(cell, "ws_a");
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    expect(await answer(await m.get("/v1/workspaces/ws_b/streams/projects", a))).toEqual([
      403, refusal("forbidden", "this session is signed in to workspace ws_a, not ws_b: sign in to ws_b"),
    ]);
    const off = refusal("workspace_not_allowed", "workspace ws_b is not on the Streams beta's allow-list");
    expect(await answer(await m.get("/v1/workspaces/ws_b/streams/projects", m.signIn("ws_b")))).toEqual([403, off]);
    expect((await m.get("/v1/workspaces/ws_a/streams/projects", a)).status).toBe(200);
    expect((await run("disallow", "--state", cell.state, "--workspace", "ws_a")).code).toBe(0);
    expect((await m.get("/v1/workspaces/ws_a/streams/projects", a)).status).toBe(200);
    m.clock.t += 5_001;
    expect(await answer(await m.get("/v1/workspaces/ws_a/streams/projects", a))).toEqual([
      403, refusal("workspace_not_allowed", "workspace ws_a is not on the Streams beta's allow-list"),
    ]);
  });

  test("fails closed while the control bucket holds no allow-list", async () => {
    const cell = await issuerCell();
    const m = await managed(cell);
    expect(await answer(await m.get("/v1/workspaces/ws_a/streams/projects", m.signIn("ws_a")))).toEqual([
      403, refusal("workspace_not_allowed", "workspace ws_a is not on the Streams beta's allow-list"),
    ]);
  });

  test("takes 60 calls a minute from one session, then answers 429 rate_limited with Retry-After", async () => {
    const cell = await issuerCell();
    await allow(cell, "ws_a");
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    for (let i = 0; i < 60; i++) expect((await m.get("/v1/workspaces/ws_a/streams/projects", a)).status).toBe(200);
    const res = await m.get("/v1/workspaces/ws_a/streams/projects", a);
    expect([res.headers.get("retry-after"), ...(await answer(res))]).toEqual([
      "1", 429, refusal("rate_limited", "too many management calls in this session; retry after 1 s"),
    ]);
    // Another session of the same user has its own budget.
    expect((await m.get("/v1/workspaces/ws_a/streams/projects", m.signIn("ws_a"))).status).toBe(200);
    expect(m.issuer.limits.refused.management_session).toBe(1);
  });

  test("answers 503 issuer_unavailable while its copy of the state is over 300 s old", async () => {
    const cell = await issuerCell();
    await allow(cell, "ws_a");
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    m.clock.t += 301_000;
    const res = await m.get("/v1/workspaces/ws_a/streams/projects", m.signIn("ws_a"));
    expect([res.headers.get("retry-after"), ...(await answer(res))]).toEqual([
      "5", 503, refusal("issuer_unavailable", "the issuer cannot see the cell's current state; retry shortly"),
    ]);
    await m.cache.refresh();
    expect((await m.get("/v1/workspaces/ws_a/streams/projects", a)).status).toBe(200);
  });

  test("answers another method 405 with Allow, and a path it does not serve 404", async () => {
    const cell = await issuerCell();
    await allow(cell, "ws_a");
    const m = await managed(cell);
    const res = await m.call("PUT", "/v1/workspaces/ws_a/streams/projects", m.signIn("ws_a"));
    expect([res.headers.get("allow"), ...(await answer(res))]).toEqual([
      "GET", 405, refusal("method_not_allowed", "GET only"),
    ]);
    for (const path of ["/v1/workspaces/ws_a/streams", "/v1/workspaces/ws%2Fa/streams/projects", "/v1/workspaces//streams/projects"]) {
      expect((await m.get(path, m.signIn("ws_a"))).status).toBe(404);
    }
  });
});
