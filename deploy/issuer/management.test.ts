// The management API (public-beta issuer triage B3): a signed-in
// workspace's projects and credentials. Run with `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { FakeCell } from "../cell-admin/fake-cell";
import { cleanup, readBundle, run } from "../cell-admin/test-rig";
import {
  admin,
  CELL,
  CELL_ORIGIN,
  cellState,
  customerSecret,
  envWith,
  fakeS3,
  type IssuerCell,
  issuerCell,
  managed,
  managementAudits,
} from "./rig";
import { readSession, SESSION_COOKIE, SESSION_TTL_SECS, sessionCookie } from "./session";

afterEach(cleanup);

const answer = async (res: Response): Promise<[number, any]> => [res.status, await res.json()];
const refusal = (code: string, message: string) => ({ error: { code, message } });
const PROJECTS = "/v1/workspaces/ws_a/streams/projects";
/// The refusal of what would give a workspace off the allow-list something
/// new: a project, a credential or a secret (the owner's call 16).
const OFF_LIST = (w: string) =>
  `workspace ${w} is not on the Streams beta's allow-list: its projects and credentials can be seen, revoked and ` +
  "deleted, not created or rotated";
const offList = (w: string) => refusal("workspace_not_allowed", OFF_LIST(w));

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

  test("refuses another workspace 403, and a create of a workspace off the allow-list 403, re-reading the allow-list within 5 s", async () => {
    const cell = await issuerCell();
    await allow(cell, "ws_a", "--max-projects", "3");
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    expect(await answer(await m.get("/v1/workspaces/ws_b/streams/projects", a))).toEqual([
      403, refusal("forbidden", "this session is signed in to workspace ws_a, not ws_b: sign in to ws_b"),
    ]);
    const create = (cookie: string, w: string, name: string) =>
      m.send("POST", `/v1/workspaces/${w}/streams/projects`, cookie, { displayName: name });
    expect(await answer(await create(m.signIn("ws_b"), "ws_b", "one"))).toEqual([403, offList("ws_b")]);
    expect((await create(a, "ws_a", "one")).status).toBe(201);
    await admin(cell, "disallow", "--workspace", "ws_a");
    expect((await create(a, "ws_a", "two")).status).toBe(201);
    m.clock.t += 5_001;
    expect(await answer(await create(a, "ws_a", "three"))).toEqual([403, offList("ws_a")]);
    expect(Object.values(cellState(cell).projects).filter((p) => p.workspace_id === "ws_a").map((p) => p.display_name))
      .toEqual(["one", "two"]);
  });

  test("keeps a workspace off the allow-list to seeing, revoking and deleting, and refuses its credential create and rotation 403, audited", async () => {
    const cell = await issuerCell();
    await allow(cell, "ws_a");
    const fake = new FakeCell();
    try {
      const m = await managed(cell, envWith(cell, { STREAMS_CELL_ORIGIN: fake.url }));
      const a = m.signIn("ws_a");
      const made = await (await m.send("POST", PROJECTS, a, { displayName: "Orders" })).json();
      const [id, first] = [made.project.id as string, made.credential.id as string];
      const creds = `/v1/projects/${id}/streams/credentials`;
      const second = (await (await m.send("POST", creds, a, { displayName: "ci" })).json()).credential.id as string;
      fake.publish(readBundle(cell.state));
      await admin(cell, "disallow", "--workspace", "ws_a");
      m.clock.t += 5_001;
      const before = cellState(cell);
      const audits = managementAudits(cell).length;
      // No new credential, no new secret.
      const refusedNow = [
        await m.send("POST", creds, a, { displayName: "more" }),
        await m.send("POST", `${creds}/${first}/rotate`, a),
      ];
      for (const res of refusedNow) expect(await answer(res)).toEqual([403, offList("ws_a")]);
      expect(cellState(cell)).toEqual(before);
      expect(managementAudits(cell).slice(audits).map((d) => [d.action, d.outcome, d.reason, d.request_id])).toEqual(
        refusedNow.map((res, i) => [["credential.create", "credential.rotate"][i], "refused",
          `workspace_not_allowed: ${OFF_LIST("ws_a")}`, res.headers.get("x-request-id")]),
      );
      // Everything a user sees, as before.
      const listed = await answer(await m.get(PROJECTS, a));
      expect(listed).toEqual([200, { projects: [made.project] }]);
      expect(await answer(await m.get(`${PROJECTS}/${id}`, a))).toEqual([200, { ...made.project, ready: true }]);
      const [status, { credentials }] = await answer(await m.get(creds, a));
      expect([status, credentials.map((c: any) => [c.id, c.status])]).toEqual([200, [[first, "active"], [second, "active"]]]);
      expect((await answer(await m.get(`${creds}/${second}`, a)))[1]).toMatchObject({ id: second, served: true });
      // A leaked secret is revoked, and the project deleted, without the owner.
      const revoked = await answer(await m.send("POST", `${creds}/${second}/revoke`, a));
      expect([revoked[0], revoked[1].credential.status, revoked[1].credential.grantVersion]).toEqual([200, "revoked", 2]);
      expect(await answer(await m.send("DELETE", `${PROJECTS}/${id}`, a))).toEqual([
        202, { project: { ...made.project, status: "deleting" } },
      ]);
      const after = cellState(cell);
      expect(after.projects[id]).toMatchObject({ phase: "offboarding", status: "active" });
      expect([after.credentials[first], after.credentials[second]].map((c) => [c.status, c.grant_version])).toEqual([
        ["revoked", 2], ["revoked", 2],
      ]);
      expect(after.feed_version).toBe(before.feed_version + 2);
      expect(managementAudits(cell).slice(audits + 2).map((d) => [d.action, d.outcome])).toEqual([
        ["credential.revoke", "done"], ["project.delete", "done"],
      ]);
    } finally {
      fake.stop();
    }
  });

  test("fails a create closed while the control bucket holds no allow-list, and answers it 503 while the list cannot be read; reads need no list", async () => {
    const cell = await issuerCell();
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    expect(await answer(await m.send("POST", PROJECTS, a, { displayName: "app" }))).toEqual([403, offList("ws_a")]);
    m.clock.t += 5_001;
    fakeS3().failReads = true;
    const res = await m.send("POST", PROJECTS, a, { displayName: "app" });
    expect([res.headers.get("retry-after"), ...(await answer(res))]).toEqual([
      "5", 503, refusal("issuer_unavailable", "the issuer cannot read the beta's allow-list; retry shortly"),
    ]);
    expect(await answer(await m.get(PROJECTS, a))).toEqual([200, { projects: [] }]);
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
      "GET, POST", 405, refusal("method_not_allowed", "GET or POST only"),
    ]);
    for (const path of ["/v1/workspaces/ws_a/streams", "/v1/workspaces/ws%2Fa/streams/projects", "/v1/workspaces//streams/projects"]) {
      expect((await m.get(path, m.signIn("ws_a"))).status).toBe(404);
    }
  });
});
