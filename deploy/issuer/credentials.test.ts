// A project's credentials through the management API (public-beta issuer
// triage B3.4). Run with `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { FakeCell } from "../cell-admin/fake-cell";
import { CUSTOMER_SCOPES } from "../cell-admin/rules";
import { hashSecret, parsePeppers } from "../cell-admin/secrets";
import { cleanup, customerOnly, decodeJwt, readBundle, storeTexts } from "../cell-admin/test-rig";
import { admin, cellState, customerSecret, type IssuerCell, issuerCell, managed, managementAudits } from "./rig";

afterEach(cleanup);

const answer = async (res: Response): Promise<[number, any]> => [res.status, await res.json()];
const refusal = (code: string, message: string) => ({ error: { code, message } });

/// A cell with workspace ws_a allowed and one project it created.
async function withProject(...allowArgs: string[]) {
  const cell: IssuerCell = await issuerCell();
  await admin(cell, "allow", "--workspace", "ws_a", ...allowArgs);
  const m = await managed(cell);
  const a = m.signIn("ws_a");
  const made = await (await m.send("POST", "/v1/workspaces/ws_a/streams/projects", a, { displayName: "Orders" })).json();
  const id: string = made.project.id;
  return { cell, m, a, id, first: made.credential, path: `/v1/projects/${id}/streams/credentials` };
}

const exchange = (m: Awaited<ReturnType<typeof managed>>, secret: string, body?: unknown) =>
  m.issuer.fetch(new Request("https://issuer.example.com/v1/token/streams", {
    method: "POST",
    headers: { authorization: `StreamsCredential ${secret}` },
    body: body === undefined ? undefined : JSON.stringify(body),
  }));

describe("a project's credentials", () => {
  test("creates one with a secret shown once, publishes its grant, and lists the project's credentials without secrets", async () => {
    const { cell, m, a, id, first, path } = await withProject();
    const before = cellState(cell).feed_version;
    const expiresAt = new Date(m.clock.t + 30 * 86_400_000).toISOString().replace(/\.\d{3}Z$/, ".000Z");
    const res = await m.send("POST", path, a, {
      displayName: "ci", scopes: ["streams.records.read", "streams.catalog.read"], streamPrefixes: ["orders/"], expiresAt,
    });
    const [status, body] = await answer(res);
    expect(status).toBe(201);
    expect(Object.keys(body)).toEqual(["credential", "secret"]);
    const cid = body.credential.id;
    const raw = new RegExp(`^prisma_streams_${cid}\\.([A-Za-z0-9_-]{43})$`).exec(body.secret)![1];
    const state = cellState(cell);
    const by = { userId: "usr_ws_a", email: "owner@ws_a.example.com" };
    expect(body.credential).toEqual({
      id: cid, projectId: id, displayName: "ci", scopes: ["streams.records.read", "streams.catalog.read"],
      streamPrefixes: ["orders/"], grantVersion: 1, status: "active", valueHint: raw.slice(-6),
      createdAt: state.credentials[cid].created_at, expiresAt, createdBy: by,
    });
    const pepper = parsePeppers(readFileSync(cell.pepper, "utf8"), "pepper")[0];
    const expSecs = Date.parse(expiresAt) / 1000;
    expect(state.credentials[cid]).toMatchObject({
      project_id: id, role: "customer", grant_version: 1, status: "active", scopes: "streams.records.read streams.catalog.read",
      stream_prefixes: ["orders/"], expires_at: expSecs, secret_hash: hashSecret(pepper, raw), hv: 1, display_name: "ci",
      created_by: { user_id: "usr_ws_a", email: "owner@ws_a.example.com" },
    });
    expect(state.feed_version).toBe(before + 1);
    const grants = customerOnly(readBundle(cell.state)).grants.credentials as any[];
    expect(grants.find((g) => g.credential_id === cid)).toEqual({
      credential_id: cid, project_id: id, grant_version: 1, status: "active",
      scopes: "streams.records.read streams.catalog.read", stream_prefixes: ["orders/"], expires_at: expSecs,
    });
    expect(storeTexts(cell.state).join("\n")).not.toContain(raw);
    const token = (await (await exchange(m, body.secret)).json()).accessToken;
    expect(decodeJwt(token).claims).toMatchObject({
      credential_id: cid, scope: "streams.records.read streams.catalog.read", stream_prefixes: ["orders/"],
    });
    expect(await answer(await m.get(path, a))).toEqual([200, { credentials: [first, body.credential] }]);
    expect(managementAudits(cell).at(-1)).toMatchObject({
      action: "credential.create", outcome: "done", project_id: id, credential_ids: [cid],
      before: { feed_version: before }, after: { feed_version: before + 1, grant_version: 1 },
      request_id: res.headers.get("x-request-id"),
    });
  });

  test("gives every scope, no prefix and no expiry by default, and refuses a body it cannot take 400 invalid_request", async () => {
    const { m, a, path } = await withProject();
    const [status, body] = await answer(await m.send("POST", path, a, {}));
    expect([status, body.credential.scopes, body.credential.streamPrefixes, body.credential.expiresAt, body.credential.displayName])
      .toEqual([201, [...CUSTOMER_SCOPES], null, null, body.credential.id]);
    const future = new Date(m.clock.t + 3_600_000).toISOString();
    const cases: [unknown, string][] = [
      ["{", "the body is not JSON"],
      [{ scopes: [] }, "scopes is a non-empty array of Streams scopes"],
      [{ scopes: "streams.records.read" }, "scopes is a non-empty array of Streams scopes"],
      [{ scopes: ["streams.records.read", "streams.everything"] }, 'scopes: "streams.everything" is not a Streams scope'],
      [{ streamPrefixes: [] }, "streamPrefixes is null or 1 to 64 non-empty strings"],
      [{ streamPrefixes: [""] }, "streamPrefixes is null or 1 to 64 non-empty strings"],
      [{ streamPrefixes: Array(65).fill("p") }, "streamPrefixes is null or 1 to 64 non-empty strings"],
      [{ expiresAt: "tomorrow" }, "expiresAt is null or a date-time in the future"],
      [{ expiresAt: new Date(m.clock.t - 1000).toISOString() }, "expiresAt is null or a date-time in the future"],
      [{ expiresAt: 1_900_000_000 }, "expiresAt is null or a date-time in the future"],
      [{ displayName: "" }, "displayName is 1 to 64 characters, without control characters"],
      [{ secret: "mine", expiresAt: future }, 'unknown member "secret": the body takes displayName, scopes, streamPrefixes, expiresAt'],
    ];
    for (const [b, message] of cases) {
      expect(await answer(await m.send("POST", path, a, b))).toEqual([400, refusal("invalid_request", message)]);
    }
    const ok = await answer(await m.send("POST", path, a, { scopes: ["streams.records.read", "streams.records.read"], streamPrefixes: null, expiresAt: null }));
    expect([ok[0], ok[1].credential.scopes]).toEqual([201, ["streams.records.read"]]);
  });

  test("holds a project to 10 live credentials, 409 credential_limit", async () => {
    const { m, a, id, path } = await withProject();
    for (let i = 2; i <= 10; i++) expect((await m.send("POST", path, a, { displayName: `c${i}` })).status).toBe(201);
    expect(await answer(await m.send("POST", path, a, { displayName: "c11" }))).toEqual([
      409, refusal("credential_limit", `project ${id} has its 10 live credentials: revoke one first`),
    ]);
  });

  test("takes 20 credential creates or rotations a day per project, then answers 429 rate_limited", async () => {
    const { m, a, id, path } = await withProject();
    for (let i = 0; i < 19; i++) m.issuer.limits.take("credential_change_project", id);
    expect((await m.send("POST", path, a, {})).status).toBe(201);
    const res = await m.send("POST", path, a, {});
    expect([res.headers.get("retry-after"), ...(await answer(res))]).toEqual([
      "4320", 429, refusal("rate_limited", `project ${id} may create or rotate 20 credentials a day; retry after 4320 s`),
    ]);
  });

  test("answers 404 for a project that is not the workspace's, and 409 project_not_active for one being deleted", async () => {
    const { cell, m, a, id, path } = await withProject();
    await customerSecret(cell, "sp_other");
    await m.cache.refresh();
    for (const p of ["sp_other", "sp_canary_cell_sc1", `sp_${"0".repeat(32)}`]) {
      for (const res of [await m.get(`/v1/projects/${p}/streams/credentials`, a), await m.send("POST", `/v1/projects/${p}/streams/credentials`, a, {})]) {
        expect(await answer(res)).toEqual([404, refusal("not_found", `workspace ws_a has no Streams project ${p}`)]);
      }
    }
    await admin(cell, "offboard", "--project", id);
    await m.cache.refresh();
    expect(await answer(await m.send("POST", path, a, {}))).toEqual([
      409, refusal("project_not_active", `project ${id} is being deleted`),
    ]);
    // The listing still answers: every credential revoked.
    const listed = (await (await m.get(path, a)).json()).credentials;
    expect(listed.map((c: any) => [c.status, c.grantVersion])).toEqual([["revoked", 2]]);
  });
});

describe("rotating a credential", () => {
  test("gives it a new secret at grant_version + 1: the old secret exchanges no more and the cell refuses tokens minted under it", async () => {
    const { cell, m, a, id, first, path } = await withProject();
    const fake = new FakeCell();
    try {
      fake.publish(readBundle(cell.state));
      const created = await (await m.send("POST", path, a, { displayName: "svc" })).json();
      fake.publish(readBundle(cell.state));
      const oldToken = (await (await exchange(m, created.secret)).json()).accessToken;
      const before = cellState(cell);
      const res = await m.send("POST", `${path}/${created.credential.id}/rotate`, a);
      const [status, body] = await answer(res);
      expect(status).toBe(200);
      expect(Object.keys(body)).toEqual(["credential", "secret"]);
      const raw = new RegExp(`^prisma_streams_${created.credential.id}\\.([A-Za-z0-9_-]{43})$`).exec(body.secret)![1];
      expect(body.credential).toEqual({ ...created.credential, grantVersion: 2, valueHint: raw.slice(-6) });
      const after = cellState(cell);
      const pepper = parsePeppers(readFileSync(cell.pepper, "utf8"), "pepper")[0];
      expect(after.credentials[created.credential.id]).toMatchObject({
        grant_version: 2, status: "active", secret_hash: hashSecret(pepper, raw), hint: raw.slice(-6),
      });
      expect(after.feed_version).toBe(before.feed_version + 1);
      expect(after.credentials[first.id].grant_version).toBe(1);
      // The old secret is refused at once; the new one mints at version 2.
      expect((await exchange(m, created.secret)).status).toBe(401);
      const fresh = await (await exchange(m, body.secret)).json();
      expect(decodeJwt(fresh.accessToken).claims.grant_version).toBe(2);
      // Once the cell holds the bundle, a token minted before is refused.
      fake.publish(readBundle(cell.state));
      const call = (token: string) => fetch(`${fake.url}/v1/streams?limit=1`, { headers: { authorization: `Bearer ${token}` } });
      expect([(await call(oldToken)).status, fake.requests.at(-1)?.status]).toEqual([401, 401]);
      expect((await (await call(oldToken)).json()).error.code).toBe("grant_version_mismatch");
      expect((await call(fresh.accessToken)).status).toBe(200);
      expect(managementAudits(cell).at(-1)).toMatchObject({
        action: "credential.rotate", outcome: "done", project_id: id, credential_ids: [created.credential.id],
        before: { feed_version: before.feed_version, grant_version: 1 }, after: { feed_version: after.feed_version, grant_version: 2 },
        request_id: res.headers.get("x-request-id"),
      });
    } finally {
      fake.stop();
    }
  });

  test("refuses a revoked or expired credential 409, one that is not the project's 404, and a body 400", async () => {
    const { cell, m, a, id, first, path } = await withProject();
    const other = await customerSecret(cell, "sp_other");
    await m.cache.refresh();
    const canary = Object.entries(cellState(cell).credentials).find(([, c]) => c.hidden)![0];
    for (const cid of [other.id, canary, `strcred_${"0".repeat(32)}`]) {
      expect(await answer(await m.send("POST", `${path}/${cid}/rotate`, a))).toEqual([
        404, refusal("not_found", `project ${id} has no credential ${cid}`),
      ]);
    }
    expect(await answer(await m.send("POST", `${path}/${first.id}/rotate`, a, { scopes: [] }))).toEqual([
      400, refusal("invalid_request", 'unknown member "scopes": the body takes nothing'),
    ]);
    const soon = await (await m.send("POST", path, a, { expiresAt: new Date(m.clock.t + 60_000).toISOString() })).json();
    m.clock.t += 60_000;
    expect(await answer(await m.send("POST", `${path}/${soon.credential.id}/rotate`, a, {}))).toEqual([
      409, refusal("credential_not_active", `credential ${soon.credential.id} has expired`),
    ]);
    await admin(cell, "offboard", "--project", id);
    await m.cache.refresh();
    expect(await answer(await m.send("POST", `${path}/${first.id}/rotate`, a))).toEqual([
      409, refusal("credential_not_active", `credential ${first.id} is revoked`),
    ]);
  });

  test("counts toward the project's 20 creates or rotations a day", async () => {
    const { m, a, id, first, path } = await withProject();
    for (let i = 0; i < 20; i++) expect((await m.send("POST", `${path}/${first.id}/rotate`, a)).status).toBe(200);
    const res = await m.send("POST", `${path}/${first.id}/rotate`, a);
    expect([res.status, (await res.json()).error.message]).toEqual([
      429, `project ${id} may create or rotate 20 credentials a day; retry after 4320 s`,
    ]);
  });
});
