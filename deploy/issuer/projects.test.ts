// Self-serve project creation (public-beta issuer triage B3.2). Run with
// `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { FakeCell } from "../cell-admin/fake-cell";
import { OPERATOR_SCOPES } from "../cell-admin/offboard";
import { CUSTOMER_SCOPES } from "../cell-admin/rules";
import { hashSecret, parsePeppers } from "../cell-admin/secrets";
import { cleanup, customerOnly, denylistText, readBundle, run, storeTexts, writeStateText } from "../cell-admin/test-rig";
import { FREE_TIER_REQUESTS_PER_SEC } from "./projects";
import { csrfToken, readSession } from "./session";
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

afterEach(cleanup);

const answer = async (res: Response): Promise<[number, any]> => [res.status, await res.json()];
const refusal = (code: string, message: string) => ({ error: { code, message } });
const PROJECTS = "/v1/workspaces/ws_a/streams/projects";
/// The shared-cell profile's PROJECT_STORED_BYTES_DEFAULT, 10 GiB: every
/// project the issuer places carries it explicitly.
const STORED_DEFAULT = 10737418240;

async function allowedCell(...allowArgs: string[]): Promise<IssuerCell> {
  const cell = await issuerCell();
  await admin(cell, "allow", "--workspace", "ws_a", ...allowArgs);
  return cell;
}

describe("creating a project", () => {
  test("places it active for the signed-in workspace with a first credential, publishes it and shows the secret once", async () => {
    const cell = await allowedCell();
    const m = await managed(cell);
    const before = cellState(cell).feed_version;
    const res = await m.send("POST", PROJECTS, m.signIn("ws_a"), { displayName: "Orders" });
    const [status, body] = await answer(res);
    expect(status).toBe(201);
    expect(Object.keys(body)).toEqual(["project", "endpoint", "credential", "secret"]);
    const state = cellState(cell);
    const id = body.project.id;
    const cid = body.credential.id;
    expect(id).toMatch(/^sp_[0-9a-f]{32}$/);
    expect(cid).toMatch(/^strcred_[0-9a-f]{32}$/);
    const raw = /^prisma_streams_strcred_[0-9a-f]{32}\.([A-Za-z0-9_-]{43})$/.exec(body.secret)![1];
    expect(body.secret.startsWith(`prisma_streams_${cid}.`)).toBe(true);
    const by = { userId: "usr_ws_a", email: "owner@ws_a.example.com" };
    const quotas = {
      ...state.cell.ceilings, requests_per_sec: FREE_TIER_REQUESTS_PER_SEC, append_records_per_sec: 0,
      max_stored_bytes: STORED_DEFAULT,
    };
    expect(body.project).toEqual({
      id, workspaceId: "ws_a", displayName: "Orders", status: "active", createdAt: state.projects[id].admitted_at,
      createdBy: by, cellId: CELL, endpoint: CELL_ORIGIN, quotas,
    });
    expect(body.endpoint).toBe(CELL_ORIGIN);
    expect(body.credential).toEqual({
      id: cid, projectId: id, displayName: "default", scopes: [...CUSTOMER_SCOPES], streamPrefixes: null, grantVersion: 1,
      status: "active", valueHint: raw.slice(-6), createdAt: state.credentials[cid].created_at, expiresAt: null, createdBy: by,
    });
    // The state: the project and its credential, the secret's hash only.
    const pepper = parsePeppers(readFileSync(cell.pepper, "utf8"), "pepper")[0];
    expect(state.projects[id]).toMatchObject({
      workspace_id: "ws_a", phase: "active", status: "active", project_policy_version: 1, display_name: "Orders",
      created_by: { user_id: "usr_ws_a", email: "owner@ws_a.example.com" }, quotas,
    });
    expect(state.credentials[cid]).toMatchObject({
      project_id: id, role: "customer", grant_version: 1, status: "active", scopes: CUSTOMER_SCOPES.join(" "),
      secret_hash: hashSecret(pepper, raw), hv: pepper.version, hint: raw.slice(-6), display_name: "default",
    });
    expect(state.feed_version).toBe(before + 1);
    expect(denylistText(cell.state).split("\n")).toContain(id);
    // Published: the cell's next bundle holds the project and its grant.
    const bundle = customerOnly(readBundle(cell.state));
    expect([bundle.keys.feed_version, (bundle.policies.projects as any[]).map((p) => p.project_id)]).toEqual([before + 1, [id]]);
    expect((bundle.grants.credentials as any[]).map((c) => [c.credential_id, c.grant_version])).toEqual([[cid, 1]]);
    // The secret is nowhere but in the answer, and it exchanges at once.
    expect(storeTexts(cell.state).join("\n")).not.toContain(raw);
    const minted = await m.issuer.fetch(new Request("https://issuer.example.com/v1/token/streams", {
      method: "POST", headers: { authorization: `StreamsCredential ${body.secret}` },
    }));
    expect([minted.status, (await minted.json()).projectId]).toEqual([200, id]);
    expect(managementAudits(cell)).toEqual([{
      kind: "management", at: new Date(m.clock.t).toISOString(), cell_id: CELL,
      actor: { user_id: "usr_ws_a", email: "owner@ws_a.example.com", workspace_id: "ws_a" },
      action: "project.create", outcome: "done", project_id: id, credential_ids: [cid],
      before: { feed_version: before }, after: { feed_version: before + 1, project_policy_version: 1, grant_version: 1 },
      request_id: res.headers.get("x-request-id"),
    }]);
  });

  test("answers a create whose state landed but whose bundle failed to publish as made, and the next revalidation publishes it", async () => {
    const cell = await allowedCell();
    const m = await managed(cell);
    const published = readBundle(cell.state).keys.feed_version;
    let failed = 0;
    fakeS3().failPut = (key) => key.endsWith("/feeds-bundle.json") && failed++ === 0;
    const res = await m.send("POST", PROJECTS, m.signIn("ws_a"), { displayName: "Orders" });
    const [status, body] = await answer(res);
    expect([status, failed]).toEqual([201, 1]);
    expect(cellState(cell).projects[body.project.id].phase).toBe("active");
    expect(readBundle(cell.state).keys.feed_version).toBe(published);
    // The secret works at once, and the next revalidation publishes.
    const minted = await m.issuer.fetch(new Request("https://issuer.example.com/v1/token/streams", {
      method: "POST", headers: { authorization: `StreamsCredential ${body.secret}` },
    }));
    expect(minted.status).toBe(200);
    await m.cache.refresh();
    expect(readBundle(cell.state).keys.feed_version).toBe(published + 1);
    expect((customerOnly(readBundle(cell.state)).policies.projects as any[]).map((p) => p.project_id)).toEqual([body.project.id]);
  });

  test("answers a store it cannot reach 503 issuer_unavailable, audited, and any other failure 500 with its request id", async () => {
    const cell = await allowedCell();
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    expect((await m.get("/auth/session", a)).status).toBe(200); // the allow-list is read and kept 5 s
    const unavailable = (detail: string) => [503, refusal("issuer_unavailable", "the issuer cannot reach the cell's state; retry shortly"), "5", detail];
    const outcome = async (res: Response) => {
      const [status, body] = await answer(res);
      const id = res.headers.get("x-request-id");
      const audit = managementAudits(cell).at(-1);
      expect([audit?.request_id, audit?.outcome]).toEqual([id, "refused"]);
      return [status, body, res.headers.get("retry-after"), audit?.reason];
    };
    // The denylist cannot be read.
    fakeS3().failReads = true;
    expect(await outcome(await m.send("POST", PROJECTS, a, { displayName: "A" }))).toEqual(unavailable(
      "issuer_unavailable: GET s3://ctl-1/denylist.txt answered 500 InternalError"));
    fakeS3().failReads = false;
    // The state's write fails (as a timed-out or refused PUT would).
    fakeS3().failPut = (key) => key.endsWith("/cell-state.json");
    expect(await outcome(await m.send("POST", PROJECTS, a, { displayName: "A" }))).toEqual(unavailable(
      "issuer_unavailable: PUT s3://ctl-1/cells/cell_sc1/cell-state.json answered 500 InternalError"));
    fakeS3().failPut = undefined;
    expect(Object.values(cellState(cell).projects).filter((p) => p.workspace_id === "ws_a")).toEqual([]);
    // A failure that is no store's (a state another writer broke): 500, with its request id.
    const broken = cellState(cell);
    broken.credentials[Object.keys(broken.credentials)[0]].stream_prefixes = ["a/"];
    writeStateText(cell.state, JSON.stringify(broken));
    await m.cache.refresh();
    const res = await m.send("POST", PROJECTS, a, { displayName: "A" });
    expect([res.status, (await res.json()).error.code, res.headers.get("x-request-id")]).toEqual([
      500, "internal", expect.stringMatching(/^[0-9a-f]{16}$/),
    ]);
    expect(m.logs.at(-1)).toContain(`POST ${PROJECTS} (request ${res.headers.get("x-request-id")}) failed: `);
  });

  test("refuses a change without JSON or without the session's CSRF value 403 csrf_failed, and audits it", async () => {
    const cell = await allowedCell();
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    const body = JSON.stringify({ displayName: "Orders" });
    const send = (headers: Record<string, string>) => m.call("POST", PROJECTS, a, { body, headers });
    const json = { "content-type": "application/json" };
    const cases: [Record<string, string>, string][] = [
      [{}, "send Content-Type: application/json"],
      [{ "content-type": "text/plain", "x-csrf-token": "x" }, "send Content-Type: application/json"],
      [json, "send X-CSRF-Token with this session's value"],
      [{ ...json, "x-csrf-token": "" }, "send X-CSRF-Token with this session's value"],
      [{ ...json, "x-csrf-token": "AAAA" }, "send X-CSRF-Token with this session's value"],
    ];
    for (const [headers, message] of cases) {
      expect(await answer(await send(headers))).toEqual([403, refusal("csrf_failed", message)]);
    }
    // Another session's value is not this one's.
    const other = m.signIn("ws_a");
    const res = await m.send("POST", PROJECTS, other, { displayName: "Orders" });
    expect(res.status).toBe(201);
    const stolen = await m.call("POST", PROJECTS, a, {
      body, headers: { ...json, "x-csrf-token": csrfFor(m, other) },
    });
    expect(await answer(stolen)).toEqual([403, refusal("csrf_failed", "send X-CSRF-Token with this session's value")]);
    const audits = managementAudits(cell);
    expect(audits.map((a) => [a.action, a.outcome, a.reason])).toEqual([
      ...cases.map(([, message]) => ["project.create", "refused", `csrf_failed: ${message}`]),
      ["project.create", "done", undefined],
      ["project.create", "refused", "csrf_failed: send X-CSRF-Token with this session's value"],
    ]);
    expect(Object.keys(cellState(cell).projects).filter((p) => p.startsWith("sp_") && !p.startsWith("sp_canary"))).toHaveLength(1);
  });

  test("gives the free-tier request rate unless the allow-list names one, and every other bounded axis its ceiling", async () => {
    const cell = await allowedCell();
    await admin(cell, "allow", "--workspace", "ws_b", "--requests-per-sec", "50");
    const m = await managed(cell);
    const ceilings = cellState(cell).cell.ceilings;
    const a = await answer(await m.send("POST", PROJECTS, m.signIn("ws_a"), { displayName: "A" }));
    const b = await answer(await m.send("POST", "/v1/workspaces/ws_b/streams/projects", m.signIn("ws_b"), { displayName: "B" }));
    expect(FREE_TIER_REQUESTS_PER_SEC).toBe(10);
    const rest = { append_records_per_sec: 0, max_stored_bytes: STORED_DEFAULT };
    expect([a[0], a[1].project.quotas]).toEqual([201, { ...ceilings, requests_per_sec: 10, ...rest }]);
    expect([b[0], b[1].project.quotas]).toEqual([201, { ...ceilings, requests_per_sec: 50, ...rest }]);
  });

  test("holds a workspace to one project unless the allow-list names more, 409 project_limit", async () => {
    const cell = await allowedCell();
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    // The cell's workspace cap, 1 unless init's --workspace-cap says otherwise (I6 (a)).
    expect(cellState(cell).cell.workspace_cap).toBe(1);
    expect((await m.send("POST", PROJECTS, a, { displayName: "One" })).status).toBe(201);
    const limit = refusal("project_limit", "workspace ws_a already has 1 Streams project(s), its limit");
    expect(await answer(await m.send("POST", PROJECTS, a, { displayName: "Two" }))).toEqual([409, limit]);
    await admin(cell, "allow", "--workspace", "ws_a", "--max-projects", "2");
    m.clock.t += 5_001; // the allow-list is re-read
    expect((await m.send("POST", PROJECTS, a, { displayName: "Two" })).status).toBe(201);
    expect(await answer(await m.send("POST", PROJECTS, a, { displayName: "Three" }))).toEqual([
      409, refusal("project_limit", "workspace ws_a already has 2 Streams project(s), its limit"),
    ]);
    expect(managementAudits(cell).map((r) => [r.outcome, r.reason])).toEqual([
      ["done", undefined],
      ["refused", "project_limit: workspace ws_a already has 1 project(s) on cell cell_sc1 (cap 1)"],
      ["done", undefined],
      ["refused", "project_limit: workspace ws_a already has 2 project(s) on cell cell_sc1 (cap 2)"],
    ]);
  });

  test("holds a workspace without its own limit to the cap the cell was initialised with", async () => {
    const cell = await issuerCell(["--workspace-cap", "2"]);
    await admin(cell, "allow", "--workspace", "ws_a");
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    expect((await m.send("POST", PROJECTS, a, { displayName: "One" })).status).toBe(201);
    expect((await m.send("POST", PROJECTS, a, { displayName: "Two" })).status).toBe(201);
    expect(await answer(await m.send("POST", PROJECTS, a, { displayName: "Three" }))).toEqual([
      409, refusal("project_limit", "workspace ws_a already has 2 Streams project(s), its limit"),
    ]);
    // The allow-list entry's own limit still wins over the cell's.
    await admin(cell, "allow", "--workspace", "ws_a", "--max-projects", "3");
    m.clock.t += 5_001; // the allow-list is re-read
    expect((await m.send("POST", PROJECTS, a, { displayName: "Three" })).status).toBe(201);
  });

  test("counts a project being deleted toward the cell's max_projects but not toward its workspace's cap", async () => {
    const cell = await allowedCell();
    await admin(cell, "allow", "--workspace", "ws_b");
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    const first = (await (await m.send("POST", PROJECTS, a, { displayName: "One" })).json()).project.id;
    await admin(cell, "offboard", "--project", first);
    await m.cache.refresh();
    const second = await answer(await m.send("POST", PROJECTS, a, { displayName: "Two" }));
    expect(second[0]).toBe(201);
    const listed = await answer(await m.get(PROJECTS, a));
    expect(listed[1].projects.map((p: any) => [p.id, p.status])).toEqual([[first, "deleting"], [second[1].project.id, "active"]]);
    // The owner's admit counts the same way (I9 (a), one rule on a cell):
    // the active project fills ws_a's cap, the one being deleted does not.
    let r = await run("admit", "--state", cell.state, "--project", "proj_x", "--workspace", "ws_a");
    expect(r.err).toBe("cell-admin admit: workspace ws_a already has 1 project(s) on cell cell_sc1 (cap 1)");
    await m.cache.refresh();
    expect((await m.send("DELETE", `${PROJECTS}/${second[1].project.id}`, a)).status).toBe(202);
    r = await run("admit", "--state", cell.state, "--project", "proj_x", "--workspace", "ws_a");
    expect([r.code, r.err]).toEqual([0, ""]);
    await m.cache.refresh();
    // The cell's max_projects still counts it: the drain is still to run.
    await m.cache.mutate((s) => {
      s.cell.max_projects = 2;
    });
    expect(await answer(await m.send("POST", "/v1/workspaces/ws_b/streams/projects", m.signIn("ws_b"), { displayName: "B" })))
      .toEqual([409, refusal("beta_at_capacity", "the Streams beta is at capacity; try again later")]);
    expect(managementAudits(cell).at(-1)?.reason).toBe("beta_at_capacity: cell cell_sc1 already holds its 2 projects");
  });

  test("refuses 409 beta_at_capacity while the gate is closed or n_max customer projects are active", async () => {
    const cell = await allowedCell();
    await customerSecret(cell, "sp_x");
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    const full = refusal("beta_at_capacity", "the Streams beta is at capacity; try again later");
    await admin(cell, "gate", "--close");
    await m.cache.refresh();
    expect(await answer(await m.send("POST", PROJECTS, a, { displayName: "A" }))).toEqual([409, full]);
    await admin(cell, "gate", "--open", "--n-max", "1");
    await m.cache.refresh();
    expect(await answer(await m.send("POST", PROJECTS, a, { displayName: "A" }))).toEqual([409, full]);
    await admin(cell, "gate", "--n-max", "2");
    await m.cache.refresh();
    expect((await m.send("POST", PROJECTS, a, { displayName: "A" })).status).toBe(201);
    expect(managementAudits(cell).map((r) => r.reason)).toEqual([
      "beta_at_capacity: the admission gate is closed",
      "beta_at_capacity: 1 customer project(s) active, the gate's n_max 1",
      undefined,
    ]);
  });

  test("places one of two creates made at once for a workspace with room for one, refusing the other on the state it lands on", async () => {
    const cell = await allowedCell();
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    // Both pass the checks on the issuer's copy before either commits; the
    // compare-and-swap's re-check of the placement refuses the loser.
    const sent = await Promise.all([
      m.send("POST", PROJECTS, a, { displayName: "One" }),
      m.send("POST", PROJECTS, a, { displayName: "Two" }),
    ]);
    const answers = (await Promise.all(sent.map(answer))).sort(([x], [y]) => x - y);
    expect(answers.map(([status]) => status)).toEqual([201, 409]);
    expect(answers[1][1]).toEqual(refusal("project_limit", "workspace ws_a already has 1 Streams project(s), its limit"));
    const placed = Object.entries(cellState(cell).projects).filter(([, p]) => p.workspace_id === "ws_a");
    expect(placed.map(([id]) => id)).toEqual([answers[0][1].project.id]);
    expect(customerOnly(readBundle(cell.state)).policies.projects.length).toBe(1);
    expect(managementAudits(cell).map((r) => [r.outcome, r.reason]).sort()).toEqual([
      ["done", undefined],
      ["refused", "project_limit: workspace ws_a already has 1 project(s) on cell cell_sc1 (cap 1)"],
    ]);
  });

  test("refuses a create whose gate the owner closed after the issuer's checks, on the state its compare-and-swap lands on", async () => {
    const cell = await allowedCell();
    const m = await managed(cell);
    const before = cellState(cell);
    // `cell-admin gate --close` lands just before the issuer's first write
    // of the state: its copy still shows the gate open.
    const fake = fakeS3();
    const key = cell.state.slice("s3://".length) + "/cell-state.json";
    let closed = false;
    fake.beforePut = (k) => {
      if (k !== key || closed) return;
      closed = true;
      const [bucket, ...rest] = key.split("/");
      const state = JSON.parse(fake.text(bucket, rest.join("/"))!);
      state.cell.gate.open = false;
      state.feed_version += 1;
      fake.put(bucket, rest.join("/"), `${JSON.stringify(state, null, 2)}\n`);
    };
    expect(await answer(await m.send("POST", PROJECTS, m.signIn("ws_a"), { displayName: "A" }))).toEqual([
      409, refusal("beta_at_capacity", "the Streams beta is at capacity; try again later"),
    ]);
    const after = cellState(cell);
    expect([closed, after.cell.gate.open, after.feed_version]).toEqual([true, false, before.feed_version + 1]);
    expect(Object.keys(after.projects)).toEqual(Object.keys(before.projects));
    expect(managementAudits(cell).map((r) => [r.outcome, r.reason])).toEqual([
      ["refused", "beta_at_capacity: the admission gate is closed"],
    ]);
  });

  test("refuses 409 beta_at_capacity while its compiled profile is not the one the cell was initialised with", async () => {
    const cell = await issuerCell();
    // As a cell initialised with another profile than the one compiled in.
    await (await managed(cell)).cache.mutate((s) => {
      s.cell.profile_digest = "0".repeat(64);
    });
    await admin(cell, "allow", "--workspace", "ws_a");
    const m = await managed(cell);
    expect(await answer(await m.send("POST", PROJECTS, m.signIn("ws_a"), { displayName: "A" }))).toEqual([
      409, refusal("beta_at_capacity", "the Streams beta is at capacity; try again later"),
    ]);
    expect(managementAudits(cell).at(-1)?.reason).toMatch(
      /^beta_at_capacity: the layered profile differs from the one init recorded \(digest 000000000000 -> [0-9a-f]{12}\)/,
    );
  });

  test("takes 5 creates a day from one workspace, then answers 429 rate_limited", async () => {
    const cell = await allowedCell("--max-projects", "10");
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    for (let i = 0; i < 5; i++) expect((await m.send("POST", PROJECTS, a, { displayName: `P${i}` })).status).toBe(201);
    const res = await m.send("POST", PROJECTS, m.signIn("ws_a"), { displayName: "P5" });
    expect([res.headers.get("retry-after"), ...(await answer(res))]).toEqual([
      "17280", 429, refusal("rate_limited", "workspace ws_a may create 5 projects a day; retry after 17280 s"),
    ]);
  });

  test("refuses a body it cannot take 400 invalid_request, placing nothing", async () => {
    const cell = await allowedCell();
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    const cases: [unknown, string][] = [
      [undefined, "the body is not JSON"],
      ["{", "the body is not JSON"],
      [[], "the body is not a JSON object"],
      [{}, "displayName is 1 to 64 characters, without control characters"],
      [{ displayName: "" }, "displayName is 1 to 64 characters, without control characters"],
      [{ displayName: "   " }, "displayName is 1 to 64 characters, without control characters"],
      [{ displayName: "x".repeat(65) }, "displayName is 1 to 64 characters, without control characters"],
      [{ displayName: "a\nb" }, "displayName is 1 to 64 characters, without control characters"],
      [{ displayName: 5 }, "displayName is 1 to 64 characters, without control characters"],
      [{ displayName: "A", quotas: {} }, 'unknown member "quotas": the body takes displayName'],
      [JSON.stringify({ displayName: "A", pad: "x".repeat(5000) }), "the body is larger than 4096 bytes"],
    ];
    for (const [body, message] of cases) {
      expect(await answer(await m.send("POST", PROJECTS, a, body))).toEqual([400, refusal("invalid_request", message)]);
    }
    const created = await answer(await m.send("POST", PROJECTS, a, { displayName: "  Orders  " }));
    expect([created[0], created[1].project.displayName]).toEqual([201, "Orders"]);
  });
});

/// The CSRF value of the session in `cookie`.
function csrfFor(m: Awaited<ReturnType<typeof managed>>, cookie: string): string {
  const s = readSession(m.cfg.sessionKey, new Request("https://issuer.example.com/", { headers: { cookie } }), m.clock.t);
  return csrfToken(m.cfg.sessionKey, s!.sid);
}

describe("a project's readiness", () => {
  /// A cell whose fake serves the bundle as it was before the project.
  async function created() {
    const cell = await allowedCell();
    const fake = new FakeCell();
    fakes.push(fake);
    fake.publish(readBundle(cell.state));
    const m = await managed(cell, envWith(cell, { STREAMS_CELL_ORIGIN: fake.url }));
    // The cell's clock is the test's: a credential's expiry is read on it.
    fake.now = () => m.clock.t;
    const a = m.signIn("ws_a");
    const body = await (await m.send("POST", PROJECTS, a, { displayName: "Orders" })).json();
    const id: string = body.project.id;
    return { cell, fake, m, a, id, cid: body.credential.id as string, creds: `/v1/projects/${id}/streams/credentials` };
  }
  /// The project as the workspace's listing shows it.
  const listed = async (m: Awaited<ReturnType<typeof managed>>, a: string) =>
    (await (await m.get(PROJECTS, a)).json()).projects[0];
  const fakes: FakeCell[] = [];
  afterEach(() => {
    for (const f of fakes.splice(0)) f.stop();
  });

  test("is ready once a catalog-read token of its own first credential answers 200 at the cell, probed at most every 5 s", async () => {
    const { cell, fake, m, a, id, cid } = await created();
    const project = `${PROJECTS}/${id}`;
    const got = async () => answer(await m.get(project, a));
    const [status, body] = await got();
    expect(status).toBe(200);
    expect(body).toEqual({ ...(await (await m.get(PROJECTS, a)).json()).projects[0], ready: false });
    expect(fake.requests.map((r) => [r.path, r.status])).toEqual([["/v1/streams?limit=1", 421]]);
    // Within 5 s of a probe, no other.
    fake.publish(readBundle(cell.state));
    expect((await got())[1].ready).toBe(false);
    expect(fake.requests).toHaveLength(1);
    m.clock.t += 5_000;
    expect((await got())[1].ready).toBe(true);
    expect(fake.requests.map((r) => [r.path, r.status])).toEqual([["/v1/streams?limit=1", 421], ["/v1/streams?limit=1", 200]]);
    // The probe's token: the project's first credential at its current
    // grant_version, narrowed to streams.catalog.read, for 300 s.
    expect(fake.tokens.at(-1)).toMatchObject({
      credential_id: cid, project_id: id, grant_version: 1, scope: "streams.catalog.read", exp: Math.floor(m.clock.t / 1000) - 60 + 300,
    });
    // Ready stays ready without another probe.
    m.clock.t += 60_000;
    expect((await got())[1].ready).toBe(true);
    expect(fake.requests).toHaveLength(2);
  });

  test("is not ready while the cell holds the new grants but not yet the new policies", async () => {
    const { cell, fake, m, a, id } = await created();
    const before = structuredClone(fake.live!);
    const after = readBundle(cell.state);
    fake.publish({ keys: after.keys, policies: before.policies, grants: after.grants });
    expect((await (await m.get(`${PROJECTS}/${id}`, a)).json()).ready).toBe(false);
    expect(fake.requests.map((r) => r.status)).toEqual([421]);
  });

  test("answers 404 for a project that is not the workspace's, the canary, or none, without probing", async () => {
    const { cell, fake, m, a, id } = await created();
    await customerSecret(cell, "sp_other");
    await m.cache.refresh();
    const missing = (p: string): [number, any] => [404, refusal("not_found", `workspace ws_a has no Streams project ${p}`)];
    for (const p of ["sp_other", "sp_canary_cell_sc1", `sp_${"0".repeat(32)}`]) {
      expect(await answer(await m.get(`${PROJECTS}/${p}`, a))).toEqual(missing(p));
    }
    expect(await answer(await m.get(`/v1/workspaces/ws_sp_other/streams/projects/${id}`, a))).toEqual([
      403, refusal("forbidden", "this session is signed in to workspace ws_a, not ws_sp_other: sign in to ws_sp_other"),
    ]);
    expect(fake.requests).toEqual([]);
  });

  test("is not ready, and not probed, while the project is being deleted", async () => {
    const { cell, fake, m, a, id } = await created();
    await admin(cell, "offboard", "--project", id);
    await m.cache.refresh();
    const body = await (await m.get(`${PROJECTS}/${id}`, a)).json();
    expect([body.status, body.ready, body.reason]).toEqual(["deleting", false, "project_deleting"]);
    expect(fake.requests).toEqual([]);
  });

  test("is not ready, and not probed, while the owner has the project suspended, and is probed again once resumed", async () => {
    const { cell, fake, m, a, id } = await created();
    const project = `${PROJECTS}/${id}`;
    const ready = async () => (await (await m.get(project, a)).json()).ready;
    const statuses = () => fake.requests.map((r) => r.status);
    // Proven ready before the suspension.
    fake.publish(readBundle(cell.state));
    expect(await ready()).toBe(true);
    await admin(cell, "suspend", "--project", id);
    await m.cache.refresh();
    expect(await answer(await m.get(project, a))).toEqual([
      200, { ...(await listed(m, a)), ready: false, reason: "project_suspended" },
    ]);
    expect(statuses()).toEqual([200]);
    // The cell takes the suspension, then the owner resumes the project:
    // not ready while the cell refuses it 403 project_not_active, ready
    // once it takes the resume.
    fake.publish(readBundle(cell.state));
    await admin(cell, "resume", "--project", id);
    await m.cache.refresh();
    expect(await answer(await m.get(project, a))).toEqual([200, { ...(await listed(m, a)), ready: false }]);
    expect(statuses()).toEqual([200, 403]);
    fake.publish(readBundle(cell.state));
    m.clock.t += 5_000;
    expect(await ready()).toBe(true);
    expect(statuses()).toEqual([200, 403, 200]);
    // A suspension and a resume that no ask saw: probed again all the same.
    await admin(cell, "suspend", "--project", id);
    fake.publish(readBundle(cell.state));
    await admin(cell, "resume", "--project", id);
    await m.cache.refresh();
    m.clock.t += 5_000;
    expect(await ready()).toBe(false);
    fake.publish(readBundle(cell.state));
    m.clock.t += 5_000;
    expect(await ready()).toBe(true);
    expect(statuses()).toEqual([200, 403, 200, 403, 200]);
  });

  test("passes over an expired credential: a newer live one proves the project ready", async () => {
    const { cell, fake, m, a, id, cid, creds } = await created();
    const expiresAt = new Date(m.clock.t + 3_600_000).toISOString();
    expect((await m.send("POST", creds, a, { displayName: "expiring", expiresAt })).status).toBe(201);
    const live: string = (await (await m.send("POST", creds, a, { displayName: "live" })).json()).credential.id;
    expect((await m.send("POST", `${creds}/${cid}/revoke`, a)).status).toBe(200);
    fake.publish(readBundle(cell.state));
    m.clock.t += 2 * 3_600_000;
    await m.cache.refresh();
    expect(await answer(await m.get(`${PROJECTS}/${id}`, a))).toEqual([200, { ...(await listed(m, a)), ready: true }]);
    expect(fake.tokens.map((t) => [t.credential_id, t.scope])).toEqual([[live, "streams.catalog.read"]]);
    expect(fake.requests.map((r) => r.status)).toEqual([200]);
  });

  test("is proven by a live credential without catalog reads: the cell's 403 missing_scope comes after the policy and the grant", async () => {
    const { cell, fake, m, a, id, cid, creds } = await created();
    const narrow: string = (await (await m.send("POST", creds, a, { scopes: ["streams.records.read"] })).json()).credential.id;
    expect((await m.send("POST", `${creds}/${cid}/revoke`, a)).status).toBe(200);
    fake.publish(readBundle(cell.state));
    expect(await answer(await m.get(`${PROJECTS}/${id}`, a))).toEqual([200, { ...(await listed(m, a)), ready: true }]);
    expect(fake.tokens.map((t) => [t.credential_id, t.scope])).toEqual([[narrow, "streams.records.read"]]);
    expect(fake.requests.map((r) => r.status)).toEqual([403]);
  });

  test("says no_live_credential, without probing, while the project has no live credential, and is probed once it has one", async () => {
    const { cell, fake, m, a, id, cid, creds } = await created();
    const get = async () => answer(await m.get(`${PROJECTS}/${id}`, a));
    expect((await m.send("POST", `${creds}/${cid}/revoke`, a)).status).toBe(200);
    expect(await get()).toEqual([200, { ...(await listed(m, a)), ready: false, reason: "no_live_credential" }]);
    // An expired credential is not live either.
    const expiresAt = new Date(m.clock.t + 3_600_000).toISOString();
    expect((await m.send("POST", creds, a, { expiresAt })).status).toBe(201);
    m.clock.t += 2 * 3_600_000;
    await m.cache.refresh();
    expect((await get())[1]).toMatchObject({ ready: false, reason: "no_live_credential" });
    expect(fake.requests).toEqual([]);
    const live: string = (await (await m.send("POST", creds, a, {})).json()).credential.id;
    fake.publish(readBundle(cell.state));
    expect(await get()).toEqual([200, { ...(await listed(m, a)), ready: true }]);
    expect([fake.tokens.map((t) => t.credential_id), fake.requests.map((r) => r.status)]).toEqual([[live], [200]]);
  });
});

describe("deleting a project", () => {
  test("revokes every credential and adds an operator's, shows the project deleting, and drops it once the drain omits it", async () => {
    const cell = await allowedCell();
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    const made = await (await m.send("POST", PROJECTS, a, { displayName: "Orders" })).json();
    const id: string = made.project.id;
    const creds = `/v1/projects/${id}/streams/credentials`;
    const second = await (await m.send("POST", creds, a, {})).json();
    const before = cellState(cell);
    const res = await m.send("DELETE", `${PROJECTS}/${id}`, a);
    expect(await answer(res)).toEqual([202, { project: { ...made.project, status: "deleting" } }]);
    const after = cellState(cell);
    expect(after.projects[id]).toMatchObject({ phase: "offboarding", status: "active", project_policy_version: 1 });
    const operator = Object.entries(after.credentials).find(([, c]) => c.project_id === id && c.role === "operator")!;
    expect(operator[0]).toMatch(/^strcred_op_[0-9a-f]{32}$/);
    expect(operator[1]).toMatchObject({ status: "active", grant_version: 1, scopes: OPERATOR_SCOPES });
    for (const cid of [made.credential.id, second.credential.id]) {
      expect(after.credentials[cid]).toMatchObject({ status: "revoked", grant_version: 2 });
    }
    expect(after.feed_version).toBe(before.feed_version + 1);
    const grants = (readBundle(cell.state).grants.credentials as any[]).filter((g) => g.project_id === id);
    expect(grants.map((g) => [g.credential_id, g.status]).sort()).toEqual(
      [[made.credential.id, "revoked"], [second.credential.id, "revoked"], [operator[0], "active"]].sort(),
    );
    const minted = await m.issuer.fetch(new Request("https://issuer.example.com/v1/token/streams", {
      method: "POST", headers: { authorization: `StreamsCredential ${second.secret}` },
    }));
    expect(minted.status).toBe(403);
    expect((await (await m.get(PROJECTS, a)).json()).projects.map((p: any) => p.status)).toEqual(["deleting"]);
    expect((await (await m.get(creds, a)).json()).credentials.map((c: any) => c.status)).toEqual(["revoked", "revoked"]);
    expect(managementAudits(cell).at(-1)).toMatchObject({
      action: "project.delete", outcome: "done", project_id: id,
      credential_ids: [made.credential.id, second.credential.id, operator[0]].sort(),
      before: { feed_version: before.feed_version }, after: { feed_version: after.feed_version },
      request_id: res.headers.get("x-request-id"),
    });
    // A drain omits it (here the owner's cell-admin offboard; the issuer's
    // own job in offboarding.test.ts): the project is gone from the API.
    const fake = new FakeCell();
    try {
      fake.publish(readBundle(cell.state));
      await admin(cell, "offboard", "--project", id, "--cell-url", fake.url, "--key-file", cell.key, "--settle-secs", "0");
    } finally {
      fake.stop();
    }
    await m.cache.refresh();
    expect((await (await m.get(PROJECTS, a)).json()).projects).toEqual([]);
    expect((await m.get(`${PROJECTS}/${id}`, a)).status).toBe(404);
    expect(cellState(cell).projects[id].phase).toBe("offboarded");
  });

  test("refuses a project already being deleted or suspended 409, another workspace's 404, and a body 400", async () => {
    const cell = await allowedCell();
    const m = await managed(cell);
    const a = m.signIn("ws_a");
    const id: string = (await (await m.send("POST", PROJECTS, a, { displayName: "Orders" })).json()).project.id;
    await customerSecret(cell, "sp_other");
    await admin(cell, "suspend", "--project", id);
    await m.cache.refresh();
    expect(await answer(await m.send("DELETE", `${PROJECTS}/${id}`, a))).toEqual([
      409, refusal("project_not_active", `project ${id} is suspended: the Streams beta's operator resumes it before it can be deleted`),
    ]);
    await admin(cell, "resume", "--project", id);
    await m.cache.refresh();
    expect(await answer(await m.send("DELETE", `${PROJECTS}/${id}`, a, { confirm: true }))).toEqual([
      400, refusal("invalid_request", 'unknown member "confirm": the body takes nothing'),
    ]);
    expect((await m.send("DELETE", `${PROJECTS}/${id}`, a, {})).status).toBe(202);
    expect(await answer(await m.send("DELETE", `${PROJECTS}/${id}`, a))).toEqual([
      409, refusal("project_not_active", `project ${id} is already being deleted`),
    ]);
    expect(await answer(await m.send("DELETE", `${PROJECTS}/sp_other`, a))).toEqual([
      404, refusal("not_found", "workspace ws_a has no Streams project sp_other"),
    ]);
    expect(cellState(cell).projects.sp_other.phase).toBe("active");
  });
});
