// The issuer's offboarding job (public-beta issuer triage B5.1; design
// §10), against the fake cell (deploy/cell-admin/fake-cell.ts). Run with
// `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { generateKeyPairSync } from "node:crypto";
import { FakeCell } from "../cell-admin/fake-cell";
import { OPERATOR_SCOPES } from "../cell-admin/offboard";
import { canaryProbeCredential, setProjectField } from "../cell-admin/rules";
import { cleanup, denylistText, fakeS3, readBundle, readBundleText } from "../cell-admin/test-rig";
import { publicKeyRecord } from "../cell-admin/tokens";
import { Offboarding } from "./offboarding";
import { admin, cellState, customerSecret, envWith, issuerCell, managed, managementAudits } from "./rig";

const fakes: FakeCell[] = [];
afterEach(() => {
  for (const f of fakes.splice(0)) f.stop();
  cleanup();
});

const PROJECTS = "/v1/workspaces/ws_a/streams/projects";

/// A project the signed-in workspace ws_a created and then deleted, with
/// streams at the fake cell, beside a neighbour sp_b with streams of its
/// own; the fake serves the bundle from before the delete.
async function deleted() {
  const cell = await issuerCell();
  await admin(cell, "allow", "--workspace", "ws_a");
  const fake = new FakeCell();
  fakes.push(fake);
  const m = await managed(cell, envWith(cell, { STREAMS_CELL_ORIGIN: fake.url }));
  const a = m.signIn("ws_a");
  const made = await (await m.send("POST", PROJECTS, a, { displayName: "Orders" })).json();
  const id: string = made.project.id;
  await customerSecret(cell, "sp_b");
  await m.cache.refresh();
  fake.publish(readBundle(cell.state));
  fake.streams.set(id, new Set(["orders", "dir/a 2", "ü3"]));
  fake.streams.set("sp_b", new Set(["orders", "b2"]));
  expect((await m.send("DELETE", `${PROJECTS}/${id}`, a)).status).toBe(202);
  return { cell, fake, m, a, id, made };
}

const entriesOf = (cell: { state: string }, project: string) => {
  const b = readBundle(cell.state);
  return {
    policies: (b.policies.projects as any[]).filter((p) => p.project_id === project),
    grants: (b.grants.credentials as any[]).filter((g) => g.project_id === project),
  };
};

const MINUTE = 60_000;

describe("the offboarding job", () => {
  test("drains a deleted project unattended: proves the revocation, deletes its streams, omits it and audits it", async () => {
    const { cell, fake, m, a, id, made } = await deleted();
    fake.publish(readBundle(cell.state));
    const before = cellState(cell);
    const operator = Object.entries(before.credentials).find(([, c]) => c.project_id === id && c.role === "operator")![0];
    const neighbour = entriesOf(cell, "sp_b");
    // A create that passed authorization before the revocation lands
    // after the first walk.
    fake.afterWalk = (walks) => walks === 1 && fake.streams.get(id)!.add("late/arrival");
    await m.issuer.offboarding.runDue();
    // The proof: the operator reads the catalog, the revoked credential
    // is refused as revoked; then the walks.
    expect(fake.requests.slice(0, 2).map((r) => [r.path, r.status])).toEqual([
      ["/v1/streams?limit=1", 200],
      ["/v1/streams?limit=1", 403],
    ]);
    expect(fake.tokens[0]).toMatchObject({
      credential_id: operator, project_id: id, sub: "issuer-offboard", scope: OPERATOR_SCOPES, grant_version: 1,
    });
    expect(fake.tokens[1]).toMatchObject({ credential_id: made.credential.id, grant_version: 2 });
    // Pages of twice the project's rate (10 a second, the free tier).
    expect(fake.requests.filter((r) => r.path.startsWith("/v1/streams?limit=20")).map((r) => r.status)).toEqual(
      [200, 200, 200, 200],
    );
    expect([...fake.streams.get(id)!]).toEqual([]);
    expect([...fake.streams.get("sp_b")!].sort()).toEqual(["b2", "orders"]);
    // Omitted for good, in one published change; the neighbour untouched.
    const after = cellState(cell);
    expect(after.projects[id]).toMatchObject({ phase: "offboarded", status: "active", project_policy_version: 1 });
    expect(after.projects[id].offboarded_at).toBeString();
    expect(after.projects[id].offboard_attempts).toBeUndefined();
    const omitted = Object.entries(after.credentials).filter(([, c]) => c.project_id === id);
    expect(omitted.map(([cid, c]) => [cid, c.omitted]).sort()).toEqual([[made.credential.id, true], [operator, true]].sort());
    expect(after.feed_version).toBe(before.feed_version + 1);
    expect(entriesOf(cell, id)).toEqual({ policies: [], grants: [] });
    expect(entriesOf(cell, "sp_b")).toEqual(neighbour);
    expect(readBundle(cell.state).policies.feed_version).toBe(after.feed_version);
    expect(after.journal.at(-1)).toMatchObject({ command: "offboard", detail: `${id} catalog empty, 4 deleted, omitted` });
    // Audit objects are keyed by the millisecond: the delete's and the
    // omit's share one here, so the omit's is found by its action.
    const audits = managementAudits(cell).filter((e) => e.action === "project.offboard");
    expect(audits).toHaveLength(1);
    expect(audits[0]).toMatchObject({
      actor: "issuer:offboard", action: "project.offboard", outcome: "done", project_id: id,
      reason: "catalog empty; 4 stream(s) deleted by this attempt",
      credential_ids: [made.credential.id, operator].sort(),
      before: { feed_version: before.feed_version }, after: { feed_version: after.feed_version },
    });
    // Gone from the API; its id stays on the denylist; nothing is due.
    expect((await (await m.get(PROJECTS, a)).json()).projects).toEqual([]);
    expect((await m.get(`${PROJECTS}/${id}`, a)).status).toBe(404);
    expect(denylistText(cell.state).split("\n")).toContain(id);
    const asked = fake.requests.length;
    await m.issuer.offboarding.runDue();
    expect(fake.requests).toHaveLength(asked);
  });

  test("backs a failed attempt off 1, 2, 4 ... minutes up to an hour in the project's record, then finishes", async () => {
    const { cell, fake, m, id } = await deleted();
    const bundle = readBundleText(cell.state);
    const version = cellState(cell).feed_version;
    const waits = [1, 2, 4, 8, 16, 32, 60, 60].map((n) => n * MINUTE);
    for (const [i, wait] of waits.entries()) {
      await m.cache.refresh();
      const asked = fake.requests.length;
      await m.issuer.offboarding.runDue();
      // The cell does not serve the revocation: the operator is unknown.
      expect(fake.requests.slice(asked).map((r) => r.status)).toEqual([401]);
      const p = cellState(cell).projects[id];
      expect([p.phase, p.offboard_attempts, p.next_attempt_at]).toEqual([
        "offboarding", i + 1, new Date(m.clock.t + wait).toISOString(),
      ]);
      expect(m.logs.at(-1)).toBe(
        `offboarding ${id}: attempt ${i + 1} failed (the cell does not serve the offboarding bundle yet (operator ` +
          "credential answered 401 credential_unknown): publish it and run again); next attempt at " +
          `${p.next_attempt_at}`,
      );
      // Not before then.
      m.clock.t += wait - 1;
      await m.cache.refresh();
      await m.issuer.offboarding.runDue();
      expect(fake.requests).toHaveLength(asked + 1);
      m.clock.t += 1;
    }
    // Bookkeeping only: nothing the cell holds changed.
    expect([cellState(cell).feed_version, readBundleText(cell.state)]).toEqual([version, bundle]);
    fake.publish(readBundle(cell.state));
    await m.cache.refresh();
    await m.issuer.offboarding.runDue();
    expect(cellState(cell).projects[id]).toMatchObject({ phase: "offboarded", offboard_attempts: 8 });
    expect([...fake.streams.get(id)!]).toEqual([]);
  });

  test("leaves a project another writer omitted meanwhile, and waits while its copy of the state is stale", async () => {
    const { cell, fake, m, id } = await deleted();
    // Stale: the copy was last revalidated over 300 s ago.
    m.clock.t += 301_000;
    await m.issuer.offboarding.runDue();
    expect(fake.requests).toEqual([]);
    await m.cache.refresh();
    // The owner's cell-admin finishes it while the issuer's copy is behind.
    fake.publish(readBundle(cell.state));
    await admin(cell, "offboard", "--project", id, "--cell-url", fake.url, "--key-file", cell.key, "--settle-secs", "0");
    fake.publish(readBundle(cell.state));
    const done = cellState(cell);
    expect(m.cache.state.projects[id].phase).toBe("offboarding");
    await m.issuer.offboarding.runDue();
    const after = cellState(cell);
    expect(after.projects[id]).toEqual(done.projects[id]);
    expect(after.feed_version).toBe(done.feed_version);
    expect(managementAudits(cell).filter((e) => e.action === "project.offboard")).toEqual([]);
  });

  test("checks it can mint before each attempt, not once a round: a key retired during one attempt stops the next", async () => {
    const { cell, fake, m, id } = await deleted();
    // A second project being deleted, due in the same round after the first.
    await customerSecret(cell, "sp_z");
    await admin(cell, "offboard", "--project", "sp_z");
    await m.cache.refresh();
    fake.publish(readBundle(cell.state));
    fake.streams.set("sp_z", new Set(["z1"]));
    expect([id, "sp_z"].sort()).toEqual([id, "sp_z"]);
    // During the first project's first walk the owner's compromise drill
    // (keys add --retire-others) lands in the store: the issuer's key is
    // no longer the cell's.
    const { publicKey } = generateKeyPairSync("ed25519");
    const next = publicKeyRecord(publicKey.export({ type: "spki", format: "pem" }).toString());
    const [bucket, ...rest] = `${cell.state.slice("s3://".length)}/cell-state.json`.split("/");
    fake.afterWalk = (walks) => {
      if (walks !== 1) return;
      const s = JSON.parse(fakeS3().text(bucket, rest.join("/"))!);
      s.retired_kids.push(...s.keys.map((k: { kid: string }) => k.kid));
      s.keys = [next];
      s.feed_version += 1;
      fakeS3().put(bucket, rest.join("/"), JSON.stringify(s));
    };
    await m.issuer.offboarding.runDue();
    // The first attempt finished, and its commit brought the new keys in.
    expect([cellState(cell).projects[id].phase, m.cache.state.keys.map((k) => k.kid)]).toEqual(["offboarded", [next.kid]]);
    // The second made no request under the retired key, and no attempt.
    expect(fake.tokens.filter((t) => t.project_id === "sp_z")).toEqual([]);
    expect(cellState(cell).projects.sp_z).toMatchObject({ phase: "offboarding" });
    expect(cellState(cell).projects.sp_z.offboard_attempts).toBeUndefined();
  });

  test("drains a project cell-admin offboarded while suspended in one attempt: waits for the canary witness, resumes it, waits for the proof, drains and omits it, auditing the resume", async () => {
    const { cell, fake, m, a, id } = await deleted();
    await customerSecret(cell, "sp_c");
    const customer = Object.entries(cellState(cell).credentials).find(([, c]) => c.project_id === "sp_c")![0];
    await admin(cell, "suspend", "--project", "sp_c");
    const lagging = readBundle(cell.state);
    await admin(cell, "offboard", "--project", "sp_c");
    await m.cache.refresh();
    const offboarded = cellState(cell);
    const canary = canaryProbeCredential(offboarded);
    const operator = Object.entries(offboarded.credentials).find(([, c]) => c.project_id === "sp_c" && c.role === "operator")![0];
    expect(offboarded.projects.sp_c).toMatchObject({ status: "suspended", witness_grant_version: 2 });
    // The cell holds the suspension, not the revocation; it takes what
    // the store holds only during the job's pauses.
    fake.streams.set("sp_c", new Set(["c1", "c2"]));
    let served = lagging;
    fake.follow = () => served;
    const slept: number[] = [];
    const job = new Offboarding(m.cfg, {
      cache: m.cache, audit: m.issuer.audit, now: () => m.clock.t, log: (l) => m.logs.push(l), settleMs: 0,
      sleep: async (ms) => {
        slept.push(ms);
        served = readBundle(cell.state);
      },
    });
    await job.runDue();
    const after = cellState(cell);
    expect(after.projects.sp_c).toMatchObject({ phase: "offboarded", status: "active", project_policy_version: 3 });
    expect(after.projects.sp_c.offboard_attempts).toBeUndefined();
    expect(slept).toEqual([2000, 2000]);
    // The witness (not yet, then 200 once the cell took the revocation),
    // then the proof (the operator refused for the project's status until
    // the cell took the resume), then the walks.
    const kind: Record<string, string> = { [canary]: "canary", [operator]: "operator", [customer]: "customer" };
    expect(fake.requests.filter((q) => kind[q.credential ?? ""]).slice(0, 5).map((q) => [kind[q.credential!], q.status]))
      .toEqual([["canary", 401], ["canary", 200], ["operator", 403], ["operator", 200], ["customer", 403]]);
    expect([...fake.streams.get("sp_c")!]).toEqual([]);
    expect(m.logs.filter((l) => l.startsWith("offboarding sp_c: ")).slice(0, 2)).toEqual([
      "offboarding sp_c: the cell holds the revocation of sp_c: the canary's catalog-read credential answered 200 " +
        "at grant_version 2",
      "offboarding sp_c: sp_c is active again at project_policy_version 3, for its drain",
    ]);
    // Audit objects are keyed by the millisecond, which this clock keeps:
    // found by their actions.
    const audits = managementAudits(cell).filter((e) => e.project_id === "sp_c");
    expect(audits.map((e) => [e.actor, e.action, e.outcome]).sort()).toEqual([
      ["issuer:offboard", "project.offboard", "done"], ["issuer:offboard", "project.resume", "done"],
    ]);
    const resume = audits.find((e) => e.action === "project.resume");
    expect(resume).toMatchObject({
      reason: "the cell holds the revocation (the canary answered 200 at grant_version 2): active for its drain",
      before: { project_policy_version: 2 }, after: { project_policy_version: 3 },
    });
    // One change: the deleted project's omit may come before it.
    expect(resume.after.feed_version).toBe(resume.before.feed_version + 1);
    // The deleted project drained beside it.
    expect(after.projects[id].phase).toBe("offboarded");
    expect((await m.get(`${PROJECTS}/${id}`, a)).status).toBe(404);
  });

  test("mints each witness probe at the canary's grant_version as its revalidated copy holds it then: another suspended project offboarded during the wait does not fail the attempt", async () => {
    const { cell, fake, m } = await deleted();
    for (const p of ["sp_c", "sp_d"]) {
      await customerSecret(cell, p);
      await admin(cell, "suspend", "--project", p);
    }
    const lagging = readBundle(cell.state);
    await admin(cell, "offboard", "--project", "sp_c");
    await m.cache.refresh();
    const canary = canaryProbeCredential(cellState(cell));
    // The cell holds the suspensions, not sp_c's revocation. During the
    // job's first pause the owner offboards sp_d too (the canary moves on
    // to grant_version 3), and the cell takes that bundle, which holds
    // both revocations, never sp_c's own.
    let served = lagging;
    fake.follow = () => served;
    const slept: number[] = [];
    const job = new Offboarding(m.cfg, {
      cache: m.cache, audit: m.issuer.audit, now: () => m.clock.t, log: (l) => m.logs.push(l), settleMs: 0,
      waitMs: 10_000,
      sleep: async (ms) => {
        slept.push(ms);
        if (slept.length === 1) await admin(cell, "offboard", "--project", "sp_d");
        served = readBundle(cell.state);
      },
    });
    await job.runDue();
    expect(cellState(cell).projects.sp_c).toMatchObject({ phase: "offboarded", status: "active", witness_grant_version: 2 });
    expect(cellState(cell).projects.sp_c.offboard_attempts).toBeUndefined();
    expect(fake.requests.filter((q) => q.credential === canary).map((q) => q.status)).toEqual([401, 200]);
    expect(fake.tokens.filter((t) => t.credential_id === canary).map((t) => t.grant_version)).toEqual([2, 3]);
    expect(m.logs).toContain(
      "offboarding sp_c: the cell holds the revocation of sp_c: the canary's catalog-read credential answered 200 " +
        "at grant_version 3",
    );
    expect(managementAudits(cell).find((e) => e.project_id === "sp_c" && e.action === "project.resume")).toMatchObject({
      reason: "the cell holds the revocation (the canary answered 200 at grant_version 3): active for its drain",
    });
    expect(cellState(cell).projects.sp_d).toMatchObject({ phase: "offboarding", status: "suspended", witness_grant_version: 3 });
  });

  test("fails, asking the cell nothing for it, the attempts at a suspended project being offboarded without a witness version", async () => {
    const { cell, fake, m, id } = await deleted();
    // The issuer's delete refuses a suspended project, and cell-admin
    // records a witness for one; a state edited by hand can still hold one
    // without, whose revocation no canary answer proves.
    await customerSecret(cell, "sp_c");
    await admin(cell, "offboard", "--project", "sp_c");
    await m.cache.refresh();
    await m.cache.mutate((s) => setProjectField(s, "sp_c", { status: "suspended" }));
    fake.publish(readBundle(cell.state));
    await m.cache.refresh();
    await m.issuer.offboarding.runDue();
    expect(cellState(cell).projects.sp_c).toMatchObject({ phase: "offboarding", status: "suspended", offboard_attempts: 1 });
    expect(fake.tokens.map((t) => t.project_id).filter((p) => p === "sp_c" || p.startsWith("sp_canary_"))).toEqual([]);
    expect(m.logs).toContain(
      "offboarding sp_c: attempt 1 failed (project sp_c is suspended and being offboarded without a witness " +
        "version: its revocation was not made by offboard's first run, so no canary answer proves the cell holds " +
        `it); next attempt at ${cellState(cell).projects.sp_c.next_attempt_at}`,
    );
    expect(cellState(cell).projects[id].phase).toBe("offboarded");
  });

  test("runs on its interval once started", async () => {
    const { cell, fake, m, id } = await deleted();
    fake.publish(readBundle(cell.state));
    const stop = m.issuer.offboarding.start(10);
    try {
      // Up to 10 s of wall time: a loaded 4-vCPU CI runner is slow.
      for (let i = 0; i < 1000 && cellState(cell).projects[id].phase !== "offboarded"; i++) {
        await new Promise((r) => setTimeout(r, 10));
      }
    } finally {
      stop();
    }
    expect(cellState(cell).projects[id].phase).toBe("offboarded");
  });
});
