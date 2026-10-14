// cell-admin offboard against a fake cell (review findings M4 and R6, the
// F-reuse leg of PLAN step 9). Run with `bun test ./deploy/cell-admin`.
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { join } from "node:path";
import { validateDocument } from "../../platform-demo/src/validate.mjs";
import { type Bundle, bundleOf, schema } from "./feeds";
import { drainProject, type Minter, OPERATOR_SCOPES, resumeForDrain, witnessRevocation } from "./offboard";
import { FakeCell } from "./fake-cell";
import { canaryProbeCredential } from "./rules";
import { customerClaims, loadSigningKey, signJwt } from "./tokens";
import {
  type Cell,
  cleanup,
  customerCredential,
  initCell,
  readBundle,
  readBundleText,
  readState,
  run,
  STORES,
  storeTexts,
  useStore,
  writeStateText,
} from "./test-rig";

let fake: FakeCell | undefined;
afterEach(() => {
  fake?.stop();
  fake = undefined;
  cleanup();
});

/// A cell with proj_a (offboarded by each test) and its neighbour proj_b.
async function twoProjects(): Promise<Cell> {
  const c = await initCell();
  await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "wksp_a");
  await run("admit", "--state", c.state, "--project", "proj_b", "--workspace", "wksp_b");
  return c;
}

const entriesOf = (c: Cell, project: string) => {
  const b = readBundle(c.state);
  return {
    policies: (b.policies.projects as any[]).filter((p) => p.project_id === project),
    grants: (b.grants.credentials as any[]).filter((g) => g.project_id === project),
  };
};

/// `project`'s entry in a bundle's policy feed, or undefined.
const policyOf = (b: Bundle, project: string) =>
  (b.policies.projects as any[]).find((p) => p.project_id === project) as { status: string } | undefined;

const second = (c: Cell, extra: string[] = []) =>
  run("offboard", "--state", c.state, "--project", "proj_a", "--cell-url", fake!.url, "--key-file", c.key,
    "--settle-secs", "0", ...extra);

for (const kind of STORES) {
  describe(`on the ${kind} store`, () => {
    beforeEach(() => useStore(kind));

    describe("offboard", () => {
      test("the first run revokes the customer credentials at a newer version beside an operator credential", async () => {
        const c = await twoProjects();
        const cred = customerCredential(c.state, "proj_a");
        const neighbour = entriesOf(c, "proj_b");
        const v = readBundle(c.state).grants.feed_version;
        const r = await run("offboard", "--state", c.state, "--project", "proj_a");
        expect(r.code).toBe(0);
        const a = entriesOf(c, "proj_a");
        expect(a.policies).toHaveLength(1);
        expect(a.policies[0].project_policy_version).toBe(1);
        const op = a.grants.find((g) => g.credential_id !== cred);
        expect(a.grants.find((g) => g.credential_id === cred)).toMatchObject({ status: "revoked", grant_version: 2 });
        expect(op).toMatchObject({ status: "active", grant_version: 1, scopes: OPERATOR_SCOPES });
        expect(op.credential_id).toMatch(/^strcred_op_[0-9a-f]{32}$/);
        expect(op.stream_prefixes).toBeUndefined();
        expect(entriesOf(c, "proj_b")).toEqual(neighbour);
        expect(readBundle(c.state).grants.feed_version).toBe(v + 1);
        expect(validateDocument(readBundle(c.state).grants, schema("credential-grants.schema.json"))).toEqual([]);
        const t = await run("token", "--state", c.state, "--credential", cred, "--key-file", c.key, "--out",
          join(c.root, "t.jwt"));
        expect(t.err).toBe(`cell-admin token: credential ${cred} is not active (project proj_a)`);
      });

      test("the first run on a suspended project revokes it and bumps the canary's catalog-read credential in one change, recorded as its witness; it stays suspended", async () => {
        const c = await twoProjects();
        const cred = customerCredential(c.state, "proj_a");
        expect((await run("suspend", "--state", c.state, "--project", "proj_a")).code).toBe(0);
        const before = readState(c.state);
        const canary = canaryProbeCredential(before);
        const neighbour = entriesOf(c, "proj_b");
        const r = await run("offboard", "--state", c.state, "--project", "proj_a");
        expect([r.code, r.err]).toEqual([0, ""]);
        expect(r.out.split("\n")[1]).toBe("proj_a stays suspended: the canary's catalog-read credential moved to " +
          "grant_version 2 in the same change, the witness the second run waits for");
        const after = readState(c.state);
        expect(after.projects.proj_a).toMatchObject({
          phase: "offboarding", status: "suspended", project_policy_version: 2, witness_grant_version: 2,
        });
        expect([after.credentials[canary].grant_version, after.feed_version]).toEqual([2, before.feed_version + 1]);
        const bundle = readBundle(c.state);
        expect(entriesOf(c, "proj_a").policies).toEqual([expect.objectContaining({ status: "suspended", project_policy_version: 2 })]);
        expect(entriesOf(c, "proj_a").grants.find((g) => g.credential_id === cred)).toMatchObject({ status: "revoked", grant_version: 2 });
        expect((bundle.grants.credentials as any[]).find((g) => g.credential_id === canary)).toEqual({
          credential_id: canary, project_id: "sp_canary_cell_sc1", grant_version: 2, status: "active",
          scopes: "streams.catalog.read", expires_at: null,
        });
        expect(bundle.grants.feed_version).toBe(after.feed_version);
        expect(readBundleText(c.state)).not.toContain("witness");
        expect(entriesOf(c, "proj_b")).toEqual(neighbour);
        // While it stays suspended the cell refuses a token minted before
        // the revocation for the project's status, before its credential.
        fake = new FakeCell();
        fake.publish(bundle);
        const old = signJwt(loadSigningKey(c.key), customerClaims(before, cred, 300, "test"));
        const res = await fetch(`${fake.url}/v1/streams?limit=1`, { headers: { authorization: `Bearer ${old}` } });
        expect([res.status, (await res.json()).error.code]).toEqual([403, "project_not_active"]);
      });

      test("the second run sets a suspended project active only once the canary answers 200 at its witness version, then proves, drains and omits it; no bundle holds an active customer credential beside an active policy", async () => {
        const c = await twoProjects();
        const cred = customerCredential(c.state, "proj_a");
        expect((await run("suspend", "--state", c.state, "--project", "proj_a")).code).toBe(0);
        const suspended = readBundle(c.state);
        expect((await run("offboard", "--state", c.state, "--project", "proj_a")).code).toBe(0);
        const canary = canaryProbeCredential(readState(c.state));
        const operator = Object.entries(readState(c.state).credentials).find(([, k]) => k.role === "operator")![0];
        // A cell that holds the suspension but not yet the revocation.
        fake = new FakeCell();
        fake.publish(suspended);
        fake.streams.set("proj_a", new Set(["a1", "a2"]));
        const before = readBundleText(c.state);
        let r = await second(c, ["--wait-secs", "0"]);
        expect([r.code, r.err]).toEqual([3, "cell-admin offboard: the cell does not hold the revocation of proj_a yet: " +
          "the canary's catalog-read credential answered 401 grant_version_mismatch at grant_version 2, not 200; " +
          "publish the bundle, wait, run again"]);
        expect([readBundleText(c.state), readState(c.state).projects.proj_a.status]).toEqual([before, "suspended"]);
        expect(fake.requests.map((q) => [q.credential, q.status])).toEqual([[canary, 401]]);
        // From now on the cell takes whatever the store holds at each
        // request; every bundle it takes is kept, with whether the canary
        // had answered 200 before it.
        const taken = new Map<number, { bundle: Bundle; witnessed: boolean }>();
        fake.follow = () => {
          const b = readBundle(c.state);
          const witnessed = fake!.requests.some((q) => q.credential === canary && q.status === 200);
          if (!taken.has(b.policies.feed_version)) taken.set(b.policies.feed_version, { bundle: b, witnessed });
          return b;
        };
        fake.requests = [];
        r = await second(c, ["--wait-secs", "0"]);
        expect([r.code, r.err]).toEqual([0, ""]);
        expect(r.out.split("\n").slice(0, 2)).toEqual([
          "the cell holds the revocation of proj_a: the canary's catalog-read credential answered 200 at grant_version 2",
          "proj_a is active again at project_policy_version 3, for its drain",
        ]);
        expect(r.out.split("\n").filter((l) => l.startsWith("walk "))).toEqual([
          "walk 1: listed 2, deleted 2",
          "walk 2: listed 0, deleted 0",
        ]);
        // The witness, then the proof (the operator reads, the revoked
        // credential is refused as revoked), then the walks.
        expect(fake.requests.slice(0, 3).map((q) => [q.credential, q.status])).toEqual([
          [canary, 200], [operator, 200], [cred, 403],
        ]);
        expect([...fake.streams.get("proj_a")!]).toEqual([]);
        const done = readState(c.state);
        expect(done.projects.proj_a).toMatchObject({ phase: "offboarded", status: "active", project_policy_version: 3 });
        expect(done.journal.slice(-2).map((j) => j.detail)).toEqual([
          "proj_a resumed for its drain at policy=v3", "proj_a catalog empty, 2 deleted, omitted",
        ]);
        // The cell took the revocation and then the resume, each bundle in
        // turn; the last is the omit. The policy turned active only in a
        // bundle the cell took after the witness, and beside revoked
        // credentials only.
        const v = JSON.parse(before).policies.feed_version;
        expect([...taken.keys(), readBundle(c.state).policies.feed_version]).toEqual([v, v + 1, v + 2]);
        expect([...taken.values()].map((t) => [policyOf(t.bundle, "proj_a")?.status, t.witnessed])).toEqual([
          ["suspended", false], ["active", true],
        ]);
        for (const b of [suspended, ...[...taken.values()].map((t) => t.bundle), readBundle(c.state)]) {
          const active = (b.grants.credentials as any[]).filter((g) => g.credential_id === cred && g.status === "active");
          if (policyOf(b, "proj_a")?.status === "active") expect(active).toEqual([]);
        }
        expect(entriesOf(c, "proj_a")).toEqual({ policies: [], grants: [] });
      });

      test("the second run refuses a suspended project being offboarded without a witness version, asking the cell nothing", async () => {
        const c = await twoProjects();
        await run("offboard", "--state", c.state, "--project", "proj_a");
        // A state edited by hand: suspended after its revocation, so no
        // canary version was moved with the revocation.
        const s = readState(c.state);
        s.projects.proj_a.status = "suspended";
        s.projects.proj_a.project_policy_version += 1;
        s.feed_version += 1;
        writeStateText(c.state, JSON.stringify(s));
        fake = new FakeCell();
        fake.publish(readBundle(c.state));
        const r = await second(c, ["--wait-secs", "0"]);
        expect([r.code, r.err]).toEqual([1, "cell-admin offboard: project proj_a is suspended and being offboarded " +
          "without a witness version: its revocation was not made by offboard's first run, so no canary answer " +
          "proves the cell holds it"]);
        expect(fake.requests).toEqual([]);
        expect(readState(c.state).projects.proj_a).toMatchObject({ phase: "offboarding", status: "suspended" });
      });

      test("the second run waits (exit 3) until the cell serves the revocation, and changes nothing", async () => {
        const c = await twoProjects();
        fake = new FakeCell();
        fake.publish(readBundle(c.state));
        await run("offboard", "--state", c.state, "--project", "proj_a");
        const before = readBundleText(c.state);
        let r = await second(c);
        expect([r.code, r.err]).toEqual([3, "cell-admin offboard: the cell does not serve the offboarding bundle " +
          "yet (operator credential answered 401 credential_unknown): publish it and run again"]);
        // A cell that knows the operator but still holds the customer active.
        const tampered = readBundle(c.state);
        const cred = customerCredential(c.state, "proj_a");
        (tampered.grants.credentials as any[]).find((g) => g.credential_id === cred).status = "active";
        fake.publish(tampered);
        r = await second(c);
        expect([r.code, r.err]).toEqual([3, `cell-admin offboard: revoked credential ${cred} answered 200, not ` +
          "403 credential_not_active: the revocation has not reached the cell; run again"]);
        expect(readBundleText(c.state)).toBe(before);
        expect(readState(c.state).projects.proj_a.phase).toBe("offboarding");
      });

      test("walks until one full walk lists nothing, then omits the project; the neighbour is untouched", async () => {
        const c = await twoProjects();
        await run("offboard", "--state", c.state, "--project", "proj_a");
        fake = new FakeCell();
        fake.publish(readBundle(c.state));
        fake.streams.set("proj_a", new Set(["a1", "dir/a 2", "ü3", "a4", "a5"]));
        fake.streams.set("proj_b", new Set(["a1", "b2", "b3"]));
        // A create that passed authorization before the revocation lands
        // after the first walk.
        fake.afterWalk = (walks) => walks === 1 && fake!.streams.get("proj_a")!.add("late/arrival");
        fake.rateLimitNext = 1;
        const neighbour = entriesOf(c, "proj_b");
        const r = await second(c);
        expect(r.code).toBe(0);
        expect(r.out.split("\n").filter((l) => l.startsWith("walk "))).toEqual([
          "walk 1: listed 5, deleted 5",
          "walk 2: listed 1, deleted 1",
          "walk 3: listed 0, deleted 0",
        ]);
        expect([...fake.streams.get("proj_a")!]).toEqual([]);
        expect([...fake.streams.get("proj_b")!].sort()).toEqual(["a1", "b2", "b3"]);
        expect(fake.requests.filter((q) => q.status === 429)).toHaveLength(1);
        expect(entriesOf(c, "proj_a")).toEqual({ policies: [], grants: [] });
        expect(entriesOf(c, "proj_b")).toEqual(neighbour);
        expect(readState(c.state).projects.proj_a.phase).toBe("offboarded");
        // F-reuse: the id is never placed again, and offboarding is final.
        expect((await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "wksp_z")).err)
          .toBe("cell-admin admit: project id proj_a was used before: an id is never placed twice");
        expect((await run("offboard", "--state", c.state, "--project", "proj_a")).err)
          .toBe("cell-admin offboard: project proj_a is already offboarded");
        for (const text of storeTexts(c.state)) expect(text).not.toMatch(/eyJ[A-Za-z0-9_-]+\.eyJ/);
      });

      test("a catalog that never empties stops after --max-walks with the project still placed", async () => {
        const c = await twoProjects();
        await run("offboard", "--state", c.state, "--project", "proj_a");
        fake = new FakeCell();
        fake.publish(readBundle(c.state));
        // Each walk deletes what it listed, and a new stream lands behind it.
        fake.streams.set("proj_a", new Set(["seed"]));
        let n = 0;
        fake.afterWalk = () => fake!.streams.get("proj_a")!.add(`again-${n++}`);
        const r = await second(c, ["--max-walks", "3"]);
        expect([r.code, r.err]).toEqual([3, "cell-admin offboard: the catalog of proj_a was not empty after 3 walks; run again"]);
        expect(entriesOf(c, "proj_a").policies).toHaveLength(1);
        expect(readState(c.state).projects.proj_a.phase).toBe("offboarding");
      });

      test("an empty walk ends offboarding only once the settle time has passed since the proof", async () => {
        const c = await twoProjects();
        await run("offboard", "--state", c.state, "--project", "proj_a");
        fake = new FakeCell();
        fake.publish(readBundle(c.state));
        const t0 = Date.now();
        const r = await run("offboard", "--state", c.state, "--project", "proj_a", "--cell-url", fake.url,
          "--key-file", c.key, "--settle-secs", "1");
        expect(r.code).toBe(0);
        expect(Date.now() - t0).toBeGreaterThanOrEqual(1000);
        expect(r.out.split("\n").filter((l) => l.startsWith("walk "))).toEqual([
          "walk 1: listed 0, deleted 0",
          "walk 2: listed 0, deleted 0",
        ]);
      });

      test("a token never travels over plain http to another host", async () => {
        const c = await twoProjects();
        await run("offboard", "--state", c.state, "--project", "proj_a");
        const r = await run("offboard", "--state", c.state, "--project", "proj_a", "--cell-url",
          "http://cell.example.invalid", "--key-file", c.key);
        expect([r.code, r.err.split("\n")[0]]).toEqual([2,
          "cell-admin offboard: --cell-url http://cell.example.invalid: tokens go to a cell over https only"]);
      });
    });
  });
}

describe("on the s3 store, which has no lock", () => {
  beforeEach(() => useStore("s3"));

  test("the second run's witness mints each probe at the canary's grant_version as the state holds it then: another suspended project's first run during its wait does not time it out", async () => {
    const c = await twoProjects();
    for (const p of ["proj_a", "proj_b"]) expect((await run("suspend", "--state", c.state, "--project", p)).code).toBe(0);
    const suspended = readBundle(c.state);
    expect((await run("offboard", "--state", c.state, "--project", "proj_a")).code).toBe(0);
    const canary = canaryProbeCredential(readState(c.state));
    // A cell that holds the suspensions, not yet proj_a's revocation.
    fake = new FakeCell();
    fake.publish(suspended);
    const drained = second(c, ["--wait-secs", "10"]);
    while (fake.requests.length === 0) await Bun.sleep(10);
    // Before the witness's second probe, 2 s later, the owner offboards
    // proj_b too (the canary moves on to grant_version 3), and the cell
    // takes that bundle, which holds both revocations, never proj_a's own.
    expect((await run("offboard", "--state", c.state, "--project", "proj_b")).code).toBe(0);
    fake.follow = () => readBundle(c.state);
    const r = await drained;
    expect([r.code, r.err]).toEqual([0, ""]);
    expect(r.out.split("\n")[0]).toBe(
      "the cell holds the revocation of proj_a: the canary's catalog-read credential answered 200 at grant_version 3",
    );
    expect(fake.requests.filter((q) => q.credential === canary).map((q) => q.status)).toEqual([401, 200]);
    expect(fake.tokens.filter((t) => t.credential_id === canary).map((t) => t.grant_version)).toEqual([2, 3]);
    expect(readState(c.state).projects.proj_a).toMatchObject({ phase: "offboarded", witness_grant_version: 2 });
    expect(readState(c.state).projects.proj_b).toMatchObject({ phase: "offboarding", status: "suspended", witness_grant_version: 3 });
  }, 30_000);
});

describe("waiting for the cell inside one run", () => {
  /// proj_a suspended and then offboarded (the first run), with the
  /// bundle from before the revocation, and a minter under the cell's key.
  async function suspendedOffboarding() {
    const c = await initCell();
    await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "wksp_a");
    expect((await run("suspend", "--state", c.state, "--project", "proj_a")).code).toBe(0);
    const suspended = readBundle(c.state);
    expect((await run("offboard", "--state", c.state, "--project", "proj_a")).code).toBe(0);
    const sk = loadSigningKey(c.key);
    const mint: Minter = (s, credential, ttl) => signJwt(sk, customerClaims(s, credential, ttl, "test"));
    fake = new FakeCell();
    fake.publish(suspended);
    return { c, suspended, mint, state: readState(c.state) };
  }

  test("the witness asks every 2 s for up to its wait: 401 grant_version_mismatch and 403 are not yet, any other answer refuses at once", async () => {
    const { c, suspended, mint, state } = await suspendedOffboarding();
    const canary = canaryProbeCredential(state);
    const slept: number[] = [];
    // The cell takes the revocation during the second pause.
    const sleep = async (ms: number) => {
      slept.push(ms);
      if (slept.length === 2) fake!.publish(readBundle(c.state));
    };
    const reach = { base: fake!.url, mint, log: () => {}, sleep };
    expect(await witnessRevocation(state, "proj_a", { ...reach, waitMs: 120_000 })).toBe(2);
    expect([slept, fake!.requests.map((q) => [q.credential, q.status])]).toEqual([
      [2000, 2000], [[canary, 401], [canary, 401], [canary, 200]],
    ]);
    // A wait of 4 s asks three times, then exit 3.
    fake!.publish(suspended);
    fake!.requests = [];
    await expect(witnessRevocation(state, "proj_a", { ...reach, waitMs: 4_000, sleep: async () => {} })).rejects.toMatchObject({
      code: 3,
      message: "the cell does not hold the revocation of proj_a yet: the canary's catalog-read credential answered " +
        "401 grant_version_mismatch at grant_version 2, not 200; publish the bundle, wait, run again",
    });
    expect(fake!.requests).toHaveLength(3);
    // A canary refused as not active is not yet either.
    const refused = readBundle(c.state);
    (refused.grants.credentials as any[]).find((g) => g.credential_id === canary).status = "revoked";
    fake!.publish(refused);
    await expect(witnessRevocation(state, "proj_a", { ...reach, waitMs: 0 })).rejects.toMatchObject({ code: 3 });
    // A cell that does not serve the key answers what no later bundle of
    // this state turns into 200: refused at once, exit 1.
    fake!.live = undefined;
    fake!.requests = [];
    await expect(witnessRevocation(state, "proj_a", { ...reach, waitMs: 120_000 })).rejects.toMatchObject({
      code: 1,
      message: "the canary's catalog-read credential answered 401 bad_signature, which is not a grant the cell " +
        "has yet to take: the cell does not serve this key or the canary (cell-admin canary shows which)",
    });
    expect(fake!.requests).toHaveLength(1);
  });

  test("after the resume, the proof asks every 2 s until the cell serves the active policy, then drains", async () => {
    const { c, mint, state } = await suspendedOffboarding();
    expect(resumeForDrain(state, "proj_a")).toBe(true);
    // The cell holds the revocation, the project still suspended.
    fake!.publish(readBundle(c.state));
    fake!.streams.set("proj_a", new Set(["a1"]));
    const slept: number[] = [];
    const deleted = await drainProject(state, "proj_a", {
      base: fake!.url, mint, maxWalks: 10, settleMs: 0, waitMs: 120_000, log: () => {},
      sleep: async (ms) => {
        slept.push(ms);
        fake!.publish(bundleOf(state));
      },
    });
    expect([deleted, slept]).toEqual([1, [2000]]);
    // The operator refused for the project's status, then served.
    expect(fake!.requests.slice(0, 3).map((q) => q.status)).toEqual([403, 200, 403]);
  });

  test("resumeForDrain sets a suspended project being offboarded active at a newer policy version, only while none of its customer credentials is active, and leaves one already active", async () => {
    const { state } = await suspendedOffboarding();
    const cred = Object.entries(state.credentials).find(([, k]) => k.project_id === "proj_a" && k.role === "customer")![0];
    const tampered = structuredClone(state);
    tampered.credentials[cred].status = "active";
    expect(() => resumeForDrain(tampered, "proj_a")).toThrow(
      `project proj_a still has active customer credential(s) ${cred}: it is not resumed for its drain`,
    );
    expect(tampered.projects.proj_a.status).toBe("suspended");
    const v = state.feed_version;
    expect(resumeForDrain(state, "proj_a")).toBe(true);
    expect([state.projects.proj_a.status, state.projects.proj_a.project_policy_version, state.feed_version]).toEqual([
      "active", 3, v + 1,
    ]);
    expect(state.journal.at(-1)?.detail).toBe("proj_a resumed for its drain at policy=v3");
    expect([resumeForDrain(state, "proj_a"), state.feed_version]).toEqual([false, v + 1]);
  });
});

describe("the drain at a project's own rate", () => {
  test("drains a 2,000-stream project at 10 requests a second under phase B's store bucket, longer than an operator token lives: small pages, Retry-After honoured", async () => {
    const c = await initCell();
    await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "wksp_a", "--quota", "requests_per_sec=10");
    expect((await run("offboard", "--state", c.state, "--project", "proj_a")).code).toBe(0);
    // One clock for the drain's waits, the minter and the fake cell (its
    // tokens' expiry and its store bucket).
    const clock = { t: Date.now() };
    const t0 = clock.t;
    fake = new FakeCell();
    fake.publish(readBundle(c.state));
    fake.pageSize = 1000;
    fake.now = () => clock.t;
    // P1 (b′): listing N streams weighs N + 10, a delete 11, at the
    // project's 10 a second; a listing of 1,000 would leave ~100 s of debt.
    // About 1.25 s a stream, so the walk outlasts the 1,770 s an operator
    // token is usable (30 min, less the 60 s backdate and the cell's 30 s
    // of skew) after about 1,400 streams.
    fake.storeOps = { perSec: 10 };
    fake.streams.set("proj_a", new Set(Array.from({ length: 2000 }, (_, i) => `s${String(i).padStart(4, "0")}`)));
    const sk = loadSigningKey(c.key);
    const deleted = await drainProject(readState(c.state), "proj_a", {
      base: fake.url,
      mint: (s, credential, ttl) => signJwt(sk, customerClaims(s, credential, ttl, "test", Math.floor(clock.t / 1000))),
      maxWalks: 10,
      settleMs: 0,
      log: () => {},
      sleep: async (ms) => {
        clock.t += ms;
      },
    });
    expect([deleted, fake.streams.get("proj_a")!.size]).toEqual([2000, 0]);
    expect(fake.requests.filter((q) => q.status === 401)).toEqual([]);
    expect(clock.t - t0).toBeGreaterThan(1770 * 1000);
    // A fresh token for every request (the proof's two, each listing,
    // each delete); only a request's own 429 retries resend it.
    expect(new Set(fake.tokens.map((claims) => claims.jti)).size).toBe(fake.requests.filter((q) => q.status !== 429).length);
    const listings = fake.requests.filter((q) => q.method === "GET").map((q) => new URLSearchParams(q.path.split("?")[1]).get("limit"));
    expect([...new Set(listings)].sort()).toEqual(["1", "20"]);
    expect(fake.requests.filter((q) => q.status === 429).length).toBeGreaterThan(0);
    // Over 2,000 requests to the fake cell, each with a freshly signed and
    // verified token: about 0.5 s on an idle host, 5 to 6 s at a load
    // average of 25, past bun's 5 s default.
  }, 60_000);
});
