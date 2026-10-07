// cell-admin offboard against a fake cell (review findings M4 and R6, the
// F-reuse leg of PLAN step 9). Run with `bun test ./deploy/cell-admin`.
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { validateDocument } from "../../platform-demo/src/validate.mjs";
import { schema } from "./feeds";
import { OPERATOR_SCOPES } from "./offboard";
import { FakeCell } from "./fake-cell";
import { type Cell, cleanup, customerCredential, initCell, readBundle, readState, run } from "./test-rig";

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

const second = (c: Cell, extra: string[] = []) =>
  run("offboard", "--state", c.state, "--project", "proj_a", "--cell-url", fake!.url, "--key-file", c.key,
    "--settle-secs", "0", ...extra);

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

  test("the second run waits (exit 3) until the cell serves the revocation, and changes nothing", async () => {
    const c = await twoProjects();
    fake = new FakeCell();
    fake.publish(readBundle(c.state));
    await run("offboard", "--state", c.state, "--project", "proj_a");
    const before = readFileSync(join(c.state, "feeds-bundle.json"), "utf8");
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
    expect(readFileSync(join(c.state, "feeds-bundle.json"), "utf8")).toBe(before);
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
    for (const name of readdirSync(c.state)) {
      expect(readFileSync(join(c.state, name), "utf8")).not.toMatch(/eyJ[A-Za-z0-9_-]+\.eyJ/);
    }
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
