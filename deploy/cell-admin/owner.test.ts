// The owner's commands on a shared cell beside the interim issuer
// (public-beta issuer triage B1.6-B1.8), on both state stores. Run with
// `bun test ./deploy/cell-admin`.
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { bundleOf } from "./feeds";
import { unversionedChanges } from "./publish";
import { setCredentialField, setProjectField } from "./rules";
import { cleanup, customerCredential, initCell, readBundle, readState, run, STORES, useStore } from "./test-rig";

afterEach(cleanup);

const policyOf = (where: string, project: string) =>
  (readBundle(where).policies.projects as any[]).find((p) => p.project_id === project);

describe("the version-bump invariant", () => {
  test("two states that differ in a published field differ in that entry's version and in feed_version", async () => {
    const c = await initCell();
    await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w");
    const cred = customerCredential(c.state, "proj_a");
    const changes: [string, (s: any) => boolean][] = [
      ["status", (s) => setProjectField(s, "proj_a", { status: "suspended" })],
      ["a quota", (s) => setProjectField(s, "proj_a", { quotas: { ...s.projects.proj_a.quotas, max_streams: 7 } })],
      ["a credential's status", (s) => setCredentialField(s, cred, { status: "revoked" })],
      ["its scopes", (s) => setCredentialField(s, cred, { scopes: "streams.catalog.read" })],
      ["its prefixes", (s) => setCredentialField(s, cred, { stream_prefixes: ["a/"] })],
      ["its expiry", (s) => setCredentialField(s, cred, { expires_at: 1_900_000_000 })],
      ["a secret, rotated", (s) => setCredentialField(s, cred, { secret_hash: "h", hv: 1 }, { bump: true })],
    ];
    for (const [what, change] of changes) {
      const before = readState(c.state);
      const after = readState(c.state);
      expect([what, change(after)]).toEqual([what, true]);
      expect([what, after.feed_version]).toEqual([what, before.feed_version + 1]);
      const [b, a] = [bundleOf(before), bundleOf(after)];
      expect([what, unversionedChanges(b, a)]).toEqual([what, []]);
      expect([what, JSON.stringify(a.policies.projects) === JSON.stringify(b.policies.projects)
        && JSON.stringify(a.grants.credentials) === JSON.stringify(b.grants.credentials)]).toEqual([what, false]);
    }
    const s = readState(c.state);
    expect(setProjectField(s, "proj_a", { display_name: "Orders" })).toBe(false);
    expect(setCredentialField(s, cred, { display_name: "ci", hint: "abc123" })).toBe(false);
    expect(s.feed_version).toBe(readState(c.state).feed_version);
    // Several published changes in one write publish one new generation.
    setProjectField(s, "proj_a", { status: "suspended" });
    setCredentialField(s, cred, { status: "revoked" });
    expect(s.feed_version).toBe(readState(c.state).feed_version + 1);
  });
});

for (const kind of STORES) {
  describe(`on the ${kind} store`, () => {
    beforeEach(() => useStore(kind));

    test("suspend publishes the project suspended at a newer policy version, and resume reverses it", async () => {
      const c = await initCell();
      await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w");
      let r = await run("suspend", "--state", c.state, "--project", "proj_a");
      expect([r.code, r.err]).toEqual([0, ""]);
      expect([policyOf(c.state, "proj_a").status, policyOf(c.state, "proj_a").project_policy_version]).toEqual(["suspended", 2]);
      expect(readBundle(c.state).policies.feed_version).toBe(3);
      expect((await run("suspend", "--state", c.state, "--project", "proj_a")).err)
        .toBe("cell-admin suspend: project proj_a is already suspended");
      r = await run("resume", "--state", c.state, "--project", "proj_a");
      expect([r.code, policyOf(c.state, "proj_a").status, policyOf(c.state, "proj_a").project_policy_version])
        .toEqual([0, "active", 3]);
      expect((await run("resume", "--state", c.state, "--project", "proj_a")).err)
        .toBe("cell-admin resume: project proj_a is already active");
      expect((await run("suspend", "--state", c.state, "--project", "proj_z")).err)
        .toBe("cell-admin suspend: project proj_z is not on this cell");
      await run("offboard", "--state", c.state, "--project", "proj_a");
      expect((await run("suspend", "--state", c.state, "--project", "proj_a")).err)
        .toBe("cell-admin suspend: project proj_a is offboarding: only an active project is suspended or resumed");
    });

    test("quota sets a project's quotas at most their ceilings at a newer policy version, keeping the rest", async () => {
      const c = await initCell();
      await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w", "--quota", "max_streams=50");
      const before = policyOf(c.state, "proj_a").quotas;
      const r = await run("quota", "--state", c.state, "--project", "proj_a", "--quota", "requests_per_sec=10",
        "--quota", "append_records_per_sec=500");
      expect([r.code, r.err]).toEqual([0, ""]);
      expect(policyOf(c.state, "proj_a")).toMatchObject({
        project_policy_version: 2,
        quotas: { ...before, requests_per_sec: 10, append_records_per_sec: 500, max_streams: 50 },
      });
      const refused: [string[], string][] = [
        [["--quota", "requests_per_sec=177"],
          "--quota requests_per_sec=177 exceeds the cell's ceiling 176 (bound / k, k = 8)"],
        [["--quota", "max_stream=1"], "--quota max_stream: not a quota field"],
        [["--quota", "requests_per_sec=10"], "project proj_a already has these quotas"],
      ];
      for (const [flags, message] of refused) {
        const no = await run("quota", "--state", c.state, "--project", "proj_a", ...flags);
        expect([no.code === 0, no.err.split("\n")[0]]).toEqual([false, expect.stringContaining(message)]);
      }
      expect((await run("quota", "--state", c.state, "--project", "proj_a")).code).toBe(2);
      expect(policyOf(c.state, "proj_a").project_policy_version).toBe(2);
    });
  });
}
