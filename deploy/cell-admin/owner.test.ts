// The owner's commands on a shared cell beside the interim issuer
// (public-beta issuer triage B1.6-B1.8), on both state stores. Run with
// `bun test ./deploy/cell-admin`.
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { bundleOf } from "./feeds";
import { unversionedChanges } from "./publish";
import { admitProject, admittedQuotas, newCredentialId, setCredentialField, setProjectField } from "./rules";
import { generateKeyPairSync } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  cleanup,
  customerCredential,
  initCell,
  keyFile,
  publicKeyFile,
  readBundle,
  readState,
  run,
  STORES,
  useStore,
  verifiedClaims,
} from "./test-rig";
import { loadSigningKeyFromPem, publicKeyRecord } from "./tokens";

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

describe("keys from PEM text", () => {
  test("the signing key and its public half load from text with the file's checks, never quoting the key", () => {
    const { privateKey, publicKey } = generateKeyPairSync("ed25519");
    const pem = privateKey.export({ type: "pkcs8", format: "pem" }).toString();
    const sk = loadSigningKeyFromPem(pem, "STREAMS_ISSUER_SIGNING_KEY");
    expect(publicKeyRecord(publicKey.export({ type: "spki", format: "pem" }).toString())).toEqual(sk.record);
    const weak = generateKeyPairSync("rsa", { modulusLength: 1024 }).privateKey.export({ type: "pkcs8", format: "pem" });
    expect(() => loadSigningKeyFromPem(weak.toString(), "STREAMS_ISSUER_SIGNING_KEY"))
      .toThrow("STREAMS_ISSUER_SIGNING_KEY holds an RSA key shorter than 2048 bits");
    const ec = generateKeyPairSync("ec", { namedCurve: "P-256" }).privateKey.export({ type: "pkcs8", format: "pem" });
    expect(() => loadSigningKeyFromPem(ec.toString(), "STREAMS_ISSUER_SIGNING_KEY"))
      .toThrow("STREAMS_ISSUER_SIGNING_KEY holds a ec key; use RSA or Ed25519");
    let message = "";
    try {
      loadSigningKeyFromPem(`${pem.slice(0, 60)}garbage`, "STREAMS_ISSUER_SIGNING_KEY");
    } catch (e) {
      message = (e as Error).message;
    }
    expect(message).toBe("STREAMS_ISSUER_SIGNING_KEY does not hold a PEM private key");
  });
});

describe("admitProject", () => {
  test("places a project with a first credential whose secret's hash stays out of the bundle", async () => {
    const c = await initCell();
    const s = readState(c.state);
    const id = newCredentialId(s);
    admitProject(s, {
      project: "sp_0123", workspace: "wksp_9", quotas: admittedQuotas(s, ["requests_per_sec=10"]),
      scopes: "streams.catalog.read streams.records.read", display_name: "Orders",
      created_by: { user_id: "u1", email: "u1@example.com" },
      credential: { id, secret_hash: "aGFzaA", hv: 1, hint: "x1y2z3", display_name: "ci" },
    });
    expect(s.feed_version).toBe(2);
    expect(s.projects.sp_0123).toMatchObject({ phase: "active", status: "active", project_policy_version: 1,
      display_name: "Orders", created_by: { user_id: "u1", email: "u1@example.com" } });
    expect(s.credentials[id]).toMatchObject({ project_id: "sp_0123", role: "customer", grant_version: 1,
      status: "active", secret_hash: "aGFzaA", hv: 1, hint: "x1y2z3", expires_at: null, omitted: false });
    const b = bundleOf(s);
    expect((b.policies.projects as any[]).map((p) => [p.project_id, p.quotas.requests_per_sec])).toEqual([["sp_0123", 10]]);
    expect(b.grants.credentials).toEqual([{ credential_id: id, project_id: "sp_0123", grant_version: 1,
      status: "active", scopes: "streams.catalog.read streams.records.read", expires_at: null }]);
    expect(() => admitProject(s, { project: "sp_0123", workspace: "w", quotas: s.projects.sp_0123.quotas,
      scopes: "streams.catalog.read", credential: { id: newCredentialId(s) } })).toThrow("project sp_0123 is already on this cell");
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

    test("keys add publishes an overlap generation, and keys retire omits a kid for good", async () => {
      const c = await initCell();
      await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w");
      const kids = () => (readBundle(c.state).keys.keys as any[]).map((k) => k.kid);
      const [old] = kids();
      const next = keyFile(c.root, "ed25519", 0o600, "next.pem");
      let r = await run("keys", "add", "--state", c.state, "--public-key", publicKeyFile(next, "next.pub"));
      expect([r.code, r.err]).toEqual([0, ""]);
      const [, added] = kids();
      expect([kids().length, readBundle(c.state).keys.feed_version]).toEqual([2, 3]);
      // The drill's check: a token under the new kid verifies against the published keys.
      const out = join(c.root, "t.jwt");
      expect((await run("token", "--state", c.state, "--credential", customerCredential(c.state, "proj_a"),
        "--key-file", next, "--out", out)).code).toBe(0);
      expect(verifiedClaims(readBundle(c.state), readFileSync(out, "utf8"))?.project_id).toBe("proj_a");
      expect((await run("keys", "add", "--state", c.state, "--public-key", publicKeyFile(next, "again.pub"))).err)
        .toBe(`cell-admin keys add: key ${added} is already a key of cell cell_sc1`);
      r = await run("keys", "retire", "--state", c.state, "--kid", old);
      expect([r.code, kids()]).toEqual([0, [added]]);
      expect((await run("keys", "retire", "--state", c.state, "--kid", added)).err)
        .toBe(`cell-admin keys retire: key ${added} is the cell's last key: add its successor first (keys add)`);
      expect((await run("keys", "retire", "--state", c.state, "--kid", "streams-eddsa-0000")).err)
        .toBe("cell-admin keys retire: key streams-eddsa-0000 is not a key of cell cell_sc1");
      expect((await run("keys", "add", "--state", c.state, "--public-key", publicKeyFile(c.key, "old.pub"))).err)
        .toBe(`cell-admin keys add: key ${old} was retired: a retired kid never returns to a cell; make a new key`);
      expect((await run("token", "--state", c.state, "--credential", customerCredential(c.state, "proj_a"),
        "--key-file", c.key, "--out", out)).err).toBe(`cell-admin token: key file ${c.key} is not a key of cell cell_sc1`);
      expect((await run("keys", "rotate", "--state", c.state)).code).toBe(2);
    });

    test("keys add --retire-others publishes the new key alone (the compromise drill)", async () => {
      const c = await initCell();
      const next = keyFile(c.root, "ed25519", 0o600, "next.pem");
      const r = await run("keys", "add", "--state", c.state, "--public-key", publicKeyFile(next, "next.pub"),
        "--retire-others");
      expect([r.code, r.err]).toEqual([0, ""]);
      const keys = readBundle(c.state).keys.keys as any[];
      expect([keys.length, keys[0].alg, readState(c.state).retired_kids.length]).toEqual([1, "EdDSA", 1]);
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
