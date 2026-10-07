// cell-admin init, admit, token, list and bundle (shared-cells PLAN step 9:
// the tool side of H1, H5, M3, M4 and L2). Run with
// `bun test ./deploy/cell-admin`.
import { afterEach, describe, expect, test } from "bun:test";
import { existsSync, readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { validateDocument } from "../../platform-demo/src/validate.mjs";
import { schema } from "./feeds";
import {
  cleanup,
  customerCredential,
  decodeJwt,
  initCell,
  keyFile,
  readBundle,
  readState,
  run,
  scratchDir,
  verifiedClaims,
} from "./test-rig";

afterEach(cleanup);

const CEILINGS = {
  requests_per_sec: 176,
  append_bytes_per_sec: 257500,
  append_records_per_sec: 0,
  read_bytes_per_sec: 3750000,
  max_inflight_requests: 64,
  max_live_subscriptions: 150,
  max_streams: 8192,
  queued_append_bytes: 0,
};

/// Nothing cell-admin writes may hold private key material or a token
/// other than the token file it was asked for.
function expectNoSecrets(dir: string, except: string[] = []): void {
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    if (except.includes(path) || statSync(path).isDirectory()) continue;
    const text = readFileSync(path, "utf8");
    expect(text).not.toContain("PRIVATE KEY");
    expect(text).not.toMatch(/eyJ[A-Za-z0-9_-]+\.eyJ/);
  }
}

describe("init", () => {
  test("writes a schema-valid bundle with the cell's one customer-audience key and nothing private", async () => {
    const c = await initCell();
    const b = readBundle(c.state);
    expect(b.keys.feed_version).toBe(1);
    expect(b.keys.keys).toHaveLength(1);
    expect((b.keys.keys[0] as any).aud).toBe("prisma-streams-data");
    expect((b.keys.keys[0] as any).kid).toMatch(/^streams-rs256-[0-9a-f]{16}$/);
    expect([b.policies.projects, b.grants.credentials]).toEqual([[], []]);
    expect(validateDocument(b.keys, schema("keys.schema.json"))).toEqual([]);
    expect(readState(c.state).cell.ceilings).toEqual({
      requests_per_sec: 176,
      append_bytes_per_sec: 257500,
      read_bytes_per_sec: 3750000,
      max_inflight_requests: 64,
      max_live_subscriptions: 150,
      max_streams: 8192,
    });
    expect(statSync(c.state).mode & 0o777).toBe(0o700);
    expectNoSecrets(c.state);
  });

  test("refuses a key file group or others can read, and never quotes a bad key file", async () => {
    const root = scratchDir();
    const open = keyFile(root, "rsa", 0o640, "open.pem");
    let r = await run("init", "--state", join(root, "s"), "--cell-id", "c1", "--deployment-project", "proj_d",
      "--account", "acct_s", "--key-file", open);
    expect([r.code, r.err.split("\n")[0]]).toEqual([
      1, `cell-admin init: key file ${open} has mode 0640: group or others can read the cell's signing key; chmod 600 it`,
    ]);
    const junk = join(root, "junk.pem");
    writeFileSync(junk, "not a key but a secret-looking line 7f3a", { mode: 0o600 });
    r = await run("init", "--state", join(root, "s"), "--cell-id", "c1", "--deployment-project", "proj_d",
      "--account", "acct_s", "--key-file", junk);
    expect([r.code, r.err]).toEqual([1, `cell-admin init: key file ${junk} does not hold a PEM private key`]);
    expect(existsSync(join(root, "s", "cell-state.json"))).toBe(false);
  });

  test("refuses placeholder identities and a second init", async () => {
    const c = await initCell();
    let r = await run("init", "--state", join(c.root, "x"), "--cell-id", "c1", "--deployment-project", "proj_local",
      "--account", "acct_s", "--key-file", c.key);
    expect(r.err).toBe("cell-admin init: a shared cell needs a real PROJECT_ID and ACCOUNT_ID, not a placeholder");
    r = await run("init", "--state", c.state, "--cell-id", "c1", "--deployment-project", "proj_d",
      "--account", "acct_s", "--key-file", c.key);
    expect(r.err).toBe(`cell-admin init: ${join(c.state, "cell-state.json")} exists: a cell is initialised once`);
  });
});

describe("admit", () => {
  test("publishes the project at its ceilings, explicit quotas below them, and a fresh credential", async () => {
    const c = await initCell();
    const r = await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "wksp_a",
      "--quota", "max_streams=100", "--quota", "queued_append_bytes=1048576");
    expect(r.code).toBe(0);
    const b = readBundle(c.state);
    expect(b.policies).toEqual({
      feed_version: 2,
      projects: [{
        project_id: "proj_a", workspace_id: "wksp_a", cell_id: "cell_sc1", project_policy_version: 1,
        ownership_version: 1, status: "active",
        quotas: { ...CEILINGS, max_streams: 100, queued_append_bytes: 1048576 },
      }],
    });
    const cred = customerCredential(c.state, "proj_a");
    expect(cred).toMatch(/^strcred_[0-9a-f]{32}$/);
    expect(b.grants.credentials).toEqual([{
      credential_id: cred, project_id: "proj_a", grant_version: 1, status: "active",
      scopes: expect.stringContaining("streams.records.append"), expires_at: null,
    }]);
    expect(validateDocument(b.policies, schema("project-policies.schema.json"))).toEqual([]);
    expect(validateDocument(b.grants, schema("credential-grants.schema.json"))).toEqual([]);
    expect(readFileSync(join(c.state, "denylist.txt"), "utf8")).toBe("proj_a\n");
  });

  const refusals: [string, string[], string][] = [
    ["a quota above its ceiling", ["--quota", "max_inflight_requests=65"],
      "--quota max_inflight_requests=65 exceeds the cell's ceiling 64 (bound / k, k = 8)"],
    ["a 0 quota", ["--quota", "requests_per_sec=0"],
      "--quota requests_per_sec=0: 0 means no limit; omit the field to take the ceiling"],
    ["an unknown quota", ["--quota", "max_stream=5"], "--quota max_stream: not a quota field"],
    ["the system project", ["--project", "system"], "project system is reserved on cell cell_sc1"],
    ["the deployment's PROJECT_ID", ["--project", "proj_deploy"], "project proj_deploy is reserved on cell cell_sc1"],
    ["the cell's ACCOUNT_ID as workspace", ["--workspace", "acct_sink"],
      "workspace acct_sink is the cell's ACCOUNT_ID, the unowned-event sink"],
    ["an id outside the grammar", ["--project", "proj/a"], '--project "proj/a" is not a valid id'],
  ];
  test.each(refusals)("refuses %s and publishes nothing", async (_what, flags, message) => {
    const c = await initCell();
    const args = ["--project", "proj_a", "--workspace", "wksp_a"];
    for (let i = 0; i < flags.length; i += 2) {
      const at = args.indexOf(flags[i]);
      if (at >= 0) args[at + 1] = flags[i + 1];
      else args.push(flags[i], flags[i + 1]);
    }
    const before = readFileSync(join(c.state, "feeds-bundle.json"), "utf8");
    const r = await run("admit", "--state", c.state, ...args);
    expect(r.code).not.toBe(0);
    expect(r.err.split("\n")[0]).toContain(message);
    expect(readFileSync(join(c.state, "feeds-bundle.json"), "utf8")).toBe(before);
    expect(readState(c.state).projects).toEqual({});
  });

  test("caps projects per workspace and per cell", async () => {
    const c = await initCell(["--workspace-cap", "2", "--max-projects", "3"]);
    for (const [p, w] of [["proj_a", "w1"], ["proj_b", "w1"], ["proj_c", "w2"]]) {
      expect((await run("admit", "--state", c.state, "--project", p, "--workspace", w)).code).toBe(0);
    }
    expect((await run("admit", "--state", c.state, "--project", "proj_d", "--workspace", "w2")).err)
      .toBe("cell-admin admit: cell cell_sc1 already holds its 3 projects");
    const c2 = await initCell(["--workspace-cap", "2"]);
    await run("admit", "--state", c2.state, "--project", "proj_a", "--workspace", "w1");
    await run("admit", "--state", c2.state, "--project", "proj_b", "--workspace", "w1");
    expect((await run("admit", "--state", c2.state, "--project", "proj_c", "--workspace", "w1")).err)
      .toBe("cell-admin admit: workspace w1 already has 2 project(s) on cell cell_sc1 (cap 2)");
  });

  test("refuses an id on a shared denylist and an id this cell placed before", async () => {
    const root = scratchDir();
    const deny = join(root, "fleet-denylist.txt");
    writeFileSync(deny, "# ids used anywhere\nproj_old\n");
    const c = await initCell(["--denylist", deny]);
    expect((await run("admit", "--state", c.state, "--project", "proj_old", "--workspace", "w")).err)
      .toBe("cell-admin admit: project id proj_old was used before: an id is never placed twice");
    await run("admit", "--state", c.state, "--project", "proj_new", "--workspace", "w");
    expect(readFileSync(deny, "utf8")).toBe("# ids used anywhere\nproj_old\nproj_new\n");
    expect((await run("admit", "--state", c.state, "--project", "proj_new", "--workspace", "w2")).err)
      .toBe("cell-admin admit: project id proj_new was used before: an id is never placed twice");
  });

  test("refuses to admit once the profiles' ceilings moved", async () => {
    const root = scratchDir();
    const prof = join(root, "shared.env");
    writeFileSync(prof, readFileSync(join(import.meta.dir, "..", "profiles", "shared-cell.env")));
    const c = await initCell(["--profile", join(import.meta.dir, "..", "profiles", "compute-1g.env"), "--profile", prof]);
    writeFileSync(prof, `${readFileSync(prof, "utf8")}SSE_MAX_CONNECTIONS=800\n`);
    expect((await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w")).err).toBe(
      "cell-admin admit: the profiles changed since init (max_live_subscriptions 150 -> 100): " +
        "a cell's ceilings are fixed for its life",
    );
  });
});

describe("token", () => {
  test("mints a 24 h token the cell's keys verify, at the credential's versions, into a 0600 file only", async () => {
    const c = await initCell();
    await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "wksp_a", "--prefix", "orders/");
    const cred = customerCredential(c.state, "proj_a");
    const out = join(c.root, "token.jwt");
    const r = await run("token", "--state", c.state, "--credential", cred, "--key-file", c.key, "--out", out,
      "--ttl", "86400");
    expect(r.code).toBe(0);
    const token = readFileSync(out, "utf8");
    expect(statSync(out).mode & 0o777).toBe(0o600);
    expect(r.out).not.toContain(token.split(".")[2]);
    const claims = verifiedClaims(readBundle(c.state), token);
    expect(claims).toMatchObject({
      aud: "prisma-streams-data", iss: "https://auth.prisma.io", credential_id: cred, project_id: "proj_a",
      workspace_id: "wksp_a", cell_id: "cell_sc1", ownership_version: 1, grant_version: 1,
      stream_prefixes: ["orders/"],
    });
    expect(claims.exp - claims.iat).toBe(86400);
    expect(decodeJwt(token).header.kid).toBe((readBundle(c.state).keys.keys[0] as any).kid);
    expect(validateDocument(claims, schema("customer-token-claims.schema.json"))).toEqual([]);
    expectNoSecrets(c.state);
  });

  test("refuses a lifetime over 24 h, a key the cell does not carry, and an unknown credential", async () => {
    const c = await initCell();
    await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "wksp_a");
    const cred = customerCredential(c.state, "proj_a");
    const out = join(c.root, "t.jwt");
    const base = ["token", "--state", c.state, "--credential", cred, "--out", out];
    expect((await run(...base, "--key-file", c.key, "--ttl", "86401")).err)
      .toBe("cell-admin token: --ttl must be a whole number of seconds in 120..86400 (24 h)");
    const other = keyFile(c.root, "ed25519", 0o600, "other.pem");
    expect((await run(...base, "--key-file", other)).err)
      .toBe(`cell-admin token: key file ${other} is not a key of cell cell_sc1`);
    expect((await run("token", "--state", c.state, "--credential", "strcred_nope", "--out", out, "--key-file", c.key)).err)
      .toBe("cell-admin token: credential strcred_nope is not a customer credential of this cell");
    expect(existsSync(out)).toBe(false);
  });

  test("signs with an Ed25519 cell key as EdDSA", async () => {
    const root = scratchDir();
    const key = keyFile(root, "ed25519");
    const state = join(root, "state");
    expect((await run("init", "--state", state, "--cell-id", "c2", "--deployment-project", "proj_d",
      "--account", "acct_s", "--key-file", key)).code).toBe(0);
    await run("admit", "--state", state, "--project", "proj_a", "--workspace", "w");
    const out = join(root, "t.jwt");
    await run("token", "--state", state, "--credential", customerCredential(state, "proj_a"), "--key-file", key,
      "--out", out);
    const token = readFileSync(out, "utf8");
    expect(decodeJwt(token).header.alg).toBe("EdDSA");
    expect(verifiedClaims(readBundle(state), token)?.project_id).toBe("proj_a");
  });
});

describe("durable, monotonic state", () => {
  test("refuses to write from a state older than its last bundle (a stale restore)", async () => {
    const c = await initCell();
    const stale = readFileSync(join(c.state, "cell-state.json"), "utf8");
    await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w");
    writeFileSync(join(c.state, "cell-state.json"), stale);
    const r = await run("admit", "--state", c.state, "--project", "proj_b", "--workspace", "w2");
    expect(r.err).toContain("is at feed_version 1 but its bundle is at 2: the state was restored from a stale copy");
  });

  test("refuses while another run holds the cell", async () => {
    const c = await initCell();
    writeFileSync(join(c.state, ".lock"), "4242\n");
    const r = await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w");
    expect(r.err).toContain(".lock exists: another cell-admin run holds this cell");
  });

  test("bundle republishes the same content at a newer feed_version", async () => {
    const c = await initCell();
    await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w");
    const before = readBundle(c.state);
    expect((await run("bundle", "--state", c.state)).code).toBe(0);
    const after = readBundle(c.state);
    expect(after.policies.feed_version).toBe(before.policies.feed_version + 1);
    expect(after.policies.projects).toEqual(before.policies.projects);
    expect(after.grants.credentials).toEqual(before.grants.credentials);
  });

  test("list shows the cell without key material; --json carries kids only", async () => {
    const c = await initCell();
    await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w");
    const r = await run("list", "--state", c.state);
    expect(r.out).toContain("projects: 1 placed of 1000, 0 offboarded; workspace cap 1");
    expect(r.out).toContain("proj_a workspace=w active policy=v1");
    const j = JSON.parse((await run("list", "--state", c.state, "--json")).out);
    expect(Object.keys(j.keys[0]).sort()).toEqual(["alg", "aud", "kid"]);
    expect(JSON.stringify(j)).not.toContain("BEGIN");
  });

  test("usage errors exit 2", async () => {
    const c = await initCell();
    expect((await run("admit", "--state", c.state, "--project", "p")).code).toBe(2);
    expect((await run("admit", "--state", c.state, "--project", "p", "--workspace", "w", "--bogus", "1")).code).toBe(2);
    expect((await run("nonsense")).code).toBe(2);
  });
});
