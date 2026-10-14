// The owner's commands on a shared cell beside the interim issuer
// (public-beta issuer triage B1.6-B1.8), on both state stores. Run with
// `bun test ./deploy/cell-admin`.
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { FakeCell } from "./fake-cell";
import { bundleOf } from "./feeds";
import { unversionedChanges } from "./publish";
import { admitProject, admittedQuotas, newCredentialId, setCredentialField, setProjectField } from "./rules";
import { createHmac, generateKeyPairSync, randomBytes } from "node:crypto";
import { existsSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  allowlistOf,
  cleanup,
  customerCredential,
  customerOnly,
  fakeS3,
  initCell,
  keyFile,
  pepperFile,
  publicKeyFile,
  readBundle,
  readState,
  run,
  STORES,
  storeTexts,
  useStore,
  verifiedClaims,
} from "./test-rig";
import { parsePeppers, secretFields } from "./secrets";
import { loadSigningKeyFromPem, publicKeyRecord } from "./tokens";

let fake: FakeCell | undefined;
afterEach(() => {
  fake?.stop();
  fake = undefined;
  cleanup();
});

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
      ["its prefixes", (s) => setCredentialField(s, cred, { stream_prefixes: ["a"] })],
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

describe("secrets", () => {
  test("peppers are versioned newest first, and a secret is hashed under the newest", () => {
    const [k2, k1] = [randomBytes(32), randomBytes(48)];
    const peppers = parsePeppers(`v2:${k2.toString("base64")},v1:${k1.toString("base64url")}`, "STREAMS_ISSUER_PEPPER");
    expect(peppers.map((p) => [p.version, p.key.length])).toEqual([[2, 32], [1, 48]]);
    expect(secretFields(peppers, "s3cret-abcdef")).toEqual({
      secret_hash: createHmac("sha256", k2).update("s3cret-abcdef").digest("base64url"), hv: 2, hint: "abcdef",
    });
    expect(() => parsePeppers(`v1:${k1.toString("base64")},v2:${k2.toString("base64")}`, "P"))
      .toThrow("P: versions must fall from the newest, first, to the oldest");
    expect(() => parsePeppers(`v1:${k1.toString("base64")},2:${k2.toString("base64")}`, "P"))
      .toThrow("P: entry 2 is not v<N>:<base64>");
    expect(() => parsePeppers("", "P")).toThrow("P: entry 1 is not v<N>:<base64>");
  });
});

describe("admitProject", () => {
  test("places a project with a first credential whose secret's hash stays out of the bundle", async () => {
    const c = await initCell();
    const s = readState(c.state);
    const id = newCredentialId(s);
    admitProject(s, {
      project: "sp_0123", workspace: "wksp_9", quotas: admittedQuotas(s, ["requests_per_sec=10"], 10737418240),
      scopes: "streams.catalog.read streams.records.read", display_name: "Orders",
      created_by: { user_id: "u1", email: "u1@example.com" },
      credential: { id, secret_hash: "aGFzaA", hv: 1, hint: "x1y2z3", display_name: "ci" },
    });
    expect(s.feed_version).toBe(2);
    expect(s.projects.sp_0123).toMatchObject({ phase: "active", status: "active", project_policy_version: 1,
      display_name: "Orders", created_by: { user_id: "u1", email: "u1@example.com" } });
    expect(s.credentials[id]).toMatchObject({ project_id: "sp_0123", role: "customer", grant_version: 1,
      status: "active", secret_hash: "aGFzaA", hv: 1, hint: "x1y2z3", expires_at: null, omitted: false });
    const b = customerOnly(bundleOf(s));
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

    test("refuses to offboard, suspend, resume or set the quotas of the issuer's canary", async () => {
      const c = await initCell();
      const canary = "sp_canary_cell_sc1";
      const [state, bundle] = [readState(c.state), readBundle(c.state)];
      for (const [command, ...flags] of [["offboard"], ["suspend"], ["resume"], ["quota", "--quota", "max_streams=1"]]) {
        const r = await run(command, "--state", c.state, "--project", canary, ...flags);
        expect([r.code, r.err]).toEqual([1, `cell-admin ${command}: project ${canary} is the issuer's canary: no owner ` +
          "command changes it (its catalog-read credential proves the cell serves a key for the cell's life)"]);
      }
      expect([readState(c.state), readBundle(c.state)]).toEqual([state, bundle]);
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

    test("credential writes a new credential's secret once to a 0600 file, and the state only its peppered hash", async () => {
      const c = await initCell();
      await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w");
      const pepper = pepperFile(c.root);
      const out = join(c.root, "ci.secret");
      const r = await run("credential", "--state", c.state, "--project", "proj_a", "--scopes",
        "streams.records.read streams.records.append", "--prefix", "orders", "--name", "ci",
        "--pepper-file", pepper.path, "--out", out);
      expect([r.code, r.err]).toEqual([0, ""]);
      const presented = readFileSync(out, "utf8").trim();
      expect(statSync(out).mode & 0o777).toBe(0o600);
      const m = /^prisma_streams_(strcred_[0-9a-f]{32})\.([A-Za-z0-9_-]{43})$/.exec(presented);
      expect(m).not.toBeNull();
      const [, id, secret] = m!;
      expect(readState(c.state).credentials[id]).toMatchObject({
        project_id: "proj_a", role: "customer", grant_version: 1, status: "active", display_name: "ci",
        secret_hash: createHmac("sha256", pepper.key).update(secret).digest("base64url"), hv: 1, hint: secret.slice(-6),
      });
      expect((readBundle(c.state).grants.credentials as any[]).find((g) => g.credential_id === id)).toEqual({
        credential_id: id, project_id: "proj_a", grant_version: 1, status: "active",
        scopes: "streams.records.read streams.records.append", stream_prefixes: ["orders"], expires_at: null,
      });
      expect(r.out).not.toContain(secret);
      for (const text of storeTexts(c.state)) expect(text).not.toContain(secret);
      expect((await run("credential", "--state", c.state, "--project", "proj_a", "--pepper-file", pepper.path,
        "--out", out)).err).toBe(`cell-admin credential: --out ${out} exists: a secret is written once, to a new file`);
    });

    test("credential keeps a committed credential's secret when its bundle fails to publish, and refuses a missing --out directory first", async () => {
      const c = await initCell();
      await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w");
      const pepper = pepperFile(c.root);
      const before = readState(c.state);
      const nowhere = join(c.root, "no-such-dir", "ci.secret");
      let r = await run("credential", "--state", c.state, "--project", "proj_a", "--pepper-file", pepper.path,
        "--out", nowhere);
      expect([r.code, r.err.startsWith(`cell-admin credential: cannot write the secret beside ${nowhere}:`)]).toEqual([1, true]);
      expect(readState(c.state)).toEqual(before);
      if (kind !== "s3") return;
      // The feeds bucket refuses the bundle: the state's commit has landed.
      fakeS3().failPut = (key) => key.endsWith("/feeds-bundle.json");
      const out = join(c.root, "ci.secret");
      r = await run("credential", "--state", c.state, "--project", "proj_a", "--pepper-file", pepper.path, "--out", out);
      const state = readState(c.state);
      const [id] = Object.keys(state.credentials).filter((k) => !before.credentials[k]);
      expect(state.credentials[id]).toMatchObject({ project_id: "proj_a", status: "active", grant_version: 1 });
      expect(readFileSync(out, "utf8")).toStartWith(`prisma_streams_${id}.`);
      expect(statSync(out).mode & 0o777).toBe(0o600);
      expect([r.code, r.err]).toEqual([1, `cell-admin credential: the change is committed at feed_version ` +
        `${state.feed_version}, but its bundle is not published (PUT s3://feeds-1/cells/cell_sc1/feeds-bundle.json ` +
        "answered 500 InternalError): run cell-admin bundle, not this command again"]);
      expect(r.out).toContain(`its secret is in ${out} (mode 0600) and nowhere else`);
      // Any other change says the same, and `bundle` publishes it once the bucket takes writes.
      r = await run("suspend", "--state", c.state, "--project", "proj_a");
      expect(r.err).toBe(`cell-admin suspend: the change is committed at feed_version ${state.feed_version + 1}, but ` +
        "its bundle is not published (PUT s3://feeds-1/cells/cell_sc1/feeds-bundle.json answered 500 InternalError): " +
        "run cell-admin bundle, not this command again");
      fakeS3().failPut = undefined;
      expect((await run("bundle", "--state", c.state)).code).toBe(0);
      expect(policyOf(c.state, "proj_a").status).toBe("suspended");
      expect((readBundle(c.state).grants.credentials as any[]).map((g) => g.credential_id)).toContain(id);
    });

    test("credential refuses an open pepper file, an 11th live credential, and hidden ones outside the canary", async () => {
      const c = await initCell();
      await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w");
      const make = (out: string, ...flags: string[]) =>
        run("credential", "--state", c.state, "--pepper-file", pepper.path, "--out", join(c.root, out), ...flags);
      const open = pepperFile(c.root, 0o644, 2);
      expect((await run("credential", "--state", c.state, "--project", "proj_a", "--pepper-file", open.path,
        "--out", join(c.root, "x"))).err).toBe(`cell-admin credential: pepper file ${open.path} has mode 0644: ` +
          "group or others can read the pepper; chmod 600 it");
      const short = join(c.root, "short");
      writeFileSync(short, "v3:c2hvcnQ=\n", { mode: 0o600 });
      expect((await run("credential", "--state", c.state, "--project", "proj_a", "--pepper-file", short,
        "--out", join(c.root, "x"))).err).toBe(`cell-admin credential: pepper file ${short}: pepper v3 is shorter than 32 bytes`);
      const pepper = pepperFile(c.root);
      for (let i = 2; i <= 10; i++) expect((await make(`s${i}`, "--project", "proj_a")).code).toBe(0);
      expect((await make("s11", "--project", "proj_a")).err)
        .toBe("cell-admin credential: project proj_a has its 10 live credentials: revoke one first");
      expect(existsSync(join(c.root, "s11"))).toBe(false);
      expect((await make("h", "--project", "proj_a", "--hidden")).err)
        .toBe("cell-admin credential: --hidden is for the canary's credentials only");
      expect((await make("w", "--project", "sp_canary_cell_sc1")).err)
        .toBe("cell-admin credential: the canary's credentials are hidden: pass --hidden");
      const watch = await make("watch", "--project", "sp_canary_cell_sc1", "--hidden", "--name", "watch",
        "--scopes", "streams.create streams.records.append streams.records.read");
      expect([watch.code, watch.err]).toEqual([0, ""]);
      const hidden = Object.values(readState(c.state).credentials).filter((cr) => cr.hidden);
      expect(hidden.map((cr) => [cr.display_name, cr.secret_hash === undefined])).toEqual([["catalog-read", true], ["watch", false]]);
    });

    test("allow and disallow keep the beta allow-list, a disallowed workspace's projects keep running (I8 (a)), and disallow names the reduced mode and its levers (call 16)", async () => {
      const c = await initCell();
      let r = await run("allow", "--state", c.state, "--workspace", "wksp_b", "--note", "design partner");
      expect([r.code, r.err]).toEqual([0, ""]);
      r = await run("allow", "--state", c.state, "--workspace", "wksp_a", "--max-projects", "3",
        "--requests-per-sec", "50", "--note", "pilot");
      expect([r.code, r.err]).toEqual([0, ""]);
      const first = allowlistOf(c.state);
      expect(first.version).toBe(2);
      expect(first.entries.map((e: any) => [e.workspace_id, e.max_projects, e.requests_per_sec, e.note])).toEqual([
        ["wksp_a", 3, 50, "pilot"], ["wksp_b", undefined, undefined, "design partner"]]);
      // Allowing again replaces the entry's limits and keeps when it was added.
      r = await run("allow", "--state", c.state, "--workspace", "wksp_a", "--note", "pilot, default limits");
      const again = allowlistOf(c.state).entries[0];
      expect([r.code, again.max_projects, again.note, again.added_at]).toEqual(
        [0, undefined, "pilot, default limits", first.entries[0].added_at]);
      const refused: [string[], string][] = [
        [["--workspace", "wksp_c", "--requests-per-sec", "177"],
          "--requests-per-sec 177 exceeds the cell's ceiling 176"],
        [["--workspace", "acct_sink"], "workspace acct_sink is the cell's ACCOUNT_ID, the unowned-event sink"],
        [["--workspace", "streams-issuer-canary"], "workspace streams-issuer-canary is the issuer's canary workspace"],
        [["--workspace", "w/x"], '--workspace "w/x" is not a valid id'],
      ];
      for (const [flags, message] of refused) {
        expect((await run("allow", "--state", c.state, ...flags)).err.split("\n")[0]).toContain(message);
      }
      await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "wksp_a");
      r = await run("disallow", "--state", c.state, "--workspace", "wksp_a");
      expect([r.code, allowlistOf(c.state).entries.map((e: any) => e.workspace_id), allowlistOf(c.state).version])
        .toEqual([0, ["wksp_b"], 4]);
      expect(r.out).toBe([
        "allow-list v4: wksp_a removed",
        "wksp_a gets no new project, credential or secret within 5 s; its 1 project(s) keep running: proj_a",
        "while it holds a project its users keep sign-in in the issuer's reduced mode: they see its projects and " +
          "credentials, revoke a credential and delete a project",
        "levers against a hostile session: cell-admin suspend --project P stops a delete, not a revoke; rotating " +
          "STREAMS_ISSUER_SESSION_KEY ends every session; cell-admin credential --project P (or allow) replaces a " +
          "credential it revoked",
      ].join("\n"));
      expect(policyOf(c.state, "proj_a").status).toBe("active");
      // A workspace that holds no project signs in no more.
      r = await run("disallow", "--state", c.state, "--workspace", "wksp_b");
      expect([r.code, r.out]).toEqual([0, [
        "allow-list v5: wksp_b removed",
        "wksp_b gets no new project, credential or secret within 5 s; it holds no project, so its users sign in no more",
      ].join("\n")]);
      expect((await run("disallow", "--state", c.state, "--workspace", "wksp_a")).err)
        .toBe("cell-admin disallow: workspace wksp_a is not on the allow-list");
    });

    test("gate opens and closes self-serve admission and sets its project count, publishing nothing", async () => {
      const c = await initCell();
      expect(readState(c.state).cell.gate).toEqual({ open: true, n_max: 50 });
      const v = readBundle(c.state).keys.feed_version;
      let r = await run("gate", "--state", c.state, "--close");
      expect([r.code, r.out, readState(c.state).cell.gate]).toEqual(
        [0, "gate: closed, n_max 50 (0 project(s) placed)", { open: false, n_max: 50 }]);
      r = await run("gate", "--state", c.state, "--open", "--n-max", "10");
      expect([r.code, readState(c.state).cell.gate]).toEqual([0, { open: true, n_max: 10 }]);
      expect(readBundle(c.state).keys.feed_version).toBe(v);
      for (const flags of [[], ["--open", "--close"], ["--n-max", "0"]]) {
        expect((await run("gate", "--state", c.state, ...flags)).code).toBe(2);
      }
      expect(readState(c.state).journal.map((j) => j.command).slice(-2)).toEqual(["gate", "gate"]);
    });

    test("canary proves the cell serves a key: 200 under a published kid, exit 3 until the bundle lands", async () => {
      const c = await initCell();
      fake = new FakeCell();
      fake.publish(readBundle(c.state));
      const canary = (key: string, ...extra: string[]) =>
        run("canary", "--state", c.state, "--key-file", key, "--cell-url", fake!.url, ...extra);
      let r = await canary(c.key);
      expect([r.code, r.err, r.out.split("\n")[0]]).toEqual([0, "",
        `canary: the cell serves key ${readState(c.state).keys[0].kid}: GET /v1/streams?limit=1 answered 200`]);
      // The rotation drill: the new key is in the state, not yet at the cell.
      const next = keyFile(c.root, "ed25519", 0o600, "next.pem");
      await run("keys", "add", "--state", c.state, "--public-key", publicKeyFile(next, "next.pub"));
      r = await canary(next, "--wait-secs", "1");
      expect([r.code, r.err.replace(/kid \S+/, "kid K")]).toEqual([3, "cell-admin canary: the canary answered 401 " +
        "bad_signature for 1 s, not 200: the cell does not serve kid K yet; publish the bundle, wait, run again"]);
      fake.publish(readBundle(c.state));
      expect((await canary(next)).code).toBe(0);
      const probes = fake.requests.filter((q) => q.path === "/v1/streams?limit=1");
      expect(probes.map((q) => q.status).slice(-1)).toEqual([200]);
      const stranger = keyFile(c.root, "ed25519", 0o600, "stranger.pem");
      expect((await canary(stranger)).err).toBe(`cell-admin canary: key file ${stranger} is not a key of cell cell_sc1`);
    });

    test("canary mints each probe at the catalog-read credential's version as the state holds it then: an offboarding that moves it during a drill does not time the drill out", async () => {
      const c = await initCell();
      await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "wksp_a");
      await run("suspend", "--state", c.state, "--project", "proj_a");
      // A cell that serves no bundle yet: the first probe is refused.
      fake = new FakeCell();
      const drill = run("canary", "--state", c.state, "--key-file", c.key, "--cell-url", fake.url, "--wait-secs", "3");
      while (fake.requests.length === 0) await Bun.sleep(10);
      // Before the second probe, 2 s later, the owner offboards the
      // suspended project (the canary's catalog-read credential moves to
      // grant_version 2) and the cell takes that bundle.
      expect((await run("offboard", "--state", c.state, "--project", "proj_a")).code).toBe(0);
      fake.publish(readBundle(c.state));
      const r = await drill;
      expect([r.code, r.err]).toEqual([0, ""]);
      expect(fake.requests.map((q) => q.status)).toEqual([401, 200]);
      expect(fake.tokens.map((t) => t.grant_version)).toEqual([2]);
    }, 15_000);

    test("export writes the Stage 1 import set of the cell's active customer projects to a 0600 file", async () => {
      const c = await initCell();
      await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "wksp_a", "--quota", "max_streams=10");
      await run("admit", "--state", c.state, "--project", "proj_b", "--workspace", "wksp_b");
      await run("suspend", "--state", c.state, "--project", "proj_a");
      const pepper = pepperFile(c.root, 0o600, 4);
      await run("credential", "--state", c.state, "--project", "proj_a", "--pepper-file", pepper.path,
        "--out", join(c.root, "a.secret"), "--prefix", "x");
      await run("offboard", "--state", c.state, "--project", "proj_b");
      const out = join(c.root, "export.json");
      const r = await run("export", "--state", c.state, "--out", out);
      expect([r.code, r.err]).toEqual([0, ""]);
      expect(statSync(out).mode & 0o777).toBe(0o600);
      const s = readState(c.state);
      const [first, second] = Object.entries(s.credentials).filter(([, cr]) => cr.project_id === "proj_a");
      const exported = JSON.parse(readFileSync(out, "utf8"));
      expect(exported).toEqual({
        format: "streams-cell-admin-export/v1", cell_id: "cell_sc1", feed_version: s.feed_version,
        projects: [{
          project_id: "proj_a", workspace_id: "wksp_a", status: "suspended", project_policy_version: 2,
          ownership_version: 1, quotas: s.projects.proj_a.quotas,
          credentials: [[first[0], first[1]], [second[0], second[1]]]
            .sort(([a], [b]) => ((a as string) < (b as string) ? -1 : 1))
            .map(([id, cr]: any) => ({
              credential_id: id, grant_version: 1, status: "active", scopes: cr.scopes,
              ...(cr.stream_prefixes ? { stream_prefixes: cr.stream_prefixes } : {}), expires_at: null,
              ...(cr.secret_hash ? { secret_hash: cr.secret_hash, hv: 4 } : {}),
            })),
        }],
      });
      expect(exported.projects[0].quotas.max_streams).toBe(10);
    });

    test("quota sets a project's quotas at most their ceilings at a newer policy version, keeping the rest", async () => {
      const c = await initCell();
      await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w", "--quota", "max_streams=50");
      const before = policyOf(c.state, "proj_a").quotas;
      const r = await run("quota", "--state", c.state, "--project", "proj_a", "--quota", "requests_per_sec=10",
        "--quota", "append_records_per_sec=500", "--quota", "max_stored_bytes=21474836480");
      expect([r.code, r.err]).toEqual([0, ""]);
      expect(before.max_stored_bytes).toBe(10737418240);
      expect(policyOf(c.state, "proj_a")).toMatchObject({
        project_policy_version: 2,
        quotas: {
          ...before, requests_per_sec: 10, append_records_per_sec: 500, max_streams: 50, max_stored_bytes: 21474836480,
        },
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
