// cell-admin init, admit, token, list and bundle (shared-cells PLAN step 9:
// the tool side of H1, H5, M3, M4 and L2). Run with
// `bun test ./deploy/cell-admin`.
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { validateDocument } from "../../platform-demo/src/validate.mjs";
import { CHILD_NAMES } from "../composer/streams/env";
import { schema } from "./feeds";
import {
  auditTexts,
  cleanup,
  customerCredential,
  customerOnly,
  decodeJwt,
  deleteState,
  denylistText,
  fakeS3,
  freshState,
  initCell,
  keyFile,
  publicKeyFile,
  readBundle,
  readBundleText,
  readState,
  readStateText,
  run,
  STORES,
  scratchDir,
  stateExists,
  statePathOf,
  storeTexts,
  useStore,
  verifiedClaims,
  writeStateText,
} from "./test-rig";

afterEach(cleanup);

/// The quotas a project is admitted with when none is given: each bounded
/// axis at its ceiling, append records/s 0 (no project limit, as the
/// server reads it), and the stored bytes at the profile's
/// PROJECT_STORED_BYTES_DEFAULT (10 GiB), which has no ceiling, and weight 1
/// on both weights.
const CEILINGS = {
  requests_per_sec: 176,
  append_bytes_per_sec: 257500,
  append_records_per_sec: 0,
  read_bytes_per_sec: 3750000,
  max_inflight_requests: 64,
  max_live_subscriptions: 150,
  max_streams: 8192,
  queued_append_bytes: 67108864,
  max_stored_bytes: 10737418240,
  request_weight: 1,
  byte_weight: 1,
};

/// Nothing cell-admin writes to its store may hold private key material
/// or a token.
function expectNoSecrets(where: string): void {
  for (const text of storeTexts(where)) {
    expect(text).not.toContain("PRIVATE KEY");
    expect(text).not.toMatch(/eyJ[A-Za-z0-9_-]+\.eyJ/);
  }
}

for (const kind of STORES) {
  describe(`on the ${kind} store`, () => {
    beforeEach(() => useStore(kind));

    describe("init", () => {
      test("writes a schema-valid bundle with the cell's one customer-audience key and nothing private", async () => {
        const c = await initCell();
        const b = readBundle(c.state);
        expect(b.keys.feed_version).toBe(1);
        expect(b.keys.keys).toHaveLength(1);
        expect((b.keys.keys[0] as any).aud).toBe("prisma-streams-data");
        expect((b.keys.keys[0] as any).kid).toMatch(/^streams-rs256-[0-9a-f]{16}$/);
        // The issuer's canary (B1.9): a reserved project, one hidden catalog-read
        // credential without a secret, at the smallest quotas that serve a probe:
        // two streams, one for cell-check's `cell-check` stream and one for the
        // alerting watch's canary stream, so neither's create meets the other's.
        expect(b.policies.projects).toEqual([{
          project_id: "sp_canary_cell_sc1", workspace_id: "streams-issuer-canary", cell_id: "cell_sc1",
          project_policy_version: 1, ownership_version: 1, status: "active",
          quotas: { ...CEILINGS, requests_per_sec: 5, max_streams: 2 },
        }]);
        const [[id, canary]] = Object.entries(readState(c.state).credentials);
        expect(b.grants.credentials).toEqual([{ credential_id: id, project_id: "sp_canary_cell_sc1", grant_version: 1,
          status: "active", scopes: "streams.catalog.read", expires_at: null }]);
        expect([canary.role, canary.hidden, canary.display_name, canary.secret_hash]).toEqual(
          ["customer", true, "catalog-read", undefined]);
        expect(validateDocument(b.keys, schema("keys.schema.json"))).toEqual([]);
        expect(readState(c.state).cell.ceilings).toEqual({
          requests_per_sec: 176,
          append_bytes_per_sec: 257500,
          read_bytes_per_sec: 3750000,
          max_inflight_requests: 64,
          max_live_subscriptions: 150,
          max_streams: 8192,
          queued_append_bytes: 67108864,
        });
        if (kind === "dir") expect(statSync(c.state).mode & 0o777).toBe(0o700);
        expectNoSecrets(c.state);
      });

      test("refuses a key file group or others can read, and never quotes a bad key file", async () => {
        const root = scratchDir();
        const open = keyFile(root, "rsa", 0o640, "open.pem");
        const state = freshState(root, "c1");
        let r = await run("init", "--state", state, "--cell-id", "c1", "--deployment-project", "proj_d",
          "--account", "acct_s", "--key-file", open);
        expect([r.code, r.err.split("\n")[0]]).toEqual([
          1, `cell-admin init: key file ${open} has mode 0640: group or others can read the cell's signing key; chmod 600 it`,
        ]);
        const junk = join(root, "junk.pem");
        writeFileSync(junk, "not a key but a secret-looking line 7f3a", { mode: 0o600 });
        r = await run("init", "--state", state, "--cell-id", "c1", "--deployment-project", "proj_d",
          "--account", "acct_s", "--key-file", junk);
        expect([r.code, r.err]).toEqual([1, `cell-admin init: key file ${junk} does not hold a PEM private key`]);
        expect(stateExists(state)).toBe(false);
      });

      test("--public-key initialises the cell from the public half alone, which the private key then signs for", async () => {
        const root = scratchDir();
        const key = keyFile(root, "ed25519");
        const state = freshState(root);
        const init = (...flags: string[]) => run("init", "--state", state, "--cell-id", "cell_sc1",
          "--deployment-project", "proj_deploy", "--account", "acct_sink", ...flags);
        expect((await init("--public-key", key)).err).toBe(
          `cell-admin init: --public-key ${key} holds a private key: give the public half (openssl pkey -pubout)`);
        expect((await init("--public-key", publicKeyFile(key), "--key-file", key)).code).toBe(2);
        expect((await init()).code).toBe(2);
        const r = await init("--public-key", publicKeyFile(key));
        expect([r.code, r.err]).toEqual([0, ""]);
        expect(readBundle(state).keys.keys).toEqual([readState(state).keys[0]]);
        expect((readBundle(state).keys.keys[0] as any).kid).toMatch(/^streams-eddsa-[0-9a-f]{16}$/);
        await run("admit", "--state", state, "--project", "proj_a", "--workspace", "w");
        const out = join(root, "t.jwt");
        expect((await run("token", "--state", state, "--credential", customerCredential(state, "proj_a"),
          "--key-file", key, "--out", out)).code).toBe(0);
        expect(verifiedClaims(readBundle(state), readFileSync(out, "utf8"))?.project_id).toBe("proj_a");
      });

      test("refuses placeholder identities and a second init", async () => {
        const c = await initCell();
        let r = await run("init", "--state", freshState(c.root, "c1"), "--cell-id", "c1", "--deployment-project",
          "proj_local", "--account", "acct_s", "--key-file", c.key);
        expect(r.err).toBe("cell-admin init: a shared cell needs a real PROJECT_ID and ACCOUNT_ID, not a placeholder");
        r = await run("init", "--state", c.state, "--cell-id", "cell_sc1", "--deployment-project", "proj_d",
          "--account", "acct_s", "--key-file", c.key);
        expect(r.err).toBe(`cell-admin init: ${statePathOf(c.state)} exists: a cell is initialised once`);
      });

      test("refuses a cell whose bundle exists though its state does not, publishing nothing", async () => {
        const c = await initCell();
        await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w");
        const published = readBundleText(c.state);
        deleteState(c.state);
        const r = await run("init", "--state", c.state, "--cell-id", "cell_sc1", "--deployment-project", "proj_deploy",
          "--account", "acct_sink", "--key-file", c.key);
        const bundlePath = statePathOf(c.state).replace("cell-state.json", "feeds-bundle.json")
          .replace(/^s3:\/\/ctl-(\d+)\//, "s3://feeds-$1/");
        expect([r.code, r.err]).toEqual([1, `cell-admin init: ${bundlePath} exists at feed_version 2: this cell has ` +
          "published, and a new state would start below what it serves; recover the newest state, or initialise a new " +
          "cell id"]);
        expect(stateExists(c.state)).toBe(false);
        expect(readBundleText(c.state)).toBe(published);
      });

      // deploy-q5 triage B4: the cell's identity and issuer have one source.
      describe("--cell-env", () => {
        const initInto = (c: { root: string; state: string; key: string }, ...flags: string[]) =>
          run("init", "--state", c.state, "--cell-id", "cell_sc1", "--deployment-project", "proj_deploy",
            "--account", "acct_sink", "--key-file", c.key, ...flags);
        const fresh = () => {
          const root = scratchDir();
          return { root, state: freshState(root), key: keyFile(root) };
        };
        const staged = (dir: string) => readdirSync(dir).filter((n) => n.includes(".staged-"));

        test("sets the four values by key in the stage's cells file and keeps every other line as it was", async () => {
          const c = fresh();
          const cells = join(c.root, "scratch.env");
          writeFileSync(cells, "# the scratch cell\nCELL_STAGE=scratch\nCELL_ID=cell_old\n\n  REGION=fra\n" +
            "ACCOUNT_ID=acct_old\nRUST_LOG=info", { mode: 0o640 });
          const r = await initInto(c, "--issuer", "https://issuer.example/streams", "--cell-env", cells);
          expect([r.code, r.err, r.out.split("\n").at(-1)]).toEqual([0, "",
            `cells file: ${cells} (CELL_ID, PROJECT_ID, ACCOUNT_ID, STREAMS_AUTH_ISSUER)`]);
          expect(readFileSync(cells, "utf8")).toBe("# the scratch cell\nCELL_STAGE=scratch\nCELL_ID=cell_sc1\n\n" +
            "  REGION=fra\nACCOUNT_ID=acct_sink\nRUST_LOG=info\nPROJECT_ID=proj_deploy\n" +
            "STREAMS_AUTH_ISSUER=https://issuer.example/streams\n");
          expect(readState(c.state).cell).toMatchObject({ cell_id: "cell_sc1", deployment_project_id: "proj_deploy",
            account_id: "acct_sink", issuer: "https://issuer.example/streams" });
          expect(statSync(cells).mode & 0o777).toBe(0o640);
          expect(staged(c.root)).toEqual([]);
          // The Composer entry takes exactly these four from the cells file.
          for (const key of ["CELL_ID", "PROJECT_ID", "ACCOUNT_ID", "STREAMS_AUTH_ISSUER"]) {
            expect(CHILD_NAMES[key]).toEqual({ from: "cells", required: true });
          }
        });

        test("makes a missing cells file, readable by all (it holds no secret), with the default issuer", async () => {
          const c = fresh();
          const cells = join(c.root, "cert.env");
          expect((await initInto(c, "--cell-env", cells)).code).toBe(0);
          expect(readFileSync(cells, "utf8")).toBe(
            "CELL_ID=cell_sc1\nPROJECT_ID=proj_deploy\nACCOUNT_ID=acct_sink\nSTREAMS_AUTH_ISSUER=https://auth.prisma.io\n");
          expect(statSync(cells).mode & 0o777).toBe(0o644);
        });

        test("refuses a cells file it cannot set by key, or a value it cannot write, before the cell exists", async () => {
          const c = fresh();
          const cells = join(c.root, "scratch.env");
          const refused = async (text: string, ...flags: string[]) => {
            writeFileSync(cells, text);
            const r = await initInto(c, "--cell-env", cells, ...flags);
            expect(stateExists(c.state)).toBe(false);
            expect(readFileSync(cells, "utf8")).toBe(text);
            expect(staged(c.root)).toEqual([]);
            return [r.code, r.err.split("\n")[0]];
          };
          expect(await refused("CELL_STAGE=scratch\nRUST_LOG=info warn\n")).toEqual(
            [1, `cell-admin init: ${cells}:2: not a space-free KEY=VALUE line`]);
          expect(await refused("CELL_STAGE=scratch\nCELL_STAGE\n")).toEqual(
            [1, `cell-admin init: ${cells}:2: not a space-free KEY=VALUE line`]);
          expect(await refused("CELL_ID=a\nREGION=fra\nCELL_ID=b\n")).toEqual(
            [1, `cell-admin init: ${cells} sets CELL_ID twice`]);
          expect(await refused("CELL_STAGE=scratch\n", "--issuer", "https://issuer.example/a b")).toEqual(
            [2, `cell-admin init: STREAMS_AUTH_ISSUER "https://issuer.example/a b" cannot go in ${cells}: ` +
              "a cells file's values are space-free"]);
          const nowhere = join(c.root, "no-such-dir", "scratch.env");
          const r = await initInto(c, "--cell-env", nowhere);
          expect([r.code, r.err.split(":").slice(0, 2).join(":")]).toEqual(
            [1, `cell-admin init: cannot write the cells file beside ${nowhere}`]);
          expect(stateExists(c.state)).toBe(false);
        });

        test("leaves the cells file as it was when init is refused", async () => {
          const c = await initCell();
          const cells = join(c.root, "scratch.env");
          writeFileSync(cells, "CELL_STAGE=scratch\n");
          const again = () => run("init", "--state", c.state, "--cell-id", "cell_sc1", "--deployment-project",
            "proj_other", "--account", "acct_other", "--key-file", c.key, "--cell-env", cells);
          let r = await again();
          expect([r.code, r.err]).toEqual([1, `cell-admin init: ${statePathOf(c.state)} exists: a cell is initialised once`]);
          expect(readFileSync(cells, "utf8")).toBe("CELL_STAGE=scratch\n");
          expect(staged(c.root)).toEqual([]);
          // A lost state: refused under the store's lock, after the file was staged.
          deleteState(c.state);
          r = await again();
          expect([r.code, r.err.includes("exists at feed_version 1: this cell has published")]).toEqual([1, true]);
          expect(readFileSync(cells, "utf8")).toBe("CELL_STAGE=scratch\n");
          expect(staged(c.root)).toEqual([]);
        });
      });
    });

    describe("admit", () => {
      test("refuses a stream prefix the cell's grants parser refuses, and no writer publishes one", async () => {
        const c = await initCell();
        const refused: [string, string][] = [
          ["orders/", "has an empty component (a leading, trailing or double slash)"],
          ["__ds/x", "is in the reserved __ds namespace"],
          ["a b", "holds a control character or a space"],
          ["..", 'has a "." or ".." component'],
        ];
        for (const [prefix, why] of refused) {
          const r = await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w", "--prefix", prefix);
          expect([r.code, r.err.split("\n")[0]]).toEqual([2, `cell-admin admit: --prefix ${JSON.stringify(prefix)} ${why}`]);
        }
        expect(Object.keys(readState(c.state).projects)).toEqual(["sp_canary_cell_sc1"]);
        // Nothing burned: a directory state has no denylist file yet.
        const burned = (() => {
          try {
            return denylistText(c.state);
          } catch {
            return "";
          }
        })();
        expect(burned).not.toContain("proj_a");
        // Another writer's slip: a state holding such a prefix is refused
        // before it is committed again or published.
        const slipped = readState(c.state);
        const [canaryCred] = Object.keys(slipped.credentials);
        slipped.credentials[canaryCred].stream_prefixes = ["a/"];
        writeStateText(c.state, `${JSON.stringify(slipped, null, 2)}\n`);
        const published = readBundleText(c.state);
        const r = await run("bundle", "--state", c.state);
        expect([r.code, r.err]).toEqual([1, `cell-admin bundle: credential ${canaryCred}'s stream_prefixes: "a/" has an ` +
          "empty component (a leading, trailing or double slash): the cell would refuse the whole grants snapshot"]);
        expect(readBundleText(c.state)).toBe(published);
      });

      test("publishes the project at its ceilings, explicit quotas below them, and a fresh credential", async () => {
        const c = await initCell();
        const r = await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "wksp_a",
          "--quota", "max_streams=100", "--quota", "queued_append_bytes=1048576");
        expect(r.code).toBe(0);
        const b = customerOnly(readBundle(c.state));
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
        expect(denylistText(c.state)).toBe("proj_a\n");
      });

      test("publishes a stored-bytes quota above the cell default: that axis has no ceiling", async () => {
        const c = await initCell();
        const r = await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "wksp_a",
          "--quota", "max_stored_bytes=21474836480");
        expect([r.code, r.err]).toEqual([0, ""]);
        expect(r.out).toContain(" queued_append_bytes=67108864 max_stored_bytes=21474836480 request_weight=1 byte_weight=1\n");
        const b = customerOnly(readBundle(c.state));
        expect((b.policies.projects as any[]).map((p) => p.quotas)).toEqual([{ ...CEILINGS, max_stored_bytes: 21474836480 }]);
        expect(validateDocument(b.policies, schema("project-policies.schema.json"))).toEqual([]);
      });

      const refusals: [string, string[], string][] = [
        ["a quota above its ceiling", ["--quota", "max_inflight_requests=65"],
          "--quota max_inflight_requests=65 exceeds the cell's ceiling 64 (bound / k, k = 8)"],
        ["a 0 quota", ["--quota", "requests_per_sec=0"],
          "--quota requests_per_sec=0: 0 means no limit; omit the field to take the ceiling"],
        ["a 0 stored-bytes quota", ["--quota", "max_stored_bytes=0"],
          "--quota max_stored_bytes=0: omit the field to take the cell default"],
        ["an unknown quota", ["--quota", "max_stream=5"], "--quota max_stream: not a quota field"],
        ["a weight as a quota", ["--quota", "byte_weight=5"],
          "--quota byte_weight: a weight is not a quota; admit sets it with --weight"],
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
        const before = readBundleText(c.state);
        const r = await run("admit", "--state", c.state, ...args);
        expect(r.code).not.toBe(0);
        expect(r.err.split("\n")[0]).toContain(message);
        expect(readBundleText(c.state)).toBe(before);
        expect(Object.keys(readState(c.state).projects)).toEqual(["sp_canary_cell_sc1"]);
      });

      // Layout 6A package 1: weights scale a project's ceilings (bound / k x w
      // on the request axes and the byte rates, queued bytes unscaled), and
      // heavy projects pack so their weights on each axis sum to at most k - 1.
      test("admits a weighted project at its weighted ceilings and packs heavy projects within k - 1", async () => {
        const c = await initCell();
        const m = await run("admit", "--state", c.state, "--project", "proj_m", "--workspace", "w1",
          "--weight", "request=2", "--weight", "bytes=5");
        expect([m.code, m.err]).toEqual([0, ""]);
        const weighted = {
          ...CEILINGS, requests_per_sec: 352, append_bytes_per_sec: 1287500, read_bytes_per_sec: 18750000,
          max_inflight_requests: 128, max_live_subscriptions: 300, max_streams: 16384,
          request_weight: 2, byte_weight: 5,
        };
        const quotasOf = () =>
          Object.fromEntries((customerOnly(readBundle(c.state)).policies.projects as any[]).map((p) => [p.project_id, p.quotas]));
        expect(quotasOf()).toEqual({ proj_m: weighted });
        expect(validateDocument(readBundle(c.state).policies, schema("project-policies.schema.json"))).toEqual([]);
        const over = await run("admit", "--state", c.state, "--project", "proj_l", "--workspace", "w2", "--weight", "bytes=3");
        expect([over.code, over.err]).toEqual([1,
          "cell-admin admit: --weight bytes=3: the cell's heavy projects would hold 8 byte_weight units, over k - 1 = 7 (they hold 5)"]);
        // A second byte-weighted project fits the k - 1 byte budget (5 + 2 = 7) but
        // not the read memory: two weighted lines and two unit lines would fill it.
        const memory = await run("admit", "--state", c.state, "--project", "proj_s2", "--workspace", "w2", "--weight", "bytes=2");
        expect(memory.err).toBe(
          "cell-admin admit: --weight bytes=2: the byte-weighted projects' memory lines (127926270) and two unit " +
            "lines (67108864) would fill the instance read memory (131072000)");
        const fits = await run("admit", "--state", c.state, "--project", "proj_s2", "--workspace", "w2",
          "--weight", "request=2");
        expect(fits.code).toBe(0);
        const unit = await run("admit", "--state", c.state, "--project", "proj_t", "--workspace", "w3");
        expect(unit.code).toBe(0);
        const requests = await run("admit", "--state", c.state, "--project", "proj_r", "--workspace", "w4",
          "--weight", "request=3");
        expect(requests.code).toBe(0);
        const full = await run("admit", "--state", c.state, "--project", "proj_r2", "--workspace", "w5",
          "--weight", "request=2");
        expect(full.err).toBe(
          "cell-admin admit: --weight request=2: the cell's heavy projects would hold 9 request_weight units, over k - 1 = 7 (they hold 7)");
        const q = quotasOf();
        expect([q.proj_s2.append_bytes_per_sec, q.proj_s2.requests_per_sec, q.proj_t]).toEqual([257500, 352, CEILINGS]);
        expect(Object.keys(q).sort()).toEqual(["proj_m", "proj_r", "proj_s2", "proj_t"]);
        const above = await run("quota", "--state", c.state, "--project", "proj_m", "--quota", "append_bytes_per_sec=1287501");
        expect(above.err).toBe(
          "cell-admin quota: --quota append_bytes_per_sec=1287501 exceeds the cell's ceiling 1287500 (bound / k x byte_weight 5, k = 8)");
        const below = await run("quota", "--state", c.state, "--project", "proj_m", "--quota", "append_bytes_per_sec=1200000");
        expect(below.code).toBe(0);
        expect(quotasOf().proj_m).toEqual({ ...weighted, append_bytes_per_sec: 1200000 });
      });

      const weightRefusals: [string, string[], string][] = [
        ["a weight of k", ["--weight", "bytes=8"], "--weight bytes=8: a weight is 1 to k - 1 = 7"],
        ["a weight of 0", ["--weight", "request=0"], "--weight request=0: a weight is 1 to k - 1 = 7"],
        ["an unknown weight", ["--weight", "reads=2"], "--weight reads=2 is not request=N or bytes=N"],
        ["a weight given twice", ["--weight", "bytes=2", "--weight", "bytes=3"], "--weight bytes given twice"],
        ["a quota above the weighted ceiling", ["--weight", "request=2", "--quota", "max_inflight_requests=129"],
          "--quota max_inflight_requests=129 exceeds the cell's ceiling 128 (bound / k x request_weight 2, k = 8)"],
      ];
      test.each(weightRefusals)("refuses %s and publishes nothing", async (_what, flags, message) => {
        const c = await initCell();
        const before = readBundleText(c.state);
        const r = await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "wksp_a", ...flags);
        expect(r.code).not.toBe(0);
        expect(r.err.split("\n")[0]).toContain(message);
        expect(readBundleText(c.state)).toBe(before);
      });

      test("the canary is reserved and hidden: no customer takes its id or workspace, no cap counts it", async () => {
        const c = await initCell(["--max-projects", "1"]);
        expect((await run("admit", "--state", c.state, "--project", "sp_canary_x", "--workspace", "w")).err)
          .toBe("cell-admin admit: project sp_canary_x is reserved on cell cell_sc1");
        expect((await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "streams-issuer-canary")).err)
          .toBe("cell-admin admit: workspace streams-issuer-canary is the issuer's canary workspace");
        expect((await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w")).code).toBe(0);
        const r = await run("list", "--state", c.state);
        expect(r.out).toContain("projects: 1 placed of 1, 0 offboarded; workspace cap 1");
        expect(r.out).not.toContain("sp_canary");
        expect(r.out).not.toContain(Object.keys(readState(c.state).credentials)[0]);
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
        const seed = "# ids used anywhere\nproj_old\n";
        let c;
        if (kind === "dir") {
          const deny = join(scratchDir(), "fleet-denylist.txt");
          writeFileSync(deny, seed);
          c = await initCell(["--denylist", deny]);
        } else {
          // An s3 state's denylist is its control bucket's, shared by its cells.
          c = await initCell();
          const [bucket] = c.state.slice("s3://".length).split("/");
          fakeS3().put(bucket, "denylist.txt", seed);
        }
        expect((await run("admit", "--state", c.state, "--project", "proj_old", "--workspace", "w")).err)
          .toBe("cell-admin admit: project id proj_old was used before: an id is never placed twice");
        await run("admit", "--state", c.state, "--project", "proj_new", "--workspace", "w");
        expect(denylistText(c.state)).toBe("# ids used anywhere\nproj_old\nproj_new\n");
        expect((await run("admit", "--state", c.state, "--project", "proj_new", "--workspace", "w2")).err)
          .toBe("cell-admin admit: project id proj_new was used before: an id is never placed twice");
      });

      test("refuses to admit once the profiles' ceilings moved", async () => {
        const root = scratchDir();
        const prof = join(root, "shared.env");
        writeFileSync(prof, readFileSync(join(import.meta.dir, "..", "profiles", "shared-cell.env")));
        const profiles = ["--profile", join(import.meta.dir, "..", "profiles", "compute-1g.env"), "--profile", prof];
        const c = await initCell(profiles);
        writeFileSync(prof, `${readFileSync(prof, "utf8")}SSE_MAX_CONNECTIONS=800\n`);
        expect((await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w", ...profiles)).err).toBe(
          "cell-admin admit: the profiles changed since init (max_live_subscriptions 150 -> 100): " +
            "a cell's ceilings are fixed for its life",
        );
      });

      test("checks the shipped profile against the digest init recorded, not the files init read", async () => {
        // The issuer admits on Compute, where the paths init read do not exist:
        // it compares its bundled copy of the profile with the cell's record.
        const root = scratchDir();
        const copies = ["compute-1g.env", "shared-cell.env"].map((name) => {
          const path = join(root, name);
          writeFileSync(path, readFileSync(join(import.meta.dir, "..", "profiles", name)));
          return path;
        });
        const c = await initCell(copies.flatMap((p) => ["--profile", p]));
        for (const p of copies) rmSync(p);
        expect((await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w")).code).toBe(0);
        expect(readState(c.state).cell.profile_digest).toMatch(/^[0-9a-f]{64}$/);
      });

      test("refuses to admit once the layered profile changed, even where no ceiling moved", async () => {
        const root = scratchDir();
        const prof = join(root, "shared.env");
        writeFileSync(prof, `${readFileSync(join(import.meta.dir, "..", "profiles", "shared-cell.env"), "utf8")}SEGMENT_TARGET_BYTES=1\n`);
        const c = await initCell();
        const r = await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w",
          "--profile", join(import.meta.dir, "..", "profiles", "compute-1g.env"), "--profile", prof);
        expect([r.code, r.err.replace(/[0-9a-f]{12}/g, "<d>")]).toEqual([1,
          "cell-admin admit: the layered profile differs from the one init recorded (digest <d> -> <d>): " +
            "a cell's profile is fixed for its life"]);
        expect(Object.keys(readState(c.state).projects)).toEqual(["sp_canary_cell_sc1"]);
      });
    });

    // deploy-q5 triage B4 (D9): a release's profiles against the cell's
    // fixed sizing, before the release deploys.
    describe("check-sizing", () => {
      const shipped = (name: string) => readFileSync(join(import.meta.dir, "..", "profiles", name), "utf8");
      /// A release's copy of the two profiles, the second with `extra` appended.
      const release = (extra = "") => {
        const root = scratchDir();
        return ["compute-1g.env", "shared-cell.env"].flatMap((name, i) => {
          const path = join(root, name);
          writeFileSync(path, `${shipped(name)}${i === 1 ? extra : ""}`);
          return ["--profile", path];
        });
      };
      /// What check-sizing must leave as it was: the state, the bundle, and
      /// a directory store's listing.
      const unchanged = (state: string) => [readStateText(state), readBundleText(state),
        ...(state.startsWith("s3://") ? [] : readdirSync(state).sort())];

      test("accepts a release's profiles that give the cell's sizing, reads the state and writes nothing", async () => {
        const c = await initCell();
        const before = unchanged(c.state);
        const r = await run("check-sizing", "--state", c.state, ...release("# a comment the digest ignores\n"));
        const digest = readState(c.state).cell.profile_digest.slice(0, 12);
        expect([r.code, r.out, r.err]).toEqual([0,
          `cell cell_sc1: the profiles give its fixed sizing (k=8, the ceilings init fixed, profile digest ${digest})`, ""]);
        expect(unchanged(c.state)).toEqual(before);
      });

      test("refuses, exit 1 and with admit's message, profiles that move a ceiling or change the layered profile", async () => {
        const c = await initCell();
        const before = unchanged(c.state);
        let r = await run("check-sizing", "--state", c.state, ...release("SSE_MAX_CONNECTIONS=800\n"));
        expect([r.code, r.out, r.err]).toEqual([1, "",
          "cell-admin check-sizing: the profiles changed since init (max_live_subscriptions 150 -> 100): " +
            "a cell's ceilings are fixed for its life"]);
        r = await run("check-sizing", "--state", c.state, ...release("SEGMENT_TARGET_BYTES=1\n"));
        expect([r.code, r.err.replace(/[0-9a-f]{12}/g, "<d>")]).toEqual([1,
          "cell-admin check-sizing: the layered profile differs from the one init recorded (digest <d> -> <d>): " +
            "a cell's profile is fixed for its life"]);
        r = await run("check-sizing", "--state", c.state, ...release("KEEP_AWAKE=0\n"));
        expect([r.code, r.err]).toEqual([1,
          "cell-admin check-sizing: profile KEEP_AWAKE=0: a shared cell needs KEEP_AWAKE=1"]);
        expect(unchanged(c.state)).toEqual(before);
      });

      test("needs the release's --profile files, never the tool's own copies, and a cell", async () => {
        const c = await initCell();
        let r = await run("check-sizing", "--state", c.state);
        expect([r.code, r.err.split("\n")[0]]).toEqual([2, "cell-admin check-sizing: --profile is required"]);
        deleteState(c.state);
        r = await run("check-sizing", "--state", c.state, ...release());
        expect([r.code, r.err]).toEqual([1,
          `cell-admin check-sizing: ${statePathOf(c.state)} does not exist: run cell-admin init first`]);
      });
    });

    describe("token", () => {
      test("mints a 24 h token the cell's keys verify, at the credential's versions, into a 0600 file only", async () => {
        const c = await initCell();
        await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "wksp_a", "--prefix", "orders");
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
          stream_prefixes: ["orders"],
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
        const state = freshState(root, "c2");
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
        const stale = readStateText(c.state);
        await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w");
        writeStateText(c.state, stale);
        const r = await run("admit", "--state", c.state, "--project", "proj_b", "--workspace", "w2");
        expect(r.err).toContain("is at feed_version 1 but its bundle is at 2: the state was restored from a stale copy");
      });

      test("refuses a streams-cell-admin/v1 state: no reader for it exists, the cell is re-initialised", async () => {
        const c = await initCell();
        const path = statePathOf(c.state);
        writeStateText(c.state, JSON.stringify({ ...readState(c.state), format: "streams-cell-admin/v1" }));
        const r = await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w");
        expect([r.code, r.err]).toEqual([1, `cell-admin admit: ${path} is a streams-cell-admin/v1 state, ` +
          "which this cell-admin does not read: re-initialise the cell"]);
      });

      test("the bundle publishes each project's status and each credential's expiry from the state", async () => {
        const c = await initCell();
        await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w");
        const cred = customerCredential(c.state, "proj_a");
        const s = readState(c.state) as any;
        Object.assign(s.projects.proj_a, { status: "suspended", project_policy_version: 2 });
        Object.assign(s.credentials[cred], { expires_at: 1_900_000_000, grant_version: 2 });
        writeStateText(c.state, JSON.stringify(s));
        expect((await run("bundle", "--state", c.state)).code).toBe(0);
        const b = customerOnly(readBundle(c.state));
        expect((b.policies.projects as any[]).map((p) => [p.status, p.project_policy_version])).toEqual([["suspended", 2]]);
        expect((b.grants.credentials as any[]).map((g) => [g.expires_at, g.grant_version])).toEqual([[1_900_000_000, 2]]);
      });

      test("keeps the newest 100 journal entries in the state and writes older ones to the audit prefix", async () => {
        const c = await initCell();
        const s = readState(c.state);
        s.journal = Array.from({ length: 100 }, (_, i) => ({ at: "2026-10-12T00:00:00.000Z", command: "x", detail: `j${i}` }));
        writeStateText(c.state, JSON.stringify(s));
        expect((await run("bundle", "--state", c.state)).code).toBe(0);
        const journal = readState(c.state).journal;
        expect([journal.length, journal[0].detail, journal[99].command]).toEqual([100, "j1", "bundle"]);
        const audit = auditTexts(c.state).map((t) => JSON.parse(t));
        expect(audit).toEqual([{ kind: "journal", cell_id: "cell_sc1", entries: [s.journal[0]] }]);
      });

      test("refuses to publish an entry whose content changed at a version the cell already holds", async () => {
        // The cell drops a snapshot that changes an entry without its version
        // and goes dark when its feeds age past 300 s (publication.rs).
        const c = await initCell();
        await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w");
        const cred = customerCredential(c.state, "proj_a");
        const before = readBundleText(c.state);
        const s = readState(c.state) as any;
        s.projects.proj_a.quotas.max_streams = 5;
        s.credentials[cred].scopes = "streams.catalog.read";
        writeStateText(c.state, JSON.stringify(s));
        const r = await run("bundle", "--state", c.state);
        expect([r.code, r.err]).toEqual([1, "cell-admin bundle: the bundle at feed_version 3 changes what the cell " +
          `holds without a new version: project proj_a at project_policy_version 1, credential ${cred} at ` +
          "grant_version 1; a published field changes only with its entry's version"]);
        expect(readBundleText(c.state)).toBe(before);
      });

      test.if(kind === "dir")("refuses while another run holds the directory", async () => {
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
  });
}
