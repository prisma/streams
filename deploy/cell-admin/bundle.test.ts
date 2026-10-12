// cell-admin as one bundled file (public-beta issuer triage B2.1: the
// issuer ships as "one self-contained ESM file", and cell-admin is its
// library). A bundle runs where the repository is absent, so nothing the
// library needs may be read from the repository at run time.
// Run with `bun test ./deploy/cell-admin`.
import { afterEach, expect, test } from "bun:test";
import { copyFileSync } from "node:fs";
import { join } from "node:path";
import { validateDocument } from "../../platform-demo/src/validate.mjs";
import { schema } from "./feeds";
import { cleanup, keyFile, readBundle, scratchDir } from "./test-rig";

afterEach(cleanup);

const PROFILES = join(import.meta.dir, "..", "profiles");

test("a single-file bundle of cell-admin initialises a cell away from the repository", async () => {
  const dir = scratchDir();
  const built = await Bun.build({
    entrypoints: [join(import.meta.dir, "cell-admin.ts")],
    target: "bun",
    outdir: dir,
    naming: "cell-admin.js",
  });
  expect(built.success).toBe(true);
  const profiles = ["compute-1g.env", "shared-cell.env"].map((name) => {
    copyFileSync(join(PROFILES, name), join(dir, name));
    return join(dir, name);
  });
  const state = join(dir, "state");
  const run = Bun.spawnSync(
    [
      process.execPath, join(dir, "cell-admin.js"), "init", "--state", state, "--cell-id", "cell_b1",
      "--deployment-project", "proj_deploy", "--account", "acct_sink", "--key-file", keyFile(dir, "ed25519"),
      ...profiles.flatMap((p) => ["--profile", p]),
    ],
    { cwd: dir },
  );
  expect([run.exitCode, run.stderr.toString()]).toEqual([0, ""]);
  const bundle = readBundle(state);
  expect(validateDocument(bundle.keys, schema("keys.schema.json"))).toEqual([]);
  expect(bundle.keys.feed_version).toBe(1);
});
