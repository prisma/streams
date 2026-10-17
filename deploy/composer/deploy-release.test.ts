// deploy-release.sh (public-beta deploy-q5 triage B5, design §2.3): the
// self-tests of every refusal before a deploy (a secret in the deploy
// shell's environment, a dotfile in the release, a region mismatch, a
// missing secret name, KEEP_AWAKE_UNTIL_MS in production's cells file,
// each of the three crossings against the running cell's release, from
// the cell's /v1/debug/load or the deploy log), and of the deploy itself:
// "no cell yet" accepted, the release's own CLI in an environment of seven
// names (the deploy's nonce kept across re-runs), cell-check after, one
// log line per deploy. Offline: a fake
// Management API, a fake cell, releases staged by release.ts from a
// scratch git tree, a cell-admin state in a directory, and a fake `prisma`
// that records how it was run and puts the release in place on the fake
// cell. Run with `bun test ./deploy/composer`.
import { afterAll, describe, expect, test } from "bun:test";
import { existsSync, mkdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { cleanup, initCell, readBundle } from "../cell-admin/test-rig";
import { FakeStreamsCell } from "./fake-cell";
import { FakePlatform } from "./fake-platform";
import type { Release } from "./release";
import {
  cellsText, cleanupScratch, fakeTree, issuerSettingsText, scratchDir, stage, stageIssuer, stageWatch, watchSettingsText,
} from "./release-rig";

const SCRIPT = join(import.meta.dir, "deploy-release.sh");
const NAMES = [
  "STREAMS_OPERATOR_TOKEN",
  "STREAMS_USAGE_STREAM_KEY",
  "STREAMS_CURSOR_KEY",
  "STREAMS_DATA_S3_ACCESS_KEY_ID",
  "STREAMS_DATA_S3_SECRET_ACCESS_KEY",
  "STREAMS_FEEDS_S3_ACCESS_KEY_ID",
  "STREAMS_FEEDS_S3_SECRET_ACCESS_KEY",
];
const CANARY = "sp_canary_cell_sc1";
const WATCH_NAMES = [
  "STREAMS_OPERATOR_TOKEN", "ALERT_SINK_URL", "ALERT_SINK_KEY", "HEARTBEAT_URL", "STREAMS_ISSUER_METRICS_TOKEN", "CANARY_CREDENTIAL",
];
const ISSUER_NAMES = [
  "STREAMS_ISSUER_SIGNING_KEY",
  "STREAMS_ISSUER_PEPPER",
  "STREAMS_ISSUER_SESSION_KEY",
  "STREAMS_ISSUER_OAUTH_CLIENT_SECRET",
  "STREAMS_ISSUER_METRICS_TOKEN",
  "STREAMS_CONTROL_S3_ACCESS_KEY_ID",
  "STREAMS_CONTROL_S3_SECRET_ACCESS_KEY",
  "STREAMS_FEEDS_S3_ACCESS_KEY_ID",
  "STREAMS_FEEDS_S3_SECRET_ACCESS_KEY",
];

const stops: { stop(): void }[] = [];
afterAll(() => {
  for (const s of stops.splice(0)) s.stop();
  cleanupScratch();
  cleanup();
});

/// A fake `prisma`: records its arguments, environment and directory, and
/// puts the release it runs from in place on the fake cell.
const prismaScript = (record: string, cellUrl: string | null, code = 0) => `#!/usr/bin/env bun
import { appendFileSync, readFileSync } from "node:fs";
appendFileSync(${JSON.stringify(record)}, JSON.stringify({ argv: process.argv.slice(2), env: process.env, cwd: process.cwd() }) + "\\n");
${cellUrl ? `const r = JSON.parse(readFileSync("deploy/release.json", "utf8"));
await fetch(${JSON.stringify(`${cellUrl}/__deploy`)}, {
  method: "POST", body: JSON.stringify({ commit: r.commit, sha: r.server_sha256, nonce: process.env.CELL_DEPLOY_NONCE }),
});` : ""}
process.exit(${code});
`;

/// A fake destroy.ts, as the release carries it at its root: records how
/// it was run.
const destroyScript = (record: string) => `import { appendFileSync } from "node:fs";
appendFileSync(${JSON.stringify(record)}, JSON.stringify({ argv: ["destroy", ...process.argv.slice(2)], env: process.env, cwd: process.cwd() }) + "\\n");
`;

interface World {
  root: string;
  releases: string;
  api: FakePlatform;
  project: string;
  tokenFile: string;
  record: string;
  state: string;
  cell: FakeStreamsCell;
  files: { bearer: string; canary: string };
  tree: { tree: string; commit: string };
}

/// A project in eu-central-1 holding the cell's seven secrets in its
/// production class, a cell-admin state for cell_sc1, a fake cell, and an
/// empty releases directory.
async function world(): Promise<World> {
  const root = scratchDir();
  const releases = join(root, "releases");
  mkdirSync(releases);
  const api = new FakePlatform();
  const project = api.addProject("streams-cell", "eu-central-1").id;
  for (const n of NAMES) api.addVariable({ projectId: project, branchId: null, class: "production", key: n }, `${n}-v`);
  const tokenFile = join(root, "token");
  writeFileSync(tokenFile, api.token, { mode: 0o600 });
  const c = await initCell();
  const v = readBundle(c.state).policies.feed_version;
  const canary = ["{}", JSON.stringify({ project_id: CANARY, cell_id: "cell_sc1" }), "s"]
    .map((p) => Buffer.from(p).toString("base64url"))
    .join(".");
  const cell = new FakeStreamsCell({ commit: "none", sha: "none", policies: v, grants: v }, "operator-bearer", canary, CANARY);
  stops.push(api, cell);
  const files = { bearer: join(root, "bearer"), canary: join(root, "canary") };
  writeFileSync(files.bearer, "operator-bearer", { mode: 0o600 });
  writeFileSync(files.canary, canary, { mode: 0o600 });
  const record = join(root, "prisma-runs");
  const tree = fakeTree((t) => writeFileSync(join(t, "deploy/composer/destroy.ts"), destroyScript(record)));
  return { root, releases, api, project, tokenFile, record, state: c.state, cell, files, tree };
}

/// A release <tag> in the world's releases directory, from its tree unless
/// another is given.
function release(w: World, tag: string, o: { cells?: Record<string, string>; tree?: World["tree"]; prismaCode?: number } = {}) {
  const t = o.tree ?? w.tree;
  return stage({
    ...t, tag, releases: w.releases,
    cells: o.cells ?? { production: cellsText("production"), cert: cellsText("cert") },
    prismaScript: prismaScript(w.record, w.cell.url, o.prismaCode ?? 0),
  });
}

/// The cell runs `r`, deployed with its tag as the nonce.
const runs = (w: World, r: Release) => {
  w.cell.identity = { ...w.cell.identity, commit: r.commit, sha: r.server_sha256 };
  w.cell.nonce = r.tag;
};

const withCell = (w: World) => [
  "--cell-url", w.cell.url, "--bearer-file", w.files.bearer, "--canary-token-file", w.files.canary,
];

async function deploy(w: World, stageName: string, dir: string, extra: string[] = [], env: Record<string, string> = {}) {
  const proc = Bun.spawn(
    ["bash", SCRIPT, stageName, dir, "--project", w.project, "--workspace", "wksp_1", "--state", w.state,
      "--management-api", w.api.url, ...extra],
    {
      env: { PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: process.env.HOME ?? "/", PRISMA_TOKEN_FILE: w.tokenFile, ...env },
      stdout: "pipe",
      stderr: "pipe",
    },
  );
  const [out, err, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
  return { code, out, err };
}

const prismaRuns = (w: World) =>
  existsSync(w.record) ? readFileSync(w.record, "utf8").trim().split("\n").map((l) => JSON.parse(l)) : [];
const log = (w: World) =>
  existsSync(join(w.releases, "deploy-log.jsonl"))
    ? readFileSync(join(w.releases, "deploy-log.jsonl"), "utf8").trim().split("\n").map((l) => JSON.parse(l))
    : [];

/// A refusal before anything deploys: exit 1, the message, no CLI run.
function refusedBeforeDeploy(w: World, r: { code: number; err: string }, message: string): void {
  expect([r.code, r.err.trim(), prismaRuns(w).length, log(w).length]).toEqual([1, `deploy-release: ${message}`, 0, 0]);
}

describe("deploy-release.sh", () => {
  test("no cell yet: deploys with the release's own CLI, from its root, in an environment of seven names, and logs it unchecked", async () => {
    const w = await world();
    const { dir, release: r } = release(w, "rc-1");
    const run = await deploy(w, "production", dir);
    expect([run.code, run.err]).toEqual([3, ""]);
    expect(run.out).toContain("deploy-release: no cell yet: nothing answers for production and the deploy log holds no live deploy of it");
    expect(run.out).toContain("deploy-release: cell cell_sc1: the profiles give its fixed sizing");
    const [p] = prismaRuns(w);
    expect([p.argv, realpathSync(p.cwd), Object.keys(p.env).sort()]).toEqual([
      ["deploy", "deploy/composer/app.ts"],
      realpathSync(dir),
      ["CELL_DEPLOY_NONCE", "CELL_STAGE", "HOME", "PATH", "PRISMA_REGION", "PRISMA_SERVICE_TOKEN", "PRISMA_WORKSPACE_ID"],
    ]);
    expect([p.env.CELL_STAGE, p.env.CELL_DEPLOY_NONCE, p.env.PRISMA_REGION, p.env.PRISMA_WORKSPACE_ID, p.env.PRISMA_SERVICE_TOKEN]).toEqual([
      "production", "rc-1", "eu-central-1", "wksp_1", w.api.token,
    ]);
    const [entry] = log(w);
    expect({ ...entry, at: typeof entry.at }).toEqual({
      app: "cell",
      stage: "production", project: w.project, branch: null, tag: "rc-1", nonce: "rc-1", server_sha256: r.server_sha256, at: "string",
      result: "unchecked",
    });
    expect(run.out.includes(w.api.token)).toBe(false);
  }, 30_000);

  test("reads the running release from the cell, deploys the next one and passes cell-check", async () => {
    const w = await world();
    runs(w, release(w, "rc-1").release);
    const { dir, release: r2 } = release(w, "rc-2");
    const run = await deploy(w, "production", dir, withCell(w));
    expect([run.code, run.err]).toEqual([0, ""]);
    expect(run.out).toContain(`deploy-release: the running cell is release rc-1`);
    expect(run.out).toContain("CELL_CHECK_PASS production");
    expect(run.out).toContain("deploy-release: rc-2 is deployed to production and passed cell-check");
    expect([w.cell.identity.sha, log(w).map((e) => [e.tag, e.result])]).toEqual([r2.server_sha256, [["rc-2", "pass"]]]);
  }, 60_000);

  test("refuses a secret the release binds set in the deploy shell's environment, before it asks the platform", async () => {
    const w = await world();
    const { dir } = release(w, "rc-1");
    const r = await deploy(w, "production", dir, [], { STREAMS_CURSOR_KEY: "x", STREAMS_OPERATOR_TOKEN: "y" });
    refusedBeforeDeploy(w, r,
      "STREAMS_OPERATOR_TOKEN, STREAMS_CURSOR_KEY are set in this shell's environment: a secret reaches the platform " +
        "through set-secrets only (Composer's preflight would push it from here); unset it");
    expect(w.api.requests).toEqual([]);
  }, 30_000);

  test("refuses a release directory holding a dotfile", async () => {
    const w = await world();
    const { dir } = release(w, "rc-1");
    writeFileSync(join(dir, "deploy", ".env"), "STREAMS_CURSOR_KEY=x\n");
    refusedBeforeDeploy(w, await deploy(w, "production", dir), `${dir} is not the release it was cut as: deploy/.env (a dotfile)`);
  }, 30_000);

  test("refuses a project outside the cells file's REGION, and a secret name missing from the stage's scope", async () => {
    const w = await world();
    const { dir } = release(w, "rc-1", { cells: { production: cellsText("production", "cell_sc1", "us-east-1") } });
    refusedBeforeDeploy(w, await deploy(w, "production", dir),
      `project ${w.project} is in eu-central-1; the release's cells/production.env names REGION=us-east-1`);
    const w2 = await world();
    w2.api.variables = w2.api.variables.filter((v) => v.key !== "STREAMS_FEEDS_S3_ACCESS_KEY_ID");
    w2.api.addVariable({ projectId: w2.project, branchId: "br_cert", class: "preview", key: "STREAMS_FEEDS_S3_ACCESS_KEY_ID" }, "v");
    const { dir: dir2 } = release(w2, "rc-1");
    refusedBeforeDeploy(w2, await deploy(w2, "production", dir2),
      `project ${w2.project}'s production variables holds no STREAMS_FEEDS_S3_ACCESS_KEY_ID: set them with set-secrets first`);
  }, 30_000);

  test("refuses a --project other than the one prisma deploy adopts: by logicalId, else the workspace's oldest of the app's name", async () => {
    const w = await world();
    const { dir } = release(w, "rc-1");
    const holdsSecrets = (id: string) => {
      for (const n of NAMES) w.api.addVariable({ projectId: id, branchId: null, class: "production", key: n }, `${n}-x`);
      return id;
    };
    const renamed = holdsSecrets(w.api.addProject("streams-cell-old", "eu-central-1").id);
    refusedBeforeDeploy(w, await deploy({ ...w, project: renamed }, "production", dir),
      `prisma deploy puts app streams-cell of workspace wksp_1 in project ${w.project} (the oldest project of that name), ` +
        `not in --project ${renamed}`);
    holdsSecrets(w.api.addProject("streams-cell", "eu-central-1", { workspaceId: "wksp_2", createdAt: "2020-01-01T00:00:00.000Z" }).id);
    const older = holdsSecrets(w.api.addProject("streams-cell", "eu-central-1", { createdAt: "2021-01-01T00:00:00.000Z" }).id);
    refusedBeforeDeploy(w, await deploy(w, "production", dir),
      `prisma deploy puts app streams-cell of workspace wksp_1 in project ${older} (the oldest project of that name), ` +
        `not in --project ${w.project}`);
    const logical = holdsSecrets(w.api.addProject("cell-renamed", "eu-central-1", { logicalId: "streams-cell" }).id);
    refusedBeforeDeploy(w, await deploy({ ...w, project: older }, "production", dir),
      `prisma deploy puts app streams-cell of workspace wksp_1 in project ${logical} (its logicalId), not in --project ${older}`);
    const ok = await deploy({ ...w, project: logical }, "production", dir);
    expect([ok.code, ok.err, prismaRuns(w).length]).toEqual([3, "", 1]);
  }, 60_000);

  test("refuses a --branch other than the Branch prisma deploy --stage adopts, the project's Branch named as the stage", async () => {
    const w = await world();
    const { dir } = release(w, "rc-1");
    const other = w.api.addBranch(w.project, "cert-old").id;
    for (const n of NAMES) w.api.addVariable({ projectId: w.project, branchId: other, class: "preview", key: n }, `${n}-c`);
    refusedBeforeDeploy(w, await deploy(w, "cert", dir, ["--branch", other]),
      `project ${w.project} has no Branch named cert: prisma deploy --stage cert would create one, not deploy to --branch ${other}`);
    const cert = w.api.addBranch(w.project, "cert").id;
    refusedBeforeDeploy(w, await deploy(w, "cert", dir, ["--branch", other]),
      `prisma deploy --stage cert deploys to Branch ${cert} of project ${w.project} (its Branch named cert), not to --branch ${other}`);
  }, 30_000);

  test("judges a release by its own env builder and app.ts, not the checkout's", async () => {
    const w = await world();
    const edit = (path: string, from: string, to: string) => (t: string) => {
      const p = join(t, path);
      const text = readFileSync(p, "utf8");
      if (!text.includes(from)) throw new Error(`${path} holds no ${from}`);
      writeFileSync(p, text.replace(from, to));
    };
    const knob = fakeTree(edit("deploy/composer/streams/env.ts", "  RUST_LOG: optional,\n", "  RUST_LOG: optional,\n  FUTURE_KNOB: optional,\n"));
    const cells = { production: cellsText("production", "cell_sc1", "eu-central-1", ["FUTURE_KNOB=1"]) };
    const admits = await deploy(w, "production", release(w, "rc-1", { tree: knob, cells }).dir);
    expect([admits.code, admits.err, prismaRuns(w).length]).toEqual([3, "", 1]);
    const strict = fakeTree(edit("deploy/composer/streams/env.ts", "  RUST_LOG: optional,\n", ""));
    const verbose = { production: cellsText("production", "cell_sc1", "eu-central-1", ["RUST_LOG=info"]) };
    const refused = await deploy(w, "production", release(w, "rc-2", { tree: strict, cells: verbose }).dir);
    expect([refused.code, refused.err.trim().split(": ").slice(0, 2), prismaRuns(w).length]).toEqual([
      1, ["deploy-release", "the cell's entry would refuse this release on production"], 1,
    ]);
    const secret = fakeTree(edit("deploy/composer/app.ts", "      cellStage:", '      futureKey: envSecret("STREAMS_FUTURE_KEY"),\n      cellStage:'));
    const { dir } = release(w, "rc-3", { tree: secret });
    const inShell = await deploy(w, "production", dir, [], { STREAMS_FUTURE_KEY: "x" });
    expect([inShell.code, inShell.err.trim(), prismaRuns(w).length]).toEqual([
      1, "deploy-release: STREAMS_FUTURE_KEY is set in this shell's environment: a secret reaches the platform through " +
        "set-secrets only (Composer's preflight would push it from here); unset it", 1,
    ]);
  }, 60_000);

  test("refuses KEEP_AWAKE_UNTIL_MS in production's cells file", async () => {
    const w = await world();
    const { dir } = release(w, "rc-1", { cells: { production: cellsText("production", "cell_sc1", "eu-central-1", ["KEEP_AWAKE_UNTIL_MS=1760000000000"]) } });
    refusedBeforeDeploy(w, await deploy(w, "production", dir),
      `${join(dir, "deploy", "cells", "production.env")} sets KEEP_AWAKE_UNTIL_MS: a production cell is kept awake for good`);
  }, 30_000);

  test("refuses a release whose layout, keys contract or body ceiling differs from the running cell's release", async () => {
    const w = await world();
    runs(w, release(w, "rc-1").release);
    const edited = (tag: string, change: Partial<Release>) => {
      const { dir } = release(w, tag);
      const path = join(dir, "deploy", "release.json");
      writeFileSync(path, JSON.stringify({ ...JSON.parse(readFileSync(path, "utf8")), ...change }));
      return dir;
    };
    const layout = edited("rc-2", { layout_version: 6 });
    refusedBeforeDeploy(w, await deploy(w, "production", layout, withCell(w)),
      "release rc-2 has layout_version 6, the running cell's release rc-1 5: a deploy never crosses a layout");
    const keys = edited("rc-3", { keys_contract: "r8" });
    refusedBeforeDeploy(w, await deploy(w, "production", keys, withCell(w)),
      "release rc-3 has keys contract r8, the running cell's release rc-1 r7: the keys feed and the binary change together (RUNBOOK), never by a deploy alone");
    const bigger = fakeTree((t) => {
      const p = join(t, "deploy/profiles/shared-cell.env");
      writeFileSync(p, readFileSync(p, "utf8").replace(/^MAX_REQUEST_BODY_BYTES=.*$/m, "MAX_REQUEST_BODY_BYTES=16777216"));
    });
    const { dir: body } = release(w, "rc-4", { tree: bigger });
    const was = readFileSync(join(w.tree.tree, "deploy/profiles/shared-cell.env"), "utf8").match(/^MAX_REQUEST_BODY_BYTES=(.*)$/m)?.[1];
    refusedBeforeDeploy(w, await deploy(w, "production", body, withCell(w)),
      `release rc-4 sets MAX_REQUEST_BODY_BYTES 16777216, the running cell's release rc-1 ${was}: the cell's namespaces ` +
        "stored their body ceiling at creation and the binary refuses another");
    expect(w.cell.requests.filter((q) => q.startsWith("GET /v1/debug/load")).length).toBe(3);
  }, 60_000);

  test("a plain deploy of the stage's last release keeps the nonce the log records for it, so a re-run is the same input", async () => {
    const w = await world();
    const { dir: d1, release: r1 } = release(w, "rc-1");
    const line = (stage: string, tag: string, nonce: string, result: string) =>
      JSON.stringify({ app: "cell", stage, project: w.project, branch: null, tag, nonce, server_sha256: r1.server_sha256, at: "t", result });
    writeFileSync(join(w.releases, "deploy-log.jsonl"), `${[
      line("production", "rc-1", "rc-1.r1760000000", "pass"),
      line("production", "rc-1", "rc-1.r1760000999", "deploy_failed"),
    ].join("\n")}\n`);
    expect((await deploy(w, "production", d1)).code).toBe(3);
    const { dir: d2 } = release(w, "rc-2");
    expect((await deploy(w, "production", d2)).code).toBe(3);
    expect((await deploy(w, "production", d1)).code).toBe(3);
    expect([prismaRuns(w).map((p) => p.env.CELL_DEPLOY_NONCE), log(w).slice(2).map((e) => [e.tag, e.nonce])]).toEqual([
      ["rc-1.r1760000000", "rc-2", "rc-1"],
      [["rc-1", "rc-1.r1760000000"], ["rc-2", "rc-2"], ["rc-1", "rc-1"]],
    ]);
  }, 60_000);

  test("--restart redeploys the running release with a new nonce and passes only a new instance; a plain re-run then keeps it", async () => {
    const w = await world();
    const { dir, release: r1 } = release(w, "rc-1");
    runs(w, r1);
    const run = await deploy(w, "production", dir, [...withCell(w), "--restart"]);
    expect([run.code, run.err]).toEqual([0, ""]);
    const nonce = prismaRuns(w)[0].env.CELL_DEPLOY_NONCE;
    expect([/^rc-1\.r[0-9]{10}$/.test(nonce), w.cell.identity.boot, w.cell.nonce]).toEqual([true, "b1", nonce]);
    expect(run.out).toContain(`deploy-release: rc-1 is restarted on production (nonce ${nonce}): a new instance passed cell-check`);
    expect(log(w).map((e) => [e.tag, e.nonce, e.result])).toEqual([["rc-1", nonce, "pass"]]);
    const again = await deploy(w, "production", dir, withCell(w));
    expect([again.code, again.err, prismaRuns(w)[1].env.CELL_DEPLOY_NONCE, w.cell.identity.boot, w.cell.boots]).toEqual([0, "", nonce, "b1", 1]);
  }, 60_000);

  test("--restart refuses a cell that runs another release or none, and goes only with --cell-url, never with the issuer or --destroy", async () => {
    const w = await world();
    const { release: r1 } = release(w, "rc-1");
    const { dir: d2, release: r2 } = release(w, "rc-2");
    runs(w, r1);
    refusedBeforeDeploy(w, await deploy(w, "production", d2, [...withCell(w), "--restart"]),
      `--restart restarts the release the cell runs: it runs rc-1 (${r1.server_sha256}), not rc-2 (${r2.server_sha256}); ` +
        "deploy rc-2 without --restart");
    w.cell.healthStatus = 503;
    refusedBeforeDeploy(w, await deploy(w, "production", d2, [...withCell(w), "--restart"]),
      `--restart restarts the cell that answers at ${w.cell.url}, and none answers`);
    for (const extra of [["--restart"], [...withCell(w), "--restart", "--issuer-release-dir", d2, "--issuer-project", "p"]]) {
      const usage = await deploy(w, "production", d2, extra);
      expect([usage.code, usage.err.split("\n")[0], prismaRuns(w).length]).toEqual([
        2, "deploy-release: --restart restarts the running cell: it needs --cell-url (with cell-check's files) and takes no --issuer-release-dir or --destroy", 0,
      ]);
    }
  }, 60_000);

  test("when no cell answers, checks against the release the deploy log last deployed to the stage", async () => {
    const w = await world();
    const { release: r1 } = release(w, "rc-1");
    writeFileSync(join(w.releases, "deploy-log.jsonl"), `${JSON.stringify({
      stage: "production", project: w.project, branch: null, tag: "rc-1", server_sha256: r1.server_sha256, at: "t", result: "fail",
    })}\n`);
    const { dir } = release(w, "rc-2");
    const path = join(dir, "deploy", "release.json");
    writeFileSync(path, JSON.stringify({ ...JSON.parse(readFileSync(path, "utf8")), layout_version: 6 }));
    const r = await deploy(w, "production", dir);
    expect([r.code, r.err.trim(), prismaRuns(w).length]).toEqual([
      1, "deploy-release: release rc-2 has layout_version 6, the running cell's release rc-1 5: a deploy never crosses a layout", 0,
    ]);
    expect(r.out).toContain("deploy-release: no cell answers; deploy-log.jsonl last deployed release rc-1 to production");
  }, 30_000);

  test("refuses a running binary that no release beside this one holds", async () => {
    const w = await world();
    w.cell.identity = { ...w.cell.identity, commit: "f".repeat(40), sha: "e".repeat(64) };
    const { dir } = release(w, "rc-1");
    refusedBeforeDeploy(w, await deploy(w, "production", dir, withCell(w)),
      `the cell runs binary ${"e".repeat(64)} (${"f".repeat(40)}), which no release directory in ${w.releases} holds: ` +
        "its layout, keys contract and body ceiling cannot be checked");
  }, 30_000);

  test("a named stage deploys with --stage, and needs its Branch's own secrets, never branchless ones", async () => {
    const w = await world();
    const branch = w.api.addBranch(w.project, "cert").id;
    for (const n of NAMES) w.api.addVariable({ projectId: w.project, branchId: branch, class: "preview", key: n }, `${n}-c`);
    const { dir } = release(w, "rc-1");
    const run = await deploy(w, "cert", dir, ["--branch", branch]);
    expect([run.code, run.err]).toEqual([3, ""]);
    expect([prismaRuns(w)[0].argv, prismaRuns(w)[0].env.CELL_STAGE, log(w)[0].branch]).toEqual([
      ["deploy", "deploy/composer/app.ts", "--stage", "cert"], "cert", branch,
    ]);
    w.api.addVariable({ projectId: w.project, branchId: null, class: "preview", key: "STREAMS_CURSOR_KEY" }, "every-stage");
    const shared = await deploy(w, "cert", dir, ["--branch", branch]);
    expect([shared.code, shared.err.trim(), prismaRuns(w).length]).toEqual([
      1, `deploy-release: project ${w.project} holds branchless preview STREAMS_CURSOR_KEY, which every named stage sees: delete them`, 1,
    ]);
  }, 30_000);

  test("--destroy never takes the default stage: refused before it reads anything", async () => {
    const w = await world();
    const { dir } = release(w, "rc-1");
    const r = await deploy(w, "production", dir, ["--destroy"]);
    refusedBeforeDeploy(w, r,
      "--destroy takes a named stage (scratch or cert, deployed with --stage); the default stage, production, is never destroyed");
    expect(w.api.requests).toEqual([]);
  }, 30_000);

  test("--destroy of a named stage runs the release's destroy.ts in the narrow environment, and the stage has no cell after", async () => {
    const w = await world();
    const branch = w.api.addBranch(w.project, "cert").id;
    for (const n of NAMES) w.api.addVariable({ projectId: w.project, branchId: branch, class: "preview", key: n }, `${n}-c`);
    const { dir } = release(w, "rc-1");
    expect((await deploy(w, "cert", dir, ["--branch", branch])).code).toBe(3);
    const gone = await deploy(w, "cert", dir, ["--branch", branch, "--destroy"]);
    expect([gone.code, gone.err]).toEqual([0, ""]);
    expect(gone.out).toContain("deploy-release: stage cert is destroyed");
    const d = prismaRuns(w)[1];
    expect([d.argv, realpathSync(d.cwd), Object.keys(d.env).sort(), d.env.PRISMA_SERVICE_TOKEN]).toEqual([
      ["destroy", "cert"], realpathSync(dir), ["HOME", "PATH", "PRISMA_SERVICE_TOKEN", "PRISMA_WORKSPACE_ID"], w.api.token,
    ]);
    expect(log(w).map((e) => [e.stage, e.tag, e.result])).toEqual([["cert", "rc-1", "unchecked"], ["cert", "rc-1", "destroyed"]]);
    const { dir: next } = release(w, "rc-2");
    const path = join(next, "deploy", "release.json");
    writeFileSync(path, JSON.stringify({ ...JSON.parse(readFileSync(path, "utf8")), layout_version: 6 }));
    const fresh = await deploy(w, "cert", next, ["--branch", branch]);
    expect([fresh.code, fresh.err]).toEqual([3, ""]);
    expect(fresh.out).toContain("deploy-release: no cell yet: nothing answers for cert and the deploy log holds no live deploy of it");
  }, 30_000);

  /// The issuer's project holding its nine secrets, and its release of
  /// `tag` from `tree` with production's settings.
  function issuer(w: World, tag: string, o: { tree?: World["tree"]; settings?: string } = {}) {
    const project = w.api.projects.find((p) => p.name === "streams-issuer") ?? w.api.addProject("streams-issuer", "eu-central-1");
    if (!w.api.variables.some((v) => v.projectId === project.id)) {
      for (const n of ISSUER_NAMES) w.api.addVariable({ projectId: project.id, branchId: null, class: "production", key: n }, `${n}-i`);
    }
    const { dir } = stageIssuer({
      ...(o.tree ?? w.tree), tag, releases: w.releases, settings: { production: o.settings ?? issuerSettingsText() },
      prismaScript: prismaScript(w.record, null),
    });
    return { dir, project: project.id, args: ["--issuer-release-dir", dir, "--issuer-project", project.id] };
  }

  test("--issuer-release-dir deploys the issuer after the cell passed cell-check, from its own root, its settings as envParams", async () => {
    const w = await world();
    runs(w, release(w, "rc-1").release);
    const { dir } = release(w, "rc-2");
    const i = issuer(w, "rc-2");
    const run = await deploy(w, "production", dir, [...withCell(w), ...i.args]);
    expect([run.code, run.err]).toEqual([0, ""]);
    expect(run.out).toContain("deploy-release: rc-2 is deployed to production and passed cell-check, and its issuer after it");
    const [cell, iss] = prismaRuns(w);
    expect(cell.argv).toEqual(["deploy", "deploy/composer/app.ts"]);
    const settings = Object.fromEntries(issuerSettingsText().trim().split("\n").map((l) => l.split("=")));
    expect([iss.argv, realpathSync(iss.cwd), iss.env]).toEqual([
      ["deploy", "app.ts"], realpathSync(i.dir),
      {
        PATH: process.env.PATH, HOME: process.env.HOME, PRISMA_WORKSPACE_ID: "wksp_1", PRISMA_SERVICE_TOKEN: w.api.token,
        PRISMA_REGION: "eu-central-1", ...settings,
      },
    ]);
    expect(log(w).map((e) => [e.app, e.project, e.tag, e.result])).toEqual([
      ["cell", w.project, "rc-2", "pass"], ["issuer", i.project, "rc-2", "unchecked"],
    ]);
  }, 60_000);

  test("refuses, before the cell deploys: the issuer cut from another commit, settings naming another cell, an issuer secret unset or in the shell", async () => {
    const w = await world();
    const { dir } = release(w, "rc-1");
    const other = issuer(w, "rc-1", { tree: fakeTree((t) => writeFileSync(join(t, "deploy/composer/streams/env.ts"), "// another commit\n")) });
    refusedBeforeDeploy(w, await deploy(w, "production", dir, other.args),
      `the issuer's release ${other.dir} is cut from ${JSON.parse(readFileSync(join(other.dir, "release.json"), "utf8")).commit}, ` +
        `the cell's from ${w.tree.commit}: both deploy from one commit`);
    const w2 = await world();
    const { dir: dir2 } = release(w2, "rc-1");
    const named = issuer(w2, "rc-1", { settings: issuerSettingsText("cell_other") });
    refusedBeforeDeploy(w2, await deploy(w2, "production", dir2, named.args),
      `${join(named.dir, "stages", "production.env")} names STREAMS_CELL_ID=cell_other; the cell is cell_sc1`);
    const w3 = await world();
    const { dir: dir3 } = release(w3, "rc-1");
    const unset = issuer(w3, "rc-1");
    w3.api.variables = w3.api.variables.filter((v) => !(v.projectId === unset.project && v.key === "STREAMS_ISSUER_PEPPER"));
    refusedBeforeDeploy(w3, await deploy(w3, "production", dir3, unset.args),
      `project ${unset.project}'s production variables holds no STREAMS_ISSUER_PEPPER: set them with set-secrets first`);
    refusedBeforeDeploy(w3, await deploy(w3, "production", dir3, unset.args, { STREAMS_ISSUER_SIGNING_KEY: "pem" }),
      "STREAMS_ISSUER_SIGNING_KEY is set in this shell's environment: a secret reaches the platform through set-secrets only " +
        "(Composer's preflight would push it from here); unset it");
  }, 60_000);

  /// The world's cell watch: its project holding its six secrets in the
  /// production class, and its release <tag>-watch beside the cell's.
  function watch(w: World, tag: string, o: { settings?: string; tree?: World["tree"] } = {}) {
    const project = w.api.addProject("streams-cell-watch", "eu-central-1");
    for (const n of WATCH_NAMES) w.api.addVariable({ projectId: project.id, branchId: null, class: "production", key: n }, `${n}-v`);
    const { dir } = stageWatch({
      ...(o.tree ?? w.tree), tag, releases: w.releases, settings: { production: o.settings ?? watchSettingsText(w.tree.commit) },
      prismaScript: prismaScript(w.record, null),
    });
    return { dir, project: project.id, args: ["--app", "cell-watch", "--project", project.id, "--workspace", "wksp_1"] };
  }

  /// deploy-release.sh for the watch: its own --project, no cell flags.
  async function deployWatch(w: World, dir: string, args: string[], env: Record<string, string> = {}) {
    const proc = Bun.spawn(["bash", SCRIPT, "production", dir, ...args, "--management-api", w.api.url], {
      env: { PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: process.env.HOME ?? "/", PRISMA_TOKEN_FILE: w.tokenFile, ...env },
      stdout: "pipe",
      stderr: "pipe",
    });
    const [out, err, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
    return { code, out, err };
  }

  test("--app cell-watch deploys the watch's release on its own, from its root, its settings as envParams, in the cell's region", async () => {
    const w = await world();
    const cell = release(w, "rc-1").release;
    const v = watch(w, "rc-1");
    const run = await deployWatch(w, v.dir, v.args);
    expect([run.code, run.err]).toEqual([0, ""]);
    expect(run.out.trim().split("\n")).toEqual([
      `deploy-release: prisma deploy app.ts (the cell watch of rc-1, project ${v.project}'s production variables, eu-central-1)`,
      "deploy-release: the cell watch of rc-1 is deployed to production; its check is its heartbeat reaching HEARTBEAT_URL " +
        "within 3 minutes (row 2)",
    ]);
    const settings = Object.fromEntries(watchSettingsText(w.tree.commit).trim().split("\n").map((l) => l.split("=")));
    const [only, ...more] = prismaRuns(w);
    expect([more.length, only.argv, realpathSync(only.cwd), only.env]).toEqual([
      0, ["deploy", "app.ts"], realpathSync(v.dir),
      {
        PATH: process.env.PATH, HOME: process.env.HOME, PRISMA_WORKSPACE_ID: "wksp_1", PRISMA_SERVICE_TOKEN: w.api.token,
        PRISMA_REGION: "eu-central-1", ...settings,
      },
    ]);
    expect(log(w).map((e) => [e.app, e.stage, e.project, e.branch, e.tag, e.server_sha256, e.result])).toEqual([
      ["cell-watch", "production", v.project, null, "rc-1", cell.server_sha256, "unchecked"],
    ]);
  }, 30_000);

  test("--app cell-watch refuses before it deploys: no cell release beside it, settings naming another cell, a secret unset or in the shell", async () => {
    const w = await world();
    const alone = watch(w, "rc-9");
    refusedBeforeDeploy(w, await deployWatch(w, alone.dir, alone.args),
      `no cell release ${join(w.releases, "rc-9")} beside the watch's: the watch deploys beside the cell release of its cut`);
    const w2 = await world();
    release(w2, "rc-1");
    const named = watch(w2, "rc-1", { settings: watchSettingsText(w2.tree.commit, "cell_other") });
    refusedBeforeDeploy(w2, await deployWatch(w2, named.dir, named.args),
      `${join(named.dir, "stages", "production.env")} names STREAMS_CELL_ID=cell_other; the cell is cell_sc1`);
    const w3 = await world();
    release(w3, "rc-1");
    const unset = watch(w3, "rc-1");
    w3.api.variables = w3.api.variables.filter((x) => !(x.projectId === unset.project && x.key === "HEARTBEAT_URL"));
    refusedBeforeDeploy(w3, await deployWatch(w3, unset.dir, unset.args),
      `project ${unset.project}'s production variables holds no HEARTBEAT_URL: set them with set-secrets first`);
    refusedBeforeDeploy(w3, await deployWatch(w3, unset.dir, unset.args, { ALERT_SINK_KEY: "k" }),
      "ALERT_SINK_KEY is set in this shell's environment: a secret reaches the platform through set-secrets only " +
        "(Composer's preflight would push it from here); unset it");
    const other = await deployWatch(w3, unset.dir, ["--app", "issuer", "--project", unset.project, "--workspace", "wksp_1"]);
    const destroy = await deployWatch(w3, unset.dir, [...unset.args, "--destroy"]);
    expect([other, destroy].map((x) => [x.code, x.err.split("\n")[0]])).toEqual([
      [2, "deploy-release: --app issuer is cell-watch or pager-standin (the cell deploys without --app)"],
      [2, "deploy-release: --app takes no --destroy: delete the app's Branch (or its project) by hand"],
    ]);
    expect(prismaRuns(w3)).toEqual([]);
  }, 60_000);

  test("--app pager-standin deploys the paging stand-in's app from the watch's release, on a named stage only, with the cell's origin", async () => {
    const w = await world();
    const cell = release(w, "rc-1", { cells: { production: cellsText("production"), scratch: cellsText("scratch") } }).release;
    const project = w.api.addProject("streams-pager-standin", "eu-central-1");
    w.api.addBranch(project.id, "main");
    const b = w.api.addBranch(project.id, "scratch");
    for (const n of ["PAGER_ROUTING_KEY", "PAGER_HEARTBEAT_TOKEN"]) {
      w.api.addVariable({ projectId: project.id, branchId: b.id, class: "preview", key: n }, `${n}-v`);
    }
    const settings = watchSettingsText(w.tree.commit);
    const { dir } = stageWatch({
      ...w.tree, tag: "rc-1", releases: w.releases, settings: { production: settings, scratch: settings },
      prismaScript: prismaScript(w.record, null),
    });
    const args = ["--app", "pager-standin", "--project", project.id, "--workspace", "wksp_1", "--management-api", w.api.url];
    const spawn = async (stageName: string, more: string[]) => {
      const proc = Bun.spawn(["bash", SCRIPT, stageName, dir, ...args, ...more], {
        env: { PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: process.env.HOME ?? "/", PRISMA_TOKEN_FILE: w.tokenFile },
        stdout: "pipe", stderr: "pipe",
      });
      const [out, err, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
      return { code, out, err };
    };
    refusedBeforeDeploy(w, await spawn("production", []),
      "the paging stand-in deploys to scratch or cert only: production pages through the real paging service (RUNBOOK §7.5)");
    const run = await spawn("scratch", ["--branch", b.id]);
    expect([run.code, run.err]).toEqual([0, ""]);
    const [only] = prismaRuns(w);
    expect([only.argv, realpathSync(only.cwd), only.env]).toEqual([
      ["deploy", "pager-app.ts", "--stage", "scratch"], realpathSync(dir),
      {
        PATH: process.env.PATH, HOME: process.env.HOME, PRISMA_WORKSPACE_ID: "wksp_1", PRISMA_SERVICE_TOKEN: w.api.token,
        PRISMA_REGION: "eu-central-1", STREAMS_CELL_ORIGIN: "https://cell.example",
      },
    ]);
    expect(run.out).toContain("deploy-release: the paging stand-in of rc-1 is deployed to scratch; give the watch its");
    expect(log(w).map((e) => [e.app, e.stage, e.branch, e.server_sha256, e.result])).toEqual([
      ["pager-standin", "scratch", b.id, cell.server_sha256, "unchecked"],
    ]);
  }, 30_000);

  test("a failed CLI run is logged and refused; --cell-url without cell-check's files is usage", async () => {
    const w = await world();
    const { dir } = release(w, "rc-1", { prismaCode: 1 });
    const failed = await deploy(w, "production", dir);
    expect([failed.code, failed.err.trim(), log(w).map((e) => e.result)]).toEqual([
      1, "deploy-release: prisma deploy failed: release rc-1 did not reach production", ["deploy_failed"],
    ]);
    const usage = await deploy(w, "production", dir, ["--cell-url", w.cell.url]);
    expect([usage.code, usage.err.split("\n")[0]]).toEqual([
      2, "deploy-release: --cell-url goes with --bearer-file and --canary-token-file (cell-check needs both)",
    ]);
  }, 30_000);
});
