// deploy-release.sh (public-beta deploy-q5 triage B5, design §2.3): the
// self-tests of every refusal before a deploy (a secret in the deploy
// shell's environment, a dotfile in the release, a region mismatch, a
// missing secret name, KEEP_AWAKE_UNTIL_MS in production's cells file,
// each of the three crossings against the running cell's release, from
// the cell's /v1/debug/load or the deploy log), and of the deploy itself:
// "no cell yet" accepted, the release's own CLI in an environment of six
// names, cell-check after, one log line per deploy. Offline: a fake
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
import { cellsText, cleanupScratch, fakeTree, scratchDir, stage } from "./release-rig";

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
const r = JSON.parse(readFileSync("deploy/release.json", "utf8"));
${cellUrl ? `await fetch(${JSON.stringify(`${cellUrl}/__deploy`)}, { method: "POST", body: JSON.stringify({ commit: r.commit, sha: r.server_sha256 }) });` : ""}
process.exit(${code});
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
  return { root, releases, api, project, tokenFile, record: join(root, "prisma-runs"), state: c.state, cell, files, tree: fakeTree() };
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

/// The cell runs `r`.
const runs = (w: World, r: Release) => {
  w.cell.identity = { ...w.cell.identity, commit: r.commit, sha: r.server_sha256 };
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
  test("no cell yet: deploys with the release's own CLI, from its root, in an environment of six names, and logs it unchecked", async () => {
    const w = await world();
    const { dir, release: r } = release(w, "rc-1");
    const run = await deploy(w, "production", dir);
    expect([run.code, run.err]).toEqual([3, ""]);
    expect(run.out).toContain("deploy-release: no cell yet: nothing answers for production and no deploy of it is logged");
    expect(run.out).toContain("deploy-release: cell cell_sc1: the profiles give its fixed sizing");
    const [p] = prismaRuns(w);
    expect([p.argv, realpathSync(p.cwd), Object.keys(p.env).sort()]).toEqual([
      ["deploy", "deploy/composer/app.ts"],
      realpathSync(dir),
      ["CELL_STAGE", "HOME", "PATH", "PRISMA_REGION", "PRISMA_SERVICE_TOKEN", "PRISMA_WORKSPACE_ID"],
    ]);
    expect([p.env.CELL_STAGE, p.env.PRISMA_REGION, p.env.PRISMA_WORKSPACE_ID, p.env.PRISMA_SERVICE_TOKEN]).toEqual([
      "production", "eu-central-1", "wksp_1", w.api.token,
    ]);
    const [entry] = log(w);
    expect({ ...entry, at: typeof entry.at }).toEqual({
      stage: "production", project: w.project, branch: null, tag: "rc-1", server_sha256: r.server_sha256, at: "string",
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

  test("refuses a secret set in the deploy shell's environment, before it reads the release or asks the platform", async () => {
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
