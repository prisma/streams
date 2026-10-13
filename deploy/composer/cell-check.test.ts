// cell-check (public-beta deploy-q5 triage B5): a cell passes only when
// it runs exactly the release (/health's commit, /v1/debug/load's sha256
// and commit), serves the bundle cell-admin published for it, and takes
// the canary's write, read and usage round trip; it fails, with the
// reason, when the deadline passes first. Offline, against a fake cell
// (fake-cell.ts) and a cell-admin state in a directory. Run with
// `bun test ./deploy/composer`.
import { afterAll, describe, expect, test } from "bun:test";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { cleanup, initCell, readBundle, scratchDir } from "../cell-admin/test-rig";
import { FakeStreamsCell, type Identity } from "./fake-cell";

const TOOL = join(import.meta.dir, "cell-check");
const COMMIT = "c0ffee".padEnd(40, "0");
const SHA = "ab".repeat(32);
const CANARY = "sp_canary_cell_sc1";

const cells: FakeStreamsCell[] = [];
afterAll(() => {
  for (const c of cells.splice(0)) c.stop();
  cleanup();
});

/// A release directory as far as cell-check reads one: release.json and
/// the stage's cells file.
function fakeRelease(root: string, stage = "production", cellId = "cell_sc1"): string {
  const dir = join(root, "release");
  mkdirSync(join(dir, "deploy", "cells"), { recursive: true });
  writeFileSync(
    join(dir, "deploy", "release.json"),
    JSON.stringify({ tag: "rc-1", commit: COMMIT, server_sha256: SHA, cells: { [stage]: "x" }, files: {} }),
  );
  writeFileSync(join(dir, "deploy", "cells", `${stage}.env`), `CELL_STAGE=${stage}\nCELL_ID=${cellId}\n`);
  return dir;
}

const jwt = (claims: object) =>
  ["{}", JSON.stringify(claims), "sig"].map((p) => Buffer.from(p).toString("base64url")).join(".");

async function rig(identity: Partial<Identity> = {}, claims: object = { project_id: CANARY, cell_id: "cell_sc1" }) {
  const c = await initCell();
  const v = readBundle(c.state).policies.feed_version;
  const bearer = `bearer-${Math.random().toString(36).slice(2)}`;
  const canary = jwt(claims);
  const cell = new FakeStreamsCell({ commit: COMMIT, sha: SHA, policies: v, grants: v, ...identity }, bearer, canary, CANARY);
  cells.push(cell);
  const files = scratchDir();
  writeFileSync(join(files, "bearer"), bearer, { mode: 0o600 });
  writeFileSync(join(files, "canary"), `${canary}\n`, { mode: 0o600 });
  const args = [
    "production", fakeRelease(c.root), "--cell-url", cell.url, "--state", c.state,
    "--bearer-file", join(files, "bearer"), "--canary-token-file", join(files, "canary"),
  ];
  return { c, cell, bearer, canary, args, v };
}

async function check(args: string[]) {
  const proc = Bun.spawn([process.execPath, TOOL, ...args], { stdout: "pipe", stderr: "pipe" });
  const [out, err, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited]);
  return { code, out, err };
}

describe("cell-check", () => {
  test("passes a cell that runs the release, serves the published feeds and takes the canary round trip", async () => {
    const r = await rig();
    r.cell.usageLag = 2;
    const run = await check(r.args);
    expect([run.code, run.err]).toEqual([0, ""]);
    expect(run.out.trim().split("\n")).toEqual([
      `cell-check: production at ${r.cell.url} must run ${SHA} (${COMMIT}) and serve feeds ${r.v}/${r.v}`,
      "CELL_CHECK_PASS production: identity, feeds and the canary round trip",
    ]);
    const usage = `GET /v1/projects/${CANARY}/usage`;
    expect(r.cell.requests).toEqual([
      "GET /health",
      "GET /v1/debug/load",
      "GET /v1/debug/auth",
      "DELETE /v1/streams/cell-check",
      "PUT /v1/streams/cell-check",
      usage,
      "POST /v1/streams/cell-check/records",
      "GET /v1/streams/cell-check/records?cursor=beginning",
      usage,
      usage,
      usage,
      "DELETE /v1/streams/cell-check",
    ]);
    expect([r.cell.streams.size, r.cell.ingest > 0]).toEqual([0, true]);
    expect([run.out.includes(r.bearer), run.out.includes(r.canary)]).toEqual([false, false]);
  }, 30_000);

  test("--not-boot-id passes only an instance with another boot id, waiting while the named one answers", async () => {
    const r = await rig({ boot: "b7" });
    const stuck = await check([...r.args, "--not-boot-id", "b7", "--deadline-secs", "4"]);
    expect([stuck.code, stuck.err, stuck.out.trim().split("\n").at(-1)]).toEqual([
      1, "", "CELL_CHECK_FAIL production: /health answers from boot b7, the instance --not-boot-id excludes: no new instance yet",
    ]);
    setTimeout(() => {
      r.cell.identity = { ...r.cell.identity, boot: "b8" };
    }, 1_000);
    const moved = await check([...r.args, "--not-boot-id", "b7", "--deadline-secs", "30"]);
    expect([moved.code, moved.err]).toEqual([0, ""]);
    expect(moved.out).toContain("CELL_CHECK_PASS production");
  }, 60_000);

  test("waits while the old version still answers, then passes the new one", async () => {
    const r = await rig();
    r.cell.previous = { identity: { ...r.cell.identity, commit: "0ld".padEnd(40, "0") }, until: Date.now() + 1_000 };
    const run = await check([...r.args, "--deadline-secs", "30"]);
    expect([run.code, run.err]).toEqual([0, ""]);
    expect(run.out).toContain(`cell-check: not yet: /health names commit ${"0ld".padEnd(40, "0")}, the release is ${COMMIT}`);
    expect(run.out).toContain("CELL_CHECK_PASS production");
  }, 30_000);

  test("fails at the deadline a cell that runs another binary, or serves older feeds, or is not up", async () => {
    const other = await rig({ sha: "cd".repeat(32) });
    expect(await check([...other.args, "--deadline-secs", "1"])).toMatchObject({
      code: 1,
      err: "",
      out: expect.stringContaining(
        `CELL_CHECK_FAIL production: /v1/debug/load reports binary ${"cd".repeat(32)} at ${COMMIT}, the release is ${SHA} at ${COMMIT}\n`,
      ),
    });
    const behind = await rig();
    behind.cell.identity.grants = behind.v - 1;
    expect((await check([...behind.args, "--deadline-secs", "1"])).out).toContain(
      `CELL_CHECK_FAIL production: the cell serves policies ${behind.v} and grants ${behind.v - 1}, the published bundle is at ${behind.v} and ${behind.v}\n`,
    );
    const down = await rig();
    down.cell.healthStatus = 500;
    expect((await check([...down.args, "--deadline-secs", "1"])).out).toContain(
      "CELL_CHECK_FAIL production: GET /health answered 500 booting\n",
    );
    expect([...other.cell.requests, ...behind.cell.requests].some((q) => q.includes("/v1/streams/"))).toBe(false);
  }, 30_000);

  test("fails at once on a refusal the cell marks not retryable, and retries one it marks retryable", async () => {
    const full = await rig();
    full.cell.maxStreams = 1;
    full.cell.streams.set("watch", { key: "k", records: [] });
    const started = Date.now();
    const refused = await check([...full.args, "--deadline-secs", "20"]);
    const took = Date.now() - started;
    const body = JSON.stringify({ error: { code: "stream_limit", message: "stream_limit", retryable: false } });
    expect([refused.code, refused.err]).toEqual([1, ""]);
    expect(refused.out).toContain(
      `CELL_CHECK_FAIL production: PUT /v1/streams/cell-check answered 429 ${body}: the cell marks it not retryable, so no retry passes it\n`,
    );
    expect([full.cell.requests.filter((q) => q.startsWith("PUT ")), took < 10_000]).toEqual([["PUT /v1/streams/cell-check"], true]);
    const limited = await rig();
    limited.cell.rateLimitOnce = "PUT /v1/streams/cell-check";
    const passed = await check(limited.args);
    expect([passed.code, passed.err]).toEqual([0, ""]);
    expect(passed.out).toContain("CELL_CHECK_PASS production");
    expect(limited.cell.requests.filter((q) => q.startsWith("PUT "))).toEqual(["PUT /v1/streams/cell-check", "PUT /v1/streams/cell-check"]);
  }, 60_000);

  test("refuses a canary token of another project, and a state of another cell, before it asks the cell anything", async () => {
    const r = await rig({}, { project_id: "proj_customer", cell_id: "cell_sc1" });
    expect(await check(r.args)).toEqual({
      code: 1, out: "",
      err: `cell-check: the canary token is for project proj_customer on cell_sc1, not ${CANARY} on cell_sc1\n`,
    });
    const s = await rig();
    const args = [...s.args];
    args[1] = fakeRelease(scratchDir(), "production", "cell_other");
    expect(await check(args)).toEqual({
      code: 1, out: "", err: "cell-check: --state holds cell cell_sc1; " +
        `${join(args[1], "deploy", "cells", "production.env")} names cell_other\n`,
    });
    expect([r.cell.requests, s.cell.requests]).toEqual([[], []]);
  }, 30_000);
});
