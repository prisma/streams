// deploy-release.sh's work (public-beta deploy-q5 triage B5, design §2.3):
// the only way a release reaches a shared cell, and the way back (a
// rollback is this command with the previous release directory). Its
// header in deploy-release.sh is the operator's reference; the steps:
//
//   1. no secret the cell's app binds in this process's environment (D10:
//      Composer's preflight would push one it finds there; set-secrets is
//      the only path);
//   2. the release is exactly what was cut (release.ts's rules);
//   3. the stage's cells file is in the release; production's carries no
//      KEEP_AWAKE_UNTIL_MS; the entry's own env builder accepts the
//      profiles and the cells file (so a refusal is met here, not as a
//      crash-looping deploy);
//   4. the stage's project is in the cells file's REGION;
//   5. every secret name the app binds is set in the stage's own scope,
//      and a named stage's names are its Branch's overrides (D0 finding
//      10: a branchless preview variable reaches every named stage);
//   6. the running release: the one whose server_sha256 the running cell
//      reports on /v1/debug/load, found among the releases beside this
//      one; when no cell answers, the last release deploy-log.jsonl
//      records as deployed to the stage; neither is "no cell yet",
//      accepted (S27). Refused: a release whose layout_version,
//      keys_contract or MAX_REQUEST_BODY_BYTES differs from the running
//      one's (the body ceiling first, though check-sizing would refuse a
//      changed profile too, so each refusal is met as itself);
//   7. `cell-admin check-sizing` accepts the release's profiles for the
//      cell's state (D9: a cell's ceilings are fixed for its life);
//   8. `prisma deploy deploy/composer/app.ts [--stage <stage>]` with the
//      release's own pinned CLI, from the release root, in an environment
//      of PATH, HOME, CELL_STAGE, PRISMA_REGION, PRISMA_WORKSPACE_ID and
//      PRISMA_SERVICE_TOKEN only;
//   9. cell-check, within 5 minutes, when --cell-url names the cell;
//  10. one line appended to deploy-log.jsonl beside the releases:
//      {stage, project, branch, tag, server_sha256, at, result}.
// With --destroy, after steps 1 and 2 (and never for production): the
// release's destroy.ts, Composer's destroy of the named stage, from the
// release root in the same narrow environment, logged as destroyed.
import { appendFileSync, existsSync, readdirSync, readFileSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { type Flags, parseFlags } from "../cell-admin/args";
import { main as cellAdmin } from "../cell-admin/cell-admin";
import { cellBase } from "../cell-admin/cell-client";
import { AdminError } from "../cell-admin/errors";
import { parseProfiles } from "../cell-admin/profile";
import {
  describeScope,
  envSecretNames,
  inScope,
  MANAGEMENT_API,
  ownerOnlyFile,
  Platform,
  type Scope,
  scopeOf,
  scopeQuery,
} from "./platform";
import { checkRelease, type Release, readRelease } from "./release";
import { buildChildEnv, PROFILES, STAGES } from "./streams/env";

const USAGE = `usage: PRISMA_TOKEN_FILE=F deploy/composer/deploy-release.sh <stage> <release-dir>
  --project ID [--branch ID] --workspace ID --state DIR|s3://... [--s3-credentials F]
  [--cell-url URL --bearer-file F --canary-token-file T] [--management-api URL]
or: ... <scratch|cert> <release-dir> --project ID --branch ID --workspace ID --destroy`;
export const DEPLOY_LOG = "deploy-log.jsonl";
const CELL_CHECK_DEADLINE_SECS = 300;
const PROBE_TIMEOUT_MS = 15_000;
/// What a run did to the stage, as deploy-log.jsonl records it; the first
/// three mean the release was deployed, "destroyed" that no cell is left.
type Result = "pass" | "fail" | "unchecked" | "deploy_failed" | "destroyed" | "destroy_failed";
const DEPLOYED: readonly Result[] = ["pass", "fail", "unchecked"];

interface Run {
  stage: string;
  dir: string;
  release: Release;
  scope: Scope;
  flags: Flags;
  out: (line: string) => void;
}

const fileIn = (dir: string, path: string) => join(dir, "deploy", path);
const layered = (dir: string) =>
  parseProfiles(PROFILES.map((p) => ({ path: fileIn(dir, p), text: readFileSync(fileIn(dir, p), "utf8") })));

/// Step 1.
export function refuseSecretsInEnv(names: readonly string[], env: Readonly<Record<string, string | undefined>>): void {
  const set = names.filter((n) => env[n] !== undefined);
  if (set.length) {
    throw new AdminError(
      `${set.join(", ")} ${set.length === 1 ? "is" : "are"} set in this shell's environment: a secret reaches the platform ` +
        "through set-secrets only (Composer's preflight would push it from here); unset it",
    );
  }
}

/// Step 3: the stage's cells file, as the entry will read it.
function cellsFile(run: Run): Map<string, string> {
  const { stage, dir, release } = run;
  if (release.cells[stage] === undefined) throw new AdminError(`release ${release.tag} carries no cells/${stage}.env`);
  const path = fileIn(dir, `cells/${stage}.env`);
  const text = readFileSync(path, "utf8");
  const cells = parseProfiles([{ path, text }]);
  if (stage === "production" && cells.has("KEEP_AWAKE_UNTIL_MS")) {
    throw new AdminError(`${path} sets KEEP_AWAKE_UNTIL_MS: a production cell is kept awake for good`);
  }
  const placeholder = Object.fromEntries(envSecretNames(readFileSync(fileIn(dir, "composer/app.ts"), "utf8")).map((n) => [n, "x"]));
  const built = buildChildEnv({
    profiles: PROFILES.map((p) => ({ path: fileIn(dir, p), text: readFileSync(fileIn(dir, p), "utf8") })),
    cellEnv: { path, text },
    stage,
    platformEnv: placeholder,
    binarySha256: release.server_sha256,
    port: "8080",
  });
  if ("refused" in built) throw new AdminError(`the cell's entry would refuse this release on ${stage}: ${built.message}`);
  return cells;
}

/// Steps 4 and 5.
async function platformChecks(run: Run, api: Platform, region: string, names: readonly string[]): Promise<void> {
  const { scope } = run;
  const project = (await api.call("GET", `/projects/${encodeURIComponent(scope.project)}`)).body?.data;
  if (project?.defaultRegion !== region) {
    throw new AdminError(
      `project ${scope.project} is in ${project?.defaultRegion ?? "no reported region"}; the release's cells/${run.stage}.env names REGION=${region}`,
    );
  }
  const rows = await api.list(`/environment-variables?${scopeQuery(scope)}`);
  const missing = names.filter((n) => !rows.some((r) => r.key === n && inScope(scope, r)));
  if (missing.length) {
    throw new AdminError(`${describeScope(scope)} holds no ${missing.join(", ")}: set them with set-secrets first`);
  }
  if (scope.class === "preview") {
    const shared = (await api.list(`/environment-variables?projectId=${encodeURIComponent(scope.project)}&class=preview`))
      .filter((r) => names.includes(r.key) && (r.branchId === null || r.branchId === undefined))
      .map((r) => r.key);
    if (shared.length) {
      throw new AdminError(`project ${scope.project} holds branchless preview ${shared.join(", ")}, which every named stage sees: delete them`);
    }
  }
}

/// Step 7.
async function checkSizing(run: Run): Promise<void> {
  const lines: string[] = [];
  const io = { out: (l: string) => lines.push(l), err: (l: string) => lines.push(l) };
  const state = ["--state", run.flags.must("state")];
  const creds = run.flags.get("s3-credentials");
  const argv = ["check-sizing", ...state, ...(creds ? ["--s3-credentials", creds] : [])];
  const code = await cellAdmin([...argv, ...PROFILES.flatMap((p) => ["--profile", fileIn(run.dir, p)])], io);
  if (code !== 0) throw new AdminError(`the cell refuses the release's profiles: ${lines.join("; ")}`);
  for (const l of lines) run.out(`deploy-release: ${l}`);
}

/// Every release directory beside this one (a release lives in a
/// directory named by its tag, as cut-release places it).
function releasesBeside(dir: string): Release[] {
  const root = dirname(dir);
  return readdirSync(root)
    .filter((name) => !name.startsWith(".") && existsSync(join(root, name, "deploy", "release.json")))
    .map((name) => readRelease(join(root, name)))
    .filter((r) => existsSync(join(root, r.tag, "deploy", "release.json")));
}

/// The running cell's binary and commit, or null when no cell answers.
async function runningIdentity(run: Run): Promise<{ sha: string; commit: string } | null> {
  const url = run.flags.get("cell-url");
  if (url === undefined) return null;
  const base = cellBase(url);
  let health: Response;
  try {
    health = await fetch(`${base}/health`, { signal: AbortSignal.timeout(PROBE_TIMEOUT_MS) });
    await health.arrayBuffer();
  } catch {
    return null;
  }
  if (health.status !== 200 || !health.headers.get("x-streams-git")) return null;
  const bearer = ownerOnlyFile(run.flags.must("bearer-file"), "the bearer file");
  const res = await fetch(`${base}/v1/debug/load`, {
    headers: { authorization: `Bearer ${bearer}` },
    signal: AbortSignal.timeout(PROBE_TIMEOUT_MS),
  }).catch((e) => {
    throw new AdminError(`GET ${base}/v1/debug/load: ${(e as Error).message}`);
  });
  const load = res.status === 200 ? await res.json().catch(() => null) : null;
  if (typeof load?.binary_sha256 !== "string" || typeof load?.git_commit !== "string") {
    throw new AdminError(`the cell at ${base} answers /health but /v1/debug/load answered ${res.status} without its identity`);
  }
  return { sha: load.binary_sha256, commit: load.git_commit };
}

/// The release the deploy log last deployed to the stage's scope, unless
/// a destroy came after it.
function lastDeployed(run: Run): string | null {
  const log = join(dirname(run.dir), DEPLOY_LOG);
  if (!existsSync(log)) return null;
  const branch = run.scope.class === "preview" ? run.scope.branch : null;
  const mine = readFileSync(log, "utf8")
    .split("\n")
    .filter((l) => l.trim() !== "")
    .map((l) => JSON.parse(l))
    .filter((e) => e.stage === run.stage && e.project === run.scope.project && (e.branch ?? null) === branch)
    .filter((e) => DEPLOYED.includes(e.result) || e.result === "destroyed");
  const last = mine[mine.length - 1];
  return last && last.result !== "destroyed" ? last.tag : null;
}

/// Step 6: the running release, or null for "no cell yet".
async function runningRelease(run: Run): Promise<Release | null> {
  const live = await runningIdentity(run);
  if (live) {
    const found = releasesBeside(run.dir).filter((r) => r.server_sha256 === live.sha);
    if (found.length === 0) {
      throw new AdminError(
        `the cell runs binary ${live.sha} (${live.commit}), which no release directory in ${dirname(run.dir)} holds: ` +
          "its layout, keys contract and body ceiling cannot be checked",
      );
    }
    if (found[0].commit !== live.commit) {
      throw new AdminError(`the cell runs binary ${live.sha} at ${live.commit}; release ${found[0].tag} cut it from ${found[0].commit}`);
    }
    run.out(`deploy-release: the running cell is release ${found[0].tag} (${live.sha})`);
    return found[0];
  }
  const tag = lastDeployed(run);
  if (tag === null) {
    run.out(`deploy-release: no cell yet: nothing answers for ${run.stage} and the deploy log holds no live deploy of it`);
    return null;
  }
  run.out(`deploy-release: no cell answers; ${DEPLOY_LOG} last deployed release ${tag} to ${run.stage}`);
  return readRelease(join(dirname(run.dir), tag));
}

/// Step 6's refusals.
function refuseCrossing(run: Run, running: Release): void {
  const { release } = run;
  const was = `the running cell's release ${running.tag}`;
  if (release.layout_version !== running.layout_version) {
    throw new AdminError(
      `release ${release.tag} has layout_version ${release.layout_version}, ${was} ${running.layout_version}: ` +
        "a deploy never crosses a layout",
    );
  }
  if (release.keys_contract !== running.keys_contract) {
    throw new AdminError(
      `release ${release.tag} has keys contract ${release.keys_contract}, ${was} ${running.keys_contract}: ` +
        "the keys feed and the binary change together (RUNBOOK), never by a deploy alone",
    );
  }
  const body = (dir: string) => layered(dir).get("MAX_REQUEST_BODY_BYTES") ?? "(unset)";
  const [mine, theirs] = [body(run.dir), body(join(dirname(run.dir), running.tag))];
  if (mine !== theirs) {
    throw new AdminError(
      `release ${release.tag} sets MAX_REQUEST_BODY_BYTES ${mine}, ${was} ${theirs}: the cell's namespaces stored ` +
        "their body ceiling at creation and the binary refuses another",
    );
  }
}

/// Run `argv` from the release root in an environment of PATH, HOME, the
/// workspace, the token and `extra`, and nothing else.
async function fromRelease(run: Run, api: Platform, argv: string[], extra: Record<string, string>): Promise<boolean> {
  const child = Bun.spawn(argv, {
    cwd: run.dir,
    env: {
      PATH: process.env.PATH ?? "/usr/bin:/bin",
      HOME: process.env.HOME ?? "/",
      PRISMA_WORKSPACE_ID: run.flags.must("workspace"),
      PRISMA_SERVICE_TOKEN: api.bearer,
      ...extra,
    },
    stdout: "inherit",
    stderr: "inherit",
  });
  return (await child.exited) === 0;
}

/// Step 8: the release's own pinned CLI.
async function prismaDeploy(run: Run, api: Platform, region: string): Promise<boolean> {
  const cli = join(run.dir, "node_modules", ".bin", "prisma");
  if (!existsSync(cli)) throw new AdminError(`${cli} does not exist: the release carries its own pinned CLI`);
  const args = ["deploy", "deploy/composer/app.ts", ...(run.stage === "production" ? [] : ["--stage", run.stage])];
  run.out(`deploy-release: prisma ${args.join(" ")} (release ${run.release.tag}, ${describeScope(run.scope)}, ${region})`);
  return fromRelease(run, api, [cli, ...args], { CELL_STAGE: run.stage, PRISMA_REGION: region });
}

/// --destroy: Composer's destroy of a named stage (the release's
/// destroy.ts, D0 finding 3). The buckets stay.
async function destroyStage(run: Run, api: Platform): Promise<number> {
  run.out(`deploy-release: destroying stage ${run.stage} (${describeScope(run.scope)}); its buckets stay for provision-cell.py --teardown`);
  const destroyed = await fromRelease(run, api, [process.execPath, join(run.dir, "destroy.ts"), run.stage], {});
  logDeploy(run, destroyed ? "destroyed" : "destroy_failed");
  if (!destroyed) throw new AdminError(`the destroy of stage ${run.stage} failed`);
  run.out(`deploy-release: stage ${run.stage} is destroyed`);
  return 0;
}

/// Step 9.
async function cellCheck(run: Run): Promise<boolean> {
  const f = run.flags;
  const pass = ["cell-url", "bearer-file", "canary-token-file", "state", "s3-credentials"].flatMap((n) =>
    f.get(n) === undefined ? [] : [`--${n}`, f.get(n) as string],
  );
  const child = Bun.spawn(
    [process.execPath, join(import.meta.dir, "cell-check"), run.stage, run.dir, ...pass, "--deadline-secs", String(CELL_CHECK_DEADLINE_SECS)],
    { stdout: "inherit", stderr: "inherit" },
  );
  return (await child.exited) === 0;
}

/// Step 10.
function logDeploy(run: Run, result: Result): void {
  const entry = {
    stage: run.stage,
    project: run.scope.project,
    branch: run.scope.class === "preview" ? run.scope.branch : null,
    tag: run.release.tag,
    server_sha256: run.release.server_sha256,
    at: new Date().toISOString(),
    result,
  };
  appendFileSync(join(dirname(run.dir), DEPLOY_LOG), `${JSON.stringify(entry)}\n`);
}

/// Exit 0 deployed and checked (or destroyed), 3 deployed with no
/// --cell-url to check.
export async function main(argv: readonly string[], out: (line: string) => void): Promise<number> {
  const [stage, rawDir, ...rest] = argv;
  if (!(STAGES as readonly string[]).includes(stage ?? "") || !rawDir || rawDir.startsWith("--")) {
    throw new AdminError("give the stage and the release directory first", 2);
  }
  const flags = parseFlags(rest, {
    single: [
      "project", "branch", "workspace", "state", "s3-credentials", "cell-url", "bearer-file", "canary-token-file",
      "management-api",
    ],
    bool: ["destroy"],
    required: ["project", "workspace"],
  });
  const destroying = flags.has("destroy");
  if (destroying && stage === "production") {
    throw new AdminError(
      "--destroy takes a named stage (scratch or cert, deployed with --stage); the default stage, production, is never destroyed",
    );
  }
  if (!destroying) flags.must("state");
  if (flags.has("cell-url") && !(flags.has("bearer-file") && flags.has("canary-token-file"))) {
    throw new AdminError("--cell-url goes with --bearer-file and --canary-token-file (cell-check needs both)", 2);
  }
  refuseSecretsInEnv(envSecretNames(readFileSync(join(import.meta.dir, "app.ts"), "utf8")), process.env);
  const dir = resolve(rawDir);
  const run: Run = {
    stage, dir, flags, out, release: checkRelease(dir), scope: scopeOf(stage, flags.must("project"), flags.get("branch")),
  };
  if (basename(dir) !== run.release.tag) {
    throw new AdminError(`${dir} holds release ${run.release.tag}: a release lives in the directory of its tag`);
  }
  const api = Platform.open(flags.get("management-api") ?? MANAGEMENT_API);
  if (destroying) return destroyStage(run, api);
  const region = cellsFile(run).get("REGION") as string;
  await platformChecks(run, api, region, envSecretNames(readFileSync(fileIn(dir, "composer/app.ts"), "utf8")));
  const running = await runningRelease(run);
  if (running) refuseCrossing(run, running);
  await checkSizing(run);
  if (!(await prismaDeploy(run, api, region))) {
    logDeploy(run, "deploy_failed");
    throw new AdminError(`prisma deploy failed: release ${run.release.tag} did not reach ${stage}`);
  }
  if (!flags.has("cell-url")) {
    logDeploy(run, "unchecked");
    out(`deploy-release: deployed ${run.release.tag} to ${stage}; run cell-check with the service's address (--cell-url) now`);
    return 3;
  }
  const passed = await cellCheck(run);
  logDeploy(run, passed ? "pass" : "fail");
  if (!passed) throw new AdminError(`release ${run.release.tag} is deployed to ${stage} but cell-check failed: roll back with the previous release`);
  out(`deploy-release: ${run.release.tag} is deployed to ${stage} and passed cell-check`);
  return 0;
}

if (import.meta.main) {
  try {
    process.exit(await main(process.argv.slice(2), (line) => console.log(line)));
  } catch (e) {
    const known = e instanceof AdminError;
    console.error(`deploy-release: ${known ? e.message : `unexpected failure: ${(e as Error).message}`}`);
    if (known && e.code === 2) console.error(USAGE);
    process.exit(known ? e.code : 1);
  }
}
