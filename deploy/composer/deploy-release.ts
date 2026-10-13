// deploy-release.sh's work (public-beta deploy-q5 triage B5, design §2.3):
// the only way a release reaches a shared cell, and the way back (a
// rollback is this command with the previous release directory). Its
// header in deploy-release.sh is the operator's reference; the steps:
//
//   1. the release is exactly what was cut (release.ts's rules);
//   2. no secret the release's app binds in this process's environment
//      (D10: Composer's preflight would push one it finds there;
//      set-secrets is the only path);
//   3. the stage's cells file is in the release; production's carries no
//      KEEP_AWAKE_UNTIL_MS; the release's own env builder (its
//      deploy/composer/streams/env.ts, which its entry runs) accepts its
//      profiles and the cells file (so a refusal is met here, not as a
//      crash-looping deploy);
//   4. --project, and a named stage's --branch, are the project and Branch
//      `prisma deploy` itself picks (Composer 0.29.1's own rule, D0 finding
//      4: in --workspace, the project whose logicalId is the app's module
//      name, else the oldest project of that name; the project's Branch
//      named as the stage), so every check below is of the scope the
//      deploy reaches; the project is in the cells file's REGION;
//   5. every secret name the app binds is set in the stage's own scope,
//      and a named stage's names are its Branch's overrides (D0 finding
//      10: a branchless preview variable reaches every named stage);
//   6. the running release: the one whose server_sha256 the running cell
//      reports on /v1/debug/load, found among the releases beside this
//      one; when no cell answers, the last release deploy-log.jsonl
//      records as deployed to the stage; neither is "no cell yet",
//      accepted (S27). Refused: a release whose layout_version,
//      keys_contract or MAX_REQUEST_BODY_BYTES differs from the running
//      one's (before step 7, which refuses any changed profile too, so
//      each refusal is met as itself);
//   7. `cell-admin check-sizing` accepts the release's profiles for the
//      cell's state (D9: a cell's ceilings are fixed for its life);
//   8. `prisma deploy deploy/composer/app.ts [--stage <stage>]` with the
//      release's own pinned CLI, from the release root, in an environment
//      of PATH, HOME, CELL_STAGE, CELL_DEPLOY_NONCE, PRISMA_REGION,
//      PRISMA_WORKSPACE_ID and PRISMA_SERVICE_TOKEN only. The nonce (the
//      app's envParam, lead decision deploy-q5 #6 (2)) is the release's
//      tag, or, when deploy-log.jsonl's last live deploy of the stage was
//      this release, the nonce it recorded: a re-run is the same input,
//      which Composer deploys as a no-op (D0 finding 8);
//   9. cell-check, within 5 minutes, when --cell-url names the cell;
//  10. one line appended to deploy-log.jsonl beside the releases:
//      {app, stage, project, branch, tag, nonce, server_sha256, at, result}.
// With --issuer-release-dir (the interim issuer, Q6 (b)): its release
// (cut-release's <tag>-issuer) must be cut from the cell's commit and pass
// the same rules, its stages/<stage>.env must name the cell's CELL_ID,
// and its project (--issuer-project, --issuer-branch) must be the one
// `prisma deploy` picks for its module, be in the region and hold its
// secrets, all before the cell deploys; the issuer
// deploys from its release root after the cell's deploy (and cell-check,
// when run) passed, with its settings as the envParams `prisma deploy`
// reads, and is logged as app "issuer".
// Each release is judged by its own rules: its env.ts (the profile list
// and the child environment's) and its app.ts (the secret names and the
// module), never the checkout's, which a rollback to a release cut from
// another commit would get wrong.
// With --restart (lead decision deploy-q5 #6 (2)): the same steps for the
// release the cell at --cell-url runs, refused when it runs another one
// or none answers, with a new nonce, `<tag>.r<unix secs>`, so the deploy
// is a new version of the same release; cell-check then also requires an
// x-streams-boot-id other than the one the cell answered with before
// (--not-boot-id), so a restart passes only on a new instance. A plain
// deploy after it keeps that nonce (step 8).
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
  appModuleName,
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
import { checkIssuerRelease, checkRelease, type IssuerRelease, issuerSettings, type Release, readRelease } from "./release";
import { STAGES } from "./streams/env";

const USAGE = `usage: PRISMA_TOKEN_FILE=F deploy/composer/deploy-release.sh <stage> <release-dir>
  --project ID [--branch ID] --workspace ID --state DIR|s3://... [--s3-credentials F]
  [--cell-url URL --bearer-file F --canary-token-file T [--restart]] [--management-api URL]
  [--issuer-release-dir DIR --issuer-project ID [--issuer-branch ID]]
or: ... <scratch|cert> <release-dir> --project ID --branch ID --workspace ID --destroy`;
const DEPLOY_LOG = "deploy-log.jsonl";
const CELL_CHECK_DEADLINE_SECS = 300;
const PROBE_TIMEOUT_MS = 15_000;
/// What a run did to the stage, as deploy-log.jsonl records it; the first
/// three mean the release was deployed, "destroyed" that no cell is left.
type Result = "pass" | "fail" | "unchecked" | "deploy_failed" | "destroyed" | "destroy_failed";
const DEPLOYED: readonly Result[] = ["pass", "fail", "unchecked"];

/// A release's own env.ts: its profile list and child-environment builder.
type Rules = typeof import("./streams/env");

interface Run {
  stage: string;
  dir: string;
  release: Release;
  rules: Rules;
  scope: Scope;
  /// CELL_DEPLOY_NONCE for a deploy (step 8); null for --destroy.
  nonce: string | null;
  flags: Flags;
  out: (line: string) => void;
}

const fileIn = (dir: string, path: string) => join(dir, "deploy", path);
const profilesOf = (dir: string, rules: Rules) =>
  rules.PROFILES.map((p) => ({ path: fileIn(dir, p), text: readFileSync(fileIn(dir, p), "utf8") }));

/// The rules a checked release carries (Bun loads its env.ts and the two
/// cell-admin files that imports, all in release.json's list).
const rulesOf = (dir: string): Promise<Rules> => import(fileIn(dir, "composer/streams/env.ts"));

/// Step 2.
function refuseSecretsInEnv(names: readonly string[], env: Readonly<Record<string, string | undefined>>): void {
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
  const built = run.rules.buildChildEnv({
    profiles: profilesOf(dir, run.rules),
    cellEnv: { path, text },
    stage,
    platformEnv: placeholder,
    binarySha256: release.server_sha256,
    port: "8080",
  });
  if ("refused" in built) throw new AdminError(`the cell's entry would refuse this release on ${stage}: ${built.message}`);
  return cells;
}

/// An app as the platform checks see it: the scope the operator named,
/// the app's module name (its project's name) and its envSecret names.
interface Target {
  scope: Scope;
  app: string;
  names: readonly string[];
}

const bareWorkspace = (id: string) => (id.startsWith("wksp_") ? id.slice(5) : id);

/// The project `prisma deploy` puts `app` in, by Composer's own rule
/// (@prisma/composer-prisma-cloud 0.29.1's resolveProject): among the
/// workspace's projects, the one whose logicalId is the app's name, else
/// the oldest one of that name; null when it would create one.
async function adoptedProject(api: Platform, workspace: string, app: string): Promise<{ id: string; how: string } | null> {
  const mine = (await api.list("/projects")).filter(
    (p) => typeof p?.workspace?.id === "string" && bareWorkspace(p.workspace.id) === bareWorkspace(workspace),
  );
  const logical = mine.find((p) => p.logicalId != null && p.logicalId === app);
  if (logical) return { id: logical.id, how: "its logicalId" };
  const named = mine.filter((p) => p.name === app).sort((a, b) => String(a.createdAt).localeCompare(String(b.createdAt)))[0];
  return named ? { id: named.id, how: "the oldest project of that name" } : null;
}

/// Step 4's first half: the named scope is the one the deploy reaches.
async function refuseOtherScope(run: Run, api: Platform, { scope, app }: Target): Promise<void> {
  const workspace = run.flags.must("workspace");
  const adopted = await adoptedProject(api, workspace, app);
  if (adopted === null) {
    throw new AdminError(`workspace ${workspace} holds no project ${app}: prisma deploy would create one, not deploy to --project ${scope.project}`);
  }
  if (adopted.id !== scope.project) {
    throw new AdminError(
      `prisma deploy puts app ${app} of workspace ${workspace} in project ${adopted.id} (${adopted.how}), not in --project ${scope.project}`,
    );
  }
  if (scope.class !== "preview") return;
  // Composer's findBranchId: the first Branch the gitName filter answers.
  const path = `/projects/${encodeURIComponent(scope.project)}/branches?gitName=${encodeURIComponent(run.stage)}`;
  const branch = (await api.call("GET", path)).body?.data?.[0]?.id;
  if (branch === undefined) {
    throw new AdminError(
      `project ${scope.project} has no Branch named ${run.stage}: prisma deploy --stage ${run.stage} would create one, ` +
        `not deploy to --branch ${scope.branch}`,
    );
  }
  if (branch !== scope.branch) {
    throw new AdminError(
      `prisma deploy --stage ${run.stage} deploys to Branch ${branch} of project ${scope.project} (its Branch named ` +
        `${run.stage}), not to --branch ${scope.branch}`,
    );
  }
}

/// Steps 4 and 5, for the cell's scope or the issuer's.
async function platformChecks(run: Run, api: Platform, target: Target, region: string): Promise<void> {
  const { scope, names } = target;
  await refuseOtherScope(run, api, target);
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
  const code = await cellAdmin([...argv, ...run.rules.PROFILES.flatMap((p) => ["--profile", fileIn(run.dir, p)])], io);
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

/// The running cell's binary, commit and instance (its /health boot id),
/// or null when no cell answers.
async function runningIdentity(run: Run): Promise<{ sha: string; commit: string; boot: string | null } | null> {
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
  return { sha: load.binary_sha256, commit: load.git_commit, boot: health.headers.get("x-streams-boot-id") };
}

/// The deploy log's last line that deployed a cell to the stage's scope
/// (a failed deploy changed nothing), unless a destroy came after it.
function lastLive(dir: string, stage: string, scope: Scope): { tag: string; nonce?: string } | null {
  const log = join(dirname(dir), DEPLOY_LOG);
  if (!existsSync(log)) return null;
  const branch = scope.class === "preview" ? scope.branch : null;
  const mine = readFileSync(log, "utf8")
    .split("\n")
    .filter((l) => l.trim() !== "")
    .map((l) => JSON.parse(l))
    .filter((e) => (e.app ?? "cell") === "cell" && e.stage === stage && e.project === scope.project && (e.branch ?? null) === branch)
    .filter((e) => DEPLOYED.includes(e.result) || e.result === "destroyed");
  const last = mine[mine.length - 1];
  return last && last.result !== "destroyed" ? last : null;
}

/// The release the deploy log last deployed to the stage's scope.
const lastDeployed = (run: Run): string | null => lastLive(run.dir, run.stage, run.scope)?.tag ?? null;

/// Step 8's nonce: the one the stage's last live deploy of this release
/// recorded, else the tag.
function deployNonce(dir: string, stage: string, scope: Scope, tag: string): string {
  const last = lastLive(dir, stage, scope);
  return last?.tag === tag && typeof last.nonce === "string" ? last.nonce : tag;
}

/// The running release; when the cell answered for it (not the deploy
/// log), the instance's boot id (null when /health named none).
interface Running {
  release: Release;
  answered: boolean;
  boot: string | null;
}

/// Step 6: the running release, or null for "no cell yet".
async function runningRelease(run: Run): Promise<Running | null> {
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
    return { release: found[0], answered: true, boot: live.boot };
  }
  const tag = lastDeployed(run);
  if (tag === null) {
    run.out(`deploy-release: no cell yet: nothing answers for ${run.stage} and the deploy log holds no live deploy of it`);
    return null;
  }
  run.out(`deploy-release: no cell answers; ${DEPLOY_LOG} last deployed release ${tag} to ${run.stage}`);
  return { release: readRelease(join(dirname(run.dir), tag)), answered: false, boot: null };
}

/// --restart's refusals: it restarts the instance that answers, of this
/// release; returns that instance's boot id.
function restarted(run: Run, running: Running | null): string {
  const url = run.flags.must("cell-url");
  if (!running?.answered) throw new AdminError(`--restart restarts the cell that answers at ${url}, and none answers`);
  if (running.boot === null) throw new AdminError(`the cell at ${url} names no x-streams-boot-id: a new instance cannot be told from it`);
  const [was, is] = [running.release, run.release];
  if (was.server_sha256 !== is.server_sha256) {
    throw new AdminError(
      `--restart restarts the release the cell runs: it runs ${was.tag} (${was.server_sha256}), not ${is.tag} (${is.server_sha256}); ` +
        `deploy ${is.tag} without --restart`,
    );
  }
  return running.boot;
}

/// Step 6's refusals.
async function refuseCrossing(run: Run, running: Release): Promise<void> {
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
  const body = (dir: string, rules: Rules) => parseProfiles(profilesOf(dir, rules)).get("MAX_REQUEST_BODY_BYTES") ?? "(unset)";
  const runningDir = join(dirname(run.dir), running.tag);
  checkRelease(runningDir);
  const [mine, theirs] = [body(run.dir, run.rules), body(runningDir, await rulesOf(runningDir))];
  if (mine !== theirs) {
    throw new AdminError(
      `release ${release.tag} sets MAX_REQUEST_BODY_BYTES ${mine}, ${was} ${theirs}: the cell's namespaces stored ` +
        "their body ceiling at creation and the binary refuses another",
    );
  }
}

/// Run `argv` from a release root (`cwd`) in an environment of PATH, HOME,
/// the workspace, the token and `extra`, and nothing else.
async function fromRelease(run: Run, api: Platform, cwd: string, argv: string[], extra: Record<string, string>): Promise<boolean> {
  const child = Bun.spawn(argv, {
    cwd,
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
  const nonce = run.nonce as string;
  run.out(`deploy-release: prisma ${args.join(" ")} (release ${run.release.tag}, nonce ${nonce}, ${describeScope(run.scope)}, ${region})`);
  return fromRelease(run, api, run.dir, [cli, ...args], { CELL_STAGE: run.stage, CELL_DEPLOY_NONCE: nonce, PRISMA_REGION: region });
}

/// --destroy: Composer's destroy of a named stage (the release's
/// destroy.ts, D0 finding 3). The buckets stay.
async function destroyStage(run: Run, api: Platform): Promise<number> {
  run.out(`deploy-release: destroying stage ${run.stage} (${describeScope(run.scope)}); its buckets stay for provision-cell.py --teardown`);
  const destroyed = await fromRelease(run, api, run.dir, [process.execPath, join(run.dir, "destroy.ts"), run.stage], {});
  logDeploy(run, destroyed ? "destroyed" : "destroy_failed");
  if (!destroyed) throw new AdminError(`the destroy of stage ${run.stage} failed`);
  run.out(`deploy-release: stage ${run.stage} is destroyed`);
  return 0;
}

/// Step 9; after --restart, only an instance other than `replaced` passes.
async function cellCheck(run: Run, replaced: string | null): Promise<boolean> {
  const f = run.flags;
  const pass = ["cell-url", "bearer-file", "canary-token-file", "state", "s3-credentials"].flatMap((n) =>
    f.get(n) === undefined ? [] : [`--${n}`, f.get(n) as string],
  );
  if (replaced !== null) pass.push("--not-boot-id", replaced);
  const child = Bun.spawn(
    [process.execPath, join(import.meta.dir, "cell-check"), run.stage, run.dir, ...pass, "--deadline-secs", String(CELL_CHECK_DEADLINE_SECS)],
    { stdout: "inherit", stderr: "inherit" },
  );
  return (await child.exited) === 0;
}

/// Step 10: one line per app a run deploys or destroys.
function logDeploy(run: Run, result: Result, issuer?: IssuerRun): void {
  const scope = issuer ? issuer.scope : run.scope;
  const entry = {
    app: issuer ? "issuer" : "cell",
    stage: run.stage,
    project: scope.project,
    branch: scope.class === "preview" ? scope.branch : null,
    tag: run.release.tag,
    nonce: issuer ? null : run.nonce,
    server_sha256: run.release.server_sha256,
    at: new Date().toISOString(),
    result,
  };
  appendFileSync(join(dirname(run.dir), DEPLOY_LOG), `${JSON.stringify(entry)}\n`);
}

/// The interim issuer's release, deployed in the same run (Q6 (b)).
interface IssuerRun {
  dir: string;
  release: IssuerRelease;
  scope: Scope;
  settings: Map<string, string>;
}

/// The issuer's release (--issuer-release-dir), checked under the rules.
interface IssuerCut {
  dir: string;
  release: IssuerRelease;
}

/// The issuer's preconditions, all met before the cell deploys: its
/// release (checked under the rules) cut from the cell's commit; its
/// settings for the stage, naming the cell's CELL_ID; its project the one
/// prisma deploy picks, in the cell's region, holding its secrets.
async function issuerChecks(run: Run, api: Platform, region: string, cells: Map<string, string>, cut: IssuerCut | null): Promise<IssuerRun | null> {
  if (cut === null) return null;
  const { dir, release } = cut;
  if (release.commit !== run.release.commit) {
    throw new AdminError(`the issuer's release ${dir} is cut from ${release.commit}, the cell's from ${run.release.commit}: both deploy from one commit`);
  }
  if (release.stages[run.stage] === undefined) throw new AdminError(`the issuer's release ${dir} carries no stages/${run.stage}.env`);
  const path = join(dir, "stages", `${run.stage}.env`);
  const read = (p: string) => readFileSync(join(dir, p), "utf8");
  const settings = issuerSettings(path, readFileSync(path, "utf8"), read("app.ts"), read("issuer/service.ts"));
  if (settings.get("STREAMS_CELL_ID") !== cells.get("CELL_ID")) {
    throw new AdminError(`${path} names STREAMS_CELL_ID=${settings.get("STREAMS_CELL_ID")}; the cell is ${cells.get("CELL_ID")}`);
  }
  const scope = scopeOf(run.stage, run.flags.must("issuer-project"), run.flags.get("issuer-branch"));
  await platformChecks(run, api, { scope, app: appModuleName(read("app.ts")), names: envSecretNames(read("app.ts")) }, region);
  return { dir, release, scope, settings };
}

/// The issuer's deploy: its release's own CLI, from its root, its settings
/// as the envParams prisma deploy reads.
async function issuerDeploy(run: Run, api: Platform, region: string, issuer: IssuerRun): Promise<void> {
  const cli = join(issuer.dir, "node_modules", ".bin", "prisma");
  if (!existsSync(cli)) throw new AdminError(`${cli} does not exist: the issuer's release carries its own pinned CLI`);
  const args = ["deploy", "app.ts", ...(run.stage === "production" ? [] : ["--stage", run.stage])];
  run.out(`deploy-release: prisma ${args.join(" ")} (the issuer of ${issuer.release.tag}, ${describeScope(issuer.scope)})`);
  const ok = await fromRelease(run, api, issuer.dir, [cli, ...args], { PRISMA_REGION: region, ...Object.fromEntries(issuer.settings) });
  logDeploy(run, ok ? "unchecked" : "deploy_failed", issuer);
  if (!ok) throw new AdminError(`the issuer's prisma deploy failed (the cell's deploy of ${run.release.tag} stands)`);
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
      "management-api", "issuer-release-dir", "issuer-project", "issuer-branch",
    ],
    bool: ["destroy", "restart"],
    required: ["project", "workspace"],
  });
  const destroying = flags.has("destroy");
  if (destroying && stage === "production") {
    throw new AdminError(
      "--destroy takes a named stage (scratch or cert, deployed with --stage); the default stage, production, is never destroyed",
    );
  }
  if (!destroying) flags.must("state");
  if (flags.has("issuer-release-dir") && (destroying || !flags.has("issuer-project"))) {
    throw new AdminError("--issuer-release-dir deploys the issuer with the cell: it needs --issuer-project and takes no --destroy", 2);
  }
  if (flags.has("cell-url") && !(flags.has("bearer-file") && flags.has("canary-token-file"))) {
    throw new AdminError("--cell-url goes with --bearer-file and --canary-token-file (cell-check needs both)", 2);
  }
  const restart = flags.has("restart");
  if (restart && (!flags.has("cell-url") || destroying || flags.has("issuer-release-dir"))) {
    throw new AdminError(
      "--restart restarts the running cell: it needs --cell-url (with cell-check's files) and takes no --issuer-release-dir or --destroy",
      2,
    );
  }
  const dir = resolve(rawDir);
  const release = checkRelease(dir);
  if (basename(dir) !== release.tag) {
    throw new AdminError(`${dir} holds release ${release.tag}: a release lives in the directory of its tag`);
  }
  const issuerDir = flags.get("issuer-release-dir");
  const cut = issuerDir === undefined ? null : { dir: resolve(issuerDir), release: checkIssuerRelease(resolve(issuerDir)) };
  const apps = [fileIn(dir, "composer/app.ts"), ...(cut ? [join(cut.dir, "app.ts")] : [])];
  refuseSecretsInEnv([...new Set(apps.flatMap((a) => envSecretNames(readFileSync(a, "utf8"))))], process.env);
  const scope = scopeOf(stage, flags.must("project"), flags.get("branch"));
  const nonce = destroying
    ? null
    : restart
      ? `${release.tag}.r${Math.floor(Date.now() / 1000)}`
      : deployNonce(dir, stage, scope, release.tag);
  const run: Run = { stage, dir, flags, out, release, rules: await rulesOf(dir), scope, nonce };
  const api = Platform.open(flags.get("management-api") ?? MANAGEMENT_API);
  if (destroying) return destroyStage(run, api);
  const cells = cellsFile(run);
  const region = cells.get("REGION") as string;
  const appTs = readFileSync(fileIn(dir, "composer/app.ts"), "utf8");
  await platformChecks(run, api, { scope: run.scope, app: appModuleName(appTs), names: envSecretNames(appTs) }, region);
  const issuer = await issuerChecks(run, api, region, cells, cut);
  const running = await runningRelease(run);
  const replaced = restart ? restarted(run, running) : null;
  if (running) await refuseCrossing(run, running.release);
  await checkSizing(run);
  if (!(await prismaDeploy(run, api, region))) {
    logDeploy(run, "deploy_failed");
    throw new AdminError(`prisma deploy failed: release ${run.release.tag} did not reach ${stage}`);
  }
  if (!flags.has("cell-url")) {
    logDeploy(run, "unchecked");
    if (issuer) await issuerDeploy(run, api, region, issuer);
    out(`deploy-release: deployed ${run.release.tag} to ${stage}; run cell-check with the service's address (--cell-url) now`);
    return 3;
  }
  const passed = await cellCheck(run, replaced);
  logDeploy(run, passed ? "pass" : "fail");
  if (restart) {
    const how = `${run.release.tag} ${passed ? "is" : "was"} restarted on ${stage} (nonce ${run.nonce})`;
    if (!passed) throw new AdminError(`${how}, but no new instance passed cell-check (its last reason is above)`);
    out(`deploy-release: ${how}: a new instance passed cell-check`);
    return 0;
  }
  if (!passed) {
    throw new AdminError(
      `release ${run.release.tag} is deployed to ${stage} but cell-check failed${issuer ? " (the issuer is not deployed)" : ""}: ` +
        "roll back with the previous release",
    );
  }
  if (issuer) await issuerDeploy(run, api, region, issuer);
  out(`deploy-release: ${run.release.tag} is deployed to ${stage} and passed cell-check${issuer ? ", and its issuer after it" : ""}`);
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
