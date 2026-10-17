// deploy-release.sh's cell watch (public-beta alerting triage B4; the
// field-readiness gap "the watch is deployed by hand"): the only way a
// watch release (watch-release.ts) reaches a stage, and the way back (a
// rollback is this command with the previous <tag>-watch).
//
//   PRISMA_TOKEN_FILE=F deploy/composer/deploy-release.sh <stage>
//     <releases>/<tag>-watch --app cell-watch --project ID [--branch ID]
//     --workspace ID [--management-api URL]
//
// The watch deploys on its own, after its cell (and the issuer it reads)
// answer, so a cut that only changes its settings redeploys nothing else.
// Refused, in this order, before anything deploys: a watch release that
// is not exactly what was cut, or not in the directory <tag>-watch; a
// secret its module binds set in this shell's environment (set-secrets
// is the only path, D10); no cell release <tag> beside it from the same
// commit (the stage's cells file there names the cell's CELL_ID and
// REGION); a stage the watch release carries no settings for, or
// settings naming another STREAMS_CELL_ID; a --project or --branch other
// than the one `prisma deploy` picks for the app streams-cell-watch, a
// project outside the cell's REGION, a secret name missing from the
// stage's own scope (deploy-release.ts's step 4 and 5). Then the
// release's own pinned `prisma deploy app.ts [--stage <stage>]`, from its
// root, in an environment of PATH, HOME, PRISMA_WORKSPACE_ID,
// PRISMA_SERVICE_TOKEN, PRISMA_REGION and the stage's settings, logged in
// deploy-log.jsonl as app "cell-watch". The watch has no address to check:
// its proof of life is its heartbeat reaching HEARTBEAT_URL (row 2), so
// the deploy is logged unchecked and exits 0.
//
// --destroy is not taken: the watch's named stage goes with its Branch
// (or the watch's project), which the operator deletes by hand.
import { appendFileSync, existsSync, readFileSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { parseFlags } from "../cell-admin/args";
import { AdminError } from "../cell-admin/errors";
import { parseProfiles } from "../cell-admin/profile";
import { DEPLOY_LOG, fromRelease, platformChecks, refuseSecretsInEnv } from "./deploy-release";
import { appModuleName, describeScope, envSecretNames, MANAGEMENT_API, Platform, scopeOf } from "./platform";
import { checkRelease } from "./release";
import { checkWatchRelease, watchSettings } from "./watch-release";

/// The apps deploy-release.sh deploys with --app.
const APPS = ["cell-watch"];

export async function deployWatch(stage: string, rawDir: string, argv: readonly string[], out: (line: string) => void): Promise<number> {
  if (argv.includes("--destroy")) {
    throw new AdminError("--app cell-watch takes no --destroy: delete the watch's Branch (or its project) by hand", 2);
  }
  const flags = parseFlags(argv, {
    single: ["app", "project", "branch", "workspace", "management-api"],
    required: ["app", "project", "workspace"],
  });
  const app = flags.must("app");
  if (!APPS.includes(app)) throw new AdminError(`--app ${app} is ${APPS.join(" or ")} (the cell deploys without --app)`, 2);
  const dir = resolve(rawDir);
  const release = checkWatchRelease(dir);
  if (basename(dir) !== `${release.tag}-watch`) {
    throw new AdminError(`${dir} holds the cell watch of ${release.tag}: it lives in the directory ${release.tag}-watch`);
  }
  const read = (p: string) => readFileSync(join(dir, p), "utf8");
  const moduleTs = read("module.ts");
  refuseSecretsInEnv(envSecretNames(moduleTs), process.env);
  const cellDir = join(dirname(dir), release.tag);
  if (!existsSync(join(cellDir, "deploy", "release.json"))) {
    throw new AdminError(`no cell release ${cellDir} beside the watch's: the watch deploys beside the cell release of its cut`);
  }
  const cell = checkRelease(cellDir);
  if (cell.commit !== release.commit) {
    throw new AdminError(`the cell release ${cellDir} is cut from ${cell.commit}, the watch's from ${release.commit}: both are one cut`);
  }
  if (cell.cells[stage] === undefined) throw new AdminError(`release ${release.tag} carries no cells/${stage}.env: no cell to watch on ${stage}`);
  const cellsPath = join(cellDir, "deploy", "cells", `${stage}.env`);
  const cells = parseProfiles([{ path: cellsPath, text: readFileSync(cellsPath, "utf8") }]);
  if (release.stages[stage] === undefined) throw new AdminError(`the cell watch's release ${dir} carries no stages/${stage}.env`);
  const settingsPath = join(dir, "stages", `${stage}.env`);
  const settings = watchSettings(settingsPath, readFileSync(settingsPath, "utf8"), moduleTs);
  if (settings.get("STREAMS_CELL_ID") !== cells.get("CELL_ID")) {
    throw new AdminError(`${settingsPath} names STREAMS_CELL_ID=${settings.get("STREAMS_CELL_ID")}; the cell is ${cells.get("CELL_ID")}`);
  }
  const region = cells.get("REGION") as string;
  const scope = scopeOf(stage, flags.must("project"), flags.get("branch"));
  const api = Platform.open(flags.get("management-api") ?? MANAGEMENT_API);
  await platformChecks({ stage, flags }, api, { scope, app: appModuleName(read("app.ts")), names: envSecretNames(moduleTs) }, region);
  const cli = join(dir, "node_modules", ".bin", "prisma");
  if (!existsSync(cli)) throw new AdminError(`${cli} does not exist: the watch's release carries its own pinned CLI`);
  const args = ["deploy", "app.ts", ...(stage === "production" ? [] : ["--stage", stage])];
  out(`deploy-release: prisma ${args.join(" ")} (the cell watch of ${release.tag}, ${describeScope(scope)}, ${region})`);
  const ok = await fromRelease({ flags }, api, dir, [cli, ...args], { PRISMA_REGION: region, ...Object.fromEntries(settings) });
  const entry = {
    app: "cell-watch",
    stage,
    project: scope.project,
    branch: scope.class === "preview" ? scope.branch : null,
    tag: release.tag,
    nonce: null,
    server_sha256: cell.server_sha256,
    at: new Date().toISOString(),
    result: ok ? "unchecked" : "deploy_failed",
  };
  appendFileSync(join(dirname(dir), DEPLOY_LOG), `${JSON.stringify(entry)}\n`);
  if (!ok) throw new AdminError(`the cell watch's prisma deploy failed: ${release.tag}-watch did not reach ${stage}`);
  out(
    `deploy-release: the cell watch of ${release.tag} is deployed to ${stage}; its check is its heartbeat reaching ` +
      "HEARTBEAT_URL within 3 minutes (row 2)",
  );
  return 0;
}
