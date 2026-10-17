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
// --app pager-standin deploys the paging stand-in's app (pager-app.ts,
// from the same release root) the same way, into its own project
// (streams-pager-standin), with the stage's STREAMS_CELL_ORIGIN from the
// watch's settings as its one envParam; on scratch or cert only:
// production pages through the real paging service (RUNBOOK §7.5). Its
// domain then gives the watch its ALERT_SINK_URL and HEARTBEAT_URL
// (set-secrets --app cell-watch, then redeploy the watch).
//
// --destroy is not taken: an app's named stage goes with its Branch (or
// its project), which the operator deletes by hand.
import { appendFileSync, existsSync, readFileSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { parseFlags } from "../cell-admin/args";
import { AdminError } from "../cell-admin/errors";
import { parseProfiles } from "../cell-admin/profile";
import { DEPLOY_LOG, fromRelease, platformChecks, refuseSecretsInEnv } from "./deploy-release";
import { appModuleName, describeScope, envSecretNames, MANAGEMENT_API, Platform, scopeOf } from "./platform";
import { checkRelease, envParamNames } from "./release";
import { checkWatchRelease, watchSettings } from "./watch-release";

/// The apps deploy-release.sh deploys with --app from a watch release:
/// each one's app file, the file binding its inputs, and what it is.
const APPS: Record<string, { appTs: string; binds: string; what: string }> = {
  "cell-watch": { appTs: "app.ts", binds: "module.ts", what: "the cell watch" },
  "pager-standin": { appTs: "pager-app.ts", binds: "pager-app.ts", what: "the paging stand-in" },
};

export async function deployWatch(stage: string, rawDir: string, argv: readonly string[], out: (line: string) => void): Promise<number> {
  if (argv.includes("--destroy")) {
    throw new AdminError("--app takes no --destroy: delete the app's Branch (or its project) by hand", 2);
  }
  const flags = parseFlags(argv, {
    single: ["app", "project", "branch", "workspace", "management-api"],
    required: ["app", "project", "workspace"],
  });
  const app = flags.must("app");
  if (!Object.hasOwn(APPS, app)) {
    throw new AdminError(`--app ${app} is ${Object.keys(APPS).join(" or ")} (the cell deploys without --app)`, 2);
  }
  const { appTs, binds, what } = APPS[app];
  if (app === "pager-standin" && stage === "production") {
    throw new AdminError("the paging stand-in deploys to scratch or cert only: production pages through the real paging service (RUNBOOK §7.5)");
  }
  const dir = resolve(rawDir);
  const release = checkWatchRelease(dir);
  if (basename(dir) !== `${release.tag}-watch`) {
    throw new AdminError(`${dir} holds the cell watch of ${release.tag}: it lives in the directory ${release.tag}-watch`);
  }
  const read = (p: string) => readFileSync(join(dir, p), "utf8");
  const moduleTs = read("module.ts");
  const names = envSecretNames(read(binds));
  refuseSecretsInEnv(names, process.env);
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
  await platformChecks({ stage, flags }, api, { scope, app: appModuleName(read(appTs)), names }, region);
  const cli = join(dir, "node_modules", ".bin", "prisma");
  if (!existsSync(cli)) throw new AdminError(`${cli} does not exist: the watch's release carries its own pinned CLI`);
  const args = ["deploy", appTs, ...(stage === "production" ? [] : ["--stage", stage])];
  out(`deploy-release: prisma ${args.join(" ")} (${what} of ${release.tag}, ${describeScope(scope)}, ${region})`);
  const params = envParamNames(read(binds)).map((n) => [n, settings.get(n) as string]);
  const ok = await fromRelease({ flags }, api, dir, [cli, ...args], { PRISMA_REGION: region, ...Object.fromEntries(params) });
  const entry = {
    app,
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
  if (!ok) throw new AdminError(`${what}'s prisma deploy failed: ${release.tag}-watch did not reach ${stage}`);
  out(
    app === "cell-watch"
      ? `deploy-release: the cell watch of ${release.tag} is deployed to ${stage}; its check is its heartbeat reaching ` +
          "HEARTBEAT_URL within 3 minutes (row 2)"
      : `deploy-release: the paging stand-in of ${release.tag} is deployed to ${stage}; give the watch its domain: ` +
          "ALERT_SINK_URL https://<domain>/v2/enqueue (ALERT_SINK_KIND events-v2, ALERT_SINK_KEY its routing key) and " +
          "HEARTBEAT_URL https://<domain>/heartbeat/<token>; GET /changes with the routing key's bearer lists what it took",
  );
  return 0;
}
