// The cell watch's release directory (public-beta alerting triage B4; the
// field-readiness gap "not scripted or cut as a release"): <releases>/
// <tag>-watch, cut by scripts/cut-release.sh with the cell's <tag> and the
// issuer's <tag>-issuer from the same commit, and deployed by
// `deploy-release.sh <stage> <releases>/<tag>-watch --app cell-watch`. It
// is the watch app's Composer project root (deploy/cell-watch's app.ts
// says so):
//   package.json, bun.lock, prisma.config.ts  deploy/cell-watch's, byte for byte
//   app.ts, module.ts, service.ts             the app, its module and its one service
//   entry.js                                  entry.ts bundled at the cut (service.ts's
//                                             build: Composer ships this file alone)
//   stages/<stage>.env                        the stage's settings: module.ts's envParams,
//                                             which `prisma deploy` reads from its environment
//   release.json                              WatchRelease
//   node_modules/                             `bun install --frozen-lockfile` at the cut
// under the cell's release rules (release.ts's checkTree: no dotfile, no
// symlink, every file listed with its sha256).
//
// A stage's settings come from the cells directory's <stage>.watch.env:
// space-free KEY=VALUE, each of module.ts's envParams once and no other
// name, accepted by the commit's own deploy/cell-watch/config.ts (with
// stand-ins for the secrets, which reach the platform through set-secrets
// only), both origins https (a release deploys to the cloud), and
// EXPECTED_GIT the release's commit (or a prefix of it) or none, so row 5
// never pages the release it watches as the wrong build.
//
//   bun deploy/composer/watch-release.ts stage --tree DIR --commit SHA
//     --tag TAG --cells DIR --entry-js FILE --node-modules DIR --out DIR
// is cut-release's staging step; it moves --node-modules into the release.
import { existsSync, lstatSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { parseFlags } from "../cell-admin/args";
import { AdminError } from "../cell-admin/errors";
import { parseProfiles } from "../cell-admin/profile";
import { InputError, readWatchConfig } from "../cell-watch/config";
import { envSecretNames } from "./platform";
import { checkTree, committedFile, envParamNames, exactPin, type IssuerStageInput, isLink, sha256 } from "./release";
import { STAGES } from "./streams/env";

export const WATCH_RELEASE_JSON = "release.json";
/// The app's directory in the repository.
const WATCH_DIR = "deploy/cell-watch";
const WATCH_FILES = ["package.json", "bun.lock", "prisma.config.ts", "app.ts", "module.ts", "service.ts"] as const;
/// A stage's settings in the operator's cells directory.
export const watchSettingsFile = (stage: string) => `${stage}.watch.env`;

export interface WatchRelease {
  app: "cell-watch";
  tag: string;
  commit: string;
  /// stage -> sha256 of stages/<stage>.env.
  stages: Record<string, string>;
  composer_version: string;
  lockfile_sha256: string;
  files: Record<string, string>;
}

/// A stage's watch settings by name: space-free KEY=VALUE, each key once,
/// exactly the envParams module.ts binds (the watch's service marks none
/// optional; `none` is how an optional one says it is absent).
export function watchSettings(path: string, text: string, moduleTs: string): Map<string, string> {
  const env = parseProfiles([{ path, text }]);
  const keys = text.split("\n").map((l) => l.trim()).filter((l) => l !== "" && !l.startsWith("#")).map((l) => l.slice(0, l.indexOf("=")));
  const twice = keys.find((k, i) => keys.indexOf(k) !== i);
  if (twice) throw new AdminError(`${path} sets ${twice} twice`);
  const params = envParamNames(moduleTs);
  for (const key of env.keys()) {
    if (!params.includes(key)) throw new AdminError(`${path} sets ${key}, which is not one of the cell watch's settings`);
  }
  const missing = params.filter((p) => !env.has(p));
  if (missing.length) throw new AdminError(`${path} does not set ${missing.join(", ")}`);
  return env;
}

/// Stand-ins for the watch's secrets, consistent with `settings` (the
/// issuer's two are none exactly when ISSUER_ORIGIN is), so config.ts
/// judges the settings alone.
function standIns(moduleTs: string, settings: Map<string, string>): Record<string, string> {
  const issuer = settings.get("ISSUER_ORIGIN") !== "none";
  const value: Record<string, string> = {
    ALERT_SINK_URL: "https://sink.invalid/",
    HEARTBEAT_URL: "https://heartbeat.invalid/",
    STREAMS_ISSUER_METRICS_TOKEN: issuer ? "stand-in" : "none",
    CANARY_CREDENTIAL: issuer ? `prisma_streams_strcred_${"0".repeat(32)}.${"A".repeat(43)}` : "none",
  };
  return Object.fromEntries(envSecretNames(moduleTs).map((n) => [n, value[n] ?? "stand-in"]));
}

/// The settings a release takes for a stage: watchSettings, then what the
/// commit's config.ts accepts, both origins https and EXPECTED_GIT this
/// commit's.
export function releaseWatchSettings(path: string, text: string, moduleTs: string, commit: string): Map<string, string> {
  const settings = watchSettings(path, text, moduleTs);
  try {
    readWatchConfig({ ...standIns(moduleTs, settings), ...Object.fromEntries(settings) });
  } catch (e) {
    if (e instanceof InputError) throw new AdminError(`${path}: the watch would refuse it: ${e.message}`);
    throw e;
  }
  for (const name of ["STREAMS_CELL_ORIGIN", "ISSUER_ORIGIN"]) {
    const v = settings.get(name) as string;
    if (v !== "none" && !v.startsWith("https://")) throw new AdminError(`${path} sets ${name}=${v}: a release's watch runs in the cloud, so https`);
  }
  const git = settings.get("EXPECTED_GIT") as string;
  if (git !== "none" && !commit.startsWith(git)) {
    throw new AdminError(
      `${path} sets EXPECTED_GIT=${git}, not this release's commit ${commit}: row 5 would page the release it watches as the wrong build`,
    );
  }
  return settings;
}

/// Stage the watch's release at `out`, which must not exist; its stages
/// are the cells directory's <stage>.watch.env files.
export function stageWatchRelease(input: IssuerStageInput): WatchRelease {
  const { tree, commit, out } = input;
  if (existsSync(out)) throw new AdminError(`${out} exists: a release directory is never overwritten`);
  const files = new Map<string, Uint8Array>();
  for (const f of WATCH_FILES) files.set(f, committedFile(tree, commit, `${WATCH_DIR}/${f}`));
  files.set("entry.js", readFileSync(input.entryJs));
  const moduleTs = new TextDecoder().decode(files.get("module.ts"));
  const stages: Record<string, string> = {};
  for (const stage of STAGES) {
    const path = join(input.cells, watchSettingsFile(stage));
    if (!existsSync(path) && !isLink(path)) continue;
    if (!lstatSync(path).isFile()) throw new AdminError(`${path} is not a regular file`);
    const bytes = readFileSync(path);
    releaseWatchSettings(path, bytes.toString("utf8"), moduleTs, commit);
    files.set(`stages/${stage}.env`, bytes);
    stages[stage] = sha256(bytes);
  }
  const pkg = JSON.parse(new TextDecoder().decode(files.get("package.json")));
  const release: WatchRelease = {
    app: "cell-watch",
    tag: input.tag,
    commit,
    stages,
    composer_version: exactPin(pkg, "@prisma/composer", WATCH_DIR),
    lockfile_sha256: sha256(files.get("bun.lock") as Uint8Array),
    files: Object.fromEntries([...files].map(([p, b]) => [p, sha256(b)]).sort(([a], [b]) => (a < b ? -1 : 1))),
  };
  for (const [path, bytes] of files) {
    mkdirSync(dirname(join(out, path)), { recursive: true });
    writeFileSync(join(out, path), bytes, { mode: 0o644 });
  }
  writeFileSync(join(out, WATCH_RELEASE_JSON), `${JSON.stringify(release, null, 2)}\n`, { mode: 0o644 });
  renameSync(input.nodeModules, join(out, "node_modules"));
  return checkWatchRelease(out);
}

/// Refuse a watch release root that breaks the rules; its release.json
/// otherwise.
export function checkWatchRelease(root: string): WatchRelease {
  let release: WatchRelease;
  try {
    release = JSON.parse(readFileSync(join(root, WATCH_RELEASE_JSON), "utf8"));
  } catch {
    throw new AdminError(`${root} has no readable ${WATCH_RELEASE_JSON}: not a cell watch release directory`);
  }
  if (release?.app !== "cell-watch" || typeof release.files !== "object") {
    throw new AdminError(`${join(root, WATCH_RELEASE_JSON)} is not a cell watch release.json cut-release wrote`);
  }
  checkTree(root, WATCH_RELEASE_JSON, release.files);
  return release;
}

if (import.meta.main) {
  const [command, ...argv] = process.argv.slice(2);
  try {
    if (command !== "stage") throw new AdminError("usage: watch-release.ts stage --tree DIR ... (see the header)", 2);
    const names = ["tree", "commit", "tag", "cells", "entry-js", "node-modules", "out"];
    const f = parseFlags(argv, { single: names, required: names });
    const r = stageWatchRelease({
      tree: f.must("tree"),
      commit: f.must("commit"),
      tag: f.must("tag"),
      cells: f.must("cells"),
      entryJs: f.must("entry-js"),
      nodeModules: f.must("node-modules"),
      out: f.must("out"),
    });
    console.log(`staged the cell watch of ${r.tag} (${r.commit}): stages ${Object.keys(r.stages).join(", ") || "none"}, ` +
      `${Object.keys(r.files).length} files`);
  } catch (e) {
    console.error(`watch-release: ${e instanceof AdminError ? e.message : `unexpected failure: ${(e as Error).message}`}`);
    process.exit(e instanceof AdminError ? e.code : 1);
  }
}
