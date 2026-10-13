// A shared cell's release directory (public-beta deploy-q5 triage B5, the
// layout as D0's local findings 1 and 2 correct it): everything one commit
// deploys, staged once by scripts/cut-release.sh and never changed after,
// and the rules deploy-release.sh checks before it deploys one.
//
// The release root is the Composer project root, the working directory of
// `prisma deploy deploy/composer/app.ts`:
//   package.json, bun.lock, prisma.config.ts  deploy/composer's, byte for byte
//   destroy.ts                                deploy/composer's: --destroy's control script
//   node_modules/                             `bun install --frozen-lockfile` at the cut
//   bin/s3lite                                the commit's x86_64-musl s3lite (the rigs')
//   deploy/                                   the cell service's build dir, shipped whole:
//     release.json                            Release, below
//     cells/<stage>.env                       the operator's cells files, one per stage
//     composer/app.ts, composer/streams/{service,entry,env}.ts
//     composer/streams/entry.js               entry.ts bundled at the cut
//     composer/streams/streams-slate          the commit's x86_64-musl server
//     app-server/{supervise,feeds,downloader}.ts, cell-admin/{profile,errors}.ts
//     profiles/*.env
// Composer keeps its working state under the root (.prisma-composer/ and
// .alchemy/, made by a deploy), never under deploy/.
//
// The rules (stage-app's, triage D5): under the root every entry but
// node_modules and Composer's two is a directory or a regular file, never
// a symlink and never a dotfile, and every file is one release.json lists,
// with its sha256. Composer ships deploy/ whole, and Bun loads a `.env`
// beside an entry on its own, so a stray file would change what deploys
// behind the release's back.
//
//   bun deploy/composer/release.ts stage --tree DIR --commit SHA --tag TAG
//     --build-unix N --cells DIR --server BIN --s3lite BIN --entry-js FILE
//     --node-modules DIR --out DIR
//   bun deploy/composer/release.ts stage-issuer --tree DIR --commit SHA
//     --tag TAG --cells DIR --entry-js FILE --node-modules DIR --out DIR
// are cut-release's staging steps (the cell's, and the interim issuer's,
// below); each moves --node-modules into its release.
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { chmodSync, existsSync, lstatSync, mkdirSync, readdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { parseFlags } from "../cell-admin/args";
import { AdminError } from "../cell-admin/errors";
import { parseProfiles } from "../cell-admin/profile";
import { STAGES } from "./streams/env";

export const RELEASE_JSON = "deploy/release.json";
/// The server and the entry's bundle inside the build dir.
export const SERVER = "deploy/composer/streams/streams-slate";
const ENTRY_JS = "deploy/composer/streams/entry.js";
const S3LITE = "bin/s3lite";
/// Root entries no rule covers: the install, and what a deploy leaves.
const UNCHECKED_ROOT = new Set(["node_modules", ".prisma-composer", ".alchemy"]);
/// Repository files a release carries: its path, then the repository's.
const REPO_FILES: readonly (readonly [string, string])[] = [
  ["package.json", "deploy/composer/package.json"],
  ["bun.lock", "deploy/composer/bun.lock"],
  ["prisma.config.ts", "deploy/composer/prisma.config.ts"],
  ["destroy.ts", "deploy/composer/destroy.ts"],
  ...[
    "deploy/composer/app.ts",
    "deploy/composer/streams/service.ts",
    "deploy/composer/streams/entry.ts",
    "deploy/composer/streams/env.ts",
    "deploy/app-server/supervise.ts",
    "deploy/app-server/feeds.ts",
    "deploy/app-server/downloader.ts",
    "deploy/cell-admin/profile.ts",
    "deploy/cell-admin/errors.ts",
  ].map((p) => [p, p] as const),
];
const PROFILES_DIR = "deploy/profiles";
const ELF_X86_64 = 0x3e;

/// deploy/release.json.
export interface Release {
  tag: string;
  commit: string;
  build_unix: number;
  /// Lowercase hex, as the entry and cell-check compare it.
  server_sha256: string;
  server_bytes: number;
  s3lite_sha256: string;
  /// deploy/profiles/<file> -> sha256.
  profiles: Record<string, string>;
  /// stage -> sha256 of deploy/cells/<stage>.env.
  cells: Record<string, string>;
  /// src/registry.rs's LAYOUT_VERSION at the commit.
  layout_version: number;
  /// The keys feed's contract revision (MULTITENANCY §14.1, "r7").
  keys_contract: string;
  composer_version: string;
  compute_version: string;
  lockfile_sha256: string;
  /// Every file of the release but release.json and node_modules/, by
  /// its path under the root.
  files: Record<string, string>;
}

const sha256 = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");

/// Refuse a release root that breaks the rules; its release.json otherwise.
export function checkRelease(root: string): Release {
  const release = readRelease(root);
  checkTree(root, RELEASE_JSON, release.files);
  return release;
}

/// The rules over a release root whose manifest (at `manifest`, itself
/// unlisted) lists `files`.
function checkTree(root: string, manifest: string, files: Record<string, string>): void {
  const found = new Set<string>();
  const strays: string[] = [];
  const walk = (rel: string) => {
    for (const name of readdirSync(join(root, rel)).sort()) {
      const path = rel === "" ? name : `${rel}/${name}`;
      if (rel === "" && UNCHECKED_ROOT.has(name)) continue;
      const st = lstatSync(join(root, path));
      found.add(path);
      if (name.startsWith(".")) strays.push(`${path} (a dotfile)`);
      else if (st.isSymbolicLink()) strays.push(`${path} (a symlink)`);
      else if (st.isDirectory()) walk(path);
      else if (!st.isFile()) strays.push(`${path} (not a regular file)`);
      else if (path === manifest) continue;
      else if (files[path] === undefined) strays.push(`${path} (not in release.json)`);
      else if (sha256(readFileSync(join(root, path))) !== files[path]) strays.push(`${path} (changed since the cut)`);
    }
  };
  walk("");
  for (const path of Object.keys(files)) if (!found.has(path)) strays.push(`${path} (missing)`);
  if (strays.length) {
    throw new AdminError(`${root} is not the release it was cut as: ${strays.join(", ")}`);
  }
}

/// deploy/release.json of the release at `root`, unchecked.
export function readRelease(root: string): Release {
  let doc: Release;
  try {
    doc = JSON.parse(readFileSync(join(root, RELEASE_JSON), "utf8"));
  } catch {
    throw new AdminError(`${root} has no readable ${RELEASE_JSON}: not a release directory`);
  }
  if (typeof doc?.files !== "object" || !/^[0-9a-f]{64}$/.test(doc.server_sha256 ?? "")) {
    throw new AdminError(`${join(root, RELEASE_JSON)} is not a release.json cut-release wrote`);
  }
  return doc;
}

export interface StageInput {
  /// A checkout of `commit` whose files the release takes.
  tree: string;
  commit: string;
  tag: string;
  buildUnix: number;
  /// The operator's cells files, <stage>.env for each stage it deploys.
  cells: string;
  server: string;
  s3lite: string;
  entryJs: string;
  /// deploy/composer's install, moved into the release.
  nodeModules: string;
  out: string;
}

/// Stage a release at `out`, which must not exist.
export function stageRelease(input: StageInput): Release {
  const { tree, commit, out } = input;
  if (existsSync(out)) throw new AdminError(`${out} exists: a release directory is never overwritten`);
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(input.tag)) throw new AdminError(`tag ${input.tag} is not [A-Za-z0-9._-]`);
  const files = new Map<string, Uint8Array>();
  for (const [path, repoPath] of [...REPO_FILES, ...profilePaths(tree, commit)]) {
    files.set(path, committedFile(tree, commit, repoPath));
  }
  const cells = cellsFiles(input.cells);
  for (const [stage, bytes] of cells) files.set(`deploy/cells/${stage}.env`, bytes);
  files.set(SERVER, x86Elf(input.server));
  files.set(S3LITE, x86Elf(input.s3lite));
  files.set(ENTRY_JS, readFileSync(input.entryJs));
  const pkg = JSON.parse(new TextDecoder().decode(files.get("package.json")));
  const release: Release = {
    tag: input.tag,
    commit,
    build_unix: input.buildUnix,
    server_sha256: sha256(files.get(SERVER) as Uint8Array),
    server_bytes: (files.get(SERVER) as Uint8Array).length,
    s3lite_sha256: sha256(files.get(S3LITE) as Uint8Array),
    profiles: Object.fromEntries(
      [...files].filter(([p]) => p.startsWith("deploy/profiles/")).map(([p, b]) => [p.slice(16), sha256(b)]),
    ),
    cells: Object.fromEntries([...cells].map(([stage, b]) => [stage, sha256(b)])),
    layout_version: layoutVersion(committedText(tree, commit, "src/registry.rs")),
    keys_contract: keysContract(
      committedText(tree, commit, "contracts/streams-platform/v1/keys.schema.json"),
      committedText(tree, commit, "docs/MULTITENANCY.md"),
    ),
    composer_version: exactPin(pkg, "@prisma/composer"),
    compute_version: exactPin(pkg, "@prisma/compute"),
    lockfile_sha256: sha256(files.get("bun.lock") as Uint8Array),
    files: Object.fromEntries([...files].map(([p, b]) => [p, sha256(b)]).sort(([a], [b]) => (a < b ? -1 : 1))),
  };
  for (const [path, bytes] of files) {
    mkdirSync(dirname(join(out, path)), { recursive: true });
    writeFileSync(join(out, path), bytes);
    chmodSync(join(out, path), path === SERVER || path === S3LITE ? 0o755 : 0o644);
  }
  writeFileSync(join(out, RELEASE_JSON), `${JSON.stringify(release, null, 2)}\n`, { mode: 0o644 });
  renameSync(input.nodeModules, join(out, "node_modules"));
  return checkRelease(out);
}

/// deploy/profiles/*.env at the commit.
function profilePaths(tree: string, commit: string): [string, string][] {
  const names = git(tree, ["ls-tree", "--name-only", `${commit}:${PROFILES_DIR}`]).toString().split("\n");
  const envs = names.filter((n) => n.endsWith(".env")).sort();
  if (envs.length === 0) throw new AdminError(`${commit} has no ${PROFILES_DIR}/*.env`);
  return envs.map((n) => [`${PROFILES_DIR}/${n}`, `${PROFILES_DIR}/${n}`]);
}

function git(tree: string, args: string[]): Buffer {
  try {
    return execFileSync("git", ["-C", tree, ...args], { stdio: ["ignore", "pipe", "pipe"], maxBuffer: 1 << 26 });
  } catch (e) {
    throw new AdminError(`git ${args.join(" ")} in ${tree} failed: ${String((e as { stderr?: Buffer }).stderr ?? e).trim()}`);
  }
}

const committedText = (tree: string, commit: string, path: string) =>
  git(tree, ["show", `${commit}:${path}`]).toString("utf8");

/// `path` as the tree holds it, which must be a regular file byte-identical
/// to the commit's.
function committedFile(tree: string, commit: string, path: string): Uint8Array {
  const at = join(tree, path);
  if (!existsSync(at) || !lstatSync(at).isFile()) throw new AdminError(`${at} is not a regular file`);
  const bytes = readFileSync(at);
  if (!bytes.equals(git(tree, ["show", `${commit}:${path}`]))) {
    throw new AdminError(`${at} differs from ${path} at ${commit}: a release takes the commit's files only`);
  }
  return bytes;
}

/// <stage>.env for each stage the cells directory holds, as space-free
/// KEY=VALUE files naming their own stage.
function cellsFiles(dir: string): Map<string, Uint8Array> {
  const out = new Map<string, Uint8Array>();
  for (const stage of STAGES) {
    const path = join(dir, `${stage}.env`);
    if (!existsSync(path) && !isLink(path)) continue;
    if (!lstatSync(path).isFile()) throw new AdminError(`${path} is not a regular file`);
    const bytes = readFileSync(path);
    const env = parseProfiles([{ path, text: bytes.toString("utf8") }]);
    if (env.get("CELL_STAGE") !== stage) throw new AdminError(`${path} does not set CELL_STAGE=${stage}`);
    out.set(stage, bytes);
  }
  if (out.size === 0) throw new AdminError(`${dir} holds no cells file (${STAGES.map((s) => `${s}.env`).join(", ")})`);
  return out;
}

function isLink(path: string): boolean {
  try {
    return lstatSync(path).isSymbolicLink();
  } catch {
    return false;
  }
}

/// The file at `path`, which must be an ELF for x86_64.
function x86Elf(path: string): Uint8Array {
  const bytes = readFileSync(path);
  if (bytes.length < 20 || !bytes.subarray(0, 4).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46]))) {
    throw new AdminError(`${path} is not an ELF executable`);
  }
  const machine = bytes.readUInt16LE(18);
  if (machine !== ELF_X86_64) throw new AdminError(`${path} is an ELF for machine 0x${machine.toString(16)}, not x86_64 (0x3e)`);
  return bytes;
}

export function layoutVersion(registryRs: string): number {
  const found = [...registryRs.matchAll(/^pub\(crate\) const LAYOUT_VERSION: u32 = ([0-9]+);$/gm)];
  if (found.length !== 1) throw new AdminError("src/registry.rs does not declare LAYOUT_VERSION once");
  return Number(found[0][1]);
}

/// The keys feed's contract revision: the one the keys schema's `aud`
/// cites (MULTITENANCY §14.1 rN), which MULTITENANCY's revision log must
/// list as a §14.1 revision.
export function keysContract(keysSchema: string, multitenancy: string): string {
  const aud = JSON.parse(keysSchema)?.properties?.keys?.items?.properties?.aud?.description;
  const cited = typeof aud === "string" ? [...aud.matchAll(/MULTITENANCY §14\.1 (r[0-9]+)/g)] : [];
  if (cited.length !== 1) throw new AdminError("the keys schema's aud does not cite one MULTITENANCY §14.1 revision");
  const rev = cited[0][1];
  // An entry ends at the next entry or at the blank line that ends the log,
  // so the last entry never takes in the sections after the log.
  const entry = multitenancy
    .split(/\n(?=- r[0-9]+ \()/)
    .map((e) => e.split("\n\n")[0])
    .find((e) => e.startsWith(`- ${rev} (`));
  if (!entry?.includes("§14.1")) throw new AdminError(`MULTITENANCY's revision log has no ${rev} entry for §14.1`);
  return rev;
}

function exactPin(pkg: { dependencies?: Record<string, string> }, name: string, where = "deploy/composer"): string {
  const v = pkg.dependencies?.[name];
  if (typeof v !== "string" || !/^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$/.test(v)) {
    throw new AdminError(`${where}/package.json pins ${name} at ${v}, not an exact version`);
  }
  return v;
}

/// The interim issuer's release directory (owner default Q6 (b); the
/// issuer triage's B6): <releases>/<tag>-issuer, cut with the cell's from
/// the same commit, the issuer app's Composer project root:
///   package.json, bun.lock, prisma.config.ts, app.ts  deploy/composer-issuer's
///   issuer/service.ts, issuer/entry.js                the service's build dir, which Composer ships
///   stages/<stage>.env                                the stage's settings: the envParams
///                                                     `prisma deploy` reads from its environment
///   release.json                                      IssuerRelease
///   node_modules/
/// under the same rules as the cell's.
export const ISSUER_RELEASE_JSON = "release.json";
const ISSUER_FILES: readonly (readonly [string, string])[] = [
  ["package.json", "deploy/composer-issuer/package.json"],
  ["bun.lock", "deploy/composer-issuer/bun.lock"],
  ["prisma.config.ts", "deploy/composer-issuer/prisma.config.ts"],
  ["app.ts", "deploy/composer-issuer/app.ts"],
  ["issuer/service.ts", "deploy/composer-issuer/issuer/service.ts"],
];
/// The prisma dev mock identity provider's setting: a deployed issuer has none.
const DEV_ONLY_SETTING = "STREAMS_ISSUER_OAUTH_AUTHORITY";

export interface IssuerRelease {
  app: "issuer";
  tag: string;
  commit: string;
  /// stage -> sha256 of stages/<stage>.env.
  stages: Record<string, string>;
  composer_version: string;
  lockfile_sha256: string;
  files: Record<string, string>;
}

export type IssuerStageInput = Omit<StageInput, "server" | "s3lite" | "buildUnix">;

/// The envParam names an app's app.ts binds, in its order.
export const envParamNames = (appTs: string) => [...appTs.matchAll(/^\s*\w+: envParam\("([A-Z0-9_]+)"\),$/gm)].map((m) => m[1]);

/// A stage's issuer settings (<stage>.issuer.env): space-free KEY=VALUE,
/// each key once, only the envParams the issuer's app binds, every one its
/// service does not mark optional, and never the dev-only one.
export function issuerSettings(path: string, text: string, appTs: string, serviceTs: string): Map<string, string> {
  const env = parseProfiles([{ path, text }]);
  const keys = text.split("\n").map((l) => l.trim()).filter((l) => l !== "" && !l.startsWith("#")).map((l) => l.slice(0, l.indexOf("=")));
  const twice = keys.find((k, i) => keys.indexOf(k) !== i);
  if (twice) throw new AdminError(`${path} sets ${twice} twice`);
  const params = envParamNames(appTs);
  const optional = new Set([...serviceTs.matchAll(/^\s*"([A-Z0-9_]+)\?":/gm)].map((m) => m[1]));
  for (const key of env.keys()) {
    if (!params.includes(key)) throw new AdminError(`${path} sets ${key}, which is not one of the issuer's settings`);
  }
  if (env.has(DEV_ONLY_SETTING)) throw new AdminError(`${path} sets ${DEV_ONLY_SETTING}, prisma dev's mock identity provider`);
  const missing = params.filter((p) => !optional.has(p) && !env.has(p));
  if (missing.length) throw new AdminError(`${path} does not set ${missing.join(", ")}`);
  return env;
}

/// Stage the issuer's release at `out`, which must not exist; its stages
/// are the cells directory's <stage>.issuer.env files.
export function stageIssuerRelease(input: IssuerStageInput): IssuerRelease {
  const { tree, commit, out } = input;
  if (existsSync(out)) throw new AdminError(`${out} exists: a release directory is never overwritten`);
  const files = new Map<string, Uint8Array>();
  for (const [path, repoPath] of ISSUER_FILES) files.set(path, committedFile(tree, commit, repoPath));
  files.set("issuer/entry.js", readFileSync(input.entryJs));
  const [appTs, serviceTs] = ["app.ts", "issuer/service.ts"].map((p) => new TextDecoder().decode(files.get(p)));
  const stages: Record<string, string> = {};
  for (const stage of STAGES) {
    const path = join(input.cells, `${stage}.issuer.env`);
    if (!existsSync(path) && !isLink(path)) continue;
    if (!lstatSync(path).isFile()) throw new AdminError(`${path} is not a regular file`);
    const bytes = readFileSync(path);
    issuerSettings(path, bytes.toString("utf8"), appTs, serviceTs);
    files.set(`stages/${stage}.env`, bytes);
    stages[stage] = sha256(bytes);
  }
  const pkg = JSON.parse(new TextDecoder().decode(files.get("package.json")));
  const release: IssuerRelease = {
    app: "issuer",
    tag: input.tag,
    commit,
    stages,
    composer_version: exactPin(pkg, "@prisma/composer", "deploy/composer-issuer"),
    lockfile_sha256: sha256(files.get("bun.lock") as Uint8Array),
    files: Object.fromEntries([...files].map(([p, b]) => [p, sha256(b)]).sort(([a], [b]) => (a < b ? -1 : 1))),
  };
  for (const [path, bytes] of files) {
    mkdirSync(dirname(join(out, path)), { recursive: true });
    writeFileSync(join(out, path), bytes, { mode: 0o644 });
  }
  writeFileSync(join(out, ISSUER_RELEASE_JSON), `${JSON.stringify(release, null, 2)}\n`, { mode: 0o644 });
  renameSync(input.nodeModules, join(out, "node_modules"));
  return checkIssuerRelease(out);
}

/// Refuse an issuer release root that breaks the rules; its release.json
/// otherwise.
export function checkIssuerRelease(root: string): IssuerRelease {
  let release: IssuerRelease;
  try {
    release = JSON.parse(readFileSync(join(root, ISSUER_RELEASE_JSON), "utf8"));
  } catch {
    throw new AdminError(`${root} has no readable ${ISSUER_RELEASE_JSON}: not an issuer release directory`);
  }
  if (release?.app !== "issuer" || typeof release.files !== "object") {
    throw new AdminError(`${join(root, ISSUER_RELEASE_JSON)} is not an issuer release.json cut-release wrote`);
  }
  checkTree(root, ISSUER_RELEASE_JSON, release.files);
  return release;
}

function stageIssuerCli(argv: string[]): void {
  const names = ["tree", "commit", "tag", "cells", "entry-js", "node-modules", "out"];
  const f = parseFlags(argv, { single: names, required: names });
  const r = stageIssuerRelease({
    tree: f.must("tree"),
    commit: f.must("commit"),
    tag: f.must("tag"),
    cells: f.must("cells"),
    entryJs: f.must("entry-js"),
    nodeModules: f.must("node-modules"),
    out: f.must("out"),
  });
  console.log(`staged the issuer of ${r.tag} (${r.commit}): stages ${Object.keys(r.stages).join(", ") || "none"}, ` +
    `${Object.keys(r.files).length} files`);
}

if (import.meta.main) {
  const [command, ...argv] = process.argv.slice(2);
  try {
    if (command === "stage-issuer") {
      stageIssuerCli(argv);
      process.exit(0);
    }
    if (command !== "stage") throw new AdminError("usage: release.ts stage|stage-issuer --tree DIR ... (see the header)", 2);
    const f = parseFlags(argv, {
      single: ["tree", "commit", "tag", "build-unix", "cells", "server", "s3lite", "entry-js", "node-modules", "out"],
      required: ["tree", "commit", "tag", "build-unix", "cells", "server", "s3lite", "entry-js", "node-modules", "out"],
    });
    const r = stageRelease({
      tree: f.must("tree"),
      commit: f.must("commit"),
      tag: f.must("tag"),
      buildUnix: f.count("build-unix", 0),
      cells: f.must("cells"),
      server: f.must("server"),
      s3lite: f.must("s3lite"),
      entryJs: f.must("entry-js"),
      nodeModules: f.must("node-modules"),
      out: f.must("out"),
    });
    console.log(`staged ${r.tag} (${r.commit}): server sha256 ${r.server_sha256}, layout ${r.layout_version}, ` +
      `keys ${r.keys_contract}, stages ${Object.keys(r.cells).join(", ")}, ${Object.keys(r.files).length} files`);
  } catch (e) {
    console.error(`release: ${e instanceof AdminError ? e.message : `unexpected failure: ${(e as Error).message}`}`);
    process.exit(e instanceof AdminError ? e.code : 1);
  }
}
