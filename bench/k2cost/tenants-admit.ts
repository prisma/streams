#!/usr/bin/env bun
// tenants-admit: a tenants manifest's projects onto a cell-admin cell, and
// their tokens (deploy-q5 triage B7: "admission at scale"). Operator side:
// it runs where the cell's signing key is, never inside a generator.
//
//   bun bench/k2cost/tenants-admit.ts admit --manifest M --state DIR --key-file K
//       [--cell-id C --deployment-project P --account A [--issuer URL] [--profile F]...]
//   bun bench/k2cost/tenants-admit.ts tokens --manifest M --state DIR --key-file K --out MAP
//       [--ttl 43200] [--every 28800] [--upload s3://BUCKET/KEY --s3-endpoint URL --s3-creds FILE]
//   bun bench/k2cost/tenants-admit.ts publish --state DIR --s3-endpoint URL --s3-creds FILE
//       --upload s3://BUCKET/KEY
//   bun bench/k2cost/tenants-admit.ts feeds --state DIR --out FEEDS_DIR
//
// admit runs cell-admin's own CLI (deploy/cell-admin/cell-admin.ts main):
// `init` when DIR holds no cell yet (with the ids given), then `admit`
// for every project of the manifest the state does not hold, with its
// workspace and its class's quota set as --quota flags (none: the cell's
// ceilings). A rerun admits only what is missing; no cell-admin command
// grows for this. Publish the bundle afterwards (publish, or the operator's
// own upload): a project is served once the cell has its policy.
//
// tokens mints one customer token per project with deploy/cell-admin's
// tokens.ts (12 h, TOKEN_TTL_SECS) into a token map, the JSON list
// [{"project", "token"}] that tenants.ts reads, written atomically with
// mode 0600 and never printed. With --every it re-mints every 8 h
// (REFRESH_SECS) until stopped, so tokens never come within 4 h of expiry;
// with --upload it puts each map into the bucket and reads its first 16
// bytes back, as bench/soak/wc-ladder.sh uploads its token map. S3
// credentials come only from a 0600 JSON file {"accessKeyId",
// "secretAccessKey"}.
//
// publish uploads the state's feeds-bundle.json the same way: for a rig
// whose cell's wrapper polls FEEDS_S3_KEY (run-shared.sh).
//
// feeds writes the bundle's three feeds into FEEDS_DIR as keys.json,
// policies.json and grants.json, each atomically, as deploy/app-server
// materializes them from the bundle (feeds.ts `materialize`): for a rig
// that runs the binary natively, with no wrapper, and points its
// STREAMS_AUTH_{KEYS,POLICY,GRANTS}_FILE there (run-local.sh's tenants
// mode). Rerun it after an admission to publish that admission.

import { S3Client } from "bun";
import { mkdirSync, readFileSync, renameSync, statSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { main as cellAdmin } from "../../deploy/cell-admin/cell-admin.ts";
import { loadChecked } from "../../deploy/cell-admin/publish.ts";
import { BUNDLE_FILE, type CellState, STATE_FILE, writeDurable } from "../../deploy/cell-admin/state.ts";
import { DirStore } from "../../deploy/cell-admin/store.ts";
import { cellSigningKey, customerClaims, signJwt } from "../../deploy/cell-admin/tokens.ts";
import { Opts, installSignals, parseFlags, sleep, stop } from "./common.ts";
import { expand, readManifest, type Plan } from "./tenant-plan.ts";

/** 12 h tokens, re-minted every 8 h (deploy-q5 design §3.2). */
export const TOKEN_TTL_SECS = 12 * 3600;
export const REFRESH_SECS = 8 * 3600;
const SUB = "tenants-harness";

type Say = (line: string) => void;

// ---------------------------------------------------------------- admit

/** The cell's state as cell-admin reads it: through its directory store,
 *  refused when it is older than its published bundle (a stale restore). */
async function readCell(dir: string): Promise<CellState> {
  return (await loadChecked(new DirStore(resolve(dir)))).state;
}

/** cell-admin's CLI, in-process; its output goes to `say` prefixed. */
async function cli(argv: string[], say: Say): Promise<number> {
  const err: string[] = [];
  const code = await cellAdmin(argv, { out: () => {}, err: (l) => err.push(l) });
  if (code !== 0) say(`cell-admin ${argv[0]}: exit ${code}: ${err.join(" | ")}`);
  return code;
}

export async function admit(plan: Plan, o: Opts, say: Say): Promise<{ admitted: number; present: number; failed: string[] }> {
  const dir = resolve(o.str("state")!);
  let fresh = false;
  try {
    statSync(join(dir, STATE_FILE));
  } catch {
    fresh = true;
  }
  if (fresh) {
    const ids = ["cell-id", "deployment-project", "account"].map((k) => o.str(k));
    if (ids.some((v) => !v)) throw new Error(`${dir} holds no cell: give --cell-id, --deployment-project and --account to init it`);
    const argv = ["init", "--state", dir, "--cell-id", ids[0]!, "--deployment-project", ids[1]!, "--account", ids[2]!,
      "--key-file", o.str("key-file")!, "--max-projects", String(Math.max(1000, plan.projects.length))];
    if (o.has("issuer")) argv.push("--issuer", o.str("issuer")!);
    for (const p of o.flags.get("profile")?.split(",") ?? []) argv.push("--profile", p);
    if ((await cli(argv, say)) !== 0) throw new Error("cell-admin init failed");
    say(`initialised cell ${ids[0]} in ${dir}`);
  }
  const held = new Set(Object.keys((await readCell(dir)).projects));
  let admitted = 0;
  const failed: string[] = [];
  for (const p of plan.projects) {
    if (held.has(p.id)) continue;
    const argv = ["admit", "--state", dir, "--project", p.id, "--workspace", p.workspace];
    for (const q of p.quotaFlags) argv.push("--quota", q);
    if ((await cli(argv, say)) === 0) admitted++;
    else failed.push(p.id);
    if (stop.signal.aborted) break;
  }
  return { admitted, present: plan.projects.filter((p) => held.has(p.id)).length, failed };
}

// ---------------------------------------------------------------- tokens

interface Upload {
  client: S3Client;
  key: string;
}

/** The 0600 credentials file's S3 client for an s3://BUCKET/KEY target. */
export function uploadTarget(o: Opts): Upload | null {
  const url = o.str("upload");
  if (!url) return null;
  const m = /^s3:\/\/([^/]+)\/(.+)$/.exec(url);
  if (!m) throw new Error(`--upload ${url} is not s3://BUCKET/KEY`);
  const credsFile = o.str("s3-creds") ?? (() => { throw new Error("--upload needs --s3-creds FILE"); })();
  const mode = statSync(credsFile).mode & 0o077;
  if (mode !== 0) throw new Error(`--s3-creds ${credsFile}: group or others can read it; chmod 600 it`);
  const creds = JSON.parse(readFileSync(credsFile, "utf8")) as { accessKeyId: string; secretAccessKey: string };
  const endpoint = o.str("s3-endpoint") ?? (() => { throw new Error("--upload needs --s3-endpoint URL"); })();
  return { client: new S3Client({ endpoint, bucket: m[1]!, region: "auto", ...creds }), key: m[2]! };
}

/** PUT, then read the first 16 bytes back (wc-ladder.sh's check). */
export async function putVerified(up: Upload, data: string): Promise<void> {
  await up.client.write(up.key, data);
  const head = new Uint8Array(await up.client.file(up.key).slice(0, 16).arrayBuffer());
  if (Buffer.compare(Buffer.from(head), Buffer.from(data).subarray(0, 16)) !== 0) {
    throw new Error(`s3 ${up.key}: the first 16 bytes read back differ from those written`);
  }
}

/** One token per project of the manifest, from its customer credential. */
export async function mintMap(plan: Plan, stateDir: string, keyFile: string, ttl: number): Promise<Array<{ project: string; token: string }>> {
  const state = await readCell(stateDir);
  const sk = cellSigningKey(state, keyFile);
  const byProject = new Map<string, string>();
  for (const [id, c] of Object.entries(state.credentials)) {
    if (c.role === "customer" && c.status === "active" && !c.omitted) byProject.set(c.project_id, id);
  }
  return plan.projects.map((p) => {
    const cred = byProject.get(p.id);
    if (!cred) throw new Error(`project ${p.id} has no active customer credential on cell ${state.cell.cell_id}: run admit`);
    return { project: p.id, token: signJwt(sk, customerClaims(state, cred, ttl, SUB)) };
  });
}

export async function tokens(plan: Plan, o: Opts, say: Say, signal: AbortSignal): Promise<number> {
  const [state, keyFile, out] = [o.str("state")!, o.str("key-file")!, resolve(o.str("out")!)];
  const ttl = o.int("ttl", TOKEN_TTL_SECS);
  const every = o.has("every") ? o.num("every", REFRESH_SECS) : 0;
  if (every && every * 1.5 > ttl) throw new Error(`--every ${every} s leaves under a third of the ${ttl} s token life`);
  const up = uploadTarget(o);
  let rounds = 0;
  do {
    const map = await mintMap(plan, state, keyFile, ttl);
    const text = JSON.stringify(map);
    writeDurable(out, text);
    if (up) await putVerified(up, text);
    rounds++;
    say(`token map ${out}: ${map.length} tokens, ${ttl} s${up ? `, uploaded to ${up.key}` : ""} (round ${rounds})`);
    if (every) await sleep(every * 1000, signal);
  } while (every && !signal.aborted);
  return rounds;
}

// ---------------------------------------------------------------- feeds

/** The feeds a bundle carries, by the file each one is materialized to. */
export const FEED_NAMES = ["keys", "policies", "grants"] as const;

/** The state's bundle as three feed files in `outDir`, each replaced
 * atomically (tmp + rename), refused before any is written when the bundle
 * lacks one; the keys feed's version. */
export function writeFeeds(stateDir: string, outDir: string): number {
  const bundle = JSON.parse(readFileSync(join(resolve(stateDir), BUNDLE_FILE), "utf8")) as Record<string, unknown>;
  const missing = FEED_NAMES.filter((f) => typeof bundle[f] !== "object" || bundle[f] === null);
  if (missing.length) throw new Error(`${stateDir}'s ${BUNDLE_FILE} has no ${missing.join(", ")} feed`);
  mkdirSync(outDir, { recursive: true });
  for (const name of FEED_NAMES) {
    const path = join(outDir, `${name}.json`);
    writeFileSync(`${path}.tmp`, JSON.stringify(bundle[name]));
    renameSync(`${path}.tmp`, path);
  }
  return (bundle.keys as { feed_version: number }).feed_version;
}

// ---------------------------------------------------------------- main

const USAGE = `usage: tenants-admit.ts <admit|tokens|publish|feeds> [flags]
  admit    --manifest M --state DIR --key-file K [--cell-id C --deployment-project P --account A
           [--issuer URL] [--profile F,F]]
  tokens   --manifest M --state DIR --key-file K --out MAP [--ttl ${TOKEN_TTL_SECS}] [--every ${REFRESH_SECS}]
           [--upload s3://BUCKET/KEY --s3-endpoint URL --s3-creds FILE]
  publish  --state DIR --upload s3://BUCKET/KEY --s3-endpoint URL --s3-creds FILE
  feeds    --state DIR --out FEEDS_DIR
`;

export async function main(argv: string[], say: Say = (l) => console.log(l)): Promise<number> {
  const { positional, flags } = parseFlags(argv);
  const cmd = positional[0] ?? "";
  const o = new Opts(flags);
  const need: Record<string, string[]> = {
    admit: ["manifest", "state", "key-file"], tokens: ["manifest", "state", "key-file", "out"], publish: ["state", "upload"],
    feeds: ["state", "out"],
  };
  if (!need[cmd] || positional.length > 1 || need[cmd]!.some((k) => !o.has(k))) {
    process.stderr.write(USAGE);
    return 2;
  }
  if (cmd === "publish") {
    const up = uploadTarget(o)!;
    const bundle = readFileSync(join(resolve(o.str("state")!), "feeds-bundle.json"), "utf8");
    await putVerified(up, bundle);
    say(`published feed_version ${JSON.parse(bundle).keys.feed_version} to ${up.key}`);
    return 0;
  }
  if (cmd === "feeds") {
    const out = resolve(o.str("out")!);
    say(`feeds ${out}: feed_version ${writeFeeds(o.str("state")!, out)}`);
    return 0;
  }
  const { manifest, sha256 } = readManifest(o.str("manifest")!);
  const plan = expand(manifest, sha256);
  if (cmd === "admit") {
    const r = await admit(plan, o, say);
    say(`admit: ${r.admitted} admitted, ${r.present} already held, ${r.failed.length} failed of ${plan.projects.length}`);
    return r.failed.length ? 1 : 0;
  }
  await tokens(plan, o, say, stop.signal);
  return 0;
}

if (import.meta.main) {
  installSignals();
  try {
    process.exit(await main(process.argv.slice(2)));
  } catch (e) {
    process.stderr.write(`tenants-admit: ${(e as Error).message}\n`);
    process.exit(1);
  }
}
