// init, admit, token, list and bundle. offboard lives in offboard.ts.
import { randomBytes } from "node:crypto";
import { existsSync } from "node:fs";
import { join, resolve } from "node:path";
import type { Flags } from "./args";
import { AdminError } from "./errors";
import { bundleText } from "./feeds";
import {
  BOUNDED_FIELDS,
  type BoundedField,
  cellSizing,
  QUOTA_FIELDS,
  type QuotaField,
  type Quotas,
  readProfiles,
} from "./profile";
import {
  BUNDLE_FILE,
  type CellState,
  createStateDir,
  denyForever,
  journal,
  loadState,
  readDenylist,
  STATE_FORMAT,
  saveState,
  withLock,
  writeDurable,
} from "./state";
import { cellSigningKey, customerClaims, loadSigningKey, signJwt } from "./tokens";

export interface Io {
  out: (line: string) => void;
}

/// The id grammar of the policy feed and the server (src/tenant.rs).
const ID = /^[A-Za-z0-9_-]{1,128}$/;
/// The reserved system project (src/tenant.rs, SYSTEM_PROJECT).
export const SYSTEM_PROJECT = "system";
/// Every scope the server defines (src/tenant.rs, Scope::as_str); a
/// design partner's credential gets all of them unless --scopes narrows it.
export const CUSTOMER_SCOPES = [
  "streams.metadata.read",
  "streams.records.read",
  "streams.records.append",
  "streams.create",
  "streams.lifecycle.manage",
  "streams.consumers.pull",
  "streams.consumers.settle",
  "streams.consumers.configure",
  "streams.forks.create",
  "streams.dlq.configure",
  "streams.watches.manage",
  "streams.catalog.read",
  "streams.usage.read",
] as const;
const PROFILE_DIR = join(import.meta.dir, "..", "profiles");
export const DEFAULT_PROFILES = [join(PROFILE_DIR, "compute-1g.env"), join(PROFILE_DIR, "shared-cell.env")];

function checkId(what: string, id: string): void {
  if (!ID.test(id)) throw new AdminError(`${what} ${JSON.stringify(id)} is not a valid id (${ID.source})`, 2);
}

export async function init(f: Flags, io: Io): Promise<void> {
  const dir = resolve(f.must("state"));
  const cell = f.must("cell-id");
  const deployment = f.must("deployment-project");
  const account = f.must("account");
  checkId("--cell-id", cell);
  checkId("--deployment-project", deployment);
  checkId("--account", account);
  if (deployment === "proj_local" || deployment === SYSTEM_PROJECT || account === "acct_local") {
    throw new AdminError("a shared cell needs a real PROJECT_ID and ACCOUNT_ID, not a placeholder");
  }
  const profiles = (f.all("profile").length > 0 ? f.all("profile") : DEFAULT_PROFILES).map((p) => resolve(p));
  const sizing = cellSizing(readProfiles(profiles));
  const sk = loadSigningKey(f.must("key-file"));
  const [workspace_cap, max_projects] = [f.count("workspace-cap", 1), f.count("max-projects", 1000)];
  const denylist = resolve(f.get("denylist") ?? join(dir, "denylist.txt"));
  createStateDir(dir);
  const state: CellState = {
    format: STATE_FORMAT,
    cell: {
      cell_id: cell,
      deployment_project_id: deployment,
      account_id: account,
      issuer: f.get("issuer") ?? "https://auth.prisma.io",
      share_k: sizing.share_k,
      ceilings: sizing.ceilings,
      profiles,
      workspace_cap,
      max_projects,
      denylist,
    },
    keys: [sk.record],
    feed_version: 1,
    projects: {},
    credentials: {},
    journal: [],
  };
  await withLock(dir, async () => {
    journal(state, "init", `k=${sizing.share_k} kid=${sk.record.kid}`);
    saveState(dir, state, bundleText(state));
  });
  io.out(`cell ${cell}: k=${sizing.share_k}, key ${sk.record.kid} (${sk.record.alg}, aud prisma-streams-data)`);
  io.out(`ceilings: ${BOUNDED_FIELDS.map((q) => `${q}=${sizing.ceilings[q]}`).join(" ")}`);
  io.out(`denylist: ${denylist}`);
  io.out(`bundle: ${join(dir, BUNDLE_FILE)} (feed_version 1)`);
}

/// A cell's ceilings are fixed when it is initialised: refuse to admit
/// against ceilings the profiles no longer give.
function checkSizing(state: CellState): void {
  const now = cellSizing(readProfiles(state.cell.profiles));
  const changes = [
    ...(now.share_k !== state.cell.share_k ? [`k ${state.cell.share_k} -> ${now.share_k}`] : []),
    ...BOUNDED_FIELDS.filter((q) => now.ceilings[q] !== state.cell.ceilings[q]).map(
      (q) => `${q} ${state.cell.ceilings[q]} -> ${now.ceilings[q]}`,
    ),
  ];
  if (changes.length > 0) {
    throw new AdminError(
      `the profiles changed since init (${changes.join(", ")}): a cell's ceilings are fixed for its life`,
    );
  }
}

/// The project's quotas: every bounded axis at most its ceiling and a
/// missing one AT the ceiling; 0 is refused because a binary without the
/// cell ceiling reads it as "no limit". The two unbounded axes keep the
/// value given, 0 (no project limit) when absent, as the server does.
export function admittedQuotas(state: CellState, given: readonly string[]): Quotas {
  const quotas = Object.fromEntries(QUOTA_FIELDS.map((q) => [q, 0])) as Quotas;
  const seen = new Set<string>();
  for (const item of given) {
    const m = /^([a-z_]+)=([0-9]+)$/.exec(item);
    if (!m) throw new AdminError(`--quota ${item} is not field=value`, 2);
    const [, field, raw] = m;
    if (!(QUOTA_FIELDS as readonly string[]).includes(field)) {
      throw new AdminError(`--quota ${field}: not a quota field (${QUOTA_FIELDS.join(", ")})`, 2);
    }
    if (seen.has(field)) throw new AdminError(`--quota ${field} given twice`, 2);
    seen.add(field);
    const value = Number(raw);
    if (!Number.isSafeInteger(value)) throw new AdminError(`--quota ${field}=${raw} is out of range`, 2);
    if (value < 1) {
      throw new AdminError(`--quota ${field}=${raw}: 0 means no limit; omit the field to take the ceiling`);
    }
    const ceiling = state.cell.ceilings[field as BoundedField];
    if (ceiling !== undefined && value > ceiling) {
      throw new AdminError(
        `--quota ${field}=${value} exceeds the cell's ceiling ${ceiling} (bound / k, k = ${state.cell.share_k})`,
      );
    }
    quotas[field as QuotaField] = value;
  }
  for (const q of BOUNDED_FIELDS) if (!seen.has(q)) quotas[q] = state.cell.ceilings[q];
  return quotas;
}

function checkPlacement(state: CellState, project: string, workspace: string): void {
  checkId("--project", project);
  checkId("--workspace", workspace);
  const cell = state.cell;
  if (project === SYSTEM_PROJECT || project === cell.deployment_project_id) {
    throw new AdminError(`project ${project} is reserved on cell ${cell.cell_id}`);
  }
  if (workspace === cell.account_id) {
    throw new AdminError(`workspace ${workspace} is the cell's ACCOUNT_ID, the unowned-event sink`);
  }
  if (state.projects[project] || readDenylist(cell.denylist).has(project)) {
    throw new AdminError(`project id ${project} was used before: an id is never placed twice`);
  }
  const placed = Object.values(state.projects).filter((p) => p.phase !== "offboarded");
  if (placed.length >= cell.max_projects) {
    throw new AdminError(`cell ${cell.cell_id} already holds its ${cell.max_projects} projects`);
  }
  const mine = placed.filter((p) => p.workspace_id === workspace).length;
  if (mine >= cell.workspace_cap) {
    throw new AdminError(
      `workspace ${workspace} already has ${mine} project(s) on cell ${cell.cell_id} (cap ${cell.workspace_cap})`,
    );
  }
}

function scopesOf(f: Flags): string {
  const raw = f.get("scopes");
  if (raw === undefined) return CUSTOMER_SCOPES.join(" ");
  const words = raw.split(" ").filter((w) => w !== "");
  const unknown = words.filter((w) => !(CUSTOMER_SCOPES as readonly string[]).includes(w));
  if (words.length === 0 || unknown.length > 0) {
    throw new AdminError(`--scopes: unknown or empty scope list (${unknown.join(" ")})`, 2);
  }
  return [...new Set(words)].join(" ");
}

function prefixesOf(f: Flags): string[] | undefined {
  const prefixes = f.all("prefix");
  if (prefixes.length === 0) return undefined;
  if (prefixes.length > 64 || prefixes.some((p) => p === "")) {
    throw new AdminError("--prefix: 1 to 64 non-empty prefixes", 2);
  }
  return prefixes;
}

export async function admit(f: Flags, io: Io): Promise<void> {
  const dir = resolve(f.must("state"));
  const project = f.must("project");
  const workspace = f.must("workspace");
  await withLock(dir, async () => {
    const state = loadState(dir);
    checkSizing(state);
    checkPlacement(state, project, workspace);
    const quotas = admittedQuotas(state, f.all("quota"));
    const scopes = scopesOf(f);
    const stream_prefixes = prefixesOf(f);
    let credential = "";
    do credential = `strcred_${randomBytes(16).toString("hex")}`;
    while (state.credentials[credential]);
    denyForever(state.cell.denylist, project);
    state.projects[project] = {
      workspace_id: workspace,
      phase: "active",
      project_policy_version: 1,
      ownership_version: 1,
      quotas,
      admitted_at: new Date().toISOString(),
    };
    state.credentials[credential] = {
      project_id: project,
      role: "customer",
      grant_version: 1,
      status: "active",
      scopes,
      ...(stream_prefixes ? { stream_prefixes } : {}),
      omitted: false,
    };
    state.feed_version += 1;
    journal(state, "admit", `${project} workspace=${workspace} credential=${credential}`);
    saveState(dir, state, bundleText(state));
    io.out(`admitted ${project} (workspace ${workspace}) with credential ${credential}`);
    io.out(`quotas: ${QUOTA_FIELDS.map((q) => `${q}=${quotas[q]}`).join(" ")}`);
    io.out(`bundle: ${join(dir, BUNDLE_FILE)} (feed_version ${state.feed_version}); publish it to the cell`);
  });
}

export async function token(f: Flags, io: Io): Promise<void> {
  const dir = resolve(f.must("state"));
  const credential = f.must("credential");
  const out = resolve(f.must("out"));
  await withLock(dir, async () => {
    const state = loadState(dir);
    const cred = state.credentials[credential];
    if (!cred || cred.role !== "customer") {
      throw new AdminError(`credential ${credential} is not a customer credential of this cell`);
    }
    if (cred.status !== "active" || cred.omitted || state.projects[cred.project_id].phase !== "active") {
      throw new AdminError(`credential ${credential} is not active (project ${cred.project_id})`);
    }
    const sk = cellSigningKey(state, f.must("key-file"));
    const ttl = Number(f.get("ttl") ?? "3600");
    const claims = customerClaims(state, credential, ttl, f.get("sub") ?? "design-partner");
    writeDurable(out, signJwt(sk, claims));
    const exp = new Date(Number(claims.exp) * 1000).toISOString();
    io.out(`token for ${cred.project_id} (credential ${credential}, kid ${sk.record.kid}) expires ${exp}`);
    io.out(`written to ${out} (mode 0600); it is a bearer credential: hand it over, never paste it`);
  });
}

export async function bundle(f: Flags, io: Io): Promise<void> {
  const dir = resolve(f.must("state"));
  await withLock(dir, async () => {
    const state = loadState(dir);
    state.feed_version += 1;
    journal(state, "bundle", "rewritten");
    saveState(dir, state, bundleText(state));
    io.out(`bundle: ${join(dir, BUNDLE_FILE)} (feed_version ${state.feed_version})`);
  });
}

export function list(f: Flags, io: Io): void {
  const dir = resolve(f.must("state"));
  const state = loadState(dir);
  if (f.has("json")) {
    const { journal: _journal, ...view } = state;
    io.out(JSON.stringify({ ...view, keys: state.keys.map((k) => ({ kid: k.kid, alg: k.alg, aud: k.aud })) }));
    return;
  }
  const c = state.cell;
  const projects = Object.entries(state.projects);
  const placed = projects.filter(([, p]) => p.phase !== "offboarded").length;
  io.out(`cell ${c.cell_id}: k=${c.share_k}, feed_version ${state.feed_version}, keys ${state.keys.map((k) => k.kid).join(",")}`);
  io.out(`reserved: PROJECT_ID ${c.deployment_project_id}, ACCOUNT_ID ${c.account_id}, ${SYSTEM_PROJECT}`);
  io.out(`ceilings: ${BOUNDED_FIELDS.map((q) => `${q}=${c.ceilings[q]}`).join(" ")}`);
  io.out(`projects: ${placed} placed of ${c.max_projects}, ${projects.length - placed} offboarded; workspace cap ${c.workspace_cap}`);
  for (const [id, p] of projects.sort(([a], [b]) => (a < b ? -1 : 1))) {
    const creds = Object.entries(state.credentials)
      .filter(([, cr]) => cr.project_id === id)
      .map(([cid, cr]) => `${cid} ${cr.role} ${cr.omitted ? "omitted" : cr.status} v${cr.grant_version}`);
    io.out(`${id} workspace=${p.workspace_id} ${p.phase} policy=v${p.project_policy_version}`);
    io.out(`  quotas: ${QUOTA_FIELDS.map((q) => `${q}=${p.quotas[q]}`).join(" ")}`);
    io.out(`  credentials: ${creds.join("; ") || "none"}`);
  }
  if (!existsSync(join(dir, BUNDLE_FILE))) io.out("bundle: missing; run cell-admin bundle");
}

