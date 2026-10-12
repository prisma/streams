// The rules every writer of a cell's state follows, cell-admin and the
// interim issuer alike (public-beta issuer triage B1.5, B1.6). Pure: each
// takes the state a `mutate` loaded and changes it, or refuses.
//
// The version-bump invariant (skeptic S5b): the cell refuses a snapshot
// that changes an entry at a version it already holds, so a published
// field of a project or a credential is written only by setProjectField
// or setCredentialField, which bump the entry's version when its
// published form changes, and feed_version once per change.
import { randomBytes } from "node:crypto";
import { AdminError } from "./errors";
import { grantEntry, policyEntry } from "./feeds";
import {
  BOUNDED_FIELDS,
  type BoundedField,
  cellSizing,
  profileDigest,
  QUOTA_FIELDS,
  type QuotaField,
  type Quotas,
  readProfiles,
} from "./profile";
import type { Actor, CellState, CredentialRecord, ProjectRecord } from "./state";

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

export function checkId(what: string, id: string): void {
  if (!ID.test(id)) throw new AdminError(`${what} ${JSON.stringify(id)} is not a valid id (${ID.source})`, 2);
}

const BUMPED = Symbol("feed_version moved in this change");

/// Move feed_version forward, once per loaded state: a change that
/// touches several entries still publishes one new generation. The mark
/// is a symbol, which JSON never writes.
export function bumpFeed(state: CellState): void {
  const marked = state as CellState & { [BUMPED]?: true };
  if (marked[BUMPED]) return;
  marked.feed_version += 1;
  marked[BUMPED] = true;
}

export type ProjectPatch = Partial<Pick<ProjectRecord, "status" | "quotas" | "display_name">>;
export type CredentialPatch = Partial<
  Pick<CredentialRecord, "status" | "scopes" | "stream_prefixes" | "expires_at" | "secret_hash" | "hv" | "hint" | "display_name">
>;

export function projectOf(state: CellState, id: string): ProjectRecord {
  const p = state.projects[id];
  if (!p) throw new AdminError(`project ${id} is not on this cell`);
  return p;
}

export function credentialOf(state: CellState, id: string): CredentialRecord {
  const c = state.credentials[id];
  if (!c) throw new AdminError(`credential ${id} is not on this cell`);
  return c;
}

/// Patch a project; returns whether its published entry changed (and so
/// its project_policy_version and feed_version moved).
export function setProjectField(state: CellState, id: string, patch: ProjectPatch): boolean {
  const p = projectOf(state, id);
  const before = JSON.stringify(policyEntry(state, id, p));
  Object.assign(p, patch);
  if (JSON.stringify(policyEntry(state, id, p)) === before) return false;
  p.project_policy_version += 1;
  bumpFeed(state);
  return true;
}

/// Patch a credential; returns whether its grant_version moved: when its
/// published entry changed, or always with `bump` (a rotated secret, so
/// tokens minted at the old version die when the feed lands).
export function setCredentialField(
  state: CellState,
  id: string,
  patch: CredentialPatch,
  opts: { bump?: boolean } = {},
): boolean {
  const c = credentialOf(state, id);
  const before = JSON.stringify(grantEntry(id, c));
  Object.assign(c, patch);
  if (!opts.bump && JSON.stringify(grantEntry(id, c)) === before) return false;
  c.grant_version += 1;
  bumpFeed(state);
  return true;
}

/// A cell's profile is fixed when it is initialised: refuse to admit
/// against ceilings the profile no longer gives, or against a profile that
/// is not the one `init` recorded. `bundledProfiles` is the caller's own
/// copy of the profile files (this tool's deploy/profiles, the issuer's
/// bundled copy of the same files), never the paths `init` read, which do
/// not exist where the issuer runs.
export function checkSizing(state: CellState, bundledProfiles: readonly string[]): void {
  const env = readProfiles(bundledProfiles);
  const now = cellSizing(env);
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
  const digest = profileDigest(env);
  if (digest !== state.cell.profile_digest) {
    throw new AdminError(
      `the layered profile differs from the one init recorded (digest ${state.cell.profile_digest.slice(0, 12)} -> ` +
        `${digest.slice(0, 12)}): a cell's profile is fixed for its life`,
    );
  }
}

/// The project's quotas: every bounded axis at most its ceiling and a
/// missing one AT the ceiling; 0 is refused because a binary without the
/// cell ceiling reads it as "no limit". The unbounded axis (append
/// records/s) keeps the value given, 0 (no project limit) when absent, as
/// the server does.
export function admittedQuotas(state: CellState, given: readonly string[]): Quotas {
  const quotas = Object.fromEntries(QUOTA_FIELDS.map((q) => [q, 0])) as Quotas;
  const set = quotaValues(state, given);
  for (const q of QUOTA_FIELDS) if (set[q] !== undefined) quotas[q] = set[q];
  for (const q of BOUNDED_FIELDS) if (set[q] === undefined) quotas[q] = state.cell.ceilings[q];
  return quotas;
}

/// The `field=value` quota settings given, each a known field, given once,
/// at least 1 and at most its ceiling.
export function quotaValues(state: CellState, given: readonly string[]): Partial<Quotas> {
  const quotas: Partial<Quotas> = {};
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
  return quotas;
}

/// Where a project may be placed: not a reserved id, not the cell's
/// ACCOUNT_ID as workspace, an id never used before (`denylist` is the
/// denylist as read before this placement burns the id), and within the
/// cell's and the workspace's caps.
export function checkPlacement(state: CellState, project: string, workspace: string, denylist: Set<string>): void {
  checkId("--project", project);
  checkId("--workspace", workspace);
  const cell = state.cell;
  if (project === SYSTEM_PROJECT || project === cell.deployment_project_id) {
    throw new AdminError(`project ${project} is reserved on cell ${cell.cell_id}`);
  }
  if (workspace === cell.account_id) {
    throw new AdminError(`workspace ${workspace} is the cell's ACCOUNT_ID, the unowned-event sink`);
  }
  if (state.projects[project] || denylist.has(project)) {
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

/// A credential's scopes from `--scopes`' words: every scope when absent,
/// else known scopes only, deduplicated.
export function scopesOf(raw: string | undefined): string {
  if (raw === undefined) return CUSTOMER_SCOPES.join(" ");
  const words = raw.split(" ").filter((w) => w !== "");
  const unknown = words.filter((w) => !(CUSTOMER_SCOPES as readonly string[]).includes(w));
  if (words.length === 0 || unknown.length > 0) {
    throw new AdminError(`--scopes: unknown or empty scope list (${unknown.join(" ")})`, 2);
  }
  return [...new Set(words)].join(" ");
}

/// A credential's stream prefixes: none, or 1 to 64 non-empty ones.
export function prefixesOf(prefixes: readonly string[]): string[] | undefined {
  if (prefixes.length === 0) return undefined;
  if (prefixes.length > 64 || prefixes.some((p) => p === "")) {
    throw new AdminError("--prefix: 1 to 64 non-empty prefixes", 2);
  }
  return [...prefixes];
}

/// A credential id no record of this cell carries.
export function newCredentialId(state: CellState): string {
  let id = "";
  do id = `strcred_${randomBytes(16).toString("hex")}`;
  while (state.credentials[id]);
  return id;
}

/// A new customer credential's identity and, for one with a secret, the
/// secret's hash (secrets.ts); the CLI's `admit` makes one without.
export interface NewCredential {
  id: string;
  secret_hash?: string;
  hv?: number;
  hint?: string;
  display_name?: string;
  created_by?: Actor;
  hidden?: boolean;
}

/// Place a project with its first customer credential: the mutation the
/// CLI's `admit` and the issuer's project create both make, after
/// checkSizing and checkPlacement and after the id is burned (burnId).
export function admitProject(
  state: CellState,
  a: {
    project: string;
    workspace: string;
    quotas: Quotas;
    scopes: string;
    stream_prefixes?: string[];
    display_name?: string;
    created_by?: Actor;
    credential: NewCredential;
  },
): void {
  if (state.projects[a.project]) throw new AdminError(`project ${a.project} is already on this cell`);
  const now = new Date().toISOString();
  state.projects[a.project] = {
    workspace_id: a.workspace,
    phase: "active",
    status: "active",
    project_policy_version: 1,
    ownership_version: 1,
    quotas: { ...a.quotas },
    admitted_at: now,
    ...(a.display_name !== undefined ? { display_name: a.display_name } : {}),
    ...(a.created_by ? { created_by: a.created_by } : {}),
  };
  addCredentialRecord(state, a.project, a.scopes, a.stream_prefixes, a.credential, now);
}

/// A new active customer credential of `project`, published at
/// grant_version 1.
function addCredentialRecord(
  state: CellState,
  project: string,
  scopes: string,
  stream_prefixes: string[] | undefined,
  cred: NewCredential,
  now: string,
): void {
  if (state.credentials[cred.id]) throw new AdminError(`credential ${cred.id} is already on this cell`);
  const { id: _id, ...rest } = cred;
  state.credentials[cred.id] = {
    project_id: project,
    role: "customer",
    grant_version: 1,
    status: "active",
    scopes,
    ...(stream_prefixes ? { stream_prefixes: [...stream_prefixes] } : {}),
    expires_at: null,
    omitted: false,
    ...rest,
    created_at: now,
  };
  bumpFeed(state);
}
