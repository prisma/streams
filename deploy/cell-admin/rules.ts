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
import { PREFIX_MAX_COUNT, prefixRefusal } from "./prefixes";
import {
  BOUNDED_FIELDS,
  type BoundedField,
  type CellSizing,
  cellSizing,
  profileDigest,
  QUOTA_FIELDS,
  type QuotaField,
  type Quotas,
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

/// The issuer's canary (B1.9; alerting correction 4): one reserved project
/// per cell, `sp_canary_<cell id>` in its own workspace (never ACCOUNT_ID,
/// which placement refuses), at 5 requests/s and 1 stream, hidden from
/// every listing. Its catalog-read credential (no secret) lets the issuer
/// and `cell-admin canary` prove the cell serves a key or a generation,
/// and offboarding a suspended project prove the cell holds its
/// revocation: that offboarding moves the credential's grant_version, so
/// a probe mints at its current version, never an earlier token's. The
/// watch's credential is added later with `credential --hidden`.
export const CANARY_WORKSPACE = "streams-issuer-canary";
const CANARY_PREFIX = "sp_canary_";
export const canaryProjectId = (cellId: string) => `${CANARY_PREFIX}${cellId}`;
export const isCanary = (projectId: string) => projectId.startsWith(CANARY_PREFIX);

/// Refused for the canary: `init` places it once, and its catalog-read
/// credential is what the issuer's probes, `cell-admin canary` and the
/// watch mint for, for the cell's life; offboarding, suspending or
/// re-quota-ing it would break them for good.
export function refuseCanary(project: string): void {
  if (!isCanary(project)) return;
  throw new AdminError(
    `project ${project} is the issuer's canary: no owner command changes it ` +
      "(its catalog-read credential proves the cell serves a key for the cell's life)",
  );
}

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
/// is not the one `init` recorded. `env` is the caller's own copy of the
/// layered profile (this tool's deploy/profiles, read by readProfiles;
/// the issuer's copy of the same files, compiled into its bundle), never
/// the paths `init` read, which do not exist where the issuer runs.
/// Answers the sizing the profile gives, for `admittedQuotas`.
export function checkSizing(state: CellState, env: Map<string, string>): CellSizing {
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
  return now;
}

/// The project's quotas: every bounded axis at most its ceiling and a
/// missing one AT the ceiling; 0 is refused because a binary without the
/// cell ceiling reads it as "no limit". Append records/s, without a bound,
/// keeps the value given, 0 (no project limit) when absent, as the server
/// does. Stored bytes, without a bound either, take any value given and
/// `storedDefault` (the profile's PROJECT_STORED_BYTES_DEFAULT, from
/// checkSizing or cellSizing) when absent, written explicitly as the
/// ceilings are.
export function admittedQuotas(
  state: CellState,
  given: readonly string[],
  storedDefault: number,
  weights: Weights = UNIT_WEIGHTS,
): Quotas {
  const quotas = Object.fromEntries(QUOTA_FIELDS.map((q) => [q, 0])) as Quotas;
  const set = quotaValues(state, given, weights);
  const ceilings = weightedCeilings(state, weights);
  for (const q of QUOTA_FIELDS) if (set[q] !== undefined) quotas[q] = set[q];
  for (const q of BOUNDED_FIELDS) if (set[q] === undefined) quotas[q] = ceilings[q];
  if (set.max_stored_bytes === undefined) quotas.max_stored_bytes = storedDefault;
  for (const w of WEIGHT_FIELDS) quotas[w] = weights[w];
  return quotas;
}

/// The policy feed's two weights (layout 6A package 1; src/auth/ceiling.rs):
/// the server makes a project's ceiling on each request axis bound x
/// request_weight / k and on the append and read byte rates bound x
/// byte_weight / k, each weight held to 1..k-1; queued_append_bytes stays
/// bound / k. Set at admission (`--weight request=N`, `--weight bytes=N`),
/// fixed for the project's life.
export const WEIGHT_FIELDS = ["request_weight", "byte_weight"] as const;
export type Weights = Record<(typeof WEIGHT_FIELDS)[number], number>;
export const UNIT_WEIGHTS: Weights = { request_weight: 1, byte_weight: 1 };

/// The `--weight` names and the weight each sets.
const WEIGHT_FLAGS: Record<string, keyof Weights> = { request: "request_weight", bytes: "byte_weight" };

/// The weight that scales each bounded axis; queued_append_bytes is a
/// committer burst buffer and keeps bound / k whatever the weights.
const AXIS_WEIGHT: Record<BoundedField, keyof Weights | null> = {
  requests_per_sec: "request_weight",
  append_bytes_per_sec: "byte_weight",
  read_bytes_per_sec: "byte_weight",
  max_inflight_requests: "request_weight",
  max_live_subscriptions: "request_weight",
  max_streams: "request_weight",
  queued_append_bytes: null,
};

/// The `--weight name=N` settings given (`request`, `bytes`), each once,
/// each 1 to k - 1; an absent one is 1.
export function weightValues(state: CellState, given: readonly string[]): Weights {
  const weights = { ...UNIT_WEIGHTS };
  const seen = new Set<string>();
  const most = state.cell.share_k - 1;
  for (const item of given) {
    const m = /^([a-z]+)=([0-9]+)$/.exec(item);
    const field = m ? WEIGHT_FLAGS[m[1]] : undefined;
    if (!m || field === undefined) throw new AdminError(`--weight ${item} is not request=N or bytes=N`, 2);
    if (seen.has(m[1])) throw new AdminError(`--weight ${m[1]} given twice`, 2);
    seen.add(m[1]);
    const value = Number(m[2]);
    if (!Number.isSafeInteger(value) || value < 1 || value > most) {
      throw new AdminError(`--weight ${m[1]}=${m[2]}: a weight is 1 to k - 1 = ${most}`);
    }
    weights[field] = value;
  }
  return weights;
}

/// The project's weights as its quotas record them (1 when absent).
export const weightsOf = (quotas: Partial<Quotas>): Weights => ({
  request_weight: quotas.request_weight || 1,
  byte_weight: quotas.byte_weight || 1,
});

/// The project's ceilings at `weights`: each bounded axis's unit ceiling
/// (bound / k) times the weight that scales it. A multiple of the floored
/// unit is never above the server's floor(bound x weight / k), so every
/// quota cell-admin publishes stays at or below the server's ceiling; a
/// unit ceiling of 1, which may be the server's floor of 1 for a bound
/// below k, is not scaled.
export function weightedCeilings(state: CellState, weights: Weights): Record<BoundedField, number> {
  return Object.fromEntries(
    BOUNDED_FIELDS.map((q) => {
      const w = AXIS_WEIGHT[q];
      const unit = state.cell.ceilings[q];
      return [q, w === null || unit === 1 ? unit : unit * weights[w]];
    }),
  ) as Record<BoundedField, number>;
}

/// The packing rule (layout 6A package 1): a project heavy on an axis
/// (weight above 1) is placed only while the weights of the cell's heavy
/// projects on that axis, its own included, sum to at most k - 1, so the
/// unit-weight projects, which are not summed (overbooked as before), keep
/// at least one k-th of every bound. And the memory lines of the
/// byte-weighted projects (the server's: min(line x w, max(line, read memory
/// - 2 x line - 1)), its own included, leave room for two unit lines in the
/// instance read memory, so it still takes four projects at their lines to
/// fill it (the owner's acceptance; on the 1 GiB profile, one byte-weighted
/// project per cell). `except` is a project whose own weights the sums
/// leave out.
export function checkPacking(
  state: CellState,
  weights: Weights,
  memory: Pick<CellSizing, "memory_line" | "read_memory">,
  except?: string,
): void {
  const most = state.cell.share_k - 1;
  for (const [flag, field] of Object.entries(WEIGHT_FLAGS)) {
    if (weights[field] <= 1) continue;
    const held = placedProjects(state)
      .filter(([id]) => id !== except)
      .map(([, p]) => weightsOf(p.quotas)[field])
      .filter((w) => w > 1)
      .reduce((a, b) => a + b, 0);
    if (held + weights[field] > most) {
      throw new AdminError(
        `--weight ${flag}=${weights[field]}: the cell's heavy projects would hold ${held + weights[field]} ` +
          `${field} units, over k - 1 = ${most} (they hold ${held})`,
      );
    }
  }
  if (weights.byte_weight <= 1) return;
  const { memory_line: line, read_memory: read } = memory;
  const lineOf = (w: number) => Math.min(line * w, Math.max(line, read - 2 * line - 1));
  const lines = placedProjects(state)
    .filter(([id]) => id !== except)
    .map(([, p]) => weightsOf(p.quotas).byte_weight)
    .filter((w) => w > 1)
    .map(lineOf)
    .reduce((a, b) => a + b, lineOf(weights.byte_weight));
  if (lines + 2 * line >= read) {
    throw new AdminError(
      `--weight bytes=${weights.byte_weight}: the byte-weighted projects' memory lines (${lines}) and two unit ` +
        `lines (${2 * line}) would fill the instance read memory (${read})`,
    );
  }
}

/// The `field=value` quota settings given, each a known field, given once,
/// at least 1 and at most its ceiling at the project's `weights` (stored
/// bytes have none).
export function quotaValues(
  state: CellState,
  given: readonly string[],
  weights: Weights = UNIT_WEIGHTS,
): Partial<Quotas> {
  const quotas: Partial<Quotas> = {};
  const ceilings = weightedCeilings(state, weights);
  const seen = new Set<string>();
  for (const item of given) {
    const m = /^([a-z_]+)=([0-9]+)$/.exec(item);
    if (!m) throw new AdminError(`--quota ${item} is not field=value`, 2);
    const [, field, raw] = m;
    if (!(QUOTA_FIELDS as readonly string[]).includes(field)) {
      throw new AdminError(`--quota ${field}: not a quota field (${QUOTA_FIELDS.join(", ")})`, 2);
    }
    if ((WEIGHT_FIELDS as readonly string[]).includes(field)) {
      throw new AdminError(`--quota ${field}: a weight is not a quota; admit sets it with --weight`, 2);
    }
    if (seen.has(field)) throw new AdminError(`--quota ${field} given twice`, 2);
    seen.add(field);
    const value = Number(raw);
    if (!Number.isSafeInteger(value)) throw new AdminError(`--quota ${field}=${raw} is out of range`, 2);
    if (value < 1) {
      throw new AdminError(
        field === "max_stored_bytes"
          ? `--quota ${field}=${raw}: omit the field to take the cell default`
          : `--quota ${field}=${raw}: 0 means no limit; omit the field to take the ceiling`,
      );
    }
    const ceiling = ceilings[field as BoundedField];
    if (ceiling !== undefined && value > ceiling) {
      const w = AXIS_WEIGHT[field as BoundedField];
      const scale = w !== null && weights[w] > 1 ? ` x ${w} ${weights[w]}` : "";
      throw new AdminError(
        `--quota ${field}=${value} exceeds the cell's ceiling ${ceiling} (bound / k${scale}, k = ${state.cell.share_k})`,
      );
    }
    quotas[field as QuotaField] = value;
  }
  return quotas;
}

/// Why a placement is refused, for a caller that answers each its own way
/// (the issuer's management API); the message is the CLI's.
export type PlacementRefusal = "reserved_project" | "reserved_workspace" | "id_used" | "cell_full" | "workspace_full";

export class PlacementRefused extends AdminError {
  constructor(
    readonly refusal: PlacementRefusal,
    message: string,
  ) {
    super(message);
  }
}

/// Where a project may be placed: not a reserved id, not the cell's
/// ACCOUNT_ID as workspace, an id never used before (`denylist` is the
/// denylist as read before this placement burns the id), and within the
/// cell's and the workspace's caps. Every project not offboarded counts
/// toward the cell's max_projects: a project being offboarded still holds
/// its streams until its drain ends. Only active projects count toward
/// the workspace's cap (the owner's answer I9 (a), 2026-10-12, for every
/// writer): a project being offboarded has its credentials revoked, so its
/// workspace may place the next one at once. The workspace's cap is the
/// cell's unless `opts.workspaceCap` names the workspace's own (the
/// issuer's allow-list entry).
export function checkPlacement(
  state: CellState,
  project: string,
  workspace: string,
  denylist: Set<string>,
  opts: { workspaceCap?: number } = {},
): void {
  checkId("--project", project);
  checkId("--workspace", workspace);
  const cell = state.cell;
  if (project === SYSTEM_PROJECT || project === cell.deployment_project_id || isCanary(project)) {
    throw new PlacementRefused("reserved_project", `project ${project} is reserved on cell ${cell.cell_id}`);
  }
  if (workspace === cell.account_id) {
    throw new PlacementRefused(
      "reserved_workspace",
      `workspace ${workspace} is the cell's ACCOUNT_ID, the unowned-event sink`,
    );
  }
  if (workspace === CANARY_WORKSPACE) {
    throw new PlacementRefused("reserved_workspace", `workspace ${workspace} is the issuer's canary workspace`);
  }
  if (state.projects[project] || denylist.has(project)) {
    throw new PlacementRefused("id_used", `project id ${project} was used before: an id is never placed twice`);
  }
  const placed = placedProjects(state).map(([, p]) => p);
  if (placed.length >= cell.max_projects) {
    throw new PlacementRefused("cell_full", `cell ${cell.cell_id} already holds its ${cell.max_projects} projects`);
  }
  const cap = opts.workspaceCap ?? cell.workspace_cap;
  const mine = placed.filter((p) => p.workspace_id === workspace && p.phase === "active").length;
  if (mine >= cap) {
    throw new PlacementRefused(
      "workspace_full",
      `workspace ${workspace} already has ${mine} project(s) on cell ${cell.cell_id} (cap ${cap})`,
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

/// A credential's stream prefixes from `--prefix`: none, or 1 to 64 that
/// the cell's grants parser takes (prefixes.ts).
export function prefixesOf(prefixes: readonly string[]): string[] | undefined {
  if (prefixes.length === 0) return undefined;
  if (prefixes.length > PREFIX_MAX_COUNT) throw new AdminError(`--prefix: 1 to ${PREFIX_MAX_COUNT} prefixes`, 2);
  for (const p of prefixes) {
    const why = prefixRefusal(p);
    if (why !== null) throw new AdminError(`--prefix ${JSON.stringify(p)} ${why}`, 2);
  }
  return [...prefixes];
}

/// A fresh credential id: `strcred_` and 32 random hex characters.
export const randomCredentialId = () => `strcred_${randomBytes(16).toString("hex")}`;

/// A credential id no record of this cell carries.
export function newCredentialId(state: CellState): string {
  let id = "";
  do id = randomCredentialId();
  while (state.credentials[id]);
  return id;
}

/// The `seq` of the next credential made on this cell: one more than any
/// before it. A state written before credentials held `seq` has records
/// without one (null once NaN was saved), which count as 0: the first made
/// on it takes 1, where Math.max over a missing `seq` gave every later one
/// NaN.
export const nextCredentialSeq = (state: CellState): number =>
  Object.values(state.credentials).reduce((max, c) => (Number.isFinite(c.seq) ? Math.max(max, c.seq) : max), 0) + 1;

/// A new customer credential's identity and, for one with a secret, the
/// secret's hash (secrets.ts); the CLI's `admit` makes one without. It
/// expires at `expires_at` (epoch seconds) when given, else never.
export interface NewCredential {
  id: string;
  expires_at?: number | null;
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

/// Live customer credentials a project may hold (design §6.1).
export const MAX_LIVE_CREDENTIALS = 10;

/// Add a customer credential to an active project: the CLI's `credential`
/// and the issuer's credential create. Refused at MAX_LIVE_CREDENTIALS
/// live (active, not omitted) ones.
export function addCredential(
  state: CellState,
  project: string,
  a: { scopes: string; stream_prefixes?: string[]; credential: NewCredential },
): void {
  const p = projectOf(state, project);
  if (p.phase !== "active") throw new AdminError(`project ${project} is ${p.phase}: it takes no new credential`);
  const live = Object.values(state.credentials).filter(
    (c) => c.project_id === project && c.role === "customer" && c.status === "active" && !c.omitted,
  ).length;
  if (live >= MAX_LIVE_CREDENTIALS) {
    throw new AdminError(
      `project ${project} has its ${MAX_LIVE_CREDENTIALS} live credentials: revoke one first ` +
        `(cell-admin revoke --project ${project} --credential C)`,
    );
  }
  addCredentialRecord(state, project, a.scopes, a.stream_prefixes, a.credential, new Date().toISOString());
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
    seq: nextCredentialSeq(state),
  };
  bumpFeed(state);
}

/// The customer projects on the cell (not offboarded, not the canary):
/// what the caps count and what a listing shows.
export const placedProjects = (state: CellState) =>
  Object.entries(state.projects).filter(([id, p]) => p.phase !== "offboarded" && !isCanary(id));

/// Place the cell's canary; `init` calls it on a new state, with the
/// stored bytes its profile gives a project (`CellSizing.stored_default`).
/// Two streams: cell-check's own `cell-check` stream and the alerting
/// watch's canary stream, one each, so neither's create meets the other's
/// `429 stream_limit`.
export function placeCanary(state: CellState, storedDefault: number): void {
  admitProject(state, {
    project: canaryProjectId(state.cell.cell_id),
    workspace: CANARY_WORKSPACE,
    quotas: admittedQuotas(state, ["requests_per_sec=5", "max_streams=2"], storedDefault),
    scopes: "streams.catalog.read",
    display_name: "canary",
    credential: { id: newCredentialId(state), display_name: "catalog-read", hidden: true },
  });
}

/// Whether `c` is a catalog-read credential of the canary: hidden,
/// `streams.catalog.read` only, without a secret.
const isCanaryCatalogRead = (state: CellState, c: CredentialRecord) =>
  c.project_id === canaryProjectId(state.cell.cell_id) &&
  c.hidden === true &&
  c.scopes === "streams.catalog.read" &&
  c.secret_hash === undefined;

/// The canary's catalog-read credential, live. The issuer's and
/// `cell-admin canary`'s probes mint tokens for it.
export function canaryProbeCredential(state: CellState): string {
  const found = Object.entries(state.credentials).find(
    ([, c]) => isCanaryCatalogRead(state, c) && c.status === "active" && !c.omitted,
  );
  if (!found) throw new AdminError(`cell ${state.cell.cell_id} has no live canary catalog-read credential`);
  return found[0];
}

/// Revoke one credential of `project` for good: status revoked at a newer
/// grant_version (cell-admin's `revoke`). Refused unless it is the
/// project's customer credential, active and not omitted, and not the
/// canary's catalog-read credential, which the probes mint for for the
/// cell's life. The canary's other hidden credentials (the watch's, or
/// one planted with a leaked writer key) may be revoked: no page shows
/// them, so nobody else can.
export function revokeCredential(state: CellState, project: string, id: string): void {
  projectOf(state, project);
  const c = state.credentials[id];
  if (!c || c.project_id !== project) throw new AdminError(`project ${project} has no credential ${id}`);
  if (c.role !== "customer") {
    throw new AdminError(`credential ${id} is ${project}'s operator credential, which offboarding omits once its drain ends`);
  }
  if (c.omitted) throw new AdminError(`credential ${id} is omitted: its project was offboarded`);
  if (c.status !== "active") throw new AdminError(`credential ${id} is already revoked`);
  if (isCanaryCatalogRead(state, c)) {
    throw new AdminError(
      `credential ${id} is the canary's catalog-read credential, which the issuer's probes, cell-admin canary ` +
        "and offboarding's witness mint for: it is never revoked",
    );
  }
  setCredentialField(state, id, { status: "revoked" });
}
