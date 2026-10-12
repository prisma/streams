// cell-admin's durable, monotonic state for one cell (review finding M3:
// a tool that loses or rolls back its state can republish a revoked
// credential). One JSON document per cell, kept by a StateStore (store.ts)
// and replaced only by compare-and-swap. Versions
// only move forward: every write bumps `feed_version`, a changed project
// bumps its `project_policy_version`, a changed credential its
// `grant_version`. Nothing is ever deleted from the state: an offboarded
// project stays as a tombstone and its credentials as omitted, so no id
// is ever reused (finding M4). The state holds no private key and no token.
import { closeSync, fsyncSync, openSync, renameSync, rmSync, writeSync } from "node:fs";
import { dirname, join } from "node:path";
import { AdminError } from "./errors";
import type { Ceilings, Quotas } from "./profile";

export const STATE_FORMAT = "streams-cell-admin/v2";
/// The format before the issuer's fields. No production cell holds one
/// and no reader for it is kept: a v1 cell is initialised again.
const STATE_FORMAT_V1 = "streams-cell-admin/v1";
export const STATE_FILE = "cell-state.json";
export const BUNDLE_FILE = "feeds-bundle.json";

export interface CellConfig {
  cell_id: string;
  /// The deployment's PROJECT_ID: reserved, never placed.
  deployment_project_id: string;
  /// The deployment's ACCOUNT_ID: the unowned-event sink, never a workspace.
  account_id: string;
  issuer: string;
  share_k: number;
  ceilings: Ceilings;
  /// The profile files `init` read, a record only: nothing re-reads them.
  profiles: string[];
  /// profileDigest of the layered profile at `init`: every admission
  /// compares the caller's shipped copy of the profile with it.
  profile_digest: string;
  workspace_cap: number;
  max_projects: number;
  /// One project id per line; shared by every cell that should never
  /// reuse each other's ids.
  denylist: string;
}

export interface KeyRecord {
  kid: string;
  alg: "RS256" | "EdDSA";
  aud: "prisma-streams-data";
  pem: string;
}

export type Phase = "active" | "offboarding" | "offboarded";

/// Who made a record through the issuer's management API.
export interface Actor {
  user_id: string;
  email: string;
}

export interface ProjectRecord {
  workspace_id: string;
  phase: Phase;
  /// Published as the policy entry's status; `suspended` makes the cell
  /// answer the project's requests 403 project_not_active.
  status: "active" | "suspended";
  project_policy_version: number;
  ownership_version: number;
  quotas: Quotas;
  admitted_at: string;
  offboarded_at?: string;
  display_name?: string;
  created_by?: Actor;
}

export interface CredentialRecord {
  project_id: string;
  /// A customer's credential, or the operator's own one that offboarding
  /// walks and deletes with.
  role: "customer" | "operator";
  grant_version: number;
  status: "active" | "revoked";
  scopes: string;
  stream_prefixes?: string[];
  /// Published as the grant entry's expires_at: epoch seconds, or null.
  expires_at: number | null;
  /// Omitted from the feed for good (its project was offboarded).
  omitted: boolean;
  /// A credential with a secret: HMAC-SHA-256 of the secret under the
  /// pepper of version `hv` (base64url), and the secret's last characters
  /// for recognition. Never the secret.
  secret_hash?: string;
  hv?: number;
  hint?: string;
  display_name?: string;
  created_by?: Actor;
  created_at?: string;
  /// Hidden from every listing (the canary's credentials).
  hidden?: boolean;
}

export interface CellState {
  format: typeof STATE_FORMAT;
  cell: CellConfig;
  keys: KeyRecord[];
  /// Kids once published and then omitted: retired at the cell for good,
  /// so never published again.
  retired_kids: string[];
  feed_version: number;
  projects: Record<string, ProjectRecord>;
  credentials: Record<string, CredentialRecord>;
  journal: { at: string; command: string; detail: string }[];
}

function syncDir(dir: string): void {
  try {
    const fd = openSync(dir, "r");
    try {
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
  } catch {
    // A platform that cannot fsync a directory still renamed atomically.
  }
}

/// Replace `path` atomically and durably with `text`.
export function writeDurable(path: string, text: string): void {
  const tmp = `${path}.tmp-${process.pid}`;
  const fd = openSync(tmp, "w", 0o600);
  try {
    writeSync(fd, text);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
  renameSync(tmp, path);
  syncDir(dirname(path));
}

/// Run `fn` holding the state directory's exclusive lock.
export async function withLock<T>(dir: string, fn: () => Promise<T>): Promise<T> {
  const lock = join(dir, ".lock");
  let fd: number;
  try {
    fd = openSync(lock, "wx", 0o600);
  } catch (e) {
    if ((e as NodeJS.ErrnoException).code === "EEXIST") {
      throw new AdminError(
        `${lock} exists: another cell-admin run holds this cell (remove it only when none runs)`,
      );
    }
    throw new AdminError(`cannot lock ${dir}: ${(e as Error).message}`);
  }
  writeSync(fd, `${process.pid}\n`);
  closeSync(fd);
  try {
    return await fn();
  } finally {
    rmSync(lock, { force: true });
  }
}

/// A state document's text as a v2 state, or a refusal naming `where`.
export function parseState(text: string, where: string): CellState {
  const state = JSON.parse(text) as CellState;
  if ((state.format as string) === STATE_FORMAT_V1) {
    throw new AdminError(`${where} is a ${STATE_FORMAT_V1} state, which this cell-admin does not read: re-initialise the cell`);
  }
  if (state.format !== STATE_FORMAT) {
    throw new AdminError(`${where} is not a ${STATE_FORMAT} state`);
  }
  return state;
}

export const stateText = (state: CellState) => `${JSON.stringify(state, null, 2)}\n`;

/// A denylist's ids (one per line, `#` comments).
export function parseDenylist(text: string): Set<string> {
  return new Set(
    text
      .split("\n")
      .map((l) => l.trim())
      .filter((l) => l !== "" && !l.startsWith("#")),
  );
}

export function journal(state: CellState, command: string, detail: string): void {
  state.journal.push({ at: new Date().toISOString(), command, detail });
}
