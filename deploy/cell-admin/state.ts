// cell-admin's durable, monotonic state for one cell (review finding M3:
// a tool that loses or rolls back its state can republish a revoked
// credential). One JSON file per cell, rewritten atomically (temporary
// file, fsync, rename, directory fsync) under an exclusive lock. Versions
// only move forward: every write bumps `feed_version`, a changed project
// bumps its `project_policy_version`, a changed credential its
// `grant_version`. Nothing is ever deleted from the state: an offboarded
// project stays as a tombstone and its credentials as omitted, so no id
// is ever reused (finding M4). The state holds no private key and no token.
import {
  appendFileSync,
  closeSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  writeSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { AdminError } from "./errors";
import type { Ceilings, Quotas } from "./profile";

export const STATE_FORMAT = "streams-cell-admin/v1";
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
  /// The profile files the ceilings came from, re-read before every change.
  profiles: string[];
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

export interface ProjectRecord {
  workspace_id: string;
  phase: Phase;
  project_policy_version: number;
  ownership_version: number;
  quotas: Quotas;
  admitted_at: string;
  offboarded_at?: string;
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
  /// Omitted from the feed for good (its project was offboarded).
  omitted: boolean;
}

export interface CellState {
  format: typeof STATE_FORMAT;
  cell: CellConfig;
  keys: KeyRecord[];
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

function bundleVersion(dir: string): number | undefined {
  const path = join(dir, BUNDLE_FILE);
  if (!existsSync(path)) return undefined;
  const v = JSON.parse(readFileSync(path, "utf8"))?.keys?.feed_version;
  if (!Number.isSafeInteger(v)) throw new AdminError(`${path} carries no keys.feed_version`);
  return v;
}

/// Load a cell's state. A bundle newer than the state means the state was
/// restored from a stale copy: writing from it would reuse versions the
/// cell has already seen with other contents, so refuse.
export function loadState(dir: string): CellState {
  const path = join(dir, STATE_FILE);
  if (!existsSync(path)) throw new AdminError(`${path} does not exist: run cell-admin init first`);
  const state = JSON.parse(readFileSync(path, "utf8")) as CellState;
  if (state.format !== STATE_FORMAT) {
    throw new AdminError(`${path} is not a ${STATE_FORMAT} state`);
  }
  const published = bundleVersion(dir);
  if (published !== undefined && published > state.feed_version) {
    throw new AdminError(
      `${path} is at feed_version ${state.feed_version} but its bundle is at ${published}: ` +
        "the state was restored from a stale copy; recover the newest state before any change",
    );
  }
  return state;
}

/// The state is written first, then the bundle it implies: a crash
/// between the two leaves a bundle behind the state, which the next write
/// replaces at a newer version.
export function saveState(dir: string, state: CellState, bundleText: string): void {
  writeDurable(join(dir, STATE_FILE), `${JSON.stringify(state, null, 2)}\n`);
  writeDurable(join(dir, BUNDLE_FILE), bundleText);
}

export function createStateDir(dir: string): void {
  if (existsSync(join(dir, STATE_FILE))) {
    throw new AdminError(`${join(dir, STATE_FILE)} exists: a cell is initialised once`);
  }
  mkdirSync(dir, { recursive: true, mode: 0o700 });
}

/// The denylist's ids (one per line, `#` comments).
export function readDenylist(path: string): Set<string> {
  if (!existsSync(path)) return new Set();
  return new Set(
    readFileSync(path, "utf8")
      .split("\n")
      .map((l) => l.trim())
      .filter((l) => l !== "" && !l.startsWith("#")),
  );
}

/// Burn an id for good, durably, before anything publishes it.
export function denyForever(path: string, id: string): void {
  appendFileSync(path, `${id}\n`, { mode: 0o600 });
  const fd = openSync(path, "r");
  try {
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }
}

export function journal(state: CellState, command: string, detail: string): void {
  state.journal.push({ at: new Date().toISOString(), command, detail });
}
