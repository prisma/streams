// The rules every writer of a cell's state follows, cell-admin and the
// interim issuer alike (public-beta issuer triage B1.5, B1.6). Pure: each
// takes the state a `mutate` loaded and changes it, or refuses.
//
// The version-bump invariant (skeptic S5b): the cell refuses a snapshot
// that changes an entry at a version it already holds, so a published
// field of a project or a credential is written only by setProjectField
// or setCredentialField, which bump the entry's version when its
// published form changes, and feed_version once per change.
import { AdminError } from "./errors";
import { grantEntry, policyEntry } from "./feeds";
import type { CellState, CredentialRecord, ProjectRecord } from "./state";

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
