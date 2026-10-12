// One state change, end to end: read, refuse a stale restore, change,
// commit by compare-and-swap, publish the bundle the new state implies.
// The CLI and the issuer make every change through `mutate`.
import { AdminError, notYet } from "./errors";
import { type Bundle, bundleOf, bundleText } from "./feeds";
import type { CellState } from "./state";
import { auditKey, Conflict, type PublishedBundle, type StateStore } from "./store";

/// Compare-and-swap attempts before a writer gives up (exit 3: run again).
export const CAS_ATTEMPTS = 5;
/// The journal entries a state keeps; older ones go to the audit prefix.
export const JOURNAL_MAX = 100;

/// Move the journal's oldest entries past JOURNAL_MAX into one audit
/// object, before the state that drops them is committed: a lost
/// compare-and-swap writes them again (a duplicate), never loses them.
async function spillJournal(store: StateStore, state: CellState): Promise<void> {
  const over = state.journal.length - JOURNAL_MAX;
  if (over <= 0) return;
  const entries = state.journal.splice(0, over);
  await store.putAudit(auditKey(), { kind: "journal", cell_id: state.cell.cell_id, entries });
}

/// The state, refused when the published bundle is newer than it: the
/// state was restored from a stale copy, and writing from it would reuse
/// versions the cell has already seen with other contents (PLAN M3). The
/// bundle is read before the state, and every writer commits the state
/// before it publishes, so a writer that commits in between cannot make a
/// current state look stale.
export async function loadChecked(
  store: StateStore,
): Promise<{ state: CellState; etag: string; published: PublishedBundle | null }> {
  const published = await store.readBundle();
  const loaded = await store.load();
  if (published && published.version > loaded.state.feed_version) {
    throw new AdminError(
      `${store.statePath} is at feed_version ${loaded.state.feed_version} but its bundle is at ${published.version}: ` +
        "the state was restored from a stale copy; recover the newest state before any change",
    );
  }
  return { ...loaded, published };
}

/// JSON with every object's keys sorted, so two producers' entries compare
/// by content alone.
function canonical(v: unknown): string {
  if (Array.isArray(v)) return `[${v.map(canonical).join(",")}]`;
  if (v !== null && typeof v === "object") {
    const keys = Object.keys(v).sort();
    return `{${keys.map((k) => `${JSON.stringify(k)}:${canonical((v as Record<string, unknown>)[k])}`).join(",")}}`;
  }
  return JSON.stringify(v);
}

type Entry = Record<string, unknown>;

/// The entries of `ours` that carry other content than `published` at the
/// same version: a key under a kid the cell holds with other material, a
/// policy at the same project_policy_version, a grant at the same
/// grant_version. The cell refuses a snapshot that holds one
/// (src/auth/publication.rs, "versions pin bytes") and goes dark once its
/// feeds age past their 300 s freshness, so none may be published.
export function unversionedChanges(published: Bundle, ours: Bundle): string[] {
  const changed: string[] = [];
  const check = (was: unknown[], now: unknown[], id: string, version: string | null, what: string) => {
    const before = new Map((was as Entry[]).map((e) => [e[id], e]));
    for (const e of now as Entry[]) {
      const old = before.get(e[id]);
      if (!old || (version !== null && old[version] !== e[version])) continue;
      if (canonical(old) !== canonical(e)) {
        changed.push(version === null ? `${what} ${e[id]}` : `${what} ${e[id]} at ${version} ${e[version]}`);
      }
    }
  };
  check(published.keys.keys, ours.keys.keys, "kid", null, "key");
  check(published.policies.projects, ours.policies.projects, "project_id", "project_policy_version", "project");
  check(published.grants.credentials, ours.grants.credentials, "credential_id", "grant_version", "credential");
  return changed;
}

/// Refuse `state`'s bundle when it changes a published entry in place.
function guard(published: PublishedBundle | null, state: CellState): Bundle {
  const ours = bundleOf(state);
  const changed = published ? unversionedChanges(published.bundle, ours) : [];
  if (changed.length > 0) {
    throw new AdminError(
      `the bundle at feed_version ${state.feed_version} changes what the cell holds without a new version: ` +
        `${changed.join(", ")}; a published field changes only with its entry's version`,
    );
  }
  return ours;
}

/// Publishes the bundle a state implies, monotonically.
export class BundlePublisher {
  constructor(private readonly store: StateStore) {}

  /// Publish `state`'s bundle unless the published one is as new; a
  /// concurrent publish (a lost compare-and-swap) is re-read and the
  /// decision taken again.
  async publish(state: CellState): Promise<"published" | "current"> {
    for (let attempt = 1; ; attempt++) {
      const published = await this.store.readBundle();
      if (published && published.version >= state.feed_version) return "current";
      const text = bundleText(guard(published, state));
      try {
        await this.store.publishBundle(text, published?.etag ?? null);
        return "published";
      } catch (e) {
        if (!(e instanceof Conflict)) throw e;
        if (attempt >= CAS_ATTEMPTS) {
          throw notYet(`${this.store.bundlePath} changed under ${CAS_ATTEMPTS} publishes: run again`);
        }
      }
    }
  }
}

/// The state to change first: `copy` (a copy of the state read earlier,
/// with its ETag, which the attempt clones) unless the published bundle
/// is newer than it, then the stored state as loadChecked reads it. A
/// copy older than the bundle is not a stale restore, only an old copy:
/// another writer committed and published since it was read.
async function startFrom(
  store: StateStore,
  copy: { state: CellState; etag: string },
): Promise<{ state: CellState; etag: string; published: PublishedBundle | null }> {
  const published = await store.readBundle();
  if (published && published.version > copy.state.feed_version) return loadChecked(store);
  return { state: structuredClone(copy.state), etag: copy.etag, published };
}

/// Apply `change` to the current state and commit it, re-reading and
/// re-applying on a lost compare-and-swap; then publish. `change` may run
/// more than once and must only change the state it is given (and refuse
/// by throwing). With `copy` (the issuer's in-memory state), the first
/// attempt starts from it instead of reading the state again; a copy that
/// is no longer current loses its compare-and-swap and the change is
/// re-applied to the stored state. Returns its result, the committed state
/// and its ETag.
export async function mutate<T>(
  store: StateStore,
  change: (state: CellState) => T | Promise<T>,
  copy?: { state: CellState; etag: string },
): Promise<{ result: T; state: CellState; etag: string }> {
  for (let attempt = 1; ; attempt++) {
    const { state, etag, published } = attempt === 1 && copy ? await startFrom(store, copy) : await loadChecked(store);
    const result = await change(state);
    guard(published, state);
    await spillJournal(store, state);
    let saved: string;
    try {
      saved = await store.save(state, etag);
    } catch (e) {
      if (!(e instanceof Conflict)) throw e;
      if (attempt >= CAS_ATTEMPTS) throw notYet(`${store.statePath} changed under ${CAS_ATTEMPTS} attempts: run again`);
      continue;
    }
    await new BundlePublisher(store).publish(state);
    return { result, state, etag: saved };
  }
}

/// Burn `id` for good before anything publishes it: refused when the
/// denylist already holds it, appended by compare-and-swap otherwise.
export async function burnId(store: StateStore, id: string): Promise<void> {
  for (let attempt = 1; ; attempt++) {
    const { ids, etag } = await store.readDenylist();
    if (ids.has(id)) throw new AdminError(`project id ${id} was used before: an id is never placed twice`);
    try {
      await store.appendDenylist(id, etag);
      return;
    } catch (e) {
      if (!(e instanceof Conflict)) throw e;
      if (attempt >= CAS_ATTEMPTS) throw notYet(`the denylist changed under ${CAS_ATTEMPTS} attempts: run again`);
    }
  }
}
