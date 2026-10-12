// One state change, end to end: read, refuse a stale restore, change,
// commit by compare-and-swap, publish the bundle the new state implies.
// The CLI and the issuer make every change through `mutate`.
import { AdminError, notYet } from "./errors";
import { bundleText } from "./feeds";
import type { CellState } from "./state";
import { Conflict, type StateStore } from "./store";

/// Compare-and-swap attempts before a writer gives up (exit 3: run again).
export const CAS_ATTEMPTS = 5;

/// The state, refused when the published bundle is newer than it: the
/// state was restored from a stale copy, and writing from it would reuse
/// versions the cell has already seen with other contents (PLAN M3). The
/// bundle is read before the state, and every writer commits the state
/// before it publishes, so a writer that commits in between cannot make a
/// current state look stale.
export async function loadChecked(store: StateStore): Promise<{ state: CellState; etag: string }> {
  const published = await store.readBundle();
  const loaded = await store.load();
  if (published && published.version > loaded.state.feed_version) {
    throw new AdminError(
      `${store.statePath} is at feed_version ${loaded.state.feed_version} but its bundle is at ${published.version}: ` +
        "the state was restored from a stale copy; recover the newest state before any change",
    );
  }
  return loaded;
}

/// Publishes the bundle a state implies, monotonically.
export class BundlePublisher {
  constructor(private readonly store: StateStore) {}

  /// Publish `state`'s bundle unless the published one is as new; a
  /// concurrent publish (a lost compare-and-swap) is re-read and the
  /// decision taken again.
  async publish(state: CellState): Promise<"published" | "current"> {
    const text = bundleText(state);
    for (let attempt = 1; ; attempt++) {
      const published = await this.store.readBundle();
      if (published && published.version >= state.feed_version) return "current";
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

/// Apply `change` to the current state and commit it, re-reading and
/// re-applying on a lost compare-and-swap; then publish. `change` may run
/// more than once and must only change the state it is given (and refuse
/// by throwing). Returns its result and the committed state.
export async function mutate<T>(
  store: StateStore,
  change: (state: CellState) => T | Promise<T>,
): Promise<{ result: T; state: CellState }> {
  for (let attempt = 1; ; attempt++) {
    const { state, etag } = await loadChecked(store);
    const result = await change(state);
    try {
      await store.save(state, etag);
    } catch (e) {
      if (!(e instanceof Conflict)) throw e;
      if (attempt >= CAS_ATTEMPTS) throw notYet(`${store.statePath} changed under ${CAS_ATTEMPTS} attempts: run again`);
      continue;
    }
    await new BundlePublisher(store).publish(state);
    return { result, state };
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
