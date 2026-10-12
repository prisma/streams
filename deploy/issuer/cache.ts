// The issuer's copy of the cell's state (public-beta issuer triage B2.4):
// held in memory with its ETag and revalidated every REFRESH_MS by a
// conditional GET, which an unchanged state answers 304. The owner's
// cell-admin changes reach the issuer within one interval. The copy's age
// is the time since the last revalidation that succeeded: past MAX_AGE_MS
// the issuer refuses to mint (503), on the same window after which the
// cell refuses its own feeds (src/auth.rs, 300 s), so an issuer that
// cannot see a revocation stops minting no later than the cell stops
// serving.
//
// Every writer commits the state, then publishes its bundle; one that
// stops in between (a crashed cell-admin, an issuer redeployed mid-write)
// leaves a change the cell does not serve, a revocation included, until
// the next change (skeptic S5c). So whenever the state is ahead of the
// bundle as last seen, the cache reads the bundle and republishes it
// through BundlePublisher, which publishes only forward: bundleOf is
// deterministic, so two writers republishing the same state agree.
import { BundlePublisher, mutate } from "../cell-admin/publish";
import type { CellState } from "../cell-admin/state";
import type { StateStore } from "../cell-admin/store";

export const REFRESH_MS = 5_000;
export const MAX_AGE_MS = 300_000;

export interface CacheOptions {
  now?: () => number;
  log?: (line: string) => void;
  refreshMs?: number;
}

export class StateCache {
  private current: CellState;
  private etag: string;
  private checkedAt: number;
  private readonly now: () => number;
  private readonly log: (line: string) => void;
  private readonly refreshMs: number;
  private running = false;
  /// The published bundle's feed_version as last seen; null before the
  /// first look.
  private published: number | null = null;
  /// Since boot, for the metrics route.
  revalidationFailures = 0;
  republishes = 0;

  constructor(
    private readonly store: StateStore,
    first: { state: CellState; etag: string },
    opts: CacheOptions = {},
  ) {
    this.now = opts.now ?? Date.now;
    this.log = opts.log ?? console.log;
    this.refreshMs = opts.refreshMs ?? REFRESH_MS;
    this.current = first.state;
    this.etag = first.etag;
    this.checkedAt = this.now();
  }

  /// The state as last read. Never changed in place: a writer clones it.
  get state(): CellState {
    return this.current;
  }

  /// Milliseconds since the last revalidation that succeeded.
  ageMs(): number {
    return Math.max(0, this.now() - this.checkedAt);
  }

  fresh(): boolean {
    return this.ageMs() <= MAX_AGE_MS;
  }

  /// The published bundle's feed_version as last seen, or null.
  get publishedVersion(): number | null {
    return this.published;
  }

  /// One revalidation, then the repair of a bundle behind the state; a
  /// failure of either keeps what the cache holds and is tried again at
  /// the next refresh.
  async refresh(): Promise<void> {
    try {
      const changed = await this.store.loadChanged(this.etag);
      if (changed !== null) {
        // Versions only move forward: an older stored state was restored
        // from a stale copy (or this read raced the issuer's own newer
        // commit). Keep the copy and let it age: past 300 s the issuer
        // stops minting until the owner recovers the newest state.
        if (changed.state.feed_version < this.current.feed_version) {
          throw new Error(
            `the stored state is at feed_version ${changed.state.feed_version}, behind this copy's ` +
              `${this.current.feed_version}: was it restored from a stale copy?`,
          );
        }
        this.current = changed.state;
        this.etag = changed.etag;
      }
      this.checkedAt = this.now();
    } catch (e) {
      this.revalidationFailures += 1;
      this.log(`state revalidation failed (copy ${Math.round(this.ageMs() / 1000)} s old): ${(e as Error).message}`);
      return;
    }
    try {
      await this.repair();
    } catch (e) {
      this.log(`republishing the bundle at feed_version ${this.current.feed_version} failed: ${(e as Error).message}`);
    }
  }

  private async repair(): Promise<void> {
    const state = this.current;
    if (this.published !== null && this.published >= state.feed_version) return;
    this.published = (await this.store.readBundle())?.version ?? 0;
    if (this.published >= state.feed_version) return;
    const behind = this.published;
    const outcome = await new BundlePublisher(this.store).publish(state);
    this.published = state.feed_version;
    if (outcome === "published") {
      this.republishes += 1;
      this.log(
        `republished the bundle at feed_version ${state.feed_version} (it was at ${behind}): ` +
          "the state's writer had not published it",
      );
    }
  }

  /// Change the state, through cell-admin's mutate, from this copy: the
  /// first compare-and-swap costs no state read; a copy no longer current
  /// loses it, and the change is reloaded, re-checked and re-applied, at
  /// most CAS_ATTEMPTS times in all, then refused with exit-3's AdminError
  /// (the management API's 503). The committed state takes effect in this
  /// copy at once.
  async mutate<T>(change: (state: CellState) => T | Promise<T>): Promise<{ result: T; state: CellState }> {
    const done = await mutate(this.store, change, { state: this.current, etag: this.etag });
    if (done.state.feed_version >= this.current.feed_version) {
      this.current = done.state;
      this.etag = done.etag;
      this.checkedAt = this.now();
    }
    this.published = Math.max(this.published ?? 0, done.state.feed_version);
    return { result: done.result, state: done.state };
  }

  /// Revalidate every refreshMs, one at a time, until the returned
  /// function is called.
  start(): () => void {
    const timer = setInterval(() => {
      if (this.running) return;
      this.running = true;
      this.refresh().finally(() => {
        this.running = false;
      });
    }, this.refreshMs);
    return () => clearInterval(timer);
  }
}
