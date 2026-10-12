// The issuer's copy of the cell's state (public-beta issuer triage B2.4):
// held in memory with its ETag and revalidated every REFRESH_MS by a
// conditional GET, which an unchanged state answers 304. The owner's
// cell-admin changes reach the issuer within one interval. The copy's age
// is the time since the last revalidation that succeeded: past MAX_AGE_MS
// the issuer refuses to mint (503), on the same window after which the
// cell refuses its own feeds (src/auth.rs, 300 s), so an issuer that
// cannot see a revocation stops minting no later than the cell stops
// serving.
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

  /// One revalidation; a failure keeps the copy and lets it age.
  async refresh(): Promise<void> {
    try {
      const changed = await this.store.loadChanged(this.etag);
      if (changed !== null) {
        this.current = changed.state;
        this.etag = changed.etag;
      }
      this.checkedAt = this.now();
    } catch (e) {
      this.log(`state revalidation failed (copy ${Math.round(this.ageMs() / 1000)} s old): ${(e as Error).message}`);
    }
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
