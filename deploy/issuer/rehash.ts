// Re-hashing credentials under the newest pepper (public-beta issuer
// triage B2.5; design §6.1; skeptic S5g). After the owner adds a pepper,
// a credential hashed under an older one still verifies, and its secret
// is only ever in hand while an exchange presents it. The exchange then
// computes the hash under the newest pepper and notes it here: never the
// secret, and no write, since a compare-and-swap of the whole state on
// the hot path would contend with every other writer. The notes live in
// memory only, so the sweep writes them soon and before the issuer
// stops: REHASH_DELAY_MS after the oldest note (looked at every
// REHASH_TICK_MS), and once more when the process stops (run.ts, on
// SIGTERM), so an issuer redeployed more often than daily still writes
// them. A sweep is one compare-and-swap per batch, each credential only if
// its record still holds the hash that was verified (a rotation in
// between wins). Once every credential reports the newest pepper (the
// metrics route), the owner drops the older one.
import { setCredentialField } from "../cell-admin/rules";
import { type Pepper, secretFields } from "../cell-admin/secrets";
import { type CellState, type CredentialRecord, journal } from "../cell-admin/state";
import type { Audit } from "./audit";
import type { StateCache } from "./cache";

/// A note is written at most this long after it is taken (plus a tick).
export const REHASH_DELAY_MS = 10 * 60_000;
export const REHASH_TICK_MS = 60_000;
/// Credentials one compare-and-swap re-hashes.
export const REHASH_BATCH = 100;

interface Noted {
  /// The stored hash the secret was verified against, and its pepper.
  from: { secret_hash: string; hv: number };
  to: { secret_hash: string; hv: number };
}

export class Rehasher {
  private readonly noted = new Map<string, Noted>();
  private running = false;
  /// When the oldest note not yet written was taken.
  private since: number | null = null;

  constructor(
    private readonly cache: StateCache,
    private readonly peppers: readonly Pepper[],
    private readonly audit: Audit,
    private readonly log: (line: string) => void = console.log,
    private readonly now: () => number = Date.now,
  ) {}

  /// Credentials noted and not yet written.
  get pending(): number {
    return this.noted.size;
  }

  /// `secret` verified for `cred` under `matched`: note its hash under
  /// the newest pepper when that is another one.
  note(id: string, cred: CredentialRecord, matched: Pepper, secret: string): void {
    const [newest] = this.peppers;
    if (matched.version === newest.version || cred.secret_hash === undefined) return;
    const { secret_hash, hv } = secretFields(this.peppers, secret);
    this.noted.set(id, { from: { secret_hash: cred.secret_hash, hv: matched.version }, to: { secret_hash, hv } });
    this.since ??= this.now();
  }

  /// Write what is noted, one compare-and-swap per batch; returns how many
  /// credentials were re-hashed.
  async sweep(): Promise<number> {
    let done = 0;
    this.since = null;
    while (this.noted.size > 0) {
      const batch = [...this.noted.entries()].slice(0, REHASH_BATCH);
      for (const [id] of batch) this.noted.delete(id);
      // Nothing to write when the copy shows every one rotated since.
      if (!batch.some(([id, n]) => holds(this.cache.state, id, n))) continue;
      let changed: { result: string[]; state: CellState };
      try {
        changed = await this.cache.mutate((state) => apply(state, batch));
      } catch (e) {
        for (const [id, n] of batch) if (!this.noted.has(id)) this.noted.set(id, n);
        this.since ??= this.now();
        throw e;
      }
      done += changed.result.length;
      if (changed.result.length > 0) {
        await this.audit.record({
          actor: "issuer:rehash",
          action: "credential.rehash",
          outcome: "done",
          reason: `re-hashed under pepper v${this.peppers[0].version}`,
          credential_ids: changed.result,
          after: { feed_version: changed.state.feed_version },
        });
      }
    }
    return done;
  }

  /// Sweep when the oldest note is `delayMs` old; one sweep at a time.
  async tick(delayMs = REHASH_DELAY_MS): Promise<void> {
    if (this.running || this.since === null || this.now() - this.since < delayMs) return;
    this.running = true;
    try {
      const n = await this.sweep();
      this.log(`re-hashed ${n} credential(s) under pepper v${this.peppers[0].version}`);
    } catch (e) {
      this.log(`re-hashing under pepper v${this.peppers[0].version} failed: ${(e as Error).message}`);
    } finally {
      this.running = false;
    }
  }

  /// Tick every `tickMs` until the returned function is called.
  start(tickMs = REHASH_TICK_MS, delayMs = REHASH_DELAY_MS): () => void {
    const timer = setInterval(() => void this.tick(delayMs), tickMs);
    return () => clearInterval(timer);
  }
}

/// Whether `id`'s record still holds the hash that was verified.
function holds(state: CellState, id: string, n: Noted): boolean {
  const c = state.credentials[id];
  return c !== undefined && c.secret_hash === n.from.secret_hash && c.hv === n.from.hv;
}

/// Re-hash each noted credential whose record still holds the hash that
/// was verified; returns their ids. The hash is not a published field: no
/// version moves.
function apply(state: CellState, batch: [string, Noted][]): string[] {
  const applied: string[] = [];
  for (const [id, n] of batch) {
    if (!holds(state, id, n)) continue;
    setCredentialField(state, id, n.to);
    applied.push(id);
  }
  if (applied.length > 0) journal(state, "rehash", `${applied.length} credential(s) to pepper v${batch[0][1].to.hv}`);
  return applied;
}
