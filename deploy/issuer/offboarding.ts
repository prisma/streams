// The issuer's offboarding job (public-beta issuer triage B5.1; design
// §10). A project a user deleted (projects.ts) or the owner offboarded
// (`cell-admin offboard`'s first run) is in phase `offboarding`: its
// customer credentials revoked, an operator credential added, its policy
// still active so the operator can walk it. The job finishes it
// unattended, as `cell-admin offboard`'s second run would
// (deploy/cell-admin/offboard.ts, drainProject and omitDrained):
//   1. it proves the cell serves the revocation: the operator credential
//      reads the catalog, and every revoked customer credential answers
//      403 credential_not_active;
//   2. it walks the catalog and deletes every stream it lists, until a
//      walk that starts at least 60 s after the proof lists nothing, at
//      most 10 walks an attempt;
//   3. it omits the project and its credentials from the feeds (phase
//      `offboarded`) by one compare-and-swap, publishes the bundle and
//      writes one audit object.
// The project's id stays on the denylist. Each stream's data is
// crypto-erased as it is deleted (its keys were never stored) and its
// bytes reclaimed by E11 (#112).
//
// The job is resumable: its only state is the project's phase and, in its
// record, `offboard_attempts` and `next_attempt_at`. An attempt that fails
// (the cell does not serve the revocation yet, a catalog that does not
// empty in 10 walks, a cell that does not answer) counts one more attempt
// and puts the next one off by 1 min, 2, 4, ..., at most 1 h; that
// bookkeeping is never published. Another writer may finish the same
// project meanwhile (the owner's cell-admin, a second issuer during a
// deploy): the omit's compare-and-swap re-checks the phase. One attempt
// runs at a time, and none while the issuer cannot mint (its copy of the
// state stale, or its key not the cell's): its tokens would be refused.
import { AdminError } from "../cell-admin/errors";
import { drainProject, type Minter, omitDrained } from "../cell-admin/offboard";
import type { CellState } from "../cell-admin/state";
import { customerClaims, signJwt } from "../cell-admin/tokens";
import type { Audit } from "./audit";
import { stateMismatch } from "./boot";
import type { StateCache } from "./cache";
import type { IssuerConfig } from "./config";

/// How often the job looks for a project whose attempt is due.
export const OFFBOARD_TICK_MS = 15_000;
/// An empty walk ends the drain only this long after the proof, so a
/// request that passed authorization before the revocation has finished.
export const OFFBOARD_SETTLE_MS = 60_000;
export const OFFBOARD_MAX_WALKS = 10;
/// The wait after a failed attempt: 1 min, doubling, at most 1 h.
export const OFFBOARD_BACKOFF_FIRST_MS = 60_000;
export const OFFBOARD_BACKOFF_MAX_MS = 3_600_000;

/// The wait after the `attempts`-th failed attempt.
export const offboardBackoffMs = (attempts: number) =>
  Math.min(OFFBOARD_BACKOFF_MAX_MS, OFFBOARD_BACKOFF_FIRST_MS * 2 ** (attempts - 1));

export interface OffboardingDeps {
  cache: StateCache;
  audit: Audit;
  now: () => number;
  log: (line: string) => void;
  /// OFFBOARD_SETTLE_MS unless a test shortens it.
  settleMs?: number;
}

/// The projects being offboarded whose next attempt is due at `now`.
function due(state: CellState, now: number): string[] {
  return Object.entries(state.projects)
    .filter(([, p]) => p.phase === "offboarding" && (p.next_attempt_at === undefined || Date.parse(p.next_attempt_at) <= now))
    .map(([id]) => id)
    .sort();
}

/// Count a failed attempt at `id`'s drain and put the next one off; null
/// when the project is no longer being offboarded (another writer
/// finished it).
function noteFailure(state: CellState, id: string, now: number): { attempts: number; next: string } | null {
  const p = state.projects[id];
  if (p?.phase !== "offboarding") return null;
  const attempts = (p.offboard_attempts ?? 0) + 1;
  p.offboard_attempts = attempts;
  p.next_attempt_at = new Date(now + offboardBackoffMs(attempts)).toISOString();
  return { attempts, next: p.next_attempt_at };
}

export class Offboarding {
  private running = false;
  private readonly settleMs: number;
  /// The operator's and the probes' tokens, signed with the issuer's key.
  private readonly mint: Minter;

  constructor(
    private readonly cfg: IssuerConfig,
    private readonly deps: OffboardingDeps,
  ) {
    this.settleMs = deps.settleMs ?? OFFBOARD_SETTLE_MS;
    this.mint = (state, credential, ttlSecs) =>
      signJwt(cfg.signingKey, customerClaims(state, credential, ttlSecs, "issuer-offboard", Math.floor(deps.now() / 1000)));
  }

  /// Make one attempt at every project whose attempt is due, in turn;
  /// nothing while the issuer cannot mint.
  async runDue(): Promise<void> {
    const { cache } = this.deps;
    if (!cache.fresh() || stateMismatch(this.cfg, cache.state) !== null) return;
    for (const id of due(cache.state, this.deps.now())) await this.attempt(id);
  }

  /// One attempt at `id`'s drain, from the state as the copy holds it.
  private async attempt(id: string): Promise<void> {
    const state = this.deps.cache.state;
    if (state.projects[id]?.phase !== "offboarding") return;
    const log = (line: string) => this.deps.log(`offboarding ${id}: ${line}`);
    let deleted: number;
    try {
      if (state.projects[id].status !== "active") {
        throw new AdminError(`project ${id} is suspended: the cell refuses the drain's operator token for it`);
      }
      deleted = await drainProject(state, id, {
        base: this.cfg.cellOrigin,
        mint: this.mint,
        maxWalks: OFFBOARD_MAX_WALKS,
        settleMs: this.settleMs,
        log,
      });
    } catch (e) {
      await this.failed(id, e as Error, log);
      return;
    }
    let done: { result: string; state: CellState };
    try {
      done = await this.deps.cache.mutate((s) => omitDrained(s, id, deleted));
    } catch (e) {
      await this.failed(id, e as Error, log);
      return;
    }
    log(done.result);
    await this.audited(id, deleted, state.feed_version, done.state, log);
  }

  /// Record a failed attempt in the project's record, or nothing when
  /// another writer finished the project.
  private async failed(id: string, e: Error, log: (line: string) => void): Promise<void> {
    let noted: { attempts: number; next: string } | null;
    try {
      ({ result: noted } = await this.deps.cache.mutate((s) => noteFailure(s, id, this.deps.now())));
    } catch (err) {
      log(`an attempt failed (${e.message}) and could not be recorded: ${(err as Error).message}`);
      return;
    }
    if (noted === null) {
      log("another writer finished it");
      return;
    }
    log(`attempt ${noted.attempts} failed (${e.message}); next attempt at ${noted.next}`);
  }

  /// One audit object for the omit; a failure is logged: the change is made.
  private async audited(id: string, deleted: number, before: number, after: CellState, log: (line: string) => void) {
    const credential_ids = Object.entries(after.credentials)
      .filter(([, c]) => c.project_id === id)
      .map(([cid]) => cid)
      .sort();
    try {
      await this.deps.audit.record({
        actor: "issuer:offboard",
        action: "project.offboard",
        outcome: "done",
        reason: `catalog empty; ${deleted} stream(s) deleted by this attempt`,
        project_id: id,
        credential_ids,
        before: { feed_version: before },
        after: { feed_version: after.feed_version },
      });
    } catch (e) {
      log(`writing the audit of its omit failed: ${(e as Error).message}`);
    }
  }

  /// Run the due attempts every `everyMs`, one round at a time, until the
  /// returned function is called.
  start(everyMs = OFFBOARD_TICK_MS): () => void {
    const timer = setInterval(() => {
      if (this.running) return;
      this.running = true;
      this.runDue()
        .catch((e) => this.deps.log(`the offboarding job failed: ${(e as Error).message}`))
        .finally(() => {
          this.running = false;
        });
    }, everyMs);
    return () => clearInterval(timer);
  }
}
