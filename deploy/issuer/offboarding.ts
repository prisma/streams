// The issuer's offboarding job (public-beta issuer triage B5.1; design
// §10). A project a user deleted (projects.ts) or the owner offboarded
// (`cell-admin offboard`'s first run) is in phase `offboarding`: its
// customer credentials revoked, an operator credential added. The job
// finishes it unattended, as `cell-admin offboard`'s second run would
// (deploy/cell-admin/offboard.ts, witnessRevocation, resumeForDrain,
// drainProject and omitDrained):
//   0. a project the owner offboarded while suspended (its policy still
//      suspended, so the cell would refuse the operator's token) is first
//      witnessed and resumed (the owner's call 15 of 2026-10-13): once a
//      canary token answers 200, asked every 2 s for up to 120 s, each
//      minted at the catalog-read credential's version as the copy,
//      revalidated, holds it then (at least the one its revocation moved
//      it to; another suspended project's offboarding may move it on
//      meanwhile), the cell holds the revocation, and the job sets the
//      project active for its drain, publishes that bundle and writes one
//      audit object (`project.resume`); the proof below then waits up to
//      120 s for it in the same attempt;
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
// The job is resumable: its only state is the project's phase and status
// and, in its record, `offboard_attempts` and `next_attempt_at`. An
// attempt that fails (the cell does not hold or serve the revocation yet,
// a catalog that does not empty in 10 walks, a cell that does not answer,
// a suspended project without a witness version) counts one more attempt
// and puts the next one off by 1 min, 2, 4, ..., at most 1 h; that
// bookkeeping is never published. Another writer may finish the same
// project meanwhile (the owner's cell-admin, a second issuer during a
// deploy): the omit's compare-and-swap re-checks the phase. One attempt
// runs at a time, and none starts while the issuer cannot mint (its copy
// of the state stale, or its key not the cell's; checked before each
// attempt): its tokens would be refused.
import {
  CELL_WAIT_MS,
  drainProject,
  type Minter,
  omitDrained,
  type Reach,
  resumeForDrain,
  witnessRevocation,
} from "../cell-admin/offboard";
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
  /// How long an attempt at a suspended project waits for the cell, for
  /// the witness and then for the proof (CELL_WAIT_MS unless a test
  /// shortens it), and how it sleeps between two asks (a test's).
  waitMs?: number;
  sleep?: (ms: number) => Promise<void>;
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
  private readonly waitMs: number;
  /// The operator's and the probes' tokens, signed with the issuer's key.
  private readonly mint: Minter;

  constructor(
    private readonly cfg: IssuerConfig,
    private readonly deps: OffboardingDeps,
  ) {
    this.settleMs = deps.settleMs ?? OFFBOARD_SETTLE_MS;
    this.waitMs = deps.waitMs ?? CELL_WAIT_MS;
    this.mint = (state, credential, ttlSecs) =>
      signJwt(cfg.signingKey, customerClaims(state, credential, ttlSecs, "issuer-offboard", Math.floor(deps.now() / 1000)));
  }

  /// Whether the issuer can mint now: its copy fresh, its key the cell's.
  private canMint(): boolean {
    const { cache } = this.deps;
    return cache.fresh() && stateMismatch(this.cfg, cache.state) === null;
  }

  /// Make one attempt at every project whose attempt is due, in turn;
  /// none while the issuer cannot mint, checked before each attempt: a
  /// round of long drains can outlast the copy's freshness, or a key a
  /// compromise drill retires meanwhile.
  async runDue(): Promise<void> {
    for (const id of due(this.deps.cache.state, this.deps.now())) {
      if (!this.canMint()) return;
      await this.attempt(id);
    }
  }

  /// One attempt at `id`'s drain, from the state as the copy holds it.
  private async attempt(id: string): Promise<void> {
    const state = this.deps.cache.state;
    if (state.projects[id]?.phase !== "offboarding") return;
    const log = (line: string) => this.deps.log(`offboarding ${id}: ${line}`);
    let drained: { deleted: number; from: CellState };
    try {
      drained = await this.drain(id, state, log);
    } catch (e) {
      await this.failed(id, e as Error, log);
      return;
    }
    let done: { result: string; state: CellState };
    try {
      done = await this.deps.cache.mutate((s) => omitDrained(s, id, drained.deleted));
    } catch (e) {
      await this.failed(id, e as Error, log);
      return;
    }
    log(done.result);
    await this.audited(id, drained.deleted, drained.from.feed_version, done.state, log);
  }

  /// `id`'s drain from the copy `state`: a suspended project is first
  /// witnessed and resumed, and its proof then waits for the resume.
  /// Returns how many streams it deleted and the state it drained from.
  private async drain(
    id: string,
    state: CellState,
    log: (line: string) => void,
  ): Promise<{ deleted: number; from: CellState }> {
    const { cache } = this.deps;
    const reach: Reach = {
      base: this.cfg.cellOrigin,
      mint: this.mint,
      log,
      waitMs: this.waitMs,
      ...(this.deps.sleep ? { sleep: this.deps.sleep } : {}),
      // Before each witness probe the copy is revalidated, so the probe
      // mints at the canary's version as the store holds it then.
      current: async () => {
        await cache.refresh();
        return cache.state;
      },
    };
    const suspended = state.projects[id].status !== "active";
    const from = suspended ? await this.resumed(id, state, reach) : state;
    const deleted = await drainProject(from, id, {
      ...reach,
      waitMs: suspended ? this.waitMs : 0,
      maxWalks: OFFBOARD_MAX_WALKS,
      settleMs: this.settleMs,
    });
    return { deleted, from };
  }

  /// A suspended project resumed for its drain once the canary witness
  /// answers, in one published change, audited `project.resume` (unless
  /// another writer resumed it first). Returns the committed state.
  private async resumed(id: string, state: CellState, reach: Reach): Promise<CellState> {
    const version = await witnessRevocation(state, id, reach);
    const done = await this.deps.cache.mutate((s) => resumeForDrain(s, id));
    const p = done.state.projects[id];
    reach.log(`${id} is active again at project_policy_version ${p.project_policy_version}, for its drain`);
    if (!done.result) return done.state;
    try {
      await this.deps.audit.record({
        actor: "issuer:offboard",
        action: "project.resume",
        outcome: "done",
        reason: `the cell holds the revocation (the canary answered 200 at grant_version ${version}): active for its drain`,
        project_id: id,
        before: { feed_version: state.feed_version, project_policy_version: state.projects[id].project_policy_version },
        after: { feed_version: done.state.feed_version, project_policy_version: p.project_policy_version },
      });
    } catch (e) {
      reach.log(`writing the audit of its resume failed: ${(e as Error).message}`);
    }
    return done.state;
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
