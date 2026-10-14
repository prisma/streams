// offboard: take a project off a shared cell for good (review findings M4
// and R6; blockers.md section 7). Two runs, each safe to repeat:
//
// 1. Without a cell: revoke every customer credential of the project
//    (a newer grant_version, status revoked) and add an operator
//    credential (catalog, metadata, lifecycle) to the same grants
//    snapshot. An active project stays active, or the operator could not
//    walk it. A suspended one stays suspended, and the same change moves
//    the canary's catalog-read credential to a newer grant_version, which
//    the project's record keeps as its witness (the owner's call 15 of
//    2026-10-13): the cell checks a project's status before any
//    credential, so no token of a suspended project, the operator's
//    included, can show that the revocation has arrived. The operator
//    publishes the bundle.
// 2. With --cell-url: for a suspended project, wait until a canary token
//    at the witness version or later answers 200, then set the project
//    active, for its drain alone, in a bundle of its own. The cell
//    compares grant versions exactly and never takes an older grants
//    snapshot, so from that 200 on it holds the revocation whatever
//    policy feed it reads beside it. Then prove the cell serves that
//    snapshot (the operator credential reads the catalog, and every
//    revoked credential answers 403 credential_not_active), then walk the
//    catalog and delete every stream it lists, again and again, until one
//    full walk lists none and starts at least --settle-secs after the
//    proof, so a request that passed authorization before the revocation
//    has finished. Only then is the project omitted from the policy feed
//    and its credentials from the grants feed. Its id stays on the
//    denylist: it is never placed again. The witness and, after the
//    resume, the proof ask the cell every 2 s for up to --wait-secs
//    (default 120) inside the run.
//    On a cell with the interim issuer, the issuer's offboarding job
//    (deploy/issuer/offboarding.ts) makes this second run on its own, with
//    drainProject and omitDrained.
//
// Product streams have no forks (forks exist only on the raw surface,
// which a customer project cannot reach), so the walk deletes in catalog
// order; a 410 (a source retained for forks) is re-walked like any other.
// Deleted streams' bytes stay until reclamation (E11); their content is
// unreadable without keys the server never stored.
import { randomBytes } from "node:crypto";
import type { Flags } from "./args";
import { call, cellBase, describeReply, WALK_PAGE_MAX, type Waits, walkAndDelete } from "./cell-client";
import type { Io } from "./commands";
import { AdminError, notYet } from "./errors";
import { loadChecked, mutate } from "./publish";
import {
  bumpFeed,
  canaryProbeCredential,
  nextCredentialSeq,
  refuseCanary,
  setCredentialField,
  setProjectField,
} from "./rules";
import { type CellState, journal } from "./state";
import type { StateStore } from "./store";
import { openStore, publishNote } from "./stores";
import { cellSigningKey, customerClaims, signJwt } from "./tokens";

export const OPERATOR_SCOPES = "streams.catalog.read streams.lifecycle.manage streams.metadata.read";
const OPERATOR_TOKEN_TTL_SECS = 1800;
const PROBE_TOKEN_TTL_SECS = 300;
/// The longest Retry-After a drain waits out (cell-client's default is
/// 5 s): an unattended drain can afford the project's whole debt.
export const DRAIN_MAX_RETRY_AFTER_SECS = 120;
/// How long a suspended project's second run waits for the cell inside
/// one run, first for the canary witness and then, after the resume, for
/// the proof, asking every POLL_MS: the cell takes a bundle within about
/// 45 s of its publish.
export const CELL_WAIT_MS = 120_000;
const POLL_MS = 2_000;

/// A drain's catalog page: twice the project's request rate, at most
/// WALK_PAGE_MAX. The operator's token draws on the project's own
/// buckets, and under phase B's store bucket (P1 (b′)) a listing of N
/// streams weighs N + 10 against it: a page of 1,000 at 10 requests a
/// second would leave ~100 s of debt, longer than any one request waits.
export const drainPageLimit = (requestsPerSec: number) =>
  Math.max(1, Math.min(WALK_PAGE_MAX, 2 * requestsPerSec));

const credentialsOf = (state: CellState, project: string) =>
  Object.entries(state.credentials).filter(([, c]) => c.project_id === project && !c.omitted);

/// Offboarding's first phase, on a loaded state: revoke the project's
/// active customer credentials, add the operator's, mark it offboarding;
/// for a suspended project, also move the canary's catalog-read
/// credential to a newer grant_version and keep it as the witness. The
/// CLI's first run and the issuer's project delete (never of a suspended
/// project) both make it. Returns the credentials it revoked, the
/// operator's id and the witness version, if any.
export function revoke(state: CellState, project: string): { revoked: string[]; operator: string; witness?: number } {
  const revoked: string[] = [];
  for (const [id, c] of credentialsOf(state, project)) {
    if (c.role === "customer" && c.status === "active") {
      setCredentialField(state, id, { status: "revoked" });
      revoked.push(id);
    }
  }
  let id = "";
  do id = `strcred_op_${randomBytes(16).toString("hex")}`;
  while (state.credentials[id]);
  state.credentials[id] = {
    project_id: project,
    role: "operator",
    grant_version: 1,
    status: "active",
    scopes: OPERATOR_SCOPES,
    expires_at: null,
    omitted: false,
    seq: nextCredentialSeq(state),
  };
  const p = state.projects[project];
  p.phase = "offboarding";
  const witness = p.status === "suspended" ? witnessBump(state, project) : undefined;
  bumpFeed(state);
  const witnessed = witness === undefined ? "" : ` witness=v${witness}`;
  journal(state, "offboard", `${project} revoked=${revoked.length} operator=${id}${witnessed}`);
  return { revoked, operator: id, ...(witness === undefined ? {} : { witness }) };
}

/// The canary's catalog-read credential at a newer grant_version, its
/// published entry otherwise unchanged (the cell takes a version bump
/// alone), kept in `project`'s record as its witness. Returns it.
function witnessBump(state: CellState, project: string): number {
  const canary = canaryProbeCredential(state);
  setCredentialField(state, canary, {}, { bump: true });
  const version = state.credentials[canary].grant_version;
  state.projects[project].witness_grant_version = version;
  return version;
}

/// Signs a token of `state`'s credential `credential` at its current
/// versions, valid `ttlSecs`: cell-admin under --key-file, the issuer
/// under its own key.
export type Minter = (state: CellState, credential: string, ttlSecs: number) => string;

/// How a run reaches the cell and waits for it.
export interface Reach {
  /// The cell's origin.
  base: string;
  mint: Minter;
  log: (line: string) => void;
  /// How long a check keeps asking the cell, every 2 s, before it refuses
  /// with exit 3: 0, or absent, asks once.
  waitMs?: number;
  /// How the run sleeps out a pause or a Retry-After (a test's clock).
  sleep?: (ms: number) => Promise<void>;
  /// The state as it is now, which the canary witness reads before each
  /// probe (the CLI reloads it, the issuer revalidates its copy):
  /// another suspended project's offboarding moves the canary's version
  /// meanwhile. Absent, the state the run began with.
  current?: () => Promise<CellState>;
}

const waitsOf = (r: Reach): Waits => ({
  maxRetryAfterSecs: DRAIN_MAX_RETRY_AFTER_SECS,
  ...(r.sleep ? { sleep: r.sleep } : {}),
});

/// Ask `probe` until it answers null, every POLL_MS for up to `r.waitMs`,
/// then refuse with exit 3 and its last answer. A probe throws to refuse
/// at once.
async function untilServed(r: Reach, probe: () => Promise<string | null>): Promise<void> {
  const pause = r.sleep ?? ((ms: number) => new Promise<void>((done) => setTimeout(done, ms)));
  for (let left = Math.floor((r.waitMs ?? 0) / POLL_MS); ; left--) {
    const why = await probe();
    if (why === null) return;
    if (left <= 0) throw notYet(why);
    await pause(POLL_MS);
  }
}

/// A suspended project's revocation has reached the cell: a token of the
/// canary's catalog-read credential at its grant_version as the state
/// holds it at that probe (`r.current`; at least the witness version,
/// since versions only move forward) answers 200. Another suspended
/// project's offboarding may move that version during the wait, and the
/// cell may take its bundle without ever holding this one's: a token at
/// the version the run began with would never answer 200 again. The cell
/// compares grant versions exactly and never takes an older grants
/// snapshot, so from then on it holds the revoked credentials, whatever
/// policy it holds beside them. 401 grant_version_mismatch and 403 are
/// "not yet", asked again every 2 s for up to `r.waitMs`; any other
/// answer is refused at once. Refused for a project without a witness
/// version (a state edited by hand): no canary answer proves its
/// revocation. The witness proves the snapshot of the instance that
/// answered: exact on the single-server cell the beta runs, as the proof
/// is; a fleet would need one per instance. Returns the canary version
/// that answered.
export async function witnessRevocation(state: CellState, project: string, r: Reach): Promise<number> {
  const witness = state.projects[project].witness_grant_version;
  let version = state.credentials[canaryProbeCredential(state)].grant_version;
  if (witness === undefined || version < witness) {
    throw new AdminError(
      `project ${project} is suspended and being offboarded without a witness version: its revocation was not ` +
        "made by offboard's first run, so no canary answer proves the cell holds it",
    );
  }
  const waits = waitsOf(r);
  await untilServed(r, async () => {
    const latest = (await r.current?.()) ?? state;
    const canary = canaryProbeCredential(latest);
    version = latest.credentials[canary].grant_version;
    const reply = await call(r.base, "GET", "/v1/streams?limit=1", r.mint(latest, canary, PROBE_TOKEN_TTL_SECS), waits);
    if (reply.status === 200) return null;
    if (reply.status === 403 || (reply.status === 401 && reply.code === "grant_version_mismatch")) {
      return (
        `the cell does not hold the revocation of ${project} yet: the canary's catalog-read credential answered ` +
        `${describeReply(reply)} at grant_version ${version}, not 200; publish the bundle, wait, run again`
      );
    }
    throw new AdminError(
      `the canary's catalog-read credential answered ${describeReply(reply)}, which is not a grant the cell has ` +
        "yet to take: the cell does not serve this key or the canary (cell-admin canary shows which)",
    );
  });
  r.log(
    `the cell holds the revocation of ${project}: the canary's catalog-read credential answered 200 at ` +
      `grant_version ${version}`,
  );
  return version;
}

/// Offboarding's resume of a suspended project once its revocation is
/// witnessed: active again at a newer project_policy_version, so the
/// cell serves the drain's operator token (it checks a project's status
/// before any credential). Refused unless the project is still being
/// offboarded and none of its customer credentials is active; nothing
/// when another writer resumed it already. Pure; whether it changed the
/// state.
export function resumeForDrain(state: CellState, project: string): boolean {
  stillIn(state, project, "offboarding");
  if (state.projects[project].status === "active") return false;
  const live = credentialsOf(state, project)
    .filter(([, c]) => c.role === "customer" && c.status === "active")
    .map(([id]) => id);
  if (live.length > 0) {
    throw new AdminError(
      `project ${project} still has active customer credential(s) ${live.join(", ")}: it is not resumed for its drain`,
    );
  }
  setProjectField(state, project, { status: "active" });
  journal(state, "offboard", `${project} resumed for its drain at policy=v${state.projects[project].project_policy_version}`);
  return true;
}

/// The cell serves the offboarding snapshot: the operator reads the
/// catalog and every revoked credential is refused as revoked, asked
/// every 2 s for up to `r.waitMs`. Returns a minter of fresh operator
/// tokens, which a walk calls for each request, so a drain longer than
/// one token's life is never refused as expired.
async function proveRevocation(state: CellState, project: string, r: Reach, waits: Waits): Promise<() => string> {
  const creds = credentialsOf(state, project);
  const operator = creds.find(([, c]) => c.role === "operator" && c.status === "active");
  if (!operator) throw new AdminError(`project ${project} is offboarding but has no operator credential`);
  const opToken = () => r.mint(state, operator[0], OPERATOR_TOKEN_TTL_SECS);
  await untilServed(r, async () => {
    const seen = await call(r.base, "GET", "/v1/streams?limit=1", opToken(), waits);
    if (seen.status !== 200) {
      return (
        `the cell does not serve the offboarding bundle yet (operator credential answered ` +
        `${describeReply(seen)}): publish it and run again`
      );
    }
    for (const [id, c] of creds) {
      if (c.role !== "customer") continue;
      const probe = await call(r.base, "GET", "/v1/streams?limit=1", r.mint(state, id, PROBE_TOKEN_TTL_SECS), waits);
      if (probe.status !== 403 || probe.code !== "credential_not_active") {
        return (
          `revoked credential ${id} answered ${describeReply(probe)}, not 403 ` +
          "credential_not_active: the revocation has not reached the cell; run again"
        );
      }
    }
    return null;
  });
  return opToken;
}

/// How offboarding's second phase walks: at most `maxWalks` walks, and
/// how long after the proof an empty walk must start.
export interface Drain extends Reach {
  maxWalks: number;
  settleMs: number;
}

async function drain(
  opToken: () => string,
  project: string,
  d: Drain,
  walking: Waits & { pageLimit: number },
): Promise<number> {
  const proven = Date.now();
  let deleted = 0;
  for (let walk = 1; walk <= d.maxWalks; walk++) {
    const settled = Date.now() - proven >= d.settleMs;
    const w = await walkAndDelete(d.base, opToken, d.log, walking);
    deleted += w.deleted;
    d.log(`walk ${walk}: listed ${w.listed}, deleted ${w.deleted}`);
    if (w.listed === 0 && settled) return deleted;
    if (w.listed === 0) {
      await new Promise((r) => setTimeout(r, Math.max(0, d.settleMs - (Date.now() - proven))));
    }
  }
  throw notYet(`the catalog of ${project} was not empty after ${d.maxWalks} walks; run again`);
}

/// Offboarding's second phase up to the omit, on a state that holds the
/// project offboarding and active: prove the cell serves the revocation
/// (for up to `d.waitMs`), then walk and delete until an empty walk
/// starts `settleMs` after the proof. Returns how many streams it
/// deleted; refuses with exit 3 (run again) while the cell does not serve
/// the revocation or the catalog does not empty within `maxWalks` walks.
export async function drainProject(state: CellState, project: string, d: Drain): Promise<number> {
  const waits = waitsOf(d);
  const opToken = await proveRevocation(state, project, d, waits);
  d.log(`the cell serves the revocation of ${project}; walking its catalog`);
  return drain(opToken, project, d, { ...waits, pageLimit: drainPageLimit(state.projects[project].quotas.requests_per_sec) });
}

/// Returns what it did, for the operator.
function omit(state: CellState, project: string, deleted: number): string {
  for (const [, c] of credentialsOf(state, project)) c.omitted = true;
  const p = state.projects[project];
  p.phase = "offboarded";
  p.offboarded_at = new Date().toISOString();
  bumpFeed(state);
  journal(state, "offboard", `${project} catalog empty, ${deleted} deleted, omitted`);
  return `offboard ${project}: catalog empty (${deleted} deleted in this run); omitted from the feeds`;
}

/// The project's phase, refused when it is not on the cell or already gone.
function phaseOf(state: CellState, project: string): "active" | "offboarding" {
  const p = state.projects[project];
  if (!p) throw new AdminError(`project ${project} is not on this cell`);
  if (p.phase === "offboarded") throw new AdminError(`project ${project} is already offboarded`);
  return p.phase;
}

/// Refused unless the project is still in `phase`: another writer moved it.
function stillIn(state: CellState, project: string, phase: "active" | "offboarding"): void {
  if (phaseOf(state, project) !== phase) {
    throw notYet(`project ${project} changed phase while this run worked: run again`);
  }
}

/// Offboarding's last step, on the state its commit lands on: omit the
/// drained project and its credentials from the feeds, refused unless it
/// is still offboarding. Returns what it did, for the operator.
export function omitDrained(state: CellState, project: string, deleted: number): string {
  stillIn(state, project, "offboarding");
  return omit(state, project, deleted);
}

/// A `--<name>` of seconds, at least 0, in milliseconds.
function msOf(f: Flags, name: string, dfltSecs: number): number {
  const ms = Number(f.get(name) ?? String(dfltSecs)) * 1000;
  if (!Number.isFinite(ms) || ms < 0) throw new AdminError(`--${name} must be >= 0`, 2);
  return ms;
}

/// The first run: offboarding's first phase, in one published change.
async function firstRun(store: StateStore, project: string, io: Io): Promise<void> {
  const done = await mutate(store, (s) => {
    stillIn(s, project, "active");
    return revoke(s, project);
  });
  const { revoked, operator, witness } = done.result;
  io.out(`offboard ${project}: revoked ${revoked.length} customer credential(s); operator credential ${operator} added`);
  if (witness !== undefined) {
    io.out(
      `${project} stays suspended: the canary's catalog-read credential moved to grant_version ${witness} in the ` +
        "same change, the witness the second run waits for",
    );
  }
  io.out(`bundle: ${store.bundlePath} (feed_version ${done.state.feed_version}); ${publishNote(store)}`);
  io.out("once the cell serves it, run offboard again with --cell-url");
}

/// The second run, on the state it loaded: a suspended project witnessed
/// and resumed, then the drain and the omit.
async function secondRun(store: StateStore, state: CellState, project: string, f: Flags, io: Io): Promise<void> {
  const base = cellBase(f.must("cell-url"));
  const sk = cellSigningKey(state, f.must("key-file"));
  const mint: Minter = (s, credential, ttl) => signJwt(sk, customerClaims(s, credential, ttl, "cell-admin-offboard"));
  const maxWalks = f.count("max-walks", 10);
  const settleMs = msOf(f, "settle-secs", 60);
  const waitMs = msOf(f, "wait-secs", CELL_WAIT_MS / 1000);
  const current = async () => (await loadChecked(store)).state;
  const reach: Reach = { base, mint, log: io.out, waitMs, current };
  const suspended = state.projects[project].status === "suspended";
  let from = state;
  if (suspended) {
    await witnessRevocation(state, project, reach);
    from = (await mutate(store, (s) => resumeForDrain(s, project))).state;
    io.out(`${project} is active again at project_policy_version ${from.projects[project].project_policy_version}, for its drain`);
    io.out(`bundle: ${store.bundlePath} (feed_version ${from.feed_version}); ${publishNote(store)}`);
  }
  // The proof waits only for the resume this run published; otherwise it
  // asks once (exit 3: publish, wait, run again).
  const deleted = await drainProject(from, project, { ...reach, waitMs: suspended ? waitMs : 0, maxWalks, settleMs });
  const done = await mutate(store, (s) => omitDrained(s, project, deleted));
  io.out(done.result);
  io.out(`bundle: ${store.bundlePath} (feed_version ${done.state.feed_version}); ${publishNote(store)}`);
  io.out(`${project} stays on the denylist`);
}

export async function offboard(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const project = f.must("project");
  refuseCanary(project);
  await store.exclusive(async () => {
    const { state } = await loadChecked(store);
    if (phaseOf(state, project) === "active") return firstRun(store, project, io);
    if (!f.has("cell-url")) {
      throw new AdminError(`project ${project} is offboarding: run again with --cell-url and --key-file`, 2);
    }
    return secondRun(store, state, project, f, io);
  });
}
