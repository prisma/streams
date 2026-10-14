// offboard: take a project off a shared cell for good (review findings M4
// and R6; blockers.md section 7). Two runs, each safe to repeat:
//
// 1. Without a cell: revoke every customer credential of the project
//    (a newer grant_version, status revoked) and add an operator
//    credential (catalog, metadata, lifecycle) to the same grants
//    snapshot. The project stays active, or the operator could not walk
//    it. The operator publishes the bundle.
// 2. With --cell-url: prove the cell serves that snapshot (the operator
//    credential reads the catalog, and every revoked credential answers
//    403 credential_not_active), then walk the catalog and delete every
//    stream it lists, again and again, until one full walk lists none
//    and starts at least --settle-secs after the proof, so a request that
//    passed authorization before the revocation has finished. Only then
//    is the project omitted from the policy feed and its credentials from
//    the grants feed. Its id stays on the denylist: it is never placed again.
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
import { bumpFeed, refuseCanary, setCredentialField } from "./rules";
import { type CellState, journal } from "./state";
import { openStore, publishNote } from "./stores";
import { cellSigningKey, customerClaims, signJwt } from "./tokens";

export const OPERATOR_SCOPES = "streams.catalog.read streams.lifecycle.manage streams.metadata.read";
const OPERATOR_TOKEN_TTL_SECS = 1800;
const PROBE_TOKEN_TTL_SECS = 300;
/// The longest Retry-After a drain waits out (cell-client's default is
/// 5 s): an unattended drain can afford the project's whole debt.
export const DRAIN_MAX_RETRY_AFTER_SECS = 120;

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
/// active customer credentials, add the operator's, mark it offboarding.
/// The CLI's first run and the issuer's project delete both make it.
/// Returns the credentials it revoked and the operator's id.
export function revoke(state: CellState, project: string): { revoked: string[]; operator: string } {
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
  };
  state.projects[project].phase = "offboarding";
  bumpFeed(state);
  journal(state, "offboard", `${project} revoked=${revoked.length} operator=${id}`);
  return { revoked, operator: id };
}

/// Signs a token of `state`'s credential `credential` at its current
/// versions, valid `ttlSecs`: cell-admin under --key-file, the issuer
/// under its own key.
export type Minter = (state: CellState, credential: string, ttlSecs: number) => string;

/// The cell serves the offboarding snapshot: the operator reads the
/// catalog and every revoked credential is refused as revoked. Returns a
/// minter of fresh operator tokens, which a walk calls for each request,
/// so a drain longer than one token's life is never refused as expired.
async function proveRevocation(
  state: CellState,
  project: string,
  mint: Minter,
  base: string,
  waits: Waits,
): Promise<() => string> {
  const creds = credentialsOf(state, project);
  const operator = creds.find(([, c]) => c.role === "operator" && c.status === "active");
  if (!operator) throw new AdminError(`project ${project} is offboarding but has no operator credential`);
  const opToken = () => mint(state, operator[0], OPERATOR_TOKEN_TTL_SECS);
  const seen = await call(base, "GET", "/v1/streams?limit=1", opToken(), waits);
  if (seen.status !== 200) {
    throw notYet(
      `the cell does not serve the offboarding bundle yet (operator credential answered ` +
        `${describeReply(seen)}): publish it and run again`,
    );
  }
  for (const [id, c] of creds) {
    if (c.role !== "customer") continue;
    const probe = await call(base, "GET", "/v1/streams?limit=1", mint(state, id, PROBE_TOKEN_TTL_SECS), waits);
    if (probe.status !== 403 || probe.code !== "credential_not_active") {
      throw notYet(
        `revoked credential ${id} answered ${describeReply(probe)}, not 403 ` +
          "credential_not_active: the revocation has not reached the cell; run again",
      );
    }
  }
  return opToken;
}

/// How offboarding's second phase reaches the cell and how long it walks.
export interface Drain {
  /// The cell's origin.
  base: string;
  mint: Minter;
  /// Walks at most, and how long after the proof an empty walk must start.
  maxWalks: number;
  settleMs: number;
  log: (line: string) => void;
  /// How the drain sleeps out a Retry-After (a test's clock).
  sleep?: (ms: number) => Promise<void>;
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
/// project offboarding: prove the cell serves the revocation, then walk
/// and delete until an empty walk starts `settleMs` after the proof.
/// Returns how many streams it deleted; refuses with exit 3 (run again)
/// while the cell does not serve the revocation or the catalog does not
/// empty within `maxWalks` walks.
export async function drainProject(state: CellState, project: string, d: Drain): Promise<number> {
  const waits: Waits = { maxRetryAfterSecs: DRAIN_MAX_RETRY_AFTER_SECS, ...(d.sleep ? { sleep: d.sleep } : {}) };
  const opToken = await proveRevocation(state, project, d.mint, d.base, waits);
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

/// Refused for a suspended project: the cell checks a project's status
/// before any credential (src/auth.rs, verify_customer), so it would refuse
/// the drain's operator token too, and `resume` refuses a project being
/// offboarded: the offboarding could never finish. The issuer's delete
/// refuses it the same way (deploy/issuer/projects.ts).
function refuseSuspended(state: CellState, project: string): void {
  if (state.projects[project].status !== "suspended") return;
  throw new AdminError(
    `project ${project} is suspended: the cell refuses the drain's operator token for a suspended project, so its ` +
      'offboarding could never finish; resume it first (README, "Offboarding a suspended project")',
  );
}

/// Offboarding's last step, on the state its commit lands on: omit the
/// drained project and its credentials from the feeds, refused unless it
/// is still offboarding. Returns what it did, for the operator.
export function omitDrained(state: CellState, project: string, deleted: number): string {
  stillIn(state, project, "offboarding");
  return omit(state, project, deleted);
}

export async function offboard(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const project = f.must("project");
  refuseCanary(project);
  await store.exclusive(async () => {
    const { state } = await loadChecked(store);
    if (phaseOf(state, project) === "active") {
      const done = await mutate(store, (s) => {
        stillIn(s, project, "active");
        refuseSuspended(s, project);
        return revoke(s, project);
      });
      const { revoked, operator } = done.result;
      io.out(`offboard ${project}: revoked ${revoked.length} customer credential(s); operator credential ${operator} added`);
      io.out(`bundle: ${store.bundlePath} (feed_version ${done.state.feed_version}); ${publishNote(store)}`);
      io.out("once the cell serves it, run offboard again with --cell-url");
      return;
    }
    if (!f.has("cell-url")) {
      throw new AdminError(`project ${project} is offboarding: run again with --cell-url and --key-file`, 2);
    }
    const base = cellBase(f.must("cell-url"));
    const sk = cellSigningKey(state, f.must("key-file"));
    const mint: Minter = (s, credential, ttl) => signJwt(sk, customerClaims(s, credential, ttl, "cell-admin-offboard"));
    const maxWalks = f.count("max-walks", 10);
    const settleMs = Number(f.get("settle-secs") ?? "60") * 1000;
    if (!Number.isFinite(settleMs) || settleMs < 0) throw new AdminError("--settle-secs must be >= 0", 2);
    const deleted = await drainProject(state, project, { base, mint, maxWalks, settleMs, log: io.out });
    const done = await mutate(store, (s) => omitDrained(s, project, deleted));
    io.out(done.result);
    io.out(`bundle: ${store.bundlePath} (feed_version ${done.state.feed_version}); ${publishNote(store)}`);
    io.out(`${project} stays on the denylist`);
  });
}
