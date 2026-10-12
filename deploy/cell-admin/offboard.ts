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
//
// Product streams have no forks (forks exist only on the raw surface,
// which a customer project cannot reach), so the walk deletes in catalog
// order; a 410 (a source retained for forks) is re-walked like any other.
// Deleted streams' bytes stay until reclamation (E11); their content is
// unreadable without keys the server never stored.
import { randomBytes } from "node:crypto";
import type { Flags } from "./args";
import { call, cellBase, describeReply, walkAndDelete } from "./cell-client";
import type { Io } from "./commands";
import { AdminError, notYet } from "./errors";
import { loadChecked, mutate } from "./publish";
import { type CellState, journal } from "./state";
import { openStore } from "./store";
import { cellSigningKey, customerClaims, type SigningKey, signJwt } from "./tokens";

export const OPERATOR_SCOPES = "streams.catalog.read streams.lifecycle.manage streams.metadata.read";
const OPERATOR_TOKEN_TTL_SECS = 1800;
const PROBE_TOKEN_TTL_SECS = 300;

const credentialsOf = (state: CellState, project: string) =>
  Object.entries(state.credentials).filter(([, c]) => c.project_id === project && !c.omitted);

/// Returns what it did, for the operator.
function revoke(state: CellState, project: string): string {
  let revoked = 0;
  for (const [, c] of credentialsOf(state, project)) {
    if (c.role === "customer" && c.status === "active") {
      c.status = "revoked";
      c.grant_version += 1;
      revoked += 1;
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
  state.feed_version += 1;
  journal(state, "offboard", `${project} revoked=${revoked} operator=${id}`);
  return `offboard ${project}: revoked ${revoked} customer credential(s); operator credential ${id} added`;
}

const mint = (state: CellState, sk: SigningKey, credential: string, ttl: number) =>
  signJwt(sk, customerClaims(state, credential, ttl, "cell-admin-offboard"));

/// The cell serves the offboarding snapshot: the operator reads the
/// catalog and every revoked credential is refused as revoked. Returns a
/// minter of fresh operator tokens, so a long drain never outlives one.
async function proveRevocation(
  state: CellState,
  project: string,
  sk: SigningKey,
  base: string,
): Promise<() => string> {
  const creds = credentialsOf(state, project);
  const operator = creds.find(([, c]) => c.role === "operator" && c.status === "active");
  if (!operator) throw new AdminError(`project ${project} is offboarding but has no operator credential`);
  const opToken = () => mint(state, sk, operator[0], OPERATOR_TOKEN_TTL_SECS);
  const seen = await call(base, "GET", "/v1/streams?limit=1", opToken());
  if (seen.status !== 200) {
    throw notYet(
      `the cell does not serve the offboarding bundle yet (operator credential answered ` +
        `${describeReply(seen)}): publish it and run again`,
    );
  }
  for (const [id, c] of creds) {
    if (c.role !== "customer") continue;
    const probe = await call(base, "GET", "/v1/streams?limit=1", mint(state, sk, id, PROBE_TOKEN_TTL_SECS));
    if (probe.status !== 403 || probe.code !== "credential_not_active") {
      throw notYet(
        `revoked credential ${id} answered ${describeReply(probe)}, not 403 ` +
          "credential_not_active: the revocation has not reached the cell; run again",
      );
    }
  }
  return opToken;
}

async function drain(base: string, opToken: () => string, f: Flags, project: string, io: Io): Promise<number> {
  const maxWalks = f.count("max-walks", 10);
  const settleMs = Number(f.get("settle-secs") ?? "60") * 1000;
  if (!Number.isFinite(settleMs) || settleMs < 0) throw new AdminError("--settle-secs must be >= 0", 2);
  const proven = Date.now();
  let deleted = 0;
  for (let walk = 1; walk <= maxWalks; walk++) {
    const settled = Date.now() - proven >= settleMs;
    const w = await walkAndDelete(base, opToken(), io.out);
    deleted += w.deleted;
    io.out(`walk ${walk}: listed ${w.listed}, deleted ${w.deleted}`);
    if (w.listed === 0 && settled) return deleted;
    if (w.listed === 0) {
      await new Promise((r) => setTimeout(r, Math.max(0, settleMs - (Date.now() - proven))));
    }
  }
  throw notYet(`the catalog of ${project} was not empty after ${maxWalks} walks; run again`);
}

/// Returns what it did, for the operator.
function omit(state: CellState, project: string, deleted: number): string {
  for (const [, c] of credentialsOf(state, project)) c.omitted = true;
  const p = state.projects[project];
  p.phase = "offboarded";
  p.offboarded_at = new Date().toISOString();
  state.feed_version += 1;
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

export async function offboard(f: Flags, io: Io): Promise<void> {
  const store = openStore(f);
  const project = f.must("project");
  await store.exclusive(async () => {
    const { state } = await loadChecked(store);
    if (phaseOf(state, project) === "active") {
      const done = await mutate(store, (s) => {
        stillIn(s, project, "active");
        return revoke(s, project);
      });
      io.out(done.result);
      io.out(`publish the bundle (feed_version ${done.state.feed_version}), then run offboard again with --cell-url`);
      return;
    }
    if (!f.has("cell-url")) {
      throw new AdminError(`project ${project} is offboarding: run again with --cell-url and --key-file`, 2);
    }
    const base = cellBase(f.must("cell-url"));
    const sk = cellSigningKey(state, f.must("key-file"));
    const opToken = await proveRevocation(state, project, sk, base);
    io.out(`the cell serves the revocation of ${project}; walking its catalog`);
    const deleted = await drain(base, opToken, f, project, io);
    const done = await mutate(store, (s) => {
      stillIn(s, project, "offboarding");
      return omit(s, project, deleted);
    });
    io.out(done.result);
    io.out(`publish the bundle (feed_version ${done.state.feed_version}); ${project} stays on the denylist`);
  });
}
