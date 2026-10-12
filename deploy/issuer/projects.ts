// A workspace's projects (public-beta issuer triage B3.2; design §4,
// §4.1, §5): the listing, and self-serve creation, the unattended admit →
// publish path.
//
// POST /v1/workspaces/{w}/streams/projects {"displayName"} checks, after
// management.ts's checks (the allow-list among them), in order, each
// refusal audited:
//   1. the body (400 invalid_request);
//   2. the workspace's 5 creates a day (429 rate_limited);
//   3. the issuer's compiled profile is the one the cell was initialised
//      with (checkSizing; 409 beta_at_capacity, the digest audited);
//   4. the admission gate: open, and fewer than its n_max customer
//      projects active (the static u = 0.5 rule; 409 beta_at_capacity);
//   5. checkPlacement, one call: a reserved workspace (403
//      workspace_not_allowed), the cell's max_projects, counting every
//      project not offboarded (409 beta_at_capacity), the workspace's
//      cap, the allow-list entry's max_projects or
//      PROJECTS_PER_WORKSPACE_DEFAULT, counting its active projects only
//      (409 project_limit).
// Then: a fresh `sp_<32 hex>` id is burned on the denylist, and one
// compare-and-swap from the issuer's copy places the project (the gate
// and the placement checked again on the state it lands on) with its
// first customer credential, every scope, its secret's peppered hash
// only; the bundle is published. 201 {project, endpoint, credential,
// secret}: the secret is in this answer and nowhere else.
import { randomBytes } from "node:crypto";
import { AdminError } from "../cell-admin/errors";
import { burnId } from "../cell-admin/publish";
import {
  admitProject,
  admittedQuotas,
  CUSTOMER_SCOPES,
  checkPlacement,
  checkSizing,
  newCredentialId,
  PlacementRefused,
  placedProjects,
} from "../cell-admin/rules";
import { newSecret, presentedSecret, secretFields } from "../cell-admin/secrets";
import { type CellState, journal } from "../cell-admin/state";
import { audited, type Call, displayNameOf, type ManagementDeps, readObject, refused } from "./calls";
import type { IssuerConfig } from "./config";
import { UNAVAILABLE_RETRY_SECS } from "./exchange";
import { LIMITS } from "./limits";
import { BUNDLED_PROFILE } from "./profiles";
import { json } from "./respond";
import { credentialView, projectView, workspaceProjects } from "./views";

/// Owner question I6, answer (a) (the build default): a workspace holds
/// one project unless its allow-list entry names more.
export const PROJECTS_PER_WORKSPACE_DEFAULT = 1;
/// Owner question I7, answer (a) (the build default): a new project gets
/// 10 requests/s unless its workspace's allow-list entry names a rate;
/// every other bounded axis gets its ceiling.
export const FREE_TIER_REQUESTS_PER_SEC = 10;
/// Owner question I9, answer (a) (the build default): a project being
/// deleted (offboarding: its credentials revoked, its drain to run) no
/// longer counts toward its workspace's cap, so its user may create the
/// next one at once; it still counts toward the cell's max_projects.
/// `true` is answer (b), cell-admin's own rule.
export const OFFBOARDING_COUNTS_TOWARD_WORKSPACE_CAP = false;
const FIRST_CREDENTIAL_NAME = "default";
const AT_CAPACITY = "the Streams beta is at capacity; try again later";

/// The admission gate refuses: the static u = 0.5 rule (design §4.1).
class GateClosed extends Error {}

/// Why the gate refuses a placement on `state` now, or null.
function gateRefusal(state: CellState): string | null {
  const gate = state.cell.gate;
  if (!gate.open) return "the admission gate is closed";
  const active = placedProjects(state).filter(([, p]) => p.phase === "active").length;
  return active >= gate.n_max ? `${active} customer project(s) active, the gate's n_max ${gate.n_max}` : null;
}

/// A project id neither the state nor the denylist holds.
function freshProjectId(state: CellState, denylist: Set<string>): string {
  let id = "";
  do id = `sp_${randomBytes(16).toString("hex")}`;
  while (state.projects[id] || denylist.has(id));
  return id;
}

export class Projects {
  constructor(
    private readonly cfg: IssuerConfig,
    private readonly deps: ManagementDeps,
  ) {}

  list(c: Call): Response {
    const state = this.deps.cache.state;
    const projects = workspaceProjects(state, c.session.workspace_id).map(([id, p]) => projectView(this.cfg, state, id, p));
    return json(200, { projects });
  }

  /// A refusal of a placement: the gate, or checkPlacement's.
  private placementRefused(c: Call, e: unknown, cap: number): Promise<Response> {
    if (e instanceof GateClosed) {
      return refused(this.deps, c, { status: 409, code: "beta_at_capacity", message: AT_CAPACITY, detail: e.message });
    }
    if (!(e instanceof PlacementRefused)) throw e;
    const w = c.session.workspace_id;
    switch (e.refusal) {
      case "workspace_full":
        return refused(this.deps, c, {
          status: 409, code: "project_limit",
          message: `workspace ${w} already has ${cap} Streams project(s), its limit`, detail: e.message,
        });
      case "reserved_workspace":
        return refused(this.deps, c, {
          status: 403, code: "workspace_not_allowed", message: `workspace ${w} cannot hold Streams projects`, detail: e.message,
        });
      case "cell_full":
        return refused(this.deps, c, { status: 409, code: "beta_at_capacity", message: AT_CAPACITY, detail: e.message });
      default:
        throw e;
    }
  }

  async create(c: Call): Promise<Response> {
    const body = await readObject(c.req, ["displayName"]);
    const name = typeof body === "string" ? null : displayNameOf(body.displayName);
    if (typeof body === "string" || name === null) {
      const message = typeof body === "string" ? body : "displayName is 1 to 64 characters, without control characters";
      return refused(this.deps, c, { status: 400, code: "invalid_request", message });
    }
    const w = c.session.workspace_id;
    const wait = this.deps.limits.take("project_create_workspace", w);
    if (wait > 0) {
      return refused(this.deps, c, {
        status: 429, code: "rate_limited",
        message: `workspace ${w} may create ${LIMITS.project_create_workspace.burst} projects a day; retry after ${wait} s`,
        headers: { "retry-after": String(wait) },
      });
    }
    const state = this.deps.cache.state;
    try {
      checkSizing(state, BUNDLED_PROFILE);
    } catch (e) {
      if (!(e instanceof AdminError)) throw e;
      this.deps.log(`refusing to place projects: ${e.message}`);
      return refused(this.deps, c, { status: 409, code: "beta_at_capacity", message: AT_CAPACITY, detail: e.message });
    }
    const cap = c.entry.max_projects ?? PROJECTS_PER_WORKSPACE_DEFAULT;
    const { ids: denylist } = await this.deps.store.readDenylist();
    const id = freshProjectId(state, denylist);
    const place = (s: CellState) => {
      const gate = gateRefusal(s);
      if (gate !== null) throw new GateClosed(gate);
      checkPlacement(s, id, w, denylist, { workspaceCap: cap, offboardingCounts: OFFBOARDING_COUNTS_TOWARD_WORKSPACE_CAP });
    };
    try {
      place(state);
    } catch (e) {
      return this.placementRefused(c, e, cap);
    }
    return this.place(c, { id, name, cap, denylist, place });
  }

  /// Burn the id, then place the project in one compare-and-swap.
  private async place(
    c: Call,
    a: { id: string; name: string; cap: number; denylist: Set<string>; place: (s: CellState) => void },
  ): Promise<Response> {
    const w = c.session.workspace_id;
    const by = { user_id: c.session.user_id, email: c.session.email };
    const secret = newSecret();
    const fields = secretFields(this.cfg.peppers, secret);
    const before = this.deps.cache.state.feed_version;
    const rps = c.entry.requests_per_sec ?? FREE_TIER_REQUESTS_PER_SEC;
    let done: { result: string; state: CellState };
    try {
      await burnId(this.deps.store, a.id);
      done = await this.deps.cache.mutate((s) => {
        a.place(s);
        const cid = newCredentialId(s);
        admitProject(s, {
          project: a.id,
          workspace: w,
          quotas: admittedQuotas(s, [`requests_per_sec=${rps}`]),
          scopes: CUSTOMER_SCOPES.join(" "),
          display_name: a.name,
          created_by: by,
          credential: { id: cid, ...fields, display_name: FIRST_CREDENTIAL_NAME, created_by: by },
        });
        journal(s, "project.create", `${a.id} workspace=${w} credential=${cid} user=${by.user_id}`);
        return cid;
      });
    } catch (e) {
      if (e instanceof GateClosed || e instanceof PlacementRefused) return this.placementRefused(c, e, a.cap);
      if (e instanceof AdminError && e.code === 3) {
        return refused(this.deps, c, {
          status: 503, code: "issuer_unavailable", message: "the cell's state is busy; retry shortly", detail: e.message,
          headers: { "retry-after": String(UNAVAILABLE_RETRY_SECS) },
        });
      }
      throw e;
    }
    const { result: cid, state } = done;
    await audited(this.deps, c, {
      outcome: "done",
      project_id: a.id,
      credential_ids: [cid],
      before: { feed_version: before },
      after: { feed_version: state.feed_version, project_policy_version: 1, grant_version: 1 },
    });
    return json(201, {
      project: projectView(this.cfg, state, a.id, state.projects[a.id]),
      endpoint: this.cfg.cellOrigin,
      credential: credentialView(cid, state.credentials[cid]),
      secret: presentedSecret(cid, secret),
    });
  }
}
