// The management API (public-beta issuer triage B3; design §3.2, §4,
// §5): a workspace's Streams projects, for a browser signed in to that
// workspace (session.ts). Every call is checked, in order:
//
//   1. the route (else the server's 404) and its method (405 with Allow);
//   2. a session cookie the issuer's key made, issued < 8 h ago (401);
//   3. the session's budget, 60 calls a minute (429 with Retry-After);
//   4. the issuer's copy of the state revalidated within 300 s (503);
//   5. the workspace in the path is the session's (403 forbidden);
//   6. the session's workspace on the beta's allow-list, read from the
//      control bucket at most every 5 s and refused while there is none
//      (403 workspace_not_allowed; 503 while it cannot be read): taking
//      a workspace off the list stops its management within 5 s (owner
//      question I8 (a): its projects keep running until the owner
//      suspends them).
//
//   GET /v1/workspaces/{w}/streams/projects   the workspace's projects
//
// No answer carries a CORS header (respond.ts): the page that calls these
// routes is the issuer's own. Every answer carries X-Request-Id.
import { randomBytes } from "node:crypto";
import { isCanary } from "../cell-admin/rules";
import type { CellState, ProjectRecord } from "../cell-admin/state";
import type { Allowlist, StateStore } from "../cell-admin/store";
import type { StateCache } from "./cache";
import type { IssuerConfig } from "./config";
import { rateLimited, unavailable } from "./exchange";
import type { RateLimits } from "./limits";
import { json, refuse } from "./respond";
import { readSession, type Session } from "./session";

/// How long a read of the allow-list is taken as current.
export const ALLOWLIST_MAX_AGE_MS = 5_000;

const ID = /^[A-Za-z0-9_-]{1,128}$/;

export interface ManagementDeps {
  cache: StateCache;
  store: StateStore;
  limits: RateLimits;
  now: () => number;
  log: (line: string) => void;
}

interface Call {
  req: Request;
  session: Session;
  /// The path's ids, decoded.
  ids: string[];
}

type Handler = (call: Call) => Promise<Response> | Response;

interface Route {
  path: RegExp;
  methods: Partial<Record<string, Handler>>;
}

/// A project as the API shows it. `status` is `deleting` while it is
/// being offboarded.
export function projectView(cfg: IssuerConfig, state: CellState, id: string, p: ProjectRecord) {
  return {
    id,
    workspaceId: p.workspace_id,
    displayName: p.display_name ?? id,
    status: p.phase === "active" ? p.status : "deleting",
    createdAt: p.admitted_at,
    ...(p.created_by ? { createdBy: { userId: p.created_by.user_id, email: p.created_by.email } } : {}),
    cellId: state.cell.cell_id,
    endpoint: cfg.cellOrigin,
    quotas: { ...p.quotas },
  };
}

/// The customer projects of `workspace` a user sees: not the canary, not
/// offboarded; oldest first.
export function workspaceProjects(state: CellState, workspace: string): [string, ProjectRecord][] {
  return Object.entries(state.projects)
    .filter(([id, p]) => p.workspace_id === workspace && p.phase !== "offboarded" && !isCanary(id))
    .sort(([a, p], [b, q]) => (p.admitted_at < q.admitted_at ? -1 : p.admitted_at > q.admitted_at ? 1 : a < b ? -1 : 1));
}

export class Management {
  private readonly routes: Route[];
  private allowlist: { at: number; doc: Allowlist | null } | null = null;

  constructor(
    private readonly cfg: IssuerConfig,
    private readonly deps: ManagementDeps,
  ) {
    this.routes = [
      { path: /^\/v1\/workspaces\/([^/]+)\/streams\/projects$/, methods: { GET: (c) => this.listProjects(c) } },
    ];
  }

  /// Answer a management call, or null when no route matches the path.
  async handle(req: Request, pathname: string): Promise<Response | null> {
    for (const route of this.routes) {
      const m = route.path.exec(pathname);
      if (!m) continue;
      const ids = decodeIds(m.slice(1));
      if (ids === null) return null;
      const res = await this.checked(req, route, ids);
      res.headers.set("x-request-id", randomBytes(8).toString("hex"));
      return res;
    }
    return null;
  }

  private async checked(req: Request, route: Route, ids: string[]): Promise<Response> {
    const handler = route.methods[req.method];
    if (handler === undefined) {
      const allow = Object.keys(route.methods).join(", ");
      return refuse(405, "method_not_allowed", `${allow} only`, { allow });
    }
    const session = readSession(this.cfg.sessionKey, req, this.deps.now());
    if (session === null) return refuse(401, "unauthorized", "sign in with Prisma to manage Streams projects");
    const wait = this.deps.limits.take("management_session", session.sid);
    if (wait > 0) return rateLimited(wait, "too many management calls in this session");
    if (!this.deps.cache.fresh()) return unavailable("the issuer cannot see the cell's current state; retry shortly");
    const [workspace] = ids;
    if (workspace !== session.workspace_id) {
      return refuse(403, "forbidden",
        `this session is signed in to workspace ${session.workspace_id}, not ${workspace}: sign in to ${workspace}`);
    }
    const allowed = await this.allowed(workspace);
    if (allowed === null) return unavailable("the issuer cannot read the beta's allow-list; retry shortly");
    if (!allowed) {
      return refuse(403, "workspace_not_allowed", `workspace ${workspace} is not on the Streams beta's allow-list`);
    }
    return handler({ req, session, ids });
  }

  /// Whether the allow-list holds `workspace` (not while there is none),
  /// or null when it cannot be read.
  private async allowed(workspace: string): Promise<boolean | null> {
    const now = this.deps.now();
    if (this.allowlist === null || now - this.allowlist.at > ALLOWLIST_MAX_AGE_MS) {
      try {
        this.allowlist = { at: now, doc: (await this.deps.store.readAllowlist())?.doc ?? null };
      } catch (e) {
        this.deps.log(`reading the allow-list failed: ${(e as Error).message}`);
        this.allowlist = null;
        return null;
      }
    }
    return this.allowlist.doc?.entries.some((e) => e.workspace_id === workspace) ?? false;
  }

  private listProjects(c: Call): Response {
    const state = this.deps.cache.state;
    const projects = workspaceProjects(state, c.session.workspace_id).map(([id, p]) => projectView(this.cfg, state, id, p));
    return json(200, { projects });
  }
}

/// The path's ids decoded, or null when one is not an id.
function decodeIds(raw: string[]): string[] | null {
  const ids: string[] = [];
  for (const r of raw) {
    let id: string;
    try {
      id = decodeURIComponent(r);
    } catch {
      return null;
    }
    if (!ID.test(id)) return null;
    ids.push(id);
  }
  return ids;
}
