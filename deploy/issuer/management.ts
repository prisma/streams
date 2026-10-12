// The management API (public-beta issuer triage B3; design §3.2, §3.3,
// §4, §5, §6.1): a workspace's Streams projects and their credentials,
// for a browser signed in to that workspace (session.ts). Every call is checked, in order:
//
//   1. the route (else the server's 404) and its method (405 with Allow);
//   2. a session cookie the issuer's key made, issued < 8 h ago (401);
//   3. the session's budget, 60 calls a minute (429 with Retry-After);
//   4. for a change (any method but GET): `Content-Type:
//      application/json` and `X-CSRF-Token` equal to the session's CSRF
//      value (403 csrf_failed);
//   5. the issuer's copy of the state revalidated within 300 s (503);
//   6. the workspace in the path is the session's (403 forbidden), or
//      the project in the path is one of its projects, not the canary,
//      not offboarded (404 not_found);
//   7. the session's workspace on the beta's allow-list, read from the
//      control bucket at most every 5 s and refused while there is none
//      (403 workspace_not_allowed; 503 while it cannot be read): taking
//      a workspace off the list stops its management within 5 s (owner
//      question I8 (a): its projects keep running until the owner
//      suspends them).
// A refusal of a change from step 4 on is audited (calls.ts).
//
//   GET  /v1/workspaces/{w}/streams/projects       the workspace's projects
//   POST /v1/workspaces/{w}/streams/projects       create one (projects.ts)
//   GET  /v1/workspaces/{w}/streams/projects/{p}   one, and whether the
//                                                  cell serves it (probe.ts)
//   GET  /v1/projects/{p}/streams/credentials      its credentials
//   POST /v1/projects/{p}/streams/credentials      create one (credentials.ts)
//
// No answer carries a CORS header (respond.ts): the page that calls these
// routes is the issuer's own. Every answer carries X-Request-Id.
import { randomBytes } from "node:crypto";
import type { AllowEntry, Allowlist } from "../cell-admin/store";
import { type Asker, type Call, type ManagementDeps, refused } from "./calls";
import type { IssuerConfig } from "./config";
import { Credentials } from "./credentials";
import { rateLimited, UNAVAILABLE_RETRY_SECS } from "./exchange";
import { Projects } from "./projects";
import { refuse } from "./respond";
import { csrfMatches, readSession } from "./session";
import { ownProject } from "./views";

/// How long a read of the allow-list is taken as current.
export const ALLOWLIST_MAX_AGE_MS = 5_000;

const ID = /^[A-Za-z0-9_-]{1,128}$/;
const JSON_TYPE = /^application\/json\s*(;|$)/i;

interface Method {
  /// The audit's name for what it does.
  action: string;
  handle: (c: Call) => Promise<Response> | Response;
}

interface Route {
  path: RegExp;
  /// What the path's first id names: a workspace, which must be the
  /// session's, or a project, which must be the session's workspace's.
  scope: "workspace" | "project";
  methods: Partial<Record<string, Method>>;
}

export class Management {
  private readonly routes: Route[];
  private allowlist: { at: number; doc: Allowlist | null } | null = null;

  constructor(
    private readonly cfg: IssuerConfig,
    private readonly deps: ManagementDeps,
  ) {
    const projects = new Projects(cfg, deps);
    const credentials = new Credentials(cfg, deps);
    this.routes = [
      {
        path: /^\/v1\/workspaces\/([^/]+)\/streams\/projects$/,
        scope: "workspace",
        methods: {
          GET: { action: "project.list", handle: (c) => projects.list(c) },
          POST: { action: "project.create", handle: (c) => projects.create(c) },
        },
      },
      {
        path: /^\/v1\/workspaces\/([^/]+)\/streams\/projects\/([^/]+)$/,
        scope: "workspace",
        methods: { GET: { action: "project.get", handle: (c) => projects.get(c) } },
      },
      {
        path: /^\/v1\/projects\/([^/]+)\/streams\/credentials$/,
        scope: "project",
        methods: {
          GET: { action: "credential.list", handle: (c) => credentials.list(c) },
          POST: { action: "credential.create", handle: (c) => credentials.create(c) },
        },
      },
    ];
  }

  /// Answer a management call, or null when no route matches the path.
  async handle(req: Request, pathname: string): Promise<Response | null> {
    for (const route of this.routes) {
      const m = route.path.exec(pathname);
      if (!m) continue;
      const ids = decodeIds(m.slice(1));
      if (ids === null) return null;
      const requestId = randomBytes(8).toString("hex");
      const res = await this.checked(req, route, ids, requestId);
      res.headers.set("x-request-id", requestId);
      return res;
    }
    return null;
  }

  private async checked(req: Request, route: Route, ids: string[], requestId: string): Promise<Response> {
    const method = route.methods[req.method];
    if (method === undefined) {
      const allowed = Object.keys(route.methods);
      return refuse(405, "method_not_allowed", `${allowed.join(" or ")} only`, { allow: allowed.join(", ") });
    }
    const session = readSession(this.cfg.sessionKey, req, this.deps.now());
    if (session === null) return refuse(401, "unauthorized", "sign in with Prisma to manage Streams projects");
    const wait = this.deps.limits.take("management_session", session.sid);
    if (wait > 0) return rateLimited(wait, "too many management calls in this session");
    const asker: Asker = { session, requestId, action: method.action, change: req.method !== "GET" };
    const no = (status: number, code: string, message: string, headers?: Record<string, string>) =>
      refused(this.deps, asker, { status, code, message, headers });
    if (asker.change) {
      if (!JSON_TYPE.test(req.headers.get("content-type") ?? "")) {
        return no(403, "csrf_failed", "send Content-Type: application/json");
      }
      if (!csrfMatches(this.cfg.sessionKey, session.sid, req.headers.get("x-csrf-token"))) {
        return no(403, "csrf_failed", "send X-CSRF-Token with this session's value");
      }
    }
    const unavailable = (message: string) =>
      no(503, "issuer_unavailable", message, { "retry-after": String(UNAVAILABLE_RETRY_SECS) });
    if (!this.deps.cache.fresh()) return unavailable("the issuer cannot see the cell's current state; retry shortly");
    const workspace = session.workspace_id;
    if (route.scope === "workspace" && ids[0] !== workspace) {
      return no(403, "forbidden", `this session is signed in to workspace ${workspace}, not ${ids[0]}: sign in to ${ids[0]}`);
    }
    if (route.scope === "project" && ownProject(this.deps.cache.state, workspace, ids[0]) === undefined) {
      return no(404, "not_found", `workspace ${workspace} has no Streams project ${ids[0]}`);
    }
    const entry = await this.entryOf(workspace);
    if (entry === null) return unavailable("the issuer cannot read the beta's allow-list; retry shortly");
    if (entry === undefined) {
      return no(403, "workspace_not_allowed", `workspace ${workspace} is not on the Streams beta's allow-list`);
    }
    return method.handle({ ...asker, req, ids, entry });
  }

  /// The allow-list's entry for `workspace`: undefined when it has none
  /// (or there is no allow-list), null when the list cannot be read.
  private async entryOf(workspace: string): Promise<AllowEntry | undefined | null> {
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
    return this.allowlist.doc?.entries.find((e) => e.workspace_id === workspace);
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
