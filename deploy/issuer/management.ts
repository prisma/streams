// The management API (public-beta issuer triage B3; design §3.2, §3.3,
// §4, §5, §6.1): a workspace's Streams projects and their credentials,
// for a browser signed in to that workspace (session.ts). Every call is
// checked, in order:
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
//   7. for a call that gives the workspace something new (a project, a
//      credential or a secret: a project create, a credential create, a
//      rotation), the session's workspace on the beta's allow-list
//      (allowlist.ts), read from the control bucket at most every 5 s and
//      refused while there is none (403 workspace_not_allowed; 503 while
//      it cannot be read). Every other call reads no list. A workspace
//      taken off the list (the owner's call 16 of 2026-10-13, answer (b),
//      the reduced mode) gets nothing new within 5 s; its projects keep
//      running (I8 (a)), and its users still see them, revoke a credential
//      and delete a project, so a leaked secret is cut without the owner.
// A refusal of a change from step 4 on is audited (calls.ts).
//
//   GET  /v1/workspaces/{w}/streams/projects       the workspace's projects
//   POST /v1/workspaces/{w}/streams/projects       create one (projects.ts)
//   GET  /v1/workspaces/{w}/streams/projects/{p}   one, and whether the
//                                                  cell serves it (probe.ts)
//   DELETE /v1/workspaces/{w}/streams/projects/{p} delete it
//   GET  /v1/projects/{p}/streams/credentials      its credentials
//   POST /v1/projects/{p}/streams/credentials      create one (credentials.ts)
//   GET  /v1/projects/{p}/streams/credentials/{c}  one, and whether the
//                                                  cell serves its last change
//   POST /v1/projects/{p}/streams/credentials/{c}/rotate   a new secret
//   POST /v1/projects/{p}/streams/credentials/{c}/revoke   revoke it
//
// No answer carries a CORS header (respond.ts): the page that calls these
// routes is the issuer's own. Every answer carries X-Request-Id.
import { randomBytes } from "node:crypto";
import { type Asker, type Call, type GrowthCall, type ManagementDeps, refused } from "./calls";
import type { IssuerConfig } from "./config";
import { Credentials } from "./credentials";
import { rateLimited, UNAVAILABLE_RETRY_SECS } from "./exchange";
import { Projects } from "./projects";
import { refuse } from "./respond";
import { csrfMatches, readSession } from "./session";
import { ownProject } from "./views";

const ID = /^[A-Za-z0-9_-]{1,128}$/;
const JSON_TYPE = /^application\/json\s*(;|$)/i;

type Answer = Promise<Response> | Response;

/// What a route's method does, under the audit's name for it (`action`).
/// One that gives the workspace something new (`grows`) is answered only
/// for a workspace on the allow-list, and is handed its entry.
type Method =
  | { action: string; grows: false; handle: (c: Call) => Answer }
  | { action: string; grows: true; handle: (c: GrowthCall) => Answer };

const keeps = (action: string, handle: (c: Call) => Answer): Method => ({ action, grows: false, handle });
const grows = (action: string, handle: (c: GrowthCall) => Answer): Method => ({ action, grows: true, handle });

/// The refusal of what would give a workspace off the allow-list something new.
const offListMessage = (w: string) =>
  `workspace ${w} is not on the Streams beta's allow-list: its projects and credentials can be seen, revoked and ` +
  "deleted, not created or rotated";

interface Route {
  path: RegExp;
  /// What the path's first id names: a workspace, which must be the
  /// session's, or a project, which must be the session's workspace's.
  scope: "workspace" | "project";
  methods: Partial<Record<string, Method>>;
}

export class Management {
  private readonly routes: Route[];

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
          GET: keeps("project.list", (c) => projects.list(c)),
          POST: grows("project.create", (c) => projects.create(c)),
        },
      },
      {
        path: /^\/v1\/workspaces\/([^/]+)\/streams\/projects\/([^/]+)$/,
        scope: "workspace",
        methods: {
          GET: keeps("project.get", (c) => projects.get(c)),
          DELETE: keeps("project.delete", (c) => projects.delete(c)),
        },
      },
      {
        path: /^\/v1\/projects\/([^/]+)\/streams\/credentials$/,
        scope: "project",
        methods: {
          GET: keeps("credential.list", (c) => credentials.list(c)),
          POST: grows("credential.create", (c) => credentials.create(c)),
        },
      },
      {
        path: /^\/v1\/projects\/([^/]+)\/streams\/credentials\/([^/]+)$/,
        scope: "project",
        methods: { GET: keeps("credential.get", (c) => credentials.get(c)) },
      },
      {
        path: /^\/v1\/projects\/([^/]+)\/streams\/credentials\/([^/]+)\/rotate$/,
        scope: "project",
        methods: { POST: grows("credential.rotate", (c) => credentials.rotate(c)) },
      },
      {
        path: /^\/v1\/projects\/([^/]+)\/streams\/credentials\/([^/]+)\/revoke$/,
        scope: "project",
        methods: { POST: keeps("credential.revoke", (c) => credentials.revoke(c)) },
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
      let res: Response;
      try {
        res = await this.checked(req, route, ids, requestId);
      } catch (e) {
        // Not a refusal any check makes: logged under the request id the
        // answer carries, so the two can be matched.
        this.deps.log(`${req.method} ${pathname} (request ${requestId}) failed: ${(e as Error).message}`);
        res = refuse(500, "internal", "the issuer failed to answer; retry");
      }
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
    if (!method.grows) return method.handle({ ...asker, req, ids });
    const entry = await this.deps.allowlist.entryOf(workspace);
    if (entry === null) return unavailable("the issuer cannot read the beta's allow-list; retry shortly");
    if (entry === undefined) return no(403, "workspace_not_allowed", offListMessage(workspace));
    return method.handle({ ...asker, req, ids, entry });
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
