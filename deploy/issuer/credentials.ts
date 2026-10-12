// A project's credentials (public-beta issuer triage B3.4; design §6.1),
// on the contract's paths (contracts/streams-platform/v1/
// management.openapi.yaml), so the platform's Stage 1 can replace the
// issuer without its users changing code:
//
//   GET  /v1/projects/{p}/streams/credentials   the project's customer
//        credentials (never the canary's or an operator's), revoked ones
//        included, oldest first, in the contract's Credential shape
//   POST /v1/projects/{p}/streams/credentials   create one
//   GET  /v1/projects/{p}/streams/credentials/{c}
//        one, and whether the cell serves its last change (probe.ts)
//   POST /v1/projects/{p}/streams/credentials/{c}/rotate
//        a new secret, at grant_version + 1
//   POST /v1/projects/{p}/streams/credentials/{c}/revoke
//        revoked for good, at grant_version + 1
//
// A create takes {displayName?, scopes?, streamPrefixes?, expiresAt?}
// (every scope, no prefix and no expiry by default) and checks, in order,
// each refusal audited: the body (400 invalid_request); the project's 20
// creates or rotations a day (429 rate_limited); the project not being
// deleted (409 project_not_active); its 10 live credentials (409
// credential_limit). One compare-and-swap adds the credential at
// grant_version 1 with a new 32-byte secret, of which the state keeps
// only the HMAC under the newest pepper; the bundle is published. 201
// {credential, secret}: the secret is in this answer and nowhere else.
//
// A rotation takes no body and checks the credential (the project's
// customer credential a user sees, else 404 not_found; active and not
// expired, else 409 credential_not_active), then the same daily budget.
// One compare-and-swap replaces the secret's hash and bumps the
// grant_version; the bundle is published. 200 {credential, secret}. A
// rotation has no overlap: zero-downtime replacement is "create a second
// credential, move, revoke the first".
//
// A revocation takes no body and checks the credential (404 not_found as
// above; already revoked, 409 credential_not_active); it takes no daily
// budget. One compare-and-swap sets it revoked, which bumps its
// grant_version; the bundle is published. 200 {credential}.
import {
  addCredential,
  CUSTOMER_SCOPES,
  MAX_LIVE_CREDENTIALS,
  newCredentialId,
  setCredentialField,
} from "../cell-admin/rules";
import { newSecret, presentedSecret, secretFields } from "../cell-admin/secrets";
import { type CellState, journal } from "../cell-admin/state";
import { audited, type Call, commit, displayNameOf, type ManagementDeps, readObject, Refusal, refused } from "./calls";
import type { IssuerConfig } from "./config";
import { LIMITS } from "./limits";
import { Prober } from "./probe";
import { json } from "./respond";
import { credentialView } from "./views";

const DATE_TIME = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;

interface Ask {
  display_name?: string;
  scopes: string;
  stream_prefixes?: string[];
  expires_at: number | null;
}

/// What a create's body asks for, or why it is refused.
function askOf(body: Record<string, unknown>, nowMs: number): Ask | string {
  const { displayName, scopes, streamPrefixes, expiresAt } = body;
  const ask: Ask = { scopes: CUSTOMER_SCOPES.join(" "), expires_at: null };
  if (displayName !== undefined) {
    const name = displayNameOf(displayName);
    if (name === null) return "displayName is 1 to 64 characters, without control characters";
    ask.display_name = name;
  }
  if (scopes !== undefined) {
    if (!Array.isArray(scopes) || scopes.length === 0 || scopes.some((s) => typeof s !== "string")) {
      return "scopes is a non-empty array of Streams scopes";
    }
    const unknown = scopes.find((s) => !(CUSTOMER_SCOPES as readonly string[]).includes(s));
    if (unknown !== undefined) return `scopes: ${JSON.stringify(unknown)} is not a Streams scope`;
    ask.scopes = [...new Set(scopes as string[])].join(" ");
  }
  if (streamPrefixes !== undefined && streamPrefixes !== null) {
    const p = streamPrefixes as unknown[];
    if (!Array.isArray(p) || p.length < 1 || p.length > 64 || p.some((x) => typeof x !== "string" || x === "")) {
      return "streamPrefixes is null or 1 to 64 non-empty strings";
    }
    ask.stream_prefixes = [...(p as string[])];
  }
  if (expiresAt !== undefined && expiresAt !== null) {
    const ms = typeof expiresAt === "string" && DATE_TIME.test(expiresAt) ? Date.parse(expiresAt) : Number.NaN;
    if (!Number.isFinite(ms) || ms <= nowMs) return "expiresAt is null or a date-time in the future";
    ask.expires_at = Math.floor(ms / 1000);
  }
  return ask;
}

/// The customer credentials of `project` a user sees, oldest first.
export function projectCredentials(state: CellState, project: string) {
  return Object.entries(state.credentials)
    .filter(([, c]) => c.project_id === project && c.role === "customer" && !c.omitted && !c.hidden)
    .sort(([a, c], [b, d]) => {
      const [x, y] = [c.created_at ?? "", d.created_at ?? ""];
      return x < y ? -1 : x > y ? 1 : a < b ? -1 : 1;
    });
}

/// Why credential `id` of `project` cannot be rotated on `state` (as of
/// `nowSecs`), or null: it must be the project's customer credential a
/// user sees, active and not expired.
function rotateRefusal(state: CellState, project: string, id: string, nowSecs: number): Refusal | null {
  const c = state.credentials[id];
  if (!c || c.project_id !== project || c.role !== "customer" || c.omitted || c.hidden) {
    return new Refusal(404, "not_found", `project ${project} has no credential ${id}`);
  }
  if (c.status !== "active") return new Refusal(409, "credential_not_active", `credential ${id} is revoked`);
  if (c.expires_at !== null && c.expires_at <= nowSecs) {
    return new Refusal(409, "credential_not_active", `credential ${id} has expired`);
  }
  return null;
}

/// Why credential `id` of `project` cannot be revoked on `state`, or
/// null: it must be the project's customer credential a user sees, not
/// yet revoked. An expired one may be.
function revokeRefusal(state: CellState, project: string, id: string): Refusal | null {
  const c = state.credentials[id];
  if (!c || c.project_id !== project || c.role !== "customer" || c.omitted || c.hidden) {
    return new Refusal(404, "not_found", `project ${project} has no credential ${id}`);
  }
  return c.status === "active" ? null : new Refusal(409, "credential_not_active", `credential ${id} is already revoked`);
}

/// Why `project` takes no new credential on `state`, or null.
function createRefusal(state: CellState, project: string): Refusal | null {
  if (state.projects[project]?.phase !== "active") {
    return new Refusal(409, "project_not_active", `project ${project} is being deleted`);
  }
  const live = projectCredentials(state, project).filter(([, c]) => c.status === "active").length;
  if (live >= MAX_LIVE_CREDENTIALS) {
    return new Refusal(409, "credential_limit",
      `project ${project} has its ${MAX_LIVE_CREDENTIALS} live credentials: revoke one first`);
  }
  return null;
}

export class Credentials {
  private readonly prober: Prober;

  constructor(
    private readonly cfg: IssuerConfig,
    private readonly deps: ManagementDeps,
  ) {
    this.prober = new Prober(cfg, deps);
  }

  list(c: Call): Response {
    const credentials = projectCredentials(this.deps.cache.state, c.ids[0]).map(([id, cred]) => credentialView(id, cred));
    return json(200, { credentials });
  }

  /// One credential, with whether the cell serves its last change
  /// (`served`, probe.ts): the page polls it after a rotation or a
  /// revocation.
  async get(c: Call): Promise<Response> {
    const [project, id] = c.ids;
    const state = this.deps.cache.state;
    const cred = state.credentials[id];
    if (!cred || cred.project_id !== project || cred.role !== "customer" || cred.omitted || cred.hidden) {
      return refused(this.deps, c, { status: 404, code: "not_found", message: `project ${project} has no credential ${id}` });
    }
    return json(200, { ...credentialView(id, cred), served: await this.prober.credentialServed(state, id) });
  }

  /// The project's budget of creates and rotations: a 429, or null.
  async budget(c: Call, project: string): Promise<Response | null> {
    const wait = this.deps.limits.take("credential_change_project", project);
    if (wait === 0) return null;
    const burst = LIMITS.credential_change_project.burst;
    return refused(this.deps, c, {
      status: 429, code: "rate_limited",
      message: `project ${project} may create or rotate ${burst} credentials a day; retry after ${wait} s`,
      headers: { "retry-after": String(wait) },
    });
  }

  async create(c: Call): Promise<Response> {
    const [project] = c.ids;
    const body = await readObject(c.req, ["displayName", "scopes", "streamPrefixes", "expiresAt"]);
    const ask = typeof body === "string" ? body : askOf(body, this.deps.now());
    if (typeof ask === "string") return refused(this.deps, c, { status: 400, code: "invalid_request", message: ask });
    const limited = await this.budget(c, project);
    if (limited !== null) return limited;
    const early = createRefusal(this.deps.cache.state, project);
    if (early !== null) return refused(this.deps, c, early);
    const secret = newSecret();
    const by = { user_id: c.session.user_id, email: c.session.email };
    const before = this.deps.cache.state.feed_version;
    const done = await commit(this.deps, c, (s) => {
      const refusal = createRefusal(s, project);
      if (refusal !== null) throw refusal;
      const id = newCredentialId(s);
      addCredential(s, project, {
        scopes: ask.scopes,
        stream_prefixes: ask.stream_prefixes,
        credential: {
          id,
          ...secretFields(this.cfg.peppers, secret),
          ...(ask.display_name !== undefined ? { display_name: ask.display_name } : {}),
          created_by: by,
          expires_at: ask.expires_at,
        },
      });
      journal(s, "credential.create", `${project} credential=${id} user=${by.user_id}`);
      return id;
    });
    if (done instanceof Response) return done;
    const { result: id, state } = done;
    await audited(this.deps, c, {
      outcome: "done",
      project_id: project,
      credential_ids: [id],
      before: { feed_version: before },
      after: { feed_version: state.feed_version, grant_version: 1 },
    });
    return json(201, { credential: credentialView(id, state.credentials[id]), secret: presentedSecret(id, secret) });
  }

  /// A new secret for an active credential, at grant_version + 1: the old
  /// secret exchanges no more at once, and tokens minted under it die
  /// where the cell holds the new grant (exact grant_version).
  async rotate(c: Call): Promise<Response> {
    const [project, id] = c.ids;
    const body = await readObject(c.req, [], true);
    if (typeof body === "string") return refused(this.deps, c, { status: 400, code: "invalid_request", message: body });
    const nowSecs = () => Math.floor(this.deps.now() / 1000);
    const early = rotateRefusal(this.deps.cache.state, project, id, nowSecs());
    if (early !== null) return refused(this.deps, c, early);
    const limited = await this.budget(c, project);
    if (limited !== null) return limited;
    const secret = newSecret();
    const was = this.deps.cache.state;
    const before = { feed_version: was.feed_version, grant_version: was.credentials[id].grant_version };
    const done = await commit(this.deps, c, (s) => {
      const refusal = rotateRefusal(s, project, id, nowSecs());
      if (refusal !== null) throw refusal;
      setCredentialField(s, id, secretFields(this.cfg.peppers, secret), { bump: true });
      journal(s, "credential.rotate", `${project} credential=${id} grant=v${s.credentials[id].grant_version} user=${c.session.user_id}`);
    });
    if (done instanceof Response) return done;
    const cred = done.state.credentials[id];
    await audited(this.deps, c, {
      outcome: "done",
      project_id: project,
      credential_ids: [id],
      before,
      after: { feed_version: done.state.feed_version, grant_version: cred.grant_version },
    });
    return json(200, { credential: credentialView(id, cred), secret: presentedSecret(id, secret) });
  }

  /// Revoke a credential for good: status revoked at grant_version + 1.
  /// Its secret exchanges no more at once; the cell refuses its tokens
  /// once it holds the new grant.
  async revoke(c: Call): Promise<Response> {
    const [project, id] = c.ids;
    const body = await readObject(c.req, [], true);
    if (typeof body === "string") return refused(this.deps, c, { status: 400, code: "invalid_request", message: body });
    const early = revokeRefusal(this.deps.cache.state, project, id);
    if (early !== null) return refused(this.deps, c, early);
    const was = this.deps.cache.state;
    const before = { feed_version: was.feed_version, grant_version: was.credentials[id].grant_version };
    const done = await commit(this.deps, c, (s) => {
      const refusal = revokeRefusal(s, project, id);
      if (refusal !== null) throw refusal;
      setCredentialField(s, id, { status: "revoked" });
      journal(s, "credential.revoke", `${project} credential=${id} grant=v${s.credentials[id].grant_version} user=${c.session.user_id}`);
    });
    if (done instanceof Response) return done;
    const cred = done.state.credentials[id];
    await audited(this.deps, c, {
      outcome: "done",
      project_id: project,
      credential_ids: [id],
      before,
      after: { feed_version: done.state.feed_version, grant_version: cred.grant_version },
    });
    return json(200, { credential: credentialView(id, cred) });
  }
}
