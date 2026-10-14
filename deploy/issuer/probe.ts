// Whether the cell serves a project yet (public-beta issuer triage B3.3,
// D2): asked of the cell itself with one of the project's own live
// credentials. The issuer mints a 300 s token for it at its current
// grant_version, narrowed to streams.catalog.read when it grants that
// and else to its first scope, and sends `GET /v1/streams?limit=1` to the
// cell's origin. A 200, or a 403 missing_scope (the cell checks the scope
// after the policy and the grant), proves the cell holds the project's
// policy and its credential's grant together: the cell's wrapper writes
// the keys, policies and grants feeds one after another, and a refresh
// between two of them serves new grants with old policies (the project
// then answers 421 wrong_cell, which the SDK does not refresh on), so a
// canary that proves only a grants generation would call such a project
// ready. The probe mints only what the credential already grants (the
// cell intersects again). One probe per project at most every 5 s; once
// ready, a project stays ready without another while its policy is
// unchanged: after a resume, or any other change of its policy (a
// quota), it is probed again. The cell refuses a resumed project 403
// project_not_active until it takes the resume, and nothing else tells
// the issuer whether a project was suspended since its last probe.
//
// The same probe acknowledges a credential's last change (B3.4, D2): a
// token at its current grant_version tells whether the cell holds that
// version yet.
//
// Where the cell refuses every token the probe could mint, the issuer
// asks nothing and says why instead (`reason`), so a caller stops
// waiting: a project with no live credential (none active, unexpired
// and not omitted), a project suspended or being deleted, a credential
// that has expired while active.
import { customerClaims, signJwt } from "../cell-admin/tokens";
import type { CellState, CredentialRecord, ProjectRecord } from "../cell-admin/state";
import type { IssuerConfig } from "./config";

export const PROBE_TOKEN_TTL_SECS = 300;
export const PROBE_EVERY_MS = 5_000;
const PROBE_TIMEOUT_MS = 5_000;
const PROBE_SCOPE = "streams.catalog.read";

export interface ProbeDeps {
  now: () => number;
  log: (line: string) => void;
}

/// Why the cell cannot be asked: no token the probe could mint would get
/// past the cell's checks until something else changes.
export type Unprovable = "no_live_credential" | "project_suspended" | "project_deleting" | "credential_expired";

/// Whether a project is ready, or why the cell cannot be asked.
export type Readiness = { ready: true } | { ready: false; reason?: Unprovable };

/// Whether a credential's last change is served, or why the cell cannot
/// be asked.
export type Served = { served: true } | { served: false; reason?: Unprovable };

const expired = (c: CredentialRecord, nowSecs: number) => c.expires_at !== null && c.expires_at <= nowSecs;

/// The scope a probe token of `c` carries: catalog reads when `c` grants
/// them, else its first scope.
function probeScope(c: CredentialRecord): string {
  const scopes = c.scopes.split(" ");
  return scopes.includes(PROBE_SCOPE) ? PROBE_SCOPE : scopes[0];
}

/// The project's live customer credential (active, unexpired as of
/// `nowSecs`, not omitted) the probe uses: the first made that grants
/// catalog reads, else the first made; undefined when it has none.
function probeCredential(state: CellState, project: string, nowSecs: number): string | undefined {
  return Object.entries(state.credentials)
    .filter(
      ([, c]) =>
        c.project_id === project && c.role === "customer" && c.status === "active" && !c.omitted && !expired(c, nowSecs),
    )
    .sort(([, c], [, d]) => {
      const reads = Number(probeScope(d) === PROBE_SCOPE) - Number(probeScope(c) === PROBE_SCOPE);
      return reads !== 0 ? reads : c.seq - d.seq;
    })
    .at(0)?.[0];
}

/// Why a project in `p`'s phase and status cannot be ready, or undefined.
function projectUnprovable(p: ProjectRecord): Unprovable | undefined {
  if (p.phase !== "active") return "project_deleting";
  return p.status === "active" ? undefined : "project_suspended";
}

export class Prober {
  /// Projects ready at a policy version, `<project>@<project_policy_version>`,
  /// and credentials served at a grant version, `<credential>@<grant_version>`.
  private readonly ready = new Set<string>();
  /// When a project or a credential was last probed.
  private readonly probedAt = new Map<string, number>();

  constructor(
    private readonly cfg: IssuerConfig,
    private readonly deps: ProbeDeps,
  ) {}

  /// Whether the cell serves `project` of `state`, probing it when the
  /// last probe is 5 s old; or why the cell cannot be asked. A project
  /// suspended or being deleted is not ready, however it was before; one
  /// proven ready stays ready only at the policy version it was proven
  /// at, so a resume is probed again.
  async projectReady(state: CellState, project: string): Promise<Readiness> {
    const p = state.projects[project];
    const unprovable = projectUnprovable(p);
    if (unprovable !== undefined) return { ready: false, reason: unprovable };
    const key = `${project}@${p.project_policy_version}`;
    if (this.ready.has(key)) return { ready: true };
    const now = this.deps.now();
    const credential = probeCredential(state, project, Math.floor(now / 1000));
    if (credential === undefined) return { ready: false, reason: "no_live_credential" };
    const last = this.probedAt.get(project);
    if (last !== undefined && now - last < PROBE_EVERY_MS) return { ready: false };
    this.probedAt.set(project, now);
    const claims = {
      ...customerClaims(state, credential, PROBE_TOKEN_TTL_SECS, "issuer-readiness", Math.floor(now / 1000)),
      scope: probeScope(state.credentials[credential]),
    };
    const { status, code } = await this.get(signJwt(this.cfg.signingKey, claims), project);
    if (!(status === 200 || (status === 403 && code === "missing_scope"))) return { ready: false };
    this.ready.add(key);
    this.probedAt.delete(project);
    return { ready: true };
  }

  /// Whether the cell serves credential `id` at its grant_version in
  /// `state` (its last create, rotation or revocation), probing it when
  /// the last probe is 5 s old. The token is minted at that version,
  /// narrowed to catalog reads or else to one of the credential's own
  /// scopes. The cell checks the grant before the scope, so a 200 or a
  /// 403 missing_scope proves it holds the version, as does a 403
  /// credential_not_active for a credential the state holds revoked;
  /// 401 credential_unknown or grant_version_mismatch prove it does not.
  /// Not asked, with the reason, while its project is suspended (the cell
  /// checks that before any credential) or while it is active and has
  /// expired (the cell checks that before the grant_version).
  async credentialServed(state: CellState, id: string): Promise<Served> {
    const c = state.credentials[id];
    const key = `${id}@${c.grant_version}`;
    if (this.ready.has(key)) return { served: true };
    const now = this.deps.now();
    if (state.projects[c.project_id].status !== "active") return { served: false, reason: "project_suspended" };
    if (c.status === "active" && expired(c, Math.floor(now / 1000))) return { served: false, reason: "credential_expired" };
    const last = this.probedAt.get(id);
    if (last !== undefined && now - last < PROBE_EVERY_MS) return { served: false };
    this.probedAt.set(id, now);
    const claims = {
      ...customerClaims(state, id, PROBE_TOKEN_TTL_SECS, "issuer-acknowledgement", Math.floor(now / 1000)),
      scope: probeScope(c),
    };
    const { status, code } = await this.get(signJwt(this.cfg.signingKey, claims), id);
    const served =
      status === 200 ||
      (status === 403 && (code === "missing_scope" || (c.status === "revoked" && code === "credential_not_active")));
    if (!served) return { served: false };
    this.ready.add(key);
    this.probedAt.delete(id);
    return { served: true };
  }

  /// The cell's answer to one catalog read under `token` (status 0 when
  /// it cannot be reached), with the product envelope's error code.
  private async get(token: string, what: string): Promise<{ status: number; code?: string }> {
    try {
      const res = await fetch(`${this.cfg.cellOrigin}/v1/streams?limit=1`, {
        headers: { authorization: `Bearer ${token}` },
        signal: AbortSignal.timeout(PROBE_TIMEOUT_MS),
      });
      if (res.status === 200) {
        await res.body?.cancel();
        return { status: 200 };
      }
      const body = (await res.json().catch(() => undefined)) as { error?: { code?: unknown } } | undefined;
      const code = body?.error?.code;
      return { status: res.status, ...(typeof code === "string" ? { code } : {}) };
    } catch (e) {
      this.deps.log(`probing ${what} at the cell failed: ${(e as Error).message}`);
      return { status: 0 };
    }
  }
}
