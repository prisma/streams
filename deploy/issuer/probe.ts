// Whether the cell serves a project yet (public-beta issuer triage B3.3,
// D2): asked of the cell itself with the project's own first credential.
// The issuer mints a 300 s token for it at its current grant_version,
// narrowed to streams.catalog.read, and sends `GET /v1/streams?limit=1`
// to the cell's origin. A 200 proves the cell holds the project's policy
// and its credential's grant together: the cell's wrapper writes the
// keys, policies and grants feeds one after another, and a refresh between
// two of them serves new grants with old policies (the project then
// answers 421 wrong_cell, which the SDK does not refresh on), so a canary
// that proves only a grants generation would call such a project ready.
// The probe mints only what the credential already grants (the cell
// intersects again). One probe per project at most every 5 s; once
// ready, a project stays ready without another.
//
// The same probe acknowledges a credential's last change (B3.4, D2): a
// token at its current grant_version tells whether the cell holds that
// version yet.
import { customerClaims, signJwt } from "../cell-admin/tokens";
import type { CellState } from "../cell-admin/state";
import type { IssuerConfig } from "./config";

export const PROBE_TOKEN_TTL_SECS = 300;
export const PROBE_EVERY_MS = 5_000;
const PROBE_TIMEOUT_MS = 5_000;
const PROBE_SCOPE = "streams.catalog.read";

export interface ProbeDeps {
  now: () => number;
  log: (line: string) => void;
}

/// The project's first live customer credential that grants catalog
/// reads, or undefined.
function firstCredential(state: CellState, project: string): string | undefined {
  return Object.entries(state.credentials)
    .filter(
      ([, c]) =>
        c.project_id === project &&
        c.role === "customer" &&
        c.status === "active" &&
        !c.omitted &&
        c.scopes.split(" ").includes(PROBE_SCOPE),
    )
    .sort(([, c], [, d]) => c.seq - d.seq)
    .at(0)?.[0];
}

export class Prober {
  /// Projects ready, and `<credential>@<grant_version>` served.
  private readonly ready = new Set<string>();
  /// When a project or a credential was last probed.
  private readonly probedAt = new Map<string, number>();

  constructor(
    private readonly cfg: IssuerConfig,
    private readonly deps: ProbeDeps,
  ) {}

  /// Whether the cell serves `project` of `state`, probing it when the
  /// last probe is 5 s old.
  async projectReady(state: CellState, project: string): Promise<boolean> {
    if (this.ready.has(project)) return true;
    const now = this.deps.now();
    const last = this.probedAt.get(project);
    if (last !== undefined && now - last < PROBE_EVERY_MS) return false;
    this.probedAt.set(project, now);
    const credential = firstCredential(state, project);
    if (credential === undefined) return false;
    const claims = {
      ...customerClaims(state, credential, PROBE_TOKEN_TTL_SECS, "issuer-readiness", Math.floor(now / 1000)),
      scope: PROBE_SCOPE,
    };
    const { status } = await this.get(signJwt(this.cfg.signingKey, claims), project);
    if (status !== 200) return false;
    this.ready.add(project);
    this.probedAt.delete(project);
    return true;
  }

  /// Whether the cell serves credential `id` at its grant_version in
  /// `state` (its last create, rotation or revocation), probing it when
  /// the last probe is 5 s old. The token is minted at that version,
  /// narrowed to catalog reads or else to one of the credential's own
  /// scopes. The cell checks the grant before the scope, so a 200 or a
  /// 403 missing_scope proves it holds the version, as does a 403
  /// credential_not_active for a credential the state holds revoked;
  /// 401 credential_unknown or grant_version_mismatch prove it does not.
  async credentialServed(state: CellState, id: string): Promise<boolean> {
    const c = state.credentials[id];
    const key = `${id}@${c.grant_version}`;
    if (this.ready.has(key)) return true;
    const now = this.deps.now();
    const last = this.probedAt.get(id);
    if (last !== undefined && now - last < PROBE_EVERY_MS) return false;
    this.probedAt.set(id, now);
    const scopes = c.scopes.split(" ");
    const claims = {
      ...customerClaims(state, id, PROBE_TOKEN_TTL_SECS, "issuer-acknowledgement", Math.floor(now / 1000)),
      scope: scopes.includes(PROBE_SCOPE) ? PROBE_SCOPE : scopes[0],
    };
    const { status, code } = await this.get(signJwt(this.cfg.signingKey, claims), id);
    const served =
      status === 200 ||
      (status === 403 && (code === "missing_scope" || (c.status === "revoked" && code === "credential_not_active")));
    if (!served) return false;
    this.ready.add(key);
    this.probedAt.delete(id);
    return true;
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
