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
    .sort(([a, c], [b, d]) => ((c.created_at ?? "") < (d.created_at ?? "") ? -1 : (c.created_at ?? "") > (d.created_at ?? "") ? 1 : a < b ? -1 : 1))
    .at(0)?.[0];
}

export class Prober {
  private readonly ready = new Set<string>();
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
    const status = await this.get(signJwt(this.cfg.signingKey, claims), project);
    if (status !== 200) return false;
    this.ready.add(project);
    this.probedAt.delete(project);
    return true;
  }

  /// The cell's status for one catalog read under `token`, or 0 when it
  /// cannot be reached.
  private async get(token: string, project: string): Promise<number> {
    try {
      const res = await fetch(`${this.cfg.cellOrigin}/v1/streams?limit=1`, {
        headers: { authorization: `Bearer ${token}` },
        signal: AbortSignal.timeout(PROBE_TIMEOUT_MS),
      });
      await res.body?.cancel();
      return res.status;
    } catch (e) {
      this.deps.log(`probing ${project} at the cell failed: ${(e as Error).message}`);
      return 0;
    }
  }
}
