// The issuer's operator routes (public-beta issuer triage B2.9), what the
// watch polls (alerting rows 21, 27 and 28):
//
//   GET /healthz           200 {"status":"ok"} while the issuer can mint:
//                          its copy of the state is fresh and the cell's
//                          keys feed holds its key; else 503 with the
//                          reason. Unauthenticated; it names no id.
//   GET /v1/admin/metrics  behind `Authorization: Bearer
//                          <STREAMS_ISSUER_METRICS_TOKEN>`: the counters,
//                          the copy's age, the bundle's version, the
//                          admission gate. Flat JSON, the keys below.
import { createHash, timingSafeEqual } from "node:crypto";
import type { Audit } from "./audit";
import { stateMismatch } from "./boot";
import type { StateCache } from "./cache";
import type { IssuerConfig } from "./config";
import type { RateLimits } from "./limits";
import type { Rehasher } from "./rehash";
import { json, refuse } from "./respond";

export interface OpsDeps {
  cache: StateCache;
  limits: RateLimits;
  audit: Audit;
  rehasher: Rehasher;
  now: () => number;
}

const digest = (text: string) => createHash("sha256").update(text).digest();

export class Ops {
  private readonly bootedAt: number;
  private readonly token: Buffer;

  constructor(
    private readonly cfg: IssuerConfig,
    private readonly deps: OpsDeps,
  ) {
    this.bootedAt = deps.now();
    this.token = digest(cfg.metricsToken);
  }

  /// Why the issuer cannot mint now, or null.
  private unhealthy(): string | null {
    if (!this.deps.cache.fresh()) return "state_stale";
    if (stateMismatch(this.cfg, this.deps.cache.state) !== null) return "key_not_in_state";
    return null;
  }

  health(): Response {
    const reason = this.unhealthy();
    return reason === null ? json(200, { status: "ok" }) : json(503, { status: "unavailable", reason });
  }

  metrics(req: Request): Response {
    const presented = /^Bearer (.+)$/.exec(req.headers.get("authorization") ?? "")?.[1] ?? "";
    if (!timingSafeEqual(digest(presented), this.token)) {
      return refuse(401, "unauthorized", "send Authorization: Bearer <the metrics token>", {
        "www-authenticate": 'Bearer realm="streams-issuer"',
      });
    }
    const { cache, limits, audit, rehasher } = this.deps;
    const state = cache.state;
    const byPepper: Record<string, number> = {};
    for (const c of Object.values(state.credentials)) {
      if (c.hv === undefined || c.omitted || c.status !== "active") continue;
      byPepper[`v${c.hv}`] = (byPepper[`v${c.hv}`] ?? 0) + 1;
    }
    return json(200, {
      cell_id: this.cfg.cellId,
      kid: this.cfg.signingKey.record.kid,
      uptime_secs: Math.floor((this.deps.now() - this.bootedAt) / 1000),
      healthy: this.unhealthy() === null,
      cache_age_secs: Math.floor(cache.ageMs() / 1000),
      cache_fresh: cache.fresh(),
      state_feed_version: state.feed_version,
      published_feed_version: cache.publishedVersion,
      gate_open: state.cell.gate.open,
      gate_n_max: state.cell.gate.n_max,
      exchanges: { ...audit.totals },
      rate_limit_refusals: { ...limits.refused },
      revalidation_failures: cache.revalidationFailures,
      republishes: cache.republishes,
      rehash_pending: rehasher.pending,
      live_credentials_by_pepper: byPepper,
    });
  }
}
