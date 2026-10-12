// The interim issuer's HTTP surface (public-beta issuer triage B2): one
// handler over the routes below; everything else answers 404, and a route
// asked with another method 405.
//
//   POST /v1/token/streams   the exchange (exchange.ts)
import type { StateStore } from "../cell-admin/store";
import type { StateCache } from "./cache";
import type { IssuerConfig } from "./config";
import { Exchange } from "./exchange";
import { RateLimits } from "./limits";
import { Rehasher } from "./rehash";
import { refuse } from "./respond";

export interface IssuerDeps {
  store: StateStore;
  cache: StateCache;
  now?: () => number;
  log?: (line: string) => void;
  /// The edge's trusted client-IP header (limits.ts, K4).
  ipHeader?: string | null;
}

export class Issuer {
  readonly limits: RateLimits;
  readonly rehasher: Rehasher;
  private readonly exchange: Exchange;

  constructor(cfg: IssuerConfig, deps: IssuerDeps) {
    this.limits = new RateLimits(deps.now);
    this.rehasher = new Rehasher(deps.store, deps.cache, cfg.peppers, deps.log);
    this.exchange = new Exchange(cfg, { ...deps, limits: this.limits, rehasher: this.rehasher });
  }

  async fetch(req: Request): Promise<Response> {
    const { pathname } = new URL(req.url);
    if (pathname === "/v1/token/streams") {
      if (req.method !== "POST") return refuse(405, "method_not_allowed", "POST only", { allow: "POST" });
      return this.exchange.handle(req);
    }
    return refuse(404, "not_found", `no route ${pathname}`);
  }
}
