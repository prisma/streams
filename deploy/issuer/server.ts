// The interim issuer's HTTP surface (public-beta issuer triage B2): one
// handler over the routes below; everything else answers 404, and a route
// asked with another method 405.
//
//   POST /v1/token/streams   the exchange (exchange.ts)
//   GET  /healthz            whether it can mint (ops.ts)
//   GET  /v1/admin/metrics   its counters, behind the metrics bearer (ops.ts)
import type { StateStore } from "../cell-admin/store";
import { Audit } from "./audit";
import type { StateCache } from "./cache";
import type { IssuerConfig } from "./config";
import { Exchange } from "./exchange";
import { RateLimits } from "./limits";
import { Ops } from "./ops";
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
  readonly audit: Audit;
  readonly rehasher: Rehasher;
  private readonly exchange: Exchange;
  private readonly ops: Ops;

  constructor(cfg: IssuerConfig, deps: IssuerDeps) {
    const now = deps.now ?? Date.now;
    this.limits = new RateLimits(now);
    this.audit = new Audit(deps.store, cfg.cellId, { now, log: deps.log });
    this.rehasher = new Rehasher(deps.store, deps.cache, cfg.peppers, this.audit, deps.log);
    const shared = { cache: deps.cache, limits: this.limits, audit: this.audit, rehasher: this.rehasher, now };
    this.exchange = new Exchange(cfg, { ...shared, ...(deps.ipHeader !== undefined ? { ipHeader: deps.ipHeader } : {}) });
    this.ops = new Ops(cfg, shared);
  }

  async fetch(req: Request): Promise<Response> {
    const { pathname } = new URL(req.url);
    const only = (method: string) =>
      req.method === method ? null : refuse(405, "method_not_allowed", `${method} only`, { allow: method });
    switch (pathname) {
      case "/v1/token/streams":
        return only("POST") ?? this.exchange.handle(req);
      case "/healthz":
        return only("GET") ?? this.ops.health();
      case "/v1/admin/metrics":
        return only("GET") ?? this.ops.metrics(req);
      default:
        return refuse(404, "not_found", `no route ${pathname}`);
    }
  }
}
