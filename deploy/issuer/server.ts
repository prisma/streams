// The interim issuer's HTTP surface (public-beta issuer triage B2): one
// handler over the routes below; everything else answers 404, and a route
// asked with another method 405.
//
//   POST /v1/token/streams   the exchange (exchange.ts)
import type { StateCache } from "./cache";
import type { IssuerConfig } from "./config";
import { Exchange } from "./exchange";
import { refuse } from "./respond";

export interface IssuerDeps {
  cache: StateCache;
  now?: () => number;
}

export class Issuer {
  private readonly exchange: Exchange;

  constructor(cfg: IssuerConfig, deps: IssuerDeps) {
    this.exchange = new Exchange(cfg, deps.cache, deps.now);
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
