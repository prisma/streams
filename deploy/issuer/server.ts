// The interim issuer's HTTP surface (public-beta issuer triage B2): one
// handler over the routes below; everything else answers 404, and a route
// asked with another method 405.
//
//   POST /v1/token/streams   the exchange (exchange.ts)
//   GET  /healthz            whether it can mint (ops.ts)
//   GET  /v1/admin/metrics   its counters, behind the metrics bearer (ops.ts)
//   GET  /auth/login         "Sign in with Prisma" starts (signin.ts)
//   GET  /auth/callback      and finishes with a session (signin.ts)
//   GET  /auth/session       who the page is signed in as (signin.ts)
//   GET  /                   the page (page.ts)
//   /v1/workspaces/{w}/streams/projects...
//                            the management API, for a signed-in browser
//                            (management.ts)
// Beside the routes, it holds the jobs main.ts starts: the audit's tick,
// the re-hash sweep (rehash.ts) and the offboarding job (offboarding.ts).
import type { StateStore } from "../cell-admin/store";
import { AllowlistReader } from "./allowlist";
import { Audit } from "./audit";
import type { StateCache } from "./cache";
import type { IssuerConfig } from "./config";
import { Exchange } from "./exchange";
import { RateLimits } from "./limits";
import { Management } from "./management";
import { Offboarding } from "./offboarding";
import { Ops } from "./ops";
import { servePage } from "./page";
import { Rehasher } from "./rehash";
import { refuse } from "./respond";
import { SignIn } from "./signin";

export interface IssuerDeps {
  store: StateStore;
  cache: StateCache;
  now?: () => number;
  log?: (line: string) => void;
  /// The edge's trusted client-IP header (limits.ts, K4).
  ipHeader?: string | null;
  /// The offboarding drain's settle time, shortened by tests (60 s).
  offboardSettleMs?: number;
}

export class Issuer {
  readonly limits: RateLimits;
  readonly audit: Audit;
  readonly rehasher: Rehasher;
  readonly offboarding: Offboarding;
  private readonly exchange: Exchange;
  private readonly ops: Ops;
  private readonly management: Management;
  private readonly signIn: SignIn;

  constructor(cfg: IssuerConfig, deps: IssuerDeps) {
    const now = deps.now ?? Date.now;
    const log = deps.log ?? console.log;
    this.limits = new RateLimits(now);
    this.audit = new Audit(deps.store, cfg.cellId, { now, log: deps.log });
    this.rehasher = new Rehasher(deps.cache, cfg.peppers, this.audit, deps.log);
    this.offboarding = new Offboarding(cfg, {
      cache: deps.cache,
      audit: this.audit,
      now,
      log,
      ...(deps.offboardSettleMs !== undefined ? { settleMs: deps.offboardSettleMs } : {}),
    });
    const shared = { cache: deps.cache, limits: this.limits, audit: this.audit, rehasher: this.rehasher, now };
    this.exchange = new Exchange(cfg, { ...shared, ...(deps.ipHeader !== undefined ? { ipHeader: deps.ipHeader } : {}) });
    this.ops = new Ops(cfg, shared);
    const allowlist = new AllowlistReader(deps.store, now, log);
    this.management = new Management(cfg, {
      cache: deps.cache,
      store: deps.store,
      limits: this.limits,
      audit: this.audit,
      allowlist,
      now,
      log,
    });
    this.signIn = new SignIn(cfg, {
      limits: this.limits,
      allowlist,
      now,
      log,
      ...(deps.ipHeader !== undefined ? { ipHeader: deps.ipHeader } : {}),
    });
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
      case "/auth/login":
        return only("GET") ?? this.signIn.start(req);
      case "/auth/callback":
        return only("GET") ?? this.signIn.finish(req);
      case "/auth/session":
        return only("GET") ?? this.signIn.session(req);
      case "/":
        return only("GET") ?? servePage();
      default:
        return (await this.management.handle(req, pathname)) ?? refuse(404, "not_found", `no route ${pathname}`);
    }
  }
}
