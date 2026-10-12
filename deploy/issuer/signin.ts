// Signing in to the issuer (public-beta issuer triage B4.1-B4.3, B4.6;
// design §3): the two routes a browser takes, and what the issuer checks
// between them.
//
//   GET /auth/login     30 a minute for the instance (`oauth_start`; per
//                       address too once K4 names the edge's header), else
//                       429 rate_limited; then 303 to the authorization
//                       server with the sign-in's state cookie (oauth.ts).
//   GET /auth/callback  the manager the sign-in proves (oauth.ts
//                       verifyManager), that manager's workspace on the
//                       beta's allow-list (allowlist.ts), then a new
//                       8-hour session (session.ts) and 303 to the page at
//                       `/`. A refusal is 303 to `/?signin=<code>`, which
//                       the page words: denied, expired, failed,
//                       not_oauth, no_user, no_workspace,
//                       invalid_workspace, workspace_not_allowed,
//                       unavailable.
//
// Both answers carry no body, are never cached and send no Referer on;
// the callback always clears the sign-in's state cookie. Signing in to
// another workspace is another sign-in: an OAuth grant names one
// workspace, and its session replaces the browser's.
//
// Owner question I4, answer (a) (the build default): whoever signs in to
// an allow-listed workspace manages its Streams projects; there is no role
// check. The allow-list, the page's name-typed delete confirmation and the
// audit of every change are the controls.
import type { AllowlistReader } from "./allowlist";
import type { IssuerConfig } from "./config";
import { rateLimited } from "./exchange";
import { clientIp, type RateLimits, TRUSTED_CLIENT_IP_HEADER } from "./limits";
import { PrismaSignIn, SIGNIN_COOKIE, SignInRefused } from "./oauth";
import { clearedCookie, newSession, sessionCookie } from "./session";

export interface SignInDeps {
  limits: RateLimits;
  allowlist: AllowlistReader;
  now: () => number;
  log: (line: string) => void;
  /// The edge's trusted client-IP header (K4); null until it is known.
  ipHeader?: string | null;
}

function redirect(location: string, cookies: string[]): Response {
  const headers = new Headers({ location, "cache-control": "no-store", "referrer-policy": "no-referrer" });
  for (const c of cookies) headers.append("set-cookie", c);
  return new Response(null, { status: 303, headers });
}

export class SignIn {
  private readonly prisma: PrismaSignIn;
  private readonly ipHeader: string | null;

  constructor(
    private readonly cfg: IssuerConfig,
    private readonly deps: SignInDeps,
  ) {
    this.prisma = new PrismaSignIn(cfg, deps.now);
    this.ipHeader = deps.ipHeader === undefined ? TRUSTED_CLIENT_IP_HEADER : deps.ipHeader;
  }

  /// GET /auth/login.
  start(req: Request): Response {
    const all = this.deps.limits.take("oauth_start", "*");
    if (all > 0) return rateLimited(all, "too many sign-ins are starting");
    const ip = clientIp(req, this.ipHeader);
    const mine = ip === null ? 0 : this.deps.limits.take("oauth_start_ip", ip);
    if (mine > 0) return rateLimited(mine, "too many sign-ins from this address");
    const { location, cookie } = this.prisma.begin();
    return redirect(location, [cookie]);
  }

  /// GET /auth/callback.
  async finish(req: Request): Promise<Response> {
    const refused = (code: string, why: string) => {
      this.deps.log(`sign-in refused (${code}): ${why}`);
      return redirect(`/?signin=${code}`, [clearedCookie(SIGNIN_COOKIE)]);
    };
    let manager;
    try {
      manager = await this.prisma.verifyManager(req);
    } catch (e) {
      if (e instanceof SignInRefused) return refused(e.code, e.message);
      throw e;
    }
    const { userId, email, workspaceId } = manager;
    const entry = await this.deps.allowlist.entryOf(workspaceId);
    if (entry === null) return refused("unavailable", "the beta's allow-list cannot be read");
    if (entry === undefined) return refused("workspace_not_allowed", `workspace ${workspaceId} is not on the beta's allow-list`);
    const session = newSession({ user_id: userId, email, workspace_id: workspaceId }, this.deps.now());
    this.deps.log(`signed in user ${userId} to workspace ${workspaceId}`);
    return redirect("/", [sessionCookie(this.cfg.sessionKey, session), clearedCookie(SIGNIN_COOKIE)]);
  }
}
