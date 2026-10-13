// The cell watch's poller (public-beta alerting triage B3; D6): one Poll
// for rules.ts each cycle, from GETs to the configured origins at the
// paths in PATHS and nowhere else (never /v1/debug/*):
//
//   cell   /health                 every cycle, on a fresh connection
//                                  (`Connection: close`, keep-alive off,
//                                  no bearer): the edge's wall of 1,536
//                                  sockets per origin service refuses only
//                                  new connections, so a watch that reused
//                                  one would never meet it (row 3).
//   cell   /operator/data.json     every cycle, then /operator/billing.json
//                                  every fourth (60 s), one after the other
//                                  with the deployment bearer, so they ride
//                                  one kept-alive connection.
//   issuer /healthz                every cycle, then /v1/admin/metrics every
//                                  fourth with the metrics bearer, when
//                                  ISSUER_ORIGIN is set (and the token, for
//                                  the metrics).
//
// Each GET has a 5 s timeout that covers its body, and none follows a
// redirect: a 3xx is an answer like any other, and a bearer goes to its
// configured origin only. A document that is not a 200 with JSON is null
// in the poll, which rules.ts reads as a source it lacks. /health's body
// is kept to its first 256 characters, which rules.ts classifies into an
// enum and never forwards. The canary is triage B7's: `canary` stays null.
import type { HealthPoll, IssuerHealthPoll, Json, Poll } from "./rules";

/// One cycle every 15 s; billing and the issuer's metrics every fourth.
export const POLL_MS = 15_000;
export const SLOW_EVERY = 4;
export const TIMEOUT_MS = 5_000;
/// /health's 503 reasons and the wrapper's `binary_exited` open its body.
export const BODY_PREFIX_CHARS = 256;

/// Every path the watch reads.
export const PATHS = {
  health: "/health",
  data: "/operator/data.json",
  billing: "/operator/billing.json",
  issuerHealth: "/healthz",
  issuerMetrics: "/v1/admin/metrics",
} as const;

export interface PollConfig {
  /// STREAMS_CELL_ORIGIN.
  cellOrigin: string;
  /// STREAMS_OPERATOR_TOKEN: the cell's deployment bearer.
  operatorToken: string;
  /// ISSUER_ORIGIN, or null: no issuer GET.
  issuerOrigin: string | null;
  /// STREAMS_ISSUER_METRICS_TOKEN, or null: no metrics GET.
  issuerMetricsToken: string | null;
  timeoutMs?: number;
}

type Got = { status: number; headers: Headers; body: string } | { status: null; timedOut: boolean };

/// One GET under the timeout, its body read whole.
async function get(url: URL, init: RequestInit, timeoutMs: number): Promise<Got> {
  const signal = AbortSignal.timeout(timeoutMs);
  try {
    const res = await fetch(url, { ...init, method: "GET", signal, redirect: "manual" });
    return { status: res.status, headers: res.headers, body: await res.text() };
  } catch {
    return { status: null, timedOut: signal.aborted };
  }
}

function jsonOf(got: Got): Json | null {
  if (got.status !== 200) return null;
  try {
    return JSON.parse(got.body) as Json;
  } catch {
    return null;
  }
}

export class Poller {
  private cycles = 0;
  private readonly timeoutMs: number;

  constructor(
    private readonly cfg: PollConfig,
    private readonly now: () => number = Date.now,
  ) {
    this.timeoutMs = cfg.timeoutMs ?? TIMEOUT_MS;
  }

  /// One cycle's GETs, the cell's two connections and the issuer's at once.
  async poll(): Promise<Poll> {
    const at = this.now();
    const slow = this.cycles % SLOW_EVERY === 0;
    this.cycles += 1;
    const [health, [data, billing], [issuerHealth, issuerMetrics]] = await Promise.all([
      this.health(),
      this.operator(slow),
      this.issuer(slow),
    ]);
    return { at, health, data, billing, issuerHealth, issuerMetrics, canary: null };
  }

  private async health(): Promise<HealthPoll> {
    const url = new URL(PATHS.health, this.cfg.cellOrigin);
    const got = await get(url, { keepalive: false, headers: { connection: "close" } }, this.timeoutMs);
    if (got.status === null) return { status: null, timedOut: got.timedOut, origin: false, git: null, bootId: null, bodyPrefix: "" };
    return {
      status: got.status,
      timedOut: false,
      origin: got.headers.has("prisma-streams-origin"),
      git: got.headers.get("x-streams-git"),
      bootId: got.headers.get("x-streams-boot-id"),
      bodyPrefix: got.body.slice(0, BODY_PREFIX_CHARS),
    };
  }

  private bearer(origin: string, path: string, token: string): Promise<Got> {
    return get(new URL(path, origin), { headers: { authorization: `Bearer ${token}` } }, this.timeoutMs);
  }

  private async operator(slow: boolean): Promise<[Json | null, Json | null]> {
    const { cellOrigin, operatorToken } = this.cfg;
    const data = jsonOf(await this.bearer(cellOrigin, PATHS.data, operatorToken));
    const billing = slow ? jsonOf(await this.bearer(cellOrigin, PATHS.billing, operatorToken)) : null;
    return [data, billing];
  }

  private async issuer(slow: boolean): Promise<[IssuerHealthPoll | null, Json | null]> {
    const { issuerOrigin, issuerMetricsToken } = this.cfg;
    if (issuerOrigin === null) return [null, null];
    const got = await get(new URL(PATHS.issuerHealth, issuerOrigin), {}, this.timeoutMs);
    const health = got.status === null ? { status: null, timedOut: got.timedOut } : { status: got.status, timedOut: false };
    if (!slow || issuerMetricsToken === null) return [health, null];
    return [health, jsonOf(await this.bearer(issuerOrigin, PATHS.issuerMetrics, issuerMetricsToken))];
  }
}
