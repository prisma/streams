// A local stand-in for the paging tool (public-beta alerting triage B8),
// for the watch's drills: the three things the real tool, or pair of
// tools, is set up to do (the owner's answer A1 of 2026-10-12: Prisma's
// existing on-call tool, else a standalone one), on one local port, with
// the numbers RUNBOOK's shared-cell rows 1 and 2 state.
//
//   events     POST /v2/enqueue: PagerDuty's Events API v2, as sinks.ts
//              sends it. An event with another routing key, or without a
//              field the API requires, is refused 400 and changes nothing.
//              A trigger opens an incident on its dedup key (another
//              trigger on an open key folds into it), a resolve closes it.
//              Severity critical is a page, the phone; any other a
//              notification, the NOTIFY channel (the urgency rule the real
//              service is set up with).
//   heartbeat  GET /heartbeat/<token>: a beat. Once the first has come, no
//              beat for 3 minutes opens the `heartbeat` page (row 2); the
//              next beat closes it.
//   uptime     every 60 s, GET <cell origin>/health under a 10 s timeout;
//              3 failures in a row open the `uptime check` page (row 1), a
//              2xx closes it. The real check runs from two locations; this
//              one from here.
//
//   listing  GET /changes with `Authorization: Bearer <routing key>`: what
//              it took, as JSON (the changes, the open keys, the beats);
//              GET / answers `ok`. Anything else is 404.
//
// Escalation (to the secondary after 15 minutes unacknowledged) is the
// real tool's own and is not modelled: the drills stop at the page.
// Nothing is forwarded anywhere; what arrives is kept in memory, in order.
//
// The same stand-in deploys as a Composer app of its own (pager-app.ts,
// pager-service.ts, pager-entry.ts) for a scratch or cert cell that has no
// paging service: its https domain is then the watch's ALERT_SINK_URL
// (<domain>/v2/enqueue, ALERT_SINK_KIND events-v2, ALERT_SINK_KEY the
// routing key) and HEARTBEAT_URL (<domain>/heartbeat/<token>), on a
// service apart from the watch, so the dead-man pages while the watch is
// down. pagerOptions reads its inputs. Production pages through the real
// paging service (RUNBOOK §7.5), never this.

import { timingSafeEqual } from "node:crypto";

export const EVENTS_PATH = "/v2/enqueue";
export const CHANGES_PATH = "/changes";
export const HEARTBEAT_PREFIX = "/heartbeat/";
/// Row 2: no beat for 3 minutes.
export const HEARTBEAT_WINDOW_MS = 3 * 60_000;
/// Row 1: a check every 60 s, 10 s timeout, 3 failures in a row.
export const UPTIME_EVERY_MS = 60_000;
export const UPTIME_TIMEOUT_MS = 10_000;
export const UPTIME_FAILURES = 3;
/// The incidents the tool raises itself.
export const HEARTBEAT_KEY = "heartbeat";
export const UPTIME_KEY = "uptime check";

const ACTIONS = ["trigger", "acknowledge", "resolve"];
const SEVERITIES = ["critical", "error", "warning", "info"];

/// The deployed stand-in's inputs, by their platform names.
export const PAGER_INPUTS = ["PAGER_ROUTING_KEY", "PAGER_HEARTBEAT_TOKEN", "STREAMS_CELL_ORIGIN"] as const;
const TOKEN = /^[A-Za-z0-9_-]{16,128}$/;
const LOCAL_HOSTS = ["localhost", "127.0.0.1", "[::1]"];

/// The deployed stand-in's options from its inputs; a refusal names the
/// input, never its value. STREAMS_CELL_ORIGIN `none` runs no uptime check.
export function pagerOptions(inputs: Readonly<Partial<Record<(typeof PAGER_INPUTS)[number], string>>>): PagerOptions {
  for (const name of PAGER_INPUTS) if (!inputs[name]) throw new Error(`${name} is required`);
  const routingKey = inputs.PAGER_ROUTING_KEY as string;
  if (routingKey.length < 16) throw new Error("PAGER_ROUTING_KEY must be at least 16 characters: its endpoint is public");
  const heartbeatToken = inputs.PAGER_HEARTBEAT_TOKEN as string;
  if (!TOKEN.test(heartbeatToken)) {
    throw new Error("PAGER_HEARTBEAT_TOKEN must be 16 to 128 of [A-Za-z0-9_-]: it is the heartbeat URL's last segment");
  }
  const raw = inputs.STREAMS_CELL_ORIGIN as string;
  if (raw === "none") return { routingKey, heartbeatToken, uptimeOrigin: null };
  let u: URL | null = null;
  try {
    u = new URL(raw);
  } catch {}
  const secure = u !== null && (u.protocol === "https:" || (u.protocol === "http:" && LOCAL_HOSTS.includes(u.hostname)));
  if (u === null || !secure || u.pathname !== "/" || u.search !== "" || u.hash !== "") {
    throw new Error("STREAMS_CELL_ORIGIN must be an https origin (http only to this machine), or none");
  }
  return { routingKey, heartbeatToken, uptimeOrigin: u.origin };
}

export interface PagerOptions {
  /// The service's routing key: an event under any other is refused.
  routingKey: string;
  /// The heartbeat monitor's URL is /heartbeat/<token>.
  heartbeatToken: string;
  /// The cell's origin, for the uptime check; null runs none.
  uptimeOrigin: string | null;
  heartbeatWindowMs?: number;
  uptimeEveryMs?: number;
  uptimeTimeoutMs?: number;
  /// Where it listens: 127.0.0.1 on a free port unless given (the deployed
  /// stand-in takes the service's port on every interface).
  port?: number;
  hostname?: string;
}

/// One change on the pager: an incident opened or resolved on a key. A
/// page rings the phone; any other goes to the NOTIFY channel. `summary`
/// is the line an opened incident shows (an event's payload summary).
export interface PagerChange {
  at: number;
  action: "open" | "resolve";
  key: string;
  page: boolean;
  summary: string;
}

type Body = Record<string, unknown>;
const text = (v: unknown, max: number) => typeof v === "string" && v.length > 0 && v.length <= max;

/// Why an Events v2 body is refused, or null when it is one the API takes.
function invalid(b: Body, routingKey: string): string | null {
  if (b.routing_key !== routingKey) return "routing_key";
  if (!ACTIONS.includes(String(b.event_action))) return "event_action";
  if (b.dedup_key !== undefined && !text(b.dedup_key, 255)) return "dedup_key";
  if (b.event_action !== "trigger") return b.dedup_key === undefined ? "dedup_key" : null;
  const p = Object(b.payload) as Body;
  if (!text(p.summary, 1024)) return "payload.summary";
  if (!text(p.source, 255)) return "payload.source";
  if (!SEVERITIES.includes(String(p.severity))) return "payload.severity";
  if (p.timestamp !== undefined && Number.isNaN(Date.parse(String(p.timestamp)))) return "payload.timestamp";
  return null;
}

const answer = (status: number, doc: unknown) =>
  new Response(JSON.stringify(doc), { status, headers: { "content-type": "application/json" } });

export class Pager {
  /// Every change, oldest first.
  readonly changes: PagerChange[] = [];
  /// When each beat came.
  readonly beats: number[] = [];
  /// The open incidents: key -> whether it is a page.
  private readonly open = new Map<string, boolean>();
  private readonly server: ReturnType<typeof Bun.serve>;
  private readonly timers: ReturnType<typeof setInterval>[] = [];
  private failures = 0;

  constructor(private readonly opts: PagerOptions) {
    this.server = Bun.serve({ port: opts.port ?? 0, hostname: opts.hostname ?? "127.0.0.1", fetch: (req) => this.serve(req) });
    const window = opts.heartbeatWindowMs ?? HEARTBEAT_WINDOW_MS;
    this.timers.push(setInterval(() => this.checkHeartbeat(window), Math.min(1_000, window / 4)));
    if (opts.uptimeOrigin !== null) {
      this.timers.push(setInterval(() => void this.checkUptime(), opts.uptimeEveryMs ?? UPTIME_EVERY_MS));
    }
  }

  get eventsUrl(): string {
    return `http://127.0.0.1:${this.server.port}${EVENTS_PATH}`;
  }

  get heartbeatUrl(): string {
    return `http://127.0.0.1:${this.server.port}${HEARTBEAT_PREFIX}${this.opts.heartbeatToken}`;
  }

  /// The changes from index `from` on, each as `[action, key, page]`.
  since(from: number): [PagerChange["action"], string, boolean][] {
    return this.changes.slice(from).map((c) => [c.action, c.key, c.page]);
  }

  stop(): void {
    for (const t of this.timers) clearInterval(t);
    this.server.stop(true);
  }

  /// Opens an incident on `key` unless one is open.
  private raise(key: string, page: boolean, summary: string): void {
    if (this.open.has(key)) return;
    this.open.set(key, page);
    this.changes.push({ at: Date.now(), action: "open", key, page, summary });
  }

  /// Resolves the incident open on `key`, if one is.
  private close(key: string): void {
    const page = this.open.get(key);
    if (page === undefined) return;
    this.open.delete(key);
    this.changes.push({ at: Date.now(), action: "resolve", key, page, summary: "" });
  }

  private async serve(req: Request): Promise<Response> {
    const { pathname } = new URL(req.url);
    if (pathname === `${HEARTBEAT_PREFIX}${this.opts.heartbeatToken}` && req.method === "GET") {
      this.beats.push(Date.now());
      this.close(HEARTBEAT_KEY);
      return new Response("OK");
    }
    if (pathname === "/" && req.method === "GET") return new Response("ok");
    if (pathname === CHANGES_PATH && req.method === "GET") return this.listing(req);
    if (pathname !== EVENTS_PATH || req.method !== "POST") return new Response(null, { status: 404 });
    let b: Body;
    try {
      b = (await req.json()) as Body;
    } catch {
      return answer(400, { status: "invalid event", message: "not JSON" });
    }
    const why = invalid(Object(b) as Body, this.opts.routingKey);
    if (why !== null) return answer(400, { status: "invalid event", message: "Event object is invalid", errors: [why] });
    const key = String(b.dedup_key);
    const p = b.payload as Body;
    if (b.event_action === "trigger") this.raise(key, p.severity === "critical", String(p.summary));
    if (b.event_action === "resolve") this.close(key);
    return answer(202, { status: "success", message: "Event processed", dedup_key: key });
  }

  /// GET /changes, to the routing key's bearer only.
  private listing(req: Request): Response {
    const want = new TextEncoder().encode(`Bearer ${this.opts.routingKey}`);
    const got = new TextEncoder().encode(req.headers.get("authorization") ?? "");
    if (got.length !== want.length || !timingSafeEqual(got, want)) return answer(401, { status: "unauthorized" });
    return answer(200, { open: [...this.open.keys()], changes: this.changes, beats: this.beats.length, last_beat_ms: this.beats.at(-1) ?? null });
  }

  /// Before the first beat the monitor waits (as the real one does).
  private checkHeartbeat(window: number): void {
    const now = Date.now();
    if (now - (this.beats.at(-1) ?? now) > window) this.raise(HEARTBEAT_KEY, true, `no heartbeat for ${window / 1_000} s`);
  }

  private async checkUptime(): Promise<void> {
    let up = false;
    try {
      const timeout = AbortSignal.timeout(this.opts.uptimeTimeoutMs ?? UPTIME_TIMEOUT_MS);
      up = (await fetch(`${this.opts.uptimeOrigin}/health`, { signal: timeout })).ok;
    } catch {}
    this.failures = up ? 0 : this.failures + 1;
    if (up) this.close(UPTIME_KEY);
    else if (this.failures >= UPTIME_FAILURES) this.raise(UPTIME_KEY, true, `${this.failures} failed checks of /health in a row`);
  }
}
