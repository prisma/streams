// Local stand-ins for what the cell watch talks to, for its tests: a cell
// answering /health and the two operator documents as the captured
// fixtures do (rig.ts), and an issuer answering /healthz and
// /v1/admin/metrics; FakeServer alone stands in for a sink or a heartbeat
// monitor. Every server records each request it got: the path, the
// client's port (one port is one connection), the connection,
// authorization and content-type headers and the body, so a test sees
// which connection carried what.
import { BILLING, BOOT, DATA, GIT } from "./rig";
import type { Json } from "./rules";

export const OPERATOR_TOKEN = "operator-bearer-for-the-watch-tests";
export const METRICS_TOKEN = "issuer-metrics-bearer-for-the-watch-tests";

export interface Seen {
  method: string;
  path: string;
  /// The client's port: requests with one port came on one connection.
  port: number;
  connection: string | null;
  authorization: string | null;
  contentType: string | null;
  body: string;
}

export interface Answer {
  status: number;
  headers?: Record<string, string>;
  body: string;
  /// Held this long before the answer goes out.
  delayMs?: number;
}

type Route = (req: Request) => Answer;

/// A server on a free local port answering its routes (404 elsewhere).
export class FakeServer {
  readonly seen: Seen[] = [];
  private readonly server: ReturnType<typeof Bun.serve>;

  constructor(public routes: Record<string, Route>) {
    this.server = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch: async (req, srv) => {
        const path = new URL(req.url).pathname;
        this.seen.push({
          method: req.method,
          path,
          port: srv.requestIP(req)?.port ?? 0,
          connection: req.headers.get("connection"),
          authorization: req.headers.get("authorization"),
          contentType: req.headers.get("content-type"),
          body: await req.text(),
        });
        const route = this.routes[path];
        const a = route === undefined ? { status: 404, body: "" } : route(req);
        if (a.delayMs !== undefined) await Bun.sleep(a.delayMs);
        return new Response(a.body, { status: a.status, headers: a.headers });
      },
    });
  }

  get origin(): string {
    return `http://127.0.0.1:${this.server.port}`;
  }

  /// The paths requested, in order.
  paths(): string[] {
    return this.seen.map((s) => s.path);
  }

  stop(): void {
    this.server.stop(true);
  }
}

const json = (doc: Json): Answer => ({ status: 200, headers: { "content-type": "application/json" }, body: JSON.stringify(doc) });

/// 401 unless the request carries `Bearer <token>`.
function bearer(req: Request, token: string, answer: () => Answer): Answer {
  return req.headers.get("authorization") === `Bearer ${token}` ? answer() : { status: 401, body: '{"error":"unauthorized"}' };
}

/// The healthy cell's /health: 200 "ok" with the origin and identity headers.
export const HEALTHY: Answer = {
  status: 200,
  headers: { "prisma-streams-origin": "streams/test", "x-streams-git": GIT, "x-streams-boot-id": BOOT },
  body: "ok",
};

/// The cell: /health, and /operator/data.json and /operator/billing.json
/// behind OPERATOR_TOKEN. Each answer can be replaced while it runs.
export class FakeCell {
  health: Answer = HEALTHY;
  data: Answer = json(DATA);
  billing: Answer = json(BILLING);
  readonly server: FakeServer;

  constructor() {
    this.server = new FakeServer({
      "/health": () => this.health,
      "/operator/data.json": (req) => bearer(req, OPERATOR_TOKEN, () => this.data),
      "/operator/billing.json": (req) => bearer(req, OPERATOR_TOKEN, () => this.billing),
    });
  }

  /// Serve `doc` as /operator/data.json.
  setData(doc: Json): void {
    this.data = json(doc);
  }

  get origin(): string {
    return this.server.origin;
  }

  get seen(): Seen[] {
    return this.server.seen;
  }

  stop(): void {
    this.server.stop();
  }
}

export const ISSUER_METRICS = { cache_age_secs: 12, gate_open: true, profile_ok: true };

/// The issuer: /healthz, and /v1/admin/metrics behind METRICS_TOKEN.
export class FakeIssuer {
  health: Answer = json({ status: "ok" });
  metrics: Answer = json(ISSUER_METRICS);
  readonly server: FakeServer;

  constructor() {
    this.server = new FakeServer({
      "/healthz": () => this.health,
      "/v1/admin/metrics": (req) => bearer(req, METRICS_TOKEN, () => this.metrics),
    });
  }

  get origin(): string {
    return this.server.origin;
  }

  get seen(): Seen[] {
    return this.server.seen;
  }

  stop(): void {
    this.server.stop();
  }
}
