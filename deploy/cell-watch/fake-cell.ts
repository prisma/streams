// Local stand-ins for what the cell watch talks to, for its tests: a cell
// answering /health and the two operator documents as the captured
// fixtures do (rig.ts) and keeping the canary's stream as its product
// surface does, and an issuer answering /healthz, /v1/admin/metrics and
// the canary's exchange; FakeServer alone stands in for a sink or a
// heartbeat monitor. Every server records each request it got: the path
// and query, the client's port (one port is one connection), the
// connection, authorization, content-type and stream-key headers and the
// body, so a test sees which connection carried what.
import { RECORDS_PATH, STREAM_PATH } from "./canary";
import { BILLING, BOOT, DATA, GIT } from "./rig";
import type { Json } from "./rules";

export const OPERATOR_TOKEN = "operator-bearer-for-the-watch-tests";
export const METRICS_TOKEN = "issuer-metrics-bearer-for-the-watch-tests";
/// The canary's credential, in the form the issuer's exchange takes
/// (`prisma_streams_<credential id>.<secret>`), and the token it mints.
export const CANARY_CREDENTIAL = `prisma_streams_strcred_${"c0".repeat(16)}.${"canarysecretforthewatchtests".padEnd(43, "x")}`;
export const CANARY_TOKEN = "canary-access-token-for-the-watch-tests";

export interface Seen {
  method: string;
  path: string;
  /// The query, with its `?` (empty without one).
  query: string;
  /// The client's port: requests with one port came on one connection.
  port: number;
  connection: string | null;
  authorization: string | null;
  contentType: string | null;
  /// `streams-encryption-key`: the stream key the canary presents. The
  /// Composer edge strips every `Prisma-*` request header, so a deployed
  /// cell sees a key only under its `Streams-` name (edge change #173).
  encryptionKey: string | null;
  body: string;
}

export interface Answer {
  status: number;
  headers?: Record<string, string>;
  body: string;
  /// Held this long before the answer goes out.
  delayMs?: number;
}

type Route = (req: Request, body: string) => Answer;

/// A server on a free local port answering its routes (404 elsewhere).
export class FakeServer {
  readonly seen: Seen[] = [];
  private readonly server: ReturnType<typeof Bun.serve>;

  constructor(public routes: Record<string, Route>) {
    this.server = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch: async (req, srv) => {
        const { pathname: path, search: query } = new URL(req.url);
        const body = await req.text();
        this.seen.push({
          method: req.method,
          path,
          query,
          port: srv.requestIP(req)?.port ?? 0,
          connection: req.headers.get("connection"),
          authorization: req.headers.get("authorization"),
          contentType: req.headers.get("content-type"),
          encryptionKey: req.headers.get("streams-encryption-key"),
          body,
        });
        const route = this.routes[path];
        const a = route === undefined ? { status: 404, body: "" } : route(req, body);
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

/// What the cell's origin marker layer adds to every answer (src/http.rs).
const ORIGIN = { "prisma-streams-origin": "streams/test" };

/// A refusal in the product envelope, from the server.
const refusal = (status: number, code: string): Answer => ({
  status,
  headers: { ...ORIGIN, "content-type": "application/json" },
  body: JSON.stringify({ error: { code, message: `refused: ${code}`, retryable: false } }),
});

/// The canary's stream as the cell's product surface keeps it: the key it
/// was created under and its records, each read position `c<n>` the
/// count of records before it.
type StreamCall = "create" | "append" | "read";

export interface FakeStream {
  key: string;
  config: string;
  records: Json[];
}

/// The cell: /health, and /operator/data.json and /operator/billing.json
/// behind OPERATOR_TOKEN, and the canary's stream behind CANARY_TOKEN.
/// Each answer can be replaced while it runs.
export class FakeCell {
  health: Answer = HEALTHY;
  data: Answer = json(DATA);
  billing: Answer = json(BILLING);
  /// The canary's stream; null until it is created.
  stream: FakeStream | null = null;
  /// An answer that replaces the stream's own for that request while set.
  canary: Partial<Record<StreamCall, Answer>> = {};
  /// An answer that replaces the stream's own for the next such request only.
  once: Partial<Record<StreamCall, Answer>> = {};
  /// Acknowledges appends without keeping them (a cell that loses them).
  losesAppends = false;
  readonly server: FakeServer;

  constructor() {
    this.server = new FakeServer({
      "/health": () => this.health,
      "/operator/data.json": (req) => bearer(req, OPERATOR_TOKEN, () => this.data),
      "/operator/billing.json": (req) => bearer(req, OPERATOR_TOKEN, () => this.billing),
      [STREAM_PATH]: (req, body) => this.replaced("create") ?? this.streamCall(req, () => this.create(req, body)),
      [RECORDS_PATH]: (req, body) =>
        req.method === "POST"
          ? (this.replaced("append") ?? this.streamCall(req, (s) => this.append(s, body)))
          : (this.replaced("read") ?? this.streamCall(req, (s) => this.read(s, req))),
    });
  }

  private replaced(call: StreamCall): Answer | undefined {
    const once = this.once[call];
    delete this.once[call];
    return once ?? this.canary[call];
  }

  /// The checks the cell makes before a stream request: the token, then
  /// the key against the one the stream was created under.
  private streamCall(req: Request, answer: (s: FakeStream) => Answer): Answer {
    if (req.headers.get("authorization") !== `Bearer ${CANARY_TOKEN}`) return refusal(401, "kid_unknown");
    // The edge strips `Prisma-*` request headers (edge change #173).
    const key = req.headers.get("streams-encryption-key");
    if (key === null) return refusal(400, "missing_key");
    if (req.method === "PUT") return answer(this.stream as FakeStream);
    if (this.stream === null) return refusal(404, "not_found");
    return this.stream.key === key ? answer(this.stream) : refusal(403, "wrong_key");
  }

  private create(req: Request, config: string): Answer {
    const key = req.headers.get("streams-encryption-key") as string;
    if (this.stream === null) {
      this.stream = { key, config, records: [] };
      return { status: 201, headers: ORIGIN, body: "{}" };
    }
    if (this.stream.key !== key) return refusal(403, "wrong_key");
    return this.stream.config === config ? { status: 200, headers: ORIGIN, body: "{}" } : refusal(409, "config_mismatch");
  }

  private append(s: FakeStream, body: string): Answer {
    if (!this.losesAppends) s.records.push(JSON.parse(body) as Json);
    const cursor = `c${s.records.length}`;
    return { status: 200, headers: ORIGIN, body: JSON.stringify({ cursor, count: 1, duplicate: false, sealed: false }) };
  }

  private read(s: FakeStream, req: Request): Answer {
    const cursor = new URL(req.url).searchParams.get("cursor");
    const at = cursor === "now" ? s.records.length : /^c(\d+)$/.exec(cursor ?? "")?.[1];
    if (at === undefined) return refusal(400, "invalid_cursor");
    return {
      status: 200,
      headers: { ...ORIGIN, "prisma-next-cursor": `c${s.records.length}`, "prisma-up-to-date": "true" },
      body: JSON.stringify(s.records.slice(Number(at))),
    };
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

/// The issuer: /healthz, /v1/admin/metrics behind METRICS_TOKEN, and the
/// exchange, which mints CANARY_TOKEN for CANARY_CREDENTIAL (and refuses
/// any other) with the configured `endpoint`, as deploy/issuer/exchange.ts
/// answers.
export class FakeIssuer {
  health: Answer = json({ status: "ok" });
  metrics: Answer = json(ISSUER_METRICS);
  /// The `endpoint` the exchange answers.
  endpoint: string | null = null;
  expiresIn = 3600;
  /// An answer that replaces the exchange's own while set.
  exchange: Answer | null = null;
  readonly server: FakeServer;

  constructor() {
    this.server = new FakeServer({
      "/healthz": () => this.health,
      "/v1/admin/metrics": (req) => bearer(req, METRICS_TOKEN, () => this.metrics),
      "/v1/token/streams": (req) => this.exchange ?? this.mint(req),
    });
  }

  private mint(req: Request): Answer {
    if (req.method !== "POST") return { status: 405, body: "" };
    if (req.headers.get("authorization") !== `StreamsCredential ${CANARY_CREDENTIAL}`) {
      return { status: 401, headers: { "content-type": "application/json" }, body: '{"error":{"code":"invalid_credential","message":"unknown credential or wrong secret"}}' };
    }
    return json({
      accessToken: CANARY_TOKEN,
      tokenType: "Bearer",
      expiresIn: this.expiresIn,
      projectId: "sp_canary_cell_sc1",
      cellId: "cell_sc1",
      endpoint: this.endpoint,
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
