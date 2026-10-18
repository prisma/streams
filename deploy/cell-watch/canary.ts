// The cell watch's canary (public-beta alerting triage B7; D4): the end to
// end probe of rows 19 and 22, made as any customer makes it. The canary
// project `sp_canary_<cell id>` (cell-admin places it at `init`: workspace
// streams-issuer-canary, 5 requests/s, 1 stream) holds a hidden customer
// credential with a secret, which the watch holds as CANARY_CREDENTIAL. A
// round:
//
//   issuer POST /v1/token/streams    the credential, `Authorization:
//                                    StreamsCredential <credential>`, no
//                                    body, as the SDK sends it; only when
//                                    the watch holds no token, its token is
//                                    two minutes from expiring
//                                    (`expiresIn`), or fewer than the last
//                                    four exchanges were good (below), so
//                                    most rounds exchange nothing
//                                    (`exchange` null)
//   cell   GET  .../records?cursor=now
//                                    the tail to read back from, when the
//                                    watch has none (its first round, and
//                                    after a read that missed or a 404)
//   cell   PUT  /v1/streams/cell-watch-canary
//                                    only when that GET answered 404: a
//                                    JSON stream removed after a day
//                                    without an append (`expiry.idle`),
//                                    then the GET again. A create weighs
//                                    11 on the project's 5 requests/s
//                                    store budget, and another create is
//                                    refused 429 project_rate_limit while
//                                    it is in debt (edge record #143), so
//                                    a watch that starts beside a running
//                                    one (a deploy's overlap) or after it
//                                    never creates
//   cell   POST .../records          one record, `{"canary": <nonce>, "at":
//                                    <the watch's clock>}`
//   cell   GET  .../records?cursor=<tail>
//                                    durable (the default): the record must
//                                    be among those after the tail (G1, G5);
//                                    the page's end is the next tail
//
// The exchange is the issuer's step (row 22, which also judges its
// `endpoint` against STREAMS_CELL_ORIGIN); the create and the append are
// the append step and the GETs the read step (row 19). A step that fails
// ends the round. Each step reports its status (null without an answer),
// the body's error code (the product envelope's `error.code`, or the
// wrapper's flat `error`) and whether the server's origin header came
// back; rules.ts classifies them and forwards nothing else. A cell's 401
// drops the token, so the next round exchanges again.
//
// Row 22 judges only the rounds that exchange, and opens and resolves on
// rounds in a row (rules.ts: three to open, RESOLVE_AFTER clean to
// resolve). So an exchange that is not good (refused, or a 200 whose
// `endpoint` is not STREAMS_CELL_ORIGIN) makes the canary exchange every
// round until RESOLVE_AFTER in a row are good: a wrong `endpoint` pages
// in three minutes, not three token lifetimes, and a recovered issuer
// resolves in four. The token itself is used until two minutes before it
// expires, whatever the exchanges in between answer: the cell's probe
// (row 19) does not stop because the issuer did.
//
// The credential goes to ISSUER_ORIGIN's exchange and nowhere else, the
// token to STREAMS_CELL_ORIGIN and nowhere else: never to the `endpoint`
// the issuer answered. Every request has the poller's 5 s timeout and
// follows no redirect. The stream's key protects records that carry only
// a nonce and a clock, so it is derived from the cell's id: every watch of
// the cell, a restarted one and one whose credential was replaced
// included, presents the key the stream was created under.
import { createHash, randomBytes } from "node:crypto";
import { type CanaryResult, type CanaryStep, RESOLVE_AFTER, sameOrigin } from "./rules";

export const EXCHANGE_PATH = "/v1/token/streams";
/// The canary project's one stream (its quota is one).
export const CANARY_STREAM = "cell-watch-canary";
export const STREAM_PATH = `/v1/streams/${CANARY_STREAM}`;
export const RECORDS_PATH = `${STREAM_PATH}/records`;
/// The stream's creation document: JSON records, removed a day after its
/// last append, so a cell whose watch is gone keeps it at most a day. The
/// product trims nothing by age (the triage's "1-day retention" has no
/// setting): while a watch appends, the stream keeps every record, about
/// 1,440 of 64 bytes a day.
export const STREAM_CONFIG = { format: { kind: "json" }, expiry: { idle: "1d" } } as const;
/// What the watch's credential grants (`cell-admin credential --hidden
/// --scopes`): its stream's create, append and read, and nothing else.
export const CANARY_SCOPES = "streams.create streams.records.append streams.records.read";
/// A token is renewed this long before it expires.
export const RENEW_BEFORE_MS = 120_000;
/// A read page: the smallest the cell takes, about 60 of the canary's records.
export const PAGE_BYTES = 4096;

export interface CanaryConfig {
  /// STREAMS_CELL_ORIGIN.
  cellOrigin: string;
  /// ISSUER_ORIGIN.
  issuerOrigin: string;
  /// CANARY_CREDENTIAL: `prisma_streams_<credential id>.<secret>`.
  credential: string;
  /// STREAMS_CELL_ID: the stream key's source.
  cellId: string;
  timeoutMs: number;
}

/// The canary stream's key for the cell `cellId`: 32 bytes, base64url.
export function canaryStreamKey(cellId: string): string {
  return createHash("sha256").update(`prisma-streams-cell-watch-canary:${cellId}`).digest("base64url");
}

/// An answer; a status of null is none (a timeout, a refused connection).
interface Got {
  status: number | null;
  headers: Headers;
  body: string;
}

/// One request under the timeout, its body read whole; no redirect followed.
async function send(url: URL, init: RequestInit, timeoutMs: number): Promise<Got> {
  try {
    const res = await fetch(url, { ...init, signal: AbortSignal.timeout(timeoutMs), redirect: "manual" });
    return { status: res.status, headers: res.headers, body: await res.text() };
  } catch {
    return { status: null, headers: new Headers(), body: "" };
  }
}

function parsed(body: string): unknown {
  try {
    return JSON.parse(body);
  } catch {
    return undefined;
  }
}

const field = (doc: unknown, name: string): unknown =>
  doc !== null && typeof doc === "object" && !Array.isArray(doc) ? (doc as Record<string, unknown>)[name] : undefined;

/// The answer's error code: `error.code`, or the wrapper's flat `error`.
function codeOf(body: string): string | null {
  const error = field(parsed(body), "error");
  const code = typeof error === "string" ? error : field(error, "code");
  return typeof code === "string" ? code : null;
}

/// A cell's response header by its name after `Prisma-`: the `Streams-`
/// form first, the only one a client behind the Prisma Compute edge gets
/// (the edge strips `Prisma-*` response headers; edge change #176), then
/// the `Prisma-` form a cell reached directly, or an older one, sends. A
/// `Prisma-Streams-<X>` name's `Streams-` form is `Streams-<X>`
/// (`streams-origin` for the origin marker), the prefix once.
export function cellHeader(headers: Headers, name: string): string | null {
  const streams = name.startsWith("streams-") ? name : `streams-${name}`;
  return headers.get(streams) ?? headers.get(`prisma-${name}`);
}

/// A step's report; a step without an answer has no code and no origin.
function stepOf(got: Got, ok: boolean): CanaryStep {
  return { ok, status: got.status, errorCode: codeOf(got.body), origin: cellHeader(got.headers, "streams-origin") !== null };
}

export class Canary {
  private token: { value: string; renewAt: number } | null = null;
  /// Good exchanges in a row since the last that was not (a new watch has
  /// seen none that was not).
  private good = RESOLVE_AFTER;
  /// Where the next record is read back from: the end of the last read.
  private tail: string | null = null;
  private readonly key: string;

  constructor(
    private readonly cfg: CanaryConfig,
    private readonly now: () => number = Date.now,
    private readonly nonce: () => string = () => randomBytes(16).toString("hex"),
  ) {
    this.key = canaryStreamKey(cfg.cellId);
  }

  /// One round: the exchange when due, then the stream's steps with the
  /// token while it is usable.
  async run(): Promise<CanaryResult> {
    const exchange = this.usable() === null || this.good < RESOLVE_AFTER ? await this.exchange() : null;
    const token = this.usable();
    if (token === null) return { exchange, append: null, read: null };
    return { exchange, ...(await this.probe(token)) };
  }

  /// The token, until two minutes before it expires.
  private usable(): string | null {
    return this.token !== null && this.now() < this.token.renewAt ? this.token.value : null;
  }

  private async exchange(): Promise<NonNullable<CanaryResult["exchange"]>> {
    const at = this.now();
    const got = await send(
      new URL(EXCHANGE_PATH, this.cfg.issuerOrigin),
      { method: "POST", headers: { authorization: `StreamsCredential ${this.cfg.credential}` } },
      this.cfg.timeoutMs,
    );
    const doc = got.status === 200 ? parsed(got.body) : undefined;
    const value = field(doc, "accessToken");
    const expiresIn = field(doc, "expiresIn");
    const endpoint = field(doc, "endpoint");
    const ok = typeof value === "string" && value !== "" && typeof expiresIn === "number" && expiresIn > 0;
    if (ok) this.token = { value, renewAt: at + expiresIn * 1_000 - RENEW_BEFORE_MS };
    const answered = ok && typeof endpoint === "string" ? endpoint : null;
    this.good = ok && sameOrigin(answered, this.cfg.cellOrigin) ? this.good + 1 : 0;
    return { ...stepOf(got, ok), endpoint: answered };
  }

  /// The tail when due (the stream created when it is missing), the
  /// append and the read back.
  private async probe(token: string): Promise<Pick<CanaryResult, "append" | "read">> {
    const call = (method: string, path: string, body?: string) => this.call(token, method, path, body);
    if (this.tail === null) {
      let got = await call("GET", this.page("now"));
      if (got.status === 404) {
        const created = await call("PUT", STREAM_PATH, JSON.stringify(STREAM_CONFIG));
        if (created.status !== 200 && created.status !== 201) return { append: stepOf(created, false), read: null };
        got = await call("GET", this.page("now"));
      }
      this.tail = got.status === 200 ? cellHeader(got.headers, "next-cursor") : null;
      if (this.tail === null) return { append: null, read: stepOf(got, false) };
    }
    const nonce = this.nonce();
    const appended = await call("POST", RECORDS_PATH, JSON.stringify({ canary: nonce, at: this.now() }));
    if (appended.status !== 200) return { append: stepOf(appended, false), read: null };
    const got = await call("GET", this.page(this.tail));
    const records = got.status === 200 ? parsed(got.body) : undefined;
    const found = Array.isArray(records) && records.some((r) => field(r, "canary") === nonce);
    this.tail = found ? cellHeader(got.headers, "next-cursor") : null;
    return { append: stepOf(appended, true), read: stepOf(got, found) };
  }

  private page(cursor: string): string {
    return `${RECORDS_PATH}?${new URLSearchParams({ cursor, maxBytes: String(PAGE_BYTES) })}`;
  }

  /// One request to the canary's stream with the token and the stream's
  /// key. A 401 drops the token; a 404 drops the tail, so the next round
  /// finds the stream missing and creates it.
  private async call(token: string, method: string, path: string, body?: string): Promise<Got> {
    const headers: Record<string, string> = { authorization: `Bearer ${token}`, "streams-encryption-key": this.key };
    if (body !== undefined) headers["content-type"] = "application/json";
    const got = await send(new URL(path, this.cfg.cellOrigin), { method, headers, body }, this.cfg.timeoutMs);
    if (got.status === 401) this.token = null;
    if (got.status === 404) this.tail = null;
    return got;
  }
}
