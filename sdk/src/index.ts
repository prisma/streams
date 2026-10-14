/**
 * Prisma Streams product SDK (spec Stage 8 §12 — the final surface).
 *
 * A Prisma Stream is an encrypted, append-only collection of records.
 * Records are ordered by routing key, partitions scale automatically,
 * and consumers resume with opaque cursors.
 *
 * Zero dependencies; Node 18+, Bun, Deno, browsers. Authentication
 * (token) belongs to the client; encryption (encryptionKey) belongs to
 * the stream handle. All cursors, message ids, and lease tokens are
 * opaque signed strings minted by the server.
 *
 *   const client = new StreamsClient({ url, token });
 *   const orders = await client.createStream("orders", {
 *     encryptionKey,
 *     format: { kind: "json" },
 *     watches: [{ name: "by-customer", fields: ["/customerId"] }],
 *   });
 *   const producer = orders.producer("checkout", { state: store });
 *   await producer.append(order, { routingKey: order.customerId });
 *   for await (const rec of orders.subscribe({ routingKey })) { ... }
 */

// ---------------------------------------------------------------------
// Options and wire types

export interface StreamsClientOptions {
  url: string;
  token?: string;
  /**
   * Source of short-lived bearer tokens (MULTITENANCY §5: one
   * credential per project, tokens expire). Called lazily on the
   * first authenticated request and again after a `401` — never on
   * `wrong_cell`, which by contract is `421`/`503` precisely so a
   * valid credential is not refreshed at the wrong cell. A token
   * returned with its expiry (`TokenWithExpiry`) is also refreshed
   * before it expires. Takes precedence over the static `token` when
   * both are set... don't set both.
   */
  tokenProvider?: () => string | TokenWithExpiry | Promise<string | TokenWithExpiry>;
  /**
   * The stream's project id. Required for minting watch-observation
   * capabilities (the capability is bound to
   * project + stream + epoch + watch + key + method + expiry).
   */
  project?: string;
  fetch?: typeof fetch;
  /**
   * `transport` (default `true`): a request whose `fetch` fails is sent
   * again after 1, 2 and 4 s, within the four attempts it shares with the
   * 429/503 replay; a producer append is sent again with the same
   * sequence, which the server deduplicates (a replay it refuses rejects
   * with `AppendOutcomeUnknownError`), and a batch settlement with
   * the same decisions (an ack the lost attempt applied counts `stale`; a
   * retry or an extend it applied is applied again). Plain appends, stream
   * deletion and a subscription's reads (its loop reconnects) are never
   * replayed this way. Set it to `false` when a `fetch` failure means
   * configuration (a wrong URL, a blocked origin) rather than a restart;
   * consumer, subscription and watch loops still reconnect.
   */
  retry?: { transport?: boolean };
}

/**
 * A bearer token and when it expires (epoch milliseconds or a `Date`), as
 * a `tokenProvider` may return it. The first request made after 80% of
 * its lifetime, or two minutes before it expires when that is earlier (a
 * 3600 s token at 2880 s, a 600 s token at 480 s), starts a refresh
 * through the provider and is sent with this token at once: while the
 * token is valid no request waits for a refresh, one refresh runs at a
 * time, and the new token replaces this one when it arrives. A failed
 * refresh is not surfaced then; the first request 30 s later tries again.
 * A request made once the token has expired waits for a refresh, and its
 * failure rejects the request. The client starts no timer.
 */
export interface TokenWithExpiry {
  token: string;
  expiresAt: number | Date;
}

export interface StreamHandleOptions {
  /** base64url 32-byte stream encryption key. */
  encryptionKey: string;
}

export type StreamFormat =
  | { kind: "json" }
  | { kind: "bytes"; contentType?: string };

export type StreamExpiry = { idle: string } | { at: string };

export interface WatchDefinition {
  name: string;
  /** JSON pointers, order significant. */
  fields: string[];
}

export interface CreateStreamOptions extends StreamHandleOptions {
  format: StreamFormat;
  expiry?: StreamExpiry;
  watches?: WatchDefinition[];
}

export interface StreamMetadata {
  name: string;
  contentType: string;
  createdAt?: string | number;
  sealed: boolean;
  expiry?: unknown;
  watches?: WatchDefinition[];
}

/** A project's metered usage for one UTC month (`GET
 * /v1/projects/{project}/usage`): the ingest, read and storage counters,
 * corrections and effective totals the server reports beside these. */
export interface ProjectUsage {
  accountId: string;
  projectId: string;
  /** `YYYY-MM`. */
  month: string;
  [counter: string]: unknown;
}

export interface AppendResult {
  cursor: string;
  count: number;
  duplicate: boolean;
  sealed: boolean;
}

export interface ReadOptions {
  routingKey?: string;
  from?: "beginning" | "now" | string;
  maxBytes?: number;
  /**
   * Read visibility. `"durable"` (the default) serves only records that
   * are durable in object storage. `"applied"` additionally serves the
   * live tail's applied-but-not-yet-durable suffix — records arrive one
   * storage round-trip earlier (typically tens of milliseconds), with a
   * documented crash window: if the server crashes before the suffix is
   * flushed, those records are gone (their producers were never acked
   * and will retry), and re-used offsets may carry different content.
   * Use for low-latency fan-out (dashboards, presence, progress); keep
   * the default for anything transactional.
   */
  deliver?: "durable" | "applied";
}

export interface ReadPage<T> {
  records: T[];
  cursor: string;
  upToDate: boolean;
  sealed: boolean;
  /**
   * deliver:"applied" only — a resume position clamped to the durable
   * frontier. Persist THIS one; `cursor` may point into the provisional
   * suffix and is only good for the next poll against the same live
   * server.
   */
  durableCursor?: string;
  /**
   * deliver:"applied" only — index into `records` from which the
   * records were not yet durable when served. Absent when every record
   * in the page was durable.
   */
  pendingFrom?: number;
}

export interface SubscribeOptions extends ReadOptions {
  signal?: AbortSignal;
}

export interface ScanOptions {
  from?: string;
  maxBytes?: number;
}

export interface ScanRecord<T> {
  routingKey: string;
  value: T;
}

export interface ConsumerConfig {
  visibilityTimeoutMs?: number;
  maxAttempts?: number;
  deadLetterStream?: string;
  maxBatchRecords?: number;
}

export interface ConsumerMessage<T> {
  id: string;
  routingKey: string;
  attempts: number;
  leaseToken: string;
  value: T;
  ack(): void;
  retry(options?: { delayMs?: number }): void;
  extend(options: { visibilityMs: number }): void;
}

export interface PullOptions {
  /** The most records one batch leases; the server caps it at the
   * consumer's `maxBatchRecords`, which also applies when it is unset.
   * A batch can hold several consecutive records of one routing key (a
   * run), each with its own lease; no other pull receives the key while
   * any of them is leased. Handle a key's messages in batch order, one at
   * a time, to keep its order: a message retried (or left undecided)
   * returns after the same key's later messages that were acknowledged. */
  max?: number;
  waitMs?: number;
  visibilityMs?: number;
  signal?: AbortSignal;
}

export type ConsumerIteratorOutcome =
  | { status: "closed" }
  | { status: "failed"; error: unknown };

export interface ConsumerIterator<T> extends AsyncIterableIterator<ConsumerMessage<T>> {
  /** Always resolves, including when JavaScript preserves a loop body's
   * exception over a concurrent settlement failure during return(). */
  readonly closed: Promise<ConsumerIteratorOutcome>;
}

export interface PullBatch<T> {
  messages: ConsumerMessage<T>[];
  backlog: number;
  /** Apply the recorded ack/retry/extend decisions in one request, sent
   * again with the same decisions after a transport failure or a retryable
   * 429/503. A replay reports an ack its first attempt applied as `stale`,
   * and applies a retry or an extend that attempt applied again (counted
   * `retried`/`extended`, its delay or visibility counted from the replay)
   * unless the message was delivered again in between (then `stale`).
   * Every call shares that one settlement. */
  settle(): Promise<SettleResult>;
}

export interface SettleResult {
  acked: number;
  retried: number;
  extended: number;
  dlq: number;
  stale: number;
  backlog: number;
}

export interface WatchEvent {
  invalidated: boolean;
  reason?: string;
  cursor: string;
  streamCursor?: string;
}

export interface SealOptions<T> {
  final?: T;
  routingKey?: string;
  producer?: Producer<T>;
}

// ---------------------------------------------------------------------
// Producer sessions (spec Stage 5)

export interface ProducerScope {
  stream: string;
  producerId: string;
  routingKey: string;
  /** The stream's project ("" when the client sets none). One durable
   * state store shared across projects must not mix their epochs. */
  project: string;
  /** The service base URL — two deployments with the same stream name
   * are different producers. For a client made by `connect`, the
   * issuer URL, which stays the same when the service's URL changes. */
  endpoint: string;
}

export interface ProducerState {
  epoch: number;
  nextSeq: number;
}

export interface ProducerStateStore {
  /** One Producer instance must exclusively own each scope. These
   * methods are not a cross-instance/process compare-and-swap lock. */
  load(scope: ProducerScope): Promise<ProducerState | undefined>;
  save(scope: ProducerScope, state: ProducerState): Promise<void>;
}

/** Tests only — a durable store is required for production sessions. */
export class MemoryProducerStateStore implements ProducerStateStore {
  private m = new Map<string, ProducerState>();
  private key(s: ProducerScope): string {
    // Length-prefixed rather than delimiter-joined: a routing key
    // may contain any byte, so a separator can be ambiguous, and
    // literal control bytes do not belong in source.
    return (
      `${s.endpoint.length}:${s.endpoint}|${s.project.length}:${s.project}|` +
      `${s.stream.length}:${s.stream}|${s.producerId.length}:${s.producerId}|${s.routingKey}`
    );
  }
  async load(scope: ProducerScope): Promise<ProducerState | undefined> {
    return this.m.get(this.key(scope));
  }
  async save(scope: ProducerScope, state: ProducerState): Promise<void> {
    this.m.set(this.key(scope), { ...state });
  }
}

export interface ProducerOptions {
  /**
   * Where sequence state lives. Exactly-once redelivery survives a
   * restart only if this store does, so it is required unless the
   * session is declared `ephemeral`.
   */
  state?: ProducerStateStore;
  /** Opt-in: claim a fresh epoch on 403 stale-epoch (can fence a live
   * producer sharing this id). Not for a fence that refuses the replay of
   * an append whose answer was lost (`AppendOutcomeUnknownError`): that
   * append may have been written, and claiming would store it again. Read
   * the stream; if it is absent, sending it again claims above the fence. */
  autoClaim?: boolean;
  /**
   * This session's guarantees end when the process does: state is kept
   * in memory, and a restart re-appends anything it had not confirmed.
   * Required when no `state` store is given, so that losing dedup
   * across restarts is always something you chose.
   */
  ephemeral?: boolean;
}

// ---------------------------------------------------------------------
// Errors

export class StreamsError extends Error {
  status: number;
  code: string;
  retryable: boolean;
  details?: unknown;
  /**
   * On `consumer_deleting` conflicts: the DELETING incarnation's
   * version token. If the process that started the deletion died
   * without persisting it, pass this to `Consumer.delete()`'s wire
   * equivalent (DELETE with `Prisma-Consumer-Version`) to resume and
   * finish the saga.
   */
  consumerVersion?: string;
  constructor(
    status: number,
    code: string,
    message: string,
    retryable = false,
    details?: unknown,
  ) {
    super(message);
    this.name = "StreamsError";
    this.status = status;
    this.code = code;
    this.retryable = retryable;
    this.details = details;
  }
}

/**
 * MULTITENANCY §8.1: a valid token presented to a cell that does not
 * serve its project. Not an authentication failure — refreshing the
 * credential cannot help. Re-resolve the project's endpoint (token
 * exchange / gateway) and retry there.
 */
export class WrongCellError extends StreamsError {
  constructor(status: number, message: string) {
    super(status, "wrong_cell", message, false);
    this.name = "WrongCellError";
  }
}

/** A fetch transport failure, distinct from credential-provider errors. */
export class StreamsTransportError extends StreamsError {
  constructor(cause: unknown) {
    super(0, "transport_error", "the request transport failed", true);
    this.name = "StreamsTransportError";
    this.cause = cause;
  }
}

/** How to settle a producer append whose replay the server refused after a
 * lost answer, by the refusal (`cause`). A fence: a resend under the
 * producer's own epoch is refused again, and one under the fencing writer's
 * epoch can be answered `duplicate: true` unwritten (`decide_producer` checks
 * no body below its last sequence), so only an epoch above the fence's writes
 * it. A seal is checked before the duplicate check, so a resend cannot settle
 * it. Any other refusal (a quota, a `503`, a `401`, a `421`, another `4xx`)
 * passes once its cause does, and the server, which keeps the last sequence
 * and request hash per routing key and producer id, then answers the same
 * value at the same sequence `duplicate: true` if it was written and writes
 * it once if not. */
function refusedReplayRecovery(cause: unknown): string {
  if (cause instanceof ProducerFencedError) {
    const fenced = cause.currentEpoch;
    return `another writer fenced this producer id at epoch ${fenced}: read the ` +
      "stream, and send the value again only if it is absent, and only under an " +
      `epoch above ${fenced}, which a producer with autoClaim claims when the ` +
      "resend is refused (bumpEpoch() adds one to this producer's epoch, which " +
      `can land on epoch ${fenced}, where the server can answer the resend ` +
      "duplicate: true without writing it); without autoClaim, stop using this " +
      "producer id";
  }
  if (cause instanceof StreamsError && cause.code === "sealed") {
    return "the collection is sealed, so the stream is final: read it to learn " +
      "whether the value was written; sending it again is refused 409 sealed " +
      "before the server's duplicate check";
  }
  const settles = "the server answers duplicate: true if it was written and " +
    "writes it once if not";
  if (cause instanceof WrongCellError) {
    return "send the same value on the same routing key through this producer " +
      "id on a client for the project's new endpoint, before any other value " +
      `on that key: ${settles}; a client made by connect keeps this producer's ` +
      "epoch and sequence there, while one made with new StreamsClient({ url }) " +
      "starts them at 0 unless your state store carries them over";
  }
  return "send the same value on the same routing key through this producer " +
    `before any other value on that key: ${settles}; a different value sent ` +
    "first is refused 409 producer_sequence_reused if it was written";
}

/**
 * A plain append (`append`, `appendBytes`, `appendMany`: no producer
 * identity) whose last attempt ended in an answer that leaves its outcome
 * unknown: a transport failure, a 408, a 5xx other than 503, or a 503
 * whose JSON body does not itself say `"retryable": true` (the server's
 * `outcome_unknown`, an edge's 503 without the server's body, a body
 * without the field). The record may or may not have been written, and the
 * SDK does not send it again. (An earlier attempt the SDK made was a
 * refusal that writes nothing: a 401 before its token refresh, a retryable
 * 429, or a 503 the server marked retryable.) `status` is the HTTP status
 * (0 for a transport failure); `cause` is the server's answer as a
 * `StreamsError`, or the value `fetch` threw. A retry the server
 * deduplicates needs a producer.
 *
 * Also a producer append (`Producer.append`, `Producer.appendMany`) whose
 * replay after a transport failure the server refused before its duplicate
 * check (another writer fenced the producer id, the collection was sealed,
 * a quota still refused it after the attempts, or any other refusal): the
 * attempt whose answer was lost may have been written. `status` is the
 * refusal's and `cause` the refusal (a `ProducerFencedError` for a fence);
 * the producer keeps its epoch and sequence, and `autoClaim` claims no new
 * epoch for it. The message names the recovery for the refusal
 * (`refusedReplayRecovery`; README, "Producer state ownership").
 */
export class AppendOutcomeUnknownError extends StreamsError {
  constructor(status: number, cause: unknown, replayRefused = false) {
    const answer = status === 0 || !(cause instanceof StreamsError)
      ? "a transport failure"
      : `HTTP ${status} ${cause.code}`;
    super(
      status,
      "outcome_unknown",
      replayRefused
        ? "the producer append's outcome is unknown: an attempt's answer was " +
          `lost and the server refused its replay (${answer}), so the SDK ` +
          "cannot tell whether it was written; no new epoch was claimed for " +
          `it; ${refusedReplayRecovery(cause)}`
        : `the append's last attempt ended in ${answer}, so the SDK cannot ` +
          "tell whether it was written; it will not send it again; retry only " +
          "if a second copy is acceptable, or append through a producer",
      false,
    );
    this.name = "AppendOutcomeUnknownError";
    this.cause = cause;
  }
}

export class ProducerFencedError extends StreamsError {
  currentEpoch: number;
  constructor(currentEpoch: number, status: number, message: string) {
    super(status, "stale_producer_epoch", message, false);
    this.name = "ProducerFencedError";
    this.currentEpoch = currentEpoch;
  }
}

export class ProducerGapError extends StreamsError {
  expectedSeq: number;
  receivedSeq: number;
  constructor(
    expectedSeq: number,
    receivedSeq: number,
    status: number,
    message: string,
  ) {
    super(status, "producer_gap", message, false);
    this.name = "ProducerGapError";
    this.expectedSeq = expectedSeq;
    this.receivedSeq = receivedSeq;
  }
}

export class ProducerSequenceReusedError extends StreamsError {
  constructor(status: number, message: string) {
    super(status, "producer_sequence_reused", message, false);
    this.name = "ProducerSequenceReusedError";
  }
}

/**
 * The issuer refused the credential exchange, or answered it with
 * something that is not a token. `code` is the issuer's: `401`
 * `invalid_credential`, `403` `credential_not_active` or
 * `project_not_active`, `429` `rate_limited`, `503` `issuer_unavailable`
 * (the last two `retryable`); `exchange_failed` when the answer names no
 * code, `invalid_exchange_response` for a `2xx` without a token, and
 * `invalid_credential_format` (`status` 0) for a credential that is not
 * `prisma_streams_<id>.<secret>`, refused before any request.
 */
export class CredentialExchangeError extends StreamsError {
  constructor(status: number, code: string, message: string, retryable: boolean) {
    super(status, code, message, retryable);
    this.name = "CredentialExchangeError";
  }
}

// ---------------------------------------------------------------------
// HTTP plumbing

/** A cached provider token. `refreshAt` and `expiresAt` (epoch ms) are
 * set only for a `TokenWithExpiry`. */
interface CachedToken {
  value: string;
  expiresAt?: number;
  refreshAt?: number;
}

/** A refresh's token and the generation the cache gave it. */
interface Refreshed {
  value: string;
  generation: number;
}

/** The tokenProvider cache; single-flight so a burst of requests after
 * expiry fetches ONE token, not one per request. `generation` moves on at
 * each 401 invalidation and each token a refresh caches, so a request's 401
 * invalidates only the token that request sent. */
interface TokenCache {
  generation: number;
  token?: CachedToken;
  inflight?: Promise<Refreshed>;
  /** After a refresh fails, a valid cached token starts no new refresh
   * before this time (epoch ms). */
  retryAt?: number;
}

interface Ctx {
  base: string;
  token?: string;
  tokenProvider?: () => string | TokenWithExpiry | Promise<string | TokenWithExpiry>;
  tokens?: TokenCache;
  project?: string;
  fetch: typeof fetch;
  /** StreamsClientOptions.retry.transport. */
  transportRetry: boolean;
  /** How a refusal is read, when not the Streams service's `errorFrom`. */
  errors?: (res: Response) => Promise<StreamsError>;
  /** The `endpoint` of producer scopes when it is not `base`: a client
   * made by `connect` keys them on the issuer URL, so following a new
   * endpoint keeps every producer's sequence. */
  scopeEndpoint?: string;
}

/** A token is refreshed once less than this margin of its lifetime is
 * left: 20% of it, at least two minutes. */
const REFRESH_MARGIN_MIN_MS = 120_000;
const REFRESH_MARGIN_FRACTION = 0.2;
/** While the cached token is valid, a failed refresh is tried again at the
 * first request this long after it, not at every request. */
const REFRESH_RETRY_MS = 30_000;

function cachedToken(t: string | TokenWithExpiry): CachedToken {
  if (typeof t === "string") return { value: t };
  const expiresAt = t.expiresAt instanceof Date ? t.expiresAt.getTime() : t.expiresAt;
  if (typeof expiresAt !== "number" || !Number.isFinite(expiresAt)) return { value: t.token };
  const margin = Math.max(REFRESH_MARGIN_MIN_MS, REFRESH_MARGIN_FRACTION * (expiresAt - Date.now()));
  return { value: t.token, expiresAt, refreshAt: expiresAt - margin };
}

/** One provider call, shared by every request that waits for it. A token it
 * returns while it is still the cache's refresh is cached under a new
 * generation; one a 401 invalidated meanwhile is handed only to the
 * requests that waited for it. */
function refreshToken(ctx: Ctx, cache: TokenCache): Promise<Refreshed> {
  const generation = cache.generation;
  // Defer invocation so synchronous provider throws also reach every
  // waiter on the same promise and leave the cache recoverable.
  const inflight: Promise<Refreshed> = Promise.resolve().then(() => ctx.tokenProvider!()).then(
    (t) => {
      const token = cachedToken(t);
      if (cache.inflight !== inflight) return { value: token.value, generation };
      cache.generation++;
      cache.token = token;
      cache.inflight = undefined;
      cache.retryAt = undefined;
      return { value: token.value, generation: cache.generation };
    },
    (e) => {
      if (cache.inflight === inflight) {
        cache.inflight = undefined;
        cache.retryAt = Date.now() + REFRESH_RETRY_MS;
      }
      throw e;
    },
  );
  cache.inflight = inflight;
  return inflight;
}

async function authHeader(
  ctx: Ctx,
  rejectedGeneration?: number,
): Promise<{ header?: string; generation?: number }> {
  if (ctx.tokenProvider) {
    const cache = (ctx.tokens ??= { generation: 0 });
    // A 401 can invalidate only the credential that request actually
    // sent. Concurrent and late 401s join or reuse its successor.
    if (rejectedGeneration === cache.generation) {
      cache.generation++;
      cache.token = undefined;
      cache.inflight = undefined;
    }
    const now = Date.now();
    const token = cache.token;
    if (token !== undefined && (token.expiresAt === undefined || now < token.expiresAt)) {
      // Past its refresh point a valid token is still sent, at once: the
      // refresh runs beside the request, one at a time, and replaces the
      // token when it arrives. No request waits for it.
      if (
        token.refreshAt !== undefined && now >= token.refreshAt &&
        cache.inflight === undefined && (cache.retryAt === undefined || now >= cache.retryAt)
      ) {
        refreshToken(ctx, cache).catch(() => {
          // Not surfaced while the token is valid; tried again after
          // REFRESH_RETRY_MS, and a request after expiry waits for one.
        });
      }
      return { header: `Bearer ${token.value}`, generation: cache.generation };
    }
    // No token, or an expired one: wait for a refresh (the one running, if
    // any), and surface its failure.
    const refreshed = await (cache.inflight ?? refreshToken(ctx, cache));
    return { header: `Bearer ${refreshed.value}`, generation: refreshed.generation };
  }
  return { header: ctx.token ? `Bearer ${ctx.token}` : undefined };
}

/** Cancel this request's wait, without cancelling a shared credential provider.
 * Keep both promise handlers attached so a late rejection is still observed. */
function requestWait<T>(pending: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (!signal) return pending;
  return new Promise<T>((resolve, reject) => {
    let listening = true;
    const stopListening = () => {
      if (!listening) return;
      listening = false;
      signal.removeEventListener("abort", aborted);
    };
    const aborted = () => { stopListening(); reject(signal.reason); };
    signal.addEventListener("abort", aborted, { once: true });
    pending.then(
      value => { stopListening(); resolve(value); },
      error => { stopListening(); reject(error); },
    );
    if (signal.aborted) aborted();
  });
}

/** Bytes as a fetch body across the runtimes this SDK supports. */
function toBody(v: Uint8Array): BodyInit {
  return v.buffer.slice(
    v.byteOffset,
    v.byteOffset + v.byteLength,
  ) as ArrayBuffer;
}

function encName(name: string): string {
  return name.split("/").map(encodeURIComponent).join("/");
}

/** The refusals `errorFrom` read `"retryable": true` from: the server's own
 * word that the request may be sent again. `errorFrom` also reads a `429` or
 * `503` without that field as retryable; a plain append's `503` may not rely
 * on that default (`plainAppendReplays`). */
const statedRetryable = new WeakSet<StreamsError>();

async function errorFrom(res: Response): Promise<StreamsError> {
  if (res.status === 421 || res.headers.get("prisma-error-code") === "wrong_cell") {
    const error = new WrongCellError(res.status,
      "this cell does not serve the project; re-resolve the project's endpoint (the credential itself is fine)");
    // Header-only routing failures need no body read. Release even an
    // unending body before handing endpoint resolution back to the caller.
    try { await res.body?.cancel(); } catch (cause) { error.cause = cause; }
    return error;
  }
  const consumerVersion =
    res.headers.get("prisma-consumer-version") ?? undefined;
  let code = "http_error";
  let message = `HTTP ${res.status}`;
  let retryable = res.status === 429 || res.status === 503;
  let stated = false;
  let details: unknown;
  try {
    const body = (await res.json()) as {
      error?: {
        code?: string;
        message?: string;
        retryable?: boolean;
        details?: unknown;
      };
    };
    if (body.error) {
      code = body.error.code ?? code;
      message = body.error.message ?? message;
      retryable = body.error.retryable ?? retryable;
      stated = body.error.retryable === true;
      details = body.error.details;
    }
  } catch {
    // non-JSON error body: keep defaults
  }
  // The fallback wrong-cell response may carry only the header, no
  // JSON body (contract §8.1).
  if (
    code === "wrong_cell" ||
    res.status === 421 ||
    res.headers.get("prisma-error-code") === "wrong_cell"
  ) {
    return new WrongCellError(
      res.status,
      message === `HTTP ${res.status}`
        ? "this cell does not serve the project; re-resolve the " +
          "project's endpoint (the credential itself is fine)"
        : message,
    );
  }
  if (code === "stale_producer_epoch") {
    const d = details as { currentEpoch?: number } | undefined;
    return new ProducerFencedError(d?.currentEpoch ?? 0, res.status, message);
  }
  if (code === "producer_gap") {
    const d = details as { expected?: number; received?: number } | undefined;
    return new ProducerGapError(
      d?.expected ?? 0,
      d?.received ?? 0,
      res.status,
      message,
    );
  }
  if (code === "producer_sequence_reused") {
    return new ProducerSequenceReusedError(res.status, message);
  }
  const err = new StreamsError(res.status, code, message, retryable, details);
  if (consumerVersion !== undefined) err.consumerVersion = consumerVersion;
  if (stated) statedRetryable.add(err);
  return err;
}

/** A sleep that ends when the caller aborts, not 25 seconds later. */
function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const t = setTimeout(done, ms);
    function done() {
      clearTimeout(t);
      signal?.removeEventListener("abort", done);
      resolve();
    }
    signal?.addEventListener("abort", done, { once: true });
  });
}

/** Whether a plain append sends a refusal again (`req()`'s `replays`): a
 * retryable `429`, as every request does, and a `503` only when its JSON body
 * itself says `"retryable": true` and is not `outcome_unknown`, the server's
 * word that nothing was written. Any other `503` (the server's
 * `outcome_unknown`, an edge's answer without the server's body, a body
 * without the field) may have followed a write. */
function plainAppendReplays(error: StreamsError): boolean {
  return error.status === 503
    ? statedRetryable.has(error) && error.code !== "outcome_unknown"
    : retryableRequestError(error);
}

/** A plain append request's own failure (`req()`'s `classify`): an answer
 * that cannot tell whether the record was written becomes
 * `AppendOutcomeUnknownError` (a `408`, or a `5xx` that `plainAppendReplays`
 * would not send again); a definite refusal (a `429`, a `503` the server
 * marked retryable whose attempts are spent, another `4xx`, wrong cell)
 * passes through. */
function plainAppendFailure(e: unknown): unknown {
  if (e instanceof StreamsTransportError) {
    return new AppendOutcomeUnknownError(0, e.cause);
  }
  if (
    e instanceof StreamsError && !(e instanceof WrongCellError) &&
    (e.status === 408 || (e.status >= 500 && !plainAppendReplays(e)))
  ) {
    return new AppendOutcomeUnknownError(e.status, e);
  }
  return e;
}

/** A producer append request's own failure (`req()`'s `classify`): an
 * error answer after an earlier attempt's transport failure (a replay the
 * server refused before its duplicate check: another writer's fence, a
 * seal, a quota) cannot say the lost attempt was not written, so it becomes
 * `AppendOutcomeUnknownError`'s producer variant, which `Producer.sendNow`
 * does not answer with a new epoch. A transport failure that outlasts the
 * attempts, and an answer with no lost attempt before it, pass through. */
function producerAppendFailure(failure: StreamsError, answerLost: boolean): unknown {
  return answerLost && !(failure instanceof StreamsTransportError)
    ? new AppendOutcomeUnknownError(failure.status, failure, true)
    : failure;
}

/** How to go on after a producer append's unreadable 2xx: `Producer.sendNow`
 * saves the producer's state only from an answer it read, so the producer
 * keeps its sequence (a 2xx an intermediary made up wrote nothing, and the
 * next sequence would be refused `producer_gap`), and the server, which
 * keeps the last sequence and request hash per routing key and producer id,
 * answers the same value at that sequence `duplicate: true`. */
const PRODUCER_UNREADABLE_NEXT = "this producer keeps its sequence, so send the same value on " +
  "the same routing key through it before any other value on that key: the server answers it " +
  "duplicate: true and the producer moves on, while a different value sent first is refused 409 " +
  "producer_sequence_reused";

/** An append's 2xx answer. The server wrote the record, so a body that
 * cannot be read (not JSON, or a connection lost mid-body) is neither a
 * retryable failure nor an unknown outcome: a second plain append would be
 * a second copy, and a producer append goes on as `next` says. */
async function appendedResult(res: Response, next = "do not send it again"): Promise<AppendResult> {
  try {
    return (await res.json()) as AppendResult;
  } catch (cause) {
    const error = new StreamsError(
      res.status,
      "unreadable_response",
      `the server accepted the append (HTTP ${res.status}), so it was written, ` +
        `but its answer could not be read; ${next}`,
      false,
    );
    error.cause = cause;
    throw error;
  }
}

function retryableRequestError(error: unknown): boolean {
  return error instanceof StreamsTransportError ||
    (error instanceof StreamsError && error.retryable &&
      (error.status === 429 || error.status === 503));
}

/** The wait before a transport failure's replay, by the attempt that failed. */
const TRANSPORT_BACKOFF_MS = [1000, 2000, 4000];

/** A loop's reconnection: sleep `backoffMs` (abort-aware) and return the
 * next wait, doubled to at most 5 s; a successful request restarts at 1 s. */
async function reconnectAfter(backoffMs: number, signal?: AbortSignal): Promise<number> {
  await sleep(backoffMs, signal);
  return Math.min(backoffMs * 2, 5000);
}

/** `path` is a function where each attempt needs its own (a watch wait's
 * capability, which expires).
 *
 * `replayTransport` is false where a replay could repeat an effect the
 * server does not deduplicate (plain appends, stream deletion), and where
 * the caller handles a transport failure itself (a subscription's reads).
 *
 * `classify` maps the failure that ends this request (its transport
 * failure, or the answer it gives up on) to the error it rejects with;
 * `answerLost` tells it that an earlier attempt failed in transport, so
 * that attempt may have taken effect (a producer append's replay). A
 * token provider's rejection and an abort's reason are not this request's
 * answer and pass through unchanged.
 *
 * `replays` decides which refusals are sent again on their `Retry-After`
 * within the attempts: a retryable `429` or `503`, except where a `503`
 * may have followed a write the server does not deduplicate (plain
 * appends). */
async function req(
  ctx: Ctx,
  method: string,
  path: string | (() => Promise<string>),
  headers: Record<string, string>,
  body?: BodyInit,
  signal?: AbortSignal,
  replayTransport = true,
  classify: (failure: StreamsError, answerLost: boolean) => unknown = (failure) => failure,
  replays: (error: StreamsError) => boolean = retryableRequestError,
): Promise<Response> {
  const h: Record<string, string> = { ...headers };
  let rejectedGeneration: number | undefined;
  let refreshed = false;
  let answerLost = false;
  for (let attempt = 0; ; attempt++) {
    signal?.throwIfAborted();
    const auth = await requestWait(authHeader(ctx, rejectedGeneration), signal);
    signal?.throwIfAborted();
    rejectedGeneration = undefined;
    if (auth.header) h["authorization"] = auth.header;
    const target = typeof path === "string" ? path : await path();
    // The signal reaches fetch itself, so an abort ends the in-flight
    // long poll instead of leaving it running until its timeout.
    let res: Response;
    try {
      res = await ctx.fetch(`${ctx.base}${target}`, {
        method,
        headers: h,
        body,
        signal,
      });
    } catch (cause) {
      signal?.throwIfAborted();
      const failure = new StreamsTransportError(cause);
      // The four attempts the 429/503 replay counts too. A 401's single
      // token refresh is not bounded by them, so a request makes at most
      // five attempts: the fifth only after a 401 at the fourth.
      if (!ctx.transportRetry || !replayTransport || attempt >= 3) throw classify(failure, answerLost);
      answerLost = true;
      await sleep(TRANSPORT_BACKOFF_MS[attempt], signal);
      signal?.throwIfAborted();
      continue;
    }
    if (res.ok) return res;
    // Consume/classify failure before applying generic retry. In
    // particular a 503 routing response is permanent at this endpoint.
    const error = await (ctx.errors ?? errorFrom)(res);
    if (error instanceof WrongCellError) throw classify(error, answerLost);
    // 401 = the token itself is bad or expired: refresh ONCE through
    // the provider and replay. `wrong_cell` is 421/503 by contract
    // (never 401), so a misdirected request never burns a refresh.
    if (res.status === 401 && ctx.tokenProvider && !refreshed) {
      refreshed = true;
      rejectedGeneration = auth.generation;
      continue;
    }
    if (replays(error) && attempt < 3) {
      signal?.throwIfAborted();
      const ra = Number(res.headers.get("retry-after") ?? "1");
      // Abort-aware: a plain setTimeout here made cancellation wait out
      // the backoff — up to ~15 s across three attempts.
      await sleep((Number.isFinite(ra) ? Math.max(0, Math.min(ra, 5)) : 1) * 1000, signal);
      signal?.throwIfAborted();
      continue;
    }
    throw classify(error, answerLost);
  }
}

// ---------------------------------------------------------------------
// Credentials (MULTITENANCY §3.2: a project credential is exchanged at
// the issuer for short-lived bearer tokens)

/** The issuer's answer to a credential exchange. */
export interface CredentialExchange {
  accessToken: string;
  /** The access token's lifetime in seconds. */
  expiresIn: number;
  projectId: string;
  /** The service's base URL for the project, or `null` when the issuer
   * names none. */
  endpoint: string | null;
}

export interface CredentialProvider {
  /** One exchange: `POST {issuerUrl}/v1/token/streams`. */
  exchange(): Promise<CredentialExchange>;
  /** An exchange as a `tokenProvider` answer: the token and its expiry. */
  tokenProvider(): Promise<TokenWithExpiry>;
  /** The last successful exchange. */
  readonly last: CredentialExchange | undefined;
}

/** An issuer refusal: the issuer's `{error: {code, message}}`, or a plain
 * `{error: "<text>"}` (code `exchange_failed`); retryable when `429` or
 * `503`. */
async function exchangeError(res: Response): Promise<StreamsError> {
  let code = "exchange_failed";
  let message = `HTTP ${res.status}`;
  try {
    const failure = ((await res.json()) as { error?: unknown }).error;
    if (typeof failure === "string") {
      message = failure;
    } else if (typeof failure === "object" && failure !== null) {
      const e = failure as { code?: unknown; message?: unknown };
      if (typeof e.code === "string") code = e.code;
      if (typeof e.message === "string") message = e.message;
    }
  } catch {
    // not a JSON answer: keep the status
  }
  return new CredentialExchangeError(res.status, code, message, res.status === 429 || res.status === 503);
}

function exchangeFrom(status: number, body: unknown): CredentialExchange {
  const b = (typeof body === "object" && body !== null ? body : {}) as Record<string, unknown>;
  const { accessToken, expiresIn, projectId, endpoint } = b;
  if (
    typeof accessToken !== "string" || accessToken === "" ||
    typeof expiresIn !== "number" || !Number.isFinite(expiresIn) || expiresIn <= 0 ||
    typeof projectId !== "string" || projectId === ""
  ) {
    throw new CredentialExchangeError(status, "invalid_exchange_response",
      "the issuer's answer lacks an accessToken, an expiresIn or a projectId", false);
  }
  return {
    accessToken,
    expiresIn,
    projectId,
    endpoint: typeof endpoint === "string" && endpoint !== "" ? endpoint : null,
  };
}

/** Each exchange attempt's deadline: an issuer that accepted the
 * connection and never answers fails the attempt as a transport failure,
 * which the request loop replays, instead of holding a refresh (and the
 * requests waiting for it) for the runtime's own timeout, if it has one. */
const EXCHANGE_ATTEMPT_TIMEOUT_MS = 10_000;

/** A credential as it is sent: `prisma_streams_<id>.<secret>`, in printable
 * ASCII without spaces. */
const CREDENTIAL_FORMAT = /^prisma_streams_[A-Za-z0-9_]+\.[\x21-\x7e]+$/;

/** The credential without the whitespace around it (fetch trims a header
 * value's ends too), or a refusal that does not echo it: fetch rejects a
 * header value with a control character in an error that quotes it. */
function presentedCredential(credential: string): string {
  const presented = credential.replace(/^[ \t\r\n]+|[ \t\r\n]+$/g, "");
  if (!CREDENTIAL_FORMAT.test(presented)) {
    throw new CredentialExchangeError(0, "invalid_credential_format",
      "the credential is not of the form prisma_streams_<id>.<secret> in printable characters " +
        "without spaces or line breaks; copy it again where it was created", false);
  }
  return presented;
}

/**
 * A Prisma Streams credential (`prisma_streams_<id>.<secret>`, shown once
 * when it is created) as a source of access tokens. A credential of
 * another form is refused at once (`invalid_credential_format`), before
 * any request. Each exchange is sent through the client's request loop: a
 * `429` or `503` is replayed on its `Retry-After` and a transport failure
 * after 1, 2 and 4 s, within four attempts, each attempt given up after
 * 10 s without an answer (a transport failure too); any other refusal
 * rejects at once as `CredentialExchangeError`. Neither the credential nor
 * a token is logged or put in an error.
 */
export function prismaStreamsCredential(
  credential: string,
  options: { issuerUrl: string; fetch?: typeof fetch; retry?: StreamsClientOptions["retry"] },
): CredentialProvider {
  const presented = presentedCredential(credential);
  const send = options.fetch ?? fetch.bind(globalThis);
  const ctx: Ctx = {
    base: options.issuerUrl.replace(/\/$/, ""),
    // A deadline per attempt (req() sends the exchange with no signal).
    fetch: (input, init) => send(input, { ...init, signal: AbortSignal.timeout(EXCHANGE_ATTEMPT_TIMEOUT_MS) }),
    transportRetry: options.retry?.transport ?? true,
    errors: exchangeError,
  };
  let last: CredentialExchange | undefined;
  const exchange = async (): Promise<CredentialExchange> => {
    // No token in this context: req() keeps this header and never
    // refreshes on a 401.
    const res = await req(ctx, "POST", "/v1/token/streams", {
      authorization: `StreamsCredential ${presented}`,
      accept: "application/json",
    });
    const body: unknown = await res.json().catch(() => undefined);
    last = exchangeFrom(res.status, body);
    return last;
  };
  return {
    exchange,
    async tokenProvider() {
      return exchangedToken(await exchange());
    },
    get last() {
      return last;
    },
  };
}

function exchangedToken(exchanged: CredentialExchange): TokenWithExpiry {
  return { token: exchanged.accessToken, expiresAt: Date.now() + exchanged.expiresIn * 1000 };
}

export interface ConnectOptions {
  /** The project credential, `prisma_streams_<id>.<secret>`. */
  credential: string;
  /** The token issuer's base URL. */
  issuerUrl: string;
  /** The service's base URL. When set it is used as given and the
   * issuer's `endpoint` is never followed. */
  url?: string;
  fetch?: typeof fetch;
  retry?: StreamsClientOptions["retry"];
  /** Accept an issuer `endpoint` that is neither `https:` nor a loopback
   * host. Every request carries the stream key. */
  allowInsecureEndpoint?: boolean;
}

/** `https:`, or `http:` to a loopback host. */
function secureEndpoint(endpoint: string): boolean {
  let url: URL;
  try {
    url = new URL(endpoint);
  } catch {
    return false;
  }
  const host = url.hostname;
  const loopback = host === "localhost" || host.endsWith(".localhost") ||
    host === "127.0.0.1" || host === "[::1]";
  return url.protocol === "https:" || (url.protocol === "http:" && loopback);
}

/** The issuer's endpoint as a base URL, or why it cannot be one. */
function issuedEndpoint(endpoint: string | null, allowInsecure: boolean): string {
  if (endpoint === null) {
    throw new StreamsError(400, "endpoint_required", "the issuer returned no endpoint; pass url");
  }
  if (!allowInsecure && !secureEndpoint(endpoint)) {
    throw new StreamsError(400, "insecure_endpoint",
      "the issuer returned an endpoint that is neither https nor a loopback host; " +
        "pass url, or allowInsecureEndpoint to accept it");
  }
  return endpoint.replace(/\/$/, "");
}

// ---------------------------------------------------------------------
// Client

export class StreamsClient {
  private ctx: Ctx;

  constructor(options: StreamsClientOptions) {
    this.ctx = {
      base: options.url.replace(/\/$/, ""),
      project: options.project,
      token: options.token,
      tokenProvider: options.tokenProvider,
      fetch: options.fetch ?? fetch.bind(globalThis),
      transportRetry: options.retry?.transport ?? true,
    };
  }

  /**
   * A client for the project a credential belongs to, from one credential
   * exchange: the base URL is `url` when given, else the issuer's
   * `endpoint` (`https:` or a loopback host unless
   * `allowInsecureEndpoint`); the project is the credential's. Tokens are
   * exchanged again before they expire and after a `401`; when an
   * exchange names a new endpoint and no `url` was given, the client
   * moves there: every request made after the exchange returns goes to
   * it, a request that waited for the exchange (its token had expired, or
   * was refused `401`) included; one sent while a refresh ran beside it
   * went to the old endpoint. Producer state is keyed on the issuer URL,
   * so a move keeps every producer's sequence.
   */
  static async connect(options: ConnectOptions): Promise<StreamsClient> {
    const allowInsecure = options.allowInsecureEndpoint ?? false;
    const provider = prismaStreamsCredential(options.credential, {
      issuerUrl: options.issuerUrl,
      fetch: options.fetch,
      retry: options.retry,
    });
    const first = await provider.exchange();
    const follow = options.url === undefined;
    const client: StreamsClient = new StreamsClient({
      url: options.url ?? issuedEndpoint(first.endpoint, allowInsecure),
      project: first.projectId,
      fetch: options.fetch,
      retry: options.retry,
      tokenProvider: async () => {
        const exchanged = await provider.exchange();
        if (follow && exchanged.endpoint !== null && (allowInsecure || secureEndpoint(exchanged.endpoint))) {
          client.ctx.base = exchanged.endpoint.replace(/\/$/, "");
        }
        return exchangedToken(exchanged);
      },
    });
    client.ctx.scopeEndpoint = options.issuerUrl.replace(/\/$/, "");
    client.ctx.tokens = { generation: 0, token: cachedToken(exchangedToken(first)) };
    return client;
  }

  stream<T = unknown>(name: string, options: StreamHandleOptions): Stream<T> {
    return new Stream<T>(this.ctx, name, options.encryptionKey);
  }

  async createStream<T = unknown>(
    name: string,
    options: CreateStreamOptions,
  ): Promise<Stream<T>> {
    const doc: Record<string, unknown> = { format: options.format };
    if (options.expiry) doc["expiry"] = options.expiry;
    if (options.watches) doc["watches"] = options.watches;
    const res = await req(
      this.ctx,
      "PUT",
      `/v1/streams/${encName(name)}`,
      {
        "content-type": "application/json",
        "prisma-encryption-key": options.encryptionKey,
      },
      JSON.stringify(doc),
    );
    return new Stream<T>(this.ctx, name, options.encryptionKey);
  }

  async *listStreams(options?: {
    limit?: number;
  }): AsyncIterable<StreamMetadata> {
    let cursor: string | undefined;
    for (;;) {
      const q = new URLSearchParams();
      if (options?.limit) q.set("limit", String(options.limit));
      if (cursor) q.set("cursor", cursor);
      const res = await req(this.ctx, "GET", `/v1/streams?${q}`, {});
      const body = (await res.json()) as {
        streams: StreamMetadata[];
        cursor?: string;
      };
      for (const s of body.streams) yield s;
      if (!body.cursor) return;
      cursor = body.cursor;
    }
  }

  /**
   * The project's metered usage, for the current month or `month`
   * (`YYYY-MM`). Needs the client's `project` and a credential with the
   * `streams.usage.read` scope and no stream-prefix restriction (a
   * restricted one is refused `403` `prefix_denied`). The figures trail
   * writes by about 10 s and reads by about 20 s.
   */
  async usage(options?: { month?: string }): Promise<ProjectUsage> {
    if (!this.ctx.project) {
      throw new StreamsError(
        400,
        "project_required",
        "project usage is read per project; set StreamsClientOptions.project",
        false,
      );
    }
    const query = options?.month !== undefined
      ? `?${new URLSearchParams({ month: options.month })}`
      : "";
    const res = await req(
      this.ctx,
      "GET",
      `/v1/projects/${encodeURIComponent(this.ctx.project)}/usage${query}`,
      {},
    );
    return (await res.json()) as ProjectUsage;
  }
}

// ---------------------------------------------------------------------
// Stream handle

export class Stream<T = unknown> {
  private ctx: Ctx;
  readonly name: string;
  private key: string;
  private defsCache?: Promise<WatchDefinition[]>;
  private epochCache?: Promise<string>;

  constructor(ctx: Ctx, name: string, key: string) {
    this.ctx = ctx;
    this.name = name;
    this.key = key;
  }

  private path(rest = ""): string {
    return `/v1/streams/${encName(this.name)}${rest}`;
  }

  private kh(): Record<string, string> {
    return { "prisma-encryption-key": this.key };
  }

  // ---- append (spec Stage 4) ----------------------------------------

  async append(
    value: T,
    options?: { routingKey?: string },
  ): Promise<AppendResult> {
    return this.appendRaw(JSON.stringify(value), options?.routingKey);
  }

  async appendBytes(
    value: Uint8Array,
    options?: { routingKey?: string },
  ): Promise<AppendResult> {
    // A Uint8Array is a valid fetch body at runtime everywhere we
    // support; TypeScript's BodyInit does not name the generic
    // Uint8Array<ArrayBufferLike> form, so normalize through the
    // buffer view.
    return this.appendRaw(toBody(value), options?.routingKey, true);
  }

  async appendMany(
    values: readonly T[],
    options?: { routingKey?: string },
  ): Promise<AppendResult> {
    const h: Record<string, string> = {
      ...this.kh(),
      "content-type": "application/json",
    };
    if (options?.routingKey) h["prisma-routing-key"] = options.routingKey;
    const res = await req(
      this.ctx,
      "POST",
      this.path("/records:batch"),
      h,
      JSON.stringify(values),
      undefined,
      false,
      plainAppendFailure,
      plainAppendReplays,
    );
    return appendedResult(res);
  }

  private async appendRaw(
    body: BodyInit,
    routingKey?: string,
    bytes = false,
  ): Promise<AppendResult> {
    const h: Record<string, string> = {
      ...this.kh(),
      "content-type": bytes ? "application/octet-stream" : "application/json",
    };
    if (routingKey) h["prisma-routing-key"] = routingKey;
    const res = await req(
      this.ctx, "POST", this.path("/records"), h, body, undefined, false,
      plainAppendFailure,
      plainAppendReplays,
    );
    return appendedResult(res);
  }

  // ---- producer sessions (spec Stage 5) -----------------------------

  producer(id: string, options: ProducerOptions = {}): Producer<T> {
    if (!options.state && !options.ephemeral) {
      throw new StreamsError(
        400,
        "producer_state_required",
        "producer sessions need a durable state store to dedup across " +
          "restarts: pass { state } — or { ephemeral: true } to accept " +
          "in-memory state that a restart discards",
        false,
      );
    }
    const state = options.state ?? new MemoryProducerStateStore();
    return new Producer<T>(this, this.ctx, this.name, this.key, id, {
      ...options,
      state,
    });
  }

  /** @internal producer transport. */
  async _producerAppend(
    payload: string,
    routingKey: string | undefined,
    batch: boolean,
    ph: Record<string, string>,
  ): Promise<AppendResult> {
    const h: Record<string, string> = {
      ...this.kh(),
      "content-type": "application/json",
      ...ph,
    };
    if (routingKey) h["prisma-routing-key"] = routingKey;
    // A transport failure is replayed with the same producer id, epoch and
    // sequence, which the server deduplicates (an append that landed is
    // answered `duplicate: true`); the scope's queue holds later operations
    // until it settles. A replay the server refuses before that check (a
    // fence by another writer, a seal, a quota) cannot say the lost attempt
    // was not written: it rejects as `AppendOutcomeUnknownError`
    // (`producerAppendFailure`), which Producer.sendNow does not answer
    // with a new epoch.
    const res = await req(
      this.ctx,
      "POST",
      this.path(batch ? "/records:batch" : "/records"),
      h,
      payload,
      undefined,
      true,
      producerAppendFailure,
    );
    return appendedResult(res, PRODUCER_UNREADABLE_NEXT);
  }

  // ---- read / subscribe / scan (spec Stage 6) -----------------------

  async read(options?: ReadOptions): Promise<ReadPage<T>> {
    const q = new URLSearchParams();
    if (options?.routingKey) q.set("routingKey", options.routingKey);
    if (options?.from) q.set("cursor", options.from);
    if (options?.maxBytes) q.set("maxBytes", String(options.maxBytes));
    if (options?.deliver === "applied") q.set("deliver", "applied");
    const res = await req(
      this.ctx,
      "GET",
      this.path(`/records?${q}`),
      this.kh(),
    );
    const records =
      res.status === 204 ? [] : ((await res.json()) as T[]) ?? [];
    const page: ReadPage<T> = {
      records,
      cursor: res.headers.get("prisma-next-cursor") ?? "",
      upToDate: res.headers.get("prisma-up-to-date") === "true",
      sealed: res.headers.get("prisma-sealed") === "true",
    };
    const durable = res.headers.get("prisma-durable-cursor");
    if (durable !== null) page.durableCursor = durable;
    const pending = res.headers.get("prisma-pending-from");
    if (pending !== null) page.pendingFrom = Number(pending);
    return page;
  }

  async *subscribe(options?: SubscribeOptions): AsyncIterable<T> {
    // Catch-up from the requested position, then long-poll the tail.
    // The committed cursor only advances after a page's records are
    // fully yielded (spec §4.3).
    let cursor = options?.from ?? "beginning";
    let live = false;
    const applied = options?.deliver === "applied";
    // deliver:"applied" — the session cursor can point into the
    // provisional (not-yet-durable) suffix, which does not survive a
    // server crash. Track the durable-clamped cursor from every
    // response; any reconnect resumes from THERE, accepting the
    // documented re-delivery in exchange for never stalling on (or
    // silently skipping past) offsets that were rewritten after a
    // crash. The server refuses stale session cursors with 409
    // cursor_beyond_tail as a backstop.
    let durableCursor: string | undefined;
    for (;;) {
      if (options?.signal?.aborted) return;
      const q = new URLSearchParams();
      if (options?.routingKey) q.set("routingKey", options.routingKey);
      q.set("cursor", cursor);
      if (options?.maxBytes) q.set("maxBytes", String(options.maxBytes));
      if (applied) q.set("deliver", "applied");
      if (live) q.set("waitMs", "25000");
      const path = this.path(
        live ? `/records:long-poll?${q}` : `/records?${q}`,
      );
      let res: Response;
      try {
        // The signal reaches fetch, so aborting ends the in-flight
        // 25-second long poll instead of leaving it running to term. A
        // transport failure is not replayed at this cursor: the loop below
        // reconnects, from the durable cursor for applied reads.
        res = await req(
          this.ctx,
          "GET",
          path,
          this.kh(),
          undefined,
          options?.signal,
          false,
        );
      } catch (e) {
        if (options?.signal?.aborted) return;
        if (
          applied &&
          e instanceof StreamsError && e.code === "cursor_beyond_tail" &&
          durableCursor !== undefined
        ) {
          cursor = durableCursor;
          continue;
        }
        if (!retryableRequestError(e)) throw e;
        if (applied && durableCursor !== undefined) cursor = durableCursor;
        await sleep(1000, options?.signal);
        continue;
      }
      const records =
        res.status === 204 ? [] : ((await res.json()) as T[]) ?? [];
      for (const r of records) yield r;
      cursor = res.headers.get("prisma-next-cursor") ?? cursor;
      const d = res.headers.get("prisma-durable-cursor");
      if (d !== null) durableCursor = d;
      const sealed = res.headers.get("prisma-sealed") === "true";
      const upToDate = res.headers.get("prisma-up-to-date") === "true";
      if (sealed && upToDate) return;
      if (upToDate || res.status === 204) live = true;
    }
  }

  async *scan(options?: ScanOptions): AsyncIterable<ScanRecord<T>> {
    let cursor = options?.from;
    for (;;) {
      const q = new URLSearchParams();
      if (cursor) q.set("cursor", cursor);
      if (options?.maxBytes) q.set("maxBytes", String(options.maxBytes));
      const res = await req(
        this.ctx,
        "GET",
        this.path(`:scan?${q}`),
        this.kh(),
      );
      const items = (await res.json()) as ScanRecord<T>[];
      for (const i of items) yield i;
      if (res.headers.get("prisma-scan-complete") === "true") return;
      const next = res.headers.get("prisma-next-scan-cursor");
      if (!next) return;
      cursor = next;
    }
  }

  // ---- consumers (spec Stage 2) -------------------------------------

  async consumer(name: string, config?: ConsumerConfig): Promise<Consumer<T>> {
    const res = await req(
      this.ctx,
      "PUT",
      this.path(`/consumers/${encodeURIComponent(name)}`),
      { ...this.kh(), "content-type": "application/json" },
      JSON.stringify(config ?? {}),
    );
    // The consumer's incarnation token ({stream epoch, generation},
    // opaque). delete() sends it so a stale retry can never delete a
    // RECREATED consumer of the same name.
    const version = res.headers.get("prisma-consumer-version") ?? "";
    return new Consumer<T>(this.ctx, this, name, version);
  }

  /** @internal */
  _ctx(): Ctx {
    return this.ctx;
  }
  /** @internal */
  _kh(): Record<string, string> {
    return this.kh();
  }
  /** @internal */
  _path(rest?: string): string {
    return this.path(rest);
  }

  // ---- watches (spec Stage 2 §3) ------------------------------------

  /**
   * A watch on one set of field values — `watch("by-customer", ["c1"])`,
   * with the values in the definition's declared field order. The key
   * and the URL signature are derived here from the stream key, so the
   * result can be handed to a browser as a plain URL that needs no
   * credentials and can observe nothing else.
   */
  async watch(name: string, values: unknown[] = []): Promise<Watch> {
    const defs = await this._definitions();
    const def = defs.find((d) => d.name === name);
    if (!def) {
      throw new StreamsError(
        404,
        "unknown_watch",
        `no watch definition named ${JSON.stringify(name)} on ${this.name}`,
        false,
      );
    }
    if (values.length !== def.fields.length) {
      throw new StreamsError(
        400,
        "invalid_watch_values",
        `watch ${JSON.stringify(name)} declares ${def.fields.length} field(s) ` +
          `(${def.fields.join(", ")}); got ${values.length} value(s)`,
        false,
      );
    }
    const keyHex = await deriveWatchKey(name, def.fields, values);
    if (!this.ctx.project) {
      throw new StreamsError(
        400,
        "project_required",
        "watch capabilities are project-bound; set StreamsClientOptions.project",
        false,
      );
    }
    return new Watch(
      this.ctx,
      this,
      name,
      keyHex,
      this.key,
      await this._epoch(),
    );
  }

  /** Definitions are immutable for one incarnation, so fetch them once. */
  private async _definitions(): Promise<WatchDefinition[]> {
    if (!this.defsCache) this.defsCache = this.watches();
    return this.defsCache;
  }

  private async _epoch(): Promise<string> {
    if (!this.epochCache) {
      this.epochCache = this.metadata().then((m) => {
        const e = (m as StreamMetadata & { epoch?: string }).epoch;
        if (!e) {
          throw new StreamsError(
            400,
            "no_watches",
            `${this.name} was created without watch definitions`,
            false,
          );
        }
        return e;
      });
    }
    return this.epochCache;
  }

  async watches(): Promise<WatchDefinition[]> {
    const res = await req(this.ctx, "GET", this.path("/watches"), this.kh());
    const body = (await res.json()) as { watches: WatchDefinition[] };
    return body.watches;
  }

  // ---- lifecycle (spec Stage 8) -------------------------------------

  async metadata(): Promise<StreamMetadata> {
    const res = await req(this.ctx, "GET", this.path(), this.kh());
    return (await res.json()) as StreamMetadata;
  }

  async seal(options?: SealOptions<T>): Promise<void> {
    const doc: Record<string, unknown> = {};
    if (options?.final !== undefined) {
      doc["final"] = options.final;
      if (options.routingKey) doc["routingKey"] = options.routingKey;
    }
    const h: Record<string, string> = {
      ...this.kh(),
      "content-type": "application/json",
    };
    const send = async () => {
      const res = await req(
        this.ctx,
        "POST",
        this.path(":seal"),
        h,
        JSON.stringify(doc),
      );
    };
    // A producer-backed final seal consumes a sequence number, so it has
    // to ride the SAME per-routing-key chain as append/appendMany.
    // Running it outside meant a concurrent append could read the same
    // nextSeq and one of the two would be rejected as reuse — or worse,
    // ordered wrongly against the record that closes the collection.
    if (options?.final !== undefined && options.producer) {
      const rk = options.routingKey ?? "";
      await options.producer._chain(rk, async () => {
        Object.assign(h, await options.producer!._sealHeaders(rk));
        await send();
        await options.producer!._sealCommitted(rk);
      });
      return;
    }
    await send();
  }

  async delete(): Promise<void> {
    // Not replayed after a transport failure: a delete that succeeded
    // would be answered 404 on the replay.
    const res = await req(this.ctx, "DELETE", this.path(), this.kh(), undefined, undefined, false);
  }
}

// ---------------------------------------------------------------------
// Producer session

export class Producer<T> {
  private chains = new Map<string, Promise<void>>();
  private stream: Stream<T>;
  private ctx: Ctx;
  private streamName: string;
  private id: string;
  private options: ProducerOptions & { state: ProducerStateStore };

  constructor(
    stream: Stream<T>,
    ctx: Ctx,
    streamName: string,
    _key: string,
    id: string,
    options: ProducerOptions & { state: ProducerStateStore },
  ) {
    this.stream = stream;
    this.ctx = ctx;
    this.streamName = streamName;
    this.id = id;
    this.options = options;
  }

  async append(value: T, options?: { routingKey?: string }): Promise<AppendResult> {
    return this.send(JSON.stringify(value), options?.routingKey ?? "", false);
  }

  async appendMany(
    values: readonly T[],
    options?: { routingKey?: string },
  ): Promise<AppendResult> {
    if (values.length === 0) {
      throw new StreamsError(400, "empty_batch", "appendMany requires records");
    }
    return this.send(JSON.stringify(values), options?.routingKey ?? "", true);
  }

  /** @internal producer headers for a seal's final record. */
  async _sealHeaders(routingKey: string): Promise<Record<string, string>> {
    const scope = this.scope(routingKey);
    const st = (await this.options.state.load(scope)) ?? {
      epoch: 0,
      nextSeq: 0,
    };
    return {
      "producer-id": this.id,
      "producer-epoch": String(st.epoch),
      "producer-seq": String(st.nextSeq),
    };
  }

  /** @internal advance the sequence after a seal's final record lands. */
  async _sealCommitted(routingKey: string): Promise<void> {
    const scope = this.scope(routingKey);
    const st = (await this.options.state.load(scope)) ?? {
      epoch: 0,
      nextSeq: 0,
    };
    await this.options.state.save(scope, {
      epoch: st.epoch,
      nextSeq: st.nextSeq + 1,
    });
  }

  async bumpEpoch(routingKey = ""): Promise<void> {
    return this._chain(routingKey, async () => {
      const scope = this.scope(routingKey);
      const st = (await this.options.state.load(scope)) ?? {
        epoch: 0,
        nextSeq: 0,
      };
      await this.options.state.save(scope, { epoch: st.epoch + 1, nextSeq: 0 });
    });
  }

  private scope(routingKey: string): ProducerScope {
    return {
      stream: this.streamName,
      producerId: this.id,
      routingKey,
      project: this.ctx.project ?? "",
      endpoint: this.ctx.scopeEndpoint ?? this.ctx.base,
    };
  }

  /**
   * @internal Run `op` in this producer's per-routing-key queue, so
   * anything that consumes a sequence number is serialized with the
   * appends — including the final seal.
   */
  _chain<R>(routingKey: string, op: () => Promise<R>): Promise<R> {
    const prev = this.chains.get(routingKey) ?? Promise.resolve();
    const result = prev.then(op);
    const release = () => {
      if (this.chains.get(routingKey) === tail) this.chains.delete(routingKey);
    };
    // The queue tail always completes, while the original result carries
    // any failure to its caller. Rejection cannot poison or leak a key.
    const tail = result.then(release, release);
    this.chains.set(routingKey, tail);
    return result;
  }

  /** Same-key requests serialize in sequence order (spec §5.1). */
  private send(
    payload: string,
    routingKey: string,
    batch: boolean,
  ): Promise<AppendResult> {
    return this._chain(routingKey, () => this.sendNow(payload, routingKey, batch));
  }

  private async sendNow(
    payload: string,
    routingKey: string,
    batch: boolean,
    reclaimed = false,
  ): Promise<AppendResult> {
    const scope = this.scope(routingKey);
    const st = (await this.options.state.load(scope)) ?? {
      epoch: 0,
      nextSeq: 0,
    };
    const ph = {
      "producer-id": this.id,
      "producer-epoch": String(st.epoch),
      "producer-seq": String(st.nextSeq),
    };
    try {
      const out = await this.stream._producerAppend(
        payload,
        routingKey || undefined,
        batch,
        ph,
      );
      await this.options.state.save(scope, {
        epoch: st.epoch,
        nextSeq: st.nextSeq + 1,
      });
      return out;
    } catch (e) {
      if (
        e instanceof ProducerFencedError &&
        this.options.autoClaim &&
        !reclaimed
      ) {
        await this.options.state.save(scope, {
          epoch: e.currentEpoch + 1,
          nextSeq: 0,
        });
        return this.sendNow(payload, routingKey, batch, true);
      }
      throw e;
    }
  }
}

// ---------------------------------------------------------------------
// Consumer

export class Consumer<T> {
  private ctx: Ctx;
  private stream: Stream<T>;
  readonly name: string;
  /**
   * Opaque incarnation token ({stream epoch, consumer generation})
   * from create/get. DELETE requires it: a deletion targets this exact
   * incarnation, so a stale retry after a recreation gets an
   * idempotent 204 instead of deleting the replacement.
   */
  readonly version: string;

  constructor(ctx: Ctx, stream: Stream<T>, name: string, version: string) {
    this.ctx = ctx;
    this.stream = stream;
    this.name = name;
    this.version = version;
  }

  private base(): string {
    return this.stream._path(`/consumers/${encodeURIComponent(this.name)}`);
  }

  async pull(options?: PullOptions): Promise<PullBatch<T>> {
    const { signal, ...pullOptions } = options ?? {};
    const res = await req(
      this.ctx,
      "POST",
      `${this.base()}:pull`,
      { ...this.stream._kh(), "content-type": "application/json" },
      JSON.stringify(pullOptions),
      signal,
    );
    const body = (await res.json()) as {
      messages: Array<{
        id: string;
        routingKey: string;
        attempts: number;
        leaseToken: string;
        value: T;
      }>;
      backlog: number;
    };
    const acks: Array<{ leaseToken: string }> = [];
    const retries: Array<{ leaseToken: string; delayMs?: number }> = [];
    const extends_: Array<{ leaseToken: string; visibilityMs: number }> = [];
    let settlement: Promise<SettleResult> | undefined;
    const decide = (record: () => void) => {
      if (settlement) throw new Error("this batch's settlement has already started");
      record();
    };
    const messages: ConsumerMessage<T>[] = body.messages.map((m) => ({
      ...m,
      ack: () => decide(() => { acks.push({ leaseToken: m.leaseToken }); }),
      retry: (o?: { delayMs?: number }) =>
        decide(() => { retries.push({ leaseToken: m.leaseToken, delayMs: o?.delayMs }); }),
      extend: (o: { visibilityMs: number }) =>
        decide(() => { extends_.push({ leaseToken: m.leaseToken, visibilityMs: o.visibilityMs }); }),
    }));
    const submit = async (): Promise<SettleResult> => {
      if (acks.length + retries.length + extends_.length === 0) {
        return { acked: 0, retried: 0, extended: 0, dlq: 0, stale: 0, backlog: body.backlog };
      }
      // A transport failure is replayed with the same decisions. The server
      // applies a decision only to a lease of the generation it names: an
      // ack the lost attempt applied removed its lease, so the replay counts
      // it `stale`; a retry or an extend kept its lease and generation, so
      // the replay applies it again and sets its deadline from the replay's
      // time, unless a pull delivered the message again in between. A
      // failure after the attempts is this batch's settlement failure;
      // leases it did not settle expire and are delivered again.
      const res2 = await req(
        this.ctx,
        "POST",
        `${this.base()}:settle`,
        { ...this.stream._kh(), "content-type": "application/json" },
        JSON.stringify({ acks, retries, extends: extends_ }),
      );
      return (await res2.json()) as SettleResult;
    };
    return { messages, backlog: body.backlog, settle: () => settlement ??= submit() };
  }

  /** Pull-process-settle loop: `for await (const msg of consumer) { ...; msg.ack(); }` */
  [Symbol.asyncIterator](): ConsumerIterator<T> {
    return this.messages();
  }

  /** An iterator whose return() immediately cancels a parked pull.
   * Recorded decisions are settled independently of pull cancellation. */
  messages(options?: { signal?: AbortSignal }): ConsumerIterator<T> {
    const controller = new AbortController();
    const abort = () => controller.abort(options?.signal?.reason);
    if (options?.signal?.aborted) abort();
    else options?.signal?.addEventListener("abort", abort, { once: true });
    let complete!: (outcome: ConsumerIteratorOutcome) => void;
    const closed = new Promise<ConsumerIteratorOutcome>(resolve => { complete = resolve; });
    const finish = (outcome: ConsumerIteratorOutcome) => {
      options?.signal?.removeEventListener("abort", abort);
      complete(outcome);
    };
    const consumer = this;
    const generator = (async function* () {
      let backoff = 1000;
      while (!controller.signal.aborted) {
        let batch: PullBatch<T>;
        try {
          batch = await consumer.pull({ waitMs: 20000, signal: controller.signal });
        } catch (error) {
          if (controller.signal.aborted && error === controller.signal.reason) return;
          // A restart or a busy cell (a transport failure, or a 429/503
          // after the request's own replays): pull again.
          if (!retryableRequestError(error)) throw error;
          backoff = await reconnectAfter(backoff, controller.signal);
          continue;
        }
        backoff = 1000;
        let processingFailed = false;
        let processingError: unknown;
        try {
          for (const message of batch.messages) {
            if (controller.signal.aborted) return;
            yield message;
          }
        } catch (error) {
          processingFailed = true;
          processingError = error;
          throw error;
        } finally {
          try {
            await batch.settle();
          } catch (error) {
            if (processingFailed) {
              throw new AggregateError([processingError, error], "consumer processing and settlement failed");
            }
            throw error;
          }
        }
      }
    })();
    const observe = async (operation: Promise<IteratorResult<ConsumerMessage<T>, void>>) => {
      try {
        const result = await operation;
        if (result.done) finish({ status: "closed" });
        return result;
      } catch (error) {
        finish({ status: "failed", error });
        throw error;
      }
    };
    return {
      closed,
      [Symbol.asyncIterator]() { return this; },
      next: () => observe(generator.next()),
      return: () => { controller.abort(); return observe(generator.return()); },
      throw: (error?: unknown) => { controller.abort(); return observe(generator.throw(error)); },
    };
  }

  async config(): Promise<ConsumerConfig & { name: string }> {
    const res = await req(this.ctx, "GET", this.base(), this.stream._kh());
    return (await res.json()) as ConsumerConfig & { name: string };
  }

  /** Delete this consumer collection-wide. The server runs a
   * generation-fenced saga against THIS incarnation (the request
   * carries this object's version token): 204 means the targeted
   * incarnation's delivery state is gone on every segment — including
   * the case where someone else already deleted it, or deleted and
   * recreated the name (the replacement is never touched). A 5xx means
   * the deletion is incomplete — retry this call to resume it;
   * progress is durable, so every retry advances. */
  async delete(): Promise<void> {
    const res = await req(this.ctx, "DELETE", this.base(), {
      ...this.stream._kh(),
      "prisma-consumer-version": this.version,
    });
  }
}

// ---------------------------------------------------------------------
// Watch-key derivation and URL signing.
//
// A signed observation URL is built entirely on the client, from the
// stream key it already holds — no round trip, and the caller never
// handles template ids, journal cursors or HMAC keys (spec Stage 2
// §3.3/§3.5). Every step mirrors the server byte for byte:
// `src/touch_keys.rs` for the key layouts, `src/crypto.rs` for the
// signature chain. Changing either side alone silently stops matching,
// so the smoke test derives a key here and asserts the server agrees.

let cryptoCache: Crypto | undefined;
async function webCrypto(): Promise<Crypto> {
  if (cryptoCache) return cryptoCache;
  const c = (globalThis as { crypto?: Crypto }).crypto;
  if (c?.subtle) return (cryptoCache = c);
  // Node 18 — the engines floor — ships WebCrypto only at
  // node:crypto.webcrypto; the GLOBAL landed in Node 19. This branch
  // is dead code wherever globalThis.crypto exists (browsers, Bun,
  // Deno, Node 19+), so bundlers never execute the import.
  try {
    // Variable specifier: the package ships without Node types, and a
    // literal import would make TS (and bundlers) try to resolve a
    // module that only exists at runtime on Node.
    const spec = "node:crypto";
    const nc = (await import(/* @vite-ignore */ spec)) as {
      webcrypto?: Crypto;
    };
    if (nc.webcrypto?.subtle) return (cryptoCache = nc.webcrypto);
  } catch {
    // no node:crypto either — fall through to the error
  }
  throw new Error(
    "WebCrypto is required to derive watch URLs and generate keys (Node 18+, or a browser over HTTPS)",
  );
}

async function subtle(): Promise<SubtleCrypto> {
  return (await webCrypto()).subtle;
}

/**
 * A new stream encryption key: 32 random bytes from WebCrypto, as 43
 * characters of unpadded base64url. Create it once per stream and keep it
 * in your secret store: the service never stores it, and a stream whose
 * key is lost cannot be read by anyone.
 */
export async function generateEncryptionKey(): Promise<string> {
  const bytes = (await webCrypto()).getRandomValues(new Uint8Array(32));
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

const TE = new TextEncoder();

function bytesOf(...parts: (Uint8Array | string)[]): Uint8Array {
  const chunks = parts.map((p) => (typeof p === "string" ? TE.encode(p) : p));
  const out = new Uint8Array(chunks.reduce((n, c) => n + c.length, 0));
  let at = 0;
  for (const c of chunks) {
    out.set(c, at);
    at += c.length;
  }
  return out;
}

function b64ToBytes(b64: string): Uint8Array {
  // The server accepts URL-safe, unpadded base64 keys; `atob` does not.
  // Without this a key works for appends and reads and then fails only
  // when deriving a watch URL.
  const std = b64.replace(/-/g, "+").replace(/_/g, "/");
  const bin = atob(std.padEnd(std.length + ((4 - (std.length % 4)) % 4), "="));
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

function hexToBytes(h: string): Uint8Array {
  const out = new Uint8Array(h.length >> 1);
  for (let i = 0; i < out.length; i++) {
    out[i] = parseInt(h.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

/**
 * WebCrypto takes a BufferSource. Handing it the backing ArrayBuffer
 * keeps this compiling across TypeScript versions, which disagree about
 * whether a Uint8Array's buffer might be shared.
 */
function ab(b: Uint8Array): ArrayBuffer {
  return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength) as ArrayBuffer;
}

function toHex(b: Uint8Array): string {
  return Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
}

/** h64: the first 8 bytes of SHA-256, big-endian, as 16 lowercase hex. */
async function h64Hex(buf: Uint8Array): Promise<string> {
  const d = new Uint8Array(await (await subtle()).digest("SHA-256", ab(buf)));
  return toHex(d.subarray(0, 8));
}

/**
 * Canonical value encoding for watch keys. Compact JSON, so "1", 1,
 * true and null are all distinct — with two rules that keep JavaScript
 * and the server byte-identical: object keys are sorted, and a whole
 * number is written without a fractional part.
 */
export function canonicalWatchValue(v: unknown): string {
  if (v === null || v === undefined) return "null";
  if (typeof v === "boolean") return v ? "true" : "false";
  if (typeof v === "number") {
    if (!Number.isFinite(v)) throw new Error("watch values must be finite");
    return String(v);
  }
  if (typeof v === "string") return JSON.stringify(v);
  if (Array.isArray(v)) return `[${v.map(canonicalWatchValue).join(",")}]`;
  if (typeof v === "object") {
    const o = v as Record<string, unknown>;
    const keys = Object.keys(o).sort();
    return `{${keys.map((k) => `${JSON.stringify(k)}:${canonicalWatchValue(o[k])}`).join(",")}}`;
  }
  throw new Error(`cannot encode ${typeof v} as a watch value`);
}

/** watchKey = h64("key\0" + templateIdBE + ("\0" + arg)*), field order as declared. */
export async function deriveWatchKey(
  watchName: string,
  fields: string[],
  values: unknown[],
): Promise<string> {
  const tplHex = await h64Hex(
    bytesOf("tpl\0", watchName, "\0", fields.join("\0")),
  );
  const args = values.map(canonicalWatchValue).map((a) => `\0${a}`).join("");
  return h64Hex(bytesOf("key\0", hexToBytes(tplHex), args));
}

async function hkdf32(
  ikm: Uint8Array,
  salt: Uint8Array,
  info: string,
): Promise<Uint8Array> {
  const k = await (await subtle()).importKey("raw", ab(ikm), "HKDF", false, ["deriveBits"]);
  const bits = await (await subtle()).deriveBits(
    { name: "HKDF", hash: "SHA-256", salt: ab(salt), info: ab(TE.encode(info)) },
    k,
    256,
  );
  return new Uint8Array(bits);
}

/**
 * The observation capability for one watch key: an HMAC over the key,
 * under a signing key two HKDF steps below the stream key. It grants
 * observation and nothing else — no decryption, append, consumer or
 * management rights — and the server verifies it against the verifier
 * it persisted at create.
 */
/**
 * The §2.1 canonical length-prefixed encoding, mirrored from the
 * server's tenant::encode_hash_input: every component (the domain tag
 * first) is prefixed with its 4-byte big-endian length, so component
 * boundaries can never alias.
 */
function encodeCapabilityInput(components: (Uint8Array | string)[]): Uint8Array {
  const parts: Uint8Array[] = [];
  let total = 0;
  for (const c of components) {
    const b = typeof c === "string" ? new TextEncoder().encode(c) : c;
    const len = new Uint8Array(4);
    new DataView(len.buffer).setUint32(0, b.length, false);
    parts.push(len, b);
    total += 4 + b.length;
  }
  const out = new Uint8Array(total);
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

async function watchCapability(
  encryptionKeyB64: string,
  epochHex: string,
  project: string,
  streamName: string,
  watchName: string,
  watchKeyHex: string,
  expiresUnix: number,
): Promise<string> {
  const epoch = hexToBytes(epochHex);
  const token = await hkdf32(b64ToBytes(encryptionKeyB64), epoch, "touch-capability-v1");
  const sigKey = await hkdf32(token, epoch, "wait-sig-v1");
  const expBytes = new Uint8Array(8);
  new DataView(expBytes.buffer).setBigInt64(0, BigInt(expiresUnix), false);
  const input = encodeCapabilityInput([
    "watch-capability-v1",
    project,
    streamName,
    epochHex,
    watchName,
    watchKeyHex,
    "GET",
    expBytes,
  ]);
  const mac = await (await subtle()).importKey(
    "raw",
    ab(sigKey),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const out = new Uint8Array(await (await subtle()).sign("HMAC", mac, ab(input)));
  // Wire v2: the capability CARRIES the project so the server resolves
  // which project's same-named stream it targets before any lookup.
  // The signature input has always bound the project, so a swapped
  // prefix cannot verify.
  return `${project}.${expiresUnix}.${toHex(out.subarray(0, 16))}`;
}

// ---------------------------------------------------------------------
// Watch observation

/** §15: capabilities are SHORT-LIVED; default mint horizon. */
const WATCH_CAP_TTL_SECS = 240;

export class Watch {
  private ctx: Ctx;
  private stream: Stream<unknown>;
  readonly name: string;
  /** The derived 64-bit watch key, 16 lowercase hex. */
  readonly key: string;
  private encryptionKey: string;
  private epochHex: string;

  constructor(
    ctx: Ctx,
    stream: Stream<unknown>,
    name: string,
    key: string,
    encryptionKey: string,
    epochHex: string,
  ) {
    this.ctx = ctx;
    this.stream = stream;
    this.name = name;
    this.key = key;
    this.encryptionKey = encryptionKey;
    this.epochHex = epochHex;
  }

  /**
   * Mint a fresh observation capability for this key: bound to
   * project + stream + epoch + watch + key + GET + expiry, valid for
   * at most 5 minutes (server-enforced). It grants observation of
   * this one key and nothing else — no decryption, append, consumer
   * or management rights.
   */
  async capability(ttlSecs: number = WATCH_CAP_TTL_SECS): Promise<string> {
    const exp = Math.floor(Date.now() / 1000) + Math.min(ttlSecs, 300);
    return watchCapability(
      this.encryptionKey,
      this.epochHex,
      this.ctx.project!,
      this.stream.name,
      this.name,
      this.key,
      exp,
    );
  }

  /**
   * An absolute observation URL carrying a FRESH short-lived
   * capability (<=5 minutes). Safe to hand to an untrusted client —
   * it carries no stream key and no account token, observes this one
   * key only, and expires — but treat it as a secret while live:
   * never log it, and prefer `capability()` with the
   * `Authorization: Prisma-Watch` header where the client can set
   * headers.
   */
  async url(options?: { cursor?: string; timeoutMs?: number }): Promise<string> {
    return `${this.ctx.base}${await this.path(options)}`;
  }

  private async path(options?: {
    cursor?: string;
    timeoutMs?: number;
  }): Promise<string> {
    const q = new URLSearchParams();
    q.set("cursor", options?.cursor ?? "now");
    if (options?.timeoutMs) q.set("timeoutMs", String(options.timeoutMs));
    q.set("cap", await this.capability());
    return this.stream._path(
      `/watches/${encodeURIComponent(this.name)}/keys/${this.key}?${q}`,
    );
  }

  async wait(options?: {
    cursor?: string;
    timeoutMs?: number;
    signal?: AbortSignal;
  }): Promise<WatchEvent> {
    // Each attempt mints its own capability: a replay after a parked wait
    // the network dropped silently (fetch gives up on the headers minutes
    // later) or a sleep would otherwise carry one that expired meanwhile,
    // and the server refuses that 403 for good.
    const res = await req(
      this.ctx,
      "GET",
      () => this.path(options),
      {},
      undefined,
      options?.signal,
    );
    return (await res.json()) as WatchEvent;
  }

  async *subscribe(options?: {
    from?: string;
    signal?: AbortSignal;
  }): AsyncIterable<WatchEvent> {
    let cursor = options?.from ?? "now";
    let backoff = 1000;
    for (;;) {
      if (options?.signal?.aborted) return;
      let ev: WatchEvent;
      try {
        // Each wait mints a fresh capability, so a reconnection after a
        // long outage is not refused for an expired one.
        ev = await this.wait({
          cursor,
          timeoutMs: 25000,
          signal: options?.signal,
        });
      } catch (error) {
        if (options?.signal?.aborted) return;
        if (!retryableRequestError(error)) throw error;
        backoff = await reconnectAfter(backoff, options?.signal);
        continue;
      }
      backoff = 1000;
      cursor = ev.cursor;
      if (ev.invalidated) yield ev;
    }
  }
}
