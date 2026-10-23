/**
 * Native queues (docs/QUEUES-DESIGN.md; first release: the `default`
 * group). A queue holds JSON or byte messages encrypted with the stream key;
 * workers claim leases on them and settle each lease by its token.
 *
 *   const emails = client.queue<EmailJob>("emails", { encryptionKey });
 *   await emails.send(job, { dedupKey: job.id, delay: "2m" });
 *   const worker = emails.process(async (msg, { signal }) => {
 *     await deliver(msg.body, { signal });
 *     // return → ack · throw new RetryLater({ after: "30s" }) → retry
 *     // throw anything else → retry with the queue's backoff
 *     // return msg.fail("unsubscribed") · return msg.discard()
 *   }, { concurrency: 10, lease: "5m" });
 *   await worker.stop();
 *
 * A Cloudflare Worker (or any `fetch` handler) drains on a wake-up:
 *
 *   fetch: (request, env, ctx) => emails.handleWakeup(request, handler,
 *     { secret: env.WAKEUP_SECRET, waitUntil: (p) => ctx.waitUntil(p) })
 */

// Used inside functions only: index.ts imports this module, so at load time
// one of the two is not evaluated yet.
import { StreamsError } from "./index.js";
import {
  drainQueue, durationMs, runWorker, SettleDecision,
  type Duration, type Handler, type Worker, type WorkerOptions,
} from "./queue_runner.js";
import { subtle } from "./webcrypto.js";

export {
  HandlerAbortedError, RetryLater, SettleDecision,
  type Duration, type Handler, type Worker, type WorkerOptions,
} from "./queue_runner.js";

// ---------------------------------------------------------------------
// Wire types (the HTTP contract of the first release)

export interface QueueOptions<T> {
  /** base64url 32-byte stream encryption key; needed to create, send,
   * claim and read bodies (and for a settle that carries error text). */
  encryptionKey?: string;
  /** Turns a message's body (JSON, or the bytes of a `bodyBase64` message)
   * into `T`. A body it throws on is `invalid` and never reaches a handler. */
  parse?: (body: unknown) => T;
  /** What `process()` does with an invalid body: `"retry"` (default) with
   * the queue's backoff (a deploy window with a schema mismatch would
   * otherwise dead-letter everything), or `"fail"` with `invalid_payload`. */
  onInvalid?: "retry" | "fail";
}

export interface QueueGroupConfig {
  maxAttempts?: number;
  backoff?: { initialMs?: number; maxMs?: number; multiplier?: number };
  leaseMs?: number;
  maxInFlight?: number;
  maxBatch?: number;
}

export interface QueueConfig {
  retentionMs?: number;
  failureRetentionMs?: number;
  maxQueuedBytes?: number;
  maxQueuedMessages?: number;
  dedupWindowMs?: number;
  maxDedupEntries?: number;
  group?: QueueGroupConfig;
}

export interface QueueStats {
  due: number;
  scheduled: number;
  leased: number;
  retrying: number;
  failed: number;
  oldestDueAgeMs: number | null;
  nextAvailableAt: number | null;
  paused: boolean;
  retentionGap: { count: number };
  queuedBytes: number;
  queuedMessages: number;
  counters: Record<
    "sent" | "delivered" | "acked" | "retried" | "failed" | "expired" | "discarded" | "redriven" | "stale",
    number
  >;
}

export interface SendOptions {
  /** Stored with the message (ordering by key comes later). */
  key?: string;
  /** A second send with this key inside the dedup window answers the
   * original id with `duplicate: true`. A send with one is also sent again
   * after a lost answer; one without is not (that could make a copy). */
  dedupKey?: string;
  /** Deliver after this long; or `deliverAt`, not both. */
  delay?: Duration;
  /** Deliver at this time (Unix ms or a Date). */
  deliverAt?: number | Date;
  /** At most 16 string entries, 4 KiB. */
  headers?: Record<string, string>;
  /** Defaults to `application/json`, or `application/octet-stream` for bytes. */
  contentType?: string;
}

export interface SendItem<T> extends SendOptions {
  /** JSON, or bytes (sent as `bodyBase64`). */
  body: T | Uint8Array;
}

export interface SendResult {
  id: string;
  enqueuedAt: number;
  availableAt: number;
  duplicate: boolean;
}

/** A message as get, the dead-letter view and peek return it. */
export interface StoredMessage<T> {
  id: string;
  attempt: number;
  cycle: number;
  enqueuedAt: number;
  availableAt: number;
  leaseDeadline: number;
  /** The parsed body: JSON as sent, the bytes of a `bodyBase64` message, or
   * what `parse` made of either; `undefined` when `invalid`. */
  body: T;
  bodyBase64: string | null;
  contentType: string;
  headers: Record<string, string>;
  key: string | null;
  dedupKey: string | null;
  lineage: { originId: string; republishedFrom: string | null };
  /** Set when `parse` threw on the body. */
  invalid?: { error: unknown };
}

/** A claimed message: a lease, settled by its `token`. */
export interface QueueMessage<T> extends StoredMessage<T> {
  token: string;
  /** Returned from a `process()` handler: settle as `fail` (to the
   * dead-letter view) with this code and error text. */
  fail(code?: string, error?: string): SettleDecision;
  /** Returned from a `process()` handler: done without success. */
  discard(code?: string): SettleDecision;
}

export interface ClaimOptions {
  /** At most this many messages (the server clamps to the group's `maxBatch`). */
  max?: number;
  maxBytes?: number;
  lease?: Duration;
  /** Park up to this long (≤ 25,000 ms) while nothing is due. */
  waitMs?: number;
  signal?: AbortSignal;
}

export interface ClaimResult<T> {
  messages: QueueMessage<T>[];
  paused: boolean;
}

export type SettleOp =
  | { token: string; op: "ack" }
  | { token: string; op: "retry"; delayMs?: number; code?: string; error?: string }
  | { token: string; op: "extend"; leaseMs: number }
  | { token: string; op: "fail"; code?: string; error?: string }
  | { token: string; op: "discard"; code?: string }
  | { token: string; op: "release" };

export interface SettleOutcome {
  token: string;
  outcome: "ok" | "stale" | "expired";
  acceptedLate: boolean;
}

export interface MessageDelivery {
  state: "pending" | "leased" | "retrying" | "failed" | "done";
  attempts: number;
  cycle: number;
}

export interface DeadLetter<T> extends StoredMessage<T> {
  failure: {
    cause: "explicit" | "exhausted" | "retention" | "lease_expired";
    code: string | null;
    error: string | null;
    attempts: number;
    failedAt: number;
    failureExpiresAt: number;
  };
}

export interface Page<M> {
  messages: M[];
  next: string | null;
}

export interface RedriveResult {
  redriven: number;
  remaining: number;
  stoppedAt: null | "capacity";
}

export interface WakeupSubscription {
  url: string;
  /** base64url; returned once. Keep it where the receiver reads it. */
  secret: string;
  version: number;
}

export interface WakeupNotification {
  queue: string;
  group: string;
  due: number;
  oldestDueAgeMs: number;
  nonce: string;
  sentAt: number;
}

export interface WakeupOptions<T> extends WorkerOptions<T> {
  /** The subscription's secret; during a rotation, both. */
  secret: string | string[];
  /** How long the drain may run from the notification's arrival
   * (default 25,000 ms: Cloudflare runs `waitUntil` work about 30 s). */
  drainBudgetMs?: number;
  /** The end of the budget kept for settling (default 2,000 ms): claiming
   * stops when less than this is left. */
  settleMarginMs?: number;
  /** The runtime's background registration (Workers: `ctx.waitUntil`).
   * Without it the drain runs unregistered after the response. */
  waitUntil?: (work: Promise<unknown>) => void;
}

/** How the client sends a queue request (index.ts wires it to `req()`):
 * `once` sends no replay after a lost answer, for a request whose replay
 * could make a copy. */
export type QueueRequest = (
  method: string,
  path: string,
  headers: Record<string, string>,
  body?: string,
  signal?: AbortSignal,
  once?: boolean,
) => Promise<Response>;

interface RawMessage {
  id: string;
  token?: string;
  body?: unknown;
  bodyBase64?: string | null;
  [field: string]: unknown;
}

const JSON_TYPE = { "content-type": "application/json" };
const WAKEUP_SKEW_MS = 60_000;
const TE = new TextEncoder();

function encName(name: string): string {
  return name.split("/").map(encodeURIComponent).join("/");
}

/** Standard or URL-safe base64, padded or not. */
function base64Bytes(text: string): Uint8Array {
  const std = text.replace(/-/g, "+").replace(/_/g, "/");
  const bin = atob(std.padEnd(std.length + ((4 - (std.length % 4)) % 4), "="));
  return Uint8Array.from(bin, (c) => c.charCodeAt(0));
}

function bytesBase64(bytes: Uint8Array): string {
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin);
}

function hexBytes(hex: string): Uint8Array | undefined {
  if (!/^(?:[0-9a-fA-F]{2})+$/.test(hex)) return undefined;
  return Uint8Array.from({ length: hex.length / 2 }, (_, i) => parseInt(hex.slice(i * 2, i * 2 + 2), 16));
}

function ab(b: Uint8Array): ArrayBuffer {
  return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength) as ArrayBuffer;
}

function refusal(status: number, code: string, message: string): Response {
  return new Response(JSON.stringify({ error: { code, message, retryable: false } }), {
    status,
    headers: JSON_TYPE,
  });
}

function query(options: { limit?: number; cursor?: string } | undefined): string {
  const q = new URLSearchParams();
  if (options?.limit !== undefined) q.set("limit", String(options.limit));
  if (options?.cursor !== undefined) q.set("cursor", options.cursor);
  const text = q.toString();
  return text === "" ? "" : `?${text}`;
}

// ---------------------------------------------------------------------
// The queue handle

export class Queue<T = unknown> {
  readonly name: string;
  private readonly request: QueueRequest;
  private readonly key?: string;
  private readonly parse?: (body: unknown) => T;
  private readonly onInvalid: "retry" | "fail";

  /** @internal Made by `StreamsClient.queue()`. */
  constructor(request: QueueRequest, name: string, options: QueueOptions<T>, key?: string) {
    this.request = request;
    this.name = name;
    this.key = key;
    this.parse = options.parse;
    this.onInvalid = options.onInvalid ?? "retry";
  }

  // ---- configuration ----------------------------------------------------

  /** Creates the queue, or finds it with the same fixed configuration
   * (`created: false`); different fixed fields are `409 queue_config_conflict`. */
  async create(config: QueueConfig = {}): Promise<{ name: string; config: QueueConfig; created: boolean }> {
    const res = await this.request("PUT", this.path(), { ...JSON_TYPE, ...this.kh("create") }, JSON.stringify(config));
    const body = (await res.json()) as { name: string; config: QueueConfig };
    return { ...body, created: res.status === 201 };
  }

  async get(): Promise<{ name: string; config: QueueConfig; stats: QueueStats }> {
    return this.json("GET", this.path(), {});
  }

  /** Starts the deletion: sends, claims and redrives then answer
   * `409 queue_deleting`, and `get()` `404` once the cleanup finished. */
  async delete(): Promise<{ deleting: boolean }> {
    return this.json("DELETE", this.path(), {}, undefined, true);
  }

  // ---- send -------------------------------------------------------------

  async send(body: T | Uint8Array, options: SendOptions = {}): Promise<SendResult> {
    const doc = sendDoc({ ...options, body });
    return this.json("POST", this.path("/messages"), { ...JSON_TYPE, ...this.kh("send") }, JSON.stringify(doc),
      options.dedupKey === undefined);
  }

  /** One atomic batch (≤ 1,000 messages): a refusal refuses all of them. */
  async sendBatch(items: SendItem<T>[]): Promise<SendResult[]> {
    const messages = items.map(sendDoc);
    const once = items.some((item) => item.dedupKey === undefined);
    const body = await this.json<{ results: SendResult[] }>("POST", this.path("/messages:batch"),
      { ...JSON_TYPE, ...this.kh("send") }, JSON.stringify({ messages }), once);
    return body.results;
  }

  // ---- claim and settle -------------------------------------------------

  async claim(options: ClaimOptions = {}): Promise<ClaimResult<T>> {
    const doc = {
      max: options.max,
      maxBytes: options.maxBytes,
      leaseMs: options.lease === undefined ? undefined : durationMs(options.lease, "lease"),
      waitMs: options.waitMs,
    };
    const body = await this.json<{ messages: RawMessage[]; paused?: boolean }>("POST", this.group(":claim"),
      { ...JSON_TYPE, ...this.kh("claim") }, JSON.stringify(doc), false, options.signal);
    return { messages: body.messages.map((raw) => this.claimed(raw)), paused: body.paused === true };
  }

  /** Settles leases by token (≤ 1,000 operations); one result per operation. */
  async settle(ops: SettleOp[]): Promise<SettleOutcome[]> {
    // Error text is encrypted by the server, so it needs the key.
    const headers = ops.some((op) => "error" in op && op.error !== undefined)
      ? { ...JSON_TYPE, ...this.kh("a settle with error text") }
      : JSON_TYPE;
    const body = await this.json<{ results: SettleOutcome[] }>("POST", this.group(":settle"), headers,
      JSON.stringify({ ops }));
    return body.results;
  }

  /** Runs `handler` on claimed messages until `stop()` (§9.1). */
  process(handler: Handler<T>, options: WorkerOptions<T> = {}): Worker {
    return runWorker(this, handler, { ...options, onInvalid: this.onInvalid });
  }

  // ---- operator actions -------------------------------------------------

  async pause(): Promise<{ paused: boolean }> {
    return this.json("POST", this.group(":pause"), {});
  }

  async resume(): Promise<{ paused: boolean }> {
    return this.json("POST", this.group(":resume"), {});
  }

  async message(id: string): Promise<{ message: StoredMessage<T>; delivery: MessageDelivery }> {
    const body = await this.json<{ message: RawMessage; delivery: MessageDelivery }>("GET",
      this.path(`/messages/${encodeURIComponent(id)}`), this.kh("reading a message"));
    return { message: this.stored(body.message), delivery: body.delivery };
  }

  /** The dead-letter view: failed deliveries, oldest first, paginated. */
  async deadLetters(options?: { limit?: number; cursor?: string }): Promise<Page<DeadLetter<T>>> {
    const body = await this.json<Page<RawMessage>>("GET", this.group(`/failed${query(options)}`),
      this.kh("the dead-letter view"));
    return { messages: body.messages.map((raw) => this.stored(raw) as DeadLetter<T>), next: body.next ?? null };
  }

  /** Open deliveries and the next due messages, without leasing them. */
  async peek(
    options?: { limit?: number; cursor?: string },
  ): Promise<Page<StoredMessage<T> & { state: string }>> {
    const body = await this.json<Page<RawMessage>>("GET", this.group(`/pending${query(options)}`), this.kh("peek"));
    return {
      messages: body.messages.map((raw) => this.stored(raw) as StoredMessage<T> & { state: string }),
      next: body.next ?? null,
    };
  }

  /** Failed deliveries back to due: by id, or up to `limit` of them. */
  async redrive(target: string[] | { all: true; limit?: number }): Promise<RedriveResult> {
    const doc = Array.isArray(target) ? { ids: target } : { all: true, limit: target.limit };
    return this.json("POST", this.group(":redrive"), JSON_TYPE, JSON.stringify(doc));
  }

  /** Failed deliveries removed from the dead-letter view. */
  async discard(ids: string[]): Promise<{ discarded: number }> {
    return this.json("POST", this.group(":discard"), JSON_TYPE, JSON.stringify({ ids }));
  }

  /** Every group skips these messages; open leases answer `stale`. */
  async cancel(ids: string[]): Promise<{ cancelled: number }> {
    return this.json("POST", this.path(":cancel"), JSON_TYPE, JSON.stringify({ ids }));
  }

  /** Failed messages sent again as new messages; never sent twice after a
   * lost answer (each attempt makes copies). */
  async republish(ids: string[]): Promise<Array<{ from: string; id: string }>> {
    const headers = this.key === undefined ? JSON_TYPE : { ...JSON_TYPE, ...this.kh("republish") };
    const body = await this.json<{ results: Array<{ from: string; id: string }> }>("POST", this.path(":republish"),
      headers, JSON.stringify({ ids }), true);
    return body.results;
  }

  // ---- wake-up ----------------------------------------------------------

  /** Subscribes `url` to wake-up notifications; the secret is returned once. */
  async setWakeup(url: string): Promise<WakeupSubscription> {
    return this.json("PUT", this.group("/wakeup"), JSON_TYPE, JSON.stringify({ url }));
  }

  async deleteWakeup(): Promise<void> {
    await this.request("DELETE", this.group("/wakeup"), {});
  }

  /**
   * A wake-up receiver (§9.2): verifies the notification's
   * `streams-signature` (`v1=<hex HMAC-SHA256(secret, sentAt + "." + body)>`),
   * that it names this queue and the default group, and that its `sentAt`
   * is within 60 s of this clock; then registers a bounded drain through
   * `waitUntil` and answers `202` at once. A refused notification answers
   * `401` (signature, age) or `400` (body, queue) and starts nothing.
   */
  async handleWakeup(request: Request, handler: Handler<T>, options: WakeupOptions<T>): Promise<Response> {
    const arrived = Date.now();
    const verdict = await verifyWakeup(request, this.name, options.secret, arrived);
    if (verdict instanceof Response) return verdict;
    const drain = drainQueue(this, handler, { ...options, onInvalid: this.onInvalid }, arrived,
      options.drainBudgetMs ?? 25_000, options.settleMarginMs ?? 2_000).catch(() => {
      // Reported through `onError` already; the next notification retries.
    });
    options.waitUntil?.(drain);
    return new Response(null, { status: 202 });
  }

  // ---- internals ----------------------------------------------------------

  private path(rest = ""): string {
    return `/v1/queues/${encName(this.name)}${rest}`;
  }

  private group(rest: string): string {
    return this.path(`/groups/default${rest}`);
  }

  private kh(what: string): Record<string, string> {
    if (this.key === undefined) {
      throw new StreamsError(0, "encryption_key_required",
        `${what} needs the queue's encryption key: pass encryptionKey to client.queue(); nothing was sent`, false);
    }
    return { "streams-encryption-key": this.key };
  }

  private async json<R>(
    method: string,
    path: string,
    headers: Record<string, string>,
    body?: string,
    once = false,
    signal?: AbortSignal,
  ): Promise<R> {
    const res = await this.request(method, path, headers, body, signal, once);
    return (await res.json()) as R;
  }

  private stored(raw: RawMessage): StoredMessage<T> {
    const payload = raw.bodyBase64 != null ? base64Bytes(raw.bodyBase64) : raw.body;
    const message = { ...raw } as unknown as StoredMessage<T>;
    if (this.parse === undefined) {
      message.body = payload as T;
      return message;
    }
    try {
      message.body = this.parse(payload);
    } catch (error) {
      message.body = undefined as T;
      message.invalid = { error };
    }
    return message;
  }

  private claimed(raw: RawMessage): QueueMessage<T> {
    const message = this.stored(raw) as QueueMessage<T>;
    message.fail = (code?: string, error?: string) => new SettleDecision("fail", code, error);
    message.discard = (code?: string) => new SettleDecision("discard", code);
    return message;
  }
}

// ---------------------------------------------------------------------
// Send documents and wake-up verification

function sendDoc<T>(item: SendItem<T>): Record<string, unknown> {
  if (item.delay !== undefined && item.deliverAt !== undefined) {
    throw new StreamsError(0, "invalid_send_options", "a send takes delay or deliverAt, not both; nothing was sent",
      false);
  }
  const bytes = item.body instanceof Uint8Array;
  const doc: Record<string, unknown> = bytes
    ? { bodyBase64: bytesBase64(item.body as Uint8Array) }
    : { body: item.body };
  doc["contentType"] = item.contentType ?? (bytes ? "application/octet-stream" : "application/json");
  if (item.headers !== undefined) doc["headers"] = item.headers;
  if (item.key !== undefined) doc["key"] = item.key;
  if (item.dedupKey !== undefined) doc["dedupKey"] = item.dedupKey;
  if (item.delay !== undefined) doc["delayMs"] = durationMs(item.delay, "delay");
  if (item.deliverAt !== undefined) {
    doc["deliverAt"] = item.deliverAt instanceof Date ? item.deliverAt.getTime() : item.deliverAt;
  }
  return doc;
}

/** The notification, or the response refusing it. The HMAC key is the
 * secret's bytes (the base64url text decoded), as the server derives them. */
async function verifyWakeup(
  request: Request,
  queue: string,
  secret: string | string[],
  now: number,
): Promise<WakeupNotification | Response> {
  const raw = await request.text();
  const header = request.headers.get("streams-signature");
  const signatures = (header ?? "").split(",").map((part) => part.trim())
    .filter((part) => part.startsWith("v1=")).map((part) => hexBytes(part.slice(3)))
    .filter((sig): sig is Uint8Array => sig !== undefined && sig.length === 32);
  if (signatures.length === 0) return refusal(401, "invalid_signature", "no v1 streams-signature");
  let note: WakeupNotification;
  try {
    note = JSON.parse(raw) as WakeupNotification;
  } catch {
    return refusal(400, "invalid_notification", "the notification is not JSON");
  }
  if (typeof note !== "object" || note === null || !Number.isSafeInteger(note.sentAt)) {
    return refusal(400, "invalid_notification", "the notification has no integer sentAt");
  }
  const signed = ab(TE.encode(`${note.sentAt}.${raw}`));
  const crypto = await subtle();
  let authentic = false;
  for (const s of Array.isArray(secret) ? secret : [secret]) {
    const key = await crypto.importKey("raw", ab(base64Bytes(s)), { name: "HMAC", hash: "SHA-256" }, false,
      ["verify"]);
    for (const sig of signatures) {
      if (await crypto.verify("HMAC", key, ab(sig), signed)) authentic = true;
    }
  }
  if (!authentic) return refusal(401, "invalid_signature", "the streams-signature does not match");
  if (Math.abs(now - note.sentAt) > WAKEUP_SKEW_MS) {
    return refusal(401, "stale_notification", "the notification's sentAt is more than 60 s from this clock");
  }
  if (note.queue !== queue || note.group !== "default") {
    return refusal(400, "wrong_queue", `this receiver serves queue ${queue}, group default`);
  }
  return note;
}
