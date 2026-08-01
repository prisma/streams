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
  fetch?: typeof fetch;
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
}

export interface ReadPage<T> {
  records: T[];
  cursor: string;
  upToDate: boolean;
  sealed: boolean;
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
  max?: number;
  waitMs?: number;
  visibilityMs?: number;
}

export interface PullBatch<T> {
  messages: ConsumerMessage<T>[];
  backlog: number;
  /** Apply the recorded ack/retry/extend decisions in one request. */
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
}

export interface ProducerState {
  epoch: number;
  nextSeq: number;
}

export interface ProducerStateStore {
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
    return `${s.stream.length}:${s.stream}|${s.producerId.length}:${s.producerId}|${s.routingKey}`;
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
   * producer sharing this id). */
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

// ---------------------------------------------------------------------
// HTTP plumbing

interface Ctx {
  base: string;
  token?: string;
  fetch: typeof fetch;
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

async function errorFrom(res: Response): Promise<StreamsError> {
  let code = "http_error";
  let message = `HTTP ${res.status}`;
  let retryable = res.status === 429 || res.status === 503;
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
      details = body.error.details;
    }
  } catch {
    // non-JSON error body: keep defaults
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
  return new StreamsError(res.status, code, message, retryable, details);
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

async function req(
  ctx: Ctx,
  method: string,
  path: string,
  headers: Record<string, string>,
  body?: BodyInit,
  signal?: AbortSignal,
): Promise<Response> {
  const h: Record<string, string> = { ...headers };
  if (ctx.token) h["authorization"] = `Bearer ${ctx.token}`;
  for (let attempt = 0; ; attempt++) {
    // The signal reaches fetch itself, so an abort ends the in-flight
    // long poll instead of leaving it running until its timeout.
    const res = await ctx.fetch(`${ctx.base}${path}`, {
      method,
      headers: h,
      body,
      signal,
    });
    if ((res.status === 429 || res.status === 503) && attempt < 3) {
      if (signal?.aborted) return res;
      const ra = Number(res.headers.get("retry-after") ?? "1");
      // Abort-aware: a plain setTimeout here made cancellation wait out
      // the backoff — up to ~15 s across three attempts.
      await sleep(Math.min(ra, 5) * 1000, signal);
      if (signal?.aborted) return res;
      continue;
    }
    return res;
  }
}

// ---------------------------------------------------------------------
// Client

export class StreamsClient {
  private ctx: Ctx;

  constructor(options: StreamsClientOptions) {
    this.ctx = {
      base: options.url.replace(/\/$/, ""),
      token: options.token,
      fetch: options.fetch ?? fetch.bind(globalThis),
    };
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
    if (!res.ok) throw await errorFrom(res);
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
      if (!res.ok) throw await errorFrom(res);
      const body = (await res.json()) as {
        streams: StreamMetadata[];
        cursor?: string;
      };
      for (const s of body.streams) yield s;
      if (!body.cursor) return;
      cursor = body.cursor;
    }
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
    );
    if (!res.ok) throw await errorFrom(res);
    return (await res.json()) as AppendResult;
  }

  private async appendRaw(
    body: BodyInit,
    routingKey?: string,
    bytes = false,
    producerHeaders?: Record<string, string>,
  ): Promise<AppendResult> {
    const h: Record<string, string> = {
      ...this.kh(),
      "content-type": bytes ? "application/octet-stream" : "application/json",
      ...(producerHeaders ?? {}),
    };
    if (routingKey) h["prisma-routing-key"] = routingKey;
    const res = await req(this.ctx, "POST", this.path("/records"), h, body);
    if (!res.ok) throw await errorFrom(res);
    return (await res.json()) as AppendResult;
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
    const res = await req(
      this.ctx,
      "POST",
      this.path(batch ? "/records:batch" : "/records"),
      h,
      payload,
    );
    if (!res.ok) throw await errorFrom(res);
    return (await res.json()) as AppendResult;
  }

  // ---- read / subscribe / scan (spec Stage 6) -----------------------

  async read(options?: ReadOptions): Promise<ReadPage<T>> {
    const q = new URLSearchParams();
    if (options?.routingKey) q.set("routingKey", options.routingKey);
    if (options?.from) q.set("cursor", options.from);
    if (options?.maxBytes) q.set("maxBytes", String(options.maxBytes));
    const res = await req(
      this.ctx,
      "GET",
      this.path(`/records?${q}`),
      this.kh(),
    );
    if (!res.ok) throw await errorFrom(res);
    const records =
      res.status === 204 ? [] : ((await res.json()) as T[]) ?? [];
    return {
      records,
      cursor: res.headers.get("prisma-next-cursor") ?? "",
      upToDate: res.headers.get("prisma-up-to-date") === "true",
      sealed: res.headers.get("prisma-sealed") === "true",
    };
  }

  async *subscribe(options?: SubscribeOptions): AsyncIterable<T> {
    // Catch-up from the requested position, then long-poll the tail.
    // The committed cursor only advances after a page's records are
    // fully yielded (spec §4.3).
    let cursor = options?.from ?? "beginning";
    let live = false;
    for (;;) {
      if (options?.signal?.aborted) return;
      const q = new URLSearchParams();
      if (options?.routingKey) q.set("routingKey", options.routingKey);
      q.set("cursor", cursor);
      if (options?.maxBytes) q.set("maxBytes", String(options.maxBytes));
      if (live) q.set("waitMs", "25000");
      const path = this.path(
        live ? `/records:long-poll?${q}` : `/records?${q}`,
      );
      let res: Response;
      try {
        // The signal reaches fetch, so aborting ends the in-flight
        // 25-second long poll instead of leaving it running to term.
        res = await req(
          this.ctx,
          "GET",
          path,
          this.kh(),
          undefined,
          options?.signal,
        );
      } catch (e) {
        if (options?.signal?.aborted) return;
        await sleep(1000, options?.signal);
        continue;
      }
      if (res.status === 429 || res.status === 503) {
        await sleep(1000, options?.signal);
        if (options?.signal?.aborted) return;
        continue;
      }
      if (!res.ok && res.status !== 204) throw await errorFrom(res);
      const records =
        res.status === 204 ? [] : ((await res.json()) as T[]) ?? [];
      for (const r of records) yield r;
      cursor = res.headers.get("prisma-next-cursor") ?? cursor;
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
      if (!res.ok) throw await errorFrom(res);
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
    if (!res.ok) throw await errorFrom(res);
    return new Consumer<T>(this.ctx, this, name);
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
    const sig = await watchUrlSig(this.key, await this._epoch(), keyHex);
    return new Watch(this.ctx, this, name, keyHex, sig);
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
    if (!res.ok) throw await errorFrom(res);
    const body = (await res.json()) as { watches: WatchDefinition[] };
    return body.watches;
  }

  // ---- lifecycle (spec Stage 8) -------------------------------------

  async metadata(): Promise<StreamMetadata> {
    const res = await req(this.ctx, "GET", this.path(), this.kh());
    if (!res.ok) throw await errorFrom(res);
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
    // A supplied producer session owns the final record's identity, so
    // a retried seal deduplicates through the caller's own sequence
    // rather than the server's fallback identity.
    if (options?.final !== undefined && options.producer) {
      Object.assign(
        h,
        await options.producer._sealHeaders(options.routingKey ?? ""),
      );
    }
    const res = await req(
      this.ctx,
      "POST",
      this.path(":seal"),
      h,
      JSON.stringify(doc),
    );
    if (!res.ok && res.status !== 204) throw await errorFrom(res);
    if (options?.final !== undefined && options.producer) {
      await options.producer._sealCommitted(options.routingKey ?? "");
    }
  }

  async delete(): Promise<void> {
    const res = await req(this.ctx, "DELETE", this.path(), this.kh());
    if (!res.ok && res.status !== 204) throw await errorFrom(res);
  }
}

// ---------------------------------------------------------------------
// Producer session

export class Producer<T> {
  private chains = new Map<string, Promise<void>>();
  private stream: Stream<T>;
  private streamName: string;
  private id: string;
  private options: ProducerOptions & { state: ProducerStateStore };

  constructor(
    stream: Stream<T>,
    _ctx: Ctx,
    streamName: string,
    _key: string,
    id: string,
    options: ProducerOptions & { state: ProducerStateStore },
  ) {
    this.stream = stream;
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
    const scope = this.scope(routingKey);
    const st = (await this.options.state.load(scope)) ?? {
      epoch: 0,
      nextSeq: 0,
    };
    await this.options.state.save(scope, { epoch: st.epoch + 1, nextSeq: 0 });
  }

  private scope(routingKey: string): ProducerScope {
    return { stream: this.streamName, producerId: this.id, routingKey };
  }

  /** Same-key requests serialize in sequence order (spec §5.1). */
  private send(
    payload: string,
    routingKey: string,
    batch: boolean,
  ): Promise<AppendResult> {
    const prev = this.chains.get(routingKey) ?? Promise.resolve();
    let result!: Promise<AppendResult>;
    const next = prev.then(async () => {
      result = this.sendNow(payload, routingKey, batch);
      await result.catch(() => {});
    });
    this.chains.set(routingKey, next);
    // Drop the key once its queue drains, or a producer that writes to
    // many routing keys accumulates one settled promise per key it has
    // ever touched. Only the tail clears itself, so a chain that grew
    // while this one ran is left alone.
    void next.then(() => {
      if (this.chains.get(routingKey) === next) this.chains.delete(routingKey);
    });
    return next.then(() => result);
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

  constructor(ctx: Ctx, stream: Stream<T>, name: string) {
    this.ctx = ctx;
    this.stream = stream;
    this.name = name;
  }

  private base(): string {
    return this.stream._path(`/consumers/${encodeURIComponent(this.name)}`);
  }

  async pull(options?: PullOptions): Promise<PullBatch<T>> {
    const res = await req(
      this.ctx,
      "POST",
      `${this.base()}:pull`,
      { ...this.stream._kh(), "content-type": "application/json" },
      JSON.stringify(options ?? {}),
    );
    if (!res.ok) throw await errorFrom(res);
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
    const messages: ConsumerMessage<T>[] = body.messages.map((m) => ({
      ...m,
      ack: () => acks.push({ leaseToken: m.leaseToken }),
      retry: (o?: { delayMs?: number }) =>
        retries.push({ leaseToken: m.leaseToken, delayMs: o?.delayMs }),
      extend: (o: { visibilityMs: number }) =>
        extends_.push({ leaseToken: m.leaseToken, visibilityMs: o.visibilityMs }),
    }));
    const settle = async (): Promise<SettleResult> => {
      const res2 = await req(
        this.ctx,
        "POST",
        `${this.base()}:settle`,
        { ...this.stream._kh(), "content-type": "application/json" },
        JSON.stringify({ acks, retries, extends: extends_ }),
      );
      if (!res2.ok) throw await errorFrom(res2);
      return (await res2.json()) as SettleResult;
    };
    return { messages, backlog: body.backlog, settle };
  }

  /** Pull-process-settle loop: `for await (const msg of consumer) { ...; msg.ack(); }` */
  async *[Symbol.asyncIterator](): AsyncIterator<ConsumerMessage<T>> {
    for (;;) {
      const batch = await this.pull({ waitMs: 20000 });
      for (const m of batch.messages) yield m;
      if (batch.messages.length > 0) await batch.settle();
    }
  }

  async config(): Promise<ConsumerConfig & { name: string }> {
    const res = await req(this.ctx, "GET", this.base(), this.stream._kh());
    if (!res.ok) throw await errorFrom(res);
    return (await res.json()) as ConsumerConfig & { name: string };
  }

  async delete(): Promise<void> {
    const res = await req(this.ctx, "DELETE", this.base(), this.stream._kh());
    if (!res.ok && res.status !== 204) throw await errorFrom(res);
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

function subtle(): SubtleCrypto {
  const c = (globalThis as { crypto?: Crypto }).crypto;
  if (!c?.subtle) {
    throw new Error(
      "WebCrypto is required to derive watch URLs (Node 18+, or a browser over HTTPS)",
    );
  }
  return c.subtle;
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
  const d = new Uint8Array(await subtle().digest("SHA-256", ab(buf)));
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
  const k = await subtle().importKey("raw", ab(ikm), "HKDF", false, ["deriveBits"]);
  const bits = await subtle().deriveBits(
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
async function watchUrlSig(
  encryptionKeyB64: string,
  epochHex: string,
  watchKeyHex: string,
): Promise<string> {
  const epoch = hexToBytes(epochHex);
  const token = await hkdf32(b64ToBytes(encryptionKeyB64), epoch, "touch-capability-v1");
  const sigKey = await hkdf32(token, epoch, "wait-sig-v1");
  const mac = await subtle().importKey(
    "raw",
    ab(sigKey),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const out = new Uint8Array(
    await subtle().sign("HMAC", mac, ab(bytesOf("wait-url\0", watchKeyHex))),
  );
  return toHex(out.subarray(0, 8));
}

// ---------------------------------------------------------------------
// Watch observation

export class Watch {
  private ctx: Ctx;
  private stream: Stream<unknown>;
  readonly name: string;
  /** The derived 64-bit watch key, 16 lowercase hex. */
  readonly key: string;
  /** The observation capability for this key. */
  readonly sig: string;

  constructor(
    ctx: Ctx,
    stream: Stream<unknown>,
    name: string,
    key: string,
    sig: string,
  ) {
    this.ctx = ctx;
    this.stream = stream;
    this.name = name;
    this.key = key;
    this.sig = sig;
  }

  /**
   * The absolute observation URL. Safe to hand to an untrusted client:
   * it carries no stream key and no account token, and observes this
   * one key only.
   */
  url(options?: { cursor?: string; timeoutMs?: number }): string {
    return `${this.ctx.base}${this.path(options)}`;
  }

  private path(options?: { cursor?: string; timeoutMs?: number }): string {
    const q = new URLSearchParams();
    q.set("cursor", options?.cursor ?? "now");
    if (options?.timeoutMs) q.set("timeoutMs", String(options.timeoutMs));
    q.set("sig", this.sig);
    return this.stream._path(
      `/watches/${encodeURIComponent(this.name)}/keys/${this.key}?${q}`,
    );
  }

  async wait(options?: {
    cursor?: string;
    timeoutMs?: number;
    signal?: AbortSignal;
  }): Promise<WatchEvent> {
    const res = await req(
      this.ctx,
      "GET",
      this.path(options),
      {},
      undefined,
      options?.signal,
    );
    if (!res.ok) throw await errorFrom(res);
    return (await res.json()) as WatchEvent;
  }

  async *subscribe(options?: {
    from?: string;
    signal?: AbortSignal;
  }): AsyncIterable<WatchEvent> {
    let cursor = options?.from ?? "now";
    for (;;) {
      if (options?.signal?.aborted) return;
      const ev = await this.wait({
        cursor,
        timeoutMs: 25000,
        signal: options?.signal,
      });
      cursor = ev.cursor;
      if (ev.invalidated) yield ev;
    }
  }
}
