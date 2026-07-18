/**
 * Durable Streams TypeScript SDK (ergonomics preview).
 *
 * Zero dependencies; works in Bun, Node 18+, Deno, and browsers. The stream
 * key is passed per request (the server holds no key state); reads return
 * server-decrypted payloads.
 *
 *   const client = new StreamsClient({ url: "http://127.0.0.1:8090", key });
 *   const stream = await client.create("orders", { contentType: "application/json" });
 *   await stream.append({ orderId: 1 }, { key: "customer-42" });
 *   for await (const msg of stream.tail({ key: "customer-42" })) { ... }
 *
 *   const q = await client.create("jobs", { profile: "queue" });
 *   await q.queue("worker").consume(async (msg) => { ... }); // CF-style
 */

export interface ClientOptions {
  url: string;
  /** base64url 32-byte stream encryption key (streams-keys generate). */
  key: string;
  fetch?: typeof fetch;
}

export interface CreateOptions {
  contentType?: string;
  ttlSeconds?: number;
  profile?: "generic" | "state-protocol" | "queue";
  ordering?: "total" | "per-key";
  segments?: number;
  queueMaxDeliveries?: number;
}

export interface AppendOptions {
  /** Routing key (per-key ordering partition / FIFO group / live watch key). */
  key?: string;
  /** Optimistic writer coordination (lexicographic). */
  seq?: string;
  close?: boolean;
}

export interface ReadOptions {
  offset?: string;
  key?: string;
  live?: "long-poll";
  timeout?: string;
}

export class StreamsClient {
  constructor(private opts: ClientOptions) {}

  private get f() {
    return this.opts.fetch ?? fetch;
  }

  headers(extra: Record<string, string> = {}): Record<string, string> {
    return { "stream-encryption-key": this.opts.key, ...extra };
  }

  stream(name: string): DurableStream {
    return new DurableStream(this, this.opts.url, name);
  }

  async create(name: string, o: CreateOptions = {}): Promise<DurableStream> {
    const h = this.headers();
    if (o.contentType) h["content-type"] = o.contentType;
    if (o.ttlSeconds != null) h["stream-ttl"] = String(o.ttlSeconds);
    if (o.profile) h["stream-profile"] = o.profile;
    if (o.ordering === "per-key") {
      h["stream-ordering"] = "per-key";
      if (o.segments) h["stream-segments"] = String(o.segments);
    }
    if (o.queueMaxDeliveries != null)
      h["stream-queue-max-deliveries"] = String(o.queueMaxDeliveries);
    const r = await this.f(`${this.opts.url}/v1/stream/${name}`, {
      method: "PUT",
      headers: h,
    });
    if (!r.ok) throw new StreamsError(`create ${name}`, r);
    return this.stream(name);
  }

  async fetchRaw(path: string, init: RequestInit = {}): Promise<Response> {
    return this.f(`${this.opts.url}${path}`, {
      ...init,
      headers: { ...this.headers(), ...(init.headers as Record<string, string>) },
    });
  }
}

export class StreamsError extends Error {
  constructor(op: string, public response: Response) {
    super(`${op}: HTTP ${response.status}`);
  }
}

export interface Appended {
  nextOffset: string;
}

export class DurableStream {
  constructor(
    private client: StreamsClient,
    private url: string,
    public readonly name: string,
  ) {}

  private path(sub = ""): string {
    return `/v1/stream/${this.name}${sub}`;
  }

  /** Append one value (JSON streams) or raw bytes; returns the new cursor. */
  async append(
    value: unknown | Uint8Array,
    o: AppendOptions = {},
  ): Promise<Appended> {
    const isBytes = value instanceof Uint8Array;
    const headers: Record<string, string> = {
      "content-type": isBytes ? "application/octet-stream" : "application/json",
    };
    if (o.key) headers["stream-key"] = o.key;
    if (o.seq) headers["stream-seq"] = o.seq;
    if (o.close) headers["stream-closed"] = "true";
    const body = isBytes ? (value as Uint8Array) : JSON.stringify([value]);
    const r = await this.client.fetchRaw(this.path(), {
      method: "POST",
      headers,
      body: body as BodyInit,
    });
    if (!r.ok) throw new StreamsError(`append ${this.name}`, r);
    return { nextOffset: r.headers.get("stream-next-offset") ?? "" };
  }

  /** One catch-up read page. JSON streams give parsed messages. */
  async read(o: ReadOptions = {}): Promise<{
    messages: unknown[];
    nextOffset: string;
    upToDate: boolean;
    closed: boolean;
    raw: Response;
  }> {
    const qs = new URLSearchParams();
    if (o.offset) qs.set("offset", o.offset);
    if (o.key) qs.set("key", o.key);
    if (o.live) qs.set("live", o.live);
    if (o.timeout) qs.set("timeout", o.timeout);
    const r = await this.client.fetchRaw(`${this.path()}?${qs}`);
    if (!r.ok && r.status !== 204) throw new StreamsError(`read ${this.name}`, r);
    const ct = r.headers.get("content-type") ?? "";
    const messages =
      r.status === 204
        ? []
        : ct.startsWith("application/json")
          ? ((await r.json()) as unknown[])
          : [new Uint8Array(await r.arrayBuffer())];
    return {
      messages,
      nextOffset: r.headers.get("stream-next-offset") ?? o.offset ?? "-1",
      upToDate: r.headers.get("stream-up-to-date") === "true",
      closed: r.headers.get("stream-closed") === "true",
      raw: r,
    };
  }

  /** Follow the stream (or one routing key) forever via long-poll. */
  async *tail(o: { offset?: string; key?: string } = {}): AsyncGenerator<unknown> {
    let offset = o.offset ?? "-1";
    let live = false;
    for (;;) {
      const page = await this.read({
        offset,
        key: o.key,
        live: live ? "long-poll" : undefined,
        timeout: "20s",
      });
      for (const m of page.messages) yield m;
      offset = page.nextOffset;
      if (page.upToDate) live = true;
      if (page.closed && page.upToDate) return;
    }
  }

  queue(consumer: string): QueueConsumer {
    return new QueueConsumer(this.client, this.path(), consumer);
  }
}

// ---- queue profile (Cloudflare-Queues-style pull consumers) ----

export interface QueueMessage {
  id: string;
  offset: number;
  attempts: number;
  body: unknown;
  /** Settle this message successfully. */
  ack(): void;
  /** Redeliver after an optional delay. */
  retry(o?: { delayMs?: number }): void;
}

export interface ReceiveOptions {
  batchSize?: number;
  visibilityMs?: number;
  waitMs?: number;
}

export class QueueConsumer {
  constructor(
    private client: StreamsClient,
    private streamPath: string,
    public readonly consumer: string,
  ) {}

  private qpath(verb: string): string {
    return `${this.streamPath}/queue/${this.consumer}/${verb}`;
  }

  /** Pull one batch. Call batch.settle() after handling. */
  async receive(o: ReceiveOptions = {}): Promise<QueueBatch> {
    const r = await this.client.fetchRaw(this.qpath("receive"), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(o),
    });
    if (!r.ok) throw new StreamsError("queue receive", r);
    const data = (await r.json()) as {
      messages: Array<{
        id: string;
        offset: number;
        attempts: number;
        leaseToken: string;
        body: unknown;
      }>;
      backlog: number;
    };
    return new QueueBatch(this, data.messages, data.backlog);
  }

  async settle(body: {
    acks?: { leaseToken: string }[];
    retries?: { leaseToken: string; delayMs?: number }[];
    extends?: { leaseToken: string; visibilityMs?: number }[];
  }): Promise<{ acked: number; retried: number; dlq: number; backlog: number }> {
    const r = await this.client.fetchRaw(this.qpath("ack"), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!r.ok) throw new StreamsError("queue settle", r);
    return (await r.json()) as never;
  }

  /**
   * Cloudflare-style consume loop: pulls batches and calls the handler per
   * message. Unhandled = auto-ack; throw = retry (with backoff delay);
   * message.retry()/ack() override. Runs until `signal` aborts.
   */
  async consume(
    handler: (msg: QueueMessage) => Promise<void> | void,
    o: ReceiveOptions & { signal?: AbortSignal; retryDelayMs?: number } = {},
  ): Promise<void> {
    for (;;) {
      if (o.signal?.aborted) return;
      const batch = await this.receive({
        batchSize: o.batchSize ?? 5,
        visibilityMs: o.visibilityMs ?? 30_000,
        waitMs: o.waitMs ?? 20_000,
      });
      for (const msg of batch.messages) {
        try {
          await handler(msg);
        } catch {
          msg.retry({ delayMs: o.retryDelayMs ?? 1_000 });
        }
      }
      await batch.settle();
    }
  }
}

export class QueueBatch {
  readonly messages: QueueMessage[];
  private acks = new Set<string>();
  private retries = new Map<string, number>();

  constructor(
    private consumer: QueueConsumer,
    raw: Array<{ id: string; offset: number; attempts: number; leaseToken: string; body: unknown }>,
    public readonly backlog: number,
  ) {
    this.messages = raw.map((m) => ({
      id: m.id,
      offset: m.offset,
      attempts: m.attempts,
      body: m.body,
      ack: () => {
        this.retries.delete(m.leaseToken);
        this.acks.add(m.leaseToken);
      },
      retry: (o?: { delayMs?: number }) => {
        this.acks.delete(m.leaseToken);
        this.retries.set(m.leaseToken, o?.delayMs ?? 0);
      },
    }));
    // Default disposition: ack everything not explicitly retried.
    for (const m of raw) this.acks.add(m.leaseToken);
  }

  /** Flush dispositions in one round trip (acks + retries combined). */
  async settle() {
    if (this.acks.size === 0 && this.retries.size === 0) return;
    const body = {
      acks: [...this.acks]
        .filter((t) => !this.retries.has(t))
        .map((leaseToken) => ({ leaseToken })),
      retries: [...this.retries].map(([leaseToken, delayMs]) => ({ leaseToken, delayMs })),
    };
    this.acks.clear();
    this.retries.clear();
    return this.consumer.settle(body);
  }
}
