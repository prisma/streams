// A product-surface fake for the harness tests (oracle.test.ts,
// tenants.test.ts, hostile.test.ts): just enough of /v1/streams to drive
// k2gen's modes per project, with an injection hook for each failure the
// harness must see. Never used outside tests.
//
// - Bearer tokens map to projects (`tokens`); an unknown token is 401
//   `unauthorized`. Each project has its own stream namespace.
// - Records are kept as the exact JSON text appended, and a page is
//   "[" + records joined by "," + "]", so the readers' byte counts are
//   exact. Cursors are key cursors in the server's shape (kind 0x12, the
//   offset at byte 37, audit.ts cursorOffset).
// - GET …/records, :long-poll, :sse, consumers (PUT, :pull, :settle; a
//   pull with nothing to lease waits up to its waitMs for an append),
//   POST /v1/projects/{p}/live:wait (tails from `now` pinned at once, else
//   the first ready tail pages and group leases, waiting up to waitMs for
//   an append), GET /v1/streams (catalog), DELETE, GET /v1/projects/{p}/usage (meters
//   counted the way the generators count them) and the watch capability
//   path (403 `watch_unauthorized`; each request recorded in `watches`,
//   `watchHold` holds every one that arrives while it is set, and
//   `watchPeak` is the most answered at once).
// - The server's descriptor cache, per (project, name): a create fills the
//   slot, and any authenticated request naming the stream fills it again
//   once it is `slotTtlMs` old (a hit on a fresh slot does not renew it); a
//   delete empties it. A capability on a fresh slot (the stream's or its
//   absence's) is refused at once; on an expired slot or none the cell
//   takes a store fetch (`storeMs`, the same for a stream and a name nobody
//   holds, as s3lite's fixed sleep) and fills nothing.
// - `refuse(project, n, status, code)` answers the project's next n
//   requests with that status (code null: an untyped body); `plant` puts a
//   record into a project's stream without metering it (a leak).
// - `delayMs` answers every authenticated request that much later (a slow
//   rig); `maxStreams` refuses a project's create past that many streams
//   (429 `stream_limit`, retry-after 0); `commitThenAnswer(project, n,
//   status, code)` stores and meters the project's next n appends and then
//   answers them with that status (an append whose outcome the client
//   cannot know: the server wrote it).

export interface Meters {
  ingestRecords: number;
  ingestPayloadBytes: number;
  readRecords: number;
  readPayloadBytes: number;
}

interface Refusal {
  left: number;
  status: number;
  code: string | null;
  /** The retry-after header seconds, when set. */
  retryAfter?: number;
}

function cursorOf(offset: number): string {
  const b = Buffer.alloc(45);
  b[0] = 0x12;
  b.writeBigUInt64LE(BigInt(offset), 37);
  return b.toString("base64url");
}

function offsetOf(cursor: string, end: number): number {
  if (cursor === "beginning") return 0;
  if (cursor === "now") return end;
  const b = Buffer.from(cursor, "base64url");
  if (b.length < 45) return -1;
  return Number(b.readBigUInt64LE(37));
}

const json = (status: number, body: unknown, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });
const error = (status: number, code: string) => json(status, { error: { code, message: code } });

export class FakeCell {
  server: ReturnType<typeof Bun.serve>;
  readonly base: string;
  tokens = new Map<string, string>();
  streams = new Map<string, Map<string, string[]>>();
  meters = new Map<string, Meters>();
  refusals = new Map<string, Refusal>();
  requests = new Map<string, number>();
  /** Pages are metered when rendered and sent this many ms later (a slow
   * transfer): a reader that abandons one leaves it metered, uncounted. */
  pageDelayMs = 0;
  groups = new Map<string, number>();
  /** A descriptor slot's freshness (the server's is 5 s). */
  slotTtlMs = 5000;
  /** A store fetch's latency. */
  storeMs = 0;
  /** A defect to detect: an expired slot refuses a capability at once, as
   * a fresh one does, where the server takes the store fetch it takes for
   * a name with no slot. */
  expiredAnsweredFree = false;
  /** A defect to detect: a read of a name another project holds is
   * refused instead of answering like a name nobody holds. */
  existenceLeak = false;
  /** When set, every authenticated request as [project, method, path,
   * when it arrived (performance.now())]. */
  trace: Array<[string, string, string, number]> | null = null;
  /** Every watch request, in arrival order: when it arrived
   * (performance.now()), the project its capability claims, the name,
   * whether that project holds the name, and whether the cell took a
   * store fetch to answer it. */
  watches: Array<{ t: number; project: string; name: string; held: boolean; store: boolean }> = [];
  /** While set, every watch request waits for it before it is answered:
   * a stall the timed pairs must not catch up after. */
  watchHold: Promise<void> | null = null;
  /** Watch requests being answered now, and the most at once. */
  watchInflight = 0;
  watchPeak = 0;
  /** Every authenticated request is answered this many ms late. */
  delayMs = 0;
  /** A project's stream limit: a create past it is refused. */
  maxStreams = new Map<string, number>();
  /** Appends stored and metered, then answered with an error. */
  committedRefusals = new Map<string, Refusal>();
  private waiters = new Set<() => void>();
  /** When each (project, name) slot was last filled (performance.now()). */
  private slots = new Map<string, number>();

  constructor() {
    this.server = Bun.serve({ port: 0, hostname: "127.0.0.1", idleTimeout: 0, fetch: (req) => this.handle(req) });
    this.base = `http://127.0.0.1:${this.server.port}`;
  }
  stop(): void {
    for (const w of this.waiters) w();
    this.server.stop(true);
  }
  meter(project: string): Meters {
    let m = this.meters.get(project);
    if (!m) this.meters.set(project, (m = { ingestRecords: 0, ingestPayloadBytes: 0, readRecords: 0, readPayloadBytes: 0 }));
    return m;
  }
  ns(project: string): Map<string, string[]> {
    let n = this.streams.get(project);
    if (!n) this.streams.set(project, (n = new Map()));
    return n;
  }
  /** A read through the cache: fills the slot when it is absent or
   * expired; a fresh slot stays as it is. */
  private fill(project: string, name: string): void {
    const k = `${project} ${name}`;
    const at = this.slots.get(k);
    if (at === undefined || performance.now() - at >= this.slotTtlMs) this.slots.set(k, performance.now());
  }
  refuse(project: string, n: number, status: number, code: string | null, retryAfter?: number): void {
    this.refusals.set(project, { left: n, status, code, retryAfter });
  }
  commitThenAnswer(project: string, n: number, status: number, code: string | null): void {
    this.committedRefusals.set(project, { left: n, status, code });
  }
  plant(project: string, stream: string, record: string): void {
    this.ns(project).get(stream)!.push(record);
    this.wake();
  }
  private wake(): void {
    const ws = [...this.waiters];
    this.waiters.clear();
    for (const w of ws) w();
  }
  private waitAppend(ms: number, signal: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
      const done = () => {
        clearTimeout(t);
        this.waiters.delete(done);
        resolve();
      };
      const t = setTimeout(done, ms);
      this.waiters.add(done);
      signal.addEventListener("abort", done, { once: true });
    });
  }

  private page(project: string, recs: string[], from: number, maxBytes: number): Response {
    const out: string[] = [];
    let bytes = 0;
    for (let i = from; i < recs.length; i++) {
      if (out.length && bytes + recs[i]!.length > maxBytes) break;
      out.push(recs[i]!);
      bytes += recs[i]!.length;
    }
    const next = from + out.length;
    const m = this.meter(project);
    m.readRecords += out.length;
    m.readPayloadBytes += bytes;
    return new Response(`[${out.join(",")}]`, {
      status: 200,
      headers: {
        "content-type": "application/json",
        "streams-next-cursor": cursorOf(next),
        "streams-up-to-date": String(next === recs.length),
      },
    });
  }

  private sse(project: string, recs: string[], from: number, signal: AbortSignal): Response {
    const enc = new TextEncoder();
    const meter = this.meter(project);
    let at = from;
    const body = new ReadableStream<Uint8Array>({
      start: async (c) => {
        try {
          c.enqueue(enc.encode(`event: control\ndata:{"nextCursor":"${cursorOf(at)}"}\n\n`));
          while (!signal.aborted) {
            while (at < recs.length) {
              const r = recs[at++]!;
              meter.readRecords++;
              meter.readPayloadBytes += r.length;
              c.enqueue(enc.encode(`event: data\ndata:[${r}]\n\nevent: control\ndata:{"nextCursor":"${cursorOf(at)}"}\n\n`));
            }
            await this.waitAppend(1000, signal);
          }
        } catch {
          // the client went away
        }
        try {
          c.close();
        } catch {
          // already closed
        }
      },
    });
    return new Response(body, { headers: { "content-type": "text/event-stream" } });
  }

  private async handle(req: Request): Promise<Response> {
    const url = new URL(req.url);
    const path = url.pathname;
    const watch = /^\/v1\/streams\/(.+)\/watches\/[^/]+\/keys\/[^/]+$/.exec(path);
    if (watch) {
      // The anonymous capability path: one uniform 403, at once on a
      // fresh slot, after a store fetch on an expired slot or none.
      const name = watch[1]!.split("/").map(decodeURIComponent).join("/");
      const cap = url.searchParams.get("cap") ?? "";
      const project = cap.split(".")[0] ?? "";
      const at = this.slots.get(`${project} ${name}`);
      const free = at !== undefined && (performance.now() - at < this.slotTtlMs || this.expiredAnsweredFree);
      this.watches.push({ t: performance.now(), project, name, held: this.ns(project).has(name), store: !free });
      this.watchPeak = Math.max(this.watchPeak, ++this.watchInflight);
      try {
        if (this.watchHold) await this.watchHold;
        if (!free && this.storeMs > 0) await Bun.sleep(this.storeMs);
      } finally {
        this.watchInflight--;
      }
      return error(403, "watch_unauthorized");
    }
    const token = (req.headers.get("authorization") ?? "").replace(/^Bearer /, "");
    const project = this.tokens.get(token);
    if (!project) return error(401, "unauthorized");
    this.requests.set(project, (this.requests.get(project) ?? 0) + 1);
    this.trace?.push([project, req.method, path, performance.now()]);
    if (this.delayMs > 0) await Bun.sleep(this.delayMs);
    const refusal = this.refusals.get(project);
    if (refusal && refusal.left > 0) {
      refusal.left--;
      if (refusal.code && refusal.retryAfter !== undefined) {
        return json(refusal.status, { error: { code: refusal.code, message: refusal.code } }, { "retry-after": String(refusal.retryAfter) });
      }
      return refusal.code ? error(refusal.status, refusal.code) : new Response("upstream gone", { status: refusal.status });
    }
    const live = /^\/v1\/projects\/([^/]+)\/live:wait$/.exec(path);
    if (live) return live[1] === project ? this.liveWait(project, req) : error(404, "unknown_project");
    const usage = /^\/v1\/projects\/([^/]+)\/usage$/.exec(path);
    if (usage) return usage[1] === project ? json(200, { ...this.meter(project) }) : error(403, "forbidden");
    const ns = this.ns(project);
    if (path === "/v1/streams" && req.method === "GET") {
      return json(200, { streams: [...ns.keys()].sort().map((name) => ({ name })) });
    }
    const m = /^\/v1\/streams\/(.+?)(\/records(?::batch|:long-poll|:sse)?|\/consumers\/[^/]+(?::pull|:settle)?)?$/.exec(path);
    if (!m) return error(404, "not_found");
    const name = m[1]!.split("/").map(decodeURIComponent).join("/");
    const op = m[2] ?? "";
    if (op === "" && req.method === "DELETE") {
      this.slots.delete(`${project} ${name}`);
      return ns.delete(name) ? new Response(null, { status: 204 }) : error(404, "stream_not_found");
    }
    if (op === "" && req.method === "PUT" && !ns.has(name) && ns.size >= (this.maxStreams.get(project) ?? Infinity)) {
      return json(429, { error: { code: "stream_limit", message: "stream_limit" } }, { "retry-after": "0" });
    }
    if (op === "" && req.method === "PUT" && !ns.has(name)) {
      ns.set(name, []);
      this.slots.set(`${project} ${name}`, performance.now());
      return json(201, { name });
    }
    this.fill(project, name);
    if (op === "") {
      if (req.method === "PUT") return json(200, { name });
      if (req.method === "GET") return ns.has(name) ? json(200, { name, contentType: "application/json" }) : error(404, "stream_not_found");
    }
    const recs = ns.get(name);
    if (!recs && this.existenceLeak && [...this.streams.values()].some((n) => n.has(name))) return error(403, "forbidden");
    if (!recs) return error(404, "stream_not_found");
    const meter = this.meter(project);
    if (req.method === "POST" && op.startsWith("/records")) {
      const text = await req.text();
      const batch: string[] = op === "/records:batch" ? splitTop(text) : [text];
      for (const r of batch) {
        recs.push(r);
        meter.ingestRecords++;
        meter.ingestPayloadBytes += r.length;
      }
      this.wake();
      const after = this.committedRefusals.get(project);
      if (after && after.left > 0) {
        after.left--;
        return after.code ? error(after.status, after.code) : new Response("upstream gone", { status: after.status });
      }
      return json(200, { count: batch.length });
    }
    if (op.startsWith("/consumers/")) return this.consumer(project, name, op, recs, req);
    const from = offsetOf(url.searchParams.get("cursor") ?? "beginning", recs.length);
    if (from < 0 || from > recs.length) return error(400, "invalid_cursor");
    const maxBytes = Number(url.searchParams.get("maxBytes") ?? 1 << 20);
    if (op === "/records:sse") return this.sse(project, recs, from, req.signal);
    if (op === "/records:long-poll" && from === recs.length) {
      await this.waitAppend(Number(url.searchParams.get("waitMs") ?? 1000), req.signal);
      if (from === recs.length) {
        return new Response(null, { status: 204, headers: { "streams-next-cursor": cursorOf(from), "streams-up-to-date": "true" } });
      }
    }
    const page = this.page(project, recs, from, maxBytes);
    if (this.pageDelayMs > 0) await Bun.sleep(this.pageDelayMs);
    return page;
  }

  /** The multiplexed wait: every ready subscription's entry (a tail from
   * `now` pins its cursor, one behind the tail pages; a group leases what
   * is past its consumer's position), or, with none ready, the same after
   * the first append within waitMs. */
  private async liveWait(project: string, req: Request): Promise<Response> {
    type Sub = { id: string; kind: string; stream: string; cursor?: string; group?: string; max?: number };
    const body = (await req.json()) as { waitMs?: number; subs: Sub[] };
    const ns = this.ns(project);
    const ready = (): unknown[] => {
      const out: unknown[] = [];
      for (const sub of body.subs) {
        const recs = ns.get(sub.stream);
        if (!recs) {
          out.push({ id: sub.id, status: 404, error: { code: "stream_not_found", message: "stream_not_found", retryable: false } });
          continue;
        }
        if (sub.kind === "tail") {
          const from = offsetOf(sub.cursor ?? "beginning", recs.length);
          if (sub.cursor === "now" || from < recs.length) {
            const page = recs.slice(from);
            const m = this.meter(project);
            m.readRecords += page.length;
            m.readPayloadBytes += page.reduce((n, r) => n + r.length, 0);
            out.push({ id: sub.id, records: page.map((r) => JSON.parse(r)), cursor: cursorOf(recs.length), upToDate: true, sealed: false });
          }
          continue;
        }
        const key = `${project}/${sub.stream}/${sub.group}`;
        const at = this.groups.get(key) ?? 0;
        if (at >= recs.length) continue;
        const leased = recs.slice(at, at + (sub.max ?? 10));
        this.groups.set(key, at + leased.length);
        const m = this.meter(project);
        const messages = leased.map((r, i) => {
          const value = JSON.parse(r);
          m.readRecords++;
          m.readPayloadBytes += Buffer.byteLength(JSON.stringify(value));
          return { id: `${at + i}`, routingKey: "", leaseToken: `${key}:${at + i}`, attempts: 1, value };
        });
        out.push({ id: sub.id, backlog: recs.length - at - leased.length, messages });
      }
      return out;
    };
    let results = ready();
    if (results.length === 0) {
      await this.waitAppend(Math.min(body.waitMs ?? 25_000, 25_000), req.signal);
      results = ready();
    }
    return json(200, { results });
  }

  private async consumer(project: string, name: string, op: string, recs: string[], req: Request): Promise<Response> {
    const key = `${project}/${name}/${op.split("/")[2]!.split(":")[0]}`;
    if (op.endsWith(":settle")) {
      const body = (await req.json()) as { acks?: unknown[] };
      return json(200, { acked: body.acks?.length ?? 0, retried: 0, extended: 0, dlq: 0, stale: 0 });
    }
    if (!op.endsWith(":pull")) return json(201, {});
    const { max, waitMs } = (await req.json()) as { max: number; waitMs?: number };
    // Nothing to lease: wait up to waitMs for an append, as the server does.
    if ((this.groups.get(key) ?? 0) >= recs.length && waitMs) await this.waitAppend(waitMs, req.signal);
    const at = this.groups.get(key) ?? 0;
    const out = recs.slice(at, at + max);
    this.groups.set(key, at + out.length);
    const meter = this.meter(project);
    const messages = out.map((r, i) => {
      const value = JSON.parse(r);
      meter.readRecords++;
      meter.readPayloadBytes += Buffer.byteLength(JSON.stringify(value));
      return { leaseToken: `${key}:${at + i}`, attempts: 1, value };
    });
    return json(200, { messages, backlog: recs.length - at - out.length });
  }
}

/** The raw text of each element of a JSON array. */
function splitTop(text: string): string[] {
  const out: string[] = [];
  let depth = 0, inStr = false, esc = false, start = 1;
  for (let i = 1; i < text.length; i++) {
    const c = text[i]!;
    if (inStr) {
      if (esc) esc = false;
      else if (c === "\\") esc = true;
      else if (c === '"') inStr = false;
      continue;
    }
    if (c === '"') inStr = true;
    else if (c === "{" || c === "[") depth++;
    else if ((c === "}" || c === "]") && depth > 0) depth--;
    else if ((c === "," && depth === 0) || (c === "]" && depth === 0)) {
      out.push(text.slice(start, i));
      start = i + 1;
    }
  }
  return out;
}
