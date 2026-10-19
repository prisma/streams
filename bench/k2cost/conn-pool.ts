// A bounded keep-alive connection pool for the drivers' short requests
// (Http.call in common.ts; hostile.ts's anonymous h11t pairs).
//
// Bun's fetch keeps at most 64 idle keep-alive sockets per process and
// closes every other socket when its answer ends. A driver with more
// requests in flight than that opens a new connection for most requests,
// and each close leaves a TIME_WAIT on a local port: on macOS (16,383
// ephemeral ports, 30 s TIME_WAIT) about 546 new connections a second
// exhaust the ports (EADDRNOTAVAIL for every class and the scrape;
// Layer B rehearsal of 2026-10-19, h1/h1k at 400 req/s with thousands in
// flight). A pool sends on at most `size` sockets of its own keep-alive
// agent and keeps them all open between requests; a request that finds
// every socket busy waits for one in the pool's FIFO, which is the
// driver's own backlog, counted by the pool and never a server answer.
// SSE subscriptions (Http.open) stay on fetch: each holds its connection
// for as long as it lives, so they do not churn ports.

import http from "node:http";
import https from "node:https";

export interface Sent {
  status: number;
  headers: Headers;
  body: Uint8Array;
}

export class ConnPool {
  private readonly agents = new Map<string, http.Agent>();
  private inUse = 0;
  private readonly waiting: Array<() => void> = [];
  /** Requests that waited for a socket, and the longest and summed waits. */
  waits = 0;
  waitMsMax = 0;
  waitMsTotal = 0;
  /** Requests whose caller gave up while they waited: never sent. */
  unsent = 0;
  peakInUse = 0;
  peakWaiting = 0;
  constructor(readonly size: number) {
    if (!(Number.isInteger(size) && size >= 1)) throw new Error(`a connection pool needs at least one socket (${size})`);
  }

  /** Waits for a free socket in arrival order: the wait in ms (0 when one
   * was free), or null when `signal` fired first (the request must not be
   * sent). A non-null answer holds a socket until release(). */
  async acquire(signal?: AbortSignal): Promise<number | null> {
    if (signal?.aborted) return this.giveUp();
    if (this.inUse < this.size && this.waiting.length === 0) {
      this.take();
      return 0;
    }
    const t0 = performance.now();
    const got = await new Promise<boolean>((resolve) => {
      const wake = () => {
        signal?.removeEventListener("abort", quit);
        resolve(true);
      };
      const quit = () => {
        const i = this.waiting.indexOf(wake);
        if (i >= 0) this.waiting.splice(i, 1);
        resolve(false);
      };
      this.waiting.push(wake);
      if (this.waiting.length > this.peakWaiting) this.peakWaiting = this.waiting.length;
      signal?.addEventListener("abort", quit, { once: true });
    });
    if (!got) return this.giveUp();
    const ms = performance.now() - t0;
    this.waits++;
    this.waitMsTotal += ms;
    if (ms > this.waitMsMax) this.waitMsMax = ms;
    return ms;
  }

  /** Frees the socket acquire() held; the next waiter takes it at once. */
  release(): void {
    const next = this.waiting.shift();
    if (next) next();
    else this.inUse--;
  }

  /** One request on one of the pool's keep-alive sockets; the caller holds
   * a slot (acquire). Rejects on a transport failure or when `signal`
   * fires (the socket is then destroyed, never reused). */
  send(url: string, method: string, headers: Record<string, string>, body: Uint8Array | string | undefined, signal: AbortSignal): Promise<Sent> {
    const u = new URL(url);
    const payload = body === undefined ? undefined : typeof body === "string" ? Buffer.from(body) : body;
    const hdrs = payload === undefined ? headers : { ...headers, "content-length": String(payload.byteLength) };
    return new Promise((resolve, reject) => {
      const lib = u.protocol === "https:" ? https : http;
      const req = lib.request(u, { method, headers: hdrs, agent: this.agent(u.protocol), signal }, (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (d: Buffer) => chunks.push(d));
        res.on("error", reject);
        res.on("end", () => {
          const h = new Headers();
          for (let i = 0; i + 1 < res.rawHeaders.length; i += 2) h.append(res.rawHeaders[i]!, res.rawHeaders[i + 1]!);
          resolve({ status: res.statusCode ?? 0, headers: h, body: Buffer.concat(chunks) });
        });
      });
      req.on("error", reject);
      req.end(payload);
    });
  }

  /** What the run ledger records for this pool. */
  summary(): Record<string, number> {
    return {
      sockets: this.size, peak_in_use: this.peakInUse, peak_waiting: this.peakWaiting, waits: this.waits,
      wait_ms_max: Math.round(this.waitMsMax), wait_ms_total: Math.round(this.waitMsTotal), unsent: this.unsent,
    };
  }

  private take(): void {
    this.inUse++;
    if (this.inUse > this.peakInUse) this.peakInUse = this.inUse;
  }

  private giveUp(): null {
    this.unsent++;
    return null;
  }

  private agent(protocol: string): http.Agent {
    let a = this.agents.get(protocol);
    if (!a) {
      const opts = { keepAlive: true, maxSockets: this.size, maxFreeSockets: this.size };
      a = protocol === "https:" ? new https.Agent(opts) : new http.Agent(opts);
      this.agents.set(protocol, a);
    }
    return a;
  }
}

/** Pools by key (a tenants class, or a run-level leg), each `size` sockets. */
export class Pools {
  private readonly m = new Map<string, ConnPool>();
  constructor(readonly size: number) {}
  get(key: string): ConnPool {
    let p = this.m.get(key);
    if (!p) this.m.set(key, (p = new ConnPool(this.size)));
    return p;
  }
  summary(): Record<string, Record<string, number>> {
    return Object.fromEntries([...this.m.entries()].sort(([a], [b]) => (a < b ? -1 : 1)).map(([k, p]) => [k, p.summary()]));
  }
}
