// The drivers' bounded keep-alive pools (conn-pool.ts): an open-loop
// producer whose answers are slow sends on at most its pool's sockets and
// queues the rest as its own backlog, so it cannot churn the host's
// ephemeral ports (Layer B rehearsal of 2026-10-19: h1/h1k at 400 req/s
// with thousands in flight ran a macOS host out of ports). The server
// counts the distinct client ports it saw: one per connection. Run with
// `bun test ./bench/k2cost`.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { HeaderSource, Http, Meter, Opts, deadlineSignal } from "./common.ts";
import { ConnPool } from "./conn-pool.ts";
import { produce, type Ctx } from "./produce.ts";

const ports = new Set<number>();
let delayMs = 0;
let srv: ReturnType<typeof Bun.serve>;
beforeAll(() => {
  srv = Bun.serve({
    port: 0,
    async fetch(req, s) {
      ports.add(s.requestIP(req)!.port);
      await req.arrayBuffer();
      await Bun.sleep(delayMs);
      return Response.json({ count: 1 });
    },
  });
});
afterAll(() => srv.stop(true));

function ctx(pool: ConnPool, flags: Record<string, string>): Ctx {
  const meter = new Meter();
  const hdrs = new HeaderSource(undefined, { authorization: "Bearer t" });
  const http = new Http(`http://127.0.0.1:${srv.port}`, hdrs, meter, pool);
  return { http, meter, o: new Opts(new Map(Object.entries(flags))), prefix: "t/", streams: 4, seed: 7, duration: 2 };
}

test("an open-loop producer with hundreds in flight sends on at most its pool's sockets, the rest its own backlog", async () => {
  ports.clear();
  delayMs = 300;
  const pool = new ConnPool(32);
  const c = ctx(pool, { rate: "400", "record-bytes": "128", "drain-secs": "30" });
  // 1 s at 400 req/s against 300 ms answers: about 120 in flight if
  // unbounded, more than fetch's 64 idle sockets keep; 32 sockets serve
  // about 107 a second, so most arrivals wait for one.
  await produce(c, deadlineSignal(1));
  const t = c.meter.tot;
  const sent = t.get("requests");
  expect(sent).toBeGreaterThan(100);
  expect(ports.size).toBeLessThanOrEqual(32);
  // Every request was answered by the server; the wait for a socket is
  // the driver's own (pool_waits), never a status or a transport error.
  expect([t.status, t.get("transport_errors"), t.get("acked_requests")]).toEqual([{ "200": sent }, 0, sent]);
  expect(t.get("pool_waits")).toBeGreaterThan(0);
  expect(pool.summary()).toMatchObject({ sockets: 32, peak_in_use: 32, unsent: 0 });
  expect(pool.summary().peak_waiting).toBeGreaterThan(32);
}, 30_000);

test("a request whose caller gives up while it waits for a socket is never sent, and counted as the driver's", async () => {
  ports.clear();
  delayMs = 300;
  const pool = new ConnPool(1);
  const c = ctx(pool, {});
  const first = c.http.call("GET", "/v1/streams");
  const stop = new AbortController();
  const second = c.http.call("GET", "/v1/streams", { signal: stop.signal });
  stop.abort();
  const [a, b] = await Promise.all([first, second]);
  const t = c.meter.tot;
  expect([a.status, b.status, t.get("requests"), t.get("pool_unsent"), t.status, ports.size]).toEqual([200, 0, 1, 1, { "200": 1 }, 1]);
  expect(b.error).toStartWith("unsent:");
  expect(pool.summary()).toMatchObject({ unsent: 1, waits: 0 });
});

test("a pool keeps its sockets open between requests: sequential requests use one connection", async () => {
  ports.clear();
  delayMs = 0;
  const c = ctx(new ConnPool(4), {});
  for (let i = 0; i < 50; i++) expect((await c.http.call("GET", "/v1/streams")).status).toBe(200);
  expect(ports.size).toBe(1);
});
