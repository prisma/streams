// The edge arm's probe (deploy-q5 triage B6; design §3.5): each mode run
// against a local raw-HTTP origin that plays the Compute edge's behaviours
// on demand (an idle close, a stale-upstream 502, a connection wall, a
// first-byte deadline), and the report that turns their records into facts.
// Run with `bun test ./bench/edgeprobe`.
import { afterAll, afterEach, beforeAll, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer, type Server, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

const PROBE = join(import.meta.dir, "edgeprobe.ts");
const dir = mkdtempSync(join(tmpdir(), "edgeprobe-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

/// capMs: the binary caps every long-poll's wait (25 s; wait_tail).
type Mode = { idleCloseMs?: number; staleAfterMs?: number; wall?: number; deadlineMs?: number; capMs?: number };

/// A raw HTTP/1.1 origin: /health; GET …/records:sse (chunked, a control
/// event, then a data event per append); GET …/records:long-poll (204 after
/// waitMs, or 200 at the next append); POST …/records. `mode` plays the edge.
/// It marks its answers as the binary does: x-streams-boot-id on /health
/// only, the product CORS header on every /v1/streams answer.
class Origin {
  server: Server;
  port = 0;
  open = new Set<Socket>();
  sse = new Set<Socket>();
  polls = new Set<() => void>();
  constructor(public mode: Mode = {}) {
    this.server = createServer((s) => this.accept(s));
  }
  async start(): Promise<void> {
    await new Promise<void>((r) => this.server.listen(0, "127.0.0.1", r));
    this.port = (this.server.address() as { port: number }).port;
  }
  get url(): string {
    return `http://127.0.0.1:${this.port}`;
  }
  async stop(): Promise<void> {
    for (const s of this.open) s.destroy();
    await new Promise<void>((r) => this.server.close(() => r()));
  }
  accept(s: Socket): void {
    if (this.mode.wall !== undefined && this.open.size >= this.mode.wall) {
      s.destroy();
      return;
    }
    this.open.add(s);
    s.on("close", () => (this.open.delete(s), this.sse.delete(s)));
    s.on("error", () => {});
    let buf = Buffer.alloc(0);
    let last = Date.now();
    let idle: ReturnType<typeof setTimeout> | undefined;
    const arm = () => {
      if (this.mode.idleCloseMs === undefined) return;
      clearTimeout(idle);
      idle = setTimeout(() => s.destroy(), this.mode.idleCloseMs);
    };
    arm();
    s.on("data", (d: Buffer) => {
      buf = Buffer.concat([buf, d]);
      for (;;) {
        const end = buf.indexOf("\r\n\r\n");
        if (end < 0) return;
        const head = buf.subarray(0, end).toString();
        const len = Number(/content-length: *(\d+)/i.exec(head)?.[1] ?? 0);
        if (buf.length < end + 4 + len) return;
        buf = buf.subarray(end + 4 + len);
        const [method, path] = head.split(" ");
        const stale = this.mode.staleAfterMs !== undefined && Date.now() - last > this.mode.staleAfterMs;
        last = Date.now();
        clearTimeout(idle);
        this.handle(s, method!, path!, stale, arm);
      }
    });
  }
  reply(s: Socket, status: number, body: string, who: "health" | "product" | "edge" = "product"): void {
    const mark = who === "health" ? "x-streams-boot-id: b1\r\n" : who === "product" ? PRODUCT_CORS : "";
    s.write(`HTTP/1.1 ${status} X\r\n${mark}content-type: application/json\r\ncontent-length: ${Buffer.byteLength(body)}\r\n\r\n${body}`);
  }
  handle(s: Socket, method: string, path: string, stale: boolean, arm: () => void): void {
    if (stale) return (this.reply(s, 502, "<html>bad gateway</html>", "edge"), arm());
    if (path === "/health") return (this.reply(s, 200, "ok", "health"), arm());
    if (method === "POST") {
      this.reply(s, 200, `{"count":1}`);
      arm();
      for (const c of this.sse) c.write(chunk(`event: data\ndata: [{"x":1}]\n\n`));
      for (const p of [...this.polls]) p();
      return;
    }
    if (path.includes("records:sse")) {
      this.sse.add(s);
      s.write(`HTTP/1.1 200 OK\r\n${PRODUCT_CORS}content-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n`);
      s.write(chunk(`event: control\ndata: {"nextCursor":"c"}\n\n`));
      return;
    }
    if (path.includes("records:long-poll")) {
      const wait = Math.min(Number(/waitMs=(\d+)/.exec(path)?.[1] ?? 0), this.mode.capMs ?? Infinity);
      let done = false;
      const finish = (status: number, body: string, who: "product" | "edge" = "product") => {
        if (done) return;
        done = true;
        this.polls.delete(wake);
        clearTimeout(t1);
        clearTimeout(t2);
        this.reply(s, status, body, who);
        arm();
      };
      const wake = () => finish(200, `[{"x":1}]`);
      this.polls.add(wake);
      const t1 = setTimeout(() => finish(204, ""), wait);
      const t2 = this.mode.deadlineMs === undefined ? undefined : setTimeout(() => finish(504, "<html>gateway timeout</html>", "edge"), this.mode.deadlineMs);
      return;
    }
    this.reply(s, 404, `{"error":{"code":"not_found"}}`);
    arm();
  }
}

/// The header the binary puts on every product answer (src/product.rs with_product_cors).
const PRODUCT_CORS = "access-control-allow-origin: *\r\naccess-control-expose-headers: content-type, retry-after, " +
  "prisma-next-cursor, prisma-up-to-date, prisma-sealed, prisma-next-scan-cursor, prisma-scan-complete, prisma-routing-key, " +
  "prisma-durable-cursor, prisma-pending-from, prisma-consumer-version\r\n";

function chunk(text: string): string {
  return `${Buffer.byteLength(text).toString(16)}\r\n${text}\r\n`;
}

const origins: Origin[] = [];
afterEach(async () => {
  for (const o of origins.splice(0)) await o.stop();
});
async function origin(mode: Mode = {}): Promise<Origin> {
  const o = new Origin(mode);
  await o.start();
  origins.push(o);
  return o;
}

let headers = "";
beforeAll(() => {
  headers = join(dir, "headers.txt");
  writeFileSync(headers, "Authorization: Bearer test-token\n");
});

async function probe(...args: string[]): Promise<{ code: number; out: string; err: string }> {
  const p = Bun.spawn([process.execPath, PROBE, ...args], { stdout: "pipe", stderr: "pipe" });
  const [out, err, code] = await Promise.all([new Response(p.stdout).text(), new Response(p.stderr).text(), p.exited]);
  return { code, out, err };
}

const records = (file: string) => readFileSync(file, "utf8").trim().split("\n").map((l) => JSON.parse(l));

test("idle: a connection reused inside the idle timeout answers; past it the peer closed it; a stale upstream answers 502", async () => {
  const o = await origin({ idleCloseMs: 600 });
  const out = join(dir, "idle.jsonl");
  const r = await probe("idle", "--target", o.url, "--gaps", "0.2,1", "--vantage", "lap", "--out", out);
  expect(r.err).toBe("");
  expect(r.code).toBe(0);
  const byGap = Object.fromEntries(records(out).map((x) => [x.gap_s, x]));
  expect(byGap[0.2]).toMatchObject({ kind: "idle", vantage: "lap", first_status: 200, outcome: "reused", status: 200 });
  expect(byGap[1]).toMatchObject({ kind: "idle", first_status: 200, outcome: "closed" });
  expect(byGap[1].closed_after_s).toBeGreaterThanOrEqual(0.5);
  expect(byGap[1].closed_after_s).toBeLessThan(0.9);

  const stale = await origin({ staleAfterMs: 400 });
  const out2 = join(dir, "idle-stale.jsonl");
  await probe("idle", "--target", stale.url, "--gaps", "0.1,0.7", "--vantage", "lap", "--out", out2);
  const s = Object.fromEntries(records(out2).map((x) => [x.gap_s, x]));
  expect(s[0.1]).toMatchObject({ outcome: "reused", status: 200 });
  expect(s[0.7]).toMatchObject({ outcome: "status", status: 502, origin: false });
  // A product path the origin answers carries no boot id, only its CORS header.
  const out3 = join(dir, "idle-product.jsonl");
  await probe("idle", "--target", o.url, "--gaps", "0.1", "--path", "/v1/streams/e/s00000/records", "--vantage", "lap", "--out", out3);
  expect(records(out3)[0]).toMatchObject({ outcome: "status", status: 404, origin: true });
  const rep = await probe("report", out, out2);
  expect(rep.out).toContain("FACT idle: lap 1 s: the peer closed the idle connection after 0.");
  expect(rep.out).toContain("FINDING idle: lap 0.7 s: reuse answered 502 by the edge, not the origin: the edge's upstream idle timeout outlives the origin's");
});

test("wall: parked SSE and long-polls ramp until a fresh client elsewhere fails; the report names the count and how it failed", async () => {
  const o = await origin({ wall: 24 });
  const park = join(dir, "park.jsonl");
  const fresh = join(dir, "fresh.jsonl");
  const freshing = probe("fresh", "--target", o.url, "--every", "0.05", "--duration", "1.5", "--vantage", "lap", "--out", fresh);
  await Bun.sleep(300);
  const p = await probe("park", "--target", o.url, "--headers-file", headers, "--streams", "4", "--prefix", "e/s", "--via", "mix",
    "--count", "30", "--rate", "200", "--hold", "1", "--vantage", "gen-a", "--out", park);
  const r = await freshing;
  expect([p.code, r.code]).toEqual([0, 0]);
  const parked = records(park);
  const peak = Math.max(...parked.filter((x) => x.kind === "park").map((x) => x.held_sse + x.held_lp));
  expect(peak).toBeLessThanOrEqual(24);
  expect(peak).toBeGreaterThanOrEqual(20);
  expect(parked.some((x) => x.kind === "park_fail" && x.error === "reset")).toBe(true);
  const f = records(fresh);
  expect(f.some((x) => x.ok)).toBe(true);
  expect(f.some((x) => !x.ok && x.error === "reset")).toBe(true);
  const rep = await probe("report", park, fresh, "--wall", "24");
  expect(rep.out).toMatch(/FACT wall: a fresh client on lap first failed \(reset\) with (2[0-4]) connections parked \(gen-a \1\)/);
});

test("sse-first times the first data event after an acked append, and deadline says who answered each wait", async () => {
  const o = await origin({ deadlineMs: 500 });
  const out = join(dir, "first.jsonl");
  const r = await probe("sse-first", "--target", o.url, "--headers-file", headers, "--stream", "e/s00000", "--n", "2",
    "--settle", "0.05", "--vantage", "in-region", "--out", out);
  expect(r.code).toBe(0);
  const firsts = records(out);
  expect(firsts.length).toBe(2);
  for (const x of firsts) {
    expect(x).toMatchObject({ kind: "sse_first", vantage: "in-region", status: 200 });
    expect(x.event_ms).toBeGreaterThanOrEqual(0);
    expect(x.event_ms).toBeLessThan(500);
  }
  const d = join(dir, "deadline.jsonl");
  await probe("deadline", "--target", o.url, "--headers-file", headers, "--stream", "e/s00000", "--waits", "0.2,0.8",
    "--repeat", "1", "--vantage", "in-region", "--out", d);
  const by = Object.fromEntries(records(d).map((x) => [x.wait_s, x]));
  expect(by[0.2]).toMatchObject({ kind: "deadline", status: 204, origin: true });
  expect(by[0.8]).toMatchObject({ kind: "deadline", status: 504, origin: false });
  const rep = await probe("report", out, d);
  expect(rep.out).toContain("FACT sse-first: in-region: 2 subscription(s), first event p50");
  expect(rep.out).toMatch(/FACT deadline: in-region: the origin held a long-poll's first byte at most 0\.[23] s \(waits asked: 0\.2, 0\.8 s\)/);
  expect(rep.out).toMatch(/FACT deadline: in-region: a wait of 0\.8 s was answered by the edge, 504, after 0\.[56] s/);
  expect(rep.out).not.toContain("caps a long-poll's wait");
});

test("deadline: an origin that caps a long-poll's wait below the edge's deadline answers every wait itself, and the report says the deadline is out of reach", async () => {
  // The binary's 25 s cap and Compute's 60 s deadline, scaled: a 0.6 s cap, a 1 s deadline.
  const o = await origin({ capMs: 600, deadlineMs: 1000 });
  const d = join(dir, "deadline-capped.jsonl");
  await probe("deadline", "--target", o.url, "--headers-file", headers, "--stream", "e/s00000", "--waits", "0.2,0.8,1.5",
    "--repeat", "1", "--vantage", "in-region", "--out", d);
  const by = Object.fromEntries(records(d).map((x) => [x.wait_s, x]));
  for (const w of [0.2, 0.8, 1.5]) expect(by[w]).toMatchObject({ kind: "deadline", status: 204, origin: true });
  expect(by[1.5].elapsed_ms).toBeLessThan(900);
  const rep = await probe("report", d);
  expect(rep.out).toMatch(/FACT deadline: in-region: the origin held a long-poll's first byte at most 0\.[67] s \(waits asked: 0\.2, 0\.8, 1\.5 s\)/);
  expect(rep.out).toMatch(/FACT deadline: in-region: the origin answered waits of 0\.8, 1\.5 s with 204 by 0\.[67] s: it caps a long-poll's wait, so no long-poll holds its first byte to the edge's deadline, which is unreachable through one/);
  expect(rep.out).not.toContain("answered by the edge");
});

test("write keeps its connections and counts acked appends; tls-idle refuses to run without a dated notice naming the service and this vantage, and above the cap", async () => {
  const o = await origin();
  const w = join(dir, "write.jsonl");
  const r = await probe("write", "--target", o.url, "--headers-file", headers, "--streams", "2", "--prefix", "e/s", "--rate", "40",
    "--conns", "2", "--duration", "0.6", "--vantage", "gen-b", "--out", w);
  expect(r.code).toBe(0);
  const sum = records(w).filter((x) => x.kind === "write").reduce((a, x) => [a[0] + x.ok, a[1] + x.failed], [0, 0]);
  expect(sum[1]).toBe(0);
  expect(sum[0]).toBeGreaterThanOrEqual(15);

  const notice = join(dir, "notice.json");
  const base = ["tls-idle", "--target", o.url, "--measured-wall", "20", "--rate", "100", "--hold", "0.2", "--out", join(dir, "tls.jsonl")];
  expect((await probe(...base, "--vantage", "lap")).err).toContain("tls-idle needs --notice");
  writeFileSync(notice, JSON.stringify({ notified: "2099-01-01", service: "127.0.0.1", vantages: ["lap"], reply: "ok" }));
  expect((await probe(...base, "--vantage", "lap", "--notice", notice)).err).toContain("the notice is dated 2099-01-01, after today");
  writeFileSync(notice, JSON.stringify({ notified: "2026-10-01", service: "other.example", vantages: ["lap"], reply: "ok" }));
  expect((await probe(...base, "--vantage", "lap", "--notice", notice)).err).toContain("names service other.example, not 127.0.0.1");
  writeFileSync(notice, JSON.stringify({ notified: "2026-10-01", service: "127.0.0.1", vantages: ["gen-a"], reply: "ok" }));
  expect((await probe(...base, "--vantage", "lap", "--notice", notice)).err).toContain("vantage lap is not one the notice lists");
  writeFileSync(notice, JSON.stringify({ notified: "2026-10-01", service: "127.0.0.1", vantages: ["lap"] }));
  expect((await probe(...base, "--vantage", "lap", "--notice", notice)).err).toContain(`the notice has no "reply": Compute's answer`);
  writeFileSync(notice, JSON.stringify({ notified: "2026-10-01", service: "127.0.0.1", vantages: ["lap"], reply: " " }));
  expect((await probe(...base, "--vantage", "lap", "--notice", notice)).err).toContain(`the notice has no "reply": Compute's answer`);
  writeFileSync(notice, JSON.stringify({ notified: "2026-10-01", service: "127.0.0.1", vantages: ["lap"], reply: "please don't" }));
  expect((await probe(...base, "--vantage", "lap", "--notice", notice)).err).toContain(`the notice has no "go_ahead": true`);
  writeFileSync(notice, JSON.stringify({ notified: "2026-10-01", service: "127.0.0.1", vantages: ["lap"], reply: "ok", go_ahead: true }));
  expect((await probe(...base, "--vantage", "lap", "--notice", notice, "--count", "23")).err).toContain("--count 23 is over the cap 22");
  // The cap rounds down: a wall of 1,536 allows 1,689 (1,536 x 1.1 = 1,689.6), never 1,690.
  const wide = ["tls-idle", "--target", o.url, "--measured-wall", "1536", "--vantage", "lap", "--notice", notice];
  expect((await probe(...wide, "--count", "1690")).err).toContain("--count 1690 is over the cap 1689");
  const ok = await probe(...base, "--vantage", "lap", "--notice", notice);
  expect(ok.code).toBe(0);
  const held = records(join(dir, "tls.jsonl"));
  expect(held[0]).toMatchObject({ kind: "tls_idle_start", cap: 22, notice: { notified: "2026-10-01", service: "127.0.0.1", reply: "ok", go_ahead: true } });
  expect(Math.max(...held.filter((x) => x.kind === "tls_idle").map((x) => x.held))).toBe(22);
});

test("posture: SSE_MAX_CONNECTIONS from the layered profiles plus long-polls plus writer headroom must stay under the wall", async () => {
  const pass = await probe("posture", "--wall", "1536", "--long-polls", "200", "--writer-headroom", "100");
  expect(pass.out.trim()).toBe(
    "PASS posture: SSE_MAX_CONNECTIONS 1200 (deploy/profiles) + long-polls 200 + writer headroom 100 = 1500 < the wall 1536");
  expect(pass.code).toBe(0);
  const fail = await probe("posture", "--wall", "1536", "--long-polls", "300", "--writer-headroom", "100");
  expect(fail.out.trim()).toBe(
    "FAIL posture: SSE_MAX_CONNECTIONS 1200 (deploy/profiles) + long-polls 300 + writer headroom 100 = 1600 >= the wall 1536");
  expect(fail.code).toBe(1);
  const equal = await probe("posture", "--wall", "1536", "--long-polls", "236", "--writer-headroom", "100");
  expect(equal.out.trim()).toBe(
    "FAIL posture: SSE_MAX_CONNECTIONS 1200 (deploy/profiles) + long-polls 236 + writer headroom 100 = 1536 >= the wall 1536");
  expect((await probe("posture", "--wall", "1536")).err).toBe("edgeprobe posture needs --wall, --long-polls and --writer-headroom\n");
});
