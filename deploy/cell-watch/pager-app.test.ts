// The paging tool's stand-in as a Composer app of its own (pager-app.ts,
// pager-service.ts, pager-entry.ts; the field-readiness gap "no alert sink
// or heartbeat URL"): a scratch or cert cell's watch gets https endpoints
// for its Events v2 sink and its heartbeat without a third-party service,
// on a service apart from the watch, so the dead-man still pages when the
// watch is down. Production pages through the real paging service
// (RUNBOOK §7.5). Run with `bun test ./deploy/cell-watch`; the bundled
// entry's test needs this directory's packages and is opt-in, as
// composer.test.ts's is:
//
//   (cd deploy/cell-watch && bun install --frozen-lockfile)
//   CELL_WATCH_COMPOSER_E2E=1 bun test ./deploy/cell-watch/pager-app.test.ts
import { afterEach, describe, expect, test } from "bun:test";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { EVENTS_PATH, Pager, pagerOptions } from "./pager";

const HERE = import.meta.dir;
const read = (file: string) => readFileSync(join(HERE, file), "utf8");
const code = (file: string) => read(file).split("\n").filter((l) => !/^\s*\/\//.test(l));
const KEY = "routing-key-of-the-stand-in-0001";
const TOKEN = "heartbeat-token-0123456789";

const open: { stop(): void }[] = [];
const children: ReturnType<typeof Bun.spawn>[] = [];
const dirs: string[] = [];
afterEach(() => {
  for (const s of open.splice(0)) s.stop();
  for (const c of children.splice(0)) c.kill("SIGKILL");
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true });
});
const maybe = process.env.CELL_WATCH_COMPOSER_E2E === "1" ? test : test.skip;

const refusal = (fn: () => unknown) => {
  try {
    fn();
  } catch (e) {
    return (e as Error).message;
  }
  return "no refusal";
};

describe("the stand-in's Composer app", () => {
  test("binds its two secrets and the cell's origin under their own names, as its service declares them, and nothing else", () => {
    const bound = [...read("pager-app.ts").matchAll(/^\s*(\w+): (envSecret|envParam)\("([A-Z0-9_]+)"\),$/gm)].map((m) => [m[1], m[2], m[3]]);
    expect(bound).toEqual([
      ["PAGER_ROUTING_KEY", "envSecret", "PAGER_ROUTING_KEY"],
      ["PAGER_HEARTBEAT_TOKEN", "envSecret", "PAGER_HEARTBEAT_TOKEN"],
      ["STREAMS_CELL_ORIGIN", "envParam", "STREAMS_CELL_ORIGIN"],
    ]);
    const fields = [...read("pager-service.ts").matchAll(/^\s*"?(\w+)(\??)"?: (secretString\(\)|"string"),$/gm)].map((m) => [m[1], m[2], m[3]]);
    expect(fields).toEqual([
      ["PAGER_ROUTING_KEY", "", "secretString()"],
      ["PAGER_HEARTBEAT_TOKEN", "", "secretString()"],
      ["STREAMS_CELL_ORIGIN", "", '"string"'],
    ]);
    expect(/\bmodule\("([^"]+)"/.exec(read("pager-app.ts"))?.[1]).toBe("streams-pager-standin");
    expect(read("pager-service.ts")).toContain('build: node({ module: import.meta.url, entry: "pager-entry.js" }),');
    for (const file of ["pager-app.ts", "pager-service.ts"]) {
      expect(code(file).join("\n").match(/\b(generatedParam|serviceOrigin|paramSource|secretSource|durableStreams|rpc)\b/g)).toBeNull();
    }
    expect(code("pager-service.ts").join("\n")).toContain("deps: {},");
  });

  test("reads its inputs by name, refusing a short key, a token unfit for a URL path, a cell origin that is not https or none", () => {
    const ok = { PAGER_ROUTING_KEY: KEY, PAGER_HEARTBEAT_TOKEN: TOKEN, STREAMS_CELL_ORIGIN: "https://cell.example" };
    expect(pagerOptions(ok)).toEqual({ routingKey: KEY, heartbeatToken: TOKEN, uptimeOrigin: "https://cell.example" });
    expect(pagerOptions({ ...ok, STREAMS_CELL_ORIGIN: "none" }).uptimeOrigin).toBeNull();
    expect([
      refusal(() => pagerOptions({ ...ok, PAGER_ROUTING_KEY: undefined })),
      refusal(() => pagerOptions({ ...ok, PAGER_ROUTING_KEY: "short" })),
      refusal(() => pagerOptions({ ...ok, PAGER_HEARTBEAT_TOKEN: "a/b-0123456789abcdef" })),
      refusal(() => pagerOptions({ ...ok, STREAMS_CELL_ORIGIN: "http://cell.example" })),
      refusal(() => pagerOptions({ ...ok, STREAMS_CELL_ORIGIN: "https://cell.example/health" })),
    ]).toEqual([
      "PAGER_ROUTING_KEY is required",
      "PAGER_ROUTING_KEY must be at least 16 characters: its endpoint is public",
      "PAGER_HEARTBEAT_TOKEN must be 16 to 128 of [A-Za-z0-9_-]: it is the heartbeat URL's last segment",
      "STREAMS_CELL_ORIGIN must be an https origin (http only to this machine), or none",
      "STREAMS_CELL_ORIGIN must be an https origin (http only to this machine), or none",
    ]);
    expect(JSON.stringify(refusal(() => pagerOptions({ ...ok, PAGER_ROUTING_KEY: "short" })))).not.toContain("short\"");
  });

  test("answers ok on /, and lists what it took on /changes only to the routing key's bearer", async () => {
    const p = new Pager({ routingKey: KEY, heartbeatToken: TOKEN, uptimeOrigin: null });
    open.push(p);
    const base = p.eventsUrl.slice(0, -EVENTS_PATH.length);
    expect([(await fetch(`${base}/`)).status, await (await fetch(`${base}/`)).text()]).toEqual([200, "ok"]);
    const trigger = {
      routing_key: KEY, event_action: "trigger", dedup_key: "cell_sc1/r03/PAGE",
      payload: { summary: "[cell_sc1] r03", source: "cell_sc1", severity: "critical" },
    };
    expect((await fetch(p.eventsUrl, { method: "POST", body: JSON.stringify(trigger) })).status).toBe(202);
    expect((await fetch(p.heartbeatUrl)).status).toBe(200);
    const listing = (auth?: string) => fetch(`${base}/changes`, { headers: auth ? { authorization: auth } : {} });
    expect([(await listing()).status, (await listing(`Bearer ${TOKEN}`)).status, (await listing(`Bearer ${KEY}x`)).status]).toEqual([401, 401, 401]);
    const doc = await (await listing(`Bearer ${KEY}`)).json();
    expect([doc.open, doc.changes.map((c: { action: string; key: string; page: boolean }) => [c.action, c.key, c.page]), doc.beats]).toEqual([
      ["cell_sc1/r03/PAGE"], [["open", "cell_sc1/r03/PAGE", true]], 1,
    ]);
    expect(typeof doc.last_beat_ms).toBe("number");
  });

  maybe("its bundled entry serves the stand-in on the service's port from the input document, and refuses a bad input by name", async () => {
    if (!existsSync(join(HERE, "node_modules"))) throw new Error("run `bun install --frozen-lockfile` in deploy/cell-watch first");
    const dir = mkdtempSync(join(tmpdir(), "pager-entry-"));
    dirs.push(dir);
    const built = await Bun.build({ entrypoints: [join(HERE, "pager-entry.ts")], target: "bun", format: "esm", outdir: dir, naming: "pager-entry.js" });
    expect(built.success).toBe(true);
    const free = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response() });
    const port = Number(free.port);
    free.stop(true);
    const doc = {
      PAGER_ROUTING_KEY: { $secret: "PAGER_ROUTING_KEY" },
      PAGER_HEARTBEAT_TOKEN: { $secret: "PAGER_HEARTBEAT_TOKEN" },
      STREAMS_CELL_ORIGIN: "none",
    };
    const launch = (key: string) => {
      const child = Bun.spawn([process.execPath, join(dir, "pager-entry.js")], {
        cwd: dir, stdout: "pipe", stderr: "pipe",
        env: {
          PATH: process.env.PATH ?? "/usr/bin:/bin", HOME: process.env.HOME ?? "/", PAGER_ROUTING_KEY: key,
          PAGER_HEARTBEAT_TOKEN: TOKEN, STREAMS_CELL_ORIGIN: "laced", COMPOSER_INPUT: JSON.stringify(doc), COMPOSER_PORT: String(port),
        },
      });
      children.push(child);
      return child;
    };
    const bad = launch("short");
    expect([await bad.exited, await new Response(bad.stderr as ReadableStream).text()]).toEqual([
      1, "pager-standin: refused: PAGER_ROUTING_KEY must be at least 16 characters: its endpoint is public\n",
    ]);
    launch(KEY);
    const base = `http://127.0.0.1:${port}`;
    for (const end = Date.now() + 15_000; ; await Bun.sleep(50)) {
      if (await fetch(`${base}/`).then((r) => r.ok, () => false)) break;
      if (Date.now() > end) throw new Error("the stand-in did not answer");
    }
    expect((await fetch(`${base}/heartbeat/${TOKEN}`)).status).toBe(200);
    const doc2 = await (await fetch(`${base}/changes`, { headers: { authorization: `Bearer ${KEY}` } })).json();
    expect([doc2.beats, doc2.changes]).toEqual([1, []]);
  }, 30_000);
});
