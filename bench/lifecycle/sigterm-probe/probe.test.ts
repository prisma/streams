// The SIGTERM probe (deploy-q5 triage B6; design §3.4 leg 1): what its log
// says when the platform stops it. Run with
// `bun test ./bench/lifecycle/sigterm-probe`. Each case runs the probe as its
// own process-group leader, as an instance's first process would be, and
// signals it from outside the way a platform could: the probe's process
// alone, or its whole group.
import { afterEach, expect, test } from "bun:test";
import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const PROBE = join(import.meta.dir, "app", "probe.ts");
const LINE = /^sigterm-probe (main|child) (boot|SIGTERM|alive) t=(\S+)(.*)$/;

type Line = { role: string; event: string; t: number; fields: Record<string, string> };

function parse(raw: string): Line | null {
  const m = LINE.exec(raw);
  if (!m) return null;
  const fields: Record<string, string> = {};
  for (const f of m[4]!.matchAll(/(\w+)=("(?:[^"\\]|\\.)*"|\S+)/g)) fields[f[1]!] = f[2]!;
  return { role: m[1]!, event: m[2]!, t: Date.parse(m[3]!), fields };
}

const running: ChildProcess[] = [];
afterEach(() => {
  for (const p of running.splice(0)) {
    try {
      process.kill(-p.pid!, "SIGKILL");
    } catch {}
  }
});

async function freePort(): Promise<number> {
  const server = Bun.serve({ port: 0, fetch: () => new Response("") });
  const port = server.port;
  server.stop(true);
  return port;
}

/// The probe as a group leader; every stdout line it and its child print.
function start(port: number, env: Record<string, string> = {}) {
  const p = spawn(process.execPath, [PROBE], {
    detached: true,
    env: { ...process.env, PORT: String(port), ...env },
    stdio: ["ignore", "pipe", "pipe"],
  });
  running.push(p);
  const raw: string[] = [];
  const err: string[] = [];
  p.stderr!.on("data", (d: Buffer) => err.push(d.toString()));
  let buf = "";
  p.stdout!.on("data", (d: Buffer) => {
    buf += d.toString();
    const parts = buf.split("\n");
    buf = parts.pop()!;
    raw.push(...parts);
  });
  const lines = () => raw.map(parse).filter((l): l is Line => l !== null);
  /// Waits for `cond`; a timeout names what it waited for and the probe's stderr.
  const until = async (what: string, cond: () => boolean, ms: number): Promise<void> => {
    const end = Date.now() + ms;
    while (!cond()) {
      if (Date.now() > end) throw new Error(`timed out waiting for ${what}; probe stderr: ${err.join("").trim() || "(none)"}`);
      await Bun.sleep(25);
    }
  };
  return { p, raw, lines, until };
}

test("a SIGTERM to the probe alone: the main process logs it and then once a second until killed; the child hears nothing", async () => {
  const port = await freePort();
  const { p, raw, lines, until } = start(port);
  await until("both boot lines", () => lines().filter((l) => l.event === "boot").length === 2, 3000);
  const boot = lines().find((l) => l.role === "main" && l.event === "boot")!;
  expect(boot.fields.pid).toBe(String(p.pid));
  expect(Object.keys(boot.fields).sort()).toEqual(["keep_awake", "nonce", "pgid", "pid", "pid1", "port", "ppid", "runtime"]);
  expect(boot.fields.port).toBe(String(port));
  // Off Compute there is no scale-to-zero control file to hold.
  expect(boot.fields.keep_awake).toBe("unavailable");
  expect(boot.fields.runtime).toBe(`bun-${Bun.version}`);
  const child = lines().find((l) => l.role === "child" && l.event === "boot")!;
  expect(child.fields.ppid).toBe(String(p.pid));
  expect((await fetch(`http://127.0.0.1:${port}/`)).status).toBe(200);

  const sent = Date.now();
  process.kill(p.pid!, "SIGTERM");
  await until("two ticks", () => lines().filter((l) => l.event === "alive").length >= 2, 4000);
  // Still serving after the signal: the probe never stops on its own.
  expect((await fetch(`http://127.0.0.1:${port}/`)).status).toBe(200);
  process.kill(-p.pid!, "SIGKILL");
  await Bun.sleep(100);

  const events = lines().filter((l) => l.event !== "boot");
  expect(events.map((l) => `${l.role} ${l.event}`)).toEqual(["main SIGTERM", "main alive", "main alive"]);
  expect(events[0]!.fields.pid).toBe(String(p.pid));
  expect(events[1]!.fields.after).toBe("1s");
  expect(events[2]!.fields.after).toBe("2s");
  expect(events[0]!.t).toBeGreaterThanOrEqual(sent - 5);
  expect(events[1]!.t - events[0]!.t).toBeGreaterThanOrEqual(900);
  expect(raw.filter((l) => !LINE.test(l))).toEqual([]);
}, 15_000);

test("a SIGTERM to the probe's process group: the child logs it too, which the main process never forwards", async () => {
  const { p, lines, until } = start(await freePort());
  await until("both boot lines", () => lines().filter((l) => l.event === "boot").length === 2, 3000);
  process.kill(-p.pid!, "SIGTERM");
  await until("ticks from both", () => lines().filter((l) => l.event === "alive").length >= 2, 4000);
  process.kill(-p.pid!, "SIGKILL");
  await Bun.sleep(100);
  const got = lines().filter((l) => l.event === "SIGTERM").map((l) => l.role).sort();
  expect(got).toEqual(["child", "main"]);
  const alive = lines().filter((l) => l.event === "alive");
  expect(new Set(alive.map((l) => l.role))).toEqual(new Set(["child", "main"]));
}, 15_000);

test("the nonce file beside the entry names the deploy in the boot line", async () => {
  const { lines, until } = start(await freePort());
  await until("the boot line", () => lines().some((l) => l.role === "main" && l.event === "boot"), 3000);
  const nonce = lines().find((l) => l.role === "main" && l.event === "boot")!.fields.nonce!;
  const file = Bun.file(join(import.meta.dir, "app", "nonce.txt"));
  expect(nonce).toBe((await file.exists()) ? JSON.stringify((await file.text()).trim()) : "-");
});

test("the main process holds the instance awake: it writes + to the scale-to-zero control and says so in its boot line", async () => {
  const dir = mkdtempSync(join(tmpdir(), "sigterm-probe-"));
  const control = join(dir, "scale_to_zero_disable");
  writeFileSync(control, "");
  try {
    const { lines, until } = start(await freePort(), { SIGTERM_PROBE_KEEP_AWAKE_CONTROL: control });
    await until("both boot lines", () => lines().filter((l) => l.event === "boot").length === 2, 3000);
    const boot = lines().find((l) => l.role === "main" && l.event === "boot")!;
    expect(boot.fields.keep_awake).toBe("held");
    // One acquire, by the main process only.
    expect(readFileSync(control, "utf8")).toBe("+");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
