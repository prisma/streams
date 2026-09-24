// Item 39: what the deploy wrapper does when the binary it supervises dies.
// Run with `bun test ./deploy/supervise.test.ts`. The wrapper ends its own
// process, so each case runs it in a child Bun process over a scripted
// "binary" (another Bun process) and watches it from outside.
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { App, DeathPolicy } from "./app-server/supervise";

const APPS: App[] = ["app-server", "app-lb", "app-gen"];
const COPIES = APPS.map((app) => join(import.meta.dir, app, "supervise.ts"));
/// The wrapper module the in-process cases call (the copies are identical).
const supervise = () => import(COPIES[0]);

// READY_UPTIME_MS, when set, shortens how long a child must have been up
// before its death counts as a runtime death; unset, the wrapper's own
// default (60 s) applies.
const RUNNER = `
const { superviseBinary } = await import(process.env.SUPERVISE_MODULE);
const policy = { onDeathAfterReady: process.env.DEATH_POLICY };
if (process.env.READY_UPTIME_MS) policy.readyUptimeMs = Number(process.env.READY_UPTIME_MS);
await superviseBinary(process.execPath, ["-e", process.env.CHILD_SCRIPT], process.env, policy);
`;

/// Serves on $PORT, then dies with `code` 1.5 s after it started: code 1 is
/// streams-slate after a critical loop's exit (item 38), code 0 its
/// graceful stop.
const servesThenExits = (code: number) => `
Bun.serve({ port: Number(process.env.PORT), fetch: () => new Response("ok") });
setTimeout(() => process.exit(${code}), 1500);
`;
/// Serves on $PORT, then is killed by SIGKILL 1.5 s after it started, as the
/// kernel's OOM killer would.
const SERVES_THEN_KILLED = `
Bun.serve({ port: Number(process.env.PORT), fetch: () => new Response("ok") });
setTimeout(() => process.kill(process.pid, "SIGKILL"), 1500);
`;
/// Never binds, and dies with code 2 after 1.5 s: a boot failure that took
/// its time.
const EXITS_2_WITHOUT_BINDING = `setTimeout(() => process.exit(2), 1500);`;

async function freePort(): Promise<number> {
  const server = Bun.serve({ port: 0, fetch: () => new Response("") });
  const port = server.port;
  server.stop(true);
  return port;
}

function wrapper(
  port: number,
  child: string,
  policy: "exit" | "hold",
  readyUptimeMs?: number,
  stderr: "pipe" | "ignore" = "ignore",
) {
  return Bun.spawn([process.execPath, "-e", RUNNER], {
    env: {
      ...process.env,
      PORT: String(port),
      SUPERVISE_MODULE: COPIES[0],
      CHILD_SCRIPT: child,
      DEATH_POLICY: policy,
      READY_UPTIME_MS: readyUptimeMs === undefined ? "" : String(readyUptimeMs),
    },
    stdout: "ignore",
    stderr,
  });
}

async function exitWithin(proc: { exited: Promise<number> }, ms: number) {
  return Promise.race([proc.exited, Bun.sleep(ms).then(() => "running" as const)]);
}

/// Polls $PORT until it answers 500 (the wrapper's diagnostic), or gives up.
async function diagnostic(port: number, ms: number): Promise<Response | undefined> {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    try {
      const response = await fetch(`http://127.0.0.1:${port}/`);
      if (response.status === 500) return response;
    } catch {}
    await Bun.sleep(100);
  }
  return undefined;
}

/// The wrapper holds the death: $PORT serves the diagnostic with the
/// child's exit code, and the wrapper stays up.
async function expectHeld(proc: { exited: Promise<number> }, port: number, code: number) {
  const response = await diagnostic(port, 10_000);
  expect(response?.status).toBe(500);
  const body = await response!.json();
  expect([body.error, body.exitCode]).toEqual(["binary_exited", code]);
  expect(await exitWithin(proc, 500)).toBe("running");
}

test("the three wrapper copies are byte-identical", () => {
  const [first, ...rest] = COPIES.map((path) => readFileSync(path, "utf8"));
  for (const copy of rest) expect(copy).toBe(first);
});

// Owner decisions D5 and C2: the stream server and the pilot router hand a
// ready death to the platform; every load generator holds it.
test("each app's death policy, for every deploy environment", async () => {
  const { policyFor } = await supervise();
  const cases: [App, Record<string, string>, "exit" | "hold"][] = [
    ["app-server", {}, "exit"],
    ["app-server", { PILOT_MODE: "gen" }, "exit"],
    ["app-lb", {}, "exit"],
    ["app-lb", { PILOT_MODE: "lb" }, "exit"],
    ["app-lb", { PILOT_MODE: "gen" }, "hold"],
    ["app-lb", { PILOT_MODE: "bench" }, "hold"],
    ["app-gen", {}, "hold"],
    ["app-gen", { PILOT_MODE: "lb" }, "hold"],
  ];
  for (const [app, env, death] of cases) {
    expect([app, env, policyFor(app, env)]).toEqual([app, env, { onDeathAfterReady: death }]);
  }
});

// A dropped policy argument must not fall back to "hold" (the unreplaced
// 500 item 39 removed): the wrapper refuses before it starts the binary.
test("a missing death policy is refused before the binary starts", async () => {
  const { superviseBinary } = await supervise();
  const unset = undefined as unknown as DeathPolicy;
  await expect(superviseBinary("/nonexistent/streams-slate", [], {}, unset)).rejects.toThrow(
    "a death policy is required",
  );
});

// The wiring itself: each index.ts hands its wrapper the policy of its own
// app, computed from the environment it was deployed with.
test("each app's index.ts supervises its binary under its own app's policy", () => {
  for (const app of APPS) {
    const source = readFileSync(join(import.meta.dir, app, "index.ts"), "utf8");
    const calls = [...source.matchAll(/superviseBinary\(([\s\S]*?)\);/g)].map((m) => m[1].trim());
    expect([app, calls.length]).toEqual([app, 1]);
    expect([app, calls[0].endsWith(`policyFor("${app}", process.env)`)]).toEqual([app, true]);
  }
});

test("a child that died after it was ready ends the wrapper with its code", async () => {
  const port = await freePort();
  const proc = wrapper(port, servesThenExits(1), "exit", 1_000, "pipe");
  try {
    expect(await exitWithin(proc, 10_000)).toBe(1);
    const log = await new Response(proc.stderr).text();
    expect(log).toMatch(
      new RegExp(
        `binary exited with code 1 after serving on :${port} \\(up \\d+ ms\\); ` +
          "exiting 1 so the platform replaces this instance",
      ),
    );
  } finally {
    proc.kill();
  }
}, 15_000);

test("a ready child's graceful stop ends the wrapper with code 0", async () => {
  const port = await freePort();
  const proc = wrapper(port, servesThenExits(0), "exit", 1_000);
  try {
    expect(await exitWithin(proc, 10_000)).toBe(0);
  } finally {
    proc.kill();
  }
}, 15_000);

test("a ready child killed by a signal ends the wrapper with 128 + its number", async () => {
  const port = await freePort();
  const proc = wrapper(port, SERVES_THEN_KILLED, "exit", 1_000);
  try {
    expect(await exitWithin(proc, 10_000)).toBe(137);
  } finally {
    proc.kill();
  }
}, 15_000);

test("a child that never accepted is held and served as a diagnostic", async () => {
  const port = await freePort();
  const proc = wrapper(port, EXITS_2_WITHOUT_BINDING, "exit", 1_000);
  try {
    await expectHeld(proc, port, 2);
  } finally {
    proc.kill();
  }
}, 15_000);

test("a child that died within its first minute of serving is held as a boot failure", async () => {
  const port = await freePort();
  const proc = wrapper(port, servesThenExits(1), "exit");
  try {
    await expectHeld(proc, port, 1);
  } finally {
    proc.kill();
  }
}, 15_000);

test("a holding caller serves the diagnostic after a ready child's death", async () => {
  const port = await freePort();
  const proc = wrapper(port, servesThenExits(1), "hold", 1_000);
  try {
    await expectHeld(proc, port, 1);
  } finally {
    proc.kill();
  }
}, 15_000);
