// The opt-in end-to-end tests' rig (dev.e2e.test.ts, drills.e2e.test.ts):
// a shared cell on the real binary over s3lite with cell-admin's feeds, as
// cell-admin's e2e test boots it, and the watch's app in `prisma dev`,
// deployed by Composer's local target from app.ts. Every process and
// server it starts is ended by `teardown` (the tests' afterAll). Like
// every other file of the watch it reads no environment: the tests hand
// it the shell's PATH and HOME (composer.test.ts holds the directory to
// that).
import { randomBytes } from "node:crypto";
import { existsSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { DEFAULT_PROFILES } from "../cell-admin/commands";
import type { Bundle } from "../cell-admin/feeds";
import { readProfiles } from "../cell-admin/profile";
import { cleanup, readBundle, scratchDir } from "../cell-admin/test-rig";

const HERE = import.meta.dir;

/// What the rig's processes inherit from the test's shell.
export type Shell = Readonly<Record<"PATH" | "HOME", string>>;

const children: ReturnType<typeof Bun.spawn>[] = [];
const stops: (() => void)[] = [];
const watches: DevWatch[] = [];

/// Ends every watch, process and server the rig started, newest first.
export async function teardown(): Promise<void> {
  for (const w of watches.splice(0).reverse()) await w.stop();
  for (const c of children.splice(0).reverse()) await ended(c, "SIGINT");
  for (const s of stops.splice(0).reverse()) s();
  cleanup();
}

/// `child` sent `signal`, then killed if it has not exited within `graceMs`.
async function ended(child: ReturnType<typeof Bun.spawn>, signal: NodeJS.Signals, graceMs = 10_000): Promise<void> {
  child.kill(signal);
  await Promise.race([child.exited, Bun.sleep(graceMs)]);
  child.kill("SIGKILL");
  await child.exited;
}

/// `stop` runs at teardown.
export function onTeardown(stop: () => void): void {
  stops.push(stop);
}

export function freePort(): number {
  const s = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: () => new Response() });
  const port = Number(s.port);
  s.stop(true);
  return port;
}

/// `probe`'s first answer that is not undefined, looked for every 250 ms for `secs`.
export async function until<T>(what: string, secs: number, probe: () => Promise<T | undefined> | T | undefined): Promise<T> {
  for (let i = 0; i < secs * 4; i++) {
    const v = await Promise.resolve(probe()).catch(() => undefined);
    if (v !== undefined) return v;
    await Bun.sleep(250);
  }
  throw new Error(`timed out waiting for ${what}`);
}

/// One feed file replaced atomically, as deploy/app-server does.
export function writeFeed(feeds: string, name: "keys" | "policies" | "grants", doc: unknown): void {
  writeFileSync(join(feeds, `${name}.json.tmp`), JSON.stringify(doc));
  renameSync(join(feeds, `${name}.json.tmp`), join(feeds, `${name}.json`));
}

/// What deploy/app-server does with the bundle: three files, each replaced atomically.
export function publish(stateDir: string, feeds: string): Bundle {
  const b = readBundle(stateDir);
  writeFeed(feeds, "keys", b.keys);
  writeFeed(feeds, "policies", b.policies);
  writeFeed(feeds, "grants", b.grants);
  return b;
}

/// s3lite on a free port, at 2 ms; its endpoint.
export function startS3(bin: string): string {
  const port = freePort();
  children.push(Bun.spawn([join(bin, "s3lite"), "--listen", `127.0.0.1:${port}`, "--latency-ms", "2"], { stdout: "ignore", stderr: "ignore" }));
  return `http://127.0.0.1:${port}`;
}

export interface CellSpec {
  shell: Shell;
  bin: string;
  port: number;
  s3: string;
  bucket: string;
  feeds: string;
  log: string;
  /// The deployment bearer (AUTH_TOKEN).
  operator: string;
  /// Settings over the shipped profiles'.
  env?: Record<string, string>;
}

export interface LocalCell {
  child: ReturnType<typeof Bun.spawn>;
  origin: string;
  git: string;
  boot: string;
}

/// The shared cell `spec` names: started, and ready once /health answers 200.
export async function startCell(spec: CellSpec): Promise<LocalCell> {
  const env: Record<string, string> = {
    ...spec.shell,
    ...Object.fromEntries(readProfiles(DEFAULT_PROFILES)),
    STREAMS_AUTH_KEYS_FILE: join(spec.feeds, "keys.json"),
    STREAMS_AUTH_POLICY_FILE: join(spec.feeds, "policies.json"),
    STREAMS_AUTH_GRANTS_FILE: join(spec.feeds, "grants.json"),
    CELL_ID: "cell_sc1",
    PROJECT_ID: "proj_deploy",
    ACCOUNT_ID: "acct_sink",
    USAGE_STREAM_KEY: randomBytes(32).toString("base64url"),
    AUTH_TOKEN: spec.operator,
    ...spec.env,
  };
  const argv = [join(spec.bin, "streams-slate"), "--listen", `127.0.0.1:${spec.port}`, "--s3-endpoint", spec.s3, "--bucket", spec.bucket];
  const child = Bun.spawn(argv, { env, stdout: Bun.file(spec.log), stderr: Bun.file(spec.log) });
  children.push(child);
  const origin = `http://127.0.0.1:${spec.port}`;
  const health = await until("the cell to be ready", 90, async () => {
    const res = await fetch(`${origin}/health`);
    return res.ok ? res : undefined;
  });
  return { child, origin, git: health.headers.get("x-streams-git") ?? "", boot: health.headers.get("x-streams-boot-id") ?? "" };
}

/// Stops `cell` as the platform stops an instance (SIGTERM) and waits for
/// its ordered stop (bounded at 30 s; killed after 60).
export async function stopCell(cell: LocalCell): Promise<void> {
  await ended(cell.child, "SIGTERM", 60_000);
}

export interface DevWatch {
  /// The service's endpoint.
  url: string;
  /// What `prisma dev` printed so far.
  out: () => string;
  /// The service's own log (the emulator's).
  log: () => string;
  /// Ends `prisma dev` and the compute daemon it started: the watch is gone.
  stop: () => Promise<void>;
}

/// `prisma dev app.ts` with the watch's inputs in its shell, in an emulator
/// directory of its own (`--fresh`); entry.js is bundled first, since
/// `prisma dev` builds nothing.
export async function startWatch(shell: Shell, inputs: Record<string, string>): Promise<DevWatch> {
  const built = await Bun.build({ entrypoints: [join(HERE, "entry.ts")], target: "bun", format: "esm", outdir: HERE, naming: "entry.js" });
  if (!built.success) throw new Error(`bun build failed: ${built.logs.join("\n")}`);
  const emulators = scratchDir();
  const dev = Bun.spawn([join(HERE, "node_modules", ".bin", "prisma"), "dev", "app.ts", "--fresh", "--no-interactive", "--no-color"], {
    cwd: HERE,
    env: { ...shell, PRISMA_COMPOSER_EMULATORS_DIR: emulators, ...inputs },
    stdout: "pipe",
    stderr: "pipe",
  });
  const out: string[] = [];
  const pump = async (s: ReadableStream<Uint8Array>) => {
    for await (const chunk of s) out.push(new TextDecoder().decode(chunk));
  };
  void pump(dev.stdout as ReadableStream<Uint8Array>);
  void pump(dev.stderr as ReadableStream<Uint8Array>);
  let stopped = false;
  const watch: DevWatch = {
    url: "",
    out: () => out.join(""),
    log: () => readFileSync(join(emulators, "compute", "logs", "streams-cell-watch", "cellwatch-watch.log"), "utf8"),
    stop: async () => {
      if (stopped) return;
      stopped = true;
      await ended(dev, "SIGINT");
      // The compute daemon `prisma dev` started for this emulator directory.
      const daemon = join(emulators, "compute.json");
      if (existsSync(daemon)) {
        try {
          process.kill(JSON.parse(readFileSync(daemon, "utf8")).pid, "SIGTERM");
        } catch {}
      }
    },
  };
  watches.push(watch);
  watch.url = await until(`prisma dev's endpoint line (output so far: ${out.join("").slice(-2000)})`, 120, () =>
    /"kind":"endpoint","name":"cellwatch\.watch","url":"([^"]+)"/.exec(out.join(""))?.[1]);
  return watch;
}
