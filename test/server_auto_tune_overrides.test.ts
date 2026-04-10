import { afterEach, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const roots: string[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) {
    rmSync(root, { recursive: true, force: true });
  }
});

async function readChildText(proc: Bun.Subprocess<"pipe", "pipe", "ignore">): Promise<{ stdout: string; stderr: string }> {
  const [stdout, stderr] = await Promise.all([
    proc.stdout ? new Response(proc.stdout).text() : Promise.resolve(""),
    proc.stderr ? new Response(proc.stderr).text() : Promise.resolve(""),
  ]);
  return { stdout, stderr };
}

test("server starts with auto-tune and explicit async index overrides", async () => {
  const root = mkdtempSync(join(tmpdir(), "server-auto-tune-override-"));
  roots.push(root);
  const port = 21000 + Math.floor(Math.random() * 2000);
  const env: Record<string, string> = {};
  for (const [name, value] of Object.entries(process.env)) {
    if (value != null) env[name] = value;
  }
  env.DS_ROOT = root;
  env.DS_HOST = "127.0.0.1";
  env.PORT = String(port);
  env.DS_INDEX_BUILDERS = "4";
  env.DS_ASYNC_INDEX_CONCURRENCY = "4";

  const proc = Bun.spawn({
    cmd: ["bun", "run", "src/server.ts", "--object-store", "local", "--auto-tune=2048"],
    cwd: process.cwd(),
    env,
    stdout: "pipe",
    stderr: "pipe",
    stdin: "ignore",
  });

  try {
    let healthy = false;
    for (let attempt = 0; attempt < 20; attempt += 1) {
      const exitCode = await Promise.race([proc.exited, Bun.sleep(250).then(() => null as number | null)]);
      if (exitCode != null) {
        const { stdout, stderr } = await readChildText(proc);
        throw new Error(`server exited before becoming healthy exit=${exitCode}\nstdout:\n${stdout}\nstderr:\n${stderr}`);
      }
      try {
        const res = await fetch(`http://127.0.0.1:${port}/health`);
        if (res.ok) {
          expect(await res.json()).toEqual({ ok: true });
          healthy = true;
          break;
        }
      } catch {
        // keep polling until either the process exits or the health endpoint comes up
      }
    }
    expect(healthy).toBe(true);
  } finally {
    proc.kill();
    await proc.exited;
  }
}, 15_000);
