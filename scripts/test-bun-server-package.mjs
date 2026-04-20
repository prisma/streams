import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { spawn, spawnSync } from "node:child_process";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function run(cmd, args, cwd) {
  const result = spawnSync(cmd, args, {
    cwd,
    stdio: "pipe",
    encoding: "utf8",
    env: process.env,
  });
  if (result.status !== 0) {
    if (result.stdout) process.stdout.write(result.stdout);
    if (result.stderr) process.stderr.write(result.stderr);
    throw new Error(`${cmd} ${args.join(" ")} failed with code ${result.status}`);
  }
  return result.stdout.trim();
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForListening(child, timeoutMs) {
  const startedAt = Date.now();
  let stdout = "";
  let stderr = "";

  child.stdout.on("data", (chunk) => {
    stdout += String(chunk);
  });
  child.stderr.on("data", (chunk) => {
    stderr += String(chunk);
  });

  for (;;) {
    const match = stdout.match(/prisma-streams server listening on 127\.0\.0\.1:(\d+)/);
    if (match) {
      return {
        port: Number(match[1]),
        stdout,
        stderr,
      };
    }
    if (child.exitCode != null) {
      throw new Error(`server process exited early: code=${child.exitCode}\nSTDOUT:\n${stdout}\nSTDERR:\n${stderr}`);
    }
    if (Date.now() - startedAt > timeoutMs) {
      throw new Error(`timed out waiting for server startup\nSTDOUT:\n${stdout}\nSTDERR:\n${stderr}`);
    }
    await delay(50);
  }
}

async function waitForExit(child, timeoutMs) {
  const startedAt = Date.now();
  for (;;) {
    if (child.exitCode != null) return child.exitCode;
    if (Date.now() - startedAt > timeoutMs) throw new Error("timed out waiting for server shutdown");
    await delay(50);
  }
}

const tmpRoot = mkdtempSync(join(tmpdir(), "prisma-streams-bun-server-e2e-"));

try {
  run("node", ["scripts/build-npm-packages.mjs"], repoRoot);

  const packDir = join(tmpRoot, "pack");
  const consumerDir = join(tmpRoot, "consumer");
  mkdirSync(packDir, { recursive: true });
  mkdirSync(consumerDir, { recursive: true });

  const serverPackageDir = join(repoRoot, "dist", "npm", "streams-server");
  const tarballPath = run("bun", ["pm", "pack", "--destination", packDir, "--quiet"], serverPackageDir)
    .split(/\r?\n/)
    .filter(Boolean)
    .at(-1);
  if (!tarballPath) throw new Error("bun pm pack did not produce a tarball path");

  writeFileSync(
    join(consumerDir, "package.json"),
    JSON.stringify(
      {
        name: "prisma-streams-bun-server-consumer-smoke",
        private: true,
        type: "module",
      },
      null,
      2
    )
  );

  run("bun", ["add", tarballPath], consumerDir);

  const child = spawn("bun", ["x", "prisma-streams-server", "--object-store", "local"], {
    cwd: consumerDir,
    env: {
      ...process.env,
      PORT: "0",
      DS_HOST: "127.0.0.1",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  try {
    const started = await waitForListening(child, 30_000);
    const health = await fetch(`http://127.0.0.1:${started.port}/health`);
    if (health.status !== 200) {
      throw new Error(`unexpected /health status: ${health.status}`);
    }
  } finally {
    child.kill("SIGTERM");
    const exitCode = await waitForExit(child, 10_000);
    if (exitCode !== 0) {
      throw new Error(`server process exited with code ${exitCode}`);
    }
  }
} finally {
  rmSync(tmpRoot, { recursive: true, force: true });
}
