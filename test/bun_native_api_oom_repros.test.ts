import { describe, expect, test } from "bun:test";
import { createServer } from "node:net";

const MINIO_IMAGE = "minio/minio:latest";
const MC_IMAGE = "minio/mc:latest";
const BUN_IMAGE = "oven/bun:1.3.11";
const MINIO_USER = "minioadmin";
const MINIO_PASSWORD = "minioadmin";
const BUCKET = "streams-rss-test";

function dockerAvailable(): boolean {
  const proc = Bun.spawnSync({
    cmd: ["docker", "info"],
    stdout: "ignore",
    stderr: "ignore",
    cwd: process.cwd(),
  });
  return proc.exitCode === 0;
}

async function findFreePort(): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const server = createServer();
    server.unref();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close();
        reject(new Error("failed to allocate free port"));
        return;
      }
      const port = address.port;
      server.close((err) => {
        if (err) reject(err);
        else resolve(port);
      });
    });
  });
}

function runCommand(cmd: string[], env?: Record<string, string>): string {
  const proc = Bun.spawnSync({
    cmd,
    stdout: "pipe",
    stderr: "pipe",
    cwd: process.cwd(),
    env: env ? { ...process.env, ...env } : process.env,
  });
  const stdout = proc.stdout.toString();
  const stderr = proc.stderr.toString();
  if (proc.exitCode !== 0) {
    throw new Error(`command failed: ${cmd.join(" ")}\nstdout:\n${stdout}\nstderr:\n${stderr}`);
  }
  return stdout;
}

async function waitForMinio(apiPort: number): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://127.0.0.1:${apiPort}/minio/health/live`);
      if (res.ok) return;
    } catch {
      // retry
    }
    await Bun.sleep(250);
  }
  throw new Error("minio did not become healthy in time");
}

function runContainerRepro(args: { scriptPath: string; env?: Record<string, string>; network?: string }): string {
  const envPairs = Object.entries(args.env ?? {}).flatMap(([key, value]) => ["-e", `${key}=${value}`]);
  const networkArgs = args.network ? ["--network", args.network] : [];
  const proc = Bun.spawnSync({
    cmd: [
      "docker",
      "run",
      "--rm",
      "--memory=1g",
      "--memory-swap=1g",
      ...networkArgs,
      ...envPairs,
      "-v",
      `${process.cwd()}:/work`,
      "-w",
      "/work",
      BUN_IMAGE,
      "bun",
      "run",
      args.scriptPath,
    ],
    stdout: "pipe",
    stderr: "pipe",
    cwd: process.cwd(),
    env: process.env,
  });
  const stdout = proc.stdout.toString();
  const stderr = proc.stderr.toString();
  if (proc.exitCode !== 0) {
    throw new Error(`container repro failed: ${args.scriptPath}\nstdout:\n${stdout}\nstderr:\n${stderr}`);
  }
  return stdout;
}

describe("bun native api oom repros in 1 GiB linux containers", () => {
  test(
    "S3 arrayBuffer crosses the threshold inside a 1 GiB container",
    async () => {
      if (!dockerAvailable()) {
        // eslint-disable-next-line no-console
        console.log("[bun-native-api-oom] skipping: docker daemon unavailable");
        return;
      }

      const apiPort = await findFreePort();
      const consolePort = await findFreePort();
      const networkName = `ds-bun-native-oom-${process.pid}-${Date.now()}`;
      const minioName = `ds-bun-native-oom-minio-${process.pid}-${Date.now()}`;

      try {
        runCommand(["docker", "network", "create", networkName]);
        runCommand([
          "docker",
          "run",
          "-d",
          "--rm",
          "--name",
          minioName,
          "--network",
          networkName,
          "-p",
          `${apiPort}:9000`,
          "-p",
          `${consolePort}:9001`,
          "-e",
          `MINIO_ROOT_USER=${MINIO_USER}`,
          "-e",
          `MINIO_ROOT_PASSWORD=${MINIO_PASSWORD}`,
          MINIO_IMAGE,
          "server",
          "/data",
          "--console-address",
          ":9001",
        ]);

        await waitForMinio(apiPort);

        runCommand([
          "docker",
          "run",
          "--rm",
          "--network",
          networkName,
          "--entrypoint",
          "sh",
          MC_IMAGE,
          "-lc",
          [
            `mc alias set local http://${minioName}:9000 ${MINIO_USER} ${MINIO_PASSWORD} >/dev/null`,
            `mc mb -p local/${BUCKET} >/dev/null 2>&1 || true`,
          ].join(" && "),
        ]);

        const stdout = runContainerRepro({
          scriptPath: "test/repros/bun-s3-arraybuffer-oom/repro.ts",
          network: networkName,
          env: {
            S3_TEST_ENDPOINT: `http://${minioName}:9000`,
            S3_TEST_BUCKET: BUCKET,
            S3_TEST_ACCESS_KEY_ID: MINIO_USER,
            S3_TEST_SECRET_ACCESS_KEY: MINIO_PASSWORD,
          },
        });
        expect(stdout).toContain("REPRO_START");
        expect(stdout).toContain("REPRO_BASELINE");
        expect(stdout).toContain("REPRO_THRESHOLD_REACHED");
      } finally {
        Bun.spawnSync({
          cmd: ["docker", "rm", "-f", minioName],
          stdout: "ignore",
          stderr: "ignore",
          cwd: process.cwd(),
        });
        Bun.spawnSync({
          cmd: ["docker", "network", "rm", networkName],
          stdout: "ignore",
          stderr: "ignore",
          cwd: process.cwd(),
        });
      }
    },
    180_000
  );
});
