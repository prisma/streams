import { describe, expect, test } from "bun:test";
import { createServer } from "node:net";

type UploadRssSummary = {
  mode: "presign-fetch" | "native-write";
  iterations: number;
  payloadBytes: number;
  baselineRssBytes: number;
  peakRssBytes: number;
  settledRssBytes: number;
  peakContributedRssBytes: number;
  settledContributedRssBytes: number;
};

const MINIO_IMAGE = "minio/minio:latest";
const MC_IMAGE = "minio/mc:latest";
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

async function runScenario(mode: UploadRssSummary["mode"], env: Record<string, string>): Promise<UploadRssSummary> {
  const proc = Bun.spawn({
    cmd: ["bun", "run", "test/helpers/s3_upload_rss_runner.ts", mode],
    cwd: process.cwd(),
    env: { ...process.env, ...env, NO_COLOR: "1" },
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  if (exitCode !== 0) {
    throw new Error(`s3 upload RSS runner failed mode=${mode} exit=${exitCode}\nstdout:\n${stdout}\nstderr:\n${stderr}`);
  }
  const marker = "S3_UPLOAD_RSS_SUMMARY ";
  const summaryLine = stdout
    .trim()
    .split(/\n+/)
    .reverse()
    .find((line) => line.startsWith(marker));
  if (!summaryLine) {
    throw new Error(`s3 upload RSS runner missing summary mode=${mode}\nstdout:\n${stdout}\nstderr:\n${stderr}`);
  }
  return JSON.parse(summaryLine.slice(marker.length)) as UploadRssSummary;
}

describe("r2 upload rss", () => {
  test(
    "measures presigned fetch and native S3 upload RSS under a real S3-compatible backend",
    async () => {
      if (!dockerAvailable()) {
        // eslint-disable-next-line no-console
        console.log("[r2-upload-rss] skipping: docker daemon unavailable");
        return;
      }

      const apiPort = await findFreePort();
      const consolePort = await findFreePort();
      const containerName = `ds-minio-rss-${process.pid}-${Date.now()}`;

      try {
        runCommand([
          "docker",
          "run",
          "-d",
          "--rm",
          "--name",
          containerName,
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
          `container:${containerName}`,
          "--entrypoint",
          "sh",
          MC_IMAGE,
          "-lc",
          [
            `mc alias set local http://127.0.0.1:9000 ${MINIO_USER} ${MINIO_PASSWORD} >/dev/null`,
            `mc mb -p local/${BUCKET} >/dev/null 2>&1 || true`,
          ].join(" && "),
        ]);

        const env = {
          S3_TEST_ENDPOINT: `http://127.0.0.1:${apiPort}`,
          S3_TEST_BUCKET: BUCKET,
          S3_TEST_ACCESS_KEY_ID: MINIO_USER,
          S3_TEST_SECRET_ACCESS_KEY: MINIO_PASSWORD,
        };

        const legacy = await runScenario("presign-fetch", env);
        const native = await runScenario("native-write", env);

        expect(legacy.settledContributedRssBytes).toBeGreaterThanOrEqual(0);
        expect(native.settledContributedRssBytes).toBeGreaterThanOrEqual(0);

        // eslint-disable-next-line no-console
        console.log(
          `[r2-upload-rss] presign-fetch settled=${legacy.settledContributedRssBytes} peak=${legacy.peakContributedRssBytes} ` +
            `native-write settled=${native.settledContributedRssBytes} peak=${native.peakContributedRssBytes}`
        );
      } finally {
        Bun.spawnSync({
          cmd: ["docker", "rm", "-f", containerName],
          stdout: "ignore",
          stderr: "ignore",
          cwd: process.cwd(),
        });
      }
    },
    120_000
  );
});
