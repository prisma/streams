import { describe, expect, test } from "bun:test";
import { createServer } from "node:net";
import {
  BUN_NATIVE_API_RSS_SCENARIOS,
  type BunNativeApiRssMeasurement,
  type BunNativeApiRssScenario,
} from "./helpers/bun_native_api_rss_scenarios";

const MINIO_IMAGE = "minio/minio:latest";
const MC_IMAGE = "minio/mc:latest";
const MINIO_USER = "minioadmin";
const MINIO_PASSWORD = "minioadmin";
const BUCKET = "streams-rss-test";
const MIB = 1024 * 1024;

const LOCAL_SCENARIOS = BUN_NATIVE_API_RSS_SCENARIOS.filter((scenario) => !scenario.startsWith("s3_"));
const S3_SCENARIOS = BUN_NATIVE_API_RSS_SCENARIOS.filter((scenario) => scenario.startsWith("s3_"));

function formatMiB(bytes: number): string {
  return `${(bytes / MIB).toFixed(1)} MiB`;
}

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

async function runScenarioInSubprocess(
  scenario: BunNativeApiRssScenario,
  extraEnv: Record<string, string> = {}
): Promise<BunNativeApiRssMeasurement> {
  const env: Record<string, string> = {};
  for (const [name, value] of Object.entries(process.env)) {
    if (value != null) env[name] = value;
  }
  Object.assign(env, extraEnv);

  const proc = Bun.spawn({
    cmd: ["bun", "run", "test/helpers/bun_native_api_rss_runner.ts", scenario],
    cwd: process.cwd(),
    env,
    stdout: "pipe",
    stderr: "pipe",
  });

  const [stdout, stderr, exitCode] = await Promise.all([
    proc.stdout ? new Response(proc.stdout).text() : Promise.resolve(""),
    proc.stderr ? new Response(proc.stderr).text() : Promise.resolve(""),
    proc.exited,
  ]);

  if (exitCode !== 0) {
    throw new Error(`bun native api rss child failed scenario=${scenario} exit=${exitCode}\nstdout:\n${stdout}\nstderr:\n${stderr}`);
  }

  const marker = "BUN_NATIVE_API_RSS_SUMMARY ";
  const summaryLine = stdout
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .reverse()
    .find((line) => line.startsWith(marker));
  if (!summaryLine) {
    throw new Error(`bun native api rss child missing summary scenario=${scenario}\nstdout:\n${stdout}\nstderr:\n${stderr}`);
  }
  return JSON.parse(summaryLine.slice(marker.length)) as BunNativeApiRssMeasurement;
}

function assertMeasurementSanity(measurement: BunNativeApiRssMeasurement, scenario: BunNativeApiRssScenario): void {
  expect(measurement.scenario).toBe(scenario);
  expect(measurement.durationMs).toBeGreaterThan(0);
  expect(measurement.payloadBytes).toBeGreaterThan(0);
  expect(measurement.iterations).toBeGreaterThan(0);
  expect(measurement.productionUse.length).toBeGreaterThan(0);
  expect(measurement.baselineRssBytes).toBeGreaterThan(0);
  expect(measurement.peakRssBytes).toBeGreaterThanOrEqual(measurement.baselineRssBytes);
  expect(measurement.settledRssBytes).toBeGreaterThan(0);
  expect(measurement.peakContributedRssBytes).toBeGreaterThanOrEqual(0);
  expect(measurement.settledContributedRssBytes).toBeGreaterThanOrEqual(0);
  expect(measurement.settledContributedRssBytes).toBeLessThanOrEqual(measurement.peakContributedRssBytes);
  expect(measurement.baselineHeapUsedBytes).toBeGreaterThan(0);
  expect(measurement.peakHeapUsedBytes).toBeGreaterThanOrEqual(measurement.baselineHeapUsedBytes);
  expect(measurement.settledHeapUsedBytes).toBeGreaterThan(0);
  expect(measurement.baselineExternalBytes).toBeGreaterThanOrEqual(0);
  expect(measurement.peakExternalBytes).toBeGreaterThanOrEqual(measurement.baselineExternalBytes);
  expect(measurement.settledExternalBytes).toBeGreaterThanOrEqual(0);
  expect(measurement.baselineArrayBuffersBytes).toBeGreaterThanOrEqual(0);
  expect(measurement.peakArrayBuffersBytes).toBeGreaterThanOrEqual(measurement.baselineArrayBuffersBytes);
  expect(measurement.settledArrayBuffersBytes).toBeGreaterThanOrEqual(0);
  expect(measurement.peakAccountedBytes).toBeGreaterThanOrEqual(0);
  expect(measurement.settledAccountedBytes).toBeGreaterThanOrEqual(0);
  expect(measurement.peakUnaccountedBytes).toBeGreaterThanOrEqual(0);
  expect(measurement.settledUnaccountedBytes).toBeGreaterThanOrEqual(0);
}

function logMeasurement(measurement: BunNativeApiRssMeasurement): void {
  // eslint-disable-next-line no-console
  console.log(
    `[bun-native-api-rss] scenario=${measurement.scenario} duration=${measurement.durationMs.toFixed(1)}ms ` +
      `peak.rss=${formatMiB(measurement.peakContributedRssBytes)} settled.rss=${formatMiB(measurement.settledContributedRssBytes)} ` +
      `peak.accounted=${formatMiB(measurement.peakAccountedBytes)} settled.accounted=${formatMiB(measurement.settledAccountedBytes)} ` +
      `peak.unaccounted=${formatMiB(measurement.peakUnaccountedBytes)} settled.unaccounted=${formatMiB(measurement.settledUnaccountedBytes)} ` +
      `use=\"${measurement.productionUse}\"`
  );
}

describe("bun native api rss micro-benchmarks", () => {
  for (const scenario of LOCAL_SCENARIOS) {
    test(
      `measures ${scenario} in a clean child process`,
      async () => {
        const measurement = await runScenarioInSubprocess(scenario);
        assertMeasurementSanity(measurement, scenario);
        logMeasurement(measurement);
      },
      120_000
    );
  }

  test(
    "measures S3-backed Bun byte paths against a real S3-compatible backend",
    async () => {
      if (!dockerAvailable()) {
        // eslint-disable-next-line no-console
        console.log("[bun-native-api-rss] skipping S3-backed scenarios: docker daemon unavailable");
        return;
      }

      const apiPort = await findFreePort();
      const consolePort = await findFreePort();
      const containerName = `ds-bun-native-rss-${process.pid}-${Date.now()}`;

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

        for (const scenario of S3_SCENARIOS) {
          const measurement = await runScenarioInSubprocess(scenario, env);
          assertMeasurementSanity(measurement, scenario);
          logMeasurement(measurement);
        }
      } finally {
        Bun.spawnSync({
          cmd: ["docker", "rm", "-f", containerName],
          stdout: "ignore",
          stderr: "ignore",
          cwd: process.cwd(),
        });
      }
    },
    240_000
  );
});
