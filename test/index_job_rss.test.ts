import { describe, expect, test } from "bun:test";
import {
  INDEX_JOB_RSS_SCENARIOS,
  type IndexJobRssMeasurement,
  type IndexJobRssScenario,
} from "./helpers/index_job_rss_scenarios";

const MIB = 1024 * 1024;
const MAX_JOB_PEAK_CONTRIBUTED_RSS_BYTES = 100 * MIB;

function formatMiB(bytes: number): string {
  return `${(bytes / MIB).toFixed(1)} MiB`;
}

async function runScenarioInSubprocess(scenario: IndexJobRssScenario): Promise<IndexJobRssMeasurement> {
  const env: Record<string, string> = {};
  for (const [name, value] of Object.entries(process.env)) {
    if (value != null) env[name] = value;
  }

  const proc = Bun.spawn({
    cmd: ["bun", "run", "test/helpers/index_job_rss_runner.ts", scenario],
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
    throw new Error(
      `index job rss child failed scenario=${scenario} exit=${exitCode}\nstdout:\n${stdout}\nstderr:\n${stderr}`
    );
  }

  const marker = "INDEX_JOB_RSS_SUMMARY ";
  const summaryLine = stdout
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .reverse()
    .find((line) => line.startsWith(marker));

  if (!summaryLine) {
    throw new Error(`index job rss child missing summary scenario=${scenario}\nstdout:\n${stdout}\nstderr:\n${stderr}`);
  }

  return JSON.parse(summaryLine.slice(marker.length)) as IndexJobRssMeasurement;
}

describe("index job rss profiles", () => {
  for (const scenario of INDEX_JOB_RSS_SCENARIOS) {
    test(
      `measures ${scenario} peak and settled contributed rss in a clean child process`,
      async () => {
        const measurement = await runScenarioInSubprocess(scenario);

        expect(measurement.scenario).toBe(scenario);
        expect(measurement.durationMs).toBeGreaterThan(0);
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
        expect(measurement.peakContributedRssBytes).toBeLessThanOrEqual(MAX_JOB_PEAK_CONTRIBUTED_RSS_BYTES);

        // eslint-disable-next-line no-console
        console.log(
          `[index-job-rss] scenario=${scenario} duration=${measurement.durationMs.toFixed(1)}ms ` +
            `peak.rss=${formatMiB(measurement.peakContributedRssBytes)} settled.rss=${formatMiB(measurement.settledContributedRssBytes)} ` +
            `peak.heap=${formatMiB(measurement.peakHeapUsedBytes - measurement.baselineHeapUsedBytes)} ` +
            `peak.external=${formatMiB(measurement.peakExternalBytes - measurement.baselineExternalBytes)} ` +
            `peak.arrayBuffers=${formatMiB(measurement.peakArrayBuffersBytes - measurement.baselineArrayBuffersBytes)} ` +
            `baseline.rss=${formatMiB(measurement.baselineRssBytes)}`
        );
      },
      120_000
    );
  }
});
