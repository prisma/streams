import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Result } from "better-result";
import { IndexBuildWorkerPool } from "../src/index/index_build_worker_pool";
import {
  INDEX_JOB_RSS_SCENARIOS,
  buildIndexJobInputForScenario,
  type IndexJobRssScenario,
} from "./helpers/index_job_rss_scenarios";

const MIB = 1024 * 1024;
const MAX_PARENT_PEAK_CONTRIBUTED_RSS_BYTES = 100 * MIB;

function formatMiB(bytes: number): string {
  return `${(bytes / MIB).toFixed(1)} MiB`;
}

function maybeForceGc(): void {
  const maybeGc = (Bun as unknown as { gc?: (full?: boolean) => void }).gc;
  if (typeof maybeGc !== "function") return;
  try {
    maybeGc(true);
  } catch {
    // best effort only
  }
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function measureParentProcessBuildResult(scenario: IndexJobRssScenario): Promise<{
  baselineRssBytes: number;
  peakRssBytes: number;
  settledRssBytes: number;
}> {
  const root = mkdtempSync(join(tmpdir(), `ds-index-worker-pool-rss-${scenario}-`));
  const pool = new IndexBuildWorkerPool(1);
  pool.start();
  try {
    const job = buildIndexJobInputForScenario(root, scenario);
    maybeForceGc();
    await sleep(25);
    const baselineRssBytes = process.memoryUsage().rss;
    let peakRssBytes = baselineRssBytes;
    const interval = setInterval(() => {
      peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss);
    }, 5);
    try {
      const result = await pool.buildResult(job);
      expect(Result.isOk(result)).toBe(true);
    } finally {
      clearInterval(interval);
    }
    peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss);
    maybeForceGc();
    await sleep(250);
    const settledRssBytes = process.memoryUsage().rss;
    peakRssBytes = Math.max(peakRssBytes, settledRssBytes);
    return { baselineRssBytes, peakRssBytes, settledRssBytes };
  } finally {
    pool.stop();
    rmSync(root, { recursive: true, force: true });
    maybeForceGc();
  }
}

async function measureParentProcessRepeatedBuildsResult(
  scenario: IndexJobRssScenario,
  iterations: number
): Promise<{
  baselineRssBytes: number;
  peakRssBytes: number;
  settledRssBytes: number;
}> {
  const root = mkdtempSync(join(tmpdir(), `ds-index-worker-pool-rss-repeat-${scenario}-`));
  const pool = new IndexBuildWorkerPool(1);
  pool.start();
  try {
    maybeForceGc();
    await sleep(25);
    const baselineRssBytes = process.memoryUsage().rss;
    let peakRssBytes = baselineRssBytes;
    const interval = setInterval(() => {
      peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss);
    }, 5);
    try {
      for (let i = 0; i < iterations; i += 1) {
        const result = await pool.buildResult(buildIndexJobInputForScenario(root, scenario));
        expect(Result.isOk(result)).toBe(true);
        maybeForceGc();
      }
    } finally {
      clearInterval(interval);
    }
    peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss);
    maybeForceGc();
    await sleep(250);
    const settledRssBytes = process.memoryUsage().rss;
    peakRssBytes = Math.max(peakRssBytes, settledRssBytes);
    return { baselineRssBytes, peakRssBytes, settledRssBytes };
  } finally {
    pool.stop();
    rmSync(root, { recursive: true, force: true });
    maybeForceGc();
  }
}

describe("index build worker pool rss isolation", () => {
  for (const scenario of INDEX_JOB_RSS_SCENARIOS) {
    test(
      `keeps parent-process contributed rss bounded for ${scenario}`,
      async () => {
        const measurement = await measureParentProcessBuildResult(scenario);
        const peakContributedRssBytes = Math.max(0, measurement.peakRssBytes - measurement.baselineRssBytes);
        const settledContributedRssBytes = Math.max(0, measurement.settledRssBytes - measurement.baselineRssBytes);

        expect(peakContributedRssBytes).toBeLessThanOrEqual(MAX_PARENT_PEAK_CONTRIBUTED_RSS_BYTES);
        expect(settledContributedRssBytes).toBeLessThanOrEqual(MAX_PARENT_PEAK_CONTRIBUTED_RSS_BYTES);

        // eslint-disable-next-line no-console
        console.log(
          `[index-build-worker-pool-rss] scenario=${scenario} ` +
            `peak=${formatMiB(peakContributedRssBytes)} settled=${formatMiB(settledContributedRssBytes)}`
        );
      },
      120_000
    );
  }

  for (const scenario of ["secondary_l0_build", "search_segment_build_exact_only"] satisfies IndexJobRssScenario[]) {
    test(
      `does not ratchet parent-process rss across repeated ${scenario} subprocess jobs`,
      async () => {
        const measurement = await measureParentProcessRepeatedBuildsResult(scenario, 24);
        const peakContributedRssBytes = Math.max(0, measurement.peakRssBytes - measurement.baselineRssBytes);
        const settledContributedRssBytes = Math.max(0, measurement.settledRssBytes - measurement.baselineRssBytes);

        expect(peakContributedRssBytes).toBeLessThanOrEqual(MAX_PARENT_PEAK_CONTRIBUTED_RSS_BYTES);
        expect(settledContributedRssBytes).toBeLessThanOrEqual(MAX_PARENT_PEAK_CONTRIBUTED_RSS_BYTES);

        // eslint-disable-next-line no-console
        console.log(
          `[index-build-worker-pool-rss-repeat] scenario=${scenario} ` +
            `peak=${formatMiB(peakContributedRssBytes)} settled=${formatMiB(settledContributedRssBytes)}`
        );
      },
      120_000
    );
  }
});
