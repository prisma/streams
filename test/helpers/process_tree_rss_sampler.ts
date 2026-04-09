import { existsSync, writeFileSync } from "node:fs";
import { readProcessTreeRssBytes } from "../../src/index/index_build_telemetry";

type SamplerOutput = {
  peakRssBytes: number | null;
};

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function main(): Promise<void> {
  const pidText = process.argv[2];
  const stopPath = process.argv[3];
  const outputPath = process.argv[4];
  if (!pidText || !stopPath || !outputPath) {
    throw new Error("process tree rss sampler requires pid, stop path, and output path");
  }
  const pid = Number.parseInt(pidText, 10);
  if (!Number.isFinite(pid) || pid <= 0) throw new Error("invalid pid for process tree rss sampler");

  let peakRssBytes: number | null = null;
  while (true) {
    const rssBytes = readProcessTreeRssBytes(pid);
    if (rssBytes == null) break;
    const samplerRssBytes = readProcessTreeRssBytes(process.pid) ?? 0;
    const measuredRssBytes = Math.max(0, rssBytes - samplerRssBytes);
    if (peakRssBytes == null || measuredRssBytes > peakRssBytes) peakRssBytes = measuredRssBytes;
    if (existsSync(stopPath)) break;
    await sleep(5);
  }
  writeFileSync(outputPath, JSON.stringify({ peakRssBytes } satisfies SamplerOutput));
}

try {
  await main();
  process.exit(0);
} catch (error: unknown) {
  // eslint-disable-next-line no-console
  console.error(error);
  process.exit(1);
}
