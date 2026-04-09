import { existsSync, readFileSync } from "node:fs";

export type IndexBuildJobTelemetry = {
  workerPid: number;
  workerRssBaselineBytes: number | null;
  workerRssPeakBytes: number | null;
  workerRssPeakContributedBytes: number | null;
};

type IndexBuildTelemetrySampler = {
  stop: () => Promise<IndexBuildJobTelemetry | null>;
};

const SAMPLE_INTERVAL_MS = 10;

function parseProcStatusRssBytes(statusText: string): number | null {
  const match = statusText.match(/^VmRSS:\s+(\d+)\s+kB$/m);
  if (!match) return null;
  const kb = Number.parseInt(match[1] ?? "", 10);
  return Number.isFinite(kb) && kb >= 0 ? kb * 1024 : null;
}

function readLinuxProcessRssBytes(pid: number): number | null {
  const procStatusPath = `/proc/${pid}/status`;
  if (!existsSync(procStatusPath)) return null;
  try {
    return parseProcStatusRssBytes(readFileSync(procStatusPath, "utf8"));
  } catch {
    return null;
  }
}

function readPortableProcessRssBytes(pid: number): number | null {
  try {
    const res = Bun.spawnSync({
      cmd: ["ps", "-o", "rss=", "-p", String(pid)],
      stdout: "pipe",
      stderr: "ignore",
    });
    if (res.exitCode !== 0) return null;
    const text = new TextDecoder().decode(res.stdout).trim();
    if (text.length === 0) return null;
    const kb = Number.parseInt(text, 10);
    return Number.isFinite(kb) && kb >= 0 ? kb * 1024 : null;
  } catch {
    return null;
  }
}

function readProcessRssBytes(pid: number): number | null {
  return readLinuxProcessRssBytes(pid) ?? readPortableProcessRssBytes(pid);
}

type ProcessTreeSample = {
  pid: number;
  ppid: number;
  rssBytes: number;
};

function readPortableProcessTreeSamples(): ProcessTreeSample[] {
  try {
    const res = Bun.spawnSync({
      cmd: ["ps", "-axo", "pid=,ppid=,rss="],
      stdout: "pipe",
      stderr: "ignore",
    });
    if (res.exitCode !== 0) return [];
    const text = new TextDecoder().decode(res.stdout);
    const samples: ProcessTreeSample[] = [];
    for (const line of text.split("\n")) {
      const trimmed = line.trim();
      if (trimmed.length === 0) continue;
      const match = trimmed.match(/^(\d+)\s+(\d+)\s+(\d+)$/);
      if (!match) continue;
      const pid = Number.parseInt(match[1] ?? "", 10);
      const ppid = Number.parseInt(match[2] ?? "", 10);
      const rssKb = Number.parseInt(match[3] ?? "", 10);
      if (!Number.isFinite(pid) || !Number.isFinite(ppid) || !Number.isFinite(rssKb)) continue;
      samples.push({ pid, ppid, rssBytes: rssKb * 1024 });
    }
    return samples;
  } catch {
    return [];
  }
}

export function readProcessTreeRssBytes(rootPid: number): number | null {
  const samples = readPortableProcessTreeSamples();
  if (samples.length === 0) return readProcessRssBytes(rootPid);
  const childrenByParent = new Map<number, number[]>();
  const rssByPid = new Map<number, number>();
  for (const sample of samples) {
    rssByPid.set(sample.pid, sample.rssBytes);
    const siblings = childrenByParent.get(sample.ppid);
    if (siblings) siblings.push(sample.pid);
    else childrenByParent.set(sample.ppid, [sample.pid]);
  }
  if (!rssByPid.has(rootPid)) return null;
  let total = 0;
  const stack = [rootPid];
  const seen = new Set<number>();
  while (stack.length > 0) {
    const pid = stack.pop()!;
    if (seen.has(pid)) continue;
    seen.add(pid);
    total += rssByPid.get(pid) ?? 0;
    for (const childPid of childrenByParent.get(pid) ?? []) stack.push(childPid);
  }
  return total;
}

export function beginIndexBuildTelemetrySampling(pid: number): IndexBuildTelemetrySampler {
  let baselineRssBytes: number | null = null;
  let peakRssBytes: number | null = null;

  const sample = (): void => {
    const rssBytes = readProcessTreeRssBytes(pid);
    if (rssBytes == null) return;
    if (baselineRssBytes == null) baselineRssBytes = rssBytes;
    if (peakRssBytes == null || rssBytes > peakRssBytes) peakRssBytes = rssBytes;
  };

  sample();
  const interval = setInterval(sample, SAMPLE_INTERVAL_MS);

  return {
    stop: async () => {
      clearInterval(interval);
      sample();
      if (baselineRssBytes == null && peakRssBytes == null) return null;
      const safeBaseline = baselineRssBytes ?? peakRssBytes ?? 0;
      const safePeak = peakRssBytes ?? baselineRssBytes ?? safeBaseline;
      return {
        workerPid: pid,
        workerRssBaselineBytes: baselineRssBytes,
        workerRssPeakBytes: peakRssBytes,
        workerRssPeakContributedBytes:
          baselineRssBytes == null || peakRssBytes == null ? null : Math.max(0, safePeak - safeBaseline),
      };
    },
  };
}
