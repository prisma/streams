import { execFile } from "node:child_process";
import { access, mkdir, rm } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { parseArgs, promisify } from "node:util";

const execFileAsync = promisify(execFile);

export const DEFAULT_COMPUTE_BUNDLE_OUTDIR = ".compute-build/bundle";
export const COMPUTE_BUNDLE_ENTRYPOINTS = [
  "src/compute/entry.ts",
  "src/segment/segmenter_worker.ts",
  "src/touch/processor_worker.ts",
];

export async function buildComputeBundle(options = {}) {
  const cwd = path.resolve(options.cwd ?? process.cwd());
  const outDir = path.resolve(cwd, options.outDir ?? DEFAULT_COMPUTE_BUNDLE_OUTDIR);
  const entrypoints = COMPUTE_BUNDLE_ENTRYPOINTS.map((entrypoint) => path.resolve(cwd, entrypoint));

  await rm(outDir, { recursive: true, force: true });
  await mkdir(outDir, { recursive: true });

  try {
    await execFileAsync(
      "bun",
      [
        "build",
        ...entrypoints,
        "--outdir",
        outDir,
        "--target",
        "bun",
        "--sourcemap=external",
      ],
      { cwd }
    );
  } catch (error) {
    const stderr = error?.stderr?.trim?.();
    const message = stderr && stderr.length > 0 ? stderr : error instanceof Error ? error.message : String(error);
    throw new Error(`bun build failed:\n${message}`);
  }

  await access(path.join(outDir, "compute", "entry.js"));
  await access(path.join(outDir, "segment", "segmenter_worker.js"));
  await access(path.join(outDir, "touch", "processor_worker.js"));

  return { bundleDir: outDir, entrypoint: "compute/entry.js" };
}

const isMain = process.argv[1] != null && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (isMain) {
  const { values } = parseArgs({
    args: process.argv.slice(2),
    options: {
      outdir: { type: "string" },
    },
    allowPositionals: false,
  });

  const { bundleDir, entrypoint } = await buildComputeBundle({ outDir: values.outdir });
  console.log(`Built Compute bundle at ${bundleDir} (entrypoint ${entrypoint})`);
}
