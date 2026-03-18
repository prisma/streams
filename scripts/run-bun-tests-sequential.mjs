import { readdirSync } from "node:fs";
import { join, relative } from "node:path";
import { spawnSync } from "node:child_process";

const repoRoot = process.cwd();
const testDir = join(repoRoot, "test");
const maxCrashAttempts = 2;

function collectTests(dir) {
  const out = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const fullPath = join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...collectTests(fullPath));
      continue;
    }
    if (entry.isFile() && entry.name.endsWith(".test.ts")) {
      out.push(relative(repoRoot, fullPath));
    }
  }
  return out.sort();
}

const testFiles = collectTests(testDir);

function isBunCrash(text) {
  return (
    text.includes("oh no: Bun has crashed.") ||
    text.includes("panic: Segmentation fault") ||
    text.includes("panic(main thread): Segmentation fault") ||
    text.includes("Illegal instruction")
  );
}

function runTestFile(file) {
  for (let attempt = 1; attempt <= maxCrashAttempts; attempt++) {
    const result = spawnSync("bun", ["test", "--max-concurrency=1", file], {
      cwd: repoRoot,
      stdio: "pipe",
      encoding: "utf8",
      env: process.env,
    });
    process.stdout.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");

    if (result.status === 0 && !result.signal) {
      return;
    }

    const combinedOutput = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;
    if (attempt < maxCrashAttempts && (result.signal || isBunCrash(combinedOutput))) {
      console.warn(`retrying ${file} after Bun crash (attempt ${attempt + 1}/${maxCrashAttempts})`);
      continue;
    }

    if (result.signal) {
      process.kill(process.pid, result.signal);
    }
    process.exit(result.status ?? 1);
  }
}

for (const file of testFiles) {
  console.log(`\n==> ${file}`);
  runTestFile(file);
}
