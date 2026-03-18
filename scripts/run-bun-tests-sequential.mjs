import { readdirSync } from "node:fs";
import { join, relative } from "node:path";
import { spawnSync } from "node:child_process";

const repoRoot = process.cwd();
const testDir = join(repoRoot, "test");

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

for (const file of testFiles) {
  console.log(`\n==> ${file}`);
  const result = spawnSync("bun", ["test", "--max-concurrency=1", file], {
    cwd: repoRoot,
    stdio: "inherit",
    env: process.env,
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
  if (result.signal) {
    process.kill(process.pid, result.signal);
  }
}
