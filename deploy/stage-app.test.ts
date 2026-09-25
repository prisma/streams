// bench/stage-app.sh: a campaign's staged wrapper directory must hold
// exactly deploy/<app>'s sources plus node_modules (second external review:
// Bun loads a stray `.env` on its own, and `--path .` deploys everything).
// Run with `bun test ./deploy/stage-app.test.ts`. The script runs for real
// against the repo's deploy/app-server, with a stub `bun` first on PATH so
// no install touches the network: it records each call and creates
// node_modules, or fails when STUB_BUN_FAIL is set.
import { afterEach, expect, test } from "bun:test";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
  chmodSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const SCRIPT = join(import.meta.dir, "..", "bench", "stage-app.sh");
const SOURCES = readdirSync(join(import.meta.dir, "app-server"))
  .filter((name) => name !== "node_modules")
  .sort();
const STUB = `#!/bin/bash
echo "$PWD" >> "$STUB_BUN_CALLS"
[ -n "\${STUB_BUN_FAIL:-}" ] && { echo "stub bun: install failed" >&2; exit 1; }
mkdir -p node_modules/stub
`;

const scratch: string[] = [];
afterEach(() => {
  for (const dir of scratch.splice(0)) rmSync(dir, { recursive: true, force: true });
});

/// A scratch root with the stub `bun`, and the staged directory's path in it.
function rig() {
  const root = mkdtempSync(join(tmpdir(), "stage-app-"));
  scratch.push(root);
  mkdirSync(join(root, "bin"));
  writeFileSync(join(root, "bin", "bun"), STUB);
  chmodSync(join(root, "bin", "bun"), 0o755);
  return { root, dir: join(root, "app-server-x"), calls: join(root, "calls") };
}

function stage(r: ReturnType<typeof rig>, fail = false) {
  const env: Record<string, string> = {
    ...process.env,
    PATH: `${join(r.root, "bin")}:${process.env.PATH}`,
    STUB_BUN_CALLS: r.calls,
  };
  if (fail) env.STUB_BUN_FAIL = "1";
  const run = Bun.spawnSync(["bash", SCRIPT, "app-server", r.dir], { env });
  return { code: run.exitCode, stderr: run.stderr.toString() };
}

const installs = (r: ReturnType<typeof rig>) =>
  existsSync(r.calls) ? readFileSync(r.calls, "utf8").trim().split("\n").length : 0;
const listing = (dir: string) => readdirSync(dir).sort();
const leftovers = (r: ReturnType<typeof rig>) =>
  readdirSync(r.root).filter((name) => name.includes(".staging.") || name.includes(".replaced."));

test("a first stage installs once and holds exactly the sources plus node_modules", () => {
  const r = rig();
  expect(stage(r).code).toBe(0);
  expect(listing(r.dir)).toEqual([...SOURCES, "node_modules"].sort());
  expect(installs(r)).toBe(1);
  for (const name of SOURCES) {
    expect(readFileSync(join(r.dir, name), "utf8")).toBe(
      readFileSync(join(import.meta.dir, "app-server", name), "utf8"),
    );
  }
});

test("a restage reuses the install its marker records and refreshes the sources", () => {
  const r = rig();
  expect(stage(r).code).toBe(0);
  writeFileSync(join(r.dir, "supervise.ts"), "// a stale wrapper that holds every death\n");
  expect(stage(r).code).toBe(0);
  expect(installs(r)).toBe(1);
  expect(readFileSync(join(r.dir, "supervise.ts"), "utf8")).toBe(
    readFileSync(join(import.meta.dir, "app-server", "supervise.ts"), "utf8"),
  );
  expect(leftovers(r)).toEqual([]);
});

test("a marker for another manifest installs again", () => {
  const r = rig();
  expect(stage(r).code).toBe(0);
  writeFileSync(join(r.dir, "node_modules", ".stage-app-installed"), "another manifest\n");
  expect(stage(r).code).toBe(0);
  expect(installs(r)).toBe(2);
});

// Each stray entry is refused before any install, named, and left where it
// is: nothing is replaced and nothing is deleted.
for (const [what, plant] of [
  [".env", (dir: string) => writeFileSync(join(dir, ".env"), "FLEET_MIN=9\n")],
  [".env.local", (dir: string) => writeFileSync(join(dir, ".env.local"), "FLEET_MIN=9\n")],
  ["extra", (dir: string) => mkdirSync(join(dir, "extra"))],
  ["supervise.tsx", (dir: string) => writeFileSync(join(dir, "supervise.tsx"), "// stale\n")],
  ["link", (dir: string) => symlinkSync("/etc/hosts", join(dir, "link"))],
  [
    "index.ts",
    (dir: string) => {
      rmSync(join(dir, "index.ts"));
      symlinkSync("/etc/hosts", join(dir, "index.ts"));
    },
  ],
] as [string, (dir: string) => void][]) {
  test(`a staged ${what} is refused before any install`, () => {
    const r = rig();
    expect(stage(r).code).toBe(0);
    plant(r.dir);
    const before = listing(r.dir);
    const run = stage(r);
    expect(run.code).toBe(1);
    expect(run.stderr).toContain(what);
    expect(run.stderr).toContain("refusing to deploy");
    expect(installs(r)).toBe(1);
    expect(listing(r.dir)).toEqual(before);
    expect(leftovers(r)).toEqual([]);
  });
}

test("a failed install refuses and leaves the staged directory as it was", () => {
  const r = rig();
  expect(stage(r).code).toBe(0);
  writeFileSync(join(r.dir, "node_modules", ".stage-app-installed"), "another manifest\n");
  const run = stage(r, true);
  expect(run.code).toBe(1);
  expect(run.stderr).toContain("stub bun: install failed");
  expect(run.stderr).toContain("bun install failed");
  expect(listing(r.dir)).toEqual([...SOURCES, "node_modules"].sort());
  expect(leftovers(r)).toEqual([]);
});
