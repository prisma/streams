// What `npm pack` puts in the @prisma/streams tarball, and the metadata npm
// publishes with it (sdk.md section 3.3). Runs after `npm run build`, as
// every test here does: the source-map check reads dist/.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, posix, relative } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const sdkDir = join(dirname(fileURLToPath(import.meta.url)), "..");
const manifest = JSON.parse(readFileSync(join(sdkDir, "package.json"), "utf8"));

// `--ignore-scripts`: list what is there now, without prepack's rebuild.
function packed() {
  const out = execFileSync("npm", ["pack", "--dry-run", "--json", "--ignore-scripts"], {
    cwd: sdkDir,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
  return new Set(JSON.parse(out)[0].files.map(f => f.path));
}

test("the package names its public repository directory, homepage and issue tracker", () => {
  assert.deepEqual(manifest.repository, {
    type: "git",
    url: "git+https://github.com/prisma/streams.git",
    directory: "sdk",
  });
  assert.match(manifest.homepage, /^https:\/\//);
  assert.match(manifest.bugs?.url ?? "", /^https:\/\//);
  assert.ok(Array.isArray(manifest.keywords) && manifest.keywords.length > 0, "keywords");
  assert.equal(manifest.license, "Apache-2.0");
  assert.deepEqual(manifest.engines, { node: ">=18" });
});

test("a publish is public and lands on the beta dist-tag unless told otherwise", () => {
  assert.deepEqual(manifest.publishConfig, { access: "public", tag: "beta" });
});

test("the tarball carries the licence, the changelog, the readme and the sources its maps name", () => {
  const files = packed();
  for (const path of ["package.json", "README.md", "CHANGELOG.md", "LICENSE", "src/index.ts"]) {
    assert.ok(files.has(path), `${path} is packed`);
  }
  for (const path of files) {
    assert.ok(!path.startsWith("scripts/") && path !== "smoke.mjs" && !path.startsWith("node_modules/"),
      `${path} is a test or a dependency, not part of the package`);
  }
  const maps = [...files].filter(path => path.endsWith(".map"));
  assert.ok(maps.length > 0, "build first (npm run build): no source map is packed");
  for (const map of maps) {
    const { sources } = JSON.parse(readFileSync(join(sdkDir, map), "utf8"));
    for (const source of sources) {
      const target = posix.normalize(posix.join(posix.dirname(map), source));
      assert.ok(files.has(target), `${map} names ${source}, which resolves to ${target}: not packed`);
    }
  }
});

test("the packed LICENSE is the repository's licence text", () => {
  const root = join(sdkDir, "..", "LICENSE");
  const own = join(sdkDir, "LICENSE");
  assert.ok(existsSync(own), `${relative(sdkDir, own)} exists`);
  assert.equal(readFileSync(own, "utf8"), readFileSync(root, "utf8"));
});
