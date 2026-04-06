import { cpSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, "..");
const distDir = join(repoRoot, "dist");
const distLocalDir = join(distDir, "local");
const distTouchDir = join(distDir, "touch");
const sourceHashVendorDir = join(repoRoot, "src", "runtime", "hash_vendor");

function run(cmd, args) {
  const result = spawnSync(cmd, args, {
    cwd: repoRoot,
    stdio: "inherit",
    env: process.env,
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

function replaceInBuiltLocalFiles(replacements) {
  for (const name of readdirSync(distLocalDir)) {
    if (!name.endsWith(".js")) continue;
    const path = join(distLocalDir, name);
    let text = readFileSync(path, "utf8");
    for (const [from, to] of replacements) {
      text = text.split(from).join(to);
    }
    writeFileSync(path, text);
  }
}

function writeDistReadme() {
  const readme = `# Prisma Streams Local Build

This directory contains the generated Node-compatible package artifacts for the
published \`@prisma/streams-local\` runtime.

## What Local Streams Is

Prisma Streams local is a trusted-development Durable Streams server intended
for embedded workflows such as \`prisma dev\`.

It keeps all state in a single local SQLite database and supports the live /
touch system, but it does not run the full production segmenting and object
store pipeline.

The embedded local runtime always applies the built-in \`1024 MB\` auto-tune
preset, so Prisma CLI gets a predictable cache and concurrency budget and the
same current HTTP surface, including \`GET /v1/server/_details\`.

## Supported Package Surface

- \`@prisma/streams-local\`
- \`@prisma/streams-local/internal/daemon\` (internal Prisma CLI plumbing)

The full self-hosted server remains Bun-only and is not part of this Node build
surface.

## Integrating It

1. Start a named local server from \`@prisma/streams-local\`.
2. Install your touch-enabled \`state-protocol\` profile via \`/_profile\`.
3. Feed normalized State Protocol change events into the server.
4. Use \`/touch/meta\` and \`/touch/wait\` to drive invalidation.

Programmatic example:

\`\`\`ts
import { startLocalDurableStreamsServer } from "@prisma/streams-local";

const server = await startLocalDurableStreamsServer({
  name: "default",
  hostname: "127.0.0.1",
  port: 0,
});

console.log(server.exports.http.url);
console.log(server.exports.sqlite.path);

await server.close();
\`\`\`

Daemon example:

\`\`\`ts
import { fork } from "node:child_process";

const child = fork(require.resolve("@prisma/streams-local/internal/daemon"), [
  "--name",
  "default",
  "--port",
  "0",
], {
  stdio: "inherit",
});
\`\`\`

See ../docs/overview.md and ../docs/local-dev.md for the full runtime and release
documentation.
`;

  writeFileSync(join(distDir, "README.md"), readme);
}

rmSync(distDir, { recursive: true, force: true });
mkdirSync(distLocalDir, { recursive: true });
mkdirSync(distTouchDir, { recursive: true });

run("bun", [
  "build",
  "./src/local/index.ts",
  "./src/local/daemon.ts",
  "--target=node",
  "--format=esm",
  "--splitting",
  "--packages=external",
  "--outdir",
  "./dist/local",
]);
run("bun", ["build", "./src/touch/processor_worker.ts", "--target=node", "--format=esm", "--packages=external", "--outdir", "./dist/touch"]);
run("bunx", ["tsc", "--project", "./tsconfig.build.types.json"]);

cpSync(sourceHashVendorDir, join(distLocalDir, "hash_vendor"), { recursive: true });
cpSync(sourceHashVendorDir, join(distTouchDir, "hash_vendor"), { recursive: true });

const localReplacements = [
  ['new URL("./processor_worker.ts", import.meta.url)', 'new URL("../touch/processor_worker.js", import.meta.url)'],
];

replaceInBuiltLocalFiles(localReplacements);
writeDistReadme();
