import { chmodSync, cpSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(__dirname, "..");
const docsDir = join(repoRoot, "docs");
const distDir = join(repoRoot, "dist");
const distNpmDir = join(distDir, "npm");
const localPackageDir = join(distNpmDir, "streams-local");
const serverPackageDir = join(distNpmDir, "streams-server");
const rootPackage = JSON.parse(readFileSync(join(repoRoot, "package.json"), "utf8"));
const repository = rootPackage.repository;
const bugs = rootPackage.bugs;
const homepage = rootPackage.homepage;

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

function writeJson(path, value) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`);
}

function copyCommonDocs(destDir, readmeText) {
  mkdirSync(destDir, { recursive: true });
  writeFileSync(join(destDir, "README.md"), readmeText);
  cpSync(join(repoRoot, "LICENSE"), join(destDir, "LICENSE"));
  cpSync(join(docsDir, "security.md"), join(destDir, "SECURITY.md"));
  cpSync(join(docsDir, "contributing.md"), join(destDir, "CONTRIBUTING.md"));
  cpSync(join(docsDir, "code-of-conduct.md"), join(destDir, "CODE_OF_CONDUCT.md"));
}

function copyDir(src, dest, filter = () => true) {
  cpSync(src, dest, {
    recursive: true,
    filter: (source) => filter(source),
  });
}

function writeServerBin(destDir) {
  const binDir = join(destDir, "bin");
  const binPath = join(binDir, "prisma-streams-server");
  mkdirSync(binDir, { recursive: true });
  writeFileSync(
    binPath,
    `#!/usr/bin/env bun
import "../src/server.ts";
`
  );
  chmodSync(binPath, 0o755);
}

function copyLocalTypes(destDir) {
  const localTypesDir = join(destDir, "dist", "types", "local");
  mkdirSync(localTypesDir, { recursive: true });
  for (const name of ["index.d.ts", "server.d.ts", "daemon.d.ts"]) {
    cpSync(join(distDir, "types", "local", name), join(localTypesDir, name));
  }
}

function buildLocalPackage() {
  const readme = readFileSync(join(distDir, "README.md"), "utf8");
  copyCommonDocs(localPackageDir, readme);
  mkdirSync(join(localPackageDir, "dist"), { recursive: true });
  cpSync(join(distDir, "README.md"), join(localPackageDir, "dist", "README.md"));
  copyDir(join(distDir, "local"), join(localPackageDir, "dist", "local"));
  copyDir(join(distDir, "touch"), join(localPackageDir, "dist", "touch"));
  copyLocalTypes(localPackageDir);

  writeJson(join(localPackageDir, "package.json"), {
    name: "@prisma/streams-local",
    version: rootPackage.version,
    description: "Node and Bun local Prisma Streams runtime for trusted development workflows.",
    repository,
    bugs,
    homepage,
    license: rootPackage.license,
    type: "module",
    engines: {
      bun: rootPackage.engines.bun,
      node: rootPackage.engines.node,
    },
    publishConfig: {
      access: "public",
    },
    dependencies: rootPackage.dependencies,
    files: ["README.md", "LICENSE", "SECURITY.md", "CONTRIBUTING.md", "CODE_OF_CONDUCT.md", "dist/"],
    exports: {
      ".": {
        types: "./dist/types/local/index.d.ts",
        default: "./dist/local/index.js",
      },
      "./internal/daemon": {
        types: "./dist/types/local/daemon.d.ts",
        default: "./dist/local/daemon.js",
      },
      "./package.json": "./package.json",
    },
  });
}

function buildServerPackage() {
  const readme = `# @prisma/streams-server

This package contains the Bun-only self-hosted Prisma Streams server.

## What It Is

\`@prisma/streams-server\` is the full Prisma Streams runtime: SQLite WAL
storage, segmenting, upload support, indexing, recovery, and the live / touch
system.

It is intended for Bun-based self-hosted deployment. For trusted local
development embedding, use \`@prisma/streams-local\` instead.

## Running It

Recommended:

\`\`\`bash
bunx --package @prisma/streams-server prisma-streams-server --object-store local
\`\`\`

After installation in a project:

\`\`\`bash
bun x prisma-streams-server --object-store local
\`\`\`

Useful environment variables:

- \`PORT\`
- \`DS_HOST\`
- \`DS_HTTP_IDLE_TIMEOUT_SECONDS\`

For R2 mode set:

- \`DURABLE_STREAMS_R2_BUCKET\`
- \`DURABLE_STREAMS_R2_ACCOUNT_ID\`
- \`DURABLE_STREAMS_R2_ACCESS_KEY_ID\`
- \`DURABLE_STREAMS_R2_SECRET_ACCESS_KEY\`

See ../docs/overview.md and ../docs/conformance.md in the repository for the full
runtime documentation.
`;

  copyCommonDocs(serverPackageDir, readme);
  copyDir(join(repoRoot, "src"), join(serverPackageDir, "src"), (source) => !source.includes(`${join(repoRoot, "src", "local")}`));
  writeServerBin(serverPackageDir);

  writeJson(join(serverPackageDir, "package.json"), {
    name: "@prisma/streams-server",
    version: rootPackage.version,
    description: "Bun-only self-hosted Prisma Streams server.",
    repository,
    bugs,
    homepage,
    license: rootPackage.license,
    type: "module",
    engines: {
      bun: rootPackage.engines.bun,
    },
    publishConfig: {
      access: "public",
    },
    files: ["README.md", "LICENSE", "SECURITY.md", "CONTRIBUTING.md", "CODE_OF_CONDUCT.md", "bin/", "src/"],
    bin: {
      "prisma-streams-server": "./bin/prisma-streams-server",
    },
    exports: {
      ".": "./src/server.ts",
      "./package.json": "./package.json",
    },
    dependencies: rootPackage.dependencies,
  });
}

run("node", ["scripts/build-local-node.mjs"]);
rmSync(distNpmDir, { recursive: true, force: true });
mkdirSync(distNpmDir, { recursive: true });
buildLocalPackage();
buildServerPackage();
