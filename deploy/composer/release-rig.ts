// Shared rig for the release tools' offline self-tests (release.test.ts,
// deploy-release.test.ts): scratch directories, a git repository holding
// the repository's own release files at one commit, ELF headers that never
// run, a fake `prisma` CLI, and releases staged from them by release.ts
// exactly as cut-release stages a real one.
import { execFileSync } from "node:child_process";
import { chmodSync, copyFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { type Release, type StageInput, stageRelease } from "./release";

export const REPO = join(import.meta.dir, "..", "..");

const scratch: string[] = [];

export function scratchDir(): string {
  const dir = mkdtempSync(join(tmpdir(), "release-"));
  scratch.push(dir);
  return dir;
}

export function cleanupScratch(): void {
  for (const dir of scratch.splice(0)) rmSync(dir, { recursive: true, force: true });
}

/// An ELF header for `machine` that never runs.
export const elf = (machine: number, tag: string) =>
  Buffer.concat([Buffer.from([0x7f, 0x45, 0x4c, 0x46, 2, 1, 1, ...new Array(9).fill(0), 2, 0, machine, 0]), Buffer.from(tag)]);

/// The repository files a release takes, and the three it reads facts from.
export const TREE_FILES = [
  "deploy/composer/package.json",
  "deploy/composer/bun.lock",
  "deploy/composer/prisma.config.ts",
  "deploy/composer/destroy.ts",
  "deploy/composer/app.ts",
  "deploy/composer/streams/service.ts",
  "deploy/composer/streams/entry.ts",
  "deploy/composer/streams/env.ts",
  "deploy/app-server/supervise.ts",
  "deploy/app-server/feeds.ts",
  "deploy/app-server/downloader.ts",
  "deploy/cell-admin/profile.ts",
  "deploy/cell-admin/errors.ts",
  "deploy/profiles/compute-1g.env",
  "deploy/profiles/shared-cell.env",
  "src/registry.rs",
  "contracts/streams-platform/v1/keys.schema.json",
  "docs/MULTITENANCY.md",
];

export function git(dir: string, ...args: string[]): string {
  return execFileSync("git", ["-C", dir, "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false", ...args])
    .toString()
    .trim();
}

/// A git repository holding the repository's files at one commit, after
/// `edit` changed them.
export function fakeTree(edit: (tree: string) => void = () => {}): { tree: string; commit: string } {
  const tree = scratchDir();
  for (const p of TREE_FILES) {
    mkdirSync(dirname(join(tree, p)), { recursive: true });
    copyFileSync(join(REPO, p), join(tree, p));
  }
  edit(tree);
  git(tree, "init", "-q");
  git(tree, "add", ".");
  git(tree, "commit", "-qm", "release");
  return { tree, commit: git(tree, "rev-parse", "HEAD") };
}

/// A cells file the entry accepts.
export function cellsText(stage: string, cellId = "cell_sc1", region = "eu-central-1", extra: string[] = []): string {
  return `${[
    `CELL_STAGE=${stage}`,
    `CELL_ID=${cellId}`,
    "PROJECT_ID=proj_deploy",
    "ACCOUNT_ID=acct_sink",
    "STREAMS_AUTH_ISSUER=https://auth.prisma.io",
    `PATH_PREFIX=cells/${cellId}`,
    `REGION=${region}`,
    "SLATE_S3_ENDPOINT=https://t3.storage.example",
    "SLATE_S3_BUCKET=phys-data",
    "SLATE_S3_REGION=auto",
    "FEEDS_S3_ENDPOINT=https://t3.storage.example",
    "FEEDS_S3_BUCKET=phys-feeds",
    "FEEDS_S3_REGION=auto",
    `FEEDS_S3_KEY=cells/${cellId}/feeds-bundle.json`,
    ...extra,
  ].join("\n")}\n`;
}

/// Every input of a stage, in a fresh scratch root: the cells files as
/// given, two x86_64 ELF headers, a bundle and an install holding `prisma`
/// (`prismaScript`, or a stub). `out` is <releases>/<tag>.
export function stageInputs(o: {
  tree: string;
  commit: string;
  tag: string;
  cells: Record<string, string>;
  releases?: string;
  server?: Buffer;
  prismaScript?: string;
}): StageInput {
  const root = scratchDir();
  const cells = join(root, "cells");
  mkdirSync(cells);
  for (const [stage, text] of Object.entries(o.cells)) writeFileSync(join(cells, `${stage}.env`), text);
  writeFileSync(join(root, "streams-slate"), o.server ?? elf(0x3e, `server-${o.tag}`));
  writeFileSync(join(root, "s3lite"), elf(0x3e, "s3lite"));
  writeFileSync(join(root, "entry.js"), "// the bundle\n");
  mkdirSync(join(root, "node_modules", ".bin"), { recursive: true });
  writeFileSync(join(root, "node_modules", ".bin", "prisma"), o.prismaScript ?? "#!/bin/sh\nexit 0\n");
  chmodSync(join(root, "node_modules", ".bin", "prisma"), 0o755);
  return {
    tree: o.tree, commit: o.commit, tag: o.tag, buildUnix: 1_760_000_000, cells,
    server: join(root, "streams-slate"), s3lite: join(root, "s3lite"), entryJs: join(root, "entry.js"),
    nodeModules: join(root, "node_modules"), out: join(o.releases ?? join(root, "releases"), o.tag),
  };
}

/// A release staged as cut-release stages one.
export function stage(o: Parameters<typeof stageInputs>[0]): { dir: string; release: Release } {
  const input = stageInputs(o);
  return { dir: input.out, release: stageRelease(input) };
}
