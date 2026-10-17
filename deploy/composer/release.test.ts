// A shared cell's release directory (public-beta deploy-q5 triage B5):
// release.ts stages one commit's files, binaries and cells files with a
// release.json describing every byte, and refuses a release that is not
// what was cut; scripts/cut-release.sh refuses to overwrite one before it
// does anything else. Offline: the tree is a scratch git repository of the
// repository's own files, and the binaries are ELF headers that never run.
// Run with `bun test ./deploy/composer`.
import { afterAll, describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, readdirSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { checkIssuerRelease, checkRelease, issuerSettings, keysContract, layoutVersion, type Release, stageRelease } from "./release";
import {
  cleanupScratch, elf, fakeTree, git, issuerSettingsText, REPO, scratchDir, stageInputs, stageIssuer, stageWatch, watchSettingsText,
} from "./release-rig";
import { checkWatchRelease, releaseWatchSettings } from "./watch-release";

afterAll(cleanupScratch);

const sha256 = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

const CELLS_PRODUCTION = "CELL_STAGE=production\nCELL_ID=cell_p1\nREGION=eu-central-1\n";

/// Every input of a stage of the production and cert cells files (and a
/// file of the operator's that is not a cells file, never read).
function inputs() {
  const { tree, commit } = fakeTree();
  const i = stageInputs({
    tree, commit, tag: "rc-1",
    cells: { production: CELLS_PRODUCTION, cert: "CELL_STAGE=cert\nCELL_ID=cell_c1\n" },
    server: elf(0x3e, "server"),
  });
  writeFileSync(join(i.cells, "production.provision.json"), "{}");
  return i;
}

/// Every path under `dir`, node_modules' contents aside.
function listing(dir: string, rel = ""): string[] {
  return readdirSync(join(dir, rel)).sort().flatMap((name) => {
    const path = rel === "" ? name : `${rel}/${name}`;
    if (path === "node_modules") return [path];
    return statSync(join(dir, path)).isDirectory() ? [`${path}/`, ...listing(dir, path)] : [path];
  });
}

const refusal = (fn: () => unknown) => {
  try {
    fn();
  } catch (e) {
    return (e as Error).message;
  }
  return "no refusal";
};

describe("stageRelease", () => {
  test("stages the commit's files, both binaries, the bundle and the cells files, and release.json lists every byte", () => {
    const i = inputs();
    const r: Release = stageRelease(i);
    expect(listing(i.out)).toEqual([
      "bin/",
      "bin/s3lite",
      "bun.lock",
      "deploy/",
      "deploy/app-server/",
      "deploy/app-server/downloader.ts",
      "deploy/app-server/feeds.ts",
      "deploy/app-server/supervise.ts",
      "deploy/cell-admin/",
      "deploy/cell-admin/errors.ts",
      "deploy/cell-admin/profile.ts",
      "deploy/cells/",
      "deploy/cells/cert.env",
      "deploy/cells/production.env",
      "deploy/composer/",
      "deploy/composer/app.ts",
      "deploy/composer/streams/",
      "deploy/composer/streams/entry.js",
      "deploy/composer/streams/entry.ts",
      "deploy/composer/streams/env.ts",
      "deploy/composer/streams/service.ts",
      "deploy/composer/streams/streams-slate",
      "deploy/profiles/",
      "deploy/profiles/compute-1g.env",
      "deploy/profiles/shared-cell.env",
      "deploy/release.json",
      "destroy.ts",
      "node_modules",
      "package.json",
      "prisma.config.ts",
    ]);
    const read = (p: string) => readFileSync(join(REPO, p));
    const pkg = JSON.parse(read("deploy/composer/package.json").toString());
    expect({ ...r, files: Object.keys(r.files) }).toEqual({
      tag: "rc-1",
      commit: i.commit,
      build_unix: 1_760_000_000,
      server_sha256: sha256(elf(0x3e, "server")),
      server_bytes: elf(0x3e, "server").length,
      s3lite_sha256: sha256(elf(0x3e, "s3lite")),
      profiles: {
        "compute-1g.env": sha256(read("deploy/profiles/compute-1g.env")),
        "shared-cell.env": sha256(read("deploy/profiles/shared-cell.env")),
      },
      cells: { cert: sha256(Buffer.from("CELL_STAGE=cert\nCELL_ID=cell_c1\n")), production: sha256(Buffer.from(CELLS_PRODUCTION)) },
      layout_version: layoutVersion(read("src/registry.rs").toString()),
      keys_contract: "r7",
      composer_version: pkg.dependencies["@prisma/composer"],
      compute_version: pkg.dependencies["@prisma/compute"],
      lockfile_sha256: sha256(read("deploy/composer/bun.lock")),
      files: listing(i.out).filter((p) => !p.endsWith("/") && p !== "node_modules" && p !== "deploy/release.json"),
    });
    expect(JSON.parse(readFileSync(join(i.out, "deploy/release.json"), "utf8"))).toEqual(r);
    expect(readFileSync(join(i.out, "package.json"))).toEqual(read("deploy/composer/package.json"));
    expect(readFileSync(join(i.out, "deploy/composer/app.ts"))).toEqual(read("deploy/composer/app.ts"));
    expect(r.files["deploy/composer/streams/streams-slate"]).toBe(r.server_sha256);
    const mode = (p: string) => statSync(join(i.out, p)).mode & 0o777;
    expect([mode("deploy/composer/streams/streams-slate"), mode("bin/s3lite"), mode("deploy/cells/production.env")])
      .toEqual([0o755, 0o755, 0o644]);
    expect([existsSync(i.nodeModules), existsSync(join(i.out, "node_modules", ".bin", "prisma"))]).toEqual([false, true]);
  });

  test("never overwrites a release directory", () => {
    const i = inputs();
    mkdirSync(i.out, { recursive: true });
    writeFileSync(join(i.out, "keep"), "an earlier cut");
    expect(refusal(() => stageRelease(i))).toBe(`${i.out} exists: a release directory is never overwritten`);
    expect([readdirSync(i.out), existsSync(i.nodeModules)]).toEqual([["keep"], true]);
  });

  test("takes a file only as the commit holds it", () => {
    const i = inputs();
    writeFileSync(join(i.tree, "deploy/profiles/shared-cell.env"), "KEEP_AWAKE=1\n");
    expect(refusal(() => stageRelease(i))).toBe(
      `${join(i.tree, "deploy/profiles/shared-cell.env")} differs from deploy/profiles/shared-cell.env at ${i.commit}: ` +
        "a release takes the commit's files only",
    );
    expect(existsSync(i.out)).toBe(false);
  });

  test("refuses a binary that is not an x86_64 ELF", () => {
    const i = inputs();
    writeFileSync(i.server, elf(0xb7, "aarch64"));
    expect(refusal(() => stageRelease(i))).toBe(`${i.server} is an ELF for machine 0xb7, not x86_64 (0x3e)`);
    writeFileSync(i.s3lite, "#!/bin/sh\n");
    writeFileSync(i.server, elf(0x3e, "server"));
    expect(refusal(() => stageRelease(i))).toBe(`${i.s3lite} is not an ELF executable`);
  });

  test("refuses a cells file that names another stage, or that is a symlink, or none at all", () => {
    const i = inputs();
    writeFileSync(join(i.cells, "cert.env"), "CELL_STAGE=production\n");
    expect(refusal(() => stageRelease(i))).toBe(`${join(i.cells, "cert.env")} does not set CELL_STAGE=cert`);
    rmSync(join(i.cells, "cert.env"));
    symlinkSync(join(i.cells, "production.env"), join(i.cells, "cert.env"));
    expect(refusal(() => stageRelease(i))).toBe(`${join(i.cells, "cert.env")} is not a regular file`);
    rmSync(join(i.cells, "cert.env"));
    rmSync(join(i.cells, "production.env"));
    expect(refusal(() => stageRelease(i))).toBe(`${i.cells} holds no cells file (scratch.env, cert.env, production.env)`);
  });
});

describe("checkRelease", () => {
  test("refuses a dotfile, a symlink, a file release.json does not list, a changed file and a missing one", () => {
    const i = inputs();
    stageRelease(i);
    mkdirSync(join(i.out, ".prisma-composer", "artifacts"), { recursive: true });
    mkdirSync(join(i.out, ".alchemy"));
    expect(checkRelease(i.out).tag).toBe("rc-1");
    const refused = (what: string) => `${i.out} is not the release it was cut as: ${what}`;
    for (const stray of ["deploy/.env", ".env", "deploy/composer/streams/.env.local"]) {
      writeFileSync(join(i.out, stray), "STREAMS_OPERATOR_TOKEN=x\n");
      expect(refusal(() => checkRelease(i.out))).toBe(refused(`${stray} (a dotfile)`));
      rmSync(join(i.out, stray));
    }
    symlinkSync("/etc/hosts", join(i.out, "deploy/hosts"));
    expect(refusal(() => checkRelease(i.out))).toBe(refused("deploy/hosts (a symlink)"));
    rmSync(join(i.out, "deploy/hosts"));
    writeFileSync(join(i.out, "deploy/composer/streams/extra.js"), "");
    expect(refusal(() => checkRelease(i.out))).toBe(refused("deploy/composer/streams/extra.js (not in release.json)"));
    rmSync(join(i.out, "deploy/composer/streams/extra.js"));
    const cells = join(i.out, "deploy/cells/production.env");
    writeFileSync(cells, `${CELLS_PRODUCTION}KEEP_AWAKE_UNTIL_MS=1\n`);
    expect(refusal(() => checkRelease(i.out))).toBe(refused("deploy/cells/production.env (changed since the cut)"));
    rmSync(cells);
    expect(refusal(() => checkRelease(i.out))).toBe(refused("deploy/cells/production.env (missing)"));
    writeFileSync(cells, CELLS_PRODUCTION);
    expect(checkRelease(i.out).cells.production).toBe(sha256(Buffer.from(CELLS_PRODUCTION)));
  });
});

describe("the issuer's release", () => {
  test("stages deploy/composer-issuer's files from the same commit, the bundle, and a stage's settings from <stage>.issuer.env", () => {
    const { tree, commit } = fakeTree();
    const releases = join(scratchDir(), "releases");
    const settings = issuerSettingsText("cell_p1", ["STREAMS_ISSUER_TOKEN_TTL_SECS=1800"]);
    const { dir, release } = stageIssuer({ tree, commit, tag: "rc-1", settings: { production: settings }, releases });
    expect([dir, listing(dir)]).toEqual([join(releases, "rc-1-issuer"), [
      "app.ts", "bun.lock", "issuer/", "issuer/entry.js", "issuer/service.ts", "node_modules", "package.json",
      "prisma.config.ts", "release.json", "stages/", "stages/production.env",
    ]]);
    const pkg = JSON.parse(readFileSync(join(REPO, "deploy/composer-issuer/package.json"), "utf8"));
    expect({ ...release, files: Object.keys(release.files) }).toEqual({
      app: "issuer", tag: "rc-1", commit,
      stages: { production: sha256(Buffer.from(settings)) },
      composer_version: pkg.dependencies["@prisma/composer"],
      lockfile_sha256: sha256(readFileSync(join(REPO, "deploy/composer-issuer/bun.lock"))),
      files: ["app.ts", "bun.lock", "issuer/entry.js", "issuer/service.ts", "package.json", "prisma.config.ts", "stages/production.env"],
    });
    expect(readFileSync(join(dir, "app.ts"))).toEqual(readFileSync(join(REPO, "deploy/composer-issuer/app.ts")));
    writeFileSync(join(dir, "issuer", ".env"), "STREAMS_ISSUER_PEPPER=x\n");
    expect(refusal(() => checkIssuerRelease(dir))).toBe(`${dir} is not the release it was cut as: issuer/.env (a dotfile)`);
  });

  test("a stage's settings are the issuer app's envParams only, its required ones all, never prisma dev's identity provider", () => {
    const read = (p: string) => readFileSync(join(REPO, p), "utf8");
    const [app, service] = [read("deploy/composer-issuer/app.ts"), read("deploy/composer-issuer/issuer/service.ts")];
    const check = (text: string) => refusal(() => issuerSettings("p.env", text, app, service));
    expect(issuerSettings("p.env", issuerSettingsText(), app, service).get("STREAMS_CELL_ID")).toBe("cell_sc1");
    expect(check(issuerSettingsText("cell_sc1", ["STREAMS_ISSUER_PEPPER=x"]))).toBe(
      "p.env sets STREAMS_ISSUER_PEPPER, which is not one of the issuer's settings",
    );
    expect(check(issuerSettingsText("cell_sc1", ["STREAMS_ISSUER_OAUTH_AUTHORITY=http://127.0.0.1:9"]))).toBe(
      "p.env sets STREAMS_ISSUER_OAUTH_AUTHORITY, prisma dev's mock identity provider",
    );
    expect(check(issuerSettingsText().replace(/^STREAMS_ISSUER_ORIGIN=.*\n/m, ""))).toBe("p.env does not set STREAMS_ISSUER_ORIGIN");
    expect(check(issuerSettingsText("cell_sc1", ["STREAMS_CELL_ID=cell_x"]))).toBe("p.env sets STREAMS_CELL_ID twice");
  });
});

describe("the cell watch's release", () => {
  test("stages deploy/cell-watch's files from the same commit, the bundle, and a stage's settings from <stage>.watch.env", () => {
    const { tree, commit } = fakeTree();
    const releases = join(scratchDir(), "releases");
    const settings = watchSettingsText(commit.slice(0, 12), "cell_p1");
    const { dir, release } = stageWatch({ tree, commit, tag: "rc-1", settings: { production: settings }, releases });
    expect([dir, listing(dir)]).toEqual([join(releases, "rc-1-watch"), [
      "app.ts", "bun.lock", "entry.js", "module.ts", "node_modules", "package.json", "prisma.config.ts", "release.json",
      "service.ts", "stages/", "stages/production.env",
    ]]);
    const pkg = JSON.parse(readFileSync(join(REPO, "deploy/cell-watch/package.json"), "utf8"));
    expect({ ...release, files: Object.keys(release.files) }).toEqual({
      app: "cell-watch", tag: "rc-1", commit,
      stages: { production: sha256(Buffer.from(settings)) },
      composer_version: pkg.dependencies["@prisma/composer"],
      lockfile_sha256: sha256(readFileSync(join(REPO, "deploy/cell-watch/bun.lock"))),
      files: ["app.ts", "bun.lock", "entry.js", "module.ts", "package.json", "prisma.config.ts", "service.ts", "stages/production.env"],
    });
    expect(readFileSync(join(dir, "module.ts"))).toEqual(readFileSync(join(REPO, "deploy/cell-watch/module.ts")));
    writeFileSync(join(dir, ".env"), "ALERT_SINK_KEY=x\n");
    expect(refusal(() => checkWatchRelease(dir))).toBe(`${dir} is not the release it was cut as: .env (a dotfile)`);
  });

  test("a stage's settings are the watch module's envParams, all of them, as its config accepts them, https, and EXPECTED_GIT this commit's", () => {
    const moduleTs = readFileSync(join(REPO, "deploy/cell-watch/module.ts"), "utf8");
    const commit = "0123456789abcdef0123456789abcdef01234567";
    const check = (text: string) => refusal(() => releaseWatchSettings("w.env", text, moduleTs, commit));
    expect(releaseWatchSettings("w.env", watchSettingsText(commit), moduleTs, commit).get("STREAMS_CELL_ID")).toBe("cell_sc1");
    expect(releaseWatchSettings("w.env", watchSettingsText("none"), moduleTs, commit).get("EXPECTED_GIT")).toBe("none");
    expect([
      check(watchSettingsText(commit, "cell_sc1", ["ALERT_SINK_URL=https://x.example"])),
      check(watchSettingsText(commit).replace(/^NOTIFY_ONLY=.*\n/m, "")),
      check(watchSettingsText(commit, "cell_sc1", ["NOTIFY_ONLY=0"])),
      check(watchSettingsText(commit).replace("NOTIFY_ONLY=1", "NOTIFY_ONLY=2")),
      check(watchSettingsText(commit).replace("https://cell.example", "http://127.0.0.1:8080")),
      check(watchSettingsText("fedcba9")),
    ]).toEqual([
      "w.env sets ALERT_SINK_URL, which is not one of the cell watch's settings",
      "w.env does not set NOTIFY_ONLY",
      "w.env sets NOTIFY_ONLY twice",
      "w.env: the watch would refuse it: NOTIFY_ONLY is 0 or 1",
      "w.env sets STREAMS_CELL_ORIGIN=http://127.0.0.1:8080: a release's watch runs in the cloud, so https",
      `w.env sets EXPECTED_GIT=fedcba9, not this release's commit ${commit}: row 5 would page the release it watches as the wrong build`,
    ]);
  });
});

describe("the release's facts", () => {
  test("LAYOUT_VERSION is read from its one declaration", () => {
    const rs = "pub(crate) const LAYOUT_VERSION: u32 = 6;\n";
    expect(layoutVersion(rs)).toBe(6);
    expect(refusal(() => layoutVersion(rs + rs))).toBe("src/registry.rs does not declare LAYOUT_VERSION once");
  });

  test("the keys contract is the §14.1 revision the keys schema cites and MULTITENANCY's log lists", () => {
    const schema = readFileSync(join(REPO, "contracts/streams-platform/v1/keys.schema.json"), "utf8");
    const log = readFileSync(join(REPO, "docs/MULTITENANCY.md"), "utf8");
    expect(keysContract(schema, log)).toBe("r7");
    expect(refusal(() => keysContract(schema.replace("§14.1 r7", "§14.1 r9"), log))).toBe(
      "MULTITENANCY's revision log has no r9 entry for §14.1",
    );
    // The log's last entry ends at the blank line after it: a later section
    // that names §14.1 does not make it a §14.1 revision.
    const lastOnly = "**Revision log**\n- r1 (x): §17.3 only\n\n---\n\n### 14.1 Fleet identity\n§14.1\n";
    expect(refusal(() => keysContract(schema.replace("§14.1 r7", "§14.1 r1"), lastOnly))).toBe(
      "MULTITENANCY's revision log has no r1 entry for §14.1",
    );
    expect(refusal(() => keysContract(schema.replace("§14.1 r7", "r7"), log))).toBe(
      "the keys schema's aud does not cite one MULTITENANCY §14.1 revision",
    );
  });
});

describe("scripts/cut-release.sh", () => {
  /// Run the script with `gh`, `git`'s worktrees, `cargo` and `bun` never
  /// reached: each is a stub on PATH that records it ran.
  function cut(releases: string, soak: string, cells: string) {
    const bin = scratchDir();
    for (const tool of ["gh", "cargo", "bun", "python3"]) {
      writeFileSync(join(bin, tool), `#!/bin/sh\necho ${tool} >> ${join(bin, "ran")}\nexit 1\n`);
      chmodSync(join(bin, tool), 0o755);
    }
    const run = spawnSync("bash", [join(REPO, "scripts", "cut-release.sh"), "HEAD", "rc-1", "--cells", cells], {
      env: { PATH: `${bin}:${process.env.PATH}`, HOME: process.env.HOME ?? "/", STREAMS_RELEASES: releases, SOAK_HOME: soak },
    });
    const ran = existsSync(join(bin, "ran")) ? readFileSync(join(bin, "ran"), "utf8") : "";
    return { code: run.status, err: run.stderr.toString(), ran };
  }

  test("refuses an existing release directory, the cell's, the issuer's or the watch's, or results of the tag, before anything else", () => {
    const root = scratchDir();
    const [releases, soak, cells] = ["releases", "soak", "cells"].map((d) => join(root, d));
    mkdirSync(join(releases, "rc-1"), { recursive: true });
    mkdirSync(cells);
    const worktrees = git(REPO, "worktree", "list");
    expect(cut(releases, soak, cells)).toEqual({
      code: 1,
      err: `cut-release: refused: ${join(releases, "rc-1")} exists: a release directory is never overwritten\n`,
      ran: "",
    });
    rmSync(join(releases, "rc-1"), { recursive: true });
    mkdirSync(join(soak, "results", "rc-1"), { recursive: true });
    expect(cut(releases, soak, cells)).toEqual({
      code: 1,
      err: `cut-release: refused: ${join(soak, "results", "rc-1")} exists: tag rc-1 was cut before\n`,
      ran: "",
    });
    rmSync(join(soak, "results"), { recursive: true });
    mkdirSync(join(releases, "rc-1-issuer"));
    expect(cut(releases, soak, cells)).toEqual({
      code: 1,
      err: `cut-release: refused: ${join(releases, "rc-1-issuer")} exists: a release directory is never overwritten\n`,
      ran: "",
    });
    rmSync(join(releases, "rc-1-issuer"), { recursive: true });
    mkdirSync(join(releases, "rc-1-watch"));
    expect(cut(releases, soak, cells)).toEqual({
      code: 1,
      err: `cut-release: refused: ${join(releases, "rc-1-watch")} exists: a release directory is never overwritten\n`,
      ran: "",
    });
    rmSync(join(releases, "rc-1-watch"), { recursive: true });
    expect([git(REPO, "worktree", "list"), readdirSync(releases)]).toEqual([worktrees, []]);
  });
});
