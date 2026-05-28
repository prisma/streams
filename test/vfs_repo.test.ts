import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Result } from "better-result";
import { Bash, InMemoryFs, MountableFs } from "just-bash";
import { createProfileTestApp, fetchJsonApp } from "./profile_test_utils";
import { migrateVfsRepoToGitRepoResult, openVfsRepo, type VfsFetch } from "../src/vfs";
import { objectsStreamName, toBase64 } from "../src/vfs/model";
import { createWorkspaceGitCommands, openWorkspaceFsRepo, PrismaStreamsWorkspaceFs, WorkspaceFsClientError } from "../src/workspace_fs";

function appFetch(app: ReturnType<typeof createProfileTestApp>["app"]): VfsFetch {
  return (input, init) => app.fetch(new Request(input, init));
}

async function installGitRepoProfile(app: ReturnType<typeof createProfileTestApp>["app"], stream: string): Promise<void> {
  const base = `http://local/v1/stream/${encodeURIComponent(stream)}`;
  const create = await app.fetch(new Request(base, {
    method: "PUT",
    headers: { "content-type": "application/json" },
  }));
  expect(create.ok).toBe(true);
  const profile = await fetchJsonApp(app, `${base}/_profile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      apiVersion: "durable.streams/profile/v1",
      profile: { kind: "git-repo", version: 1, objectFormat: "sha1", defaultBranch: "main" },
    }),
  });
  expect(profile.status).toBe(200);
}

describe("vfs-repo profile", () => {
  test("commits durable workspace ops and serves lazy file metadata/content", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-vfs-repo-"));
    const { app } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    try {
      const repo = openVfsRepo({
        streamsUrl: "http://local",
        stream: "vfs/test/repo/control",
        fetch: appFetch(app),
      });
      await repo.ensure();

      const seed = await repo.checkout({ ref: "main", workspaceId: "seed" });
      await seed.writeFile("/README.md", "# demo\n");
      await seed.mkdir("/src");
      await seed.writeFile("/src/app.ts", 'console.log("hello");\n');
      const first = await seed.commit({
        message: "Initial import",
        author: { id: "agent-1", name: "Agent One" },
      });
      expect(first.oldCommitId).toBeNull();
      expect(first.commit.changeSummary).toMatchObject({ added: 3, modified: 0, deleted: 0 });

      const wc = await repo.checkout({ ref: "main", workspaceId: "agent" });
      expect(await wc.readFile("/README.md")).toBe("# demo\n");
      const rootEntries = await wc.readdir("/");
      expect(rootEntries.entries.map((entry) => entry.path).sort()).toEqual(["/README.md", "/src"]);

      await wc.writeFile("/src/app.ts", 'console.log("hello vfs");\n');
      await wc.writeFile("/notes.txt", "draft\n");
      const status = await wc.status();
      expect(status.state).toBe("open");
      expect(status.changedPaths).toEqual(["/notes.txt", "/src/app.ts"]);
      const index = await wc.index("/src");
      expect(index.children).toEqual(["app.ts"]);
      const changes = await wc.changes("/src");
      expect(changes.paths).toEqual(["/src/app.ts"]);
      const compacted = await wc.compact();
      expect(compacted.latestPaths).toEqual(["/notes.txt", "/src/app.ts"]);
      expect(await wc.readFile("/notes.txt")).toBe("draft\n");

      const second = await wc.commit({
        message: "Update app",
        author: { id: "agent-1" },
      });
      expect(second.oldCommitId).toBe(first.newCommitId);

      const committed = await repo.checkout({ ref: "main", workspaceId: "reader" });
      expect(await committed.readFile("/src/app.ts")).toBe('console.log("hello vfs");\n');
      expect(await committed.readFile("/notes.txt")).toBe("draft\n");

      const commits = await repo.log("main");
      expect(commits.map((commit) => commit.message)).toEqual(["Update app", "Initial import"]);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("rejects stale expected heads during commit", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-vfs-cas-"));
    const { app } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    try {
      const repo = openVfsRepo({
        streamsUrl: "http://local",
        stream: "vfs/test/cas/control",
        fetch: appFetch(app),
      });
      await repo.ensure();

      const a = await repo.checkout({ ref: "main", workspaceId: "a" });
      const b = await repo.checkout({ ref: "main", workspaceId: "b" });
      await a.writeFile("/a.txt", "a\n");
      await b.writeFile("/b.txt", "b\n");
      await a.commit({ message: "A", author: { id: "a" } });

      await expect(b.commit({ message: "B", author: { id: "b" } })).rejects.toMatchObject({
        status: 409,
      });
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("deduplicates identical blob objects within one workspace op batch", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-vfs-dedupe-"));
    const { app } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    try {
      const repo = openVfsRepo({
        streamsUrl: "http://local",
        stream: "vfs/test/dedupe/control",
        fetch: appFetch(app),
      });
      await repo.ensure();

      const wc = await repo.checkout({ ref: "main", workspaceId: "dedupe" });
      const content = new Uint8Array(8192);
      content.fill(7);
      await repo.appendWorkspaceOps(wc.workspaceId, [
        { kind: "put-file", path: "/a.bin", contentBase64: toBase64(content) },
        { kind: "put-file", path: "/b.bin", contentBase64: toBase64(content) },
      ]);

      const objectStream = app.deps.db.getStream(objectsStreamName(repo.stream));
      expect(objectStream?.wal_rows).toBe(2n);

      await wc.commit({ message: "Add duplicate blobs", author: { id: "agent" } });
      const reader = await repo.checkout({ ref: "main", workspaceId: "reader" });
      expect(await reader.readFileBuffer("/a.bin")).toEqual(content);
      expect(await reader.readFileBuffer("/b.bin")).toEqual(content);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("can commit workspace snapshots into a canonical git-repo stream", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-vfs-git-commit-"));
    const { app } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    try {
      const gitStream = "git/test/workspace-canonical";
      await installGitRepoProfile(app, gitStream);

      const repo = openVfsRepo({
        streamsUrl: "http://local",
        stream: "vfs/test/workspace-git/control",
        fetch: appFetch(app),
      });
      await repo.ensure({ gitRepoStream: gitStream });

      const seed = await repo.checkout({ ref: "main", workspaceId: "seed" });
      await seed.writeFile("/README.md", "# canonical git\n");
      await seed.mkdir("/src");
      await seed.writeFile("/src/app.ts", "export const value = 1;\n");
      const first = await seed.commit({ message: "Initial Git-backed import", author: { id: "seed", name: "Seed Agent" } });
      expect(first.git?.repoStream).toBe(gitStream);
      expect(first.git?.oldOid).toBeNull();
      expect(first.git?.objectCount).toBeGreaterThanOrEqual(5);

      const gitBase = `http://local/v1/stream/${encodeURIComponent(gitStream)}`;
      const gitRef = await fetchJsonApp(app, `${gitBase}/_git/ref/main`, { method: "GET" });
      expect(gitRef.status).toBe(200);
      expect(gitRef.body.oid).toBe(first.git?.newOid);

      const firstCommit = await app.fetch(new Request(`${gitBase}/_git/object/${first.git!.newOid}`, { method: "GET" }));
      expect(firstCommit.status).toBe(200);
      const firstCommitText = await firstCommit.text();
      expect(firstCommitText).toContain("tree ");
      expect(firstCommitText).toContain("Initial Git-backed import");

      const wc = await repo.checkout({ ref: "main", workspaceId: "agent" });
      await wc.writeFile("/src/app.ts", "export const value = 2;\n");
      const second = await wc.commit({ message: "Update Git-backed workspace", author: { id: "agent" } });
      expect(second.git?.oldOid).toBe(first.git?.newOid);

      const secondCommit = await app.fetch(new Request(`${gitBase}/_git/object/${second.git!.newOid}`, { method: "GET" }));
      expect(secondCommit.status).toBe(200);
      const secondCommitText = await secondCommit.text();
      expect(secondCommitText).toContain(`parent ${first.git!.newOid}`);
      expect(secondCommitText).toContain("Update Git-backed workspace");

      const gitCheckout = await fetchJsonApp(app, `${gitBase}/_git/checkout?ref=main`, { method: "GET" });
      expect(gitCheckout.status).toBe(200);
      expect(gitCheckout.body.commitOid).toBe(second.git?.newOid);
      expect(gitCheckout.body.rootTreeOid).toMatch(/^[0-9a-f]{40}$/);

      const gitRoot = await fetchJsonApp(app, `${gitBase}/_git/readdir?commit=${second.git!.newOid}&path=${encodeURIComponent("/")}`, { method: "GET" });
      expect(gitRoot.status).toBe(200);
      expect(gitRoot.body.entries.map((entry: { path: string }) => entry.path).sort()).toEqual(["/README.md", "/src"]);

      const gitStat = await fetchJsonApp(app, `${gitBase}/_git/stat?commit=${second.git!.newOid}&path=${encodeURIComponent("/src/app.ts")}`, { method: "GET" });
      expect(gitStat.status).toBe(200);
      expect(gitStat.body.node).toMatchObject({
        path: "/src/app.ts",
        type: "file",
        mode: "100644",
        size: "export const value = 2;\n".length,
      });

      const gitBlob = await app.fetch(new Request(`${gitBase}/_git/blob?commit=${second.git!.newOid}&path=${encodeURIComponent("/src/app.ts")}`, {
        method: "GET",
        headers: { range: "bytes=0-5" },
      }));
      expect(gitBlob.status).toBe(206);
      expect(gitBlob.headers.get("content-range")).toBe(`bytes 0-5/${"export const value = 2;\n".length}`);
      expect(await gitBlob.text()).toBe("export");

      const status = await wc.status();
      expect(status.lastCommitId).toBe(second.newCommitId);
      const stored = await repo.show(second.newCommitId);
      expect(stored.git?.newOid).toBe(second.git?.newOid);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("workspace-fs profile name serves the workspace route surface", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-workspace-fs-"));
    const { app } = createProfileTestApp(root, {
      metricsFlushIntervalMs: 0,
      segmentCheckIntervalMs: 10,
      uploadIntervalMs: 10,
      segmentTargetRows: 1,
    });
    try {
      const gitStream = "git/test/workspace-fs-canonical";
      await installGitRepoProfile(app, gitStream);

      const stream = "workspace/test/profile/control";
      const base = `http://local/v1/stream/${encodeURIComponent(stream)}`;
      const repo = openWorkspaceFsRepo({
        streamsUrl: "http://local",
        stream,
        fetch: appFetch(app),
      });
      await repo.ensure({ gitRepoStream: gitStream });
      const profile = await fetchJsonApp(app, `${base}/_profile`, { method: "GET" });
      expect(profile.status).toBe(200);
      expect(profile.body.profile.kind).toBe("workspace-fs");
      const workspace = await repo.checkout({ ref: "main", workspaceId: "workspace-fs" });
      await workspace.writeFile("/README.md", "workspace-fs\n");
      await workspace.mkdir("/src");
      await workspace.writeFile("/src/app.ts", "export const value = 1;\n");
      const commit = await workspace.commit({
        message: "Workspace profile commit",
        author: { id: "workspace-fs" },
        durability: "verified",
        durabilityTimeoutMs: 5000,
      });
      expect(commit.git?.repoStream).toBe(gitStream);
      expect(commit.newCommitId).toBe(commit.git?.newOid);
      expect(commit.git?.durability).toBe("verified");

      const gitBase = `http://local/v1/stream/${encodeURIComponent(gitStream)}`;
      const gitRef = await fetchJsonApp(app, `${gitBase}/_git/ref/main`, { method: "GET" });
      expect(gitRef.status).toBe(200);
      expect(gitRef.body.oid).toBe(commit.git?.newOid);

      const reader = await repo.checkout({ ref: "main", workspaceId: "workspace-fs-reader" });
      expect(reader.baseCommitId).toBe(commit.git?.newOid);
      expect(await reader.readFile("/README.md")).toBe("workspace-fs\n");
      const root = await reader.readdir("/");
      expect(root.entries.map((entry) => entry.path).sort()).toEqual(["/README.md", "/src"]);

      await reader.writeFile("/src/app.ts", "export const value = 2;\n");
      const status = await reader.status();
      expect(status.changedPaths).toEqual(["/src/app.ts"]);
      expect(await reader.diff()).toContain("-export const value = 1;");
      expect(await reader.diff()).toContain("+export const value = 2;");
      const second = await reader.commit({ message: "Update workspace-fs over Git", author: { id: "agent" } });
      expect(second.git?.oldOid).toBe(commit.git?.newOid);
      const gitBlob = await app.fetch(new Request(`${gitBase}/_git/blob?commit=${second.git!.newOid}&path=${encodeURIComponent("/src/app.ts")}`, { method: "GET" }));
      expect(gitBlob.status).toBe(200);
      expect(await gitBlob.text()).toBe("export const value = 2;\n");

      const log = await repo.log("main");
      expect(log.map((entry) => entry.message)).toEqual(["Update workspace-fs over Git\n", "Workspace profile commit\n"]);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("rebases git-backed workspaces with path-level conflict detection", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-workspace-rebase-"));
    const { app } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    try {
      const gitStream = "git/test/workspace-rebase-canonical";
      await installGitRepoProfile(app, gitStream);

      const repo = openWorkspaceFsRepo({
        streamsUrl: "http://local",
        stream: "workspace/test/rebase/control",
        fetch: appFetch(app),
      });
      await repo.ensure({ gitRepoStream: gitStream });

      const seed = await repo.checkout({ ref: "main", workspaceId: "rebase-seed" });
      await seed.writeFile("/README.md", "base\n");
      await seed.mkdir("/src");
      await seed.writeFile("/src/app.ts", "export const value = 1;\n");
      const initial = await seed.commit({ message: "Initial", author: { id: "seed" } });

      const agent = await repo.checkout({ ref: "main", workspaceId: "rebase-agent" });
      await agent.writeFile("/notes.txt", "agent notes\n");

      const mover = await repo.checkout({ ref: "main", workspaceId: "rebase-mover" });
      await mover.writeFile("/src/app.ts", "export const value = 2;\n");
      const upstream = await mover.commit({ message: "Move branch", author: { id: "mover" } });
      expect(upstream.oldCommitId).toBe(initial.newCommitId);

      const cleanConflicts = await agent.conflicts();
      expect(cleanConflicts.baseCommitId).toBe(initial.newCommitId);
      expect(cleanConflicts.currentHead).toBe(upstream.newCommitId);
      expect(cleanConflicts.changedPaths).toEqual(["/notes.txt"]);
      expect(cleanConflicts.upstreamChangedPaths).toEqual(["/src/app.ts"]);
      expect(cleanConflicts.conflictPaths).toEqual([]);
      expect(cleanConflicts.canRebase).toBe(true);

      const rebase = await agent.rebase();
      expect(rebase.rebased).toBe(true);
      expect(rebase.oldBaseCommitId).toBe(initial.newCommitId);
      expect(rebase.newBaseCommitId).toBe(upstream.newCommitId);
      expect(agent.baseCommitId).toBe(upstream.newCommitId);

      const agentCommit = await agent.commit({ message: "Agent notes", author: { id: "agent" } });
      expect(agentCommit.oldCommitId).toBe(upstream.newCommitId);
      const reader = await repo.checkout({ ref: "main", workspaceId: "rebase-reader" });
      expect(await reader.readFile("/src/app.ts")).toBe("export const value = 2;\n");
      expect(await reader.readFile("/notes.txt")).toBe("agent notes\n");

      const conflictAgent = await repo.checkout({ ref: "main", workspaceId: "conflict-agent" });
      const conflictMover = await repo.checkout({ ref: "main", workspaceId: "conflict-mover" });
      await conflictAgent.writeFile("/src/app.ts", "export const value = 3;\n");
      await conflictMover.writeFile("/src/app.ts", "export const value = 4;\n");
      const conflictHead = await conflictMover.commit({ message: "Conflicting move", author: { id: "mover" } });

      const conflicts = await conflictAgent.conflicts();
      expect(conflicts.currentHead).toBe(conflictHead.newCommitId);
      expect(conflicts.changedPaths).toEqual(["/src/app.ts"]);
      expect(conflicts.upstreamChangedPaths).toEqual(["/src/app.ts"]);
      expect(conflicts.conflictPaths).toEqual(["/src/app.ts"]);
      expect(conflicts.canRebase).toBe(false);

      try {
        await conflictAgent.rebase();
        throw new Error("expected rebase conflict");
      } catch (error) {
        expect(error).toBeInstanceOf(WorkspaceFsClientError);
        expect((error as WorkspaceFsClientError).status).toBe(409);
        expect((error as WorkspaceFsClientError).body).toMatchObject({
          conflictPaths: ["/src/app.ts"],
          error: { code: "workspace_conflict" },
        });
      }
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("migrates MVP vfs-repo commits into a canonical git-repo stream", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-vfs-migrate-"));
    const { app } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    try {
      const sourceStream = "vfs/test/migrate/source/control";
      const targetStream = "git/test/migrate/target";
      const repo = openVfsRepo({
        streamsUrl: "http://local",
        stream: sourceStream,
        fetch: appFetch(app),
      });
      await repo.ensure();
      const seed = await repo.checkout({ ref: "main", workspaceId: "migrate-seed" });
      await seed.writeFile("/README.md", "migrated\n");
      await seed.mkdir("/src");
      await seed.writeFile("/src/app.ts", "export const migrated = true;\n");
      await seed.commit({ message: "VFS source commit", author: { id: "source" } });

      const migration = await migrateVfsRepoToGitRepoResult({
        streamsUrl: "http://local",
        sourceStream,
        targetStream,
        fetch: appFetch(app),
      });
      expect(Result.isOk(migration)).toBe(true);
      if (Result.isError(migration)) throw new Error(migration.error.message);
      expect(migration.value.migratedCommits).toBe(1);
      expect(migration.value.newOid).toMatch(/^[0-9a-f]{40}$/);

      const gitBase = `http://local/v1/stream/${encodeURIComponent(targetStream)}`;
      const gitRef = await fetchJsonApp(app, `${gitBase}/_git/ref/main`, { method: "GET" });
      expect(gitRef.status).toBe(200);
      expect(gitRef.body.oid).toBe(migration.value.newOid);

      const migratedReadme = await app.fetch(new Request(`${gitBase}/_git/blob?commit=${migration.value.newOid}&path=${encodeURIComponent("/README.md")}`, { method: "GET" }));
      expect(migratedReadme.status).toBe(200);
      expect(await migratedReadme.text()).toBe("migrated\n");
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("runs just-bash with workspace-fs adapter and git-like commands", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-vfs-bash-"));
    const { app } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    try {
      const gitStream = "git/test/workspace-bash-canonical";
      await installGitRepoProfile(app, gitStream);
      const repo = openWorkspaceFsRepo({
        streamsUrl: "http://local",
        stream: "workspace/test/bash/control",
        fetch: appFetch(app),
      });
      await repo.ensure({ gitRepoStream: gitStream });

      const seed = await repo.checkout({ ref: "main", workspaceId: "seed" });
      await seed.writeFile("/README.md", "hello\n");
      await seed.mkdir("/src");
      await seed.writeFile("/src/app.ts", "export const value = 1;\n");
      await seed.commit({ message: "Initial import", author: { id: "seed" } });

      let workspace = await repo.checkout({ ref: "main", workspaceId: "shell" });
      const workspaceFs = new PrismaStreamsWorkspaceFs(workspace, { mountPath: "/" });
      const fs = new MountableFs({
        base: new InMemoryFs(),
        mounts: [{ mountPoint: "/workspace", filesystem: workspaceFs }],
      });
      const bash = new Bash({
        fs,
        cwd: "/workspace",
        customCommands: createWorkspaceGitCommands({
          getWorkspace: () => workspace,
          setWorkspace: (next) => {
            workspace = next;
            workspaceFs.setWorkspace(next);
          },
          author: { id: "agent", name: "Agent" },
        }),
        commands: ["cat", "echo", "grep", "ls", "mkdir", "pwd", "sed", "wc"],
        defenseInDepth: false,
      });

      const read = await bash.exec("cat README.md");
      expect(read.stdout).toBe("hello\n");

      const write = await bash.exec('echo "export const value = 2;" > src/app.ts && echo notes > notes.txt');
      expect(write.exitCode).toBe(0);
      const status = await bash.exec("git status");
      expect(status.stdout).toContain("src/app.ts");
      expect(status.stdout).toContain("notes.txt");

      const commit = await bash.exec('git commit -m "Shell edits"');
      expect(commit.exitCode).toBe(0);
      expect(commit.stdout).toContain("Shell edits");
      const log = await bash.exec("git log");
      expect(log.stdout).toContain("Shell edits");
      expect(log.stdout).toContain("Initial import");
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });
});
