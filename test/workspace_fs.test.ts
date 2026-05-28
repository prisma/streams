import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Bash, InMemoryFs, MountableFs } from "just-bash";
import { createProfileTestApp, fetchJsonApp } from "./profile_test_utils";
import { createWorkspaceGitCommands, openWorkspaceFsRepo, PrismaStreamsWorkspaceFs, type WorkspaceFsFetch, WorkspaceFsClientError } from "../src/workspace_fs";

function appFetch(app: ReturnType<typeof createProfileTestApp>["app"]): WorkspaceFsFetch {
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

async function installEvlogProfile(app: ReturnType<typeof createProfileTestApp>["app"], stream: string): Promise<void> {
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
      profile: { kind: "evlog" },
    }),
  });
  expect(profile.status).toBe(200);
}

describe("workspace-fs profile", () => {
  test("rejects removed vfs-repo profile and workspace-fs without git backing", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-workspace-profile-validation-"));
    const { app } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    try {
      const stream = "workspace/test/profile-validation/control";
      const base = `http://local/v1/stream/${encodeURIComponent(stream)}`;
      const create = await app.fetch(new Request(base, {
        method: "PUT",
        headers: { "content-type": "application/json" },
      }));
      expect(create.ok).toBe(true);

      const vfsProfile = await fetchJsonApp(app, `${base}/_profile`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: { kind: "vfs-repo", version: 1 },
        }),
      });
      expect(vfsProfile.status).toBe(400);
      expect(vfsProfile.body.error.message).toContain("profile.kind");

      const workspaceWithoutGit = await fetchJsonApp(app, `${base}/_profile`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          apiVersion: "durable.streams/profile/v1",
          profile: { kind: "workspace-fs", version: 1 },
        }),
      });
      expect(workspaceWithoutGit.status).toBe(400);
      expect(workspaceWithoutGit.body.error.message).toContain("gitRepo is required");
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

      const oldVfsRoute = await fetchJsonApp(app, `${base}/_vfs/checkout`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ ref: "main", workspaceId: "old-vfs-route" }),
      });
      expect(oldVfsRoute.status).toBe(404);

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

  test("writes workspace-fs audit events to an evlog stream", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-workspace-audit-"));
    const { app } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    try {
      const gitStream = "git/test/workspace-audit-canonical";
      const auditStream = "evlog/test/workspace-audit";
      await installGitRepoProfile(app, gitStream);
      await installEvlogProfile(app, auditStream);

      const workspaceStream = "workspace/test/audit/control";
      const repo = openWorkspaceFsRepo({
        streamsUrl: "http://local",
        stream: workspaceStream,
        fetch: appFetch(app),
      });
      await repo.ensure({ gitRepoStream: gitStream, auditStream });

      const workspace = await repo.checkout({ ref: "main", workspaceId: "audit-agent" });
      await workspace.writeFile("/README.md", "audited\n");
      expect(await workspace.readFile("/README.md")).toBe("audited\n");
      const commit = await workspace.commit({ message: "Audited commit", author: { id: "agent" } });
      expect(commit.newCommitId).toMatch(/^[0-9a-f]{40}$/);

      const draft = await repo.checkout({ ref: "main", workspaceId: "audit-discard" });
      await draft.discard();

      const read = await fetchJsonApp(app, `http://local/v1/stream/${encodeURIComponent(auditStream)}?format=json`, { method: "GET" });
      expect(read.status).toBe(200);
      const events = read.body as Array<{ message: string; service: string; context: Record<string, unknown> }>;
      expect(events.map((event) => event.message)).toEqual([
        "workspace_checked_out",
        "workspace_ops_appended",
        "workspace_file_read",
        "workspace_commit_started",
        "workspace_commit_succeeded",
        "workspace_checked_out",
        "workspace_discarded",
      ]);
      expect(events.every((event) => event.service === "workspace-fs")).toBe(true);
      const opEvent = events.find((event) => event.message === "workspace_ops_appended");
      expect(opEvent?.context).toMatchObject({
        workspaceStream,
        workspaceId: "audit-agent",
        event: "workspace_ops_appended",
        changedPaths: ["/README.md"],
      });
      const readEvent = events.find((event) => event.message === "workspace_file_read");
      expect(readEvent?.context).toMatchObject({
        workspaceStream,
        workspaceId: "audit-agent",
        event: "workspace_file_read",
        path: "/README.md",
      });
      const commitEvent = events.find((event) => event.message === "workspace_commit_succeeded");
      expect(commitEvent?.context).toMatchObject({
        workspaceStream,
        workspaceId: "audit-agent",
        actorId: "agent",
        event: "workspace_commit_succeeded",
        newCommitId: commit.newCommitId,
      });
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("runs just-bash with workspace-fs adapter and git-like commands", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-workspace-bash-"));
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
