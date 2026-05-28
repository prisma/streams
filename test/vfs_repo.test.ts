import { describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Bash, InMemoryFs, MountableFs } from "just-bash";
import { createProfileTestApp } from "./profile_test_utils";
import { openVfsRepo, PrismaStreamsVfsFs, createVfsGitCommands, type VfsFetch } from "../src/vfs";
import { objectsStreamName, toBase64 } from "../src/vfs/model";

function appFetch(app: ReturnType<typeof createProfileTestApp>["app"]): VfsFetch {
  return (input, init) => app.fetch(new Request(input, init));
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

  test("runs just-bash with VFS adapter and git-like commands", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-vfs-bash-"));
    const { app } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    try {
      const repo = openVfsRepo({
        streamsUrl: "http://local",
        stream: "vfs/test/bash/control",
        fetch: appFetch(app),
      });
      await repo.ensure();

      const seed = await repo.checkout({ ref: "main", workspaceId: "seed" });
      await seed.writeFile("/README.md", "hello\n");
      await seed.mkdir("/src");
      await seed.writeFile("/src/app.ts", "export const value = 1;\n");
      await seed.commit({ message: "Initial import", author: { id: "seed" } });

      let workspace = await repo.checkout({ ref: "main", workspaceId: "shell" });
      const vfsFs = new PrismaStreamsVfsFs(workspace, { mountPath: "/" });
      const fs = new MountableFs({
        base: new InMemoryFs(),
        mounts: [{ mountPoint: "/workspace", filesystem: vfsFs }],
      });
      const bash = new Bash({
        fs,
        cwd: "/workspace",
        customCommands: createVfsGitCommands({
          getWorkspace: () => workspace,
          setWorkspace: (next) => {
            workspace = next;
            vfsFs.setWorkspace(next);
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
