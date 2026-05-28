import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Bash, InMemoryFs, MountableFs } from "just-bash";
import { createApp } from "../../src/app";
import { loadConfig } from "../../src/config";
import { MockR2Store } from "../../src/objectstore/mock_r2";
import { createWorkspaceGitCommands, openWorkspaceFsRepo, PrismaStreamsWorkspaceFs, type WorkspaceFsFetch } from "../../src/workspace_fs";

function makeDemoApp(rootDir: string) {
  const cfg = {
    ...loadConfig(),
    rootDir,
    dbPath: `${rootDir}/wal.sqlite`,
    port: 0,
    segmentCheckIntervalMs: 60_000,
    uploadIntervalMs: 60_000,
    metricsFlushIntervalMs: 0,
  };
  return createApp(cfg, new MockR2Store());
}

function appFetch(app: ReturnType<typeof makeDemoApp>): WorkspaceFsFetch {
  return (input, init) => app.fetch(new Request(input, init));
}

async function fetchJson(app: ReturnType<typeof makeDemoApp>, url: string, init: RequestInit) {
  const res = await app.fetch(new Request(url, init));
  const text = await res.text();
  if (!res.ok) throw new Error(`${init.method ?? "GET"} ${url} failed: ${res.status} ${text}`);
  return text === "" ? null : JSON.parse(text);
}

async function runAndPrint(bash: Bash, command: string): Promise<void> {
  const result = await bash.exec(command);
  process.stdout.write(`\n$ ${command}\n`);
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);
  process.stdout.write(`[exit ${result.exitCode}]\n`);
}

const root = mkdtempSync(join(tmpdir(), "streams-workspace-demo-"));
const app = makeDemoApp(root);

try {
  const gitStream = "git/demo/agent-repo";
  const gitBase = `http://local/v1/stream/${encodeURIComponent(gitStream)}`;
  await app.fetch(new Request(gitBase, {
    method: "PUT",
    headers: { "content-type": "application/json" },
  }));
  await fetchJson(app, `${gitBase}/_profile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      apiVersion: "durable.streams/profile/v1",
      profile: { kind: "git-repo", version: 1, objectFormat: "sha1", defaultBranch: "main" },
    }),
  });

  const workspaceStream = "workspace/demo/agent-repo/control";
  const repo = openWorkspaceFsRepo({
    streamsUrl: "http://local",
    stream: workspaceStream,
    fetch: appFetch(app),
  });
  await repo.ensure({ gitRepoStream: gitStream });

  const seed = await repo.checkout({ ref: "main", workspaceId: "seed" });
  await seed.writeFile("/README.md", "# workspace-fs demo\n\nThis repository lives in Prisma Streams.\n");
  await seed.mkdir("/src");
  await seed.writeFile("/src/app.ts", 'export function greeting() {\n  return "hello streams";\n}\n');
  await seed.writeFile("/package.json", '{ "type": "module", "scripts": { "test": "echo ok" } }\n');
  await seed.commit({
    message: "Initial workspace-fs demo import",
    author: { id: "demo-seed", name: "Demo Seed" },
  });

  let workspace = await repo.checkout({ ref: "main", workspaceId: "agent-demo" });
  const workspaceFs = new PrismaStreamsWorkspaceFs(workspace, { mountPath: "/" });
  const fs = new MountableFs({
    base: new InMemoryFs(),
    mounts: [{ mountPoint: "/workspace", filesystem: workspaceFs }],
  });
  const bash = new Bash({
    fs,
    cwd: "/workspace",
    commands: ["cat", "echo", "find", "grep", "head", "ls", "mkdir", "pwd", "sed", "wc"],
    customCommands: createWorkspaceGitCommands({
      getWorkspace: () => workspace,
      setWorkspace: (next) => {
        workspace = next;
        workspaceFs.setWorkspace(next);
      },
      author: { id: "agent-demo", name: "Demo Agent" },
    }),
    defenseInDepth: false,
    python: false,
    javascript: false,
  });

  await runAndPrint(bash, "pwd");
  await runAndPrint(bash, "ls -la");
  await runAndPrint(bash, "cat README.md");
  await runAndPrint(bash, 'sed -i "s/hello streams/hello durable streams/" src/app.ts');
  await runAndPrint(bash, 'echo "agent notes" > NOTES.md');
  await runAndPrint(bash, 'grep -R "durable" .');
  await runAndPrint(bash, "git status");
  await runAndPrint(bash, "git diff");
  await runAndPrint(bash, 'git commit -m "Agent edits through just-bash"');
  await runAndPrint(bash, "git log");
  const refs = await fetchJson(app, `${gitBase}/_git/refs`, { method: "GET" });
  process.stdout.write(`\ncanonical git refs: ${JSON.stringify((refs as { refs: unknown }).refs, null, 2)}\n`);
} finally {
  app.close();
  rmSync(root, { recursive: true, force: true });
}
