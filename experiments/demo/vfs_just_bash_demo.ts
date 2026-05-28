import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Bash, InMemoryFs, MountableFs } from "just-bash";
import { createApp } from "../../src/app";
import { loadConfig } from "../../src/config";
import { MockR2Store } from "../../src/objectstore/mock_r2";
import { createVfsGitCommands, openVfsRepo, PrismaStreamsVfsFs, type VfsFetch } from "../../src/vfs";

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

function appFetch(app: ReturnType<typeof makeDemoApp>): VfsFetch {
  return (input, init) => app.fetch(new Request(input, init));
}

async function runAndPrint(bash: Bash, command: string): Promise<void> {
  const result = await bash.exec(command);
  process.stdout.write(`\n$ ${command}\n`);
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);
  process.stdout.write(`[exit ${result.exitCode}]\n`);
}

const root = mkdtempSync(join(tmpdir(), "streams-vfs-demo-"));
const app = makeDemoApp(root);

try {
  const repo = openVfsRepo({
    streamsUrl: "http://local",
    stream: "vfs/demo/agent-repo/control",
    fetch: appFetch(app),
  });
  await repo.ensure();

  const seed = await repo.checkout({ ref: "main", workspaceId: "seed" });
  await seed.writeFile("/README.md", "# VFS demo\n\nThis repository lives in Prisma Streams.\n");
  await seed.mkdir("/src");
  await seed.writeFile("/src/app.ts", 'export function greeting() {\n  return "hello streams";\n}\n');
  await seed.writeFile("/package.json", '{ "type": "module", "scripts": { "test": "echo ok" } }\n');
  await seed.commit({
    message: "Initial VFS demo import",
    author: { id: "demo-seed", name: "Demo Seed" },
  });

  let workspace = await repo.checkout({ ref: "main", workspaceId: "agent-demo" });
  const vfsFs = new PrismaStreamsVfsFs(workspace, { mountPath: "/" });
  const fs = new MountableFs({
    base: new InMemoryFs(),
    mounts: [{ mountPoint: "/workspace", filesystem: vfsFs }],
  });
  const bash = new Bash({
    fs,
    cwd: "/workspace",
    commands: ["cat", "echo", "find", "grep", "head", "ls", "mkdir", "pwd", "sed", "wc"],
    customCommands: createVfsGitCommands({
      getWorkspace: () => workspace,
      setWorkspace: (next) => {
        workspace = next;
        vfsFs.setWorkspace(next);
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
} finally {
  app.close();
  rmSync(root, { recursive: true, force: true });
}
