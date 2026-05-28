import { Buffer } from "node:buffer";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createApp } from "../../src/app";
import { loadConfig } from "../../src/config";
import { MockR2Store } from "../../src/objectstore/mock_r2";
import { writeGitBlob, writeGitCommitResult, writeGitTreeResult } from "../../src/git_repo";
import { Result } from "better-result";

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
  const store = new MockR2Store();
  return { app: createApp(cfg, store), store };
}

async function fetchJson(app: ReturnType<typeof createApp>, url: string, init: RequestInit) {
  const res = await app.fetch(new Request(url, init));
  const text = await res.text();
  if (!res.ok) throw new Error(`${init.method ?? "GET"} ${url} failed: ${res.status} ${text}`);
  return text === "" ? null : JSON.parse(text);
}

async function writeObject(app: ReturnType<typeof createApp>, base: string, object: { type: string; oid: string; body: Uint8Array }) {
  return fetchJson(app, `${base}/_git/objects`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      type: object.type,
      expectedOid: object.oid,
      bodyBase64: Buffer.from(object.body).toString("base64"),
    }),
  });
}

function runGitOutput(cmd: string[], cwd?: string): string {
  const proc = Bun.spawnSync({ cmd, cwd, stdout: "pipe", stderr: "pipe" });
  if (proc.exitCode !== 0) {
    throw new Error(`git command failed: ${cmd.join(" ")}\n${new TextDecoder().decode(proc.stderr)}`);
  }
  return new TextDecoder().decode(proc.stdout).trim();
}

function runGitChecked(cmd: string[], cwd?: string): void {
  runGitOutput(cmd, cwd);
}

async function runGitCheckedAsync(cmd: string[], cwd?: string): Promise<void> {
  const proc = Bun.spawn({ cmd, cwd, stdout: "pipe", stderr: "pipe" });
  const [exitCode, stdout, stderr] = await Promise.all([
    proc.exited,
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
  ]);
  if (exitCode !== 0) {
    throw new Error(`git command failed: ${cmd.join(" ")}\n${stdout}${stderr}`);
  }
}

const root = mkdtempSync(join(tmpdir(), "streams-git-repo-demo-"));
const { app, store } = makeDemoApp(root);
const server = Bun.serve({ port: 0, fetch: app.fetch });

try {
  const stream = "git/demo/repo";
  const base = `http://local/v1/stream/${encodeURIComponent(stream)}`;
  await app.fetch(new Request(base, {
    method: "PUT",
    headers: { "content-type": "application/json" },
  }));
  await fetchJson(app, `${base}/_profile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      apiVersion: "durable.streams/profile/v1",
      profile: {
        kind: "git-repo",
        version: 1,
        objectFormat: "sha1",
        defaultBranch: "main",
        http: { enabled: true, allowFetch: true, allowPush: true },
        fetch: { protocolVersion: 2, allowFilter: true, allowDepth: true, allowPackfileUris: false },
      },
    }),
  });
  store.resetStats();

  const blob = writeGitBlob("hello from git-repo profile\n");
  const treeRes = writeGitTreeResult([{ mode: "100644", name: "README.md", oid: blob.oid }]);
  if (Result.isError(treeRes)) throw new Error(treeRes.error.message);
  const commitRes = writeGitCommitResult({
    tree: treeRes.value.oid,
    author: {
      name: "Demo Agent",
      email: "demo@example.com",
      timestampSeconds: Math.floor(Date.now() / 1000),
      timezone: "+0000",
    },
    message: "Initial git-repo profile commit\n",
  });
  if (Result.isError(commitRes)) throw new Error(commitRes.error.message);

  const blobArtifact = await writeObject(app, base, blob);
  const treeArtifact = await writeObject(app, base, treeRes.value);
  const commitArtifact = await writeObject(app, base, commitRes.value);
  const objectUris = [blobArtifact.objectKey, treeArtifact.objectKey, commitArtifact.objectKey];

  const txn = await fetchJson(app, `${base}/_git/transactions/ref`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      txnId: "demo-initial-commit",
      actor: "demo-agent",
      refUpdates: [{ ref: "main", oldOid: null, newOid: commitRes.value.oid }],
      objects: {
        looseObjectUris: objectUris,
        objectCount: objectUris.length,
        bytes: blobArtifact.framedSize + treeArtifact.framedSize + commitArtifact.framedSize,
      },
    }),
  });

  const range = await app.fetch(new Request(`${base}/_git/object/${blob.oid}`, {
    method: "GET",
    headers: { range: "bytes=0-4" },
  }));
  const rangeText = await range.text();
  const refs = await fetchJson(app, `${base}/_git/refs`, { method: "GET" });
  const txnStatus = await fetchJson(app, `${base}/_git/transactions/demo-initial-commit`, { method: "GET" });
  const checkout = await fetchJson(app, `${base}/_git/checkout?ref=main`, { method: "GET" });
  const stat = await fetchJson(app, `${base}/_git/stat?ref=main&path=${encodeURIComponent("/README.md")}`, { method: "GET" });
  const readdir = await fetchJson(app, `${base}/_git/readdir?ref=main&path=${encodeURIComponent("/")}`, { method: "GET" });
  const pathRange = await app.fetch(new Request(`${base}/_git/blob?ref=main&path=${encodeURIComponent("/README.md")}`, {
    method: "GET",
    headers: { range: "bytes=0-4" },
  }));
  const pathRangeText = await pathRange.text();
  const bundleRes = await app.fetch(new Request(`${base}/_git/export.bundle`, { method: "GET" }));
  if (!bundleRes.ok) throw new Error(`bundle export failed: ${bundleRes.status} ${await bundleRes.text()}`);
  const bundleBytes = Buffer.from(await bundleRes.arrayBuffer());
  const packRes = await app.fetch(new Request(`${base}/_git/export.pack`, { method: "GET" }));
  if (!packRes.ok) throw new Error(`pack export failed: ${packRes.status} ${await packRes.text()}`);
  const packBytes = Buffer.from(await packRes.arrayBuffer());

  const importedStream = "git/demo/imported";
  const importedBase = `http://local/v1/stream/${encodeURIComponent(importedStream)}`;
  await app.fetch(new Request(importedBase, {
    method: "PUT",
    headers: { "content-type": "application/json" },
  }));
  await fetchJson(app, `${importedBase}/_profile`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      apiVersion: "durable.streams/profile/v1",
      profile: { kind: "git-repo", version: 1, objectFormat: "sha1", defaultBranch: "main" },
    }),
  });
  const imported = await fetchJson(app, `${importedBase}/_git/import`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      format: "bundle",
      bundleBase64: bundleBytes.toString("base64"),
      txnId: "demo-bundle-import",
      actor: "demo-agent",
    }),
  });
  const importedBlob = await (await app.fetch(new Request(`${importedBase}/_git/blob?ref=main&path=${encodeURIComponent("/README.md")}`, {
    method: "GET",
  }))).text();

  const pushWork = join(root, "push-work");
  mkdirSync(pushWork, { recursive: true });
  runGitChecked(["git", "init"], pushWork);
  runGitChecked(["git", "config", "user.email", "demo@example.com"], pushWork);
  runGitChecked(["git", "config", "user.name", "Demo Agent"], pushWork);
  writeFileSync(join(pushWork, "PUSHED.md"), "created through git receive-pack\n");
  runGitChecked(["git", "add", "PUSHED.md"], pushWork);
  runGitChecked(["git", "commit", "-m", "Push through smart HTTP"], pushWork);
  const pushedOid = runGitOutput(["git", "rev-parse", "HEAD"], pushWork);
  const remote = `http://127.0.0.1:${server.port}/${encodeURIComponent(stream)}.git`;
  await runGitCheckedAsync(["git", "push", remote, "HEAD:refs/heads/pushed"], pushWork);
  const pushedRefs = await fetchJson(app, `${base}/_git/refs`, { method: "GET" });
  const pushedBlob = await (await app.fetch(new Request(`${base}/_git/blob?ref=pushed&path=${encodeURIComponent("/PUSHED.md")}`, {
    method: "GET",
  }))).text();

  const partialClone = join(root, "partial-clone");
  await runGitCheckedAsync(["git", "clone", "--filter=blob:none", "--no-checkout", remote, partialClone]);
  const partialFilter = runGitOutput(["git", "-C", partialClone, "config", "--get", "remote.origin.partialclonefilter"]);
  const partialMissing = runGitOutput(["git", "-C", partialClone, "rev-list", "--objects", "--missing=print", "HEAD"]);

  console.log("git-repo profile demo");
  console.log(`stream=${stream}`);
  console.log(`blob=${blob.oid}`);
  console.log(`tree=${treeRes.value.oid}`);
  console.log(`commit=${commitRes.value.oid}`);
  console.log(`ref main=${refs.refs["refs/heads/main"]}`);
  console.log(`checkout root=${checkout.rootTreeOid}`);
  console.log(`stat README size=${stat.node.size} oid=${stat.node.oid}`);
  console.log(`readdir / -> ${readdir.entries.map((entry: { path: string }) => entry.path).join(", ")}`);
  console.log(`transaction=${txn.transaction.txnId}`);
  console.log(`transaction status=${txnStatus.status}`);
  console.log(`range bytes=0-4 -> ${JSON.stringify(rangeText)}`);
  console.log(`blob path bytes=0-4 -> ${JSON.stringify(pathRangeText)}`);
  console.log(`export bundle bytes=${bundleBytes.byteLength}`);
  console.log(`export pack bytes=${packBytes.byteLength}`);
  console.log(`imported refs=${imported.imported.refs} objects=${imported.imported.objects}`);
  console.log(`imported README=${JSON.stringify(importedBlob)}`);
  console.log(`smart HTTP pushed ref=${pushedRefs.refs["refs/heads/pushed"]}`);
  console.log(`smart HTTP pushed commit=${pushedOid}`);
  console.log(`smart HTTP pushed file=${JSON.stringify(pushedBlob)}`);
  console.log(`partial clone filter=${partialFilter}`);
  console.log(`partial clone promised README=${partialMissing.includes(`?${blob.oid}`)}`);
  console.log(`mockR2 puts=${store.stats().puts} getBytes=${store.stats().getBytes}`);
} finally {
  server.stop(true);
  app.close();
  rmSync(root, { recursive: true, force: true });
}
