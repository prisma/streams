import { describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createProfileTestApp, fetchJsonApp } from "./profile_test_utils";
import {
  encodeGitCommitResult,
  encodeGitTreeResult,
  writeGitBlob,
  writeGitCommitResult,
  writeGitTreeResult,
} from "../src/git_repo";
import { commitGitRefTransactionResult } from "../src/git_repo/service";
import { Result } from "better-result";

const TEXT_ENCODER = new TextEncoder();
const TEXT_DECODER = new TextDecoder();

function hashObjectWithGit(type: "blob" | "tree" | "commit", body: Uint8Array): string {
  const proc = Bun.spawnSync({
    cmd: ["git", "hash-object", "-t", type, "--stdin"],
    stdin: body,
    stdout: "pipe",
    stderr: "pipe",
  });
  if (proc.exitCode !== 0) {
    const stderr = TEXT_DECODER.decode(proc.stderr);
    throw new Error(`git hash-object failed: ${stderr}`);
  }
  return TEXT_DECODER.decode(proc.stdout).trim();
}

function runGitChecked(cmd: string[], cwd?: string): void {
  const proc = Bun.spawnSync({ cmd, cwd, stdout: "pipe", stderr: "pipe" });
  if (proc.exitCode !== 0) {
    throw new Error(`git command failed: ${cmd.join(" ")}\n${TEXT_DECODER.decode(proc.stderr)}`);
  }
}

function runGitOutput(cmd: string[], cwd?: string): string {
  const proc = Bun.spawnSync({ cmd, cwd, stdout: "pipe", stderr: "pipe" });
  if (proc.exitCode !== 0) {
    throw new Error(`git command failed: ${cmd.join(" ")}\n${TEXT_DECODER.decode(proc.stderr)}`);
  }
  return TEXT_DECODER.decode(proc.stdout).trim();
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

function pktLine(text: string): Buffer {
  const body = Buffer.from(text);
  return Buffer.concat([Buffer.from((body.byteLength + 4).toString(16).padStart(4, "0")), body]);
}

async function installGitRepoProfile(
  app: ReturnType<typeof createProfileTestApp>["app"],
  stream: string,
  overrides: Record<string, unknown> = {}
): Promise<void> {
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
      profile: { kind: "git-repo", version: 1, objectFormat: "sha1", defaultBranch: "main", ...overrides },
    }),
  });
  expect(profile.status).toBe(200);
}

async function writeGitObjectApp(
  app: ReturnType<typeof createProfileTestApp>["app"],
  base: string,
  object: { type: "blob" | "tree" | "commit" | "tag"; oid: string; body: Uint8Array }
): Promise<any> {
  const res = await fetchJsonApp(app, `${base}/_git/objects`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      type: object.type,
      expectedOid: object.oid,
      bodyBase64: Buffer.from(object.body).toString("base64"),
    }),
  });
  expect(res.status).toBe(200);
  return res.body;
}

describe("git object writer", () => {
  test("hashes blobs with Git object identity", () => {
    const blob = writeGitBlob("hello\n");
    expect(blob.oid).toBe("ce013625030ba8dba906f756967f9e9ca394464a");
    expect(hashObjectWithGit("blob", blob.body)).toBe(blob.oid);
  });

  test("encodes tree and commit objects compatible with git hash-object", () => {
    const blob = writeGitBlob("hello\n");
    const treeBody = encodeGitTreeResult([{ mode: "100644", name: "hello.txt", oid: blob.oid }]);
    expect(Result.isOk(treeBody)).toBe(true);
    if (Result.isError(treeBody)) throw new Error(treeBody.error.message);
    const tree = writeGitTreeResult([{ mode: "100644", name: "hello.txt", oid: blob.oid }]);
    expect(Result.isOk(tree)).toBe(true);
    if (Result.isError(tree)) throw new Error(tree.error.message);
    expect(hashObjectWithGit("tree", treeBody.value)).toBe(tree.value.oid);

    const commitInput = {
      tree: tree.value.oid,
      author: {
        name: "Agent",
        email: "agent@example.com",
        timestampSeconds: 1_700_000_000,
        timezone: "+0000",
      },
      message: "Initial import\n",
    };
    const commitBody = encodeGitCommitResult(commitInput);
    expect(Result.isOk(commitBody)).toBe(true);
    if (Result.isError(commitBody)) throw new Error(commitBody.error.message);
    const commit = writeGitCommitResult(commitInput);
    expect(Result.isOk(commit)).toBe(true);
    if (Result.isError(commit)) throw new Error(commit.error.message);
    expect(hashObjectWithGit("commit", commitBody.value)).toBe(commit.value.oid);
  });
});

describe("git-repo profile", () => {
  test("stores loose Git objects as de-duplicated object-store artifacts and reads ranges lazily", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-git-objects-"));
    const { app, store } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    try {
      const stream = "git/test/objects";
      const base = `http://local/v1/stream/${encodeURIComponent(stream)}`;
      await installGitRepoProfile(app, stream);
      store.resetStats();

      const bytes = new Uint8Array(1024 * 1024);
      for (let i = 0; i < bytes.byteLength; i++) bytes[i] = i % 251;
      const expected = writeGitBlob(bytes);
      const create = await fetchJsonApp(app, `${base}/_git/objects`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          type: "blob",
          bodyBase64: Buffer.from(bytes).toString("base64"),
          expectedOid: expected.oid,
        }),
      });
      expect(create.status).toBe(200);
      expect(create.body.oid).toBe(expected.oid);
      expect(create.body.objectKey).toContain("/git/sha1/objects/");
      expect(create.body.deduplicated).toBe(false);
      expect(store.stats().puts).toBe(1);

      const duplicate = await fetchJsonApp(app, `${base}/_git/objects`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          type: "blob",
          bodyBase64: Buffer.from(bytes).toString("base64"),
          expectedOid: expected.oid,
        }),
      });
      expect(duplicate.status).toBe(200);
      expect(duplicate.body.deduplicated).toBe(true);
      expect(store.stats().puts).toBe(1);

      store.resetStats();
      const range = await app.fetch(new Request(`${base}/_git/object/${expected.oid}`, {
        method: "GET",
        headers: { range: "bytes=10-19" },
      }));
      expect(range.status).toBe(206);
      expect(range.headers.get("content-range")).toBe(`bytes 10-19/${bytes.byteLength}`);
      expect(new Uint8Array(await range.arrayBuffer())).toEqual(bytes.slice(10, 20));
      expect(store.stats().getBytes).toBeLessThan(2048);

      const commit = await fetchJsonApp(app, `${base}/_git/transactions/ref`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          txnId: "object-txn",
          refUpdates: [{ ref: "main", oldOid: null, newOid: expected.oid }],
          objects: {
            looseObjectUris: [create.body.objectKey],
            objectCount: 1,
            bytes: create.body.framedSize,
          },
        }),
      });
      expect(commit.status).toBe(200);
      expect(commit.body.refs["refs/heads/main"]).toBe(expected.oid);

      const missing = await fetchJsonApp(app, `${base}/_git/transactions/ref`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          txnId: "missing-object-txn",
          refUpdates: [{ ref: "refs/heads/other", oldOid: null, newOid: expected.oid }],
          objects: {
            looseObjectUris: [`${create.body.objectKey}.missing`],
            objectCount: 1,
            bytes: create.body.framedSize,
          },
        }),
      });
      expect(missing.status).toBe(409);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("commits ref transactions with compare-and-swap semantics", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-git-repo-"));
    const { app } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    try {
      const stream = "git/test/repo";
      const base = `http://local/v1/stream/${encodeURIComponent(stream)}`;
      await installGitRepoProfile(app, stream);

      const blob = writeGitBlob(TEXT_ENCODER.encode("hello\n"));
      const commit = await fetchJsonApp(app, `${base}/_git/transactions/ref`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          txnId: "txn-1",
          actor: "agent",
          refUpdates: [{ ref: "refs/heads/main", oldOid: null, newOid: blob.oid }],
          objects: { objectCount: 1, bytes: blob.size },
        }),
      });
      expect(commit.status).toBe(200);
      expect(commit.body.idempotent).toBe(false);
      expect(commit.body.refs["refs/heads/main"]).toBe(blob.oid);

      const txnStatus = await fetchJsonApp(app, `${base}/_git/transactions/txn-1`, { method: "GET" });
      expect(txnStatus.status).toBe(200);
      expect(txnStatus.body.txnId).toBe("txn-1");
      expect(txnStatus.body.transaction.txnId).toBe("txn-1");
      expect(["accepted", "published"]).toContain(txnStatus.body.status);

      const waitStatus = await fetchJsonApp(app, `${base}/_git/transactions/txn-1/wait-published?timeout_ms=1`, { method: "POST" });
      expect([200, 202]).toContain(waitStatus.status);
      expect(waitStatus.body.txnId).toBe("txn-1");

      const idempotent = await fetchJsonApp(app, `${base}/_git/transactions/ref`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          txnId: "txn-1",
          refUpdates: [{ ref: "refs/heads/main", oldOid: null, newOid: blob.oid }],
        }),
      });
      expect(idempotent.status).toBe(200);
      expect(idempotent.body.idempotent).toBe(true);

      const stale = await fetchJsonApp(app, `${base}/_git/transactions/ref`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          txnId: "txn-2",
          refUpdates: [{ ref: "refs/heads/main", oldOid: null, newOid: writeGitBlob("next\n").oid }],
        }),
      });
      expect(stale.status).toBe(409);

      const ref = await fetchJsonApp(app, `${base}/_git/ref/refs/heads/main`, { method: "GET" });
      expect(ref.status).toBe(200);
      expect(ref.body.oid).toBe(blob.oid);

      const checkpoint = await fetchJsonApp(app, `${base}/_git/maintenance/publish-ref-checkpoint`, { method: "POST" });
      expect(checkpoint.status).toBe(200);
      expect(checkpoint.body.checkpoint.refs["refs/heads/main"]).toBe(blob.oid);
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("retries durable stream offset races before committing a ref transaction", async () => {
    const mainOid = writeGitBlob("main\n").oid;
    const sideOid = writeGitBlob("side\n").oid;
    const records: any[] = [];
    let raced = false;
    let successfulExpectedOffset: bigint | null = null;

    const reader = {
      async readResult() {
        const end = BigInt(records.length - 1);
        return Result.ok({
          stream: "git/test/race",
          format: "raw" as const,
          key: null,
          requestOffset: "-1",
          endOffset: "unused",
          nextOffset: "unused",
          endOffsetSeq: end,
          nextOffsetSeq: end,
          records: records.map((value, index) => ({
            offset: BigInt(index),
            payload: TEXT_ENCODER.encode(JSON.stringify(value)),
          })),
        });
      },
    };

    const appendJsonRecords = async (args: { records: Array<{ value: unknown }>; expectedNextOffset?: bigint | null }) => {
      if (!raced) {
        raced = true;
        records.push({
          type: "ref-transaction-committed",
          repoId: "git/test/race",
          txnId: "competing-txn",
          createdAt: new Date().toISOString(),
          refUpdates: [{ ref: "refs/heads/side", oldOid: null, newOid: sideOid }],
        });
        return Result.err({ kind: "offset_mismatch" as const, message: "simulated stream race" });
      }
      successfulExpectedOffset = args.expectedNextOffset ?? null;
      records.push(...args.records.map((record) => record.value));
      return Result.ok({
        result: {
          lastOffset: BigInt(records.length - 1),
          appendedRows: args.records.length,
          closed: false,
          duplicate: false,
        },
      });
    };

    const result = await commitGitRefTransactionResult({
      stream: "git/test/race",
      reader: reader as any,
      objectStore: {} as any,
      appendJsonRecords: appendJsonRecords as any,
      format: "sha1",
      request: {
        txnId: "main-txn",
        refUpdates: [{ ref: "refs/heads/main", oldOid: null, newOid: mainOid }],
      },
    });

    expect(Result.isOk(result)).toBe(true);
    if (Result.isError(result)) throw new Error(result.error.message);
    expect(successfulExpectedOffset).toBe(1n);
    expect(result.value.refs["refs/heads/side"]).toBe(sideOid);
    expect(result.value.refs["refs/heads/main"]).toBe(mainOid);
  });

  test("exports bundles and packs and imports a bundle into another git-repo profile", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-git-import-export-"));
    const { app, store } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    try {
      const source = "git/test/export-source";
      const target = "git/test/import-target";
      const sourceBase = `http://local/v1/stream/${encodeURIComponent(source)}`;
      const targetBase = `http://local/v1/stream/${encodeURIComponent(target)}`;
      await installGitRepoProfile(app, source, { http: { enabled: true, allowFetch: true, allowPush: false } });
      await installGitRepoProfile(app, target);

      const blob = writeGitBlob("hello import/export\n");
      const tree = writeGitTreeResult([{ mode: "100644", name: "hello.txt", oid: blob.oid }]);
      expect(Result.isOk(tree)).toBe(true);
      if (Result.isError(tree)) throw new Error(tree.error.message);
      const commit = writeGitCommitResult({
        tree: tree.value.oid,
        author: {
          name: "Agent",
          email: "agent@example.com",
          timestampSeconds: 1_700_000_100,
          timezone: "+0000",
        },
        message: "Import export source\n",
      });
      expect(Result.isOk(commit)).toBe(true);
      if (Result.isError(commit)) throw new Error(commit.error.message);

      const blobArtifact = await writeGitObjectApp(app, sourceBase, blob);
      const treeArtifact = await writeGitObjectApp(app, sourceBase, tree.value);
      const commitArtifact = await writeGitObjectApp(app, sourceBase, commit.value);
      const txn = await fetchJsonApp(app, `${sourceBase}/_git/transactions/ref`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          txnId: "source-commit",
          refUpdates: [{ ref: "main", oldOid: null, newOid: commit.value.oid }],
          objects: {
            looseObjectUris: [blobArtifact.objectKey, treeArtifact.objectKey, commitArtifact.objectKey],
            objectCount: 3,
            bytes: blobArtifact.framedSize + treeArtifact.framedSize + commitArtifact.framedSize,
          },
        }),
      });
      expect(txn.status).toBe(200);

      const unpublishedRefs = await fetchJsonApp(app, `${sourceBase}/_git/refs?publishedOnly=true`, { method: "GET" });
      expect(unpublishedRefs.status).toBe(200);
      expect(unpublishedRefs.body.refs["refs/heads/main"]).toBeUndefined();

      const verified = await fetchJsonApp(app, `${sourceBase}/_git/maintenance/verify-reachability`, { method: "POST" });
      expect(verified.status).toBe(200);
      expect(verified.body.status).toBe("verified");
      expect(verified.body.refs["refs/heads/main"]).toBe(commit.value.oid);
      expect(verified.body.objectCount).toBe(3);

      const bundleRes = await app.fetch(new Request(`${sourceBase}/_git/export.bundle`, { method: "GET" }));
      expect(bundleRes.status).toBe(200);
      const bundle = Buffer.from(await bundleRes.arrayBuffer());
      expect(bundle.subarray(0, 16).toString()).toContain("git bundle");

      const packRes = await app.fetch(new Request(`${sourceBase}/_git/export.pack`, { method: "GET" }));
      expect(packRes.status).toBe(200);
      const pack = Buffer.from(await packRes.arrayBuffer());
      expect(pack.subarray(0, 4).toString()).toBe("PACK");

      const uploadPackRefs = await app.fetch(new Request(`${sourceBase}/_git/smart/info/refs?service=git-upload-pack`, { method: "GET" }));
      expect(uploadPackRefs.status).toBe(200);
      expect(uploadPackRefs.headers.get("content-type")).toBe("application/x-git-upload-pack-advertisement");
      const advertised = Buffer.from(await uploadPackRefs.arrayBuffer()).toString("utf8");
      expect(advertised).toContain("# service=git-upload-pack");
      expect(advertised).toContain(commit.value.oid);

      const topLevelRefs = await app.fetch(new Request(`http://local/${source}.git/info/refs?service=git-upload-pack`, { method: "GET" }));
      expect(topLevelRefs.status).toBe(200);
      expect(topLevelRefs.headers.get("content-type")).toBe("application/x-git-upload-pack-advertisement");
      expect(Buffer.from(await topLevelRefs.arrayBuffer()).toString("utf8")).toContain(commit.value.oid);

      const publishedPack = await fetchJsonApp(app, `${sourceBase}/_git/maintenance/publish-pack`, { method: "POST" });
      expect(publishedPack.status).toBe(200);
      expect(publishedPack.body.packUri).toContain("/git/sha1/packs/");
      expect(publishedPack.body.idxUri).toContain("/git/sha1/packs/");
      expect(publishedPack.body.packBytes).toBeGreaterThan(0);
      expect(publishedPack.body.idxBytes).toBeGreaterThan(0);
      expect(publishedPack.body.objectCount).toBe(3);
      expect(store.has(publishedPack.body.packUri)).toBe(true);
      expect(store.has(publishedPack.body.idxUri)).toBe(true);
      expect(publishedPack.body.record.preferredClonePackUris).toEqual([publishedPack.body.packUri]);
      expect(publishedPack.body.record.refCheckpoint.refs["refs/heads/main"]).toBe(commit.value.oid);

      const localPathDenied = await fetchJsonApp(app, `${targetBase}/_git/import`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ format: "local-bare-repo", path: root }),
      });
      expect(localPathDenied.status).toBe(409);

      const imported = await fetchJsonApp(app, `${targetBase}/_git/import`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          format: "bundle",
          bundleBase64: bundle.toString("base64"),
          txnId: "bundle-import",
          actor: "importer",
        }),
      });
      expect(imported.status).toBe(200);
      expect(imported.body.imported.refs).toBeGreaterThanOrEqual(1);
      expect(imported.body.imported.objects).toBeGreaterThanOrEqual(3);
      expect(imported.body.refs["refs/heads/main"]).toBe(commit.value.oid);

      const importedBlob = await app.fetch(new Request(`${targetBase}/_git/blob?ref=main&path=${encodeURIComponent("/hello.txt")}`, { method: "GET" }));
      expect(importedBlob.status).toBe(200);
      expect(await importedBlob.text()).toBe("hello import/export\n");
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("accepts Git smart HTTP receive-pack pushes through durable ref transactions", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-git-receive-pack-"));
    const { app } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    try {
      const stream = "git-push-test";
      const base = `http://local/v1/stream/${encodeURIComponent(stream)}`;
      await installGitRepoProfile(app, stream, { http: { enabled: true, allowFetch: true, allowPush: true } });

      const work = join(root, "work");
      mkdirSync(work, { recursive: true });
      runGitChecked(["git", "init"], work);
      runGitChecked(["git", "config", "user.email", "agent@example.com"], work);
      runGitChecked(["git", "config", "user.name", "Agent"], work);
      writeFileSync(join(work, "README.md"), "pushed over smart http\n");
      runGitChecked(["git", "add", "README.md"], work);
      runGitChecked(["git", "commit", "-m", "Smart HTTP push"], work);
      const pushedOid = runGitOutput(["git", "rev-parse", "HEAD"], work);

      const remote = `http://127.0.0.1:${server.port}/${encodeURIComponent(stream)}.git`;
      const advertised = await fetch(`${remote}/info/refs?service=git-receive-pack`, { method: "GET" });
      expect(advertised.status).toBe(200);
      expect(advertised.headers.get("content-type")).toBe("application/x-git-receive-pack-advertisement");
      expect(Buffer.from(await advertised.arrayBuffer()).toString("utf8")).toContain("# service=git-receive-pack");

      await runGitCheckedAsync(["git", "push", remote, "HEAD:refs/heads/main"], work);

      const refs = await fetchJsonApp(app, `${base}/_git/refs`, { method: "GET" });
      expect(refs.status).toBe(200);
      expect(refs.body.refs["refs/heads/main"]).toBe(pushedOid);

      const pushedReadme = await app.fetch(new Request(`${base}/_git/blob?ref=main&path=${encodeURIComponent("/README.md")}`, { method: "GET" }));
      expect(pushedReadme.status).toBe(200);
      expect(await pushedReadme.text()).toBe("pushed over smart http\n");

      const verified = await fetchJsonApp(app, `${base}/_git/maintenance/verify-reachability`, { method: "POST" });
      expect(verified.status).toBe(200);
      expect(verified.body.status).toBe("verified");
      expect(verified.body.refs["refs/heads/main"]).toBe(pushedOid);
      expect(verified.body.objectCount).toBe(3);
    } finally {
      server.stop(true);
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("supports partial clone blob filters through upload-pack", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-git-partial-clone-"));
    const { app } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    const server = Bun.serve({ port: 0, fetch: app.fetch });
    try {
      const stream = "git-filter-test";
      const base = `http://local/v1/stream/${encodeURIComponent(stream)}`;
      await installGitRepoProfile(app, stream, {
        http: { enabled: true, allowFetch: true, allowPush: false },
        fetch: { protocolVersion: 2, allowFilter: true, allowDepth: true, allowPackfileUris: false },
      });

      const blob = writeGitBlob("large-ish blob that should stay promised\n".repeat(1024));
      const tree = writeGitTreeResult([{ mode: "100644", name: "big.txt", oid: blob.oid }]);
      expect(Result.isOk(tree)).toBe(true);
      if (Result.isError(tree)) throw new Error(tree.error.message);
      const commit = writeGitCommitResult({
        tree: tree.value.oid,
        author: {
          name: "Agent",
          email: "agent@example.com",
          timestampSeconds: 1_700_000_200,
          timezone: "+0000",
        },
        message: "Partial clone source\n",
      });
      expect(Result.isOk(commit)).toBe(true);
      if (Result.isError(commit)) throw new Error(commit.error.message);

      const blobArtifact = await writeGitObjectApp(app, base, blob);
      const treeArtifact = await writeGitObjectApp(app, base, tree.value);
      const commitArtifact = await writeGitObjectApp(app, base, commit.value);
      const txn = await fetchJsonApp(app, `${base}/_git/transactions/ref`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          txnId: "partial-clone-source",
          refUpdates: [{ ref: "main", oldOid: null, newOid: commit.value.oid }],
          objects: {
            looseObjectUris: [blobArtifact.objectKey, treeArtifact.objectKey, commitArtifact.objectKey],
            objectCount: 3,
            bytes: blobArtifact.framedSize + treeArtifact.framedSize + commitArtifact.framedSize,
          },
        }),
      });
      expect(txn.status).toBe(200);

      const v2Advertised = await fetch(`http://127.0.0.1:${server.port}/${stream}.git/info/refs?service=git-upload-pack`, {
        method: "GET",
        headers: { "git-protocol": "version=2" },
      });
      expect(v2Advertised.status).toBe(200);
      expect(Buffer.from(await v2Advertised.arrayBuffer()).toString("utf8")).toContain("filter");

      const cloneDir = join(root, "clone");
      const remote = `http://127.0.0.1:${server.port}/${stream}.git`;
      await runGitCheckedAsync(["git", "clone", "--filter=blob:none", "--no-checkout", remote, cloneDir]);
      expect(runGitOutput(["git", "-C", cloneDir, "config", "--get", "remote.origin.promisor"])).toBe("true");
      expect(runGitOutput(["git", "-C", cloneDir, "config", "--get", "remote.origin.partialclonefilter"])).toBe("blob:none");
      const missing = runGitOutput(["git", "-C", cloneDir, "rev-list", "--objects", "--missing=print", "HEAD"]);
      expect(missing).toContain(`?${blob.oid}`);

      const disallowedStream = "git-filter-disabled";
      const disallowedBase = `http://local/v1/stream/${encodeURIComponent(disallowedStream)}`;
      await installGitRepoProfile(app, disallowedStream, {
        http: { enabled: true, allowFetch: true, allowPush: false },
        fetch: { protocolVersion: 2, allowFilter: false, allowDepth: true, allowPackfileUris: false },
      });
      const rejected = await app.fetch(new Request(`${disallowedBase}/_git/smart/git-upload-pack`, {
        method: "POST",
        headers: {
          "content-type": "application/x-git-upload-pack-request",
          "git-protocol": "version=2",
        },
        body: Buffer.concat([pktLine("command=fetch\n"), pktLine("filter blob:none\n"), Buffer.from("0000")]),
      }));
      expect(rejected.status).toBe(400);
      expect(await rejected.text()).toContain("filter fetches are disabled");
    } finally {
      server.stop(true);
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("imports an operator-enabled local bare repository path", async () => {
    const root = mkdtempSync(join(tmpdir(), "ds-git-local-import-"));
    const { app } = createProfileTestApp(root, { metricsFlushIntervalMs: 0 });
    try {
      const work = join(root, "work");
      const bare = join(root, "source.git");
      mkdirSync(work, { recursive: true });
      runGitChecked(["git", "init"], work);
      runGitChecked(["git", "config", "user.email", "agent@example.com"], work);
      runGitChecked(["git", "config", "user.name", "Agent"], work);
      writeFileSync(join(work, "README.md"), "local import\n");
      runGitChecked(["git", "add", "README.md"], work);
      runGitChecked(["git", "commit", "-m", "Local import source"], work);
      runGitChecked(["git", "branch", "-M", "main"], work);
      runGitChecked(["git", "clone", "--bare", work, bare]);

      const stream = "git/test/local-import";
      const base = `http://local/v1/stream/${encodeURIComponent(stream)}`;
      await installGitRepoProfile(app, stream, {
        importExport: { enabled: true, allowLocalPathImport: true, maxBytes: 64 * 1024 * 1024, gitBinary: "git" },
      });
      const imported = await fetchJsonApp(app, `${base}/_git/import`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          format: "local-bare-repo",
          path: bare,
          txnId: "local-import",
          actor: "operator",
        }),
      });
      expect(imported.status).toBe(200);
      expect(imported.body.imported.refs).toBeGreaterThanOrEqual(1);
      expect(imported.body.refs["refs/heads/main"]).toMatch(/^[0-9a-f]{40}$/);

      const readme = await app.fetch(new Request(`${base}/_git/blob?ref=main&path=${encodeURIComponent("/README.md")}`, { method: "GET" }));
      expect(readme.status).toBe(200);
      expect(await readme.text()).toBe("local import\n");
    } finally {
      app.close();
      rmSync(root, { recursive: true, force: true });
    }
  });
});
