import { Buffer } from "node:buffer";
import { Result } from "better-result";
import {
  writeGitBlob,
  writeGitCommitResult,
  writeGitTreeResult,
  type GitObject,
  type GitObjectFormat,
  type GitPerson,
  type GitRefTransactionResponse,
  type GitTreeEntryInput,
} from "../git_repo";
import { bytesFromText, normalizeRef, toBase64 } from "./model";
import { openVfsRepo, VfsClientError, type VfsFetch } from "./client";
import type { VfsCommit, VfsStoredObject, VfsTreePage } from "./types";

export type VfsMigrationError = {
  message: string;
};

export type MigrateVfsRepoToGitRepoOptions = {
  streamsUrl: string;
  sourceStream: string;
  targetStream: string;
  ref?: string;
  limit?: number;
  authToken?: string;
  fetch?: VfsFetch;
  objectFormat?: GitObjectFormat;
  actor?: string;
};

export type VfsToGitRepoMigrationResult = {
  sourceStream: string;
  targetStream: string;
  ref: string;
  migratedCommits: number;
  migratedObjects: number;
  oldOid: string | null;
  newOid: string | null;
  transaction: GitRefTransactionResponse | null;
};

type TargetGitClient = {
  writeObject(object: GitObject): Promise<void>;
  getRef(ref: string): Promise<string | null>;
  commitRef(args: { ref: string; oldOid: string | null; newOid: string; actor?: string }): Promise<GitRefTransactionResponse>;
};

function joinUrl(base: string, path: string): string {
  return `${base.replace(/\/+$/, "")}${path}`;
}

function encodeStreamPath(stream: string): string {
  return `/v1/stream/${encodeURIComponent(stream)}`;
}

function headers(authToken: string | undefined, extra: HeadersInit = {}): Headers {
  const out = new Headers(extra);
  if (authToken) out.set("authorization", `Bearer ${authToken}`);
  return out;
}

async function requestJson<T>(args: {
  fetch: VfsFetch;
  url: string;
  init?: RequestInit;
}): Promise<T> {
  const res = await args.fetch(args.url, args.init);
  const text = await res.text();
  const body = text === "" ? null : JSON.parse(text);
  if (!res.ok) {
    const message = body && typeof body === "object" && typeof (body as { error?: { message?: unknown } }).error?.message === "string"
      ? String((body as { error: { message: unknown } }).error.message)
      : `request failed with ${res.status}`;
    throw new VfsClientError(message, res.status, body);
  }
  return body as T;
}

async function ensureGitRepoProfile(args: {
  fetch: VfsFetch;
  streamsUrl: string;
  targetStream: string;
  authToken?: string;
  objectFormat: GitObjectFormat;
}): Promise<void> {
  const base = joinUrl(args.streamsUrl, encodeStreamPath(args.targetStream));
  const create = await args.fetch(base, {
    method: "PUT",
    headers: headers(args.authToken, { "content-type": "application/json" }),
  });
  if (!create.ok) throw new VfsClientError(`target stream create failed with ${create.status}`, create.status);
  await requestJson({
    fetch: args.fetch,
    url: `${base}/_profile`,
    init: {
      method: "POST",
      headers: headers(args.authToken, { "content-type": "application/json" }),
      body: JSON.stringify({
        apiVersion: "durable.streams/profile/v1",
        profile: {
          kind: "git-repo",
          version: 1,
          objectFormat: args.objectFormat,
          defaultBranch: "refs/heads/main",
        },
      }),
    },
  });
}

function targetGitClient(args: {
  fetch: VfsFetch;
  streamsUrl: string;
  targetStream: string;
  authToken?: string;
}): TargetGitClient {
  const base = joinUrl(args.streamsUrl, encodeStreamPath(args.targetStream));
  return {
    async writeObject(object) {
      await requestJson({
        fetch: args.fetch,
        url: `${base}/_git/objects`,
        init: {
          method: "POST",
          headers: headers(args.authToken, { "content-type": "application/json" }),
          body: JSON.stringify({
            type: object.type,
            bodyBase64: toBase64(object.body),
            expectedOid: object.oid,
          }),
        },
      });
    },
    async getRef(ref) {
      const body = await requestJson<{ oid: string | null }>({
        fetch: args.fetch,
        url: `${base}/_git/ref/${encodeURIComponent(normalizeRef(ref))}`,
        init: { method: "GET", headers: headers(args.authToken) },
      });
      return body.oid;
    },
    async commitRef(input) {
      return requestJson<GitRefTransactionResponse>({
        fetch: args.fetch,
        url: `${base}/_git/transactions/ref`,
        init: {
          method: "POST",
          headers: headers(args.authToken, { "content-type": "application/json" }),
          body: JSON.stringify({
            txnId: `vfs-migration:${input.ref}:${input.newOid}`,
            idempotencyKey: `vfs-migration:${input.ref}:${input.newOid}`,
            actor: input.actor,
            refUpdates: [{ ref: normalizeRef(input.ref), oldOid: input.oldOid, newOid: input.newOid }],
          }),
        },
      });
    },
  };
}

function gitPersonForCommit(commit: VfsCommit): GitPerson {
  const timestampMs = Date.parse(commit.createdAt);
  const id = commit.author.id.replace(/[^A-Za-z0-9._+-]/g, "-").replace(/^-+|-+$/g, "") || "vfs";
  return {
    name: (commit.author.name ?? commit.author.id).replace(/[<>\r\n]/g, " ").trim() || "VFS Import",
    email: `${id}@vfs-migration.prisma-streams.local`,
    timestampSeconds: Number.isFinite(timestampMs) ? Math.floor(timestampMs / 1000) : Math.floor(Date.now() / 1000),
    timezone: "+0000",
  };
}

export async function migrateVfsRepoToGitRepoResult(
  options: MigrateVfsRepoToGitRepoOptions
): Promise<Result<VfsToGitRepoMigrationResult, VfsMigrationError>> {
  const fetchImpl = options.fetch ?? globalThis.fetch.bind(globalThis);
  const ref = normalizeRef(options.ref ?? "main");
  const objectFormat = options.objectFormat ?? "sha1";
  try {
    await ensureGitRepoProfile({
      fetch: fetchImpl,
      streamsUrl: options.streamsUrl,
      targetStream: options.targetStream,
      authToken: options.authToken,
      objectFormat,
    });

    const source = openVfsRepo({
      streamsUrl: options.streamsUrl,
      stream: options.sourceStream,
      authToken: options.authToken,
      fetch: fetchImpl,
    });
    const target = targetGitClient({
      fetch: fetchImpl,
      streamsUrl: options.streamsUrl,
      targetStream: options.targetStream,
      authToken: options.authToken,
    });

    const commits = (await source.log(ref, options.limit ?? 100)).slice().reverse();
    const migrated = new Map<string, string>();
    const writtenObjects = new Set<string>();

    async function writeOnce(object: GitObject): Promise<void> {
      if (writtenObjects.has(object.oid)) return;
      await target.writeObject(object);
      writtenObjects.add(object.oid);
    }

    async function metadata<T extends VfsStoredObject>(id: string, kind: T["kind"]): Promise<T> {
      const [record] = await source.batchReadMetadata([id]);
      if (!record?.object) throw new VfsClientError(`VFS object not found: ${id}`, 404);
      if (record.object.kind !== kind) throw new VfsClientError(`VFS object ${id} is not ${kind}`, 500);
      return record.object as T;
    }

    async function treeEntries(treeId: string): Promise<GitTreeEntryInput[]> {
      const out: GitTreeEntryInput[] = [];
      let current: string | undefined = treeId;
      while (current) {
        const page: VfsTreePage = await metadata<VfsTreePage>(current, "tree-page");
        for (const entry of page.entries) {
          if (entry.type === "dir") {
            if (!entry.treeId) throw new VfsClientError(`directory ${entry.name} is missing tree id`, 500);
            out.push({ mode: "40000", name: entry.name, oid: await materializeTree(entry.treeId) });
          } else if (entry.type === "symlink") {
            const blob = writeGitBlob(bytesFromText(entry.symlinkTarget ?? ""), objectFormat);
            await writeOnce(blob);
            out.push({ mode: "120000", name: entry.name, oid: blob.oid });
          } else {
            if (!entry.blobId) throw new VfsClientError(`file ${entry.name} is missing blob id`, 500);
            const bytes = await source.readBlob(entry.blobId);
            const blob = writeGitBlob(bytes, objectFormat);
            await writeOnce(blob);
            out.push({ mode: entry.mode === 0o100755 ? "100755" : "100644", name: entry.name, oid: blob.oid });
          }
        }
        current = page.nextPageId;
      }
      return out;
    }

    async function materializeTree(treeId: string): Promise<string> {
      const tree = writeGitTreeResult(await treeEntries(treeId), objectFormat);
      if (Result.isError(tree)) throw new VfsClientError(tree.error.message, 500);
      await writeOnce(tree.value);
      return tree.value.oid;
    }

    async function materializeEmptyTree(): Promise<string> {
      const tree = writeGitTreeResult([], objectFormat);
      if (Result.isError(tree)) throw new VfsClientError(tree.error.message, 500);
      await writeOnce(tree.value);
      return tree.value.oid;
    }

    for (const commit of commits) {
      const treeOid = commit.rootTreeId ? await materializeTree(commit.rootTreeId) : await materializeEmptyTree();
      const parents = commit.parents.map((parent) => migrated.get(parent)).filter((oid): oid is string => typeof oid === "string");
      const person = gitPersonForCommit(commit);
      const gitCommit = writeGitCommitResult({
        tree: treeOid,
        parents,
        author: person,
        committer: person,
        message: commit.message,
      }, objectFormat);
      if (Result.isError(gitCommit)) throw new VfsClientError(gitCommit.error.message, 500);
      await writeOnce(gitCommit.value);
      migrated.set(commit.id, gitCommit.value.oid);
    }

    const oldOid = await target.getRef(ref);
    const newOid = commits.length > 0 ? migrated.get(commits[commits.length - 1]!.id) ?? null : null;
    const transaction = newOid ? await target.commitRef({ ref, oldOid, newOid, actor: options.actor ?? "vfs-migration" }) : null;
    return Result.ok({
      sourceStream: options.sourceStream,
      targetStream: options.targetStream,
      ref,
      migratedCommits: commits.length,
      migratedObjects: writtenObjects.size,
      oldOid,
      newOid,
      transaction,
    });
  } catch (error) {
    return Result.err({ message: error instanceof Error ? error.message : String(error) });
  }
}
