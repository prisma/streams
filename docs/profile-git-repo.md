# `git-repo` Profile

Status: initial implementation

`git-repo` is the canonical repository profile. It stores repository mutation
records in a JSON stream and uses Git object identity for repository content.
The first implementation adds profile installation, `_git` status/ref
endpoints, compare-and-swap ref transactions, loose Git object artifacts in the
remote object store, Git object encoding helpers, lazy object range reads, and
path-local commit/tree/blob reads. Ref transactions use a durable stream
expected-offset append to avoid cross-process races. Git CLI-backed bundle/pack
export and bundle/pack/local-bare-repo import are implemented as profile
endpoints, pack/idx artifact maintenance publication, and smart HTTP
upload-pack/receive-pack compatibility are implemented. Upload-pack forwards
Git protocol v2 negotiation and supports blob filters when `fetch.allowFilter`
is enabled. When `fetch.allowPackfileUris` is enabled and a pack has been
published, upload-pack advertises Git packfile URIs backed by profile-owned
pack artifacts.

Install it on an empty JSON stream:

```http
PUT /v1/stream/git%2Ftenant%2Frepo
Content-Type: application/json

POST /v1/stream/git%2Ftenant%2Frepo/_profile
Content-Type: application/json

{
  "apiVersion": "durable.streams/profile/v1",
  "profile": {
    "kind": "git-repo",
    "version": 1,
    "objectFormat": "sha1",
    "defaultBranch": "refs/heads/main"
  }
}
```

## Profile Shape

```ts
type GitRepoProfileConfig = {
  kind: "git-repo";
  version: 1;
  objectFormat: "sha1" | "sha256";
  defaultBranch: string;
  http?: {
    enabled: boolean;
    allowFetch: boolean;
    allowPush: boolean;
  };
  fetch?: {
    protocolVersion: 2;
    allowFilter: boolean;
    allowDepth: boolean;
    allowPackfileUris: boolean;
  };
  push?: {
    allowAtomic: boolean;
    allowDeleteRefs: boolean;
    maxPackBytes: number;
  };
  materialization?: {
    publishRefCheckpoint: boolean;
    targetPackSizeBytes: number;
    treeIndexPageSize: number;
  };
  importExport?: {
    enabled: boolean;
    allowLocalPathImport: boolean;
    maxBytes: number;
    gitBinary: string;
    gitCommandTimeoutMs: number;
    gitCommandConcurrency: number;
  };
};
```

The stream content type must be `application/json`, and the profile must be
installed before repository records are appended.

## Records

The canonical stream records are repository transactions and maintenance
records, not file diffs:

```ts
type GitRepoRecord =
  | {
      type: "repo-created";
      repoId: string;
      objectFormat: "sha1" | "sha256";
      defaultBranch: string;
      createdAt: string;
    }
  | {
      type: "ref-transaction-committed";
      repoId: string;
      txnId: string;
      idempotencyKey?: string;
      requestHash: string;
      actor?: string;
      createdAt: string;
      refUpdates: Array<{
        ref: string;
        oldOid: string | null;
        newOid: string | null;
      }>;
      objects?: {
        packUri?: string;
        idxUri?: string;
        looseObjectUris?: string[];
        objectCount: number;
        bytes: number;
      };
    }
  | {
      type: "transaction-key-indexed";
      repoId: string;
      keyType: "idempotency-key";
      key: string;
      txnId: string;
      requestHash: string;
      createdAt: string;
    }
  | {
      type: "maintenance-published";
      repoId: string;
      createdAt: string;
      refCheckpointUri?: string;
      packIndexManifestUri?: string;
      preferredClonePackUris?: string[];
      preferredClonePacks?: Array<{
        packUri: string;
        idxUri: string;
        packHash: string;
        objectCount: number;
        bytes: number;
        blobOids: string[];
      }>;
    }
  | {
      type: "repo-deleted";
      repoId: string;
      createdAt: string;
    };
```

The current implementation stores ref checkpoints both inline in
`maintenance-published` records and as profile-owned object-store artifacts. A
fixed latest-checkpoint artifact lets hot ref reads load the checkpoint and
replay only the durable stream tail after `streamOffset` instead of replaying
the full repository history.

## Object Store Data

`git-repo` stores canonical Git object bytes as immutable object-store
artifacts, not as base64 payloads in the durable stream. The durable stream
contains ref transactions and artifact pointers. A framed Git object artifact
is stored under a repo-scoped, content-addressed key:

```text
streams/{streamHash}/git/{objectFormat}/objects/{oid[0..2]}/{oid[2..]}
```

The object-store value is the framed Git object, not Git's zlib-compressed
loose-object file format:

```text
<type> <size>\0<body>
```

`POST /_git/objects` computes the OID from the framed bytes before writing. If
the object key already exists with the same framed size, the profile returns the
existing artifact and skips the PUT. This gives physical de-duplication by Git
object ID and avoids rewriting identical content.

`GET /_git/object/{oid}` reads the object header first, then reads only the
requested body range from the object store. A request such as
`Range: bytes=0-1023` does not fetch the full object body.

Ref transactions may include `objects.looseObjectUris`. The profile verifies
that each referenced URI belongs to the same repo stream prefix and exists in
the object store before committing the transaction. New ref targets are also
read back from the object store, hash-checked against their OID, type-checked,
and walked through reachable commit, tree, tag, and blob objects before the ref
transaction is accepted. The ref transaction record is still the visibility
point: object artifacts can be uploaded first, but they are not reachable as
repository state until a committed ref transaction points at them.

## Endpoints

```text
GET  /v1/stream/{repo}/_git/status
GET  /v1/stream/{repo}/_git/checkout?ref=...
GET  /v1/stream/{repo}/_git/refs
GET  /v1/stream/{repo}/_git/refs?publishedOnly=true
GET  /v1/stream/{repo}/_git/ref/{ref}
GET  /v1/stream/{repo}/_git/stat?ref=...&path=...
GET  /v1/stream/{repo}/_git/readdir?ref=...&path=...&cursor=...
GET  /v1/stream/{repo}/_git/blob?ref=...&path=...
POST /v1/stream/{repo}/_git/import
GET  /v1/stream/{repo}/_git/export.bundle
GET  /v1/stream/{repo}/_git/export.pack
POST /v1/stream/{repo}/_git/objects
GET  /v1/stream/{repo}/_git/object/{oid}
GET  /v1/stream/{repo}/_git/packfile/{packHash}.pack
POST /v1/stream/{repo}/_git/transactions/ref
GET  /v1/stream/{repo}/_git/transactions/{txnId}
POST /v1/stream/{repo}/_git/transactions/{txnId}/wait-published
POST /v1/stream/{repo}/_git/transactions/{txnId}/wait-verified
POST /v1/stream/{repo}/_git/maintenance/publish-ref-checkpoint
POST /v1/stream/{repo}/_git/maintenance/publish-tree-index
POST /v1/stream/{repo}/_git/maintenance/publish-pack
POST /v1/stream/{repo}/_git/maintenance/verify-reachability
GET  /v1/stream/{repo}/_git/smart/info/refs?service=git-upload-pack
GET  /v1/stream/{repo}/_git/smart/info/refs?service=git-receive-pack
POST /v1/stream/{repo}/_git/smart/git-upload-pack
POST /v1/stream/{repo}/_git/smart/git-receive-pack
GET  /{repo}.git/info/refs?service=git-upload-pack
GET  /{repo}.git/info/refs?service=git-receive-pack
POST /{repo}.git/git-upload-pack
POST /{repo}.git/git-receive-pack
```

The path-local read endpoints accept either `commit=<oid>` or `ref=<ref>`.
If neither is present, they use the profile `defaultBranch`.

- `checkout` returns the resolved commit OID and root tree OID for a ref
- `refs?publishedOnly=true` returns refs derived only from records at or below
  the stream `uploaded_through` marker
- normal `status`, `refs`, `ref`, and checkout ref resolution use the latest
  ref checkpoint artifact when present, then replay only records after the
  checkpoint `streamOffset`; within one server process, a bounded ref snapshot
  cache tails the durable stream from the cached offset so hot ref transactions
  do not replay the full repo history
- `stat` resolves one path by loading only trees along that path
- `readdir` loads one tree and returns a paginated directory listing
- when a tree index artifact exists for a directory tree, `stat` loads only the
  matching index page and `readdir` loads only the pages needed for the
  requested cursor/limit
- `blob` resolves a file path, then range-reads only the requested blob bytes

Publish a paged tree index artifact for a large directory:

```json
POST /_git/maintenance/publish-tree-index

{
  "ref": "main",
  "path": "/src",
  "pageSize": 512
}
```

The endpoint can also accept `treeOid` directly. The manifest is keyed by the
Git tree OID under the repo object-store prefix, and pages are keyed by both tree
OID and page size. That avoids mixed-page races when two maintenance jobs build
the same immutable tree with different page sizes. Pages contain sorted entries
plus blob sizes, allowing directory listing and exact-name lookup without
reparsing the monolithic Git tree object on hot paths.

Write a loose object artifact:

```json
{
  "type": "blob",
  "bodyBase64": "aGVsbG8K",
  "expectedOid": "ce013625030ba8dba906f756967f9e9ca394464a"
}
```

The response includes the computed `oid`, `objectKey`, framed byte size, etag,
and whether the write was de-duplicated.

Ref transactions use compare-and-swap semantics:

```json
{
  "txnId": "txn-123",
  "actor": "agent-1",
  "refUpdates": [
    {
      "ref": "refs/heads/main",
      "oldOid": "old commit oid or null",
      "newOid": "new commit oid or null"
    }
  ],
  "objects": {
    "looseObjectUris": [
      "streams/.../git/sha1/objects/..."
    ],
    "objectCount": 3,
    "bytes": 1234
  }
}
```

If any current ref does not match its requested `oldOid`, the transaction
returns `409 Conflict`. Ref names are normalized to `refs/heads/*` for short
branch names and then checked against Git refname rules. `HEAD` is rejected as a
symbolic ref, and empty components, repeated slashes, `..`, `@{`, trailing
dots, `.lock` components, backslashes, control characters, spaces, and Git
wildcard characters are rejected. Requests with more than 1000 ref updates are
rejected rather than truncated.

Every committed transaction stores `requestHash`, a SHA-256 hash of the
canonicalized `refUpdates` and object set. Reusing an already committed `txnId`
or `idempotencyKey` with the same request hash returns the previous transaction
result. Reusing either value with a different request body returns
`409 Conflict`.

Before a transaction becomes visible, every non-null `oldOid` and `newOid` must
match the repository object format. Every `newOid` must exist as a framed Git
object artifact, branch refs must point at commits, other refs must point at
commits or tags, and the referenced commit/tag/tree/blob graph is walked and
hash-verified. Malformed objects, missing reachable objects, wrong tree-entry
target types, and object hash mismatches reject the transaction.

`GET /_git/transactions/{txnId}` reports `accepted`, `published`, or
`verified`. `accepted` means the transaction record is locally durable in
SQLite and visible to the serving process. `published` means the stream record
offset is at or below the stream's `uploaded_through` marker, so the canonical
stream record has reached the remote visibility point. `verified` means the
published transaction's object artifacts still exist and every new ref target
walks through a hash-checked reachable Git object graph. The response includes a
`verification` object with `not_checked`, `verified`, or `failed` status so a
published-but-corrupt transaction is visible instead of being hidden behind a
generic status.

`POST /_git/transactions/{txnId}/wait-published` waits until the stream record
is published and returns either `published` or `verified`. `POST
/_git/transactions/{txnId}/wait-verified` waits until the transaction is both
published and reachability-verified.

## Import And Export

`git-repo` can export the reachable repository state through Git's own
interchange formats:

```text
GET /_git/export.bundle
GET /_git/export.pack
```

Export materializes a temporary bare repository from the canonical loose object
artifacts and committed refs, then asks the configured Git CLI to write a
bundle or pack to the response. The temporary repository is removed before the
request finishes.

`POST /_git/maintenance/publish-pack` materializes the current reachable object
set into a Git pack and idx pair, writes both immutable artifacts to the
repo-scoped Git object-store prefix, and appends a `maintenance-published`
record containing a ref checkpoint, `packIndexManifestUri`, and
`preferredClonePackUris`. The record also includes `preferredClonePacks`, which
captures the Git pack hash, pack/idx artifact URIs, object count, byte size, and
the blob OIDs that can be replaced by a packfile URI during fetch.

`GET /_git/packfile/{packHash}.pack` serves a published preferred clone pack
from the object store when `fetch.allowPackfileUris=true`. The endpoint is used
as the HTTP URI advertised to Git clients that opt into packfile URIs.

`POST /_git/maintenance/verify-reachability` walks current refs through commit,
tree, tag, and blob objects and returns `verified` only when all reachable
objects are available from the canonical object-store namespace.

## Smart HTTP Facade

When profile config sets `http.enabled=true` and `http.allowFetch=true`,
`git-repo` exposes a Git upload-pack facade:

```text
GET  /_git/smart/info/refs?service=git-upload-pack
POST /_git/smart/git-upload-pack
GET  /{repo}.git/info/refs?service=git-upload-pack
POST /{repo}.git/git-upload-pack
```

Fetch materializes the current canonical refs and loose objects into a
process-local bare mirror cache keyed by the repository stream and ref digest,
then delegates protocol bytes to Git's `upload-pack --stateless-rpc`. Repeated
fetch advertisements and RPC requests reuse the cached mirror while refs are
unchanged; when refs change, the mirror is rebuilt from canonical `git-repo`
objects before serving the next fetch. The cache is an optimization only: the
canonical repository remains the stream transaction log plus object-store Git
artifacts.

The profile forwards the HTTP `Git-Protocol` header to the Git process, so
protocol v2 clients can negotiate fetch capabilities. `fetch.allowFilter=true`
configures the cached repository with `uploadpack.allowFilter=true`, enabling
partial-clone requests such as `git clone --filter=blob:none`. If
`fetch.allowFilter=false`, upload-pack requests containing a filter line are
rejected before invoking Git. If `fetch.allowDepth=false`, shallow fetch
requests containing deepen commands are rejected before invoking Git.

If `fetch.allowPackfileUris=true`, upload-pack also configures Git's
experimental `uploadpack.blobPackfileUri` entries from the latest published
preferred clone pack. Protocol v2 clients that opt in with
`fetch.uriprotocols=http` can receive a `packfile-uris` section that points at
`/_git/packfile/{packHash}.pack`. Git still sends the normal packfile section;
clients download and index both before checking connectivity. This follows
Git's packfile URI protocol model while keeping the immutable pack bytes owned
by the profile object-store namespace.

When `http.allowPush=true`, the same profile also exposes receive-pack:

```text
GET  /_git/smart/info/refs?service=git-receive-pack
POST /_git/smart/git-receive-pack
GET  /{repo}.git/info/refs?service=git-receive-pack
POST /{repo}.git/git-receive-pack
```

Push requests are unpacked and validated by Git in a temporary bare repository.
The profile then imports the reachable loose objects into the repo-scoped
object-store namespace and commits the resulting ref changes through the
durable `git-repo` ref transaction service. This keeps the stream transaction
as the canonical visibility point and avoids process-local ref races. If the
canonical refs change after the temporary receive-pack accepts a push but before
the stream transaction commits, the profile returns `409 Conflict` and does not
publish the pushed refs.

The top-level route maps `{repo}` directly to the stream name, so streams
containing slashes can be addressed with raw slashes or percent encoding. The
top-level Git route rejects decoded stream names containing null bytes,
backslashes, empty path components, `.`, or `..` before profile dispatch.

## Multi-Tenant Authorization

Do not expose a multi-tenant `git-repo` server with a raw all-access
`api-key` credential. Use `--auth-strategy scoped-api-key` or an external
tenant gateway that enforces equivalent stream-prefix capabilities before
forwarding to Streams.

For the built-in scoped mode, grant repository streams, workspace streams, and
audit streams separately:

```json
[
  {
    "key": "tenant-a-agent-token",
    "streams": [
      "git/tenant-a/*",
      "workspace/tenant-a/*",
      "evlog/tenant-a/*"
    ],
    "permissions": ["write"]
  }
]
```

`read` allows upload-pack fetch, ref/object reads, and workspace reads. `write`
allows receive-pack push, workspace commits, object uploads, and ref
transactions. `admin` is required for stream profile/schema changes,
`git-repo` import, and maintenance endpoints.

Import accepts:

```json
{ "format": "bundle", "bundleBase64": "..." }
```

```json
{
  "format": "pack",
  "packBase64": "...",
  "refs": {
    "refs/heads/main": "commit oid"
  }
}
```

```json
{
  "format": "local-bare-repo",
  "path": "/absolute/path/to/repo.git"
}
```

Bundle and pack imports unpack into a temporary bare repository, copy reachable
objects into the profile's object-store namespace, and commit one
`ref-transaction-committed` record. Local bare repository import is disabled by
default because it reads server-local paths; it requires
`importExport.allowLocalPathImport=true` and is intended for development or
operator-controlled migration jobs, not tenant-supplied requests.

Loose object writes, path reads, and ref transactions support both
`objectFormat: "sha1"` and `objectFormat: "sha256"`. The current
import/export, pack maintenance, packfile URI, and smart HTTP implementations
require `objectFormat: "sha1"` because they delegate bundle, pack, and protocol
validation to the local Git CLI. SHA-256 repositories fail those compatibility
paths early with `400` rather than invoking Git CLI subprocesses.
Git subprocesses run through an async bounded runner instead of blocking the
request thread. The runner enforces `importExport.gitCommandTimeoutMs` (default
30000), caps concurrent Git jobs with `importExport.gitCommandConcurrency`
(default 2), disables terminal prompting, sets `GIT_CONFIG_NOSYSTEM=1`, and
uses a disabled global Git config path so request-path behavior does not depend
on host-level Git configuration.

## Ref Race Avoidance

The profile does not rely on a process-local repository lock for correctness.
For a ref transaction, the server:

1. Loads the process-local ref snapshot when present and tails the durable
   stream from that offset. Without a cache entry, it loads the latest ref
   checkpoint artifact when present and replays only the durable stream tail
   after its `streamOffset`.
2. Builds the current ref map and stable expected next stream offset from that
   checkpoint plus tail. Transactions with a `txnId` use a keyed transaction
   lookup for idempotent replay. Transactions with an `idempotencyKey` use a
   keyed `transaction-key-indexed` record that points to the canonical
   transaction id and request hash.
3. Verifies every requested `oldOid` against that map, so stale updates fail
   before object graph work.
4. Validates ref names, object IDs, object artifacts, and the reachable Git
   object graph for new ref targets.
5. Appends exactly one `ref-transaction-committed` record, plus one
   `transaction-key-indexed` record when an idempotency key is present, with the
   stream's expected next offset.

The public `_git/transactions/ref` endpoint uses full reachable-graph
validation. Trusted internal producers that just built the new Git objects, such
as `workspace-fs`, may call the service with changed-object verification: the
service still validates ref CAS state, object IDs, object artifact existence,
and the new ref tip object type/hash, but it does not re-walk every historical
tree and blob reachable from the old head. This keeps incremental workspace
commits from rereading the whole repository while preserving full validation for
external ref transaction requests.

If another process or request appends to the repository stream between steps 1
and 4, the append fails with an offset mismatch. The profile then rereads the
stream and retries the transaction. If the retry sees that a requested ref head
changed, it returns `409 Conflict`; if it sees the same `txnId` or
`idempotencyKey` with the same `requestHash`, it returns the previously
committed result. If the hash differs, the retry is rejected as an idempotency
conflict.

## Git Object Identity

The helper module `src/git_repo/objects.ts` encodes Git object identity:

```text
blob   oid = hash("blob <size>\0" + bytes)
tree   oid = hash("tree <size>\0" + canonical tree bytes)
commit oid = hash("commit <size>\0" + canonical commit bytes)
```

The test suite checks blob, tree, and commit hashes against `git hash-object`,
`git mktree`, and `git commit-tree`.

## Demo

Run:

```bash
bun run demo:git-repo
```

The demo installs a `git-repo` profile, writes blob/tree/commit objects as raw
object-store artifacts, commits `refs/heads/main` through a ref transaction, and
uses `checkout`, `stat`, `readdir`, and `blob` to read back the repository from
canonical Git objects.

## Workspace Commits

The `workspace-fs` profile can be configured with:

```json
{
  "kind": "workspace-fs",
  "version": 1,
  "gitRepo": {
    "stream": "git/tenant/repo"
  }
}
```

When configured, `workspace-fs` treats the configured `git-repo` stream as
canonical. Checkout reads the Git ref head, base path metadata resolves from
Git tree objects, base file reads use the Git loose-object range reader, and
commit builds a Git commit before submitting a `git-repo` ref transaction. The
workspace stream only records draft operations and the final committed marker.
Workspace callers can request `durability: "published"` or
`durability: "verified"` to make the commit wait for the corresponding
canonical transaction state before returning.

## Current Limits

- Loose Git object artifacts, bundle/pack import/export, and pack/idx
  maintenance publication are implemented. Incremental pack compaction policy is
  still minimal.
- Ref checkpoint artifacts are implemented for hot ref reads and ordinary ref
  transactions. `txnId` and `idempotencyKey` replay use keyed reads.
- Tree index artifacts are implemented for large directory hot paths. They are
  published by maintenance and used opportunistically by `stat` and `readdir`;
  canonical Git trees remain the source of truth.
- Upload-pack and receive-pack smart HTTP are implemented under `_git/smart/*`
  and `/{repo}.git/*` when enabled in profile config. Upload-pack supports
  protocol v2 negotiation, blob filters when `fetch.allowFilter=true`, and
  packfile URI advertisement/download when `fetch.allowPackfileUris=true` and a
  preferred clone pack has been published.
- Packfile URI support currently uses Git's blob-level
  `uploadpack.blobPackfileUri` mechanism against the latest published pack.
  More granular pack partitioning and resumable clone state are future
  maintenance work.
- `workspace-fs` checkout/read/commit paths use `git-repo` as the canonical
  repository.
