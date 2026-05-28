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
endpoints, pack/idx artifact maintenance publication, and read-only upload-pack
smart HTTP are implemented. Receive-pack push is follow-up work.

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
  };
  importExport?: {
    enabled: boolean;
    allowLocalPathImport: boolean;
    maxBytes: number;
    gitBinary: string;
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
      type: "maintenance-published";
      repoId: string;
      createdAt: string;
      refCheckpointUri?: string;
      packIndexManifestUri?: string;
      preferredClonePackUris?: string[];
    }
  | {
      type: "repo-deleted";
      repoId: string;
      createdAt: string;
    };
```

The current implementation stores ref checkpoints inline in
`maintenance-published` records. Object-store checkpoint artifacts are planned
but not implemented yet.

## Object Store Data

`git-repo` stores canonical Git object bytes as immutable object-store
artifacts, not as base64 payloads in the durable stream. The durable stream
contains ref transactions and artifact pointers. A loose object is stored under
a repo-scoped, content-addressed key:

```text
streams/{streamHash}/git/{objectFormat}/objects/{oid[0..2]}/{oid[2..]}
```

The object-store value is the framed Git object:

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
the object store before committing the transaction. The ref transaction record
is still the visibility point: object artifacts can be uploaded first, but they
are not reachable as repository state until a committed ref transaction points
at them.

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
POST /v1/stream/{repo}/_git/transactions/ref
GET  /v1/stream/{repo}/_git/transactions/{txnId}
POST /v1/stream/{repo}/_git/transactions/{txnId}/wait-published
POST /v1/stream/{repo}/_git/maintenance/publish-ref-checkpoint
POST /v1/stream/{repo}/_git/maintenance/publish-pack
POST /v1/stream/{repo}/_git/maintenance/verify-reachability
GET  /v1/stream/{repo}/_git/smart/info/refs?service=git-upload-pack
POST /v1/stream/{repo}/_git/smart/git-upload-pack
GET  /{repo}.git/info/refs?service=git-upload-pack
POST /{repo}.git/git-upload-pack
```

The path-local read endpoints accept either `commit=<oid>` or `ref=<ref>`.
If neither is present, they use the profile `defaultBranch`.

- `checkout` returns the resolved commit OID and root tree OID for a ref
- `refs?publishedOnly=true` returns refs derived only from records at or below
  the stream `uploaded_through` marker
- `stat` resolves one path by loading only trees along that path
- `readdir` loads one tree and returns a paginated directory listing
- `blob` resolves a file path, then range-reads only the requested blob bytes

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
returns `409 Conflict`. Reusing an already committed `txnId` or
`idempotencyKey` returns the previous transaction result.

`GET /_git/transactions/{txnId}` reports whether the transaction is
`accepted` or `published`. `accepted` means the transaction record is locally
durable in SQLite and visible to the serving process. `published` means the
stream record offset is at or below the stream's `uploaded_through` marker, so
the canonical stream record has reached the remote visibility point. Object
artifacts referenced by the transaction are verified in the object store before
the transaction can be accepted.

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
`preferredClonePackUris`.

`POST /_git/maintenance/verify-reachability` walks current refs through commit,
tree, tag, and blob objects and returns `verified` only when all reachable
objects are available from the canonical object-store namespace.

## Smart HTTP Facade

When profile config sets `http.enabled=true` and `http.allowFetch=true`,
`git-repo` exposes a read-only Git upload-pack facade:

```text
GET  /_git/smart/info/refs?service=git-upload-pack
POST /_git/smart/git-upload-pack
GET  /{repo}.git/info/refs?service=git-upload-pack
POST /{repo}.git/git-upload-pack
```

The facade materializes the current canonical refs and loose objects into a
temporary bare repository and delegates protocol bytes to Git's
`upload-pack --stateless-rpc`. This is an initial compatibility layer for
fetch/clone behavior. The top-level route maps `{repo}` directly to the stream
name, so streams containing slashes can be addressed with raw slashes or percent
encoding. It does not yet provide receive-pack push, partial clone filters, or
packfile-uri support.

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

The current import/export implementation requires `objectFormat: "sha1"`
because it delegates bundle and pack validation to the local Git CLI.

## Ref Race Avoidance

The profile does not rely on a process-local repository lock for correctness.
For a ref transaction, the server:

1. Reads the git-repo stream to a stable end offset.
2. Builds the current ref map from committed transaction records.
3. Verifies every requested `oldOid` against that map.
4. Appends exactly one `ref-transaction-committed` record with the stream's
   expected next offset.

If another process or request appends to the repository stream between steps 1
and 4, the append fails with an offset mismatch. The profile then rereads the
stream and retries the transaction. If the retry sees that a requested ref head
changed, it returns `409 Conflict`; if it sees the same `txnId` or
`idempotencyKey`, it returns the previously committed result.

## Git Object Identity

The helper module `src/git_repo/objects.ts` encodes Git object identity:

```text
blob   oid = hash("blob <size>\0" + bytes)
tree   oid = hash("tree <size>\0" + canonical tree bytes)
commit oid = hash("commit <size>\0" + canonical commit bytes)
```

The test suite checks blob, tree, and commit hashes against `git hash-object`.

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

The compatibility `vfs-repo` profile can be configured with:

```json
{
  "kind": "vfs-repo",
  "version": 1,
  "gitRepo": {
    "stream": "git/tenant/repo"
  }
}
```

When configured, the `workspace-fs` profile treats the configured `git-repo`
stream as canonical. Checkout reads the Git ref head, base path metadata resolves
from Git tree objects, base file reads use the Git loose-object range reader, and
commit builds a Git commit before submitting a `git-repo` ref transaction. The
workspace stream only records draft operations and the final committed marker.

The older `vfs-repo` compatibility profile can also mirror commits into
`git-repo`, but it still appends compatibility VFS ref records for MVP data.

## Current Limits

- Loose Git object artifacts, bundle/pack import/export, and pack/idx
  maintenance publication are implemented. Incremental pack compaction policy is
  still minimal.
- Read-only upload-pack smart HTTP is implemented under `_git/smart/*` and
  `/{repo}.git/*` when enabled in profile config. Receive-pack push, partial
  clone filters, and packfile-uri support are not implemented.
- `workspace-fs` checkout/read/commit paths can use `git-repo` as the canonical
  repository. The older `vfs-repo` profile still retains compatibility VFS
  trees and refs.
