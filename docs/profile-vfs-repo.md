# `vfs-repo` Profile

`vfs-repo` is the MVP Git-like virtual filesystem profile for agent workspaces.
It is now a compatibility profile while the repository architecture is split
into [git-repo](./profile-git-repo.md) for canonical repository storage and
[workspace-fs](./profile-workspace-fs.md) for derived lazy agent workspaces.

The profile still stores immutable snapshot objects and durable workspace draft
operations on top of ordinary Prisma Streams JSON appends, but new canonical
repository work should target `git-repo` rather than expanding `vfs-repo`.
The runtime route is a thin legacy export over the `workspace-fs` server module
so the agent workspace implementation has one code path.

Install it on an empty JSON stream:

```http
PUT /v1/stream/vfs%2Ftenant%2Frepo%2Fcontrol
Content-Type: application/json

POST /v1/stream/vfs%2Ftenant%2Frepo%2Fcontrol/_profile
Content-Type: application/json

{
  "apiVersion": "durable.streams/profile/v1",
  "profile": {
    "kind": "vfs-repo",
    "version": 1,
    "gitRepo": {
      "stream": "git/tenant/repo"
    }
  }
}
```

`gitRepo.stream` is optional. When it is set, workspace commits still write the
compatibility VFS snapshot records, but they first materialize the workspace
tree as real Git blob/tree/commit objects in the configured `git-repo` stream
and commit that stream's ref transaction.

The repo control stream stores ref updates keyed by `ref:<ref>`. Companion
streams under the repo name store immutable objects and workspace operations:

```text
{repo}/_vfs/objects
{repo}/_vfs/workspaces/{workspaceId}
```

## Object Model

The profile uses Git-like object shapes without exposing Git protocol:

- `blob` manifests are content addressed and either inline small content or
  reference immutable `chunk` objects.
- `tree-page` objects store sorted directory entries. The MVP writes one page
  per directory and keeps `nextPageId` available for larger-page evolution.
- `commit` objects point at a root tree and parent commits.
- `ref-update` records in the control stream are the visibility commit point.

The ref update remains separate from object writes. Objects may be appended
before the commit is visible; the commit becomes visible when the ref update is
accepted.

## Remote Object Store Interaction

The VFS profile does not write repository objects directly to R2 or MockR2.
Blob manifests, chunk objects, tree pages, commit objects, ref updates, and
workspace records are ordinary JSON records in Streams. In full mode, those
records reach the remote object store through the normal Streams pipeline:

```text
SQLite WAL append
  -> sealed segment
  -> segment object upload
  -> stream manifest publication
```

This means VFS has two visibility states:

- `accepted`: the VFS endpoint has appended the required stream records and the
  local SQLite transaction has been acknowledged.
- `published`: the streams that contain those records have uploaded their
  segments and published manifests that include the records.

An accepted commit is immediately visible to the same server process through
SQLite-backed stream reads. It is not recoverable from remote object storage
until the relevant stream manifests have been published. Remote recovery and
`--bootstrap-from-r2` use only published stream history, matching the
durability model in [overview.md](./overview.md#current-durability-model).

### Remote Publication Boundaries

The VFS repo is stored across multiple streams:

```text
{repo}                         ref updates
{repo}/_vfs/objects            blobs, chunks, tree pages, commits
{repo}/_vfs/workspaces/{id}    workspace draft records and markers
```

Streams publishes manifests independently per stream. There is no cross-stream
remote transaction in the MVP. The local commit endpoint writes object records
before the ref update, but object-store publication may expose the control
stream manifest and object stream manifest at different times.

A remote/bootstrap reader must therefore treat a ref as remotely usable only
after the referenced commit object, root tree, and recursively referenced tree
and blob metadata can be resolved from published object-stream history. If the
control stream manifest contains a ref update whose referenced objects are not
yet published, the supported behavior is to treat that commit as not yet
published and retry later rather than serving a partial checkout. Object ids are
immutable and content addressed, so late publication fills in the exact missing
objects instead of changing their meaning.

### Race Avoidance

Request-path races are avoided by the VFS commit endpoint:

- The endpoint holds an in-process lock per repo stream while committing.
- It reads the current ref head and compares it with `expectedHead`.
- A stale `expectedHead` returns `409 Conflict` before any ref update is
  appended.
- New objects are appended before the ref update record.
- The ref update is the local visibility commit point.
- The workspace is marked committed only after the ref update append succeeds.

Remote object-store races are avoided by the generic Streams publication
contract:

- For each stream, uploaded segments form a contiguous prefix; later segments do
  not bypass an earlier missing segment in the published manifest.
- `uploaded_through` advances only after manifest upload succeeds.
- WAL rows are garbage collected only after the manifest publication point.
- Readers recovering from object storage follow manifests, not uncommitted or
  partially uploaded segment objects.

The remaining MVP limitation is cross-stream publication. VFS does not yet have
a repo-wide published-commit marker that atomically covers the control stream
and object stream. Until that exists, remote readers must validate object
reachability before treating a published ref update as a complete remote
checkout.

### Compression And De-duplication

VFS object identity is content-addressed:

- blob ids are derived from their canonical manifest
- large file chunks are addressed by the chunk byte hash
- tree pages are addressed by canonical directory entries
- commit ids are derived from canonical commit content

This prevents identity races: the same id always refers to the same content.
It also gives logical de-duplication across file contents and metadata. If two
files or commits reference the same chunk, manifest, or tree page id, readers
load the same immutable object.

The current MVP performs request-local physical de-duplication before appending
VFS object records: if one workspace operation batch or commit writes the same
content-addressed object more than once, only one copy is appended in that
batch. It does not yet perform stream-history de-duplication. Rewriting content
that was already accepted in an earlier request may append another JSON record
with the same content-addressed id, and remote stream segments may therefore
contain duplicate historical object records. Routing keys by object id make
lookups converge on that immutable object, but they do not by themselves remove
duplicate bytes from already accepted stream history.

Current VFS blob storage is conservative:

- small blobs are inlined as base64 inside the blob manifest
- larger blobs are split into 64 KiB chunks
- chunk records store base64 data with `compression: "none"`
- tree pages, commits, refs, and workspace operations are JSON records

Any compression applied below that level is the normal Streams segment/storage
compression behavior. The VFS profile does not yet zstd-compress individual
blob chunks or write chunk bytes directly as object-store-native companion
objects. If that is added later, the profile must preserve the same immutable
object id semantics and keep the ref update as the visibility point.

## Multi-Tenant Security

The recommended stream naming shape is tenant-scoped:

```text
vfs/{tenant}/{repo}/control
```

That naming shape is for organization and routing only. It is not an
authorization boundary inside Streams. The full server's built-in auth model is
authentication-only: one valid API key can access every endpoint and every
stream served by that process. The VFS profile does not add per-tenant ACLs,
scoped keys, object-store prefixes with independent credentials, or
profile-owned authorization checks.

Multi-tenant deployments must therefore put VFS behind one of these supported
trust boundaries:

- a gateway that authenticates the caller and authorizes every repo stream name
  before forwarding to Streams
- isolated Streams server instances per tenant or trust boundary
- separate object-store buckets/accounts when tenants must not share a remote
  storage namespace

The gateway must enforce tenant ownership for all VFS streams that belong to a
repo, including the control stream, the object stream, and workspace streams.
It must also enforce quotas such as maximum file size, total workspace bytes,
workspace count, command timeout, and object-store egress. just-bash execution
limits and disabled network access are defense-in-depth for agent commands;
they do not replace tenant authorization.

Object-store keys are derived from stream hashes and companion object layouts,
not from a tenant-aware authorization model. Anyone with direct access to the
underlying bucket or a server-wide Streams credential can bypass VFS tenant
intent. Do not expose R2 credentials or the raw Streams API directly to
untrusted tenants.

## Workspace Model

Checkout creates a durable workspace stream and appends a
`workspace-checkout` record with the base ref, base commit, and root tree. File
writes append draft operations to that workspace stream:

- `put-file`
- `delete`
- `mkdir`
- `rename`
- `symlink`

Repo state does not change until the workspace commit endpoint succeeds. The
commit endpoint holds an in-process per-repo lock, checks the expected ref head,
materializes the workspace overlay into new tree pages, writes the commit
object, appends the ref update, and marks the workspace committed. A stale
expected head returns `409 Conflict`.

If `gitRepo.stream` is configured, the commit endpoint also converts the new VFS
tree into Git objects:

```text
VFS blob/symlink/dir entries
  -> Git blob/tree objects in the git-repo object store
  -> Git commit object
  -> git-repo ref transaction with oldOid/newOid
  -> VFS ref update and workspace committed marker
```

The VFS ref update and `workspace-committed` marker are appended only after the
`git-repo` transaction is accepted. The commit response includes a `git` object
with the canonical repo stream, Git ref, old OID, new commit OID, transaction
id, object count, and framed object bytes.

## Endpoints

Endpoints are profile-owned and live under the repo stream:

```text
POST /v1/stream/{repo}/_vfs/checkout
GET  /v1/stream/{repo}/_vfs/ref/{ref}
GET  /v1/stream/{repo}/_vfs/stat?commit=...&workspaceId=...&path=...
GET  /v1/stream/{repo}/_vfs/readdir?commit=...&workspaceId=...&path=...
GET  /v1/stream/{repo}/_vfs/blob/{blobId}
GET  /v1/stream/{repo}/_vfs/log?ref=...&limit=...
GET  /v1/stream/{repo}/_vfs/show/{commitId}
POST /v1/stream/{repo}/_vfs/workspace/{workspaceId}/ops
GET  /v1/stream/{repo}/_vfs/workspace/{workspaceId}/status
POST /v1/stream/{repo}/_vfs/workspace/{workspaceId}/commit
POST /v1/stream/{repo}/_vfs/workspace/{workspaceId}/discard
POST /v1/stream/{repo}/_vfs/batch/stat
POST /v1/stream/{repo}/_vfs/batch/read-metadata
POST /v1/stream/{repo}/_vfs/batch/read-blobs
```

The client library in `src/vfs/client.ts` wraps these endpoints. The
`PrismaStreamsVfsFs` adapter implements the just-bash filesystem interface, and
`createVfsGitCommands` provides the MVP command subset:

```text
git status
git diff
git add
git commit -m ...
git log
git show
git checkout <ref>
```

Run the demo with:

```bash
bun run demo:vfs-just-bash
```

The demo mounts the VFS at `/workspace` inside just-bash, disables network,
Python, and JavaScript execution, modifies files through shell commands, and
commits through the VFS-backed `git` command.

## Stress Benchmark

The VFS commit stress benchmark is intentionally outside the default test
suite. It uses the existing `MockR2Store` with a 50ms PUT delay, performs 1000
individual commits editing 100000 files, and waits until at least 1 GiB has
been written through object-store PUTs:

```bash
bun run bench:vfs-commit-stress
```

Use flags such as `--commits`, `--files`, `--target-object-store-bytes`,
`--mock-put-delay-ms`, `--segment-max-bytes`, and `--keep-root` for smaller
smoke runs or investigation.

On the current implementation, the benchmark separates accepted commit time
from remote object-store publication time. The 1000-commit / 100000-file / 1
GiB profile completed accepted commits in about 77 seconds, or roughly 13
commits/second for one repository. Waiting for the delayed mock object store to
reach the 1 GiB target took about 104 seconds total. The difference is expected:
accepted commits are SQLite-backed stream appends, while published commits wait
for background segment upload and manifest publication.

## MVP Constraints

- This is not Git wire protocol compatibility.
- Ref compare-and-swap is enforced by the profile endpoint in a single server
  process with an in-process per-repo lock.
- Commit acknowledgment means Streams accepted the local append. Full-mode
  object-store publication follows the durability model in
  [overview.md](./overview.md#current-durability-model).
- The profile does not add tenant authorization or object-store bucket
  partitioning. Multi-tenant deployments still need a gateway or isolated
  server instances as described in [auth.md](./auth.md).
