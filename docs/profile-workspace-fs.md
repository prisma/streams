# `workspace-fs` Profile

Status: supported agent workspace implementation

`workspace-fs` is the agent workspace layer. It is responsible for lazy
writable checkouts, durable draft operations, just-bash integration, and
explicit commits back to a canonical `git-repo` repository.

Install it on a JSON stream and configure the canonical Git repository stream:

```json
{
  "apiVersion": "durable.streams/profile/v1",
  "profile": {
    "kind": "workspace-fs",
    "version": 1,
    "gitRepo": {
      "stream": "git/tenant/repo"
    },
    "audit": {
      "stream": "evlog/tenant/repo/agent-audit"
    }
  }
}
```

The profile route, client, model, type surface, just-bash adapter, and
agent-facing Git commands live under `src/workspace_fs`:

```ts
import {
  openWorkspaceFsRepo,
  PrismaStreamsWorkspaceFs,
  createWorkspaceGitCommands,
} from "../src/workspace_fs";
```

`openWorkspaceFsRepo(...).ensure({ gitRepoStream })` installs a
`workspace-fs` profile. It does not install `vfs-repo`.

The legacy `vfs-repo` route now delegates to the workspace-fs server module for
workspace behavior. New code should import from `src/workspace_fs`; do not add
new workspace behavior under `src/vfs`.

The profile supports these workspace behaviors:

- checkout creates a durable workspace stream
- file operations append workspace draft records
- reads resolve overlay changes before base content
- commit checks an expected head
- just-bash can mount the workspace filesystem at `/workspace`
- custom `git status`, `git diff`, `git commit`, `git log`, `git show`, and
  `git checkout` commands run inside just-bash without a real `.git` directory
- when configured with `gitRepo.stream`, checkout reads the canonical Git ref,
  base `stat`/`readdir`/file reads resolve from Git trees and loose object
  artifacts, and commits create a canonical Git commit through a `git-repo` ref
  transaction
- when configured with `audit.stream`, workspace lifecycle, draft operation,
  commit, rebase, and discard events are appended to that stream; the target
  must be an existing `evlog` stream

## Responsibilities

`workspace-fs` owns:

- workspace stream lifecycle and TTL
- workspace operation log
- compacted overlay indexes for large workspaces
- path-local `stat`, `readdir`, and file resolution
- just-bash adapter
- agent-facing Git-like commands

`workspace-fs` does not own canonical refs, Git object identity, packfiles, or
Git import/export. Those belong to [profile-git-repo.md](./profile-git-repo.md).

## Commit Flow

```text
workspace checkout
  -> durable workspace ops
  -> build Git blobs/trees/commit
  -> git-repo ref transaction
  -> workspace committed marker
```

The canonical branch head is read from `git-repo` during checkout. Base
metadata and file reads are path-local over Git commit/tree/blob
objects, while workspace draft operations are overlaid from the workspace stream.
The hot path does not recursively expand the full repository tree.

Commit reads the current Git head, compares it with the workspace expected head,
builds only the affected Git tree path objects plus changed blobs and the new
commit object, then submits a `git-repo` ref transaction. A workspace committed
marker is appended only after the canonical transaction succeeds.

Commit requests accept an optional durability target:

```ts
await workspace.commit({
  message: "Update parser",
  author: { id: "agent" },
  durability: "verified",
  durabilityTimeoutMs: 30_000,
});
```

`accepted` returns after the canonical `git-repo` transaction is locally
durable. `published` waits until the git-repo stream record reaches the remote
visibility point. `verified` waits until the published transaction's object
artifacts still exist and the new Git ref target walks through a hash-checked
reachable object graph. The default is `accepted`.

## Conflict And Rebase Endpoints

Workspace streams expose path-level branch-move handling for agents:

```text
GET  /v1/stream/{workspace}/_vfs/workspace/{id}/conflicts
POST /v1/stream/{workspace}/_vfs/workspace/{id}/rebase
```

`conflicts` compares the workspace base commit with the current canonical Git
head for the checked-out ref, then reports:

- workspace changed paths from the durable operation log
- upstream changed paths from the Git commit range `base..head`
- conflict paths where a workspace path and upstream path are identical or have
  an ancestor/descendant relationship

`rebase` appends a `workspace-rebased` marker when there are no path conflicts.
The workspace operation log is not rewritten; future reads and commits use the
new base commit and replay the existing draft operations over that base.

This is intentionally a path-level primitive. It does not perform text merges
or conflict resolution yet. If conflicts exist, `rebase` returns `409` with the
same conflict details so agents can inspect the paths before retrying.

## Audit Events

`workspace-fs` can write an agent audit trail to an `evlog` stream:

```ts
await openWorkspaceFsRepo(...).ensure({
  gitRepoStream: "git/tenant/repo",
  auditStream: "evlog/tenant/repo/agent-audit",
});
```

The audit stream must already exist, use `application/json`, and have the
`evlog` profile installed. When configured, audit appends are part of the
workspace operation path; if the audit stream is missing or not `evlog`, the
workspace request fails instead of silently losing the event.

Current event names:

- `workspace_checked_out`
- `workspace_file_read`
- `workspace_ops_appended`
- `workspace_commit_started`
- `workspace_commit_succeeded`
- `workspace_commit_failed`
- `workspace_rebased`
- `workspace_rebase_failed`
- `workspace_discarded`

Events are normalized by `evlog` and include the workspace stream, workspace id,
ref, actor id when available, status, and operation-specific details in
`context`.

## Overlay Index Endpoints

Workspace streams expose a compact overlay view for agents and indexers:

```text
GET  /v1/stream/{workspace}/_vfs/workspace/{id}/index?path=/src
GET  /v1/stream/{workspace}/_vfs/workspace/{id}/changes?prefix=/src
POST /v1/stream/{workspace}/_vfs/workspace/{id}/compact
```

`index` returns the current latest operation by path, child names by directory,
deleted paths, and a generation equal to the indexed op count. `changes` returns
changed paths under a prefix. `compact` appends a `workspace-overlay-index`
record to the workspace stream and returns the same snapshot, so large workspace
readers can use a profile-owned compaction point instead of deriving structure
from raw ops themselves.
