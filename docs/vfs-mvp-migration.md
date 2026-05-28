# VFS MVP Migration

Status: initial command implemented

The original `vfs-repo` profile is an MVP that combines canonical repository
storage and workspace filesystem behavior. The target architecture splits it
into:

- `git-repo`: canonical Git-compatible repository records, refs, objects, and
  maintenance artifacts
- `workspace-fs`: derived lazy writable workspaces for agents and just-bash

## Migration Shape

For each existing `vfs-repo` commit:

```text
1. Read the VFS commit object.
2. Walk its tree pages.
3. Read referenced VFS blobs.
4. Write Git blob objects.
5. Write Git tree objects.
6. Write a Git commit object with preserved author/message/time where possible.
7. Append a git-repo ref transaction that imports the resulting commit.
```

Preserve:

- commit message
- author id/name where available
- createdAt timestamp
- parent relationships where the parent VFS commits are migrated
- file modes
- symlinks

## Command

Run:

```bash
bun run vfs:migrate-to-git-repo \
  --streams-url http://127.0.0.1:8080 \
  --source-stream vfs/tenant/repo/control \
  --target-stream git/tenant/repo
```

Optional flags:

- `--ref main` chooses the source ref to migrate and the target ref to update.
- `--limit 100` caps how many source commits are read from the compatibility
  log.
- `--auth-token ...` sends a bearer token to both source and target streams.

The command is one-way and explicit. It creates or updates the target
`git-repo` stream/profile, writes Git loose-object artifacts through `_git`
object endpoints, and commits the target ref through a `git-repo` ref
transaction. It does not mutate the source `vfs-repo` stream.

The initial command migrates commits reachable through the compatibility
`/_vfs/log` endpoint, which is currently bounded by that endpoint's log limit.
Large production migrations should page or stream the source commit history
instead of relying on the compatibility log cap.

## Compatibility

Existing `vfs-repo` streams remain readable by the compatibility client. New
canonical repository work should target `git-repo`, and new agent workspace
work should target the workspace-facing API names.
