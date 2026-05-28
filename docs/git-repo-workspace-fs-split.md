# Git Repo / Workspace FS Split

Status: initial implementation

The VFS MVP proved the agent workflow: lazy checkout, durable workspace ops,
explicit commit, just-bash filesystem adapter, and Git-like shell commands. It
is not the long-term canonical repository backend.

The supported direction is a split:

```text
git-repo
  canonical repository records
  Git object identity
  ref transactions
  ref checkpoints
  bundle/pack import-export
  pack/idx artifacts
  smart HTTP fetch and push

workspace-fs
  lazy writable checkout for agents
  durable uncommitted operation log
  path-oriented filesystem API
  just-bash adapter and agent git-like commands
  explicit commit back to git-repo
```

Agent runtimes should continue to use the workspace API and just-bash adapter.
They should not need a real `.git` directory. Human and external tooling can use
Git import/export and smart HTTP through the canonical `git-repo` profile.

## Request Path

```text
agent
  -> just-bash
    -> workspace-fs adapter
      -> workspace operation stream
      -> git-repo ref transaction
        -> Git objects / packs / checkpoints
```

The current `vfs-repo` profile remains as an MVP compatibility profile while
the split lands. New canonical repository work should target `git-repo`, and
new agent workspace work should target `workspace-fs`.

## Commit Semantics

Workspace commits now follow this shape for `workspace-fs` profiles configured
with `gitRepo.stream`:

```text
1. Read base Git commit/ref from git-repo.
2. Read workspace ops.
3. Build Git blob, tree, and commit objects.
4. Submit one git-repo ref transaction with expected old refs.
5. Mark the workspace committed after git-repo accepts the transaction.
```

`git-repo` owns canonical refs. `workspace-fs` owns draft local operations,
workspace status, path-local base reads from Git trees, and the just-bash
adapter surface.

## Durability States

The split preserves the existing Streams durability distinction:

- `accepted`: locally durable in SQLite and visible to the serving process
- `published`: recoverable from object storage after segment and manifest
  publication
- `verified`: refs point only to reachable, available Git objects

Low-latency agent commits can request accepted durability. Strict callers should
wait for published transaction status and run reachability verification before
treating a ref as remotely durable.
