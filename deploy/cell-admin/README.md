# cell-admin: operating a shared cell

A shared cell serves many small projects from one server (shared-cells
PLAN, phase A). The launch shape is one 1 GiB server, one shard, no
routers, the fleet off, and design partners the operator admits by hand.
`cell-admin` owns the cell's authorization feeds: it admits, lists and
offboards projects, and mints design partners' tokens. Every change it
makes is a new feed bundle that the operator publishes to the cell.

```bash
bun deploy/cell-admin/cell-admin.ts <command> --state DIR [flags]
bun deploy/cell-admin/cell-admin.ts <command> --state s3://BUCKET/cells/CELL --s3-credentials FILE [flags]
```

Exit codes: 0 done, 1 refused or failed, 2 usage, 3 not yet (publish the
bundle or wait, then run the same command again).

## The cell

Deploy the server with `deploy/profiles/compute-1g.env`, then
`deploy/profiles/shared-cell.env` (later wins), and these per-cell values:
`CELL_ID`, `PROJECT_ID` (reserved, never placed), `ACCOUNT_ID` (the sink
for unowned meter events), `USAGE_STREAM_KEY`, `AUTH_TOKEN` (the
deployment bearer, an operator credential), `SLATE_S3_*`, `PATH_PREFIX`
and `FEEDS_S3_KEY` (the bundle). The profile's header lists what it sets
and what must stay absent. In particular, no `FLEET_PREFIX`, no
`FLEET_INTERNAL_TOKEN`, and no key for `prisma-streams-internal` in the
keys feed, so nothing can open the cell's internal surface.

The cell is kept awake (the owner's public-beta answer (4) of 2026-10-11,
"Keep awake"). The profile sets `KEEP_AWAKE=1`, which the wrapper
(`deploy/app-server/index.ts`), not the binary, reads: it holds Compute's
keep-awake guard for the process's life, so the cell does not sleep when
idle and its clients meet no wake after a quiet spell. Only a deploy or a
restart boots it cold. `init`, and `admit`, which re-reads the profiles,
refuse a profile that leaves `KEEP_AWAKE` unset or sets it to anything
but `1`, or that sets `KEEP_AWAKE_UNTIL_MS` (which ends the guard at that
instant); never set `KEEP_AWAKE_UNTIL_MS` per cell either. An awake cell
bills for every hour it is up.

The server installs its ceilings at boot from `PROJECT_SHARE_K`, the
`CELL_ENVELOPE_*` values and the bounds the profile states; `cell-admin`
derives the same ceilings and publishes every quota explicitly at or below
them. `PROJECT_MEMORY_PRESSURE_BYTES` and the envelope values await
certification on the one-shard shape.

## The signing key

One key per cell, RSA (2048 bits or more) or Ed25519, made offline and
kept in one file that only its owner can read:

```bash
openssl genpkey -algorithm ed25519 -out cell-sc1.pem && chmod 600 cell-sc1.pem
```

A cell can be initialised from the public half alone, so the private key
never meets the state's machine: `openssl pkey -in cell-sc1.pem -pubout
-out cell-sc1.pub`, then `init --public-key cell-sc1.pub`. This is how the
interim issuer's key enters a cell: the private half becomes its
environment, the public half the cell's keys feed.

`cell-admin` reads the key only from the `--key-file` path you give it.
It refuses a key file that group or others can read, and it never copies,
prints or logs the key. Only the public half goes into the keys feed,
pinned to the customer audience `prisma-streams-data`.

## Commands

| Command | What it does |
|---|---|
| `init --cell-id C --deployment-project P --account A (--key-file K \| --public-key FILE)` | Creates the state and the first bundle. It reads the profiles (by default the two above; `--profile` repeats, later wins), checks they describe a single-server shared cell, and fixes the cell's ceilings at bound ÷ k. Also: `--workspace-cap N` (default 1), `--max-projects N` (default 1,000), `--denylist FILE` (default `DIR/denylist.txt`; point several cells at one file) and `--issuer URL` |
| `admit --project P --workspace W` | Places a project and creates its customer credential. Each bounded quota defaults to the cell's ceiling; `--quota field=value` sets a lower one. Also `--scopes "..."` (default: every scope), `--prefix p` (repeatable) and `--profile F` (repeatable; default the two shipped profiles), the profile it checks against the one `init` recorded |
| `token --credential C --key-file K --out FILE [--ttl SECS]` | Mints a customer token, valid at most 24 h (default 1 h; the first minute is backdated for clock skew). The token goes to FILE with mode 0600 and is never printed. Hand the file over; never paste the token |
| `offboard --project P` | First run: revokes the project's customer credentials and adds an operator credential. Publish the bundle |
| `offboard --project P --cell-url URL --key-file K` | Second run: checks the cell serves the revocation, deletes every stream, re-walks the catalog until a walk at least `--settle-secs` (default 60) after that check lists nothing, then omits the project. Publish the bundle. Exit 3 means "publish, wait, run again"; the walk is bounded by `--max-walks` (default 10) |
| `list [--json]` | Shows the cell, its ceilings, and each project's phase, quotas and credentials. Holds no secrets |
| `bundle` | Rewrites the bundle at a newer `feed_version`, for example after a crash between the state and the bundle |
| `suspend --project P` / `resume --project P` | Publishes the project `suspended` (the cell answers its requests `403 project_not_active`; the issuer mints it no token) or `active` again, at a newer `project_policy_version`. Only an active (not offboarding) project |
| `quota --project P --quota field=value...` | Sets some of an active project's quotas, each at most its ceiling as in `admit`, keeping the others, at a newer `project_policy_version` |
| `keys add --public-key FILE [--retire-others]` | Publishes a new key's public half beside the cell's keys: the rotation drill's overlap generation. With `--retire-others`, alone: the compromise drill, after which every token under another kid answers 401 |
| `keys retire --kid K` | Omits a kid, which retires it at the cell for good (refused for the cell's last key). The state remembers retired kids, and `keys add` refuses one: the cell refuses a keys snapshot in which a retired kid returns |

`admit` refuses, and publishes nothing, when:
- a quota is above its ceiling, or is 0 (a binary without the cell ceiling reads 0 as "no limit");
- the project is `system` or the cell's `PROJECT_ID`;
- the workspace is the cell's `ACCOUNT_ID`;
- the project id was ever used on this cell or appears in the denylist (ids are single-use; a move is offboarding plus a new id);
- the workspace already has its cap of projects on the cell;
- the cell already holds `--max-projects`;
- the profile (the shipped files, or `--profile`) gives other ceilings than at `init`, or is not the profile `init` recorded: `init` records a digest of the layered profile (its `KEY=VALUE` lines sorted by key), and `admit` compares its own copy of the profile with it, never re-reading the files `init` read, so the issuer can run the same check with its bundled copy.

Every change to a field the cell reads (a project's status or quotas; a
credential's status, scopes, prefixes or expiry) bumps that entry's
version and the bundle's `feed_version`: the cell refuses a snapshot that
changes an entry at a version it already holds.

## Publishing

The state directory holds `cell-state.json` (the source of truth),
`feeds-bundle.json` (what to publish) and the denylist. Back the directory
up after every change. The state's journal keeps its newest 100 entries;
older ones are written, before the state that drops them, to one object
under `audit/YYYY/MM/DD/` (in the state directory, or in the control
bucket for an s3 state). Versions only move forward: a state restored from
an older copy is refused, because republishing from it could revive a
revoked credential at a version the cell already saw. Publishing means
uploading the bundle to `FEEDS_S3_KEY`, nothing more: the cell's wrapper
(`deploy/app-server/feeds.ts`) polls it every 15 s with `If-None-Match`
and rewrites the three feed files atomically when it changed, and the
server reads them at its next 30 s refresh, so an admission or a revocation
takes effect within about 45 s without a restart. A wrapper that cannot
fetch the bundle for 120 s of awake time deletes the feed files, and the
cell then refuses every request once its feeds' freshness window (300 s)
passes, until a poll succeeds again: keep the bundle's bucket reachable.
A published bundle that is not JSON or lacks one of its three feeds is a
failed poll too: it replaces nothing, and left in place it deletes the feed
files after the same 120 s, so the cell stops serving about 7 minutes after
a bad publish. Publish only what `cell-admin` wrote, and republish a good
bundle at once if a bad one went up.

## The state in buckets

`--state s3://<control bucket>/cells/<cell id>` keeps the cell where the
interim token issuer keeps it too, and both write it safely at once:

| Bucket | Object | What |
|---|---|---|
| control | `cells/<cell id>/cell-state.json` | the state |
| control | `denylist.txt` | ids never placed again, shared by the bucket's cells |
| feeds | `cells/<cell id>/feeds-bundle.json` | the bundle: the object the cell's wrapper polls (its `FEEDS_S3_KEY`), so a change needs no upload |

The bundle has its own bucket (the owner's question D3, answer (a)): the
cell reads it with a read-only key and holds no key to the control
bucket. The bucket keys come from `--s3-credentials FILE`, a file only
its owner can read (mode 0600), never from the command line:
`{"endpoint", "bucket", "region", "accessKeyId", "secretAccessKey",
"feeds": {the same five}}`. A writer key to either bucket can publish any
bundle: keep them like the signing key.

Every write is conditional: a state, bundle or denylist is created with
`If-None-Match: *` and replaced with `If-Match` on the ETag read. A
writer that loses re-reads and applies its change again, at most five
times (then exit 3: run again). The bundle only moves forward: a bundle
the bucket already holds at the same or a newer `feed_version` is kept.
Before anything, `cell-admin` checks that each bucket answers both
preconditions with 412 where it must, and refuses to run otherwise: a
store that ignored them would make every write last-writer-wins.

Every change also refuses a bundle that would change a published entry
without its version (a key's material under its kid, a project's policy
at its `project_policy_version`, a credential at its `grant_version`): the
cell drops such a snapshot and goes dark once its feeds age past 300 s.

## Offboarding

Streams are deleted one by one; their bytes stay in the bucket until
reclamation (E11). Their content is unreadable without keys the server
never stored.

The project stays in the policy feed until its catalog is empty, so the
operator credential can still walk it. Customer projects have no forks
(forks exist only on the raw surface), so the walk deletes in catalog
order.

What the catalog does not list, offboarding cannot see:
- a stream still being created when the walk runs (the settle time covers
  requests that passed authorization before the revocation);
- expired streams and tombstones, which hold no readable data.

## Watch

- `/v1/debug/auth` (deployment bearer): `feeds.policies.reservedDropped`
  must stay 0.
- `unowned_meter_events_total` must stay flat.
- A shard or instance shed (`ShardBytes`, `InstanceBytes`, `LagSecs`) on a
  shared cell needs a look at its largest writers. Per-project attribution
  is phase B.
