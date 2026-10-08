# cell-admin: operating a shared cell

A shared cell serves many small projects from one server (shared-cells
PLAN, phase A). The launch shape is one 1 GiB server, one shard, no
routers, the fleet off, and design partners the operator admits by hand.
`cell-admin` owns the cell's authorization feeds: it admits, lists and
offboards projects, and mints design partners' tokens. Every change it
makes is a new feed bundle that the operator publishes to the cell.

```bash
bun deploy/cell-admin/cell-admin.ts <command> --state DIR [flags]
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

`cell-admin` reads the key only from the `--key-file` path you give it.
It refuses a key file that group or others can read, and it never copies,
prints or logs the key. Only the public half goes into the keys feed,
pinned to the customer audience `prisma-streams-data`.

## Commands

| Command | What it does |
|---|---|
| `init --cell-id C --deployment-project P --account A --key-file K` | Creates the state and the first bundle. It reads the profiles (by default the two above; `--profile` repeats, later wins), checks they describe a single-server shared cell, and fixes the cell's ceilings at bound ÷ k. Also: `--workspace-cap N` (default 1), `--max-projects N` (default 1,000), `--denylist FILE` (default `DIR/denylist.txt`; point several cells at one file) and `--issuer URL` |
| `admit --project P --workspace W` | Places a project and creates its customer credential. Each bounded quota defaults to the cell's ceiling; `--quota field=value` sets a lower one. Also `--scopes "..."` (default: every scope) and `--prefix p` (repeatable) |
| `token --credential C --key-file K --out FILE [--ttl SECS]` | Mints a customer token, valid at most 24 h (default 1 h; the first minute is backdated for clock skew). The token goes to FILE with mode 0600 and is never printed. Hand the file over; never paste the token |
| `offboard --project P` | First run: revokes the project's customer credentials and adds an operator credential. Publish the bundle |
| `offboard --project P --cell-url URL --key-file K` | Second run: checks the cell serves the revocation, deletes every stream, re-walks the catalog until a walk at least `--settle-secs` (default 60) after that check lists nothing, then omits the project. Publish the bundle. Exit 3 means "publish, wait, run again"; the walk is bounded by `--max-walks` (default 10) |
| `list [--json]` | Shows the cell, its ceilings, and each project's phase, quotas and credentials. Holds no secrets |
| `bundle` | Rewrites the bundle at a newer `feed_version`, for example after a crash between the state and the bundle |

`admit` refuses, and publishes nothing, when:
- a quota is above its ceiling, or is 0 (a binary without the cell ceiling reads 0 as "no limit");
- the project is `system` or the cell's `PROJECT_ID`;
- the workspace is the cell's `ACCOUNT_ID`;
- the project id was ever used on this cell or appears in the denylist (ids are single-use; a move is offboarding plus a new id);
- the workspace already has its cap of projects on the cell;
- the cell already holds `--max-projects`;
- the profiles' ceilings changed since `init`.

## Publishing

The state directory holds `cell-state.json` (the source of truth),
`feeds-bundle.json` (what to publish) and the denylist. Back the directory
up after every change. Versions only move forward: a state restored from
an older copy is refused, because republishing from it could revive a
revoked credential at a version the cell already saw. Today a bundle
reaches a cell only when it boots (`deploy/app-server` downloads
`FEEDS_S3_KEY` once), so publishing means uploading the bundle and
restarting the cell. Live delivery is PLAN step 4.

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
