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
bundle or wait, then run the same command again). A change whose state is
committed but whose bundle failed to publish exits 1 with "the change is
committed at feed_version N, but its bundle is not published (...): run
cell-admin bundle, not this command again": running it again would make
the change twice.

## The cell

Deploy the server with `deploy/profiles/compute-1g.env`, then
`deploy/profiles/shared-cell.env` (later wins), and these per-cell values:
`CELL_ID`, `PROJECT_ID` (reserved, never placed), `ACCOUNT_ID` (the sink
for unowned meter events), `STREAMS_AUTH_ISSUER` (the `iss` every customer
token must carry, refused otherwise: `init --issuer`, default
`https://auth.prisma.io`, and the interim issuer's `STREAMS_ISSUER_ID`,
which it checks at boot), `USAGE_STREAM_KEY`, `AUTH_TOKEN` (the
deployment bearer, an operator credential), `SLATE_S3_*`, `PATH_PREFIX`,
`FEEDS_S3_KEY` (the bundle) and `FEEDS_S3_*` (the bundle's bucket: the
wrapper reads it by these names only, never `BIN_S3_*`; see
`deploy/README.md`). `init --cell-env FILE` writes the first four into the
stage's cells file (`cells/<stage>.env`, which the Composer entry
`deploy/composer/streams/entry.ts` reads), so they have one source: the
cell's state. The profile's header lists what it sets
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
certification on the one-shard shape. `init` and `admit` refuse a profile
whose `PROJECT_SHARE_K` projects at their memory lines would pass the one
shard's unabsorbed-bytes line (`MAX_UNABSORBED_BYTES_PER_SHARD`, stated in
the profile at the binary's 256 MiB), past which every append to the cell
is refused; the profile's comment states the real tolerance.

The stored bytes a project may own, `max_stored_bytes`, have no ceiling:
the profile's `PROJECT_STORED_BYTES_DEFAULT` (10 GiB, the owner's answer
S1 of 2026-10-12) is what `admit` and the issuer give a project that is
given none, written explicitly, and the server gives the same default to a
0 or absent value. `init`, and `admit`, which re-reads the profiles,
refuse a profile without a positive `PROJECT_STORED_BYTES_DEFAULT`. The
profile is fixed for a cell's life (its digest), so the default is too;
`quota` changes one project's value. A project that stores its quota is
refused appends `429 stored_bytes_limit` until it deletes or `quota`
raises it; the RUNBOOK's `PROJECT_STORED_BYTES_DEFAULT` row says how fresh
the figure is, and its "What to alert on" table watches the cell's stored
growth and spend.

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

### Rotating the issuer's key, and a compromised key

The rotation drill, for a cell whose interim issuer signs: before the
allow-list opens, then yearly or on any doubt. In this order (`S` is the
cell's `--state`, with its `--s3-credentials`; `deploy/issuer/drills.test.ts`
runs these steps against a fake cell):

1. Make the new key offline, and its public half:
   `openssl genpkey -algorithm ed25519 -out new.pem && chmod 600 new.pem`,
   `openssl pkey -in new.pem -pubout -out new.pub`.
2. `keys add --state S --public-key new.pub`: the overlap generation,
   the old and the new kid. Publish it. The running issuer keeps minting
   under the old key, and every token stays valid.
3. `canary --state S --key-file new.pem --cell-url URL`: a token under the
   new key answers 200, so the cell serves the new kid (exit 3 until it
   does; run it again).
4. Set the issuer's `STREAMS_ISSUER_SIGNING_KEY` from `new.pem` through
   Composer's `envSecret` path (the value is read from the file, never
   pasted on a command line, into a chat or a ticket) and redeploy it (a
   changed value needs a redeploy). It refuses to boot with a key the
   cell's keys feed does not hold, so step 2 comes first. Shred `new.pem`.
5. After the longest token lifetime plus 10 minutes (70 minutes at the
   issuer's default `STREAMS_ISSUER_TOKEN_TTL_SECS`, 3,600 s):
   `keys retire --state S --kid <the old kid>`. Publish it. The old kid is
   retired at the cell for good; a token under it answers 401.

A compromised key, in this order:

0. Scope it first. Under the owner's answer I2 (2026-10-12) the key lives
   in the issuer's encrypted project environment, beside the control
   bucket's writer key pair, `STREAMS_ISSUER_SESSION_KEY` and
   `STREAMS_ISSUER_METRICS_TOKEN`, so the likeliest leak is that whole
   environment. Whoever holds the writer key can publish a kid of their
   own again (the cell takes any new kid; the writer key is as powerful as
   the signing key), and the session key forges sessions that create and
   delete projects. Unless you know only the signing key leaked: first
   rotate the control bucket's writer keys (`/v1/buckets/{id}/keys`: a new
   pair to the issuer's environment and to your `--s3-credentials` file,
   the old pair revoked), then do the steps below, and in the same
   redeploy rotate `STREAMS_ISSUER_SESSION_KEY` (every browser signs in
   again) and `STREAMS_ISSUER_METRICS_TOKEN` (and the watch's copy).
   Afterwards check the keys feed and the state for kids, credentials or
   projects you did not make (`list`, `list --json`).
1. Make the new key as above.
2. `keys add --state S --public-key new.pub --retire-others`: one
   generation with the new kid alone. Publish it. Within one revalidation
   (5 s) the running issuer stops minting (503 `issuer_unavailable`,
   `/healthz` 503 `key_not_in_state`), and once the cell holds the
   generation (about 45 s) every outstanding token answers 401.
3. Set `STREAMS_ISSUER_SIGNING_KEY` from the new key's file as in the
   rotation's step 4 and redeploy the issuer; shred `new.pem`. SDK clients
   refresh after the 401 and get tokens under the new key. The outage is
   the longer of the propagation and the redeploy.

## Commands

| Command | What it does |
|---|---|
| `init --cell-id C --deployment-project P --account A (--key-file K \| --public-key FILE)` | Creates the state and the first bundle, refused while either exists (a bundle without a state is a lost state: recover it, or use a new cell id). It reads the profiles (by default the two above; `--profile` repeats, later wins), checks they describe a single-server shared cell, and fixes the cell's ceilings at bound ÷ k. Also: `--workspace-cap N` (default 1), `--max-projects N` (default 1,000), `--denylist FILE` (default `DIR/denylist.txt`; point several cells at one file), `--issuer URL` and `--cell-env FILE`: sets `CELL_ID`, `PROJECT_ID`, `ACCOUNT_ID` and `STREAMS_AUTH_ISSUER` in that cells file by key, once the state is saved, keeping every other line (the provisioning script writes the rest); a missing file is made, mode 0644 (it holds no secret). It refuses, before the cell exists, a file with a line that is not space-free `KEY=VALUE` or a key set twice, and an issuer that is not space-free |
| `admit --project P --workspace W` | Places a project and creates its customer credential. Each bounded quota defaults to the cell's ceiling; `--quota field=value` sets a lower one. `max_stored_bytes` defaults to the profile's `PROJECT_STORED_BYTES_DEFAULT`, and `--quota max_stored_bytes=N` sets any positive N, larger included. Also `--scopes "..."` (default: every scope), `--prefix p` (repeatable; a prefix the cell's grants parser takes: at most 256 UTF-8 bytes, no control character or space, no empty, `.` or `..` component, not under `__ds`, so `orders`, never `orders/`) and `--profile F` (repeatable; default the two shipped profiles), the profile it checks against the one `init` recorded |
| `check-sizing --profile F [--profile F]...` | Checks a release's profiles (the files it ships, given in full: `--profile` is required and repeats, later wins) against the cell, exactly as `admit` checks its own copy: the same k and ceilings as at `init`, and the layered profile `init` recorded. Exit 0, or 1 with `admit`'s refusal ("the profiles changed since init ..." or "the layered profile differs ..."). A cell's ceilings are fixed for its life, and the server installs its own from the profiles it boots with, so a release that moves one would make every later `admit` fail: run it before a release deploys to the cell. It changes neither the state nor the bundle (on an `s3://` state, the buckets' self-test that every command runs writes and deletes one probe object) |
| `token --credential C --key-file K --out FILE [--ttl SECS]` | Mints a customer token, valid at most 24 h (default 1 h; the first minute is backdated for clock skew). The token goes to FILE with mode 0600 and is never printed. Hand the file over; never paste the token |
| `offboard --project P` | First run: revokes the project's customer credentials and adds an operator credential; for a suspended project, also moves the canary's catalog-read credential to a newer `grant_version`, its witness (below). Publish the bundle. On a cell with the interim issuer, its offboarding job makes the second run on its own (`deploy/issuer/offboarding.ts`: every 15 s, failed attempts retried after 1, 2, 4, ... minutes, at most an hour) |
| `offboard --project P --cell-url URL --key-file K` | Second run: for a suspended project, waits until the canary answers 200 at its witness version and sets the project active for its drain (publish that bundle too; on an `s3://` state the cell's wrapper polls it); then checks the cell serves the revocation, deletes every stream (listing pages of twice the project's `requests_per_sec`, each deleted before the next is listed, and waiting out a `Retry-After` of up to 120 s: the operator's token draws on the project's own buckets), re-walks the catalog until a walk at least `--settle-secs` (default 60) after that check lists nothing, then omits the project. Publish the bundle. Exit 3 means "publish, wait, run again"; the walk is bounded by `--max-walks` (default 10), and a suspended project's two waits by `--wait-secs` each (default 120; 0 asks once) |
| `list [--json]` | Shows the cell, its ceilings, and each project's phase, quotas and credentials. Holds no secrets |
| `bundle` | Rewrites the bundle at a newer `feed_version`, for example after a crash between the state and the bundle |
| `suspend --project P` / `resume --project P` | Publishes the project `suspended` (the cell answers its requests `403 project_not_active`; the issuer mints it no token) or `active` again, at a newer `project_policy_version`. Only an active (not offboarding) project |
| `quota --project P --quota field=value...` | Sets some of an active project's quotas, each at most its ceiling as in `admit` (`max_stored_bytes` has none), keeping the others, at a newer `project_policy_version` |
| `credential --project P --pepper-file F --out FILE` | Adds a customer credential with a secret (at most 10 live per project): the secret, `prisma_streams_<credential id>.<32 random bytes, base64url>`, goes once to a new file of mode 0600 (written beside it before the change commits, so a directory it cannot write refuses first, and moved into place once the commit lands, even when the bundle then fails to publish); the state keeps only HMAC-SHA-256 of it under the newest pepper (`v<N>:<base64>[,v<N-1>:<base64>]`, read from a mode-0600 file), the pepper's version and the secret's last 6 characters, exactly as the issuer stores the credentials it makes. Also `--scopes`, `--prefix` and `--name`; `--hidden` only for the canary's credentials (the watch's) |
| `allow --workspace W [--max-projects N] [--requests-per-sec R] [--note TEXT]` | Puts a Prisma workspace on the beta's allow-list (`allowlist.json` beside a directory state; `beta/allowlist.json` in the control bucket, shared by its cells), or replaces its limits: `--max-projects` over the cell's workspace cap (`init --workspace-cap`), `--requests-per-sec` (at most the ceiling) over the free-tier rate. The issuer creates projects only for listed workspaces |
| `disallow --workspace W` | Takes a workspace off the allow-list: it creates no new project, and its projects keep running until you suspend them (the owner's question I8, answer (a)) |
| `gate --open \| --close \| --n-max N` | Opens or closes self-serve admission, or sets how many customer projects the issuer places (default open, 50: the static u = 0.5 rule). It publishes nothing |
| `canary --key-file K --cell-url URL [--wait-secs S]` | Mints a 5-minute token for the canary's catalog-read credential under K (a key of the cell) and asks the cell `GET /v1/streams?limit=1` until it answers 200, for at most S seconds (default 120): the proof that the cell serves that key, the rotation drill's step 2. Each probe re-reads the state and mints at the credential's current `grant_version`, which offboarding a suspended project moves. Exit 3 when it does not yet |
| `export --out FILE` | Writes the Stage 1 import set: every active customer project (suspended included; offboarding ones and the canary not) with its versions and quotas, and its customer credentials with their grant versions and, where they have a secret, its hash and pepper version. Mode 0600: keep it like the state |
| `keys add --public-key FILE [--retire-others]` | Publishes a new key's public half beside the cell's keys: the rotation drill's overlap generation. With `--retire-others`, alone: the compromise drill, after which every token under another kid answers 401 |
| `keys retire --kid K` | Omits a kid, which retires it at the cell for good (refused for the cell's last key). The state remembers retired kids, and `keys add` refuses one: the cell refuses a keys snapshot in which a retired kid returns |

`init` also places the issuer's canary: the reserved project
`sp_canary_<cell id>` in the workspace `streams-issuer-canary`, at 5
requests/s and 2 streams, one for cell-check's own `cell-check` stream and
one for the alerting watch's canary stream (every other quota at the
ceiling, and the stored bytes at the profile's default), with one
hidden credential holding only `streams.catalog.read` and no secret. The
issuer and `cell-admin canary` mint tokens for it to prove the cell
serves a key or a generation; the watch's own credential is added with
`credential --hidden`. It is hidden from `list` and counts toward no cap,
no customer project takes its id prefix or its workspace, and `offboard`,
`suspend`, `resume` and `quota` refuse it (only `init` places it, once).

`admit` refuses, and publishes nothing, when:
- a quota is above its ceiling, or is 0 (a binary without the cell ceiling reads 0 as "no limit"; for `max_stored_bytes`, omit the field to take the cell default);
- the project is `system`, the cell's `PROJECT_ID` or a canary id (`sp_canary_...`);
- the workspace is the cell's `ACCOUNT_ID` or the canary's;
- the project id was ever used on this cell or appears in the denylist (ids are single-use; a move is offboarding plus a new id);
- the workspace already has its cap of active projects on the cell (a project being offboarded counts toward `--max-projects` until its drain ends, but not toward its workspace's cap: the owner's answer I9 (a), the issuer's rule too);
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

### Offboarding a suspended project

`offboard` takes a suspended (abusive) project without resuming it first
(the owner's call 15 of 2026-10-13, the canary witness). The cell checks
a project's status before any credential (`src/auth.rs`,
`verify_customer`), so while the project is suspended none of its own
tokens, the operator's included, can show whether the revocation has
reached the cell; the canary, another project, can.

1. The first run revokes every customer credential and adds the operator
   credential, as for any project, and the project stays suspended. In
   the same change it moves the canary's catalog-read credential to a
   newer `grant_version` (its published entry otherwise unchanged) and
   records that version in the project's record as
   `witness_grant_version` (never published). Publish the bundle.
2. The second run, or the issuer's offboarding job in one attempt, asks
   `GET /v1/streams?limit=1` every 2 s for up to `--wait-secs` (default
   120; the job's 120 s) with a canary token minted for each probe at the
   credential's `grant_version` as the state holds it then (the run
   reloads it, the job revalidates its copy), at least the witness
   version: another suspended project's offboarding may move it on during
   the wait, and the cell may take that bundle without ever holding this
   one's. `401
   grant_version_mismatch` and `403` are "not yet", any other answer
   refuses (exit 1), and the wait running out is exit 3 (for the job, a
   failed attempt, retried after its backoff). A 200 proves the cell holds a grants snapshot at least as
   new as the revocation: it compares grant versions exactly and never
   takes an older snapshot, so whatever policy feed it reads beside it
   (the three feeds are read one by one), the project's customer
   credentials are revoked there.
3. Only then does the run set the project active, at a newer
   `project_policy_version`, in a bundle of its own (refused unless every
   customer credential of the project is revoked or omitted; the job
   audits it, `project.resume`). The cell's
   answer to a token minted before the revocation turns from `403
   project_not_active` to `403 credential_not_active`. Publish the
   bundle; the run's proof waits for it the same way, then it drains and
   omits the project as for any other.

The second run refuses (exit 1, before asking the cell anything) a
suspended project being offboarded without a witness version, and the
job fails its attempts the same way: a state edited by hand, whose
revocation no canary answer can prove.

Conditions:
- The witness proves the snapshot of the instance that answered: exact on
  the single-server, fleet-off cell this beta runs (the proof has the
  same property); a fleet needs a witness per instance.
- Each such offboarding moves the canary's catalog-read credential, so
  its tokens minted at an older version answer `401
  grant_version_mismatch` once the cell takes the bundle. Whatever probes
  the canary mints a token per probe at the credential's current
  `grant_version` and never keeps one across minutes: `cell-admin canary`
  re-reads the state before each probe, and the alerting watch must do
  the same. The watch's own canary credential (append and read) is not
  moved.
- From the resume to each stream's delete (about one feed delay, then
  the drain), anyone holding a stream's key can long-poll its change
  cursors (no record data, no writes), within the project's quotas: watch
  capabilities are checked against the project's status only
  (`src/application/watch.rs`). A residual the owner accepted; closing it
  needs a feed-contract change.

The issuer's delete still refuses a suspended project `409
project_not_active`: the owner looks before an abusive project's data
goes, and offboards it with `cell-admin offboard`.

## Watch

- `/v1/debug/auth` (deployment bearer): `feeds.policies.reservedDropped`
  must stay 0.
- `unowned_meter_events_total` must stay flat.
- A shard or instance shed (`ShardBytes`, `InstanceBytes`, `LagSecs`) on a
  shared cell needs a look at its largest writers. Per-project attribution
  is phase B.
