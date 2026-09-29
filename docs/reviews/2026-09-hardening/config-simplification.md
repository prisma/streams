# Configuration: what could be simpler (audit of 2026-09-29)

The owner asked, after approving the L0 cap default (823b3269): are there
other defaults or tunables to change, to simplify as much as possible. This
page is the answer, for the owner to decide. Nothing on it was changed when
it was written; the status lines under each package say what has been
changed since.

Method: five readers inventoried every setting the binary reads, every value
anything deploys and the documented history; one classified; six verifiers
tried to break each recommendation against the code, the tests and the
documents. All of it by reading at 823b3269: nothing was built or run, except
the three local measurements under "Measured". 39 of 51 recommendations held, 12
did not and are listed as kept. The verified detail of every item, with its
file and line references, is in
`evidence/config-audit-2026-09-29/detail.md` (170 KB; read one item with
`rg -n '^#### SHARED_CACHE_BYTES' -A 60 <file>`).

## The numbers

| | Count |
|---|---|
| Settings the binary reads (85 arguments, 69 environment-only, 2 read both ways) | 152 |
| Lines in the production profile (`deploy/profiles/compute-1g.env`) | 24 |
| of which already equal the binary default | 7 |
| of which differ from the binary default | 14 |
| Settings nothing ever sets (no profile, script, rig or family) | 47 |
| Settings that are accepted and do nothing | 7 |

If packages 1 to 4 are taken, the profile shrinks to its certification line,
and the binary reads about 113 settings instead of 152.

## What applies to every change

- A default lives in three places that must move together: the clap
  attribute (or `model.rs`), the fixture `CliArgs::deterministic`, and the
  pins in `src/config/tests.rs` (`cli_surface_is_pinned` calls a default
  change "a product decision").
- `src/config/` feeds no formal receipt. Removing a code path does: files
  under `src/shard`, `src/history` and `src/bootstrap.rs` are receipt inputs.
- Almost every setting is wired inside `bootstrap::run`, whose exception rows
  are frozen. A setting can become a constant without touching `run` if its
  field stays and only stops being an argument (`#[arg(skip)]`); removing
  the field needs the owner to update those rows.
- On Compute the environment is project-scoped and merged (RUNBOOK §7.3):
  a deploy script restates its complete environment, and a line that
  restates a default is the reset of a stale value. Script lines therefore
  stay; only the profile's own lines can go, and
  `bench/soak/oom-acceptance.sh` requires 12 of them to be present in the
  file until it compares against the resolved configuration instead.
- The profile's sha256 is pinned by release-candidate evidence
  (`scripts/verify-rc-evidence.py`), so an edit to it falls under the release
  hold.
- Client-visible defaults need an edge-change record and ratification.

## Package 1: settings that do nothing (no behaviour changes anywhere)

Status (2026-09-29): implemented on the owner's delegation ("For other
questions, please make your own judgement call"); each edge record awaits
ratification. First the preparation (c480e615): the effective-configuration
tool counts the 156 leaves HEAD prints (its pin said 155; 0d40dc2a had
added `FORK_DEBT_SWEEP_SECS`), and a unit test holds that count to the
source.

Status (2026-09-29, rows 1, 2 and 4): done in one commit, edge record #74.
The three absorber options and their startup notice, the three scaler
names and the two GC aliases are not declared; the tool's pin of HEAD's
leaves is 150. The scaler's documents now state the merge rule the code
runs; the 15% / 180 evaluations / 64 segments they listed was never
implemented. Rows 3 (`--compactor-max-concurrent`) and 5 (`PATH_PREFIX`)
are not changed yet.

Status (2026-09-29, row 5): done, edge record #75. The read spool opens
under the prefix clap resolved; `BillingConfig::path_prefix_env` and its key
in the startup summary are gone; the tool's pin of HEAD's leaves is 149.
Only a deployment that gives the prefix on argv and meters usage gets a new
spool location, and none is known; nothing is migrated (RUNBOOK §11 has the
upgrade step). Row 3 (`--compactor-max-concurrent`) is not changed yet.

Status (2026-09-29, row 3): done, edge record #76. The argument is wired,
not deleted: `with_knob_defaults` copies the value clap resolved (argv, then
`COMPACTOR_MAX_CONCURRENT`, then 1) and the overlay no longer reads the
name, as edge record #48 did for the poll interval. A process that holds
only the environment name runs as before. New with it: a certified process
(`MEMPROFILE_CERT=compute-1g`) that passes a concurrency other than 1 on
argv does not start. Deleting the argument would have refused every process
that passes it and would have let a malformed environment value through
unnoticed. With this row every row of package 1 is done. The counts under
"The numbers" are the audit's and are not rewritten.

| Setting | Today | Proposed |
|---|---|---|
| `ABSORB_PASS_BYTES`, `ABSORB_CONCURRENCY`, `ABSORB_SMALL_BYTES` | accepted, ignored, startup notice | not declared |
| `SCALE_COLD_PCT`, `SCALE_COLD_EVALS`, `MAX_SEGMENTS_PER_STREAM` | parsed and logged, no reader | not declared |
| `--compactor-max-concurrent` (the argument; the environment name works) | parsed, never read | delete, or wire it as edge record #48 did for the poll interval |
| `HISTORY_GC_MAX_INTERVAL_SECS`, `--gc-max-interval-secs` | legacy aliases | not accepted |
| `PATH_PREFIX` read twice (the read spool reads the raw environment, everything else the argument) | two readers | one reader |

Ten names. An argument that is removed makes a process that still passes it
refuse to boot; an environment name that is removed is ignored silently.
Recommended.

## Package 2: the binary's defaults become the certified 1 GiB posture

Today a server started without the profile is not the server production
runs, and `MEMPROFILE_CERT=compute-1g` refuses it. The question for the
owner is one: **should a bare binary be the certified production posture?**
If yes, these defaults move to the profile's values and the profile reduces
to its certification line. Larger instance classes would then set the larger
values.

| Setting | Default today | Profile | Note |
|---|---|---|---|
| `COMPACTOR_MAX_CONCURRENT` | 4 | 1 | see "The L0 cap and compaction" below |
| `COMPACT_MAX_SUBCOMPACTIONS` / `COMPACT_MAX_FETCH_TASKS` / `COMPACT_BYTES_TO_FETCH` | 4 / 4 / 2 MiB | 1 / 1 / 1 MiB | upstream SlateDB values today |
| `COMPACT_MAX_SST_SIZE_BYTES` | 256 MiB | 32 MiB | one name feeds two fields |
| `STORE_BULK_INFLIGHT_MAX_BYTES` | 0 (gate off) | 32 MiB | latency of cold reads on a server that sets nothing |
| `ABSORB_GATHER_MAX_BYTES` | 32 MiB | 8 MiB | |
| `ABSORB_GLOBAL_BUDGET_BYTES` | 64 MiB, floored at run time to 100,859,904 | 100,859,904 | derive it: (body ceiling + 64 KiB) x 3 when unset |
| `ABSORB_GLOBAL_GATHERS` | 2 | 1 | a behaviour change without the profile: two gathers do run concurrently today |
| `SHARED_CACHE_BYTES` with `ADMIT_RSS_SHED_MB` | 192 MiB, 600 | 128 MiB, 500 | only together: 500 with the 192 MiB cache leaves no margin |
| `SLATEDB_RT_THREADS` | 2 | 4 | reason for 4 not documented |
| `SSE_FEED_TOTAL_BYTES` with `SSE_FEED_PROJECT_BYTES` | 16 MiB, total / 4 | 64 MiB, 32 MiB | only as a pair; client-visible |
| `SSE_MAX_CONNECTIONS` | 10,000 | 1,200 | client-visible: the 1,201st subscription is refused |
| `MAX_RECORD_PAYLOAD_BYTES` | unlimited | 131,072 | client-visible: 413 above it |

Seven profile lines already equal the default (the three cache budgets,
`MAX_UNFLUSHED_BYTES`, `L0_SST_SIZE_BYTES`, `SSE_FEED_RING_BYTES`, and since
823b3269 `L0_MAX_SSTS`).

**The L0 cap and compaction.** The approved default of 32 was measured with
the compaction worker at its upstream defaults (the local rig sets none).
Production runs 32 with one compaction, one subcompaction and 32 MiB rolls.
`docs/CAPACITY-R27.md` records that with the upstream worker values a
32-input L0 merge can stage about 1 GB. Production is covered by the
profile; a server without it on a 1 GiB instance now reaches that case more
easily than at a cap of 8. Measured (below): the profile's lines cost batch
ingest 27% of its rate on the local rig and lower the peak resident memory
from 1,197 MB to 814 MB; request-per-record appends lose nothing.

Recommended: take the six compaction and store lines first (rows 1 to 4),
because they are what the new L0 default depends on; decide the memory and
client-visible rows with the question above.

## Package 3: the commit pipeline, as one decision

Status (2026-09-29, the three WAL settings): done, edge record #77, on the
owner's delegation ("For other questions, please make your own judgement
call"); the record awaits ratification. The binary's defaults are
`WAL_GROUP_COMMIT=1`, `WAL_FLUSH_GAP_MS=10` and `WAL_POST_ACK_GATHER_MS=6`.
The switch stays: `WAL_GROUP_COMMIT=0` selects the tick, and
`scripts/bench-fra-ab.sh` now sets it, because its recorded baseline is the
50 ms tick. Before the defaults moved, the provider contract's SlateDB
writer was made to flush as the configured pipeline does, and the SlateDB
and HTTP cases run under both pipelines on both local stores; no verdict
differs between them. Not done: the second step (deleting the switch: one
WAL path), and `ShardConfig::default` in `src/shard.rs`, which only tests
read and which stays tick mode (about 60 test sites pair it with their own
5 ms SlateDB timer; the file has no line headroom and feeds six receipts).
Not run: the rigs that start the binary (conformance, the field gate, the
platform e2e, the LiveFeed certification, the SDK smoke, the noisy-neighbour
campaign); they change pipeline with this default and are to be run before
the push. The field measurement on Tigris that the paragraph below asks for
has not been made. The rows of the table below are not changed yet. The
text that follows is the audit's and is not rewritten.

Status (2026-09-29, `ABSORB_AGE_SECS`, the first row of the table below):
done, edge record #78, on the same delegation; the record awaits
ratification. The binary's default is 60. It is the value the deployments
run, not a measured improvement: the one run that names the setting changed
two variables, and the cost of more frequent absorption on sparse streams
has not been measured. By the family files eight of the nine families that
set the name set 60 (the table's "seven of eight" is the audit's count);
`fra-ab-server` sets 300 and keeps it, with its script. Not changed:
`AbsorberConfig::default` in `src/history.rs`, which only tests read and
which still says 300 (no line headroom; three receipts). Not run: the rigs
that start the binary. The other three rows are not changed yet.

Status (2026-09-29, `ADMIT_MAX_INFLIGHT` and
`ADMIT_MAX_INFLIGHT_PER_STREAM`, rows 2 and 3 of the table below): done as
one change, edge record #79 (medium), on the same delegation; the record
awaits ratification. The binary's defaults are 512 and 256. The instance
cap was off: a server that sets nothing now refuses appends above 512
requests in flight and, above 2,048, every request to a stream path before
authentication; the count covers every request on every route. The two are
one change so that no commit has a per-stream cap of 256 without an
instance cap above it. They are the values the deployments run; neither is
derived or measured as an optimum. `fra-ab-server` and its script, which
set the instance cap to 256, now set the per-stream cap to 64, the value
they ran. No other script, family or profile is edited. Not run: the rigs
that start the binary. After this status the row of `SLATE_S3_REGION` is
the one that is not changed.

`WAL_GROUP_COMMIT=1`, `WAL_FLUSH_GAP_MS=10` and `WAL_POST_ACK_GATHER_MS=6`
are set together by eight of the nine server families; the binary defaults
to tick mode (0, 0, 0). Changing one alone gives a combination nothing runs,
so the three are one decision, and its second step is to delete the switch:
one WAL path. Measured (below): with clients that wait for each
acknowledgement the pump halves the latency and doubles the rate, for 2.2
times the WAL writes.

Also in this package, each an owner performance decision:

| Setting | Default | Deployed | Note |
|---|---|---|---|
| `ABSORB_AGE_SECS` | 300 | 60 in seven of eight families | the value production runs, not a measured improvement |
| `ADMIT_MAX_INFLIGHT` | 0 (off) | 512 | client-visible: 429 above it |
| `ADMIT_MAX_INFLIGHT_PER_STREAM` | 64 | 256 | client-visible |
| `SLATE_S3_REGION` | us-east-1 | auto | effect on a store that is not Tigris not determined |

## Package 4: settings nothing sets become constants

Status (2026-09-29): fourteen of the twenty-four names are constants, in one
commit, edge record #80, on the owner's delegation; it awaits ratification.
Done: the six GC names, `L0_MAX_SSTS_PER_KEY`, the two gather skips, the
three bucket arguments, `TAIL_MAX_BYTES` and `SSE_H1_MAX_BUF`. Their
arguments are refused and their environment names are ignored. The fields of
the gather skips and the buckets stay in `CliArgs`, not settable, because
`bootstrap::run` reads them and may not change. Kept settable on purpose:
`ABSORB_READ_PAR` (the memory lever of a gather, and the lever the documents
name for the append-latency dip). Not attempted: `TRIM_PER_OP`,
`HANDLE_IDLE_EVICT_SECS`, the six billing and metrics names and
`REBALANCE_MOVE_COOLDOWN_SECS`, which the page calls the owner's taste.

| Setting | Value |
|---|---|
| `WAL_GATHER_SKIP_REQS`, `WAL_GATHER_SKIP_BYTES` | 32, 1 MiB |
| `ABSORB_READ_PAR` | 8 |
| `TAIL_MAX_BYTES` | 1 MiB |
| `WAL_GC_INTERVAL_SECS`, `WAL_GC_MIN_AGE_SECS`, `COMPACTIONS_GC_INTERVAL_SECS`, `COMPACTIONS_GC_MIN_AGE_SECS`, `GC_QUIET_INTERVAL_SECS`, `HISTORY_GC_INTERVAL_SECS` | 30, 60, 30, 120, 600, 600 |
| `L0_MAX_SSTS_PER_KEY` | always `L0_MAX_SSTS` |
| `TRIM_PER_OP` | equal to `TRIM_GLOBAL_BUDGET` |
| `HANDLE_IDLE_EVICT_SECS` | 600 |
| `METRICS_INTERVAL_SECS`, `MONTH_CLOSE_GRACE_MS`, `SWEEP_DISCOVERY_MAX`, `SWEEP_MAINT_RESIDENT`, `SWEEP_RESIDENT_QUANTUM`, `ALERT_USAGE_OUTBOX_DIRTY` | 15, 24 h, 8, 2, 4, 1000 |
| `REBALANCE_MOVE_COOLDOWN_SECS` | 60 |
| `SSE_H1_MAX_BUF` | 64 KiB |
| `--ops-bucket`, `--shard-bucket`, `--data-bucket` | every role uses `SLATE_S3_BUCKET` |

Twenty-four names. No deployment has to write any of them today, so removing
them simplifies the binary, not a deployment. What is given up is a lever
that needs no rebuild during an incident. Recommended for the GC intervals,
the gather skips, the per-key cap and the buckets; the others are the
owner's taste.

## Package 5: switches with one live path

Status (2026-09-29): the first row is done, in one commit, edge record #81,
on the owner's delegation; it awaits ratification. `ABSORB_PACE_MS` and
`ABSORB_PACE_WINDOW_MS` are not options (the arguments are refused, the
environment names are ignored) and the pacing code is gone: a gather never
parks between read waves. Left for the owner: the counter of the pace time
and its three reporters (`gather_last_pace_ms` on /v1/debug/load and in the
ops gauges, `absorber.lastPaceMs` on /v1/debug/absorb) stay and report 0,
because `collect_snapshot` has an exact exception row that only the owner
rewrites.

Status (2026-09-29, second row): done, in one commit, edge record #82, on
the owner's delegation; it awaits ratification. `STORE_MAX_CONCURRENT` is
not read and the count semaphore of the store wrapper is gone with its six
call sites: no store call waits for a count permit, which is what the
default of 0 meant. The byte gate (`STORE_BULK_INFLIGHT_MAX_BYTES`) is not
changed. For the owner: the R10 mechanism test
`runtime_store_concurrency_is_shared_locally_and_independent_of_first_access`
exercised the semaphore; it is rewritten on the byte gate under the same
name and re-pinned in `docs/refactor/review-mechanisms.json`. The other
three rows are not started.

| Setting | Today | Proposed |
|---|---|---|
| `ABSORB_PACE_MS`, `ABSORB_PACE_WINDOW_MS` | off | no option, no pacing code |
| `STORE_MAX_CONCURRENT` | off | no option, no semaphore |
| `SCALE_RPS_CAPACITY` | off | no option, no rps dimension |
| `HISTORY_COMPACTOR` | on unless "off" | always on |
| `BILLING_METER` | on unless "off" | always on |

These remove code, so they cost receipts and mutation legs. Recommended, in
the order above, each with the next change to the file it touches.

## Kept, with the reason

| Setting | Why it stays |
|---|---|
| `TAIL_RING_BYTES` | A switch between two read paths, which the invariant "one permanent production path" forbids, but neither direction is safe today: the budget is per engine, and no budget is certified for 1 GiB. A design item for the owner. |
| `FRAME_COMPRESS` | Client-visible (frame version 5 on the frames read), under the cryptographic hold, and not every family sets it. |
| `COMPACTOR_POLL_MS` | Edge record #48 made it take effect this month and three tests pin that; the only compaction lever that needs no rebuild. |
| `SHARD_OPEN_DEADLINE_MS`, `SHARD_OPEN_WAIT_MS`, `UNREADY_EXIT_AFTER_SECS` | The only remedy for a slow-open loop without a rebuild. |
| `TRIM_GLOBAL_BUDGET` | Caps `TRIM_PER_OP`; the documents name larger values for throughput shards. |
| `FORK_DEBT_SWEEP_SECS` | A correctness repair, named by the formal record; not a billing cadence. |
| Deploy-script lines that restate a default | On Compute they are the reset of a stale value. |
| `INITIAL_SHARDS`, `MEMPROFILE_CERT`, identity, credentials, addresses, limits and quotas (`LIMIT_*`), the scalers' `SCALE_*`, `FLEET_*`, `MANIFEST_POLL_MS`, `MAX_REQUEST_BODY_BYTES`, `OUTBOX_SWEEP_SECS`, `TOKIO_WORKERS`, posture switches, debug hooks | Topology, policy or per-deployment by nature. |

## Measured

On the local rig (`~/.streams-ab`, outside the repository): one binary
(0b72bddb) in both arms, settings through the environment only, s3lite with
2 ms per operation, three ABBA/BAAB groups, six pairs per point, medians
with bootstrap 95% intervals. P1 is 1,024 clients on 32 streams, P2 64
clients, P4 32 clients on one stream, P3 JSON batches. The rig is a Mac; the
ratios are the evidence, not the absolute figures, and none of this is a
field measurement on Tigris.

**The L0 cap under the production profile and commit pipeline, 8 against
32** (what 823b3269 changed, in the posture production runs):

| Point | Rate | CPU per unit | Errors | Backpressure warnings |
|---|---|---|---|---|
| P3 | 46,454 -> 70,336 records/s, +57% [+30%, +82%] | -14% [-19%, -9%] | 125 -> 0 | 228 -> 0 |
| P1 | 36,600 -> 40,942 req/s, +11% [+9%, +15%] | unchanged | 0 -> 0 | |

**The profile's compaction, store and memory lines against the binary
defaults**, both at cap 32 with the production commit pipeline:

| Point | Rate | CPU per unit | Peak resident memory |
|---|---|---|---|
| P3 | 98,015 -> 71,131 records/s, -27% [-30%, -24%] | unchanged | 1,197 -> 814 MB |
| P1 | 38,994 -> 40,230 req/s, unchanged [-3%, +13%] | unchanged | 705 -> 614 MB |

**The commit pipeline, tick mode against pump + 10 ms gap + 6 ms gather:**

| Point | Rate | p50 | p99 | CPU per unit | WAL writes |
|---|---|---|---|---|---|
| P1 | 30,752 -> 40,100 req/s, +31% [+27%, +39%] | 28.4 -> 15.7 ms | 58.0 -> 32.0 ms | +6.3% [+4.0%, +7.7%] | 3,276 -> 7,191 |
| P2 | 1,927 -> 4,670 req/s, +142% | 33.9 -> 13.6 ms | 41.9 -> 17.6 ms | -20% [-26%, -11%] | 3,043 -> 6,836 |
| P4 | 973 -> 2,238 req/s, +133% | 33.0 -> 14.2 ms | 37.5 -> 18.9 ms | unchanged | 729 -> 1,649 |
| P3 | 83,288 -> 98,673 records/s, +18% [+16%, +22%] | -49% | -38% | +9.3% [+7.7%, +13.0%] | |

On Tigris a WAL write costs about 40 ms and a request, so the rate gain will
be smaller and the write count is a cost: that is the field evidence the
decision needs.

## Not determined

- What any real Compute project holds: only a platform export shows it.
- The reason for `SLATEDB_RT_THREADS=4`, `WAL_FLUSH_GAP_MS=10`, and
  `ADMIT_MAX_INFLIGHT` 512.
- The number of vCPUs of the 1 GiB class.
- A memory-safe tail-ring budget for 1 GiB.
