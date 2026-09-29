# Configuration audit of 2026-09-29: every recommendation, as verified

The detail behind `../../config-simplification.md`. One section per setting
or group: what was proposed, whether it survived an adversarial reading of
the code, the tests and the documents at 823b3269, what would have to change
with it, what a client could observe, and what only the owner can decide.
Nothing was built or run for this file. Line numbers are those of 823b3269.

### Defaults

#### COMPACTOR_MAX_CONCURRENT (env) and --compactor-max-concurrent (dead clap field)

- Proposed: 4; the argv flag is parsed and never read -> 1; delete the clap field or wire it as edge record #48 did for the poll interval
- Verification: **holds**
- Reason:
  The profile sets 1 and MEMPROFILE_CERT refuses any other value. The profile
  records that the upstream value leaves 200-340 MB of buffer the bulk gate
  cannot bound. The clap field has no reader: only the env overlay reaches the
  engine. This default matters more now that the L0 cap default is 32.
- What the verifier found:
  Tree read: HEAD 823b3269 (the L0 default is already 32 there), not 9d6cb381.
  The stated facts are right. Default 4: src/config/model.rs:376 and
  src/config/cli.rs:167. The env overlay is the only path to the engine:
  src/config/load.rs:63-65 -> model.rs:100,109. The clap field has no reader:
  every other hit is self.engine.* (src/config/summary.rs:24) or the fixture
  (cli.rs:585). Profile sets 1 (deploy/profiles/compute-1g.env:88);
  certification refuses anything else (src/config/profile.rs:96-100,109-113).
  I could not refute the direction, but three things weaken it as written. (1)
  It is incomplete. docs/CAPACITY-R27.md:128-149 records that with the gate at
  cap and ONE compaction per DB the rig still waved 478->886 MB; the mass was
  the worker defaults (4 subcompactions, 4 x 2 MiB read-ahead, 256 MiB rolls;
  'a 32-input L0 merge can stage ~1 GB'). Those are still binary defaults
  (model.rs:377-380) and the bulk gate defaults to off (model.rs:366). With
  the L0 cap default now 32 (cli.rs:136) a server without the profile on 1 GiB
  is exactly that case; changing concurrency alone leaves it uncertified on 5
  of 7 measurements (src/config/certification_tests.rs:12-20). (2) It is
  unmeasured. The +60% L0 result ran with COMPACTOR_MAX_CONCURRENT unset, i.e.
  4 (~/.streams-ab/ab.py:133-147 sets no compaction knob; commit 823b3269 says
  'the compactor kept up'). Whether one compaction keeps up at cap 32 with the
  limiter lifted is unknown. (3) It changes every DST engine:
  EngineConfig::default() feeds ShardConfig::default() (src/shard.rs:989) and
  the history partitions (src/shard.rs:1256), so the suite and the
  load-sensitive capacity test run with a different compaction concurrency.
- Must change with it:
  src/config/model.rs:76 (doc), :376; src/config/cli.rs:165-168 and :585
  (fixture); src/config/tests.rs:213 (default pin), :408 (EXPECTED_CLI_SURFACE
  row, or its removal), :153-156 (comment 'five names clap and the overlay
  both read'; :167 keeps passing either way);
  src/config/certification_tests.rs:12-21 (expects 7 mismatches including
  'max_concurrent_compactions=4' and 'worker_max_concurrent_compactions=4';
  becomes 5); deploy/profiles/compute-1g.env:83-84 (comment 'binary default is
  4'); docs/STAGING.md:158 sets COMPACTOR_MAX_CONCURRENT=2 (with :156-157
  MANIFEST_POLL_MS=1000, COMPACTOR_POLL_MS=500), which contradicts the
  certificate and RUNBOOK.md:103-104; RUNBOOK.md has no row for this knob
  (only a mention at :104) and should get one;
  scripts/effective-config/effective_config.py:72-76 (leaf-count pins fail
  until updated if the clap field goes); if wired like item 32:
  src/config/model.rs:337-341 (with_knob_defaults) and load.rs:62-65. No
  formal receipt lists src/config/* (verification/manifest.json), and
  src/config is not a critical mutation prefix
  (scripts/quality/verification_plan.py:22-31). Bench scripts that set 2
  explicitly are unaffected (bench/costab/run-wide.sh:35,
  bench/fleet/local-fanout.sh:61, bench/docker/harness/cluster-deploy.sh:49).
- What a client can observe:
  No product or wire change. Operator-visible: the debug compactor_profile
  (src/http.rs:947) and the startup summary (src/config/summary.rs:24) show 1
  instead of 4 on a server without the profile. If the clap field is deleted,
  --compactor-max-concurrent on argv changes from a silent no-op to a clap
  parse error (boot refusal), and a malformed COMPACTOR_MAX_CONCURRENT changes
  from a boot refusal (clap) to silently ignored (load.rs:11-13). Edge record
  #48 recorded the same class as surface 'process'
  (edge-changes.md:1125-1140).
- For the owner:
  (a) Is 1 the default for every instance class, or a 1 GiB value that belongs
  in the profile? (b) Should the whole certified compaction posture move into
  defaults together (COMPACT_MAX_SUBCOMPACTIONS=1, COMPACT_MAX_FETCH_TASKS=1,
  COMPACT_BYTES_TO_FETCH=1 MiB, COMPACT_MAX_SST_SIZE_BYTES=32 MiB,
  STORE_BULK_INFLIGHT_MAX_BYTES=32 MiB)? That closes the prefetch hazard the
  L0=32 default opened; concurrency alone does not. (c) Delete the flag (the
  R29 precedent, cli.rs:170-176) or wire it (item 32 / #48)? (d) Require an
  A/B leg at concurrency 1 vs 4 at cap 32 before accepting. (e) Keep the
  profile line: an older binary deployed with a thinned profile and
  MEMPROFILE_CERT refuses to boot.

#### COMPACT_MAX_SUBCOMPACTIONS / COMPACT_MAX_FETCH_TASKS / COMPACT_BYTES_TO_FETCH

- Proposed: 4 / 4 / 2 MiB (SlateDB upstream) -> 1 / 1 / 1 MiB
- Verification: **holds**
- Reason:
  The profile sets the certified values and MEMPROFILE_CERT enforces them. The
  profile attributes an RSS wave of 478 to 886 MB in 30 s to the worker's own
  prefetch at the upstream values and says a 32-input L0 merge can stage about
  1 GB. With the L0 cap default now 32, a deployment without the profile runs
  exactly that combination.
- What the verifier found:
  Holds, but two statements in its reason are wrong and the gain is smaller
  than stated. Paths are under  unless absolute.
  VERIFIED. Defaults are 4 / 4 / 2 MiB (src/config/model.rs:377-379) and equal
  SlateDB's at the pinned rev 0717cc1e (Cargo.toml:123;
  ~/.cargo/git/checkouts/slatedb-a6e73982df30678a/0717cc1/slatedb/src/config.rs:1376-1382).
  The profile sets 1 / 1 / 1048576 (deploy/profiles/compute-1g.env:98-100) and
  MEMPROFILE_CERT refuses anything else (src/config/profile.rs:94-103,
  :115-121). One resolved profile reaches all four DB families
  (profile.rs:56-77), so the default change applies to shard, history v1,
  history v2 and telemetry together. A documented deployment without the
  profile exists: docs/GUIDE-COMPOSER.md:202-218 starts the binary on Compute
  with none of the memory knobs, so it runs the upstream worker today.
  WRONG IN THE REASON. (1) Merge fan-in, which the recommendation could not
  determine, is at most 8. SlateDB's size-tiered scheduler clamps L0
  candidates and sorted-run candidates to max_compaction_sources
  (size_tiered_compaction.rs:347 and :387-392 in the checkout above), whose
  default is 8 (config.rs:1414-1419), and nothing in src sets scheduler
  options (no match for scheduler_options, max_compaction_sources). A 32-input
  L0 merge is not reachable, so raising the L0 cap to 32 did not create a new
  unsafe combination; it only lets more L0s wait. The case for the change
  rests on the measured RSS wave (docs/CAPACITY-R27.md:129-147), not on the L0
  cap. (2) The binary default does not stop being the unsafe model:
  COMPACTOR_MAX_CONCURRENT stays 4, the roll size stays 256 MiB and the bulk
  gate stays off (model.rs:376, :380, :366), and the profile keeps those lines
  and MEMPROFILE_CERT.
  NOT MEASURED. The harness behind the +60% result sets no COMPACT_* variable
  (~/.streams-ab/ab.py:135-147; knobs.sh:9-10 passes only
  L0_MAX_SSTS and L0_SST_SIZE_BYTES), so cap 32 was measured with the upstream
  worker. Nobody has measured cap 32 with the 1 / 1 / 1 MiB worker and four
  concurrent compactions.
  COULD NOT DETERMINE. Whether any timing-sensitive DST or the capacity test
  moves: every DST engine inherits the default through ShardConfig::default
  (src/shard.rs:989). I did not run anything.
  ROLLBACK HAZARD of dropping the three profile lines: deploy scripts take the
  profile from the tree and the binary by object key
  (scripts/bench-fra-ab.sh:41-50, :74). A binary built before the change,
  deployed with the trimmed profile, refuses to boot under MEMPROFILE_CERT.
- Must change with it:
  Paths under .
  - src/config/model.rs:377-379 (values), :78-83 (doc comments state the old
  defaults).
  - src/config/tests.rs:214-216 (default_values_are_pinned).
  - src/config/certification_tests.rs:12-21: expects seven mismatches
  including bytes_to_fetch=2097152, max_fetch_tasks=4 and
  max_subcompactions=4; four remain, and the length assertion at :21 follows.
  - deploy/profiles/compute-1g.env:89-100: the comment (upstream defaults, the
  32-input figure) and, if the owner wants, lines 98-100.
  - bench/livefeed-perf/run-one.sh:76-78 sets the three explicitly (redundant
  afterwards).
  - docs/CAPACITY-R27.md:123-124 and :129-147 describe the binary default as
  4.
  - Comments that call 4 / 4x2 MiB the running default:
  src/config/profile.rs:17-24, src/billing.rs:1491-1498,
  src/config/cli.rs:170-176.
  - Inherit without an edit: src/shard.rs:989,
  src/dst/tests/read_history_lifecycle.rs:219.
  - Not affected: EXPECTED_CLI_SURFACE and CliArgs::deterministic (the three
  are environment-only, cli.rs:170-176);
  src/config/validation_tests.rs:667-675 sets COMPACT_MAX_SUBCOMPACTIONS=4
  itself; validation_tests.rs:14-38 compares families with the resolved
  profile, not with literals.
  - RUNBOOK.md has no row for these three names.
  - No formal receipt lists a src/config path (verification/manifest.json),
  and src/config is not a mutation-critical prefix
  (scripts/quality/verification_plan.py:22-31).
  - scripts/effective-config: the two families without the profile
  (families/defaults.family, families/platform-e2e.family) will show three
  changed engine values on the next comparison run.
- What a client can observe:
  No product wire change. Operators see it: /v1/debug/load compactor_profile
  (src/http.rs:947) and the startup summary (src/config/summary.rs:25-27)
  report 1 / 1 / 1048576 on a deployment without the profile. Indirectly,
  compaction pace changes when backpressure starts under load.
- For the owner:
  Performance hold: accept 1 / 1 / 1 MiB for every instance class without a
  measurement at the new L0 cap of 32? Keep the three profile lines for one
  release so the profile still certifies the previous binary? Should the other
  certified values (COMPACTOR_MAX_CONCURRENT=1, 32 MiB rolls, 32 MiB bulk
  gate) become defaults too, so the whole compaction block leaves the profile?

#### COMPACT_MAX_SST_SIZE_BYTES

- Proposed: one name, two fields: 256 MiB compactor output roll and 8 MiB nominal GET weight -> 32 MiB for both
- Verification: **holds**
- Reason:
  The profile sets 32 MiB and the certificate requires it for the output roll.
  One environment name feeds two fields with different defaults; with the
  profile both already run at 32 MiB, so one default removes the divergence
  the code comments call preserved.
- What the verifier found:
  Read-only verification at the repository root (all paths below
  are relative to it). Note: the checkout is at 823b3269, one commit after the
  9d6cb381 the task names; L0_MAX_SSTS is already 32 there
  (src/config/cli.rs:136, :582; src/config/tests.rs:405).
  I could not break this one.
  - Current defaults are as claimed: nominal GET weight 8 MiB
  (src/config/model.rs:367), compactor roll 256 MiB (model.rs:380), one name
  feeding both (src/config/load.rs:50-58). 256 MiB is SlateDB's own default
  (slatedb rev 0717cc1, config.rs:1379).
  - Deployed value is as claimed: the profile sets 33554432
  (deploy/profiles/compute-1g.env:101) and the certificate requires it
  (src/config/profile.rs:102).
  - The nominal weight is inert without the profile: it is read only when the
  bulk gate exists (src/store_timing.rs:114-117;
  src/store_timing/resources.rs:18-21, :42-48) and the gate is off by default
  (model.rs:366). The only two setters of the gate also set this name
  (compute-1g.env:82/:101; bench/livefeed-perf/run-one.sh:74/:79).
  - Without the profile, before: compaction output rolls at 256 MiB in every
  DB family (profile.rs:56-77). After: 32 MiB rolls, so more and smaller
  compacted objects and less buffered output. No stored format changes. Test
  rigs follow automatically (src/shard.rs:989 builds from
  EngineConfig::default()).
  Limits of what I verified:
  - I found no measurement of roll size on throughput; this is a performance
  hold.
  - The code asks for the divergence to stay (load.rs:51-53 'do not fix here';
  model.rs:6-10 assigns cleanup to WP-13/WP-14), so lifting it is the owner's
  call.
  - The profile line should stay. Deploy scripts pick the binary by tag
  (bench/soak/deploy-region.sh:163, bench/fleet/deploy-fleet.sh:107,
  scripts/bench-fra-ab.sh:77), so an older binary can be deployed with this
  tree's profile; with MEMPROFILE_CERT and no line it refuses to boot
  (profile.rs:115-121). The claimed gain 'one profile line goes' is therefore
  not free.
- Must change with it:
  - src/config/model.rs:367 and :380, with their doc text at :8-9, :61-65,
  :84-87
  - src/config/load.rs:51-53 (comment)
  - src/config/tests.rs:211 and :217 (default_values_are_pinned); :313 comment
  - src/config/certification_tests.rs:12-21: the expected list loses
  'max_sst_size=268435456 (certified 33554432)', so 7 errors become 6
  - docs/reviews/2026-09-hardening/effective-config-diff.md: the next run
  shows two changed fields for the families without the profile (defaults,
  platform-e2e)
  - bench/livefeed-perf/run-one.sh:79 becomes redundant
  Not affected: EXPECTED_CLI_SURFACE and CliArgs::deterministic (the name is
  env-only, src/config/cli.rs:170-176); RUNBOOK.md (it has no row for this
  name); formal receipts (no src/config file is in
  verification/manifest.json).
- What a client can observe:
  Not by a product client. An authorized caller of GET /v1/debug/load sees
  compactor_profile.max_sst_size change from 268435456 to 33554432 on a
  deployment without the profile (src/http.rs:947; profile.rs:36-48). The
  startup summary shows both fields (src/config/summary.rs:20, :28).
- For the owner:
  1. Lift the 'preserved divergence' now, or leave it to WP-13/WP-14?
  2. Accept 32 MiB rolls for deployments without the profile with no
  throughput measurement, or measure first as was done for L0_MAX_SSTS?
  3. Found in passing, not part of this item: RUNBOOK.md:99 lists the
  L0_SST_SIZE_BYTES default as 32 MiB while the binary default is 8 MiB
  (cli.rs:108), and --compactor-max-concurrent (cli.rs:167-168) is parsed but
  never read (only load.rs:63-65 feeds the engine). Both are simplification
  candidates.

#### STORE_BULK_INFLIGHT_MAX_BYTES

- Proposed: 0 (gate off) -> 33554432
- Verification: **holds**
- Reason:
  The profile calls this the only instance-wide bound on flush and compaction
  transfers and records a death without it (RSS 423 to 656 MB in about 60 s).
  MEMPROFILE_CERT requires exactly 32 MiB. With default 0 the unsafe path is
  selected by omission.
- What the verifier found:
  All paths are relative to the repository root. Note: the tree
  is at 823b3269 (L0_MAX_SSTS default already 32), not 9d6cb381.
  The claims check out. Default 0 (src/config/model.rs:57-60, :366; overlay
  src/config/load.rs:47-49); 0 builds no gate
  (src/store_timing/resources.rs:18-21); the certificate requires exactly
  33,554,432 (src/config/profile.rs:104-108); the profile sets it and records
  the death (deploy/profiles/compute-1g.env:69-82). No script sets a different
  value (only bench/livefeed-perf/run-one.sh:74, same value).
  I could not break it, but three limits apply:
  1. It is not sufficient alone. The gate is 'an SST leaf-I/O overlap limiter,
  not a complete memory budget' (resources.rs:64-70) and the first gated run
  still died (compute-1g.env:77-81). The other six certified values stay at
  upstream defaults (model.rs:376-380: 4/4/4/2 MiB/256 MiB against certified
  1/1/1/1 MiB/32 MiB), so a server without the profile is still uncertified.
  2. A server without the profile will not match production. The weight of an
  unknown-length SST GET comes from COMPACT_MAX_SST_SIZE_BYTES: 8 MiB by
  default (model.rs:61-65, :367; load.rs:50-55), 32 MiB (the whole gate) under
  the profile.
  3. Performance is unmeasured. The gate also queues SST-class GETs, which are
  reads (src/store_timing.rs:106-122; class 2 is any .sst outside wal/,
  src/store_timing/observations.rs:62-76). I could not determine whether the
  L0 A/B rig ran gated.
  DST rigs are unaffected: they take StorageConfig::default()
  (src/runtime.rs:236-238) but only bootstrap wraps stores in TimingStore
  (src/bootstrap.rs:58, :74). The provider-contract test would start running
  gated (src/bootstrap/tests/provider_contract.rs:154-169).
- Must change with it:
  - src/config/model.rs:57-60 (doc) and :366
  - src/config/tests.rs:210 (pins 0)
  - src/config/certification_tests.rs:12-21 (drop line 18; the expected error
  count goes 7 -> 6)
  - RUNBOOK.md has no row for this knob (section 3.3 at :151-158); add one
  - deploy/profiles/compute-1g.env:82 only if the owner wants the line gone; a
  reused project keeps the same value, so that is harmless
  - scripts/effective-config: re-run the report; the defaults and platform-e2e
  families change storage.bulk_inflight_max_bytes
  No formal receipt lists src/config or src/store_timing
  (verification/manifest.json), and neither is a mutation-critical prefix
  (scripts/quality/verification_plan.py:22-31). EXPECTED_CLI_SURFACE and
  CliArgs::deterministic are untouched: this is an environment-only knob.
- What a client can observe:
  No status, header or body changes. Latency only: on a server that sets
  nothing, SST-class GETs (cold reads) and flush/compaction PUTs now wait at
  the gate. Operator surfaces change: /v1/debug/store 'bulk_gate' goes from
  {"cap_bytes":0} to the full counters (resources.rs:54-59, :158-171;
  observations.rs:314), and the boot summary shows the new
  storage.bulk_inflight_max_bytes (src/config/summary.rs:19).
- For the owner:
  Performance hold: accept 32 MiB as the gate for every deployment without a
  gated measurement on the local rig? Should the six compaction-worker values
  of the certificate become defaults in the same change, so that a binary with
  no settings is the certified profile?

#### WAL_GROUP_COMMIT

- Proposed: 0 (tick mode) -> 1 (pump); second step, an owner decision: delete the switch so there is one WAL path
- Verification: **holds**
- Reason:
  Every Compute family except the older fra-ab benchmark sets 1; STAGING and
  GUIDE-COMPOSER prescribe 1. The tick path is what the certification rigs and
  most DST fixtures exercise (ShardConfig::default is false; 7 sites set
  true), so tests cover a write path production does not run.
- What the verifier found:
  All paths relative to the repository root. Tree read at HEAD
  823b3269 (L0 default is already 32: src/config/cli.rs:136), not 9d6cb381.
  Claims verified: default 0 (src/config/cli.rs:61-62, fixture :573); every
  Compute script sets 1 (bench/soak/deploy-region.sh:173,
  bench/fleet/deploy-fleet.sh:133, bench/soak/mt-tenants.sh:143,
  bench/soak/wc-ladder.sh:195); scripts/bench-fra-ab.sh:84 does not;
  docs/STAGING.md:153 and docs/GUIDE-COMPOSER.md:211 prescribe 1.
  ShardConfig::default is false (src/shard.rs:973) and exactly 7 test sites
  set true. I could not break the change, but four things the recommendation
  understates: (1) With only this default changed a bare binary runs pump with
  gap = FLUSH_INTERVAL_MS = 25 ms (src/bootstrap.rs:420-424) and gather 0
  (cli.rs:79), a combination no deployed family runs (all run gap 10, gather
  6); STAGING/GUIDE-COMPOSER run gather 0, so 'production' itself is not
  uniform on gather. (2) The CLI default does not touch ShardConfig::default,
  so DST rigs keep covering tick mode (src/dst/tests/fixture_storage.rs:34-38)
  until that default flips too. (3)
  src/bootstrap/tests/provider_contract.rs:78 opens a RAW SlateDB with
  shard_settings(CliArgs::deterministic) and waits await_durable
  (provider_contract/slatedb_cases.rs:19-37): with the fixture at 1 the flush
  interval becomes max(25,1000)=1000 ms (src/config/validation.rs:34-38) and
  there is no pump in that test, so each durable put waits up to 1 s and the
  test no longer exercises the production flush path. (4) The rigs that pass
  --flush-interval-ms 1 --wal-flush-gap-ms 2 are documented as 'group commit'
  (CONFORMANCE.md:72-79, .github/workflows/ci.yml:338-345) but today run 1 ms
  TICK mode: --wal-flush-gap-ms is only read by the pump, so it is a no-op
  there. After the change they really run the pump (gap 2 ms, SlateDB timer 1
  s), which matches the documented intent but changes what CI's conformance,
  field-gate and e2e legs measure. fra-ab would change from tick 50 ms to pump
  with a 50 ms gap. Step 2: the pump lives inside ShardEngine::start and the
  wiring inside bootstrap::run, both scopes with frozen exception-growth rows.
- Must change with it:
  Default change: src/config/cli.rs:54-62 (doc text is --help) and :573;
  src/config/tests.rs:392 (EXPECTED_CLI_SURFACE;
  cli_fixture_matches_scrubbed_parse follows the fixture); RUNBOOK.md:95-97;
  src/bootstrap/tests/provider_contract.rs:78 (pin its settings or accept the
  1 s timer); CONFORMANCE.md:28,:72-79 and
  .github/workflows/ci.yml:338-345,:386 (rerun, 332/6 expected); rerun
  scripts/platform-e2e.mjs:119, scripts/mt-noisy-campaign.mjs:116,
  bench/fleet/livefeed-cert.mjs:113, bench/canary/livefeed-canary.mjs:123,
  bench/livefeed-perf/run-one.sh:83; scripts/bench-fra-ab.sh:84 (add
  WAL_GROUP_COMMIT=0 if the benchmark must stay comparable); the
  effective-config report gains rows cli.wal_group_commit 0->1 for defaults,
  platform-e2e, livefeed-canary and fra-ab-server
  (scripts/effective-config/families/*.family, owner annotation). To align
  tests: src/shard.rs:973 and src/dst/tests/fixture_storage.rs:34-38. If
  script lines are then dropped: the env lines in
  scripts/effective-config/families/{region-server,region-server-scale,fleet-server-1,fleet-server-n,mt-tenants-off,mt-tenants-enforce,wc-ladder,wc-ladder-diet}.family
  (check_family 'unsourced', effective_config.py:1215-1218). Step 2
  additionally: src/shard.rs:880-893,:973,:1358; src/bootstrap.rs:419,:517;
  src/config/validation.rs:34-38; 10 test sites
  (src/shard/task_lifecycle_tests.rs:34, src/shard/retirement_tests.rs:100,
  src/dst/tests/durability_gather.rs:31,115,181,271, runtime_open_gate.rs:361,
  runtime_engine_lifecycle.rs:15, reads_applied.rs:331,
  topology_scaling.rs:409 capacity test); owner-updated rows in
  docs/quality/exception-growth.json:252-319 (crate::run) and :470-530
  (crate::ShardEngine::start); six receipts listing src/shard.rs plus TLA-011
  listing src/bootstrap.rs (verification/manifest.json:1691); a 'removed' row
  in scripts/effective-config/rename-map.json (K9);
  verification/tla/durability/README.md:200.
- What a client can observe:
  Timing only, no status, header or body change: acknowledgement latency of
  appends on a deployment that does not set the variable (RUNBOOK.md:96
  reports sequential append p50 55 -> 28 ms locally). Operators see the pump
  counters on the debug timings surface become non-zero
  (src/http.rs:1505-1518). Compute deployments already set 1 and see nothing.
- For the owner:
  (a) Accept the performance of the new bare default (pump, 25 ms gap, no
  gather) or change WAL_FLUSH_GAP_MS and WAL_POST_ACK_GATHER_MS defaults in
  the same decision, and to which values given that Compute runs 10/6 and
  STAGING/GUIDE-COMPOSER run 10/0? (b) Should ShardConfig::default flip with
  it so DSTs cover the production path? (c) Keep fra-ab in tick mode? (d) Step
  2: approve updated exception-growth rows for bootstrap::run and
  ShardEngine::start and the re-record of the stale receipts. Could not
  determine: the conformance and e2e results under the pump, and the field
  performance of gap 25/gather 0 (no build or test was run).

#### WAL_FLUSH_GAP_MS

- Proposed: 0 (= FLUSH_INTERVAL_MS, 25) -> 10
- Verification: **did not hold**. Instead: Do not change it alone. Either bundle it with WAL_GROUP_COMMIT=1 (and WAL_POST_ACK_GATHER_MS=6) as one owner decision on the commit-pipeline defaults, or leave it.
- Reason:
  Every Compute family sets 10 and the staging plan prescribes it. The value
  only acts inside the pump. No document derives the number 10.
- What the verifier found:
  The default and the mechanism are as claimed: default 0 at
  src/config/cli.rs:68, fallback to FLUSH_INTERVAL_MS at
  src/bootstrap.rs:420-424, used only inside the pump at
  src/shard.rs:1358-1360. Three things break the recommendation. (1) The pump
  is off by default (WAL_GROUP_COMMIT default 0, cli.rs:61), so a deployment
  without scripts sees no change at all; the real gap between binary and
  production is WAL_GROUP_COMMIT 0 vs 1 and WAL_POST_ACK_GATHER_MS 0 vs 6
  (deploy-region.sh:173-174), which this entry does not touch. (2) 'Every
  Compute family sets 10' is false: scripts/bench-fra-ab.sh:84 sets
  FLUSH_INTERVAL_MS=50 with no pump and no gap
  (scripts/effective-config/families/fra-ab-server.family:44). (3) 'One script
  line goes' is false: the setting shares its line with WAL_GROUP_COMMIT=1 and
  FLUSH_INTERVAL_MS=25 in every script (deploy-region.sh:173,
  deploy-fleet.sh:133, wc-ladder.sh:195, mt-tenants.sh:143), so one token
  goes. It also changes behaviour silently for pump-on deployments that rely
  on the fallback: bench/docker/compose.yml:24 sets WAL_GROUP_COMMIT=1 and no
  gap, so its mint-rate floor would move from 25 ms to 10 ms (up to 100 WAL
  SSTs/s per shard on a store faster than 10 ms; cli.rs:46-50 records that 5
  ms outran WAL GC). Confirmed that no document derives 10: the only mentions
  are configurations of record (bench/sinmax-report.md:34,
  docs/STAGING.md:154, docs/GUIDE-COMPOSER.md:212). STAGING.md is weak
  evidence: the same block prescribes MANIFEST_POLL_MS=1000,
  COMPACTOR_POLL_MS=500 and COMPACTOR_MAX_CONCURRENT=2 (:156-158), which
  RUNBOOK.md:103-104 and the profile contradict.
- Must change with it:
  If bundled: src/config/cli.rs:54-69 (defaults and --help text) and :573-575
  (fixture); src/config/tests.rs:392-394; RUNBOOK.md:95-97 and :672;
  src/config/validation.rs:34-38 (failsafe stretch keyed on the pump).
  Removing the 0-fallback edits bootstrap.rs:420-424, inside bootstrap::run,
  whose six exception rows are frozen
  (docs/quality/exception-growth.json:252-333) and bootstrap.rs feeds TLA-011.
  ShardConfig::default keeps its own 25 ms gap and pump off
  (src/shard.rs:973-975); matching it edits a zero-headroom file that feeds
  six receipts. Scripts: bench/soak/deploy-region.sh:173,
  bench/fleet/deploy-fleet.sh:133, bench/soak/wc-ladder.sh:195,
  bench/soak/mt-tenants.sh:143, bench/docker/harness/cluster-deploy.sh:46,
  bench/fleet/local-fanout.sh:58,
  bench/costab/run-{keyed,mature,soak,wide,split}.sh:28-46,
  bench/costab/wedge-liveness.sh:41. Families: fleet-server-1:84-85,
  fleet-server-n:88-89, mt-tenants-enforce:30-31 and :89-90,
  mt-tenants-off:56-57, region-server-scale:52-53, region-server:57-58,
  wc-ladder:69-70, wc-ladder-diet:68-69 (check-families fails on a name the
  script no longer sets,
  scripts/effective-config/effective_config.py:1216-1219). Docs:
  docs/STAGING.md:153-154, docs/GUIDE-COMPOSER.md:211-213.
- What a client can observe:
  No wire change. With the pump on and a store faster than the gap,
  acknowledgement latency and WAL object churn change. With the pump off (the
  binary default) nothing changes.
- For the owner:
  Performance hold: should the binary default be the production commit
  pipeline (pump on, gap 10 ms, post-ack gather 6 ms)? The memory note records
  an earlier reviewer call that the gather window be a staging env default,
  not a code default. If the pump becomes the only path, is WAL_GROUP_COMMIT=0
  removed, and do DST rigs that run pump-off keep a test-only switch?

#### WAL_POST_ACK_GATHER_MS

- Proposed: 0 (off) -> 6
- Verification: **did not hold**. Instead: do not change this default on its own; decide the WAL trio (WAL_GROUP_COMMIT, WAL_FLUSH_GAP_MS, WAL_POST_ACK_GATHER_MS) as one performance decision
- Reason:
  Every Compute family runs 6. The six-region soak with 6 brought the
  concurrency-2 to concurrency-1 append ratio from about 2x to 0.98-1.06 with
  0 errors over 1.56 M requests.
- What the verifier found:
  As written it does not do what it claims. The gather is read only inside `if
  cfg.wal_group_commit` (src/shard.rs:1358-1363; no other reader, rg over
  src), and WAL_GROUP_COMMIT defaults to 0 (src/config/cli.rs:61). A
  deployment with no settings therefore experiences nothing before or after:
  no pump, no gather. 'Every Compute family runs 6' is also not exact:
  fra-ab-server sets none of the three WAL names
  (scripts/effective-config/families/fra-ab-server.family;
  scripts/bench-fra-ab.sh:76-89), so it runs tick mode without the gather.
  Eight server families set all three together: WAL_GROUP_COMMIT=1,
  WAL_FLUSH_GAP_MS=10, WAL_POST_ACK_GATHER_MS=6
  (bench/soak/deploy-region.sh:173-174, bench/fleet/deploy-fleet.sh:133-134,
  bench/soak/mt-tenants.sh:143-144, bench/soak/wc-ladder.sh:195-196). A
  default of 6 would take effect only where a deployment sets
  WAL_GROUP_COMMIT=1 and omits the gather: docs/STAGING.md:153-154 and
  docs/GUIDE-COMPOSER.md:211-212. The field evidence for the value 6 is as
  claimed (docs/SOAK5-REPORT.md:10,25-33; RUNBOOK.md:672). The reviewer's
  rollout call ('env, not a code default; keep a 0 ms control') exists only in
  the memory note; git grep finds no such statement in the repository.
- Must change with it:
  If the trio becomes default: src/config/cli.rs:54-80 (three defaults and
  help text, 'Suggested 4-8'), :573-575 (fixture); src/config/tests.rs:392-394
  (EXPECTED_CLI_SURFACE); RUNBOOK.md:96-97 and :672; docs/STAGING.md:152-154;
  docs/GUIDE-COMPOSER.md:210-213; the eight family files
  (scripts/effective-config/families/*.family) and the script lines above.
  src/shard.rs:973-975 (ShardConfig::default: group commit false, gather ZERO)
  would stay: editing src/shard.rs stales KANI-047, TLA-002, TLA-005, TLA-006,
  TLA-011, TLA-016, and the file has no line headroom; DSTs set their own
  values (src/dst/tests/durability_gather.rs:31-33). Every rig built from
  CliArgs::deterministic would then run the pump. The local A/B rig sets all
  three to 0 explicitly (~/.streams-ab/ab.py:127) and is unaffected.
- What a client can observe:
  Timing only, and only where the pump runs: the concurrency-2 append p50
  moves from about 2x to about 1x of concurrency-1, and a busy flush cycle
  gains at most 6 ms. No status, header or body changes.
- For the owner:
  Are the binary defaults WAL_GROUP_COMMIT=1, WAL_FLUSH_GAP_MS=10 and
  WAL_POST_ACK_GATHER_MS=6 together (what eight families run), or does tick
  mode stay the default? Does the reviewer's 'keep a 0 ms control' still
  stand? Separately, RUNBOOK.md:483-496 says every deploy restates the
  COMPLETE environment and removals are explicit --unset-env, because Compute
  env is project-scoped and merged: deleting a script line does not remove the
  value from a reused project. Does that rule change to 'restate only what
  differs from the defaults'? It decides whether any 'one script line goes'
  gain is real. Script lines that already equal the defaults today:
  FLUSH_INTERVAL_MS=25, ABSORB_BYTES=4194304, POOL_IDLE_SECS=4,
  TAIL_RING_BYTES=0, STREAMS_DEBUG_TIMING=0 (deploy-region.sh:173-182).

#### FRAME_COMPRESS

- Proposed: off -> on; then the switch can go
- Verification: **did not hold**. Instead: keep off and keep the switch for now; fix RUNBOOK.md:98 only
- Reason:
  Every Compute family sets 1; RUNBOOK says to enable it for compressible
  payloads and the sinmax campaign removed a 5-6x NIC amplification with it.
  It is a writer policy: both code paths stay reachable either way because
  incompressible payloads are stored raw.
- What the verifier found:
  The recommendation does not hold as written. One supporting claim is wrong,
  and the change is blocked by holds the owner keeps.
  What is correct:
  - Default is off (src/config/load.rs:292-297; src/config/model.rs:297-301;
  src/config/tests.rs:281).
  - It is a writer policy: compression is kept only when it shrinks the
  payload and only from 256 bytes (src/crypto.rs:34-36, :520-529), so both
  frame versions stay reachable.
  - RUNBOOK.md:98 names frame v2/v3 where the code writes 4/5
  (src/crypto.rs:26-30).
  What is wrong: 'every Compute family sets 1'.
  - Of the five Compute deployers
  (scripts/effective-config/effective_config.py:70-71),
  scripts/bench-fra-ab.sh:72-88 does not set it; its family file has no such
  line.
  - The profile does not set it.
  - The release-posture rigs run with it off: scripts/platform-e2e.mjs:88-113,
  bench/canary/livefeed-canary.mjs:80 onward, bench/fleet/livefeed-cert.mjs:79
  region, and the conformance run (CONFORMANCE.md:22-32).
  - Test engines default to Disabled (src/shard.rs:987); only
  src/dst/tests/read_page_limits.rs:36 runs an engine with compression.
  - The read adapter's compressed path (src/http/read.rs:384, :469) has no
  test: the other two callers pass false (src/product.rs:2602;
  src/dst/tests/read_subset_retention.rs:218).
  So 'on' would become a default that the certification rigs and the guarantee
  suite do not exercise.
  Blockers:
  - Cryptographic hold: docs/crypto-frame-v4.md says external acceptance is
  pending.
  - Performance hold: the compression attempt runs in the serial committer
  (crypto.rs:34-36). I found no measurement of CPU per record with compression
  on at the current frame version.
  - Removing the switch edits src/bootstrap.rs:442 and :529-531, inside
  bootstrap::run (lines 138-914), which six approved rows freeze exactly
  (docs/quality/exception-growth.json:251-330). Only the owner updates those
  rows. bootstrap.rs also feeds receipt TLA-011.
  - Making the rigs match production means editing src/shard.rs:987, which
  stales six receipts (KANI-047, TLA-002, -005, -006, -011, -016).
- Must change with it:
  If the owner adopts it later:
  - src/config/model.rs:297-301 (derived Default gives false),
  src/config/load.rs:292-297
  - src/config/tests.rs:281, :322, :335
  - src/config/summary.rs:101
  - src/bootstrap.rs:442, :529-531, with the owner's update of the six rows at
  docs/quality/exception-growth.json:251-330, and receipt TLA-011
  - src/http/read.rs:337-343, :384, :469, plus a new test for the compressed
  adapter path (red first)
  - src/shard.rs:987 if rigs are to follow (six receipts)
  - RUNBOOK.md:98; docs/STAGING.md:155
  - the four deploy lines: bench/soak/deploy-region.sh:177,
  bench/fleet/deploy-fleet.sh:134, bench/soak/mt-tenants.sh:144,
  bench/soak/wc-ladder.sh:196
  - the nine 'env FRAME_COMPRESS=1' lines under
  scripts/effective-config/families/
  - scripts/effective-config/effective_config.py:74 (leaf count 155) and
  rename-map.json if the field goes
  - an edge-change record and docs/refactor/WIRE-MATRIX.md:47-49
  Safe now: the RUNBOOK.md:98 version numbers.
- What a client can observe:
  Yes.
  - GET /v1/stream/{name}?format=frames (docs/refactor/WIRE-MATRIX.md:47-49)
  would return version 5 frames, compressed before encryption, for
  compressible records of 256 bytes or more. A consumer that decodes only
  version 4 breaks.
  - bytes_out on those reads changes (src/http/read.rs:474-480).
  - Stored frame bytes change, and with them the frame_bytes totals and
  compression ratio on /v1/debug/usage and in the billing records
  (RUNBOOK.md:139-147).
- For the owner:
  1. Should this wait for the external acceptance of frame v4/v5?
  2. Before any default change, should bench-fra-ab, the release-posture rigs
  and conformance run with compression on, so the default is a certified
  configuration?
  3. Is compress-then-encrypt acceptable as the default for every tenant,
  given that ciphertext length then depends on how compressible the payload
  is?

#### ADMIT_MAX_INFLIGHT

- Proposed: 0 (off) -> 512
- Verification: **holds**
- Reason:
  RUNBOOK heads the section 'run with these ON in production' and records that
  with the guards off all four instances died in about 2 minutes. Every
  Compute family sets 512. The binary default is the state the runbook calls
  unsafe.
- What the verifier found:
  Default 0 is verified (src/config/cli.rs:502-507, :640;
  src/config/tests.rs:495). 512 is what runs: bench/soak/deploy-region.sh:178,
  bench/fleet/deploy-fleet.sh:135, bench/soak/mt-tenants.sh:145,
  bench/soak/wc-ladder.sh:197, docs/STAGING.md:165, plus eight local bench
  scripts (bench/costab/run-*.sh and wedge-liveness.sh,
  bench/docker/harness/cluster-deploy.sh:50, bench/fleet/local-fanout.sh:62).
  256 appears only in scripts/bench-fra-ab.sh:85 and RUNBOOK.md:249/:294. The
  2048 rung was an experiment: the ladder's final verdict blames RSS
  reservations, not the cap (bench/WORKLOAD-CERT-PLAN.md:249-259).
  Corrections to the recommendation:
  - The unconfigured binary is not 'guards off'. The RSS shed (600) and the
  per-stream cap (64) are on by default (cli.rs:484, :499). RUNBOOK.md:250
  wrongly lists the RSS default as 0.
  - Turning the cap on arms two behaviours. Writes get 429 + Retry-After 1
  after a 25 ms tarpit (src/application/append.rs:119-127;
  src/admission.rs:251-259). Above 4 x cap = 2048, every /v1/stream* and
  /v1/streams request, reads included, gets a pre-auth 503
  (admission.rs:238-246; src/http.rs:709-717; docs/refactor/WIRE-MATRIX.md:12,
  :268).
  - The count covers every request on every route (http.rs:691-697,
  :1253-1256), so parked long-poll reads use up the write cap.
  - 'One script line goes' is wrong: the same line carries
  ADMIT_MAX_INFLIGHT_PER_STREAM=256 (default 64).
  - The A/B rig's P1 point runs 1,024 clients (commit 823b3269 message), above
  the cap. It must set 0 explicitly or its numbers change.
  DST rigs are unaffected (src/dst/tests/fixture_http.rs:502-505 sets 0). I
  could not determine, without running them, whether the conformance, platform
  e2e or SDK suites ever hold more than 512 requests.
- Must change with it:
  - src/config/cli.rs:502-507 and the fixture :640
  - src/config/tests.rs:495 (EXPECTED_CLI_SURFACE)
  - RUNBOOK.md:247-254 (default and pilot columns, and the wrong RSS default
  at :250) and :294
  - a new record in docs/reviews/2026-09-hardening/edge-changes.md, and
  docs/refactor/WIRE-MATRIX.md:12
  - docs/STAGING.md:165
  - the A/B rig outside the repo (~/.streams-ab)
  If script lines are dropped: the five deploy scripts above and
  scripts/effective-config/families/*.family (region-server:64,
  region-server-scale:59, fleet-server-1:89, fleet-server-n:93,
  mt-tenants-off:61, mt-tenants-enforce:35/:94, wc-ladder:74,
  wc-ladder-diet:73, fra-ab-server:48). RUNBOOK.md:481-495 says every deploy
  restates the complete environment, and projects are reused
  (region-server.family:5-6), so a dropped name keeps its old value unless
  --unset-env is used.
  No formal receipt or mutation owner covers src/config/cli.rs.
- What a client can observe:
  Yes, on a server that sets nothing. Above 512 requests in flight, appends
  answer 429 'overloaded' with Retry-After: 1 after a 25 ms tarpit, where they
  queued before. Above 2048, every /v1/stream* and /v1/streams request answers
  503 'overloaded' with Retry-After: 1 before authentication.
  /operator/data.json 'admit_max_inflight' shows 512 (src/operator.rs:107).
  Deployments that already set the variable see no change.
- For the owner:
  Ratify the edge change, and pick the value: 512 (what the scripts run) or
  256 (RUNBOOK's guarded setting). Should ADMIT_MAX_INFLIGHT_PER_STREAM move
  from 64 to 256 in the same change, since every script sets 256? Do scripts
  keep restating the value, as RUNBOOK section 7.3 requires?

#### ADMIT_MAX_INFLIGHT_PER_STREAM

- Proposed: 64 -> 256
- Verification: **holds**
- Reason:
  Every Compute family sets 256. The AWS comparison found the default cap of
  64, not the architecture, held one stream to 1,944 rec/s (6,396 uncapped).
- What the verifier found:
  Paths relative to the repository root. Verified: default 64
  (src/config/cli.rs:499-500, fixture :639, pin src/config/tests.rs:490-494);
  every Compute script sets 256 (deploy-region.sh:178, deploy-fleet.sh:135,
  mt-tenants.sh:145, wc-ladder.sh:197); docs/STAGING.md:166; the AWS
  comparison attributes 1,944 rec/s to the default cap and measured 6,396 with
  the cap at 0 (bench/aws-comparison-plan.md:208-216). Only reader is
  src/bootstrap.rs:568; no DST depends on 64 (rigs pass per_segment_slots
  explicitly, src/dst/tests/fixture_http.rs:504, topology_scaling.rs:412). Not
  in the memprofile certificate (src/config/profile.rs:94-114). Caveats I
  found: (1) no document derives 256; the only measurement compares 64 with
  uncapped, so 256 is 'what scripts set', not a measured optimum. (2)
  Production's 256 sits under ADMIT_MAX_INFLIGHT=512; the binary default for
  the global cap is 0 (cli.rs:506), so a bare binary gets a per-stream cap of
  256 and no global cap. (3) scripts/bench-fra-ab.sh:85 sets
  ADMIT_MAX_INFLIGHT=256 and no per-stream value, and RUNBOOK.md:249 lists 256
  as the pilot global value: with the new default per-stream equals global
  there, so the cap's stated purpose ('one hot stream cannot occupy every
  admission slot', cli.rs:497-498) no longer holds in that family. (4)
  RUNBOOK.md has no row for this variable at all, and
  docs/refactor/WIRE-MATRIX.md does not contain the code stream_overloaded
  (only overloaded), so the edge record has to add both.
- Must change with it:
  src/config/cli.rs:497-500 and :639; src/config/tests.rs:490-494;
  AWS-readyness.md:99 (states default 64); RUNBOOK.md section 3.6 (:245-250)
  gains a row; docs/refactor/WIRE-MATRIX.md line 42 (add 429
  stream_overloaded, src/application/append/contract.rs:89); a record in
  docs/reviews/2026-09-hardening/edge-changes.md with a pinning test;
  scripts/bench-fra-ab.sh:85 (set the per-stream cap explicitly or accept
  per-stream = global); the effective-config report gains
  cli.admit_max_inflight_per_stream 64->256 for defaults, platform-e2e,
  livefeed-canary and fra-ab-server; if script lines are dropped, the matching
  env lines in the eight Compute family files (for example
  region-server.family:65, fleet-server-1.family:90, wc-ladder.family:75).
- What a client can observe:
  Yes, on a deployment that does not set the variable: the 65th to 256th
  concurrent append on one stream segment changes from 429 stream_overloaded
  with retry 1 (src/application/append.rs:344-353) to admitted. The operator
  page shows the new cap (src/operator.rs:108, src/operator.html:64). Compute
  deployments already run 256 and see nothing.
- For the owner:
  Is 256 the intended default or should the default follow a measurement (the
  only data points are 64 and uncapped)? Should ADMIT_MAX_INFLIGHT get a
  non-zero default in the same decision so the per-stream cap always sits
  below a global cap? Ratify the edge record. Could not determine: memory
  behaviour of 256 concurrent large bodies on one stream without a global cap
  (only the RSS shed line, default 600 MB, bounds it).

#### ABSORB_AGE_SECS

- Proposed: 300 -> 60
- Verification: **holds**
- Reason:
  Every Compute family sets 60. The workload ladder measured 5.18% shed at 300
  s against 1.56% at 60 s and explains it as deferral synchronizing 10k
  streams into thundering-herd gathers.
- What the verifier found:
  Default 300 verified at src/config/cli.rs:256 and AbsorberConfig::default at
  src/history.rs:656; bootstrap always overrides the latter from the CLI
  (src/bootstrap.rs:99), so history.rs:656 only reaches tests. Deployed 60
  verified at deploy-region.sh:180, deploy-fleet.sh:136, mt-tenants.sh:147,
  wc-ladder.sh:199 (parameterised, default 60). Corrections: 'every Compute
  family' is wrong by the entry's own evidence, since
  scripts/bench-fra-ab.sh:86 sets 300. The measurement is weaker than stated:
  bench/WORKLOAD-CERT-PLAN.md:214 changed two variables at once ('ABSORB_AGE
  300 s / PASS 16 MB'), is one run, and the same document says a 5.38% single
  run 'cannot be distinguished from harm at N=1' (:236-238). I could not
  determine whether ABSORB_PASS_BYTES was active on 2026-08-19. No test
  depends on 300: every DST sets threshold_age explicitly (1 ms, or 1,000,000
  s at src/dst/tests/fixture_http.rs:70) or runs far shorter than 60 s. I
  could not break the change itself. Unmeasured: age-triggered absorption five
  times as often on sparse streams means more history-tier commits and
  objects; no cited evidence covers cost. The script line does not go unless
  ABSORB_BYTES=4194304 goes with it; that value already equals the binary
  default (cli.rs:254).
- Must change with it:
  src/config/cli.rs:256 and :596; src/config/tests.rs:427; RUNBOOK.md:108.
  Optional: src/history.rs:656 (stales TLA-016, TLA-018, TLA-019; leaving it
  keeps a second, test-only default of 300). Scripts:
  bench/soak/deploy-region.sh:180, bench/fleet/deploy-fleet.sh:136,
  bench/soak/mt-tenants.sh:147, bench/docker/harness/cluster-deploy.sh:51,
  bench/costab/run-soak.sh:34, run-wide.sh:42, wedge-liveness.sh:47;
  wc-ladder.sh:199 keeps its parameter. Families: fleet-server-1:92,
  fleet-server-n:96, mt-tenants-enforce:39 and :98, mt-tenants-off:65,
  region-server-scale:63, region-server:68, wc-ladder:78, wc-ladder-diet:77.
  scripts/bench-fra-ab.sh:86 and fra-ab-server.family:50 keep 300 explicitly
  or are decided. docs/STAGING.md:160. The effective-config comparison will
  report cli.absorb_age_secs 300 -> 60 for families that do not set it
  (defaults, platform-e2e, livefeed-canary).
- What a client can observe:
  No wire change. Indirectly: the 429 shed rate and append latency under many
  sparse streams, and how soon data reaches the history tier (reads are
  transparent). On Compute, a project that already holds ABSORB_AGE_SECS keeps
  its value after a script line is removed (RUNBOOK.md:481-496) unless it is
  unset explicitly.
- For the owner:
  Performance hold: accept 60 on the strength of 'this is what ran in the
  soaks', or ask for a paired run first? Does the fra-ab benchmark keep 300?

#### ABSORB_GATHER_MAX_BYTES

- Proposed: 32 MiB -> 8 MiB
- Verification: **holds**
- Reason:
  The profile sets 8 MiB and the acceptance verifier checks it exactly. At the
  default body ceiling both values reserve the same amount per gather
  (max(packing x 3, worst frame) = 96.2 MiB), so the difference is batch size,
  not reservation.
- What the verifier found:
  The facts hold; the change has no measurement behind it.
  Verified:
  - Default 32 MiB (src/config/cli.rs:283-288, fixture :601); profile 8388608
  (deploy/profiles/compute-1g.env:26); the acceptance verifier checks it
  exactly (bench/soak/oom-acceptance.sh:61-63; the claimed line 64 is off by
  one).
  - The reservation claim is right. At the 32 MiB body ceiling the worst frame
  is (32 MiB + 64 KiB) x 3 = 100,859,904 bytes (src/history.rs:159, :170,
  :198-200). Packing x 3 is 100,663,296 at 32 MiB and 25,165,824 at 8 MiB, so
  both reserve 100,859,904 (history.rs:249-254; pinned by
  src/config/tests.rs:293-307). Effective gather concurrency stays 1.
  - What differs is the batch: a gather stops staging at the limit
  (src/history/gather.rs:420-435, :571). Real build memory differs too (about
  24 MiB against 96 MiB), although the reservation does not.
  Without the profile, before: up to 32 MiB per gather. After: up to 8 MiB, so
  up to four times as many gathers for the same backlog. I could not determine
  from the code whether absorption then keeps pace with the 5 MB/s per-shard
  limit; falling behind ends in maintenance backpressure. That is what
  production runs today.
  Caveats:
  - A second default for the same value stays at 32 MiB:
  AbsorberConfig::default() (src/history.rs:659, doc at :620-621), which test
  rigs use (src/dst/tests/fixture_http.rs:309-317). Changing it edits
  src/history.rs and stales TLA-016, TLA-018 and TLA-019.
  - The claimed gain 'one profile line goes' fails today: the verifier reports
  'profile missing <key>' for any of its twelve names absent from the profile
  (oom-acceptance.sh:62-79).
- Must change with it:
  - src/config/cli.rs:283-288 (the doc line 'Default = the history DB's
  unflushed cap' becomes false; that cap is 32 MiB at src/history.rs:500),
  :287 default, :601 fixture
  - src/config/tests.rs:432-436 (EXPECTED_CLI_SURFACE '33554432'), :304
  (packing_bytes), :555
  - RUNBOOK.md:109
  - COMPUTE-SPEC.md:95 and docs/COST-CAMPAIGN-2.md:245 (state 32 MiB)
  - src/history.rs:659 and :620-621 if the second default is to match (three
  receipts)
  - bench/soak/oom-acceptance.sh:62-79 before any profile line is removed
  - bench/livefeed-perf/run-one.sh:62 becomes redundant
  Not affected: MEMPROFILE_CERT (src/config/profile.rs:94-114 does not check
  this name); no src/config file is in a formal receipt.
- What a client can observe:
  Not directly. An authorized caller of GET /v1/debug/absorb sees
  config.gatherPackingLimitBytes change from 33554432 to 8388608
  (src/bootstrap.rs:861-863). Indirectly, under sustained load on a deployment
  without the profile, slower absorption could surface as maintenance
  backpressure refusals; not measured.
- For the owner:
  1. Measure 8 MiB against 32 MiB on the local rig first, as for L0_MAX_SSTS?
  This change lowers a batch size, where the L0 change raised headroom.
  2. Should AbsorberConfig::default() follow, at the cost of three receipts,
  or may rigs keep 32 MiB?
  3. May the acceptance verifier compare against the binary's defaults, so
  that profile lines can go? That changes what an acceptance check counts.

#### ABSORB_GLOBAL_GATHERS

- Proposed: 2 -> 1
- Verification: **did not hold**. Instead: As stated (behaviour-neutral, low risk, high confidence): refuted. A default of 1 remains defensible as 'the value production runs', but it is a behaviour change for every deployment without the profile and belongs under the performance hold.
- Reason:
  The profile sets 1 with an explicit 'do not raise' after four slots were
  OOM-killed. At the floored budget effective concurrency is already 1 with 1
  or 2 slots, so the default 2 only becomes real when someone lowers the body
  ceiling and the gather cap, which is the hazardous case.
- What the verifier found:
  Paths relative to the repository root. Default 2
  (src/config/model.rs:138-139,:404) and profile 1
  (deploy/profiles/compute-1g.env:28) are right. The central claim is wrong:
  'at the floored budget effective concurrency is already 1 with 1 or 2
  slots'. That is only the REPORTED figure, computed from the worst-case
  reservation (src/history.rs:249-259; src/config/tests.rs:303-306). Real
  gathers do not reserve the worst case: the worker reserves
  adaptive_gather_est() (src/history/worker.rs:314-332), a decaying maximum of
  observed transients floored at worst_frame_transient_for(4 MiB) = about 12.2
  MiB (src/history.rs:164,:776-794; pinned by
  src/dst/tests/history_gather.rs:150-186). Against the floored 96.2 MiB
  budget two such reservations fit, so with 2 slots two gathers DO run
  concurrently in steady state on a bare binary. The repository says so
  itself: edge record #6 names 'operators running binary defaults' as exposed
  to two concurrent gathers
  (docs/reviews/2026-09-hardening/edge-changes.md:631-635). The comments the
  recommendation relied on are stale relative to the code
  (compute-1g.env:9-15, src/bootstrap.rs:830-833, RUNBOOK.md:820-821). So 2 ->
  1 halves the maximum absorber concurrency across shards for profile-less
  deployments; it is the memory-conservative direction (the profile's 'do not
  raise' follows an OOM kill at 4 slots, compute-1g.env:17-25) and it also
  closes a gap: the certificate does not pin this knob
  (src/config/profile.rs:94-114), so today a deploy that loses the variable
  silently runs 2. Production under the profile is unchanged.
- Must change with it:
  src/config/model.rs:138,:404; src/config/tests.rs:224 and :303 (gather_slots
  2); RUNBOOK.md:131 and the stale text at :820-821;
  deploy/profiles/compute-1g.env:9-15 (stale 'every gather runs one at a time'
  rationale); edge-changes.md:631 states 'the binary default is 2'. If the
  profile line is dropped: bench/soak/oom-acceptance.sh:65 and :77-79 fail
  with 'profile missing ABSORB_GLOBAL_GATHERS';
  bench/livefeed-perf/run-one.sh:64 restates 1. Unaffected because they set 2
  explicitly: src/dst/tests/history_gather.rs:717-719,
  src/history/controller_tests.rs:26, src/runtime.rs:425-436.
  scripts/effective-config/effective_config.py:813-816 (K5) pins the OLD side
  only and stays. The effective-config report gains
  history.absorb_global_gathers 2->1 for defaults and platform-e2e. Do not
  edit the stale comment inside bootstrap::run (src/bootstrap.rs:830-833)
  without the owner: that scope has frozen exception-growth rows
  (docs/quality/exception-growth.json:252-319).
- What a client can observe:
  No wire change. Operators see gatherSlots 2 -> 1 on /v1/debug/absorb
  (src/http.rs:1406, src/bootstrap.rs:865). Indirectly, on a multi-shard
  deployment without the profile absorption may lag more under load, which
  surfaces as the existing maintenance backpressure 503s (MAX_ABSORB_LAG_SECS
  900, unabsorbed-bytes caps, src/config/model.rs:277-282). Compute
  deployments under the profile see nothing.
- For the owner:
  Do you accept halving absorber concurrency for deployments without the
  profile in exchange for the memory-safe default, and should MEMPROFILE_CERT
  pin this knob instead of (or as well as) changing the default? Keep the
  profile line as an explicit declaration even if redundant (the OOM
  acceptance check reads it)? Could not determine: the absorb-lag cost of 1
  slot on a multi-shard instance; no measurement in the tree compares 1 and 2
  slots at the adaptive reservation.

#### SHARED_CACHE_BYTES

- Proposed: 192 MiB -> 128 MiB
- Verification: **holds**
- Reason:
  RUNBOOK documents that the envelope with 192 MiB left no headroom under the
  ~750 MB kill line (4 OOM kills in one 18-minute run) and names 128 MiB as
  the envelope that gates green. The 192 default is itself a 1 GiB-class value
  that documentation says is wrong.
- What the verifier found:
  The facts hold; the stated gain is overstated.
  Verified:
  - Default 192 MiB (src/config/cli.rs:519-524, fixture :642); profile
  134217728 (deploy/profiles/compute-1g.env:31).
  - RUNBOOK.md:160-173 says the envelope with 192 MiB left no headroom under
  the roughly 750 MB kill line (4 OOM kills in one 18-minute run) and names
  128 MiB as the envelope that gates green.
  - bench/soak/wc-ladder.sh:85-87 records 137 ms to 7.3 s append p50 for the
  full cache diet (shared 64 MiB together with postings and history 16 MiB,
  wc-ladder.sh:66-72), not for the shared cache alone.
  - The only reader is the one shared block cache (src/bootstrap.rs:385-391).
  Test rigs do not read this field.
  Without the profile, before: 192 MiB cache. After: 128 MiB, so 64 MiB less
  fixed memory and more cache misses at large working sets. No measurement of
  the latency effect at 128 against 192 was found.
  Where the claim overreaches: 'a server without the profile fits the instance
  class' is not achieved by this change. Without the profile these still
  differ from the certified posture:
  - shed line 600 MB against 500 (cli.rs:484; profile :37)
  - bulk gate off (src/config/model.rs:366; profile :82)
  - compactor 4/4/4 and 2 MiB read-ahead against 1/1/1 and 1 MiB
  (model.rs:376-379; profile :88, :98-100)
  - SlateDB runtime threads 2 against 4 (model.rs:381; profile :29)
  - subscription cap 10,000 against 1,200 (cli.rs:494; profile :68)
  - feed budgets 16 MiB against 64 MiB (model.rs:424; profile :51)
  The profile line cannot go today: the acceptance verifier requires the name
  in the profile (bench/soak/oom-acceptance.sh:67, :78-79), and deploy scripts
  can deploy an older binary by tag (bench/soak/deploy-region.sh:163), which
  would silently run 192 MiB.
- Must change with it:
  - src/config/cli.rs:523 default, :642 fixture
  - src/config/tests.rs:497 (EXPECTED_CLI_SURFACE '201326592' becomes
  '134217728')
  - RUNBOOK.md:155 (default column) and :658
  - bench/soak/oom-acceptance.sh:62-79 before the profile line is removed
  - bench/livefeed-perf/run-one.sh:67 becomes redundant
  - docs/reviews/2026-09-hardening/effective-config-diff.md: the next run
  shows a changed field for the families without the profile
  Not affected: MEMPROFILE_CERT (src/config/profile.rs:94-114 does not check
  it); formal receipts; bench scripts that set 64 MiB themselves
  (bench/costab/*.sh, bench/sse-probes/*.sh).
- What a client can observe:
  Not directly. An authorized caller of GET /v1/debug/absorb sees
  config.sharedCacheBytes change from 201326592 to 134217728
  (src/bootstrap.rs:823, :868). Read and append latency may change where the
  working set exceeds 128 MiB; not measured.
- For the owner:
  1. Is the binary default meant to be the 1 GiB class value, with larger
  classes setting more?
  2. If the aim is that a server without the profile is safe on 1 GiB, the
  other memory defaults listed above would have to move too (shed line, bulk
  gate, compactor worker profile, runtime threads, subscription cap). Should
  those be proposed as one set?
  3. Measure 128 against 192 on the local rig before changing?

#### ADMIT_RSS_SHED_MB

- Proposed: 600 -> 500
- Verification: **did not hold**. Instead: change-default 600 -> 500
- Reason:
  The profile and the OOM-review posture set 500; the code comment says the
  line must sit well below the ~750 MB kill line. RUNBOOK gives this setting
  four other values (0 with pilot 800, 550, 600, 800), so the documentation
  cannot all be right.
- What the verifier found:
  The facts are right: default 600 (src/config/cli.rs:484-485), profile 500
  (deploy/profiles/compute-1g.env:37), the doc comment sits on the wrong flag
  (cli.rs:471-480), and RUNBOOK gives 550 (:175), 0/800 (:250), 800 (:294),
  600 (:581) and 500 (:824).
  The change is unsafe on its own. The shed line is one part of a memory
  posture, and the binary's other defaults are not the profile's.
  SHARED_CACHE_BYTES defaults to 192 MiB (cli.rs:523) against the profile's
  128 MiB (compute-1g.env:31).
  With binary defaults the fixed budgets are 192 + 32 + 64 + 16 + 96 = 400
  MiB. The boot check warns when fixed + 100 > line
  (src/bootstrap.rs:877-883), so 500 sits exactly on the boundary with no
  margin. The ladder measured 456 MB steady RSS with the profile's smaller
  caches (bench/soak/wc-ladder.sh:60-62; bench/WORKLOAD-CERT-PLAN.md:211). 64
  MiB more cache puts a server without the profile at or above a 500 line, and
  the shed counts RSS plus reserved absorber bytes (src/admission.rs:264-272).
  That server would refuse writes where it has 100 MB of room today.
  It holds only as a set with SHARED_CACHE_BYTES 192 -> 128 MiB.
  Second trap: wc-ladder.sh:203 passes 600 before the profile, and only the
  profile line overrides it
  (scripts/effective-config/families/wc-ladder.family:6). Dropping the profile
  line silently moves that campaign to 600.
- Must change with it:
  - src/config/cli.rs:471-485 (move the doc comment to the right field, which
  changes --help) and the fixture :637
  - src/config/tests.rs:488 (EXPECTED_CLI_SURFACE)
  - src/config/cli.rs:523 with the fixture :642 and tests.rs:497, if
  SHARED_CACHE_BYTES moves with it
  - RUNBOOK.md:175, :250, :294, :581-583, :590, :824
  - bench/soak/wc-ladder.sh:203
  - scripts/effective-config/families/wc-ladder.family:6/:83 and
  wc-ladder-diet.family:7/:82
  - bench/soak/oom-acceptance.sh:74 requires the key in the profile
  - docs/STAGING.md:149-151
  - an edge-changes.md record
  Local bench scripts set their own value (bench/costab, bench/sse-probes:
  1400 or 600) and are unaffected.
- What a client can observe:
  Yes, on a server that sets nothing. Appends answer 429 'overloaded' with
  Retry-After: 2 after a 25 ms tarpit (src/application/append.rs:129-141) once
  RSS plus reserved absorber bytes passes 500 MB instead of 600 MB.
  /operator/data.json 'rss_shed_mb' and /v1/debug/absorb 'shedLineMb' show
  500.
- For the owner:
  Is the binary default the 1 GiB Compute posture as a set (shed 500 with
  shared cache 128 MiB), or does the shed line stay a per-class value in the
  profile? Either way, an edge record and the choice of the single number
  RUNBOOK states.

#### SLATEDB_RT_THREADS

- Proposed: 2 -> 4
- Verification: **holds**
- Reason:
  The profile sets 4 for every deployed family and nothing about the value is
  specific to memory. It is what runs.
- What the verifier found:
  Paths relative to the repository root. Verified: default 2
  (src/config/model.rs:88-90,:381; pinned src/config/tests.rs:218;
  RUNBOOK.md:133); profile 4 (deploy/profiles/compute-1g.env:29); every
  Compute family includes the profile. bench/soak/wc-ladder.sh:207 passes
  SLATEDB_RT_THREADS=${WC_SLATE_RT:-2} BEFORE the profile flags (:215), so the
  profile's 4 wins and that line and its WC_SLATE_RT instrument are dead
  (confirmed by scripts/effective-config/families/wc-ladder.family:6 and
  docs/reviews/2026-09-hardening/effective-config-diff.md:319-320). The thin
  evidence is as described: the one run at 4 measured 5.38% shed against
  0.85-1.56% and 'cannot be distinguished from harm at N=1'
  (bench/WORKLOAD-CERT-PLAN.md:233-240); the only positive rationale I found
  is rollup co-location (docs/OBSERVABILITY-BILLING-STATUS.md:114). I could
  not break the change, but: (1) the profile calls itself 'MEMORY knobs only'
  (compute-1g.env:2) and lists this knob; the threads run SST builds of 4-16
  MB inline (src/config/profile.rs:8-16), so more threads means more
  concurrent build buffers. Production already runs 4, so the default would be
  no less safe than production. (2) The lazy fallback literal 2
  (src/bootstrap/process_executor.rs:53) is not strictly required to move: the
  binary calls init_slatedb_runtime_threads before any store opens
  (src/bootstrap.rs:213), and the only test that calls bootstrap::run fails
  before that line (src/bootstrap/tests.rs:47-62). If the literal does move,
  every direct-storage test and DST runs on a 4-thread SlateDB runtime, and
  AGENTS.md calls those tests real-time and load-sensitive (CI has 4 vCPUs).
  (3) bench/costab/*.sh and bench/fleet/local-fanout.sh:61 set 2 explicitly
  and keep it.
- Must change with it:
  src/config/model.rs:88 and :381; src/config/tests.rs:218; RUNBOOK.md:133 and
  :822; src/bootstrap/process_executor.rs:49-54 (literal and its comment;
  mutation owner 'process_executor', scripts/quality/mutation_owners.py:162,
  so CI selects mutants for the file); bench/soak/wc-ladder.sh:207 (dead line)
  with scripts/effective-config/families/wc-ladder.family:6,:89 and
  wc-ladder-diet.family:7,:88; if the profile line is dropped,
  bench/soak/oom-acceptance.sh:66 and :77-79 ('profile missing
  SLATEDB_RT_THREADS') and bench/livefeed-perf/run-one.sh:65; the
  effective-config report gains engine.slatedb_rt_threads 2->4 for defaults
  and platform-e2e.
- What a client can observe:
  No wire change. Operators see slatedbRuntimeThreads on /v1/debug/absorb
  (src/bootstrap.rs:867) and in the startup 'memory budget' line
  (src/bootstrap.rs:840-856). Any latency effect is unmeasured.
- For the owner:
  Is 4 a decision you want to make on N=1 evidence that could not tell it from
  harm, or should the default wait for a measurement? Should tests and DSTs
  follow to 4 threads (process_executor.rs:53) or stay at 2? Could not
  determine: the vCPU count the 1 GiB class really has (one document assumes 1
  vCPU, bench/aws-comparison-plan.md H8), and any measured benefit of 4 over
  2.

#### SSE_FEED_TOTAL_BYTES

- Proposed: 16 MiB -> 64 MiB
- Verification: **did not hold**. Instead: Amend before it goes to the owner: change the pair, total 64 MiB and project backstop 32 MiB. Changing the total alone gives a posture nobody runs.
- Reason:
  The profile sets 64 MiB after the round-12 study found the 16/4 MiB posture
  saturating; validation already allows 64 MiB in both arms. The refusal text
  still says the class certifies at 16 MiB.
- What the verifier found:
  Defaults verified: 16 MiB at src/config/model.rs:424, fallback and warning
  at src/config/load.rs:140-149; profile 64 MiB at compute-1g.env:51; both
  validation arms 64 MiB at src/config/validation.rs:271 and :286-287; stale
  refusal text at validation.rs:362-366. What breaks the entry: the project
  backstop defaults to a quarter of the total when unset
  (src/sse/feed.rs:305-311), and the profile sets it separately to 32 MiB
  (compute-1g.env:52). Raising only the total gives a deployment without the
  profile 64/16, which docs/PERF-LIVEFEED.md:96 measured at 16,742 lag/resume
  cycles and 333 ms; production runs 64/32, and the documented contract names
  32 MiB per project (docs/LIVE-FEED.md:205-225). So the profile's second line
  would have to stay and the default would still not be the production value.
  The stated risk is out of date in the entry's favour:
  PERF-LIVEFEED.md:108-111 ('current candidate') was followed by six field
  legs and the round-12 decision to keep 64/32 (PERF-LIVEFEED.md:266-300,
  LIVE-FEED.md:207-210, rc.3 anchored at :302-307). That certification was
  under the profile's caches (128 MiB shared, shed 500). Binary defaults carry
  192 MiB shared cache and shed 600 (cli.rs:523, :484); nothing measured 64
  MiB of retention in that posture. Every DST HTTP rig takes
  SseConfig::default (src/dst/tests/fixture_http.rs:448), so rigs move too;
  the two retention DSTs set their cap explicitly (admission_memory.rs:71,
  persistence_faults.rs:368) and the helper hard-codes the quarter
  (src/sse/feed/test_support.rs:21-24).
- Must change with it:
  src/config/model.rs:162 and :424; src/config/load.rs:140-149;
  src/sse/feed.rs:302-311 (project default) and :272 (doc);
  src/sse/budget.rs:15-17; src/sse/feed/test_support.rs:18-24 and the comment
  at src/dst/tests/persistence_faults.rs:368; src/config/tests.rs:233-234;
  src/config/validation.rs:362-366 and the comments at :266-289;
  deploy/profiles/compute-1g.env:38-52 (both lines and the comment that still
  says 16 MiB); docs/LIVE-FEED.md:93-98; docs/PERF-LIVEFEED.md:81;
  bench/soak/wc-ladder.sh:79-84 (comment);
  bench/livefeed-perf/run-one.sh:59-60. RC manifests list SSE_FEED_TOTAL lines
  as profile pins and fail if the pin is not in the tagged profile
  (scripts/verify-rc-evidence.py:117-119; self-test :218), so future manifests
  must drop it. No receipts stale. effective-config: sse.feed_total_bytes
  changes for the families without the profile.
- What a client can observe:
  Yes, on a deployment without the profile: how much a shared feed retains
  decides how often a slow subscriber gets a nonterminal EOF and reconnects
  from its cursor, and the delivery latency under retention pressure
  (LIVE-FEED.md:215-225). Lossless either way. Also the 'feed retention
  budget' boot line (src/bootstrap.rs:196-200) and the logged summary.
- For the owner:
  Memory hold and an edge decision: make 64 MiB total and 32 MiB per project
  the binary defaults, although the 64 MiB certification was measured only
  under the profile's cache and shed posture? Should the project default be a
  fixed 32 MiB or half of the total?

#### SSE_MAX_CONNECTIONS

- Proposed: 10000 -> 1200
- Verification: **holds**
- Reason:
  The profile says the 10k default is a certification rung, not a measured
  safe capacity, and PERF-LIVEFEED says the 1200 pin stands. The reason given
  is the platform edge, not memory.
- What the verifier found:
  Holds as a safety change for deployments without the profile. It is an owner
  decision, and the stated gain (one profile line goes) should not be taken.
  Paths under .
  Verified: default 10,000 (src/config/cli.rs:494); the profile sets 1200 and
  calls 10k a certification rung (deploy/profiles/compute-1g.env:61-68);
  docs/PERF-LIVEFEED.md:194-204 and :258-259 say the 1200 pin stands; the
  reason given is the platform edge and descriptors, not memory. Nothing in
  the tree relies on the default being 10,000: tests pass the cap as an
  argument (src/config/validation_tests.rs:298, :357-365, :375-389) or build
  the knobs directly (src/admission.rs:496-498, :613-615); SSE probes set 0
  (bench/sse-probes/*.sh); bench/livefeed-perf/run-one.sh:61 and
  run-capacity.sh:70, :92, :107 set it; wc-ladder sets 2000
  (bench/soak/wc-ladder.sh:81); the canary runs the profile with 1,000
  subscribers. The reader change is confined to cli.rs.
  WHAT THE RECOMMENDATION MISSED. (1) SPEC.md:361-368 lists 10,000 direct
  live-tail connections under limits that are stated and enforced, and
  SPEC.md:475 retains 10k connections per instance. The default is what makes
  that true without the profile. (2) 1200 is provisional and tier-specific:
  the profile calls it the first canary target plus headroom, to be raised
  after an in-VPC ladder (compute-1g.env:66-67). (3) Nothing certifies the
  cap: MEMPROFILE_CERT checks seven compaction and store values only
  (src/config/profile.rs:94-113). If the profile line is dropped, an older
  binary deployed with the newer profile runs 10,000 silently. Keep the line.
  WITHOUT THE PROFILE, before: 10,000, or 3,072 under the release posture with
  a 4,096 descriptor ceiling (validation_tests.rs:363-365). After: 1,200.
- Must change with it:
  Paths under .
  - src/config/cli.rs:487-495 (doc comment explains 10k), :638 (fixture).
  - src/config/tests.rs:489.
  - SPEC.md:368 and :475.
  - docs/reviews/2026-09-hardening/edge-changes.md: a new record;
  docs/refactor/WIRE-MATRIX.md:137 names the refusal (no number there).
  - bench/WORKLOAD-CERT-PLAN.md:185-186 (10,000 is the default rung).
  - deploy/profiles/compute-1g.env:61-68: reword the comment, keep line 68.
  - scripts/effective-config/families/wc-ladder.family:4 and
  wc-ladder-diet.family:5 (comments).
  - Not affected: scripts/effective-config/test_effective_config.py:91-98
  writes its own profile; bench/soak/oom-acceptance.sh:62-75 does not check
  this name; src/config/validation_tests.rs pass explicit values.
  - RUNBOOK.md has no row for this name.
  - No formal receipt or mutation-critical file is touched.
- What a client can observe:
  Yes. On an instance that does not set the name, the 1,201st concurrent SSE
  subscription gets the typed 503 subscription_capacity with retry-after 5
  (src/http.rs:2722; docs/refactor/WIRE-MATRIX.md:137) where today the
  10,001st does. /v1/debug/load reports the new cap (src/http.rs:883-885).
  Deployments using the profile see no change.
- For the owner:
  Ratify as an edge change and amend SPEC C9? Is 1200 a product default for
  every instance class, or only the Compute edge number? Add the cap to
  MEMPROFILE_CERT before any profile line is dropped?

#### MAX_RECORD_PAYLOAD_BYTES

- Proposed: unset (unlimited) -> 131072
- Verification: **holds**
- Reason:
  The profile and every release-posture rig set 131072, and the release
  posture refuses to boot without a ceiling whose worst frame fits the feed
  ring. The default is a value the release shape rejects.
- What the verifier found:
  The facts hold. It is a product decision because clients can observe it.
  Verified:
  - Default is unset, meaning unlimited (src/config/cli.rs:387-394, fixture
  :619; src/bootstrap.rs:576 maps unset to 0;
  src/application/creation.rs:339-344 treats 0 as no ceiling).
  - The profile sets 131072 (deploy/profiles/compute-1g.env:53), as do
  scripts/platform-e2e.mjs:104, bench/fleet/livefeed-cert.mjs:79 and
  bench/soak/wc-ladder.sh:204.
  - The release posture refuses an unset or zero ceiling and any ceiling whose
  worst frame exceeds the ring (src/config/validation.rs:128-168). 131072 is
  one eighth of the default 1 MiB ring, which the unit test accepts
  (src/config/validation_tests.rs:177).
  Without the profile, before: any record up to the request limits is
  accepted. After: a record over 131,072 bytes is refused with 413
  record_too_large. On a non-JSON stream the whole body is one record
  (src/application/append/content.rs:110-112), so the ceiling there caps the
  request body.
  Checks I made to break it:
  - The upstream conformance suite still passes by reading: its largest stored
  body is 100 KiB (server-conformance-tests dist/src-wMgS3XWd.js:2021) and its
  10 MiB test accepts 413 (:2060-2071). I did not run it.
  - Test rigs are unaffected: they fix the ceiling at 0 themselves
  (src/dst/tests/fixture_http.rs:512). That also means rigs would not run the
  production default.
  Two tests pin today's refusal and would fail:
  - validation_rejects_release_posture_without_record_ceiling
  (src/config/validation_tests.rs:635-650)
  - the platform end-to-end check 'release posture refuses to boot without
  MAX_RECORD_PAYLOAD_BYTES' (scripts/platform-e2e.mjs:143-153)
  If the field becomes a plain number, src/bootstrap.rs:576 changes inside
  bootstrap::run, which the approved rows freeze
  (docs/quality/exception-growth.json:251-330). Keeping the Option type with a
  clap default avoids that, but leaves the unset branch in
  validation.rs:136-142 unreachable.
- Must change with it:
  - src/config/cli.rs:387-394 (doc and attribute), :619 fixture
  - src/config/tests.rs:462 (EXPECTED_CLI_SURFACE row)
  - src/config/validation_tests.rs:635-650 (rewrite, for example to refuse an
  explicit 0), :161-178 if the signature changes
  - src/config/validation.rs:128-142
  - scripts/platform-e2e.mjs:143-153 (the negative check)
  - src/bootstrap.rs:576 only if the type changes (owner updates the frozen
  rows; receipt TLA-011)
  - docs/reviews/2026-09-hardening/edge-changes.md (new record) and
  docs/refactor/WIRE-MATRIX.md:33, :42, :108
  - docs/seal-transitions.md:27 and docs/OPS-RELEASE.md:188 name the ceiling
  - RUNBOOK.md has no row for this name
  - scripts/effective-config/families/platform-e2e.family:21 and
  wc-ladder*.family lines become redundant
- What a client can observe:
  Yes, on deployments that run without a ceiling today: 413 with code
  record_too_large for any record over 131,072 bytes, on append
  (src/application/append/content.rs:116-122), create with a body
  (src/application/creation/initialization.rs:76-84), fork materialization
  (src/application/creation/fork.rs:306-313) and the product seal's final
  record (src/product.rs:1612-1617). Status mapping: src/http.rs:6-17.
  Deployments that include the profile see no change.
- For the owner:
  1. Is 128 KiB the product's record limit for every deployment, or only the 1
  GiB class value that follows from the 1 MiB feed ring?
  2. Should unlimited remain reachable outside the release posture through an
  explicit 0?
  3. Would deriving the ceiling from the ring (one eighth) be preferable to a
  second number?
  4. Should test rigs run the production ceiling instead of 0?

#### SLATE_S3_REGION

- Proposed: us-east-1 -> auto
- Verification: **holds**
- Reason:
  Every Compute deployment and every guide sets auto for Tigris.
- What the verifier found:
  Verified: default us-east-1 (src/config/cli.rs:30); every Compute server
  family sets auto (nine family files, e.g. bench/soak/deploy-region.sh:167,
  bench/fleet/deploy-fleet.sh:111, scripts/bench-fra-ab.sh:80 (not :81),
  bench/soak/mt-tenants.sh:137, bench/soak/wc-ladder.sh:189); the guides set
  auto (RUNBOOK.md:79, docs/GUIDE-COMPOSER.md:206,
  docs/PROVIDER-CONTRACT.md:66). The region only feeds the signing scope
  through AmazonS3Builder::with_region (src/bootstrap.rs:31). The rigs that
  rely on the default (platform-e2e and livefeed-canary set no
  SLATE_S3_REGION) run s3lite, which ignores the Authorization header
  (src/bin/s3lite.rs:6), so they cannot break. The other binaries already
  default to auto (src/bin/pilot.rs:132, src/bin/verify.rs:32-33), and so does
  the deploy downloader, which falls back to SLATE_S3_REGION and then 'auto'
  (deploy/app-server/downloader.ts:13), so removing the script line does not
  change the downloader. Could not determine from the repository: whether
  Tigris accepts us-east-1 at all (whether today's default works against the
  production store), and what a non-Tigris store does with 'auto'.
  OPERATIONS.md:12-14 calls the S3 API the portability boundary and names a
  backup provider; the MinIO rigs mentioned in docs/CAPACITY-R27.md:26 and
  docs/CHAOS-CAMPAIGN.md:491 do not record their region setting.
- Must change with it:
  src/config/cli.rs:30 and :568 (fixture); src/config/tests.rs:387;
  RUNBOOK.md:79; the script lines above and
  bench/docker/harness/cluster-deploy.sh:39; the nine family files. Local rigs
  that set 'local' explicitly are unaffected (bench/costab/*.sh,
  bench/fleet/local-fanout.sh:53, bench/sse-probes/*.sh,
  bench/docker/compose.yml:18).
- What a client can observe:
  No. It is an address setting for the object store; a wrong value shows as
  store authentication failures at boot, not as a client-visible change.
- For the owner:
  Is Tigris the only store the binary default has to fit, or must a bare
  binary also work against a region-scoped S3 store such as the backup
  provider? If the latter, keep us-east-1 or make the region required like the
  endpoint.


### Settings that follow another setting

#### TRIM_PER_OP

- Proposed: 8192, separate setting -> equal to TRIM_GLOBAL_BUDGET (65536), no setting
- Verification: **holds**
- Reason:
  Every Compute family sets 65536, which equals the default global budget. The
  effective bound is min(remaining global budget, per-op cap), so at the
  deployed value the per-op cap never binds and one number does the work.
- What the verifier found:
  The arithmetic holds: allowed = min(remaining group budget, per-op cap) at
  src/shard/transaction/maintenance.rs:324 and :340, and the group budget
  starts at trim_global_budget (src/shard/transaction/mod.rs:88), so with
  per-op equal to the budget the per-op cap never binds. Defaults are 8192 and
  65536 (src/config/cli.rs:235,244). Eight server families set 65536; fra-ab
  sets 8192 (scripts/bench-fra-ab.sh:86), so 'every family' is not exact, as
  the risk note admits. The worst-case batch is unchanged because the global
  budget already bounds it. What breaks the FULL removal: (1) the wiring at
  src/bootstrap.rs:417 and :515 is inside bootstrap::run
  (bootstrap.rs:138-914), whose exception scopes have owner-approved growth
  rows that are frozen exactly (docs/quality/exception-growth.json:252-322;
  AGENTS.md section 5), so any edit there, shrinking included, fails as a
  stale row until the owner updates it; (2) src/shard.rs (no line headroom),
  maintenance.rs and bootstrap.rs together stale KANI-046, KANI-047, TLA-002,
  TLA-003, TLA-005, TLA-006, TLA-011, TLA-016; (3) tests use the per-op cap on
  its own: src/shard/maintenance_tests.rs:396-400 (per-op 1 to build trim
  debt) and src/dst/tests/history_recovery.rs:132-137. The cli-only step
  touches none of these. After it, ShardConfig::default() still says 8192
  (src/shard.rs:970), so DST engines and the binary differ in this value.
- Must change with it:
  cli-only step: src/config/cli.rs:229-236 (default and help), :592 (fixture);
  src/config/tests.rs:423; RUNBOOK.md:107; COMPUTE-SPEC.md:98 ('default 8k').
  Script lines that become redundant: bench/soak/deploy-region.sh:181,
  bench/fleet/deploy-fleet.sh:137, bench/soak/mt-tenants.sh:148,
  bench/soak/wc-ladder.sh:200, docs/STAGING.md:161, plus the ten family files.
  Full removal in addition: src/shard.rs:879,938-941,970;
  src/bootstrap.rs:417,515; maintenance.rs:324,340; the two tests above; every
  script that names TRIM_PER_OP (also bench/costab/*.sh,
  bench/fleet/local-fanout.sh:64, bench/docker/harness/cluster-deploy.sh:51,
  scripts/bench-fra-ab.sh:86); mutation owner transaction_maintenance
  (scripts/quality/mutation_owners.py:119) selects CI mutants.
- What a client can observe:
  No. It changes how many hot-log records one stream may trim per commit
  group; storage reclamation pace only. With the knob removed, --trim-per-op
  on argv would refuse boot and the env name would be silently unread.
- For the owner:
  Accept the cli default of 65536 now and defer the field removal until
  bootstrap::run's growth rows are next updated? With per-op equal to the
  budget the first stream in a group can take the whole budget and the others
  wait for a later tick; that is already the posture eight families run.
  Should fra-ab keep 8192?

#### ABSORB_GLOBAL_BUDGET_BYTES

- Proposed: 64 MiB nominal, floored at runtime to 100,859,904 -> (MAX_REQUEST_BODY_BYTES + 64 KiB) x 3 when unset; the profile line goes
- Verification: **holds**
- Reason:
  The runtime floors the budget at one worst-frame build, and the profile
  value is exactly that floor at the 32 MiB pin. The nominal 64 MiB default
  can never take effect unless the body ceiling is lowered below about 21 MiB.
- What the verifier found:
  Verified. Capacity is max(configured, worst_frame_transient_for(body_limit))
  (src/history.rs:198-200, :231-234), and (33,554,432 + 65,536) x 3 =
  100,859,904, exactly the profile value (deploy/profiles/compute-1g.env:27).
  A test already pins that the 64 MiB default runs at 100,859,904
  (src/config/tests.rs:287-307). 64 MiB would only take effect with a body
  ceiling below about 21.27 MiB, and no script sets MAX_REQUEST_BODY_BYTES.
  So with or without the profile, the budget is the same before and after. The
  one case that changes: a deployment that lowers the body ceiling below about
  21.3 MiB and leaves the budget unset gets a smaller budget and a smaller
  packing limit (capacity / 3, history.rs:243).
  Cheapest implementation: make the default 0. The existing floor then
  produces the derived value with no edit to src/history.rs, which has zero
  line headroom and feeds TLA-016, TLA-018 and TLA-019
  (verification/manifest.json). src/runtime.rs:467 already uses 0 this way.
  DST rigs are unchanged (HistoryResources::new, history.rs:222-224).
- Must change with it:
  - src/config/model.rs:134-137 (doc) and :400-403
  - src/config/tests.rs:223 (pins 64 MiB) and :287-307 (comment and assert
  message)
  - RUNBOOK.md:130 (default column) and :815-819
  - bench/soak/oom-acceptance.sh:64 requires the key in the profile (the
  recommendation cites :65); it fails with 'profile missing' (:78-80) if the
  line goes
  - deploy/profiles/compute-1g.env:9-15 (comment) and :27
  - src/config/summary.rs:38 would print the raw 0 or None beside the 96 MiB
  in the 'memory budget' line (src/bootstrap.rs:843-858)
  - scripts/effective-config: the K5 pin (effective_config.py:812-817) reads
  the old side only and is unaffected; re-run the report
  bench/livefeed-perf/run-one.sh:63 sets the value explicitly and can stay. No
  receipt goes stale if src/history.rs is untouched.
- What a client can observe:
  No. The resolved absorbBudgetBytes on /v1/debug/absorb stays 100859904. Only
  the raw value in the boot summary changes.
- For the owner:
  Keep the variable as an override for a larger class (recommended), or delete
  it? May oom-acceptance.sh stop requiring the key in the profile? That
  changes what that gate checks.

#### SSE_FEED_PROJECT_BYTES

- Proposed: unset = SSE_FEED_TOTAL_BYTES / 4 -> unset = SSE_FEED_TOTAL_BYTES / 2
- Verification: **holds**
- Reason:
  The profile sets exactly half of the total. The value is already derived
  when unset; only the ratio differs from what is deployed.
- What the verifier found:
  Verified: unset derives total/4 (src/sse/feed.rs:310, fallback :348); the
  profile sets 64 MiB and 32 MiB (deploy/profiles/compute-1g.env:51-52). The
  recommendation's own risk note is out of date in its favour:
  docs/LIVE-FEED.md:205-240 records the round-12 decision (2026-08-31) after
  the six field legs, keeps 64/32, and states the contract as 'one project can
  consume at most half the process retention allowance'. So half is the
  documented contract. Limits I found: (1) the gain is one profile line, not
  one setting. The default total stays 16 MiB (src/config/model.rs:424), and
  64 MiB is release-safe only under MEMPROFILE_CERT=compute-1g
  (src/config/validation.rs:277-288, 359-369), so SSE_FEED_TOTAL_BYTES stays
  in the profile. (2) A deployment without the profile moves from 16/4 to 16/8
  MiB, neither of which is the documented 32 MiB allowance. (3) Isolation
  weakens by design: two projects can fill the cell instead of four (comment
  at feed.rs:339-343). (4) The alert thresholds are absolute
  (LIVE-FEED.md:241-248: warning at 24 MiB). (5) wc-ladder rungs that pass
  only WC_FEED_TOTAL would get half of it if the profile line goes
  (bench/soak/wc-ladder.sh:80-84). Release validation still passes (project
  strictly below total; validation.rs:175-205).
- Must change with it:
  src/sse/feed.rs:302-303 (doc), :310, :347-348. The file is 1,165 lines with
  no headroom, so the edit must be line-neutral; src/sse is a critical prefix
  with owner sse_feed (scripts/quality/mutation_owners.py:181), so CI runs
  mutants. src/config/model.rs:168-171 (doc 'global/4');
  src/sse/feed/test_support.rs:19-23 hard-codes max/4 for the DST rigs
  (src/dst/tests/admission_memory.rs:71,
  src/dst/tests/persistence_faults.rs:368 'project cap 16 KiB');
  deploy/profiles/compute-1g.env:39-52; docs/LIVE-FEED.md:207-209,230-231;
  docs/CONTROL-PLANE-INTEGRATION.md:133-134; docs/PERF-LIVEFEED.md:81;
  bench/livefeed-perf/run-one.sh:60; an edge record and
  docs/refactor/WIRE-MATRIX.md. Could not determine: whether changing the
  literal at feed.rs:348, inside `#[expect(clippy::unwrap_used)] impl
  FeedMemoryBudget` (feed.rs:334-338), alters that exception contract's syntax
  facts; only the gate can say.
- What a client can observe:
  Yes, wherever the value is derived (no profile: the defaults family,
  platform-e2e, a STAGING or Composer cell). A project holding between 4 and 8
  MiB of retained feed data stops being cut: fewer nonterminal EOF /
  lag-disconnect and resume cycles on shared feeds. No record is lost in
  either case. Deployments that source the profile see no change.
- For the owner:
  Is 'half of the cell' the contract for every cell size, or is 32 MiB an
  absolute allowance that only happens to be half of 64? The Control Plane
  note bounds the future per-project quota by 'the profile's 32 MiB maximum',
  an absolute number. If absolute, keep the explicit profile line and leave
  the quarter default alone.

#### L0_MAX_SSTS_PER_KEY

- Proposed: 0 (= follow L0_MAX_SSTS), settable -> always L0_MAX_SSTS
- Verification: **holds**
- Reason:
  The documented purpose is that the per-key cap must follow the L0 cap for
  ordered streams. Only one script sets it, to 0.
- What the verifier found:
  I could not break this one.
  Verified:
  - Default 0 means follow L0_MAX_SSTS (src/config/cli.rs:139-145, fixture
  :583; src/config/validation.rs:42-46).
  - One script sets it, to 0: scripts/bench-fra-ab.sh:84, mirrored by
  scripts/effective-config/families/fra-ab-server.family:45. No script passes
  the flag on argv.
  - No test sets a nonzero value. The history and billing engines use their
  own constants (src/history.rs:508; src/billing.rs:1510) and are unaffected.
  - The documents describe exactly the derived behaviour (RUNBOOK.md:102,
  :665; SPEC.md:505-507; COMPUTE-SPEC.md:85).
  Before and after, with or without the profile: identical engine settings,
  since every known deployment runs 0.
  Code that would go: the clap field and the conditional at
  validation.rs:42-46 (becomes l0_max_ssts_per_key: args.l0_max_ssts).
  validation.rs is 983 lines and shrinks; shard_settings carries no exception;
  no src/config file is in a formal receipt.
  Gaps:
  - No test pins today that the per-key cap follows the L0 cap. The change
  should add one (red first).
  - A leftover L0_MAX_SSTS_PER_KEY variable in a project is ignored silently.
  A leftover --l0-max-ssts-per-key on argv would make clap refuse to start; I
  found no such use in the tree.
- Must change with it:
  - src/config/cli.rs:139-145, :583
  - src/config/tests.rs:406 (EXPECTED_CLI_SURFACE row removed)
  - src/config/validation.rs:42-46
  - src/config/model.rs:28 (the '85 flags' count)
  - RUNBOOK.md:102 (row) and :665 (troubleshooting row names the knob)
  - SPEC.md:505-507; COMPUTE-SPEC.md:85
  - scripts/bench-fra-ab.sh:84 and
  scripts/effective-config/families/fra-ab-server.family:45 (the family must
  mirror the script)
  - scripts/effective-config/effective_config.py:74 (leaf count) and
  rename-map.json ('removed': cli.l0_max_ssts_per_key); effective_config.py:94
  and test_effective_config.py:121 use the name only as a string in the
  secret-name rule and can stay
  Not affected: CliArgs::deterministic beyond the one field; the profile;
  certification; formal receipts.
- What a client can observe:
  No client effect. Operators: the flag --l0-max-ssts-per-key would be refused
  by the argument parser; the environment variable would be ignored without a
  message.
- For the owner:
  Should a leftover variable be ignored silently, or announced once at startup
  the way the three ignored absorber options are (src/config/cli.rs:655-664)?
  The second keeps one more name alive.

#### FORK_DEBT_SWEEP_SECS

- Proposed: 300, separate setting -> OUTBOX_SWEEP_SECS
- Verification: **did not hold**. Instead: derive-from-another (OUTBOX_SWEEP_SECS)
- Reason:
  Its own documentation says it has the same cadence as OUTBOX_SWEEP_SECS, the
  neighbouring walk; nothing sets it.
- What the verifier found:
  Default 300 and 'nothing sets it' are verified (src/config/cli.rs:347-352;
  src/config/tests.rs:456). The derivation is the wrong coupling:
  - The reconciler is a correctness repair. It releases source references that
  deleted forks still owe (TLA-019-F4,
  src/application/creation/reconcile.rs:1-30). OUTBOX_SWEEP_SECS is a billing
  cadence (src/billing/telemetry_loop.rs:24-46).
  - The platform e2e battery sets OUTBOX_SWEEP_SECS=2
  (scripts/platform-e2e.mjs:108;
  scripts/effective-config/families/platform-e2e.family:25). Deriving would
  run reconciler circles every 2 s there and open 'fork_debt_stale' after 6 s
  (3 periods, reconcile.rs:40-42).
  - The two differ at the edge. The fork value is floored at 1 s
  (src/bootstrap.rs:765); the outbox value is used raw
  (telemetry_loop.rs:41-46).
  - The formal record names this knob: verification/assumptions.md:532-533,
  verification/tla/history/ForkPin.tla:379 (an input of TLA-019; editing it
  stales the receipt), verification/tla/history/README.md:1641.
  - The read sits in bootstrap::run (bootstrap.rs:762-766), which six growth
  rows freeze exactly (docs/quality/exception-growth.json, crate::run).
  src/bootstrap.rs is also an input of TLA-011 and mutation-critical.
  The knob was added on 2026-09-24 (commit 0d40dc2a). The gain is one option.
  If the option must go, a constant 300 avoids the coupling but has the same
  bootstrap::run cost.
- Must change with it:
  If done anyway:
  - src/config/cli.rs:347-352 and the fixture :613
  - src/config/tests.rs:456
  - src/bootstrap.rs:762-766, with an owner update of the crate::run rows in
  docs/quality/exception-growth.json
  - the TLA-011 receipt, and TLA-019 if ForkPin.tla:379 is edited
  - the bootstrap mutation leg (scripts/quality/mutation_owners.py:91)
  - RUNBOOK.md:125-126 and :619
  - docs/READINESS.md:71
  - verification/assumptions.md:532-533,
  verification/tla/history/README.md:1641
  - scripts/effective-config/effective_config.py:74 and rename-map.json
  ('removed' for cli.fork_debt_sweep_secs)
  The DST uses its own period (src/dst/tests/fork_debt.rs:298-317) and is
  unaffected.
- What a client can observe:
  Not on the wire. Operators: the gauge 'fork_debt_stale_after_ms' and the
  'fork_debt_stale' alert threshold would follow OUTBOX_SWEEP_SECS
  (reconcile.rs:96-110; src/ops.rs:729-732), and '--fork-debt-sweep-secs' on
  argv becomes a boot refusal.
- For the owner:
  None needed if the recommendation is dropped. Otherwise: is tying the
  fork-reference repair cadence and its alert threshold to a billing knob
  acceptable, and is one option worth an update of the bootstrap::run
  exception rows plus stale receipts?

#### PATH_PREFIX (raw environment reader beside --path-prefix)

- Proposed: read spool reads the raw env value; stores and rollup read the clap value -> one reader: the clap value
- Verification: **holds**
- Reason:
  With --path-prefix on argv the stores and rollup move but the read spool
  does not. The customer-facing composer guide passes the prefix on argv,
  which is the case where they disagree.
- What the verifier found:
  Paths relative to the repository root. Verified: the read spool
  reads the raw environment copy (src/config/load.rs:176,
  src/config/model.rs:203-206, src/billing.rs:1115-1126) while stores and
  rollup read the clap value (src/bootstrap.rs:53-62,:686,:889-893).
  Correction: it is not true that with --path-prefix on argv 'the read spool
  does not move'. The spool opens on state.data_store, which is already the
  PrefixStore built from the clap value (src/bootstrap.rs:59-62,:216,:656),
  and then adds the prefix a second time as an inner path
  (src/billing/read_spool.rs:79-83; rollup does the same,
  src/rollup.rs:498-502). So with environment only both live at <P>/<P>/...;
  with argv only the rollup is at <P>/<P>/... and the spool at
  <P>/telemetry/read-spool/<instance>. The spool is still inside the
  deployment's prefix, so deployments sharing a bucket stay isolated; only the
  inner component differs. The spool opens only when USAGE_STREAM_KEY is set
  (src/billing/telemetry_loop.rs:18-21); docs/GUIDE-COMPOSER.md:199-223 passes
  the prefix on argv without a usage key, so that guide never opens it. All
  Compute families set the prefix through the environment only (evidence:
  cli.path_prefix equals billing.path_prefix_env in every family). There is a
  direct precedent: BILLING_MODE and ROLLUP had the same two-reader split and
  were unified under item 32 with edge record 49
  (scripts/effective-config/rename-map.json:4-20). src/config/tests.rs:153-180
  already pins that the environment channel reaches the clap value, so
  environment-only deployments are unaffected.
- Must change with it:
  src/config/load.rs:176; src/config/model.rs:8 (module doc), :203-206, :449;
  src/config/summary.rs:70 (startup summary key path_prefix_env);
  src/config/tests.rs:248; src/billing.rs:1121-1126; a 'pairs' entry
  billing.path_prefix_env -> cli.path_prefix in
  scripts/effective-config/rename-map.json (K9); the note in
  docs/reviews/2026-09-hardening/effective-config-diff.md:324-328 (D11);
  src/billing.rs is a large file, so the edit must not add lines. DST callers
  pass through unchanged (src/dst/tests/billing_readiness.rs:49,:69 use the
  rig's config, whose prefix is None).
- What a client can observe:
  No wire change. For a deployment that passes the prefix on argv AND has a
  usage key, the spool database moves from <P>/telemetry/read-spool/<instance>
  to <P>/<P>/telemetry/read-spool/<instance>; metered reads pending in the old
  database at the switch would be stranded, which is a billing under-count
  rather than a client-visible answer. No such deployment is known.
- For the owner:
  Approve the data-location change for argv deployments with a usage key, and
  say whether a pending spool at the old path must be drained or migrated
  first (stored formats have one LAYOUT_VERSION and no aliases). Should this
  get an edge record like #49? Could not determine: whether any real
  deployment passes --path-prefix on argv with USAGE_STREAM_KEY set, and
  whether the multitenancy audit fingerprints the edited lines (scripts were
  not run).


### Profile and script lines

#### L0_MAX_SSTS (already changed in 823b3269; residue only, not a new finding)

- Proposed: profile line 32 = binary default 32 -> no profile line
- Verification: **holds**
- Reason:
  The default is now the profile value, so the profile line repeats it. The
  change also leaves stale text that reasons about a cap of 64, and it pairs
  the new cap with the upstream compaction-worker defaults for any deployment
  without the profile (see the next three entries).
- What the verifier found:
  Note: HEAD is 823b3269, not 9d6cb381 as the task text says; the default
  change is committed. Verified: default 32 at src/config/cli.rs:136, fixture
  cli.rs:582, pinned surface src/config/tests.rs:405, RUNBOOK.md:101; profile
  line deploy/profiles/compute-1g.env:36; per-key cap follows it at
  src/config/validation.rs:42-46. The verifier claim is right with a one-line
  offset: bench/soak/oom-acceptance.sh:73 lists the key, :78-79 fails with
  'profile missing L0_MAX_SSTS', and every leg calls verify first (:95), so
  deleting the line fails every OOM acceptance leg. Stale '64' confirmed at
  cli.rs:159-160 and RUNBOOK.md:104, and also at docs/TIGRIS-404-COST.md:116.
  MEMPROFILE_CERT does not check the L0 cap (src/config/profile.rs:94-114).
  Corrections: (1) the reason's 'see the next three entries' is dangling; no
  entry in this list covers the compaction-worker defaults. (2) That risk is
  real by the profile's own model: compute-1g.env:89-97 says that at upstream
  worker defaults (the binary defaults, src/config/model.rs:376-381: 4
  concurrent, 4 subcompactions, 4 fetch tasks x 2 MiB, 256 MiB rolls) staged
  prefetch scales with L0 inputs ('a 32-input L0 merge can stage ~1 GB'). The
  commit message's 'an L0 costs a stored object, not memory' holds only under
  the profile's 1/1/1 MiB worker. SlateDB's scheduler caps one compaction at 8
  sources (pinned checkout slatedb/src/config.rs:1417-1418; the repo sets no
  scheduler options) but 4 may run concurrently. A deployment without the
  profile exists in the docs: docs/GUIDE-COMPOSER.md:202-219. I could not
  determine the real RSS effect; nothing measured it. Extra stale text found:
  RUNBOOK.md:99 gives the L0_SST_SIZE_BYTES default as 32 MiB (binary: 8 MiB,
  cli.rs:108); cli.rs:147-152 is the WAL-GC doc text attached to
  --compactor-poll-ms, so --help for that flag opens with the wrong paragraph;
  GUIDE-COMPOSER.md:215-217 still says l0_sst_size is 32 MiB.
- Must change with it:
  Stale text: src/config/cli.rs:159-160 (and :147-152 misplaced doc);
  RUNBOOK.md:104 and :99; docs/TIGRIS-404-COST.md:116;
  docs/GUIDE-COMPOSER.md:215-217. To delete compute-1g.env:36:
  bench/soak/oom-acceptance.sh:61-81 (check table and the missing-key arm) and
  its header :13-18 ('twelve' knobs). Optional:
  bench/livefeed-perf/run-one.sh:72 (now redundant). No family file changes
  (they include the profile by reference). No Rust test pins the profile line.
  No receipts stale (config files have 0 manifest mentions). The
  effective-config comparison (deployment gate,
  docs/reviews/2026-09-hardening/README.md:184-187) will show cli.l0_max_ssts
  8 -> 32 for the families without the profile (defaults, platform-e2e) and
  needs an annotation.
- What a client can observe:
  No wire change. RUNBOOK.md is compiled in and served on the authenticated
  operator runbook route (src/operator.rs:26, :65-82), so its text changes
  there; cli.rs doc text is the --help output. On Compute a removed profile
  line does not unset the variable: env is project-scoped and merged
  (RUNBOOK.md:481-496), harmless here because 32 = 32.
- For the owner:
  (1) May oom-acceptance verify stop requiring L0_MAX_SSTS in the file (it
  changes what a field gate checks, 12 knobs to 11)? (2) Editing the profile
  changes its sha256, which RC evidence pins
  (scripts/verify-rc-evidence.py:109-121): acceptable under the release hold?
  (3) Should COMPACTOR_MAX_CONCURRENT, COMPACT_MAX_SUBCOMPACTIONS,
  COMPACT_MAX_FETCH_TASKS, COMPACT_BYTES_TO_FETCH, COMPACT_MAX_SST_SIZE_BYTES
  and STORE_BULK_INFLIGHT_MAX_BYTES become binary defaults too, since the
  default cap of 32 is only modelled safe with them? That would rewrite
  src/config/certification_tests.rs:12-20 and src/config/tests.rs:210-217.

#### TELEMETRY_CACHE_BYTES / HISTORY_CACHE_BYTES / POSTINGS_CACHE_BYTES / MAX_UNFLUSHED_BYTES / L0_SST_SIZE_BYTES / SSE_FEED_RING_BYTES

- Proposed: profile lines equal to the binary defaults -> no profile lines
- Verification: **holds**
- Reason:
  Each profile value is the binary default: 16 MiB, 32 MiB, 64 MiB, 16 MiB, 8
  MiB, 1 MiB.
- What the verifier found:
  Each value equals the binary default:
  - 16 MiB telemetry cache (deploy/profiles/compute-1g.env:30;
  src/config/model.rs:454)
  - 32 MiB history cache (:32; model.rs:405)
  - 64 MiB postings cache (:33; model.rs:415)
  - 16 MiB unflushed (:34; src/config/cli.rs:114)
  - 8 MiB L0 SST (:35; cli.rs:108)
  - 1 MiB feed ring (:44; model.rs:423)
  At 823b3269, L0_MAX_SSTS=32 (:36) is a seventh redundant line (cli.rs:136).
  The server resolves the same values with or without the lines. The
  release-posture ring check still passes on the default
  (src/config/validation.rs:152-168). scripts/verify-rc-evidence.py:216-218
  pins only the SSE_FEED_TOTAL lines.
  Three conditions:
  1. bench/soak/oom-acceptance.sh:62-82 requires the five non-SSE keys
  (:68-72) and L0_MAX_SSTS (:73) to be in the file. Nothing else reads those
  resolved keys.
  2. Compute env is project-scoped and merged, and projects are reused
  (RUNBOOK.md:481-495;
  scripts/effective-config/families/region-server.family:5-6). Today the
  profile lines restore these values on every deploy. Once dropped, a value
  left by WC_DIET (16 MiB caches, bench/soak/wc-ladder.sh:65-74) or by an old
  campaign stays until --unset-env.
  3. The profile also serves as the acceptance specification.
- Must change with it:
  - deploy/profiles/compute-1g.env:30, :32-35, :44 (and :36)
  - bench/soak/oom-acceptance.sh:62-75 (take the expected values from
  somewhere else, or stop checking them) and its header :13-18
  - one --unset-env pass, or fresh projects, for every reused Compute project
  - RUNBOOK.md:99 (says the L0_SST_SIZE_BYTES default is 32 MiB; it is 8 MiB)
  and :822, :825
  - docs/GUIDE-COMPOSER.md:214-217 (calls MAX_UNFLUSHED_BYTES=67108864
  mandatory for the old 32 MiB reason)
  - docs/STAGING.md:149-151
  - scripts/effective-config: the families read the file at run time; re-run
  the report
  No Rust, test, receipt or ledger change.
- What a client can observe:
  No, provided no reused project holds a stale value for a dropped name.
- For the owner:
  May oom-acceptance.sh stop demanding these keys? That changes what a gate
  checks. If the profile stops pinning them, who guards against a later
  default drift: a test that pins the defaults, or a wider MEMPROFILE_CERT
  check in the binary? Do deploys keep restating the complete environment, as
  RUNBOOK section 7.3 requires?

#### Deploy-script lines that restate a default (FLUSH_INTERVAL_MS=25, ABSORB_BYTES=4194304, POOL_IDLE_SECS=4, TAIL_RING_BYTES=0, STREAMS_DEBUG_TIMING=0, STREAMS_DEBUG_EXIT=0, FLEET_MAX=4, FLEET_MIN=1, REBALANCE_LAG_SECS=60, REBALANCE_RETURN_SECS=300, MAX_ABSORB_LAG_SECS=900, INITIAL_SHARDS=16 in the fleet, STREAMS_AUTH_ISSUER)

- Proposed: passed by the scripts at the default value -> not passed
- Verification: **did not hold**. Instead: As stated (drop or make conditional, high confidence): refuted for Compute projects that are reused. Safe only where a script provisions a fresh project per run, or where each dropped name is replaced by an explicit --unset-env; the STREAMS_DEBUG_* resets must stay unconditional.
- Reason:
  Each line sets what the binary already does. INITIAL_SHARDS=16 equals the
  automatic fleet value for FLEET_MAX=4. With the pump on, FLUSH_INTERVAL_MS
  at or below 1000 has no effect at all. The knobs themselves stay.
- What the verifier found:
  Paths relative to the repository root. Every value does equal
  the binary default (src/config/cli.rs:51,:97,:254,:325-330,:544;
  src/config/model.rs:364,:481-485,:510) and INITIAL_SHARDS=16 equals the
  automatic fleet value for FLEET_MAX=4 with FLEET_PREFIX set
  (src/config/validation.rs:815-829). But on Compute a restated default is not
  redundant, it is the reset. RUNBOOK.md:484-496 records that Compute env is
  project-scoped and merged and states the rule: 'every deploy restates the
  service's COMPLETE environment ... removals are explicit --unset-env. Never
  do an incremental deploy.' The recommendation contradicts that rule.
  Projects are reused and shared between scripts: deploy-region.sh and
  wc-ladder.sh both read $SOAK_HOME/proj-<region>.txt
  (bench/soak/deploy-region.sh:93, bench/soak/wc-ladder.sh:48), and wc-ladder
  sets ABSORB_BYTES=${WC_ABSORB_BYTES:-4194304} and other instruments
  (wc-ladder.sh:199-203); without deploy-region's restating line a later
  region deploy silently inherits a campaign value. The worst case is the
  instrument lines: STREAMS_DEBUG_EXIT=1 enables POST /abort, which kills the
  process without cleanup (src/http.rs:1312-1335). With the line dropped, or
  made conditional as the recommendation proposes, a project where a campaign
  once set 1 keeps the endpoint enabled on every later deploy; the
  unconditional ${STREAMS_DEBUG_EXIT:-0} (bench/fleet/deploy-fleet.sh:130) is
  what turns it off. Same for STREAMS_DEBUG_TIMING, TAIL_RING_BYTES,
  FLEET_MIN, REBALANCE_* and MAX_ABSORB_LAG_SECS. Smaller corrections: no
  script carries 13 such lines (deploy-fleet about 10, deploy-region 5); the
  statement that FLUSH_INTERVAL_MS at or below 1000 has no effect under the
  pump is true only while WAL_FLUSH_GAP_MS is non-zero, because a zero gap
  falls back to FLUSH_INTERVAL_MS (src/bootstrap.rs:420-424); and the
  recommendation missed two dead lines in wc-ladder.sh (:205 ADMIT_RSS_SHED_MB
  and :207 SLATEDB_RT_THREADS, both overridden by the profile that follows).
- Must change with it:
  If the owner retires the restate rule anyway: RUNBOOK.md:492-496 (compiled
  into the binary); bench/soak/deploy-region.sh:173-182;
  bench/fleet/deploy-fleet.sh:122-138; bench/soak/mt-tenants.sh:142-148;
  bench/soak/wc-ladder.sh:195-210; each dropped --env needs a matching
  --unset-env on reused projects (note RUNBOOK.md:490: an --unset-env for one
  service once gutted another's env); every dropped name must leave the family
  files or check-families fails with 'sets [...] which <script> never sets'
  (scripts/effective-config/effective_config.py:1215-1218):
  scripts/effective-config/families/region-server.family:57-66,
  region-server-scale.family, fleet-server-1.family, fleet-server-n.family,
  mt-tenants-off.family, mt-tenants-enforce.family:30-40 and :89-100,
  wc-ladder.family, wc-ladder-diet.family; docs/STAGING.md:152-177 and
  docs/GUIDE-COMPOSER.md:210-213 restate the same values.
- What a client can observe:
  None when the project holds no stale value. If a stale value survives,
  clients see whatever that value does; for STREAMS_DEBUG_EXIT=1 an authorized
  caller can crash the instance through /abort.
- For the owner:
  Keep RUNBOOK section 7.3's rule (scripts restate the complete environment,
  which keeps these lines) or replace it with 'scripts list only what differs
  plus explicit --unset-env'? If the second, may agents use --unset-env on
  shared projects given the recorded incident? Could not determine: which
  Compute projects currently hold which variables; only a platform export
  shows that, and the effective-config report says the same
  (docs/reviews/2026-09-hardening/effective-config-diff.md item 7).


### Switches and dead settings

#### ABSORB_PACE_MS / ABSORB_PACE_WINDOW_MS

- Proposed: 0 (off) / 50 ms, settable -> no options, no pacing code
- Verification: **holds**
- Reason:
  The code itself records that pacing was falsified and the knob kept only for
  field experiments. No deployment turns it on; one script passes both at
  their defaults.
- What the verifier found:
  Verified: options at src/config/cli.rs:290-299, defaults 50 / 0; the code
  records the falsification at src/history.rs:661-664 and
  src/history/gather.rs:33-34 and :123-125; the park is gather.rs:543-551,
  called at :440; wiring at src/bootstrap.rs:101-102, in absorber_config
  (:96), outside the frozen bootstrap::run. No deployment turns it on;
  bench/soak/wc-ladder.sh:201 passes the defaults. Two refinements. (1)
  wc-ladder.sh:201 is an experiment hook (WC_PACE_WINDOW / WC_PACE_MS), so
  removal ends the ability to re-run the pacing experiment from that script.
  (2) The L1d8 result exists only in code comments;
  bench/WORKLOAD-CERT-PLAN.md has no L1d7 or L1d8 row, so I could not read the
  measurement itself. The metric removal is harder than the entry says: the
  gauge at src/ops.rs:449-452 is inside collect_snapshot (:391), which has a
  frozen exception row (docs/quality/exception-growth.json:349), so even
  shrinking it fails as 'stale exception growth row'. /v1/debug/load and
  /v1/debug/absorb are listed surfaces (docs/refactor/WIRE-MATRIX.md:212 and
  :222). The pinned test is not named in test-scenario-map.json or
  review-mechanisms.json. After removal an ABSORB_PACE_MS left in a Compute
  project's env is ignored silently, but --absorb-pace-ms on argv fails boot;
  the existing precedent is 'accepted but ignored' (cli.rs:247-267), which
  keeps names and so does not simplify.
- Must change with it:
  src/config/cli.rs:290-299 and :602-603; src/config/tests.rs:437-438;
  src/bootstrap.rs:101-102 (stales TLA-011); src/bootstrap/tests.rs:14-15 and
  :24-25; src/history.rs:623-641 and :660-664, plus the static at :430-433 if
  the fields go (stales TLA-016, TLA-018, TLA-019);
  src/history/gather.rs:123-129, :414-417, :440, :454, :541-551 (TLA-016);
  src/dst/tests/history_gather.rs:85-117 (helper) and :224-268 (test
  gather_pacing_preserves_outcomes_and_opens_windows), then python3
  scripts/test-inventory.py --write (docs/refactor/test-inventory.json:3335).
  If the fields go: src/http.rs:926-932 and :1423, src/ops.rs:449-452 (owner
  must update the collect_snapshot row),
  docs/quality/source-allowances.json:1851-1857 (prune through gate.py),
  docs/refactor/WIRE-MATRIX.md:212 and :222, and an edge record. Scripts:
  bench/soak/wc-ladder.sh:201; families wc-ladder.family:80-81 and
  wc-ladder-diet.family:79-80. effective-config: rename-map.json needs
  'removed' entries for cli.absorb_pace_ms and cli.absorb_pace_window_ms, and
  PIN_NEW_LEAVES (effective_config.py:74) drops by two.
- What a client can observe:
  Only if the fields are removed: gather_last_pace_ms disappears from GET
  /v1/debug/load and the _ops_metrics gauges, lastPaceMs from GET
  /v1/debug/absorb. No script in bench/ or scripts/ reads them (searched).
  Gather behaviour is unchanged because the park is off everywhere.
- For the owner:
  Remove the three pace fields (an edge change plus an updated exception row
  for collect_snapshot), or keep them reporting 0? Hard removal of the flags,
  or the accepted-but-ignored pattern?

#### ABSORB_PASS_BYTES / ABSORB_CONCURRENCY / ABSORB_SMALL_BYTES

- Proposed: accepted and ignored, with a startup notice -> not declared
- Verification: **holds**
- Reason:
  All three are documented as deprecated and have no consumer except the
  notice. NEXT-WORK already plans the removal after the platform export.
- What the verifier found:
  Verified: the three fields are read only by ignored_absorber_options
  (src/config/cli.rs:655-664), which feeds one notice
  (src/config/validation.rs:655-659; src/config/notice.rs:51-55,130-137).
  src/bootstrap/tests.rs:9-35 pins that they cannot change the absorber
  configuration. No script in the repository passes them on argv or env (git
  grep: only the effective-config probe). After removal a leftover env name is
  simply unread (no clap field reads it), so no Compute project can fail to
  boot because of it; only the three argv flags become parse errors. A
  malformed leftover value today refuses boot at clap parse and would
  afterwards be ignored. Two local campaign scripts outside the repository
  still set the env name (~/.streams-soak/chaos-sin.sh:59, oom-arm.sh:48),
  which supports the tool's note that old projects likely hold it
  (scripts/effective-config/boot.py:280-284). One correction to the reasoning:
  NEXT-WORK.md:687 sits under 'Deployment gates' and reads as removing the
  name from the projects after the export (compare effective-config-diff.md
  item 3, '--unset-env ABSORB_PASS_BYTES'); it does not plainly plan removing
  the option from the binary.
- Must change with it:
  src/config/cli.rs:247-251,259-267 (fields), :594,:597-598 (fixture),
  :651-664; src/config/validation.rs:655-660;
  src/config/notice.rs:51-55,130-137,167-174; src/config/tests.rs:425,428,429
  (EXPECTED_CLI_SURFACE) and the six tests at :538-668;
  src/bootstrap/tests.rs:31-35; RUNBOOK.md:110; COMPUTE-SPEC.md:99-100;
  scripts/effective-config/boot.py:48,53-55,280-284 (the leftover probe
  expects the notice), proposed.json:10-24 and :37-42,
  families/region-server.family:6 and region-server-scale.family:6 (notes),
  effective_config.py:72-76 (leaf pins).
- What a client can observe:
  No client change. Operator-visible: the startup warning for a leftover name
  disappears, --help loses three entries, and the three argv flags refuse
  boot.
- For the owner:
  Remove before or after the platform export? Today the boot warning is the
  only signal in the binary that a project still holds a retired name; after
  removal only the export shows it. Does this take an edge record with surface
  'process', as #48 did?

#### TAIL_RING_BYTES

- Proposed: 0 (ring off); an env switch between two read paths -> one permanent path: either the ring always on with a fixed, certified budget, or the ring removed. Owner decision.
- Verification: **did not hold**. Instead: Keep the knob for now. Report the conflict with the invariant to the owner as a design item; neither direction is a safe removal today.
- Reason:
  The product invariant forbids an env switch on a read optimisation, and the
  disposition record lists durable ring coverage as enabled unconditionally,
  yet the ring is off by default and in every deployed family, so that path is
  unreachable in deployment. Field evidence favours the ring (woken read 22-56
  ms to 2-3 ms at 99.9% hit rate), but the budget is per shard engine and was
  never part of the certified 1 GiB envelope.
- What the verifier found:
  The finding is real. Default 0 at src/config/cli.rs:97; the ring is gated at
  src/shard.rs:1299 and src/shard/tail_ring.rs:108, publication at
  src/shard/transaction/append.rs:207; the disposition lists O3 durable ring
  coverage as enabled unconditionally
  (docs/read-experiments/final-disposition.md:17) while deploy-region.sh:175
  defaults it to 0 and no other Compute script sets it. Field evidence as
  cited (docs/SOAK7-REPORT.md:128-138, one active shard). The action is what
  fails. Option (a), always on with a fixed budget: the budget is per engine
  (shard.rs:1300; one value handed to every engine at bootstrap.rs:436 and
  :522). Fleet mode defaults to 16 shards (cli.rs:37-44, validation.rs:827;
  deploy-fleet.sh:129 sets 16) and FLEET_MIN defaults to 1, so one instance
  can own all 16: 16 x 32 MiB = 512 MiB, above the profile's 500 MB shed line
  (compute-1g.env:37). A permanent ring needs a process-wide budget first,
  which is new design, not a simplification. run-wide.sh:38-41 already had to
  raise the shed line for four rings. Option (b), removal: contradicts an
  owner-accepted disposition, discards the measured latency gain and deletes
  the ring DSTs. Either way several tests use ring off to reach the canonical
  scan (src/shard/read_budget_tests.rs:56 and :170,
  src/history/worker/lane_isolation_tests.rs:63,
  src/dst/tests/read_subset_retention.rs:17, read_history_lifecycle.rs:21,
  src/shard/record_scan_tests.rs:64) and would need another way to force it. I
  could not determine what the deployed projects' persisted env holds; only a
  platform export shows that.
- Must change with it:
  Either direction: src/config/cli.rs:92-98 and :578; src/config/tests.rs:397;
  src/bootstrap.rs:436 and :522 (inside the frozen bootstrap::run,
  exception-growth.json:252-333; TLA-011); src/shard.rs:914-921, :986,
  :1128-1146, :1299-1310 (zero headroom; ShardEngine::start is frozen,
  exception-growth.json:470-531; stales KANI-047, TLA-002, TLA-005, TLA-006,
  TLA-011, TLA-016); src/shard/transaction/append.rs:207 (TLA-002, TLA-003,
  TLA-005); RUNBOOK.md:673; bench/soak/deploy-region.sh:175;
  bench/soak/wc-ladder.sh:63-71; families region-server:61,
  region-server-scale:56, wc-ladder-diet:100, wc-ladder:13 (omit);
  bench/costab/*.sh and bench/sse-probes/*.sh; every test that sets
  tail_ring_bytes (src/dst/tests/reads_ring.rs:51-521, read_page_limits.rs:37,
  src/shard/tail_ring_tests.rs, retirement_tests.rs:99,
  transaction_tests.rs:31). Removal additionally deletes
  src/shard/tail_ring.rs, the ring callers at src/shard/record.rs:173 and
  :333, DurableRingCoverage (record.rs:101-113) and the ring DSTs, with
  inventory and owners.json updates.
- What a client can observe:
  Same answers either way (the canonical scan is the fallback). Latency of
  woken live reads changes (22-56 ms to 2-3 ms in soak7), the tail_ring block
  of /v1/debug/timings changes, and ring memory counts toward RSS, so write
  shedding (429) can start earlier.
- For the owner:
  Does the one-permanent-read-path invariant cover the tail ring? If yes:
  permanent ring (needs a process-wide budget and a memory certification on 1
  GiB, including one instance owning every shard) or removal (reverses
  disposition O3)? Memory and performance hold.

#### SCALE_COLD_PCT / SCALE_COLD_EVALS / MAX_SEGMENTS_PER_STREAM

- Proposed: 15 / 180 / 64, parsed and logged -> not declared
- Verification: **holds**
- Reason:
  All three are dead: the only readers are the redacted summary and tests. The
  merge line is hot_pct x 0.05 and merge patience is hot_evals x 4. RUNBOOK
  documents all three as live, and no code enforces a per-stream segment cap.
- What the verifier found:
  Holds. I could not break it. Paths under .
  A search of the whole tree finds cold_pct, cold_evals and max_segments read
  only by the overlay (src/config/load.rs:248-250, :254-256, :260-262), the
  defaults (src/config/model.rs:496, :498, :500), the log summary
  (src/config/summary.rs:85, :87, :89) and config tests. The scaler decides
  cold as hot_pct x 0.05 (src/scaler3.rs:362-365) and merge patience as
  hot_evals x 4 (src/scaler3.rs:120-122, used at :436). No code bounds
  segments per stream: there is no segment-count comparison in src/scaler3.rs,
  src/application/topology.rs or src/segmap.rs.
  Rigs that set SCALE_COLD_EVALS=12 get nothing from it
  (bench/docker/compose.yml:29, bench/docker/harness/cluster-deploy.sh:52).
  The fleet rigs steer merges through the hot knobs
  (scripts/effective-config/families/fleet-server-1.family:80-81).
  Everything to delete is inside src/config. overlay_scaler carries an
  #[expect] (load.rs:230-234) and has no growth row, so shrinking it is
  allowed. No formal receipt and no mutation-critical file is touched.
  THE DOCUMENTS ARE WRONG TODAY: RUNBOOK.md:232-235 and docs/SCALING.md:52,
  :149-152, :175 promise merge below 15%, 180 evaluations of patience and a
  64-segment split guard. The code merges below 3.75% of the limits after 8
  evaluations at defaults and has no guard.
- Must change with it:
  Paths under .
  - src/config/load.rs:248-250, :254-256, :260-262.
  - src/config/model.rs:261-262, :265-266, :269-270, :496, :498, :500.
  - src/config/summary.rs:85, :87, :89.
  - src/config/tests.rs:268, :270, :272.
  - src/config/numeric_tests.rs:21, :23, :31, :33-38, :45, :47, :52, :54.
  - RUNBOOK.md:232, :233, :235.
  - docs/SCALING.md:52, :149, :150, :152, :175, :355.
  - docs/STAGING.md:178, :180, :182.
  - bench/docker/compose.yml:26-29, bench/docker/harness/cluster-deploy.sh:52,
  bench/docker/harness/d2run.sh:4.
  - scripts/effective-config/effective_config.py:74 (155 becomes 152) and
  rename-map.json:29 (three removed leaves).
  - scripts/clippy-baseline-fingerprints.txt:26 and
  docs/refactor/clippy-review-baseline.txt:26 name the three fields; I did not
  determine whether a gate reads those files.
  - Not affected: EXPECTED_CLI_SURFACE and CliArgs::deterministic
  (environment-only names); ScalePolicy literals in src/scaler3.rs:752, :789,
  :813, :840 and src/scaler3/tests/first_transition.rs:17, :51 do not name
  these fields.
- What a client can observe:
  No. Scaler behaviour is unchanged. The startup summary loses three keys, and
  the operator runbook text changes.
- For the owner:
  Is the documented policy (merge below 15% after 180 evaluations, at most 64
  segments per stream) what the product should do? If yes, the scaler is
  missing it and this is a behaviour item, not a removal. If the code is
  right, RUNBOOK and SCALING.md should state the real rule: 5% of the hot
  threshold, four times the split patience, no segment cap.

#### STORE_MAX_CONCURRENT

- Proposed: 0 (off) -> no option, no semaphore
- Verification: **holds**
- Reason:
  RUNBOOK calls it a diagnostic knob whose experiment gave a negative result
  and says to leave it off. Nothing sets it.
- What the verifier found:
  The removal holds, but 'risk low' misses a ledger that only the owner may
  change.
  Verified:
  - Default 0 means no semaphore (src/config/model.rs:54-56, :365;
  src/store_timing/resources.rs:16-17, :30-40).
  - Nothing in the tree sets the name: the only mentions outside src are
  documents (RUNBOOK.md:158 'leave off unless experimenting';
  EXPERIMENT-PILOT.md:1283-1289, the negative result;
  docs/PROVIDER-CONTRACT.md:78, as an example).
  - No src/store_timing file is in a formal receipt, and the file has no
  approved growth row.
  Code that would go:
  - the field and its overlay (model.rs:54-56, :365; src/config/load.rs:44-46)
  - StoreResources.concurrent and permit() with its #[expect] (resources.rs:8,
  :16-17, :26-40)
  - six call sites in src/store_timing.rs:77, :89, :122, :180, :225, :233
  What breaks the 'low risk' claim:
  - The test that exercises the semaphore,
  runtime_store_concurrency_is_shared_locally_and_independent_of_first_access
  (resources.rs:199-248), is pinned by sha256 as evidence for review
  obligation R10 (docs/refactor/review-mechanisms.json:610-614). Deleting or
  rewriting it changes what the review-evidence check counts, which is an
  owner decision.
  - A Compute project keeps every variable ever set in it
  (RUNBOOK.md:483-493). The July experiment set STORE_MAX_CONCURRENT=48; if a
  project still holds it, the cap is live there today and would silently
  vanish. I cannot determine what live projects hold; the effective-config
  document names the platform export as the open gap
  (effective-config-diff.md:36-40).
  I could not determine, without running the gate, whether the stale entry for
  the removed test in docs/quality/legacy-diagnostics.json:7062 (a never-edit
  file) fails a ratchet.
- Must change with it:
  - src/config/model.rs:54-56, :365; src/config/load.rs:44-46;
  src/config/summary.rs:18; src/config/tests.rs:209
  - src/store_timing/resources.rs:8, :16-17, :26-40, and the test at :199-248
  (rewrite it on the bulk gate, or remove it)
  - src/store_timing.rs:77, :89, :122, :180, :225, :233
  - docs/refactor/review-mechanisms.json:610-614 (owner: re-pin or remove the
  mechanism test)
  - RUNBOOK.md:158; docs/PROVIDER-CONTRACT.md:78
  - scripts/effective-config/effective_config.py:74 (new leaf count 155
  becomes 154) and scripts/effective-config/rename-map.json ('removed':
  storage.store_max_concurrent)
  Not affected: EXPECTED_CLI_SURFACE and CliArgs::deterministic (env-only
  name); the profile; certification; formal receipts.
- What a client can observe:
  No. The startup summary loses the key storage.store_max_concurrent
  (src/config/summary.rs:18). The variable, if still set somewhere, is ignored
  without a message.
- For the owner:
  1. May the R10 mechanism test be rewritten against the bulk gate and
  re-pinned, or removed from docs/refactor/review-mechanisms.json?
  2. Does any live Compute project still hold STORE_MAX_CONCURRENT from the
  July experiment?

#### SCALE_RPS_CAPACITY

- Proposed: 0 (dimension off) -> no option, no rps dimension
- Verification: **holds**
- Reason:
  Documented as a legacy dimension to leave off because assumed-capacity
  constants go stale; nothing sets it.
- What the verifier found:
  Verified. Default 0 (src/config/cli.rs:442-447). The dimension runs only
  when capacity_rps > 0 (src/fleet.rs:531-537). No script, family, test or rig
  sets the name; it appears only in cli.rs, tests.rs, a comment at
  src/fleet/heartbeat.rs:56 and documents. RUNBOOK.md:211 says to leave it
  off. The one recorded use, SCALE_RPS_CAPACITY=150 in pilot run 5, is the
  incident that retired it (COMPUTE-SPEC.md:193).
  What is deleted: the field (fleet.rs:293-295), its assignment (:356),
  need_rps (:533-537), two .max(need_rps) (:576, :597), and '({need_rps})' in
  the reason string (:971). total_rps must stay: it gates edge_hot (:561) and
  the 5 rps load gate (:507).
  The cost is larger than 'one option':
  - The boot log reads the field inside bootstrap::run
  (src/bootstrap.rs:806-812). Six approved growth rows freeze that function
  exactly (docs/quality/exception-growth.json: crate::run, scope_lines 583,
  syntax_facts 977), so any edit fails as a stale row until the owner updates
  it.
  - src/fleet.rs and src/bootstrap.rs are inputs of TLA-011
  (verification/manifest.json), so its receipt goes stale.
  - Both files are mutation-critical
  (scripts/quality/verification_plan.py:26-31;
  scripts/quality/mutation_owners.py:91, :173).
  I could not determine whether any live Compute project still holds the name,
  or what re-recording TLA-011 costs.
- Must change with it:
  - src/config/cli.rs:442-447 and the fixture :630
  - src/config/tests.rs:473 (EXPECTED_CLI_SURFACE)
  - src/fleet.rs:293-295, :356, :531-537, :576, :597, :971 (the file is 1,011
  lines and may only shrink; this shrinks it)
  - src/bootstrap.rs:808-812, with an owner update of the six crate::run rows
  in docs/quality/exception-growth.json
  - the TLA-011 receipt, re-recorded after the commit
  - the CI mutation leg for the fleet and bootstrap owners
  - scripts/effective-config/effective_config.py:74 (PIN_NEW_LEAVES 155) and a
  'removed' entry in rename-map.json
  - RUNBOOK.md:211, COMPUTE-SPEC.md:193 and :203, the comment at
  src/fleet/heartbeat.rs:56
- What a client can observe:
  Not on the wire. Operators see three things: '--scale-rps-capacity' on argv
  becomes a boot refusal (clap) and the environment variable is ignored; the
  'reason' string stored in fleet/desired.json and logged on every change
  loses '(need_rps)' (fleet.rs:969-972); the boot line 'fleet coordination on
  (prefix=, cap= rps)' changes.
- For the owner:
  Is one option worth an update of the bootstrap::run exception rows
  (owner-only), a TLA-011 re-record and a fleet mutation leg, or should it
  ride along with the next change that already touches those files? Remove
  outright, or accept and ignore as was done for the absorber options
  (cli.rs:247-267)?

#### HISTORY_COMPACTOR

- Proposed: compactor on unless 'off' -> no option; compactor always on
- Verification: **holds**
- Reason:
  A bench-only escape hatch that no bench, script or deployment references;
  the profile certificate refuses it. With it the history L0 caps rise to
  1,000,000.
- What the verifier found:
  Paths relative to the repository root. Verified: the only
  reader is src/config/load.rs:105-111; it disables the embedded compactor and
  lifts both L0 caps to 1,000,000 (src/history.rs:460-464,:507-519). A
  repository-wide search (evidence directory excluded) finds the name only in
  src/ and in two planning notes: no deploy script, bench, CI workflow, family
  file, RUNBOOK or STAGING sets it. The s3lite --discard-substr mode it was
  built for still exists (src/bin/s3lite.rs:40) but no script pairs the two.
  Under MEMPROFILE_CERT=compute-1g, which the profile sets for every Compute
  family (deploy/profiles/compute-1g.env:102), a disabled compactor refuses
  boot (src/config/profile.rs:126-131); the 2026-09-24 effective-config
  evidence shows history.compactor_off false in all 12 families on both sides.
  No test sets it to true; the only pin is the default
  (src/config/tests.rs:226). Without the certificate the variable is accepted
  silently today, which is an argument for removal. After removal the branch
  at profile.rs:127-131 stays type-reachable (compactor_options is an Option)
  and can remain as a guard.
- Must change with it:
  src/config/load.rs:105-111; src/config/model.rs:142-143 and :406;
  src/config/summary.rs:41 (startup summary key); src/config/tests.rs:226;
  src/history.rs:460-464 and :507-519; a 'removed' entry in
  scripts/effective-config/rename-map.json (a path present on one side only
  fails K9, effective_config.py:416,:848); the three obligations that list
  src/history.rs become stale and need re-recording
  (verification/manifest.json:1927,:2449,:2792); shard settings validation and
  certification tests call history_settings but never the off branch
  (src/config/validation.rs:745-748, src/config/profile.rs:61-69). No RUNBOOK
  row exists for the name.
- What a client can observe:
  None. A deployment that set HISTORY_COMPACTOR=off (none found) would
  silently run with the compactor on; an unknown environment variable is not
  an error.
- For the owner:
  Approve deleting a bench hook you may still want for discard-mode
  benchmarks, and the re-record of three receipts. Could not determine:
  whether any Compute project still holds the variable (a certified deployment
  holding it would refuse to boot today, so a running one does not).

#### BILLING_METER

- Proposed: metering on unless 'off' -> no option; metering always on
- Verification: **holds**
- Reason:
  A diagnostic arm referenced by no script, profile or RUNBOOK table.
  validate_billing_prerequisites does not refuse it under
  BILLING_MODE=required, so a required-billing deployment can boot with ingest
  metering off.
- What the verifier found:
  Verified: read at src/config/load.rs:175, carried at src/http.rs:253,
  applied at src/application/append.rs:414; with it off the committer skips
  the billing metadata, so ingest bytes are not counted
  (src/shard/transaction/append.rs:256-279). validate_billing_prerequisites
  (src/config/validation.rs:843-863) checks only the key and the identities,
  so a required-billing deployment can boot with ingest unmetered: confirmed.
  No script, profile, family or RUNBOOK table sets it (searched bench/,
  scripts/, deploy/, RUNBOOK.md). No DST or rig uses it; the only tests are
  src/config/tests.rs:246, :324 and :337. It was a diagnostic arm of the OOM
  review (docs/OBSERVABILITY-BILLING-STATUS.md:98-105). One correction to the
  label: there is no dead path to delete. The unmetered branch stays reachable
  for reserved '_' streams (append.rs:414); only the conjunct and the field
  go. I could not break the recommendation. I could not determine whether any
  Compute project still holds BILLING_METER=off from that experiment; if one
  does, removal turns its metering on.
- Must change with it:
  src/config/load.rs:175; src/config/model.rs:201-202 and :448;
  src/config/summary.rs:60; src/config/tests.rs:246, :324, :337;
  src/http.rs:253 (zero-headroom file, a removal);
  src/application/append.rs:37 and :414 (stales TLA-003);
  docs/refactor/WIRE-MATRIX.md:45; docs/OBSERVABILITY-BILLING-STATUS.md:98.
  Not in EXPECTED_CLI_SURFACE (env-only). effective-config: a 'removed' entry
  for billing.meter_enabled in scripts/effective-config/rename-map.json and
  PIN_NEW_LEAVES 155 -> 154 (effective_config.py:74). Red-first test:
  BILLING_MODE=required with BILLING_METER=off.
- What a client can observe:
  Only for a deployment that set it: its usage records start counting ingest
  bytes. No deployment in the repository sets it.
- For the owner:
  Remove outright, or keep as an investigation arm that is refused under
  BILLING_MODE=required? WIRE-MATRIX.md:45 names the knob, so the removal
  needs a matrix update and probably an edge record.

#### HISTORY_GC_MAX_INTERVAL_SECS and --gc-max-interval-secs (legacy aliases)

- Proposed: accepted as aliases -> not accepted
- Verification: **holds**
- Reason:
  Names of an adaptive backoff that no longer exists; nothing uses either
  alias.
- What the verifier found:
  Verified: the env alias is consulted only when HISTORY_GC_INTERVAL_SECS is
  unset (src/config/load.rs:112-121); the flag alias is a hidden clap alias
  (src/config/cli.rs:187-193). git grep finds no script, family file or guide
  that uses either; outside code and tests the only mention is historical
  (docs/HISTORY-V2.md:290). The flag alias is not part of the pinned surface:
  EXPECTED_CLI_SURFACE records long name, env and default only
  (src/config/tests.rs:509-525), and no test exercises the alias. There is a
  precedent: the shard-side env GC_MAX_INTERVAL_SECS named in
  HISTORY-V2.md:290 is already not accepted, because a clap alias covers the
  flag only. Could not determine: whether any Compute project still holds
  HISTORY_GC_MAX_INTERVAL_SECS with a value other than 600; that needs the
  platform export (effective-config-diff.md:285-291).
- Must change with it:
  src/config/load.rs:113-117; src/config/cli.rs:186 (help sentence) and :191;
  src/config/model.rs:144-145 (doc); src/config/tests.rs:321 and :340-344 (the
  alias assertions in env_overlay_applies_with_legacy_parse_semantics);
  src/history.rs:473-474 (comment names the alias; src/history.rs feeds
  TLA-016, TLA-018, TLA-019, so fix the comment with the next edit of that
  file or accept a re-record); scripts/effective-config/effective_config.py:76
  (knob-count pin).
- What a client can observe:
  No. Operator-visible: a project that holds the old env name with a value
  other than 600 silently moves its history GC sweep to 600 s (LIST cadence
  and reclamation latency); --gc-max-interval-secs on argv becomes a boot
  refusal.
- For the owner:
  None beyond confirming that the platform export shows no project holding the
  old name with a different value. If one does, set HISTORY_GC_INTERVAL_SECS
  there first.


### Settings nothing sets

#### WAL_GATHER_SKIP_REQS / WAL_GATHER_SKIP_BYTES

- Proposed: 32 / 1 MiB, settable, 0 = never skip -> constants 32 / 1 MiB
- Verification: **holds**
- Reason:
  No profile, script or rig sets either; the soaked behaviour is the default.
  The DST that varies the threshold does so through ShardConfig, not the
  environment.
- What the verifier found:
  Holds. Paths under .
  A search of the whole tree, ignored files included, finds the names only in
  RUNBOOK.md:677, src/config/cli.rs:85-90 and :576-577,
  src/config/tests.rs:395-396, src/bootstrap.rs:426-435 and :520-521,
  src/shard.rs:912-913, :984-985, :1362-1363, and the DST. No profile, deploy
  script, rig or effective-config family sets either. Every rig that turns the
  gather on (WAL_POST_ACK_GATHER_MS=6, for example
  bench/soak/deploy-region.sh:174, bench/fleet/deploy-fleet.sh:134) runs the
  thresholds at their defaults.
  Behaviour stays identical: ShardConfig::default carries the same 32 / 1 MiB
  (src/shard.rs:984-985) and the bootstrap literal ends in
  ..Default::default() (src/bootstrap.rs:534). The DST sets the thresholds
  through ShardConfig, including the never-skip value
  (src/dst/tests/durability_gather.rs:165-187), so the fields stay and nothing
  becomes untested.
  The thresholds matter only with WAL_GROUP_COMMIT=1 and a gather window
  (src/shard.rs:1358-1363); both are off by default (cli.rs:61, :79).
  COST, understated in the recommendation: the reader is inside
  bootstrap::run, frozen by six rows
  (docs/quality/exception-growth.json:251-335, scope_lines 583, syntax_facts
  977). AGENTS.md section 5 says any change to such a scope, shrinking
  included, fails as a stale row, and only the owner updates it.
  src/bootstrap.rs is also a source of TLA-011
  (verification/manifest.json:1691), so that receipt goes stale, and
  src/bootstrap is mutation-critical (scripts/quality/verification_plan.py:26,
  :29; mutation_owners.py:91).
- Must change with it:
  Paths under .
  - src/config/cli.rs:82-90 (options), :576-577 (CliArgs::deterministic).
  - src/config/tests.rs:395-396 (EXPECTED_CLI_SURFACE).
  - src/bootstrap.rs:426-435 (conversion), :520-521 (literal).
  - docs/quality/exception-growth.json:251-335: the six crate::run rows, owner
  only.
  - verification/receipts/TLA-011.json: re-record (manifest.json:1691 lists
  src/bootstrap.rs).
  - RUNBOOK.md:677 (row; served by the operator runbook route,
  src/operator.rs:26, :66-82).
  - src/config/model.rs:28 (comment says 85 flags; src/config/cli.rs has 85
  #[arg] attributes today).
  - scripts/effective-config/effective_config.py:74 (PIN_NEW_LEAVES 155) and
  rename-map.json:29 (removed list) for the owner's comparison run.
  - Unchanged: src/shard.rs:912-913, :984-985;
  src/dst/tests/durability_gather.rs:184-185.
- What a client can observe:
  No. For operators: a --wal-gather-skip-reqs or --wal-gather-skip-bytes
  argument would be refused by clap at boot, and the two environment names
  would be ignored silently. No script in the tree passes either.
- For the owner:
  Approve new values for the six crate::run rows for a shrink? If yes, batch
  every removal that touches bootstrap::run into one change so the rows and
  TLA-011 are redone once.

#### TRIM_GLOBAL_BUDGET

- Proposed: 65536, settable -> constant 65536
- Verification: **did not hold**. Instead: remove-knob-keep-behaviour (constant 65536)
- Reason:
  No deployed family sets it; the three cost rigs that name it pass the
  default.
- What the verifier found:
  Does not hold as written. Paths under .
  The stated facts are right: default 65,536 (src/config/cli.rs:244), the
  three cost rigs pass 65,536 (bench/costab/run-keyed.sh:37, run-split.sh:37,
  run-mature.sh:55), and no effective-config family sets it.
  WHAT BREAKS IT. The knob is coupled to TRIM_PER_OP, which stays. The
  per-stream allowance is min(remaining global budget, TRIM_PER_OP)
  (src/shard/transaction/maintenance.rs:324 and :340; the budget is seeded at
  src/shard/transaction/mod.rs:88). With the budget fixed at 65,536,
  TRIM_PER_OP remains settable but any value above 65,536 silently does
  nothing. The documents name larger values as the throughput posture:
  COMPUTE-SPEC.md:98 says throughput shards use 256k or more, and
  src/config/cli.rs:229-233 says a pass must retire about 250k records at 50k
  records/s. Removing only the global knob removes the one way to reach that
  posture.
  OTHER DEPENDENCIES the recommendation missed: the staging plan pins the name
  on purpose (docs/STAGING.md:162; docs/COST-CAMPAIGN-2.md:424-426). The
  run-mature G1 gate takes its bound from the same variable the server reads
  (bench/costab/run-mature.sh:10, :55; mature-driver.py:31, :244), so a
  non-default MATURE_TRIM_BUDGET would then judge a server that ignores it.
  The reader is inside the frozen bootstrap::run (src/bootstrap.rs:418, :516).
  COULD NOT DETERMINE: whether 65,536 deletes per commit group keeps trim
  ahead of ingest at the stated product limit of 50k events/s (SPEC.md:367).
  RELATED FINDING: every Compute deploy script sets TRIM_PER_OP=65536
  (bench/soak/deploy-region.sh:181, mt-tenants.sh:148, wc-ladder.sh:200,
  bench/fleet/deploy-fleet.sh:137) while the binary default is 8,192
  (cli.rs:235). By the owner's rule, that default is the better candidate for
  a change.
- Must change with it:
  If the owner removes it anyway (paths under ):
  - src/config/cli.rs:229-245 (both doc comments and the option), :593
  (fixture).
  - src/config/tests.rs:424.
  - src/bootstrap.rs:418, :516, and the six crate::run rows at
  docs/quality/exception-growth.json:251-335 (owner only).
  - verification/receipts/TLA-011.json (manifest.json:1691).
  - bench/costab/run-keyed.sh:37, run-split.sh:37, run-mature.sh:10 and :55,
  mature-driver.py:31.
  - docs/STAGING.md:162; COMPUTE-SPEC.md:95-99.
  - Comments: src/http.rs:843, src/shard.rs:938-941 and :2088.
  - src/config/model.rs:28; scripts/effective-config/effective_config.py:74
  and rename-map.json:29.
  - RUNBOOK.md has no row for it.
  - Unchanged: src/shard.rs:941, :978; src/dst/tests/history_recovery.rs:137.
- What a client can observe:
  No wire change. Removal would hide a throughput ceiling: trim deletes per
  commit group could no longer be raised, which shows up as hot-tier growth
  and backpressure under sustained high ingest.
- For the owner:
  Decide the two trim knobs together: make TRIM_PER_OP default 65,536, the
  value every Compute deploy script sets, and then either keep
  TRIM_GLOBAL_BUDGET or fix both as constants. Is 65,536 per commit group
  enough at 50k events/s?

#### ABSORB_READ_PAR

- Proposed: 8, settable -> constant 8
- Verification: **holds**
- Reason:
  Only one script passes it, at the default, as an experiment variable.
- What the verifier found:
  Holds, weakly. Paths under .
  Verified: default 8 (src/config/cli.rs:304), AbsorberConfig default 8
  (src/history.rs:665), and one script passes it at the default as an
  experiment variable (bench/soak/wc-ladder.sh:202, WC_READ_PAR). Behaviour is
  unchanged for every deployment and rig in the tree. The DSTs set
  gather_read_par on AbsorberConfig directly
  (src/dst/tests/history_gather.rs:206, :211, :245, :250), so the field stays.
  The reader is absorber_config (src/bootstrap.rs:96-106), outside
  bootstrap::run, so the frozen rows are not touched. The file is still a
  TLA-011 source and mutation-critical.
  MISSED by the recommendation: (1) a test pins that the option reaches the
  absorber (src/bootstrap/tests.rs:9-45 sets 3 and 5 and asserts both). (2)
  The binary's own deprecation notice tells operators to use this name
  (src/config/notice.rs:131-136), as do RUNBOOK.md:110 and COMPUTE-SPEC.md:97.
  (3) It is a memory lever: the hardening report lists as open residue that a
  read wave holds up to gather_read_par x 4 MiB before it is funded
  (docs/reviews/2026-09-hardening/report/remaining.json:708). A constant 8
  fixes that at 32 MiB on every instance class.
- Must change with it:
  Paths under .
  - src/config/cli.rs:301-305, :604.
  - src/config/tests.rs:439.
  - src/bootstrap.rs:103.
  - src/bootstrap/tests.rs:16, :26, :39, :44.
  - src/config/notice.rs:131-136 (notice text) and
  scripts/effective-config/proposed.json:37 (row keyed by that text).
  - RUNBOOK.md:109-110; COMPUTE-SPEC.md:95-98.
  - bench/soak/wc-ladder.sh:202;
  scripts/effective-config/families/wc-ladder.family:82,
  wc-ladder-diet.family:81.
  - verification/receipts/TLA-011.json (manifest.json:1691).
  - src/config/model.rs:28; scripts/effective-config/effective_config.py:74
  and rename-map.json:29.
  - Unchanged: src/history.rs:649, :665; src/history/gather.rs:419.
- What a client can observe:
  No. Operators: the argument would be refused at boot and the environment
  name ignored silently; the startup notice wording changes.
- For the owner:
  Is 8 concurrent reads (up to 32 MiB unfunded per gather) the value wanted on
  every instance class, or should this stay a memory lever until the item 8
  residue is closed?

#### TAIL_MAX_BYTES

- Proposed: 1 MiB, settable -> constant 1 MiB
- Verification: **holds**
- Reason:
  A read-path parameter set by no deployment, script or rig.
- What the verifier found:
  Verified that nothing sets it: git grep finds the name only in
  RUNBOOK.md:674, the config code and historical documents; no family file,
  deploy script or rig. One correction: there are two readers, not one. The
  product path (src/product.rs:2507) and the raw HTTP path
  (src/http/read.rs:245 through src/http.rs:150-152). The ReadCommand field
  must stay because DSTs vary it (src/dst/tests/read_page_limits.rs:21,
  read_page_assembly.rs:70, read_application.rs:21,
  reads_applied_history.rs:439 and :558, read_subset_retention.rs:209,
  src/application/read_remote_tests.rs:156). Deleting only
  src/config/load.rs:157-159 gives the whole benefit and touches no file
  without headroom, no formal receipt and no critical mutation prefix.
  Removing the field too would edit src/product.rs (3,944 lines, feeds
  TLA-003) and src/http.rs (3,102 lines) for no further gain. Related and
  larger, outside the list: TAIL_RING_BYTES defaults to 0, which turns the
  durable-tail ring off (src/config/cli.rs:97; src/shard.rs:1299), and three
  Compute families set 0 explicitly, while
  docs/read-experiments/final-disposition.md:16 records 'O3 durable ring
  coverage: Enabled unconditionally'. That is a real on/off env switch on a
  read path and deserves the owner's look more than this page-size constant
  does.
- Must change with it:
  src/config/load.rs:157-159; src/config/model.rs:179 (doc names the env);
  src/http.rs:141 (doc 'Env TAIL_MAX_BYTES'; line-neutral edit only, or leave
  until the next edit of that file); RUNBOOK.md:674; src/config/tests.rs:236
  stays valid; scripts/effective-config/effective_config.py:76 (PIN_ENV_KNOBS
  = 70 drops by one).
- What a client can observe:
  No: the value stays 1 MiB, so a woken long-poll read returns the same page.
  Operator-visible: a TAIL_MAX_BYTES set in a project is silently unread (I
  found none in the repository; what projects hold needs the platform export).
- For the owner:
  Is a fixed 1 MiB woken-read page acceptable for every instance class? Should
  TAIL_RING_BYTES be settled first (ring permanently on with a fixed budget,
  or removed), given the read-path invariant?

#### WAL_GC_INTERVAL_SECS / WAL_GC_MIN_AGE_SECS / COMPACTIONS_GC_INTERVAL_SECS / COMPACTIONS_GC_MIN_AGE_SECS / GC_QUIET_INTERVAL_SECS / HISTORY_GC_INTERVAL_SECS

- Proposed: 30 / 60 / 30 / 120 / 600 / 600, settable, unvalidated -> constants
- Verification: **holds**
- Reason:
  No profile, script or rig sets any of them, and the values match RUNBOOK.
  Two are safety floors (min ages) with no validation, so a low setting is a
  risk with no user.
- What the verifier found:
  Holds, with one open question the recommendation did not see. Paths under .
  Verified: defaults are as stated (src/config/cli.rs:177, :190, :199, :209,
  :215; src/config/model.rs:407). A search of the whole tree finds no profile,
  script, rig, deploy wrapper or effective-config family setting any of the
  six names or the two aliases (--gc-max-interval-secs at cli.rs:191,
  HISTORY_GC_MAX_INTERVAL_SECS at src/config/load.rs:116). No validation
  covers them. The misplaced WAL GC doc comment is real: cli.rs:147-152 sits
  on compactor_poll_ms, and wal_gc_interval_secs (cli.rs:177-178) has only
  plain comments above it.
  Readers are shard_settings (src/config/validation.rs:53-92) and
  history_settings (src/history.rs:475-488), both outside bootstrap::run. The
  smallest change keeps HistoryConfig.gc_interval as a field and deletes only
  its overlay (load.rs:112-121), which leaves src/history.rs, a source of
  three formal obligations, untouched.
  CORRECTION: RUNBOOK lists four of the six (RUNBOOK.md:105-106).
  GC_QUIET_INTERVAL_SECS and HISTORY_GC_INTERVAL_SECS are documented in
  docs/OPS-RELEASE.md:16, docs/TIGRIS-404-COST.md:119-120 and
  docs/COST-CAMPAIGN-2.md:527.
  OPEN: 'no user' is true of the code but not of the operations spec.
  OPERATIONS.md:64 requires WAL objects to be kept at least 24 h past a
  checkpoint as the GC floor for backup, against the 60 s default. No backup
  code exists in src, so this is a specified future use, not a current one.
  OPS-RELEASE.md:16 also says to revisit the 600 s cadence if upstream ships
  adaptive GC.
- Must change with it:
  Paths under .
  - src/config/cli.rs:147-152 (move or delete the comment), :177-216 (five
  options and the alias), :586-590 (fixture).
  - src/config/tests.rs:409-421 (EXPECTED_CLI_SURFACE), :320-321, :334,
  :340-344 (overlay and alias assertions), :227-230 (default).
  - src/config/validation.rs:53-92.
  - src/config/load.rs:112-121; src/config/model.rs:144-146.
  - src/config/numeric_tests.rs:90-96 and src/config/summary.rs:42 only if the
  history field is removed too.
  - src/history.rs:473-474 (comment).
  - RUNBOOK.md:105, :106, :666.
  - docs/OPS-RELEASE.md:16; docs/TIGRIS-404-COST.md:119-120;
  docs/SOAK-REGIONS.md:667; docs/STAGING.md:248; docs/COST-CAMPAIGN-2.md:527;
  docs/HISTORY-V2.md:290; docs/dst/DST-EXPANSION-SPEC.md:869.
  - verification/tla/history/README.md:1409 names HISTORY_GC_INTERVAL_SECS; I
  did not determine whether that file feeds a receipt digest.
  - src/config/model.rs:28; scripts/effective-config/effective_config.py:74
  and rename-map.json:29.
  - No formal receipt lists src/config, and src/config is not
  mutation-critical.
- What a client can observe:
  No. Operators: five arguments and one alias would be refused at boot, and
  six environment names plus one legacy name would be ignored silently.
  Setting 0 to stop the quiet or history sweeps would no longer be possible.
- For the owner:
  Is the 24 h WAL retention floor in OPERATIONS.md section 2.1 still planned?
  If so, WAL_GC_MIN_AGE_SECS has a future user and should stay, with a
  validated minimum. Is a way to stop GC sweeps during an incident wanted?

#### COMPACTOR_POLL_MS

- Proposed: 2500, settable -> constant 2500
- Verification: **did not hold**. Instead: remove-knob-keep-behaviour (constant 2500)
- Reason:
  No script sets it, deliberately; the value is DST-pinned and the runbook
  says deploy scripts must not re-tighten the poll posture.
- What the verifier found:
  The facts are right: no script sets it
  (bench/soak/deploy-region.sh:151-154); the constants are pinned by a DST
  (src/lib.rs:92-99; src/dst/tests/runtime_open_gate.rs:327-328);
  docs/STAGING.md:157 and bench/sinmax-report.md:36 carry a stale 500.
  It is not unsafe, but it breaks on what depends on the knob existing:
  - Edge record #48 (docs/reviews/2026-09-hardening/edge-changes.md:1125-1140)
  made the argv flag take effect this month. Three tests pin that behaviour:
  src/config/tests.rs:107-122, :124-151 (asserts 700 ms through the
  environment) and :153-180. Removing the knob deletes ratified work and its
  red-first tests.
  - It is the only lever on compaction scheduling that needs no rebuild, and
  batch-ingest L0 stalls were found two commits ago. The 2.5 s argument rests
  on the 5 MB/s per-shard limit (src/config/cli.rs:158-161).
  - MANIFEST_POLL_MS would stay settable and is set to 1000 by
  scripts/bench-fra-ab.sh:84 and STAGING.md:156, so the poll posture would be
  half constant, half knob.
  - The stale 500 in STAGING.md is a one-line document fix.
  Related finding: STAGING.md:158 sets COMPACTOR_MAX_CONCURRENT=2, which
  MEMPROFILE_CERT refuses at boot if applied after the profile
  (src/config/profile.rs:96-100).
- Must change with it:
  If done anyway:
  - src/config/cli.rs:147-163 (the doc block on this field also holds the WAL
  GC text at :147-152, which belongs on wal_gc_interval_secs at :177) and the
  fixture :584
  - src/config/model.rs:72-75, :338-341, :375
  - src/config/tests.rs:89-105, :107-122, :124-151, :153-180, :212, :407
  - src/config/summary.rs:23
  - src/config/load.rs:62 (comment)
  - edge-changes.md: a new record that reverses #48
  - scripts/effective-config/effective_config.py:74 and rename-map.json
  ('removed' for cli.compactor_poll_ms and engine.compactor_poll_ms)
  - RUNBOOK.md:104, docs/STAGING.md:157
  Recommended instead: fix docs/STAGING.md:156-158 and the stale 'L0_MAX 64'
  in cli.rs:160 and RUNBOOK.md:104.
- What a client can observe:
  Not on the wire. Operators: '--compactor-poll-ms' on argv becomes a boot
  refusal, and a COMPACTOR_POLL_MS held in a project's environment is silently
  ignored. A reused project that still holds the old 500 would move to 2500,
  which lowers the probe rate and lengthens the compaction scheduling gap.
- For the owner:
  Reverse edge record #48 for the sake of one option? I recommend no: keep the
  knob and correct STAGING.md.

#### HANDLE_IDLE_EVICT_SECS

- Proposed: 600, settable, 0 refused under enforce -> constant 600
- Verification: **holds**
- Reason:
  Nothing sets it, and the only special value (0) is refused in the production
  auth mode.
- What the verifier found:
  Paths relative to the repository root. Verified: default 600
  (src/config/cli.rs:269-274, fixture :599, pin src/config/tests.rs:430); 0 is
  refused only under STREAMS_AUTH_MODE=enforce
  (src/config/validation.rs:727-736); a repository-wide search finds no
  script, bench, deploy file or CI job that sets it, and the 2026-09-24
  evidence shows 600 in all 12 families. HANDLE_MAX_RESIDENT stays
  (wc-ladder.sh:72). Points the recommendation omits: (1) the refusal is a
  recorded owner decision, 'Item 50, option (a): ... reject
  HANDLE_IDLE_EVICT_SECS=0' (docs/reviews/2026-09-hardening/README.md:173),
  pinned by validation_rejects_never_evicting_handles_under_enforce, which
  also asserts that off mode keeps 0 valid
  (src/config/validation_tests.rs:602-618). Removing the knob reaches the same
  safety more strongly but deletes that refusal and its test, and removes the
  off-mode ability to never evict. (2) Both readers are inside bootstrap::run
  (src/bootstrap.rs:415,:523), a scope whose exception-growth rows are frozen
  exactly (docs/quality/exception-growth.json:252-319); any edit, shrinking
  included, fails as 'stale exception growth row' until the owner updates the
  rows. (3) src/bootstrap.rs is an input of TLA-011
  (verification/manifest.json:1681-1691), so that receipt goes stale. (4)
  Removing a flag fails cli_surface_is_pinned, whose message calls it a
  product decision. ShardConfig.handle_idle_evict stays for tests
  (src/shard.rs:926,:976,:1603).
- Must change with it:
  src/config/cli.rs:269-274 and :599; src/config/tests.rs:430;
  src/config/validation.rs:727-736; src/config/validation_tests.rs:602-618;
  src/bootstrap.rs:415 and :523; owner-updated rows for crate::run in
  docs/quality/exception-growth.json:252-319; re-record TLA-011; a 'removed'
  entry in scripts/effective-config/rename-map.json (K9); the Item 50 row in
  docs/reviews/2026-09-hardening/README.md:173 and the Item 50 note in
  NEXT-WORK.md:604; docs/COST-CAMPAIGN-2.md:266 mentions the variable
  (history).
- What a client can observe:
  No wire change. For operators: the environment variable would be ignored
  silently, and --handle-idle-evict-secs on argv would make clap refuse to
  start. No known deployment passes either.
- For the owner:
  Do you want to replace your Item 50 decision (refuse 0 under enforce) with
  'there is no such option', and approve the updated growth rows for
  bootstrap::run and the TLA-011 re-record for a saving of one option? Could
  not determine: whether any Compute project holds the variable, and whether
  the multitenancy audit fingerprints any of the touched lines (scripts were
  not run).

#### SHARD_OPEN_DEADLINE_MS / SHARD_OPEN_WAIT_MS / UNREADY_EXIT_AFTER_SECS

- Proposed: 180 s / 10 s / 300 s, settable -> constants
- Verification: **did not hold**. Instead: Keep all three. No deployment has to set them, so removing them simplifies no deployment, and they are the only remedy for a slow-open loop that does not need a rebuild.
- Reason:
  No deployment or rig sets them.
- What the verifier found:
  Defaults and readers verified: src/config/load.rs:80-90,
  src/config/model.rs:386-393, pinned at src/config/tests.rs:219-221. No
  script, profile or family sets them (searched), so the premise is true and
  is also the reason the gain is nil: three names nobody has to write. What
  breaks it: the open deadline exists because a replay is 'legitimately
  minutes long on a bad day' (src/sharddir.rs:55-61,
  docs/SOAK-REGIONS.md:540-546), and the watchdog exits the process after 300
  s unready (sharddir.rs:160-198). With constants, a keyspace whose replay
  needs more than 180 s would deadline, strike, exit at 300 s and repeat on
  every restart, and the only fix would be a new binary during the incident.
  UNREADY_EXIT_AFTER_SECS=0 is the documented way to disable the watchdog
  (model.rs:123, docs/CHAOS-R23.md:169-173). The DST HTTP rig takes both
  timings from the config (src/dst/tests/fixture_http.rs:489-490), and the
  effective-config controls K2 and K8 use SHARD_OPEN_WAIT_MS as their
  sensitivity probe (scripts/effective-config/effective_config.py:791-794 and
  :833-842).
- Must change with it:
  If done anyway: src/config/load.rs:27 and :80-90; src/config/model.rs:34,
  :116-125, :386-394; src/config/summary.rs:31-35;
  src/config/tests.rs:219-221; src/config/numeric_tests.rs:89-92;
  src/bootstrap.rs:561-562 and :712-717 (inside the frozen bootstrap::run;
  TLA-011); src/sharddir.rs:55-57, :99-101, :160-168;
  src/dst/tests/fixture_http.rs:489-490;
  scripts/effective-config/effective_config.py:791-794 and :833-842 (K2 and K8
  need another probe knob), PIN_NEW_LEAVES and rename-map.json 'removed'
  entries; docs/SOAK-REGIONS.md:540-541; docs/CHAOS-R23.md:169-170.
- What a client can observe:
  Not in normal operation. In a slow-open incident they decide how long a
  request waits for a shard before its 503, when /health turns unready and
  when the process exits.
- For the owner:
  None needed if they stay. If the owner still wants fewer names: which of the
  three, given that the watchdog's off switch and the open deadline are
  incident controls?

#### METRICS_INTERVAL_SECS / MONTH_CLOSE_GRACE_MS / SWEEP_DISCOVERY_MAX / SWEEP_MAINT_RESIDENT / SWEEP_RESIDENT_QUANTUM / ALERT_USAGE_OUTBOX_DIRTY

- Proposed: 15 / 24 h / 8 / 2 / 4 / 1000, settable -> constants
- Verification: **holds**
- Reason:
  No deployment or rig sets them. As a constant SWEEP_MAINT_RESIDENT also
  loses its refused value 0.
- What the verifier found:
  Verified that nothing sets them: git grep over the whole repository finds
  the six names only in RUNBOOK.md:127-129 and :786, the config code and
  tests, the effective-config K10 probe (which sets SWEEP_MAINT_RESIDENT=0 on
  purpose) and planning documents. The values are live:
  src/billing/telemetry_loop.rs:59, src/billing.rs:1391,1641,1713,1764,
  src/ops.rs:789-791, and DSTs read them through the config
  (src/dst/tests/billing_walk_custody.rs:42, billing_closure_debts.rs:373). I
  could not break the claim, but the cost is larger than 'six names'.
  SWEEP_MAINT_RESIDENT=0 is the repository's standard example of a boot
  refusal: src/config/validation.rs:716-726,
  src/config/validation_tests.rs:593-600, the collected-problems test at
  :695-707 (one of its four markers), src/config/tests.rs:319 and :333, the
  K10 boot control (scripts/effective-config/boot.py:277, 313-323) and the
  tool's unit-test data (test_effective_config.py:297-310, which
  scripts/quality.sh:58 runs). Deleting only the overlay lines avoids editing
  src/billing.rs (2,054 lines) and src/ops.rs, where evaluate_alerts and
  collect_snapshot hold frozen growth rows
  (docs/quality/exception-growth.json:349-385). The gain is smallest exactly
  where operators would want a lever in an incident: the residency bound (R28)
  and the alert threshold, which scales with fleet size.
- Must change with it:
  src/config/load.rs:183-188 and :192-207; src/config/model.rs:213-227 (docs
  name the env); src/config/validation.rs:716-726 (the refusal becomes
  unreachable from the environment); src/config/validation_tests.rs:593-600
  and :695-707; src/config/tests.rs:319,333; src/billing.rs:1711 (comment
  'Validated at startup'); src/ops.rs:599 (comment);
  src/dst/tests/runtime_sweep.rs:134,220 (comments name the env);
  RUNBOOK.md:127-129 and :786; scripts/effective-config/boot.py:277,313-323
  (K10 needs another refusal); test_effective_config.py:297-310;
  effective_config.py:76 (PIN_ENV_KNOBS). The startup summary keys stay if the
  fields stay (src/config/summary.rs:63-69).
- What a client can observe:
  No, while the values are unchanged. MONTH_CLOSE_GRACE_MS decides when a
  month's usage closes, so any later change of that constant is a billing
  change. Operator-visible: six env names are silently unread, and
  SWEEP_MAINT_RESIDENT=0 changes from a boot refusal to being ignored (the
  default 2 applies).
- For the owner:
  Which of the six are incident levers you want to keep? My reading:
  METRICS_INTERVAL_SECS, SWEEP_DISCOVERY_MAX and SWEEP_RESIDENT_QUANTUM are
  safe to fix; ALERT_USAGE_OUTBOX_DIRTY scales with fleet size;
  MONTH_CLOSE_GRACE_MS is billing semantics; removing SWEEP_MAINT_RESIDENT
  also removes the K10 boot control, which needs a replacement.

#### REBALANCE_MOVE_COOLDOWN_SECS

- Proposed: 60, settable -> constant 60
- Verification: **holds**
- Reason:
  Nothing sets it; its siblings REBALANCE_LAG_SECS and REBALANCE_RETURN_SECS
  are used by rigs and stay.
- What the verifier found:
  Holds in its smallest form; the evidence is thin, as the recommendation
  says. Paths under .
  Verified: default 60 (src/config/model.rs:482), overlay at
  src/config/load.rs:216-218, one reader (src/fleet.rs:424, used at :847-849).
  No script, rig or effective-config family sets it. The canary and the fleet
  certification turn rebalancing off through REBALANCE_LAG_SECS and
  REBALANCE_RETURN_SECS (bench/canary/livefeed-canary.mjs:110-111,
  bench/fleet/livefeed-cert.mjs:97-98), never through the cooldown.
  CORRECTION to 'nothing sets it': the staging plan lists it at the default
  (docs/STAGING.md:184).
  HOW TO DO IT: delete only the overlay and keep
  FleetConfig.rebalance_move_cooldown_secs as a field. That leaves
  src/fleet.rs untouched. The file is a TLA-011 source
  (verification/manifest.json:1688), is mutation-critical
  (scripts/quality/verification_plan.py:29-30; mutation_owners.py:173) and has
  1,011 lines, so it sits under the file ceiling rule. Removing the field as
  well would cost a receipt re-record and a mutation leg for one name.
- Must change with it:
  Smallest form (paths under ):
  - src/config/load.rs:216-218.
  - src/config/model.rs:241 (doc comment names the variable).
  - RUNBOOK.md:237.
  - docs/SCALING.md:154.
  - docs/STAGING.md:184.
  - Unchanged: src/config/tests.rs:261, src/config/summary.rs:76,
  src/fleet.rs:424, and the effective-config leaf count (the field stays).
  - Not affected: EXPECTED_CLI_SURFACE and CliArgs::deterministic
  (environment-only name).
  If the field is removed too: src/fleet.rs:424 and :847-849,
  src/config/model.rs:242 and :482, src/config/summary.rs:76,
  src/config/tests.rs:261, verification/receipts/TLA-011.json,
  scripts/effective-config/effective_config.py:74 and rename-map.json:29.
- What a client can observe:
  No. The environment name would be ignored silently.
- For the owner:
  Owner preference only: is a lever against shard move churn wanted for
  incidents? The ladder's ping-pong was fixed by the healthy-target guard, not
  by this value (src/fleet.rs:850-857).

#### SSE_H1_MAX_BUF

- Proposed: 64 KiB, settable, below 8 KiB refused -> constant 64 KiB
- Verification: **holds**
- Reason:
  Only one local probe sets it, at the default. The name says SSE but the
  value bounds every h1 connection.
- What the verifier found:
  I could not break this one; it reverses a recorded edge change, so the owner
  ratifies.
  Verified:
  - Default 64 KiB (src/config/model.rs:187-190, :439); overlay at
  src/config/load.rs:165-167; values below 8 KiB refused
  (src/config/validation.rs:798-804; model.rs:473).
  - The value bounds every h1 connection, not only SSE: one builder serves all
  (src/http/serve.rs:1-5, :34-40).
  - The only setter is a local probe whose default is the binary's
  (bench/sse-probes/sse-1per.sh:23, SSE_H1_MAX_BUF=${H1BUF:-65536}). The probe
  exists to vary the buffer; after removal its H1BUF lever does nothing,
  silently.
  - The measurement that chose 64 KiB is in the tree: a 16 KiB cap bought
  about 1.3 KB per connection, 'so 64 KiB stands'
  (bench/WORKLOAD-CERT-PLAN.md:495-501).
  - The claim about SSE_H1_HEADER_TIMEOUT_MS matches the owner's open item
  (docs/reviews/2026-09-hardening/effective-config-diff.md:293-297).
  Before and after, with or without the profile: the same 64 KiB on every
  connection.
  Dependencies on the knob existing:
  - Edge record #32 (docs/reviews/2026-09-hardening/edge-changes.md:880-894)
  records the boot refusal and names two pinning tests:
  validation_rejects_an_h1_buffer_below_hypers_floor
  (src/config/validation_tests.rs:714-719) and
  the_validated_buffer_floor_is_the_one_hyper_asserts
  (src/http/serve.rs:354-372). Both would be removed or rewritten.
  - The effective-config tool uses SSE_H1_MAX_BUF=4096 as refusal control K6
  (scripts/effective-config/effective_config.py:818-821) and needs another
  refusal.
  - src/http/serve.rs is a registered mutation owner
  (scripts/quality/mutation_owners.py:97), so CI runs its mutants on the
  change.
  None of these files is in a formal receipt, and bootstrap::run does not read
  the field.
- Must change with it:
  - src/config/model.rs:187-190, :439, :468-474
  - src/config/load.rs:165-167
  - src/config/validation.rs:789-804 (doc and check)
  - src/config/summary.rs:55; src/config/tests.rs:240
  - src/config/validation_tests.rs:711-719
  - src/http/serve.rs:1-5, :30-39, and the test at :351-372
  - docs/reviews/2026-09-hardening/edge-changes.md: a record superseding #32
  - scripts/effective-config/effective_config.py:818-821 (K6) and :74 (leaf
  count), rename-map.json ('removed': http.h1_max_buf);
  test_effective_config.py:59-64 and :82-84 use the name only as parser
  fixture text and can stay
  - bench/sse-probes/sse-1per.sh:23
  - bench/WORKLOAD-CERT-PLAN.md:497 names the variable
  Not affected: EXPECTED_CLI_SURFACE and CliArgs::deterministic (env-only
  name); the profile; certification; RUNBOOK.md (no row).
- What a client can observe:
  No client effect at 64 KiB: the request-head bound stays where the default
  has it. Operators: a value below 8,192 no longer stops the boot, it is
  ignored; the startup summary loses http.h1_max_buf.
- For the owner:
  1. Ratify the reversal of edge record #32 (a boot refusal that disappears
  with its knob)?
  2. Which refusal should replace K6 in the effective-config controls?
  3. SSE_H1_HEADER_TIMEOUT_MS stays until the edge idle-timeout question is
  decided; should it be renamed then, since it also bounds every connection?

#### --ops-bucket / --shard-bucket / --data-bucket

- Proposed: argv-only per-role bucket overrides -> every role uses SLATE_S3_BUCKET
- Verification: **holds**
- Reason:
  They have no env name, the Compute supervisor passes only --listen, and no
  script uses them, so no deployment can set them today.
- What the verifier found:
  Verified: argv only, no env name (src/config/cli.rs:23-28; pinned with empty
  env at src/config/tests.rs:384-386). I read the supervisor myself:
  deploy/app-server/index.ts:98 passes exactly ["--listen", "0.0.0.0:<port>"]
  and the process env. No script, family, harness or guide passes the flags
  (searched the tree; the only mentions are RUNBOOK.md:78 and
  docs/STAGING.md:83-84). With all three unset every role resolves to --bucket
  (src/bootstrap.rs:26-27), so behaviour is kept. I could not break it inside
  the repository. Limits: I cannot see deployments outside it; one that passes
  a role flag would fail at boot on an unknown argument, and one that really
  used separate buckets would lose sight of its data. The cost is understated
  in the entry: the three store constructions are inside bootstrap::run
  (bootstrap.rs:214-216), whose exception rows are frozen
  (docs/quality/exception-growth.json:252-333), so the owner must update those
  rows even though the function shrinks, and bootstrap.rs feeds TLA-011.
- Must change with it:
  src/config/cli.rs:20-28 and :565-567; src/config/tests.rs:384-386;
  src/bootstrap.rs:26-27, :53-57 (signatures) and :214-216 (frozen scope;
  receipt TLA-011); src/bootstrap/tests/provider_contract.rs:162-170;
  RUNBOOK.md:78; docs/STAGING.md:83-90 and :118-119. effective-config:
  'removed' entries for cli.ops_bucket, cli.shard_bucket, cli.data_bucket in
  scripts/effective-config/rename-map.json and PIN_NEW_LEAVES minus three. The
  startup canary and its log line name three buckets (bootstrap.rs:284,
  docs/CHAOS-R23.md:166); keeping behaviour means leaving the three probes.
- What a client can observe:
  No. The --help output loses three flags; a process started with one of them
  refuses to boot.
- For the owner:
  Is the three-bucket layout of docs/STAGING.md:83-90 (separate shard, data
  and ops buckets) still the plan for production? If yes the flags need env
  names instead of removal. Does any deployment outside this repository pass
  them?
