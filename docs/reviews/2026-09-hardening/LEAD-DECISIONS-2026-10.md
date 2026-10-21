# Lead decisions, unsupervised week 2026-10-17 to 2026-10-24

The owner, 2026-10-17 (verbatim): "Your goal is to bring this project to the
public beta stage. Please make all decisions yourself, and only ask Fable 5.1
when you absolutely need external review. Don't wait for me to make any
decisions. Instead document your decisions so I can review them later."

Every decision below is the lead's, taken under that delegation, and is open
to the owner's reversal on return. Each one that AGENTS.md section 1 reserves
to the owner is marked **[owner-reserved, delegated]** and is also written as
a row of the README "Owner decisions" table with the words "lead, under the
owner's delegation of 2026-10-17", so it is findable in the repository.

## How the delegation is read

- D0.1 **Reserved categories are decided, not skipped** [owner-reserved,
  delegated]: edge-record ratification, exception-growth rows, gate-selection
  changes and release holds are decided by the lead this week and recorded as
  above. Edge records are still written in full, so a reversal is a revert.
- D0.2 **Not taken even under the delegation**, because they are
  irreversible or act outside the owner's own accounts:
  - publishing `@prisma/streams` to the public npm registry (a published
    version cannot be withdrawn after 72 h): built, dry-run, and left one
    command away;
  - uploading raw logs or evidence anywhere (standing hold);
  - any deploy into a production workspace (none exists: item 8 is the
    owner's "I will provide this later");
  - spending beyond the field budget the owner approved on 2026-10-12 (D2:
    $25-40 now, $60-100 at the RC), in the scratch project D1 approved.
- D0.3 **Fable 5.1** is used only for the review of each landing stack (the
  standing practice) and for no design decision.

## Scope of "public beta" for this week

- D1.1 **Layout 6 cannot be finished this week, and the owner's "layout 6
  first" is not reversed.** The latest estimate is 65-101 engineer-days to the
  cutover (l6/verdict-challenge.json). The week delivers: (a) every other
  public-beta item that does not need the owner or Prisma (NEXT-WORK "Public
  beta" items 3, 4, 6, 7, 10, 13, 14, 17 and the should-haves) landed and green;
  (b) a release candidate of the current layout certified end to end on the
  scratch project (deployed cell, issuer, watch, SDK), so that a working beta
  exists to admit users to the day the production target exists; (c) layout 6's
  Gate S measurements and L3 (a) staging, so package 1 can start. Whether the
  beta opens on that layout-5 RC (with a data-reset notice at the cutover) or
  waits for layout 6 is the owner's call on return; this week's work is useful
  either way.
- D1.2 **Pacing.** The previous session stopped on the weekly usage limit on
  2026-10-15 with ten workflows in flight. This week runs at most three
  concurrent agents, one heavy leg per machine (the measure lock), and lands
  work before starting new work.

## Open owner calls, decided

Round 7 (owner-calls-7.txt, the L3 (a) staging plan), all as recommended:
- D2.1 Q1 [owner-reserved, delegated: gate selection]: (a) the cutover push's
  mutants width is computed per push (8-48 runners); the nightly stays 8.
- D2.2 Q3: defer the per-process ring budget, 8 MiB cap and floor to the
  cutover; P3 stages only never-evict-unabsorbed.
- D2.3 Q4 [owner-reserved, delegated]: re-pin the two absorb-retirement
  mechanism tests now under L2 (a), and again at the cutover.
- D2.4 Q5: rehearse the cutover's mutation leg with one draft PR from the
  layout-6 branch, never merged (a one-off exception to the no-PR rule).
- D2.5 Q6: a zero-lag counterexample in an absorber mechanism moves D6 back
  into the cutover; reported, not re-asked.

Round 5 leftovers (owner-calls-5.txt, never put on a page):
- D2.6 The free-tier twins' refusals count in compliant-refusals (stricter
  evidence; no leg, bar or selection change): accepted.
- D2.7 The twins as built (s1-free 0.1 batches/s of 410 records, n0-free
  0.1 appends/s, both --stagger 1, victim-free 2/s) instead of the literal
  shapes that would fail compliant-refusals in the field: accepted.
- D2.8 The twins' setup refusal (first create weighed 21) is settled by the
  owner's round-6 call 2 (seed walk uncharged, every create weighs 11), built
  in r4-rust 29aabde6; deploy commit 9d9071b6's "weighs 21" text is corrected
  at landing.

## Decisions taken during the week

(appended as they are taken: date, decision, why, where recorded)
- 2026-10-17 22:30. Landing-3 (4ea2007d) went red in CI's sdk-package Bun step only
  (runbook.test.ts matched a NEXT-WORK phrase that fbbbdbf4 re-wrapped); the commit
  gate runs no Bun tests. Fixed by 5b639a55 (the test reads the section as words).
  Rule adopted for every landing this week: run `bun test ./deploy` (CI's deploy
  line) and the SDK suite beside the gate before pushing. No CI rerun requested.
- 2026-10-17 23:00. beta-docs findings taken as beta work (lead): (1) the wrapper's
  download-failure body on the public port becomes generic; (2) a hidden canary
  credential gets a revoke path; (3) workflow permissions + jsonwebtoken triage in
  the repo (security-scanning should-have). Recorded as known beta limitations, not
  fixed this week: USAGE_STREAM_KEY cannot be rotated (its ledgers check the key's
  fingerprint); deletion writes a tombstone and reclaims no objects (E11, layout 6
  stage 3b); operator-unreadability is at rest only (page headers, descriptors and
  the usage rollup are plaintext); a leaked data-bucket key can delete all data
  (durability posture, item 18, needs the production buckets).
- 2026-10-18. Reviews this week use an Opus reviewer agent, not Fable, under the
  owner's "only ask Fable 5.1 when you absolutely need external review"; Fable is
  kept for a design call the lead cannot settle from the code.
- 2026-10-18. Landing-4 (b673bb04, 90 commits) lands with its review's five lows
  open (landing4-review.md); they are fixed in landing-5 with the hygiene and
  beta-docs lanes: L2 (concurrent first creates each run an uncharged seed walk;
  fix: one walk per project) is the one with cost weight, the rest are texts and
  test strength. L3: no unsafe/libc is added to observe the backlog; the portable
  test is renamed to what it checks.
- 2026-10-18. Landing-4's CI mutants leg missed 5 and timed out 1 (Q0 allocator and
  block cache, ops::append_metrics); landing-5 is CI_GREEN. No rerun requested: the
  survivors are real gaps, fixed red-first in the mutfix3 lane (tests, or removing
  equivalent code), landed before the RC is cut.
- 2026-10-18. Beta version 0.3.0-beta.1 (server crate and @prisma/streams); RC tags
  v0.3.0-beta.1-rc.<n> per publish-sdk.sh. v0.1-v0.15 tags in the repo are SlateDB
  upstream's, so a plain v0.3.0 GA tag would collide: the GA name stays open.
- 2026-10-18. jsonwebtoken 9.3.1 -> 11.1.0 (aws_lc_rs backend, already linked via
  rustls), closing GHSA-h395-gr6q-cpjc; claim checks unchanged.
- 2026-10-18. The beta RC is cut on storage layout 5 (D1.1): the SDK changelog says
  layout 5; the layout-6 cutover will be a data reset for any beta data, stated in
  BETA-DATA-POSTURE/terms when the owner decides to open on this RC.
- 2026-10-18. Flaky: dst history_recovery::a_large_record_absorbs_after_restart_under_
  default_policy failed once in the release gate (left (0,1) right (1,1)), 5/5 alone;
  one gate rerun; fixed in its own lane (test-side ordering of the dirty-index delete
  vs the absorbed watermark).
- 2026-10-18 10:45. 898ef1b3 red in CI's sdk-package (bench/k2cost test_shared_verdict
  drove rc-certify's verify_arm with 3 args after 7813e308 added the tag-commit
  argument and the profile fence). Fixed by 0f74f260 (fixture uses the tag tree's
  profiles). Second red of this kind this week -> every landing now runs
  ~/.streams-k2/bin/ci-local.sh <worktree> (CI's non-Rust unit steps) beside the gate;
  CI_LOCAL_OK required before push. No CI rerun requested.
- 2026-10-18 11:05. slate 590b0073 CI_GREEN = the beta RC commit for the scratch
  deployment. The scratch deployment (cell, issuer, pager stand-in, watch; owner's
  Personal workspace, eu-central-1, D1/D2) is LEFT RUNNING after verification so the
  owner returns to a working beta (~$2-4/day, inside D2's $25-40); teardown commands
  are in FIELD-REPORT.md. An annotated RC tag may be pushed only if cut-release
  requires it. Customer credentials come from cell-admin until Prisma Auth registers
  the OAuth client (C1, outside the team).
- 2026-10-18 13:30. FIELD BLOCKER: the Compute/Composer edge in front of
  *.fra.prisma.build strips every REQUEST header whose name starts with `Prisma-`
  (any case, h1 and h2); response headers pass. Prisma-Encryption-Key never reaches
  the cell, so no stream can be created (400 missing_key). The 2026-09-30 pump A/B on
  the older compute-cli deploy path sent it fine, so this is the Composer route.
  Decision (lead, under the delegation; edge change): every client REQUEST header
  the server reads under `Prisma-<Name>` is also accepted as `Streams-<Name>`
  (same value rules; both present with different values -> 400), the SDK and the
  harnesses send the `Streams-` form, docs/BETA-REFERENCE name it first. Response
  headers keep their `Prisma-` names. Chosen over (a) waiting for Platform (owner/
  Prisma, outside this week) and (b) a full rename (544 sites; the owner may get the
  edge fixed, and reverting a rename is as large). Not `Stream-<Name>`: that is the
  Durable Streams protocol's namespace and the server refuses unknown fields there.
  Reversal: drop the alias and the SDK change. To raise with Platform: Composer
  strips customer `Prisma-*` request headers.
- 2026-10-18 18:30. Issuer B8 internal adversarial pass done (issuer-b8-report.md):
  H1 (unbounded failure table, unauthenticated DoS), M2 (per-address lockout across
  tenants once K4 exists) and M3 (SDK sent credentials to http: issuers; edge #174)
  fixed. Accepted for the beta, listed in NEXT-WORK item 3: no sign-out (8 h session
  after a member leaves a workspace, once C1), sign-in budget spendable by one client
  until K4, no HSTS, audit rewritable by the bucket key holder, allow-list read retried
  per request while the bucket fails, limits reset on redeploy, workspace:admin consent
  scope. The per-address 10/s exchange budget is removed (per-credential 2/s and
  instance 200/s remain), departing from the issuer design's per-IP table.
- 2026-10-19 01:00. Field round 2 (FIELD-REPORT.md): the Composer edge strips
  `Prisma-*` RESPONSE headers too (round 1's "responses pass" was wrong): reads lose
  Prisma-Next-Cursor / -Up-To-Date / -Sealed, so SDK paging and long-poll and the
  canary's position fail. Decision (edge change, same family as #173): every
  `Prisma-<Name>` response header is also sent as `Streams-<Name>` (and exposed in
  CORS); SDK, canary, cell-check and harnesses read `Streams-` first.
- 2026-10-19 01:00. Restart window under writes measured 7.1-9.0 s per restart/deploy
  (bar 10 s, owner call 10) but 1 of 1,846 SDK calls failed: the SDK's replay budget
  (1+2+4 s) is shorter than the window. Decision (SDK edge change): a refused request
  answered with a retryable 503 carrying Retry-After is replayed, honouring Retry-
  After, until 15 s have passed since the first attempt (plain appends included only
  for refusals the server states were not applied, never for unknown outcomes, #147).
- 2026-10-19 01:00. A field credential was once placed on a local curl argv; it was
  revoked and replaced (field agent). Row 16 (storage NOTIFY) opens ~every 10 min on
  the idle scratch cell while the cell's totals show 0 errors: investigated as a watch
  rule defect.
- 2026-10-19 07:00. Field round 3 on e94a8fd3 (rc.4): SDK round trip, paging,
  long-poll, SSE, canary, row 16 quiet, k2cost audit clean, 0 acked records lost.
  Restart leg still fails call 10's zero-SDK-error bar (2/1,889). Decisions (edge
  changes): (1) the product read path's 503 temporarily_unavailable (resolve/remote
  arm) carries Retry-After: 1 like the append path, so #177's ride-through covers
  reads; (2) a graceful stop settles in-flight appends before the process exits (new
  requests refused 503 temporarily_unavailable + Retry-After, nothing applied; in-flight
  appends run to their commit or a stated refusal within the stop grace), so a planned
  restart or deploy never answers outcome_unknown; (3) a fresh cell watch resolves
  every rule key on its first clean check, so an incident opened by its predecessor
  closes (operator-only).
- 2026-10-19 03:40. d4 lane: #178 (read 503s carry Retry-After: 1) and the fresh-watch
  resolve built. #179 as first framed is NOT the cause: the outcome_unknown and the
  7-9 s window come from Compute running old and new processes side by side during a
  deploy/restart; each fences the other and reopens after its holdoff (ping-pong).
  Decision: a process fenced by a newer DB client stops re-taking that shard and
  reports itself unready (edge change, designed first: FENCE-YIELD-DESIGN.md), plus a
  bounded graceful drain on SIGTERM. Heavy local work paused until the Gate S 6 h soak
  ends (~01:30Z) so its points stay valid; earlier agent builds during the soak
  (18:25-20:30Z) may VOID some of its minutes (the Gate S agent judges).
- 2026-10-19 10:30. [owner-reserved, delegated: a gate pin] The effective-config
  new-leaf pin moves 138 -> 139 for HttpConfig.stop_drain (#180's drain bound; a
  model field with no env variable; production 10 s, DST rigs 0). No env knob added.
- 2026-10-19 13:30. GATE S RESULT (gate-s-report.md, l6/GATE-S.md): two stop rules
  fired: S2 at Stage 8 / 1,000 streams 0.232-0.236 (> 0.2; VOID-on-pacing runs whose
  counts agree, accepted for counts under §0), and call 9's single-record checks (S10
  beta shape 1.14-1.19 provisional; 6 h soak last/first 1.68 in uploads, 1.06 in
  dollars; tombstone compaction, not leaked values). Under the owner's L5 (2026-10-12)
  "layout 6 first" loses its object; under call 12 package 1 may not start.
  Decision (lead): the public beta opens on storage layout 5 (the RC line already on
  slate); layout 6 is PAUSED, not abandoned. Caveat for the owner: 23 of 25 judged
  runs were VOID (host load 6-19 from this week's concurrent lanes), so the lead
  recommends a confirming re-run of the Stage-8 1,000-stream point and S10 on an idle
  host before any final layout-6 decision, and the call 10 compactor levers
  (tombstone drop, longer seal window) as the first engineering if layout 6 resumes.
  L3(a) layout-5 staging (P1-F, P2) is not started this week: it served layout 6.
- 2026-10-19 13:30. A fleet-only timing DST (fleet_drain::a_planned_drain_hands_every_
  shard_to_its_peer_before_the_stop) failed once in the mutfix5 gate (5/5 alone);
  one gate rerun, reported.
- 2026-10-19 17:00. Field round 4 on 81c41a3f (rc.5): call 10's restart bar PASSES
  (LEG_PASS, 0/2,025 SDK calls failed, raw gap <= 4.0 s, 0 acked records lost; a
  normal deploy and a rollback+forward with 0 failures). Compute stops a superseded
  (503) old instance by itself within 2-6 s via SIGTERM. Follow-ups taken: the old
  process's shutdown usage drain is fenced (billcap lane: prove no unit lost), and
  watch row 4 counts the outgoing boot id (fix: a deploy + restart within 30 min is
  not a crash loop).
- 2026-10-19 20:30. LAYER B (rehearsal, 73cc5310, 1,000 projects, 1 h 47 min): FAIL.
  Main cause, a product defect (NEXT-WORK item 1, Q0): RSS climbs 49 -> 322 MB in
  setup and crosses the 500 MB shed line ~700 s into the load, plateauing at 537-545
  MB; the cell then refuses nearly every append 429 rate_limited (1.76 M), its own
  telemetry included. Secondary, harness/host: the Mac's ephemeral ports run out
  under the hostile open-loop producers (EADDRNOTAVAIL ~35 min), which explains the
  H11t/probes/audit shortfalls (no leak: 0 foreign/bad-MAC/unmarked records).
  Billing equals the ledger 1,000/1,000. Decision: the memory-under-load defect is
  now the beta's top blocker: heap-profile the Layer B shape, find what holds the
  ~540 MB, fix it (or, if it is caches working as designed, size the shared-cell
  profile so its working set sits well under the shed line), then re-run Layer B
  with the driver in a Linux container (no port exhaustion). The shed line itself is
  not moved before the working set is understood.
- 2026-10-20 06:00. Memory under load (mem-report.md): the 540 MB plateau is the
  designed working set (block caches 128+32+16 MiB, full from setup, plus ~175 MB of
  allocator pages their churn holds; live heap 283-290 MB); nothing grows with
  projects or streams. Decision [capacity, delegated]: the shared-cell profile sizes
  its caches at SHARED_CACHE_BYTES=32 MiB and HISTORY_CACHE_BYTES=16 MiB (compute-1g
  and ADMIT_RSS_SHED_MB unchanged). Same reproduction: compliant-phase RSS max 541 ->
  394 MB, memory sheds 719,105 -> 0; Q0 idle +41.3 MB vs the +40 bar (within noise:
  the before-run shed 60% of writes). Risk recorded: smaller caches may raise store
  GETs per second (217-357 GET/s in both arms, not separable); the field cost arm
  should price it. No edge record (no client-facing limit changes); no connection cap.
- 2026-10-20 06:30. Field round 5 on c5cad2f2 (rc.6): metering exact across a restart
  on every meter; row 4 quiet on deploy+restart; predecessorSuperseded true in 7 s;
  restart leg LEG_PASS 0/2,145. Accepted as documented beta limitations (RUNBOOK and
  BETA-REFERENCE at the next landing): (1) the Compute edge may keep routing to a
  superseded process for up to ~5 s, so raw clients can be without service up to ~9 s
  per deploy/restart (bar 10 s; SDK clients ride through); (2) operators should not
  restart a loaded cell more than ~3 times an hour: 5 boots in 50 min under load built
  compaction lag and stalled writes ~18 s once (no loss). The first deploy of the read
  handoff fix loses the outgoing old binary's last read window once (1.36% of that
  window's reads in the field), since the old binary predates the fix.
- 2026-10-20 02:00. The RC's cache-size lines change the shared-cell profile digest
  that `cell-admin init` fixes for a cell's life, so no existing cell can take
  f8456a52 (field round 6 refused it; the scratch beta stays on c5cad2f2). Decision:
  a `cell-admin reprofile` command accepts a new layered profile for an existing cell
  only when every ceiling-bearing key (the sizing rules' inputs) is unchanged and the
  diff is limited to named non-sizing keys (block-cache sizes), records the new
  digest with an audit entry, and refuses anything else; deploy-release.sh keeps
  refusing an unreprofiled digest. Chosen over relaxing the digest (it guards real
  ceilings) and over a new cell per tuning change (re-admits every project).
- 2026-10-20 03:00. `cell-admin reprofile` built (reprofile-report.md): accepts only
  SHARED/HISTORY/POSTINGS/TELEMETRY_CACHE_BYTES changes, CAS-saves the new digest,
  journals it; deploy-release's refusal names the command. Its RUNBOOK section changes
  the embedded binary text, so the beta RC becomes the reprofile commit; the Layer B
  run of f8456a52 stands as the server evidence (binaries differ only by RUNBOOK
  text); rc-certify's formal single-cell certification is re-run on the production
  RC once the production target exists (its field arms need that target anyway).
- 2026-10-20 05:00. Layer B on rc.7 (f8456a52, layerb2-report.md): FAIL. Compliant phase
  now clean (342-384 MB, 0 sheds; ports fixed). Hostile phase (from 1,800 s): fds
  950 -> 3,055, SSE 548 -> 830, RSS over 500 MB at 1,930 s, plateau 631-638 MB with
  2.75 M sheds; container peak 771 MB, above RUNBOOK's ~750 MB kill line. Decisions:
  (1) top blocker = hostile-phase memory: profile it and bound what grows (connections,
  read reservations, queued work) so a shed lowers memory and RSS stays under the
  shed line and far from the kill line; (2) harness defects fixed without moving any
  bar: h8's audit skips streams setup never created; the billing comparison resolves
  outcome-unknown appends by read-back (the server's ledger is right: it bills what
  it committed); probes and H11t send pairs concurrently so the rig can reach 2,000.
  The x86_64 binary ran under Rosetta in a 1-CPU container, which inflates queuing;
  that is recorded, not used as an excuse.
- 2026-10-20 06:00. Scratch beta reprofiled and deployed at c1433382 (rc.9): reprofile
  journaled (caches 128/32 -> 32/16 MiB, k and ceilings unchanged); restart 0 SDK
  errors; row 4 quiet; idle RSS 47 MB. Open, recorded for follow-up: the first cutover
  onto the smaller caches was slow (one append 9.5 s, one read 11.7 s inside SDK
  retries; one sample); after this deploy predecessorSuperseded read false (the
  outgoing rc.6 process wrote no superseded mark on a slow cutover) -> row 4 counts
  one extra boot (NOTIFY, never a page alone).
- 2026-10-20 13:00. mem2 (mem2-report.md): seven commits cut hostile-phase RSS (median
  561 -> 468 MB, sheds 746k -> 191k, compliant success 31-88/s -> 345-456/s) but the
  container still peaks ~766 MB on the 1-vCPU Rosetta rig: the residue is CPU
  saturation (41% of refusal CPU was RS256 verification; emulation ~2.5x) driving
  latency-sized growth (server sockets, store connections, allocator overhead).
  Decisions: land after an Opus security review of the token-verification cache
  (73bac696) with a mutation owner for it [gate selection, delegated]; no connection
  cap and no lower per-project read cap (reasons in mem2-report); get NATIVE x86_64
  Layer B evidence through a workflow_dispatch job on GitHub's ubuntu-24.04 x86
  runners that prints the verdict and leg numbers only and uploads no artifacts (the
  raw-evidence hold stands: the manifest and logs stay on the runner and are not
  published as artifacts).
- 2026-10-20 14:00. mem2 review (mem2-review.md): token-verify cache sound (exact-token
  key, tied to the published key-set snapshot, failures never cached, every claim
  checked per request). Fixed before landing: M1 (a shed append drains a bounded body
  before its 429, so an uploading client sees the 429, not a reset that the SDK would
  turn into outcome-unknown), L1/L2 (staleness check only on cache hits), record texts;
  plus a mutation owner for src/auth/verified.rs [gate selection, delegated].
- 2026-10-20 15:00. Flaky DST seen in the lbci gate: idle_release::an_idle_instance_
  releases_its_caches_and_the_next_read_refills_them (history cache inserted_bytes
  48, expected 0, right after the release; 5/5 alone). One gate rerun; queued for a
  test-side fix lane.
- 2026-10-20 16:30. Layer B workflow (06b89311) pushed; the first smoke failed at
  checkout because a short sha was passed as `ref` (actions/checkout needs the full
  sha): retriggered with the full sha; the docs' trigger command should say
  `-f ref=$(git rev-parse <sha>)` (fix at the next landing).
- 2026-10-20 14:30. NATIVE x86 Layer B (GitHub run 39370055922, 06b89311, pre-mem2;
  summary layerb-native-06b89311.txt): FAIL. Hostile phase RSS plateau 561-612 MB,
  2.17 M RSS sheds, container peak 737.6 MB, victim p99 append 13.1 s; so the memory
  problem is real on native CPUs, not a Rosetta artefact. Harness fixes confirmed:
  H11t PASS (2,000 pairs, KS p 0.88), probes reach 2,000 contacts, audits 293/293,
  0 leaks. New: probes 88 differences; billing 1 project (h9b_0000) metered 4x the
  client's read requests/bytes. Next: native run on 9d3df7c7 (mem2), and a lane on
  the billing/probe discrepancies.
- 2026-10-20 15:30. Layer B discrepancies (disc-report.md): billing stays at render
  time (OBSERVABILITY-BILLING §7.1: a page counts once accepted for delivery; billing
  only completed reads would let a client read free by dropping the last byte); the
  harness counts a drain-floor-cut page once H9b re-reads it. Probes: a pair refused
  for load (read_memory_busy, project_rate_limit, project_memory_pressure, overloaded,
  all answered before the name is looked up) is unjudged and replaced by a fresh first
  contact; 0 differences over >= 2,000 judged pairs stays the bar; differences now
  print by status class.
- 2026-10-20 20:00. Native Layer B on af9a47ab (run 39383546506; mem2 + harness fixes):
  FAIL but much better: hostile plateau median 472 MB (was 561), RSS sheds 484 k (was
  2.17 M), slope PASS, probes PASS (0 differences, 2,000 judged), H11t PASS; still
  peak 632 MB, container 768 MB, victim append p99 8.6 s, compliant tenants shed.
  Decision: build fair shedding: over the shed line, admission refuses the projects
  that drive the pressure (by recent admitted cost / in-flight / quota debt) before
  any compliant project, so the noisy tenant pays (an isolation property of the
  shared cell; edge change). Harness: h2/h7's outcome-unknown read-back must retry
  after the load instead of reporting unresolved.
- 2026-10-20 17:00. Scratch beta on 2bf52cc7 (rc.10): field round 7 all PASS (deploy and
  restart 0 failures, 1 MiB default pages read whole, token cache follows key-set
  changes, idle 48 MB). Three throwaway signing keys were added and retired during the
  key-set test; the published set is back to the main key only.
- 2026-10-20 21:00. Fair shedding (fairshed-report.md, c5b727ea): blame = a project's
  share of verified requests sent in the last 5-10 s vs 1/k (counted before its own
  quotas refuse, so a flood stays blamed); over the shed line a blamed project gets
  429 project_memory_pressure (tagged, Retry-After 2) on every product request, others'
  appends admitted up to line x 5/4; hostile repro: RSS sheds -98%, compliant 429
  -84%, no compliant project blamed. Extended (same decision) to the in-flight write
  gate, whose instance-wide refusals are most of what compliant tenants still meet.
  Known limit: a flood spread over many projects each under 1/k blames nobody.
- 2026-10-21 00:15. NATIVE Layer B on 6aab705a (fair shedding; layerb-native-fairshed.txt):
  RSS sheds 19 k (484 k before, 2.17 M baseline), compliant 429s down ~95%, victim append
  p99 3.9 s (13 s baseline), billing PASS, probes PASS, H11t PASS, slope PASS; still FAIL:
  RSS peak 642.7 MB (container 790.8 MB) vs < 500, Q0 105.6 MB vs 53.7, victim p99 bars,
  n0 read_memory_busy 5,359, h2/h7 audits. Decision: the fair-shed RSS headroom for
  non-blamed appends drops from x5/4 (625 MB) to x9/8 (562.5 MB) to keep ~190 MB under
  the ~750 MB platform kill line.
- 2026-10-21 01:30. h2/h7 audits (h2audit-report.md): the audit gave up after 50 refused
  reads of q-hostile's big streams (read quota 256 KB/s), the same limit the billing
  read-back had; it now retries refused reads for 10 min since the last good page and
  prints each failing audit's cause. TLA-003 was left stale by the fair-shedding
  landing (src/product.rs); re-recorded in the mutfix8 landing.
- 2026-10-21 03:00. [owner-reserved, delegated: gate counting] cargo-mutants' build
  timeout 600 -> 900 s (scripts/quality/mutation_driver.py): CI's 4-vCPU runners build
  an owner's baseline in 454-635 s, so the last mutants of a long shard ended TIMEOUT
  at 600 s though caught locally; the 90 s test timeout is unchanged.
- 2026-10-21 06:30. PROCESS SLIP (lead): 280d2978 (documentation only: the full decision
  log in the repo + README row) was pushed although its gate ended GATEFAIL-quality,
  because the command chained the push after printing the verdict instead of checking
  it. The failure is unrelated to the commit: cargo deny reports new advisories
  RUSTSEC-2026-0381/-0382/-0383 (distrusted certificate intermediates in rustls-webpki
  and bundled root certificates), which fail every commit's gate and CI from today.
  Fix forward: bump the affected crates, re-record all 25 receipts (Cargo.lock feeds
  them), gate, Linux, push. From now on the lead pushes only in a separate command
  after reading GATEDONE.
- 2026-10-21 09:00. [gate configuration, delegated] deny.toml's two pinned license
  exceptions for webpki-roots / webpki-root-certs move 1.0.8 -> 1.0.10 with the
  RUSTSEC-2026-0381/-0382/-0383 bump (same crates, same license, still pinned to one
  version). bench/probe and bench/awsbench lockfiles (outside the gate's cargo deny)
  still carry the old versions: left for a separate lock-only commit; the always-on
  observatory probes are not redeployed.
- 2026-10-21 10:00. pullcov (pullcov-report.md): 1 MiB pull coverage on shared cells
  removes compliant read_memory_busy (9,548 -> 0; 7,660 -> 49) but on the emulated rig
  raised RSS peak (602 -> 690, 573 -> 611 MB) and sheds. Not landed on that evidence:
  pushed to a temporary branch lab/pullcov for a native x86 Layer B (GitHub workflow),
  landed only if native peak RSS and compliant refusals are no worse than f6079cb3's
  final run; the branch is deleted after.
- 2026-10-21 12:30. pullcov NOT landed. Native Layer B on its tip (run 39529164222,
  layerb-native-pullcov.txt) vs f6079cb3's final run: RSS sheds 334 k vs 29.6 k, peak
  624 vs 586 MB, container 772 vs 741 MB (over the ~750 MB kill line), victim append p99
  8.5 vs 6.5 s; better: compliant read_memory_busy gone, victim read p99 0.26 vs 1.7 s.
  The read-memory gate was holding load back; removing that brake moves the pressure
  into RSS. Kept as refs/keep/wk-pullcov (6b88d0f0) for a later design that bounds RSS
  first; lab/pullcov deleted.
- 2026-10-21 13:00. Idle-host Gate S confirmation started (Stage 8 1,000-stream point,
  S10 beta shape, a 2-3 h stretch of the 300 B soak): no other heavy work on the Mac
  while it runs, so its points count (not VOID). The result goes into gate-s-report.md
  and the handover page; the layout 6 decision stays the owner's.
- 2026-10-21 19:30. Idle-host Gate S confirmation (gate-s-report.md, final section):
  both stop rules fire on VALID points (Stage 8 1,000 streams shard tier 0.234; S10
  beta shape 1.140; 0 self-freezes); the soak stretch was VOID (desktop load) but
  matches the 6 h soak within 1.3%. "Beta on layout 5, layout 6 paused" stands; the
  first engineering if layout 6 resumes is call 10's compactor levers.
- 2026-10-22 03:00. refusal-cost lane NOT landed. It cut per-refusal CPU 23-39% with
  byte-identical answers, but the native Layer B (run 39634068550,
  layerb-native-refusal.txt) was worse than f6079cb3's final run: RSS sheds 81.6 k vs
  29.6 k, peak 629 vs 586 MB, container 756 vs 741 MB, victim append p99 8.2 vs 6.5 s.
  Layer B's hostile clients retry in a closed loop, so cheaper refusals bring more
  requests. Kept as refs/keep/wk-refusal; lab/refusal deleted. The lead's
  recommendation for the owner: cut hostile request VOLUME (e.g. an escalating hold
  on repeated 429s for a project that ignores Retry-After), a client-visible change
  that should be designed with the owner rather than tuned unsupervised in the week's
  last two days. Engineering stops here; slate stays at the green feba595e.
