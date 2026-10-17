# Lead decisions, unsupervised week 2026-10-17 to 2026-10-24

(The repository copy of the lead's log, taken at landing-4 on 2026-10-17;
a lane commit it cites is followed by the hash it landed as.)

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
  in r4-rust 29aabde6 (landed as a00aa935); deploy commit 9d9071b6's
  (landed as 5125d2e6) "weighs 21" text is corrected at landing.

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
