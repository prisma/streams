# September 2026 hardening program: acceptance record

From 2026-09-21 a coding agent worked directly on `slate` through a numbered
list of review items and bugs. On 2026-09-24 an external reviewer reviewed
the snapshot at 24c4c77a. The verdict: keep the demonstrated fixes, but do not
approve production deployment. The next phase is release safety, not another
numbered batch. The repository owner adopted the reviewer's positions as their
decisions. This directory is the record the reviewer asked for, kept in the
repository rather than in a session scratchpad.

## Contents

| Path | What it is |
| --- | --- |
| `report/report.html` | The program report as written at the end of the batches (a self-contained page). |
| `report/ledger.json` | Every landed item: commit, what changed, how it was proven. |
| `report/decisions.json` | The decisions held for the owner, and the 52 edge changes made (`edge_changes_made`). |
| `report/remaining.json` | Items not done, open observations and follow-ups, as of 24c4c77a. |
| `report/process.json` | Method, gate traps, questions for the reviewer, risks. |
| `report/external-review-claims.json` | The reviewer's claims, each checked against the tree by independent read-only passes. |
| `edge-changes.md` | A before/after contract record for each of the 52 edge changes, grouped by risk (5 high, 11 medium, 36 low), each checked against its commit. One record (#45) did not match its recorded text: an infinite scaler cooldown disables a stream's first transition too, not only re-scaling. |
| `effective-config-diff.md` | The old-vs-new (v0.2.0-rc.4 vs HEAD) effective-configuration comparison for every deployment family: verdicts, changed fields, notices, and what the owner must decide before deploy (evidence in `evidence/effective-config/2026-09-24/`). |
| `NEXT-WORK.md` | The handoff: every task not done after the second external review, in the owner's priority order, with the owner's decision text, current state, design, tests and gates for each. |
| `plans/` | The implementation plans the program worked from, one directory per planning round (`plans18`-`plans20` hold the item 40, item 50, 38/39, absorber, F1, F2, F3, bug #7 and effective-config plans). Each plan names the tree it was written against; line numbers and scratch paths are as of that tree. |
| `evidence/split-investigation.json` | The split-boundary investigation and its skeptic's corrections (the hold's timing figures, F1, F2, F3). |
| `evidence/refused-chain-probe.patch` | The review probe that builds a refused chain's overlapping postings pages (NEXT-WORK item 1). |
| `evidence/gate-b12-capacity-leg-500.log` | The failed local gate leg: one append answered 500 right after a split in `post_split_throughput_scales` (the release hold). |
| `evidence/gate-b12-gate-output.txt` | The whole gate run that leg belongs to. |

Related records elsewhere in the repository:

- `docs/quality/exception-audit-2026-09.md`: every exception reason the program
  edited, classified with the ratchet's own contracts, the adversarial review of
  the new gate, and the owner decisions D1-D9.
- `docs/RUST-QUALITY.md` and `AGENTS.md`: the exception-growth rule the
  reviewer asked for (an agent never authorizes growth of its own exception).
- `scripts/platform-e2e-negative.mjs`: the platform-e2e battery's negative
  control, which requires two refused fault injections to fail exactly their
  three checks.

## Work packages the reviewer set, and their state

The reviewer grouped the remaining work into four packages. Commits are on
`slate`; state as of 2026-09-25.

1. **A reliable acceptance record.** This directory (d255ad6d); the exception
   audit (6a36e406); the exception-growth gate (bcddd615); the platform-e2e
   negative control (1d079e01); the preserved failed gate log; the
   effective-configuration comparison (42b92d81..26c555dd).
2. **Correctness findings.** Each fix below has a red regression and passed an
   adversarial review; findings of those reviews were fixed before push unless
   listed under "Open" below.
   - Task-supervisor deferred-drop slot and the corrected F-G wording (95bfabc6).
   - Tracker race in `QuotaRegistry::admit` (d4d631df).
   - Product-append 413: stable code, its limit, no quota debit before it
     (9b3a3829, eac89829, eda79ccf).
   - `WWW-Authenticate` on every 401 (da635ec8).
   - Capability placement: `status_and_quotas` answers from `served_policy`
     (46f668b3).
   - Item 28 metering from the typed outcome and the incarnation it committed
     to (e6453674, 2ffff86c).
   - Item 73 `ProductOperation`: authenticated unknown operations are refused
     by route, 404/405 (7fb70f65).
   - Usage `?streamId=` read another stream's usage outside the prefix grant
     (found by item 73's review): now authorized by the name the rollup
     recorded for the id (7549e28a, e347fb1a; edge change #53).
   - Second review (2026-09-25): project usage totals need an unrestricted
     grant and a final-record seal needs the append scope (1d4d8660,
     d58ebf15); the wrapper's held diagnostic is generic, it forwards the
     platform's stop, and staging refuses hidden files (b7f6dc3a); a
     recreation records the replaced incarnation's storage close as a
     durable debt (2ba4bc47); item 50 option (a) (45f8711c).
   - The split-time 500 (release hold, below): 5a6f9f56, 9c6675d7, b5751e75,
     b2a8001f; and F3, a registry read failure in an append's retry
     re-preparation answered a non-retryable 500 at the split boundary
     (0f4cd8c1, edge change #54). Found with it: a raw close whose seal intent
     failed transiently answered a false 409 `sealed` (839135a4, edge change
     #55).
3. **Upgrade and recovery.**
   - Items 38/39: the binary's process root answers a critical loop's exit
     with its ordered stop, bounded at 30 s off the executor, and exits 1; the
     deploy wrapper exits with the child's code after a death once the child
     has been ready 60 s, so Compute replaces it (f657ecab, fdb6c3bf,
     e966863d, then two review rounds: 21fd165d, 80e29dab, 8f9d2b0d, da06acad,
     27ea208c, 6eca6787). Campaign deploys re-stage their wrapper from
     `deploy/` (`bench/stage-app.sh`).
   - Effective configuration, rc.4 vs HEAD, for every deployment family: no
     verdict changes; four changed fields; `effective-config-diff.md` lists the
     owner decisions that still block a deploy (a real per-project env export
     above all).
   - Not done: item 40's step 3 (scoped withdrawal for a prefix that
     repeatedly fails to open; steps 1, 2 and 4 landed in the commits
     NEXT-WORK section 4 names, and the four required tests exist),
     item 50's pin retirement (tracker sizing itself landed, 45f8711c), bug #7
     migration tooling and its rehearsal on a consistent copy of a real rollup
     database, and the Compute validation of items 38/39 (startup, readiness,
     restart, memory pressure, rollback). Plans exist; the rehearsal and the
     Compute runs need the owner.
4. **Release-wide verification.** Not started: one mutation run over the whole
   hardening range, including billing, rollup, product and usage (owners not
   selected by the per-push plan today), on one pinned artifact.

## The split-time 500 (release hold HOLD-SPLIT-500)

Cause (probable, not established; see the elimination argument): the
absorber's dirty-index rescan rolled a lane mark back while the batch
absorbed from it was submitted but not yet applied. The next gather re-read
the in-flight range, the committer retired those bytes a second time, the
maintenance ledger's checked subtraction failed, and the whole commit group
was rejected ("maintenance accounting diverged"), so every append co-grouped
with it answered 500. Timing, as the investigation's skeptic corrected it:
across runs cap-01..cap-20 the split started at 8.598-8.742 s and the run had
16 rollbacks in the third pre-split window; the 500 fell before the split (no
post-split window can end before about 9.7 s, and the test joins every client
before `execute_split` and opens both children before restarting load, so no
client append overlaps the split). Elimination: with a fault-free store,
maintenance divergence is the only source of such a 500 reachable in the
capacity run. The initial commit message (5a6f9f56) carries the uncorrected
figures (8.64-8.92 s, 18 rollbacks); the figures here supersede them.

Fix: the committer retires an absorbed advance only from its own boundary
(9c6675d7), and a lane mark is rolled back only when no submitted advance of
its stream can still land, tracked by per-stream-bucket settlement receipts
(b5751e75). Reds assert exact ledger bytes, not only the absence of a 500.
Acceptance: 20 loaded runs of the capacity test with its absorber event
counter (b2a8001f): zero divergences, zero dropped advances, zero refused
appends; an A/B against the pre-fix tree under the same load diverged in 5 of
10 runs. Two of the 20 runs missed only the 1.8x throughput ratio (1.73,
1.77) with pre/post-split throughput equal to the pre-fix tree's, i.e. host
contention. C9: a single-instance split retires no engine, so the capacity
test never exercises the Moved/retirement settlement path; R5b covers a
refused group settling its receipt.

Residuals, recorded rather than fixed (low, adversarial review of the fix):
the rollback deletes no postings pages, so a refused group that carried a
stream advance with a later advance of the same stream chained behind it
heals into overlapping pages (ledger exact; that key's page bucket reads
through the envelope fallback). The same holds for a bucket-sharing stream's
dropped gather and for an engine retirement that drops two chained advances.
Pre-fix, the same overlap arose whenever a rescan rolled back first, and a
refused chain left a phantom ledger instead. See docs/HISTORY-V2.md.

**Closed by the owner on 2026-09-25** (second external review of 9813d1cb):
"Closed: a causal absorber-accounting defect matching the observed failure
class is fixed and regression-covered. Attribution of the original incident
remains probable." Limits that stay explicit:

- This was not a demonstrated append-during-split failure: the corrected
  investigation places the 500 before the split, and the capacity test joins
  its clients before splitting, so this evidence does not certify concurrent
  append/split behaviour.
- The two throughput-ratio misses (1.73 and 1.77 against 1.8) remain misses:
  correctness acceptance and performance acceptance are separate, and
  "consistent with host contention" is not a proven explanation.
- The overlapping-postings residual is a separate issue, accepted for this
  closure because the reader has a bounded canonical fallback
  (`o4a_stored_overlapping_pages_cannot_skip_a_canonical_match` tests overlap
  handling). Still to do: a composed test that creates the refused/chained
  state and reads it through the public path; stale-page repair is scheduled.

## Owner decisions from the second external review (2026-09-25)

| Question | Decision | State on `slate` |
| --- | --- | --- |
| Edge #53 usage `?streamId=` | Ratified | 7549e28a, e347fb1a |
| Edge #54 F3 retry re-preparation | Ratified | 0f4cd8c1 |
| Edge #55 transient raw close | Ratified; normalize the owed-claim renewal failure | 839135a4; renewal failure now 503 `seal_incomplete` (1d4d8660) |
| HOLD-SPLIT-500 | Closed (narrow claim, above) | 5a6f9f56..b2a8001f, 882004d9 |
| Restricted credentials and project-wide usage | Denied: `usage.read` AND an unrestricted effective grant | 1d4d8660 (edge #56) |
| `:seal` with a final record | Needs `lifecycle.manage` AND `records.append` | 1d4d8660, d58ebf15 (edge #57) |
| Staging hidden files, stray entries | Refuse before install; fresh allowlisted directory | b7f6dc3a |
| Held wrapper diagnostic | Generic and unhealthy; details only in the log | b7f6dc3a |
| Wrapper signal forwarding | Bounded forwarding | b7f6dc3a (Compute lifecycle still to verify) |
| Idle-expiry recreation billing | Release blocker: durable, generation-fenced cleanup obligation | 2ba4bc47 (closure debts); 40dbf0d3 (the debt cursor pages past waiting debts); 60d80607 (the seven missing tests) |
| B5, an expired source whose forks still read it (decided 2026-09-29) | An expired source its forks still read stops billing at its expiry | f7a0a26f (pinned), 7cef509c and 0cafc25b (its tests wait for the close and hold the billing clock) |
| Configuration (decided 2026-09-29) | The binary's defaults are the 1 GiB profile's values ("I want the 1gig profile to be the default"); the L0 cap of 32 first | 823b3269, 648d7df4 and the commits of edge #67 onward; `config-simplification.md` |
| A billing close of a row that is already closed (decided 2026-09-29) | A close that would change nothing is a no-op in the committer | Edge #72 |
| A failed storage close after a write failure (left to the implementer, 2026-09-29) | The refused close of a Db that had already failed is settled as closed; a healthy Db whose close fails stays failed | Edge #73 |
| B3, a close after its month was finalized (decided 2026-09-29) | A correction against a frozen invoice is allowed: the month nets to what the shard recorded up to the close, later months bill 0 for the segment | 0b7840c7, edge #66 (ratified 2026-09-29) |
| Edge records #67-#86 (ratified 2026-09-30) | Accepted, each record as written: the 1 GiB posture as the binary's defaults (#67-#71), the two close decisions (#72, #73), the five configuration packages (#74-#85) and the pace-gauge removal of the residue cleanup (#86) | `edge-changes.md` |
| Edge records #87-#89 (ratified 2026-10-01) | Accepted as written: the lost-close fix (#87, 9f25cf96), the seal-fence receiver (#88, 433b60e5) and its relaying sender (#89, 82a14a8f) | `edge-changes.md` |
| Lost close, the exact fix (approved 2026-10-01) | A reply channel on the committer's billing close and retention flag, answered with a retryable refusal at the three sites that dropped it, with a warning and a counter; the walk and the debt pass stop their page on a refusal. Approved with it: the growth of `CommitOp`'s exception contract, the module extraction from `src/shard.rs` it needs, and the six receipts it stales | NEXT-WORK section 2 |
| F1-a follow-ups (approved 2026-10-01) | The seal model's negative control for a relayed fence lost and answered false is required; the platform contract names `seal-fence` and the `segment-close` it omitted | NEXT-WORK section 5 |
| Scaler loop survivors (approved 2026-10-01) | A DST that runs the scaler loop may be selected by the `scaler` owner's filter, to kill the two survivors inside `Scaler::start` | NEXT-WORK section 10 |
| F2 observations (decided 2026-10-01) | The product `:seal` answering the unknown outcome as retryable `temporarily_unavailable`, reads reporting the stream sealed once the durable final closed it, and a retry without producer headers storing a second copy are accepted as the contract; the seal-with-final 200 gains the `Cache-Control: no-store` WIRE-MATRIX §2.5 lists | NEXT-WORK section 6 |
| Edge records #90-#93 (ratified 2026-10-02) | Accepted as written: the platform contract's two operations (#90, 89011daa), the lost close's exact fix (#91, 24493671), the seal-with-final answer's `no-store` (#92, 1f03aef2) and the rollup readiness split (#93, be2907b4) | `edge-changes.md` |
| Accepted with #91 (2026-10-02) | A hard `DELETE` answers after each segment's close is durable, one after another (performance accepted); the pinned R13 mechanism test as re-pinned; `tombstoneWalkCloseSubmits` counts applied closes | `edge-changes.md` #91 |
| The scaler loop test's cost (2026-10-02) | Accepted: every `scaler` mutant runs the 10 s loop test (up to 30 s when a mutant breaks the loop) | NEXT-WORK section 10 |
| Nightly mutation rotation (2026-10-02) | Run every owner of the night's group and fail once at the end with the whole list of survivors; split the owners into more groups so each night finishes inside the job's 240 minutes | NEXT-WORK section 10 |
| Other answers missing their listed `no-store` (2026-10-02) | The settle and watch answers follow WIRE-MATRIX §2.16-§2.18 (`Cache-Control: no-store`), as the seal-with-final answer does | NEXT-WORK section 6 |
| Workload operations in the frozen contract (2026-10-02) | MULTITENANCY.md gets a contract revision (r6) naming the ten operations, `segment-close` and `seal-fence` among them | `docs/MULTITENANCY.md` |
| A mutation owner for the fence relay (2026-10-02) | `src/application/lifecycle/fence_relay.rs` gets a `mutation_owners.py` row (F1-a plan Q7) | NEXT-WORK section 5 |
| Edge records #94-#96 (ratified 2026-10-07) | Accepted as written: `GET /v1/debug/store` `totals` with a 404 counted as billed (#94, f23ffc9d, amended by fcd16d84), the settle and watch answers' `no-store` (#95, 2aaa4282), and a keyed `DELETE` for every object the server deletes instead of a one-key `DeleteObjects` POST (#96, 77beff8a) | `edge-changes.md` |
| Billing bases (2026-10-07) | Retention is billed on the bytes the service stores after its own compression, at a unit price above Cloudflare K2's, so a customer never gains by compressing before sending. Consume is billed on the bytes the customer sees. Produce, first decided on the customer's bytes, moved later the same day to stored bytes at a higher price (row "Prices") | NEXT-WORK section 14.1 |
| Storage layout 5 page format (cryptography accepted 2026-10-07) | An append request's records are stored as pages of up to 64 KiB: one routing key and key version per page, zstd level 1 then AES-256-GCM-SIV under a key of its own HKDF label (`prisma-streams/page/v6/aes-256-gcm-siv`), one random nonce per page, the identity and the clear header bound as AAD, and no plaintext length in the clear; a new `LAYOUT_VERSION` with no backwards compatibility | NEXT-WORK section 14 |
| Consumer groups (2026-10-07) | A pull may lease up to its `max` consecutive records of one routing key to one consumer while the key stays blocked for the group's other consumers; acks stay per record, in any order; unacked records return after their visibility window from the lowest unacked offset | NEXT-WORK section 14 |
| Shared cells, first scope (2026-10-07) | Many small projects per cell, implemented and validated to lower small customers' cost, before the rest of Stage 8: the certification battery's platform-side pieces and the external security review are not part of this step, and isolation between projects must still be proven | NEXT-WORK section 14 |
| Prices (2026-10-07) | Retention $0.05 per stored GB-month; produce $0.16 per stored GB written (picked at the owner's request: K2 parity at 4x compression); consume $0.04 per GB of the customer's bytes; a 1 KiB minimum billable size per append request; a small subsidy of small tenants is accepted; router-leg egress priced at $0.01/GB | NEXT-WORK section 14.1 |
| Write tier (2026-10-07) | One 100 ms tier for everyone, no second tier; the WAL failsafe at 60 s and the usage drain at 8 s approved, each with an edge record | NEXT-WORK section 14.2 |
| Storage plan (2026-10-07) | Design B ("WAL plus one copy") is built as layout 6 before launch, after a seal spike passes; shared cells phase A runs in parallel; cross-request pages come after layout 5, one principal per page, with three small layout 5 additions now | NEXT-WORK section 14.2 |
| Retention (2026-10-07) | Unlimited by default; a maximum age of at least 1 h or an explicit trim per stream; expiry on server commit time; forks inherit; no archive tier (Prisma Buckets expose Tigris Standard only) | NEXT-WORK section 14.3 |
| Shards and WAL writers (2026-10-07) | As few shards as possible, splitting only when volume requires it; a WAL journal shared across a cell's servers is rejected: servers stay uncoupled so more servers do more work | NEXT-WORK section 14.4 |
| Launch shape and routing (2026-10-07) | Launch with one single-server shared cell. Multi-server cells come on the new Prisma Compute generation, designed to provide one hostname per cell, `/i/<server>/` routing, no 404 while waking and long-lived unbuffered responses; they use `home-v1` placement with no spare server (Compute starts a replacement in under a second) and the forward-first misroute contract. Cross-server stream splits are switched off until the split package lands before public launch | NEXT-WORK sections 14.5, 14.6 |
| Shared cells Q0, RSS after a load (2026-10-08) | Take a heap profile and fix the retention NEXT-WORK section 14.7 records before the first shared cell; not raising `ADMIT_RSS_SHED_MB` and not restarting by hand. The bar joins Q5's certification: RSS back within 20 MB of its pre-load baseline within 10 min after a load, and no instance write shed at idle | Open; blocks the first cell, not landing (NEXT-WORK section 14.7) |
| Shared cells Q1, memory of a 1 GiB cell (2026-10-08) | (A) now, with no server code: the instance read memory stays at the shed line ÷ 4 (125 MiB); the shared-cell profile sets `PROJECT_MEMORY_PRESSURE_BYTES=33554432` (four default reads per project) and `MAX_REQUEST_BODY_BYTES=8388608`; four projects' unread pages may fill the read memory (neighbours wait 2 s, then `503 read_memory_busy`), and the budgets exceed the shed line only when every one is full at once. Accepted for every 1 GiB cell, dedicated ones included, with alerts on `read_memory_bytes` and RSS, and certified under Q5. Accepted with it for hand-admitted design partners: a woken long-poll takes its whole 8 MiB budget again, and the dead-letter handoff of `delivery::settle` reads up to 4 MiB unreserved. (D) before self-serve: a read reserves the bytes between its cursor and the tail, inside `execute_read` and `ResolvedRead::execute` (their rows; TLA-018); (B), a 1 MiB default page on shared cells, is the fallback | The profile values, edge #110's amendment and the C5 and C6 legs follow landing (NEXT-WORK section 14.4) |
| Shared cells Q2, frozen scopes (2026-10-08) | (a) `bootstrap::run` installs the cell ceiling from four validated settings (`PROJECT_SHARE_K`, `CELL_ENVELOPE_{REQUESTS,APPEND_BYTES,READ_BYTES}_PER_SEC`), reserving `PROJECT_ID` on every cell and `ACCOUNT_ID` only when k > 1; the owner updates its six rows to the values the gate prints, in one commit with its edge record right after landing. (b) A new growth row for one memory reservation in `product_list` (C8), funded by an extraction from `src/product.rs`, batched with the other `product.rs` work before self-serve. (c) The `#[expect]` scopes `publish_policies` and `publish_jwks` are renamed `commit_policies` and `commit_jwks`, their contracts unchanged, so the `publish_*` functions outside them apply the ceiling and the one-audience check | (c) in phase A's first push; (a) right after landing; (b) before self-serve (NEXT-WORK section 14.4) |
| Shared cells Q3, CI selection (2026-10-08) | (a) The `mutants` job runs eight runners of 360 min (`SCHEDULE_JOBS = 8`, a cap of 270 modeled minutes), which re-deals the nightly rotation. (b) CI's Bun step runs `./deploy/cell-admin`, and the SDK job its end-to-end test against the release binaries. (c) `mt-cert-1000` also runs Layer A at 1,000 projects as two legs: every `shared_cell_` test but A6, then A6 alone. (d) Phase A's mutation rows and filters stand (`admission_body`; the DSTs added to `read_request`, `quota_parked`, `quota_read_reservation` and `admission_body`); `src/auth/ceiling.rs` and `src/auth/signing_key.rs` get owners now, and `src/admission/read_memory.rs`, `src/admission/park.rs`, `src/product/read_memory.rs` and `src/application/consumer/pull_wait.rs` once they settle after the Q1 and C7 work | (a) NEXT-WORK section 10; (b)-(d) with phase A's pushes |
| Shared cells Q4, edge records #99-#116 (ratified 2026-10-08, before landing) | Ratified as drafted: a project's ceiling is the bound ÷ k, not ÷ (k + 1); parked waits take only what open subscriptions leave, with no new refusal; reads reserve memory on every cell; a drain floor of 16 KiB per 10 s on every connection, SSE included; writes latch on held pages and parked waits; a woken long-poll without room answers its ordinary `204`; the keys feed requires `aud` (fail closed), and the key-only carrier is closed under enforce only. The operator-debug and fleet-internal surfaces fold into the records they belong to, and #102 and #104 are amended in place when the stale refusals' `Retry-After` and the 403 wording land | `edge-changes.md`; each record is written by the push that lands its change |
| Shared cells Q5, certification (2026-10-08) | Before the first cell: build the harness (7-11 engineer-days); Layer B locally on the single-server shape (H9, H9b, H11t): 0 leaks over at least 2,000 probe pairs, usage equal to the ledger for 1,000 projects, compliant tenants 0 refusals, no instance RSS shed during H9 and H9b, RSS peak under the 500 MB line and its slope under 2 MB/min over the last 30 min, and Q0's bar; a one-shard envelope arm (`WC_PMP` in fra) that sets `CELL_ENVELOPE_*`; SC0-1s (72 h idle floor) and SC1 (about 30 h of wakes, p99 at most 10 s from the waking request to its first 2xx). The certification runs on the write tier the first partner runs. For a single-server first cell the exit is Layers A and B, the envelope arm, SC0-1s and SC1 green (PLAN decision 11(a) amended); the full set returns before self-serve. About $40-60 in fra | Open; blocks the first cell (NEXT-WORK section 14.4) |
| Shared cells Q6, feeds and release posture (2026-10-08) | (a) Before the first cell the deploy wrapper polls the feed bundle every 15 s with `If-None-Match`, rewrites the three files atomically and deletes them after 120 s without a successful poll, on a monotonic timer that polls first on wake (an edge record; its Bun tests join CI; it re-opens the Compute lifecycle gate); the in-binary object source (4(a)) waits for the multi-server generation, and A9 stays red for it. (b) `validate_fleet_auth` accepts the release posture on a fleet-off cell without a workload token file, and the shared-cell profile sets `STREAMS_RELEASE_POSTURE=1` (an edge record). (c) and (d) are settled by "no users": no notice of the layout resets, and the keys feed switches to the `aud` format with its binary | Follows landing (NEXT-WORK section 14.4) |
| Shared cells Q7, deployment gates (2026-10-08) | For the first shared cell these apply: Compute lifecycle validation (re-opened by Q6(a)), the platform export and configuration comparison, a release-posture Compute family, idle compatibility with the 120 s header timeout, and x86_64-musl binaries. The Bug #7 rehearsal does not (a fresh rollup database); the release-wide mutation campaign stays a public-launch hold | Deployment gates below |
| Shared cells Q8, keys contract r7 and cryptography (accepted 2026-10-08) | Accepted: `cell-admin`'s token minting (`deploy/cell-admin/tokens.ts`: RS256 of at least 2,048 bits or EdDSA, `kid` = `streams-<alg>-<first 16 hex of SHA-256(SPKI DER)>`, header `{alg, typ, kid}`, at most 24 h, the key read from a mode-0600 file and never printed); one signing key per cell, kept offline, with a rotation drill before the first rotation; `key_fp(pem, aud)` fingerprinting the audience with the material; one key material under one audience only (F2); and the keys contract revision r7 (`keys.schema.json` with `aud` required, three golden vectors, MULTITENANCY §14.1 r7, CONTROL-PLANE-INTEGRATION §7.5 rule 7), which binds the Control Plane's Stage 1 producer too | r7 in phase A's first push (edge #103); custody and the drill go into the runbook before the first cell |
| Shared cells Q9, operator-minted tokens (2026-10-08) | Design partners get tokens the operator mints with `cell-admin`, at most 24 h (PLAN decision 1, option (c)); the platform's Stage 1 issuer serves self-serve | Edge #111 |
| Shared cells Q9, k = 8 (2026-10-08) | A shared cell is shared k = 8 ways: a project's quota on each shared bound is at most ⌊bound ÷ 8⌋, and a 0 or missing quota takes exactly that ceiling (PLAN decision 4) | Edges #100 and #110; installed in production by Q2(a) |
| Shared cells Q9, a noisy shard (2026-10-08) | A tenant aiming load at a neighbour's shard (H2) is handled by per-project ceilings and a shed alert now, and by per-(project, shard) attribution in phase B (PLAN decision 6, option (a)) | A7 stays ignored on it |
| Shared cells Q9, offboarding and erasure (2026-10-08) | A deleted stream is crypto-erased at once, and its objects are physically reclaimed with stream reclamation (cost lever E11), not before; a project id is never placed again (PLAN decision 10) | Edge #112; SPEC G8 |
| Shared cells Q9, free-tier request rate (2026-10-08) | Deferred to phase B and recorded when its step is planned; it applies to self-serve only (PLAN decision 4) | Phase B |
| Shared cells Q10, four small behaviours (2026-10-08) | (a) A touch journal with no waiter and no touch for 10 min is retired (a returning watcher gets one `stale` and resynchronises; an edge record), before a partner watches many streams. (b) A project's queued append bytes are capped at the bound ÷ k (64 MiB at k = 8) in the Q2(a) wiring commit, amending #100. (c) The SDK's retry after a wake is accepted for design partners, and the stale-feed `503`s carry `Retry-After: 1`; a bounded wait on the first stale request comes before self-serve if SC1 shows raw clients matter. (d) The operator applies the u = 0.5 packing rule by hand for the first cell; `cell-admin` checks it once phase B declares load | (c) in phase A's third push (#102 amended); (a) and (b) follow landing |
| Usage drain at 8 s (T1, 2026-10-08) | The re-pinned R09 mechanism test is approved (a graceful stop waits 10 s for the last drain), and the usage answer's `possibleReadLossWindowSeconds` is corrected to the real window under a new exception-growth row, with an edge record | NEXT-WORK section 14.2, item 2 |
| Cross-server split gate (T2, 2026-10-08) | Every server running in fleet mode declines a split, not only a ring of more than one; CI's `livefeed-fleet-cert` splits with one server outside fleet mode and then restarts all three in fleet mode on the same storage, and the LiveFeed canary and three Docker ladder steps change the same way | NEXT-WORK sections 14.2 and 14.6 |
| Fewer default shards (T3, 2026-10-08) | The fleet-mode default is the largest power of two at most `FLEET_MAX`; it lands after T2, and the fleet test rigs pin `INITIAL_SHARDS` until `home-v1` placement is built, before any multi-server cell | NEXT-WORK section 14.4 |
| Run leases, a failing record's key-mates (T4, 2026-10-08) | Option 2 lands in the push of run leases: once a key's lowest unacked record has been delivered twice, that key is leased one record at a time; option 4, the SDK documents that one batch can hold a key's run; option 3, draining a poisoned run to the dead-letter stream in one pass, comes later (it updates two approved rows) | NEXT-WORK section 14.2, item 1 |
| Admission cap at the 100 ms tier (T5, 2026-10-08) | Measured in the 100 ms model check before anything changes: the 512-request cap may bind at about 3,700-5,100 appends/s per server at 100-140 ms acknowledgements (derived, not measured) | NEXT-WORK section 14.8 |
| E7 and the holdoff helper (T6, 2026-10-08) | E7's edit of the R09 fleet-population test is accepted; exception-growth rows are approved for one `holdoff_verdict(now)` helper that removes the two `sharddir` mutants at the holdoff deadline | NEXT-WORK sections 14.7 and 14.9 |
| Layout 5's edge records (L1, 2026-10-08) | Ratified as drafted, before landing: usage and retention count stored page bytes (medium); a server reads only layout 5 and refuses older storage with `unsupported_storage_layout` (high); `FRAME_COMPRESS` is no longer read (low); `format=frames` answers uncompressed version 4 frames only (low); a history read refuses pages that do not exactly cover its window as corruption (high). Drafted as #126-#130, numbered #125-#129 in landing order | `edge-changes.md`; layout 5's pushes write them (NEXT-WORK section 14.2, item 1) |
| Layout 5's proof-bearing page type (L2, 2026-10-08) | `CheckedPage` replaces the deleted `CheckedFrame` in the compiler fixtures and the source gate's proof-bearing types, with the same checks (built only by admission, private fields), as layout 5 commits it | NEXT-WORK section 14.2, item 1 |
| `decode_frame` without a production caller (L3, 2026-10-08) | One reasoned `#[allow(dead_code)]` on `decode_frame` in the by-path `src/crypto.rs`, as on `decrypt_frame`: the keys CLI, cryptobench and the tests still call it | NEXT-WORK section 14.2, item 1 |
| `frame_compress` removed (L4, 2026-10-08) | `CryptoConfig::frame_compress` is removed in one commit before layout 5 lands: `bootstrap::run` stops passing it (its frozen growth row takes exactly the gate's `after` values), `render_raw_read` calls `read_payload(.., false)` (one growth row for its `unwrap_used` fingerprint), and the pinned `r01_retained_legacy_frames_and_new_versions_read_together` drops its `from_enabled` calls and is re-pinned | NEXT-WORK section 14.2, item 1 |
| Layout 5's open measurements (L5, 2026-10-08) | One heap profile per arm (incompressible data reaches the RSS shed line sooner), joined to Q0's memory work, and a controlled single-record rerun on an idle host (1.25x layout 4's CPU), both after landing; no rule that stores small bodies without zstd. Layout 5 is not called memory-neutral until they finish | NEXT-WORK section 14.8 |
| Smaller layout 5 calls (L6, 2026-10-08) | The usage field `frame_bytes` keeps its name until the billing meter redefines the usage surface, which renames it under one record; the keys CLI decrypts a page after landing, on the library crate, not by `#[path]` (crypto review F9); crypto review F3's guidance (do not batch secrets with attacker-controlled data into one append) is the SDK's security notes, written now | `sdk/README.md`, "Security notes"; NEXT-WORK section 14.2, item 1 |
| The Compute rung C1 under the split gate (T7, 2026-10-08) | `bench/docker/harness/cluster-run.sh` C1 takes its split as T2's rigs do: on streams-1 outside fleet mode, then all four servers redeployed in fleet mode over the same `PATH_PREFIX`. A script change only; running C1 stays a field run the owner starts | NEXT-WORK section 14.6 |
| Fleet rigs' shard count (T8, 2026-10-08) | `deploy-fleet.sh`, its two families and the RUNBOOK §7 recipe keep `INITIAL_SHARDS=16` over four servers (the placement they were measured with, warned at boot) until `home-v1`, not 4 with the uneven [1, 1, 0, 2] draw | NEXT-WORK section 14.4 |
| The usage drain's bounds (T9, 2026-10-08) | (a) A drain round takes up to 256 dirty rows per shard, not 64, so at 8 s 1,000 active segments trail by about 32 s, not 128 s. (b) A graceful stop's terminal drain round is bounded by min(cadence, 5 s), not one cadence, so `TELEMETRY_DRAIN_SECS` of 10 or more no longer lets the 10 s grace abort it, and no setting is refused. Each red first, with a low-risk edge record | NEXT-WORK section 14.2, item 2 |
| The SDK iterator after a retry (T10, 2026-10-08) | Not now: the iterator that skips and retries a key's later messages after `msg.retry()` comes with T4's option 3 (the one-pass dead-letter drain); until then the SDK's README tells consumers to handle a key's run in order | NEXT-WORK section 14.2, item 1 |
| Bench and conformance lockfiles (H1, 2026-10-08) | Lockfiles only: `bench/probe` and `bench/awsbench` take rustls and rustls-webpki at patched versions within semver, `conformance/package-lock.json` takes `npm audit fix` without `--force`; nothing is deployed, and the running probes pick the fix up at the next deploy the owner starts | The lockfile commits after the E7 push; no NEXT-WORK item |
| A pull's own reservation and its body (C1, 2026-10-08) | A consumer pull buffers its body before it reserves its coverage, so a request's own reservation never refuses its own body; past the write latch and the body check, which see only its project's other bytes, a pull refused for its project's other read bytes waits and is counted, as #114 says. #114 and #115 are amended | 35a1e06d (red first), c6d88d0e (the amendments) |
| The shard-close hang (C2, 2026-10-08) | Investigate it, and fix it red first if real. It was real: a history partition whose L0 is at its cap held its engine's close until the next 300 s manifest poll after compaction freed a slot, or for good; the partition now closes without its final flush (edge #134, awaiting ratification). The absorber's stall at the cap is the owner's call | 7529b198, e616fc75; NEXT-WORK section 14.7 |
| The chain's and the follow-ups' edge records (C3, 2026-10-08) | Ratified as drafted: #117-#121, #123, #124 and #130-#133, and the amendments to #100, #101, #110 and #116 | `edge-changes.md` |
| `bootstrap::run`'s six rows for the cell ceiling (C4, 2026-10-08) | The implementer applies the values the gate prints on the rebased tree, quoted in the commit for the owner to check afterwards | 536558f3 |
| The idle-journal sweep's thread (C5, 2026-10-08) | Accepted as built: one raw thread spawn in `src/touch.rs`, with a reasoned `#[expect(clippy::disallowed_methods)]` and an `owners.json` effect row, as the flusher has | 3c34f64f |
| T9(b)'s R09 re-pin (C6, 2026-10-08) | Approved: `r09_active_telemetry_cancels_entered_storage_and_preserves_debt` gives the stop the terminal round's 5 s bound, its oracle unchanged, and is re-pinned; T9(b) and its record #122 land after the chain | c12ef0a3, 34fefc22 (edge #122, awaiting ratification) |
| TLA-018's check replacement (C7, 2026-10-08) | Accepted: `probe-lost-durable-canonical` becomes a negative control beside a stronger passing baseline, which removes a check name from the manifest | 78c28cbf |
| `cell-admin`'s memory-line rule (C8, 2026-10-08) | Confirmed: a profile is refused unless three projects at their lines cannot fill the server's read memory (3 × line < read memory), in place of line ≤ read memory ÷ k | f0e9f2de |
| The wrapper's feed polling and the Compute gate (C9, 2026-10-08) | Validated in the Q5 certification run in fra already approved, with no extra deployment; if presigned conditional GETs misbehave there, the fallback is a stat then a GET | NEXT-WORK sections 11 and 14.4 |
| Dependency leftovers and a dead parameter (C10, 2026-10-08) | All three: (a) `bench/awsbench`'s AWS clients take the SDK's current HTTPS client, so rustls 0.21 leaves its lockfile; (b) vitest 4.1.11, pinned exactly in `conformance/package.json` and `src/protocol_pin.rs`; (c) `read_payload`'s dead `compress` parameter is removed with one growth row on a render function, which makes #128 structural | (a) b72a6387; (b) 10aade39; (c) held: the gate needs a second row, on `render_product_read`, which the owner has not approved (NEXT-WORK section 14.2, item 1) |
| Item 50 | Option (a): preserve live bindings, cap 32,768, reject `HANDLE_IDLE_EVICT_SECS=0` | 45f8711c (holder rule added beside the counted pin; the pin's full retirement not done) |
| Effective configuration | Method and E3 transcription accepted; 120 s default accepted | Deployment gates below |
| Bug #7 | Option (b), explicit migration; activation gated on a real-DB rehearsal | Not started |
| Item 40 | Separate Critical heartbeat, progress and eligibility | Steps 1, 2 and 4 landed (the commits NEXT-WORK section 4 names; edge #63 ratified 2026-09-28 and amended); step 3 (scoped withdrawal) deferred as its own edge decision; H2 name arbitration, the drain-bound amendment and the spec paragraph await the owner (NEXT-WORK section 4) |
| F1 | Authenticated fleet-internal seal-fence operation | F1-a implemented on the owner's instruction of 2026-09-30: the route table moved to `src/http/internal_routes.rs`, the owner-side receiver (edge #88) and the non-owner's relay (edge #89), both ratified on 2026-10-01; TLA-002 and TLA-003 re-recorded (932a85f7). On the owner's decision of 2026-10-01: the plan's negative model control (commit 4, 33eee6ac) and the platform contract naming `segment-close` and `seal-fence` (commit 5, edge #90, awaiting ratification). Open, the owner's: a mutation owner for `fence_relay.rs`. F1-b (split producer lanes) not started |
| F2 | Keep the unknown-outcome model; public append/seal plus successor composition test | The composition test landed (445955e9, `dst_tests::retiring_written_group`): the retryable unknown answer, one copy on the successor, the producer-keyed retry as a duplicate, the owed final kept and completed; no behaviour contradicts the model. Option A and the fenced-write mapping (plan §9 D6) await the owner |

### Deployment gates (block deployment sign-off, not merging)

- Compute validation of the wrapper -> binary -> platform lifecycle (startup,
  readiness, restart, signal delivery, memory pressure, rollback).
- The Compute deployment owner enumerates every target project and exports
  its redacted configuration; comparison and boot validation re-run on the
  final candidate with the actual persisted namespace constraints (the
  archived comparison's "HEAD" is an earlier revision).
- A release-posture Compute family (production fleet authentication, usage
  and audit configuration); the static fleet-auth bridge stays a benchmark
  exception.
- Upstream idle compatibility with the 120 s header timeout, with margin, or
  an explicit validated timeout.
- Bug #7 rehearsal on a consistent copy of a real rollup database before its
  format-changing activation.
- Release-wide verification on one final artifact: the mutation campaign over
  the whole hardening range, including billing, rollup, product and usage
  owners the per-push plan does not select. Per-push runs so far (for
  example 81 mutants over 26c555dd..9813d1cb) are not that campaign.

## Follow-ups found in review, not done

- Typed classification of an append's first registry read: a transient store
  failure still answers 500 there (F3 plan D2). Today the registry reports an
  injected or real store failure and a descriptor corruption with the same
  error shape, so the fix needs a typed registry read error first; corruption
  must stay a fail-closed 500.
- Stale-page repair after a refused chain: superseded by d16559b3 (readers
  admit agreeing overlaps); the composed public-read test is b059e4a2.
- Item 50's plan retires the counted admission pin in favour of the holder
  rule alone; the holder rule now sits beside the pin.
- Closure debts (2ba4bc47; the settlement pass's starvation fixed in
  40dbf0d3): tests for month crossing, owner movement mid-debt and a crash
  between the debt write and the replacing write.
- `parse_month` accepts a `+` sign ("2026-+9"), answering a zero row instead
  of 400 `invalid_month` (pre-existing, low).
- `dst::dst_tests::admission_maintenance::first_request_waits_for_restoration_then_sees_the_restored_ledger`
  orders its request with fixed sleeps and failed once under host load.
