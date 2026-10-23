# Native queues — review responses

The review record for [QUEUES-DESIGN.md](QUEUES-DESIGN.md): what each review
asked and where the design answers it. Section references (§) point into
the design. Full review texts outside the repository are under the lead's
analysis directory (`~/.streams-k2/analysis/queues/`).

## 1. Fable 5.1 review of revision 7 (2026-10-23): approve with changes, all taken

Full text: `DESIGN-REVIEW-7.md`. Revision 8 answers it.

| Point | Revision 8 |
|---|---|
| B1 quiet-shard GC never completes (empty-L0 cutoff fallback) | completion rule with one residual L0 file; SlateDB patch only if Q0 shows it matters (§8.2 stage 4, D22) |
| B2 name the pass primitives | memtable flush, `Db::manifest()`, `CompactionSpec::for_segment`, `Admin::submit_compaction`, overlap retry, HEAD per SST (§8.2) |
| B3 idle release and the wake row do not exist | timers through `probe_debt` and the billing sweep's rotation bound (§6.8, §5.6 table) |
| H1 age trigger rewrites every 15 minutes | minimum pass size with a maximum interval (§8.2) |
| H2 a pass stalls writers after about 2 minutes | pass-duration bound, zero-stall gate, concurrency an owner decision (§8.2, QUEUES-PLAN §2.5) |
| H3 reservations cap queues per shard | the arithmetic stated, smaller defaults with opt-up for D21, re-reserve on update (§5.7) |
| H4 Q0 too wide | storage-only Q0 with Fable's build order (QUEUES-PLAN §2) |
| M1 stable data is the shard-log resident set | co-tenant baseline defined (QUEUES-PLAN §2.2) |
| M2 the last rows' debt | shard-level debt row (§8.2) |
| M3 no process-wide compaction budget | one queue pass per instance; other schedulers as today (§8.2) |
| M4 rotation vs the 60-minute target | resident-shard target; rotated shards complete within rotations (§8.2) |
| M5 overlapping scheduler jobs hold the watermark | overlap case (QUEUES-PLAN §2.3) |
| M6 missing measurements | QUEUES-PLAN §2.4 |
| M7 failed backlog is Q3 | moved (QUEUES-PLAN §1) |
| L1-L7 | totals 85-118; reservation release at debt clear; budget refusals do not touch `lastNotifiedAt`; the hot debt row; the §5.6 row; citations; this record |

## 2. Pricing goal (2026-10-23)

The owner set the goal: Cloudflare parity with an 80% infrastructure margin,
benchmarked on the control plane's own queue month. A colleague's pricing
note supplied the billing-unit and idle-cost points. Taken: billing per
logical operation per message per 64 KB, empty claims free and writing
nothing, bandwidth and standard retention included (D14, §5.5); the
benchmark and its estimate (QUEUES-PRICING-BENCHMARK.md); a cost gate in Q0
and the decisive run in Q5. The note's lower price levels ($0.30 and $0.20
per million operations) are recorded as later options; the goal is parity.

## 3. Review of revision 6 (2026-10-23): all taken

The review approved the architecture and the Q0-first sequence.

| Point | Revision 7 |
|---|---|
| Collection needs a deletion-and-flush boundary | debt created by deleting batches; the six-step coverage rule; pass complete only at stage 4 (§8.2) |
| Launch age vs completion deadline | separate numbers, launch age derived from Q0's pass duration (§8.2) |
| Auxiliary rows have their own lifetimes | accounting by lifetime (§8.3); dedup budget (§8.4); dedup expiry creates debt (§6.8) |
| Redrive deadline conflicts with retention | per-cycle deadlines, one rule for retries, `R` claims and lease expiry (§6.3, §6.1, §7.3) |
| Redrive bypasses the admission budget | redrive reserves budget; stop-at-capacity for `all` (§6.6) |
| Delete needs a durable fence | `deleting` state per incarnation, conditional tombstone (§8.2) |
| A drain can strand notifications | inactivity predicate on `lastUsefulClaimAt` (§9.2) |
| Timers with zero debt | roster deadlines are maintenance obligations; residency or a wake row (§6.8) |
| Fair share does not isolate stored data | reservations at creation, no overcommit (§5.7); D21 |
| Request formula omits sends | producer and consumer terms (§6.5); D10 |
| Q0 definitions | new cases, a baseline-aware churn bound, a stated cost workload, batch sizes, sparse traffic (§16) |

## 4. Fable 5.1 review of revision 5 and the owner's direction

Fable's verdict was "revise"; its code claims hold. Revision 6 answered it:
queue storage mode with guarded consumers (B1, §5.6); the four-stage
lifecycle with an L0-reaching plan and quiet-shard GC (B2, §8.2); the queue
delete lifecycle (H1); the monotone retired bit (H2); the queue roster (H3);
durable re-arm state (H4); capacity rules, now reservations (H5); the failure
clock restarting on redrive (H6); and M1-M9, L1-L5 (queue clock row, citation
corrections, point gets, the new ledger path, Compute `waitUntil` to verify,
egress details, Q0 thresholds, dedup after reclamation, extend while paused).
The owner chose option (a) with an explicit storage kind, durable debt,
amount-and-age scheduling, quiet-shard GC in scope and the escalation path.

## 5. Earlier reviews

The colleague's reviews of revisions 2 and 4 are taken except where narrowed
(a settle bucket instead of a shedding exemption; no payload push; ordered
groups later; one scheduling owner per queue). Fable's reviews of revisions 1
and 3 stay answered as recorded in revision 4 (`git show a768e650:docs/QUEUES-DESIGN.md`).

