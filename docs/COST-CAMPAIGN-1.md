# Campaign report — boundary race, request-cost rounds 1–2, wide cardinality, absorber follow-ups

**2026-07-27 → 2026-07-29, `slate` 7eb06a6 → f082779.** One document
covering what changed, what was measured, and what it taught us. Detail
lives in [COST-METHODOLOGY.md](./COST-METHODOLOGY.md) (how the tests
work), [COST-AB1.md](./COST-AB1.md), [COST-WIDE1.md](./COST-WIDE1.md),
[COST-WIDE2.md](./COST-WIDE2.md) (results), and the five commit
messages (9f400bb, 583ec28, 3a95377, 31b6cf4→ddd22bd, f082779).

## Round 0 — the absorbed-boundary race (9f400bb)

A once-in-fifteen-suite-runs DST flake (`acked_records_survive_
absorption_into_history`, I1) turned out to be a **client-visible
data-loss-on-read bug**, not a test artifact. `read_merged` snapshots
`durable.absorbed` and then scans the shard-log tail — a TOCTOU pair.
Concurrent absorption could advance the boundary and durably trim the
tail mid-read, producing a page with a ten-record hole and
`completed = true`; a paginating client would skip those records
forever. Two enablers, both proven by tracing a reproduced failure
(12-way process hammer turned "once per fifteen suites" into "every few
minutes"): duplicate `Absorbed` ops collapsed the deferred-trim lag,
and trim deletes become scan-visible at batch durability while handle
state publishes only at dispatch.

Fixed by making the read path self-validating (head-gap / durable-
tracker revalidation, gap re-served from history) plus trim-on-advance-
only, absorber submit high-water mark, and a sound oracle wait
condition. Validation: deterministic regression test red→green, 1,680
hammer runs and 6 full parallel suites clean.

## Round 1 — first cost cuts, measured (583ec28, 3a95377)

Built the measurement first: s3lite now keeps a cumulative physical-
request ledger per (tier, kind, op, status) with a Class A/B/free
rollup at public-Tigris-shaped prices (`/_s3lite/stats2`). Then two
review items, A/B'd over the full 30-minute tiered soak on a local twin
of the field rig:

- **Unkeyed records no longer get the `k!` index copy in history** (a
  full payload duplicate — double history bytes for unkeyed
  workloads). Empty-key filtered reads serve from `r!` instead; API
  unchanged, DST-pinned.
- **Registry TTL refreshes are conditional GETs** — unchanged
  descriptors come back 304, uncharged.

Result: history tier Class A −21 % / Class B −39 % (sst GETs −56 %),
registry billable GETs 1,238 → 0, store bytes −28 % / −31 %, RSS
−51 MB — with the WAL ingest path byte-identical and per-tier latency
medians within 0.3 ms p50 / 1 ms p99. Both changes later held at
cardinality (100k streams: ~118k registry refreshes in a run, all
free).

## Round 2 — the wide tests (31b6cf4, ddd22bd)

New standing test: create N streams, seed each with one record, then a
15-minute steady window (paced appenders + a scanner cold-reading
random inactive streams), ledger split into setup vs steady. Run at
1k/10k/100k total streams × 10/100/1000 active, total load held
constant. Twelve clean runs (zero errors, 1.8 M records each), and the
regimes separated four load-bearing facts:

1. **Per-stream history is a rate ceiling, not just a price.** The
   serial absorber ground at a flat ~4.5 streams/s regardless of
   backlog (draining 100k seeded streams: ~6.2 hours), at ~43 Class A
   per one-record stream ≈ **$215/M sparse records vs ~$0.02/GiB for
   batched WAL ingest** — the review's "largest long-term cardinality
   risk", now with a rate and a price.
2. **The 900 s KeyCache TTL strands deep backlogs** — keys expire
   before the grind reaches them, so wide sparse data parks in the
   shard log until re-touched, and the 15-minute bill *plateaus* with
   cardinality while completion stretches toward never.
3. **Active-set size is a pure latency knob at constant throughput**:
   append p50 37 → 48 → 59 ms across 10/100/1000 active at every
   cardinality with WAL PUTs flat — arrival alignment with the pump
   cycle, not requests. The closed-loop soak's 32 ms is the aligned
   best case, not the wide-workload number.
4. **The backlog was invisible.** Absorb lag read 0 under every shape,
   including 1000 fresh-keyed streams at 3.7× over the ceiling.

## Round 3 — follow-ups implemented and verified (f082779)

- **Concurrent small-pass lane** (≤1 MiB pending overlaps at
  `ABSORB_CONCURRENCY`, default 6; big streams keep the serial
  validated path; per-tick caps, fattest-first, chunked eviction, DB
  open moved outside the cache lock). Drain ~272 → ~870 passes/min
  (**3.2×**): w10k's backlog completes in-window (24 % → ~99 %),
  append latency and error counts untouched.
- **Re-discovery sweep**: unabsorbed streams re-found from resident
  handles — signals demoted to fast path (the bounded channel provably
  dropped ~35k of 100k seed signals; restarts had none).
- **Truthful lag**: the `/v1/debug/usage` join was broken by a hash-
  keyspace mismatch (usage keys by name hash, absorber by engine hash
  — it could never match); now linked, plus a truncation-immune
  `absorb_backlog` aggregate. Verified at 100k: 86,947 lagging /
  max 1,038 s, arithmetic consistent with absorbed count. The
  heartbeat's aggregate was never broken — WIDE2 §4 corrected.
- Key-less retries back off (tick × 64); the absorb-on-next-touch
  contract is now explicit.

Trade-off stated plainly: window Class A roughly doubles because 3.2×
more per-stream passes complete per window at the unchanged per-stream
price. Faster completion, same total cost per drained stream —
partitioned history remains the structural *price* fix.

## Learnings

**About the system.**

- Boundary races hide behind publication lag: state the readers see
  (`handle.state.durable`) trails what scans can observe (batch
  durability) by dispatch, so "snapshot then scan" needs its own
  revalidation — heuristic lags (deferred trim) narrow windows but
  can't close them.
- The cardinality tax is architectural: per-stream history DBs cost
  ~43 Class A per stream *touched*, whatever the data volume, and the
  open is also the rate ceiling. Concurrency buys completion, not
  price.
- Observability can lie in three independent ways at once: a join
  across mismatched hash keyspaces (always 0), a bounded `try_send`
  (silently absent), and a listing cap (truncated). Aggregates
  computed at the source survive all three.
- Arrival alignment is a real latency tier: the same throughput costs
  +21 ms p50 when spread thin. Any per-stream pacing work must be
  measured at the 1000-active shape.
- Free responses are real money: 304s (registry) and the free class
  (deletes, 404 probes) let hot paths stay chatty where it's
  uncharged; the ledger has to price by status, not by request.

**About the method.**

- Request counts must be measured at the store, cumulatively, split by
  phase — the trailing-window latency telemetry can't answer cost
  questions. A local twin with a physical-request ledger is the sharper
  instrument for counts; the field remains authoritative for latency.
  **Everything in this campaign is local; field validation of round 1
  and the follow-ups is still owed.**
- Load must respect the engine's envelope or the run measures the
  wedge, not the change: the field tier ladder at local speeds
  reproduces the soak7-sjc-shaped non-recovering stall on demand
  (worth its own investigation; also a usable repro).
- Every A/B needs integrity gates (zero errors, decoded == acked) and
  unchanged-path guards (WAL cells byte-identical) or the comparison
  is void.

**About process — two mistakes worth keeping.**

- The first follow-up implementation collapsed under its own sweep
  (open DBs unbounded once eviction moved to tick-end: 2.3 GB RSS in
  seven minutes). The verification run caught it before it shipped;
  per-tick caps and chunked eviction exist because of that run.
  Verify-with-the-workload is not optional for scheduler changes.
- A hammer "found" 180 failures that were one wrong binary (`ls |
  head -1` picked the server, not the test executable, after a rebuild
  changed the deps dir) — and the first thing it cost was the evidence
  of a run that had never actually failed. Diagnose the artifact
  before reacting to it, and never delete failure logs while
  investigating them.

## Open items, in order

1. Partitioned/shared history DBs — the structural fix for both the
   per-stream price and the cold-reader cost (review §7).
2. Cold reads of absorbed sparse streams: ~330 ms per-stream DbReader
   open + checkpoint PUT (re-confirmed after the drain fix: w10k scan
   p50 28 → 321 ms once its population absorbed).
3. GC LIST cadence / exact-candidate GC (~18.8k Class A LISTs per
   30-min hot soak) and compactions-state churn (~3.2k).
4. Fleet tick cost (1 PUT + 2 LISTs + N GETs per 2 s per instance)
   when `FLEET_PREFIX` deployments arrive — off in all soaks so far.
5. The 5 s L0 flush ticker → recovery budget for sparse shards.
6. The non-recovering saturation wedge (history flush stall →
   permanent 429s), now locally reproducible.
7. Field runs: round-1 A/B and the absorber follow-ups on a real
   region; `ABSORB_CONCURRENCY` sizing for 1 GB instances (2–3, not
   the default 6).
