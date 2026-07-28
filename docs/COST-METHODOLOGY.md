# Object-store cost testing — methodology

Two standing local tests measure what the system spends at the object
store, per unit of useful work. Both run from `bench/costab/` against
the same rig; both treat s3lite's physical-request ledger as ground
truth. Results live beside this file (COST-AB1.md, COST-WIDE1.md); this
page is the method, so a rerun means the same thing next quarter.

## The rig (shared)

```
awsbench (generator) ──HTTP──▶ streams-slate ──S3 API──▶ s3lite (25 ms/op)
                                      │                        │
                            /v1/debug/{store,timings,usage}    /_s3lite/stats2
```

- **s3lite** (`src/bin/s3lite.rs`) injects 25 ms per op — the region-
  local Tigris figure — and keeps a cumulative ledger per
  (tier, kind, op) split by status: `/_s3lite/stats2`. Tier is
  shard | hist | fleet | registry | other from the key layout; the
  rollup prices requests the way public Tigris does (PUT/LIST/multipart
  Class A on 2xx; GET/HEAD Class B on 2xx; 304/404/412, deletes and
  errors free). One Class A = 10× Class B.
- **Server env** is the field soak posture (soak7): gather 6 ms, ring
  32 MiB, 4 shards, `FLUSH_INTERVAL_MS=25`, absorber 4 MiB / 60 s,
  compactor 500 ms, shed line 600 MB. Fleet coordination is off
  (`FLEET_PREFIX` unset), matching soak5/6/7.
- **Generator** is the field binary (`bench/awsbench`), records
  1 KiB × batch 10 unless a test says otherwise.

Measurement invariants (violate one and the numbers mean nothing):

1. Same s3lite and generator binaries across the runs being compared;
   only the server binary (or the regime parameter) differs.
2. Fresh s3lite (fresh memory, zeroed counters) and a fresh
   `PATH_PREFIX` per run — counters are cumulative, no windowing.
3. Zero generator errors and exact decode/ack integrity, or the run is
   void.
4. No builds or other CPU load on the machine during a run.
5. Latency here is a **guardrail**, not a field claim: the rig has no
   platform edge, so per-tier medians of the 20 s windows are compared
   run-to-run, never quoted as regional numbers.

## Test 1 — 30-minute tiered soak (hot-path cost A/B)

*What it answers:* did a change alter what the **hot ingest + live
read** path spends per record — and did it cost any latency?

```bash
bench/costab/run-soak.sh <label> <server-binary> <out>   # 30 min
python3 bench/costab/compare.py <baseline-out> <after-out>
```

- Ramp: `SOAK_TIERS=1,2,3,4 × SOAK_SECS=450` — closed-loop appenders
  plus one long-poll tail consumer, one hot stream.
- The ramp is deliberately NOT the field ladder (`1..64×180 s`). A
  local rig runs ~10× the field's per-tier record rate (no edge RTT,
  ~32 ms acks), and the field ladder drives a single stream past the
  absorber envelope: the history flush stalls over
  `max_unflushed_bytes`, RSS climbs through the shed line, and the
  stream 429s without recovering. conc4 ≈ 12 MB/s holds flat RSS for
  the full half hour; conc6+ accumulates. (The wedge itself is a
  standing finding — docs/COST-AB1.md §Findings.)
- Read the comparison as: billing rollup by tier → notable cells →
  bytes → per-tier latency medians (each tier's last 20 s window is
  dropped; it straddles the step-down). A change is clean when the
  shard/WAL cells are byte-identical and tier medians move < 1 ms.

## Test 2 — 15-minute wide test (cardinality cost)

*What it answers:* what does it cost to **carry** streams, as opposed
to serving them — the review's "many lightly used streams" regime.

```bash
bench/costab/run-wide.sh <label> <server-binary> <out> <n-streams>
python3 bench/costab/wide-report.py <out-1> [<out-2> ...]
```

Three standing regimes, identical except for total cardinality:

| regime | streams | active |
|---|---|---|
| w1k | 1,000 | 100 |
| w10k | 10,000 | 100 |
| w100k | 100,000 | 100 |

Each run has two phases, split in the ledger by a snapshot taken at the
generator's `SETUP_DONE` marker:

1. **Setup** — create every stream (registry PUT), then seed each with
   ONE 1 KiB record. Seeding matters: a stream with data is what
   carries cost (a shard-log resident, an absorber claim, eventually a
   per-stream history DB). Setup cost is reported per stream.
2. **Steady (15 min)** — the first 100 streams append batch 10 × 1 KiB
   every 500 ms each (≈ 200 req/s, 2 000 records/s total); a scanner
   cold-reads one random *inactive* stream from offset 0 at 2/s (the
   history-reader cardinality path: cache cap 8 against a huge working
   set). Steady cost is reported as Class A/B per active-stream-minute
   and per million records, next to the same figure from the other
   regimes.

Wide runs raise `ADMIT_RSS_SHED_MB` to 1400 (env `WIDE_SHED_MB`): with
100 active streams the workload fills all four engines' 32 MiB tail
rings (+96 MiB against the one-hot-shard soak) and absorber DB churn
adds more, so the 600 MB field line sheds mid-run and truncates exactly
the signal under test. RSS is reported per regime; any figure above
600 MB means a 1 GB field instance would have shed there — that
observation is part of the result, not hidden by it.

The regimes differ only in how many *inactive* seeded streams exist, so
any steady-state cost delta between them is the marginal cost of
carrying cardinality: absorber passes over seeded-once streams (each
one opens a per-stream history DB through an LRU of 4), their
history-DB creation traffic, scanner-driven cold reader opens, registry
and shard-log growth. Watch three things beyond the ledger:

- **absorb lag** (`/v1/debug/usage` snapshots): whether the absorber's
  sequential per-stream loop keeps up with the seeded backlog — at
  100 k it structurally cannot, and the report states the drain rate;
- **RSS** (per-snapshot `rss.log`): resident cost of handles, registry
  cache and reader churn at cardinality;
- **append/scan latency windows** (`wide.jsonl`): whether carrying
  cardinality taxes the active path.

## Reading results / history

- docs/COST-AB1.md — Test 1's first A/B: unkeyed `k!` history duplicate
  removed + registry conditional GETs.
- docs/COST-WIDE1.md — Test 2's first three-regime run.
- Both tests exist because the review's verdict was that request cost
  must be a measured property, not an estimate; when a run and the
  review disagree, trust the ledger.
