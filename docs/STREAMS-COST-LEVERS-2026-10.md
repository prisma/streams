# Streams cost levers against Cloudflare K2 (2026-10-24)

The owner asked for Streams at Cloudflare K2's price (produced $0.04/GB,
consumed $0.04/GB, retained $0.02/GB-month, uncompressed bytes, fetched
2026-10-24; billing not yet enabled) and an 80% infrastructure margin.
Fable 5.1 reviewed the design for cost to operate; its report, model and
sensitivities are kept outside the tree
(`~/.streams-k2/analysis/streams-cost-2026-10-24/`: `REPORT.md`,
`streams_cost_model.py`). This file records what was measured and landed.

## The benchmark (summary of the report's section 4)

Four one-tenant profiles on the launch shape (one single-server shared
cell, one shard, ring off, 100 ms tier, layout 5): T tiny/idle, S small
(2 req/s of 1 KiB singles, 20 streams, a tail per stream, one group), M
medium CDC (100 GiB/day batched, two groups, tails), L large sink (150
GiB/day, one daily walk). Pass rule: fully loaded cost at most 20% of the
profile's K2 bill. Measured on the local K2 rig (`bench/k2cost`) by
counts, which host load cannot move; CPU and RSS only from a 1-vCPU Linux
host. Modelled today (owner's prices, 60% window fill): T $0.055 against a
$0.0002 K2 bill, S $11.1 against $0.50, M $422 against $530 (+20% margin),
L $245 against $483 (+49%). No profile meets the 80% rule; T and S cannot
cover their per-request cost at any per-GB price (a pricing decision).

## Screening (2026-10-24, launch posture, pairs run at once on distinct ports)

`bench/k2cost/run-local.sh` with `K2_PORT_BASE`, `K2_SERVER_BIN` and
`K2_SERVER_COMMIT`, points `SCR-S`, `SCR-M` and `SCR-IDLE` (20-25 min
each), read with `counts.py --compare`. Reference: slate 2e0d1b86.

| Lever | Point | Store counts, candidate against reference | Landed |
|---|---|---|---|
| Live-window scan cache (E2: a live read keeps the blocks it fetched) | SCR-M | `shard/sst/get` 21,526 -> 1,702 (-92%), Class B -58%, GET bytes -24% | yes |
| | SCR-S (ring off) | `shard/sst/get` 4,895 -> 1,580 (-68%), Class B -19%, GET bytes -37% | |
| Tail ring 8 MiB in `shared-cell.env` | SCR-S | `shard/sst/get` 4,895 -> 1,102 (-77%), Class B -18%, GET bytes -41%; 2,547 ring hits | yes (deployment posture: the owner's hold) |
| `_ops_metrics` every 60 s, alert rules on their own 15 s tick | SCR-IDLE, per hour | Class A 2,722 -> 1,721 (-37%), WAL PUTs -19%, L0 PUTs -68%, Class B -12%, PUT bytes -29% | yes (edge #187 awaits ratification) |
| History compaction fan-in 8/16/8 (E5) | SCR-M | history compaction PUTs -30%, total PUT bytes -3%, history GETs +7% | no: the effect grows over days (copies 2.9 at 20 min, ~10 at a month); needs a 24 h pair and the owner's memory acceptance (+8 MiB a history compaction) |

Two arms were VOID by the rig's validity rules (a quiescence timeout; one
late generator send), never by an error or lost data; their counts are
read all the same, and the longer arm can only overstate its counts.

Dollar effect, by the report's model and these counts: S about -$5 a month
(ring and scan cache overlap on tail reads), M about -$22, the idle cell
floor about -$3.5 a cell-month. Deferred as small: binary billing rows
(-$0.1 to -$0.7, seven receipts to re-record).

## What decides the goal next

The report's largest levers are multi-week: consumer-pull CPU (a pull
decodes a 4 MiB coverage window whatever it delivers: M -$145) and layout 6
"WAL plus one copy" (M -$109, L -$142; owner-decided). Even with every
lever the model leaves M at $141 against a $106 ceiling and L at $98
against $97. Owner decisions are listed in the report's section 8:
delivery egress price, the ring, the metrics cadence, the history fan-in,
the memtable ticker, settles riding the next window, small-tenant pricing,
the retention basis against K2's uncompressed basis, and a production-shape
benchmark rig.

## Many tenants on one cell (2026-10-25)

The owner asked what happens when tenants of each profile are packed up to
the full capacity of one 1 vCPU / 1 GiB instance. Measured on the launch
posture (one shared-cell server, one shard, 8 MiB tail ring, k = 8, the
500 MB shed line; binary 409acbc8; Mac rig, s3lite at 20 ms) with
`bench/k2cost/run-local.sh` tenants mode (`PACK-T`, `PACK-S` points:
separately admitted projects, a fresh cell per step, 20-minute steady
window, priced by `packing.py` as the whole instance's month divided by N).
An independent recomputation reproduced every instance month to under a
cent; an A/A repeat of S x 20 agreed within 0.3%.

| Tenants on one cell | Instance month | Per tenant (owner) | Per tenant (A/B -50%) | K2 bill per tenant |
|---|---:|---:|---:|---:|
| T x 1 | $18.28 | $18.28 | $11.49 | $0.00012 |
| T x 100 | $38.96 | $0.390 | $0.221 | $0.00015 |
| T x 400 | $56.89 | $0.142 | $0.079 | $0.00016 |
| T x 1,000 | $77.56 | **$0.078** | **$0.043** | $0.00015 |
| S x 1 | $67.04 | $67.04 | $36.50 | $0.50 |
| S x 10 | $149.12 | $14.91 | $8.28 | $0.50 |
| S x 20 | $171.47 | $8.57 | $4.97 | $0.50 |
| S x 30 | $186.85 | **$6.23** | **$3.74** | $0.50 |
| S x 46 (VOID) | $209.70 | $4.56 | $2.88 | $0.50 |

- **T** binds at cell-admin's 1,000-project cap (the 1,200-place live pool
  next) at 0.05 cores and 202 MB. Packed, a T tenant costs $0.078, still
  ~2,500x its 20% ceiling ($0.00003): a minimum-fee question, not a cost
  one. One more tenant at density costs $0.035, 58% of it one WAL PUT per
  scattered append (a T-only cell never fills a pump window), 15%
  telemetry and usage SST reads.
- **S** at 30 per cell is clean ($6.23, ~62x its $0.10 ceiling; 11x cheaper
  than alone). At 46 the instance read-memory budget (125 MiB, a quarter of
  the shed line) refused 0.03% of reads (`503 read_memory_busy`) before the
  1,200-place pool filled (1,144 held), so the hard density is between 30
  and 46 and not yet demonstrated. Once the shard's WAL is saturated
  (N >= 10) one more S costs $1.5-1.9.
- **M and L** are not admitted on a k = 8 cell (their per-project append
  ceiling refuses ~79% and ~86% of their writes). On k = 1 cells (no
  per-project isolation) the model puts 2-2.6 M or 4-6 L per cell at
  $248-260 and $232-236 per tenant, +51-53% against K2: their cost is per
  byte (uploads, egress, Class B), so packing saves only 10-20%. Not yet
  measured on a cell.
- **A 1,000-tenant fleet** (700 T, 250 S, 40 M, 10 L; T and S from these
  measurements at the planned densities of 720 and 29 per cell, M and L
  modelled) needs 33 cells and costs $14.4k a month against $26.2k of K2
  revenue: +45% margin, +55% with Class A/B at -50%. It is a month-1 figure,
  89% of whose cost (M and L) is modelled. It moves most with storage that
  never expires (month 12: +11%), a consumer group that lags on M (+21%),
  the egress price ($0.01/GB: +36%) and layout 6 (+68%); the T/S packing
  density moves it by one point, and mixing T and S on shared cells is a
  wash (within $55 a month).
- **Unknowns.** Whether Tigris bills 304s: registry revalidations return
  304 about once per long-poll re-arm (one in three S requests); billed as
  Class B they would add 33-62% to T and S cells (a short in-process
  registry TTL would remove most of them either way). CPU and RSS are Mac
  figures (CPU is under 3% of an instance month; RSS headroom for 46 S on
  Linux is unverified); s3lite stands in for Tigris; the driver's socket
  pool saturated at peak from S x 20 on. `packing.py curve` fits through
  VOID steps; read its n_max as extrapolated.
