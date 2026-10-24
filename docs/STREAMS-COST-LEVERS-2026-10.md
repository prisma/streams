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
