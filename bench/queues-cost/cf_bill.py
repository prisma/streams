#!/usr/bin/env python3
"""Cloudflare Queues bill for a scenario, the cost ceiling a margin allows,
and (with --measured) the verdict for a measured Prisma Streams cost.

Pricing (developers.cloudflare.com/queues/platform/pricing, updated
2026-04-21): $0.40 per million operations on Workers Paid, 1,000,000
operations a month included; an operation per 64 KB (1,000 B KB, ~100 B of
internal metadata per message) written, read or deleted; each retry is a
read; a message moved to a dead-letter queue is a write there, and one that
expires unread costs a write and a delete. No egress charge.

Usage:
  python3 bench/queues-cost/cf_bill.py [--scenario PATH] [--scale N]
      [--margin 0.80] [--measured costs.json]

costs.json (the benchmark's output, §6 of docs/QUEUES-PRICING-BENCHMARK.md):
  {"month_usd": {"fully_loaded": float, "marginal": float,
                 "standalone": float}}
"""
import argparse
import json
import math
import os

PRICE_PER_OP = 0.40 / 1_000_000
INCLUDED_OPS = 1_000_000
CHUNK = 64_000
META = 100


def chunks(avg_bytes):
    return max(1, math.ceil((avg_bytes + META) / CHUNK))


def month(scenario, scale):
    f = scenario["month_days"] / scenario["measured_days"] * scale
    known_reads = sum(q.get("reads_7d", 0) for q in scenario["queues"])
    known_writes = sum(q["writes_7d"] for q in scenario["queues"] if "reads_7d" in q)
    rest_ratio = (scenario["week_totals"]["reads"] - known_reads) / (
        scenario["week_totals"]["writes"] - known_writes
    )
    rows = []
    for q in scenario["queues"]:
        writes = q["writes_7d"] * f
        reads = q.get("reads_7d", q["writes_7d"] * rest_ratio) * f
        dlq = q["dlq_writes_7d"] * f
        # Consumed messages are deleted once; dead-lettered ones are not
        # deleted from the primary (conservative) and expire unread in the
        # dead-letter queue: a write there and a delete at expiry.
        deletes = writes - dlq
        c = chunks(q["avg_bytes"])
        ops = c * (writes + reads + deletes + 2 * dlq)
        rows.append((q["name"], writes, reads, deletes, dlq, ops))
    return rows


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", default=os.path.join(here, "scenario.json"))
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--margin", type=float, default=0.80)
    ap.add_argument("--measured")
    a = ap.parse_args()
    with open(a.scenario) as fh:
        scenario = json.load(fh)
    rows = month(scenario, a.scale)
    print(f"{'queue':36} {'writes':>12} {'reads':>12} {'ops':>12} {'usd':>8}")
    for name, w, r, _d, _x, ops in rows:
        print(f"{name:36} {w:12,.0f} {r:12,.0f} {ops:12,.0f} {ops * PRICE_PER_OP:8.2f}")
    writes = sum(r[1] for r in rows)
    ops = sum(r[5] for r in rows)
    bill = max(0.0, ops - INCLUDED_OPS) * PRICE_PER_OP
    list_bill = ops * PRICE_PER_OP
    ceiling = bill * (1 - a.margin)
    print(f"\nmonth ({scenario['month_days']} days, scale {a.scale:g}): "
          f"{writes:,.0f} messages, {ops:,.0f} operations, "
          f"{ops / writes:.3f} ops per message")
    print(f"cloudflare bill: ${bill:,.2f} (${list_bill:,.2f} before the included million)")
    print(f"price per million messages: ${bill / writes * 1e6:.3f}")
    print(f"cost ceiling at {a.margin:.0%} margin: ${ceiling:,.2f} a month, "
          f"${ceiling / writes * 1e6:.3f} per million messages")
    if a.measured:
        with open(a.measured) as fh:
            m = json.load(fh)["month_usd"]
        verdict = "MET" if m["fully_loaded"] <= ceiling else "NOT MET"
        print(f"measured fully loaded ${m['fully_loaded']:,.2f} "
              f"(marginal ${m['marginal']:,.2f}, standalone ${m['standalone']:,.2f}); "
              f"margin {1 - m['fully_loaded'] / bill:.1%}: goal {verdict}")


if __name__ == "__main__":
    main()
