#!/usr/bin/env python3
"""Turn results.json into the markdown tables for docs/SOAK-REGIONS.md."""
import json, os, sys

S = os.environ.get("SOAK_HOME") or os.path.dirname(os.path.abspath(__file__))
d = json.load(open(f"{S}/results.json"))
R = d["regions"]
ORDER = ["us-east-1", "us-west-1", "eu-central-1", "eu-west-3",
         "ap-southeast-1", "ap-northeast-1"]
ORDER = [r for r in ORDER if r in R]


def f(v, nd=0):
    if v is None:
        return "—"
    return f"{v:,.{nd}f}" if isinstance(v, (int, float)) else str(v)


out = []
W = out.append

# ---------- 1. headline per region ----------
W("### Per-region summary\n")
W("| region | PoP | records | requests | errors | throttled | append p50 | append p99 | roundtrip p50 | roundtrip p99 |")
W("|---|---|---|---|---|---|---|---|---|---|")
for r in ORDER:
    e = R[r]
    tiers = e.get("tiers", [])
    tot = e.get("totals", {})
    ap50 = [t["appendP50"] for t in tiers if t["appendP50"]]
    ap99 = [t["appendP99"] for t in tiers if t["appendP99"]]
    rp50 = [t["rtP50"] for t in tiers if t["rtP50"]]
    rp99 = [t["rtP99"] for t in tiers if t["rtP99"]]
    med = lambda xs: sorted(xs)[len(xs) // 2] if xs else None
    W(f"| {r} | {e['pop']} | {f(tot.get('records'))} | {f(tot.get('ok'))} | "
      f"{f(tot.get('errs'))} | {f(tot.get('throttled'))} | "
      f"{f(med(ap50),1)} | {f(max(ap99) if ap99 else None,1)} | "
      f"{f(med(rp50),1)} | {f(max(rp99) if rp99 else None,1)} |")
W("")

# ---------- 2. tier ramp per region ----------
W("### Tier ramp\n")
for r in ORDER:
    e = R[r]
    W(f"**{r}** ({e['pop']})\n")
    W("| tier | conc | req/s | rec/s | append p50 | append p99 | roundtrip p50 | roundtrip p99 | errs | throttled |")
    W("|---|---|---|---|---|---|---|---|---|---|")
    for t in e.get("tiers", []):
        W(f"| {t['tier']} | {t['conc']} | {f(t['reqPerSec'])} | {f(t['recPerSec'])} | "
          f"{f(t['appendP50'],1)} | {f(t['appendP99'],1)} | "
          f"{f(t['rtP50'],1)} | {f(t['rtP99'],1)} | {t['errsCum']} | {t['throttledCum']} |")
    W("")

# ---------- 3. Tigris per-op ----------
W("### Tigris object-store latency, measured from inside the region\n")
W("| region | PoP | put:wal n | p50 | p90 | p99 | max | err | get:sst p50 | get:sst p99 | steal% |")
W("|---|---|---|---|---|---|---|---|---|---|---|")
for r in ORDER:
    e = R[r]
    st = e.get("store", {})
    ops = st.get("ops", {})
    w = ops.get("put:wal", {})
    g = ops.get("get:sst", {})
    W(f"| {r} | {e['pop']} | {f(w.get('n'))} | {f(w.get('p50_ms'))} | {f(w.get('p90_ms'))} | "
      f"{f(w.get('p99_ms'))} | {f(w.get('max_ms'))} | {f(w.get('err'))} | "
      f"{f(g.get('p50_ms'))} | {f(g.get('p99_ms'))} | {st.get('steal_pct')} |")
W("")

W("### All object-store operations\n")
allops = sorted({k for r in ORDER for k in R[r].get("store", {}).get("ops", {})})
W("| op | " + " | ".join(f"{r} p50/p99" for r in ORDER) + " |")
W("|---|" + "---|" * len(ORDER))
for op in allops:
    cells = []
    for r in ORDER:
        o = R[r].get("store", {}).get("ops", {}).get(op)
        cells.append(f"{o['p50_ms']}/{o['p99_ms']}" if o else "—")
    W(f"| `{op}` | " + " | ".join(cells) + " |")
W("")

# ---------- 4. integrity ----------
W("### Integrity: client-accepted vs server-durable\n")
W("| region | client records (ok x batch) | server records | delta | shards |")
W("|---|---|---|---|---|")
for r in ORDER:
    e = R[r]
    cli = e.get("totals", {}).get("records")
    us = e.get("usage", {})
    streams = us.get("streams", []) if isinstance(us, dict) else []
    srv = sum(s.get("records", 0) for s in streams)
    delta = (srv - cli) if (cli is not None) else None
    W(f"| {r} | {f(cli)} | {f(srv)} | {f(delta)} | {len(streams)} |")
W("")

print("\n".join(out))
