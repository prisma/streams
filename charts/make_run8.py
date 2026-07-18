#!/usr/bin/env python3
"""Run 8 (multi-generator stress + chaos + soak) — three-panel timeline.
One hue per measure (no dual axes): blue = fleet delivered req/s,
aqua = live/desired instances, violet = worst client p50 (log).
Event annotations: chaos kill, zombie, recovery, soak start.
"""
import json, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BLUE, AQUA, VIOLET = "#2a78d6", "#1baf7a", "#4a3aa7"
INK, MUT, GRID, SURF = "#0b0b0b", "#52514e", "#e8e7e3", "#fcfcfb"
plt.rcParams.update({
    "font.family": "sans-serif", "font.size": 10.5,
    "axes.edgecolor": GRID, "axes.linewidth": 1,
    "xtick.color": MUT, "ytick.color": MUT,
    "figure.facecolor": SURF, "axes.facecolor": SURF,
})

def hms_to_min(t, t0):
    h, m, s = [int(x) for x in t.split(":")]
    v = h * 60 + m + s / 60
    if v < t0 - 300:  # crossed midnight
        v += 24 * 60
    return v - t0

rows = []
t0 = None
for line in open(sys.argv[1]):
    line = line.strip()
    if not line.startswith("{"):
        continue
    try:
        d = json.loads(line)
    except json.JSONDecodeError:
        continue
    lb = d.get("lb")
    if not isinstance(lb, dict):
        continue
    gs = [g for g in (d.get("g") or []) if isinstance(g, dict) and "achievedPerSec" in g]
    tot = sum(g["achievedPerSec"] for g in gs)
    p50s = [g.get("winP50Ms", 0) for g in gs if g.get("winP50Ms", 0) > 0]
    conc = max((g.get("concurrency", 0) for g in gs), default=0)
    hb = lb.get("hb") or []
    if t0 is None:
        h, m, s2 = [int(x) for x in d["t"].split(":")]
        t0 = h * 60 + m + s2 / 60
    rows.append({
        "t": hms_to_min(d["t"], t0),
        "tot": tot,
        "p50": max(p50s) if p50s else None,
        "live": sum(1 for h2 in hb if h2.get("live")),
        "desired": lb.get("desired", 0),
        "conc": conc,
    })

events = json.loads(open(sys.argv[2]).read()) if len(sys.argv) > 2 else []

fig, axes = plt.subplots(3, 1, figsize=(12.5, 8.2), sharex=True)
fig.suptitle("Run 8 — 4 generators × 4 LBs × 4 servers: staircase, overload, chaos kill, soak",
             fontsize=13, fontweight="bold", color=INK, x=0.06, ha="left", y=0.985)
ts = [r["t"] for r in rows]

ax = axes[0]
ax.plot(ts, [r["tot"] for r in rows], color=BLUE, linewidth=1.8)
ax.set_ylabel("Fleet delivered req/s", color=INK)
bounds = [(rows[i]["t"], rows[i]["conc"]) for i in range(len(rows))
          if rows[i]["conc"] and (i == 0 or rows[i]["conc"] != rows[i-1]["conc"])]
for bt, c in bounds:
    ax.axvline(bt, color=GRID, linewidth=0.8, zorder=0)
    ax.annotate(f"c={c}", (bt, 1.0), xycoords=("data", "axes fraction"),
                xytext=(2, -10), textcoords="offset points", fontsize=7.5, color=MUT)

ax = axes[1]
ax.step(ts, [r["desired"] for r in rows], where="post", color=AQUA, linewidth=2.2, label="desired")
ax.step(ts, [r["live"] for r in rows], where="post", color=AQUA, linewidth=1.2, linestyle=":", label="live")
ax.set_ylim(-0.2, 4.6); ax.set_yticks([0, 1, 2, 3, 4])
ax.set_ylabel("Instances", color=INK)
ax.legend(loc="lower right", frameon=False, fontsize=9)

ax = axes[2]
tp = [(r["t"], r["p50"]) for r in rows if r["p50"]]
ax.plot([x for x, _ in tp], [y for _, y in tp], color=VIOLET, linewidth=1.8)
ax.set_yscale("log")
ax.set_ylabel("Worst client p50 (ms)", color=INK)
ax.set_xlabel("Run time (minutes)", color=INK)

for evt, em in events:
    for ax in axes:
        ax.axvline(em, color=INK, linewidth=1.0, linestyle="--", alpha=0.55, zorder=1)
    axes[0].annotate(evt, (em, 0.98), xycoords=("data", "axes fraction"),
                     xytext=(4, -22), textcoords="offset points", fontsize=8,
                     color=INK, fontweight="bold", rotation=90, va="top")

for ax in axes:
    ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)

fig.text(0.06, 0.005,
         "32 streams/generator, batch=16 appends (~230 B records), closed loop. Multi-signal scaling: CPU-75%, delivery envelope,\n"
         "server-ack and router-observed client latency (scale-in blocked while edge-hot). Prisma Compute (Singapore) + Tigris.",
         fontsize=8.5, color=MUT, va="bottom")
fig.tight_layout(rect=(0, 0.035, 1, 0.96))
out = __file__.rsplit("/", 1)[0] + "/chart-run8-stress.png"
fig.savefig(out, dpi=160)
print("wrote", out)
