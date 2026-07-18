#!/usr/bin/env python3
"""Fleet staircase (1→4 servers) — run 4 (before) vs run 5 (after).

Small multiples: rows = measures (one hue per measure, never dual-axis),
columns = before/after with shared row y-limits so the columns compare.
Palette: blue #2a78d6 throughput, aqua #1baf7a desired instances, violet
#4a3aa7 latency (validated set). Level boundaries (generator concurrency
doublings) are faint vertical rules labeled with the offered concurrency.
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

def load(path):
    rows = []
    for line in open(path):
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        g, lb = d.get("gen"), d.get("lb")
        if not isinstance(g, dict) or not isinstance(lb, dict):
            continue
        rows.append({
            "t": g.get("elapsedMin", 0.0),
            "rps": g.get("achievedPerSec", 0),
            "conc": g.get("concurrency", 0),
            "p50": g.get("winP50Ms", g.get("p50Ms", 0)),
            "desired": lb.get("desired", 0),
            "live": sum(1 for h in lb.get("hb", []) if h.get("live")),
        })
    rows.sort(key=lambda r: r["t"])
    return rows

before = load(sys.argv[1])
after = load(sys.argv[2])
tmax = max(max((r["t"] for r in before), default=1), max((r["t"] for r in after), default=1))

fig, axes = plt.subplots(3, 2, figsize=(11.5, 8.6), sharex=True)
fig.suptitle("Fleet staircase, 1 → 4 servers — before (run 4) vs after (run 5, tuned engine)",
             fontsize=13.5, fontweight="bold", color=INK, x=0.06, ha="left", y=0.985)

cols = [("BEFORE — run 4 (pre-fix engine, 25 ms flush)", before),
        ("AFTER — run 5 (per-key cap + 1 s manifest poll + pacing, 50 ms flush)", after)]
rows_spec = [
    ("rps", "Achieved requests / s", BLUE, False),
    ("desired", "Desired instances", AQUA, False),
    ("p50", "Window p50 latency (ms)", VIOLET, True),
]

for ci, (title, data) in enumerate(cols):
    ts = [r["t"] for r in data]
    # level boundaries where offered concurrency changes
    bounds = [(data[i]["t"], data[i]["conc"]) for i in range(len(data))
              if i == 0 or data[i]["conc"] != data[i - 1]["conc"]]
    for ri, (key, label, color, logy) in enumerate(rows_spec):
        ax = axes[ri][ci]
        for bt, conc in bounds:
            ax.axvline(bt, color=GRID, linewidth=0.9, zorder=0)
            if ri == 0:
                ax.annotate(f"c={conc}", (bt, 1.0), xycoords=("data", "axes fraction"),
                            xytext=(2, -10), textcoords="offset points",
                            fontsize=8, color=MUT)
        ys = [r[key] for r in data]
        if key == "desired":
            ax.step(ts, ys, where="post", color=color, linewidth=2.2, zorder=3)
            live = [r["live"] for r in data]
            ax.step(ts, live, where="post", color=color, linewidth=1.2,
                    linestyle=":", zorder=2)
            if ci == 1:
                ax.annotate("desired", (ts[len(ts)//2], ys[len(ys)//2]),
                            xytext=(0, 8), textcoords="offset points",
                            fontsize=8.5, color=INK, fontweight="bold")
                ax.annotate("live (heartbeats)", (ts[-1], live[-1]),
                            xytext=(-6, -12), textcoords="offset points",
                            ha="right", fontsize=8.5, color=MUT)
            ax.set_ylim(-0.2, 4.6)
            ax.set_yticks([0, 1, 2, 3, 4])
        else:
            ax.plot(ts, ys, color=color, linewidth=2, zorder=3)
            if logy:
                ax.set_yscale("log")
        ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
        if ci == 0:
            ax.set_ylabel(label, color=INK)
        if ri == 0:
            ax.set_title(title, fontsize=10, color=INK, loc="left")
        ax.set_xlim(0, tmax * 1.02)

# shared y-limits per row for honest column comparison
for ri in range(3):
    lo = min(axes[ri][0].get_ylim()[0], axes[ri][1].get_ylim()[0])
    hi = max(axes[ri][0].get_ylim()[1], axes[ri][1].get_ylim()[1])
    for ci in range(2):
        axes[ri][ci].set_ylim(lo, hi)

for ci in range(2):
    axes[2][ci].set_xlabel("Run time (minutes)", color=INK)

fig.text(0.06, 0.005,
         "Same harness both runs: 32 streams via LB, closed-loop generator doubling concurrency every 5 min (c=8...256), 4 scale-from-zero\n"
         "1-CPU/1-GB servers on Prisma Compute (Singapore) + Tigris; desired count published by the fleet itself from rps + ack-p50 heartbeats.",
         fontsize=8.5, color=MUT, va="bottom")
fig.tight_layout(rect=(0, 0.035, 1, 0.96))
out = __file__.rsplit("/", 1)[0] + "/chart-staircase-before-after.png"
fig.savefig(out, dpi=160)
print("wrote", out)
