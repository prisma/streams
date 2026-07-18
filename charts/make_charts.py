#!/usr/bin/env python3
"""Two charts for the single-stream ceiling sweep (EXPERIMENT-PILOT/REPORT),
re-created after the flusher-gate fixes with the pre-fix series kept as a
muted reference.

Small-multiples per chart (shared log-x, one measure per panel; never a
dual axis). Colors follow the measure across both charts:
events/s = blue, requests/s = aqua, MB/s = violet (palette validated with
the dataviz six-checks script; identity of before/after is carried by a
legend + muted-vs-full ink, never hue alone).
Regime encoding on the after series (shape+fill): clean = solid filled
marker; burst = open marker (dashed connector); collapsed = x at 0.
"""
import json
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

BLUE, AQUA, VIOLET = "#2a78d6", "#1baf7a", "#4a3aa7"
INK, MUT, GRID, SURF = "#0b0b0b", "#52514e", "#e8e7e3", "#fcfcfb"

HERE = __file__.rsplit("/", 1)[0]
BEFORE = json.load(open(HERE + "/sweep-data.json"))
AFTER = json.load(open(HERE + "/sweep-data2.json"))

plt.rcParams.update({
    "font.family": "sans-serif", "font.size": 11,
    "axes.edgecolor": GRID, "axes.linewidth": 1,
    "axes.titlesize": 11, "axes.titlecolor": INK,
    "xtick.color": MUT, "ytick.color": MUT,
    "figure.facecolor": SURF, "axes.facecolor": SURF,
})

def human(v):
    if v >= 1_000_000: return f"{v/1e6:.3g}M"
    if v >= 1_000: return f"{v/1e3:.3g}k"
    if v >= 10: return f"{v:.0f}"
    return f"{v:.2g}"

def bytes_label(b):
    if b >= 1 << 20: return f"{b >> 20}MB"
    if b >= 1 << 10: return f"{b >> 10}KB"
    return f"{b}B"

FLOOR = 0.004

def panelize(fig_title, xlabel, xkey, before_rows, after_rows, out, footnote):
    measures = [
        ("events_per_s", "Events / s", BLUE),
        ("requests_per_s", "Requests / s", AQUA),
        ("mb_per_s", "Payload MB / s", VIOLET),
    ]
    xs = [r[xkey] for r in after_rows]
    xticklabels = [bytes_label(x) if xkey == "event_bytes" else str(x) for x in xs]
    bmap = {r[xkey]: r for r in before_rows}
    fig, axes = plt.subplots(3, 1, figsize=(8.8, 9.2), sharex=True)
    fig.suptitle(fig_title, fontsize=14, fontweight="bold", color=INK, x=0.06,
                 ha="left", y=0.985)
    for ax, (key, label, color) in zip(axes, measures):
        # BEFORE (pre-fix) — muted reference series
        bx = [x for x in xs if x in bmap]
        bys = [max(bmap[x][key] or FLOOR, FLOOR) for x in bx]
        ax.plot(bx, bys, "--", color=MUT, linewidth=1.3, zorder=1, alpha=0.8)
        ax.plot(bx, bys, "o", ms=4.5, color=MUT, zorder=1, alpha=0.8)
        # AFTER — full series with regime markers
        ys = [max(r[key], FLOOR) if r[key] is not None else FLOOR for r in after_rows]
        regimes = [r["regime"] for r in after_rows]
        for i in range(len(xs) - 1):
            style = "-" if regimes[i] == "clean" and regimes[i + 1] == "clean" else "--"
            ax.plot(xs[i:i + 2], ys[i:i + 2], style, color=color, linewidth=2, zorder=2)
        for x, y, rg, r in zip(xs, ys, regimes, after_rows):
            if rg == "clean":
                ax.plot(x, y, "o", ms=9, color=color, zorder=3)
            elif rg == "burst":
                ax.plot(x, y, "o", ms=9, mfc=SURF, mec=color, mew=2, zorder=3)
            else:
                ax.plot(x, y, "x", ms=10, color=MUT, mew=2.5, zorder=3)
            txt = "stall" if rg == "collapsed" else human(r[key])
            ax.annotate(txt, (x, y), textcoords="offset points", xytext=(0, 9),
                        ha="center", fontsize=9, color=INK, fontweight="bold")
        ax.set_yscale("log")
        ax.set_ylabel(label, color=INK)
        ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
        ax.margins(y=0.32)
    axes[-1].set_xscale("log", base=2)
    axes[-1].set_xticks(xs)
    axes[-1].set_xticklabels(xticklabels)
    axes[-1].set_xlabel(xlabel, color=INK)
    legend = [
        Line2D([], [], marker="o", ls="-", ms=9, color=INK,
               label="after fixes — sustained (0 errors)"),
        Line2D([], [], marker="o", ls="--", ms=9, mfc=SURF, mec=INK, mew=2,
               label="after fixes — errors during window"),
        Line2D([], [], marker="x", ls="", ms=9, color=MUT, mew=2.5,
               label="write stall"),
        Line2D([], [], marker="o", ls="--", ms=4.5, color=MUT,
               label="before fixes (2026-07-14 morning)"),
    ]
    axes[0].legend(handles=legend, loc="lower left", frameon=False, fontsize=8.5)
    fig.text(0.06, 0.005, footnote, fontsize=8.5, color=MUT, va="bottom")
    fig.tight_layout(rect=(0, 0.045, 1, 0.97))
    fig.savefig(out, dpi=160)
    print("wrote", out)

panelize(
    "Single ordered stream — throughput vs EVENT SIZE (batch = 1)",
    "Event size (log scale)",
    "event_bytes",
    BEFORE["size_sweep"]["points"],
    AFTER["size_sweep"]["points"],
    HERE + "/chart-size-sweep.png",
    "After series: engine post flusher-gate fixes (l0_max_ssts_per_key, 1 s manifest poll, committer pacing), 1-core server,\n"
    "25 ms-latency object store, 40 s closed-loop windows, absorber+trim active. Before series: same protocol pre-fix\n"
    "(1-CPU cloud pilot + Tigris) — its byte ceiling was the flusher gate, not compaction. Log y; labels are after-values.",
)

panelize(
    "Single ordered stream — throughput vs BATCH SIZE (256 B events)",
    "Events per request (log scale)",
    "batch",
    BEFORE["batch_sweep"]["points"],
    AFTER["batch_sweep"]["points"],
    HERE + "/chart-batch-sweep.png",
    "After series: engine post flusher-gate fixes, 1-core server, 25 ms-latency object store, 40 s closed-loop windows,\n"
    "absorber+trim active. Before series: same protocol pre-fix (1-CPU cloud pilot + Tigris). Log y; labels are after-values.",
)
