#!/usr/bin/env python3
"""L6 spike M1: arm identity against run-to-run spread (review A6).

  python3 identity_spread.py --a RUN... --b RUN... [--tol 0.05] --out spread.md [--json spread.json]

A = unmodified 50fab9d9 runs, B = instrumented runs of the same point. Per
billed Class A/B cell over LT (load + tail, audits excluded, raw counts), the
global PUT bytes over LT, the mean RSS over the second half of the load and
the server CPU over LT: every run's value, each build's min..max, the ratio of
the build means, the largest same-build spread (max/min - 1), and whether the
two builds' ranges overlap. A cell whose build-mean ratio is within +-tol
passes outright; one outside it is attributed to run-to-run spread only when
the same-build spread is at least as large as the cross-build difference.
Python 3.9, standard library; imports census.py and compare.py beside it.
"""
import argparse, json, math, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import census as C  # noqa: E402
import compare as K  # noqa: E402


def per_run(d):
    run, by_cell, tot, put, lt = K.lt_billed_cells(d)
    cs = C.census_run(d)
    vals = {}
    for cell, v in by_cell.items():
        for cls in ("A", "B"):
            if v.get(cls):
                vals["%s %s" % (cell, cls)] = v[cls]
    vals["TOTAL A"] = tot["A"]
    vals["TOTAL B"] = tot["B"]
    vals["PUT bytes (LT)"] = put
    vals["RSS MB, mean 2nd half of load"] = cs["rss_mb"]["mean_second_half_load"]
    vals["server CPU s (LT)"] = cs["cpu"]["server_cpu_s_LT"]
    vals["stored bytes S"] = cs["load"]["stored_bytes_S"]
    return os.path.basename(d.rstrip("/")), vals, cs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", nargs="+", required=True)
    ap.add_argument("--b", nargs="+", required=True)
    ap.add_argument("--tol", type=float, default=0.05)
    ap.add_argument("--out", required=True)
    ap.add_argument("--json")
    a = ap.parse_args()
    A = [per_run(d) for d in a.a]
    B = [per_run(d) for d in a.b]
    keys = sorted(set().union(*[set(v) for _, v, _ in A + B]))
    rows, doc = [], {"a": [n for n, _, _ in A], "b": [n for n, _, _ in B], "tol": a.tol, "rows": []}
    for k in keys:
        xa = [v.get(k, 0) or 0 for _, v, _ in A]
        xb = [v.get(k, 0) or 0 for _, v, _ in B]
        ma, mb = sum(xa) / len(xa), sum(xb) / len(xb)
        ratio = mb / ma if ma else math.inf
        def spread(xs):
            return (max(xs) / min(xs) - 1) if min(xs) > 0 else math.inf
        same = max(spread(xa) if len(xa) > 1 else 0, spread(xb) if len(xb) > 1 else 0)
        overlap = not (max(xa) < min(xb) or max(xb) < min(xa))
        is_cell = k.endswith(" A") or k.endswith(" B")
        small = is_cell and not k.startswith("TOTAL") and min(xa + xb) < 100
        if abs(ratio - 1) <= a.tol:
            verdict = "pass"
        elif small:
            verdict = "Poisson-limited (< 100 requests)"
        elif overlap and same >= abs(ratio - 1):
            verdict = "outside tol; within same-build spread"
        else:
            verdict = "FAIL"
        r = {"key": k, "a": xa, "b": xb, "mean_ratio": ratio if math.isfinite(ratio) else None,
             "same_build_spread": same if math.isfinite(same) else None, "ranges_overlap": overlap, "verdict": verdict}
        doc["rows"].append(r)
        fmtv = lambda xs: ", ".join(("%.0f" % x) if abs(x) >= 100 else ("%.1f" % x) for x in xs)  # noqa: E731
        rows.append("| %s | %s | %s | %s | %s | %s | %s |" % (
            k, fmtv(xa), fmtv(xb), ("%.3f" % ratio) if math.isfinite(ratio) else "-",
            ("%.1f%%" % (100 * same)) if math.isfinite(same) else "-", "yes" if overlap else "no", verdict))
    hdr = ["# M1 arm identity against run-to-run spread (identity_spread.py)", "",
           "A (unmodified 50fab9d9): " + ", ".join("`%s`" % n for n, _, _ in A),
           "", "B (instrumented ae126ad7): " + ", ".join("`%s`" % n for n, _, _ in B), "",
           "Cells: billed Class A/B over LT (load + tail, audits excluded, raw counts). Tolerance +-%d%% on the ratio of the build means." % round(a.tol * 100), "",
           "| cell | A runs | B runs | mean B / mean A | largest same-build spread | ranges overlap | verdict |",
           "|---|---|---|---|---|---|---|"]
    with open(a.out, "w", encoding="utf-8") as f:
        f.write("\n".join(hdr + rows) + "\n")
    if a.json:
        with open(a.json, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=1, sort_keys=True)
    print("\n".join(hdr + rows))


if __name__ == "__main__":
    main()
