#!/usr/bin/env python3
"""Round-12 field-cert evaluator: exact reconciliation + charter gates.

Usage: evaluate-cert.py <results-dir> <stage> [--lag-gate-ms 250] [--rss-gate-mb 450]

Joins every generator's cert_done (stage-<stage>-gen*.json) with the
server timeline (rss-timeline-<stage>.jsonl) and prints a PASS/FAIL
verdict per the round-12 charter:

  - EXACT delivery reconciliation: a hole in acked territory is loss;
    holes are excusable ONLY at unacked/shed q's (maybe-not-committed);
    dups == 0, wrong-stream == 0, every sub's tail reaches the writer's
    final acked q, and every parked sub on a written stream received
    data (an empty sub on a written stream is 100% loss, not idleness).
  - shed <= 0.1% of offered, append errors <= 0.1% (typed classes shown;
    the exclusion rule is written HERE, before the run: only errors the
    generator itself classifies as stop-window cancellation leave the
    denominator — currently none do).
  - delivery p99 <= gate on the median of steady windows (first window
    of the release discarded: it straddles the ramp).
  - server: peak rss <= gate, admission sheds 0 for the whole stage;
    lag_disconnects / uncached are REPORTED (typed-cutoff pressure).
"""
import glob
import json
import os
import statistics
import sys


def expand(ranges):
    return set(q for a, b in ranges for q in range(a, b + 1))


def main():
    outdir, stage = sys.argv[1], sys.argv[2]
    lag_gate = float(sys.argv[sys.argv.index("--lag-gate-ms") + 1]) if "--lag-gate-ms" in sys.argv else 250.0
    rss_gate = float(sys.argv[sys.argv.index("--rss-gate-mb") + 1]) if "--rss-gate-mb" in sys.argv else 450.0
    fails, notes = [], []

    gens = {}
    for p in sorted(glob.glob(os.path.join(outdir, f"stage-{stage}-gen*.json"))):
        k = int(p.rsplit("gen", 1)[1].split(".")[0])
        gens[k] = json.load(open(p))
    if not gens:
        print(f"FAIL: no stage-{stage}-gen*.json in {outdir}")
        sys.exit(1)

    dones, certs = {}, {}
    for k, lines in gens.items():
        dn = [l for l in lines if l.get("phase") == "cert_done"]
        if not dn:
            fails.append(f"gen{k}: no cert_done")
            continue
        dones[k] = dn[-1]
        certs[k] = [l for l in lines if l.get("phase") == "cert"]

    writer_k = [k for k, d in dones.items() if d["recon"]["writerStreams"]]
    if len(writer_k) != 1:
        fails.append(f"expected exactly one writer gen, found {writer_k}")
        writer = {}
    else:
        writer = dones[writer_k[0]]["recon"]["writerStreams"]

    # ---- exact reconciliation across every gen ----------------------
    real_loss = dead_subs = tail_short = 0
    dups = sum(d["recon"]["dupsTotal"] for d in dones.values())
    wrong = sum(d["recon"]["wrongStreamTotal"] for d in dones.values())
    received = sum(d["recon"]["receivedTotal"] for d in dones.values())
    reconnects = sum(d["reconnects"] for d in dones.values())
    subs_total = sum(d["subsN"] for d in dones.values())
    live_freeze = sum(d["recon"]["subsLiveAtFreeze"] for d in dones.values())
    unacked_total = sum(v["unackedCount"] for v in writer.values())
    for k, d in dones.items():
        r = d["recon"]
        n, off, st_n = d["subsN"], d.get("subOffset", 0), d["subTenants"]
        parked_per_stream = {}
        for j in range(n):
            t = (j + off) % max(st_n, 1)
            parked_per_stream[t] = parked_per_stream.get(t, 0) + 1
        for t_s, wa in writer.items():
            t = int(t_s)
            want = parked_per_stream.get(t, 0)
            if want == 0 or wa["ackedCount"] == 0:
                continue
            sv = r["subStreams"].get(t_s)
            if sv is None or sv["subs"] < want:
                dead_subs += want - (sv["subs"] if sv else 0)
                continue
            acked = expand(wa["acked"])
            if sv["holeCount"] > 0:
                holes = expand(sv["holes"])
                distinct = sv.get("holeQsDistinct", sv["holeCount"])
                if len(holes) < distinct:
                    notes.append(f"gen{k} stream {t}: hole union truncated ({distinct}), gating conservatively")
                    real_loss += distinct - len(holes - acked)
                else:
                    real_loss += len(holes & acked)
            final_q = wa["acked"][-1][1]
            if sv["minLast"] < final_q and not any(a <= final_q <= b for a, b in wa["unacked"] + wa["shed"]):
                # tail records past minLast must all be excusable
                tail_missing = acked & set(range(sv["minLast"] + 1, final_q + 1))
                if tail_missing:
                    tail_short += len(tail_missing)
    if real_loss:
        fails.append(f"REAL LOSS: {real_loss} acked records never delivered")
    if tail_short:
        fails.append(f"TAIL SHORT: {tail_short} acked records missing at freeze (settle too short or loss)")
    if dead_subs:
        fails.append(f"DEAD SUBS: {dead_subs} parked subs on written streams received nothing")
    if dups:
        fails.append(f"DUPLICATES: {dups}")
    if wrong:
        fails.append(f"WRONG-STREAM: {wrong}")
    if live_freeze < subs_total:
        fails.append(f"LIVE AT FREEZE: {live_freeze}/{subs_total}")

    # ---- write-side gates (writer gen) ------------------------------
    ap_p99 = lag_p99 = shed_pct = err_pct = None
    if writer_k:
        d = dones[writer_k[0]]
        offered = d["offered"]
        shed_pct = 100.0 * d["apThr"] / max(offered, 1)
        err_pct = 100.0 * d["apErr"] / max(offered, 1)
        if shed_pct > 0.1:
            fails.append(f"SHED {shed_pct:.3f}% > 0.1%")
        if err_pct > 0.1:
            fails.append(
                f"APPEND ERRORS {err_pct:.3f}% > 0.1% (connect {d['errConnect']}, "
                f"timeout {d['errTimeout']}, status {d['errStatus']}, other {d['errOther']})"
            )
        steady = certs[writer_k[0]][1:]  # discard the ramp-straddling first window
        ap = [w["apWinP99Ms"] for w in steady if w.get("apWinP99Ms")]
        ap_p99 = statistics.median(ap) if ap else None
    # delivery lag: worst gen's median steady-window p99 (readers sit in
    # different regions; report per-gen, gate on the worst)
    lag_by_gen = {}
    for k, ws in certs.items():
        vals = [w["lagWinP99Ms"] for w in ws[1:] if w.get("lagWinP99Ms")]
        if vals:
            lag_by_gen[k] = statistics.median(vals)
    if lag_by_gen:
        lag_p99 = max(lag_by_gen.values())
        if lag_p99 > lag_gate:
            fails.append(f"DELIVERY P99 {lag_p99:.0f}ms > {lag_gate:.0f}ms (per-gen {lag_by_gen})")

    # ---- server timeline --------------------------------------------
    peak_rss = sheds = lagcuts = uncached = conns_peak = None
    tl_path = os.path.join(outdir, f"rss-timeline-{stage}.jsonl")
    if os.path.exists(tl_path):
        tl = []
        for line in open(tl_path):
            try:
                s = json.loads(line)
                if s.get("load"):
                    tl.append(s["load"])
            except Exception:
                pass
        if tl:
            peak_rss = max(s.get("rss_mb", 0) for s in tl)
            conns_peak = max(s.get("sse_connections", 0) for s in tl)
            last = tl[-1]
            sheds = last.get("admit_shed", 0) - tl[0].get("admit_shed", 0)
            lf_last, lf_first = last.get("sse_livefeed", {}), tl[0].get("sse_livefeed", {})
            lagcuts = lf_last.get("lag_disconnects", 0) - lf_first.get("lag_disconnects", 0)
            uncached = (lf_last.get("uncached_publish", 0) + lf_last.get("project_cap_uncached", 0)
                        - lf_first.get("uncached_publish", 0) - lf_first.get("project_cap_uncached", 0))
            if peak_rss > rss_gate:
                fails.append(f"PEAK RSS {peak_rss:.0f}MB > {rss_gate:.0f}MB")
            if sheds:
                fails.append(f"ADMISSION SHEDS {sheds}")
    else:
        notes.append("no rss timeline — server gates not evaluated")

    verdict = "PASS" if not fails else "FAIL"
    summary = {
        "stage": stage, "verdict": verdict, "fails": fails, "notes": notes,
        "subs": subs_total, "received": received, "realLoss": real_loss,
        "dups": dups, "wrongStream": wrong, "deadSubs": dead_subs,
        "tailShort": tail_short, "reconnects": reconnects,
        "unackedAppends": unacked_total, "shedPct": shed_pct, "errPct": err_pct,
        "apWinP99Ms": ap_p99, "lagWinP99Ms": lag_by_gen or None,
        "peakRssMb": peak_rss, "ssePeakConns": conns_peak,
        "admitSheds": sheds, "lagDisconnects": lagcuts, "uncachedPublish": uncached,
    }
    json.dump(summary, open(os.path.join(outdir, f"eval-{stage}.json"), "w"), indent=1)
    print(f"== {stage}: {verdict}")
    print(f"   subs {subs_total} received {received} loss {real_loss} dups {dups} "
          f"deadSubs {dead_subs} tailShort {tail_short} reconnects {reconnects} unacked {unacked_total}")
    print(f"   shed {shed_pct if shed_pct is None else round(shed_pct, 4)}% err "
          f"{err_pct if err_pct is None else round(err_pct, 4)}% apP99 {ap_p99} lagP99 {lag_by_gen}")
    print(f"   server: peakRSS {peak_rss} conns {conns_peak} admitSheds {sheds} "
          f"lagCuts {lagcuts} uncached {uncached}")
    for f in fails:
        print(f"   FAIL: {f}")
    for n in notes:
        print(f"   note: {n}")
    sys.exit(0 if verdict == "PASS" else 1)


if __name__ == "__main__":
    main()
