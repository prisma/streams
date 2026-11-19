"""The --counts 10000 leg's report (GATE-DECISION-shapec-sequencing-2026-11-18 §2, read as
GATE-DECISION-meas10k-rig-limits-2026-11-19 rules), from a ramp results dir. Reporting only.

usage: python3 bench/ramp/split_report.py <results dir>

Reads server-*.log (the servers' INFO lines of topology, fleet and open_index), seconds-*.json (the
bench's per completion second counts; the leg runs with RAMP_KEEP_SECONDS=1 so held windows keep
theirs too), steps.jsonl, and the watch/settle samples. Times from the server logs are to the ms;
times from the seconds files are to the 1 s grain (a second is the Unix second its touches completed
in). The bench counts are the whole cell's: they cannot tell one child's touches from the other's, so
a child's own first served touch is read from the server log where it exists (its open's end) and
the bench's first served second after the park is the earlier child's.

Timing model, read from code at b2e549910:
- a split (topology/split.rs): park = paused_at; release = park + pause_ms (the CAS, then the
  barrier retires); both children open concurrently from the release, `open <p>` each; then
  `drain`; the `the split published, by phase` line is logged after the drain. So
  park = line - (pause_ms + max(open) + drain), child open end = release + open <p>.
- a planned move (fleet/handoff.rs): `handoff` is logged at the hold's end; release_ms is from the
  giver's park to it, so park = line - release_ms; park_ms is the time to the park.
"""
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

ANSI = re.compile(r"\x1b\[[0-9;]*m")
LINE = re.compile(r"^(\S+Z)\s+(\w+)\s+(\S+?):\s(.*)$")
FIELD = re.compile(r'(\w+)=("(?:[^"\\]|\\.)*"|\[[^\]]*\]|\S+)')
PHASE = re.compile(r"((?:open \S+)|\S+)=(-?\d+)")


def ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def hms(t):
    return datetime.fromtimestamp(t, timezone.utc).strftime("%H:%M:%S.%f")[:-3] if t is not None else "-"


def parse_logs(out):
    events = []
    for path in sorted(glob.glob(os.path.join(out, "server-*.log"))):
        server = os.path.basename(path)[: -len(".log")].replace("server-", "streams-")
        for raw in open(path, errors="replace"):
            m = LINE.match(ANSI.sub("", raw.rstrip("\n")))
            if not m:
                continue
            t, level, target, rest = m.groups()
            first = re.search(r"\s\w+=", rest)
            msg = rest if not first else rest[: first.start()]
            fields = {k: v.strip('"') for k, v in FIELD.findall(rest[len(msg):])}
            events.append({"t": ts(t), "server": server, "level": level, "target": target,
                           "msg": msg.strip(), "f": fields})
    events.sort(key=lambda e: e["t"])
    return events


def load_seconds(out):
    """Every kept window's per-second counts, merged by Unix second."""
    merged = {}
    for path in glob.glob(os.path.join(out, "seconds-*.json")):
        for sec, counts in json.load(open(path)).items():
            row = merged.setdefault(int(sec), {})
            for k, n in counts.items():
                row[k] = row.get(k, 0) + n
    return merged


def samples(out):
    rows = []
    for path in glob.glob(os.path.join(out, "watch-*.jsonl")) + glob.glob(os.path.join(out, "settle-*.jsonl")):
        for line in open(path):
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if "server" in row:
                rows.append(row)
    rows.sort(key=lambda r: r["t"])
    return rows


def refusal_class(key):
    """`refused append 503 shard_splitting` -> `503 shard_splitting` (refused and failed apart)."""
    parts = key.split()
    return " ".join([parts[0]] + parts[2:]) if len(parts) >= 4 else key


def window_counts(seconds, lo, hi):
    out = {}
    for sec in range(int(lo), int(hi) + 1):
        for k, n in seconds.get(sec, {}).items():
            if k != "served":
                c = refusal_class(k)
                out[c] = out.get(c, 0) + n
    return out


def sheds_in(rows, lo, hi):
    """Per instance, admission's sheds counted in [lo, hi] (the samples' cumulative `shed`)."""
    out = {}
    for server in sorted({r["server"] for r in rows}):
        mine = [r for r in rows if r["server"] == server]
        before = [r for r in mine if r["t"] <= lo]
        inside = [r for r in mine if lo < r["t"] <= hi + 5]
        if not inside:
            continue
        base = before[-1] if before else inside[0]
        last = inside[-1]
        out[server] = {"shed": last.get("shed", 0) - base.get("shed", 0),
                       "by": {k: last.get("shed_by", {}).get(k, 0) - base.get("shed_by", {}).get(k, 0)
                              for k in last.get("shed_by", {})}}
    return out


def stall(seconds, park, ends):
    """The bench's view: the last second with served touches at or before the park, the first after
    it, the pre-park mean rate (the ten seconds before the park's), and the first second back at 90%
    of it (information only: one child's catch-up can reach it)."""
    p = int(park)
    before = [s for s in sorted(seconds) if s <= p and seconds[s].get("served", 0) > 0]
    last = before[-1] if before else None
    base = [seconds.get(s, {}).get("served", 0) for s in range(p - 10, p)]
    rate = sum(base) / len(base) if base else 0
    after = [s for s in sorted(seconds) if s > p and seconds[s].get("served", 0) > 0]
    first = after[0] if after else None
    back = next((s for s in sorted(seconds) if s > p and seconds[s].get("served", 0) >= 0.9 * rate), None) if rate else None
    return {"last_served_before": last, "first_served_after": first, "pre_rate": round(rate, 1),
            "back_at_rate": back, "gap_s": (first - last) if first and last else None}


def opens_near(events, lo, hi, server=None):
    return [e for e in events if e["msg"] == "shard open's indexes loaded" and lo <= e["t"] <= hi
            and (server is None or e["server"] == server)]


def open_line(e):
    f = e["f"]
    return (f"{e['server']} {hms(e['t'])} index_ms={f.get('index_ms')} scan_ms={f.get('scan_ms')} "
            f"tails_ms={f.get('tails_ms')} unsealed_members={f.get('unsealed_members')} "
            f"unsealed_bytes={f.get('unsealed_bytes')} repaired={f.get('repaired')}")


def report_splits(events, seconds, rows):
    splits = [e for e in events if e["msg"] == "the split published, by phase"]
    published = {e["f"].get("op_id"): e for e in events if e["msg"] == "the split published"}
    print(f"== splits: {len(splits)} published")
    ended = [e for e in events if e["msg"].startswith("the split ended") or "split refused" in e["msg"]]
    for e in ended:
        print(f"   not published: {e['server']} {hms(e['t'])} {e['msg']} {e['f']}")
    verdicts = []
    for e in splits:
        f = e["f"]
        pause = int(f.get("pause_ms", 0))
        phases = {k: int(v) for k, v in PHASE.findall(f.get("phases", ""))}
        opens = {k[len("open "):]: v for k, v in phases.items() if k.startswith("open ")}
        drain = phases.get("drain", 0)
        line = e["t"]
        park = line - (pause + max(opens.values() or [0]) + drain) / 1000
        release = park + pause / 1000
        ends = {c: release + ms / 1000 for c, ms in opens.items()}
        pub = published.get(f.get("op_id"))
        print(f"-- split {f.get('op_id')} on {e['server']} (children {pub['f'].get('children') if pub else '?'})")
        print(f"   pause_ms={pause}  phases: {f.get('phases')}")
        print(f"   park {hms(park)}  release {hms(release)}  " +
              "  ".join(f"child {c} open end {hms(t)} (+{(t - park) * 1000:.0f} ms from park)" for c, t in sorted(ends.items())))
        for o in opens_near(events, release - 1, max(ends.values() or [release]) + 2):
            print(f"   open line: {open_line(o)}")
        st = stall(seconds, park, ends)
        first = st["first_served_after"]
        sec = lambda s: hms(s)[:8] if s else "-"
        print(f"   bench (1 s grain): last served second <= park {sec(st['last_served_before'])} "
              f"first served second after park {sec(first)} gap {st['gap_s']} s; pre-park rate {st['pre_rate']}/s, "
              f"back at 90% of it from {sec(st['back_at_rate'])}")
        if first is not None and ends:
            early = min(ends.values())
            print(f"   first served second starts {(first - park) * 1000:.0f} ms after the park (1 s grain: its touches "
                  f"completed in [{(first - park) * 1000:.0f}, {(first + 1 - park) * 1000:.0f}] ms); earlier child "
                  f"open end +{(early - park) * 1000:.0f} ms (information: the 90% second is not evidence of both children)")
        lo, hi = park - 2, max(ends.values() or [release]) + 10
        print(f"   refusals in [{hms(lo)}, {hms(hi)}] by class: {window_counts(seconds, lo, hi) or 'none'}")
        print(f"   sheds per instance in the window: {sheds_in(rows, lo, hi) or 'no samples'}")
        verdict = judge(park, ends, seconds, pause)
        verdict["op"] = f.get("op_id")
        print(f"   conditions: (1) later open end <= 2,000 ms {verdict['children_open_within_2000_ms_of_park']}; "
              f"(2) first served second - park <= 2.0 s {verdict['first_served_within_2000_ms_of_park']} (1 s grain); "
              f"(3) served in the 3 whole seconds after the later open's second {verdict['served_3s_after_later_open']} "
              f">= 90% of 3 x {verdict['pre_park_rate']}/s: {verdict['serving_after_the_later_open']}")
        if verdict["unexplained_after_opens"]:
            print("   UNEXPLAINED: both children opened within 2,000 ms but the cell did not serve at its rate after the "
                  "later open (condition 3 failed): COME BACK")
        if verdict["come_back_low_rate"]:
            print(f"   COME BACK: pre-park rate {verdict['pre_park_rate']}/s is under 20, condition 3 is noise")
        if verdict["come_back_children"]:
            print(f"   COME BACK: {len(ends)} child open(s) on the phase line, not two")
        verdict["refused_503_shard_splitting"] = sum(n for k, n in window_counts(seconds, lo, hi).items()
                                                     if "shard_splitting" in k)
        verdicts.append(verdict)
    return verdicts


def judge(park, ends, seconds, pause):
    """One split's inputs to the rule (GATE-DECISION-meas10k-rig-limits-2026-11-19 (a)). Every
    child serves within 2,000 ms of the park when all three hold:
    1. the later child's open end is at most 2,000 ms after the park (server log, to the ms);
    2. the bench's first served second after the park begins at most 2,000 ms after it (a lower
       bound at the 1 s grain: the earlier child's touches only);
    3. the three whole seconds after the second holding the later open end serve at least 90% of
       three times the pre-park rate (a child that opened and does not serve leaves about half)."""
    st = stall(seconds, park, ends)
    first, rate = st["first_served_after"], st["pre_rate"]
    later = max(ends.values()) if ends else None
    after = (sum(seconds.get(s, {}).get("served", 0) for s in range(int(later) + 1, int(later) + 4))
             if later is not None else 0)
    opened = later is not None and later - park <= 2.0
    serving = later is not None and rate > 0 and after >= 0.9 * 3 * rate
    return {"pause_ms": pause, "pause_under_1000": pause < 1000,
            "children_open_within_2000_ms_of_park": opened,
            "first_served_within_2000_ms_of_park": first is not None and first - park <= 2.0,
            "served_3s_after_later_open": after, "pre_park_rate": rate,
            "serving_after_the_later_open": serving,
            "unexplained_after_opens": opened and not serving,
            "come_back_low_rate": rate < 20,
            "come_back_children": len(ends) != 2}


def serves_within_2000(v):
    return (v["children_open_within_2000_ms_of_park"] and v["first_served_within_2000_ms_of_park"]
            and v["serving_after_the_later_open"])


def rule(passed, verdicts):
    """The §2 table's row: 1 when the step passed and every split's pause is under 1,000 and every
    child serves within 2,000 ms of the park; 2 when it passed otherwise; "no split" when it passed
    with no published split to judge (never row 1); "failed" for rows 3 to 5, read by hand."""
    if not passed:
        return "failed"
    if not verdicts:
        return "no split"
    if all(v["pause_under_1000"] and serves_within_2000(v) for v in verdicts):
        return 1
    return 2


def report_moves(events, seconds, rows):
    handoffs = [e for e in events if e["msg"] == "handoff"]
    print(f"== planned moves (F6 handoff lines): {len(handoffs)}")
    for e in handoffs:
        f = e["f"]
        release = e["t"]
        park = release - int(f.get("release_ms", 0)) / 1000
        shard = f.get("shard")
        opened = next((o for o in events if o["msg"] == "moved-in shard opened" and o["f"].get("shard") == shard
                       and o["t"] >= park - 1), None)
        print(f"-- {shard} from {e['server']}: fenced={f.get('fenced')} end={f.get('end')} park_ms={f.get('park_ms')} "
              f"flush_ms={f.get('flush_ms')} close_ms={f.get('close_ms')} release_ms={f.get('release_ms')}")
        print(f"   giver park {hms(park)} release {hms(release)}; target open: "
              + (f"{opened['server']} {hms(opened['t'])} (+{(opened['t'] - release) * 1000:.0f} ms from release)" if opened else "no `moved-in shard opened` line"))
        for o in opens_near(events, park - 1, (opened["t"] if opened else release) + 3,
                            opened["server"] if opened else None):
            print(f"   target open line: {open_line(o)}")
        st = stall(seconds, park, {})
        first = st["first_served_after"]
        print(f"   bench (1 s grain): first served second after the giver's park {hms(first)[:8]} "
              f"(within [{(first - release) * 1000:.0f}, {(first + 1 - release) * 1000:.0f}] ms of the release)" if first else
              "   bench: no served second after the park")
        lo, hi = park - 2, release + 10
        print(f"   refusals in [{hms(lo)}, {hms(hi)}] by class: {window_counts(seconds, lo, hi) or 'none'}")
        print(f"   sheds per instance in the window: {sheds_in(rows, lo, hi) or 'no samples'}")
    others = [e for e in events if e["target"].startswith("streams_slate::fleet") and
              ("rebalancer" in e["msg"] or "count balance" in e["msg"] or "drain" in e["msg"])]
    for e in others:
        print(f"   fleet: {e['server']} {hms(e['t'])} {e['msg']} {e['f']}")
    fenced = [e for e in events if "fenced" in e["msg"] or "by fence" in e["msg"] or e["f"].get("fenced") == "true"]
    print(f"== Fenced lines: {len(fenced)}")
    for e in fenced:
        print(f"   {e['server']} {hms(e['t'])} {e['msg']} {e['f']}")


def report_step(out, events, rows):
    print("== the step")
    for line in open(os.path.join(out, "steps.jsonl")):
        step = json.loads(line)
        if step.get("count") == "drain":
            print(f"drain: pass={step.get('pass')} merged_back={step.get('merged_back')} readback={step.get('readback')}")
            continue
        settle = step.get("settle") or {}
        print(f"count {step['count']}: pass={step['pass']} checks={step['checks']} held={step['held']} "
              f"first_attempt={step.get('first_attempt', {}).get('held')} sheds={step.get('sheds')} "
              f"sheds_by={step.get('sheds_by')} rss_mb_max={step.get('rss_mb_max')} oom={step.get('oom_killed')}")
        pop = step.get("population") or {}
        print(f"  window: p99 {(pop.get('latency_ms') or {}).get('p99')} ms created {pop.get('created')} "
              f"create_errors {pop.get('create_errors')} skipped {pop.get('skipped')} late {pop.get('late')}")
        print(f"  settled_window: {step.get('settled_window')}")
        print(f"  reshaping: {step.get('reshaping')}")
        for c in settle.get("chunk_rows") or []:
            print(f"  chunk {c.get('chunk')} t={hms(c.get('t'))}: held {c.get('held')} p99 {c.get('p99_ms')} ms "
                  f"first_attempt {c.get('first_attempt')} sheds {c.get('sheds')} errors {c.get('errors')} "
                  f"shards {c.get('shards')} live {c.get('live')}")
        print(f"  first-attempt refusals (step): {step.get('first_attempt', {}).get('refusals')}")
    members = [(int(e["f"].get("unsealed_members", 0)), e) for e in events
               if e["msg"] == "shard open's indexes loaded" and e["f"].get("unsealed_members", "").isdigit()]
    if members:
        n, e = max(members, key=lambda m: m[0])
        flag = "  COME BACK: over 1,000" if n > 1000 else ""
        print(f"largest unsealed_members on any open line: {n} ({open_line(e)}){flag}")
    peaks = {}
    for r in rows:
        peaks[r["server"]] = max(peaks.get(r["server"], 0), r.get("rss_mb", 0))
    print(f"RSS peak per instance (samples): { {k: round(v, 1) for k, v in sorted(peaks.items())} }")
    warn = [e for e in events if e["level"] in ("WARN", "ERROR")]
    kinds = {}
    for e in warn:
        kinds[e["msg"][:90]] = kinds.get(e["msg"][:90], 0) + 1
    print(f"server WARN/ERROR lines by message: {kinds or 'none'}")


def decide(verdicts, out):
    steps = [json.loads(l) for l in open(os.path.join(out, "steps.jsonl"))]
    step = next((s for s in steps if s.get("count") == 10000), None)
    passed = bool(step and step.get("pass"))
    print("== §2 decision rule inputs")
    for v in verdicts:
        print(f"   {v}")
    if not step:
        print("RULE: no 10,000 step recorded")
        return
    row = rule(passed, verdicts)
    if row == 1:
        print("RULE row 1: passes; every pause under 1,000; every child serves within 2,000 ms of the park "
              "(conditions 1-3 on every split): shape C stays in NEXT-WORK, the full ramp proceeds")
    elif row == 2:
        print("RULE row 2: passes, but a split's pause is 1,000 or more, or a child does not serve within 2,000 ms of "
              "the park (a condition 1-3 failed): the ramp proceeds, come back with the decomposition")
    elif row == "no split":
        print("RULE: NO PUBLISHED SPLIT: the step passed but there is no split to judge; not row 1, come back")
    else:
        print("RULE: the step FAILED: read the failing event against rows 3-5 (split with pause < 1,000 and time "
              "between release and an open end -> shape C resumes; pause >= 1,000 or time after the opens -> come back "
              "with the phase line; a move or anything else -> come back)")


def main():
    out = sys.argv[1]
    events = parse_logs(out)
    seconds = load_seconds(out)
    rows = samples(out)
    print(f"results {out}: {len(events)} log lines, {len(seconds)} bench seconds, {len(rows)} samples")
    verdicts = report_splits(events, seconds, rows)
    report_moves(events, seconds, rows)
    report_step(out, events, rows)
    decide(verdicts, out)


if __name__ == "__main__":
    main()
