#!/usr/bin/env python3
"""Observe K2 field cells until stopped (target/k2-design.md §5, §8, §9.2).

    bench/k2cost/field/observe.py <run-id> [cell...] [--duration SECS]
        [--census-secs 300] [--hb-secs 1.5] [--desired-secs 20] [--scrape-secs 20]
        [--gen-secs 20] [--cpulog-secs 1800] [--scrape-cell CELL]... [--no-expiry-teardown]

Start it right after deploy-cell.sh and BEFORE gen.sh: a generator starts
its plan at boot, and every plan opens with `idle 300` so the observer
sees a pre-load baseline and every tier boundary falls between scrapes.
Per cell, appending stamped JSON lines to results/<run>/<cell>/observe.jsonl
(and rewriting observe-summary.json):

- census (every --census-secs; 120 under load, design §5): ListObjectsV2
  over the cell's bucket with its keys; bytes and objects per tier and kind
  under s3lite's rules (history2/ and streams/ hist, shards/ shard, fleet/
  and routers/ fleet, registry/ registry, telemetry/ split out of other),
  the UTC-daily peak, and the census's own LIST pages (harness requests).
- heartbeats (every --hb-secs, 1.5 s: faster than the 2 s beat, so no beat
  is skipped by aliasing; on their own
  thread per cell so no scrape or log read delays them): GET
  <FLEET_PREFIX>/fleet/streams-N.json and routers/router-M.json from the
  bucket, never from an instance, in parallel. Awake time from the per-boot
  beat sequence (`seq`, src/fleet/heartbeat.rs PERIOD 2 s): a continuous
  span counts its ts_ms delta, a gap above 6 s is sleep. CPU-seconds invert
  the beat's smoothing (cpu_pct = 0.6 previous + 0.4 reading, a first
  reading as is): reading = 2.5 e_n - 1.5 e_(n-1), times that beat's span;
  exact when every beat is read, interpolated over missed beats. The first
  beat after a sleep is logged (kind `wake`) with the reading both ways
  (its span with or without the sleep: whether the monotonic clock ran
  across the freeze is what that line shows). The first read credits
  min(seq x period, now - deploy start); a new boot credits min(seq x
  period, the read interval). Routers publish no seq: a span of reports
  <= 6 s apart counts its ts_ms delta, a later report one period.
- desired.json (every --desired-secs).
- scrape (every --scrape-secs = 20, design §7 C3; SCRAPE=1 cells and every
  --scrape-cell, recorded in the start line): GET
  /v1/debug/store?window=<scrape-secs> (cumulative `totals` plus the
  window's `ops` n/err cells: totals files 404 under `unbilled` with the
  free 304, while the window's err is NotFound + Failed, so the two split
  the billed 404s, src/store_timing/observations.rs), /v1/debug/load (the
  §8 gates, boot_id, git_commit and binary_sha256, checked against
  bins.json: a mismatch voids the cell) and /v1/debug/usage (per-stream
  list summed), each stamped with its own time, on each server's URL with
  the deployment bearer (AUTH_TOKEN, src/http/debug.rs). Only a server whose
  heartbeat is live (age <= 3 s) or that is KEEP_AWAKE and unexpired is
  scraped: a scrape never wakes a sleeping server, so a scraped cell's
  awake time is its own. One exception, so every load window has a
  pre-load bracket: in a scraped cell without KEEP_AWAKE (F2 on f1b) each
  server gets ONE baseline scrape at observer start even if asleep (one
  logged wake, `baseline: true`); wakes are warm and keep the totals
  (smoke sm1002e), so that scrape brackets everything until the load
  wakes it. Routers are never sent anything.
- generator (every --gen-secs): each k2gen plan's new window lines from
  <gen>/windows?since=N into gen-<g>-windows.jsonl, its plan state, and its
  ledgers once the plan has ended (gen-<g>-ledgers.json); a finished or
  destroyed generator is never polled again (its guard is released, so it
  can sleep).
- wrapper CPU (every --cpulog-secs for awake instances, and for all at
  stop): the wrappers' `cpu sample:` log lines (CPU_LOG_SECS, cumulative
  CPU seconds of the instance's processes and the kernel's busy time, per
  boot) read with `compute logs --tail`; the last line per boot is kept.
  The only CPU measure for routers.
- KEEP_AWAKE expiry (every 60 s unless --no-expiry-teardown): any ledger
  service past its expiry is destroyed with `teardown.py <run> --expired
  --yes` (the wrapper's guard has already released it).
"""
from __future__ import annotations

import argparse
import calendar
import concurrent.futures as cf
import email.utils
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time

import fieldlib as F

CADENCE_S = 2.0
GAP_S = 3 * CADENCE_S
LIVE_S = 3.0
MEM_USD_GIB_H = 0.006
CPU_USD_VCPU_H = 0.064
HB_LOG_S = 60
CPU_LINE = re.compile(r"cpu sample: (\{.*\})")


def tier_of(key: str) -> str:
    if "history2/" in key:
        return "hist"
    if "shards/" in key:
        return "shard"
    if "streams/" in key:
        return "hist"
    if "fleet/" in key or "routers/" in key:
        return "fleet"
    if "registry/" in key or key.endswith("topology.json"):
        return "registry"
    if "telemetry/" in key:
        return "telemetry"
    return "other"


def kind_of(key: str) -> str:
    if "/wal/" in key or key.startswith("wal/"):
        return "wal"
    if "compaction" in key:
        return "compactions"
    if "manifest" in key:
        return "manifest"
    if "/compacted/" in key or key.endswith(".sst"):
        return "sst"
    return "meta"


def bucket_age_s(resp) -> float | None:
    """Object age by the bucket's clock: response Date minus Last-Modified."""
    try:
        date = email.utils.parsedate_to_datetime(resp["ResponseMetadata"]["HTTPHeaders"]["date"])
        return (date - resp["LastModified"]).total_seconds()
    except (KeyError, TypeError, ValueError):
        return None


def reading_sum(e0: float, e1: float, k: int) -> tuple:
    """Sum of the k raw cpu readings behind the smoothed e0 -> e1 (percent),
    and whether it is exact. x_n = 2.5 e_n - 1.5 e_(n-1) (a first reading,
    after e = 0, is taken as is); over k > 1 beats the unseen e's are
    interpolated: sum x = sum_(j<k) e_j + 2.5 (e_k - e_0)."""
    if k == 1:
        return (e1 if e0 == 0 else 2.5 * e1 - 1.5 * e0), True
    return k * e0 + (e1 - e0) * (k - 1) / 2 + 2.5 * (e1 - e0), False


class Instance:
    STATE = ("prev", "awake_s", "cpu_s", "cpu_approx_s", "cpu_wake_extra_s", "first_credit_s", "asleep_gap_s",
             "sleeps", "boots", "samples", "beats_exact", "beats_interp", "periods", "intervals", "open_since",
             "wakes", "logged", "hb_bytes")

    def __init__(self, name: str, memory_gib, deploy_ms: int | None, keep_awake: bool):
        self.name, self.memory_gib, self.deploy_ms, self.keep_awake = name, memory_gib, deploy_ms, keep_awake
        self.prev = None
        self.awake_s = self.cpu_s = self.cpu_approx_s = self.cpu_wake_extra_s = self.first_credit_s = 0.0
        self.asleep_gap_s = 0.0
        self.sleeps = self.boots = self.samples = self.beats_exact = self.beats_interp = 0
        self.periods: list = []
        self.intervals: list = []
        self.open_since = None
        self.wakes: list = []
        self.logged = 0.0
        self.hb_bytes = None
        self.live = False

    def period(self) -> float:
        return sum(self.periods) / len(self.periods) if self.periods else CADENCE_S

    def mark_live(self, live: bool, ts_ms: int) -> None:
        self.live = live
        if live and self.open_since is None:
            self.open_since = F.utc()
        if not live and self.open_since is not None:
            self.intervals.append([self.open_since, F.utc(ts_ms)])
            self.open_since = None

    def beat(self, hb: dict, wall: float) -> dict | None:
        """Integrate one server heartbeat; an event dict for the log, or None."""
        cur = {"boot": hb.get("boot_id"), "seq": int(hb.get("seq") or 0), "ts": int(hb.get("ts_ms") or 0),
               "e": float(hb.get("cpu_pct") or 0), "wall": wall}
        p, ev = self.prev, None
        if p is None or cur["boot"] != p["boot"] or cur["seq"] < p["seq"]:
            first = p is None
            span = cur["seq"] * self.period()
            if first and self.deploy_ms:
                span = min(span, max(0.0, wall - self.deploy_ms / 1000))
            elif not first:
                span = min(span, wall - p["wall"])
                self.boots += 1
            self.awake_s += span
            cpu = cur["e"] / 100 * span
            self.cpu_s += cpu
            if cur["seq"] <= 1:
                self.beats_exact += 1
            else:
                self.cpu_approx_s += cpu
            if first:
                self.first_credit_s += span
            ev = {"note": "first" if first else "boot_change", "credit_s": round(span, 1), "seq": cur["seq"]}
        elif cur["seq"] > p["seq"]:
            k, dts = cur["seq"] - p["seq"], (cur["ts"] - p["ts"]) / 1000
            sx, exact = reading_sum(float(p.get("e") or 0), cur["e"], k)
            gap = dts - k * self.period()
            if gap <= GAP_S:
                self.awake_s += dts
                self.cpu_s += sx / 100 * dts / k
                if exact:
                    self.periods = (self.periods + [dts])[-60:]
            else:
                # Slept between the two beats: k beats ran, one of them the
                # first after the wake, whose reading spans the freeze if the
                # monotonic clock ran across it (the extra is kept apart).
                self.awake_s += k * self.period()
                self.cpu_s += sx / 100 * self.period()
                self.cpu_wake_extra_s += max(0.0, sx / 100 * (dts - k * self.period()) / k)
                self.sleeps += 1
                self.asleep_gap_s += gap
                ev = {"note": "wake", "gap_s": round(gap, 1), "beats": k, "e_prev": p.get("e"), "e": cur["e"],
                      "reading_pct": round(sx / k, 2), "dts": round(dts, 1)}
                self.wakes = (self.wakes + [{**ev, "utc": F.utc(cur["ts"])}])[-20:]
            if exact:
                self.beats_exact += 1
            else:
                self.beats_interp += k
        self.samples += 1
        self.prev = cur
        return ev

    def report(self, rep: dict, wall: float) -> dict | None:
        """Integrate one router report (no seq: spans of reports <= 6 s apart)."""
        cur = {"ts": int(rep.get("ts_ms") or 0), "wall": wall}
        p, ev = self.prev, None
        if p is None:
            ev = {"note": "first"}
        elif cur["ts"] > p["ts"]:
            dts = (cur["ts"] - p["ts"]) / 1000
            if dts <= GAP_S:
                self.awake_s += dts
                self.periods = (self.periods + [dts])[-60:]
            else:
                self.awake_s += self.period()
                self.sleeps += 1
                self.asleep_gap_s += dts - self.period()
                ev = {"note": "wake", "gap_s": round(dts - self.period(), 1)}
        self.samples += 1
        self.prev = cur
        return ev

    def summary(self) -> dict:
        cpu = max(0.0, self.cpu_s)
        return {"awake_s": round(self.awake_s, 1), "cpu_s": round(cpu, 2), "cpu_approx_s": round(self.cpu_approx_s, 2),
                "cpu_wake_extra_s": round(self.cpu_wake_extra_s, 2), "first_credit_s": round(self.first_credit_s, 1),
                "beats_exact": self.beats_exact, "beats_interp": self.beats_interp, "sleeps": self.sleeps,
                "asleep_gap_s": round(self.asleep_gap_s, 1), "boots": self.boots, "samples": self.samples,
                "beat_period_s": round(self.period(), 3), "memory_gib": self.memory_gib, "keep_awake": self.keep_awake,
                "hb_bytes": self.hb_bytes, "wakes": self.wakes[-5:],
                "awake_intervals": self.intervals[-50:] + ([[self.open_since, None]] if self.open_since else []),
                "usd_awake_memory": round(self.awake_s / 3600 * (self.memory_gib or 0) * MEM_USD_GIB_H, 6),
                "usd_cpu": round(cpu / 3600 * CPU_USD_VCPU_H, 6)}


def ms_of(utc: str | None) -> int | None:
    try:
        return calendar.timegm(time.strptime(utc, "%Y-%m-%dT%H:%M:%SZ")) * 1000 if utc else None
    except ValueError:
        return None


class Cell:
    def __init__(self, run: str, cell: str, args):
        self.run, self.cell, self.a = run, cell, args
        self.cfg = F.read_cell_file(cell)
        self.st = F.read_json(os.path.join(F.cell_dir(run, cell), "cell.json")) or {}
        self.scrape_source = "cell file" if self.cfg.get("SCRAPE") == "1" else (
            "--scrape-cell" if cell in (args.scrape_cell or []) else None)
        self.scrape = self.scrape_source is not None
        self.n = F.cell_int(self.cfg, "SERVERS", 1)
        self.m = F.cell_int(self.cfg, "ROUTERS", 0)
        self.s3, self.bucket = F.data_s3(run, cell)
        self.out = os.path.join(F.results_dir(run, cell), "observe.jsonl")
        svcs = self.ledger_services()
        by = {r.get("instance"): r for r in svcs.values()}

        def inst(name: str) -> Instance:
            r = by.get(name) or {}
            dep = ms_of(r.get("deployed"))
            start = dep - int(float(r.get("deploy_secs") or 0) * 1000) if dep else None
            return Instance(name, r.get("memory_gib"), start, bool(r.get("keep_awake")))
        self.inst = {f"streams-{i}": inst(f"streams-{i}") for i in range(1, self.n + 1)}
        self.routers = {f"router-{j}": inst(f"router-{j}") for j in range(1, self.m + 1)}
        self.harness = {"list_pages": 0, "get": 0, "scrape_http": 0, "gen_http": 0, "cpulog_reads": 0}
        self.peaks: dict = {}
        self.last_census = None
        self.gen_since: dict = {}
        self.gen_done: set = set()
        self.wrapper_cpu: dict = {}
        self.void = None
        self.baseline: set = set()  # servers given their one baseline scrape this start
        self.restored = None
        self.due = {k: 0.0 for k in ("census", "hb", "desired", "scrape", "gen", "cpulog")}
        self.due["cpulog"] = time.time() + args.cpulog_secs
        self.auth = {"Authorization": f"Bearer {F.read_secret(run, cell, 'auth-token')}"} if self.scrape else None
        man = F.read_json(os.path.join(F.FIELD, "bins.json")) or {}
        sk = (man.get("_latest") or {}).get("streams")
        self.expect = {"git_commit": (man.get(sk) or {}).get("gitCommit"), "binary_sha256": (man.get(sk) or {}).get("sha256")}
        self.state_path = os.path.join(F.results_dir(run, cell), "observe-state.json")
        self.pool = cf.ThreadPoolExecutor(max_workers=max(2, self.n + self.m))
        self.lock = threading.Lock()  # observe.jsonl and the counters: the heartbeat thread shares them
        self.restore()

    def ledger_services(self) -> dict:
        return (F.load_resources(self.run)["cells"].get(self.cell) or {}).get("services") or {}

    def restore(self) -> None:
        """An observer restart continues the integrals where they stopped."""
        doc = F.read_json(self.state_path)
        if not doc:
            return
        for name, saved in (doc.get("instances") or {}).items():
            inst = self.inst.get(name) or self.routers.get(name)
            if inst:
                for k in Instance.STATE:
                    if k in saved:
                        setattr(inst, k, saved[k])
        self.peaks = doc.get("peaks") or {}
        self.harness.update(doc.get("harness") or {})
        self.gen_since = doc.get("gen_since") or {}
        self.gen_done = set(doc.get("gen_done") or [])
        self.wrapper_cpu = doc.get("wrapper_cpu") or {}
        self.void = doc.get("void")
        self.restored = doc.get("utc")

    def save(self) -> None:
        with self.lock:
            self._save()

    def _save(self) -> None:
        F.write_json(self.state_path, {
            "utc": F.utc(), "peaks": self.peaks, "harness": self.harness, "gen_since": self.gen_since,
            "gen_done": sorted(self.gen_done), "wrapper_cpu": self.wrapper_cpu, "void": self.void,
            "instances": {k: {f: getattr(v, f) for f in Instance.STATE} for k, v in {**self.inst, **self.routers}.items()},
        })
        F.write_json(os.path.join(F.results_dir(self.run, self.cell), "observe-summary.json"), self.summary())

    def emit(self, kind: str, **rec) -> None:
        with self.lock:
            F.append_jsonl(self.out, {"t": F.now_ms(), "utc": F.utc(), "kind": kind, "cell": self.cell, **rec})

    # ------------------------------------------------------------ census
    def census(self) -> None:
        t0 = time.time()
        tiers: dict = {}
        pages = objects = total = 0
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, PaginationConfig={"PageSize": 1000}):
            pages += 1
            for o in page.get("Contents", []):
                t = tiers.setdefault(tier_of(o["Key"]), {})
                k = t.setdefault(kind_of(o["Key"]), {"objects": 0, "bytes": 0})
                k["objects"] += 1
                k["bytes"] += o["Size"]
                objects += 1
                total += o["Size"]
        self.harness["list_pages"] += pages
        day = F.utc()[:10]
        peak = self.peaks.get(day)
        if not peak or total > peak["bytes"]:
            self.peaks[day] = {"bytes": total, "objects": objects, "utc": F.utc(),
                               "tiers": {t: sum(v["bytes"] for v in kinds.values()) for t, kinds in tiers.items()}}
        self.last_census = {"bytes": total, "objects": objects, "utc": F.utc()}
        self.emit("census", bytes=total, objects=objects, tiers=tiers, list_pages=pages,
                  secs=round(time.time() - t0, 2), daily_peak=self.peaks[day])

    # ------------------------------------------------------------ heartbeats
    def read_doc(self, key: str, count: bool = True):
        if count:
            with self.lock:
                self.harness["get"] += 1
        try:
            r = self.s3.get_object(Bucket=self.bucket, Key=key)
            return json.loads(r["Body"].read()), r
        except Exception:  # noqa: BLE001 - absent (404, billed) or unreadable
            return None, None

    def heartbeats(self) -> None:
        fp = self.st.get("fleet_prefix", F.FLEET_PREFIX)
        keys = {**{n: f"{fp}/fleet/{n}.json" for n in self.inst}, **{n: f"{fp}/routers/{n}.json" for n in self.routers}}
        docs = dict(zip(keys, self.pool.map(lambda k: self.read_doc(k, count=False), keys.values())))
        wall = time.time()
        with self.lock:
            self.harness["get"] += len(keys)
            events = [self.integrate_one(name, doc, r, wall) for name, (doc, r) in docs.items()]
        for kind, fields in (e for evs in events for e in evs):
            self.emit(kind, **fields)

    def integrate_one(self, name: str, doc, r, wall: float) -> list:
        """Integrate one read (under self.lock); the lines to log."""
        out = []
        inst = self.inst.get(name) or self.routers[name]
        kind = "heartbeat" if name in self.inst else "router"
        if not doc:
            if wall - inst.logged >= HB_LOG_S:
                inst.logged = wall
                out.append((kind, {"instance": name, "missing": True}))
            return out
        age = bucket_age_s(r)
        inst.hb_bytes = (r or {}).get("ContentLength", inst.hb_bytes)
        ev = inst.beat(doc, wall) if kind == "heartbeat" else inst.report(doc, wall)
        inst.mark_live(age is not None and age <= LIVE_S, int(doc.get("ts_ms") or 0))
        if ev and ev["note"] == "wake":
            out.append(("wake", {"instance": name, **ev, "ts_ms": doc.get("ts_ms"), "boot_id": doc.get("boot_id")}))
        if ev or wall - inst.logged >= HB_LOG_S:
            inst.logged = wall
            extra = ({"seq": doc.get("seq"), "boot_id": doc.get("boot_id"), "cpu_pct": doc.get("cpu_pct"),
                      "rss_mb": doc.get("rss_mb"), "rps": doc.get("rps"),
                      "owned_shards": len(doc.get("owned_shards") or []), "draining": doc.get("draining"),
                      "withdrawn": doc.get("withdrawn"), "absorb_lag_max_secs": doc.get("absorb_lag_max_secs"),
                      "cpu_s": round(inst.cpu_s, 2)} if kind == "heartbeat"
                     else {"client_p50_ms": doc.get("client_p50_ms")})
            out.append((kind, {"instance": name, "ts_ms": doc.get("ts_ms"), "age_s": age, "live": inst.live,
                               "awake_s": round(inst.awake_s, 1), "note": (ev or {}).get("note"), **extra}))
        return out

    def desired(self) -> None:
        fp = self.st.get("fleet_prefix", F.FLEET_PREFIX)
        d, _ = self.read_doc(f"{fp}/fleet/desired.json")
        if d:
            self.emit("desired", count=d.get("count"), reason=d.get("reason"), epoch=d.get("epoch"),
                      computed_at_ms=d.get("computed_at_ms"))

    # ------------------------------------------------------------ scrape
    def scrapeable(self, name: str, svcs: dict) -> bool:
        """Never wake a server: only a live one, or an unexpired KEEP_AWAKE
        one; except one baseline scrape per server at observer start in a
        cell without KEEP_AWAKE."""
        rec = next((r for r in svcs.values() if r.get("instance") == name), {})
        if rec.get("status") == "deleted":
            return False
        if rec.get("keep_awake") and (rec.get("keep_awake_expires") or "") > F.utc():
            return True
        if self.cfg.get("KEEP_AWAKE") != "1" and name not in self.baseline:
            return True
        return self.inst[name].live

    def scrape_servers(self) -> None:
        assert self.scrape, "a cell without SCRAPE=1 or --scrape-cell is never probed"
        self.st = F.read_json(os.path.join(F.cell_dir(self.run, self.cell), "cell.json")) or self.st
        svcs = self.ledger_services()
        win = max(1, int(round(self.a.scrape_secs)))
        for name, url in sorted((self.st.get("server_urls") or {}).items()):
            if name not in self.inst or not self.scrapeable(name, svcs):
                continue
            rec = {"instance": name}
            if name not in self.baseline:
                self.baseline.add(name)
                rec["baseline"], rec["was_live"] = True, self.inst[name].live
            for path, field in ((f"/v1/debug/store?window={win}", "store"), ("/v1/debug/load", "load"),
                                ("/v1/debug/usage", "usage")):
                self.harness["scrape_http"] += 1
                t0 = F.now_ms()
                status, body, _ = F.http_get(url + path, headers=self.auth, timeout=15)
                rec[f"{field}_t"], rec[f"{field}_ms"] = t0, F.now_ms() - t0
                if status != 200:
                    rec[f"{field}_error"] = f"{status} {body[:120]!r}"
                    continue
                doc = json.loads(body)
                if field == "store":
                    ops = {k: {"n": v.get("n"), "err": v.get("err")} for k, v in (doc.get("ops") or {}).items()}
                    doc = {"totals": doc.get("totals"), "out_inflight_now": doc.get("out_inflight_now"),
                           "served_from": doc.get("served_from"),
                           "window": {"ts_ms": doc.get("ts_ms"), "window_secs": doc.get("window_secs"), "ops": ops,
                                      "saturated": sum(int(v["n"] or 0) for v in ops.values()) >= 16_000}}
                elif field == "usage":
                    streams = doc.pop("streams", None) or []
                    sums = {k: sum(int(s.get(k) or 0) for s in streams)
                            for k in ("requests", "records", "bytes_in", "bytes_out", "plaintext_bytes", "frame_bytes")}
                    doc["listed_streams"], doc["listed_sums"] = len(streams), sums
                elif field == "load":
                    self.identity(name, doc)
                rec[field] = doc
            self.emit("scrape", **rec)

    def identity(self, name: str, load: dict) -> None:
        """Design §6: refuse a run whose git_commit (or binary) differs."""
        bad = [f"{k} {load.get(k)!r} != bins.json {v!r}" for k, v in self.expect.items()
               if v and load.get(k) and load.get(k) != v]
        bad += [f"{k} missing" for k, v in self.expect.items() if v and not load.get(k)]
        if bad and not self.void:
            self.void = f"{name}: {'; '.join(bad)}"
            self.emit("identity", instance=name, ok=False, void=self.void)
            F.say(f"VOID {self.cell}: {self.void}")

    # ------------------------------------------------------------ generators
    def generators(self, final: bool = False) -> None:
        self.st = F.read_json(os.path.join(F.cell_dir(self.run, self.cell), "cell.json")) or self.st
        svcs = self.ledger_services()
        for g, info in sorted((self.st.get("gens") or {}).items()):
            if g in self.gen_done or (svcs.get(info["service"]) or {}).get("status") == "deleted":
                continue
            url, path = info["url"], os.path.join(F.results_dir(self.run, self.cell), f"gen-{g}-windows.jsonl")
            since = self.gen_since.get(g)
            if since is None:
                since = sum(1 for _ in open(path, encoding="utf-8")) if os.path.exists(path) else 0
            self.harness["gen_http"] += 1
            status, body, hdrs = F.http_get(f"{url}/windows?since={since}", timeout=20)
            lines = [l for l in body.decode(errors="replace").splitlines() if l.strip()] if status == 200 else []
            if lines:
                with open(path, "a", encoding="utf-8") as f:
                    f.write("\n".join(lines) + "\n")
            self.gen_since[g] = since + len(lines)
            self.harness["gen_http"] += 1
            s2, b2, _ = F.http_get(url + "/", timeout=15)
            state = {}
            try:
                state = json.loads(b2) if s2 == 200 else {}
            except ValueError:
                pass
            invs = [{k: i.get(k) for k in ("inv", "stage", "rc", "error", "startMs", "endMs")} for i in state.get("invs", [])]
            last = json.loads(lines[-1]) if lines else None
            self.emit("gen", gen=g, status=status, new_lines=len(lines), lines=self.gen_since[g], invs=invs,
                      started=state.get("startedMs"), ended=state.get("endedMs"), aborted=state.get("aborted"),
                      refused=state.get("refused"), keep_awake=state.get("keepAwake"),
                      last_window={k: (last or {}).get(k) for k in ("inv", "mode", "acked_requests", "acked_records",
                                                                   "payload_bytes", "append_p99_ms", "status_counts")})
            if state.get("endedMs") and len(lines) < 5000:
                self.harness["gen_http"] += 1
                s3, b3, _ = F.http_get(url + "/ledgers", timeout=30)
                if s3 == 200:
                    with open(os.path.join(F.results_dir(self.run, self.cell), f"gen-{g}-ledgers.json"), "wb") as f:
                        f.write(b3)
                    self.gen_done.add(g)  # never polled again: it may sleep

    # ------------------------------------------------------------ wrapper CPU
    def cpulog(self, final: bool = False, only: set | None = None) -> None:
        """The wrappers' cumulative CPU lines; the last one per boot."""
        done = {(self.st.get("gens") or {}).get(g, {}).get("service") for g in self.gen_done}
        for rec in self.ledger_services().values():
            name = rec.get("instance") or rec.get("name")
            inst = self.inst.get(name) or self.routers.get(name)
            if rec.get("status") == "deleted" or not rec.get("version") or (only and rec.get("name") not in only):
                continue
            awake = (rec.get("role") == "gen" and rec.get("name") not in done) or (inst and inst.live)
            if not final and not awake:
                continue  # a log read is never allowed to wake an instance mid-run
            self.harness["cpulog_reads"] += 1
            try:
                lines = F.compute_logs(rec["version"], seconds=25, tail=300, from_start=False, quiet_secs=4)
            except Exception as e:  # noqa: BLE001 - informational
                self.emit("error", source="cpulog", instance=name, error=str(e)[:200])
                continue
            boots = self.wrapper_cpu.setdefault(name, {})
            guard = [l.strip()[-160:] for l in lines if "keep-awake" in l][-5:]
            for line in lines:
                m = CPU_LINE.search(line)
                if not m:
                    continue
                try:
                    s = json.loads(m.group(1))
                except ValueError:
                    continue
                b = boots.get(str(s.get("boot_ms")))
                if not b or s.get("t_ms", 0) > b.get("t_ms", 0):
                    boots[str(s.get("boot_ms"))] = {**s, "version": rec["version"]}
            self.emit("wrapper_cpu", instance=name, version=rec["version"], lines=len(lines), boots=boots, final=final,
                      keep_awake_lines=guard)

    # ------------------------------------------------------------ expiry
    def expired(self) -> list:
        now = F.utc()
        return sorted(r["name"] for r in self.ledger_services().values() if r.get("keep_awake")
                      and r.get("status") != "deleted" and (r.get("keep_awake_expires") or "9") < now)

    # ------------------------------------------------------------ loop
    def beat_loop(self, stop: dict) -> None:
        """The heartbeat reads, on their own thread: nothing else delays them."""
        while not stop["now"]:
            t0 = time.time()
            try:
                self.heartbeats()
            except Exception as e:  # noqa: BLE001 - an observer never dies on one source
                self.emit("error", source="hb", error=f"{type(e).__name__}: {str(e)[:300]}")
            time.sleep(max(0.05, self.a.hb_secs - (time.time() - t0)))

    def tick(self) -> None:
        now = time.time()
        jobs = [("census", self.a.census_secs, self.census), ("desired", self.a.desired_secs, self.desired),
                ("gen", self.a.gen_secs, self.generators), ("cpulog", self.a.cpulog_secs, self.cpulog)]
        if self.scrape:
            jobs.append(("scrape", self.a.scrape_secs, self.scrape_servers))
        for name, every, fn in jobs:
            if now >= self.due[name]:
                self.due[name] = now + every
                try:
                    fn()
                except Exception as e:  # noqa: BLE001 - an observer never dies on one source
                    self.emit("error", source=name, error=f"{type(e).__name__}: {str(e)[:300]}")

    def summary(self) -> dict:
        inst = {k: v.summary() for k, v in {**self.inst, **self.routers}.items()}
        for k, boots in self.wrapper_cpu.items():
            if k in inst:
                inst[k]["wrapper_cpu_s"] = round(sum(float(b.get("proc_cpu_s") or 0) for b in boots.values()), 2)
                inst[k]["wrapper_vm_busy_s"] = round(sum(float(b.get("vm_busy_s") or 0) for b in boots.values()), 2)
                inst[k]["wrapper_boots"] = len(boots)
        return {"cell": self.cell, "utc": F.utc(), "scrape": self.scrape, "scrape_source": self.scrape_source,
                "void": self.void, "last_census": self.last_census, "daily_peaks": self.peaks, "instances": inst,
                "harness_requests": self.harness, "wrapper_cpu": self.wrapper_cpu,
                "harness_usd": round(self.harness["list_pages"] * 5e-6 + self.harness["get"] * 5e-7, 6),
                "gen_lines": self.gen_since}


def expiry_teardown(run: str, obs: list, state: dict) -> None:
    """Destroy every ledger service past its KEEP_AWAKE expiry."""
    late = sorted({n for c in obs for n in c.expired()})
    if not late or time.time() < state.get("backoff", 0):
        return
    F.say(f"KEEP_AWAKE expired: {late}: teardown.py {run} --expired --yes")
    for c in obs:  # their CPU log lines go with their versions
        c.cpulog(final=True, only=set(late))
    try:
        p = subprocess.run([sys.executable, os.path.join(F.HERE, "teardown.py"), run, "--expired", "--yes"],
                           capture_output=True, text=True, timeout=1500)
        rc, tail = p.returncode, (p.stdout + p.stderr)[-600:]
    except subprocess.TimeoutExpired:
        rc, tail = -1, "timeout"
    if rc != 0:
        state["backoff"] = time.time() + 600
    for c in obs:
        c.emit("expiry_teardown", services=late, rc=rc, tail=tail)


def main() -> None:
    F.banner("observe")
    ap = argparse.ArgumentParser()
    ap.add_argument("run")
    ap.add_argument("cells", nargs="*")
    ap.add_argument("--duration", type=float, default=0, help="seconds; 0 = until SIGINT/SIGTERM")
    ap.add_argument("--census-secs", type=float, default=300)
    ap.add_argument("--hb-secs", type=float, default=1.5)
    ap.add_argument("--desired-secs", type=float, default=20)
    ap.add_argument("--scrape-secs", type=float, default=20)
    ap.add_argument("--gen-secs", type=float, default=20)
    ap.add_argument("--cpulog-secs", type=float, default=1800)
    ap.add_argument("--scrape-cell", action="append", default=[],
                    help="scrape this cell's live servers although its cell file says SCRAPE=0 (F2 on f1b)")
    ap.add_argument("--no-expiry-teardown", action="store_true")
    a = ap.parse_args()
    F.check_names(a.run)
    if a.hb_secs > CADENCE_S:
        F.say(f"WARNING: --hb-secs {a.hb_secs} > {CADENCE_S}: beats are missed, CPU is interpolated")
    cells = a.cells or sorted(F.load_resources(a.run)["cells"])
    obs = [Cell(a.run, c, a) for c in cells]
    stop = {"now": False}
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.update(now=True))
    end = time.time() + a.duration if a.duration else None
    for c in obs:
        c.emit("start", scrape=c.scrape, scrape_source=c.scrape_source, servers=c.n, routers=c.m, args=vars(a),
               restored_from=c.restored, expect=c.expect)
        F.say(f"observing {a.run}/{c.cell}: servers={c.n} routers={c.m} scrape={c.scrape_source} -> {c.out}")
    beaters = [threading.Thread(target=c.beat_loop, args=(stop,), daemon=True) for c in obs]
    for t in beaters:
        t.start()
    exp_state, next_exp, next_save = {}, 0.0, 0.0
    while not stop["now"] and (end is None or time.time() < end):
        for c in obs:
            c.tick()
        if time.time() >= next_save:
            next_save = time.time() + 5
            for c in obs:
                c.save()
        if not a.no_expiry_teardown and time.time() >= next_exp:
            next_exp = time.time() + 60
            expiry_teardown(a.run, obs, exp_state)
        time.sleep(0.2)
    stop["now"] = True
    for t in beaters:
        t.join(timeout=30)
    for c in obs:
        c.generators(final=True)
        c.cpulog(final=True)
        c.emit("stop", summary=c.summary())
        c.save()
        F.say(json.dumps(c.summary(), indent=1)[:4000])


if __name__ == "__main__":
    main()
