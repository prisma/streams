"""counts.py on a synthetic run directory in run-local.sh's layout: the
windows it cuts (load, deferred, to_quiesced, tail, total and one per phase
group), each window's exact counts (requests by cell and class, bytes PUT
and GET, pump flushes and ring hits from /v1/debug/timings, CPU seconds),
no pump counts across a reboot, and the side-by-side comparison of two
runs with the environment keys that differ.

    python3 -m unittest discover -s bench/k2cost -p 'test_*.py'
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)


def server(boot: str, flushes: int, reqs: int, hits: int) -> dict:
    return {"streams-1": {"boot_id": boot, "store": None, "error": None,
                          "timings": {"0000": {"pump": {"flushes": flushes, "barrier_acked": reqs, "flushed_reqs": reqs,
                                                        "flushed_records": reqs, "flushed_bytes": 1000 * reqs},
                                               "tail_ring": {"hits": hits, "misses": 1, "published": 0,
                                                             "evicted": 0}}}}}


# label -> (t_ms, wal puts, sst gets 2xx, hist sst puts, put bytes, get bytes, pump flushes, pump reqs, ring hits, cpu)
SNAPS = {
    "start": (1_000, 10, 5, 0, 100, 50, 3, 4, 0, 1.5),
    "phase-1-start": (1_000, 10, 5, 0, 100, 50, 3, 4, 0, 1.5),
    "phase-1-end": (11_000, 12, 5, 0, 300, 50, 5, 6, 0, 1.75),
    "phase-2-start": (11_000, 12, 5, 0, 300, 50, 5, 6, 0, 1.75),
    "phase-3-end": (61_000, 40, 25, 0, 1_100, 450, 30, 60, 7, 4.0),
    "loadend": (61_000, 40, 25, 0, 1_100, 450, 30, 60, 7, 4.0),
    "quiesced": (121_000, 45, 26, 2, 5_100, 470, 35, 65, 7, 6.0),
    "end": (181_000, 49, 26, 2, 5_300, 470, 39, 69, 7, 7.0),
}


def snap(label: str, boot: str = "b1") -> dict:
    t, wal, get, hist, pb, gb, fl, rq, hits, cpu = SNAPS[label]
    cells = {"shard/wal/put": {"2xx": wal}, "shard/sst/get": {"2xx": get, "404": 1}}
    if hist:
        cells["hist/sst/put"] = {"2xx": hist}
    return {"t_ms": t, "s3lite": {"t_ms": t, "cells": cells, "stats": {"put_bytes": pb, "get_bytes": gb}},
            "servers": server(boot, fl, rq, hits), "procs": {"streams-1": {"pid": 7, "cpu_s": cpu, "rss_kb": 1}}}


def write_run(root: str, name: str, env: dict, reboot_at_end: bool = False) -> str:
    run = os.path.join(root, name)
    os.makedirs(run)
    for label in ("start", "loadend", "quiesced", "end"):
        s = snap(label, "b2" if reboot_at_end and label == "end" else "b1")
        with open(os.path.join(run, f"s3lite-{label}.json"), "w") as f:
            json.dump(s["s3lite"], f)
        with open(os.path.join(run, f"store-{label}.json"), "w") as f:
            json.dump({"t_ms": s["t_ms"], "servers": s["servers"], "procs": s["procs"]}, f)
    with open(os.path.join(run, "boundaries.jsonl"), "w") as f:
        for label in ("phase-1-start", "phase-1-end", "phase-2-start", "phase-3-end"):
            s = snap(label)
            f.write(json.dumps({"t_ms": s["t_ms"], "label": label, "s3lite": s["s3lite"], "servers": s["servers"],
                                "procs": s["procs"]}) + "\n")
    with open(os.path.join(run, "marks.jsonl"), "w") as f:
        for m in ({"mark": "phase_start", "i": 1, "mode": "setup", "bg": 0},
                  {"mark": "phase_start", "i": 2, "mode": "tail", "bg": 1},
                  {"mark": "phase_start", "i": 3, "mode": "group", "bg": 0}):
            f.write(json.dumps({"t": 0, **m}) + "\n")
    with open(os.path.join(run, "posture.json"), "w") as f:
        json.dump({"point": "SCR-S", "posture": "launch", "port_base": 9670,
                   "server_commit": {"expected": "2e0d1b86", "source": "K2_SERVER_COMMIT",
                                     "reported": ["2e0d1b86edf374719886a5f7dea68d258d1fa4e3"],
                                     "matches_expected": True, "matches_head": False},
                   "binaries_sha256": {"streams-slate": "9d61a54d" + "0" * 56},
                   "binaries": {"streams-slate": {"path": "/bin/streams-slate-2e0d1b86", "sha256": "9d61"}},
                   "servers": {"streams-1": {"env": env}}}, f)
    with open(os.path.join(run, "summary.json"), "w") as f:
        json.dump({"validity": {"valid": True}}, f)
    return run


class Counts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="counts-")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_windows_and_their_exact_counts(self):
        import counts
        c = counts.counts(write_run(self.tmp, "ref", {"INITIAL_SHARDS": "1"}))
        self.assertEqual(list(c["windows"]), ["load", "deferred", "to_quiesced", "tail", "total", "phase 1",
                                              "phases 2-3"])
        w = c["windows"]["load"]
        self.assertEqual((w["from"], w["to"], w["secs"]), ("start", "loadend", 60.0))
        self.assertEqual((w["class_a"], w["class_b"], w["free"]), (30, 20, 0))
        self.assertEqual((w["put_bytes"], w["get_bytes"]), (1_000, 400))
        self.assertEqual((w["pump_flushes"], w["pump_flushed_reqs"], w["ring_hits"], w["ring_misses"]), (27, 56, 7, 0))
        self.assertEqual(w["pump_by_shard"], {"streams-1/0000": {"flushes": 27, "barrier_acked": 56, "flushed_reqs": 56,
                                                                 "flushed_records": 56, "flushed_bytes": 56_000}})
        self.assertEqual(w["cpu_s"], {"streams-1": 2.5})
        self.assertEqual(w["cells"], {"shard/sst/get": {"class": "B", "n": 20, "by_status": {"2xx": 20}},
                                      "shard/wal/put": {"class": "A", "n": 30, "by_status": {"2xx": 30}}})
        d = c["windows"]["deferred"]
        self.assertEqual((d["class_a"], d["class_b"], d["put_bytes"], d["pump_flushes"]), (7, 1, 4_000, 5))
        self.assertEqual(d["cells"]["hist/sst/put"], {"class": "A", "n": 2, "by_status": {"2xx": 2}})
        self.assertEqual(c["windows"]["to_quiesced"]["class_a"], 37)
        self.assertEqual(c["windows"]["tail"]["pump_flushes"], 4)
        self.assertEqual(c["windows"]["phase 1"]["pump_flushes"], 2)
        p = c["windows"]["phases 2-3"]
        self.assertEqual((p["from"], p["to"], p["secs"], p["class_a"], p["pump_flushes"]),
                         ("phase-2-start", "phase-3-end", 50.0, 28, 25))
        self.assertEqual(c["server"]["expected"], "2e0d1b86")
        self.assertTrue(c["valid"])

    def test_no_pump_counts_across_a_reboot(self):
        import counts
        c = counts.counts(write_run(self.tmp, "ref", {}, reboot_at_end=True))
        self.assertIsNone(c["windows"]["total"]["pump_flushes"])
        self.assertIsNone(c["windows"]["tail"]["pump"])
        self.assertEqual(c["windows"]["total"]["class_a"], 39 + 2)  # the store's ledger does not reboot
        self.assertEqual(c["windows"]["load"]["pump_flushes"], 27)

    def test_compare_sets_two_runs_side_by_side(self):
        import counts
        ref = counts.counts(write_run(self.tmp, "ref", {"INITIAL_SHARDS": "1", "SLATE_S3_BUCKET": "k2-a"}))
        cand = counts.counts(write_run(self.tmp, "cand", {"INITIAL_SHARDS": "1", "SLATE_S3_BUCKET": "k2-b",
                                                          "TAIL_RING_BYTES": "8388608"}))
        cand["windows"]["load"]["cells"]["shard/sst/get"]["n"] = 2
        cand["windows"]["load"]["class_b"] = 2
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            counts.compare(ref, cand, ["load"], False, 5)
        text = out.getvalue()
        self.assertIn("env TAIL_RING_BYTES: ref (unset) -> cand 8388608", text)
        self.assertNotIn("SLATE_S3_BUCKET", text)
        rows = {l.split()[0]: l.split()[1:] for l in text.splitlines() if l.startswith("  ") and l.split()}
        self.assertEqual(rows["class_b"], ["20", "2", "-18", "-90%"])
        self.assertEqual(rows["shard/sst/get"], ["20", "2", "-18", "-90%"])
        self.assertEqual(rows["shard/wal/put"], ["30", "30", "0", "+0%"])
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            counts.compare(ref, cand, ["load"], True, 5)
        rows = {l.split()[0]: l.split()[1:] for l in out.getvalue().splitlines() if l.startswith("  ") and l.split()}
        self.assertEqual(rows["class_a"], ["1,800", "1,800", "0", "+0%"])  # 30 in 60 s, per hour

    def test_the_command_writes_counts_json(self):
        run = write_run(self.tmp, "ref", {})
        r = subprocess.run([sys.executable, os.path.join(HERE, "counts.py"), run], capture_output=True, text=True,
                           timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        with open(os.path.join(run, "counts.json")) as f:
            doc = json.load(f)
        self.assertEqual(doc["windows"]["load"]["class_a"], 30)
        self.assertIn("SCR-S [launch posture, ports 9670] server 2e0d1b86edf3 (K2_SERVER_COMMIT)", r.stdout)
        r = subprocess.run([sys.executable, os.path.join(HERE, "counts.py"), "--compare", run, run],
                           capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("-- phases 2-3 (50 s vs 50 s)", r.stdout)


if __name__ == "__main__":
    unittest.main()
