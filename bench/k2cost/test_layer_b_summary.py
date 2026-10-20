"""layer_b_summary.py on synthetic Layer B results: the lines the layer-b
workflow prints (each bar, the RSS summary, the driver-pools fact, the
verdict), and, when the rig ended without a verdict, the rig log's tail.

    python3 -m unittest discover -s bench/k2cost -p 'test_*.py'
"""

from __future__ import annotations

import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import layer_b_summary as S  # noqa: E402

T0 = 1_791_000_000_000  # ms
MIN = 60_000


def write_jsonl(path: str, rows: list) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


class Summary(unittest.TestCase):
    def setUp(self):
        self.results = tempfile.mkdtemp(prefix="layer-b-summary-")
        self.addCleanup(shutil.rmtree, self.results)
        self.run_dir = os.path.join(self.results, "layer-b-20261020T130000Z")
        os.makedirs(self.run_dir)
        marks = {"idle_start": T0, "load_start": T0 + 15 * MIN, "load_end": T0 + 85 * MIN, "q0_start": T0 + 100 * MIN}
        write_jsonl(os.path.join(self.run_dir, "marks.jsonl"), [{"t": t, "mark": m} for m, t in marks.items()])
        rows = []
        for m in range(0, 116):
            t = T0 + m * MIN
            if t < marks["load_start"]:
                rss, sheds = 40.0, 0
            elif t <= marks["load_end"]:
                # 300 MB until the hostile phase (the load's 30th minute),
                # then 600 MB with a 650 MB peak and 1,000 RSS sheds a minute.
                hostile = t >= marks["load_start"] + 30 * MIN
                rss = (650.0 if m == 80 else 600.0) if hostile else 300.0
                sheds = 1000 * max(0, m - 45)
            else:
                rss, sheds = 200.0, 40_000
            rows.append({"t": t, "src": "load", "server": "streams-1",
                         "body": {"boot_id": "b1", "rss_mb": rss, "admit_shed_rss": sheds, "admit_shed": 2 * sheds,
                                  "cgroup_peak_bytes": (700 + min(m, 71)) * 1_048_576}})
            rows.append({"t": t, "src": "store", "server": "streams-1", "body": {"ops": {}}})
        write_jsonl(os.path.join(self.run_dir, "scrape.jsonl"), rows)
        self.manifest = {
            "arm": "layer-b", "verdict": "FAIL",
            "legs": {"memory": "FAIL", "Q0": "FAIL", "load": "PASS"},
            "bars": {
                "load: time scale": {"bar": 1, "pass": True, "value": 1},
                "memory: RSS peak (MB)": {"bar": "< 500.0", "pass": False, "value": 650.0},
                "compliant tenants: refusals": {"bar": 0, "pass": False, "value": {"refused": {"s1": {"typed": {"429:rate_limited": 12}}}}},
            },
            "known_red": {"Q0": "Q0 (RSS after a load stays above the baseline; README Q0)"},
            "facts": {
                "H11t-fresh": {"pairs": 389},
                "driver-pools": {"sockets": 128, "pools": 37, "saturated": {
                    "h1": {"peak_in_use": 128, "peak_waiting": 3629, "sockets": 128, "unsent": 0, "wait_ms_max": 3091, "waits": 3},
                    "s1": {"peak_in_use": 128, "peak_waiting": 32656, "sockets": 128, "unsent": 2, "wait_ms_max": 164292, "waits": 9}}},
            },
        }
        with open(os.path.join(self.run_dir, "layer-b-manifest.json"), "w", encoding="utf-8") as f:
            json.dump(self.manifest, f)

    def summary(self, *args: str) -> tuple:
        out = io.StringIO()
        with redirect_stdout(out):
            code = S.main(["--results", self.results, *args])
        return code, out.getvalue().splitlines()

    def test_it_prints_each_bar_the_rss_summary_the_driver_pools_and_the_verdict_and_nothing_else(self):
        code, lines = self.summary()
        self.assertEqual(code, 1)
        self.assertEqual(lines, [
            "PASS load: time scale: 1 (bar 1)",
            "FAIL memory: RSS peak (MB): 650.0 (bar < 500.0)",
            'FAIL compliant tenants: refusals: {"refused": {"s1": {"typed": {"429:rate_limited": 12}}}} (bar 0)',
            "RSS: peak 650.0 MB; plateau (the load's last 30 min) median 600.0 MB, max 650.0 MB; "
            "RSS sheds during the load 40000 (all admission sheds 80000); container peak 771.0 MB (cgroup); boots 1",
            "FACT driver-pools: 2 of 37 pools saturated (128 sockets each)",
            "  h1: peak waiting 3629, wait max 3091 ms, unsent 0",
            "  s1: peak waiting 32656, wait max 164292 ms, unsent 2",
            'layer-b: FAIL {"Q0": "FAIL", "load": "PASS", "memory": "FAIL"} (known red: Q0)',
        ])

    def test_a_pass_exits_0_and_a_long_value_is_cut_at_300_characters(self):
        self.manifest.update(verdict="PASS", legs={"load": "PASS"}, known_red={})
        self.manifest["bars"] = {"probes: x": {"bar": 0, "pass": True, "value": "y" * 400}}
        with open(os.path.join(self.run_dir, "layer-b-manifest.json"), "w", encoding="utf-8") as f:
            json.dump(self.manifest, f)
        code, lines = self.summary()
        self.assertEqual(code, 0)
        self.assertEqual(lines[0], "PASS probes: x: " + json.dumps("y" * 400)[:300] + " (bar 0)")
        self.assertEqual(lines[-1], 'layer-b: PASS {"load": "PASS"}')

    def test_no_verdict_prints_the_rig_logs_tail_and_the_file_it_names(self):
        os.remove(os.path.join(self.run_dir, "layer-b-manifest.json"))
        build = os.path.join(self.run_dir, "docker-build.log")
        with open(build, "w", encoding="utf-8") as f:
            f.write("".join(f"build {i}\n" for i in range(100)))
        log = os.path.join(self.results, "rig.log")
        with open(log, "w", encoding="utf-8") as f:
            f.write("".join(f"line {i}\n" for i in range(100)))
            f.write(f"run-shared: docker build failed (see {build})\n")
        code, lines = self.summary("--rig-log", log)
        self.assertEqual(code, 2)
        self.assertEqual(lines[0], f"layer-b: NO VERDICT ({self.run_dir} has no layer-b-manifest.json)")
        self.assertEqual(lines[1], f"--- the last 40 lines of {log}")
        self.assertEqual(lines[2:42], [f"line {i}" for i in range(61, 100)] + [f"run-shared: docker build failed (see {build})"])
        self.assertEqual(lines[42], f"--- the last 40 lines of {build}")
        self.assertEqual(lines[43:], [f"build {i}" for i in range(60, 100)])

    def test_a_named_file_outside_the_results_is_never_printed(self):
        os.remove(os.path.join(self.run_dir, "layer-b-manifest.json"))
        log = os.path.join(self.results, "rig.log")
        with open(log, "w", encoding="utf-8") as f:
            f.write("run-shared: no token map (see /etc/hosts)\n")
        code, lines = self.summary("--rig-log", log)
        self.assertEqual((code, lines[1:]), (2, [f"--- the last 40 lines of {log}", "run-shared: no token map (see /etc/hosts)"]))

    def test_no_run_directory_is_no_verdict(self):
        shutil.rmtree(self.run_dir)
        code, lines = self.summary()
        self.assertEqual((code, lines), (2, [f"layer-b: NO VERDICT (no layer-b-* run under {self.results})"]))

    def test_two_runs_are_refused(self):
        os.makedirs(os.path.join(self.results, "layer-b-20261020T140000Z"))
        code, lines = self.summary()
        self.assertEqual(code, 2)
        self.assertEqual(lines, [f"layer-b: NO VERDICT (2 layer-b-* runs under {self.results}, expected one)"])


if __name__ == "__main__":
    unittest.main()
