"""packing.py on a synthetic tenants-mode run (run-local.sh's layout, as
test_counts.py builds it, plus the tenants run's files): the instance's
month by term at the owner's prices from the `period steady` window, the
two price cases, a tenant's K2 bill and ceiling, the cell's limits and the
validity checks; and the curve through several steps (its line, the N at
which each limit binds, the cost per tenant there).

    python3 -m unittest discover -s bench/k2cost -p 'test_*.py'
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from test_counts import snap, write_run  # noqa: E402

ENV = {"SSE_MAX_CONNECTIONS": "1200", "ADMIT_RSS_SHED_MB": "500", "CELL_ENVELOPE_REQUESTS_PER_SEC": "1411",
       "CELL_ENVELOPE_APPEND_BYTES_PER_SEC": "2060000", "TOKIO_WORKERS": "2"}


def jsonl_write(path: str, rows: list, mode: str = "w") -> None:
    with open(path, mode) as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def pack_run(root: str, name: str, tenants: int = 10, refused: int = 0, audit_ok: bool = True) -> str:
    """A tenants-mode run of `tenants` S projects whose period `steady`
    (10-60 s of the manifest) is the phase-2-start .. phase-3-end span of
    test_counts' ledgers: 50 s, 28 Class A, 20 Class B, 800 B PUT, 2.25 CPU s."""
    run = write_run(root, name, ENV)
    os.makedirs(os.path.join(run, "tenants"))
    edges = {"period-steady-start": "phase-2-start", "period-steady-end": "phase-3-end"}
    rows = []
    for label, like in edges.items():
        s = snap(like)
        rows.append({"t_ms": s["t_ms"], "label": label, "s3lite": s["s3lite"], "servers": s["servers"],
                     "procs": s["procs"]})
    jsonl_write(os.path.join(run, "boundaries.jsonl"), rows, "a")
    jsonl_write(os.path.join(run, "marks.jsonl"), [{"t": 11_000, "mark": "period_start", "name": "steady"},
                                                    {"t": 61_000, "mark": "period_end", "name": "steady"}], "a")
    with open(os.path.join(run, "tenants-manifest.json"), "w") as f:
        json.dump({"name": f"pack-s-{tenants:04d}", "duration": 70, "periods": {"steady": [10, 60]},
                   "classes": [{"class": "s", "count": tenants, "role": "compliant"}]}, f)
    with open(os.path.join(run, "tenants", "run.json"), "w") as f:
        json.dump({"projects": tenants, "time_scale": 1}, f)
    # 10 s windows ending at 10..70 s: the five whose midpoints lie in [10, 60) count.
    wins = [{"t": 0, "elapsed_s": e, "window_s": 10, "class": "s", "payload_bytes": 1000, "delivered_payload_bytes": 3000,
             "wire_bytes_down": 4000, "requests": 50, "acked_requests": 20, "status_counts": {"200": 50},
             "append_p50_ms": 3, "connected": 0, "inflight": 2} for e in range(10, 80, 10)]
    jsonl_write(os.path.join(run, "tenants", "windows.jsonl"), wins)
    with open(os.path.join(run, "tenants", "classes.json"), "w") as f:
        json.dump({"s": {"refusals": {"typed": {"429:project_rate_limit": refused} if refused else {}, "untyped": {},
                                      "transport": 0}}}, f)
    with open(os.path.join(run, "tenants", "usage.json"), "w") as f:
        json.dump({"projects": {f"p{i}": {"settled": True, "meters": {"ingestPayloadBytes": 600}}
                                for i in range(tenants)}}, f)
    with open(os.path.join(run, "tenants", "audit.json"), "w") as f:
        json.dump({"projects": {"p0": {"prefixes": {"s/": {"ok": audit_ok}}}}}, f)
    scrape = [
        {"t": 11_000, "src": "load", "body": {"rss_mb": 200, "admit_shed": 4, "admit_shed_rss": 0, "sse_connections": 0,
                                              "inflight_now": 3}},
        {"t": 31_000, "src": "operator", "body": {"local": {"parked": 240}}},
        {"t": 41_000, "src": "operator", "body": {"local": {"parked": 250}}},
        {"t": 51_000, "src": "load", "body": {"rss_mb": 300, "admit_shed": 4, "admit_shed_rss": 0, "sse_connections": 0,
                                              "inflight_now": 9}},
        {"t": 11_000, "src": "s3lite", "body": {"live_bytes": {"total": {"bytes": 1_000_000}}}},
        {"t": 61_000, "src": "s3lite", "body": {"live_bytes": {"total": {"bytes": 1_500_000}}}},
        # outside the window: never read
        {"t": 100_000, "src": "load", "body": {"rss_mb": 900, "admit_shed": 99}},
        {"t": 100_000, "src": "operator", "body": {"local": {"parked": 1199}}},
    ]
    jsonl_write(os.path.join(run, "scrape.jsonl"), scrape)
    return run


class Price(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="packing-")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_the_instance_month_per_tenant_and_the_k2_bill(self):
        import packing
        s = packing.price(pack_run(self.tmp, "s10"))
        self.assertEqual((s["window"], s["secs"], s["tenants"], s["class"], s["notes"]), ("period steady", 50.0, 10, "s", []))
        scale = 30 * 86400 / 50
        t = s["terms_month"]
        self.assertAlmostEqual(t["class_a"], 28 * scale * 3.75e-6)
        self.assertAlmostEqual(t["class_b"], 20 * scale * 0.375e-6)
        self.assertAlmostEqual(t["transfer"], 800 * scale / 1e9 * 0.01)
        self.assertAlmostEqual(t["storage"], 10_000 * 30 * 86400 / 2 / 2 ** 30 * 0.015)  # 500,000 B in 50 s
        self.assertAlmostEqual(t["cpu"], 2.25 * scale / 3600 * 0.064)
        self.assertAlmostEqual(t["memory"], 720 * 0.006)
        self.assertAlmostEqual(t["egress"], 5 * 4000 * scale / 1e9 * 0.005)  # the wire bytes the tenants were sent
        owner = sum(t.values())
        self.assertAlmostEqual(s["cases"]["owner"]["instance_month"], owner)
        self.assertAlmostEqual(s["cases"]["ab50"]["instance_month"], owner - (t["class_a"] + t["class_b"]) / 2)
        self.assertAlmostEqual(s["cases"]["owner"]["per_tenant_month"], owner / 10)
        k = s["k2"]
        produced, consumed = 5 * 1000 * scale / 10 / 1e9, 5 * 3000 * scale / 10 / 1e9
        self.assertAlmostEqual(k["produced_gb"], produced)
        self.assertAlmostEqual(k["consumed_gb"], consumed)
        self.assertAlmostEqual(k["retained_gb_month"], produced * 7 / 30)  # S keeps 7 days
        self.assertAlmostEqual(k["usd"], (produced + consumed) * 0.04 + produced * 7 / 30 * 0.02)
        self.assertAlmostEqual(k["ceiling_usd"], 0.2 * k["usd"])
        self.assertAlmostEqual(s["cases"]["owner"]["per_tenant_over_ceiling"], owner / 10 / k["ceiling_usd"])
        self.assertEqual(s["driver"]["windows"], 5)
        self.assertEqual((s["driver"]["requests"], s["driver"]["payload_bytes"]), (250, 5000))
        self.assertAlmostEqual(s["rates_per_s"]["pump_fill"], 25 / 50 / 9.9)
        self.assertEqual(s["meters"], {"projects": 10, "settled": 10, "ingestPayloadBytes": 6000, "readPayloadBytes": 0,
                                       "ingestRecords": 0, "readRecords": 0})

    def test_the_limits_come_from_the_window_and_the_server_environment(self):
        import packing
        s = packing.price(pack_run(self.tmp, "s10"), cpu_factor=2.0)
        lim = {k: (v["value"], v["limit"]) for k, v in s["limits"].items()}
        self.assertEqual(lim["projects"], (10, 1000))
        self.assertEqual(lim["parked_waits"], (250, 1200))
        self.assertEqual(lim["rss_mb"], (300, 500))
        self.assertAlmostEqual(lim["vcpu"][0], 2 * 2.25 / 50)  # the field factor scales the vCPU and its price
        self.assertAlmostEqual(s["limits"]["vcpu"]["mac_vcpu"], 2.25 / 50)
        self.assertAlmostEqual(lim["requests_per_s"][0], 5.0)
        self.assertAlmostEqual(lim["append_bytes_per_s"][0], 100.0)
        self.assertAlmostEqual(s["limits"]["rss_mb"]["use"], 0.6)
        self.assertEqual(s["gauges"]["admit_shed"], 0)  # a shed counter that did not move in the window
        self.assertTrue(s["validity"]["valid"], s["validity"])

    def test_a_refusal_a_failed_audit_or_a_missing_window_voids_the_step(self):
        import packing
        s = packing.price(pack_run(self.tmp, "refused", refused=3))
        failed = [c["check"] for c in s["validity"]["checks"] if not c["ok"]]
        self.assertEqual(failed, ["no refusal of a compliant tenant"])
        s = packing.price(pack_run(self.tmp, "leak", audit_ok=False))
        self.assertEqual([c["check"] for c in s["validity"]["checks"] if not c["ok"]],
                         ["every producing project's audit passes"])
        s = packing.price(pack_run(self.tmp, "nowin"), window="period noisy")
        self.assertEqual((s["window"], s["validity"]["valid"]), ("load", False))
        self.assertIn("no counts window 'period noisy'", s["notes"][0])

    def test_the_command_writes_packing_json(self):
        run = pack_run(self.tmp, "cmd")
        r = subprocess.run([sys.executable, os.path.join(HERE, "packing.py"), "price", run], capture_output=True,
                           text=True, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        with open(os.path.join(run, "packing.json")) as f:
            self.assertEqual(json.load(f)["tenants"], 10)
        self.assertIn("(10 tenants of class s), window period steady 50 s: VALID", r.stdout)
        r = subprocess.run([sys.executable, os.path.join(HERE, "packing.py"), "curve", run, run],
                           capture_output=True, text=True, timeout=60)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("== packing curve, class s", r.stdout)


class Curve(unittest.TestCase):
    @staticmethod
    def step(n: int) -> dict:
        lim = {"projects": n / 1000, "parked_waits": 25 * n / 1200, "vcpu": 0.01 * n, "rss_mb": None}
        inst = 20.0 + 1.0 * n
        return {"point": f"PACK-S-{n:04d}", "tenants": n, "class": "s", "validity": {"valid": True},
                "cases": {"owner": {"instance_month": inst, "per_tenant_month": inst / n, "per_tenant_over_ceiling": 1},
                          "ab50": {"instance_month": inst - 5, "per_tenant_month": (inst - 5) / n,
                                   "per_tenant_over_ceiling": 1}},
                "k2": {"usd": 0.5, "ceiling_usd": 0.1},
                "limits": {k: {"use": v, "value": None, "limit": 1} for k, v in lim.items()}}

    def test_the_line_the_first_limit_to_bind_and_the_cost_per_tenant_there(self):
        import packing
        cv = packing.curve([self.step(40), self.step(10), self.step(20)])
        self.assertEqual([s["tenants"] for s in cv["steps"]], [10, 20, 40])
        self.assertAlmostEqual(cv["fit"]["owner"]["fixed_month"], 20.0)
        self.assertAlmostEqual(cv["fit"]["owner"]["per_added_tenant_month"], 1.0)
        self.assertAlmostEqual(cv["binds"]["parked_waits"], 48.0)
        self.assertAlmostEqual(cv["binds"]["projects"], 1000.0)
        self.assertAlmostEqual(cv["binds"]["vcpu"], 100.0)
        self.assertIsNone(cv["binds"]["rss_mb"])
        self.assertEqual(cv["n_max"]["limit"], "parked_waits")
        # 48 is past the last step: the last segment's slope carries the month there.
        self.assertAlmostEqual(cv["at_n_max"]["owner"]["instance_month"], 68.0)
        self.assertAlmostEqual(cv["at_n_max"]["owner"]["per_tenant_month"], 68.0 / 48)
        self.assertAlmostEqual(cv["at_n_max"]["ab50"]["per_tenant_month"], 63.0 / 48)

    def test_interpolation_between_and_beyond_the_steps(self):
        import packing
        xs, ys = [10, 20, 40], [30.0, 40.0, 50.0]
        self.assertAlmostEqual(packing.at(xs, ys, 15), 35.0)
        self.assertAlmostEqual(packing.at(xs, ys, 30), 45.0)
        self.assertAlmostEqual(packing.at(xs, ys, 60), 60.0)
        self.assertAlmostEqual(packing.at(xs, ys, 0), 20.0)
        self.assertIsNone(packing.line([5, 5], [1, 2]))


if __name__ == "__main__":
    unittest.main()
