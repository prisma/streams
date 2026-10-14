"""shared_verdict.py on synthetic Layer B runs: a world where every bar
holds certifies, and each mutation fails exactly its own leg. The manifest
is also fed to the landed single-cell gate's own check (rc-certify.sh's
verify_arm, from this tree or from db518611) so its field names and types
are the contract's.

    python3 -m unittest discover -s bench/k2cost -p 'test_*.py'
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, HERE)
import shared_verdict as V  # noqa: E402

SHA = "ab" * 32
T0 = 1_791_000_000_000  # ms
MIN = 60_000


def head() -> str:
    return subprocess.run(["git", "-C", REPO, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()


def shipped_layer_b() -> str:
    """sha256 of HEAD's tenants/layer-b.json, as tenants.ts records it."""
    blob = subprocess.run(["git", "-C", REPO, "show", "HEAD:bench/k2cost/tenants/layer-b.json"], capture_output=True).stdout
    return hashlib.sha256(blob).hexdigest()


class World:
    """A complete Layer B run of 1,000 projects in which every bar holds."""

    def __init__(self):
        self.dir = tempfile.mkdtemp(prefix="shared-verdict-")
        prof = os.path.join(self.dir, "profiles")
        os.makedirs(prof)
        with open(os.path.join(prof, "compute-1g.env"), "w") as f:
            f.write("ADMIT_RSS_SHED_MB=500\n")
        with open(os.path.join(prof, "shared-cell.env"), "w") as f:
            f.write("STREAMS_RELEASE_POSTURE=1\nINITIAL_SHARDS=1\nPROJECT_SHARE_K=8\nKEEP_AWAKE=1\n")
        self.rig = {
            "arm": "layer-b", "rehearsal": False, "commit": head(), "region": "local",
            "binary": {"path": "/r/streams-slate", "sha256": SHA}, "wrapper_sha256": SHA, "debug_load_sha256": SHA,
            "profiles": {n: {"path": os.path.join(prof, n), "sha256": n[:2] * 32} for n in ("compute-1g.env", "shared-cell.env")},
            "cell_env": {"path": "/r/cells/scratch.env", "sha256": "ce" * 32},
            "checkout": {"head": head(), "trees": V.tree_oids(REPO, head(), V.CHECKOUT_TREES), "dirty": []},
            "started_unix": T0 // 1000, "finished_unix": T0 // 1000 + 4 * 3600,
        }
        self.marks = {"idle_start": T0, "load_start": T0 + 15 * MIN, "load_end": T0 + 85 * MIN, "q0_start": T0 + 100 * MIN}
        # RSS: 300 MB at the idle baseline, 400 MB flat through the load,
        # back to 310 MB in the Q0 window; one sample a minute.
        self.loads = []
        for m in range(0, 120):
            t = T0 + m * MIN
            rss = 300.0 if t < self.marks["load_start"] else 400.0 if t <= self.marks["load_end"] else 310.0
            self.loads.append({"t": t, "src": "load", "server": "streams-1", "body": {"boot_id": "b1", "rss_mb": rss, "admit_shed_rss": 0}})
        self.classes = {
            "app": {"role": "compliant", "count": 990, "totals": {"requests": 10_000}, "refusals": {"typed": {}, "untyped": {}, "transport": 0}},
            "victim": {"role": "victim", "count": 1, "totals": {"requests": 100}, "refusals": {"typed": {}, "untyped": {}, "transport": 0},
                       "periods": {"solo": {"append_p99_ms": 8.0, "read_p99_ms": 9.0}, "noisy": {"append_p99_ms": 20.0, "read_p99_ms": 24.0}}},
            "h1": {"role": "hostile", "count": 7, "totals": {"requests": 100_000},
                   "refusals": {"typed": {"429:project_rate_limit": 50_000}, "untyped": {"503": 10}, "transport": 0}},
            "h9": {"role": "hostile", "count": 1, "totals": {"requests": 10}, "refusals": {"typed": {}, "untyped": {}, "transport": 0}},
            "h9b": {"role": "hostile", "count": 1, "totals": {"requests": 10}, "refusals": {"typed": {}, "untyped": {}, "transport": 0}},
        }
        self.projects = {}
        for i in range(1000):
            cls = "victim" if i == 0 else "h9" if i == 1 else "h9b" if i == 2 else "h1" if i < 10 else "app"
            self.projects[f"proj_{i:04d}"] = cls
        self.ledgers = {}
        self.usage = {}
        self.audit = {}
        for p, cls in self.projects.items():
            produces = cls in ("app", "victim") and int(p[-4:]) % 10 == 0
            phases = [{"i": 0, "mode": "produce"}] if produces else []
            if cls in ("h9", "h9b"):
                phases = [{"i": 0, "mode": cls}]
            acked = 100 if produces else 0
            self.ledgers[p] = {"project": p, "class": cls, "phases": phases,
                               "totals": {"acked_records": acked, "delivered_payload_bytes": acked * 50, "delivered_records": acked,
                                          "oracle_ok": acked, "oracle_foreign": 0, "oracle_bad_mac": 0, "oracle_unmarked": 0}}
            self.usage[p] = {"settled": True, "meters": {"ingestRecords": acked, "readPayloadBytes": acked * 50, "readRecords": acked}}
            if produces:
                self.audit[p] = {"prefixes": {"t/": {"ok": True, "oracle": {"ok": acked, "foreign": 0, "bad_mac": 0, "unmarked": 0}}}}
        self.phase_ledgers = {
            "proj_0001": {"mode": "h9", "start_ms": T0 + 40 * MIN, "end_ms": T0 + 50 * MIN, "h9": {"bursts": [{"answers": {"200:-": 64}}] * 5}},
            "proj_0002": {"mode": "h9b", "start_ms": T0 + 50 * MIN, "end_ms": T0 + 55 * MIN, "h9b": {"statuses": {"200": 64}}},
        }
        timings = [0.2 + (i % 50) / 100 for i in range(2000)]
        self.h11t = {"leg": "h11t", "pairs": 2000, "rate": 8, "gap_min_ms": 125.0, "names": {"projects": 600, "names": 1800},
                     "differences": 0, "quiet_ms": timings, "never_ms": list(reversed(timings)), "quiet_p50_ms": 0.45, "never_p50_ms": 0.45}
        # A name used in the last seconds answers from its fresh slot, 20 ms
        # before a never-held one's store fetch: the gap P4 accepted, a fact.
        self.h11t_fresh = {"leg": "h11t-fresh", "pairs": 400, "rate": 8, "target": "h11t-target", "differences": 0,
                           "answers": {"fresh": {"403:watch_unauthorized": 400}, "never": {"403:watch_unauthorized": 400}},
                           "fresh_ms": timings[:400], "never_ms": [x + 20 for x in timings[:400]], "fresh_p50_ms": 0.45, "never_p50_ms": 20.45}
        self.run = {"projects": 1000, "manifest": "layer-b", "manifest_sha256": shipped_layer_b(), "time_scale": 1,
                    "classes": {c: {"count": v["count"], "role": v["role"], "quotas": "q-hostile" if c == "h1" else "ceilings"}
                                for c, v in self.classes.items()},
                    "loop_lag": {"run": {"lag_p99_ms": 1.5}, "noisy": {"lag_p99_ms": 2.0}}}
        self.probes = {"leg": "probes", "pairs": 2000, "first_contacts": 2000, "differences": {"name": 0, "usage": 0}, "put": {"201": 2000}}

    def write(self) -> str:
        d = self.dir
        t = os.path.join(d, "tenants")
        shutil.rmtree(t, ignore_errors=True)
        os.makedirs(os.path.join(t, "legs"))

        def dump(path, doc):
            with open(path, "w") as f:
                f.write(json.dumps(doc))
        dump(os.path.join(d, "rig.json"), self.rig)
        with open(os.path.join(d, "marks.jsonl"), "w") as f:
            for k, v in self.marks.items():
                f.write(json.dumps({"t": v, "mark": k}) + "\n")
        with open(os.path.join(d, "scrape.jsonl"), "w") as f:
            for rec in self.loads:
                f.write(json.dumps(rec) + "\n")
        dump(os.path.join(t, "run.json"), {**self.run, "projects": len(self.projects)})
        dump(os.path.join(t, "classes.json"), self.classes)
        dump(os.path.join(t, "usage.json"), {"projects": self.usage})
        dump(os.path.join(t, "audit.json"), {"projects": self.audit})
        for p, lg in self.ledgers.items():
            os.makedirs(os.path.join(t, "projects", p))
            dump(os.path.join(t, "projects", p, "ledger.json"), lg)
            if p in self.phase_ledgers:
                dump(os.path.join(t, "projects", p, "phase-0.ledger.json"), self.phase_ledgers[p])
        if self.h11t is not None:
            dump(os.path.join(t, "legs", "h11t-0.json"), self.h11t)
        dump(os.path.join(t, "legs", "probes-1.json"), self.probes)
        if self.h11t_fresh is not None:
            dump(os.path.join(t, "legs", "h11t-fresh-2.json"), self.h11t_fresh)
        return d

    def judge(self) -> dict:
        return V.layer_b(self.write(), REPO)


LEGS = ("load", "H9", "H9b", "H11t", "leaks", "probes", "compliant-refusals", "hostile-typed", "victim-p99", "billing", "memory", "Q0",
        "identity", "harness", "release-binary")


class LayerB(unittest.TestCase):
    def setUp(self):
        self.w = World()

    def tearDown(self):
        shutil.rmtree(self.w.dir, ignore_errors=True)

    def failing(self, m: dict) -> list:
        return sorted(k for k, v in m["legs"].items() if v != "PASS")

    def test_a_world_where_every_bar_holds_certifies(self):
        m = self.w.judge()
        self.assertEqual(sorted(m["legs"]), sorted(LEGS))
        self.assertEqual(self.failing(m), [], json.dumps({k: v for k, v in m["bars"].items() if not v["pass"]}, indent=1))
        self.assertEqual((m["arm"], m["verdict"], m["server_sha256"], m["projects"]), ("layer-b", "PASS", f"sha256:{SHA}", 1000))
        self.assertEqual((m["started_unix"], m["finished_unix"] - m["started_unix"]), (T0 // 1000, 4 * 3600))
        self.assertEqual(m["profile"]["pins"], list(V.PROFILE_PINS))
        self.assertEqual(m["profile"]["pins_missing"], [])
        self.assertEqual(m["profile"]["sha256"], {"compute-1g.env": "co" * 32, "shared-cell.env": "sh" * 32})
        self.assertEqual(m["cell_env_sha256"], "ce" * 32)
        tree = lambda path: subprocess.run(["git", "-C", REPO, "rev-parse", f"HEAD:{path}"],  # noqa: E731
                                           capture_output=True, text=True).stdout.strip()
        self.assertEqual(m["harness_trees"], {p: tree(p) for p in ("bench/k2cost", "deploy/composer", "deploy/app-server")})
        self.assertEqual(m["known_red"], {})
        self.assertEqual(m["load"], {"manifest": "layer-b", "manifest_sha256": shipped_layer_b(), "time_scale": 1, "quota_posture": {
            "ceilings": {"compliant": ["app"], "hostile": ["h9", "h9b"], "victim": ["victim"]}, "q-hostile": {"hostile": ["h1"]}}})
        self.assertEqual(m["bars"]["load: the run's manifest is the commit's bench/k2cost/tenants/layer-b.json"]["pass"], True)
        self.assertIn("tenants/legs/h11t-0.json", m["evidence"])
        self.assertIn("tenants/legs/h11t-fresh-2.json", m["evidence"])
        self.assertEqual(m["bars"]["victim: append p99 under noise"],
                         {"value": {"noisy": 20.0, "solo": 8.0, "loop_lag_p99_ms": 2.0, "harness_bound": False}, "bar": 25.0, "pass": True})

    def test_a_victim_p99_measured_under_a_lagging_driver_is_flagged_harness_bound(self):
        # The driver's loop lag p99 in the noisy period, 9 ms: 25% or more of the victim's 30 ms read p99.
        self.w.run["loop_lag"]["noisy"]["lag_p99_ms"] = 9.0
        m = self.mutated(lambda w: w.classes["victim"]["periods"]["noisy"].update(read_p99_ms=30.0), "victim-p99")
        self.assertEqual(m["bars"]["victim: read p99 under noise"],
                         {"value": {"noisy": 30.0, "solo": 9.0, "loop_lag_p99_ms": 9.0, "harness_bound": True}, "bar": 25.0, "pass": False})
        # The append p99 (20 ms) passes its bar; 9 ms of lag is a quarter of it too, so it is flagged as well.
        self.assertEqual(m["bars"]["victim: append p99 under noise"]["value"]["harness_bound"], True)
        self.assertEqual(m["bars"]["victim: append p99 under noise"]["pass"], True)

    def test_each_victim_class_has_its_own_p99_bars(self):
        # Layer B's free-tier twin of the victim (lead decision deploy-q5 #6
        # (1)) is a second victim class: one failing victim must not be
        # hidden behind the other's passing numbers.
        self.w.classes["victim-free"] = copy.deepcopy(self.w.classes["victim"])
        self.w.run["classes"]["victim-free"] = {"count": 1, "role": "victim", "quotas": "free-tier"}
        m = self.mutated(lambda w: w.classes["victim"]["periods"]["noisy"].update(read_p99_ms=30.0), "victim-p99")
        bars = {k: (v["value"]["noisy"], v["pass"]) for k, v in m["bars"].items() if "p99 under noise" in k}
        self.assertEqual(bars, {
            "victim: append p99 under noise": (20.0, True), "victim: read p99 under noise": (30.0, False),
            "victim-free: append p99 under noise": (20.0, True), "victim-free: read p99 under noise": (24.0, True)})
        self.assertEqual(m["load"]["quota_posture"]["free-tier"], {"victim": ["victim-free"]})

    def mutated(self, edit, *want):
        edit(self.w)
        m = self.w.judge()
        self.assertEqual((self.failing(m), m["verdict"]), (sorted(want), "FAIL"))
        return m

    def test_each_mutation_fails_exactly_its_own_leg(self):
        cases = {
            "a foreign record on a read": (lambda w: w.ledgers["proj_0500"]["totals"].update(oracle_foreign=1), "leaks"),
            "a failed audit": (lambda w: w.audit["proj_0010"]["prefixes"]["t/"].update(ok=False), "leaks"),
            "an unaudited producer": (lambda w: w.audit.pop("proj_0020"), "leaks"),
            "a probe difference": (lambda w: w.probes["differences"].update(name=1), "probes"),
            "1,999 probe pairs": (lambda w: w.probes.update(pairs=1999, first_contacts=1999), "probes"),
            "2,000 pairs but 140 first contacts": (lambda w: w.probes.update(first_contacts=140), "probes"),
            "probes without a first-contact count": (lambda w: w.probes.pop("first_contacts"), "probes"),
            "a probe create that was not 201": (lambda w: w.probes.update(put={"201": 1999, "409": 1}), "probes"),
            "1,999 timed pairs": (lambda w: (w.h11t.update(quiet_ms=w.h11t["quiet_ms"][:1999])), "H11t"),
            "a timed-pair answer that differs": (lambda w: w.h11t.update(differences=1), "H11t"),
            "no H11t leg": (lambda w: setattr(w, "h11t", None), "H11t"),
            # The leg found no quiet name in its target's setup record (hostile.ts): it timed nothing.
            "an H11t leg with no quiet name": (lambda w: setattr(w, "h11t", {"leg": "h11t", "error": "h11t: the setup of class cold "
                                               "recorded no stream it created (600 projects), so there is no quiet name to time"}), "H11t"),
            # A leg of the old shape (a used name's timings) is not a quiet arm.
            "fresh-vs-never timings in H11t's place": (lambda w: w.h11t.update(used_ms=w.h11t.pop("quiet_ms")), "H11t"),
            "a refused compliant tenant": (lambda w: w.classes["app"]["refusals"]["typed"].update({"429:project_rate_limit": 1}), "compliant-refusals"),
            "a victim transport failure": (lambda w: w.classes["victim"]["refusals"].update(transport=1), "compliant-refusals"),
            "hostile untyped answers just over 0.5%": (lambda w: w.classes["h1"]["refusals"]["untyped"].update({"503": 501}), "hostile-typed"),
            "a victim read p99 over 2 x solo and 25 ms": (lambda w: w.classes["victim"]["periods"]["noisy"].update(read_p99_ms=25.5), "victim-p99"),
            "a victim with no solo period": (lambda w: w.classes["victim"]["periods"].pop("solo"), "victim-p99"),
            "a meter one record short": (lambda w: w.usage["proj_0500"]["meters"].update(ingestRecords=99), "billing"),
            "an unsettled meter": (lambda w: w.usage["proj_0700"].update(settled=False), "billing"),
            "a missing project ledger": (lambda w: w.ledgers.pop("proj_0999"), "billing"),
            "an RSS shed in H9's window": (lambda w: [b["body"].update(admit_shed_rss=1) for b in w.loads if b["t"] >= T0 + 45 * MIN], "H9"),
            "an RSS shed in H9b's window only": (lambda w: [b["body"].update(admit_shed_rss=1) for b in w.loads if b["t"] >= T0 + 52 * MIN], "H9b"),
            "no H9 bursts": (lambda w: w.phase_ledgers["proj_0001"]["h9"].update(bursts=[]), "H9"),
            "a 501 MB RSS peak": (lambda w: w.loads[50]["body"].update(rss_mb=501.0), "memory"),
            "RSS climbing 3 MB/min in the load's last 30 min": (
                lambda w: [b["body"].update(rss_mb=400.0 + 3 * max(0, (b["t"] - (T0 + 55 * MIN)) / MIN)) for b in w.loads
                           if T0 + 55 * MIN <= b["t"] <= T0 + 85 * MIN], "memory"),
            "RSS 25 MB over the baseline 10 min after the load": (
                lambda w: [b["body"].update(rss_mb=325.0) for b in w.loads if b["t"] >= T0 + 100 * MIN], "Q0"),
            "a wrapper line naming another binary": (lambda w: w.rig.update(wrapper_sha256="cd" * 32), "identity"),
            "a rehearsal's --binary": (lambda w: w.rig.update(rehearsal=True), "release-binary"),
            "an edited manifest": (lambda w: w.run.update(manifest_sha256="00" * 32), "load"),
            "a harness tree other than the commit's": (lambda w: w.rig["checkout"]["trees"].update({"bench/k2cost": "0" * 40}), "harness"),
            "an uncommitted harness file": (lambda w: w.rig["checkout"].update(dirty=[" M bench/k2cost/tenants.ts"]), "harness"),
            "no checkout record": (lambda w: w.rig.pop("checkout"), "harness"),
            "a run at time scale 0.4": (lambda w: w.run.update(time_scale=0.4), "load"),
            "a run without the manifest's sha256": (lambda w: w.run.pop("manifest_sha256"), "load"),
        }
        pristine = copy.deepcopy(self.w.__dict__)
        for name, (edit, *want) in cases.items():
            with self.subTest(name):
                self.w.__dict__.update(copy.deepcopy(pristine))
                self.mutated(edit, *want)

    def test_quiet_names_apart_from_never_held_ones_fail_h11t_and_are_not_known_red(self):
        # The owner's round-3 call 9 (a): quiet versus never is a bar like
        # any other, no longer red by construction.
        self.w.h11t["never_ms"] = [x + 7 for x in self.w.h11t["quiet_ms"]]
        m = self.mutated(lambda w: None, "H11t")
        self.assertEqual(m["known_red"], {})
        b = m["bars"]["H11t: KS p (quiet vs never)"]
        self.assertLess(b["value"]["p"], 1e-10)
        self.assertEqual(b["pass"], False)
        self.assertEqual(m["bars"]["H11t: timed pairs (quiet vs never)"],
                         {"value": {"pairs": 2000, "rate": 8, "gap_min_ms": 125.0, "names": {"projects": 600, "names": 1800}, "error": None},
                          "bar": 2000, "pass": True})

    def test_the_fresh_gap_is_a_fact_in_the_manifest_never_a_leg(self):
        # The owner's round-3 call 9 (a): a name used in the last 5 s against
        # a never-held one is reported, not judged; 20 ms apart still certifies.
        m = self.w.judge()
        self.assertEqual((m["verdict"], sorted(m["legs"])), ("PASS", sorted(LEGS)))
        f = m["facts"]["H11t-fresh"]
        self.assertLess(f["ks"]["p"], 1e-10)
        self.assertEqual({k: v for k, v in f.items() if k != "ks"}, {
            "pairs": 400, "rate": 8, "target": "h11t-target", "differences": 0, "fresh_p50_ms": 0.45, "never_p50_ms": 20.45,
            "answers": {"fresh": {"403:watch_unauthorized": 400}, "never": {"403:watch_unauthorized": 400}}, "error": None})
        # Without the leg the fact is missing, and still nothing is judged on it.
        self.w.h11t_fresh = None
        m = self.w.judge()
        self.assertEqual((m["verdict"], m["facts"]), ("PASS", {"H11t-fresh": None}))
        self.assertNotIn("tenants/legs/h11t-fresh-2.json", m["evidence"])

    def test_a_known_red_leg_fails_and_is_named_never_masked(self):
        m = self.mutated(lambda w: [b["body"].update(rss_mb=325.0) for b in w.loads if b["t"] >= T0 + 100 * MIN], "Q0")
        self.assertEqual(m["known_red"], {"Q0": V.KNOWN_RED["Q0"]})
        self.assertEqual(V.KNOWN_RED, {"Q0": "Q0 (RSS after a load stays above the baseline; README Q0)"})

    def test_a_compliant_tenants_503_temporarily_unavailable_is_a_refusal_like_any_other(self):
        # The product's generic "could not serve" answer, not a boot-only holdoff: no code is excused.
        m = self.mutated(lambda w: w.classes["app"]["refusals"]["typed"].update({"503:temporarily_unavailable": 3}),
                         "compliant-refusals")
        self.assertEqual(m["bars"]["compliant tenants: refusals"]["value"],
                         {"refused": {"app": {"typed": {"503:temporarily_unavailable": 3}, "untyped": {}, "transport": 0}}})

    def test_missing_pins_and_the_identity_are_reported(self):
        with open(self.w.rig["profiles"]["shared-cell.env"]["path"], "w") as f:
            f.write("STREAMS_RELEASE_POSTURE=1\nINITIAL_SHARDS=1\nPROJECT_SHARE_K=8\n")
        self.w.rig["debug_load_sha256"] = None
        m = self.mutated(lambda w: None, "identity")
        self.assertEqual((m["profile"]["pins_missing"], m["server_sha256"]), (["KEEP_AWAKE=1"], None))

    def test_the_landed_gate_accepts_a_passing_manifest_and_refuses_a_failing_one(self):
        script = gate_check()
        if script is None:
            self.skipTest("no single-cell verify_arm in this tree or in db518611")
        d = self.w.write()
        out = os.path.join(d, "layer-b-manifest.json")
        self.assertEqual(V.main(["layer-b", d, "--out", out, "--repo", REPO]), 0)
        ok = subprocess.run([sys.executable, "-E", "-", "layer-b", out, SHA], input=script, capture_output=True, text=True)
        self.assertEqual((ok.returncode, ok.stdout.strip()), (0, f"layer-b: PASS ({len(LEGS)} legs, 1000 projects, 4.0 h) on {SHA[:16]}"))
        self.w.probes.update(pairs=10, first_contacts=10)
        self.assertEqual(V.main(["layer-b", self.w.write(), "--out", out, "--repo", REPO]), 1)
        bad = subprocess.run([sys.executable, "-E", "-", "layer-b", out, SHA], input=script, capture_output=True, text=True)
        self.assertEqual(bad.returncode, 1)
        self.assertIn("layer-b: verdict 'FAIL'", bad.stderr)


def gate_check() -> str | None:
    """verify_arm's Python, from this tree's rc-certify.sh or from the
    commit that landed it (db518611)."""
    texts = []
    try:
        with open(os.path.join(REPO, "scripts", "rc-certify.sh")) as f:
            texts.append(f.read())
    except OSError:
        pass
    shown = subprocess.run(["git", "-C", REPO, "show", "db518611:scripts/rc-certify.sh"], capture_output=True, text=True)
    if shown.returncode == 0:
        texts.append(shown.stdout)
    for text in texts:
        m = re.search(r"verify_arm\(\) \{.*?<<'PY'\n(.*?)\nPY\n", text, re.S)
        if m:
            return m.group(1)
    return None


class Bars(unittest.TestCase):
    def test_ks_matches_its_typescript_twin(self):
        a = [(i % 97) / 10 for i in range(400)]
        self.assertEqual(V.ks(a, a), (0.0, 1.0))
        d, p = V.ks(a, [x + 3 for x in a])
        self.assertEqual(d, 132 / 400)
        self.assertLess(p, 1e-10)

    def test_the_owner_question_d4_bars_are_constants_at_answer_a(self):
        self.assertEqual((V.SC01S_PROJECTION_DAYS_MIN, V.DAY90_OPEN_SECS_MAX, V.IDLE_RSS_MB_MAX, V.RESTART_WINDOW_SECS_MAX),
                         (30.0, 10.0, 400.0, 10))

    def test_projection_and_extrapolation(self):
        grow = [(h, 300 + (h - 24)) for h in range(0, 73)]
        self.assertAlmostEqual(V.days_to_line(grow), (500 - 348) / 24)
        self.assertEqual(V.days_to_line([(h, 350.0) for h in range(24, 73)]), math.inf)
        self.assertEqual(V.days_to_line([(h, 520.0 + h) for h in range(24, 73)]), 0.0)
        self.assertIsNone(V.days_to_line([(10, 300.0)]))
        self.assertAlmostEqual(V.day90_open([(1, 1.0), (30, 2.0)]), 1.0 + 89 / 29)

    def test_a_field_arm_is_refused_without_writing_a_manifest(self):
        d = tempfile.mkdtemp(prefix="shared-verdict-")
        try:
            self.assertEqual(V.main(["sc0-1s", d]), 2)
            self.assertEqual(os.listdir(d), [])
        finally:
            shutil.rmtree(d)


if __name__ == "__main__":
    unittest.main()
