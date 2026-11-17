"""bench/ramp: the ramp driver's copy of the official client's refusal rules, and ramp.py's bookkeeping.

Run: python3 -m unittest discover -s bench/ramp -p 'test_*.py'
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, HERE)

import ramp  # noqa: E402
import rig  # noqa: E402

TABLE = os.path.join(ROOT, "sdk", "scripts", "refusal-schedule.json")
PATCH = os.path.join(HERE, "population-mode.patch")

# Feeds every row to the driver's rules (population_rules.rs) and prints the waits it makes when
# every attempt gets that answer, as the SDK's refusal-schedule.test.mjs measures the SDK's.
HARNESS = r'''
#[path = "population_rules.rs"]
mod rules;
use std::io::BufRead;
fn opt(s: &str) -> Option<&str> { if s == "<none>" { None } else { Some(s) } }
fn main() {
    for line in std::io::stdin().lock().lines() {
        let line = line.unwrap();
        let f: Vec<&str> = line.split('\t').collect();
        let kind = if f[0] == "append" { rules::Kind::Append } else { rules::Kind::Read };
        let stated = match f[3] { "true" => Some(true), "false" => Some(false), _ => None };
        let answer = rules::Answer { status: f[1].parse().unwrap(), code: opt(f[2]), stated, retry_after: opt(f[4]) };
        let (mut attempt, mut elapsed, mut waits) = (0u32, 0u64, Vec::new());
        while let Some(wait) = rules::again_after(kind, &answer, attempt, elapsed) {
            waits.push(wait.to_string());
            elapsed += wait;
            attempt += 1;
            if waits.len() > 100 { break; }
        }
        println!("{}", waits.join(","));
    }
}
'''


def write(path, text):
    with open(path, "w") as f:
        f.write(text)


def read(path):
    with open(path) as f:
        return f.read()


def patch_file(path):
    """A file the population patch adds, as the patch has it."""
    with open(PATCH) as f:
        lines, inside, body = f.read().split("\n"), False, []
    for line in lines:
        if line.startswith("diff --git "):
            if inside:
                break
            inside = line == f"diff --git a/{path} b/{path}"
            continue
        if inside and line.startswith("+") and not line.startswith("+++"):
            body.append(line[1:])
    if not body:
        raise AssertionError(f"{path} is not in population-mode.patch")
    return "\n".join(body) + "\n"


def answer_fields(row):
    """What population_mode.rs's error_fields and wrong_cell read from a table row's answer."""
    code, stated = None, None
    try:
        error = json.loads(row["body"]).get("error") if row["body"] else None
    except ValueError:
        error = None
    if isinstance(error, dict):
        code = error.get("code") if isinstance(error.get("code"), str) else None
        stated = error.get("retryable") if isinstance(error.get("retryable"), bool) else None
    headers = {k.lower(): v for k, v in row["headers"].items()}
    if "wrong_cell" in (headers.get("streams-error-code"), headers.get("prisma-error-code")):
        code = "wrong_cell"
    return code, stated, headers.get("retry-after")


@unittest.skipUnless(shutil.which("rustc"), "needs rustc")
class DriverFollowsTheSdk(unittest.TestCase):
    """The gate decision's bar, point 6: the driver's copy of the SDK's rules is pinned by the table
    the SDK's own test pins (sdk/scripts/refusal-schedule.json)."""

    def test_every_row_of_the_sdks_table(self):
        rows = json.loads(read(TABLE))["rows"]
        with tempfile.TemporaryDirectory() as d:
            write(os.path.join(d, "population_rules.rs"), patch_file("bench/population_rules.rs"))
            write(os.path.join(d, "main.rs"), HARNESS)
            exe = os.path.join(d, "rules")
            subprocess.run(["rustc", "--edition", "2024", "-A", "dead_code", "-o", exe, "main.rs"], cwd=d, check=True,
                           capture_output=True)
            lines = []
            for row in rows:
                code, stated, retry_after = answer_fields(row)
                text = {True: "true", False: "false", None: "<none>"}[stated]
                lines.append("\t".join([row["kind"], str(row["status"]), code or "<none>", text, retry_after or "<none>"]))
            out = subprocess.run([exe], input="\n".join(lines) + "\n", text=True, capture_output=True, check=True).stdout
        got = [[int(w) for w in ln.split(",") if w] for ln in out.split("\n")[:len(rows)]]
        for row, waits in zip(rows, got):
            with self.subTest(row["name"]):
                self.assertEqual(waits, row["waits"])

    def test_the_driver_includes_the_rules_and_times_a_touch_from_its_first_attempt(self):
        mode = patch_file("bench/population_mode.rs")
        self.assertIn('#[path = "population_rules.rs"]\nmod rules;', mode)
        self.assertIn("rules::again_after(kind, &refused.as_answer(), attempt, ms_since(started))", mode)
        # The histogram records from before the touch's first attempt, once its last answer is in.
        self.assertIn("let started = Instant::now();\n                let mut tally = Tally::default();\n"
                      "                let touched = state.touch(i, &mut tally).await;", mode)


class Bookkeeping(unittest.TestCase):
    def test_errors_by_code_reads_the_drivers_failures(self):
        errors = {'append 409 Conflict: {"error":{"code":"not_stream_owner","retryable":true}}': 3,
                  'append 503 Service Unavailable: {"error":{"code":"outcome_unknown"}}': 2,
                  "append: error sending request": 1}
        self.assertEqual(ramp.errors_by_code(errors),
                         {"append 409 Conflict not_stream_owner": 3, "append 503 Service Unavailable outcome_unknown": 2,
                          "append": 1})

    def test_first_attempt_figures_over_windows(self):
        chunk = {"first_served_per_sec": 150.0, "first_refusals": {"append 503 shard_splitting": 600}, "retries": 700}
        window = {"first_served_per_sec": 166.0, "first_refusals": {"append 503 shard_splitting": 10,
                                                                      "read 429 rate_limited": 2}, "retries": 14}
        got = ramp.first_attempt([(chunk, 60), (window, 180)], 10000 / 60)
        self.assertEqual(got, {"held": round((150 * 60 + 166 * 180) / (10000 / 60 * 240), 4),
                               "refusals": {"append 503 shard_splitting": 610, "read 429 rate_limited": 2},
                               "retries": 714})
        self.assertEqual(ramp.first_attempt([], 1.0)["held"], None)

    def test_a_failed_windows_seconds_are_kept_and_a_held_ones_dropped(self):
        with tempfile.TemporaryDirectory() as d:
            failed = {"per_second": {"1794839722": {"served": 120, "refused append 503 shard_splitting": 40}}}
            held = {"per_second": {"1794839800": {"served": 167}}}
            ramp.keep_seconds(d, "10000-settle-3", failed, False)
            ramp.keep_seconds(d, "10000-window", held, True)
            self.assertEqual(os.listdir(d), ["seconds-10000-settle-3.json"])
            self.assertEqual(json.loads(read(os.path.join(d, "seconds-10000-settle-3.json"))),
                             {"1794839722": {"served": 120, "refused append 503 shard_splitting": 40}})
            self.assertNotIn("per_second", failed)
            self.assertNotIn("per_second", held)

    def test_sheds_by_kind_are_summed_over_servers(self):
        first = {"streams-1": {"inflight": 0, "rss": 2, "survival": 0}}
        last = {"streams-1": {"inflight": 618, "rss": 2, "survival": 0},
                "streams-2": {"inflight": 3, "rss": None, "survival": 1}}
        self.assertEqual(ramp.sheds_by(first, last), {"inflight": 621, "rss": 0, "survival": 1})

    def test_a_failed_step_keeps_the_host_store(self):
        class Proc:
            pid = 4242

        class Store:
            proc, stopped = Proc(), False

            def stop(self):
                self.stopped = True

        with tempfile.TemporaryDirectory() as d:
            store = Store()
            ramp.STORE[:], ramp.FAILED[:] = [store], [10000]
            try:
                ramp.keep_store(d)
                self.assertFalse(store.stopped)
                self.assertEqual(read(os.path.join(d, "host-store.pid")), "4242\n")
                self.assertEqual(ramp.STORE, [])
                store = Store()
                ramp.STORE[:], ramp.FAILED[:] = [store], []
                ramp.keep_store(d)
                self.assertTrue(store.stopped)
            finally:
                ramp.STORE[:], ramp.FAILED[:] = [], []

    def test_settle_restarts_its_clock_when_a_shard_changes_owner(self):
        # Gate decision meas149-rerun-10000 (g), F7: two shards on streams-1 (2/0), then one moves to
        # streams-2 (1/1) at the same shard count and topology version; settle's clock restarts there.
        def view(t, one, two):
            beats = [{"instance": "streams-1", "ts_ms": t * 1000, "owned_shards": one},
                     {"instance": "streams-2", "ts_ms": t * 1000, "owned_shards": two}]
            row = ramp.topology_view({"fleet": {"heartbeats": beats}, "topology": {"version": 7}}, now_ms=t * 1000)
            row.pop("owned")
            row["t"] = t
            return row

        topo = [view(100, ["a", "b"], []), view(105, ["a", "b"], []), view(110, ["a"], ["b"]),
                view(115, ["a"], ["b"])]
        self.assertEqual([(v["shards"], v["topology_version"]) for v in topo], [(2, 7)] * 4)
        self.assertEqual(ramp.last_change(topo, [(50, 2)], 90), 110)
        # A split still restarts it, and a cell that did not reshape keeps the later of start and resize.
        self.assertEqual(ramp.last_change(topo[:2] + [dict(topo[1], t=120, shards=3)], [], 90), 120)
        self.assertEqual(ramp.last_change(topo[:2], [(95, 2)], 90), 95)

    def test_the_settle_budget_counts_the_leaves_the_creation_burst_forces(self):
        # The 10,000 rerun: a 60 s period and a 10 s idle-out leave 1,667 resident in steady state,
        # but the creation chunk left all 10,003 resident and forced one split (5,734 a leaf).
        class Posture:
            def debug(self, k, path):
                raise OSError("no cell")

        a = type("A", (), {"settle_max_secs": "auto", "period": 60,
                           "set": ["HANDLE_IDLE_EVICT_SECS=10", "SHARD_TOPOLOGY_COOLDOWN_SECS=10"]})()
        self.assertEqual(ramp.settle_budget(a, 10_000, Posture()), 300 + 1 * (10 + 30))
        self.assertEqual(ramp.settle_budget(a, 1_000, Posture()), 300)
        self.assertEqual(ramp.settle_budget(a, 100_000, Posture()), 300 + (32 - 1) * (10 + 30))

    def test_servers_log_topology_and_fleet_at_info(self):
        self.assertEqual(ramp.SERVER_LOG, "warn,streams_slate::topology=info,streams_slate::fleet=info")

    def test_a_worker_covers_two_ridden_out_requests(self):
        # 100,000 streams once a minute: 1,667 touches a second over 32-second workers.
        self.assertEqual(rig.population_workers(100_000, 100_000 / 60), 53_337)
        self.assertEqual(rig.population_workers(10, 10 / 60), 9)


if __name__ == "__main__":
    unittest.main()
