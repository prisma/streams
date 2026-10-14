"""Tests for the Compute lifecycle checklist (deploy-q5 triage B6).

    python3 -m unittest discover -s bench/lifecycle -v

The verdicts are tested on log lines in the exact shape the code writes
them (the binary's tracing lines with their ANSI escapes, the wrapper's
lines, the probe's); the probe's own output is read once for real (Bun).
The legs run against fake platforms with a fake clock in
test_lifecycle_legs.py, which imports this file's line builders.
"""
from __future__ import annotations

import http.server
import json
import os
import re
import shutil
import signal
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import harness as H  # noqa: E402
import lifecycle as L  # noqa: E402
import verdicts as V  # noqa: E402

E = "\x1b"
T0 = V.iso_ms("2026-10-12T10:00:00Z")


def tool_flags(path: str, also: list | None = None) -> tuple[set, set]:
    """The flags a deploy tool's parseFlags call takes, and those it requires
    (plus `also`, which it requires after parsing)."""
    with open(os.path.join(H.ROOT, path), encoding="utf-8") as f:
        call = re.search(r"parseFlags\(rest, \{(.*?)\}\);", f.read(), re.S).group(1)
    lists = {k: set(re.findall(r'"([a-z0-9-]+)"', v)) for k, v in re.findall(r"(single|bool|required): \[(.*?)\]", call, re.S)}
    return lists.get("single", set()) | lists.get("bool", set()), lists.get("required", set()) | set(also or [])


def iso(ms: int, micros: bool = True) -> str:
    base = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ms // 1000))
    return f"{base}.{ms % 1000:03d}{'123' if micros else ''}Z"


def bline(ms: int, target: str, msg: str, **fields) -> str:
    """A tracing line exactly as the binary writes it (ANSI and all)."""
    f = "".join(f" {E}[3m{k}{E}[0m{E}[2m={E}[0m{v}" for k, v in fields.items())
    return f"{E}[2m{iso(ms)}{E}[0m {E}[32m INFO{E}[0m {E}[2m{target}{E}[0m{E}[2m:{E}[0m {msg}{f}"


def boot_lines(ms: int, boot: str) -> list:
    return [
        'instance shape: {"mem_total_bytes":1073741824}',
        bline(ms, "streams_slate::bootstrap", "runtime identity minted", boot_id=boot, instance="i"),
        bline(ms + 400, "streams_slate::bootstrap", "streams-slate listening on 0.0.0.0:8080"),
        bline(ms + 410, "streams_slate::bootstrap", "opening shard log shards/root (lazy; fences prior owner)"),
    ]


def stop_lines(ms: int, stop_ms: int = 23, code: int = 0) -> list:
    return [
        "wrapper received SIGTERM; forwarding it to the binary",
        bline(ms, "streams_slate::bootstrap", "termination signal: shutting down"),
        bline(ms + 1, "streams_slate::tasks::exits", "supervised loops stopped", finished='["signal"]'),
        bline(ms + stop_ms, "streams_slate::shard::lifecycle", "engine workers and stores terminated", shard=""),
        f"binary exited with code {code} after a forwarded stop; exiting {code}",
    ]


def probe_line(role: str, event: str, ms: int, rest: str = "") -> str:
    return f"sigterm-probe {role} {event} t={iso(ms, micros=False)}{rest}"


AUDIT_OK = {"rc": 0, "verify": {"ok": True, "expected_records": 1200, "blocks_missing": 0, "failed_requests": 0,
                                "unexpected_records": 0, "offset_errors": 0}}


def lines_of(report: V.Report) -> list:
    return report.render()


# ---------------------------------------------------------------- verdicts

class LogLines(unittest.TestCase):
    def test_iso_and_tracing_lines_parse_with_their_escapes(self):
        self.assertEqual(V.iso_ms("2026-10-12T01:52:29.725233Z") - V.iso_ms("2026-10-12T01:52:29Z"), 725)
        self.assertEqual(V.iso_ms("2026-10-12T01:52:29.7Z") - V.iso_ms("2026-10-12T01:52:29Z"), 700)
        self.assertIsNone(V.iso_ms("2026-10-12 01:52:29"))
        got = V.binary_lines(boot_lines(T0, "b1"))
        self.assertEqual(got[0], (T0, "streams_slate::bootstrap", "runtime identity minted boot_id=b1 instance=i"))
        self.assertEqual(len(got), 3)


class D10Probe(unittest.TestCase):
    def ev(self, lived_s: int, end: str = "vm_stopped", group: bool = False, new_nonce: str = '"n2"',
           child_lived_s: int = 0) -> dict:
        boot = ' pid=7 ppid=1 pgid=7 runtime=bun-1.3.13 port=3000 nonce="n1" keep_awake=held pid1="/sbin/init"'
        old = [probe_line("main", "boot", T0, boot), probe_line("child", "boot", T0 + 5, " pid=9 ppid=7 pgid=7"),
               probe_line("main", "SIGTERM", T0 + 60_000, " pid=7")]
        if group or child_lived_s:
            old.append(probe_line("child", "SIGTERM", T0 + 60_001, " pid=9"))
        old += [probe_line("main", "alive", T0 + 60_000 + k * 1000, f" after={k}s") for k in range(1, lived_s + 1)]
        old += [probe_line("child", "alive", T0 + 60_001 + k * 1000, f" after={k}s") for k in range(1, child_lived_s + 1)]
        new = [probe_line("main", "boot", T0 + 62_000, boot.replace('"n1"', new_nonce))]
        return {"read_window_s": 600,
                "redeploy": {"old_version": "v1", "new_version": "v2", "old": old, "new": new, "log_end": end},
                "stop": {"old_version": "v2", "old": [probe_line("main", "boot", T0, boot)] + old[2:], "log_end": end}}

    def test_a_signal_with_a_long_enough_grace_passes_and_states_the_three_answers(self):
        r = V.judge_d10_probe(self.ev(31))
        self.assertEqual(lines_of(r), [
            "FACT d10-probe: redeploy: the probe ran as pid 7 (PID 1 is \"/sbin/init\") under bun-1.3.13; the process group was not signalled",
            "FACT d10-probe: redeploy: killed 31-32 s after SIGTERM",
            "PASS d10-probe: redeploy: SIGTERM delivered and the process lived 31 s after it, >= the binary's 30 s ordered stop",
            "FACT d10-probe: stop: the probe ran as pid 7 (PID 1 is \"/sbin/init\") under bun-1.3.13; the process group was not signalled",
            "FACT d10-probe: stop: killed 31-32 s after SIGTERM",
            "PASS d10-probe: stop: SIGTERM delivered and the process lived 31 s after it, >= the binary's 30 s ordered stop",
            "PASS d10-probe: redeploy: the new version runs the new artifact (its own nonce)",
            "FACT d10-probe: redeploy: the new version booted 2.0 s after the old one's SIGTERM; old and new overlapped 29.0 s",
            "LEG_PASS d10-probe",
        ])

    def test_a_short_grace_a_group_signal_and_a_reused_artifact_are_reported(self):
        r = V.judge_d10_probe(self.ev(9, group=True, new_nonce='"n1"'))
        self.assertFalse(r.ok)
        text = "\n".join(lines_of(r))
        self.assertIn("FAIL d10-probe: redeploy: the process lived only 9 s after SIGTERM, inside the binary's 30 s ordered stop", text)
        self.assertIn("the process group was signalled", text)
        self.assertIn('FAIL d10-probe: redeploy: the new version runs the old nonce "n1": not a new artifact', text)

    def test_a_main_killed_before_its_child_is_judged_by_its_own_last_line(self):
        # The group signalled, then the main killed 5 s in while its child
        # ticked on to 40 s: the main (the process the platform started) is
        # the measure, the child's life a fact, and any old line bounds the
        # overlap.
        group = 'the probe ran as pid 7 (PID 1 is "/sbin/init") under bun-1.3.13; the process group was signalled'
        per = lambda s: [  # noqa: E731
            f"FACT d10-probe: {s}: {group}",
            f"FACT d10-probe: {s}: killed 5-6 s after SIGTERM",
            f"FACT d10-probe: {s}: the child lived 40 s after its own SIGTERM, 35 s past the main's last line: "
            "the kill hit the process, not its group",
            f"FAIL d10-probe: {s}: the process lived only 5 s after SIGTERM, inside the binary's 30 s ordered stop"]
        self.assertEqual(lines_of(V.judge_d10_probe(self.ev(5, child_lived_s=40))), per("redeploy") + per("stop") + [
            "PASS d10-probe: redeploy: the new version runs the new artifact (its own nonce)",
            "FACT d10-probe: redeploy: the new version booted 2.0 s after the old one's SIGTERM; old and new overlapped 38.0 s",
            "LEG_FAIL d10-probe"])
        # A child that dies with the main is reported without the clause.
        got = lines_of(V.judge_d10_probe(self.ev(31, child_lived_s=31)))
        self.assertIn("FACT d10-probe: stop: the child lived 31 s after its own SIGTERM", got)
        self.assertIn("PASS d10-probe: stop: SIGTERM delivered and the process lived 31 s after it, >= the binary's 30 s "
                      "ordered stop", got)

    def test_a_probe_that_was_not_kept_awake_fails(self):
        ev = self.ev(31)
        for part in ("redeploy", "stop"):
            ev[part]["old"] = [x.replace(" keep_awake=held", "") for x in ev[part]["old"]]
        ev["stop"]["old"][0] = ev["stop"]["old"][0].replace('nonce="n1"', 'nonce="n1" keep_awake=unavailable')
        got = lines_of(V.judge_d10_probe(ev))
        self.assertIn("FAIL d10-probe: redeploy: the probe was not kept awake (keep_awake=absent): Compute's "
                      "scale-to-zero could suspend it, so its stop facts would describe a sleeping service, not a "
                      "kept-awake cell", got)
        self.assertIn("FAIL d10-probe: stop: the probe was not kept awake (keep_awake=unavailable): Compute's "
                      "scale-to-zero could suspend it, so its stop facts would describe a sleeping service, not a "
                      "kept-awake cell", got)
        self.assertEqual(got[-1], "LEG_FAIL d10-probe")

    def test_a_log_that_ends_before_the_version_stops_is_never_read_as_a_kill(self):
        short = lines_of(V.judge_d10_probe(self.ev(9, end="segment_time_limit")))
        self.assertIn("FACT d10-probe: redeploy: not observed to the end: the log ended (segment_time_limit) 9 s after "
                      "SIGTERM, before the version stopped", short)
        self.assertIn("FAIL d10-probe: redeploy: not observed to the end: the log ended (segment_time_limit) 9 s after "
                      "SIGTERM, short of the binary's 30 s ordered stop", short)
        self.assertFalse(any("killed" in x or "lived only" in x for x in short), short)
        long = lines_of(V.judge_d10_probe(self.ev(31, end="upstream_error")))
        self.assertIn("PASS d10-probe: stop: SIGTERM delivered and the process lived 31 s after it, >= the binary's 30 s "
                      "ordered stop", long)
        self.assertNotIn("FACT d10-probe: stop: killed 31-32 s after SIGTERM", long)
        ev = self.ev(0, end="closed")
        ev["redeploy"]["old"] = ev["redeploy"]["old"][:2]
        self.assertIn("FAIL d10-probe: redeploy: no SIGTERM arrived before the log ended (closed), which is not the "
                      "version's stop: not observed to the end", lines_of(V.judge_d10_probe(ev)))

    def test_no_signal_before_the_log_ended_fails(self):
        ev = self.ev(0)
        ev["redeploy"]["old"] = ev["redeploy"]["old"][:2]
        r = V.judge_d10_probe(ev)
        self.assertIn("FAIL d10-probe: redeploy: no SIGTERM arrived before the log ended (vm_stopped)", lines_of(r))


def windows(t0: int, t1: int, acked: int = 5) -> list:
    """k2gen --out window lines (t: the window's end, in s) of 5 s each over [t0, t1]."""
    return [{"t": (a + 5000) / 1000, "window_s": 5.0, "acked_requests": acked, "failed_requests": 0}
            for a in range(t0, t1 - 4999, 5000)]


class D10Cell(unittest.TestCase):
    def ev(self, new_boot_at: int, load_until: int = T0 + 200_000, **old) -> dict:
        stop_at = T0 + 100_000
        return {
            "old_version": "v1", "new_version": "v2",
            "old_lines": boot_lines(T0, "b1") + stop_lines(stop_at, **old),
            "new_lines": boot_lines(new_boot_at, "b2"),
            "health": [{"t": T0 + i * 50_000, "status": 200, "boot": b} for i, b in enumerate(["b1", "b1", "b2", "b2"])],
            "load": {"rc": 0, "windows": windows(T0 - 60_000, load_until)},
            "audit": AUDIT_OK,
        }

    def test_a_clean_stop_after_which_the_new_process_boots_passes(self):
        r = V.judge_d10_cell(self.ev(T0 + 103_000))
        self.assertEqual(lines_of(r), [
            "PASS d10-cell: the wrapper received SIGTERM and forwarded it to the binary",
            "PASS d10-cell: the binary's ordered stop took 0.023 s (bound 30 s)",
            "PASS d10-cell: the binary exited 0 after the forwarded stop",
            "FACT d10-cell: old and new processes overlapped 0.0 s by their own clocks; the new process opened the shard after the old one's last line",
            "FACT d10-cell: clients saw 2 boot-id run(s) on /health: b1 -> b2",
            "PASS d10-cell: under light load across the redeploy (100 appends acked after the switch): 0 acked records lost (k2gen audit: 1200 records read back, 0 missing blocks <= 0 failed requests)",
            "LEG_PASS d10-cell",
        ])

    def test_a_load_that_ended_before_the_switch_is_not_under_load(self):
        # The new build first answers /health at +100 s; the load stopped at +90 s.
        got = lines_of(V.judge_d10_cell(self.ev(T0 + 103_000, load_until=T0 + 90_000)))
        self.assertIn("FAIL d10-cell: the load did not run across the redeploy: no append was acked after the switch "
                      "(the new build's first /health 200 and the old one's SIGTERM)", got)
        self.assertIn("PASS d10-cell: the load's audit: 0 acked records lost (k2gen audit: 1200 records read back, "
                      "0 missing blocks <= 0 failed requests)", got)
        self.assertEqual(got[-1], "LEG_FAIL d10-cell")

    def test_an_overlap_is_a_finding_and_moves_f2(self):
        r = V.judge_d10_cell(self.ev(T0 + 90_000))
        text = lines_of(r)
        self.assertIn("FACT d10-cell: old and new processes overlapped 10.0 s by their own clocks; the new process opened (fenced) the shard 9.6 s before the old one's last line", text)
        self.assertIn("FINDING d10-cell: the old and new processes overlap: F2's fenced-write 500 is reachable on one server, so F2 moves before the beta", text)
        self.assertTrue(r.ok)

    def test_a_slow_stop_a_kill_and_a_nonzero_exit_fail(self):
        slow = V.judge_d10_cell(self.ev(T0 + 200_000, stop_ms=35_000))
        self.assertIn("FAIL d10-cell: the binary's ordered stop took 35.000 s (over its 30 s bound)", lines_of(slow))
        ev = self.ev(T0 + 200_000, code=1)
        self.assertIn("FAIL d10-cell: the binary's exit after the forwarded stop: 1", lines_of(V.judge_d10_cell(ev)))
        ev["old_lines"].insert(-1, "binary did not stop within 180000 ms of SIGTERM; killing it")
        self.assertIn("FAIL d10-cell: the wrapper killed the binary after its grace", lines_of(V.judge_d10_cell(ev)))
        ev = self.ev(T0 + 200_000)
        ev["old_lines"] = [l for l in ev["old_lines"] if not l.startswith("wrapper received")]
        ev["audit"] = {"rc": 3, "verify": {"ok": False, "blocks_missing": 2, "failed_requests": 0}}
        got = lines_of(V.judge_d10_cell(ev))
        self.assertIn("FAIL d10-cell: no `wrapper received SIGTERM; forwarding it to the binary` line in version v1's log", got)
        self.assertIn("FAIL d10-cell: under light load across the redeploy (100 appends acked after the switch): the audit "
                      "failed (rc 3; missing blocks 2, failed requests 0, unexpected None, offset errors None)", got)


class Restart(unittest.TestCase):
    LISTEN = T0 + 20_000

    @staticmethod
    def tick(ms: int, boot, read: int = 200, append: int | None = None, code=None, retry_after=None,
             took_ms: int = 0) -> dict:
        """One probe tick: a read and an append, answered `took_ms` after it
        began, and the /health boot id beside them."""
        def answer(kind: str, status: int) -> dict:
            return {"t": ms, "kind": kind, "status": status, "code": code if status >= 400 else None,
                    "retry_after": retry_after if status == 503 else None}
        return {"t": ms, "t_end": ms + took_ms, "boot": boot,
                "answers": [answer("read", read), answer("append", read if append is None else append)]}

    def ev(self, ticks, sdk=None, t_stop: int = T0 + 45_000, old_lines=None) -> dict:
        """One restart triggered at 10 s: the new build listens at 20 s and
        the old one logs its `termination signal` at 24.5 s."""
        return {"restarts": [{"t_trigger": T0 + 10_000, "rc": 0, "new_version": "v2",
                              "new_lines": boot_lines(self.LISTEN - 400, "b2"), "old_version": "v1",
                              "old_lines": stop_lines(T0 + 24_500) if old_lines is None else old_lines}],
                "samples": sorted(ticks, key=lambda x: x["t"]), "probe_every_s": 0.25,
                "sdk": {"t_stop": t_stop, "calls": self.sdk(T0, t_stop) if sdk is None else sdk}}

    PATHS = ("read", "append", "producer")

    @classmethod
    def sdk(cls, t0: int, t1: int, k0: int = 0) -> list:
        """The SDK probe's calls in turn, one every 500 ms from t0 to t1, each
        answered 200 after 50 ms."""
        return [{"path": cls.PATHS[(k0 + k) % 3], "start": ms, "end": ms + 50, "ok": True,
                 "attempts": [{"t": ms, "end": ms + 50, "status": 200}]} for k, ms in enumerate(range(t0, t1, 500))]

    @staticmethod
    def call(path: str, start: int, attempts: list, error: dict | None = None) -> dict:
        """One SDK call whose attempts are (status, Retry-After, ms after the
        call began that the attempt ended); a status 0 got no answer."""
        tries = [dict({"t": start + i * 1000, "end": start + end_ms, "status": status},
                      **({"retry_after": ra} if ra else {})) for i, (status, ra, end_ms) in enumerate(attempts)]
        c = {"path": path, "start": start, "end": tries[-1]["end"], "ok": error is None, "attempts": tries}
        return dict(c, error=error) if error else c

    def switch(self, gap_s: int = 3, middle=None) -> list:
        """The old build b1 answers until 24 s (4 s after the new build's
        `listening` line at 20 s), then `gap_s` seconds of ticks (503 with
        Retry-After by default), then the new build b2."""
        ticks = [self.tick(T0 + i * 1000, "b1") for i in range(25)]
        for k in range(gap_s):
            ticks.append(middle(T0 + 25_000 + k * 1000) if middle else
                         self.tick(T0 + 25_000 + k * 1000, None, 503, code="shard_moving", retry_after="1"))
        return ticks + [self.tick(T0 + (25 + gap_s) * 1000 + k * 1000, "b2") for k in range(5)]

    def test_only_retryable_503s_and_a_short_window_pass(self):
        self.assertEqual(lines_of(V.judge_restart(self.ev(self.switch()))), [
            "FACT restart: restart 1: the new build first answered every probe 8.0 s after its `listening` line; "
            "the old build answered until 4.0 s after it",
            "PASS restart: restart 1: raw clients were without service at most 4.0 s across the switch (between completed "
            "all-2xx ticks; bar 10 s)",
            "FACT restart: restart 1: 6 x 503 with Retry-After while restarting ['503:shard_moving']",
            "PASS restart: restart 1: raw clients saw only 503 with Retry-After",
            "FACT restart: restart 1: the old build (v1) logged `termination signal` 4.5 s after `listening`",
            "PASS restart: restart 1: SDK successes came at most 0.5 s apart across the switch (bar 10 s)",
            "FACT restart: the SDK probe (sdk/dist) made 90 calls one at a time: 30 reads, 30 plain appends, 30 producer "
            "appends; 0 failed",
            "LEG_PASS restart",
        ])

    def test_a_502_after_the_old_build_kept_answering_past_listening_fails(self):
        # The old build answers 2xx for 4 s after the new one's `listening`
        # line; only then does the switch drop a 502 (no /health answer).
        r = V.judge_restart(self.ev(self.switch(1, lambda ms: self.tick(ms, None, 502))))
        got = lines_of(r)
        self.assertIn("FAIL restart: restart 1: raw clients saw 2 other answer(s): "
                      "['append 502:None (no /health)', 'read 502:None (no /health)']", got)
        self.assertEqual(got[-1], "LEG_FAIL restart")

    def test_a_fenced_write_on_the_old_build_and_a_slow_window_fail(self):
        fenced = lines_of(V.judge_restart(self.ev(self.switch(1, lambda ms: self.tick(ms, "b1", 200, 500, code="fenced")))))
        self.assertIn("FAIL restart: restart 1: raw clients saw 1 other answer(s): ['append 500:fenced (old)']", fenced)
        slow = lines_of(V.judge_restart(self.ev(self.switch(12))))
        self.assertIn("FAIL restart: restart 1: raw clients were without service 13.0 s across the switch (between completed "
                      "all-2xx ticks, from 4.0 s to 17.0 s after `listening`), over the 10 s bar", slow)

    def test_a_2xx_that_stalls_is_time_without_service(self):
        # The new build's first request waits on its lazy shard open (up to
        # SHARD_OPEN_WAIT_MS, 10 s) and then answers 2xx inside the
        # prober's 10 s timeout: no tick fails, yet clients wait. The
        # prober is one blocking thread, so the next tick starts after it.
        def stalled(took_s: int) -> list:
            ticks = [self.tick(T0 + i * 1000, "b1") for i in range(25)]
            ticks.append(self.tick(T0 + 25_000, "b2", took_ms=took_s * 1000))
            return ticks + [self.tick(T0 + (26 + took_s) * 1000 + k * 1000, "b2") for k in range(5)]
        self.assertIn("PASS restart: restart 1: raw clients were without service at most 8.0 s across the switch "
                      "(between completed all-2xx ticks; bar 10 s)", lines_of(V.judge_restart(self.ev(stalled(7)))))
        got = lines_of(V.judge_restart(self.ev(stalled(12))))
        self.assertIn("FAIL restart: restart 1: raw clients were without service 13.0 s across the switch (between "
                      "completed all-2xx ticks, from 4.0 s to 17.0 s after `listening`), over the 10 s bar", got)
        self.assertEqual(got[-1], "LEG_FAIL restart")

    def test_a_503_run_the_raw_window_passes_fails_when_the_sdk_errors(self):
        # Five seconds of 503 Retry-After: 1 pass the raw window, but the
        # SDK replays a read's 503 at most three times, a second apart, and
        # then rejects it: D4's bar counts any SDK error.
        stuck = self.call("read", T0 + 25_000, [(503, "1", 50), (503, "1", 1050), (503, "1", 2050), (503, "1", 3100)],
                          {"class": "StreamsError", "status": 503, "code": "shard_moving",
                           "answer": "HTTP 503 shard_moving", "message": "the shard is moving"})
        late = self.call("append", T0 + 28_300, [(503, "1", 50), (503, "1", 1050), (200, None, 2050)])
        sdk = self.sdk(T0, T0 + 25_000) + [stuck, late] + self.sdk(T0 + 30_850, T0 + 45_000, k0=2)
        got = lines_of(V.judge_restart(self.ev(self.switch(5), sdk)))
        self.assertIn("PASS restart: restart 1: raw clients were without service at most 6.0 s across the switch "
                      "(between completed all-2xx ticks; bar 10 s)", got)
        self.assertIn("FAIL restart: restart 1: 1 SDK call(s) failed, the first 8.1 s after `listening`: read: "
                      "StreamsError (answered HTTP 503 shard_moving)", got)
        self.assertIn("PASS restart: restart 1: SDK successes came at most 5.8 s apart across the switch (bar 10 s)", got)
        self.assertIn("FACT restart: the SDK probe (sdk/dist) made 81 calls one at a time: 28 reads, 27 plain appends, "
                      "26 producer appends; 1 failed", got)
        self.assertEqual(got[-1], "LEG_FAIL restart")

    def test_sdk_successes_further_apart_than_the_bar_fail_and_name_the_calls_between_them(self):
        # The new build cuts a read's connection while it opens the shard;
        # the SDK replays it 1, 2 and 4 s later, and the last replay is
        # answered: no SDK error, but no success for 12.5 s.
        slow = self.call("read", T0 + 25_000, [(0, None, 50), (0, None, 1100), (0, None, 3200), (200, None, 12_050)])
        sdk = self.sdk(T0, T0 + 25_000) + [slow] + self.sdk(T0 + 37_500, T0 + 45_000, k0=1)
        got = lines_of(V.judge_restart(self.ev(self.switch(), sdk)))
        self.assertIn("FAIL restart: restart 1: no SDK success for 12.5 s, from 4.5 s after `listening` to 17.1 s after "
                      "`listening`, over the 10 s bar; its calls: read ok after 12.1 s [no answer, no answer, no answer, 200]",
                      got)
        # A probe whose last call never ended: the gap runs to its stop.
        hung = [dict(self.call("read", T0 + 30_000, [(0, None, 0)]), end=T0 + 45_000, ok=False, stopped=True)]
        got = lines_of(V.judge_restart(self.ev(self.switch(), self.sdk(T0, T0 + 30_000) + hung)))
        self.assertIn("FAIL restart: restart 1: no SDK success for 15.4 s, from 9.6 s after `listening` to 25.0 s after "
                      "`listening` (the probe's stop), over the 10 s bar; its calls: read in flight at the probe's stop "
                      "after 15.0 s [no answer]", got)
        self.assertIn("FACT restart: the SDK probe (sdk/dist) made 60 calls one at a time: 20 reads, 20 plain appends, "
                      "20 producer appends; 0 failed", got)

    def test_an_sdk_error_before_the_first_restart_and_no_sdk_calls_fail(self):
        fenced = self.call("producer", T0 + 5_000, [(403, None, 40)],
                           {"class": "ProducerFencedError", "status": 403, "code": "stale_producer_epoch",
                            "answer": "HTTP 403 stale_producer_epoch", "message": "fenced"})
        got = lines_of(V.judge_restart(self.ev(self.switch(), self.sdk(T0, T0 + 5_000) + [fenced]
                                               + self.sdk(T0 + 5_500, T0 + 45_000))))
        self.assertIn("FAIL restart: before the first restart: 1 SDK call(s) failed, the first 5.0 s before the first "
                      "restart's trigger: producer append: ProducerFencedError (answered HTTP 403 stale_producer_epoch)", got)
        cut = self.call("append", T0 + 26_000, [(0, None, 3)],
                        {"class": "AppendOutcomeUnknownError", "status": 0, "code": "outcome_unknown",
                         "cause": "TypeError: fetch failed; cause: SocketError: other side closed (UND_ERR_SOCKET)"})
        self.assertEqual(V.sdk_failure(cut), "plain append: AppendOutcomeUnknownError (no answer: TypeError: fetch "
                                             "failed; cause: SocketError: other side closed (UND_ERR_SOCKET))")
        self.assertIn("FAIL restart: the SDK probe made no call: D4's bar is the shipping SDK's (restart.sdk)",
                      lines_of(V.judge_restart(self.ev(self.switch(), []))))

    def test_a_plain_append_cut_at_the_old_builds_sigterm_is_labelled_the_old_builds_abort(self):
        # The old build's stop aborts every open connection before its
        # shards close; the SDK never sends a plain append again. Still a
        # FAIL (any SDK error), labelled so the owner can choose its remedy.
        cause = "TypeError: fetch failed; cause: SocketError: other side closed (UND_ERR_SOCKET)"
        unknown = {"class": "AppendOutcomeUnknownError", "status": 0, "code": "outcome_unknown", "cause": cause}
        what = f"plain append: AppendOutcomeUnknownError (no answer: {cause})"

        def run(cut_at: int, old_lines=None, path="append", error=unknown) -> list:
            cut = self.call(path, cut_at - 30, [(0, None, 30)], error)
            sdk = self.sdk(T0, cut_at - 30) + [cut] + self.sdk(cut_at + 300, T0 + 45_000, k0=2)
            return lines_of(V.judge_restart(self.ev(self.switch(), sdk, old_lines=old_lines)))
        got = run(T0 + 24_505)
        self.assertIn("FAIL restart: restart 1: 1 SDK call(s) failed, the first 4.5 s after `listening`: "
                      f"{what}; the old build's abort reached a client (the old build logged `termination signal` 4.5 s "
                      "after `listening`)", got)
        self.assertIn("FACT restart: restart 1: the old build (v1) logged `termination signal` 4.5 s after `listening`", got)
        self.assertEqual(got[-1], "LEG_FAIL restart")
        # A cut seconds after the old build's stop is not its abort.
        self.assertIn(f"FAIL restart: restart 1: 1 SDK call(s) failed, the first 7.5 s after `listening`: {what}",
                      run(T0 + 27_505))
        # Without the old build's stop in its log the cut cannot be told.
        got = run(T0 + 24_505, old_lines=[])
        self.assertIn(f"FAIL restart: restart 1: 1 SDK call(s) failed, the first 4.5 s after `listening`: {what}; the old "
                      "build's stop is not in its log, so a cut by its abort cannot be told", got)
        self.assertIn("FACT restart: restart 1: the old build's stop (v1) is not in its log", got)
        # A read is replayed after a cut; one that still failed is not labelled.
        transport = {"class": "StreamsTransportError", "status": 0, "code": "transport_error", "cause": cause}
        self.assertIn(f"FAIL restart: restart 1: 1 SDK call(s) failed, the first 4.5 s after `listening`: read: "
                      f"StreamsTransportError (no answer: {cause})", run(T0 + 24_505, path="read", error=transport))

    def test_a_new_build_that_never_serves_and_reads_alone_fail(self):
        ticks = [t for t in self.switch() if t["boot"] != "b2"]
        self.assertIn("FAIL restart: restart 1: the new build (boot b2) never answered every probe 2xx",
                      lines_of(V.judge_restart(self.ev(ticks))))
        reads = [dict(t, answers=t["answers"][:1]) for t in self.switch()]
        self.assertIn("FAIL restart: no append was probed: a fenced write's 500 (F2) cannot show on reads alone",
                      lines_of(V.judge_restart(self.ev(reads))))


class Rollback(unittest.TestCase):
    def ev(self, fake_rc=1) -> dict:
        # Step i deploys at i x 300 s; its build answers /health 20 s later; its load runs 60 s before to 120 s after.
        steps = [{"label": lab, "deploy_rc": 0, "cell_check_rc": 0, "audit": AUDIT_OK, "t_deploy": T0 + i * 300_000,
                  "windows": windows(T0 + i * 300_000 - 60_000, T0 + i * 300_000 + 120_000)}
                 for i, lab in enumerate(("older", "newer", "older", "newer"), 1)]
        health = [{"t": T0 + k * 1000, "status": 200, "boot": f"b{max(0, (k - 20) // 300)}"} for k in range(1500)]
        return {"steps": steps, "newer_label": "newer", "health": health,
                "fake": {"rc": fake_rc, "tail": "refused: release layout_version 6, running 5", "key": "layout_version",
                         "from": 5, "to": 6, "boot_before": "b1", "boot_after": "b1", "pattern": "(?i)layout"}}

    def test_four_steps_and_the_refusal_pass(self):
        got = lines_of(V.judge_rollback(self.ev()))
        self.assertIn("PASS rollback: step 3: data written by newer read back by older", got)
        self.assertIn("PASS rollback: a release whose layout_version differs (5 -> 6) is refused (rc 1) and the running boot is untouched", got)
        self.assertEqual(got[-1], "LEG_PASS rollback")

    def test_an_accepted_cross_layout_release_fails(self):
        got = lines_of(V.judge_rollback(self.ev(fake_rc=0)))
        self.assertIn("FAIL rollback: the cross-layout release: rc 0, refusal names the layout: True, boot unchanged: True", got)


class Oom(unittest.TestCase):
    def ev(self, boots) -> dict:
        return {"old_boot": "b1", "new_boot": "b2", "kill_t": T0, "first_ok_t": T0 + 42_000, "audit": AUDIT_OK,
                "lines": ["binary exited with code 137 after serving on :8080 (up 912000 ms); exiting 137 so the platform replaces this instance"],
                "stable": {"seconds": 600, "boots": boots}}

    def test_a_visible_kill_a_replacement_and_no_crash_loop_pass(self):
        got = lines_of(V.judge_oom(self.ev(["b2", "b2"])))
        self.assertEqual(got[0], "PASS oom: NON-SHIPPING CONFIG: the wrapper logged code 137 and exited, so the platform replaces the instance")
        self.assertIn("FACT oom: NON-SHIPPING CONFIG: first 2xx 42.0 s after the kill", got)
        self.assertEqual(got[-1], "LEG_PASS oom")

    def test_a_crash_loop_fails(self):
        self.assertIn("FAIL oom: NON-SHIPPING CONFIG: 2 boots in the 600 s after recovery", lines_of(V.judge_oom(self.ev(["b2", "b3"]))))


class Feeds(unittest.TestCase):
    def ev(self, quiet=(), stale_after=300) -> dict:
        deleted_t = T0 + 125_000
        return {
            "quiet": {"seconds": 120, "lines": list(quiet)},
            "admit": {"before": 401, "t_pub": T0, "t_effect": T0 + 31_000},
            "revoke": {"before": 404, "t_pub": T0, "t_effect": T0 + 40_000},
            "malformed": {"t_pub": T0, "probe_every_s": 5, "deleted": {"after_s": 127, "t": deleted_t},
                          "stale_t": deleted_t + stale_after * 1000, "restore": {"t_pub": T0, "t_effect": T0 + 20_000}},
        }

    def test_steady_304s_effects_within_45_s_and_the_fuse_pass(self):
        self.assertEqual(lines_of(V.judge_feeds(self.ev())), [
            "PASS feeds: steady: no failed poll in 120 s (8 polls)",
            "PASS feeds: the presigned conditional GET works against the store: nothing published, nothing rewritten (each poll a 304)",
            "PASS feeds: admit: effect 31.0 s after the publish (bar 45 s)",
            "PASS feeds: revoke: effect 40.0 s after the publish (bar 45 s)",
            "PASS feeds: malformed bundle: feeds deleted after 127 s without a successful poll (about 120 s)",
            "PASS feeds: malformed bundle: 503 policy_stale 300 s after the deletion (about 300 s)",
            "PASS feeds: restore: served 20.0 s after the good republish (bar 45 s)",
            "LEG_PASS feeds",
        ])

    def test_a_store_that_ignores_if_none_match_and_an_early_stale_fail(self):
        got = lines_of(V.judge_feeds(self.ev(quiet=["feeds updated: /tmp/feeds/{keys,policies,grants}.json gen=4"], stale_after=60)))
        self.assertIn("FAIL feeds: the bundle was rewritten 1 time(s) with nothing published: the store ignores If-None-Match; "
                      "use C9's stat-then-GET fallback", got)
        self.assertIn("FAIL feeds: malformed bundle: 503 policy_stale 60 s after the deletion, outside 270-335 s", got)


class BootFailures(unittest.TestCase):
    def test_the_generic_500_with_the_detail_in_the_log_passes_and_a_leak_fails(self):
        body = json.dumps(V.GENERIC_500)
        case = {"name": "missing-secret", "status": 500, "body": body, "expect_log": "STREAMS_CURSOR_KEY", "restored": True,
                "lines": ["entry refused: secret STREAMS_CURSOR_KEY unset"], "deploy_rc": 1, "version": "v2"}
        self.assertEqual(lines_of(V.judge_boot_failures({"cases": [case]})), [
            "FACT boot-failures: missing-secret: the deploy command exited 1",
            "PASS boot-failures: missing-secret: the generic 500 binary_exited, no detail in the body",
            "PASS boot-failures: missing-secret: the detail is in the log: entry refused: secret STREAMS_CURSOR_KEY unset",
            "PASS boot-failures: missing-secret: the good release serves again",
            "LEG_PASS boot-failures",
        ])
        leak = dict(case, body=json.dumps({**V.GENERIC_500, "detail": "STREAMS_CURSOR_KEY unset"}))
        self.assertFalse(V.judge_boot_failures({"cases": [leak]}).ok)
        none = lines_of(V.judge_boot_failures({"cases": [dict(case, version=None, lines=[])]}))
        self.assertIn("FAIL boot-failures: missing-secret: the deploy made no new version, so nothing reached the platform "
                      "to fail its boot", none)


class KeepAwake(unittest.TestCase):
    def test_one_boot_for_the_whole_window_passes_and_a_recycle_is_a_finding(self):
        samples = [{"t": T0 + i * 3_600_000, "status": 200, "boot": "b1"} for i in range(73)]
        ok = V.judge_keep_awake({"hours": 72, "samples": samples, "wrapper_boots": 1})
        self.assertEqual(lines_of(ok), [
            "PASS keep-awake: observed 72.0 h of the planned 72 h",
            "FACT keep-awake: awake fraction 1.0000 (73 samples)",
            "FACT keep-awake: 1 wrapper boot(s) (`instance shape:` lines) in the version's log buffer",
            "PASS keep-awake: no recycle in 72.0 h: one boot id",
            "LEG_PASS keep-awake",
        ])
        samples[40] = {"t": samples[40]["t"], "status": 0, "boot": None}
        for s in samples[41:]:
            s["boot"] = "b2"
        got = lines_of(V.judge_keep_awake({"hours": 72, "samples": samples}))
        self.assertIn(f"FAIL keep-awake: 1 recycle(s): at {samples[41]['t']} after 7200.0 s", got)
        self.assertIn("FINDING keep-awake: an unannounced recycle of the only server is an outage for every tenant: take it to Compute", got)


# ---------------------------------------------------------------- the probe, read for real

@unittest.skipIf(shutil.which("bun") is None, "bun is not installed")
class ProbeOutput(unittest.TestCase):
    def test_the_probes_own_lines_read_as_a_group_signal_and_a_short_life(self):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        p = subprocess.Popen(["bun", os.path.join(HERE, "sigterm-probe", "app", "probe.ts")], stdout=subprocess.PIPE,
                             env={**os.environ, "PORT": str(port)}, start_new_session=True, text=True)
        out = []
        try:
            for line in p.stdout:
                out.append(line.rstrip("\n"))
                if sum("boot" in l for l in out) == 2:
                    break
            os.killpg(p.pid, signal.SIGTERM)
            time.sleep(2.4)
        finally:
            os.killpg(p.pid, signal.SIGKILL)
            rest, _ = p.communicate(timeout=10)
        out += rest.splitlines()
        facts = V.probe_facts(out)
        self.assertEqual(facts["boot"]["fields"]["pid"], str(p.pid))
        self.assertTrue(facts["sigterm"] and facts["group"])
        self.assertTrue(2 <= facts["lived_s"] < 3, facts["lived_s"])
        self.assertTrue(2 <= facts["child_lived_s"] < 3, facts["child_lived_s"])
        r = V.judge_d10_probe({"read_window_s": 9, "redeploy": {"old": out, "log_end": "vm_stopped"},
                               "stop": {"old": out, "log_end": "vm_stopped"}})
        self.assertIn("FAIL d10-probe: stop: the process lived only 2 s after SIGTERM, inside the binary's 30 s ordered stop",
                      lines_of(r))
        # The whole group was killed at once: never read as a kill of the process alone.
        self.assertIn("FACT d10-probe: stop: the child lived 2 s after its own SIGTERM", lines_of(r))
        # Off Compute the probe has no scale-to-zero control to hold.
        self.assertIn("FAIL d10-probe: stop: the probe was not kept awake (keep_awake=unavailable): Compute's "
                      "scale-to-zero could suspend it, so its stop facts would describe a sleeping service, not a "
                      "kept-awake cell", lines_of(r))


@unittest.skipIf(shutil.which("node") is None or not os.path.exists(L.SDK_DIST),
                 "node or a built sdk/dist is missing (cd sdk && npm ci && npm run build)")
class SdkProbeOutput(unittest.TestCase):
    """sdk-probe.mjs run for real (node on sdk/dist) against a fake cell that
    answers its second read 502, cuts its second plain append's connection
    without an answer, and holds its fourth read until the probe is stopped."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="lifecycle-sdk-")
        self.seen, self.release = [], threading.Event()
        self.holding = threading.Event()
        test = self

        class Cell(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def answer(self, status: int, body: bytes, **headers) -> None:
                self.send_response(status)
                for k, v in {"content-type": "application/json", **headers}.items():
                    self.send_header(k.replace("_", "-"), v)
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def take(self) -> None:
                body = self.rfile.read(int(self.headers.get("content-length") or 0))
                test.seen.append((self.command, self.path, {k.lower(): v for k, v in self.headers.items()}, body))

            def do_PUT(self):
                self.take()
                self.answer(201, b"{}")

            def do_GET(self):
                self.take()
                n = sum(1 for m, *_ in test.seen if m == "GET")
                if n == 2:
                    return self.answer(502, b"bad gateway", content_type="text/plain")
                if n == 4:
                    test.holding.set()
                    test.release.wait(20)
                self.answer(200, b"[]", prisma_next_cursor=f"c{n}")

            def do_POST(self):
                self.take()
                plain = [h for m, _, h, _ in test.seen if m == "POST" and "producer-id" not in h]
                if "producer-id" not in self.headers and len(plain) == 2:
                    self.close_connection = True  # the old build's abort: no answer at all
                    return
                self.answer(200, b'{"cursor":"c","count":1,"duplicate":false,"sealed":false}')

            def log_message(self, *a):
                pass

        class Server(http.server.ThreadingHTTPServer):
            daemon_threads = True

            def server_bind(self):
                socketserver.TCPServer.server_bind(self)
                self.server_name, self.server_port = "127.0.0.1", self.server_address[1]
        self.srv = Server(("127.0.0.1", 0), Cell)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def tearDown(self):
        self.release.set()
        self.srv.shutdown()
        self.srv.server_close()
        shutil.rmtree(self.tmp)

    def probe(self, mode: int = 0o600) -> tuple:
        headers = os.path.join(self.tmp, "headers.txt")
        with open(headers, "w") as f:
            f.write("# the SDK probe's\nauthorization: Bearer tok-1\nprisma-encryption-key: a2V5\n")
        os.chmod(headers, mode)
        out = os.path.join(self.tmp, "calls.jsonl")
        p = subprocess.Popen(["node", L.SDK_PROBE, "--url", f"http://127.0.0.1:{self.srv.server_address[1]}",
                              "--stream", "lc/sdk", "--headers-file", headers, "--out", out, "--producer-id", "lc-run-1",
                              "--state", os.path.join(self.tmp, "state.json"), "--pause-ms", "10"],
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        return p, out

    def test_each_call_names_its_path_its_attempts_and_what_ended_it(self):
        p, out = self.probe()
        try:
            self.assertTrue(self.holding.wait(30), "the probe never reached its fourth read")
            p.send_signal(signal.SIGTERM)
            said, _ = p.communicate(timeout=30)
            self.assertEqual(p.returncode, 0, said)
        finally:
            p.kill()
            p.communicate()
        lines = L.read_jsonl(out)
        self.assertEqual(lines[0]["setup"], "ok")
        calls = lines[1:]
        self.assertEqual([(c["path"], c["ok"]) for c in calls], [
            ("read", True), ("append", True), ("producer", True), ("read", False), ("append", False), ("producer", True),
            ("read", True), ("append", True), ("producer", True), ("read", False)])
        # A 502 is no SDK retry: one attempt, the edge's answer named.
        self.assertEqual(calls[3]["error"], {"class": "StreamsError", "message": "HTTP 502", "status": 502,
                                             "code": "http_error", "answer": "HTTP 502 http_error"})
        self.assertEqual([a["status"] for a in calls[3]["attempts"]], [502])
        # A plain append whose connection is cut is never sent again.
        cut = calls[4]
        self.assertEqual((cut["error"]["class"], cut["error"]["status"], [a["status"] for a in cut["attempts"]]),
                         ("AppendOutcomeUnknownError", 0, [0]))
        self.assertRegex(V.sdk_failure(cut), r"^plain append: AppendOutcomeUnknownError \(no answer: TypeError: fetch "
                                             r"failed; cause: SocketError: other side closed")
        # The read in flight at SIGTERM is written as stopped, not failed.
        self.assertEqual((calls[-1].get("stopped"), "error" in calls[-1]), (True, False))
        gets = [(path, h) for m, path, h, _ in self.seen if m == "GET"]
        self.assertEqual([path for path, _ in gets], ["/v1/streams/lc/sdk/records?cursor=now"] + [
            f"/v1/streams/lc/sdk/records?cursor=c{n}" for n in (1, 1, 3)])
        self.assertEqual({(h["authorization"], h["prisma-encryption-key"]) for _, _, h, _ in self.seen},
                         {("Bearer tok-1", "a2V5")})
        self.assertEqual(self.seen[0][3], b'{"format":{"kind":"json"}}')
        made = [(h["producer-id"], h["producer-epoch"], h["producer-seq"]) for m, _, h, _ in self.seen if "producer-id" in h]
        self.assertEqual(made, [("lc-run-1", "0", "0"), ("lc-run-1", "0", "1"), ("lc-run-1", "0", "2")])
        with open(os.path.join(self.tmp, "state.json")) as f:
            self.assertEqual(json.load(f), {"epoch": 0, "nextSeq": 3})

    def test_a_header_file_others_can_read_is_refused_before_any_request(self):
        p, out = self.probe(mode=0o644)
        said, _ = p.communicate(timeout=30)
        self.assertEqual(p.returncode, 2, said)
        (line,) = L.read_jsonl(out)
        self.assertEqual((line["setup"], line["error"]["class"]), ("failed", "Error"))
        self.assertEqual(V.sdk_failure(line), f"setup: Error (no answer: Error: {self.tmp}/headers.txt is readable by "
                                              "others (chmod 600))")
        self.assertEqual(self.seen, [])


# ---------------------------------------------------------------- plumbing

class Plumbing(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="lifecycle-test-")

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def secret(self, name: str, text: str = "s3cr3t", mode: int = 0o600) -> str:
        path = os.path.join(self.tmp, name)
        with open(path, "w") as f:
            f.write(text + "\n")
        os.chmod(path, mode)
        return path

    def test_fill_replaces_placeholders_and_refuses_an_unknown_one(self):
        self.assertEqual(H.fill(["d", "{stage}", "x{seed}y"], {"stage": "scratch", "seed": 3}), ["d", "scratch", "x3y"])
        with self.assertRaisesRegex(H.PlanError, r"no value for \{nope\}"):
            H.fill(["d", "{nope}"], {})
        with self.assertRaises(H.PlanError):
            H.fill("deploy {stage}", {"stage": "s"})

    def test_secrets_come_only_from_private_files_and_prisma_names_never_leak(self):
        plan = H.Plan({"env_files": {"MY_TOKEN": self.secret("tok")}})
        with mock.patch.dict(os.environ, {"PRISMA_COMPUTE_SERVICE_ID": "svc_stray", "KEEP": "1"}):
            env = plan.env()
        self.assertEqual((env["MY_TOKEN"], env["KEEP"]), ("s3cr3t", "1"))
        self.assertNotIn("PRISMA_COMPUTE_SERVICE_ID", env)
        with self.assertRaisesRegex(H.PlanError, "readable by others"):
            H.Plan({}).secret(self.secret("open", mode=0o644))
        with self.assertRaisesRegex(H.PlanError, "is empty"):
            H.Plan({}).secret(self.secret("empty", text=""))

    def test_the_shell_runs_a_command_with_the_plan_env_and_keeps_its_output_in_the_run_dir(self):
        plan = H.Plan({"env_files": {"MY_TOKEN": self.secret("tok")}})
        shell = H.Shell(plan, self.tmp, say=lambda _: None)
        r = shell.run("echo", [sys.executable, "-c", "import os,sys; print(len(os.environ['MY_TOKEN']), sys.argv[1]); sys.exit(4)", "{stage}"],
                      {"stage": "scratch"})
        self.assertEqual(r["rc"], 4)
        self.assertEqual(r["tail"].strip(), "6 scratch")
        self.assertEqual(os.listdir(self.tmp).count("cmd-01-echo.log"), 1)

    def test_a_log_stream_keeps_log_records_and_the_terminal_code(self):
        recs = [{"type": "log", "text": "a"}, {"type": "log", "text": "b\nc"}, {"type": "terminal", "kind": "end", "code": "vm_stopped"}]
        script = "import json\n" + "".join(f"print(json.dumps({r!r}))\n" for r in recs)
        s = H.LogStream([sys.executable, "-c", script], dict(os.environ))
        self.assertEqual(s.wait_end(10), "vm_stopped")
        self.assertEqual([t for _, t in s.snapshot()], ["a", "b", "c"])

    FAKE_CLI = """
import json, sys
argv = sys.argv[1:]
with open(argv[0], "a") as f:
    f.write(" ".join(argv[1:]) + "\\n")
mode = argv[1]
def rec(**r):
    print(json.dumps(r), flush=True)
if mode == "segment" and "--cursor" not in argv:
    rec(type="log", text="a", byteStart=0, byteEnd=10)
    rec(type="log", text="b\\nc", byteStart=10, byteEnd=20)
    rec(type="terminal", kind="end", code="segment_time_limit", message="m", retryable=False, cursor="20")
elif mode == "segment":
    rec(type="log", text="d", byteStart=20, byteEnd=30)
    rec(type="terminal", kind="end", code="vm_stopped", message="m", retryable=False, cursor="30")
elif mode == "dropped" and "--cursor" not in argv:
    rec(type="log", text="a", byteStart=0, byteEnd=10)
elif mode == "dropped":
    rec(type="terminal", kind="end", code="vm_stopped", message="m", retryable=False, cursor="10")
elif mode == "superseded":
    rec(type="log", text="a", byteStart=0, byteEnd=10)
    rec(type="terminal", kind="end", code="superseded", message="m", retryable=False, cursor="10")
else:
    rec(type="terminal", kind="end", code="segment_time_limit", message="m", retryable=False, cursor="5")
"""

    def fake_stream(self, mode: str):
        calls = os.path.join(self.tmp, f"calls-{mode}")
        cli = [sys.executable, "-c", self.FAKE_CLI, calls]
        return H.LogStream(cli + [mode], dict(os.environ), resume=lambda c: cli + [mode, "--cursor", c]), calls

    def test_a_log_stream_resumes_a_segment_end_from_its_cursor_until_the_version_stops(self):
        s, calls = self.fake_stream("segment")
        self.assertEqual(s.wait_end(10), "vm_stopped")
        self.assertEqual(([t for _, t in s.snapshot()], s.resumes), (["a", "b", "c", "d"], 1))
        with open(calls) as f:
            self.assertEqual(f.read().splitlines(), ["segment", "segment --cursor 20"])
        dropped, _ = self.fake_stream("dropped")
        self.assertEqual((dropped.wait_end(10), [t for _, t in dropped.snapshot()]), ("vm_stopped", ["a"]))

    def test_a_superseded_stream_is_not_resumed_and_empty_resumes_are_bounded(self):
        s, _ = self.fake_stream("superseded")
        self.assertEqual((s.wait_end(10), s.resumes), ("superseded", 0))
        empty, calls = self.fake_stream("empty")
        self.assertEqual((empty.wait_end(10), empty.resumes), ("segment_time_limit", H.LogStream.MAX_EMPTY_RESUMES))
        with open(calls) as f:
            self.assertEqual(len(f.read().splitlines()), H.LogStream.MAX_EMPTY_RESUMES + 1)

    def test_a_request_reads_the_error_code_retry_after_and_boot_id(self):
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = json.dumps({"error": {"code": "shard_moving", "message": "m", "retryable": True}}).encode()
                self.send_response(503)
                self.send_header("retry-after", "1")
                self.send_header("x-streams-boot-id", "b9")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass
        class Server(http.server.HTTPServer):
            def server_bind(self):  # without HTTPServer's reverse lookup of the host (slow on some hosts)
                socketserver.TCPServer.server_bind(self)
                self.server_name, self.server_port = "127.0.0.1", self.server_address[1]
        srv = Server(("127.0.0.1", 0), Handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            got = H.request(f"http://127.0.0.1:{srv.server_address[1]}/v1/streams/x")
        finally:
            srv.shutdown()
            srv.server_close()
        self.assertEqual((got["status"], got["code"], got["retry_after"], got["boot"]), (503, "shard_moving", "1", "b9"))
        self.assertEqual(H.request("http://127.0.0.1:9/")["status"], 0)

    def test_an_error_answer_whose_body_is_cut_short_is_still_a_sample(self):
        with socket.socket() as srv:
            srv.bind(("127.0.0.1", 0))
            srv.listen(1)

            def answer():
                c, _ = srv.accept()
                with c:
                    c.recv(4096)
                    c.sendall(b"HTTP/1.1 502 Bad Gateway\r\ncontent-length: 100\r\n\r\nshort")
            t = threading.Thread(target=answer, daemon=True)
            t.start()
            got = H.request(f"http://127.0.0.1:{srv.getsockname()[1]}/v1/streams/x")
            t.join(5)
        self.assertEqual((got["status"], got["code"], got["body"]), (502, None, ""))

    def test_a_prober_whose_probe_raises_keeps_sampling(self):
        calls = []

        def probe():
            calls.append(1)
            if len(calls) == 2:
                raise KeyError("boot")
            return {"t": H.now_ms(), "status": 200}
        p = H.Prober(probe, 0.01)
        deadline = time.time() + 5
        while len(p.samples) < 4 and time.time() < deadline:
            time.sleep(0.01)
        got = p.stop()
        self.assertGreaterEqual(len(got), 4)
        self.assertEqual([x["status"] for x in got[:3]], [200, 0, 200])
        self.assertEqual(got[1]["code"], "probe KeyError: 'boot'")

    def built_sdk(self, age_s: int = 0) -> mock._patch:
        """sdk/dist/index.js and sdk/src/index.ts in the temp dir, the build
        `age_s` older than the source."""
        src, dist = os.path.join(self.tmp, "index.ts"), os.path.join(self.tmp, "index.js")
        for path in (src, dist):
            with open(path, "w") as f:
                f.write("//\n")
        now = time.time()
        os.utime(src, (now, now))
        os.utime(dist, (now - age_s, now - age_s))
        return mock.patch.multiple(L, SDK_DIST=dist, SDK_SRC=src)

    def test_the_example_plan_checks_clean_for_every_leg(self):
        plan = H.Plan.load(os.path.join(HERE, "plan.example.json"))
        with self.built_sdk(), mock.patch.object(L.shutil, "which", lambda name: f"/opt/bin/{name}"):
            for leg in L.LEGS:
                self.assertEqual(L.check(plan, leg), [], leg)

    def test_check_refuses_a_restart_plan_whose_sdk_probe_lacks_node_or_a_built_sdk(self):
        plan = H.Plan.load(os.path.join(HERE, "plan.example.json"))
        plan.doc["restart"]["sdk"]["node"] = "no-such-node"
        with mock.patch.object(L, "SDK_DIST", os.path.join(self.tmp, "absent.js")):
            self.assertEqual(L.check(plan, "restart"), [
                "restart.sdk: no node binary `no-such-node` (restart.sdk.node): the SDK probe runs on node",
                "restart.sdk: no built SDK at sdk/dist/index.js: cd sdk && npm ci && npm run build"])
        plan.doc["restart"]["sdk"]["node"] = sys.executable
        with self.built_sdk(age_s=60):
            self.assertEqual(L.check(plan, "restart"), [
                "restart.sdk: sdk/dist/index.js is older than sdk/src/index.ts: rebuild it (cd sdk && npm run build), "
                "so the probe runs this tree's SDK"])
            # Only the restart leg runs the SDK probe.
            self.assertEqual(L.check(plan, "rollback"), [])
        with self.built_sdk():
            self.assertEqual(L.check(plan, "restart"), [])

    def test_the_example_plan_runs_the_deploy_tools_with_flags_they_take_and_every_flag_they_require(self):
        plan = H.Plan.load(os.path.join(HERE, "plan.example.json"))
        deploys = [plan.get(k) for k in ("commands.deploy", "commands.restart", "oom.deploy", "oom.restore",
                                         "boot_failures.restore")]
        deploys += [c["deploy"] for c in plan.get("boot_failures.cases")]
        token = ["env", "PRISMA_TOKEN_FILE={home}/.streams-k2/lifecycle/platform-token.txt"]
        self.assertEqual({tuple(c[:3]) for c in deploys}, {(*token, "deploy/composer/deploy-release.sh")})
        secrets = [step for c in plan.get("boot_failures.cases") for step in (c.get("prepare"), c.get("restore")) if step]
        self.assertEqual([c[:5] for c in secrets], [[*token, "deploy/composer/set-secrets", "scratch", "--project"]] * 2)
        self.assertIn("--blank", secrets[0])
        self.assertEqual(plan.get("commands.restart")[-1], "--restart")
        tools = {"deploy/composer/deploy-release.sh": tool_flags("deploy/composer/deploy-release.ts", also=["state"]),
                 "deploy/composer/set-secrets": tool_flags("deploy/composer/set-secrets"),
                 "deploy/composer/cell-check": tool_flags("deploy/composer/cell-check")}
        for argv in [*deploys, *secrets, [*token, *plan.get("commands.cell_check")]]:
            accepted, required = tools[argv[2]]
            given = {a[2:] for a in argv if a.startswith("--")}
            self.assertEqual((given - accepted, required - given), (set(), set()), argv)
        docs = json.dumps(plan.doc)
        self.assertEqual([w for w in ("PLACEHOLDER", "--without-secret", "not in this tree", "neither exists") if w in docs], [])

    def test_check_refuses_evidence_in_the_repo_an_unknown_placeholder_and_oom_on_production(self):
        plan = H.Plan.load(os.path.join(HERE, "plan.example.json"))
        plan.doc.update(out=HERE, stage="production")
        plan.doc["commands"]["deploy"] = ["d", "{relase}"]
        got = L.check(plan, "oom")
        self.assertIn(f"out {os.path.realpath(HERE)} is inside the repository: evidence stays outside it (or under target/)", got)
        self.assertIn("oom never runs on the production stage", got)
        self.assertIn("command ['d', '{relase}']: no value for {relase}", got)
        del plan.doc["cell"]
        self.assertIn("`cell.url` is required", L.check(plan, "d10-cell"))


if __name__ == "__main__":
    unittest.main()
