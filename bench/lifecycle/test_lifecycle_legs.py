"""The lifecycle legs against fake platforms (deploy-q5 triage B6): each leg
of lifecycle.py run with a fake shell, compute CLI, cell and clock, so no
test sleeps; the commands it runs, the evidence it keeps and its verdict
are asserted. The verdicts and the plumbing are in test_lifecycle.py.

    python3 -m unittest discover -s bench/lifecycle -v
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import harness as H  # noqa: E402
import lifecycle as L  # noqa: E402
import verdicts as V  # noqa: E402
from test_lifecycle import AUDIT_OK, T0, boot_lines, lines_of, stop_lines, windows  # noqa: E402


class FakeClock:
    def __init__(self):
        self.t = T0
        self.probers: list = []
        self.ticks: list = []

    def now(self) -> int:
        return self.t

    def sleep(self, s: float) -> None:
        end = self.t + int(s * 1000)
        while self.t < end:
            due = min([p.next for p in self.probers if not p.stopped] + [end])
            self.t = max(due, self.t + 1) if due > self.t else self.t
            if self.t > end:
                self.t = end
            for p in self.probers:
                if not p.stopped and p.next <= self.t:
                    p.samples.append(p.fn())
                    p.next = self.t + int(p.every * 1000)
            for f in self.ticks:
                f(self.t)

    def prober(self, fn, every_s):
        p = FakeProber(fn, every_s, self.t)
        self.probers.append(p)
        return p


class FakeProber:
    def __init__(self, fn, every, t):
        self.fn, self.every, self.next, self.samples, self.stopped = fn, every, t, [], False

    def stop(self):
        self.stopped = True
        return list(self.samples)


class FakeStream:
    def __init__(self, compute, version, clock):
        self.c, self.v, self.clock, self.end = compute, version, clock, None

    def snapshot(self):
        return [[t, x] for t, x in self.c.logs_of[self.v] if t <= self.clock.now()]

    def wait_for(self, pred, timeout):
        deadline = self.clock.now() + timeout * 1000
        while not pred(self.snapshot()) and self.clock.now() < deadline:
            self.clock.sleep(1)
        return bool(pred(self.snapshot()))

    def wait_end(self, timeout):
        self.wait_for(lambda _: False, timeout)
        self.end = "timeout"
        return self.end

    def close(self, why="closed"):
        self.end = self.end or why


class FakeCompute:
    def __init__(self, clock, versions):
        self.clock, self.vers, self.logs_of, self.stopped = clock, list(versions), {v: [] for v in versions}, []

    def warm(self):
        pass

    def versions(self, service):
        return [{"id": v} for v in self.vers]

    def newest(self, service, exclude=(), timeout=0, sleep=None):
        fresh = [v for v in self.vers if v not in exclude]
        return fresh[-1] if fresh else None

    def add(self, version, lines, at=None):
        self.vers.append(version) if version not in self.vers else None
        self.logs_of.setdefault(version, []).extend((self.clock.now() if at is None else at, x) for x in lines)

    def logs(self, version, from_start=False, tail_lines=200):
        return FakeStream(self, version, self.clock)

    def stop(self, version):
        self.stopped.append(version)


class FakeShell:
    def __init__(self, hooks, clock=None):
        self.hooks, self.calls, self.stopped, self.clock = hooks, [], [], clock

    def run(self, name, argv, subs, timeout=None, cwd=None):
        cmd = H.fill(argv, subs)
        self.calls.append((name, cmd))
        hook = next((h for k, h in self.hooks.items() if name.startswith(k)), None)
        rc, tail = hook(cmd, subs) if hook else (0, "")
        return {"name": name, "rc": rc, "t0": 0, "t1": 0, "tail": tail}

    def start(self, name, argv, subs, cwd=None):
        """Recorded when started; the hook answers when the job is waited
        for, with the start time in subs["_started"]."""
        self.calls.append((name, H.fill(argv, subs)))
        hook = next((h for k, h in self.hooks.items() if name.startswith(k)), None)
        subs = {**subs, "_started": self.clock.now() if self.clock else 0}
        shell = self

        class Job:
            def wait(self, timeout=None):
                rc, tail = hook(H.fill(argv, subs), subs) if hook else (0, "")
                return {"name": name, "rc": rc, "t0": 0, "t1": 0, "tail": tail}

            def stop(self):
                shell.stopped.append(name)
        return Job()


def load_hook(clock):
    """A k2gen produce job: its window lines from its start until it was
    waited for, or until its --duration ran out, whichever came first."""
    def hook(cmd, subs):
        if "_started" in subs and "windows" in subs:
            t0 = subs["_started"]
            with open(subs["windows"], "w") as f:
                for w in windows(t0, min(t0 + int(subs["duration"]) * 1000, clock.now())):
                    f.write(json.dumps(w) + "\n")
        return 0, ""
    return hook


def write_audit(cmd, subs, ok=True):
    with open(subs["ledger"], "w") as f:
        json.dump({"verify": dict(AUDIT_OK["verify"], ok=ok)}, f)
    return 0, ""


class Legs(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="lifecycle-legs-")
        self.plan = H.Plan.load(os.path.join(HERE, "plan.example.json"))
        self.plan.doc["out"] = self.tmp
        self.clock = FakeClock()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def ctx(self, compute, hooks, cell) -> L.Ctx:
        return L.Ctx(self.plan, self.tmp, shell=FakeShell({"load": load_hook(self.clock), **hooks}, self.clock),
                     compute=compute, cell=cell, clock=self.clock, say=lambda _: None)

    def test_d10_cell_reads_the_old_stop_and_the_new_boot_and_audits_the_load(self):
        compute = FakeCompute(self.clock, ["v1"])
        compute.add("v1", boot_lines(T0 - 600_000, "b1"), at=T0 - 600_000)
        boots = ["b1"]

        def restart(cmd, subs):
            t = self.clock.now()
            compute.add("v2", boot_lines(t + 5_000, "b2"), at=t + 6_000)
            compute.add("v1", stop_lines(t + 2_000), at=t + 3_000)
            self.clock.ticks.append(lambda now: boots.append("b2") if now >= t + 4_000 and boots[-1] != "b2" else None)
            return 0, ""
        cell = mock.Mock()
        cell.health.side_effect = lambda: {"t": self.clock.now(), "status": 200, "boot": boots[-1]}
        ctx = self.ctx(compute, {"restart": restart, "d10-audit": write_audit}, cell)
        ev = L.d10_cell(ctx)
        self.assertEqual([n for n, _ in ctx.shell.calls], ["load-setup", "load", "restart", "d10-audit"])
        home = os.path.expanduser("~")
        self.assertEqual(ctx.shell.calls[2][1], [
            "env", f"PRISMA_TOKEN_FILE={home}/.streams-k2/lifecycle/platform-token.txt",
            "deploy/composer/deploy-release.sh", "scratch", os.path.join(home, ".streams-releases/l5-rehearsal-b"),
            "--project", "<the scratch project id>", "--branch", "<the scratch Branch id>", "--workspace", "<the workspace id>",
            "--state", f"{home}/.streams-k2/lifecycle/cell-state", "--cell-url", "https://<the scratch cell's service domain>",
            "--bearer-file", f"{home}/.streams-k2/lifecycle/operator-bearer.txt",
            "--canary-token-file", f"{home}/.streams-k2/lifecycle/canary-token.txt", "--restart"])
        audit_cmd = ctx.shell.calls[3][1]
        self.assertEqual(audit_cmd[audit_cmd.index("--expect") + 1], os.path.join(self.tmp, "d10-load.ledger.json"))
        self.assertEqual((ev["old_version"], ev["new_version"]), ("v1", "v2"))
        self.assertEqual(ctx.shell.stopped, ["load"])
        r = L.judge({"leg": "d10-cell", **ev})
        self.assertTrue(r.ok, r.render())
        self.assertIn("FACT d10-cell: clients saw 2 boot-id run(s) on /health: b1 -> b2", r.render())

    def test_d10_cell_keeps_its_load_past_a_deploy_that_blocks_longer_than_a_fixed_load_would_run(self):
        compute = FakeCompute(self.clock, ["v1"])
        compute.add("v1", boot_lines(T0 - 600_000, "b1"), at=T0 - 600_000)
        boots = ["b1"]

        def restart(cmd, subs):
            # A deploy that blocks 400 s (preflight, prisma deploy, cell-check);
            # the new build takes over 300 s in.
            t = self.clock.now()
            compute.add("v2", boot_lines(t + 280_000, "b2"), at=t + 281_000)
            compute.add("v1", stop_lines(t + 290_000), at=t + 291_000)
            self.clock.ticks.append(lambda now: boots.append("b2") if now >= t + 300_000 and boots[-1] != "b2" else None)
            self.clock.sleep(400)
            return 0, ""
        cell = mock.Mock()
        cell.health.side_effect = lambda: {"t": self.clock.now(), "status": 200, "boot": boots[-1]}
        ev = L.d10_cell(self.ctx(compute, {"restart": restart, "d10-audit": write_audit}, cell))
        self.assertEqual(ev["load"]["t_returned"] - ev["load"]["t_deploy"], 400_000)
        self.assertGreaterEqual(ev["load"]["windows"][-1]["t"] * 1000, ev["load"]["t_returned"] + 55_000)
        r = L.judge({"leg": "d10-cell", **ev})
        self.assertTrue(r.ok, r.render())
        self.assertTrue(any("under light load across the redeploy" in x and "appends acked after the switch" in x
                            for x in r.render()), r.render())

    def test_restart_probes_a_read_an_append_and_the_boot_id_every_tick_across_the_switch(self):
        compute = FakeCompute(self.clock, ["v1"])
        self.plan.doc["restart"].update(count=1, warmup_s=5, cooldown_s=20)
        boots = ["b1"]
        calls = []

        def restart(cmd, subs):
            t = self.clock.now()
            compute.add("v2", boot_lines(t + 3_000, "b2"), at=t + 4_000)
            self.clock.ticks.append(lambda now: boots.append("b2") if now >= t + 9_000 and boots[-1] != "b2" else None)
            return 0, ""

        def product(path, headers_file=None, method="GET", body=None):
            calls.append((method, path, body))
            return {"t": self.clock.now(), "status": 201 if method == "PUT" else 200, "code": None, "retry_after": None,
                    "boot": None}
        cell = mock.Mock()
        cell.product.side_effect = product
        cell.health.side_effect = lambda: {"t": self.clock.now(), "status": 200, "boot": boots[-1]}
        ev = L.restart(self.ctx(compute, {"restart": restart}, cell))
        self.assertEqual(calls[0], ("PUT", "/v1/streams/lc/restart-probe", b'{"format":{"kind":"json"}}'))
        self.assertEqual(calls[1][:2], ("GET", "/v1/streams/lc/s00000"))
        self.assertEqual(calls[2], ("POST", "/v1/streams/lc/restart-probe/records", b'{"lifecycle": "restart", "seq": 1}'))
        first = ev["samples"][0]
        self.assertEqual((first["t"], first["boot"], [a["kind"] for a in first["answers"]]), (T0, "b1", ["read", "append"]))
        self.assertEqual(ev["samples"][-1]["boot"], "b2")
        r = L.judge({"leg": "restart", **ev})
        self.assertTrue(r.ok, r.render())
        self.assertIn("PASS restart: restart 1: raw clients were without service 0.0 s across the switch (bar 10 s)", r.render())

    def test_rollback_deploys_the_sequence_and_a_fake_cross_layout_release_that_must_be_refused(self):
        for name in ("a", "b"):
            os.makedirs(os.path.join(self.tmp, name))
            with open(os.path.join(self.tmp, name, "release.json"), "w") as f:
                json.dump({"tag": name, "layout_version": 5}, f)
        self.plan.doc["rollback"].update(older=os.path.join(self.tmp, "a"), newer=os.path.join(self.tmp, "b"))
        self.plan.doc["out"] = self.tmp
        deployed = []
        boots = ["b0"]

        def deploy(cmd, subs):
            with open(os.path.join(subs["release"], "release.json")) as f:
                doc = json.load(f)
            if doc["layout_version"] != 5:
                return 1, f"deploy-release: refused: release layout_version {doc['layout_version']}, running 5"
            deployed.append(doc["tag"])
            # The deploy blocks 120 s; its build takes over 30 s in.
            b, t = f"b{len(deployed)}", self.clock.now() + 30_000
            self.clock.ticks.append(lambda now: boots.append(b) if now >= t and b not in boots else None)
            self.clock.sleep(120)
            return 0, ""
        cell = mock.Mock()
        cell.health.side_effect = lambda: {"t": self.clock.now(), "status": 200, "boot": boots[-1]}
        ctx = self.ctx(FakeCompute(self.clock, ["v1"]), {"deploy": deploy, "rollback-audit": write_audit}, cell)
        ev = L.rollback(ctx)
        self.assertEqual(deployed, ["a", "b", "a", "b"])
        audits = [c for n, c in ctx.shell.calls if n.startswith("rollback-audit")]
        self.assertEqual(audits[-1][audits[-1].index("--expect") + 1],
                         ",".join(os.path.join(self.tmp, f"rollback-{i}.ledger.json") for i in range(1, 5)))
        with open(os.path.join(self.tmp, "b", "release.json")) as f:
            self.assertEqual(json.load(f)["layout_version"], 5)
        self.assertEqual((ev["fake"]["from"], ev["fake"]["to"], ev["fake"]["rc"]), (5, 6, 1))
        r = L.judge({"leg": "rollback", **ev})
        self.assertTrue(r.ok, r.render())
        self.assertIn("PASS rollback: step 3: data written by newer read back by older", r.render())
        self.assertIn("PASS rollback: step 2: 145 appends acked by newer after it took over", r.render())
        self.assertEqual(ctx.shell.stopped, ["load-1", "load-2", "load-3", "load-4"])

    def test_rollback_claims_newer_data_only_when_the_newer_build_acked_some(self):
        ev = {"newer_label": "newer",
              "health": [{"t": T0 + k * 1000, "status": 200, "boot": "b0" if k < 100 else "b1"} for k in range(400)],
              "steps": [{"label": "newer", "deploy_rc": 0, "cell_check_rc": 0, "audit": AUDIT_OK, "t_deploy": T0 + 50_000,
                         "windows": windows(T0, T0 + 90_000)},
                        {"label": "older", "deploy_rc": 0, "cell_check_rc": 0, "audit": AUDIT_OK, "t_deploy": T0 + 200_000,
                         "windows": windows(T0 + 190_000, T0 + 300_000)}],
              "fake": {"rc": 1, "tail": "layout", "key": "layout_version", "from": 5, "to": 6, "boot_before": "b1",
                       "boot_after": "b1"}}
        got = lines_of(V.judge_rollback(ev))
        self.assertIn("FAIL rollback: step 1: no append was acked after newer took over: the load did not reach the newer build", got)
        self.assertIn("FAIL rollback: step 2: no append was acked after older took over (no new boot id on /health after the "
                      "deploy): the load did not reach the older build", got)
        self.assertFalse(any("data written by newer" in x for x in got), got)

    def test_rollback_refuses_a_release_without_the_layout_key_instead_of_testing_nothing(self):
        os.makedirs(os.path.join(self.tmp, "r"))
        with open(os.path.join(self.tmp, "r", "release.json"), "w") as f:
            json.dump({"tag": "r"}, f)
        ctx = self.ctx(FakeCompute(self.clock, ["v1"]), {}, mock.Mock())
        with self.assertRaisesRegex(L.LegError, "no integer `layout_version`"):
            L.cross_layout(ctx, os.path.join(self.tmp, "r"))

    def feeds_run(self, revoke_answers=None):
        """The feeds leg against a fake cell: the admit takes effect 30 s
        after its publish, the revoke 35 s after its own (or as
        `revoke_answers(seconds since the publish)` says)."""
        compute = FakeCompute(self.clock, ["v1"])
        # The last line before the leg: the operator's setup publish took effect.
        compute.add("v1", ["feeds updated: /tmp/feeds/{keys,policies,grants}.json gen=3"], at=T0 - 60_000)
        state = {"admitted": False, "revoked_t": None, "deleted_t": None, "good_t": None}

        def publish(cmd, subs):
            t, what = self.clock.now(), cmd[-1]
            if what == "admit":
                self.clock.ticks.append(lambda now, t=t: state.update(admitted=True) if now >= t + 30_000 and now < t + 31_000 else None)
            elif what == "revoke":
                state["revoked_t"] = t
            elif what == "malformed":
                compute.add("v1", ["feeds poll failed: feeds bundle refused: not JSON",
                                   "feeds deleted: no successful poll for 124 s"], at=t + 115_000)
                state["deleted_t"] = t + 115_000
            elif what == "good":
                state["good_t"] = t
            return 0, ""

        def product(path, hdrs=None):
            now = self.clock.now()
            if hdrs and hdrs.endswith("headers-feeds-probe.txt"):
                if state["revoked_t"] is not None:
                    since = (now - state["revoked_t"]) / 1000
                    if revoke_answers:
                        return {"t": now, **revoke_answers(since)}
                    if since >= 35:
                        return {"t": now, "status": 403, "code": "credential_not_active"}
                return {"t": now, "status": 404} if state["admitted"] else {"t": now, "status": 401, "code": "credential_unknown"}
            if state["good_t"] and now >= state["good_t"] + 20_000:
                return {"t": now, "status": 404}
            if state["deleted_t"] and now >= state["deleted_t"] + 301_000:
                return {"t": now, "status": 503, "code": "policy_stale"}
            return {"t": now, "status": 404}
        cell = mock.Mock()
        cell.product.side_effect = product
        return L.feeds(self.ctx(compute, {"publish": publish}, cell))

    def test_a_transient_5xx_or_transport_failure_after_a_revoke_is_not_its_effect(self):
        def answers(since):
            if 5 <= since < 6:
                return {"status": 502, "code": None}
            if 10 <= since < 11:
                return {"status": 0, "code": "URLError"}
            if since >= 50:
                return {"status": 403, "code": "credential_not_active"}
            return {"status": 404}
        ev = self.feeds_run(answers)
        self.assertEqual(ev["revoke"]["effect"], {"t": ev["revoke"]["t_pub"] + 50_000, "status": 403, "code": "credential_not_active"})
        self.assertIn("FAIL feeds: revoke: effect 50.0 s after the publish, over the 45 s bar",
                      lines_of(L.judge({"leg": "feeds", **ev})))

    def test_feeds_times_publishes_and_the_fuse_from_the_log_and_the_probe(self):
        ev = self.feeds_run()
        self.assertEqual([t for _, t in ev["quiet"]["tail"]], ["feeds updated: /tmp/feeds/{keys,policies,grants}.json gen=3"])
        self.assertEqual(ev["quiet"]["lines"], [])
        self.assertEqual(ev["malformed"]["deleted"], {"after_s": 124, "t": ev["malformed"]["t_pub"] + 115_000})
        r = L.judge({"leg": "feeds", **ev})
        self.assertTrue(r.ok, r.render())
        self.assertIn("PASS feeds: admit: effect 30.0 s after the publish (bar 45 s)", r.render())
        self.assertIn("PASS feeds: revoke: effect 35.0 s after the publish (bar 45 s)", r.render())
        self.assertIn("PASS feeds: the presigned conditional GET works against the store: nothing published, nothing "
                      "rewritten (each poll a 304)", r.render())
        self.assertIn("PASS feeds: malformed bundle: 503 policy_stale 305 s after the deletion (about 300 s)", r.render())

    def test_boot_failures_reads_the_broken_versions_log_although_the_deploy_command_failed(self):
        compute = FakeCompute(self.clock, ["v1"])
        self.plan.doc["boot_failures"]["cases"] = self.plan.doc["boot_failures"]["cases"][:1]
        state = {"broken": False}

        def broken(cmd, subs):
            # deploy-release.sh: `prisma deploy` made the version, then cell-check failed.
            compute.add("v2", ["entry refused: secret STREAMS_CURSOR_KEY unset"], at=self.clock.now() + 2_000)
            state["broken"] = True
            return 1, "cell-check: /health answered 500 for 300 s"

        def restore(cmd, subs):
            state["broken"] = False
            return 0, ""
        cell = mock.Mock()
        cell.raw.side_effect = lambda path: ({"t": self.clock.now(), "status": 500, "body": json.dumps(V.GENERIC_500)}
                                             if state["broken"] else {"t": self.clock.now(), "status": 200, "body": "ok"})
        cell.health.side_effect = lambda: {"t": self.clock.now(), "status": 500 if state["broken"] else 200, "boot": None}
        ev = L.boot_failures(self.ctx(compute, {"deploy-missing-secret": broken, "restore": restore}, cell))
        self.assertEqual((ev["cases"][0]["deploy_rc"], ev["cases"][0]["version"]), (1, "v2"))
        self.assertEqual(lines_of(L.judge({"leg": "boot-failures", **ev})), [
            "FACT boot-failures: missing-secret: the deploy command exited 1",
            "PASS boot-failures: missing-secret: the generic 500 binary_exited, no detail in the body",
            "PASS boot-failures: missing-secret: the detail is in the log: entry refused: secret STREAMS_CURSOR_KEY unset",
            "PASS boot-failures: missing-secret: the good release serves again",
            "LEG_PASS boot-failures",
        ])

    def test_boot_failures_runs_a_cases_prepare_before_its_deploy_and_its_restore_before_the_good_release(self):
        self.plan.doc["boot_failures"]["cases"] = self.plan.doc["boot_failures"]["cases"][:1]
        home = os.path.expanduser("~")
        token = ["env", f"PRISMA_TOKEN_FILE={home}/.streams-k2/lifecycle/platform-token.txt", "deploy/composer/set-secrets"]
        scope = ["scratch", "--project", "<the scratch project id>", "--branch", "<the scratch Branch id>"]
        cell = mock.Mock()
        cell.raw.side_effect = lambda path: {"t": self.clock.now(), "status": 500, "body": json.dumps(V.GENERIC_500)}
        cell.health.side_effect = lambda: {"t": self.clock.now(), "status": 200, "boot": None}

        def run(hooks):
            compute = FakeCompute(self.clock, ["v1"])
            hooks = {"deploy-": lambda cmd, subs: (compute.add("v2", ["refused"], at=self.clock.now() + 1_000), (1, ""))[1],
                     **hooks}
            ctx = self.ctx(compute, hooks, cell)
            try:
                return ctx, L.boot_failures(ctx)
            except L.LegError as e:
                return ctx, e
        ctx, ev = run({})
        self.assertEqual([(n, c[:8]) for n, c in ctx.shell.calls], [
            ("prepare-missing-secret", [*token, *scope]),
            ("deploy-missing-secret", ctx.shell.calls[1][1][:8]),
            ("case-restore-missing-secret", [*token, *scope]),
            ("restore-missing-secret", ctx.shell.calls[3][1][:8]),
        ])
        self.assertEqual([ctx.shell.calls[0][1][8:], ctx.shell.calls[2][1][8:], ev["cases"][0]["restored"]], [
            ["--blank", "STREAMS_CURSOR_KEY"], ["--from", f"{home}/.streams-k2/lifecycle/scratch-secrets"], True,
        ])
        ctx, ev = run({"case-restore-": lambda cmd, subs: (1, "set-secrets: refused")})
        self.assertEqual(ev["cases"][0]["restored"], False)
        ctx, err = run({"prepare-": lambda cmd, subs: (1, "set-secrets: refused")})
        self.assertEqual([str(err)[:40], [n for n, _ in ctx.shell.calls]], [
            "prepare-missing-secret exited 1: set-sec", ["prepare-missing-secret"],
        ])

        def lost(cmd, subs):
            raise L.LegError("the platform went away")
        ctx, err = run({"deploy-": lost})
        self.assertEqual([str(err), [n for n, _ in ctx.shell.calls]], [
            "the platform went away", ["prepare-missing-secret", "deploy-missing-secret", "case-restore-missing-secret"],
        ])

    def test_destructive_legs_refuse_the_production_stage_before_any_action(self):
        self.plan.doc["stage"] = "production"
        for leg in (L.oom, L.boot_failures):
            shell = FakeShell({})
            ctx = L.Ctx(self.plan, self.tmp, shell=shell, compute=FakeCompute(self.clock, ["v1"]), cell=mock.Mock(),
                        clock=self.clock, say=lambda _: None)
            with self.assertRaisesRegex(H.PlanError, "never the production stage"):
                leg(ctx)
            self.assertEqual(shell.calls, [])

    def test_a_leg_that_fails_keeps_its_evidence_and_stops_its_load_streams_and_probers(self):
        compute = FakeCompute(self.clock, ["v1"])
        compute.add("v1", boot_lines(T0 - 600_000, "b1"), at=T0 - 600_000)
        cell = mock.Mock()
        cell.health.side_effect = lambda: {"t": self.clock.now(), "status": 200, "boot": "b1"}
        # The restart deploys nothing new, so d10-cell fails after its load started.
        ctx = self.ctx(compute, {}, cell)
        path = L.run_leg(ctx, "d10-cell")
        with open(path) as f:
            ev = json.load(f)
        self.assertEqual(ev["error"], f"no new version of service {self.plan.get('cell.service_id')}")
        self.assertEqual((ev["leg"], ev["old_version"]), ("d10-cell", "v1"))
        self.assertGreaterEqual(len(ev["health"]), 100)
        self.assertEqual(ev["health"][0], {"t": T0, "status": 200, "boot": "b1"})
        self.assertEqual([t for _, t in ev["old_lines"]], boot_lines(T0 - 600_000, "b1"))
        self.assertEqual(ctx.shell.stopped, ["load"])
        self.assertTrue(all(p.stopped for p in self.clock.probers))
        self.assertEqual(L.judge(ev).render(), [f"FAIL d10-cell: the run stopped: {ev['error']}", "LEG_FAIL d10-cell"])

    def test_an_unexpected_failure_still_saves_the_evidence(self):
        compute = FakeCompute(self.clock, ["v1"])
        compute.versions = mock.Mock(side_effect=subprocess.TimeoutExpired(["compute", "versions"], 120))
        path = L.run_leg(self.ctx(compute, {}, mock.Mock()), "restart")
        with open(path) as f:
            ev = json.load(f)
        self.assertEqual(ev, {"leg": "restart", "error": "TimeoutExpired: Command '['compute', 'versions']' timed out after 120 seconds"})

    def test_judge_reads_saved_evidence_and_a_stopped_run_fails(self):
        path = L.save(self.tmp, "restart", {"error": "restart-1 exited 2: boom"})
        with mock.patch("builtins.print") as out:
            self.assertEqual(L.main(["judge", path]), 1)
        self.assertEqual(out.call_args[0][0], "FAIL restart: the run stopped: restart-1 exited 2: boom\nLEG_FAIL restart")


if __name__ == "__main__":
    unittest.main()
