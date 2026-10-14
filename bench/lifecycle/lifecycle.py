#!/usr/bin/env python3
"""The Compute lifecycle checklist (deploy-q5 design §3.4, legs 1-8; triage
item B6): each leg drives a cell (or the SIGTERM probe) through one platform
event, keeps the evidence, and prints PASS, FAIL, FACT and FINDING lines.

    lifecycle.py <leg> --plan PLAN.json        run a leg; evidence under the plan's `out`
    lifecycle.py check <leg> --plan PLAN.json  validate the plan for a leg, offline
    lifecycle.py judge EVIDENCE.json           re-judge a saved run, offline

Legs: d10-probe (1: the platform's stop semantics, on the probe), d10-cell
(2: the cell's stop under light load), restart (3), rollback (4), oom (5,
scratch only, non-shipping config), feeds (6), boot-failures (7),
keep-awake (8). Leg 9, the platform export comparison, runs with the
effective-config family on the target. The last line is LEG_PASS or
LEG_FAIL; the exit code is 0 only for LEG_PASS.

The plan (plan.example.json documents every key) names the cell, the
operator commands each leg runs (deploy-release.sh, cell-check, k2gen; as
argv lists with {placeholders}) and the files that hold secrets. Runs need a
deployed cell: the scratch project of owner question D1 (deploy-q5 Q1) for
the rehearsal, the RC and the target for certification. README.md says
what each leg does.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import sys
import time
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import harness as H  # noqa: E402
import verdicts as V  # noqa: E402

PROBE_DEPLOY = ["bunx", "prisma", "deploy", "app.ts", "--name", "{app}", "--stage", "{probe_stage}", "--report", "{report}"]
#: A load's longest run (k2gen --duration): a leg stops it LOAD_TAIL_S after
#: its deploy command returns, so the load spans the switch however long the
#: deploy blocks (deploy-release.sh runs its preflight, `prisma deploy` and
#: cell-check, up to 5 min).
LOAD_MAX_S = 3600
LOAD_TAIL_S = 60


class LegError(RuntimeError):
    """A platform step of a leg failed; the evidence so far is kept (run_leg)."""


class RealClock:
    def now(self) -> int:
        return H.now_ms()

    def sleep(self, s: float) -> None:
        time.sleep(s)

    def prober(self, fn, every_s: float):
        return H.Prober(fn, every_s)


class Ctx:
    """What a leg runs with; tests replace the shell, compute, cell and clock."""

    def __init__(self, plan: H.Plan, out: str, shell=None, compute=None, cell=None, clock=None, say=print):
        self.plan, self.out, self.say = plan, out, say
        self.shell = shell or H.Shell(plan, out, say)
        self.compute = compute or H.Compute(plan)
        self.cell = cell or H.Cell(plan)
        self.clock = clock or RealClock()
        # Background work a leg starts (load jobs, log streams, probers):
        # close() stops all of it however the leg ends.
        self._open: list = []

    def now(self) -> int:
        return self.clock.now()

    def sleep(self, s: float) -> None:
        self.clock.sleep(s)

    def _track(self, x):
        self._open.append(x)
        return x

    def prober(self, fn, every_s: float):
        return self._track(self.clock.prober(fn, every_s))

    def logs(self, version: str, **kw):
        """`compute logs` of `version`, closed when the leg ends."""
        return self._track(self.compute.logs(version, **kw))

    def start(self, name: str, argv: list, **extra):
        """A background command, stopped when the leg ends."""
        return self._track(self.shell.start(name, argv, self.subs(**extra)))

    def close(self) -> None:
        """Stops every job, log stream and prober the leg started, newest
        first; each stop is idempotent, so what the leg stopped is a no-op."""
        while self._open:
            x = self._open.pop()
            try:
                (getattr(x, "close", None) or x.stop)()
            except Exception as e:  # noqa: BLE001 - one stuck stop must not leave the rest running
                self.say(f"  stopping {type(x).__name__} failed: {e}")

    def until(self, pred, timeout_s: float, step_s: float = 1.0) -> bool:
        deadline = self.now() + timeout_s * 1000
        while not pred():
            if self.now() >= deadline:
                return False
            self.sleep(step_s)
        return True

    def subs(self, **extra) -> dict:
        base = {"stage": self.plan.get("stage"), "out": self.out, "url": self.plan.get("cell.url", ""),
                "release": os.path.expanduser(self.plan.get("release", "")), "home": os.path.expanduser("~")}
        return {**base, **extra}

    def run(self, name: str, argv: list, timeout: float = 1800, must: bool = True, **extra) -> dict:
        r = self.shell.run(name, argv, self.subs(**extra), timeout=timeout)
        if must and r["rc"] != 0:
            raise LegError(f"{name} exited {r['rc']}: {r['tail'][-400:]}")
        return r

    def versions(self) -> set:
        return {v["id"] for v in self.compute.versions(self.plan.get("cell.service_id"))}

    def newest(self, exclude=(), timeout: float = 300) -> str:
        v = self.compute.newest(self.plan.get("cell.service_id"), exclude, timeout=timeout)
        if not v:
            raise LegError(f"no new version of service {self.plan.get('cell.service_id')}")
        return v


# ---------------------------------------------------------------- shared steps

def has_listen(lines) -> bool:
    return any(V.BIN_LISTEN.match(m) for _, _, m in V.binary_lines(lines))


def setup_load(ctx: Ctx) -> None:
    if ctx.plan.get("load.setup", None):
        ctx.run("load-setup", ctx.plan.get("load.setup"))


def windows_of(ledger: str) -> str:
    return ledger[: -len(".ledger.json")] + ".windows.jsonl" if ledger.endswith(".ledger.json") else ledger + ".windows.jsonl"


def start_load(ctx: Ctx, name: str, ledger: str, seed: int, duration: float, argv=None):
    return ctx.start(name, argv or ctx.plan.get("load.produce"), ledger=ledger, seed=seed, duration=int(duration),
                     windows=windows_of(ledger))


def read_windows(path: str) -> list:
    """k2gen's --out window lines (t: the window's end, in s), trimmed to
    what the verdicts read."""
    out = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    w = json.loads(line)
                except ValueError:
                    continue
                out.append({k: w.get(k) for k in ("t", "window_s", "acked_requests", "failed_requests")})
    except OSError:
        pass
    return out


def stop_load(ctx: Ctx, load, returned_at: int, tail_s: float) -> dict:
    """Keeps a load running `tail_s` past its deploy command's return, then
    stops it (k2gen writes its ledger on SIGTERM)."""
    left = returned_at + tail_s * 1000 - ctx.now()
    if left > 0:
        ctx.sleep(left / 1000)
    load.stop()
    return load.wait(timeout=180)


def audit(ctx: Ctx, ledgers: list, name: str) -> dict:
    """k2gen audit over `ledgers`; its verify block from the ledger it writes."""
    out = os.path.join(ctx.out, f"{name}.json")
    r = ctx.run(name, ctx.plan.get("load.audit"), timeout=7200, must=False, ledgers=",".join(ledgers), ledger=out)
    try:
        with open(out, encoding="utf-8") as f:
            verify = json.load(f).get("verify")
    except (OSError, ValueError):
        verify = None
    return {"rc": r["rc"], "verify": verify}


def refuse_production(ctx: Ctx, leg: str) -> None:
    if ctx.plan.get("stage") == "production":
        raise H.PlanError(f"{leg} deploys a deliberately broken or non-shipping cell: never the production stage")


# ---------------------------------------------------------------- leg 1

def d10_probe(ctx: Ctx, ev: dict | None = None) -> dict:
    """Deploy the probe, redeploy it, stop the new version; read both stops."""
    ev = {} if ev is None else ev
    p = ctx.plan
    pdir = os.path.expanduser(p.get("probe.dir", os.path.join(H.HERE, "sigterm-probe")))
    window = p.get("probe.read_window_s", 600)
    subs = {"app": p.get("app", H.SCRATCH_APP), "probe_stage": p.get("probe.stage", "sigterm-probe")}
    ctx.compute.warm()
    service = [p.get("probe.service_id", None)]

    def versions() -> set:
        return {v["id"] for v in ctx.compute.versions(service[0])} if service[0] else set()

    def deploy(n: int):
        before = versions()
        nonce = f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime(ctx.now() / 1000))}-{n}"
        with open(os.path.join(pdir, "app", "nonce.txt"), "w", encoding="utf-8") as f:
            f.write(nonce + "\n")
        r = ctx.shell.run(f"probe-deploy-{n}", p.get("probe.deploy", PROBE_DEPLOY),
                          {**ctx.subs(), **subs, "report": os.path.join(ctx.out, f"probe-report-{n}.json")},
                          timeout=1800, cwd=pdir)
        if r["rc"] != 0:
            raise LegError(f"probe deploy {n} exited {r['rc']}: {r['tail'][-400:]}")
        if not service[0]:
            service[0] = ctx.compute.service_named(p.get("probe.project_id"), p.get("probe.service_name", "sigtermprobe"))
        v = ctx.compute.newest(service[0], before, timeout=300)
        if not v:
            raise LegError(f"probe deploy {n}: no new version of {service[0]}")
        s = ctx.logs(v, from_start=True)
        want = f'nonce="{nonce}"'
        if not s.wait_for(lambda ls: any("sigterm-probe main boot" in t and want in t for t in V.texts(ls)), 300):
            raise LegError(f"probe version {v}: no boot line with nonce {nonce}")
        return v, s

    ev["read_window_s"] = window
    v1, s1 = deploy(1)
    red = ev["redeploy"] = {"old_version": v1, "old": s1}
    v2, s2 = deploy(2)
    red.update(new_version=v2, new=s2)
    red["log_end"] = s1.wait_end(window)
    red["new"] = s2.snapshot()
    stop = ev["stop"] = {"old_version": v2, "old": s2}
    ctx.compute.stop(v2)
    stop["log_end"] = s2.wait_end(window)
    red["old"], stop["old"] = s1.snapshot(), s2.snapshot()
    return ev


# ---------------------------------------------------------------- leg 2

def d10_cell(ctx: Ctx, ev: dict | None = None) -> dict:
    """Redeploy the cell under light load; read the old process's stop and the new one's boot."""
    ev = {} if ev is None else ev
    p = ctx.plan
    ctx.compute.warm()
    before = ctx.versions()
    v_old = ev["old_version"] = ctx.newest()
    setup_load(ctx)
    s_old = ev["old_lines"] = ctx.logs(v_old, tail_lines=50)
    health = ev["health"] = ctx.prober(ctx.cell.health, 0.5)
    ledger = os.path.join(ctx.out, "d10-load.ledger.json")
    load = start_load(ctx, "load", ledger, 1, p.get("load.max_s", LOAD_MAX_S))
    ctx.sleep(p.get("d10.warmup_s", 60))
    t_deploy = ctx.now()
    ctx.run("restart", p.get("commands.restart"))
    t_returned = ctx.now()
    v_new = ev["new_version"] = ctx.newest(before)
    s_new = ev["new_lines"] = ctx.logs(v_new, from_start=True)
    s_old.wait_for(lambda ls: V.first(ls, V.WRAPPER_STOPPED) or V.first(ls, V.WRAPPER_KILLED), 240)
    s_new.wait_for(has_listen, 300)
    s_old.close()
    s_new.close()
    lr = stop_load(ctx, load, t_returned, p.get("d10.tail_s", LOAD_TAIL_S))
    ev["load"] = {"rc": lr["rc"], "t_deploy": t_deploy, "t_returned": t_returned, "windows": read_windows(windows_of(ledger))}
    ev["health"] = health.stop()
    ev["old_lines"], ev["new_lines"] = s_old.snapshot(), s_new.snapshot()
    ev["audit"] = audit(ctx, [ledger], "d10-audit")
    return ev


# ---------------------------------------------------------------- leg 3

def stream_path(name: str) -> str:
    return "/v1/streams/" + "/".join(urllib.parse.quote(part, safe="") for part in name.split("/"))


#: The SDK probe, and the shipping SDK it imports (sdk/dist, built from this tree).
SDK_PROBE = os.path.join(H.HERE, "sdk-probe.mjs")
SDK_DIST = os.path.join(H.ROOT, "sdk", "dist", "index.js")


def read_jsonl(path: str) -> list:
    out = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    out.append(json.loads(line))
                except ValueError:
                    continue
    except OSError:
        pass
    return out


class SdkProbe:
    """sdk-probe.mjs, run by the restart leg beside the raw prober: the
    shipping SDK's read, plain append and producer append in turn, one JSON
    line per call in the run's sdk-calls.jsonl."""

    def __init__(self, ctx: Ctx):
        p = ctx.plan
        headers = os.path.expanduser(p.get("restart.sdk.headers_file"))
        # The SDK's token and key: a header file only its owner can read
        # (the probe reads it again for each token and refuses it too).
        p.secret(headers)
        self.ctx, self.path = ctx, os.path.join(ctx.out, "sdk-calls.jsonl")
        self.job = ctx.start("sdk-probe", [
            p.get("restart.sdk.node", "node"), SDK_PROBE, "--url", "{url}", "--stream", p.get("restart.sdk.stream"),
            "--headers-file", headers, "--out", self.path, "--producer-id", f"lifecycle-{os.path.basename(ctx.out)}",
            "--state", os.path.join(ctx.out, "sdk-producer-state.json"),
            "--pause-ms", str(int(p.get("restart.sdk.pause_s", 0.25) * 1000))])

    def snapshot(self) -> dict:
        return {"calls": read_jsonl(self.path)}

    def ready(self, timeout_s: float) -> None:
        """Until the probe's first successful call: a probe that cannot be
        served before the first restart (a refused token, a missing key, a
        stream it cannot create) names the plan, never the new build."""
        served = lambda: any(c.get("ok") or c.get("setup") == "failed" for c in read_jsonl(self.path))  # noqa: E731
        self.ctx.until(served, timeout_s, 0.25)
        lines = read_jsonl(self.path)
        if not any(c.get("ok") for c in lines):
            last = next((c for c in reversed(lines) if c.get("error")), None)
            why = V.sdk_failure(last) if last else "no call ended (its log is the run's cmd-*-sdk-probe.log)"
            raise LegError(f"the SDK probe made no successful call within {timeout_s} s: {why}: restart.sdk must name "
                           "a stream and a header file the cell serves")

    def stop(self) -> dict:
        t_stop = self.ctx.now()
        self.job.stop()
        self.job.wait(timeout=30)
        return {"t_stop": t_stop, "calls": read_jsonl(self.path)}


def restart(ctx: Ctx, ev: dict | None = None) -> dict:
    """The SC1-awake redeploys, timed by a raw product client: every tick a
    read of restart.probe_path, an append to restart.append_stream (so a
    fenced write's 500 shows) and /health, whose boot id says which build
    was answering then (product answers carry none). Beside it the shipping
    SDK (SdkProbe) calls a read, a plain append and a producer append in
    turn: D4's bar is the SDK's."""
    ev = {} if ev is None else ev
    p = ctx.plan
    ctx.compute.warm()
    known = ctx.versions()
    path = p.get("restart.probe_path")
    stream = stream_path(p.get("restart.append_stream"))
    made = ctx.cell.product(stream, method="PUT", body=b'{"format":{"kind":"json"}}')
    if made["status"] not in (200, 201):
        raise LegError(f"creating the append probe's stream {stream} answered {made['status']} {made.get('code')}")
    # The read probe's stream is the plan's (k2gen's setup may create it
    # with its own config), so the leg checks it rather than creating it:
    # a missing one would fail every tick and read as the build's fault.
    got = ctx.cell.product(path)
    if not 200 <= got["status"] < 300:
        raise LegError(f"restart.probe_path {path} answered {got['status']} {got.get('code')}: the read probe's stream "
                       "must exist before the leg (load.setup creates the example's; or name an existing stream)")
    sdk = ev["sdk"] = SdkProbe(ctx)
    sdk.ready(p.get("restart.sdk.ready_s", 60))
    seq = [0]

    def tick() -> dict:
        # t_end: when the product answers completed, so the verdict's
        # window counts a 2xx that took seconds as time without service.
        t = ctx.now()
        seq[0] += 1
        read = dict(ctx.cell.product(path), kind="read")
        append = dict(ctx.cell.product(stream + "/records", method="POST",
                                       body=json.dumps({"lifecycle": "restart", "seq": seq[0]}).encode()), kind="append")
        t_end = ctx.now()
        return {"t": t, "t_end": t_end, "boot": ctx.cell.health().get("boot"), "answers": [read, append]}
    every = ev["probe_every_s"] = p.get("restart.probe_every_s", 0.25)
    prober = ev["samples"] = ctx.prober(tick, every)
    restarts = ev["restarts"] = []
    for i in range(1, p.get("restart.count", 3) + 1):
        ctx.sleep(p.get("restart.spacing_s", 300) if i > 1 else p.get("restart.warmup_s", 30))
        t = ctx.now()
        r = ctx.run(f"restart-{i}", p.get("commands.restart"), must=False)
        v, lines = None, []
        if r["rc"] == 0:
            v = ctx.newest(known)
            known.add(v)
            s = ctx.logs(v, from_start=True)
            s.wait_for(has_listen, 300)
            s.close()
            lines = s.snapshot()
        restarts.append({"t_trigger": t, "t_done": ctx.now(), "rc": r["rc"], "new_version": v, "new_lines": lines})
    ctx.sleep(p.get("restart.cooldown_s", 60))
    ev["samples"] = prober.stop()
    ev["sdk"] = sdk.stop()
    return ev


# ---------------------------------------------------------------- leg 4

def rollback(ctx: Ctx, ev: dict | None = None) -> dict:
    """older -> newer -> older -> newer under load, then a cross-layout release that must be refused."""
    ev = {} if ev is None else ev
    p = ctx.plan
    releases = {"older": os.path.expanduser(p.get("rollback.older")), "newer": os.path.expanduser(p.get("rollback.newer"))}
    seq = p.get("rollback.sequence", ["older", "newer", "older", "newer"])
    setup_load(ctx)
    steps, ledgers = [], []
    ev.update(steps=steps, newer_label="newer")
    # /health throughout: its boot id says when each deploy's build took over.
    health = ev["health"] = ctx.prober(ctx.cell.health, p.get("rollback.health_every_s", 1))
    try:
        for i, label in enumerate(seq, 1):
            ledger = os.path.join(ctx.out, f"rollback-{i}.ledger.json")
            load = start_load(ctx, f"load-{i}", ledger, i, p.get("load.max_s", LOAD_MAX_S))
            ctx.sleep(p.get("rollback.warmup_s", 60))
            t_deploy = ctx.now()
            d = ctx.run(f"deploy-{i}-{label}", p.get("commands.deploy"), must=False, release=releases[label])
            t_returned = ctx.now()
            lr = stop_load(ctx, load, t_returned, p.get("rollback.tail_s", LOAD_TAIL_S))
            ledgers.append(ledger)
            step = {"label": label, "release": releases[label], "deploy_rc": d["rc"], "load_rc": lr["rc"],
                    "t_deploy": t_deploy, "t_returned": t_returned, "windows": read_windows(windows_of(ledger))}
            steps.append(step)
            if d["rc"] != 0:
                return ev
            step["cell_check_rc"] = ctx.run(f"cell-check-{i}", p.get("commands.cell_check"), timeout=600, must=False,
                                            release=releases[label])["rc"]
            step["audit"] = audit(ctx, ledgers, f"rollback-audit-{i}")
        ev["fake"] = cross_layout(ctx, releases[seq[-1]])
    finally:
        ev["health"] = health.stop()
    return ev


def cross_layout(ctx: Ctx, release: str) -> dict:
    """A copy of `release` whose release.json names another layout: deploying it must be refused."""
    key = ctx.plan.get("rollback.layout_key", "layout_version")
    fake = os.path.join(ctx.out, "fake-cross-layout")
    shutil.copytree(release, fake, symlinks=True)
    path = os.path.join(fake, "release.json")
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    if not isinstance(doc.get(key), int):
        raise LegError(f"{release}/release.json has no integer `{key}` (rollback.layout_key)")
    was = doc[key]
    doc[key] = was + 1
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=1)
    boot = ctx.cell.health().get("boot")
    r = ctx.run("deploy-cross-layout", ctx.plan.get("commands.deploy"), must=False, release=fake)
    return {"rc": r["rc"], "tail": r["tail"][-2000:], "key": key, "from": was, "to": was + 1, "boot_before": boot,
            "boot_after": ctx.cell.health().get("boot"), "pattern": ctx.plan.get("rollback.refusal_pattern", "(?i)layout")}


# ---------------------------------------------------------------- leg 5

def oom(ctx: Ctx, ev: dict | None = None) -> dict:
    """Scratch only: the non-shipping config under incompressible load until the kernel kills the binary."""
    ev = {} if ev is None else ev
    p = ctx.plan
    refuse_production(ctx, "oom")
    if p.get("oom.scratch") is not True:
        raise H.PlanError("oom runs a non-shipping config: the plan must say oom.scratch: true")
    ctx.compute.warm()
    ctx.run("deploy-oom", p.get("oom.deploy"))
    try:
        oom_run(ctx, ev)
    finally:
        ev["restore_rc"] = ctx.run("restore", p.get("oom.restore"), must=False)["rc"]
    return ev


def oom_run(ctx: Ctx, ev: dict) -> dict:
    p = ctx.plan
    if not ctx.until(lambda: ctx.cell.health()["status"] == 200, 600, 5):
        raise LegError("the OOM stage never answered /health 200")
    old_boot = ev["old_boot"] = ctx.cell.health().get("boot")
    s = ev["lines"] = ctx.logs(ctx.newest(), tail_lines=50)
    health = ev["health"] = ctx.prober(ctx.cell.health, 1)
    ledger = os.path.join(ctx.out, "oom-load.ledger.json")
    t0 = ctx.now()
    load = start_load(ctx, "oom-load", ledger, 1, p.get("oom.timeout_s", 1800), p.get("oom.load", None))
    down = lambda: next((x for x in health.samples if x["t"] >= t0 and x["status"] != 200), None)  # noqa: E731
    ctx.until(lambda: down() is not None, p.get("oom.timeout_s", 1800))
    kill = down()
    load.stop()
    load.wait(timeout=180)
    back = lambda: next((x for x in health.samples if kill and x["t"] > kill["t"] and x["status"] == 200  # noqa: E731
                         and x.get("boot") not in (None, old_boot)), None)
    ctx.until(lambda: back() is not None, p.get("oom.recover_s", 900))
    ok = back()
    ctx.sleep(p.get("oom.stable_s", 600))
    samples = health.stop()
    s.close()
    ev.update({"kill_t": kill and kill["t"], "new_boot": ok and ok["boot"],
               "first_ok_t": ok and ok["t"], "lines": s.snapshot(), "health": samples,
               "stable": {"seconds": p.get("oom.stable_s", 600),
                          "boots": [x["boot"] for x in samples if ok and x["t"] >= ok["t"] and x.get("boot")]}})
    ev["audit"] = audit(ctx, [ledger], "oom-audit")
    return ev


# ---------------------------------------------------------------- leg 6

def feeds(ctx: Ctx, ev: dict | None = None) -> dict:
    """Steady 304s, publish-to-effect for an admit and a revoke, a malformed bundle and its repair."""
    ev = {} if ev is None else ev
    p = ctx.plan
    ctx.compute.warm()
    s = ev["log"] = ctx.logs(ctx.newest(), tail_lines=1)
    # The CLI replays the last line written before the leg began (--tail
    # takes at least 1): wait for it, and judge only the lines after it.
    s.wait_for(lambda ls: bool(ls), p.get("feeds.tail_wait_s", 30))
    tail = s.snapshot()
    t0 = ctx.now()
    ctx.sleep(p.get("feeds.quiet_s", 120))
    ev["quiet"] = {"seconds": (ctx.now() - t0) / 1000, "tail": tail, "lines": s.snapshot()[len(tail):]}
    path = p.get("feeds.probe_path", "/v1/streams/lifecycle-feeds-probe")
    timeout = p.get("feeds.effect_timeout_s", 120)
    revoked_codes = tuple(p.get("feeds.revoked_codes", V.REVOKED_CODES))
    for what in ("admit", "revoke"):
        hdrs = p.get("feeds.admit_headers_file")
        before = ctx.cell.product(path, hdrs)["status"]
        ctx.run(f"publish-{what}", p.get(f"feeds.publish_{what}"))
        t_pub, hit = ctx.now(), [None]

        def effect(hdrs=hdrs, what=what, hit=hit):
            # An admit takes effect when the probe is served; a revoke only
            # when the cell refuses the credential with its revocation's
            # code: a transport failure or a 5xx is no answer, not a refusal.
            x = ctx.cell.product(path, hdrs)
            done = V.authorized(x["status"]) if what == "admit" else V.revoked(x, revoked_codes)
            hit[0] = {"t": x["t"], "status": x["status"], "code": x.get("code")} if done else None
            return done
        ctx.until(effect, timeout)
        ev[what] = {"before": before, "t_pub": t_pub, "t_effect": hit[0] and hit[0]["t"], "effect": hit[0],
                    "timeout_s": timeout}
    every = p.get("feeds.probe_every_s", 5)
    prober = ev["probes"] = ctx.prober(lambda: ctx.cell.product(path, p.get("feeds.steady_headers_file")), every)
    mark = len(s.snapshot())
    ctx.run("publish-malformed", p.get("feeds.publish_malformed"))
    t_pub = ctx.now()
    m = ev["malformed"] = {"t_pub": t_pub, "probe_every_s": every, "deleted": None, "stale_t": None}

    def seen() -> bool:
        if m["deleted"] is None:
            hit = V.first(s.snapshot()[mark:], V.FEEDS_DELETED)
            if hit:
                m["deleted"] = {"after_s": int(hit[2].group(1)), "t": hit[0]}
        if m["stale_t"] is None:
            m["stale_t"] = next((x["t"] for x in prober.samples
                                 if x["t"] >= t_pub and x["status"] == 503 and x.get("code") == "policy_stale"), None)
        return m["deleted"] is not None and m["stale_t"] is not None
    ctx.until(seen, p.get("feeds.malformed_timeout_s", 600))
    ctx.run("publish-good", p.get("feeds.publish_good"))
    t_good = ctx.now()
    served = lambda: next((x["t"] for x in prober.samples if x["t"] >= t_good and V.authorized(x["status"])), None)  # noqa: E731
    ctx.until(lambda: served() is not None, timeout)
    m["restore"] = {"t_pub": t_good, "t_effect": served()}
    ev["probes"] = prober.stop()
    s.close()
    ev["log"] = s.snapshot()
    return ev


# ---------------------------------------------------------------- leg 7

def broken_boot(ctx: Ctx, case: dict) -> dict:
    """One case's deploy, the version it made, what the cell answered and its log."""
    p = ctx.plan
    known = ctx.versions()
    r = ctx.run(f"deploy-{case['name']}", case["deploy"], must=False)
    # deploy-release.sh exits non-zero for these cases (its cell-check
    # fails, or, with no --cell-url, 3: deployed and unchecked), after the
    # version exists: look for it whatever the exit code (briefly when the
    # command failed, as a refusal before `prisma deploy` makes none).
    v = ctx.compute.newest(p.get("cell.service_id"), known, timeout=300 if r["rc"] == 0 else 30)
    s = ctx.logs(v, from_start=True) if v else None
    got = [{"status": None, "body": ""}]

    def answered(got=got):
        got[0] = ctx.cell.raw(p.get("boot_failures.probe_path", "/health"))
        return got[0]["status"] == 500
    ctx.until(answered, p.get("boot_failures.wait_s", 300), 5)
    if s:
        s.wait_for(lambda ls, pat=case["expect_log"]: any(re.search(pat, t) for t in V.texts(ls)), 120)
        s.close()
    return {"name": case["name"], "deploy_rc": r["rc"], "version": v, "status": got[0]["status"],
            "body": got[0]["body"], "lines": s.snapshot() if s else [], "expect_log": case["expect_log"]}


def boot_failures(ctx: Ctx, ev: dict | None = None) -> dict:
    """Each case deploys a cell that must fail its boot visibly, then the good release returns.

    A case's optional `prepare` makes its break before the deploy (the
    missing secret: `set-secrets scratch --blank NAME`) and must succeed;
    its optional `restore` undoes it, even when the case fails, before the
    leg's restore redeploys the good release (`set-secrets --from DIR`)."""
    ev = {} if ev is None else ev
    p = ctx.plan
    refuse_production(ctx, "boot-failures")
    ctx.compute.warm()
    cases = ev["cases"] = []
    for case in p.get("boot_failures.cases"):
        if case.get("prepare"):
            ctx.run(f"prepare-{case['name']}", case["prepare"])
        undo = {"rc": 0}
        try:
            seen = broken_boot(ctx, case)
        finally:
            if case.get("restore"):
                undo = ctx.run(f"case-restore-{case['name']}", case["restore"], must=False)
        rs = ctx.run(f"restore-{case['name']}", p.get("boot_failures.restore"), must=False)
        back = ctx.until(lambda: ctx.cell.health()["status"] == 200, 600, 5)
        cases.append({**seen, "restored": back and rs["rc"] == 0 and undo["rc"] == 0})
    return ev


# ---------------------------------------------------------------- leg 8

def keep_awake(ctx: Ctx, ev: dict | None = None) -> dict:
    """/health every interval over SC0-1s-awake's hours: every boot-id change is a recycle."""
    ev = {} if ev is None else ev
    p = ctx.plan
    hours, every = p.get("keep_awake.hours", 72), p.get("keep_awake.interval_s", 60)
    end = ctx.now() + hours * 3_600_000
    ev.update(hours=hours, samples=[], notices=[], wrapper_boots=None)
    last_save = ctx.now()
    while ctx.now() < end:
        ev["samples"].append(ctx.cell.health())
        if ctx.now() - last_save >= 3_600_000:
            save(ctx.out, "keep-awake", ev)
            last_save = ctx.now()
        ctx.sleep(every)
    notices = p.get("keep_awake.notices_file", None)
    if notices:
        with open(os.path.expanduser(notices), encoding="utf-8") as f:
            ev["notices"] = [line.strip() for line in f if line.strip()]
    ctx.compute.warm()
    s = ctx.logs(ctx.newest(), from_start=True, tail_lines=100000)
    s.wait_for(lambda _: False, p.get("keep_awake.log_read_s", 60))
    s.close()
    ev["wrapper_boots"] = sum(1 for t in V.texts(s.snapshot()) if V.INSTANCE_SHAPE in t)
    return ev


LEGS = {
    "d10-probe": d10_probe,
    "d10-cell": d10_cell,
    "restart": restart,
    "rollback": rollback,
    "oom": oom,
    "feeds": feeds,
    "boot-failures": boot_failures,
    "keep-awake": keep_awake,
}

#: The plan keys each leg needs (besides stage, out and platform_token_file),
#: and the commands whose placeholders `check` resolves.
REQUIRES = {
    "d10-probe": ["probe.project_id|probe.service_id"],
    "d10-cell": ["cell.url", "cell.service_id", "commands.restart", "load.produce", "load.audit"],
    "restart": ["cell.url", "cell.service_id", "cell.headers_file", "commands.restart", "restart.probe_path",
                "restart.append_stream", "restart.sdk.stream", "restart.sdk.headers_file"],
    "rollback": ["cell.url", "cell.service_id", "commands.deploy", "commands.cell_check", "load.produce", "load.audit",
                 "rollback.older", "rollback.newer"],
    "oom": ["cell.url", "cell.service_id", "oom.scratch", "oom.deploy", "oom.restore", "load.produce", "load.audit"],
    "feeds": ["cell.url", "cell.service_id", "feeds.admit_headers_file", "feeds.steady_headers_file",
              "feeds.publish_admit", "feeds.publish_revoke", "feeds.publish_malformed", "feeds.publish_good"],
    "boot-failures": ["cell.url", "cell.service_id", "boot_failures.cases", "boot_failures.restore"],
    "keep-awake": ["cell.url", "cell.service_id"],
}
DUMMY = {k: "x" for k in ("stage", "out", "url", "release", "home", "ledger", "ledgers", "seed", "duration", "windows",
                          "app", "probe_stage", "report")}


def check(plan: H.Plan, leg: str) -> list:
    """Problems with `plan` for `leg`, offline: missing keys, unknown placeholders, an out dir in the repo."""
    problems = []
    for key in ["stage", "out", "platform_token_file"] + REQUIRES[leg]:
        if all(plan.get(k, None) is None for k in key.split("|")):
            problems.append(f"`{key}` is required")
    if plan.get("out", None):
        out = os.path.realpath(os.path.expanduser(str(plan.get("out"))))
        inside = out == H.ROOT or out.startswith(H.ROOT + os.sep)
        if inside and not out.startswith(os.path.join(H.ROOT, "target") + os.sep):
            problems.append(f"out {out} is inside the repository: evidence stays outside it (or under target/)")
    if leg in ("oom", "boot-failures") and plan.get("stage", None) == "production":
        problems.append(f"{leg} never runs on the production stage")
    commands = [plan.get(k, None) for k in ("commands.restart", "commands.deploy", "commands.cell_check", "load.setup",
                                            "load.produce", "load.audit", "oom.deploy", "oom.restore", "oom.load",
                                            "probe.deploy", "boot_failures.restore", "feeds.publish_admit",
                                            "feeds.publish_revoke", "feeds.publish_malformed", "feeds.publish_good")]
    commands += [c.get(k) for c in plan.get("boot_failures.cases", None) or [] for k in ("prepare", "deploy", "restore")]
    for argv in filter(None, commands):
        try:
            H.fill(argv, DUMMY)
        except H.PlanError as e:
            problems.append(str(e))
    return problems


def materialize(o):
    """json's default for the live evidence a leg keeps while it runs: a log
    stream's lines so far, a prober's samples so far."""
    if hasattr(o, "snapshot"):
        return o.snapshot()
    if hasattr(o, "samples"):
        return list(o.samples)
    raise TypeError(f"evidence holds a {type(o).__name__}")


def save(out: str, leg: str, ev: dict) -> str:
    path = os.path.join(out, "evidence.json")
    with open(path + ".tmp", "w", encoding="utf-8") as f:
        json.dump({"leg": leg, **ev}, f, indent=1, default=materialize)
    os.replace(path + ".tmp", path)
    return path


def run_leg(ctx: Ctx, leg: str) -> str:
    """Runs `leg` into one evidence dict and saves it however the leg ends:
    a failure keeps the evidence collected so far beside its `error`, and
    every job, log stream and prober the leg started is stopped first.
    Returns the evidence file."""
    ev: dict = {}
    try:
        try:
            LEGS[leg](ctx, ev)
        except (LegError, H.PlanError) as e:
            ev["error"] = str(e)
        except Exception as e:  # noqa: BLE001 - any failure keeps the evidence (a CLI timeout, a missing key)
            ev["error"] = f"{type(e).__name__}: {e}"
    finally:
        ctx.close()
        path = save(ctx.out, leg, ev)
    return path


def judge(ev: dict) -> V.Report:
    if ev.get("error"):
        r = V.Report(ev["leg"])
        r.add("FAIL", f"the run stopped: {ev['error']}")
        return r
    return V.JUDGES[ev["leg"]](ev)


def main(argv: list) -> int:
    usage = "usage: lifecycle.py <leg>|check <leg> --plan PLAN.json | judge EVIDENCE.json"
    if len(argv) == 2 and argv[0] == "judge":
        with open(argv[1], encoding="utf-8") as f:
            report = judge(json.load(f))
        print("\n".join(report.render()))
        return 0 if report.ok else 1
    cmd = argv[:2] if argv[:1] == ["check"] else argv[:1]
    rest = argv[len(cmd):]
    leg = cmd[-1] if cmd else ""
    if leg not in LEGS or len(rest) != 2 or rest[0] != "--plan":
        print(f"{usage}\nlegs: {' '.join(LEGS)}", file=sys.stderr)
        return 2
    plan = H.Plan.load(rest[1])
    problems = check(plan, leg)
    if cmd[0] == "check" or problems:
        print("\n".join(f"PLAN {p}" for p in problems) or f"PLAN_OK {leg}")
        return 1 if problems else 0
    out = os.path.join(os.path.expanduser(plan.get("out")), f"{leg}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}")
    os.makedirs(out)
    ctx = Ctx(plan, out)
    print(f"lifecycle {leg}: stage {plan.get('stage')}, evidence in {out}")
    path = run_leg(ctx, leg)
    print(f"evidence: {path}")
    with open(path, encoding="utf-8") as f:
        report = judge(json.load(f))
    print("\n".join(report.render()))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
