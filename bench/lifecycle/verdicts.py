"""Verdicts of the Compute lifecycle checklist (deploy-q5 design §3.4, legs
1-8; triage item B6): pure functions from a leg's collected evidence to
PASS, FAIL, FACT and FINDING lines. Nothing here talks to the platform:
lifecycle.py collects the evidence, and `lifecycle.py judge <evidence.json>`
re-reads a saved run through these functions.

A leg passes when it has at least one PASS line and no FAIL line. FACT lines
are measurements reported as they are (the platform questions); a FINDING is
a fact that changes the plan (an overlap moves F2 before the beta, an
unannounced recycle goes to Compute).

The log lines read here are the ones the code writes, quoted from their
sources: the deploy wrapper (deploy/app-server/supervise.ts, feeds.ts), the
binary's tracing lines (src/bootstrap.rs, src/tasks/exits.rs) and the
SIGTERM probe (sigterm-probe/app/probe.ts). Evidence keeps a log line either
as its text or as [arrival ms, text]; the arrival time is the collector's
clock, the binary's own timestamp the instance's.
"""
from __future__ import annotations

import calendar
import json
import re

# ---------------------------------------------------------------- bars
# Each is a code fact or an owner default, named where it comes from.

#: The binary's ordered stop bound, PROCESS_STOP_DEADLINE (src/tasks/exits.rs).
STOP_BOUND_S = 30
#: Owner question D4 (deploy-q5 Q4), recommended answer (a): the restart
#: window, 10 s. Measured as the time raw clients are without service
#: across a redeploy's switch (judge_restart): with create-before-delete
#: deploys the first success after the new version's `listening` line is
#: the old build's, so that reading would be about 0 s whatever the switch
#: does; the new build's first answer after `listening` is reported beside it.
RESTART_WINDOW_BAR_S = 10
#: Publish to effect for an admit or a revoke: the wrapper's 15 s poll plus
#: the server's 30 s feed refresh (RUNBOOK "Shared cells", "Publishing").
PUBLISH_EFFECT_BAR_S = 45
#: The wrapper's poll period, POLL_MS (deploy/app-server/feeds.ts).
POLL_S = 15
#: Awake time without a successful poll before the feeds are deleted,
#: STALE_MS (feeds.ts, edge #131).
FEEDS_DELETE_S = 120
#: The server's feed refresh period, STREAMS_AUTH_REFRESH_SECS' default.
REFRESH_S = 30
#: Policies this stale make the cell refuse every request (edge #131).
POLICY_STALE_S = 300
#: The body every boot failure serves on $PORT (supervise.ts): the detail
#: goes to the platform log, never to the response.
GENERIC_500 = {
    "error": "binary_exited",
    "message": "the service is not running; its exit is recorded in the platform log",
}

# ---------------------------------------------------------------- log lines

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
BINARY = re.compile(
    r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z)\s+(TRACE|DEBUG|INFO|WARN|ERROR)\s+([\w:]+):\s(.*)$")
ISO = re.compile(r"^(\d{4})-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)(?:\.(\d+))?Z$")
PROBE = re.compile(r"^sigterm-probe (main|child) (boot|SIGTERM|alive) t=(\S+)(.*)$")
FIELD = re.compile(r"(\w+)=(\"(?:[^\"\\]|\\.)*\"|\S+)")

WRAPPER_SIGTERM = "wrapper received SIGTERM; forwarding it to the binary"
WRAPPER_STOPPED = re.compile(r"binary exited with code (-?\d+) after a forwarded stop; exiting (-?\d+)")
WRAPPER_KILLED = re.compile(r"binary did not stop within (\d+) ms of SIG\w+; killing it")
WRAPPER_RUNTIME_EXIT = re.compile(r"binary exited with code (-?\d+) after serving on :\d+ \(up (\d+) ms\)")
INSTANCE_SHAPE = "instance shape: "
FEEDS_FAILED = "feeds poll failed: "
FEEDS_UPDATED = re.compile(r"feeds updated: \S+ gen=(\S+)")
FEEDS_DELETED = re.compile(r"feeds deleted: no successful poll for (\d+) s")
BIN_BOOT = re.compile(r"^runtime identity minted boot_id=(\w+)")
BIN_LISTEN = re.compile(r"^streams-slate listening on (\S+)")
BIN_OPEN = re.compile(r"^opening shard log (\S+) \(lazy; fences prior owner\)")
BIN_TERM = "termination signal: shutting down"
BIN_LOOPS = "supervised loops stopped"
FENCE = re.compile(r"fenced", re.I)
#: The compute CLI's terminal code for a version that stopped: the only log
#: end that is the process's end (harness.LogStream resumes segment ends).
VERSION_STOPPED = "vm_stopped"


def iso_ms(text: str) -> int | None:
    """Epoch milliseconds of an ISO-8601 UTC time (`...Z`, any fraction)."""
    m = ISO.match(text)
    if not m:
        return None
    secs = calendar.timegm(tuple(int(m.group(i)) for i in range(1, 7)) + (0, 0, 0))
    return secs * 1000 + int((m.group(7) or "0")[:3].ljust(3, "0"))


def entries(lines) -> list:
    """(arrival ms or None, text without ANSI) for every line of a log."""
    out = []
    for line in lines or []:
        t, text = (line[0], line[1]) if isinstance(line, (list, tuple)) else (None, line)
        out.append((t, ANSI.sub("", str(text)).rstrip("\r")))
    return out


def texts(lines) -> list:
    return [text for _, text in entries(lines)]


def binary_lines(lines) -> list:
    """(instance ms, target, message) of every tracing line of the binary."""
    out = []
    for text in texts(lines):
        m = BINARY.match(text.strip())
        if m:
            out.append((iso_ms(m.group(1)), m.group(3), m.group(4)))
    return out


def first(lines, pattern):
    """The first wrapper or probe line matching `pattern` (a regex or a
    substring): (arrival ms, text, match or None)."""
    for t, text in entries(lines):
        if isinstance(pattern, str):
            if pattern in text:
                return t, text, None
        else:
            m = pattern.search(text)
            if m:
                return t, text, m
    return None


def fields(rest: str) -> dict:
    return {k: v for k, v in FIELD.findall(rest)}


# ---------------------------------------------------------------- reports

class Report:
    """The lines of one leg's verdict."""

    def __init__(self, leg: str):
        self.leg = leg
        self.lines: list = []

    def add(self, kind: str, text: str) -> None:
        self.lines.append((kind, text))

    def check(self, ok: bool, passed: str, failed: str) -> bool:
        self.add("PASS" if ok else "FAIL", passed if ok else failed)
        return ok

    @property
    def ok(self) -> bool:
        kinds = [k for k, _ in self.lines]
        return "PASS" in kinds and "FAIL" not in kinds

    def render(self) -> list:
        out = [f"{kind} {self.leg}: {text}" for kind, text in self.lines]
        out.append(f"{'LEG_PASS' if self.ok else 'LEG_FAIL'} {self.leg}")
        return out


def secs(ms: float | None) -> str:
    return "?" if ms is None else f"{ms / 1000:.1f}"


# ---------------------------------------------------------------- leg 1: D10 on the probe

def probe_events(lines) -> list:
    out = []
    for text in texts(lines):
        m = PROBE.match(text.strip())
        if m:
            out.append({"role": m.group(1), "event": m.group(2), "t": iso_ms(m.group(3)), "fields": fields(m.group(4))})
    return out


def probe_facts(lines) -> dict:
    """What one version's probe log says about its stop."""
    ev = probe_events(lines)
    pick = lambda role, event: next((e for e in ev if e["role"] == role and e["event"] == event), None)  # noqa: E731
    boot, term, cterm = pick("main", "boot"), pick("main", "SIGTERM"), pick("child", "SIGTERM")
    last = max((e["t"] for e in ev if e["t"] is not None), default=None)
    start = term or cterm
    return {
        "boot": boot,
        "sigterm": term is not None,
        "group": cterm is not None,
        "sigterm_t": start["t"] if start else None,
        "last_t": last,
        "lived_s": (last - start["t"]) / 1000 if start and last is not None else None,
    }


def judge_d10_probe(ev: dict) -> Report:
    r = Report("d10-probe")
    window = ev.get("read_window_s")
    for scenario in ("redeploy", "stop"):
        part = ev.get(scenario) or {}
        old = probe_facts(part.get("old"))
        boot = old["boot"]
        if not boot:
            r.add("FAIL", f"{scenario}: no probe boot line in version {part.get('old_version')}'s log")
            continue
        f = boot["fields"]
        where = "PID 1" if f.get("pid") == "1" else f"pid {f.get('pid')} (PID 1 is {f.get('pid1')})"
        r.add("FACT", f"{scenario}: the probe ran as {where} under {f.get('runtime')}; "
                      f"the process group {'was' if old['group'] else 'was not'} signalled")
        if f.get("keep_awake") != "held":
            r.add("FAIL", f"{scenario}: the probe was not kept awake (keep_awake={f.get('keep_awake', 'absent')}): "
                          "Compute's scale-to-zero could suspend it, so its stop facts would describe a sleeping "
                          "service, not a kept-awake cell")
        # Only the version's own end (vm_stopped) is a kill: any other end
        # (the read window, a CLI error, a stream this side closed) leaves
        # the stop not observed to the end, never a kill time.
        end = part.get("log_end")
        stopped = end == VERSION_STOPPED
        if not old["sigterm"]:
            what = "the child alone was signalled" if old["group"] else "no SIGTERM arrived"
            r.add("FAIL", f"{scenario}: {what} before the log ended ({end})" if stopped else
                  f"{scenario}: {what} before the log ended ({end}), which is not the version's stop: not observed to the end")
            continue
        lived = old["lived_s"]
        if stopped:
            r.add("FACT", f"{scenario}: killed {lived:.0f}-{lived + 1:.0f} s after SIGTERM")
        elif end == "timeout":
            r.add("FACT", f"{scenario}: not killed within the {window} s read window ({lived:.0f} s after SIGTERM)")
        else:
            r.add("FACT", f"{scenario}: not observed to the end: the log ended ({end}) {lived:.0f} s after SIGTERM, "
                          "before the version stopped")
        if lived >= STOP_BOUND_S:
            r.add("PASS", f"{scenario}: SIGTERM delivered and the process lived {lived:.0f} s after it, "
                          f">= the binary's {STOP_BOUND_S} s ordered stop")
        elif stopped:
            r.add("FAIL", f"{scenario}: the process lived only {lived:.0f} s after SIGTERM, inside the binary's "
                          f"{STOP_BOUND_S} s ordered stop")
        else:
            r.add("FAIL", f"{scenario}: not observed to the end: the log ended ({end}) {lived:.0f} s after SIGTERM, "
                          f"short of the binary's {STOP_BOUND_S} s ordered stop")
    red = ev.get("redeploy") or {}
    old, new = probe_facts(red.get("old")), probe_facts(red.get("new"))
    if old["boot"] and new["boot"]:
        r.check(old["boot"]["fields"].get("nonce") != new["boot"]["fields"].get("nonce"),
                "redeploy: the new version runs the new artifact (its own nonce)",
                f"redeploy: the new version runs the old nonce {new['boot']['fields'].get('nonce')}: not a new artifact")
        overlap = None if old["last_t"] is None else old["last_t"] - new["boot"]["t"]
        r.add("FACT", f"redeploy: the new version booted {secs(new['boot']['t'] - old['sigterm_t']) if old['sigterm_t'] else '?'} s "
                      f"after the old one's SIGTERM; old and new overlapped {secs(max(overlap, 0)) if overlap is not None else '?'} s")
    return r


# ---------------------------------------------------------------- leg 2: D10 on the cell

def cell_stop_facts(lines) -> dict:
    """The old version's stop, from the wrapper's and the binary's lines."""
    bins = binary_lines(lines)
    term = next((t for t, _, msg in bins if msg.startswith(BIN_TERM)), None)
    stopped = first(lines, WRAPPER_STOPPED)
    return {
        "wrapper_sigterm": first(lines, WRAPPER_SIGTERM) is not None,
        "term_t": term,
        "loops_t": next((t for t, _, msg in bins if msg.startswith(BIN_LOOPS)), None),
        "last_t": max((t for t, _, _ in bins if t is not None), default=None),
        "exit_code": int(stopped[2].group(1)) if stopped else None,
        "killed": first(lines, WRAPPER_KILLED) is not None,
        "boot_id": next((BIN_BOOT.match(m).group(1) for _, _, m in bins if BIN_BOOT.match(m)), None),
        "fences": [(t, msg[:160]) for t, _, msg in bins if FENCE.search(msg) and not BIN_OPEN.match(msg)],
    }


def cell_boot_facts(lines) -> dict:
    bins = binary_lines(lines)
    boot = next(((t, BIN_BOOT.match(m).group(1)) for t, _, m in bins if BIN_BOOT.match(m)), (None, None))
    return {
        "boot_t": boot[0],
        "boot_id": boot[1],
        "open_t": next((t for t, _, m in bins if BIN_OPEN.match(m)), None),
        "listen_t": next((t for t, _, m in bins if BIN_LISTEN.match(m)), None),
    }


def audit_ok(r: Report, audit: dict | None, what: str) -> bool:
    v = (audit or {}).get("verify") or {}
    ok = (audit or {}).get("rc") == 0 and v.get("ok") is True
    return r.check(
        ok,
        f"{what}: 0 acked records lost (k2gen audit: {v.get('expected_records')} records read back, "
        f"{v.get('blocks_missing')} missing blocks <= {v.get('failed_requests')} failed requests)",
        f"{what}: the audit failed (rc {(audit or {}).get('rc')}; missing blocks {v.get('blocks_missing')}, "
        f"failed requests {v.get('failed_requests')}, unexpected {v.get('unexpected_records')}, "
        f"offset errors {v.get('offset_errors')})")


def acked_after(windows, t_ms: int | None) -> int:
    """Appends the load acked in its windows that began at or after `t_ms`
    (k2gen --out lines: `t` is the window's end, in s)."""
    if t_ms is None:
        return 0
    return sum(int(w.get("acked_requests") or 0) for w in windows or []
               if w.get("t") is not None and (w["t"] - (w.get("window_s") or 0)) * 1000 >= t_ms)


def judge_d10_cell(ev: dict) -> Report:
    r = Report("d10-cell")
    old, new = cell_stop_facts(ev.get("old_lines")), cell_boot_facts(ev.get("new_lines"))
    r.check(old["wrapper_sigterm"], "the wrapper received SIGTERM and forwarded it to the binary",
            f"no `{WRAPPER_SIGTERM}` line in version {ev.get('old_version')}'s log")
    if old["term_t"] is None:
        r.add("FAIL", "the binary logged no `termination signal` line")
    else:
        stop_ms = (old["last_t"] or old["term_t"]) - old["term_t"]
        r.check(stop_ms <= STOP_BOUND_S * 1000 and old["loops_t"] is not None,
                f"the binary's ordered stop took {stop_ms / 1000:.3f} s (bound {STOP_BOUND_S} s)",
                f"the binary's ordered stop took {stop_ms / 1000:.3f} s "
                f"({'over' if stop_ms > STOP_BOUND_S * 1000 else 'no supervised-loops line; under'} its {STOP_BOUND_S} s bound)")
    r.check(old["exit_code"] == 0 and not old["killed"],
            "the binary exited 0 after the forwarded stop",
            "the wrapper killed the binary after its grace" if old["killed"]
            else f"the binary's exit after the forwarded stop: {old['exit_code']}")
    if new["boot_t"] is None:
        r.add("FAIL", f"no `runtime identity minted` line in version {ev.get('new_version')}'s log")
    elif old["last_t"] is not None:
        overlap = old["last_t"] - new["boot_t"]
        shard = "" if new["open_t"] is None else (
            f"; the new process opened (fenced) the shard {secs(old['last_t'] - new['open_t'])} s before the old one's last line"
            if new["open_t"] < old["last_t"] else "; the new process opened the shard after the old one's last line")
        r.add("FACT", f"old and new processes overlapped {secs(max(overlap, 0))} s by their own clocks{shard}")
        if overlap > 0:
            r.add("FINDING", "the old and new processes overlap: F2's fenced-write 500 is reachable on one server, "
                             "so F2 moves before the beta")
    if old["fences"]:
        r.add("FACT", f"{len(old['fences'])} fence line(s) in the old version's log; first: {old['fences'][0][1]}")
    boots = [s.get("boot") for s in ev.get("health") or [] if s.get("boot")]
    if boots:
        seq = [b for i, b in enumerate(boots) if i == 0 or b != boots[i - 1]]
        r.add("FACT", f"clients saw {len(seq)} boot-id run(s) on /health: {' -> '.join(b[:8] for b in seq)}")
        if len(seq) > len(set(seq)):
            r.add("FINDING", "clients reached both instances alternately: the edge served the old and the new process at once")
    # "Under light load" needs appends acked after the switch: after the new
    # build's first /health 200 and the old one's SIGTERM.
    switch = next((s["t"] for s in sorted(ev.get("health") or [], key=lambda s: s["t"])
                   if new["boot_id"] and s.get("boot") == new["boot_id"] and s.get("status") == 200), None)
    hit = first(ev.get("old_lines"), WRAPPER_SIGTERM)
    marks = [t for t in (switch, hit[0] if hit else None) if t is not None]
    acked = acked_after((ev.get("load") or {}).get("windows"), max(marks)) if switch is not None else 0
    if acked:
        audit_ok(r, ev.get("audit"), f"under light load across the redeploy ({acked} appends acked after the switch)")
    else:
        r.add("FAIL", "the load did not run across the redeploy: no append was acked after "
                      + ("the switch (the new build's first /health 200 and the old one's SIGTERM)" if switch is not None
                         else "a /health 200 from the new build, which never answered one"))
        audit_ok(r, ev.get("audit"), "the load's audit")
    return r


# ---------------------------------------------------------------- leg 3: restart

def ok_answer(a: dict) -> bool:
    return 200 <= (a.get("status") or 0) < 300


def retry_answer(a: dict) -> bool:
    return a.get("status") == 503 and bool(a.get("retry_after"))


def judge_restart(ev: dict) -> Report:
    """Every tick from a restart's trigger to the next trigger (or the end
    of the cooldown) is judged, not only those up to the first 2xx: a
    create-before-delete deploy keeps the old process answering after the
    new one's `listening` line, and the switch (the old process's SIGTERM,
    the shard's lazy open and fence, the edge's cutover) comes later. A
    tick's /health boot id says which build answered it."""
    r = Report("restart")
    restarts = ev.get("restarts") or []
    ticks = sorted(ev.get("samples") or [], key=lambda s: s["t"])
    every = ev.get("probe_every_s")
    if not restarts:
        r.add("FAIL", "no restart ran")
    if not any(a.get("kind") == "append" for s in ticks for a in s.get("answers") or []):
        r.add("FAIL", "no append was probed: a fenced write's 500 (F2) cannot show on reads alone")
    whole = lambda s: bool(s.get("answers")) and all(ok_answer(a) for a in s["answers"])  # noqa: E731
    for i, rs in enumerate(restarts, 1):
        if rs.get("rc") != 0:
            r.add("FAIL", f"restart {i}: the restart command exited {rs.get('rc')}")
            continue
        new = cell_boot_facts(rs.get("new_lines"))
        listen, boot = new["listen_t"], new["boot_id"]
        if listen is None or boot is None:
            r.add("FAIL", f"restart {i}: no `listening` or `runtime identity minted` line in version {rs.get('new_version')}'s log")
            continue
        end = restarts[i]["t_trigger"] if i < len(restarts) else float("inf")
        win = [s for s in ticks if rs["t_trigger"] <= s["t"] < end]
        old = next((s.get("boot") for s in reversed(ticks) if s["t"] < rs["t_trigger"] and s.get("boot")), None)
        first_new = next((s for s in win if s.get("boot") == boot and whole(s)), None)
        if first_new is None:
            r.add("FAIL", f"restart {i}: the new build (boot {boot[:8]}) never answered every probe 2xx")
            continue
        last_old = next((s for s in reversed(win) if old and s.get("boot") == old and whole(s)), None)
        r.add("FACT", f"restart {i}: the new build first answered every probe {secs(first_new['t'] - listen)} s after its "
                      f"`listening` line" + (f"; the old build answered until {secs(last_old['t'] - listen)} s after it"
                                             if last_old else ""))
        # The window: from the first tick with a failed or refused answer to
        # the next tick whose every answer is 2xx.
        failed = [s for s in win if not whole(s)]
        back = next((s for s in win if failed and s["t"] > failed[-1]["t"] and whole(s)), None)
        if failed and back is None:
            r.add("FAIL", f"restart {i}: clients were without service from {secs(failed[0]['t'] - listen)} s after "
                          "`listening` and never got it back")
        else:
            gap = (back["t"] - failed[0]["t"]) / 1000 if failed else 0.0
            r.check(gap <= RESTART_WINDOW_BAR_S,
                    f"restart {i}: raw clients were without service {gap:.1f} s across the switch (bar {RESTART_WINDOW_BAR_S} s)",
                    f"restart {i}: raw clients were without service {gap:.1f} s across the switch, over the "
                    f"{RESTART_WINDOW_BAR_S} s bar")
        build = lambda s: "new" if s.get("boot") == boot else "old" if old and s.get("boot") == old else (  # noqa: E731
            f"boot {s['boot'][:8]}" if s.get("boot") else "no /health")
        answers = [(s, a) for s in win for a in s.get("answers") or []]
        retry = [a for _, a in answers if retry_answer(a)]
        bad = [(s, a) for s, a in answers if not ok_answer(a) and not retry_answer(a)]
        codes = sorted({f"503:{a.get('code')}" for a in retry})
        r.add("FACT", f"restart {i}: {len(retry)} x 503 with Retry-After while restarting {codes or ''}".rstrip())
        seen = sorted({"%s %s:%s (%s)" % (a.get("kind"), a.get("status"), a.get("code"), build(s)) for s, a in bad})
        r.check(not bad, f"restart {i}: raw clients saw only 503 with Retry-After",
                f"restart {i}: raw clients saw {len(bad)} other answer(s): {seen}")
    if restarts and every:
        r.add("FACT", f"the windows are a raw client's, polling every {every} s; an SDK client's first success after a "
                      "switch can come up to one retry backoff later, so they understate D4's window to the first SDK success")
    return r


# ---------------------------------------------------------------- leg 4: rollback

def step_switch(health: list, t_deploy: int | None) -> int | None:
    """When a deploy's build took over: the first /health 200 after
    `t_deploy` whose boot id is not the one serving before it."""
    if t_deploy is None:
        return None
    before = next((s.get("boot") for s in reversed(health) if s["t"] < t_deploy and s.get("boot")), None)
    return next((s["t"] for s in health if s["t"] >= t_deploy and s.get("status") == 200
                 and s.get("boot") not in (None, before)), None)


def judge_rollback(ev: dict) -> Report:
    r = Report("rollback")
    newer = ev.get("newer_label", "newer")
    health = sorted(ev.get("health") or [], key=lambda s: s["t"])
    wrote: list = []
    reached: list = []  # labels whose own build acked appends of their step's load
    for i, st in enumerate(ev.get("steps") or [], 1):
        label = st.get("label")
        if not r.check(st.get("deploy_rc") == 0, f"step {i}: {label} deployed",
                       f"step {i}: deploying {label} exited {st.get('deploy_rc')}"):
            continue
        r.check(st.get("cell_check_rc") == 0, f"step {i}: cell-check passed on {label}",
                f"step {i}: cell-check exited {st.get('cell_check_rc')} on {label}")
        switch = step_switch(health, st.get("t_deploy"))
        acked = acked_after(st.get("windows"), switch)
        if r.check(acked > 0, f"step {i}: {acked} appends acked by {label} after it took over",
                   f"step {i}: no append was acked after {label} took over"
                   + (" (no new boot id on /health after the deploy)" if switch is None else "")
                   + f": the load did not reach the {label} build"):
            reached.append(label)
        wrote.append(label)
        if audit_ok(r, st.get("audit"), f"step {i} on {label} (ledgers of {', '.join(wrote)})") \
                and label != newer and newer in reached:
            r.add("PASS", f"step {i}: data written by {newer} read back by {label}")
    fake = ev.get("fake")
    if not fake:
        r.add("FAIL", "the cross-layout refusal was not tested")
    else:
        named = re.search(fake.get("pattern") or "(?i)layout", fake.get("tail") or "") is not None
        same = fake.get("boot_before") is not None and fake.get("boot_before") == fake.get("boot_after")
        r.check(fake.get("rc") not in (0, None) and named and same,
                f"a release whose {fake.get('key')} differs ({fake.get('from')} -> {fake.get('to')}) is refused "
                f"(rc {fake.get('rc')}) and the running boot is untouched",
                f"the cross-layout release: rc {fake.get('rc')}, refusal names the layout: {named}, "
                f"boot unchanged: {same}")
    return r


# ---------------------------------------------------------------- leg 5: OOM (scratch, non-shipping)

def judge_oom(ev: dict) -> Report:
    r = Report("oom")
    tag = "NON-SHIPPING CONFIG:"
    hit = first(ev.get("lines"), WRAPPER_RUNTIME_EXIT)
    code = int(hit[2].group(1)) if hit else None
    r.check(code == 137, f"{tag} the wrapper logged code 137 and exited, so the platform replaces the instance",
            f"{tag} no wrapper exit with code 137 in the log (saw {code})")
    r.check(ev.get("new_boot") is not None and ev.get("new_boot") != ev.get("old_boot"),
            f"{tag} the instance was replaced (boot {str(ev.get('old_boot'))[:8]} -> {str(ev.get('new_boot'))[:8]})",
            f"{tag} no new boot after the kill")
    if ev.get("kill_t") and ev.get("first_ok_t"):
        r.add("FACT", f"{tag} first 2xx {secs(ev['first_ok_t'] - ev['kill_t'])} s after the kill")
    audit_ok(r, ev.get("audit"), f"{tag} the load stopped at the kill")
    stable = ev.get("stable") or {}
    boots = set(stable.get("boots") or [])
    r.check(len(boots) == 1, f"{tag} no crash loop: one boot for {stable.get('seconds')} s after recovery",
            f"{tag} {len(boots)} boots in the {stable.get('seconds')} s after recovery")
    return r


# ---------------------------------------------------------------- leg 6: feed poll

def authorized(status: int) -> bool:
    return status not in (0, 401, 403) and status < 500


#: The codes a cell refuses a revoked project's credential with
#: (src/auth.rs AuthError::kind, answered 403 by product.rs
#: auth_failure_response): cell-admin's offboard revokes the credentials,
#: a suspension deactivates the project.
REVOKED_CODES = ("credential_not_active", "project_not_active")


def revoked(answer: dict, codes=REVOKED_CODES) -> bool:
    """A revocation took effect: a 401 or 403 carrying one of `codes`. A
    transport failure (0), a 5xx or an untyped refusal is no answer."""
    return answer.get("status") in (401, 403) and answer.get("code") in codes


def judge_feeds(ev: dict) -> Report:
    r = Report("feeds")
    quiet = ev.get("quiet") or {}
    failed = sum(1 for t in texts(quiet.get("lines")) if FEEDS_FAILED in t)
    updated = sum(1 for t in texts(quiet.get("lines")) if FEEDS_UPDATED.search(t))
    polls = int((quiet.get("seconds") or 0) // POLL_S)
    if polls < 4:
        r.add("FAIL", f"the quiet window was {quiet.get('seconds')} s: fewer than four polls")
    else:
        r.check(failed == 0, f"steady: no failed poll in {quiet['seconds']} s ({polls} polls)",
                f"{failed} failed poll(s) in a quiet {quiet['seconds']} s")
        r.check(updated == 0,
                "the presigned conditional GET works against the store: nothing published, nothing rewritten (each poll a 304)",
                f"the bundle was rewritten {updated} time(s) with nothing published: the store ignores If-None-Match; "
                "use C9's stat-then-GET fallback")
    for what, want in (("admit", True), ("revoke", False)):
        p = ev.get(what) or {}
        before = p.get("before")
        if before is not None and authorized(before) == want:
            r.add("FAIL", f"{what}: the probe was already {'served' if want else 'refused'} before the publish ({before})")
            continue
        if p.get("t_effect") is None:
            r.add("FAIL", f"{what}: no effect within {p.get('timeout_s')} s of the publish")
            continue
        took = (p["t_effect"] - p["t_pub"]) / 1000
        r.check(took <= PUBLISH_EFFECT_BAR_S, f"{what}: effect {took:.1f} s after the publish (bar {PUBLISH_EFFECT_BAR_S} s)",
                f"{what}: effect {took:.1f} s after the publish, over the {PUBLISH_EFFECT_BAR_S} s bar")
    m = ev.get("malformed") or {}
    deleted = m.get("deleted")
    if not deleted:
        r.add("FAIL", "malformed bundle: no `feeds deleted` line")
    else:
        lo, hi = FEEDS_DELETE_S, FEEDS_DELETE_S + 2 * POLL_S
        r.check(lo <= deleted["after_s"] <= hi,
                f"malformed bundle: feeds deleted after {deleted['after_s']} s without a successful poll (about {FEEDS_DELETE_S} s)",
                f"malformed bundle: feeds deleted after {deleted['after_s']} s, outside {lo}-{hi} s")
        if m.get("stale_t") is None:
            r.add("FAIL", "malformed bundle: no 503 policy_stale")
        else:
            # Policies go stale POLICY_STALE_S after the server's last good
            # read, which a refresh (REFRESH_S) before the deletion at most.
            stale = (m["stale_t"] - deleted["t"]) / 1000
            lo, hi = POLICY_STALE_S - REFRESH_S, POLICY_STALE_S + REFRESH_S + (m.get("probe_every_s") or 5)
            r.check(lo <= stale <= hi,
                    f"malformed bundle: 503 policy_stale {stale:.0f} s after the deletion (about {POLICY_STALE_S} s)",
                    f"malformed bundle: 503 policy_stale {stale:.0f} s after the deletion, outside {lo}-{hi} s")
    rs = m.get("restore") or {}
    if rs.get("t_effect") is None:
        r.add("FAIL", "malformed bundle: a good republish did not restore service")
    else:
        took = (rs["t_effect"] - rs["t_pub"]) / 1000
        r.check(took <= PUBLISH_EFFECT_BAR_S, f"restore: served {took:.1f} s after the good republish (bar {PUBLISH_EFFECT_BAR_S} s)",
                f"restore: served {took:.1f} s after the good republish, over the {PUBLISH_EFFECT_BAR_S} s bar")
    return r


# ---------------------------------------------------------------- leg 7: boot failures

def judge_boot_failures(ev: dict) -> Report:
    r = Report("boot-failures")
    for case in ev.get("cases") or []:
        name = case.get("name")
        r.add("FACT", f"{name}: the deploy command exited {case.get('deploy_rc')}")
        if not case.get("version"):
            r.add("FAIL", f"{name}: the deploy made no new version, so nothing reached the platform to fail its boot")
        try:
            body = json.loads(case.get("body") or "")
        except ValueError:
            body = None
        r.check(case.get("status") == 500 and body == GENERIC_500,
                f"{name}: the generic 500 binary_exited, no detail in the body",
                f"{name}: answered {case.get('status')} {str(case.get('body'))[:120]!r}, not the generic 500")
        hit = first(case.get("lines"), re.compile(case.get("expect_log") or "$^"))
        r.check(hit is not None, f"{name}: the detail is in the log: {hit[1][:160] if hit else ''}",
                f"{name}: no log line matches {case.get('expect_log')!r}")
        r.check(case.get("restored") is True, f"{name}: the good release serves again",
                f"{name}: the restore did not bring /health back to 200")
    if not ev.get("cases"):
        r.add("FAIL", "no boot-failure case ran")
    return r


# ---------------------------------------------------------------- leg 8: keep-awake persistence

def judge_keep_awake(ev: dict) -> Report:
    r = Report("keep-awake")
    samples = sorted(ev.get("samples") or [], key=lambda s: s["t"])
    if len(samples) < 2:
        r.add("FAIL", "fewer than two /health samples")
        return r
    hours = (samples[-1]["t"] - samples[0]["t"]) / 3_600_000
    planned = ev.get("hours") or 0
    r.check(hours >= planned * 0.99, f"observed {hours:.1f} h of the planned {planned} h",
            f"observed only {hours:.1f} h of the planned {planned} h")
    served = [s for s in samples if s.get("boot")]
    recycles = [(a, b) for a, b in zip(served, served[1:]) if a["boot"] != b["boot"]]
    awake = sum(1 for s in samples if s.get("status") == 200) / len(samples)
    r.add("FACT", f"awake fraction {awake:.4f} ({len(samples)} samples)")
    if ev.get("wrapper_boots") is not None:
        r.add("FACT", f"{ev['wrapper_boots']} wrapper boot(s) (`instance shape:` lines) in the version's log buffer")
    r.check(not recycles, f"no recycle in {hours:.1f} h: one boot id",
            f"{len(recycles)} recycle(s): " + "; ".join(
                f"at {b['t']} after {secs(b['t'] - a['t'])} s" for a, b in recycles))
    if recycles and not ev.get("notices"):
        r.add("FINDING", "an unannounced recycle of the only server is an outage for every tenant: take it to Compute")
    return r


JUDGES = {
    "d10-probe": judge_d10_probe,
    "d10-cell": judge_d10_cell,
    "restart": judge_restart,
    "rollback": judge_rollback,
    "oom": judge_oom,
    "feeds": judge_feeds,
    "boot-failures": judge_boot_failures,
    "keep-awake": judge_keep_awake,
}
