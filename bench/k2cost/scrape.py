#!/usr/bin/env python3
"""K2 cost experiment: the rig's instruments (target/k2-design.md §5, §8).

Subcommands (run-local.sh drives all of them):

  loop      every --interval s (default 10), one stamped JSON line per
            source to <run>/scrape.jsonl: /v1/debug/store (the cumulative
            `totals`, the ?window= ring, gauges; the slow-op list dropped),
            /v1/debug/load and /v1/debug/usage (aggregates only; the
            per-stream list is summed) from every server, and s3lite's
            /_s3lite/stats2 (+ /_s3lite/stats). Boot changes (boot_id),
            counter resets (totals.since_ms or a decrease) and s3lite resets
            become `event` lines. It answers quiescence requests (the rig's
            `load_end` mark, and a `quiesce_request` mark with an id for a
            mid-point `quiesce` phase) with a `quiesced` mark carrying the
            same id once all of these hold, counted from the request:
              - at least --quiesce-min-secs since the request (two GC cycles);
              - at least --quiesce-min-secs since the last --quiet-secs
                window of PUT bytes ABOVE the floor (2 x the quietest such
                window since the request + 256 KiB: the floor's telemetry
                writes are tens of KB, customer absorption and compaction
                MBs), so a late history compaction restarts the clock;
              - at least --quiet-secs since the last window of deletes
                above their floor (2 x the quietest + 20): the GC of that
                compaction's inputs has happened;
              - absorption not stuck (oldest eligible <= 2 absorb ages);
              - the heavy-PUT count (multipart, compaction state, history
                SST/meta) stationary over --windows windows.
            §8's literal "no such PUT for 120 s" never holds with billing on:
            the telemetry system streams append, absorb and compact on a
            ~60 s rhythm, so a deferred tail ends when it reaches that floor.
  snapshot  the exact ledger at one moment: <run>/s3lite-<label>.json and
            <run>/store-<label>.json.
  usage     the product meters after the drain: polls
            GET /v1/projects/{project}/usage on the rollup instance until
            it settles, then writes <run>/usage.json (plus every server's
            full /v1/debug/usage).
  posture   <run>/posture.json: binary sha256s, git commit, every server's
            environment with secrets redacted, build identity, startup
            summary.
  envfile   stdin KEY=VALUE lines (later wins, "-KEY" removes) to stdout
            as shell-safe KEY='value' lines; the rig's posture writer.

Debug routes are gated by the deployment bearer (AUTH_TOKEN; src/http/debug.rs):
--auth-file holds the "authorization: Bearer ..." header line. Product
routes (usage) take the customer headers file the generator uses.
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HEAVY_PUT_OPS = {"put", "copy"}
MULTIPART_OPS = {"multipart", "upload_part", "mpu_create", "mpu_complete"}
DELETE_OPS = {"delete", "delete_objects"}
PER_STREAM_MAX = 256  # debug/usage per-stream frame bytes kept per line up to this many streams


def now_ms() -> int:
    return int(time.time() * 1000)


def read_headers(path: str | None) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path:
        return out
    with open(path, encoding="utf-8") as f:
        for line in f:
            if ":" in line:
                name, value = line.split(":", 1)
                out[name.strip()] = value.strip()
    return out


def get_json(url: str, headers: dict[str, str] | None = None, timeout: float = 8.0):
    """(status, body, error). A body that is not JSON is an error."""
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw), None
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read())
        except Exception:  # noqa: BLE001 - error bodies are best effort
            body = None
        return e.code, body, f"http {e.code}"
    except Exception as e:  # noqa: BLE001 - a scrape never crashes the loop
        return 0, None, f"{type(e).__name__}: {e}"


def parse_servers(spec: str) -> list[tuple[str, str]]:
    out = []
    for part in spec.split(","):
        if part:
            name, url = part.split("=", 1)
            out.append((name, url.rstrip("/")))
    return out


def read_marks(run: str) -> list[dict]:
    path = os.path.join(run, "marks.jsonl")
    out = []
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except FileNotFoundError:
        pass
    return out


def append_mark(run: str, name: str, **extra) -> None:
    rec = {"t": now_ms(), "mark": name, **extra}
    with open(os.path.join(run, "marks.jsonl"), "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, separators=(",", ":")) + "\n")


# ---- source shaping -------------------------------------------------------

def shape_store(body: dict) -> dict:
    return {k: v for k, v in body.items() if k != "slow"}


def shape_usage(body: dict) -> dict:
    """The aggregate view: the per-stream list is summed, never copied (10k
    streams would make every line megabytes)."""
    streams = body.get("streams") or []
    sums = {k: 0 for k in ("requests", "records", "bytes_in", "bytes_out", "plaintext_bytes", "frame_bytes")}
    lag_max = 0.0
    for s in streams:
        for k in sums:
            sums[k] += int(s.get(k) or 0)
        lag_max = max(lag_max, float(s.get("absorb_lag_secs") or 0))
    out = {k: v for k, v in body.items() if k != "streams"}
    if len(streams) <= PER_STREAM_MAX:
        # The gauge series (price.py): cumulative frame bytes per stream id.
        out["frame_bytes_by_stream"] = {s.get("stream"): int(s.get("frame_bytes") or 0) for s in streams}
    out["listed_streams"] = len(streams)
    out["listed_sums"] = sums
    out["listed_absorb_lag_max_secs"] = lag_max
    return out


def totals_count_sum(store: dict) -> int | None:
    totals = store.get("totals")
    if not isinstance(totals, dict):
        return None
    n = 0
    for cell in (totals.get("ops") or {}).values():
        n += sum(int(v) for v in cell.values())
    return n


def s3_cell_count(cells: dict, pred) -> int:
    n = 0
    for key, buckets in (cells or {}).items():
        parts = key.split("/")
        if len(parts) != 3:
            continue
        tier, kind, op = parts
        if pred(tier, kind, op):
            n += int(buckets.get("2xx", 0))
    return n


def is_delete(tier: str, kind: str, op: str) -> bool:
    return op in DELETE_OPS


def is_heavy(tier: str, kind: str, op: str) -> bool:
    """Deferred engine work that is NOT a timer L0 (§8 quiescence): every
    multipart request, any compaction-state object, and history-tier SST or
    meta writes (absorption and history compaction). Shard and telemetry SST
    PUTs are the 5 s memtable timer and do not hold quiescence back."""
    if op in MULTIPART_OPS:
        return True
    if op not in HEAVY_PUT_OPS:
        return False
    return kind == "compactions" or (tier == "hist" and kind in ("sst", "meta"))


def s3_total(stats2: dict) -> int:
    t = stats2.get("total") or {}
    return int(t.get("class_a", 0)) + int(t.get("class_b", 0)) + int(t.get("free", 0))


# ---- loop -------------------------------------------------------------------

class Loop:
    def __init__(self, args):
        self.args = args
        self.run = args.run
        self.servers = parse_servers(args.servers)
        self.auth = read_headers(args.auth_file)
        self.out = open(os.path.join(self.run, "scrape.jsonl"), "a", encoding="utf-8")
        self.seq = 0
        self.boot: dict[str, str] = {}
        self.since: dict[str, int] = {}
        self.tsum: dict[str, int] = {}
        self.s3_last_total: int | None = None
        # (t_ms, cumulative heavy PUTs, cumulative deletes, cumulative PUT bytes)
        self.heavy: list[tuple[int, int, int, int]] = []
        self.backlog_ok: dict[str, bool] = {}

    def emit(self, src: str, body, **extra) -> None:
        rec = {"t": now_ms(), "seq": self.seq, "src": src, **extra, "body": body}
        self.out.write(json.dumps(rec, separators=(",", ":")) + "\n")

    def event(self, kind: str, **extra) -> None:
        self.emit("event", None, kind=kind, **extra)
        print(f"[scrape] event {kind} {extra}", flush=True)

    def scrape_server(self, name: str, url: str) -> None:
        w = self.args.interval
        st, body, err = get_json(f"{url}/v1/debug/store?window={w}", self.auth)
        if body is not None and err is None:
            self.emit("store", shape_store(body), server=name)
            since = (body.get("totals") or {}).get("since_ms")
            tsum = totals_count_sum(body)
            if since is not None and name in self.since and since != self.since[name]:
                self.event("counter_reset", server=name, reason="since_ms", before=self.since[name], after=since)
            elif tsum is not None and name in self.tsum and tsum < self.tsum[name]:
                self.event("counter_reset", server=name, reason="decrease", before=self.tsum[name], after=tsum)
            if since is not None:
                self.since[name] = since
            if tsum is not None:
                self.tsum[name] = tsum
        else:
            self.emit("store", None, server=name, ok=False, status=st, error=err)
        st, body, err = get_json(f"{url}/v1/debug/load", self.auth)
        if body is not None and err is None:
            self.emit("load", body, server=name)
            boot = body.get("boot_id")
            if boot and name in self.boot and boot != self.boot[name]:
                self.event("boot_change", server=name, before=self.boot[name], after=boot)
            if boot:
                self.boot[name] = boot
        else:
            self.emit("load", None, server=name, ok=False, status=st, error=err)
        st, body, err = get_json(f"{url}/v1/debug/usage", self.auth)
        if body is not None and err is None:
            shaped = shape_usage(body)
            self.emit("usage", shaped, server=name)
            bl = shaped.get("absorb_backlog") or {}
            # Absorption is not stuck: nothing waits longer than two absorb
            # ages. (A backlog of exactly zero never holds with billing on:
            # the telemetry streams cycle through the absorb age forever.)
            self.backlog_ok[name] = float(bl.get("oldest_eligible_secs") or 0) <= 2 * self.args.absorb_age_secs
        else:
            self.emit("usage", None, server=name, ok=False, status=st, error=err)
            self.backlog_ok[name] = False

    def scrape_s3(self) -> None:
        base = self.args.s3lite.rstrip("/")
        st, body, err = get_json(f"{base}/_s3lite/stats2")
        if body is None or err is not None:
            self.emit("s3lite", None, ok=False, status=st, error=err)
            return
        _, v1, _ = get_json(f"{base}/_s3lite/stats")
        body["stats"] = v1
        self.emit("s3lite", body)
        total = s3_total(body)
        if self.s3_last_total is not None and total < self.s3_last_total:
            self.event("s3lite_reset", before=self.s3_last_total, after=total)
        self.s3_last_total = total
        cells = body.get("cells") or {}
        put_bytes = int((v1 or {}).get("put_bytes") or 0)
        self.heavy.append((now_ms(), s3_cell_count(cells, is_heavy), s3_cell_count(cells, is_delete), put_bytes))

    def pending_request(self):
        """(id, t_ms) of the latest quiescence request not yet answered."""
        reqs, answered = [], set()
        for m in read_marks(self.run):
            if m.get("mark") == "load_end":
                reqs.append(("load_end", int(m["t"])))
            elif m.get("mark") == "quiesce_request":
                reqs.append((str(m.get("id")), int(m["t"])))
            elif m.get("mark") == "quiesced":
                answered.add(str(m.get("id", "load_end")))
        open_ = [r for r in reqs if r[0] not in answered]
        return max(open_, key=lambda r: r[1]) if open_ else None

    def check_quiescence(self) -> None:
        req = self.pending_request()
        if req is None:
            return
        rid, t_req = req
        now = now_ms()
        elapsed = (now - t_req) / 1000
        backlog = all(self.backlog_ok.get(n, False) for n, _ in self.servers)
        counts = self.window_counts(now, t_req, 1)
        stationary = counts is not None and (
            max(counts) - min(counts) <= max(5.0, 0.3 * sum(counts) / len(counts)))
        heavy_above, heavy_floor = self.last_above_floor(t_req, 3, 2.0, 262144.0)
        del_above, del_floor = self.last_above_floor(t_req, 2, 2.0, 20.0)
        since_heavy = (now - heavy_above) / 1000
        since_del = (now - del_above) / 1000
        min_secs = self.args.quiesce_min_secs
        if (elapsed >= min_secs and since_heavy >= min_secs and since_del >= self.args.quiet_secs
                and backlog and stationary):
            detail = {
                "id": rid,
                "since_request_secs": round(elapsed, 1),
                "since_last_put_bytes_above_floor_secs": round(since_heavy, 1),
                "since_last_deletes_above_floor_secs": round(since_del, 1),
                "window_secs": self.args.quiet_secs,
                "heavy_puts_per_window": counts,
                "put_bytes_floor_per_window": heavy_floor,
                "deletes_floor_per_window": del_floor,
                "heavy_puts_cumulative": self.heavy[-1][1],
                "quiesce_min_secs": min_secs,
            }
            self.emit("quiescence", detail)
            append_mark(self.run, "quiesced", **detail)
            print(f"[scrape] quiesced {detail}", flush=True)

    def at(self, t: float):
        """The last sample at or before t (samples are appended in time order)."""
        i = bisect.bisect_right(self.heavy, (t, float("inf"), float("inf"), float("inf")))
        return self.heavy[i - 1] if i else None

    def window_counts(self, now: int, t_req: int, col: int):
        """Counts (col 1 heavy PUTs, 2 deletes) in each of the last --windows
        windows of --quiet-secs, newest first; None until that history exists
        after the request."""
        w = 1000 * self.args.quiet_secs
        edges = [self.at(now - k * w) for k in range(self.args.windows + 1)]
        if any(e is None or e[0] < t_req for e in edges):
            return None
        return [edges[k][col] - edges[k + 1][col] for k in range(self.args.windows)]

    def last_above_floor(self, t_req: int, col: int, factor: float, margin: float):
        """(t of the last sliding --quiet-secs window above the floor, floor)
        for column col: above means more than factor x the quietest such
        window since the request + margin; the request time itself when no
        window has been above it, now when there is no window yet."""
        w = 1000 * self.args.quiet_secs
        slides = []
        for h in self.heavy:
            if h[0] - w < t_req:
                continue
            base = self.at(h[0] - w)
            if base is not None:
                slides.append((h[0], h[col] - base[col]))
        if not slides:
            return now_ms(), None
        floor = min(n for _, n in slides)
        above = [t for t, n in slides if n > factor * floor + margin]
        return (max(above) if above else t_req), floor

    def run_forever(self) -> None:
        start = time.time()
        while True:
            self.seq += 1
            self.auth = read_headers(self.args.auth_file)
            for name, url in self.servers:
                self.scrape_server(name, url)
            self.scrape_s3()
            self.out.flush()
            self.check_quiescence()
            next_t = start + self.seq * self.args.interval
            time.sleep(max(0.0, next_t - time.time()))


# ---- snapshot / usage / posture / envfile -------------------------------------

def cmd_snapshot(args) -> int:
    t = now_ms()
    base = args.s3lite.rstrip("/")
    st, s2, err = get_json(f"{base}/_s3lite/stats2")
    if s2 is None or err:
        print(f"snapshot: s3lite stats2 failed: {err}", file=sys.stderr)
        return 1
    _, v1, _ = get_json(f"{base}/_s3lite/stats")
    s2["stats"] = v1
    s2["t_ms"] = t
    if not args.boundary:
        with open(os.path.join(args.run, f"s3lite-{args.label}.json"), "w", encoding="utf-8") as f:
            json.dump(s2, f, indent=1, sort_keys=True)
    auth = read_headers(args.auth_file)
    servers = {}
    for name, url in parse_servers(args.servers):
        _, store, serr = get_json(f"{url}/v1/debug/store?window=10", auth)
        _, load, lerr = get_json(f"{url}/v1/debug/load", auth)
        servers[name] = {
            "store": shape_store(store) if store and not serr else None,
            "boot_id": (load or {}).get("boot_id") if not lerr else None,
            "error": serr or lerr,
        }
    procs = proc_stats(args.pids)
    if args.boundary:
        # A phase boundary: one line, so a point with many phases stays one file.
        s2.pop("live_objects", None)
        rec = {"t_ms": t, "label": args.label, "s3lite": s2, "servers": servers, "procs": procs}
        with open(os.path.join(args.run, "boundaries.jsonl"), "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
        return 0
    with open(os.path.join(args.run, f"store-{args.label}.json"), "w", encoding="utf-8") as f:
        json.dump({"t_ms": t, "servers": servers, "procs": procs}, f, indent=1, sort_keys=True)
    return 0


def cpu_secs(text: str) -> float | None:
    """ps's TIME column: [[dd-]hh:]mm:ss[.ss] (macOS prints minutes past 59)."""
    text = text.strip()
    if not text:
        return None
    days = 0
    if "-" in text:
        d, text = text.split("-", 1)
        days = int(d)
    secs = 0.0
    for part in text.split(":"):
        secs = secs * 60 + float(part)
    return days * 86400 + secs


def proc_stats(spec: str) -> dict:
    """CPU seconds and RSS of the rig's processes (name=pid[,name=pid]),
    read with ps: the local stand-in for D1's active-CPU meter."""
    out = {}
    for part in (spec or "").split(","):
        if "=" not in part:
            continue
        name, pid = part.split("=", 1)
        try:
            r = subprocess.run(["ps", "-o", "time=,rss=", "-p", pid], capture_output=True, text=True, timeout=5)
            fields = r.stdout.split()
            out[name] = {"pid": int(pid), "cpu_s": cpu_secs(fields[0]) if fields else None,
                         "rss_kb": int(fields[1]) if len(fields) > 1 else None}
        except (OSError, ValueError, subprocess.SubprocessError) as e:
            out[name] = {"pid": pid, "error": str(e)}
    return out


METER_FIELDS = ("ingestPayloadBytes", "ingestRecords", "readPayloadBytes", "readRecords",
                "readOperations", "queueOperations", "appendRequests")


def expected_meters(run: str) -> dict:
    """What the meters must reach: every acked append, whichever mode made
    it (walk/tail/group/subs run an in-process producer when given
    --rate/--mbps/--concurrency), and every delivered payload byte (each
    delivery counts, redeliveries included, as the read meter does)."""
    modes = {m.get("i"): m.get("mode") for m in read_marks(run) if m.get("mark") == "phase_start"}
    out = {"ingestRecords": 0, "readPayloadBytes": 0}
    for i in modes:
        try:
            with open(os.path.join(run, f"phase-{i}.ledger.json"), encoding="utf-8") as f:
                lg = json.load(f)
        except (FileNotFoundError, ValueError):
            continue
        out["ingestRecords"] += int(lg.get("acked_records") or 0)
        out["readPayloadBytes"] += int(lg.get("delivered_payload_bytes") or 0)
    return out


STREAM_USAGE_CAP = 1000


def live_stream_names(run: str) -> list[str]:
    """The customer streams the phases addressed and did not delete: every
    ledger's per_stream names (a `name#key` target counts once), except
    churn phases, whose streams are deleted or expire."""
    names = set()
    for m in read_marks(run):
        if m.get("mark") != "phase_start" or m.get("mode") == "churn":
            continue
        try:
            with open(os.path.join(run, f"phase-{m.get('i')}.ledger.json"), encoding="utf-8") as f:
                per = json.load(f).get("per_stream") or {}
        except (FileNotFoundError, ValueError):
            continue
        names.update(k.split("#", 1)[0] for k in per)
    return sorted(names)


def stream_usage(args) -> dict:
    """GET /v1/streams/{name}/usage/current for every live customer stream.
    The project row's storageByteSeconds is the RECORDED integral: it
    advances only when a segment's gauge is accounted (appends, sweeps,
    closes), so after the last append it stands still. The per-stream
    answer is provisional: the recorded integral plus each open segment's
    gauge extrapolated to now (src/rollup.rs storage_byte_ms_provisional).
    price.py sums these, backed off to the run's end snapshot."""
    names = live_stream_names(args.run)
    out = {"names": len(names), "capped": len(names) > STREAM_USAGE_CAP, "streams": {}}
    hdrs = read_headers(args.headers_file)
    for name in names[:STREAM_USAGE_CAP]:
        enc = "/".join(urllib.parse.quote(p, safe="") for p in name.split("/"))
        t = now_ms()
        st, b, err = get_json(f"{args.rollup.rstrip('/')}/v1/streams/{enc}/usage/current", hdrs)
        out["streams"][name] = {"t_ms": t, "status": st, "error": err, "usage": b}
    return out


def cmd_usage(args) -> int:
    expect = expected_meters(args.run)
    url = f"{args.rollup.rstrip('/')}/v1/projects/{args.project}/usage"
    deadline = time.time() + args.max_wait_secs
    prev = None
    polls = 0
    settled = False
    body = None
    status = 0
    while True:
        polls += 1
        status, body, err = get_json(url, read_headers(args.headers_file))
        if status == 200 and body:
            cur = tuple(body.get(k) for k in METER_FIELDS)
            # Reads flush every 10 s (READ_FLUSH_INTERVAL_MS) and settle later
            # than appends: wait for both meters to reach the generators.
            enough = all(int(body.get(k) or 0) >= v for k, v in expect.items())
            if enough and cur == prev:
                settled = True
                break
            prev = cur
        if time.time() >= deadline:
            break
        time.sleep(args.poll_secs)
    auth = read_headers(args.auth_file)
    debug = {}
    for name, surl in parse_servers(args.servers):
        _, b, e = get_json(f"{surl}/v1/debug/usage", auth)
        debug[name] = b if not e else {"error": e}
    doc = {
        "t_ms": now_ms(),
        "project_id": args.project,
        "status": status,
        "settled": settled,
        "polls": polls,
        "expected_ingest_records": expect["ingestRecords"],
        "expected_read_payload_bytes": expect["readPayloadBytes"],
        "project": body,
        "streams_current": stream_usage(args),
        "debug_usage": debug,
    }
    with open(os.path.join(args.run, "usage.json"), "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=1, sort_keys=True)
    got = {k: (body or {}).get(k) for k in expect}
    print(f"[usage] status={status} settled={settled} polls={polls} meters={got} expected={expect}", flush=True)
    return 0 if status == 200 else 1


ANSI = re.compile(r"\x1b\[[0-9;]*m")
SECRET_KEYS = re.compile(r"(TOKEN|SECRET|PASSWORD|STREAM_KEY|ACCESS_KEY)")


def redact(key: str, value: str) -> str:
    if key.endswith("_FILE"):
        return value
    if SECRET_KEYS.search(key):
        return "<redacted>" if value else ""
    return value


def sha256_file(path: str) -> str | None:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except FileNotFoundError:
        return None


def parse_envfile(path: str) -> dict[str, str]:
    out = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            if v.startswith("'") and v.endswith("'"):
                v = v[1:-1].replace("'\\''", "'")
            out[k] = v
    return out


def startup_summary(log_path: str):
    try:
        with open(log_path, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = ANSI.sub("", line)
                if "effective configuration (redacted)" in line:
                    m = re.search(r"config=(\{.*\})", line)
                    if m:
                        try:
                            return json.loads(m.group(1))
                        except json.JSONDecodeError:
                            return m.group(1)
                    return line.strip()
    except FileNotFoundError:
        pass
    return None


def tool_version(cmd: list[str]) -> str | None:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def cmd_posture(args) -> int:
    root = args.root
    git = lambda *a: subprocess.run(["git", "-C", root, *a], capture_output=True, text=True).stdout.strip()  # noqa: E731
    head = git("rev-parse", "HEAD")
    porcelain = subprocess.run(["git", "-C", root, "status", "--porcelain"], capture_output=True, text=True).stdout
    dirty = [l for l in porcelain.splitlines() if l.strip()]  # keep the XY columns intact
    auth = read_headers(args.auth_file)
    servers = {}
    for name, url in parse_servers(args.servers):
        n = name.rsplit("-", 1)[-1]
        env = parse_envfile(os.path.join(args.env_dir, f"server-{n}.env"))
        env_red = {k: redact(k, v).replace(args.env_dir, "<secrets>") for k, v in env.items()}
        _, load, err = get_json(f"{url}/v1/debug/load", auth)
        load = load or {}
        servers[name] = {
            "url": url,
            "env": env_red,
            "git_commit": load.get("git_commit"),
            "build_unix": load.get("build_unix"),
            "boot_id": load.get("boot_id"),
            "compactor_profile": load.get("compactor_profile"),
            "startup_summary": startup_summary(os.path.join(args.run, f"server-{n}.log")),
            "load_error": err,
        }
    commits = {s["git_commit"] for s in servers.values()}
    bins = {b: sha256_file(os.path.join(root, "target", "release", b)) for b in ("streams-slate", "s3lite", "pilot")}
    with open(os.path.join(args.run, "point.env"), encoding="utf-8") as f:
        point_src = f.read()
    doc = {
        "t_ms": now_ms(),
        "point": args.point,
        "point_file": point_src,
        "git": {"head": head, "dirty_paths": dirty},
        "binary_commit_matches_head": commits == {head},
        "binaries_sha256": bins,
        "k2gen_sha256": sha256_file(os.path.join(root, "bench", "k2cost", "k2gen.ts")),
        "k2gen_sources_sha256": {f: sha256_file(os.path.join(root, "bench", "k2cost", f))
                                 for f in ("common.ts", "produce.ts", "consume.ts", "churn.ts", "corpus.ts")},
        "bun_version": tool_version(["bun", "--version"]),
        "s3lite": {"latency_ms": int(args.latency_ms), "bucket": args.bucket, "url": args.s3lite},
        "base": args.base,
        "customer_project": args.project,
        "servers": servers,
        "prices": {
            "tigris_class_a_usd": 5e-6, "tigris_class_b_usd": 5e-7,
            "tigris_storage_usd_per_gib_month": 0.02,
            "k2_produce_usd_per_gb": 0.04, "k2_consume_usd_per_gb": 0.04,
            "k2_retained_usd_per_gb_month": 0.02, "as_of": "2026-10-02",
        },
    }
    with open(os.path.join(args.run, "posture.json"), "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=1, sort_keys=True)
    if not doc["binary_commit_matches_head"]:
        # §6: refuse a run whose binary was not built from the measured commit.
        print(f"[posture] REFUSED: server git_commit {sorted(c or '?' for c in commits)} != worktree HEAD {head[:12]}",
              flush=True)
        return 3
    return 0


def cmd_envfile(_args) -> int:
    env: dict[str, str] = {}
    for raw in sys.stdin:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("-"):
            env.pop(line[1:], None)
            continue
        if "=" not in line or not re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", line):
            print(f"envfile: not KEY=VALUE: {line!r}", file=sys.stderr)
            return 2
        k, v = line.split("=", 1)
        env.pop(k, None)  # later wins, and moves to the end
        env[k] = v
    for k, v in env.items():
        sys.stdout.write(f"{k}='" + v.replace("'", "'\\''") + "'\n")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p):
        p.add_argument("--run", required=True, help="the point's results directory")
        p.add_argument("--servers", required=True, help="name=url[,name=url]")
        p.add_argument("--s3lite", required=True)
        p.add_argument("--auth-file", required=True, help="file holding the deployment-bearer header line")

    p = sub.add_parser("loop")
    common(p)
    p.add_argument("--interval", type=float, default=10.0)
    p.add_argument("--quiesce-min-secs", type=float, default=1800.0,
                   help="minimum wait after load end: two GC cycles (2 x (600 s sweep + 300 s min age))")
    p.add_argument("--quiet-secs", type=float, default=300.0,
                   help="window over which heavy (compaction/history/multipart) PUTs are counted")
    p.add_argument("--windows", type=int, default=3,
                   help="consecutive windows whose heavy-PUT counts must agree (stationary floor)")
    p.add_argument("--absorb-age-secs", type=float, default=60.0,
                   help="the posture's ABSORB_AGE_SECS: absorption is stuck past two of them")
    p = sub.add_parser("snapshot")
    common(p)
    p.add_argument("--pids", default="", help="name=pid[,name=pid]: processes whose CPU time to record")
    p.add_argument("--label", required=True)
    p.add_argument("--boundary", action="store_true",
                   help="append to <run>/boundaries.jsonl (phase boundaries) instead of writing files")
    p = sub.add_parser("usage")
    common(p)
    p.add_argument("--rollup", required=True, help="the ROLLUP=1 server's URL")
    p.add_argument("--headers-file", required=True)
    p.add_argument("--project", required=True)
    p.add_argument("--max-wait-secs", type=float, default=330.0)
    p.add_argument("--poll-secs", type=float, default=10.0)
    p = sub.add_parser("posture")
    common(p)
    p.add_argument("--root", required=True)
    p.add_argument("--env-dir", required=True)
    p.add_argument("--point", required=True)
    p.add_argument("--base", required=True)
    p.add_argument("--latency-ms", required=True)
    p.add_argument("--bucket", required=True)
    p.add_argument("--project", required=True)
    sub.add_parser("envfile")
    args = ap.parse_args()
    if args.cmd == "loop":
        Loop(args).run_forever()
        return 0
    return {"snapshot": cmd_snapshot, "usage": cmd_usage, "posture": cmd_posture, "envfile": cmd_envfile}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main())
