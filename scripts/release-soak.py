#!/usr/bin/env python3
"""Run and judge the reproducible target-hardware release soak.

Secrets are accepted only through SOAK_STREAM_KEY, exactly one of
SOAK_AUTH_TOKEN/SOAK_AUTH_TOKEN_FILE, and SOAK_OPERATOR_TOKEN. Qualifying
production runs require the mode-0600 token file and at least one successful
JWT rotation. The evidence artifact contains target identity, workload,
bounded aggregate observations, and explicit pass/fail checks, never secrets.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import datetime as dt
import json
import math
import os
import pathlib
import stat
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request


FORMAT_VERSION = 1
MIN_RELEASE_SOAK_SECS = 24 * 60 * 60


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0 or not math.isfinite(parsed):
        raise argparse.ArgumentTypeError("must be a finite non-negative number")
    return parsed


def jwt_subject(token: str) -> str | None:
    parts = token.split(".")
    if len(parts) != 3 or len(parts[1]) > 16 * 1024:
        return None
    try:
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        value = json.loads(base64.urlsafe_b64decode(payload).decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    subject = value.get("sub") if isinstance(value, dict) else None
    if not isinstance(subject, str) or not 1 <= len(subject) <= 128:
        return None
    if any(not (character.isascii() and (character.isalnum() or character in "-_.")) for character in subject):
        return None
    return subject


def read_token_file(path: pathlib.Path, expected_subject: str | None = None) -> str:
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError("token path is not a regular file")
    if metadata.st_mode & 0o077:
        raise ValueError("token file is accessible by group or other")
    if metadata.st_size <= 0 or metadata.st_size > 16 * 1024:
        raise ValueError("token file is empty or too large")
    token = path.read_text().strip()
    if not token or len(token) > 16 * 1024 or any(character.isspace() for character in token):
        raise ValueError("token file is empty, oversized, or contains whitespace")
    if expected_subject is not None and jwt_subject(token) != expected_subject:
        raise ValueError("token JWT subject changed")
    return token


class RotatingToken:
    def __init__(self, path: pathlib.Path, subject: str, refresh_secs: int) -> None:
        self.path = path
        self.subject = subject
        self.refresh_secs = refresh_secs
        self.current = read_token_file(path, subject)
        self.refresh_successes = 0
        self.token_changes = 0
        self.refresh_failures = 0
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._refresh_loop, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=self.refresh_secs + 2)

    def token(self) -> str:
        with self.lock:
            return self.current

    def evidence(self) -> dict[str, object]:
        with self.lock:
            return {
                "source": "file",
                "subject_pinned": True,
                "refresh_successes": self.refresh_successes,
                "token_changes": self.token_changes,
                "refresh_failures": self.refresh_failures,
            }

    def _refresh_loop(self) -> None:
        while not self.stop_event.wait(self.refresh_secs):
            try:
                token = read_token_file(self.path, self.subject)
            except (OSError, UnicodeDecodeError, ValueError):
                with self.lock:
                    self.refresh_failures += 1
                continue
            with self.lock:
                self.refresh_successes += 1
                if token != self.current:
                    self.current = token
                    self.token_changes += 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True, help="load-balanced service URL")
    parser.add_argument(
        "--metrics-url",
        action="append",
        required=True,
        help="operator endpoint base for one instance; repeat for every instance",
    )
    parser.add_argument("--bench-bin", default="target/release/bench")
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--target-label", required=True)
    parser.add_argument("--instance-class", required=True)
    parser.add_argument("--storage-provider", required=True)
    parser.add_argument("--duration-secs", type=positive_int, default=MIN_RELEASE_SOAK_SECS)
    parser.add_argument("--warmup-secs", type=int, default=60)
    parser.add_argument("--monitor-secs", type=positive_int, default=30)
    parser.add_argument("--drain-secs", type=int, default=300)
    parser.add_argument("--concurrency", type=positive_int, default=64)
    parser.add_argument("--streams", type=positive_int, default=16)
    parser.add_argument("--payload-bytes", type=positive_int, default=256)
    parser.add_argument("--entries", type=positive_int, default=1)
    parser.add_argument("--prefix", default=f"release-soak-{int(time.time())}")
    parser.add_argument("--allow-short", action="store_true")
    parser.add_argument("--require-token-rotation", action="store_true")
    parser.add_argument(
        "--auth-token-refresh-secs", type=positive_int, default=30
    )
    parser.add_argument("--require-noisy-neighbor", action="store_true")
    parser.add_argument("--attacker-stream")
    parser.add_argument("--attacker-concurrency", type=positive_int, default=8)
    parser.add_argument("--attacker-payload-bytes", type=positive_int, default=16 * 1024)
    parser.add_argument("--min-attacker-attempts", type=positive_int, default=1000)
    parser.add_argument(
        "--max-attacker-non-429-rate", type=nonnegative_float, default=0.0
    )
    parser.add_argument("--require-backup", action="store_true")
    parser.add_argument("--min-req-per-sec", type=nonnegative_float, default=1.0)
    parser.add_argument("--max-error-rate", type=nonnegative_float, default=0.0005)
    parser.add_argument("--max-p99-ms", type=nonnegative_float, default=250.0)
    parser.add_argument("--max-p999-ms", type=nonnegative_float, default=1000.0)
    parser.add_argument("--max-rss-bytes", type=positive_int, default=800 * 1024 * 1024)
    parser.add_argument(
        "--max-rss-growth-bytes", type=positive_int, default=64 * 1024 * 1024
    )
    parser.add_argument(
        "--max-absorber-pending-bytes", type=positive_int, default=256 * 1024 * 1024
    )
    parser.add_argument(
        "--max-absorber-end-bytes", type=int, default=32 * 1024 * 1024
    )
    parser.add_argument("--max-l0-ssts", type=positive_int, default=24)
    parser.add_argument("--max-unflushed-wal-ssts", type=positive_int, default=1024)
    parser.add_argument("--max-fence-events", type=int, default=0)
    args = parser.parse_args()
    if args.warmup_secs < 0 or args.drain_secs < 0:
        parser.error("warmup and drain durations must be non-negative")
    if args.max_absorber_end_bytes < 0 or args.max_fence_events < 0:
        parser.error("end-backlog and fence budgets must be non-negative")
    if len(args.metrics_url) > 64 or len(set(args.metrics_url)) != len(args.metrics_url):
        parser.error("provide between 1 and 64 unique metrics URLs")
    if args.duration_secs < MIN_RELEASE_SOAK_SECS and not args.allow_short:
        parser.error("release soak must run for at least 24 hours (or use --allow-short for CI)")
    if args.auth_token_refresh_secs > 3600:
        parser.error("auth token refresh interval must be at most 3600 seconds")
    if args.attacker_concurrency > 64:
        parser.error("attacker concurrency must be at most 64")
    if args.attacker_payload_bytes > 16 * 1024 * 1024:
        parser.error("attacker payload must be at most 16 MiB")
    if args.max_attacker_non_429_rate > 1:
        parser.error("attacker non-429 rate must be between zero and one")
    if args.attacker_stream is not None and not 1 <= len(args.attacker_stream) <= 512:
        parser.error("attacker stream must be 1..=512 characters")
    return args


def request(url: str, token: str | None = None) -> tuple[int, str]:
    headers = {"authorization": f"Bearer {token}"} if token else {}
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        return 0, str(error)


def mutate_request(
    method: str,
    url: str,
    token: str,
    stream_key: str,
    body: bytes,
) -> int:
    request_object = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={
            "authorization": f"Bearer {token}",
            "stream-encryption-key": stream_key,
            "content-type": "application/octet-stream",
        },
    )
    try:
        with urllib.request.urlopen(request_object, timeout=5) as response:
            response.read()
            return response.status
    except urllib.error.HTTPError as error:
        error.read()
        return error.code
    except (urllib.error.URLError, TimeoutError, OSError):
        return 0


class Attacker:
    def __init__(
        self,
        url: str,
        stream_key: str,
        token: RotatingToken,
        payload_bytes: int,
        concurrency: int,
    ) -> None:
        self.url = url
        self.stream_key = stream_key
        self.token_source = token
        self.payload = b"a" * payload_bytes
        self.concurrency = concurrency
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.statuses: dict[int, int] = {}
        self.threads: list[threading.Thread] = []

    def start(self) -> None:
        self.token_source.start()
        self.threads = [
            threading.Thread(target=self._worker, daemon=True)
            for _ in range(self.concurrency)
        ]
        for thread in self.threads:
            thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        for thread in self.threads:
            thread.join(timeout=7)
        self.token_source.stop()

    def evidence(self) -> dict[str, object]:
        with self.lock:
            statuses = dict(sorted(self.statuses.items()))
        attempts = sum(statuses.values())
        throttled = statuses.get(429, 0)
        return {
            "attempts": attempts,
            "throttled_429": throttled,
            "non_429": attempts - throttled,
            "non_429_rate": (attempts - throttled) / attempts if attempts else 1.0,
            "status_counts": {str(status): count for status, count in statuses.items()},
            "auth": self.token_source.evidence(),
        }

    def _worker(self) -> None:
        while not self.stop_event.is_set():
            status = mutate_request(
                "POST",
                self.url,
                self.token_source.token(),
                self.stream_key,
                self.payload,
            )
            with self.lock:
                self.statuses[status] = self.statuses.get(status, 0) + 1


def parse_openmetrics(body: str) -> dict[str, float]:
    metrics: dict[str, float] = {}
    for raw in body.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.rsplit(None, 1)
        if len(fields) != 2:
            continue
        try:
            metrics[fields[0]] = float(fields[1])
        except ValueError:
            continue
    return metrics


def values(metrics: dict[str, float], name: str) -> list[float]:
    return [
        value
        for key, value in metrics.items()
        if key == name or key.startswith(name + "{")
    ]


def finite_or_none(value: float | None) -> float | None:
    return value if value is not None and math.isfinite(value) else None


def scrape(base: str, operator_token: str, elapsed: float) -> dict[str, object]:
    ready_status, _ = request(base.rstrip("/") + "/health/ready")
    metrics_status, body = request(
        base.rstrip("/") + "/v1/debug/metrics", operator_token
    )
    if metrics_status != 200:
        return {
            "elapsed_secs": round(elapsed, 3),
            "target": base,
            "ready": ready_status == 200,
            "metrics_ok": False,
        }
    metrics = parse_openmetrics(body)
    components = values(metrics, "streams_component_ready")
    backup_age = next(
        iter(values(metrics, "streams_backup_recovery_point_age_seconds")), None
    )
    backup_budget = next(iter(values(metrics, "streams_backup_rpo_budget_seconds")), 0.0)
    return {
        "elapsed_secs": round(elapsed, 3),
        "target": base,
        "ready": ready_status == 200,
        "metrics_ok": True,
        "components_ready": bool(components) and all(value == 1 for value in components),
        "rss_bytes": max(values(metrics, "streams_process_resident_memory_bytes"), default=0),
        "absorber_pending_bytes": max(
            values(metrics, "streams_absorber_pending_bytes"), default=0
        ),
        "l0_ssts": max(values(metrics, "streams_shard_l0_ssts"), default=0),
        "unflushed_wal_ssts": max(
            values(metrics, "streams_shard_unflushed_wal_ssts"), default=0
        ),
        "fence_events": sum(values(metrics, "streams_fence_events_total")),
        "backup_configured": next(
            iter(values(metrics, "streams_backup_configured")), 0.0
        )
        == 1,
        "backup_recovery_point_age_secs": finite_or_none(backup_age),
        "backup_rpo_budget_secs": finite_or_none(backup_budget),
    }


def scrape_all(
    targets: list[str], operator_token: str, elapsed: float
) -> list[dict[str, object]]:
    # A cell scrape is one observation, not N serial five-second timeouts.
    # Bound the target set above and sample every instance concurrently.
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(targets)) as pool:
        futures = [pool.submit(scrape, target, operator_token, elapsed) for target in targets]
        return [future.result() for future in futures]


def quartile_growth(samples: list[float]) -> float:
    if len(samples) < 2:
        return 0.0
    width = max(1, len(samples) // 4)
    return max(0.0, statistics.median(samples[-width:]) - statistics.median(samples[:width]))


def check(observed: object, budget: object, passed: bool) -> dict[str, object]:
    return {"passed": passed, "observed": observed, "budget": budget}


def parse_bench_summary(stdout: str) -> dict[str, object] | None:
    for line in reversed(stdout.splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                return value
    return None


def main() -> int:
    args = parse_args()
    stream_key = os.environ.get("SOAK_STREAM_KEY")
    auth_token = os.environ.get("SOAK_AUTH_TOKEN")
    auth_token_file = os.environ.get("SOAK_AUTH_TOKEN_FILE")
    operator_token = os.environ.get("SOAK_OPERATOR_TOKEN")
    attacker_stream_key = os.environ.get("SOAK_ATTACKER_STREAM_KEY")
    attacker_token_file = os.environ.get("SOAK_ATTACKER_AUTH_TOKEN_FILE")
    missing = [
        name
        for name, value in [
            ("SOAK_STREAM_KEY", stream_key),
            ("SOAK_OPERATOR_TOKEN", operator_token),
        ]
        if not value
    ]
    if missing:
        raise SystemExit(f"missing secret environment variable(s): {', '.join(missing)}")
    if bool(auth_token) == bool(auth_token_file):
        raise SystemExit("set exactly one of SOAK_AUTH_TOKEN or SOAK_AUTH_TOKEN_FILE")
    require_token_rotation = args.require_token_rotation or not args.allow_short
    require_noisy_neighbor = args.require_noisy_neighbor or not args.allow_short
    if require_token_rotation and not auth_token_file:
        raise SystemExit(
            "qualifying release soak requires SOAK_AUTH_TOKEN_FILE for JWT rotation"
        )
    if require_noisy_neighbor and (not attacker_stream_key or not attacker_token_file):
        raise SystemExit(
            "noisy-neighbor soak requires SOAK_ATTACKER_STREAM_KEY and "
            "SOAK_ATTACKER_AUTH_TOKEN_FILE"
        )
    if require_noisy_neighbor and not auth_token_file:
        raise SystemExit("noisy-neighbor soak requires SOAK_AUTH_TOKEN_FILE")
    auth_token_subject = None
    if require_token_rotation or require_noisy_neighbor:
        try:
            initial_token = read_token_file(pathlib.Path(auth_token_file))
        except (OSError, UnicodeDecodeError, ValueError) as error:
            raise SystemExit(f"cannot read SOAK_AUTH_TOKEN_FILE: {error}") from error
        auth_token_subject = jwt_subject(initial_token)
        if auth_token_subject is None:
            raise SystemExit(
                "rotating workload token must be a JWT with a valid tenant subject"
            )
    attacker_subject = None
    if require_noisy_neighbor:
        if pathlib.Path(attacker_token_file) == pathlib.Path(auth_token_file or ""):
            raise SystemExit("victim and attacker token files must differ")
        try:
            attacker_initial = read_token_file(pathlib.Path(attacker_token_file))
        except (OSError, UnicodeDecodeError, ValueError) as error:
            raise SystemExit(f"cannot read SOAK_ATTACKER_AUTH_TOKEN_FILE: {error}") from error
        attacker_subject = jwt_subject(attacker_initial)
        if attacker_subject is None or attacker_subject == auth_token_subject:
            raise SystemExit("victim and attacker JWT subjects must be valid and distinct")

    evidence_path = pathlib.Path(args.evidence)
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    started_utc = dt.datetime.now(dt.timezone.utc)
    started = time.monotonic()
    child_env = os.environ.copy()
    child_env["STREAM_KEY"] = stream_key
    child_env.pop("STREAMS_AUTH_TOKEN", None)
    child_env.pop("STREAMS_AUTH_TOKEN_FILE", None)
    if auth_token_file:
        child_env["STREAMS_AUTH_TOKEN_FILE"] = auth_token_file
    else:
        child_env["STREAMS_AUTH_TOKEN"] = auth_token
    command = [
        args.bench_bin,
        "--url",
        args.url,
        "--mode",
        "append",
        "--concurrency",
        str(args.concurrency),
        "--streams",
        str(args.streams),
        "--payload-bytes",
        str(args.payload_bytes),
        "--entries",
        str(args.entries),
        "--duration-secs",
        str(args.duration_secs),
        "--warmup-secs",
        str(args.warmup_secs),
        "--prefix",
        args.prefix,
        "--label",
        args.release_id,
        "--json",
        "--verify-offsets",
        "--auth-token-refresh-secs",
        str(args.auth_token_refresh_secs),
    ]
    if auth_token_subject:
        command.extend(["--auth-token-subject", auth_token_subject])
    attacker = None
    attacker_stream = args.attacker_stream or f"{args.prefix}-attacker"
    if require_noisy_neighbor:
        attacker_token = RotatingToken(
            pathlib.Path(attacker_token_file),
            attacker_subject,
            args.auth_token_refresh_secs,
        )
        attacker_url = f"{args.url.rstrip('/')}/v1/stream/{attacker_stream}"
        create_status = mutate_request(
            "PUT",
            attacker_url,
            attacker_token.token(),
            attacker_stream_key,
            b"",
        )
        if create_status not in (200, 201, 204):
            raise SystemExit(f"attacker stream create failed with status {create_status}")
        attacker = Attacker(
            attacker_url,
            attacker_stream_key,
            attacker_token,
            args.attacker_payload_bytes,
            args.attacker_concurrency,
        )
    bench = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=child_env,
    )
    samples: list[dict[str, object]] = []
    if attacker:
        attacker.start()
    try:
        while bench.poll() is None:
            sampled_at = time.monotonic()
            samples.extend(scrape_all(args.metrics_url, operator_token, sampled_at - started))
            time.sleep(max(0.0, args.monitor_secs - (time.monotonic() - sampled_at)))
    finally:
        if attacker:
            attacker.stop()
    stdout, stderr = bench.communicate()
    drain_until = time.monotonic() + args.drain_secs
    while time.monotonic() < drain_until:
        sampled_at = time.monotonic()
        samples.extend(scrape_all(args.metrics_url, operator_token, sampled_at - started))
        remaining = max(0.0, drain_until - time.monotonic())
        time.sleep(min(max(0.0, args.monitor_secs - (time.monotonic() - sampled_at)), remaining))
    samples.extend(scrape_all(args.metrics_url, operator_token, time.monotonic() - started))

    summary = parse_bench_summary(stdout)
    attacker_evidence = attacker.evidence() if attacker else {"required": False}
    good = [sample for sample in samples if sample.get("metrics_ok")]
    by_target: dict[str, list[dict[str, object]]] = {}
    for sample in good:
        by_target.setdefault(str(sample["target"]), []).append(sample)
    rss = [float(sample["rss_bytes"]) for sample in good]
    absorber = [float(sample["absorber_pending_bytes"]) for sample in good]
    l0 = [float(sample["l0_ssts"]) for sample in good]
    wal = [float(sample["unflushed_wal_ssts"]) for sample in good]
    rss_growth = max(
        (
            quartile_growth([float(sample["rss_bytes"]) for sample in target_samples])
            for target_samples in by_target.values()
        ),
        default=0.0,
    )
    absorber_end = max(
        (float(target_samples[-1]["absorber_pending_bytes"]) for target_samples in by_target.values()),
        default=math.inf,
    )
    fence_delta = sum(
        max(
            0.0,
            float(target_samples[-1]["fence_events"])
            - float(target_samples[0]["fence_events"]),
        )
        for target_samples in by_target.values()
    )
    req_ok = int(summary.get("requests_ok", 0)) if summary else 0
    errors = int(summary.get("errors", 0)) if summary else 0
    attempts = req_ok + errors
    error_rate = errors / attempts if attempts else 1.0
    latency = summary.get("latency_ms", {}) if summary else {}
    p99 = float(latency.get("p99", math.inf)) if isinstance(latency, dict) else math.inf
    p999 = float(latency.get("p999", math.inf)) if isinstance(latency, dict) else math.inf
    p99_observed = finite_or_none(p99)
    p999_observed = finite_or_none(p999)
    rps = float(summary.get("req_per_sec", 0.0)) if summary else 0.0
    offset_verification = summary.get("offset_verification", {}) if summary else {}
    auth_evidence = summary.get("auth", {}) if summary else {}
    offsets_verified = bool(
        isinstance(offset_verification, dict)
        and offset_verification.get("enabled")
        and offset_verification.get("passed")
    )
    expected_samples = max(
        1,
        math.floor((args.duration_secs + args.warmup_secs) / args.monitor_secs)
        * len(args.metrics_url),
    )
    rpo_samples = [
        (
            sample.get("backup_recovery_point_age_secs"),
            sample.get("backup_rpo_budget_secs"),
        )
        for sample in good
        if sample.get("backup_configured")
    ]
    rpo_within_budget = all(
        age is not None and budget is not None and float(age) <= float(budget)
        for age, budget in rpo_samples
    )
    checks = {
        "release_duration": check(
            args.duration_secs,
            f">={MIN_RELEASE_SOAK_SECS}",
            args.duration_secs >= MIN_RELEASE_SOAK_SECS or args.allow_short,
        ),
        "bench_exit": check(bench.returncode, 0, bench.returncode == 0 and summary is not None),
        "durable_offsets": check(
            offset_verification,
            "every stream next offset equals successful generated entries",
            offsets_verified,
        ),
        "auth_token_rotation": check(
            auth_evidence,
            "file source, at least one token change, and zero refresh failures"
            if require_token_rotation
            else "rotation not required for this short run",
            isinstance(auth_evidence, dict)
            and (
                not require_token_rotation
                or (
                    auth_evidence.get("source") == "file"
                    and auth_evidence.get("subject_pinned") is True
                    and int(auth_evidence.get("token_changes", 0)) >= 1
                    and int(auth_evidence.get("refresh_failures", 0)) == 0
                )
            ),
        ),
        "noisy_neighbor_isolation": check(
            attacker_evidence,
            {
                "required": require_noisy_neighbor,
                "min_attempts": args.min_attacker_attempts,
                "max_non_429_rate": args.max_attacker_non_429_rate,
                "distinct_subjects": True,
                "rotated_subject_pinned_auth": require_token_rotation,
            },
            (not require_noisy_neighbor)
            or (
                int(attacker_evidence.get("attempts", 0))
                >= args.min_attacker_attempts
                and float(attacker_evidence.get("non_429_rate", 1.0))
                <= args.max_attacker_non_429_rate
                and isinstance(attacker_evidence.get("auth"), dict)
                and attacker_evidence["auth"].get("subject_pinned") is True
                and (
                    not require_token_rotation
                    or (
                        int(attacker_evidence["auth"].get("token_changes", 0)) >= 1
                        and int(attacker_evidence["auth"].get("refresh_failures", 0)) == 0
                    )
                )
            ),
        ),
        "append_error_rate": check(error_rate, args.max_error_rate, error_rate <= args.max_error_rate),
        "throughput": check(rps, args.min_req_per_sec, rps >= args.min_req_per_sec),
        "ack_p99_ms": check(p99_observed, args.max_p99_ms, p99_observed is not None and p99 <= args.max_p99_ms),
        "ack_p999_ms": check(p999_observed, args.max_p999_ms, p999_observed is not None and p999 <= args.max_p999_ms),
        "monitor_coverage": check(len(good), expected_samples, len(good) >= expected_samples),
        "readiness": check(
            sum(not bool(sample.get("ready")) for sample in samples),
            0,
            bool(samples) and all(bool(sample.get("ready")) for sample in samples),
        ),
        "component_health": check(
            sum(not bool(sample.get("components_ready")) for sample in good),
            0,
            bool(good) and all(bool(sample.get("components_ready")) for sample in good),
        ),
        "rss_max_bytes": check(max(rss, default=0), args.max_rss_bytes, bool(rss) and max(rss) <= args.max_rss_bytes),
        "rss_growth_bytes": check(rss_growth, args.max_rss_growth_bytes, rss_growth <= args.max_rss_growth_bytes),
        "absorber_pending_max_bytes": check(max(absorber, default=0), args.max_absorber_pending_bytes, bool(absorber) and max(absorber) <= args.max_absorber_pending_bytes),
        "absorber_pending_end_bytes": check(absorber_end if math.isfinite(absorber_end) else None, args.max_absorber_end_bytes, math.isfinite(absorber_end) and absorber_end <= args.max_absorber_end_bytes),
        "l0_ssts_max": check(max(l0, default=0), args.max_l0_ssts, bool(l0) and max(l0) <= args.max_l0_ssts),
        "unflushed_wal_ssts_max": check(max(wal, default=0), args.max_unflushed_wal_ssts, bool(wal) and max(wal) <= args.max_unflushed_wal_ssts),
        "fence_events": check(fence_delta, args.max_fence_events, bool(good) and fence_delta <= args.max_fence_events),
        "backup_protection": check(
            {
                "required": args.require_backup,
                "configured_samples": len(rpo_samples),
                "within_budget": rpo_within_budget,
            },
            "configured and within RPO budget" if args.require_backup else "not required",
            (not args.require_backup)
            or (len(rpo_samples) == len(good) and bool(rpo_samples) and rpo_within_budget),
        ),
    }
    passed = all(bool(item["passed"]) for item in checks.values())
    evidence = {
        "format_version": FORMAT_VERSION,
        "status": "pass" if passed else "fail",
        "release_id": args.release_id,
        "target": {
            "label": args.target_label,
            "instance_class": args.instance_class,
            "storage_provider": args.storage_provider,
            "metrics_targets": len(args.metrics_url),
        },
        "started_utc": started_utc.isoformat(),
        "ended_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "elapsed_secs": round(time.monotonic() - started, 3),
        "workload": {
            "duration_secs": args.duration_secs,
            "warmup_secs": args.warmup_secs,
            "drain_secs": args.drain_secs,
            "concurrency": args.concurrency,
            "streams": args.streams,
            "payload_bytes": args.payload_bytes,
            "entries": args.entries,
            "prefix": args.prefix,
            "short_run": args.duration_secs < MIN_RELEASE_SOAK_SECS,
            "token_rotation_required": require_token_rotation,
            "noisy_neighbor_required": require_noisy_neighbor,
            "attacker_stream": attacker_stream if require_noisy_neighbor else None,
            "attacker_concurrency": args.attacker_concurrency
            if require_noisy_neighbor
            else 0,
            "attacker_payload_bytes": args.attacker_payload_bytes
            if require_noisy_neighbor
            else 0,
        },
        "bench": summary,
        "noisy_neighbor": attacker_evidence,
        "monitor": {
            "samples": len(samples),
            "successful_samples": len(good),
            "rss_max_bytes": max(rss, default=0),
            "rss_growth_bytes": rss_growth,
            "absorber_pending_max_bytes": max(absorber, default=0),
            "absorber_pending_end_bytes": absorber_end if math.isfinite(absorber_end) else None,
            "l0_ssts_max": max(l0, default=0),
            "unflushed_wal_ssts_max": max(wal, default=0),
            "fence_event_delta": fence_delta,
        },
        "checks": checks,
        "bench_stderr_tail": stderr[-2000:] if bench.returncode else "",
    }
    encoded = json.dumps(
        evidence, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    evidence_path.write_text(encoded + "\n")
    print(encoded)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
