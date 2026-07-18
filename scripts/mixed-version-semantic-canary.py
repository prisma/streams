#!/usr/bin/env python3
"""Run one restart-safe phase of the deployed mixed-version semantic canary.

Invoke the same run in order: read-first, canary-flip, rollback, finalize. The
deploy system changes binaries/write formats between invocations and first
produces a matching judge-mixed-version-canary.py capability artifact.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import stat
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


PHASES = ("read-first", "canary-flip", "rollback", "finalize")
MAX_FILE_BYTES = 1024 * 1024
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
SAFE_ID = re.compile(r"^[A-Za-z0-9_.+-]{1,128}$")
SAFE_STREAM = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
HEX_256 = re.compile(r"^[0-9a-f]{64}$")


class CanaryError(Exception):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise CanaryError(message)


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def read_bounded(path: Path, maximum: int = MAX_FILE_BYTES) -> bytes:
    try:
        metadata = path.stat()
    except OSError as error:
        raise CanaryError(f"cannot inspect {path.name}: {error.strerror}") from error
    require(stat.S_ISREG(metadata.st_mode), f"{path.name} is not a regular file")
    require(0 < metadata.st_size <= maximum, f"{path.name} is empty or oversized")
    try:
        return path.read_bytes()
    except OSError as error:
        raise CanaryError(f"cannot read {path.name}: {error.strerror}") from error


def read_secret(path: Path, label: str) -> str:
    metadata = path.stat()
    require(stat.S_ISREG(metadata.st_mode), f"{label} is not a regular file")
    require(metadata.st_mode & 0o077 == 0, f"{label} is accessible by group or other")
    require(0 < metadata.st_size <= 16 * 1024, f"{label} is empty or oversized")
    try:
        value = path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeDecodeError) as error:
        raise CanaryError(f"cannot read {label}") from error
    require(value and not any(character.isspace() for character in value),
            f"{label} contains whitespace")
    return value


def jwt_subject(token: str) -> str | None:
    parts = token.split(".")
    if len(parts) != 3 or len(parts[1]) > 16 * 1024:
        return None
    try:
        encoded = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(encoded))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    subject = payload.get("sub") if isinstance(payload, dict) else None
    if not isinstance(subject, str) or SAFE_ID.fullmatch(subject) is None:
        return None
    return subject


def read_stream_key(path: Path) -> str:
    value = read_secret(path, "stream key file")
    require(re.fullmatch(r"[A-Za-z0-9_-]{43}", value) is not None,
            "stream key is not canonical base64url")
    try:
        decoded = base64.urlsafe_b64decode(value + "=")
    except ValueError as error:
        raise CanaryError("stream key is not canonical base64url") from error
    require(len(decoded) == 32, "stream key must decode to 32 bytes")
    return value


def parse_json(raw: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CanaryError(f"{label} is not valid JSON") from error
    require(isinstance(value, dict), f"{label} must be a JSON object")
    return value


def validate_base_url(value: str, allow_http_loopback: bool) -> str:
    parsed = urllib.parse.urlsplit(value)
    require(parsed.scheme in ("http", "https") and parsed.hostname is not None,
            "service URL must be absolute HTTP(S)")
    require(parsed.username is None and parsed.password is None,
            "service URL must not contain credentials")
    require(not parsed.query and not parsed.fragment and parsed.path in ("", "/"),
            "service URL must be an origin without path, query, or fragment")
    if parsed.scheme != "https":
        require(
            allow_http_loopback and parsed.hostname in ("127.0.0.1", "::1", "localhost"),
            "plain HTTP is allowed only for an explicit loopback CI run",
        )
    return value.rstrip("/")


class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(self, allowed_origins: set[tuple[str, str]]) -> None:
        super().__init__()
        self.allowed_origins = allowed_origins

    def redirect_request(
        self,
        request: urllib.request.Request,
        file_pointer: Any,
        code: int,
        message: str,
        headers: Any,
        new_url: str,
    ) -> urllib.request.Request | None:
        parsed = urllib.parse.urlsplit(new_url)
        require((parsed.scheme, parsed.netloc) in self.allowed_origins,
                "service redirected a credential to an undeclared origin")
        return super().redirect_request(
            request, file_pointer, code, message, headers, new_url
        )


class Client:
    def __init__(self, origins: list[str], token: str, key: str, timeout: float) -> None:
        allowed = {
            (urllib.parse.urlsplit(origin).scheme, urllib.parse.urlsplit(origin).netloc)
            for origin in origins
        }
        self.opener = urllib.request.build_opener(SafeRedirectHandler(allowed))
        self.token = token
        self.key = key
        self.timeout = timeout

    def request(
        self,
        method: str,
        base_url: str,
        stream: str,
        body: bytes | None = None,
        producer_sequence: int | None = None,
    ) -> tuple[int, bytes]:
        encoded_stream = urllib.parse.quote(stream, safe="")
        headers = {
            "authorization": f"Bearer {self.token}",
            "stream-encryption-key": self.key,
            "content-type": "application/json",
        }
        if producer_sequence is not None:
            producer_hash = sha256(stream.encode())[:24]
            headers.update(
                {
                    "producer-id": f"migration-canary-{producer_hash}",
                    "producer-epoch": "0",
                    "producer-seq": str(producer_sequence),
                }
            )
        request = urllib.request.Request(
            f"{base_url}/v1/stream/{encoded_stream}",
            data=body,
            method=method,
            headers=headers,
        )
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                raw = response.read(MAX_RESPONSE_BYTES + 1)
                require(len(raw) <= MAX_RESPONSE_BYTES, "service response exceeds size bound")
                return response.status, raw
        except urllib.error.HTTPError as error:
            raw = error.read(MAX_RESPONSE_BYTES + 1)
            require(len(raw) <= MAX_RESPONSE_BYTES, "service error response exceeds size bound")
            return error.code, raw
        except CanaryError:
            raise
        except (urllib.error.URLError, TimeoutError, OSError):
            return 0, b""


def marker(run_id: str, phase: str, sequence: int) -> dict[str, Any]:
    return {"canary_run": run_id, "phase": phase, "sequence": sequence}


def expected_markers(run_id: str, count: int) -> list[dict[str, Any]]:
    return [marker(run_id, phase, index) for index, phase in enumerate(PHASES[:count])]


def decode_events(raw: bytes, label: str) -> list[Any]:
    try:
        events = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CanaryError(f"{label} returned invalid JSON") from error
    require(isinstance(events, list), f"{label} did not return a JSON event array")
    return events


def validate_state(
    value: dict[str, Any], run_id: str, stream: str, subject_commitment: str
) -> None:
    require(value.get("format_version") == 1, "state format is unsupported")
    require(value.get("kind") == "streams-mixed-version-semantic-state",
            "state kind is invalid")
    require(value.get("run_id") == run_id and value.get("stream") == stream,
            "state belongs to a different canary")
    require(value.get("subject_commitment") == subject_commitment,
            "authentication subject changed during the canary")
    completed = value.get("completed")
    require(isinstance(completed, list) and len(completed) <= len(PHASES),
            "state phase history is invalid")
    for index, entry in enumerate(completed):
        require(isinstance(entry, dict) and entry.get("phase") == PHASES[index],
                "state phase order is invalid")
        for field in ("marker_sha256", "capability_sha256", "evidence_sha256"):
            require(isinstance(entry.get(field), str) and HEX_256.fullmatch(entry[field]),
                    f"state {field} is invalid")


def read_capability_evidence(
    path: Path,
    run_id: str,
    phase: str,
    expected_history_writer: int,
    expected_backup_writer: int,
) -> tuple[dict[str, Any], str]:
    raw = read_bounded(path)
    value = parse_json(raw, "capability evidence")
    require(value.get("format_version") == 1, "capability evidence format is unsupported")
    require(value.get("kind") == "streams-mixed-version-capability-gate",
            "capability evidence kind is invalid")
    require(value.get("passed") is True, "capability gate did not pass")
    require(value.get("run_id") == run_id and value.get("phase") == phase,
            "capability evidence belongs to another run or phase")
    expected = value.get("expected")
    require(isinstance(expected, dict), "capability evidence expected set is missing")
    require(expected.get("history_writer") == expected_history_writer,
            "capability evidence has the wrong history writer")
    require(expected.get("backup_writer") == expected_backup_writer,
            "capability evidence has the wrong backup writer")
    return value, sha256(raw)


def write_new(path: Path, value: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    require(not path.exists(), f"{path.name} already exists")
    raw = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, path)
        fsync_directory(path.parent)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
    return sha256(raw)


def write_state(path: Path, value: dict[str, Any]) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
    return raw


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def load_state(
    path: Path, run_id: str, stream: str, subject_commitment: str
) -> tuple[dict[str, Any], bytes]:
    if not path.exists():
        return {
            "format_version": 1,
            "kind": "streams-mixed-version-semantic-state",
            "run_id": run_id,
            "stream": stream,
            "subject_commitment": subject_commitment,
            "completed": [],
        }, b""
    raw = read_bounded(path)
    metadata = path.stat()
    require(metadata.st_mode & 0o077 == 0,
            "state file is accessible by group or other")
    value = parse_json(raw, "semantic canary state")
    validate_state(value, run_id, stream, subject_commitment)
    return value, raw


def read_every_route(
    client: Client,
    routes: list[str],
    stream: str,
    expected: list[dict[str, Any]],
) -> list[int]:
    statuses: list[int] = []
    for index, route in enumerate(routes):
        status, raw = client.request("GET", route, stream)
        require(status == 200, f"read route {index} returned a non-200 status")
        require(decode_events(raw, f"read route {index}") == expected,
                f"read route {index} returned a divergent event sequence")
        statuses.append(status)
    return statuses


def validate_existing_evidence(
    value: dict[str, Any],
    run_id: str,
    phase: str,
    previous_state_sha256: str,
    capability_sha256: str,
    marker_sha256: str,
    previous_evidence_sha256: str,
    event_count: int,
    route_count: int,
    complete: bool,
    history_writer: int,
    backup_writer: int,
) -> None:
    require(value.get("format_version") == 1, "semantic evidence format is unsupported")
    require(value.get("kind") == "streams-mixed-version-semantic-phase",
            "semantic evidence kind is invalid")
    require(value.get("passed") is True, "semantic evidence did not pass")
    require(value.get("run_id") == run_id and value.get("phase") == phase,
            "semantic evidence belongs to another run or phase")
    require(value.get("previous_state_sha256") == previous_state_sha256,
            "semantic evidence does not chain from the current state")
    require(value.get("capability_sha256") == capability_sha256,
            "semantic evidence references different capability evidence")
    require(value.get("marker_sha256") == marker_sha256,
            "semantic evidence marker does not match")
    require(value.get("previous_evidence_sha256") == previous_evidence_sha256,
            "semantic evidence chain is invalid")
    require(value.get("event_count") == event_count,
            "semantic evidence event count is invalid")
    require(value.get("route_count") == route_count,
            "semantic evidence route count is invalid")
    require(value.get("all_reads_status") == [200],
            "semantic evidence does not prove successful reads")
    require(value.get("complete") is complete,
            "semantic evidence completion state is invalid")
    require(value.get("history_writer") == history_writer,
            "semantic evidence history writer is invalid")
    require(value.get("backup_writer") == backup_writer,
            "semantic evidence backup writer is invalid")
    completed_at_ms = value.get("completed_at_ms")
    require(type(completed_at_ms) is int and 0 < completed_at_ms < 10**16,
            "semantic evidence completion time is invalid")
    first_status = value.get("append_first_status")
    retry_status = value.get("append_retry_status")
    require(first_status == "resumed" or type(first_status) is int,
            "semantic evidence first append status is invalid")
    require(
        retry_status == "resumed"
        or (type(retry_status) is int and 200 <= retry_status < 300),
        "semantic evidence retry status is invalid",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=PHASES, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--stream", required=True)
    parser.add_argument("--url", required=True, help="load-balanced mutation origin")
    parser.add_argument(
        "--read-url",
        action="append",
        help="ingress origin that must read the exact corpus; defaults to --url",
    )
    parser.add_argument("--auth-token-file", type=Path, required=True)
    parser.add_argument("--stream-key-file", type=Path, required=True)
    parser.add_argument("--capability-evidence", type=Path, required=True)
    parser.add_argument("--expected-history-writer", type=int, choices=(1, 2), required=True)
    parser.add_argument("--expected-backup-writer", type=int, choices=(2, 3), required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--timeout-secs", type=float, default=10.0)
    parser.add_argument("--allow-http-loopback", action="store_true")
    parser.add_argument("--allow-opaque-token", action="store_true")
    args = parser.parse_args()
    if SAFE_ID.fullmatch(args.run_id) is None:
        parser.error("run ID must be a bounded safe identifier")
    if SAFE_STREAM.fullmatch(args.stream) is None:
        parser.error("stream must be a bounded safe name")
    if not 0 < args.timeout_secs <= 120:
        parser.error("timeout must be in (0, 120] seconds")
    if args.allow_opaque_token and not args.allow_http_loopback:
        parser.error("opaque tokens are allowed only for a loopback CI run")
    routes = args.read_url or [args.url]
    if not 1 <= len(routes) <= 64 or len(set(routes)) != len(routes):
        parser.error("provide 1..=64 unique read origins")
    try:
        args.url = validate_base_url(args.url, args.allow_http_loopback)
        args.read_url = [
            validate_base_url(route, args.allow_http_loopback) for route in routes
        ]
    except CanaryError as error:
        parser.error(str(error))
    return args


def run(args: argparse.Namespace) -> None:
    token = read_secret(args.auth_token_file, "authentication token file")
    subject = jwt_subject(token)
    require(subject is not None or args.allow_opaque_token,
            "authentication token must be a bounded JWT subject")
    key = read_stream_key(args.stream_key_file)
    key_bytes = base64.urlsafe_b64decode(key + "=")
    subject_commitment = hmac.new(
        key_bytes,
        b"streams-semantic-canary-subject-v1\0" + (subject or "opaque-ci-token").encode(),
        hashlib.sha256,
    ).hexdigest()
    _, capability_sha256 = read_capability_evidence(
        args.capability_evidence,
        args.run_id,
        args.phase,
        args.expected_history_writer,
        args.expected_backup_writer,
    )
    state, state_raw = load_state(
        args.state, args.run_id, args.stream, subject_commitment
    )
    completed: list[dict[str, Any]] = state["completed"]
    phase_index = PHASES.index(args.phase)

    if phase_index < len(completed):
        require(phase_index == len(completed) - 1,
                "cannot replay a phase older than the latest completed phase")
        require(capability_sha256 == completed[phase_index]["capability_sha256"],
                "completed phase capability evidence changed")
        evidence_raw = read_bounded(args.evidence)
        require(sha256(evidence_raw) == completed[phase_index]["evidence_sha256"],
                "completed phase evidence digest changed")
        print(f"mixed-version semantic canary already passed: {args.phase}")
        return
    require(phase_index == len(completed), "semantic canary phases must run in order")

    previous_state_sha256 = sha256(state_raw) if state_raw else "0" * 64
    current_marker = marker(args.run_id, args.phase, phase_index)
    marker_raw = json.dumps(
        [current_marker], separators=(",", ":"), sort_keys=True
    ).encode()
    marker_sha256 = sha256(marker_raw)

    all_origins = list(dict.fromkeys([args.url, *args.read_url]))
    client = Client(all_origins, token, key, args.timeout_secs)
    prior = expected_markers(args.run_id, phase_index)
    status, body = client.request("GET", args.url, args.stream)
    if status == 404 and phase_index == 0:
        create_status, _ = client.request("PUT", args.url, args.stream, b"")
        require(create_status in (200, 201), "canary stream creation failed")
        status, body = client.request("GET", args.url, args.stream)
    require(status == 200, "canary stream is unavailable before the phase write")
    observed = decode_events(body, "mutation route")
    expected_after = expected_markers(args.run_id, phase_index + 1)
    require(observed in (prior, expected_after),
            "canary stream has an unexpected pre-phase sequence")

    first_status: int | str = "resumed"
    retry_status: int | str = "resumed"
    if observed == prior:
        first_status, _ = client.request(
            "POST", args.url, args.stream, marker_raw, phase_index
        )
        retry_status, _ = client.request(
            "POST", args.url, args.stream, marker_raw, phase_index
        )
        require(200 <= retry_status < 300,
                "exact producer retry did not resolve successfully")

    read_statuses = read_every_route(
        client, list(dict.fromkeys([args.url, *args.read_url])), args.stream, expected_after
    )
    evidence = {
        "format_version": 1,
        "kind": "streams-mixed-version-semantic-phase",
        "run_id": args.run_id,
        "phase": args.phase,
        "passed": True,
        "completed_at_ms": int(time.time() * 1000),
        "previous_state_sha256": previous_state_sha256,
        "previous_evidence_sha256": (
            completed[-1]["evidence_sha256"] if completed else "0" * 64
        ),
        "capability_sha256": capability_sha256,
        "marker_sha256": marker_sha256,
        "history_writer": args.expected_history_writer,
        "backup_writer": args.expected_backup_writer,
        "event_count": len(expected_after),
        "route_count": len(read_statuses),
        "append_first_status": first_status,
        "append_retry_status": retry_status,
        "all_reads_status": sorted(set(read_statuses)),
        "complete": args.phase == PHASES[-1],
    }

    if args.evidence.exists():
        evidence_raw = read_bounded(args.evidence)
        existing = parse_json(evidence_raw, "semantic phase evidence")
        validate_existing_evidence(
            existing,
            args.run_id,
            args.phase,
            previous_state_sha256,
            capability_sha256,
            marker_sha256,
            completed[-1]["evidence_sha256"] if completed else "0" * 64,
            len(expected_after),
            len(read_statuses),
            args.phase == PHASES[-1],
            args.expected_history_writer,
            args.expected_backup_writer,
        )
        evidence_sha256 = sha256(evidence_raw)
    else:
        evidence_sha256 = write_new(args.evidence, evidence)

    completed.append(
        {
            "phase": args.phase,
            "marker_sha256": marker_sha256,
            "capability_sha256": capability_sha256,
            "evidence_sha256": evidence_sha256,
        }
    )
    write_state(args.state, state)
    print(f"mixed-version semantic canary passed: {args.phase}")


def main() -> int:
    args = parse_args()
    try:
        run(args)
    except (CanaryError, OSError) as error:
        print(f"mixed-version semantic canary failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
