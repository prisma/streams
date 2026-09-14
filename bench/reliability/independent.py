"""Independent public-API receipt journal and exact record checker (stdlib only).

This module imports no server, simulator, read planner, or producer implementation.
The oracle is the client's durably recorded invocation/response history.
"""

from __future__ import annotations

import base64
import collections
import dataclasses
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


class CheckError(RuntimeError):
    """Evidence is incomplete, inconsistent, or contradicts the API contract."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("ascii")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise CheckError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def _reject_constant(value):
    raise CheckError(f"non-finite JSON value: {value}")


def parse_json(raw):
    try:
        return json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    except (ValueError, UnicodeError) as exc:
        raise CheckError(f"invalid JSON: {exc}") from exc


def _sync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Journal:
    """One writer process, any number of threads; every event is fsynced.

    The independent head file detects whole-line truncation as well as partial
    writes. A crash between journal and head replacement fails closed. Preserve
    both files outside the serving processes' failure/storage domain.
    """

    def __init__(self, path, header):
        self.path = Path(path)
        self.head_path = self.path.with_name(self.path.name + ".head")
        if self.head_path.exists():
            raise CheckError("journal head already exists")
        self._file = self.path.open("xb", buffering=0)
        os.chmod(self.path, 0o600)
        fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        self._lock = threading.Lock()
        self._count = 0
        self._digest = "0" * 64
        self._failed = False
        self.record("header", {**header, "schema": 1, "run_id": str(uuid.uuid4())})

    def record(self, kind, data):
        with self._lock:
            if self._failed:
                raise CheckError("journal previously failed; no further request is safe")
            try:
                entry = {"index": self._count, "previous": self._digest,
                         "kind": kind, "data": data, "time_ns": time.time_ns()}
                digest = hashlib.sha256(canonical(entry)).hexdigest()
                line = canonical({**entry, "digest": digest}) + b"\n"
                view = memoryview(line)
                while view:
                    written = self._file.write(view)
                    if not written:
                        raise OSError("journal write made no progress")
                    view = view[written:]
                os.fsync(self._file.fileno())
                head = canonical({"entries": self._count + 1, "digest": digest}) + b"\n"
                temporary = self.head_path.with_name(self.head_path.name + ".tmp")
                with temporary.open("wb") as out:
                    os.chmod(temporary, 0o600)
                    out.write(head)
                    out.flush()
                    os.fsync(out.fileno())
                os.replace(temporary, self.head_path)
                _sync_directory(self.path.parent)
                self._count += 1
                self._digest = digest
                return self._count - 1
            except BaseException:
                self._failed = True
                raise

    def close(self):
        self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


@dataclasses.dataclass(frozen=True)
class Stream:
    tenant: str
    project: str
    name: str
    incarnation: str

    @property
    def namespace(self):
        return self.tenant, self.project


@dataclasses.dataclass
class Operation:
    stream: Stream
    operation_id: str
    routing_key: str
    producer: str
    sequence: int
    payload: bytes
    first_invocation: int
    attempts: dict = dataclasses.field(default_factory=dict)

    @property
    def frame(self):
        envelope = {**dataclasses.asdict(self.stream), "operation_id": self.operation_id,
                    "routing_key": self.routing_key, "producer": self.producer,
                    "producer_epoch": 1, "sequence": self.sequence,
                    "payload_b64": base64.b64encode(self.payload).decode("ascii")}
        return canonical(envelope) + b"\n"

    @property
    def acknowledged_at(self):
        return min((end for verdict, end in self.attempts.values()
                    if verdict == "acknowledged"), default=None)

    @property
    def required(self):
        return self.acknowledged_at is not None

    @property
    def permitted(self):
        return self.required or any(verdict in ("ambiguous", "pending")
                                    for verdict, _ in self.attempts.values())


@dataclasses.dataclass
class History:
    header: dict
    streams: dict
    operations: dict
    digest: str


def _decode_b64(value):
    try:
        return base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as exc:
        raise CheckError("invalid base64 payload") from exc


def read_entries(path):
    """Validate the shared journal container before interpreting its event grammar."""
    path = Path(path)
    previous, entries = "0" * 64, []
    with path.open("rb") as source:
        fcntl.flock(source.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
        for index, line in enumerate(source):
            if not line.endswith(b"\n"):
                raise CheckError("journal has a partial final line")
            entry = parse_json(line)
            if not isinstance(entry, dict) or set(entry) != {
                    "index", "previous", "kind", "data", "time_ns", "digest"}:
                raise CheckError("invalid journal entry schema")
            digest = entry.pop("digest")
            if entry["index"] != index or entry["previous"] != previous:
                raise CheckError("journal chain sequence mismatch")
            if hashlib.sha256(canonical(entry)).hexdigest() != digest:
                raise CheckError("journal checksum mismatch")
            previous = digest
            entries.append(entry)
        head = parse_json(path.with_name(path.name + ".head").read_bytes())
        if head != {"entries": len(entries), "digest": previous}:
            raise CheckError("journal head mismatch (truncated or incomplete evidence)")
    if not entries or entries[0]["kind"] != "header":
        raise CheckError("missing journal header")
    if entries[0]["data"].get("schema") != 1:
        raise CheckError("unsupported journal schema")
    return entries, previous


def load_journal(path):
    """Replay client facts only. Unknown or malformed events fail closed."""
    entries, digest = read_entries(path)
    streams, operations, attempts = {}, {}, {}
    producer_slots = {}
    try:
        for entry in entries[1:]:
            kind, data, index = entry["kind"], entry["data"], entry["index"]
            if kind == "create":
                stream = Stream(**data["stream"])
                address = (*stream.namespace, stream.name)
                if stream in streams or any((*s.namespace, s.name) == address for s in streams):
                    raise CheckError("duplicate stream address/incarnation")
                streams[stream] = False
            elif kind == "created":
                stream = Stream(**data["stream"])
                if stream not in streams or streams[stream] or data["status"] != 201:
                    raise CheckError("invalid creation acknowledgement")
                streams[stream] = True
            elif kind == "invoke":
                stream = Stream(**data["stream"])
                if not streams.get(stream):
                    raise CheckError("append without acknowledged fresh creation")
                oid, aid = data["operation_id"], data["attempt_id"]
                if not isinstance(oid, str) or not oid or aid in attempts:
                    raise CheckError("invalid operation/attempt identity")
                if not isinstance(data["sequence"], int) or data["sequence"] < 0:
                    raise CheckError("invalid producer sequence")
                op = Operation(stream, oid, data["routing_key"], data["producer"],
                               data["sequence"], _decode_b64(data["payload_b64"]), index)
                slot = (stream, op.routing_key, op.producer, op.sequence)
                if slot in producer_slots and producer_slots[slot] != oid:
                    raise CheckError("producer sequence reused by distinct logical operations")
                producer_slots[slot] = oid
                if oid in operations and operations[oid].frame != op.frame:
                    raise CheckError("retry changed logical identity or payload")
                operations.setdefault(oid, op).attempts[aid] = ("pending", None)
                attempts[aid] = oid
            elif kind == "outcome":
                aid, verdict = data["attempt_id"], data["verdict"]
                if aid not in attempts or verdict not in {"acknowledged", "rejected", "ambiguous"}:
                    raise CheckError("invalid outcome")
                op = operations[attempts[aid]]
                if op.attempts[aid][0] != "pending":
                    raise CheckError("duplicate attempt outcome")
                if verdict != classify_status(data["status"]):
                    raise CheckError("outcome contradicts HTTP status")
                op.attempts[aid] = (verdict, index)
            else:
                raise CheckError(f"unknown journal event: {kind}")
    except (KeyError, TypeError) as exc:
        raise CheckError(f"malformed journal event: {exc}") from exc
    if not streams or not all(streams.values()):
        raise CheckError("missing acknowledged stream creation; campaign incomplete")
    return History(entries[0]["data"], streams, operations, digest)


def classify_status(status):
    # Current product append always returns 200, including idempotent replay.
    # 409, 408, 429, 5xx, transport failures and unknown statuses remain ambiguous.
    if status == 200:
        return "acknowledged"
    if status in {400, 401, 403, 404, 405, 410, 413, 415, 422}:
        return "rejected"
    return "ambiguous"


@dataclasses.dataclass(frozen=True)
class Response:
    status: int | None
    headers: dict
    body: bytes
    error: str | None = None


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class HttpClient:
    """Bounded stdlib client. Never silently replays POST or forwards secrets."""

    def __init__(self, base_url, token, key, timeout=15):
        self.base_url = base_url.rstrip("/")
        if urllib.parse.urlsplit(self.base_url).scheme not in {"http", "https"}:
            raise CheckError("HTTP(S) base URL required")
        self.headers = {"Authorization": f"Bearer {token}", "Prisma-Encryption-Key": key}
        self.timeout = timeout

    def request(self, method, path, body=None, headers=None):
        request_headers = {**self.headers, **(headers or {})}
        if path.startswith("/v1/stream/"):
            request_headers["Stream-Encryption-Key"] = request_headers.pop("Prisma-Encryption-Key")
        request = urllib.request.Request(self.base_url + path, data=body, method=method,
                                         headers=request_headers)
        try:
            opener = urllib.request.build_opener(_NoRedirect())
            try:
                response = opener.open(request, timeout=self.timeout)
            except urllib.error.HTTPError as exc:
                response = exc
            with response:
                # Includes protection against a truncated body after 200 headers.
                body = response.read(32 * 1024 * 1024 + 1)
                if len(body) > 32 * 1024 * 1024:
                    return Response(None, {}, b"", "response exceeds 32 MiB safety bound")
                declared = response.headers.get("Content-Length")
                if declared is not None and (not declared.isdecimal() or int(declared) != len(body)):
                    return Response(None, {}, b"", "incomplete or invalid content length")
                return Response(response.status,
                                {k.lower(): v for k, v in response.headers.items()}, body)
        except (OSError, urllib.error.URLError, http.client.HTTPException) as exc:
            # Error class is enough; exceptions may contain credential-bearing URLs.
            return Response(None, {}, b"", type(exc).__name__)


def stream_path(stream):
    return "/v1/streams/" + urllib.parse.quote(stream.name, safe="/")


class Recorder:
    def __init__(self, journal, client):
        self.journal, self.client = journal, client

    def create(self, stream):
        self.journal.record("create", {"stream": dataclasses.asdict(stream)})
        response = self.client.request("PUT", stream_path(stream),
                                       canonical({"format": {"kind": "bytes"}}),
                                       {"Content-Type": "application/json"})
        if response.status != 201:
            raise CheckError(f"fresh stream creation failed: {response.status} {response.error}")
        self.journal.record("created", {"stream": dataclasses.asdict(stream), "status": 201})

    def append(self, stream, routing_key, producer, seq, payload, op_id=None):
        oid, aid = op_id or str(uuid.uuid4()), str(uuid.uuid4())
        operation = Operation(stream, oid, routing_key, producer, seq, payload, 0)
        self.journal.record("invoke", {
            "stream": dataclasses.asdict(stream), "operation_id": oid, "attempt_id": aid,
            "routing_key": routing_key, "producer": producer, "sequence": seq,
            "payload_b64": base64.b64encode(payload).decode("ascii")})
        response = self.client.request("POST", stream_path(stream) + "/records", operation.frame,
                                       {"Content-Type": "application/octet-stream",
                                        "Prisma-Routing-Key": routing_key,
                                        "Producer-Id": producer, "Producer-Epoch": "1",
                                        "Producer-Seq": str(seq)})
        verdict = classify_status(response.status)
        # A success with an invalid receipt must still impose durability: fail the
        # driver after persisting the acknowledged outcome, never downgrade it.
        self.journal.record("outcome", {"attempt_id": aid, "verdict": verdict,
                                       "status": response.status, "error": response.error,
                                       "response_b64": base64.b64encode(response.body).decode("ascii")})
        if verdict == "acknowledged":
            receipt = parse_json(response.body)
            if (not isinstance(receipt, dict) or type(receipt.get("duplicate")) is not bool
                    or receipt.get("count") != (0 if receipt["duplicate"] else 1)
                    or not isinstance(receipt.get("cursor"), str) or not receipt["cursor"]):
                raise CheckError("malformed acknowledged append receipt")
        return oid, verdict


def _pages(client, stream, scan, routing_key, page_bytes, max_pages):
    cursor, seen = None, set()
    complete_name = "prisma-scan-complete" if scan else "prisma-up-to-date"
    cursor_name = "prisma-next-scan-cursor" if scan else "prisma-next-cursor"
    suffix = ":scan" if scan else "/records"
    for _ in range(max_pages):
        query = {"maxBytes": str(page_bytes)}
        if not scan:
            query.update(routingKey=routing_key, deliver="durable")
        if cursor is not None:
            query["cursor"] = cursor
        response = client.request("GET", stream_path(stream) + suffix + "?" +
                                  urllib.parse.urlencode(query))
        if response.status != 200:
            raise CheckError(f"{stream.namespace}/{stream.name}: read failed ({response.status})")
        complete = response.headers.get(complete_name)
        if complete not in {None, "true", "false"}:
            raise CheckError("invalid completion header")
        yield response.body
        if complete == "true":
            return
        cursor = response.headers.get(cursor_name)
        if not cursor or cursor in seen:
            raise CheckError("pagination missing/repeated cursor without explicit completion")
        seen.add(cursor)
    raise CheckError(f"pagination exceeded {max_pages} pages without completion")


def _identify(frame, stream, routing_key, operations, inherited=False):
    envelope = parse_json(frame)
    if not isinstance(envelope, dict) or envelope.get("operation_id") not in operations:
        raise CheckError("unexpected logical operation")
    operation = operations[envelope["operation_id"]]
    if (operation.stream.namespace != stream.namespace
            or (operation.stream != stream and not inherited)
            or operation.routing_key != routing_key):
        raise CheckError("cross-tenant/incarnation/routing-key record")
    if operation.frame != frame:
        raise CheckError("payload or identity bytes differ from invocation")
    if not operation.permitted:
        raise CheckError("definitely rejected operation became visible")
    return operation.operation_id


def _exact(ids, expected):
    counts = collections.Counter(ids)
    if any(count != 1 for count in counts.values()):
        raise CheckError("duplicate logical operation")
    missing = {op.operation_id for op in expected if op.required} - counts.keys()
    if missing:
        raise CheckError(f"missing {len(missing)} acknowledged operation(s): {sorted(missing)[:3]}")
    return set(counts)


def _order(ids, operations):
    latest_prior_invocation = -1
    producer_sequences = {}
    for oid in ids:
        op = operations[oid]
        if op.acknowledged_at is not None and op.acknowledged_at < latest_prior_invocation:
            raise CheckError("per-key real-time order violated")
        latest_prior_invocation = max(latest_prior_invocation, op.first_invocation)
        producer = (op.stream, op.producer)
        if op.sequence <= producer_sequences.get(producer, -1):
            raise CheckError("per-producer sequence order violated")
        producer_sequences[producer] = op.sequence


def raw_records(client, stream, max_pages=10000):
    """Drain the raw default-key view without decoding its opaque offsets."""
    cursor, seen, frames = None, set(), []
    for _ in range(max_pages):
        path = "/v1/stream/" + urllib.parse.quote(stream.name, safe="/")
        if cursor is not None:
            path += "?" + urllib.parse.urlencode({"offset": cursor})
        response = client.request("GET", path)
        if response.status != 200:
            raise CheckError(f"raw read failed ({response.status})")
        frames.append(response.body)
        next_cursor = response.headers.get("stream-next-offset")
        if response.headers.get("stream-up-to-date") == "true":
            if not next_cursor:
                raise CheckError("raw completion has no boundary")
            return b"".join(frames).splitlines(keepends=True), next_cursor
        if not next_cursor or next_cursor in seen:
            raise CheckError("raw pagination missing/repeated offset without completion")
        cursor = next_cursor
        seen.add(cursor)
    raise CheckError("raw pagination exceeded bound without completion")


def check_records(client, stream, expected, page_bytes=4096, max_pages=10000, fork=False,
                  known_operations=None):
    """Check one independently selected view; fork origins stay in their envelopes.

    The lifecycle owner supplies an explicit, journal-derived inherited prefix.
    Raw forks in this campaign contain only the default key and are checked
    through both raw and product keyed reads. Ordinary streams retain scans.
    """
    if page_bytes < 1 or max_pages < 1:
        raise CheckError("positive page bounds required")
    selected = {op.operation_id: op for op in expected}
    if len(selected) != len(expected):
        raise CheckError("duplicate expected operation identity")
    operations = selected if known_operations is None else known_operations
    if fork and any(op.routing_key for op in expected):
        raise CheckError("raw fork campaign requires default-key records")
    scan_ids = []
    if fork:
        frames, _ = raw_records(client, stream, max_pages)
        scan_ids = [_identify(frame, stream, "", operations, True) for frame in frames]
        _order(scan_ids, operations)
    else:
        for body in _pages(client, stream, True, None, page_bytes, max_pages):
            items = parse_json(body)
            if not isinstance(items, list):
                raise CheckError("scan body is not an array")
            for item in items:
                if not isinstance(item, dict) or set(item) != {"routingKey", "valueB64"}:
                    raise CheckError("malformed bytes scan record")
                frame = _decode_b64(item["valueB64"])
                scan_ids.append(_identify(frame, stream, item["routingKey"], operations))
    scan_set = _exact(scan_ids, expected)
    keyed_set = set()
    for key in sorted({op.routing_key for op in expected} | {""}):
        data = b"".join(_pages(client, stream, False, key, page_bytes, max_pages))
        ids = [_identify(frame, stream, key, operations, fork)
               for frame in data.splitlines(keepends=True)]
        keyed_set.update(_exact(ids, [op for op in expected if op.routing_key == key]))
        _order(ids, operations)
    if keyed_set != scan_set:
        raise CheckError("scan/raw and keyed reads disagree (or campaign is not quiescent)")
    return scan_set


def check(history, clients, page_bytes=4096, max_pages=10000):
    """Check a quiescent final state through full scans AND every expected key.

    Scans catch unexpected keys and streams are selected from the journal alone.
    Keyed walks establish only the ordering promised within each routing key.
    A missing outcome is ambiguous, including an invoked request with no response.
    """
    if page_bytes < 1 or max_pages < 1:
        raise CheckError("positive page bounds required")
    total, required, ambiguous_present = 0, 0, 0
    for stream in history.streams:
        if stream.namespace not in clients:
            raise CheckError(f"missing credentials for namespace {stream.namespace}")
        client = clients[stream.namespace]
        expected = [op for op in history.operations.values() if op.stream == stream]
        scan_set = check_records(client, stream, expected, page_bytes, max_pages,
                                 known_operations=history.operations)
        total += len(scan_set)
        required += sum(op.required for op in expected)
        ambiguous_present += sum(not history.operations[oid].required for oid in scan_set)
    return {"result": "pass", "journal_digest": history.digest,
            "streams": len(history.streams), "acknowledged": required, "observed": total,
            "ambiguous_observed": ambiguous_present,
            "ambiguous_absent": sum(op.permitted and not op.required
                                    for op in history.operations.values()) - ambiguous_present,
            "page_bytes": page_bytes, "max_pages": max_pages}
