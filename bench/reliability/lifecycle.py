"""Stateful client facts and offline replay of exact public-read checkpoints.

Lifecycle transitions are deliberately serialized. Ambiguous create/delete/fork
results stop the campaign before a name can be reused. Append attempts retain
the ordinary receipt oracle's acknowledged/rejected/ambiguous semantics.
"""
from __future__ import annotations

import base64
import dataclasses
from pathlib import Path
import urllib.parse
import uuid

from independent import (CheckError, Operation, Response, Stream, canonical,
                         check_records, classify_status, parse_json, raw_records,
                         read_entries, stream_path)


def address(stream):
    return (*stream.namespace, stream.name)


def pack(response):
    return {"status": response.status, "headers": response.headers,
            "body_b64": base64.b64encode(response.body).decode(), "error": response.error}


def unpack(data):
    try:
        return Response(data["status"], data["headers"],
                        base64.b64decode(data["body_b64"], validate=True), data["error"])
    except (KeyError, ValueError, TypeError) as error:
        raise CheckError("invalid recorded response") from error


@dataclasses.dataclass
class Collection:
    stream: Stream
    live: bool = True
    parent: Stream | None = None
    inherited: tuple[str, ...] = ()
    boundary: str | None = None


class ReplayClient:
    def __init__(self, namespace, observations):
        self.namespace, self.observations = namespace, observations

    def request(self, method, path, body=None, headers=None):
        if not self.observations:
            raise CheckError("checkpoint observations truncated")
        fact = self.observations.pop(0)
        if (tuple(fact["namespace"]) != self.namespace
                or fact["method"] != method or fact["path"] != path):
            raise CheckError("checkpoint request identity/order mismatch")
        return unpack(fact["response"])


class CapturingClient:
    def __init__(self, namespace, client, observations):
        self.namespace, self.client, self.observations = namespace, client, observations

    def request(self, method, path, body=None, headers=None):
        if method != "GET":
            raise CheckError("checkpoint may only read")
        response = self.client.request(method, path, body, headers)
        self.observations.append({"namespace": list(self.namespace), "method": method,
                                  "path": path, "response": pack(response)})
        return response


class Lifecycle:
    """One canonical replay owner, independent of all server storage state."""
    def __init__(self):
        self.collections, self.active, self.operations, self.slots = {}, {}, {}, {}
        self.pending = None
        self.checkpoint_pending = None
        self.generation, self.verified_generation = 0, -1
        self.checkpoints, self.cursors, self.boundaries = [], {}, {}
        self.validated_cursors, self.transfers = {}, set()
        self.invocation_ids = set()
        self.witnesses = set()

    def visible(self, stream):
        collection = self.collections[stream]
        own = [oid for oid, op in self.operations.items() if op.stream == stream]
        return [self.operations[oid] for oid in (*collection.inherited, *own)]

    def pinned(self, stream):
        for live in self.active.values():
            parent = self.collections[live].parent
            seen = set()
            while parent is not None:
                if parent in seen:
                    raise CheckError("fork ancestry cycle")
                seen.add(parent)
                if parent == stream:
                    return True
                parent = self.collections[parent].parent
        return False

    def require_live(self, stream):
        if self.active.get(address(stream)) != stream:
            raise CheckError("operation does not target the current client incarnation")

    def require_checkpoint(self):
        if self.verified_generation != self.generation:
            raise CheckError("destructive transition requires an exact current checkpoint")

    def verify(self, clients, budgets):
        observed = 0
        for budget in budgets:
            for stream in sorted(self.active.values(), key=lambda s: address(s)):
                if stream.namespace not in clients:
                    raise CheckError("missing namespace client")
                collection = self.collections[stream]
                observed += len(check_records(clients[stream.namespace], stream,
                                             self.visible(stream), budget,
                                             fork=collection.parent is not None))
            deleted = {address(s): s for s, c in self.collections.items()
                       if not c.live and address(s) not in self.active}
            for stream in sorted(deleted.values(), key=lambda s: address(s)):
                response = clients[stream.namespace].request("GET", stream_path(stream))
                if response.status not in {404, 410}:
                    raise CheckError(f"deleted stream remains visible ({response.status})")
        return observed

    def apply(self, kind, data, index):
        if self.pending is not None and kind != "lifecycle_outcome":
            raise CheckError("overlapping event before lifecycle outcome")
        if self.checkpoint_pending is not None and kind != "checkpoint":
            raise CheckError("overlapping event before checkpoint completion")
        if kind == "lifecycle_invoke":
            self._invoke(data, index)
        elif kind == "lifecycle_outcome":
            self._outcome(data, index)
        elif kind == "checkpoint_start":
            if self.pending is not None or self.checkpoint_pending is not None:
                raise CheckError("checkpoint overlaps an unresolved operation")
            if (not data["budgets"] or any(type(x) is not int or x < 1 for x in data["budgets"])
                    or data["name"] in self.checkpoints):
                raise CheckError("invalid checkpoint identity/budgets")
            self.checkpoint_pending = data
        elif kind == "checkpoint":
            if self.checkpoint_pending is None or data["id"] != self.checkpoint_pending["id"]:
                raise CheckError("checkpoint completion has no matching invocation")
            observations = list(data["observations"])
            namespaces = {s.namespace for s in self.collections}
            clients = {ns: ReplayClient(ns, observations) for ns in namespaces}
            self.verify(clients, self.checkpoint_pending["budgets"])
            if observations:
                raise CheckError("checkpoint has unaccounted observations")
            self.checkpoints.append(self.checkpoint_pending["name"])
            self.checkpoint_pending = None
            self.verified_generation = self.generation
        elif kind == "fork_boundary":
            self.require_checkpoint()
            stream = Stream(**data["stream"])
            self.require_live(stream)
            observations = list(data["observations"])
            frames, boundary = raw_records(ReplayClient(stream.namespace, observations), stream)
            expected = self.visible(stream)
            if any(op.routing_key or not op.required for op in expected):
                raise CheckError("fork boundary requires acknowledged default-key-only prefix")
            if frames != [op.frame for op in expected] or observations:
                raise CheckError("fork boundary differs from the independent acknowledged prefix")
            self.boundaries[stream] = (boundary, self.generation)
        elif kind == "authorization_probe":
            response = unpack(data["response"])
            namespace, purpose = tuple(data["namespace"]), data["purpose"]
            if response.status not in {401, 403}:
                raise CheckError("authorization control was not refused")
            if purpose == "retired-owner":
                if namespace not in self.transfers:
                    raise CheckError("retired ownership probe lacks a transfer")
            elif purpose == "customer-raw":
                if not any(s.namespace == namespace and data["path"] == "/v1/stream/" + urllib.parse.quote(s.name, safe="/")
                           for s in self.active.values()):
                    raise CheckError("customer raw probe lacks a live namespace stream")
            else:
                raise CheckError("unknown authorization probe")
            if any(op.frame in response.body for op in self.operations.values()):
                raise CheckError("authorization refusal exposed record bytes")
            self.witnesses.add("retired-owner-refused" if purpose == "retired-owner" else "customer-raw-refused")
        elif kind == "ownership_transfer":
            response = unpack(data["response"])
            receipt = parse_json(response.body)
            namespace = tuple(data["namespace"])
            if (response.status != 200 or not isinstance(receipt, dict) or receipt.get("project_id") != namespace[1]
                    or receipt.get("workspace_id") != data["new_workspace"]
                    or data["old_workspace"] == data["new_workspace"]
                    or receipt.get("ownership_version") != data["old_version"] + 1):
                raise CheckError("ownership transfer lacks its concrete progression receipt")
            self.transfers.add(namespace)
            self.witnesses.add("ownership-transferred")
        elif kind == "cursor_valid":
            stream = Stream(**data["stream"])
            self.require_live(stream)
            response = unpack(data["response"])
            if (data["cursor"] != self.cursors.get(stream) or response.status != 200
                    or response.body or response.headers.get("prisma-up-to-date") != "true"):
                raise CheckError("stale-cursor control must first work on its original incarnation")
            self.validated_cursors[stream] = data["cursor"]
        elif kind == "stale_cursor_probe":
            old, current = Stream(**data["old"]), Stream(**data["current"])
            self.require_live(current)
            if (old == current or address(old) != address(current)
                    or self.collections[old].live
                    or data["cursor"] != self.validated_cursors.get(old)):
                raise CheckError("stale cursor probe lacks a retired incarnation's receipt")
            if unpack(data["response"]).status != 400:
                raise CheckError("recreated stream accepted a previous incarnation's cursor")
            self.witnesses.add("stale-cursor-refused")
        else:
            raise CheckError(f"unknown lifecycle event: {kind}")

    def _invoke(self, data, index):
        if self.pending is not None or self.checkpoint_pending is not None:
            raise CheckError("overlapping lifecycle operations are outside this bounded grammar")
        if (not isinstance(data["id"], str) or not data["id"]
                or data["id"] in self.invocation_ids):
            raise CheckError("duplicate or invalid lifecycle invocation identity")
        stream, action = Stream(**data["stream"]), data["action"]
        if action in {"append", "delete"}:
            self.require_live(stream)
        if action == "delete":
            self.require_checkpoint()
        elif action == "create":
            if stream in self.collections or address(stream) in self.active:
                raise CheckError("fresh creation reuses a live identity")
        elif action == "fork":
            source = Stream(**data["source"])
            if source.namespace != stream.namespace:
                raise CheckError("fork crosses a namespace")
            if stream in self.collections:
                self.require_live(stream)
                collection = self.collections[stream]
                if collection.parent != source or collection.boundary != data["boundary"]:
                    raise CheckError("fork retry changed source or boundary")
            else:
                self.require_live(source)
                self.require_checkpoint()
                if (address(stream) in self.active
                        or self.boundaries.get(source) != (data["boundary"], self.generation)):
                    raise CheckError("fork lacks a checked current boundary")
        elif action != "append":
            raise CheckError("unsupported lifecycle operation")
        if action == "append":
            op = Operation(stream, data["operation_id"], data["routing_key"], data["producer"],
                           data["sequence"], base64.b64decode(data["payload_b64"], validate=True), index)
            if type(op.sequence) is not int or op.sequence < 0:
                raise CheckError("invalid producer sequence")
            slot = (stream, op.routing_key, op.producer, op.sequence)
            if slot in self.slots and self.slots[slot] != op.operation_id:
                raise CheckError("producer sequence reused by distinct operation")
            if op.operation_id in self.operations and self.operations[op.operation_id].frame != op.frame:
                raise CheckError("retry changed identity or payload")
            self.slots[slot] = op.operation_id
            self.operations.setdefault(op.operation_id, op).attempts[data["id"]] = ("pending", None)
        self.invocation_ids.add(data["id"])
        self.pending = data

    def _outcome(self, data, index):
        if self.pending is None or data["id"] != self.pending["id"]:
            raise CheckError("outcome lacks its invocation")
        invoke, response = self.pending, unpack(data["response"])
        stream, action = Stream(**invoke["stream"]), invoke["action"]
        if action == "append":
            verdict = classify_status(response.status)
            op = self.operations[invoke["operation_id"]]
            op.attempts[invoke["id"]] = (verdict, index)
            if verdict == "acknowledged":
                receipt = parse_json(response.body)
                if (not isinstance(receipt, dict) or type(receipt.get("duplicate")) is not bool
                        or receipt.get("count") != (0 if receipt["duplicate"] else 1)
                        or not isinstance(receipt.get("cursor"), str) or not receipt["cursor"]):
                    raise CheckError("invalid acknowledged append receipt")
                self.cursors[stream] = {"routing_key": invoke["routing_key"], "cursor": receipt["cursor"]}
        elif action == "create":
            pinned = any(address(old) == address(stream) and self.pinned(old)
                         for old in self.collections)
            if response.status != (409 if pinned else 201):
                raise CheckError("creation outcome violates lifecycle or is ambiguous")
            if pinned:
                self.witnesses.add("pinned-parent-recreation-refused")
            else:
                if any(address(old) == address(stream) for old in self.collections):
                    self.witnesses.add("same-name-recreated")
                self.collections[stream] = Collection(stream)
                self.active[address(stream)] = stream
        elif action == "delete":
            if response.status != 204:
                raise CheckError("deletion outcome is not an acknowledged transition")
            self.collections[stream].live = False
            del self.active[address(stream)]
            self.witnesses.add("parent-deleted-with-live-fork" if self.pinned(stream)
                               else "unreferenced-stream-deleted")
        elif action == "fork":
            if response.status not in ({200, 201} if stream in self.collections else {201}):
                raise CheckError("fork outcome is not an acknowledged transition")
            if stream not in self.collections:
                source = Stream(**invoke["source"])
                self.collections[stream] = Collection(stream, parent=source,
                                                      inherited=tuple(op.operation_id for op in self.visible(source)),
                                                      boundary=invoke["boundary"])
                self.active[address(stream)] = stream
                self.witnesses.add("fork-created")
                if any(op.required for op in self.visible(source)):
                    self.witnesses.add("fork-nonempty-prefix")
            else:
                self.witnesses.add("fork-retry-after-parent-deletion" if not self.collections[self.collections[stream].parent].live
                                   else "fork-retry")
        self.pending = None
        self.generation += 1

    def finish(self):
        if self.pending is not None or self.checkpoint_pending is not None:
            raise CheckError("incomplete lifecycle evidence")
        self.require_checkpoint()
        if not self.collections or not any(op.required for op in self.operations.values()):
            raise CheckError("lifecycle confidence requires a nonempty acknowledged workload")
        return {"checkpoints": len(self.checkpoints), "live_streams": len(self.active),
                "created_incarnations": len(self.collections),
                "acknowledged_operations": sum(op.required for op in self.operations.values()),
                "witnesses": sorted(self.witnesses)}


class LifecycleRecorder:
    def __init__(self, journal, clients):
        self.journal, self.clients, self.model = journal, clients, Lifecycle()
        self.index = 1

    def record(self, kind, data):
        # A failed journal write ends this driver before any subsequent request.
        actual = self.journal.record(kind, data)
        if actual != self.index:
            raise CheckError("lifecycle recorder is not the journal's only writer")
        self.model.apply(kind, data, self.index)
        self.index += 1

    def operation(self, action, stream, **data):
        invoke = {"id": str(uuid.uuid4()), "action": action, "stream": dataclasses.asdict(stream), **data}
        self.record("lifecycle_invoke", invoke)
        client, path = self.clients[stream.namespace], stream_path(stream)
        if action == "create":
            response = client.request("PUT", path, canonical({"format": {"kind": "bytes"}}),
                                      {"Content-Type": "application/json"})
        elif action == "delete":
            response = client.request("DELETE", path)
        elif action == "fork":
            response = client.request("PUT", "/v1/stream/" + urllib.parse.quote(stream.name, safe="/"), b"",
                                      {"Content-Type": "application/octet-stream", "Stream-Forked-From": data["source"]["name"],
                                       "Stream-Fork-Offset": data["boundary"]})
        else:
            op = self.model.operations[data["operation_id"]]
            response = client.request("POST", path + "/records", op.frame,
                                      {"Content-Type": "application/octet-stream", "Prisma-Routing-Key": op.routing_key,
                                       "Producer-Id": op.producer, "Producer-Epoch": "1", "Producer-Seq": str(op.sequence)})
        self.record("lifecycle_outcome", {"id": invoke["id"], "response": pack(response)})
        return response

    def append(self, stream, routing_key, sequence, payload, operation_id=None, producer="same-producer"):
        return self.operation("append", stream, routing_key=routing_key, sequence=sequence,
                              payload_b64=base64.b64encode(payload).decode(), producer=producer,
                              operation_id=operation_id or str(uuid.uuid4()))

    def checkpoint(self, name, budgets=(1024, 65536)):
        identity = str(uuid.uuid4())
        self.record("checkpoint_start", {"id": identity, "name": name, "budgets": list(budgets)})
        observations = []
        clients = {ns: CapturingClient(ns, client, observations) for ns, client in self.clients.items()}
        self.model.verify(clients, budgets)
        self.record("checkpoint", {"id": identity, "observations": observations})

    def fork_boundary(self, source):
        observations = []
        _, boundary = raw_records(CapturingClient(source.namespace, self.clients[source.namespace], observations), source)
        self.record("fork_boundary", {"stream": dataclasses.asdict(source), "observations": observations})
        return boundary

    def cursor_probe(self, old, current=None):
        target = current or old
        cursor = self.model.cursors[old]
        path = stream_path(target) + "/records?" + urllib.parse.urlencode(
            {"routingKey": cursor["routing_key"], "cursor": cursor["cursor"], "deliver": "durable"})
        response = self.clients[target.namespace].request("GET", path)
        if current is None:
            self.record("cursor_valid", {"stream": dataclasses.asdict(old), "cursor": cursor,
                                         "response": pack(response)})
        else:
            self.record("stale_cursor_probe", {"old": dataclasses.asdict(old), "current": dataclasses.asdict(current),
                                               "cursor": cursor, "response": pack(response)})


def load_lifecycle(path):
    entries, digest = read_entries(Path(path))
    if entries[0]["data"].get("history_kind") != "lifecycle-v1":
        raise CheckError("not a lifecycle journal")
    model = Lifecycle()
    try:
        for entry in entries[1:]:
            model.apply(entry["kind"], entry["data"], entry["index"])
        result = model.finish()
    except (KeyError, TypeError, ValueError) as error:
        raise CheckError("malformed lifecycle evidence") from error
    return {"result": "pass", "journal_digest": digest, **result}


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Replay a sealed independent lifecycle journal and all exact read checkpoints")
    parser.add_argument("journal", type=Path)
    arguments = parser.parse_args()
    print(json.dumps(load_lifecycle(arguments.journal), indent=2))
