"""Executable wrong-history controls for the independently journaled lifecycle owner."""
import base64
import copy
import dataclasses
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import urllib.parse

from independent import CheckError, Journal, Response, Stream, canonical, parse_json, read_entries
from lifecycle import Lifecycle, LifecycleRecorder, load_lifecycle, pack
from lifecycle_campaign import Fixture, SurfaceClient


class WireFixture:
    """A tiny wire peer: physical records, copied fork bytes and opaque cursors.

    Deliberately imports no production storage/planner and no lifecycle model.
    Tests mutate its bytes separately from the journal's expected identities.
    """
    def __init__(self):
        self.live, self.generations, self.parents, self.forks = {}, {}, {}, {}
        self.append_status, self.commit, self.defect = 200, True, None

    def request(self, method, path, body=None, headers=None):
        url = urllib.parse.urlsplit(path)
        raw = url.path.startswith("/v1/stream/")
        name = urllib.parse.unquote(url.path.split("/", 3)[3])
        scan = name.endswith(":scan")
        if scan:
            name = name[:-5]
        elif name.endswith("/records"):
            name = name[:-8]
        query = urllib.parse.parse_qs(url.query, keep_blank_values=True)
        if method == "PUT":
            if raw:
                source = headers["Stream-Forked-From"]
                boundary = headers["Stream-Fork-Offset"]
                if name in self.live:
                    return Response(200 if self.forks[name] == (source, boundary) else 409, {}, b"")
                self.live[name] = list(self.live[source][:int(boundary)])
                self.parents[name] = source
                self.forks[name] = (source, boundary)
            else:
                if name in self.parents.values():
                    return Response(409, {}, b"")
                self.live[name] = []
            self.generations[name] = self.generations.get(name, 0) + 1
            return Response(201, {}, b"")
        if method == "DELETE":
            del self.live[name]
            self.parents.pop(name, None)
            return Response(204, {}, b"")
        if name not in self.live or self.defect == "missing-stream":
            return Response(404, {}, b"")
        if method == "POST":
            duplicate = any(frame == body for _, frame in self.live[name])
            if self.commit and not duplicate:
                self.live[name].append((headers["Prisma-Routing-Key"], body))
            count = sum(key == headers["Prisma-Routing-Key"] for key, _ in self.live[name])
            receipt = {"duplicate": duplicate, "count": 0 if duplicate else 1,
                       "cursor": f"{self.generations[name]}:{count}"}
            return Response(self.append_status, {}, canonical(receipt))
        if not raw and not scan and not url.path.endswith("/records"):
            return Response(200, {}, b"{}")
        records = list(self.live[name])
        key = query.get("routingKey", [""])[0]
        if not scan:
            records = [(k, frame) for k, frame in records if k == key]
        if self.defect == "omit-keyed" and not raw and not scan:
            records = records[:-1]
        if self.defect == "raw-only-reorder" and raw:
            records.reverse()
        cursor = query.get("offset" if raw else "cursor", [None])[0]
        index = 0
        if cursor is not None:
            if raw:
                index = int(cursor)
            else:
                generation, position = map(int, cursor.split(":"))
                if generation != self.generations[name]:
                    return Response(400, {}, b"")
                index = position
        page = records[index:index + 1]
        response_body = (canonical([{"routingKey": k, "valueB64": base64.b64encode(frame).decode()}
                                    for k, frame in page]) if scan else b"".join(frame for _, frame in page))
        complete = "stream-up-to-date" if raw else "prisma-scan-complete" if scan else "prisma-up-to-date"
        next_name = "stream-next-offset" if raw else "prisma-next-scan-cursor" if scan else "prisma-next-cursor"
        position = min(index + 1, len(records))
        value = str(position) if raw else f"{self.generations[name]}:{position}"
        out = {next_name: value, complete: "true" if position == len(records) else "false"}
        if self.defect == "no-completion":
            out = {}
        return Response(200, out, response_body)


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "lifecycle.jsonl"
        self.a = Stream("tenant-a", "project-a", "same-name", "first")
        self.b = Stream("tenant-b", "project-b", "same-name", "first")
        self.clients = {self.a.namespace: WireFixture(), self.b.namespace: WireFixture()}
        self.journal = Journal(self.path, {"history_kind": "lifecycle-v1"})
        self.recorder = LifecycleRecorder(self.journal, self.clients)
        self.addCleanup(self.journal.close)

    def create(self, stream=None, key=""):
        stream = stream or self.a
        self.recorder.operation("create", stream)
        self.recorder.append(stream, key, 0, b"same application bytes", operation_id="op-" + stream.project)
        return stream

    def finish(self):
        self.recorder.checkpoint("final")
        self.journal.close()
        return load_lifecycle(self.path)

    def fork(self):
        self.create()
        self.recorder.checkpoint("before-fork")
        boundary = self.recorder.fork_boundary(self.a)
        child = dataclasses.replace(self.a, name="child", incarnation="child-one")
        facts = {"source": dataclasses.asdict(self.a), "boundary": boundary}
        self.recorder.operation("fork", child, **facts)
        return child, facts

    def rewrite(self, entries):
        target = Path(self.temp.name) / "rewritten.jsonl"
        with Journal(target, entries[0]["data"]) as journal:
            for entry in entries[1:]:
                journal.record(entry["kind"], entry["data"])
        return target

    def test_same_name_same_payload_and_producer_are_independent_between_projects(self):
        self.create(self.a, "key")
        self.create(self.b, "key")
        report = self.finish()
        self.assertEqual((report["acknowledged_operations"], report["live_streams"]), (2, 2))

    def test_swapped_namespace_clients_cannot_pass(self):
        self.create(self.a, "key")
        self.create(self.b, "key")
        self.recorder.clients = {self.a.namespace: self.clients[self.b.namespace],
                                 self.b.namespace: self.clients[self.a.namespace]}
        with self.assertRaisesRegex(CheckError, "unexpected logical"):
            self.finish()

    def test_missing_namespace_client_cannot_pass(self):
        self.create()
        self.recorder.clients = {}
        with self.assertRaisesRegex(CheckError, "missing namespace"):
            self.finish()

    def test_missing_ack_is_detected_before_delete_can_erase_its_obligation(self):
        self.create()
        self.clients[self.a.namespace].live[self.a.name].clear()
        with self.assertRaisesRegex(CheckError, "missing 1 acknowledged"):
            self.recorder.checkpoint("before-delete")
        with self.assertRaisesRegex(CheckError, "overlapping"):
            self.recorder.operation("delete", self.a)

    def test_delete_requires_a_current_checkpoint_after_every_append(self):
        self.create()
        self.recorder.checkpoint("checked")
        self.recorder.append(self.a, "", 1, b"new ack")
        with self.assertRaisesRegex(CheckError, "exact current checkpoint"):
            self.recorder.operation("delete", self.a)

    def test_delete_recreate_reuses_sequences_but_rejects_retired_data_and_cursor(self):
        self.create()
        old_bytes = list(self.clients[self.a.namespace].live[self.a.name])
        self.recorder.cursor_probe(self.a)
        self.recorder.checkpoint("before-delete")
        self.recorder.operation("delete", self.a)
        self.recorder.checkpoint("deleted")
        current = dataclasses.replace(self.a, incarnation="second")
        self.recorder.operation("create", current)
        self.recorder.append(current, "", 0, b"same application bytes")
        self.recorder.cursor_probe(self.a, current)
        self.recorder.checkpoint("recreated")
        self.clients[self.a.namespace].live[current.name].extend(old_bytes)
        with self.assertRaisesRegex(CheckError, "unexpected logical"):
            self.finish()

    def test_deleted_stream_metadata_still_visible_fails(self):
        self.create()
        self.recorder.checkpoint("before-delete")
        self.recorder.operation("delete", self.a)
        self.clients[self.a.namespace].live[self.a.name] = []
        with self.assertRaisesRegex(CheckError, "deleted stream remains visible"):
            self.finish()

    def test_fork_prefix_survives_parent_deletion_retry_and_delayed_name_reuse(self):
        child, facts = self.fork()
        self.recorder.append(self.a, "", 1, b"parent after boundary")
        self.recorder.append(child, "", 0, b"child tail")
        self.recorder.checkpoint("fork-is-exact")
        self.recorder.operation("delete", self.a)
        self.recorder.checkpoint("parent-deleted")
        replacement = dataclasses.replace(self.a, incarnation="second")
        self.assertEqual(self.recorder.operation("create", replacement).status, 409)
        self.recorder.operation("fork", child, **facts)
        self.recorder.checkpoint("retried-fork")
        self.recorder.operation("delete", child)
        self.recorder.checkpoint("last-reference-deleted")
        self.recorder.operation("create", replacement)
        self.recorder.append(replacement, "", 0, b"fresh parent")
        report = self.finish()
        self.assertEqual((report["live_streams"], report["created_incarnations"]), (1, 3))
        self.assertIn("fork-nonempty-prefix", report["witnesses"])
        self.assertIn("fork-retry-after-parent-deletion", report["witnesses"])

    def test_fork_omitted_inherited_ack_fails(self):
        child, _ = self.fork()
        self.clients[self.a.namespace].live[child.name].clear()
        with self.assertRaisesRegex(CheckError, "missing 1 acknowledged"):
            self.finish()

    def test_fork_parent_record_after_boundary_is_not_inherited(self):
        child, _ = self.fork()
        self.recorder.append(self.a, "", 1, b"parent after boundary")
        self.clients[self.a.namespace].live[child.name].append(self.clients[self.a.namespace].live[self.a.name][-1])
        with self.assertRaisesRegex(CheckError, "unexpected logical"):
            self.finish()

    def test_fork_duplicate_reordering_and_byte_corruption_controls(self):
        child, _ = self.fork()
        self.recorder.append(child, "", 0, b"child tail")
        peer = self.clients[self.a.namespace]
        original = list(peer.live[child.name])
        for defect in ("duplicate", "reorder", "corrupt"):
            with self.subTest(defect=defect):
                records = list(original)
                if defect == "duplicate":
                    records.append(records[0])
                elif defect == "reorder":
                    records.reverse()
                else:
                    key, frame = records[0]
                    envelope = parse_json(frame)
                    envelope["incarnation"] = "foreign-parent-incarnation"
                    records[0] = key, canonical(envelope) + b"\n"
                peer.live[child.name] = records
                with self.assertRaises(CheckError):
                    self.recorder.model.verify(self.clients, [1024])
        peer.live[child.name] = original
        self.assertEqual(self.finish()["acknowledged_operations"], 2)

    def test_raw_only_fork_reordering_fails_while_product_keyed_order_is_healthy(self):
        child, _ = self.fork()
        self.recorder.append(child, "", 0, b"child tail")
        peer = self.clients[self.a.namespace]
        peer.defect = "raw-only-reorder"
        first = peer.request("GET", "/v1/streams/child/records?routingKey=")
        second = peer.request("GET", "/v1/streams/child/records?routingKey=&cursor="
                              + urllib.parse.quote(first.headers["prisma-next-cursor"]))
        self.assertEqual(first.body + second.body,
                         b"".join(frame for _, frame in peer.live[child.name]))
        self.assertEqual(second.headers["prisma-up-to-date"], "true")
        with self.assertRaisesRegex(CheckError, "real-time order"):
            self.finish()

    def test_fork_default_keyed_path_cannot_omit_inherited_data(self):
        self.fork()
        self.clients[self.a.namespace].defect = "omit-keyed"
        with self.assertRaisesRegex(CheckError, "missing 1 acknowledged"):
            self.finish()

    def test_fork_cannot_cross_namespace(self):
        self.create()
        self.recorder.checkpoint("before-fork")
        boundary = self.recorder.fork_boundary(self.a)
        with self.assertRaisesRegex(CheckError, "crosses a namespace"):
            self.recorder.operation("fork", self.b, source=dataclasses.asdict(self.a), boundary=boundary)

    def test_fork_requires_a_proved_boundary_not_an_arbitrary_server_cursor(self):
        self.create()
        self.recorder.checkpoint("before-fork")
        with self.assertRaisesRegex(CheckError, "checked current boundary"):
            self.recorder.operation("fork", dataclasses.replace(self.a, name="child"),
                                    source=dataclasses.asdict(self.a), boundary="invented")

    def test_raw_boundary_requires_explicit_completion_and_all_acknowledged_bytes(self):
        self.create()
        self.recorder.checkpoint("before-fork")
        self.clients[self.a.namespace].defect = "no-completion"
        with self.assertRaisesRegex(CheckError, "raw pagination"):
            self.recorder.fork_boundary(self.a)

    def test_duplicate_append_attempt_cannot_replace_acknowledgement_with_rejection(self):
        self.create()
        first = next(iter(self.recorder.model.operations.values()))
        attempt = next(iter(first.attempts))
        with self.assertRaisesRegex(CheckError, "duplicate.*invocation"):
            self.recorder.operation("append", self.a, id=attempt, operation_id=first.operation_id,
                                    routing_key="", producer=first.producer, sequence=0,
                                    payload_b64=base64.b64encode(first.payload).decode())
        self.assertTrue(first.required)
        self.journal.close()
        with self.assertRaisesRegex(CheckError, "duplicate.*invocation"):
            load_lifecycle(self.path)

    def test_retry_ambiguous_then_duplicate_ack_remains_one_logical_operation(self):
        self.recorder.operation("create", self.a)
        peer = self.clients[self.a.namespace]
        peer.append_status = 503
        self.recorder.append(self.a, "", 0, b"payload", operation_id="same-op")
        self.recorder.checkpoint("ambiguous-committed-observed")
        peer.append_status = 200
        response = self.recorder.append(self.a, "", 0, b"payload", operation_id="same-op")
        self.assertTrue(parse_json(response.body)["duplicate"])
        self.assertEqual(self.finish()["acknowledged_operations"], 1)

    def test_rejected_append_cannot_become_visible(self):
        self.recorder.operation("create", self.a)
        self.clients[self.a.namespace].append_status = 400
        self.recorder.append(self.a, "", 0, b"rejected but wrongfully committed")
        with self.assertRaisesRegex(CheckError, "definitely rejected"):
            self.finish()

    def test_malformed_success_is_durably_retained_and_cannot_pass_replay(self):
        self.recorder.operation("create", self.a)
        invoke = {"id": "bad-receipt", "action": "append", "stream": dataclasses.asdict(self.a),
                  "operation_id": "op", "routing_key": "", "producer": "p", "sequence": 0,
                  "payload_b64": ""}
        self.recorder.record("lifecycle_invoke", invoke)
        with self.assertRaisesRegex(CheckError, "invalid acknowledged"):
            self.recorder.record("lifecycle_outcome", {"id": invoke["id"], "response": pack(Response(200, {}, b"[]"))})
        self.journal.close()
        entries, _ = read_entries(self.path)
        self.assertEqual(entries[-1]["kind"], "lifecycle_outcome")
        with self.assertRaisesRegex(CheckError, "invalid acknowledged"):
            load_lifecycle(self.path)

    def test_lifecycle_ambiguity_and_incomplete_invocation_fail_closed(self):
        for status in (None, 408, 500):
            with self.subTest(status=status):
                model = Lifecycle()
                model.apply("lifecycle_invoke", {"id": "create", "action": "create", "stream": dataclasses.asdict(self.a)}, 1)
                with self.assertRaisesRegex(CheckError, "ambiguous"):
                    model.apply("lifecycle_outcome", {"id": "create", "response": pack(Response(status, {}, b""))}, 2)
                with self.assertRaisesRegex(CheckError, "incomplete"):
                    model.finish()

    def test_rehashed_checkpoint_omission_reorder_and_extra_observations_fail(self):
        self.create(self.a)
        self.create(self.b)
        self.finish()
        original, _ = read_entries(self.path)
        for defect in ("omit", "reorder", "extra"):
            with self.subTest(defect=defect):
                entries = copy.deepcopy(original)
                observations = entries[-1]["data"]["observations"]
                if defect == "omit":
                    observations.pop()
                elif defect == "reorder":
                    observations.reverse()
                else:
                    observations.append(copy.deepcopy(observations[-1]))
                target = self.rewrite(entries)
                with self.assertRaises(CheckError):
                    load_lifecycle(target)
                target.unlink()
                target.with_suffix(target.suffix + ".head").unlink()

    def test_rehashed_payload_corruption_is_caught_by_independent_invocation_bytes(self):
        self.create()
        self.finish()
        entries, _ = read_entries(self.path)
        observations = entries[-1]["data"]["observations"]
        fact = next(f for f in observations if "/records?" in f["path"])
        body = base64.b64decode(fact["response"]["body_b64"])
        envelope = parse_json(body)
        envelope["payload_b64"] = base64.b64encode(b"corrupt").decode()
        fact["response"]["body_b64"] = base64.b64encode(canonical(envelope) + b"\n").decode()
        with self.assertRaisesRegex(CheckError, "bytes differ"):
            load_lifecycle(self.rewrite(entries))

    def test_journal_byte_corruption_fails_before_semantic_replay(self):
        self.create()
        self.finish()
        content = self.path.read_bytes().replace(b'"first"', b'"stale"', 1)
        self.path.write_bytes(content)
        with self.assertRaisesRegex(CheckError, "checksum"):
            load_lifecycle(self.path)

    def test_stale_cursor_refusal_needs_a_valid_original_cursor_control(self):
        self.create()
        self.recorder.checkpoint("before-delete")
        self.recorder.operation("delete", self.a)
        current = dataclasses.replace(self.a, incarnation="second")
        self.recorder.operation("create", current)
        with self.assertRaisesRegex(CheckError, "retired incarnation"):
            self.recorder.cursor_probe(self.a, current)

    def test_ownership_requires_version_progression_and_old_credential_refusal(self):
        self.create()
        namespace = list(self.a.namespace)
        self.recorder.record("ownership_transfer", {"namespace": namespace, "old_workspace": "old", "new_workspace": "new",
                             "old_version": 1, "response": pack(Response(200, {}, canonical(
                                 {"project_id": self.a.project, "workspace_id": "new", "ownership_version": 2})))})
        with self.assertRaisesRegex(CheckError, "not refused"):
            self.recorder.record("authorization_probe", {"namespace": namespace, "purpose": "retired-owner",
                                                         "response": pack(Response(200, {}, b""))})

    def test_public_customer_raw_auth_control_requires_refusal(self):
        self.create()
        facts = {"namespace": list(self.a.namespace), "purpose": "customer-raw",
                 "path": "/v1/stream/" + self.a.name}
        self.recorder.record("authorization_probe", {**facts, "response": pack(Response(401, {}, b""))})
        self.assertIn("customer-raw-refused", self.recorder.model.witnesses)
        with self.assertRaisesRegex(CheckError, "not refused"):
            self.recorder.record("authorization_probe", {**facts, "response": pack(Response(200, {}, b""))})

    def test_replaced_campaign_binary_is_rejected_before_another_launch(self):
        paths = [Path(self.temp.name) / name for name in ("server", "store", "node")]
        for path in paths:
            path.write_bytes(path.name.encode())
        fixture = Fixture(SimpleNamespace(server=paths[0], s3lite=paths[1], node=str(paths[2])))
        fixture.verify_binaries()
        paths[0].write_bytes(b"different build")
        with self.assertRaisesRegex(CheckError, "binary changed"):
            fixture.verify_binaries()

    def test_workload_raw_client_cannot_be_used_for_another_project_or_raw_append(self):
        client = SurfaceClient(self.clients[self.b.namespace])
        with self.assertRaisesRegex(CheckError, "restricted"):
            client.request("GET", "/v1/stream/source")
        client = SurfaceClient(self.clients[self.a.namespace], self.clients[self.a.namespace])
        with self.assertRaisesRegex(CheckError, "restricted"):
            client.request("POST", "/v1/stream/source", b"data")

    def test_empty_lifecycle_trace_cannot_be_confidence_evidence(self):
        self.recorder.checkpoint("empty")
        self.journal.close()
        with self.assertRaisesRegex(CheckError, "nonempty acknowledged"):
            load_lifecycle(self.path)


if __name__ == "__main__":
    unittest.main()
