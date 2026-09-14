"""Executable negative controls for the external oracle, including HTTP faults."""

import base64
import copy
import dataclasses
import http.server
from pathlib import Path
import tempfile
import threading
import unittest
import urllib.parse

from independent import (CheckError, HttpClient, Journal, Recorder, Response, Stream,
                         canonical, check, classify_status, load_journal, parse_json)


class PublicApiFixture:
    """Only public wire behavior, deliberately no production model/oracle imports."""

    def __init__(self):
        self.records = []
        self.append_status = 200
        self.commit = True
        self.defect = None

    def request(self, method, path, body=None, headers=None):
        if method == "PUT":
            return Response(201, {}, b"{}")
        if method == "POST":
            if self.commit:
                self.records.append((headers["Prisma-Routing-Key"], body))
            receipt = {"cursor": "opaque", "count": 1, "duplicate": False, "sealed": False}
            return Response(self.append_status, {}, canonical(receipt))
        if self.defect == "missing_stream":
            return Response(404, {}, b"{}")
        url = urllib.parse.urlsplit(path)
        q = urllib.parse.parse_qs(url.query, keep_blank_values=True)
        scan = url.path.endswith(":scan")
        records = self.records if scan else [r for r in self.records if r[0] == q["routingKey"][0]]
        if self.defect == "keyed_truncation" and not scan:
            records = records[:-1]
        index = int(q.get("cursor", ["0"])[0])
        page = records[index:index + 1]
        body = canonical([{"routingKey": key, "valueB64": base64.b64encode(frame).decode()}
                          for key, frame in page]) if scan else b"".join(frame for _, frame in page)
        complete = "prisma-scan-complete" if scan else "prisma-up-to-date"
        cursor = "prisma-next-scan-cursor" if scan else "prisma-next-cursor"
        out = {complete: "true"} if index + 1 >= len(records) else {cursor: str(index + 1)}
        if self.defect == "missing_cursor":
            out = {}
        elif self.defect == "repeat_cursor":
            out = {cursor: "0"}
        elif self.defect == "false_completion":
            out = {complete: "false"}
        elif self.defect == "invalid_json" and scan:
            body = b"[{"
        elif self.defect == "malformed_scan" and scan:
            body = b'{"records":[]}'
        elif self.defect == "empty_incomplete":
            body = b"[]" if scan else b""
            out = {cursor: str(index + 1)}
        return Response(200, out, body)


class ExactCheckerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "operations.jsonl"
        self.client = PublicApiFixture()
        self.stream = Stream("tenant-a", "project-a", "same-name", "incarnation-one")

    def history(self, outcomes=(200, 200), keys=None):
        with Journal(self.path, {"test": self.id()}) as journal:
            recorder = Recorder(journal, self.client)
            recorder.create(self.stream)
            for index, status in enumerate(outcomes):
                self.client.append_status = status
                self.client.commit = status not in {400, 403}
                recorder.append(self.stream, keys[index] if keys else "key", f"producer-{index}",
                                0, b"identical application bytes", op_id=f"operation-{index}")
        return load_journal(self.path)

    def verify(self, history, **bounds):
        return check(history, {self.stream.namespace: self.client}, **bounds)

    def test_exact_bytes_identical_payloads_are_distinct_logical_operations(self):
        report = self.verify(self.history())
        self.assertEqual((report["acknowledged"], report["observed"]), (2, 2))

    def test_missing_ack_cannot_be_masked_by_ambiguous_commit_at_equal_count(self):
        history = self.history((200, 200, 408))
        del self.client.records[0]
        with self.assertRaisesRegex(CheckError, "missing 1 acknowledged"):
            self.verify(history)

    def test_ambiguous_committed_and_absent_are_both_legal(self):
        history = self.history((200, 408, 503))
        del self.client.records[-1]
        report = self.verify(history)
        self.assertEqual((report["ambiguous_observed"], report["ambiguous_absent"]), (1, 1))

    def test_transport_failure_is_ambiguous(self):
        history = self.history((None,))
        self.assertEqual(self.verify(history)["ambiguous_observed"], 1)

    def test_pending_invocation_is_ambiguous(self):
        history = self.history((408,))
        self.rewrite_without_last_entry()
        history = load_journal(self.path)
        self.assertEqual(self.verify(history)["ambiguous_observed"], 1)

    def test_rejected_append_is_required_absent(self):
        history = self.history((400,))
        self.assertEqual(self.verify(history)["observed"], 0)
        op = next(iter(history.operations.values()))
        self.client.records.append((op.routing_key, op.frame))
        with self.assertRaisesRegex(CheckError, "definitely rejected"):
            self.verify(history)

    def test_duplicate_operation_is_rejected(self):
        history = self.history()
        self.client.records.append(self.client.records[0])
        with self.assertRaisesRegex(CheckError, "duplicate"):
            self.verify(history)

    def test_payload_corruption_is_rejected(self):
        history = self.history()
        key, frame = self.client.records[0]
        envelope = parse_json(frame)
        envelope["payload_b64"] = base64.b64encode(b"corrupted bytes").decode()
        self.client.records[0] = key, canonical(envelope) + b"\n"
        with self.assertRaisesRegex(CheckError, "payload or identity bytes"):
            self.verify(history)

    def test_semantically_identical_but_byte_changed_payload_is_rejected(self):
        history = self.history()
        key, frame = self.client.records[0]
        self.client.records[0] = key, frame.replace(b'"tenant":', b'"tenant": ')
        with self.assertRaisesRegex(CheckError, "bytes differ"):
            self.verify(history)

    def test_cross_incarnation_and_tenant_envelopes_are_rejected(self):
        for field, wrong in (("incarnation", "stale-incarnation"), ("tenant", "tenant-b"),
                             ("project", "project-b")):
            with self.subTest(field=field):
                other = tempfile.TemporaryDirectory()
                self.addCleanup(other.cleanup)
                self.path = Path(other.name) / "journal"
                self.client = PublicApiFixture()
                history = self.history((200,))
                key, frame = self.client.records[0]
                envelope = parse_json(frame)
                envelope[field] = wrong
                self.client.records[0] = key, canonical(envelope) + b"\n"
                with self.assertRaisesRegex(CheckError, "identity bytes"):
                    self.verify(history)

    def test_cross_namespace_same_name_same_producer_and_payload(self):
        other = Stream("tenant-b", "project-b", self.stream.name, "incarnation-two")
        client_b = PublicApiFixture()
        with Journal(self.path, {}) as journal:
            for stream, client in ((self.stream, self.client), (other, client_b)):
                recorder = Recorder(journal, client)
                recorder.create(stream)
                recorder.append(stream, "key", "colliding-producer", 0, b"same bytes")
        history = load_journal(self.path)
        clients = {self.stream.namespace: self.client, other.namespace: client_b}
        self.assertEqual(check(history, clients)["observed"], 2)
        self.client.records, client_b.records = client_b.records, self.client.records
        with self.assertRaisesRegex(CheckError, "cross-tenant/incarnation"):
            check(history, clients)

    def test_cross_routing_key_rejected(self):
        history = self.history((200,))
        self.client.records[0] = ("wrong-key", self.client.records[0][1])
        with self.assertRaisesRegex(CheckError, "routing-key"):
            self.verify(history)

    def test_unknown_operation_rejected(self):
        history = self.history((200,))
        key, frame = self.client.records[0]
        envelope = parse_json(frame)
        envelope["operation_id"] = "never-invoked"
        self.client.records[0] = key, canonical(envelope) + b"\n"
        with self.assertRaisesRegex(CheckError, "unexpected logical"):
            self.verify(history)

    def test_real_time_order_and_cross_key_independence(self):
        history = self.history()
        self.client.records.reverse()
        with self.assertRaisesRegex(CheckError, "real-time order"):
            self.verify(history)

    def test_response_order_is_not_an_ordering_oracle_for_overlapping_operations(self):
        history = self.history()
        # Create a well-formed overlapping journal: both invokes precede either
        # response. Response arrival differs from the legal observed log order.
        entries = [parse_json(line) for line in self.path.read_bytes().splitlines()]
        entries = entries[:3] + [entries[3], entries[5], entries[4], entries[6]]
        self.rewrite(entries)
        history = load_journal(self.path)
        self.client.records.reverse()
        self.assertEqual(self.verify(history)["observed"], 2)

    def test_no_global_order_across_routing_keys(self):
        history = self.history(keys=["a", "b"])
        self.client.records.reverse()
        self.assertEqual(self.verify(history)["observed"], 2)

    def test_all_pagination_and_missing_stream_defects_fail_closed(self):
        history = self.history()
        for defect in ("missing_stream", "missing_cursor", "repeat_cursor", "false_completion",
                       "invalid_json", "malformed_scan", "empty_incomplete", "keyed_truncation"):
            with self.subTest(defect=defect):
                self.client.defect = defect
                with self.assertRaises(CheckError):
                    self.verify(history, max_pages=4)

    def test_page_bound_exhaustion_is_not_a_pass(self):
        history = self.history()
        with self.assertRaisesRegex(CheckError, "exceeded 1 pages"):
            self.verify(history, max_pages=1)

    def test_empty_expected_stream_must_still_exist(self):
        history = self.history(())
        self.assertEqual(self.verify(history)["streams"], 1)
        self.client.defect = "missing_stream"
        with self.assertRaises(CheckError):
            self.verify(history)

    def test_retry_is_one_logical_operation(self):
        with Journal(self.path, {}) as journal:
            recorder = Recorder(journal, self.client)
            recorder.create(self.stream)
            self.client.append_status = 408
            oid, verdict = recorder.append(self.stream, "key", "producer", 0, b"bytes")
            self.assertEqual(verdict, "ambiguous")
            self.client.append_status, self.client.commit = 200, False
            recorder.append(self.stream, "key", "producer", 0, b"bytes", op_id=oid)
        report = self.verify(load_journal(self.path))
        self.assertEqual((report["acknowledged"], report["observed"]), (1, 1))

    def test_later_rejection_cannot_erase_earlier_ambiguity(self):
        with Journal(self.path, {}) as journal:
            recorder = Recorder(journal, self.client)
            recorder.create(self.stream)
            self.client.append_status = 408
            oid, _ = recorder.append(self.stream, "key", "producer", 0, b"bytes")
            self.client.append_status, self.client.commit = 403, False
            recorder.append(self.stream, "key", "producer", 0, b"bytes", op_id=oid)
        self.assertEqual(self.verify(load_journal(self.path))["ambiguous_observed"], 1)

    def test_changed_payload_on_retry_rejected_by_journal(self):
        with Journal(self.path, {}) as journal:
            recorder = Recorder(journal, self.client)
            recorder.create(self.stream)
            oid, _ = recorder.append(self.stream, "key", "producer", 0, b"bytes")
            recorder.append(self.stream, "key", "producer", 0, b"changed", op_id=oid)
        with self.assertRaisesRegex(CheckError, "retry changed"):
            load_journal(self.path)

    def test_same_producer_slot_different_logical_operation_rejected(self):
        with Journal(self.path, {}) as journal:
            recorder = Recorder(journal, self.client)
            recorder.create(self.stream)
            recorder.append(self.stream, "key", "producer", 0, b"bytes")
            recorder.append(self.stream, "key", "producer", 0, b"bytes")
        with self.assertRaisesRegex(CheckError, "sequence reused"):
            load_journal(self.path)

    def test_producer_order_checked_even_when_invocations_overlap(self):
        with Journal(self.path, {}) as journal:
            recorder = Recorder(journal, self.client)
            recorder.create(self.stream)
            for sequence in range(2):
                recorder.append(self.stream, "key", "producer", sequence, b"bytes")
        entries = [parse_json(line) for line in self.path.read_bytes().splitlines()]
        self.rewrite(entries[:3] + [entries[3], entries[5], entries[4], entries[6]])
        self.client.records.reverse()
        with self.assertRaisesRegex(CheckError, "per-producer sequence"):
            self.verify(load_journal(self.path))

    def test_retry_that_commits_twice_is_detected(self):
        with Journal(self.path, {}) as journal:
            recorder = Recorder(journal, self.client)
            recorder.create(self.stream)
            self.client.append_status = 408
            oid, _ = recorder.append(self.stream, "key", "producer", 0, b"bytes")
            self.client.append_status = 200
            recorder.append(self.stream, "key", "producer", 0, b"bytes", op_id=oid)
        with self.assertRaisesRegex(CheckError, "duplicate logical"):
            self.verify(load_journal(self.path))

    def test_invocation_is_persisted_before_network_and_outcome_before_return(self):
        observed = []
        original_request = self.client.request

        def request(method, path, body=None, headers=None):
            entries = [parse_json(line) for line in self.path.read_bytes().splitlines()]
            head = parse_json(self.path.with_name(self.path.name + ".head").read_bytes())
            self.assertEqual(head, {"entries": len(entries), "digest": entries[-1]["digest"]})
            self.assertEqual(entries[-1]["kind"], "create" if method == "PUT" else "invoke")
            observed.append(method)
            return original_request(method, path, body, headers)

        self.client.request = request
        self.history((200,))
        self.assertEqual(observed, ["PUT", "POST"])
        last = parse_json(self.path.read_bytes().splitlines()[-1])
        self.assertEqual((last["kind"], last["data"]["verdict"]), ("outcome", "acknowledged"))

    def rewrite(self, entries):
        """Rebuild valid hashes solely to test semantic (not hash) validation."""
        self.path.unlink()
        self.path.with_name(self.path.name + ".head").unlink()
        with Journal(self.path, entries[0]["data"]) as journal:
            for entry in entries[1:]:
                journal.record(entry["kind"], entry["data"])

    def rewrite_without_last_entry(self):
        entries = [parse_json(line) for line in self.path.read_bytes().splitlines()]
        self.rewrite(entries[:-1])

    def test_journal_byte_corruption_and_both_forms_of_truncation_rejected(self):
        self.history()
        original = self.path.read_bytes()
        for damaged in (original.replace(b"identical", b"different", 1)
                        if b"identical" in original else original.replace(b"tenant-a", b"tenant-b", 1),
                        original[:-1], b"\n".join(original.splitlines()[:-1]) + b"\n"):
            with self.subTest(size=len(damaged)):
                self.path.write_bytes(damaged)
                with self.assertRaises(CheckError):
                    load_journal(self.path)
        self.path.write_bytes(original)
        load_journal(self.path)

    def test_unknown_event_and_contradictory_outcome_rejected(self):
        self.history()
        entries = [parse_json(line) for line in self.path.read_bytes().splitlines()]
        changed = copy.deepcopy(entries)
        changed[-1]["data"]["status"] = 400
        self.rewrite(changed)
        with self.assertRaisesRegex(CheckError, "contradicts"):
            load_journal(self.path)
        changed = copy.deepcopy(entries)
        changed[-1]["kind"] = "erase_obligations"
        self.rewrite(changed)
        with self.assertRaisesRegex(CheckError, "unknown journal event"):
            load_journal(self.path)

    def test_unacknowledged_creation_cannot_pass(self):
        with Journal(self.path, {}) as journal:
            journal.record("create", {"stream": dataclasses.asdict(self.stream)})
        with self.assertRaisesRegex(CheckError, "campaign incomplete"):
            load_journal(self.path)

    def test_active_journal_cannot_be_read_as_quiescent_history(self):
        with Journal(self.path, {}) as journal:
            with self.assertRaises(BlockingIOError):
                load_journal(self.path)
            self.assertIsNotNone(journal)


class TransportTests(unittest.TestCase):
    def test_current_status_contract_is_conservative(self):
        self.assertEqual(classify_status(200), "acknowledged")
        for status in (None, 204, 302, 408, 409, 429, 500, 502, 503):
            self.assertEqual(classify_status(status), "ambiguous")
        for status in (400, 401, 403, 404, 413):
            self.assertEqual(classify_status(status), "rejected")

    def test_duplicate_json_keys_and_nonfinite_values_rejected(self):
        for payload in (b'{"x":1,"x":2}', b'[NaN]', b'[Infinity]'):
            with self.assertRaises(CheckError):
                parse_json(payload)

    def test_real_http_truncated_success_body_is_ambiguous(self):
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                self.send_response(200)
                self.send_header("Content-Length", "100")
                self.end_headers()
                self.wfile.write(b"{partial")
                self.close_connection = True

            def log_message(self, *_):
                pass

        with http.server.HTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.handle_request)
            thread.start()
            client = HttpClient(f"http://127.0.0.1:{server.server_port}", "token", "key", 2)
            response = client.request("POST", "/records", b"input")
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertIsNone(response.status)
            self.assertEqual(response.error, "incomplete or invalid content length")
            self.assertEqual(classify_status(response.status), "ambiguous")

    def test_redirect_does_not_replay_request_or_forward_credentials(self):
        received = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                received.append(self.path)
                self.rfile.read(int(self.headers["Content-Length"]))
                self.send_response(307)
                self.send_header("Location", "/unexpected-replay")
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *_):
                pass

        with http.server.HTTPServer(("127.0.0.1", 0), Handler) as server:
            server.timeout = 1
            thread = threading.Thread(target=server.handle_request)
            thread.start()
            client = HttpClient(f"http://127.0.0.1:{server.server_port}", "token", "key", 2)
            response = client.request("POST", "/records", b"input")
            thread.join(5)
            self.assertEqual(response.status, 307)
            self.assertEqual(received, ["/records"])


if __name__ == "__main__":
    unittest.main()
