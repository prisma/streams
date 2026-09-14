"""The receipt reader cannot turn an absent or unrelated probe into evidence."""
import argparse
import json
import unittest

from provider_probe import CHECKS, contract_result, endpoint


class ProviderReceiptTests(unittest.TestCase):
    def report(self, **changes):
        value = dict(prefix="disposable/contract-123", concurrency=8, checks=sorted(CHECKS))
        value.update(changes)
        return b"PROVIDER_CONTRACT_OK " + json.dumps(value).encode() + b"\n"

    def test_complete_bound_receipt(self):
        self.assertIsNotNone(contract_result(self.report(), "disposable", 8))

    def test_missing_or_duplicated_marker_fails(self):
        for raw in (b"", b"success", self.report() * 2, b"PROVIDER_CONTRACT_OK invalid"):
            self.assertIsNone(contract_result(raw, "disposable", 8))

    def test_incomplete_checks_fail(self):
        self.assertIsNone(contract_result(self.report(checks=[]), "disposable", 8))
        self.assertIsNone(contract_result(self.report(checks=sorted(CHECKS) + [sorted(CHECKS)[0]]), "disposable", 8))

    def test_unrelated_prefix_or_concurrency_fails(self):
        self.assertIsNone(contract_result(self.report(prefix="other/contract-123"), "disposable", 8))
        self.assertIsNone(contract_result(self.report(concurrency=1), "disposable", 8))

    def test_endpoint_excludes_credentials_and_remote_plaintext(self):
        for value in ("http://example.com", "https://user:password@example.com", "https://example.com?token=x", "https://example.com/path", "https://example.com#x"):
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                endpoint(value)
        self.assertEqual(endpoint("https://example.com/"), "https://example.com")
        self.assertEqual(endpoint("http://127.0.0.1:1234"), "http://127.0.0.1:1234")


if __name__ == "__main__":
    unittest.main()
