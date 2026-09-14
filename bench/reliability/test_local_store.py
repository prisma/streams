"""Executable controls for the local restore material, before any upload."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import local_store


class RestoreMaterialTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = b"independently retained bytes"
        self.entry = {"key": "registry/a/../name", "file": "00000000.object",
                      "size": len(self.data), "sha256": hashlib.sha256(self.data).hexdigest()}
        (self.root / self.entry["file"]).write_bytes(self.data)
        self.save([self.entry])

    def save(self, entries):
        (self.root / "manifest.json").write_text(json.dumps({"version": 1, "objects": entries}))

    def test_exact_bytes_and_opaque_object_key(self):
        self.assertEqual(local_store.validate(self.root), [(self.entry["key"], self.data)])

    def test_corrupted_bytes_rejected_before_contacting_destination(self):
        (self.root / self.entry["file"]).write_bytes(b"damaged")
        with patch.object(local_store, "request") as contact:
            with self.assertRaisesRegex(ValueError, "integrity failure"):
                local_store.restore("http://127.0.0.1:9500", "test", self.root)
            contact.assert_not_called()

    def test_missing_object_rejected(self):
        (self.root / self.entry["file"]).unlink()
        with self.assertRaises(FileNotFoundError):
            local_store.validate(self.root)

    def test_manifest_cannot_escape_backup_directory(self):
        self.save([dict(self.entry, file="../secret")])
        with self.assertRaisesRegex(ValueError, "identity"):
            local_store.validate(self.root)

    def test_duplicate_object_identity_rejected(self):
        second = dict(self.entry, file="00000001.object")
        (self.root / second["file"]).write_bytes(self.data)
        self.save([self.entry, second])
        with self.assertRaisesRegex(ValueError, "identity"):
            local_store.validate(self.root)

    def test_extra_object_rejected(self):
        (self.root / "extra.object").write_bytes(b"not in manifest")
        with self.assertRaisesRegex(ValueError, "unaccounted"):
            local_store.validate(self.root)

    def test_nonempty_restore_destination_rejected(self):
        with patch.object(local_store, "request", return_value=b'{"objects":1}'):
            with self.assertRaisesRegex(ValueError, "fresh empty"):
                local_store.restore("http://127.0.0.1:9500", "test", self.root)

    def test_remote_endpoints_rejected(self):
        for url in ["https://127.0.0.1:9500", "http://example.com", "http://user@127.0.0.1",
                    "http://127.0.0.1/path", "http://127.0.0.1?query", "http://127.0.0.1#frag"]:
            with self.subTest(url=url), self.assertRaises(ValueError):
                local_store.local_url(url)

    def test_listing_missing_completion_proof_rejected(self):
        with patch.object(local_store, "request", return_value=b"<ListBucketResult/>"):
            with self.assertRaisesRegex(ValueError, "incomplete"):
                local_store.objects("http://127.0.0.1:9500", "test")

    def test_repeated_listing_cursor_rejected(self):
        xml = b"<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>x</NextContinuationToken></ListBucketResult>"
        with patch.object(local_store, "request", return_value=xml):
            with self.assertRaisesRegex(ValueError, "incomplete"):
                local_store.objects("http://127.0.0.1:9500", "test")


if __name__ == "__main__":
    unittest.main()
