"""Reject version evidence attached to different source or executable bytes."""
import hashlib
import copy
import json
from pathlib import Path
import tempfile
import unittest

from version_build import PRIOR, RELEASE, git, read_builds, reuse_historical, source_metadata
from version_transition import compression_witnesses


class VersionProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.path = self.directory / "receipt.json"
        builds = {}
        for label, revision in (("release", RELEASE), ("prior", PRIOR), ("current", git("rev-parse", "HEAD"))):
            binary = self.directory / label
            binary.write_bytes(label.encode())
            builds[label] = {**source_metadata(revision),
                             "artifacts": {"streams-slate": {
                                 "path": str(binary), "sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}}}
        store = self.directory / "s3lite"
        store.write_bytes(b"fixture object store executable")
        builds["current"]["artifacts"]["s3lite"] = {
            "path": str(store), "sha256": hashlib.sha256(store.read_bytes()).hexdigest()}
        self.receipt = {"schema": 1, "profile": "release", "rustc": "fixture compiler",
                        "builds": builds}

    def save(self):
        self.path.write_text(json.dumps(self.receipt))

    def test_matching_artifacts_are_accepted(self):
        self.save()
        self.assertEqual(read_builds(self.path), self.receipt)

    def test_replaced_binary_is_not_a_version_witness(self):
        self.save()
        (self.directory / "prior").write_bytes(b"different executable")
        with self.assertRaisesRegex(ValueError, "executable bytes changed"):
            read_builds(self.path)

    def test_same_binary_under_distinct_labels_is_not_a_matrix(self):
        builds = self.receipt["builds"]
        builds["current"]["artifacts"]["streams-slate"] = builds["prior"]["artifacts"]["streams-slate"]
        self.save()
        with self.assertRaisesRegex(ValueError, "same executable"):
            read_builds(self.path)

    def test_unreviewed_historical_source_is_rejected(self):
        self.receipt["builds"]["prior"]["revision"] = "e" * 40
        self.save()
        with self.assertRaisesRegex(ValueError, "historical revisions"):
            read_builds(self.path)

    def test_new_frame_claim_cannot_hide_legacy_source(self):
        self.receipt["builds"]["current"]["revision"] = git("rev-parse", RELEASE + "^")
        self.save()
        with self.assertRaisesRegex(ValueError, "actual immutable source"):
            read_builds(self.path)

    def test_moving_ref_is_not_immutable_provenance(self):
        self.receipt["builds"]["current"]["revision"] = "HEAD"
        self.save()
        with self.assertRaisesRegex(ValueError, "full immutable commit"):
            read_builds(self.path)

    def test_mislabeled_source_metadata_is_rejected(self):
        original = copy.deepcopy(self.receipt)
        for field in ("tree", "package_version", "cargo_lock_sha256", "crypto_source_sha256", "slatedb"):
            with self.subTest(field=field):
                self.receipt = copy.deepcopy(original)
                self.receipt["builds"]["current"][field] = "incorrect source metadata"
                self.save()
                with self.assertRaisesRegex(ValueError, "actual immutable source"):
                    read_builds(self.path)

    def test_reuse_preserves_historical_hashes_and_never_copies_current(self):
        self.save()
        destination = self.directory / "reused"
        destination.mkdir()
        historical, provenance = reuse_historical(self.path, destination, "fixture compiler")
        self.assertEqual(set(historical), {"release", "prior"})
        self.assertEqual({p.name for p in destination.iterdir()},
                         {"release-streams-slate", "prior-streams-slate", "reused-build-receipt.json"})
        for label, build in historical.items():
            original = self.receipt["builds"][label]
            copied = build["artifacts"]["streams-slate"]
            self.assertEqual(Path(copied["path"]).read_bytes(), label.encode())
            self.assertEqual(copied["sha256"], original["artifacts"]["streams-slate"]["sha256"])
            self.assertEqual(build["revision"], original["revision"])
        self.assertEqual(provenance["receipt_sha256"], hashlib.sha256(self.path.read_bytes()).hexdigest())
        self.assertEqual(Path(provenance["receipt_path"]).read_bytes(), self.path.read_bytes())

    def test_reuse_rejects_changed_artifacts_metadata_and_compiler_before_copy(self):
        original = copy.deepcopy(self.receipt)
        for failure in ("binary", "metadata", "compiler"):
            with self.subTest(failure=failure):
                self.receipt = copy.deepcopy(original)
                (self.directory / "prior").write_bytes(b"prior")
                if failure == "binary":
                    (self.directory / "prior").write_bytes(b"changed executable")
                elif failure == "metadata":
                    self.receipt["builds"]["prior"]["cargo_lock_sha256"] = "incorrect"
                self.save()
                destination = self.directory / failure
                destination.mkdir()
                with self.assertRaises(ValueError):
                    reuse_historical(self.path, destination,
                                     "different compiler" if failure == "compiler" else "fixture compiler")
                self.assertEqual(list(destination.iterdir()), [])


class CompressionWitnessTests(unittest.TestCase):
    def test_current_writer_must_actually_compress(self):
        plain = {"versions": ["prior", "current"], "phases": [
            {"label": label, "new_acknowledged_operations": 8,
             "absorption_witness": {"ingest_frame_bytes_total": 1000}}
            for label in ("prior", "current")]}
        packed = copy.deepcopy(plain)
        packed["phases"][0]["absorption_witness"]["ingest_frame_bytes_total"] = 400
        with self.assertRaisesRegex(RuntimeError, "current"):
            compression_witnesses(plain, packed)
        packed["phases"][1]["absorption_witness"]["ingest_frame_bytes_total"] = 500
        self.assertEqual(len(compression_witnesses(plain, packed)), 2)


if __name__ == "__main__":
    unittest.main()
