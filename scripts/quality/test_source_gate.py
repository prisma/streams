"""Exercise the real gate with parsed Rust and isolated allowance documents."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from common import syntax
import source_gate
import source_rules


class OwnerCeilings(unittest.TestCase):
    def check_counts(self, legacy, owned, actual, *, alias=False):
        path = 'src/owner.rs'
        call = 'launch' if alias else 'tokio::spawn'
        imports = 'use tokio::spawn as launch;\n' if alias else ''
        sources = {path: imports + 'fn owner() {\n' + f'    {call}(async {{}});\n' * actual + '}\n'}
        facts = syntax(sources)
        identity = ('effect', path, 'crate::owner', 'tokio::spawn')
        self.assertEqual(source_rules.inventory(facts)[identity], actual)
        site = dict(zip(('category', 'path', 'owner', 'syntax'), identity))
        legacy_rows = [dict(site, count=legacy)] if legacy else []
        owned_rows = [dict(site, count=owned, reason='Fixture request owner; exact site ceiling.')] if owned else []
        if alias:
            # The inventory also records the import declaration as an effect site.
            owned_rows.append(dict(site, owner='crate', count=1,
                                   reason='Fixture alias import; one declaration.'))
        documents = {
            'docs/quality/policy.json': {'immutable_sha256': {}, 'adoption_line_additions': {}},
            'docs/quality/legacy-source.json': {'occurrences': legacy_rows, 'lines': {path: 100}},
            'docs/quality/source-allowances.json': {'occurrences': legacy_rows},
            'docs/quality/owners.json': {'occurrences': owned_rows},
            'docs/refactor/architecture-policy.json': {'sse_core_files': []},
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, document in documents.items():
                target = root / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(json.dumps(document))
            with patch.object(source_gate, 'ROOT', root), \
                 patch.object(source_gate, 'merge_base', return_value='fixture-base'), \
                 patch.object(source_gate, 'previous', return_value=None):
                return source_gate.check(sources, facts)

    def assert_one_extra_site(self, errors):
        self.assertEqual(len(errors), 1, errors)
        self.assertIn('unregistered source occurrence (1)', errors[0])
        self.assertIn("('effect', 'src/owner.rs', 'crate::owner', 'tokio::spawn')", errors[0])

    def test_duplicate_registration_cannot_authorize_a_second_spawn(self):
        self.assert_one_extra_site(self.check_counts(1, 1, 2))

    def test_aliased_duplicate_registration_has_the_same_ceiling(self):
        self.assert_one_extra_site(self.check_counts(1, 1, 2, alias=True))

    def test_aliased_owner_accepts_the_reviewed_total(self):
        self.assertEqual(self.check_counts(1, 1, 1, alias=True), [])

    def test_partial_registration_does_not_add_to_existing_debt(self):
        self.assert_one_extra_site(self.check_counts(2, 1, 3))

    def test_unconverted_legacy_sites_remain_within_their_original_ceiling(self):
        self.assertEqual(self.check_counts(2, 1, 2), [])

    def test_larger_explicit_owner_can_authorize_its_reviewed_total(self):
        self.assertEqual(self.check_counts(1, 2, 2), [])

    def test_explicit_owner_total_is_still_a_ceiling(self):
        self.assert_one_extra_site(self.check_counts(1, 2, 3))

    def test_fully_owned_source_needs_no_legacy_allowance(self):
        self.assertEqual(self.check_counts(0, 2, 2), [])

    def test_unregistered_source_is_rejected(self):
        self.assert_one_extra_site(self.check_counts(0, 0, 1))


if __name__ == '__main__':
    unittest.main()
