"""Exercise the real gate with parsed Rust and isolated allowance documents."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from common import syntax
import source_gate
import source_rules

LEDGER = 'docs/quality/exception-growth.json'


def write_documents(root, documents):
    for name, document in documents.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(document if isinstance(document, str) else json.dumps(document))


def commit_tree(root, files):
    write_documents(root, files)
    for command in (['git', 'init', '-q'],
                    ['git', 'config', 'user.email', 'quality@example.test'],
                    ['git', 'config', 'user.name', 'Quality Fixture'],
                    # -f: a global ignore file must not drop target/ or node_modules/ fixtures.
                    ['git', 'add', '-f', '.'],
                    ['git', 'commit', '-qm', 'baseline']):
        subprocess.run(command, cwd=root, check=True)
    return subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()


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
            LEDGER: {'schema': 1, 'rows': []},
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_documents(root, documents)
            with patch.object(source_gate, 'ROOT', root), \
                 patch.object(source_gate, 'merge_base', return_value='fixture-base'), \
                 patch.object(source_gate, 'previous', return_value=None), \
                 patch.object(source_gate, 'base_sources', return_value=[]):
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


class ExceptionSiteCeilings(unittest.TestCase):
    BEFORE = '''#[expect(clippy::unwrap_used, reason = "owner; poison invariant; no recovery")]
pub fn check(lock: &std::sync::Mutex<()>, value: Option<u8>) {
    let _guard = lock.lock().unwrap();
    let _ = std::hint::black_box(value);
}
'''
    AFTER = BEFORE.replace('std::hint::black_box(value)', 'Option::unwrap(value)')
    LONG = ('#[expect(clippy::too_many_lines, reason = "transaction; one sequence; no split")]\n'
            'fn long() {\n' + ' let _x = 1;\n' * 101 + '}\n')
    CONTRACT = "('src/owner.rs', 'crate::long', 'function', 'clippy::too_many_lines')"

    def baseline(self, root, files, rows=()):
        documents = {
            'docs/quality/policy.json': {'immutable_sha256': {}, 'adoption_line_additions': {}},
            'docs/quality/legacy-source.json': {'occurrences': [], 'lines': {p: 100 for p in files}},
            'docs/quality/source-allowances.json': {'occurrences': []},
            'docs/quality/owners.json': {'occurrences': []},
            'docs/refactor/architecture-policy.json': {'sse_core_files': []},
            'docs/quality/syntax-fragments.json': {},
            LEDGER: {'schema': 1, 'rows': list(rows)},
        }
        return commit_tree(root, {**documents, **files})

    def check(self, root, base, sources):
        with patch.object(source_gate, 'ROOT', root), \
             patch.object(source_gate, 'merge_base', return_value=base):
            return source_gate.check(sources, syntax(sources))

    def test_real_source_gate_rejects_an_associated_call_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = self.baseline(root, {'src/owner.rs': self.BEFORE})
            self.assertEqual(self.check(root, base, {'src/owner.rs': self.BEFORE}), [])
            errors = self.check(root, base, {'src/owner.rs': self.AFTER})
            self.assertTrue(any('unwrap_site:' in error for error in errors), errors)

    def test_real_source_gate_compares_a_renamed_file_with_its_base(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = self.baseline(root, {'src/owner.rs': self.BEFORE})
            (root / 'src/owner.rs').unlink()
            self.assertEqual(self.check(root, base, {'src/moved.rs': self.BEFORE}), [])
            errors = self.check(root, base, {'src/moved.rs': self.AFTER})
            self.assertTrue(any("'clippy::unwrap_used') (moved from src/owner.rs): unwrap_sites 1 -> 2" in e
                                for e in errors), errors)

    def test_real_source_gate_admits_only_a_recorded_growth_row(self):
        grown = self.LONG.replace('\n}', '\n let _y = 2;\n}')
        recorded = dict(path='src/owner.rs', owner='crate::long', scope='function',
                        lint='clippy::too_many_lines', metrics={'scope_lines': 104},
                        rationale='one transaction; a split re-reads its state',
                        approver='Søren Bramer Schmidt')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = self.baseline(root, {'src/owner.rs': self.LONG}, [recorded])
            admitted = self.check(root, base, {'src/owner.rs': grown})
            write_documents(root, {LEDGER: {'schema': 1, 'rows': []}})
            refused = self.check(root, base, {'src/owner.rs': grown})
        self.assertEqual(admitted, [])
        self.assertEqual(refused, [
            f'accepted exception grew without an approved growth row: {self.CONTRACT}: scope_lines 103 -> 104'])

    def test_a_malformed_ledger_fails_without_any_base_source(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = self.baseline(root, {})
            write_documents(root, {LEDGER: {'schema': 2, 'rows': []}})
            with self.assertRaises(ValueError):
                self.check(root, base, {'src/new.rs': self.LONG})


class UnparsedBase(unittest.TestCase):
    def test_a_push_repairing_an_unparseable_base_file_is_checked_not_crashed(self):
        grown = ExceptionSiteCeilings.LONG.replace('\n}', '\n let _y = 2;\n}')
        for repair in ('delete', 'fix'):
            with self.subTest(repair=repair), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                base = ExceptionSiteCeilings.baseline(
                    self, root, {'src/owner.rs': ExceptionSiteCeilings.LONG,
                                 'src/broken.rs': 'pub fn half_written(\n'})
                sources = {'src/owner.rs': grown}
                if repair == 'fix':
                    sources['src/broken.rs'] = 'pub fn half_written() {}\n'
                errors = ExceptionSiteCeilings.check(self, root, base, sources)
                self.assertEqual(errors, [
                    'accepted exception grew without an approved growth row: '
                    f'{ExceptionSiteCeilings.CONTRACT}: scope_lines 103 -> 104'])


class BaseSources(unittest.TestCase):
    def test_base_sources_read_a_tree_with_a_non_utf8_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commit_tree(root, {'src/a.rs': 'fn a() {}\n', 'docs/quality/syntax-fragments.json': {}})
            # Some filesystems refuse the name, so it enters through the index.
            blob = subprocess.check_output(['git', 'hash-object', '-w', '--stdin'], cwd=root,
                                           input=b'\x00').decode().strip()
            name = os.fsdecode(b'fuzz/seed-caf\xe9.bin')
            subprocess.run(['git', 'update-index', '--add', '--cacheinfo', f'100644,{blob},{name}'],
                           cwd=root, check=True)
            subprocess.run(['git', 'commit', '-qm', 'latin-1 name'], cwd=root, check=True)
            base = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
            with patch.object(source_gate, 'ROOT', root):
                self.assertEqual(source_gate.base_sources(base), ['src/a.rs'])

    def test_base_sources_skip_what_the_checkout_skips(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = commit_tree(root, {
                'src/a.rs': 'fn a() {}\n',
                'src/target/t.rs': 'fn t() {}\n',
                'sdk/node_modules/n.rs': 'not rust {',
                '.agents/b.rs': 'not rust {',
                '.hidden/g.rs': 'not rust {',
                'node_modules/c.rs': 'not rust {',
                'target/d.rs': 'not rust {',
                'docs/e.md': 'text',
                'scripts/template.rs': 'fn {{placeholder}}() {}',
                'docs/quality/syntax-fragments.json': {'scripts/template.rs': {'reason': 'template', 'sha256': '0'}},
            })
            with patch.object(source_gate, 'ROOT', root):
                self.assertEqual(source_gate.base_sources(base), ['src/a.rs', 'src/target/t.rs'])
                # The checkout walk skips the same directories (templates are
                # skipped later, by syntax()).
                self.assertEqual(list(source_gate.tracked_sources(root)),
                                 ['scripts/template.rs', 'src/a.rs', 'src/target/t.rs'])
                with self.assertRaises(subprocess.CalledProcessError):
                    source_gate.base_sources('0' * 40)


if __name__ == '__main__':
    unittest.main()
