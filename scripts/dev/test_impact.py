"""Tests for scripts/dev/impact.py against the live repository (read-only).

Expectations are derived from the gate inputs themselves (manifest.json, the
mutation owner table, the ratchet ledgers), never hard-coded IDs, so a new
obligation or owner row does not break them; only a drift between this tool
and the gates does.
"""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('impact', ROOT / 'scripts/dev/impact.py')
impact = importlib.util.module_from_spec(spec)
spec.loader.exec_module(impact)
import source_rules  # noqa: E402  (impact.py put scripts/quality on sys.path)

PLANNED = 'src/shard/brand_new_owner.rs'


class ImpactTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        assert not (ROOT / PLANNED).exists(), f'{PLANNED} must stay hypothetical for these tests'
        cls.shard = impact.analyze(['src/shard.rs'])
        cls.manifest_edit = impact.analyze(['verification/manifest.json'])
        cls.mixed = impact.analyze(['src/segmap.rs', PLANNED, 'src/postings.rs',
                                    'src/shard/maintenance_tests.rs'])
        cls.files = {f.path: f for f in cls.mixed.files}

    def test_registered_owner_gives_its_gate_filters(self):
        segmap = self.files['src/segmap.rs']
        self.assertEqual(segmap.owner['name'], 'segmap')
        self.assertEqual(segmap.owner['filters'], ['segmap::'])
        self.assertIn('segmap', self.mixed.plan['selected_mutation_owners'])
        dev = next(c for c in self.mixed.tests if ' --lib ' in c + ' ' and '-p ' not in c)
        self.assertIn('segmap::', dev)
        self.assertNotIn('--release', dev)
        self.assertIn('--release', self.mixed.release)

    def test_harness_owner_runs_in_the_harness_package(self):
        self.assertEqual(self.files['src/postings.rs'].owner['package'], 'streams-quality-invariants')
        self.assertIn('cargo test --locked -p streams-quality-invariants --lib -- postings::', self.mixed.tests)

    def test_a_planned_critical_file_must_be_registered_first(self):
        planned = self.files[PLANNED]
        self.assertEqual(planned.status, 'planned')
        self.assertTrue(planned.unregistered)
        self.assertIn(PLANNED, self.mixed.plan['unregistered_mutation_source_files'])
        self.assertIn(f'REGISTER FIRST: {PLANNED}', impact.render_text(self.mixed))
        # Its inner loop is the nearest existing ancestor that has tests.
        self.assertIn(['--lib'], [s for s, _ in planned.filters])

    def test_whole_file_test_code_is_never_a_mutation_target(self):
        tests = self.files['src/shard/maintenance_tests.rs']
        self.assertTrue(tests.test_only)
        self.assertFalse(tests.unregistered)
        self.assertNotIn(tests.path, self.mixed.plan['mutation_source_files'])

    def test_formal_stale_set_is_what_the_manifest_digests(self):
        manifest = json.loads((ROOT / 'verification/manifest.json').read_text())
        expected = []
        for obligation in manifest['obligations']:
            files = set(obligation.get('source_paths', [])) | set(obligation.get('verification_paths', []))
            for check in obligation.get('checks', []):
                files |= {check[k] for k in ('spec', 'config', 'patch') if check.get(k)}
            if 'src/shard.rs' in files:
                expected.append(obligation['id'])
        self.assertTrue(expected, 'src/shard.rs should feed at least one obligation')
        self.assertEqual(self.shard.formal['stale_from_paths'], sorted(expected))
        self.assertEqual(self.shard.formal['selected'], sorted(expected))
        self.assertEqual(self.shard.formal['everything'], [])

    def test_manifest_edit_makes_ci_select_everything(self):
        result = self.manifest_edit.formal
        ids = sorted(o['id'] for o in json.loads((ROOT / 'verification/manifest.json').read_text())['obligations'])
        self.assertEqual(result['everything'], ['verification/manifest.json'])
        self.assertEqual(result['selected'], ids)
        self.assertIn('EVERYTHING', impact.render_text(self.manifest_edit))

    def test_by_path_include_warns_about_exception_growth(self):
        postings = self.files['src/postings.rs']
        self.assertEqual(postings.harness, ['streams-quality-fuzz', 'streams-quality-invariants'])
        text = impact.render_text(self.mixed)
        by_path = next(line for line in text.splitlines() if line.startswith('by-path'))
        self.assertIn('src/postings.rs', by_path)
        self.assertIn('exception-growth.json', by_path)

    def test_headroom_of_a_file_over_a_thousand_lines(self):
        report = self.shard.files[0]
        base = subprocess.run(['git', 'show', f'{self.shard.ratchet_base}:src/shard.rs'], cwd=ROOT,
                              capture_output=True, text=True, check=True).stdout
        ledgers = impact.ledgers_once()
        self.assertGreater(report.lines, 1000)
        self.assertEqual(report.ceiling, impact.line_ceiling(
            'src/shard.rs', ledgers.legacy['lines'], ledgers.policy['adoption_line_additions'],
            len(base.splitlines())))
        self.assertEqual(report.headroom, min(report.ceiling, report.arch_ceiling) - report.lines)
        if report.headroom <= impact.WARN_HEADROOM:
            self.assertIn('src/shard.rs', next(line for line in impact.render_text(self.shard).splitlines()
                                               if line.startswith('headroom')))

    def test_line_ceiling_matches_the_source_ratchet(self):
        """Pin the restated ceiling to source_rules.violations itself."""
        cases = [  # (legacy lines, adoption extra, base lines)
            (3009, 0, 3009), (1500, 0, 1200), (0, 0, None), (1436, 4, 1500), (0, 0, 900), (1200, 0, None),
        ]
        for legacy, extra, base in cases:
            adoption = {'x.rs': {'extra_lines': extra}} if extra else {}
            limit = impact.line_ceiling('x.rs', {'x.rs': legacy}, adoption, base)
            before = {'x.rs': legacy + extra}
            prior = {} if base is None else {'x.rs': base}
            for lines, grows in ((limit, False), (limit + 1, True)):
                with self.subTest(legacy=legacy, extra=extra, base=base, lines=lines), \
                        mock.patch.object(source_rules, 'from_compiler', return_value=(frozenset(), frozenset())):
                    failures = source_rules.violations(
                        {'x.rs': 'x\n' * lines}, {'x.rs': {'facts': [], 'items': []}},
                        before, prior, source_rules.Counter(), {'sse_core_files': []})
                    self.assertEqual(any(f.startswith('file growth: x.rs') for f in failures), grows, failures)

    def test_codemap_lists_the_shard_module(self):
        table = impact.codemap()
        row = next(line for line in table.splitlines() if line.startswith('| `shard` |'))
        self.assertIn('Shard log engine', row)

    def test_default_output_stays_short(self):
        for result in (self.shard, self.mixed):
            lines = impact.render_text(result).splitlines()
            self.assertLess(len(lines), 40, '\n'.join(lines))

    def test_json_output_is_machine_readable(self):
        document = json.loads(impact.render_json(self.mixed))
        self.assertEqual({f['path'] for f in document['files']}, set(self.files))
        self.assertIn('selected_mutation_owners', document['plan'])
        self.assertIn('stale_now', document['formal'])

    def test_module_tree_follows_path_attributes_and_nested_modules(self):
        tree = impact.module_tree()
        validated = {p.describe() for p in tree.placements['src/postings/validated.rs']}
        self.assertIn('streams-slate lib: crate::postings::validated', validated)
        self.assertIn('streams-quality-invariants lib: crate::postings::validated', validated)
        # tests/pilot_membership.rs declares an inline module before its #[path] module.
        membership = {p.target for p in tree.placements['src/bin/pilot/generator/membership.rs']}
        self.assertIn('test pilot_membership', membership)
        self.assertIn('bin pilot', membership)

    def test_module_declarations_parse_attributes(self):
        source = ('mod a {\n    #[allow(\n        dead_code,\n        reason = "x"\n    )]\n'
                  '    #[path = "inner.rs"]\n    mod b;\n}\n#[cfg(test)] mod c;\n'
                  '// mod commented;\n#[cfg(kani)]\nmod proofs;\n')
        self.assertEqual(impact.module_decls(source), [
            ('b', ('a',), 'inner.rs', ()), ('c', (), None, ('test',)), ('proofs', (), None, ('kani',))])

    def test_minimal_filters_drop_covered_substrings(self):
        self.assertEqual(impact.minimal_filters(['shard::', 'shard::x::', 'billing', 'billing::sweep::']),
                         ['billing', 'shard::'])
        self.assertEqual(impact.cargo_command(('--lib',), {'b::', 'a::'}), 'cargo test --locked --lib -- a:: b::')

    def test_pins_agree_with_the_gates_on_committed_files(self):
        """At HEAD the pin gates pass, so an unmodified file shows no drift."""
        ledgers = impact.ledgers_once()
        dirty = set(subprocess.run(['git', 'diff', '--name-only', 'HEAD'], cwd=ROOT, capture_output=True,
                                   text=True, check=True).stdout.split())
        pinned = sorted(p for p in ledgers.pins if p.endswith('.rs') and p not in dirty)[:6]
        inventoried = sorted(p for p in ledgers.inventory if p not in dirty)[:4]
        self.assertTrue(pinned and inventoried)
        for path in pinned:
            with self.subTest(path=path):
                self.assertEqual(impact.pin_drift(path, (ROOT / path).read_text(), ledgers.pins[path]), [])
        for path in inventoried:
            with self.subTest(path=path):
                self.assertEqual(impact.inventory_drift(path, (ROOT / path).read_text(),
                                                        ledgers.inventory[path]), {})

    def test_cli_resolves_repository_paths_from_any_directory(self):
        with tempfile.TemporaryDirectory() as elsewhere:
            result = subprocess.run([sys.executable, str(ROOT / 'scripts/dev/impact.py'), 'src/segmap.rs'],
                                    cwd=elsewhere, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('owner segmap', result.stdout)
        self.assertIn('== SUMMARY', result.stdout)

    def test_pin_hash_reproduces_every_committed_mechanism_pin(self):
        manifest = json.loads((ROOT / 'docs/refactor/review-mechanisms.json').read_text())
        checked = 0
        for entry in manifest['mechanisms']:
            for key in ('tests', 'support_functions'):
                for pin in entry.get(key, []):
                    if not pin['file'].endswith('.rs'):
                        continue
                    digest = impact.pin_hash(f"{pin['file']}::{pin['name']}").split()[0]
                    self.assertEqual(digest, pin['sha256'], pin)
                    checked += 1
        self.assertGreater(checked, 50)

    def test_examples_are_crate_roots(self):
        placed = impact.module_tree().placements.get('examples/effective_config.rs')
        self.assertTrue(placed, 'examples/effective_config.rs is not reached from a crate root')

    def test_the_scanner_follows_cargo_target_dir(self):
        with mock.patch.dict('os.environ', {'CARGO_TARGET_DIR': '/elsewhere'}, clear=False):
            import common  # noqa: E402
            self.assertEqual(common.syntax_binary(), Path('/elsewhere/debug/streams-quality-syntax'))

    def test_old_python_reexecs_under_a_newer_one(self):
        old = Path('/usr/bin/python3')
        if not old.is_file():
            self.skipTest('no system python3')
        version = subprocess.run([str(old), '-c', 'import sys; print(sys.version_info >= (3, 11))'],
                                 capture_output=True, text=True).stdout.strip()
        if version != 'False':
            self.skipTest('system python3 is already 3.11+')
        result = subprocess.run([str(old), str(ROOT / 'scripts/dev/impact.py'), 'src/segmap.rs'],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('== SUMMARY', result.stdout)

if __name__ == '__main__':
    unittest.main()
