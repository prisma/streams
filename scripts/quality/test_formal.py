"""The formal driver never turns an incomplete, unattributable or empty run into a pass."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import formal

TLC_PASS = '''TLC2 Version 2.19 of 08 August 2024 (rev: 5a47802)
Model checking completed. No error has been found.
  Estimates of the probability that TLC did not check all reachable states
5 states generated, 4 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 4.
'''
TLC_VIOLATION = '''Error: Invariant NewestReservationInstalls is violated.
Error: The behavior up to this point is:
State 1: <Initial predicate>
7 states generated, 6 distinct states found, 1 states left on queue.
'''
TLC_UNDEFINED = '''Error: The invariant NoSuchInvariant specified in the configuration file
is not defined in the specification.
'''
KANI_PASS = '''Checking harness offsets::proofs::kani_002_start_and_successor_indices...
RESULTS:
Check 1: offsets::proofs::kani_002_start_and_successor_indices.assertion.1
\t - Status: SUCCESS
\t - Description: "START scans from index 0"
\t - Location: src/offsets/proofs.rs:39:5 in function offsets::proofs::kani_002_start_and_successor_indices

Check 2: offsets::proofs::kani_002_start_and_successor_indices.cover.1
\t - Status: SATISFIED
\t - Description: "cover condition: next == 0"

SUMMARY:
 ** 0 of 1 failed
 ** 2 of 2 cover properties satisfied

VERIFICATION:- SUCCESSFUL
Verification Time: 1.2s

Manual Harness Summary:
Complete - 1 successfully verified harnesses, 0 failures, 1 total.
'''
KANI_FAIL = KANI_PASS.replace('''Check 1: offsets::proofs::kani_002_start_and_successor_indices.assertion.1
\t - Status: SUCCESS''', '''Check 1: offsets::proofs::kani_002_start_and_successor_indices.assertion.1
\t - Status: FAILURE''').replace('VERIFICATION:- SUCCESSFUL', 'VERIFICATION:- FAILED').replace(
    'Complete - 1 successfully verified harnesses, 0 failures, 1 total.',
    'Complete - 0 successfully verified harnesses, 1 failures, 1 total.')
HARNESS = 'offsets::proofs::kani_002_start_and_successor_indices'


class TlcVerdicts(unittest.TestCase):
    def test_only_a_complete_search_passes(self):
        self.assertEqual(formal.tlc_verdict(TLC_PASS, 0, False)[0], 'pass')
        left = TLC_PASS.replace('0 states left on queue', '12 states left on queue')
        self.assertEqual(formal.tlc_verdict(left, 0, False)[0], 'incomplete')
        self.assertEqual(formal.tlc_verdict(TLC_PASS, None, True)[0], 'incomplete')

    def test_a_violation_is_attributed_to_its_invariant(self):
        verdict, detail = formal.tlc_verdict(TLC_VIOLATION, 12, False)
        self.assertEqual((verdict, detail['property']), ('violation', 'NewestReservationInstalls'))
        expect = {'result': 'violation', 'property': 'NewestReservationInstalls'}
        self.assertTrue(formal.judge('negative-control', expect, verdict, detail))
        other = {'result': 'violation', 'property': 'FenceBeforeInstall'}
        self.assertFalse(formal.judge('negative-control', other, verdict, detail))

    def test_configuration_errors_and_deadlocks_are_never_controls_or_passes(self):
        verdict, detail = formal.tlc_verdict(TLC_UNDEFINED, 151, False)
        self.assertEqual(verdict, 'error')
        self.assertFalse(formal.judge('baseline', {'result': 'pass'}, verdict, detail))
        self.assertFalse(formal.judge('negative-control', {'result': 'violation', 'property': 'X'},
                                      verdict, detail))
        verdict, detail = formal.tlc_verdict('Error: Deadlock reached.\n', 11, False)
        self.assertEqual(verdict, 'deadlock')
        self.assertFalse(formal.judge('baseline', {'result': 'pass'}, verdict, detail))


class KaniVerdicts(unittest.TestCase):
    def test_a_harness_passes_only_when_it_ran_alone_with_every_cover(self):
        self.assertEqual(formal.kani_verdict(KANI_PASS, 0, False, HARNESS)[0], 'pass')
        uncovered = KANI_PASS.replace('2 of 2 cover properties', '1 of 2 cover properties')
        self.assertEqual(formal.kani_verdict(uncovered, 0, False, HARNESS)[0], 'incomplete')
        self.assertEqual(formal.kani_verdict(KANI_PASS, 0, False, 'offsets::proofs::other')[0], 'error')

    def test_zero_discovery_is_an_error(self):
        output = 'error: no harnesses matched the harness filter: `offsets::proofs::gone`\n'
        self.assertEqual(formal.kani_verdict(output, 1, False, 'offsets::proofs::gone')[0], 'error')

    def test_a_control_must_break_the_named_assertion(self):
        verdict, detail = formal.kani_verdict(KANI_FAIL, 1, False, HARNESS)
        self.assertEqual(verdict, 'fail')
        self.assertTrue(formal.judge('negative-control', {'result': 'fail', 'property': 'START scans'},
                                     verdict, detail))
        self.assertFalse(formal.judge('negative-control', {'result': 'fail', 'property': 'only index 0'},
                                      verdict, detail))

    def test_an_unwinding_failure_is_incomplete(self):
        unwound = KANI_FAIL.replace('"START scans from index 0"', '"unwinding assertion loop 0"')
        self.assertEqual(formal.kani_verdict(unwound, 1, False, HARNESS)[0], 'incomplete')


class ManifestValidation(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        (root / 'src/offsets').mkdir(parents=True)
        (root / 'src/offsets.rs').write_text('fn f() {}\n')
        (root / 'src/offsets/proofs.rs').write_text(
            '#[kani::proof]\n#[kani::unwind(4)]\nfn kani_x() {}\n')
        (root / 'models').mkdir()
        (root / 'models/M.tla').write_text('---- MODULE M ----\n====\n')
        (root / 'models/M.cfg').write_text('SPECIFICATION Spec\nINVARIANT Safe\n')
        (root / 'models/MC_bad.cfg').write_text('SPECIFICATION Spec\nINVARIANTS\n  Safe\n  Typed\n')
        (root / 'patch.diff').write_text('+++ b/src/offsets.rs\n')
        (root / 'assumptions.md').write_text('## ASM-CLOCK\n')
        patches = [mock.patch.object(formal, 'ROOT', root),
                   mock.patch.object(formal, 'ASSUMPTIONS', root / 'assumptions.md')]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

    def tearDown(self):
        self.directory.cleanup()

    def kani(self, **overrides):
        obligation = {
            'id': 'KANI-X', 'kind': 'kani', 'title': 't', 'status': 'pass-with-recorded-scope',
            'owner': 'o', 'source_paths': ['src/offsets.rs'],
            'verification_paths': ['src/offsets/proofs.rs'], 'requirements': [],
            'input_scope': 's', 'assumptions': ['ASM-CLOCK'],
            'checks': [
                {'id': 'KANI-X/base', 'role': 'baseline', 'harness': 'offsets::proofs::kani_x',
                 'expect': {'result': 'pass'}},
                {'id': 'KANI-X/nc', 'role': 'negative-control', 'harness': 'offsets::proofs::kani_x',
                 'patch': 'patch.diff', 'expect': {'result': 'fail', 'property': 'p'}},
            ]}
        obligation.update(overrides)
        return {'schema': 1, 'obligations': [obligation]}

    def test_a_valid_kani_obligation_passes(self):
        self.assertEqual(formal.validate(self.kani()), [])

    def test_an_undeclared_harness_is_zero_discovery(self):
        manifest = self.kani()
        manifest['obligations'][0]['checks'][0]['harness'] = 'offsets::proofs::renamed'
        self.assertTrue(any('not declared' in p for p in formal.validate(manifest)))

    def test_a_passing_obligation_needs_a_negative_control(self):
        manifest = self.kani()
        del manifest['obligations'][0]['checks'][1]
        self.assertTrue(any('needs a negative-control' in p for p in formal.validate(manifest)))

    def test_unknown_statuses_and_assumptions_fail_closed(self):
        problems = formal.validate(self.kani(status='verified', assumptions=['ASM-MAGIC']))
        self.assertTrue(any('unknown status' in p for p in problems))
        self.assertTrue(any('ASM-MAGIC' in p for p in problems))

    def test_a_control_config_checks_only_its_target(self):
        manifest = {'schema': 1, 'obligations': [{
            'id': 'TLA-X', 'kind': 'tla', 'title': 't', 'status': 'implemented-unchecked',
            'owner': 'o', 'source_paths': ['src/offsets.rs'], 'verification_paths': ['models/M.tla'],
            'requirements': [], 'input_scope': 's',
            'checks': [
                {'id': 'TLA-X/base', 'role': 'baseline', 'spec': 'models/M.tla', 'config': 'models/M.cfg',
                 'expect': {'result': 'pass'}},
                {'id': 'TLA-X/nc', 'role': 'negative-control', 'spec': 'models/M.tla',
                 'config': 'models/MC_bad.cfg', 'expect': {'result': 'violation', 'property': 'Safe'}},
                {'id': 'TLA-X/nc2', 'role': 'negative-control', 'spec': 'models/M.tla',
                 'config': 'models/M.cfg', 'expect': {'result': 'violation', 'property': 'Safe'}},
                {'id': 'TLA-X/w', 'role': 'witness', 'spec': 'models/M.tla', 'config': 'models/M.cfg',
                 'expect': {'result': 'violation', 'property': 'Safe'}},
            ]}]}
        problems = formal.validate(manifest)
        self.assertTrue(any('only its target' in p for p in problems))
        self.assertTrue(any('reuses a baseline model unchanged' in p for p in problems))
        self.assertTrue(any('Witness_' in p for p in problems))

    def test_a_known_defect_keeps_the_obligation_a_counterexample(self):
        def tla(status):
            return {'schema': 1, 'obligations': [{
                'id': 'TLA-Y', 'kind': 'tla', 'title': 't', 'status': status, 'owner': 'o',
                'source_paths': ['src/offsets.rs'], 'verification_paths': ['models/M.tla'],
                'requirements': [], 'input_scope': 's',
                'checks': [{'id': 'TLA-Y/defect', 'role': 'known-defect', 'spec': 'models/M.tla',
                            'config': 'models/M.cfg', 'expect': {'result': 'violation', 'property': 'Safe'}}]}]}
        self.assertEqual(formal.validate(tla('counterexample')), [])
        self.assertTrue(any('makes the status counterexample' in p
                            for p in formal.validate(tla('implemented-unchecked'))))
        self.assertTrue(formal.judge('known-defect', {'result': 'violation', 'property': 'Safe'},
                                     'violation', {'property': 'Safe'}))
        self.assertFalse(formal.judge('known-defect', {'result': 'violation', 'property': 'Safe'},
                                      'pass', {}))

    def test_selection_follows_owners_models_and_global_inputs(self):
        manifest = self.kani()
        self.assertEqual(formal.select(manifest, ['src/offsets.rs']), ['KANI-X'])
        self.assertEqual(formal.select(manifest, ['Cargo.lock']), ['KANI-X'])
        self.assertEqual(formal.select(manifest, ['scripts/quality/formal.py']), ['KANI-X'])
        self.assertEqual(formal.select(manifest, ['verification/manifest.json']), ['KANI-X'])
        self.assertEqual(formal.select(manifest, ['src/http.rs']), [])

    def test_a_receipt_digests_its_own_entry_not_the_whole_manifest(self):
        manifest = self.kani()
        obligation = manifest['obligations'][0]
        with mock.patch.object(formal, 'pins', return_value={'kani': '0.68.0'}):
            before = formal.input_digest(obligation)
            other = dict(obligation, id='KANI-Y')
            self.assertEqual(formal.input_digest(obligation), before)
            self.assertNotEqual(formal.input_digest(other), before)
            (Path(self.directory.name) / 'src/offsets.rs').write_text('fn g() {}\n')
            self.assertNotEqual(formal.input_digest(obligation), before)


class RealManifest(unittest.TestCase):
    def test_the_checked_in_manifest_is_valid(self):
        self.assertEqual(formal.validate(formal.load()), [])

    def test_module_paths_follow_the_file_tree(self):
        self.assertEqual(formal.module_path('src/shard/commit_plan/proofs.rs'),
                         'shard::commit_plan::proofs')
        self.assertEqual(formal.module_path('src/a/mod.rs'), 'a')
        self.assertEqual(json.loads(json.dumps(formal.load()))['schema'], 1)


if __name__ == '__main__':
    unittest.main()
