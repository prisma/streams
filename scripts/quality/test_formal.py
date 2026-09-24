"""The formal driver never turns an incomplete, unattributable or empty run into a pass."""
import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import tempfile
import time
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
LEDGER = '''# Assumption ledger

## Clocks

### ASM-CLOCK

- **Statement:** time moves forward.

#### Notes

A level-4 heading stays inside its entry.

### ASM-OTHER

- **Statement:** something else.

## Trailing section

Prose that belongs to no entry.
'''
PINS = '''[slatedb]
rev = "0717cc1e"

[formal]
kani = "0.68.0"
tlc = "2.19"
'''


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


class FixtureTree(unittest.TestCase):
    """A temporary repository: sources, a model, a ledger, pins and receipts."""

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
        (root / 'verification').mkdir()
        (root / 'verification/assumptions.md').write_text(LEDGER)
        (root / 'quality-tools.toml').write_text(PINS)
        (root / 'Cargo.lock').write_text('# lock\n')
        self.root = root
        patches = [mock.patch.object(formal, 'ROOT', root),
                   mock.patch.object(formal, 'ASSUMPTIONS', root / 'verification/assumptions.md'),
                   mock.patch.object(formal, 'RECEIPTS', root / 'verification/receipts')]
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


class ManifestValidation(FixtureTree):
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
        before = formal.input_digest(obligation)
        other = dict(obligation, id='KANI-Y')
        self.assertEqual(formal.input_digest(obligation), before)
        self.assertNotEqual(formal.input_digest(other), before)
        (Path(self.directory.name) / 'src/offsets.rs').write_text('fn g() {}\n')
        self.assertNotEqual(formal.input_digest(obligation), before)

    def test_a_ledger_id_may_have_only_one_entry(self):
        (self.root / 'verification/assumptions.md').write_text(LEDGER + '### ASM-CLOCK\n\nagain\n')
        self.assertTrue(any('ASM-CLOCK has more than one entry' in p
                            for p in formal.validate(self.kani())))

    def test_a_counterexample_needs_a_known_defect(self):
        problems = formal.validate(self.kani(status='counterexample'))
        self.assertTrue(any('needs a known-defect check' in p for p in problems))

def invoke(*argv):
    """formal.py's exit status and output for one command line."""
    out = io.StringIO()
    with mock.patch.object(sys, 'argv', ['formal.py', *argv]), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        code = formal.main()
    return code, out.getvalue()


class ReviewedReceiptGaps(unittest.TestCase):
    """The review's reproductions against the checked-in manifest, on a copy of the receipts."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.receipts = Path(self.directory.name) / 'receipts'
        shutil.copytree(formal.RECEIPTS, self.receipts)
        patch = mock.patch.object(formal, 'RECEIPTS', self.receipts)
        patch.start()
        self.addCleanup(patch.stop)
        self.addCleanup(self.directory.cleanup)

    def edit(self, oid, change):
        path = self.receipts / f'{oid}.json'
        receipt = json.loads(path.read_text())
        change(receipt)
        path.write_text(json.dumps(receipt))

    def assert_check_fails_naming(self, oid):
        code, output = invoke('check')
        self.assertEqual(code, 1, output)
        self.assertIn(f'FORMAL_FAIL: {oid}', output)

    def test_the_checked_in_receipts_are_valid_and_fresh_decides_staleness(self):
        code, output = invoke('check')
        self.assertEqual(code, 0, output)
        stale = output.count('FORMAL_STALE:')
        code, output = invoke('check', '--fresh')
        self.assertEqual(code, 1 if stale else 0, output)

    def test_1_a_counterexample_without_a_receipt_fails(self):
        manifest = formal.load()
        for obligation in manifest['obligations']:
            if obligation['id'] in ('TLA-003', 'TLA-018'):
                obligation['status'] = 'counterexample'
                (self.receipts / f"{obligation['id']}.json").unlink()
        problems, _ = formal.receipt_report(manifest)
        self.assertIn('TLA-003: status counterexample has no receipt', problems)
        self.assertIn('TLA-018: status counterexample has no receipt', problems)

    def test_1_a_passing_obligation_without_a_receipt_fails(self):
        (self.receipts / 'KANI-001.json').unlink()
        self.assert_check_fails_naming('KANI-001')

    def test_2_an_emptied_check_list_fails_with_its_digest_kept(self):
        self.edit('KANI-039', lambda r: r.update(checks=[]))
        self.assert_check_fails_naming('KANI-039')

    def test_3_a_wrong_id_fails(self):
        self.edit('TLA-005', lambda r: r.update(id='TLA-999'))
        self.assert_check_fails_naming('TLA-005')

    def test_3_an_unsupported_schema_fails(self):
        self.edit('TLA-005', lambda r: r.update(schema=7))
        self.assert_check_fails_naming('TLA-005')

    def test_3_an_incomplete_fabricated_result_fails(self):
        self.edit('TLA-005', lambda r: r.update(checks=r['checks'][:1]))
        self.assert_check_fails_naming('TLA-005')

    def test_3_a_fabricated_verdict_fails(self):
        def flip(receipt):
            receipt['checks'][0]['verdict'] = 'incomplete'
        self.edit('TLA-005', flip)
        self.assert_check_fails_naming('TLA-005')

    def test_a_receipt_for_no_obligation_fails(self):
        shutil.copy(self.receipts / 'TLA-005.json', self.receipts / 'TLA-999.json')
        self.assert_check_fails_naming('TLA-999.json')


class TrustBoundarySelection(FixtureTree):
    """Case 4: the ledger and the locked dependencies invalidate what rests on them."""

    def tla(self, assumptions=('ASM-CLOCK',)):
        return {'id': 'TLA-X', 'kind': 'tla', 'status': 'pass-with-recorded-scope',
                'source_paths': ['src/offsets.rs'], 'verification_paths': ['models/M.tla'],
                'assumptions': list(assumptions),
                'checks': [{'id': 'TLA-X/base', 'role': 'baseline', 'spec': 'models/M.tla',
                            'config': 'models/M.cfg', 'expect': {'result': 'pass'}}]}

    def test_4_a_ledger_change_selects_the_obligations_that_name_an_assumption(self):
        manifest = {'obligations': [self.tla(), dict(self.tla(()), id='TLA-Z')]}
        self.assertEqual(formal.select(manifest, ['verification/assumptions.md']), ['TLA-X'])

    def test_a_ledger_change_selects_exactly_the_changed_entries_given_the_base(self):
        manifest = {'obligations': [self.tla(), dict(self.tla(['ASM-OTHER']), id='TLA-Y')]}
        base = formal.assumption_entries()
        base['ASM-OTHER'] = base['ASM-OTHER'].replace('something else', 'something older')
        self.assertEqual(formal.select(manifest, ['verification/assumptions.md'], base), ['TLA-Y'])
        self.assertEqual(formal.select(manifest, ['verification/assumptions.md'], {}),
                         ['TLA-X', 'TLA-Y'])

    def test_4_the_lockfile_and_cargo_manifest_select_tla_obligations(self):
        manifest = {'obligations': [self.tla()]}
        self.assertEqual(formal.select(manifest, ['Cargo.lock']), ['TLA-X'])
        self.assertEqual(formal.select(manifest, ['Cargo.toml']), ['TLA-X'])

    def test_the_digest_holds_the_exact_ledger_entry_and_the_slatedb_pin(self):
        obligation = self.tla()
        before = formal.input_digest(obligation)
        ledger = self.root / 'verification/assumptions.md'
        ledger.write_text(LEDGER.replace('something else', 'something new'))
        self.assertEqual(formal.input_digest(obligation), before, 'an unnamed entry changed')
        ledger.write_text(LEDGER.replace('A level-4 heading stays', 'A level-4 heading still stays'))
        self.assertNotEqual(formal.input_digest(obligation), before)
        ledger.write_text(LEDGER)
        self.assertEqual(formal.input_digest(obligation), before)
        (self.root / 'Cargo.lock').write_text('# another lock\n')
        self.assertNotEqual(formal.input_digest(obligation), before)
        (self.root / 'Cargo.lock').write_text('# lock\n')
        (self.root / 'quality-tools.toml').write_text(PINS.replace('0717cc1e', '11111111'))
        self.assertNotEqual(formal.input_digest(obligation), before)

    def test_entries_end_at_the_next_heading_of_level_three_or_higher(self):
        entries = formal.assumption_entries()
        self.assertEqual(sorted(entries), ['ASM-CLOCK', 'ASM-OTHER'])
        self.assertIn('stays inside its entry', entries['ASM-CLOCK'])
        self.assertNotIn('Prose that belongs', entries['ASM-OTHER'])


class ExecutionBinding(FixtureTree):
    """Case 5: the recorded digest names the inputs that every check analysed."""

    def fake_run(self, mutate=None):
        def run_check(obligation, check, logs):
            if mutate and check['id'] == 'KANI-X/base':
                mutate()
            expect = check['expect']
            expected = expect['result'] + (f":{expect['property']}" if expect.get('property') else '')
            if check['role'] == 'baseline':
                detail = {'failed_checks': [], 'covers': 1, 'covers_satisfied': 1}
                verdict = 'pass'
            else:
                detail = {'failed_checks': ['"p"'], 'covers': 1, 'covers_satisfied': 0,
                          'mutated': {'src/offsets.rs': {'baseline': 'a' * 64, 'control': 'b' * 64}}}
                verdict = 'fail'
            detail['log_sha256'] = 'c' * 64
            return formal.Result(check['id'], check['role'], expected, verdict, True, 1.0, detail)
        return run_check

    def record(self, mutate=None):
        manifest = self.kani()
        with mock.patch.object(formal, 'run_check', self.fake_run(mutate)), \
                mock.patch.object(formal, 'tool_problems', return_value=([], {'kani': '0.68.0'})), \
                mock.patch.object(formal, 'git_identity', return_value=('rev', False)), \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            code = formal.run(manifest, set(), set(), self.root / 'out', True)
        return code, manifest, self.root / 'verification/receipts/KANI-X.json'

    def test_5_a_source_change_during_the_run_fails_it_and_records_nothing(self):
        code, _, receipt = self.record(
            lambda: (self.root / 'src/offsets.rs').write_text('fn changed() {}\n'))
        self.assertEqual(code, 1)
        self.assertFalse(receipt.exists())

    def test_5_a_ledger_change_during_the_run_fails_it_and_records_nothing(self):
        code, _, receipt = self.record(lambda: (self.root / 'verification/assumptions.md').write_text(
            LEDGER.replace('time moves forward', 'time may move back')))
        self.assertEqual(code, 1)
        self.assertFalse(receipt.exists())

    def test_a_recorded_receipt_is_valid_fresh_and_names_its_inputs(self):
        before = formal.input_snapshot(self.kani()['obligations'][0])
        code, manifest, path = self.record()
        self.assertEqual(code, 0)
        receipt = json.loads(path.read_text())
        self.assertEqual((receipt['schema'], receipt['complete'], receipt['inputs']), (2, True, before))
        self.assertEqual(receipt['inputs']['files']['src/offsets.rs'],
                         formal.sha256(self.root / 'src/offsets.rs'))
        self.assertEqual({c['log_sha256'] for c in receipt['checks']}, {'c' * 64})
        self.assertEqual(formal.receipt_report(manifest), ([], []))
        (self.root / 'verification/assumptions.md').write_text(
            LEDGER.replace('time moves forward', 'time may move back'))
        problems, stale = formal.receipt_report(manifest)
        self.assertEqual(problems, [])
        self.assertEqual(stale, ['KANI-X: inputs changed since its receipt (assumptions:ASM-CLOCK)'])


class ReceiptStructure(FixtureTree):
    """Every rule that makes a receipt no evidence at all."""

    def tla(self, status='pass-with-recorded-scope', defect=False):
        checks = [
            {'id': 'T/base', 'role': 'baseline', 'spec': 'models/M.tla', 'config': 'models/M.cfg',
             'expect': {'result': 'pass'}},
            {'id': 'T/nc', 'role': 'negative-control', 'spec': 'models/M.tla',
             'config': 'models/MC_bad.cfg', 'expect': {'result': 'violation', 'property': 'Safe'}},
            {'id': 'T/w', 'role': 'witness', 'spec': 'models/M.tla', 'config': 'models/M.cfg',
             'expect': {'result': 'violation', 'property': 'Witness_Reach'}},
        ]
        if defect:
            checks.append({'id': 'T/defect', 'role': 'known-defect', 'spec': 'models/M.tla',
                           'config': 'models/M.cfg', 'expect': {'result': 'violation', 'property': 'Safe'}})
        return {'id': 'T', 'kind': 'tla', 'status': status, 'assumptions': ['ASM-CLOCK'],
                'source_paths': ['src/offsets.rs'], 'verification_paths': ['models/M.tla'],
                'checks': checks}

    def receipt(self, obligation):
        inputs = formal.input_snapshot(obligation)
        checks = []
        for check in obligation['checks']:
            expect = check['expect']
            entry = {'check': check['id'], 'role': check['role'], 'seconds': 1.0,
                     'expected': formal.expected_string(expect), 'log_sha256': 'd' * 64}
            if obligation['kind'] == 'tla':
                entry |= ({'verdict': 'pass', 'property': None, 'states_left': 0}
                          if check['role'] == 'baseline'
                          else {'verdict': 'violation', 'property': expect['property']})
            elif check['role'] == 'baseline':
                entry |= {'verdict': 'pass', 'failed_checks': [], 'covers': 2, 'covers_satisfied': 2}
            else:
                entry |= {'verdict': 'fail', 'failed_checks': [f'"{expect["property"]}"'],
                          'mutated': {'src/offsets.rs': {'baseline': 'a' * 64, 'control': 'b' * 64}}}
            checks.append(entry)
        return {'id': obligation['id'], 'schema': 2, 'complete': True, 'inputs': inputs,
                'inputs_sha256': formal.canonical_sha256(inputs), 'run_on': {}, 'tools': {},
                'checks': checks}

    def problems(self, obligation, change):
        receipt = self.receipt(obligation)
        change(receipt)
        return formal.receipt_problems(obligation, receipt)

    def assert_rejected(self, change, reason, obligation=None):
        problems = self.problems(obligation or self.tla(), change)
        self.assertTrue(any(reason in p for p in problems), problems)

    def test_a_matching_receipt_is_valid_and_fresh(self):
        obligation = self.tla()
        receipt = self.receipt(obligation)
        self.assertEqual(formal.receipt_problems(obligation, receipt), [])
        self.assertIsNone(formal.receipt_staleness(obligation, receipt))

    def test_the_check_set_is_exactly_the_manifests(self):
        self.assert_rejected(lambda r: r['checks'].pop(), 'omits: T/w')
        self.assert_rejected(lambda r: r['checks'].append(dict(r['checks'][0])), 'duplicates: T/base')
        self.assert_rejected(lambda r: r['checks'].append(dict(r['checks'][0], check='T/new')),
                             'does not: T/new')

    def test_role_and_expected_string_are_the_manifests(self):
        self.assert_rejected(lambda r: r['checks'][1].update(role='witness'), 'records role')
        self.assert_rejected(lambda r: r['checks'][1].update(expected='violation:Other'), 'receipt expected')

    def test_each_verdict_is_judged_as_the_run_was(self):
        self.assert_rejected(lambda r: r['checks'][0].update(verdict='incomplete'), 'does not meet baseline')
        self.assert_rejected(lambda r: r['checks'][0].update(states_left=3), 'no state on the queue')
        self.assert_rejected(lambda r: r['checks'][1].update(property='Typed'), 'does not meet negative-control')
        self.assert_rejected(lambda r: r['checks'][2].update(verdict='error'), 'does not meet witness')

    def test_a_current_receipt_is_complete_attributable_and_self_consistent(self):
        self.assert_rejected(lambda r: r.pop('complete'), 'not of a complete run')
        self.assert_rejected(lambda r: r.update(complete=False), 'not of a complete run')
        self.assert_rejected(lambda r: r.update(schema=3), 'schema 3')
        self.assert_rejected(lambda r: r.update(schema=True), 'schema True')
        self.assert_rejected(lambda r: r['checks'][0].update(check=['T/base']), 'named check objects')
        self.assert_rejected(lambda r: r.update(id='U'), "names 'U'")
        self.assert_rejected(lambda r: r['inputs']['files'].update({'src/offsets.rs': '0' * 64}),
                             'not the digest of its recorded inputs')
        self.assert_rejected(lambda r: r['checks'][1].pop('property'), 'lacks property')
        self.assert_rejected(lambda r: r['checks'][1].pop('log_sha256'), 'lacks log_sha256')

    def test_a_schema_1_receipt_validates_but_is_always_stale(self):
        obligation = self.tla()
        receipt = self.receipt(obligation)
        receipt = {key: value for key, value in receipt.items() if key not in ('complete', 'inputs')}
        receipt['schema'] = 1
        for check in receipt['checks']:
            check.pop('log_sha256')
        self.assertEqual(formal.receipt_problems(obligation, receipt), [])
        self.assertIn('schema 1', formal.receipt_staleness(obligation, receipt))

    def test_a_passing_obligation_cannot_carry_a_known_defect(self):
        self.assert_rejected(lambda r: None, 'carries a known defect', self.tla(defect=True))

    def test_a_counterexample_must_reproduce_its_known_defect(self):
        obligation = self.tla('counterexample', defect=True)
        self.assertEqual(self.problems(obligation, lambda r: None), [])
        self.assert_rejected(lambda r: r['checks'][3].update(verdict='pass', property=None),
                             'reproduces no known defect', obligation)
        self.assert_rejected(lambda r: r['checks'][3].update(property='Typed'),
                             'reproduces no known defect', obligation)

    def test_kani_controls_break_the_named_assertion_of_a_mutated_source(self):
        obligation = self.kani()['obligations'][0]
        self.assertEqual(self.problems(obligation, lambda r: None), [])
        self.assert_rejected(lambda r: r['checks'][1].update(failed_checks=['"another assertion"']),
                             'does not meet negative-control', obligation)
        self.assert_rejected(lambda r: r['checks'][1].pop('mutated'), 'mutated', obligation)
        self.assert_rejected(lambda r: r['checks'][0].update(covers_satisfied=1),
                             'every cover satisfied', obligation)
        self.assert_rejected(lambda r: r['checks'][0].pop('failed_checks'), 'lacks failed_checks',
                             obligation)

    def write(self, name, text):
        (self.root / 'verification/receipts').mkdir(exist_ok=True)
        (self.root / f'verification/receipts/{name}').write_text(text)

    def test_the_report_covers_missing_malformed_and_orphaned_receipts(self):
        claimed, unchecked = self.tla(), dict(self.tla('implemented-unchecked'), id='U')
        manifest = {'obligations': [claimed, unchecked]}
        problems, _ = formal.receipt_report(manifest)
        self.assertEqual(problems, ['T: status pass-with-recorded-scope has no receipt'])
        self.write('T.json', '{"schema": 2, ')
        self.write('Z.json', json.dumps(self.receipt(claimed)))
        problems, _ = formal.receipt_report(manifest)
        self.assertTrue(any(p.startswith('T: receipt is not JSON') for p in problems), problems)
        self.assertIn('Z.json: receipt for no implemented obligation', problems)
        self.write('T.json', json.dumps(self.receipt(claimed)))
        self.write('U.json', json.dumps(dict(self.receipt(unchecked), checks=[])))
        problems, _ = formal.receipt_report({'obligations': [claimed, unchecked]})
        self.assertTrue(any(p.startswith('U: receipt omits') for p in problems), problems)


def surviving(pid, within=5.0):
    """Whether `pid` is still alive after `within` seconds of waiting for it to go."""
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        time.sleep(0.05)
    return True


class TlcIsolation(unittest.TestCase):
    """Case 6: TLC runs share no scratch space, and no check outlives its run."""

    CHECK = {'id': 'fixture/baseline', 'role': 'baseline', 'spec': f'{formal.FIXTURES}/Counter.tla',
             'config': f'{formal.FIXTURES}/Counter.cfg', 'expect': {'result': 'pass'}}
    # A check whose verifier leaves a child behind in its process group.
    TREE = ['sh', '-c', 'sleep 30 >/dev/null 2>&1 & echo $! > child.pid; wait']

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.cwd = Path(self.directory.name)

    def child(self):
        for _ in range(100):
            text = (self.cwd / 'child.pid').read_text().strip() if (self.cwd / 'child.pid').exists() else ''
            if text:
                pid = int(text)
                self.addCleanup(_kill_quietly, pid)
                return pid
            time.sleep(0.05)
        self.fail('the fake verifier never started its child')

    def test_6_each_tlc_run_gets_its_own_java_tmpdir_inside_its_metadir(self):
        command = formal.tlc_command(self.CHECK, '/scratch/meta-a')
        self.assertIn('-Djava.io.tmpdir=/scratch/meta-a/tmp', command)
        self.assertLess(command.index('-Djava.io.tmpdir=/scratch/meta-a/tmp'), command.index('tlc2.TLC'))
        self.assertNotEqual(formal.tlc_command(self.CHECK, '/scratch/meta-b'), command)

    def test_6_the_tmpdir_exists_before_launch_and_goes_with_the_run(self):
        seen = []

        def run_process(command, cwd, timeout, env=None):
            tmp = Path(next(a for a in command if a.startswith('-Djava.io.tmpdir=')).partition('=')[2])
            seen.append((tmp, tmp.is_dir()))
            return TLC_PASS, 0, False, 0.1

        with mock.patch.object(formal, 'run_process', run_process):
            verdict, _, _ = formal.run_tlc(self.CHECK, self.cwd)
        self.assertEqual((verdict, seen[0][1]), ('pass', True))
        self.assertFalse(seen[0][0].parent.exists())

    def test_6_a_timeout_kills_the_whole_process_tree(self):
        _, code, timed_out, _ = formal.run_process(self.TREE, self.cwd, timeout=1)
        self.assertEqual((code, timed_out), (None, True))
        self.assertFalse(surviving(self.child()), 'a verifier child outlived the timeout')

    def test_6_a_cancellation_kills_the_whole_process_tree(self):
        class Cancelled(BaseException):
            pass

        def cancel(signum, frame):
            raise Cancelled()

        previous = signal.signal(signal.SIGALRM, cancel)
        signal.setitimer(signal.ITIMER_REAL, 0.5)
        try:
            with self.assertRaises(Cancelled):
                formal.run_process(self.TREE, self.cwd, timeout=30)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
        self.assertFalse(surviving(self.child()), 'a verifier child outlived the cancellation')

    @unittest.skipUnless(shutil.which('java') and (formal.TOOLS / 'tla2tools.jar').is_file(),
                         'needs Java and the pinned tla2tools.jar (the formal job has both)')
    def test_6_concurrent_tlc_runs_each_reach_their_own_verdict(self):
        self.assertEqual(formal.concurrent_tlc_problems(self.cwd), [])
        self.assertEqual(formal.tlc_leftovers(), [])


def _kill_quietly(pid):
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGKILL)


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
