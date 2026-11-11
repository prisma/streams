import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import shutil
import unittest
from unittest import mock

import mutation_driver
from mutation_owners import MutationOwner


OWNERS = tuple(
    MutationOwner(name, (f'src/{name}.rs',), (f'{name}::',))
    for name in ('alpha', 'beta', 'gamma')
)

CARGO = '/home/runner/.cargo/bin/cargo test --profile=quality --verbose --package=streams-slate@0.1.0'
TEST_BINARY = "prlimit --data=10737418240:10737418240 -- /t/{name}-0123 '{name}::'"
PANIC = ("test {name}::works ... FAILED\n\nfailures:\n\n---- {name}::works stdout ----\n"
         "thread '{name}::works' panicked at src/{name}.rs:9:5:\n")
CARGO_FAILED = ('error: test failed, to rerun pass `-p streams-slate --lib`\n\nCaused by:\n'
                f"  process didn't exit successfully: `{TEST_BINARY}` ({{ending}})\n")
ABORTED = 'signal: 6, SIGABRT: process abort signal'
EXITED = 'exit status: 101'
FAILED_RESULT = 'test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out\n\n'
FIXTURES = Path(__file__).with_name('fixtures') / 'mutation-outcomes'

# How a test phase ends, by kind of catch: the tests' output, cargo's account
# of the test binary, its process_status in outcomes.json, the log's result
# line and the mutant's summary. A test binary that died by a signal still
# makes cargo exit 101, so cargo-mutants counts the mutant caught.
TEST_ENDINGS = {
    # An assertion failed.
    'caught': (PANIC + 'assertion failed: ok\n\n' + FAILED_RESULT, EXITED,
               {'Failure': 101}, 'Failure(101)', 'CaughtMutant'),
    # The test binary aborted on a failed allocation, as under the memory cap.
    'signalled': ('memory allocation of 1073741824 bytes failed\n', ABORTED,
                  {'Failure': 101}, 'Failure(101)', 'CaughtMutant'),
    # The test binary overflowed its stack.
    'overflowed': ("\nthread '{name}::works' has overflowed its stack\n"
                   'fatal runtime error: stack overflow\n', ABORTED,
                   {'Failure': 101}, 'Failure(101)', 'CaughtMutant'),
    # Only the allocation failure's message, as when the process that aborted
    # is a child the test spawned, not the one cargo waited on.
    'allocation-failed': (PANIC + 'helper failed: memory allocation of 1073741824 bytes failed\n\n'
                          + FAILED_RESULT, EXITED, {'Failure': 101}, 'Failure(101)', 'CaughtMutant'),
    # cargo itself died by a signal: cargo-mutants cannot classify the mutant,
    # only warns, counts it nowhere and answers 0.
    'unclassified': ('', None, {'Signalled': 9}, 'Signalled(9)', 'Failure'),
    # The tests passed: the mutant survived.
    'missed': ('test result: ok. 4 passed; 0 failed\n', None, 'Success', 'Success', 'MissedMutant'),
    # The test phase ran into cargo-mutants' timeout.
    'timeout': ('', None, 'Timeout', 'Timeout', 'Timeout'),
}


def mutant_outcome(listing, name, line, kind):
    """One mutant's outcomes.json entry and log, as cargo-mutants 27.1.0 writes
    them (src/outcome.rs, src/mutant.rs, src/output.rs). The mutation diff and
    the build's output come before the test phase in the log; the diff shows a
    parameter named `signal`, as src/tasks/signal.rs has one."""
    label = f'src/{name}.rs:{line}:1: replace {name} with ()'
    log_path = f'log/src__{name}.rs_line_{line}_col_1.log'
    output, ending, status, result, summary = TEST_ENDINGS[kind]
    tests = (output + (CARGO_FAILED if ending else '')).format(name=name, ending=ending)
    (listing / log_path).write_text(
        f'\n*** {label}\n\n*** mutation diff:\n--- src/{name}.rs\n+++ replace {name} with ()\n'
        f'@@ -{line},3 +{line},3 @@\n fn {name}(signal: i32) -> u32 {{\n-    signal as u32\n'
        f'+    ()\n }}\n\n*** {CARGO} --no-run --lib\n   Compiling streams-slate v0.1.0\n'
        f'\n*** result: Success\n\n*** {CARGO} --lib {name}::\n'
        f'     Running `{TEST_BINARY.format(name=name)}`\nrunning 4 tests\n'
        f'{tests}\n*** result: {result}\n')
    return {
        'scenario': {'Mutant': {
            'name': label, 'package': 'streams-slate', 'file': f'src/{name}.rs',
            'function': None,
            'span': {'start': {'line': line, 'column': 1}, 'end': {'line': line, 'column': 5}},
            'replacement': '()', 'genre': 'FnValue',
        }},
        'summary': summary,
        'log_path': log_path,
        'diff_path': f'diff/src__{name}.rs_line_{line}_col_1.diff',
        'phase_results': [
            {'phase': 'Build', 'duration': 1.0, 'process_status': 'Success',
             'argv': [*CARGO.split(), '--no-run', '--lib']},
            {'phase': 'Test', 'duration': 1.0, 'process_status': status,
             'argv': [*CARGO.split(), '--lib', f'{name}::']},
        ],
    }


def write_outcomes(listing, name, kind, total=2):
    """The owner's outcomes.json: the baseline passed, its first mutant ended
    as `kind` and its second was caught by an assertion; `total` is the
    number of mutants it says it recorded."""
    (listing / 'log').mkdir(parents=True, exist_ok=True)
    baseline = {'scenario': 'Baseline', 'summary': 'Success', 'log_path': 'log/baseline.log',
                'diff_path': None, 'phase_results': []}
    outcomes = [baseline, mutant_outcome(listing, name, 1, kind),
                mutant_outcome(listing, name, 2, 'caught')]
    (listing / 'outcomes.json').write_text(json.dumps({'outcomes': outcomes, 'total_mutants': total}))


def fixture_outcome(listing, fixture, summary, line=1):
    """An outcome whose log is a trimmed excerpt of a real cargo-mutants log
    (fixtures/mutation-outcomes, README.md there)."""
    (listing / 'log').mkdir(parents=True, exist_ok=True)
    log_path = f'log/{fixture}'
    shutil.copy(FIXTURES / fixture, listing / log_path)
    return {'scenario': {'Mutant': {'name': f'{fixture}:{line}'}}, 'summary': summary,
            'log_path': log_path}


class FakeCargo:
    """Stands in for the planner, git and cargo-mutants: every owner lists two
    mutants, and each owner's run answers the exit status the test assigns, or
    writes the outcomes of a run that answered 0 when it assigns a kind of
    catch from TEST_ENDINGS."""

    def __init__(self, out, plan, statuses):
        self.out = out
        self.plan = plan
        self.statuses = statuses
        self.ran = []
        self.commands = []
        self.envs = []

    def __call__(self, command, cwd=None, stdout=None, check=False, **kwargs):
        self.commands.append(command)
        if 'scripts/quality/verification_plan.py' in command:
            (self.out / 'plan.json').write_text(json.dumps(self.plan))
            return subprocess.CompletedProcess(command, 0)
        if command[:2] == ['git', 'diff']:
            return subprocess.CompletedProcess(command, 0)
        if command[:3] == ['cargo', 'mutants', '--list']:
            source = command[command.index('--file') + 1]
            stdout.write(json.dumps([
                {'file': source, 'function': None, 'span': [line], 'replacement': '()',
                 'genre': 'FnValue'}
                for line in (1, 2)
            ]))
            return subprocess.CompletedProcess(command, 0)
        if command[:2] == ['cargo', 'mutants']:
            name = Path(command[command.index('--output') + 1]).name
            self.ran.append(name)
            self.envs.append(kwargs.get('env'))
            status = self.statuses.get(name, 0)
            listing = self.out / name / 'mutants.out'
            if status == 'short':  # records one mutant fewer than were listed
                write_outcomes(listing, name, 'caught', total=1)
                status = 0
            elif status in TEST_ENDINGS:
                write_outcomes(listing, name, status)
                status = 0
            elif status in (0, 2, 3):
                write_outcomes(listing, name, {0: 'caught', 2: 'missed', 3: 'timeout'}[status])
            if status in (2, 3):
                kind = 'missed.txt' if status == 2 else 'timeout.txt'
                (listing / kind).write_text(f'src/{name}.rs:1:1: replace {name} with ()\n')
            if check and status:
                raise subprocess.CalledProcessError(status, command)
            return subprocess.CompletedProcess(command, status)
        raise AssertionError(f'unexpected command: {command}')


def run_driver(plan, statuses, shard='', intercept=None):
    """Run the driver against FakeCargo; `intercept(command, **kwargs)` may
    answer a command first (None: FakeCargo answers)."""
    with tempfile.TemporaryDirectory() as directory:
        out = Path(directory)
        fake = FakeCargo(out, plan, statuses)
        printed = io.StringIO()

        def run(command, **kwargs):
            answer = intercept(command, **kwargs) if intercept else None
            return fake(command, **kwargs) if answer is None else answer

        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(mutation_driver.subprocess, 'run', run))
            stack.enter_context(mock.patch.object(mutation_driver, 'validate_sources'))
            stack.enter_context(mock.patch.object(
                mutation_driver, 'validate_plan', return_value=OWNERS))
            stack.enter_context(mock.patch.object(mutation_driver, 'check_tool_version'))
            stack.enter_context(mock.patch.object(mutation_driver, 'make_harness_diff'))
            stack.enter_context(mock.patch.dict(os.environ, {'QUALITY_MUTANT_SHARD': shard}))
            stack.enter_context(contextlib.redirect_stdout(printed))
            try:
                mutation_driver.execute(out)
                outcome = None
            except (SystemExit, subprocess.CalledProcessError) as error:
                outcome = error
        return fake, outcome, printed.getvalue()


def owner_commands(plan, shard):
    """Each owner's cargo-mutants commands, [listing, run], keyed by the owner
    whose source they name."""
    fake, _, _ = run_driver(plan, {}, shard)
    commands = {}
    for command in fake.commands:
        if command[:2] == ['cargo', 'mutants']:
            name = Path(command[command.index('--file') + 1]).stem
            commands.setdefault(name, []).append(command)
    return commands


SCHEDULED = {'selection_kind': 'scheduled-owner-rotation', 'schedule_slot': 3}
PUSHED = {'selection_kind': 'changed-tree', 'comparison_revision': 'abc123'}


class ScheduledRotation(unittest.TestCase):
    def test_every_owner_runs_and_the_night_fails_once_with_every_survivor(self):
        fake, outcome, printed = run_driver(SCHEDULED, {'alpha': 2, 'gamma': 3})
        self.assertEqual(fake.ran, ['alpha', 'beta', 'gamma'])
        self.assertIsInstance(outcome, SystemExit)
        self.assertEqual(outcome.code, 1)
        summary = printed[printed.index('Mutation survivors'):]
        self.assertIn('2 of 3 owner(s)', summary)
        self.assertIn('alpha: missed', summary)
        self.assertIn('gamma: timeout', summary)
        self.assertNotIn('beta', summary)
        self.assertIn('alpha/mutants.out/missed.txt', summary)
        self.assertIn('gamma/mutants.out/timeout.txt', summary)
        self.assertIn('src/alpha.rs:1:1: replace alpha with ()', summary)
        self.assertIn('src/gamma.rs:1:1: replace gamma with ()', summary)

    def test_a_clean_night_passes(self):
        fake, outcome, printed = run_driver(SCHEDULED, {})
        self.assertEqual(fake.ran, ['alpha', 'beta', 'gamma'])
        self.assertIsNone(outcome)
        self.assertNotIn('Mutation survivors', printed)

    def test_a_caught_mutant_whose_tests_died_by_a_signal_is_a_survivor(self):
        for kind in ('signalled', 'overflowed', 'allocation-failed'):
            with self.subTest(kind=kind):
                fake, outcome, printed = run_driver(SCHEDULED, {'beta': kind})
                self.assertEqual(fake.ran, ['alpha', 'beta', 'gamma'])
                self.assertIsInstance(outcome, SystemExit)
                self.assertEqual(outcome.code, 1)
                summary = printed[printed.index('Mutation survivors'):]
                self.assertIn('1 of 3 owner(s)', summary)
                self.assertIn('beta: incomplete 1', summary)
                self.assertIn('beta/mutants.out/incomplete.txt', summary)
                self.assertIn('    src/beta.rs:1:1: replace beta with ()\n', summary)
                self.assertNotIn('src/beta.rs:2:1', summary)

    def test_a_mutant_cargo_mutants_could_not_classify_is_a_survivor(self):
        fake, outcome, printed = run_driver(SCHEDULED, {'gamma': 'unclassified'})
        self.assertEqual(fake.ran, ['alpha', 'beta', 'gamma'])
        self.assertIsInstance(outcome, SystemExit)
        self.assertEqual(outcome.code, 1)
        summary = printed[printed.index('Mutation survivors'):]
        self.assertIn('1 of 3 owner(s)', summary)
        self.assertIn('gamma: incomplete 1', summary)
        self.assertIn('    src/gamma.rs:1:1: replace gamma with ()\n', summary)
        self.assertNotIn('src/gamma.rs:2:1', summary)

    def test_a_catch_by_an_assertion_stays_caught_whatever_its_diff_shows(self):
        # The diff shows `fn beta(signal: i32)`: only the test phase is read.
        fake, outcome, printed = run_driver(SCHEDULED, {'beta': 'caught'})
        self.assertEqual(fake.ran, ['alpha', 'beta', 'gamma'])
        self.assertIsNone(outcome)
        self.assertNotIn('Mutation survivors', printed)

    def test_a_status_that_measured_nothing_stops_the_night_at_once(self):
        # 1 usage or error, 4 a failed baseline, 70 internal, -9 a signal:
        # none of them is a survivor, so the remaining owners do not run.
        for status in (1, 4, 70, -9):
            with self.subTest(status=status):
                fake, outcome, printed = run_driver(SCHEDULED, {'alpha': 2, 'beta': status})
                self.assertEqual(fake.ran, ['alpha', 'beta'])
                self.assertIsInstance(outcome, subprocess.CalledProcessError)
                self.assertEqual(outcome.returncode, status)
                self.assertNotIn('Mutation survivors', printed)

    def test_a_split_owner_lists_and_runs_only_its_nights_part_of_this_runners_share(self):
        plan = {**SCHEDULED, 'scheduled_owner_shares': {'beta': [1, 2]}}
        commands = owner_commands(plan, '3/4')
        self.assertEqual(sorted(commands), ['alpha', 'beta', 'gamma'])
        for name, dealt in (('alpha', '3/4'), ('beta', '7/8'), ('gamma', '3/4')):
            listing, run = commands[name]
            self.assertIn('--list', listing)
            self.assertNotIn('--list', run)
            for command in (listing, run):
                self.assertEqual(command[-4:], ['--shard', dealt, '--sharding', 'round-robin'])


class VerdictEvidence(unittest.TestCase):
    """A verdict needs positive evidence (gate decision of 2026-11-11, H1),
    replayed on trimmed excerpts of real logs."""

    def reason(self, fixture, summary):
        with tempfile.TemporaryDirectory() as directory:
            listing = Path(directory)
            return mutation_driver.incomplete_reason(
                listing, fixture_outcome(listing, fixture, summary))

    def test_a_build_tree_without_a_members_sources_is_no_evidence_of_either_verdict(self):
        # billing_ops.rs:86 and :33 at df849d2a0: the temp copy lost
        # tools/quality-syntax/src mid-run.
        for fixture, summary in (('unviable-manifest-error.log', 'Unviable'),
                                 ('caught-manifest-error.log', 'CaughtMutant')):
            with self.subTest(fixture=fixture):
                self.assertEqual(self.reason(fixture, summary),
                                 'environment: failed to load manifest')

    def test_a_parse_error_without_an_error_code_stays_unviable(self):
        # maintenance.rs:20:13: `error: \`||\` operators are not supported...`
        self.assertIsNone(self.reason('unviable-parse-error.log', 'Unviable'))

    def test_a_catch_whose_tests_panicked_with_not_found_stays_caught(self):
        self.assertIsNone(self.reason('caught-not-found-panic.log', 'CaughtMutant'))

    def test_a_signature_in_a_rustc_snippet_line_does_not_count(self):
        log = ('\n*** src/tasks/signal.rs:12:5: replace deliver_to_helper with ()\n\n'
               f'*** {CARGO} --no-run --lib\n'
               'error[E0308]: mismatched types\n'
               '  --> src/tasks/signal.rs:12:5\n'
               "12 |     fn deliver_to_helper(signal: libc::c_int) { open(PATH) } // (os error 2)\n"
               "   |     process didn't exit successfully: `rustc` (signal: 9, SIGKILL)\n"
               "error: could not compile `streams-slate` (lib test) due to 1 previous error\n"
               '\n*** result: Failure(101)\n')
        with tempfile.TemporaryDirectory() as directory:
            listing = Path(directory)
            (listing / 'log').mkdir()
            (listing / 'log' / 'signal.log').write_text(log)
            outcome = {'scenario': {'Mutant': {'name': 'signal'}}, 'summary': 'Unviable',
                       'log_path': 'log/signal.log'}
            self.assertIsNone(mutation_driver.incomplete_reason(listing, outcome))

    def test_an_unviable_mutant_the_compiler_never_rejected_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            listing = Path(directory)
            outcome = fixture_outcome(listing, 'caught-not-found-panic.log', 'Unviable')
            self.assertEqual(mutation_driver.incomplete_reason(listing, outcome),
                             'no compiler error')

    def test_a_catch_that_ran_no_test_to_a_failed_result_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            listing = Path(directory)
            outcome = fixture_outcome(listing, 'unviable-parse-error.log', 'CaughtMutant')
            self.assertEqual(mutation_driver.incomplete_reason(listing, outcome),
                             'no failed test result')

    def test_a_broken_build_tree_fails_the_push_and_names_each_mutant_as_its_owner_ends(self):
        def broken(command, **kwargs):
            if command[:2] == ['cargo', 'mutants'] and '--list' not in command \
                    and Path(command[command.index('--output') + 1]).name == 'beta':
                listing = Path(command[command.index('--output') + 1]) / 'mutants.out'
                outcomes = [fixture_outcome(listing, 'unviable-manifest-error.log', 'Unviable', 1),
                            fixture_outcome(listing, 'caught-manifest-error.log', 'CaughtMutant', 2)]
                (listing / 'outcomes.json').write_text(
                    json.dumps({'outcomes': outcomes, 'total_mutants': 2}))
                return subprocess.CompletedProcess(command, 0)
            return None
        fake, outcome, printed = run_driver(PUSHED, {}, intercept=broken)
        self.assertEqual(fake.ran, ['alpha', 'gamma'])
        self.assertIsInstance(outcome, SystemExit)
        self.assertEqual(outcome.code, 1)
        self.assertIn('INCOMPLETE unviable-manifest-error.log:1 (environment: failed to load '
                      'manifest) [', printed)
        self.assertIn('INCOMPLETE caught-manifest-error.log:2 (environment: failed to load '
                      'manifest) [', printed)
        summary = printed[printed.index('Mutation survivors'):]
        self.assertIn('1 of 3 owner(s)', summary)
        self.assertIn('beta: incomplete 2', summary)

    def test_an_owner_recording_other_than_its_listed_mutants_is_a_survivor(self):
        fake, outcome, printed = run_driver(PUSHED, {'gamma': 'short'})
        self.assertEqual(fake.ran, ['alpha', 'beta', 'gamma'])
        self.assertIsInstance(outcome, SystemExit)
        summary = printed[printed.index('Mutation survivors'):]
        self.assertIn('1 of 3 owner(s)', summary)
        self.assertIn('gamma: incomplete 1', summary)
        self.assertIn('outcomes.json records 1 mutant(s); the driver listed 2', summary)


class PushedChange(unittest.TestCase):
    def test_every_selected_owner_runs_and_the_push_fails_once_with_every_survivor(self):
        fake, outcome, printed = run_driver(PUSHED, {'alpha': 2, 'gamma': 3})
        self.assertEqual(fake.ran, ['alpha', 'beta', 'gamma'])
        self.assertIsInstance(outcome, SystemExit)
        self.assertEqual(outcome.code, 1)
        summary = printed[printed.index('Mutation survivors'):]
        self.assertIn('2 of 3 owner(s)', summary)
        self.assertIn('alpha: missed', summary)
        self.assertIn('gamma: timeout', summary)
        self.assertNotIn('beta', summary)
        self.assertIn('alpha/mutants.out/missed.txt', summary)
        self.assertIn('gamma/mutants.out/timeout.txt', summary)
        self.assertIn('src/alpha.rs:1:1: replace alpha with ()', summary)
        self.assertIn('src/gamma.rs:1:1: replace gamma with ()', summary)

    def test_a_signalled_catch_fails_the_push_though_cargo_mutants_answered_0(self):
        fake, outcome, printed = run_driver(PUSHED, {'alpha': 'signalled', 'gamma': 2})
        self.assertEqual(fake.ran, ['alpha', 'beta', 'gamma'])
        self.assertIsInstance(outcome, SystemExit)
        self.assertEqual(outcome.code, 1)
        summary = printed[printed.index('Mutation survivors'):]
        self.assertIn('2 of 3 owner(s)', summary)
        self.assertIn('alpha: incomplete 1', summary)
        self.assertIn('alpha/mutants.out/incomplete.txt', summary)
        self.assertIn('    src/alpha.rs:1:1: replace alpha with ()\n', summary)
        self.assertIn('gamma: missed 1', summary)
        self.assertNotIn('beta', summary)

    def test_a_status_that_measured_nothing_stops_the_push_at_once(self):
        # 1 usage or error, 4 a failed baseline, 70 internal, -9 a signal:
        # none of them is a survivor, so the remaining owners do not run.
        for status in (1, 4, 70, -9):
            with self.subTest(status=status):
                fake, outcome, printed = run_driver(PUSHED, {'alpha': 2, 'beta': status})
                self.assertEqual(fake.ran, ['alpha', 'beta'])
                self.assertIsInstance(outcome, subprocess.CalledProcessError)
                self.assertEqual(outcome.returncode, status)
                self.assertNotIn('Mutation survivors', printed)

    def test_a_pushed_plan_never_reads_a_share(self):
        plan = {**PUSHED, 'scheduled_owner_shares': {'beta': [1, 2]}}
        commands = owner_commands(plan, '3/4')
        self.assertEqual(commands['beta'][1][-4:], ['--shard', '3/4', '--sharding', 'round-robin'])


if __name__ == '__main__':
    unittest.main()
