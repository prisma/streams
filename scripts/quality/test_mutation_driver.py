import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import mutation_driver
from mutation_owners import MutationOwner


OWNERS = tuple(
    MutationOwner(name, (f'src/{name}.rs',), (f'{name}::',))
    for name in ('alpha', 'beta', 'gamma')
)


class FakeCargo:
    """Stands in for the planner, git and cargo-mutants: every owner lists two
    mutants, and each owner's run answers the exit status the test assigns."""

    def __init__(self, out, plan, statuses):
        self.out = out
        self.plan = plan
        self.statuses = statuses
        self.ran = []
        self.commands = []

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
            status = self.statuses.get(name, 0)
            if status in (2, 3):
                listing = self.out / name / 'mutants.out'
                listing.mkdir(parents=True, exist_ok=True)
                kind = 'missed.txt' if status == 2 else 'timeout.txt'
                (listing / kind).write_text(f'src/{name}.rs:1:1: replace {name} with ()\n')
            if check and status:
                raise subprocess.CalledProcessError(status, command)
            return subprocess.CompletedProcess(command, status)
        raise AssertionError(f'unexpected command: {command}')


def run_driver(plan, statuses, shard=''):
    with tempfile.TemporaryDirectory() as directory:
        out = Path(directory)
        fake = FakeCargo(out, plan, statuses)
        printed = io.StringIO()
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(mutation_driver.subprocess, 'run', fake))
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


class PushedChange(unittest.TestCase):
    def test_the_first_owner_with_survivors_stops_the_leg(self):
        fake, outcome, printed = run_driver(PUSHED, {'alpha': 2, 'gamma': 3})
        self.assertEqual(fake.ran, ['alpha'])
        self.assertIsInstance(outcome, subprocess.CalledProcessError)
        self.assertEqual(outcome.returncode, 2)
        self.assertNotIn('Mutation survivors', printed)

    def test_a_pushed_plan_never_reads_a_share(self):
        plan = {**PUSHED, 'scheduled_owner_shares': {'beta': [1, 2]}}
        commands = owner_commands(plan, '3/4')
        self.assertEqual(commands['beta'][1][-4:], ['--shard', '3/4', '--sharding', 'round-robin'])


if __name__ == '__main__':
    unittest.main()
