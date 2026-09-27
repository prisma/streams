"""formal_batch schedules longest-first within its budgets, refuses dirty inputs,
and copies back only receipts recorded for the live inputs. Nothing here runs
Kani or TLC: drivers are fakes and git runs only in temporary repositories."""
import contextlib
import io
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import formal_batch  # noqa: E402
from formal_batch import Job, formal, formal_shards  # noqa: E402

MANIFEST = formal.load()
ALL_IDS = sorted(o['id'] for o in MANIFEST['obligations'])
INF = math.inf


def job(oid, seconds, gib=1.0, kind='kani'):
    return Job(oid, kind, float(seconds), float(gib))


def run_main(*argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            code = formal_batch.main(list(argv))
        except SystemExit as exit_:
            code = exit_.code
    return code, out.getvalue(), err.getvalue()


class Schedule(unittest.TestCase):
    def test_a_hand_checked_longest_first_schedule(self):
        jobs = [job('a', 7), job('b', 5), job('c', 4), job('d', 3), job('e', 1)]
        rows, span = formal_batch.simulate(jobs, 2, INF)
        self.assertEqual([(j.id, lane, start, end) for j, lane, start, end in rows],
                         [('a', 0, 0, 7), ('b', 1, 0, 5), ('c', 1, 5, 9), ('d', 0, 7, 10),
                          ('e', 1, 9, 10)])
        self.assertEqual(span, 10)

    def test_unbounded_memory_gives_the_ci_lpt_makespan_on_real_receipts(self):
        jobs = formal_batch.jobs_for(MANIFEST, ALL_IDS)
        work = formal_shards.weights(ALL_IDS)
        for slots in range(1, 9):
            span = formal_batch.simulate(jobs, slots, INF)[1]
            self.assertAlmostEqual(span, formal_shards.makespan(formal_shards.lpt(work, slots), work),
                                   places=6)

    def test_jobs_start_in_lpt_order_and_never_exceed_the_slots(self):
        jobs = formal_batch.jobs_for(MANIFEST, ALL_IDS)
        self.assertEqual([j.id for j in jobs], formal_shards.lpt_order(formal_shards.weights(ALL_IDS)))
        rows, span = formal_batch.simulate(jobs, 6, formal_batch.DEFAULT_MEM_GIB)
        self.assertEqual([j.id for j, *_ in rows], [j.id for j in jobs])
        for _, _, start, _ in rows:
            running = [r for r in rows if r[2] <= start < r[3]]
            self.assertLessEqual(len(running), 6)
            self.assertEqual(len({lane for _, lane, _, _ in running}), len(running))
        self.assertEqual(span, max(end for *_, end in rows))

    def test_an_empty_schedule(self):
        self.assertEqual(formal_batch.simulate([], 6, 48), ([], 0.0))


class Admission(unittest.TestCase):
    def test_the_slot_count_bounds_concurrency(self):
        pending = [job(c, 1) for c in 'abcde']
        self.assertEqual(formal_batch.admissions(pending, [], 3, INF), pending[:3])
        self.assertEqual(formal_batch.admissions(pending, pending[:2], 3, INF), pending[:1])

    def test_memory_blocks_the_head_and_nothing_overtakes_it(self):
        big, small = job('big', 9, gib=6), job('small', 1, gib=3)
        self.assertEqual(formal_batch.admissions([big, small], [job('r', 5, gib=4)], 6, 8), [])
        self.assertEqual(formal_batch.admissions([big, small], [], 6, 9), [big, small])

    def test_a_job_larger_than_the_budget_runs_alone(self):
        huge = job('huge', 5, gib=10)
        self.assertEqual(formal_batch.admissions([huge], [], 6, 8), [huge])
        self.assertEqual(formal_batch.admissions([huge], [job('r', 1)], 6, 8), [])
        rows, span = formal_batch.simulate([huge, job('x', 1, gib=1)], 6, 8)
        self.assertEqual([(j.id, s) for j, _, s, _ in rows], [('huge', 0), ('x', 5)])
        self.assertEqual(span, 6)

    def test_the_simulated_real_batch_stays_within_a_tight_budget(self):
        jobs = formal_batch.jobs_for(MANIFEST, ALL_IDS)
        rows, _ = formal_batch.simulate(jobs, 6, 10)
        for _, _, start, _ in rows:
            used = sum(j.gib for j, _, s, e in rows if s <= start < e)
            self.assertLessEqual(used, 10)

    def test_memory_estimates(self):
        tla = {'id': 'TLA-900', 'kind': 'tla', 'checks': [{}, {'heap': '2048m'}]}
        self.assertEqual(formal_batch.mem_gib(tla), 4.0)
        self.assertEqual(formal_batch.mem_gib(dict(tla, checks=[{'heap': '6g'}, {}])), 7.0)
        self.assertEqual(formal_batch.mem_gib({'id': 'KANI-001', 'kind': 'kani', 'checks': []}), 6.0)
        self.assertEqual(formal_batch.mem_gib({'id': 'KANI-040', 'kind': 'kani', 'checks': []}), 3.0)


class FakeRevision:
    def __init__(self, entries=None, ledger=None, pins=None):
        self._entries, self._ledger, self._pins = entries or {}, ledger or {}, pins

    def entries(self):
        return self._entries

    def ledger(self):
        return self._ledger

    def pins(self):
        return self._pins


A = {'id': 'KANI-900', 'kind': 'kani', 'status': 'pass-with-recorded-scope', 'source_paths': ['src/a.rs'],
     'verification_paths': ['src/a/proofs.rs'], 'assumptions': ['ASM-X'], 'checks': []}
B = {'id': 'TLA-900', 'kind': 'tla', 'status': 'pass-with-recorded-scope', 'source_paths': ['src/b.rs'],
     'verification_paths': [], 'checks': [{'spec': 'verification/tla/b/B.tla',
                                           'config': 'verification/tla/b/B.cfg'}]}
TINY = {'obligations': [A, B]}
LEDGER = formal.ledger_path()


class InputChanges(unittest.TestCase):
    def test_files_stale_exactly_the_obligations_that_digest_them(self):
        self.assertEqual(formal_batch.input_changes(TINY, {'src/a.rs', 'README.md'}, FakeRevision()),
                         {'KANI-900': ['files:src/a.rs']})
        self.assertEqual(formal_batch.input_changes(TINY, {'build.rs'}, FakeRevision()),
                         {'KANI-900': ['files:build.rs']})
        self.assertEqual(set(formal_batch.input_changes(TINY, {'Cargo.lock'}, FakeRevision())),
                         {'KANI-900', 'TLA-900'})

    def test_a_manifest_edit_stales_only_the_entries_that_differ(self):
        before = FakeRevision(entries={'KANI-900': dict(A), 'TLA-900': dict(B, title='old')})
        self.assertEqual(formal_batch.input_changes(TINY, {formal_batch.MANIFEST_PATH}, before),
                         {'TLA-900': ['manifest_entry']})
        self.assertEqual(formal_batch.input_changes(TINY, {formal_batch.MANIFEST_PATH},
                                                    FakeRevision(entries={'KANI-900': dict(A)})),
                         {'TLA-900': ['manifest_entry (new obligation)']})

    def test_a_ledger_edit_stales_only_obligations_naming_a_changed_entry(self):
        with mock.patch.object(formal, 'assumption_entries', return_value={'ASM-X': 'new text'}):
            moved = formal_batch.input_changes(TINY, {LEDGER}, FakeRevision(ledger={'ASM-X': 'old'}))
            same = formal_batch.input_changes(TINY, {LEDGER}, FakeRevision(ledger={'ASM-X': 'new text'}))
        self.assertEqual(moved, {'KANI-900': ['assumptions:ASM-X']})
        self.assertEqual(same, {})

    def test_a_pins_edit_stales_everything_only_when_the_formal_pins_moved(self):
        pins = formal.pinned()
        self.assertEqual(formal_batch.input_changes(TINY, {'quality-tools.toml'},
                                                    FakeRevision(pins=pins)), {})
        moved = formal_batch.input_changes(TINY, {'quality-tools.toml'},
                                           FakeRevision(pins=dict(pins, slatedb={'rev': 'old'})))
        self.assertEqual(moved, {'KANI-900': ['pins'], 'TLA-900': ['pins']})

    def test_a_hypothetical_edit_marks_what_it_cannot_know(self):
        changed = {formal_batch.MANIFEST_PATH, LEDGER, 'quality-tools.toml'}
        self.assertEqual(formal_batch.input_changes(TINY, changed),
                         {'KANI-900': ['assumptions?', 'manifest_entry?', 'pins?'],
                          'TLA-900': ['manifest_entry?', 'pins?']})


class Refusal(unittest.TestCase):
    """rerecord refuses when the live inputs of a selected obligation differ from REV."""

    def rerecord(self, changed, *extra):
        source = next(o for o in MANIFEST['obligations'] if o['id'] == 'KANI-003')['source_paths'][0]
        with mock.patch.object(formal, 'changed_since', return_value={source} if changed else set()), \
                mock.patch.object(formal_batch, 'Revision', return_value=FakeRevision()), \
                mock.patch.object(formal_batch, 'resolve', return_value='f' * 40), \
                mock.patch.object(formal, 'tool_problems', return_value=([], {})), \
                mock.patch.object(formal_batch, 'prepare_worktree',
                                  side_effect=AssertionError('a dry run touched the worktree')):
            return run_main('rerecord', '--ids', 'KANI-003', '--dry-run', *extra)

    def test_dirty_inputs_are_refused(self):
        code, out, err = self.rerecord(True)
        self.assertEqual(code, 2)
        self.assertIn('KANI-003: live inputs differ from ffffffffffff (files:', out)
        self.assertIn('FORMAL_BATCH_REFUSED', err)
        self.assertNotIn('formal.py run', out)

    def test_allow_dirty_explains_that_the_receipt_records_the_committed_revision(self):
        code, out, _ = self.rerecord(True, '--allow-dirty')
        self.assertEqual(code, 0)
        self.assertIn('each receipt records ffffffffffff as committed', out)
        self.assertIn('formal.py run --id KANI-003 --record --out target/formal/rerecord/KANI-003', out)

    def test_clean_inputs_proceed_without_comment(self):
        code, out, err = self.rerecord(False)
        self.assertEqual((code, err), (0, ''))
        self.assertNotIn('differ', out)
        self.assertNotIn('--allow-dirty', out)


def recorded_outcome(directory, oid, receipt, code=0):
    path = Path(directory) / f'{oid}-recorded.json'
    path.write_text(json.dumps(receipt))
    return formal_batch.Outcome(job(oid, 1), code, 1.0, path if code == 0 else None, 0,
                                Path(directory) / 'console.log')


class CopyBack(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.receipts = Path(self.directory.name) / 'receipts'
        self.receipts.mkdir()
        (self.receipts / 'KANI-900.json').write_text('OLD')
        self.receipt = {'id': 'KANI-900', 'inputs_sha256': 'a' * 64, 'inputs': {'files': {'x': '1'}}}

    def tearDown(self):
        self.directory.cleanup()

    def copy(self, outcome, digest, problems=(), live_entry=None):
        with mock.patch.object(formal, 'input_digest', return_value=digest), \
                mock.patch.object(formal, 'input_snapshot', return_value={'files': {'x': '2'}}), \
                mock.patch.object(formal, 'receipt_problems', return_value=list(problems)):
            return formal_batch.copy_back([outcome], {'KANI-900': A}, self.receipts, emit=lambda *a: None,
                                          live={'KANI-900': live_entry or A})

    def test_a_receipt_recorded_for_the_live_inputs_is_copied_unmodified(self):
        outcome = recorded_outcome(self.directory.name, 'KANI-900', self.receipt)
        self.assertEqual(self.copy(outcome, 'a' * 64), (['KANI-900'], {}))
        self.assertEqual((self.receipts / 'KANI-900.json').read_bytes(), outcome.recorded.read_bytes())
        self.assertEqual(sorted(p.name for p in self.receipts.iterdir()), ['KANI-900.json'])

    def test_a_manifest_entry_edited_during_the_batch_refuses_its_receipt(self):
        outcome = recorded_outcome(self.directory.name, 'KANI-900', self.receipt)
        edited = {**A, 'title': 'edited while the batch ran'}
        copied, refused = self.copy(outcome, 'a' * 64, live_entry=edited)
        self.assertEqual(copied, [])
        self.assertEqual(refused, {'KANI-900': 'NOT COPIED: the manifest entry changed during the batch'})
        self.assertEqual((self.receipts / 'KANI-900.json').read_text(), 'OLD')

    def test_a_receipt_for_other_inputs_is_not_copied(self):
        outcome = recorded_outcome(self.directory.name, 'KANI-900', self.receipt)
        copied, refused = self.copy(outcome, 'b' * 64)
        self.assertEqual(copied, [])
        self.assertEqual(refused, {'KANI-900': 'NOT COPIED: live inputs differ (files:x)'})
        self.assertEqual((self.receipts / 'KANI-900.json').read_text(), 'OLD')

    def test_a_receipt_the_live_driver_rejects_is_not_copied(self):
        outcome = recorded_outcome(self.directory.name, 'KANI-900', self.receipt)
        copied, refused = self.copy(outcome, 'a' * 64, problems=['KANI-900: receipt omits: x'])
        self.assertEqual(copied, [])
        self.assertIn('the live driver rejects it', refused['KANI-900'])
        self.assertEqual((self.receipts / 'KANI-900.json').read_text(), 'OLD')

    def test_a_garbled_recording_is_not_copied(self):
        outcome = recorded_outcome(self.directory.name, 'KANI-900', self.receipt)
        outcome.recorded.write_text('[1, 2')
        self.assertEqual(self.copy(outcome, 'a' * 64)[1],
                         {'KANI-900': 'NOT COPIED: the recorded receipt is not a JSON object'})
        self.assertEqual((self.receipts / 'KANI-900.json').read_text(), 'OLD')

    def test_failed_runs_and_unknown_obligations_copy_nothing(self):
        failed = recorded_outcome(self.directory.name, 'KANI-900', self.receipt, code=1)
        self.assertEqual(self.copy(failed, 'a' * 64), ([], {}))
        stranger = recorded_outcome(self.directory.name, 'KANI-901', dict(self.receipt, id='KANI-901'))
        self.assertIn('KANI-901', self.copy(stranger, 'a' * 64)[1])
        self.assertEqual(sorted(p.name for p in self.receipts.iterdir()), ['KANI-900.json'])


class FakeProcess:
    def __init__(self, finish_at, clock, code=0, on_exit=None):
        self.finish_at, self.clock, self.code, self.on_exit = finish_at, clock, code, on_exit
        self.pid, self.signals, self.waited = 4242, [], False

    def poll(self):
        if self.clock[0] >= self.finish_at:
            if self.on_exit:
                self.on_exit()
                self.on_exit = None
            return self.code
        return None

    def send_signal(self, signum):
        self.signals.append(signum)

    def wait(self, timeout=None):
        self.waited = True
        return self.code


class Drivers(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.worktree = Path(self.directory.name)
        (self.worktree / 'verification/receipts').mkdir(parents=True)

    def tearDown(self):
        self.directory.cleanup()

    def test_child_runs_the_documented_command_and_restores_the_committed_receipt(self):
        receipt = self.worktree / 'verification/receipts/KANI-900.json'
        receipt.write_text('COMMITTED')
        calls = []

        def popen(command, **kwargs):
            calls.append((command, kwargs))
            return FakeProcess(0, [0], on_exit=lambda: receipt.write_text('NEW'))

        child = formal_batch.Child(job('KANI-900', 5), self.worktree, {'PATH': '/bin'}, popen=popen)
        command, kwargs = calls[0]
        self.assertEqual(command, [sys.executable, 'scripts/quality/formal.py', 'run', '--id', 'KANI-900',
                                   '--record', '--out', 'target/formal/rerecord/KANI-900'])
        self.assertEqual((kwargs['cwd'], kwargs['start_new_session']), (self.worktree, True))
        self.assertEqual(child.poll(), 0)
        (child.out / 'console.log').write_text('KANI-900/a: expected pass, got pass (1.0s) ... ok\n')
        outcome = child.finish(0, 5.0)
        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.recorded.read_text(), 'NEW')
        self.assertEqual(receipt.read_text(), 'COMMITTED')

    def test_a_new_receipt_is_moved_out_and_a_failed_run_records_nothing(self):
        receipt = self.worktree / 'verification/receipts/TLA-900.json'
        popen = lambda command, **kwargs: FakeProcess(0, [0], code=1)  # noqa: E731
        child = formal_batch.Child(job('TLA-900', 5), self.worktree, {}, popen=popen)
        receipt.write_text('WRITTEN BEFORE A LATER CHECK FAILED')
        (child.out / 'console.log').write_text('TLA-900/x: expected pass, got error (1.0s) ... MISMATCH\n')
        outcome = child.finish(1, 2.0)
        self.assertFalse(outcome.ok)
        self.assertIsNone(outcome.recorded)
        self.assertEqual(outcome.mismatches, 1)
        self.assertFalse(receipt.exists())
        self.assertIn('FAILED (exit 1, 1 mismatch(es))', formal_batch.finish_text(outcome))

    def test_execute_runs_longest_first_within_the_slots(self):
        clock, starts, lines, concurrency = [0.0], [], [], []
        jobs = [job('a', 7), job('b', 5), job('c', 4), job('d', 3), job('e', 1)]
        running = set()

        class Fake:
            def __init__(self, j):
                starts.append((j.id, clock[0]))
                running.add(j.id)
                concurrency.append(len(running))
                self.job, self.process = j, FakeProcess(clock[0] + j.seconds, clock)

            def poll(self):
                return self.process.poll()

            def finish(self, code, seconds):
                running.discard(self.job.id)
                return formal_batch.Outcome(self.job, code, seconds, Path('r'), 0, Path('log'))

        def sleep(seconds):
            clock[0] += seconds

        outcomes, wall = formal_batch.execute(jobs, 2, INF, Fake, clock=lambda: clock[0], sleep=sleep,
                                              emit=lambda line, **kw: lines.append(line), poll=1.0)
        self.assertEqual(starts, [('a', 0), ('b', 0), ('c', 5), ('d', 7), ('e', 9)])
        self.assertLessEqual(max(concurrency), 2)
        self.assertEqual(sorted(o.job.id for o in outcomes), list('abcde'))
        self.assertEqual(wall, 10)
        self.assertEqual(sum(line.split()[1] == 'start' for line in lines), 5)
        self.assertEqual(sum(line.split()[1] == 'done' for line in lines), 5)

    def test_an_interrupt_stops_every_running_driver(self):
        clock, children = [0.0], []

        class Fake:
            def __init__(self, j):
                if len(children) == 2:
                    raise KeyboardInterrupt
                self.terminated = self.killed = False
                children.append(self)

            def poll(self):
                return None

            def terminate(self):
                self.terminated = True

            def wait_or_kill(self, timeout):
                self.killed = True

        kept = ['an outcome finished before the interrupt']
        with self.assertRaises(KeyboardInterrupt):
            formal_batch.execute([job(c, 9) for c in 'abc'], 6, INF, Fake, clock=lambda: clock[0],
                                 sleep=lambda s: None, emit=lambda *a, **k: None, outcomes=kept)
        self.assertEqual([(c.terminated, c.killed) for c in children], [(True, True), (True, True)])
        self.assertEqual(kept, ['an outcome finished before the interrupt'])

    def test_the_driver_environment(self):
        env = formal_batch.child_env(Path('/wt'), {'PATH': '/usr/bin', 'RUSTUP_TOOLCHAIN': '1.98.1',
                                                   'CARGO_TARGET_DIR': '/live/target', 'HOME': '/h'})
        self.assertEqual(env['PATH'], f'/wt/target/quality-tools/bin{os.pathsep}/usr/bin')
        self.assertNotIn('RUSTUP_TOOLCHAIN', env)
        self.assertNotIn('CARGO_TARGET_DIR', env)
        self.assertEqual((env['HOME'], env['PYTHONDONTWRITEBYTECODE']), ('/h', '1'))


class RepoCase(unittest.TestCase):
    """A throwaway repository with two commits; never this one."""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name).resolve() / 'repo'
        self.root.mkdir()
        self.git('init', '-q')
        (self.root / '.gitignore').write_text('target/\n')
        (self.root / 'a.txt').write_text('one\n')
        self.first = self.commit('first')
        (self.root / 'a.txt').write_text('two\n')
        self.second = self.commit('second')
        self.worktree = self.root / 'target/formal-worktree'

    def tearDown(self):
        self.directory.cleanup()

    def git(self, *args, cwd=None):
        env = dict(os.environ, GIT_AUTHOR_NAME='t', GIT_AUTHOR_EMAIL='t@t', GIT_COMMITTER_NAME='t',
                   GIT_COMMITTER_EMAIL='t@t', GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM='1')
        return subprocess.run(['git', *args], cwd=cwd or self.root, env=env, check=True, text=True,
                              capture_output=True).stdout.strip()

    def commit(self, message):
        self.git('add', '-A')
        self.git('commit', '-q', '-m', message)
        return self.git('rev-parse', 'HEAD')

    def prepare(self, rev):
        with mock.patch.dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM='1'):
            return formal_batch.prepare_worktree(rev, self.worktree, self.root)


class Worktree(RepoCase):
    def test_created_moved_and_cleaned_while_the_caches_survive(self):
        self.assertEqual(self.prepare(self.first), self.worktree)
        self.assertEqual((self.worktree / 'a.txt').read_text(), 'one\n')
        link = self.worktree / 'target/quality-tools'
        self.assertEqual(Path(os.readlink(link)), self.root / 'target/quality-tools')
        (self.worktree / 'a.txt').write_text('edited\n')
        (self.worktree / 'leftover.json').write_text('{}')
        (self.worktree / 'target/kani').mkdir()
        self.prepare(self.second)
        self.assertEqual(self.git('rev-parse', 'HEAD', cwd=self.worktree), self.second)
        self.assertEqual((self.worktree / 'a.txt').read_text(), 'two\n')
        self.assertFalse((self.worktree / 'leftover.json').exists())
        self.assertTrue((self.worktree / 'target/kani').is_dir())
        self.assertTrue(link.is_symlink())
        self.assertEqual(self.git('rev-parse', 'HEAD'), self.second)  # the live HEAD never moves
        self.assertEqual(self.git('status', '--porcelain'), '')

    def test_the_live_checkout_and_foreign_directories_are_refused(self):
        with self.assertRaisesRegex(SystemExit, 'live checkout'):
            formal_batch.prepare_worktree(self.first, self.root, self.root)
        self.worktree.mkdir(parents=True)
        (self.worktree / 'mine.txt').write_text('keep me')
        with self.assertRaisesRegex(SystemExit, 'not a worktree'):
            self.prepare(self.first)
        self.assertEqual((self.worktree / 'mine.txt').read_text(), 'keep me')


FAKE_DRIVER = """import json, os, pathlib, subprocess, sys, time
args = sys.argv[1:]
oid = args[args.index('--id') + 1]
dirty = subprocess.run(['git', 'status', '--porcelain'], capture_output=True, text=True).stdout != ''
time.sleep(0.05)
if oid == 'BAD':
    print(f'{oid}/x: expected pass, got error (0.1s) ... MISMATCH')
    sys.exit(1)
pathlib.Path('verification/receipts', oid + '.json').write_text(json.dumps(
    {'id': oid, 'inputs_sha256': 'a' * 64, 'dirty': dirty,
     'rustup': os.environ.get('RUSTUP_TOOLCHAIN'), 'out': args[args.index('--out') + 1]}))
print('FORMAL_OK')
"""


class Pipeline(RepoCase):
    """Real processes and a real worktree, with a fake driver standing in for formal.py."""

    def setUp(self):
        super().setUp()
        (self.root / 'scripts/quality').mkdir(parents=True)
        (self.root / 'scripts/quality/formal.py').write_text(FAKE_DRIVER)
        (self.root / 'verification/receipts').mkdir(parents=True)
        (self.root / 'verification/receipts/GOOD1.json').write_text('{"committed": true}')
        self.head = self.commit('fake driver')

    def test_drivers_record_in_a_clean_worktree_and_only_live_digests_are_copied(self):
        worktree = self.prepare(self.head)
        env = formal_batch.child_env(worktree, {'PATH': os.environ['PATH'], 'RUSTUP_TOOLCHAIN': '1.98.1'})
        jobs = [job('GOOD1', 3), job('BAD', 2), job('GOOD2', 1)]
        lines = []
        outcomes, wall = formal_batch.execute(jobs, 1, INF, lambda j: formal_batch.Child(j, worktree, env),
                                              emit=lambda line, **kw: lines.append(line), poll=0.02)
        by_id = {o.job.id: o for o in outcomes}
        self.assertEqual([by_id[i].ok for i in ('GOOD1', 'BAD', 'GOOD2')], [True, False, True])
        self.assertEqual(by_id['BAD'].mismatches, 1)
        for oid in ('GOOD1', 'GOOD2'):
            recorded = json.loads(by_id[oid].recorded.read_text())
            self.assertEqual((recorded['dirty'], recorded['rustup'], recorded['out']),
                             (False, None, f'target/formal/rerecord/{oid}'))
        self.assertEqual(self.git('status', '--porcelain', cwd=worktree), '')
        self.assertEqual((worktree / 'verification/receipts/GOOD1.json').read_text(), '{"committed": true}')
        self.assertEqual([line.split()[1:3] for line in lines if 'start' in line],
                         [['start', 'GOOD1'], ['start', 'BAD'], ['start', 'GOOD2']])
        live = Path(self.directory.name) / 'live-receipts'
        live.mkdir()
        digests = {'GOOD1': 'a' * 64, 'GOOD2': 'b' * 64}
        with mock.patch.object(formal, 'input_digest', side_effect=lambda o: digests[o['id']]), \
                mock.patch.object(formal, 'input_snapshot', return_value={}), \
                mock.patch.object(formal, 'receipt_problems', return_value=[]):
            batch = {'GOOD1': {'id': 'GOOD1'}, 'GOOD2': {'id': 'GOOD2'}}
            copied, refused = formal_batch.copy_back(outcomes, batch, live, emit=lambda *a: None, live=batch)
        self.assertEqual(copied, ['GOOD1'])
        self.assertEqual(list(refused), ['GOOD2'])
        self.assertEqual(sorted(p.name for p in live.iterdir()), ['GOOD1.json'])


    def test_a_whole_batch_copies_what_passed_and_fails_for_what_did_not(self):
        worktree = self.prepare(self.head)
        live = Path(self.directory.name) / 'live-receipts'
        live.mkdir()
        args = formal_batch.parser().parse_args(['rerecord', '--jobs', '2'])
        out = io.StringIO()
        with mock.patch.object(formal_batch, 'prepare_worktree', return_value=worktree), \
                mock.patch.object(formal_batch, 'POLL_SECONDS', 0.02), \
                mock.patch.object(formal, 'RECEIPTS', live), \
                mock.patch.object(formal, 'input_digest', return_value='a' * 64), \
                mock.patch.object(formal, 'receipt_problems', return_value=[]), \
                mock.patch.object(formal_batch, 'live_obligations', return_value={'GOOD1': {'id': 'GOOD1'}}), \
                contextlib.redirect_stdout(out):
            code = formal_batch.run_batch([job('GOOD1', 2), job('BAD', 1)], {'GOOD1': {'id': 'GOOD1'}},
                                          'f' * 40, args)
        self.assertEqual(code, 1)
        self.assertIn('FORMAL_BATCH_FAIL: 1 copied, 0 not copied, 1 failed, 0 not run', out.getvalue())
        self.assertIn('FAILED      BAD', out.getvalue())
        self.assertEqual(sorted(p.name for p in live.iterdir()), ['GOOD1.json'])


class Lock(unittest.TestCase):
    def test_a_second_batch_is_refused_while_the_first_holds_the_worktree(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'formal-worktree.lock'
            with formal_batch.worktree_lock(path):
                with self.assertRaisesRegex(SystemExit, 'another rerecord'):
                    formal_batch.worktree_lock(path)
            formal_batch.worktree_lock(path).close()  # released with the first


class ReceiptState(unittest.TestCase):
    def test_states_come_from_the_driver(self):
        obligation = next(o for o in MANIFEST['obligations'] if o['id'] == 'KANI-003')
        receipt = json.loads((formal.RECEIPTS / 'KANI-003.json').read_text())
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(formal_batch.receipt_state(obligation, directory), ('missing', ['required']))
            path = Path(directory) / 'KANI-003.json'
            path.write_text('{')
            self.assertEqual(formal_batch.receipt_state(obligation, directory)[0], 'invalid')
            path.write_text(json.dumps(receipt))
            with mock.patch.object(formal, 'input_snapshot', return_value=receipt['inputs']):
                self.assertEqual(formal_batch.receipt_state(obligation, directory), ('fresh', []))
            moved = json.loads(json.dumps(receipt['inputs']))
            moved['files']['Cargo.lock'] = '0' * 64
            with mock.patch.object(formal, 'input_snapshot', return_value=moved):
                self.assertEqual(formal_batch.receipt_state(obligation, directory),
                                 ('stale', ['files:Cargo.lock']))
            path.write_text(json.dumps(dict(receipt, checks=receipt['checks'][:1])))
            state, detail = formal_batch.receipt_state(obligation, directory)
            self.assertEqual(state, 'invalid')
            self.assertTrue(any('omits' in d for d in detail), detail)

    def test_what_needs_recording(self):
        claims = {'status': 'pass-with-recorded-scope'}
        quiet = {'status': 'implemented-unchecked'}
        self.assertTrue(formal_batch.needs_recording(claims, 'stale'))
        self.assertTrue(formal_batch.needs_recording(claims, 'missing'))
        self.assertTrue(formal_batch.needs_recording(quiet, 'invalid'))
        self.assertFalse(formal_batch.needs_recording(quiet, 'missing'))
        self.assertFalse(formal_batch.needs_recording(claims, 'fresh'))


class Cli(unittest.TestCase):
    def test_defaults(self):
        args = formal_batch.parser().parse_args(['rerecord'])
        self.assertEqual((args.jobs, args.mem_gib, args.rev, args.ids, args.all, args.dry_run),
                         (6, 48.0, 'HEAD', None, False, False))

    def test_ids_are_split_on_commas_and_spaces_and_checked(self):
        args = formal_batch.parser().parse_args(['plan', '--ids', 'TLA-002,KANI-001', 'KANI-040'])
        self.assertEqual(formal_batch.select_ids(MANIFEST, args),
                         (['KANI-001', 'KANI-040', 'TLA-002'], 'ids'))
        args = formal_batch.parser().parse_args(['plan', '--ids', 'KANI-999'])
        with self.assertRaisesRegex(SystemExit, 'unknown obligation id'):
            formal_batch.select_ids(MANIFEST, args)

    def test_bad_arguments_are_refused(self):
        for argv in (('plan', '--all', '--stale'), ('plan', '--ids', 'KANI-001', '--all'),
                     ('rerecord', '--jobs', '0'), ('plan', '--mem-gib', '-1'),
                     ('cost', '--base', 'HEAD', 'src/lib.rs'), ('nonsense',)):
            with self.subTest(argv=argv):
                self.assertEqual(run_main(*argv)[0], 2)

    def test_plan_prints_every_obligation_and_the_makespan(self):
        code, out, _ = run_main('plan', '--all', '--jobs', '6')
        self.assertEqual(code, 0)
        for oid in ALL_IDS:
            self.assertIn(f' {oid} ', out)
        self.assertIn('at 6 job(s) within 48 GiB', out)

    def test_cost_of_a_hypothetical_edit(self):
        code, out, _ = run_main('cost', str(formal_batch.ROOT / 'Cargo.lock'))
        self.assertEqual(code, 0)
        self.assertIn(f'receipts this change stales: {len(ALL_IDS)} of {len(ALL_IDS)}', out)
        self.assertIn(f'CI formal job runs {len(ALL_IDS)} obligation(s)', out)
        code, out, _ = run_main('cost', str(formal_batch.ROOT / 'docs'))
        self.assertIn('receipts this change stales: 0 of', out)
        self.assertIn('CI formal job runs 0 obligation(s)', out)


if __name__ == '__main__':
    unittest.main()
