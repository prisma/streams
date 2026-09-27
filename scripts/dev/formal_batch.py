#!/usr/bin/env python3
"""Plan, cost and re-record formal receipts in parallel, outside the live checkout.

Developer tool, advisory only. It never judges a verdict, computes a digest of
its own or writes a receipt: receipt state and CI selection come from the
driver's own functions (scripts/quality/formal.py, imported read-only); every
check still runs through the unchanged `formal.py run --id ID --record`; and a
receipt reaches verification/receipts/ only as an unmodified copy, after the
live checkout's `formal.input_digest()` has been found equal to the digest the
receipt records.

  status [--json]          one line per obligation: fresh, stale, missing or
                           invalid; recorded minutes; what a stale one's inputs changed
  cost [--base REV] [PATH ...] [--jobs N] [--json]
                           the receipts a change stales (the diff from REV, default
                           the upstream branch, or a hypothetical edit of PATHs),
                           whether CI's formal job then runs EVERY obligation,
                           and serial and parallel minutes
  preflight [--tools]      formal.py check plus `git apply --check` of every Kani
                           control patch (about a second); --tools probes the pins
  plan [--ids ID ... | --stale | --all] [--jobs N] [--mem-gib G]
                           the re-record schedule, longest first, and its makespan
  rerecord [--ids ID ... | --stale | --all] [--jobs N] [--mem-gib G] [--rev REV]
           [--allow-dirty] [--dry-run]
                           re-record in parallel in the persistent worktree
                           target/formal-worktree at REV (default HEAD), then copy
                           back each receipt recorded for the live inputs

The selection defaults to --stale: every obligation whose receipt is invalid,
or stale or missing while its status claims a result.

rerecord is the only writer. It moves the ignored worktree to REV (its own
target/ keeps warm Kani builds between batches; target/quality-tools links to
the live checkout's pinned tools), runs one driver process per obligation,
longest first, at most --jobs at once within a --mem-gib budget, and copies
verification/receipts/<ID>.json back into the live checkout. The live
checkout's HEAD, index and every other file are never touched, so editing can
continue during a batch; the copy-back guard refuses any receipt whose inputs
the live checkout no longer has. Reclaim the worktree with
`git worktree remove --force target/formal-worktree`.
"""
import sys

import os  # noqa: E402
import shutil  # noqa: E402

if sys.version_info < (3, 11):
    # macOS's /usr/bin/python3 is 3.9: re-run under the newest 3.11+ installed.
    if not os.environ.get('STREAMS_PYTHON_REEXEC'):
        os.environ['STREAMS_PYTHON_REEXEC'] = '1'
        for candidate in ('python3.14', 'python3.13', 'python3.12', 'python3.11',
                          '/opt/homebrew/bin/python3.13', '/opt/homebrew/bin/python3.12',
                          '/opt/homebrew/bin/python3.11'):
            found = shutil.which(candidate)
            if found:
                os.execv(found, [found, *sys.argv])
    sys.exit('formal_batch.py: Python >= 3.11 is required (the gate modules import tomllib); '
             'install one (brew install python@3.12) or `. scripts/dev/env.sh` first')

import argparse
from concurrent.futures import ThreadPoolExecutor
import contextlib
from dataclasses import dataclass
import fcntl
import json
from pathlib import Path
import re
import signal
import subprocess
import time
import tomllib

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/quality'))
import formal  # noqa: E402  (the driver: read-only use of its functions)
import formal_shards  # noqa: E402  (receipt weights and LPT order, shared with CI)

WORKTREE = ROOT / 'target/formal-worktree'
WORKTREE_LOCK = ROOT / 'target/formal-worktree.lock'
RERECORD_OUT = 'target/formal/rerecord'   # per obligation, inside the worktree
CONTROLS = 'verification/kani/controls'
MANIFEST_PATH = 'verification/manifest.json'
PINS_PATH = 'quality-tools.toml'
DEFAULT_JOBS = 6
CI_SHARDS = 6                             # the formal job's matrix in rust-quality.yml
DEFAULT_MEM_GIB = 48.0
# Peak memory estimates. TLC runs -Xmx<heap> (3g unless a check sets `heap`)
# plus JVM overhead; KANI-001's CBMC peaked at 4.9 GB after its split; the
# other harnesses are smaller.
TLC_OVERHEAD_GIB = 1.0
KANI_GIB = {'KANI-001': 6.0}
KANI_DEFAULT_GIB = 3.0
POLL_SECONDS = 1.0
TERM_GRACE_SECONDS = 30.0


def git(*args, cwd=ROOT):
    return subprocess.run(['git', *args], cwd=cwd, check=True, text=True,
                          capture_output=True).stdout.strip()


def resolve(rev):
    try:
        return git('rev-parse', '--verify', '--quiet', f'{rev}^{{commit}}')
    except subprocess.CalledProcessError:
        sys.exit(f'formal_batch: unknown revision {rev!r}')


def default_base():
    """The current branch's upstream (what a push compares with), else HEAD."""
    try:
        return git('rev-parse', '--abbrev-ref', '--symbolic-full-name', '@{upstream}')
    except subprocess.CalledProcessError:
        return 'HEAD'


def rel(path):
    with contextlib.suppress(ValueError):
        return Path(path).relative_to(ROOT).as_posix()
    return str(path)


def minutes(seconds):
    return f'{seconds / 60:.1f} min'


def physical_gib():
    try:
        if sys.platform == 'darwin':
            return int(subprocess.check_output(['sysctl', '-n', 'hw.memsize'], text=True)) / 2**30
        return os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 2**30
    except (OSError, ValueError, subprocess.CalledProcessError):
        return None


def budget_note(budget):
    if (gib := physical_gib()) and budget > gib:
        print(f'note: --mem-gib {budget:g} exceeds this machine\'s {gib:.0f} GiB; lower it')


# --------------------------------------------------------------------------
# Receipts and memory estimates.
# --------------------------------------------------------------------------

def receipt_state(obligation, receipts=None):
    """('fresh'|'stale'|'missing'|'invalid', details), judged by the driver."""
    path = Path(receipts or formal.RECEIPTS) / f'{obligation["id"]}.json'
    if not path.is_file():
        return 'missing', ['required' if obligation['status'] in formal.CLAIMS else 'optional']
    try:
        receipt = json.loads(path.read_text())
    except (ValueError, UnicodeDecodeError) as error:
        return 'invalid', [f'not JSON ({error})']
    problems = formal.receipt_problems(obligation, receipt)
    if problems:
        return 'invalid', [p.removeprefix(f'{obligation["id"]}: ') for p in problems]
    if receipt['schema'] != formal.RECEIPT_SCHEMA:
        return 'stale', [f'schema {receipt["schema"]} receipt']
    snapshot = formal.input_snapshot(obligation)
    if receipt['inputs_sha256'] == formal.canonical_sha256(snapshot):
        return 'fresh', []
    return 'stale', formal.changed_inputs(receipt['inputs'], snapshot) or ['digest']


def needs_recording(obligation, state):
    return state == 'invalid' or (state in ('stale', 'missing')
                                  and obligation['status'] in formal.CLAIMS)


def heap_gib(value):
    match = re.fullmatch(r'(\d+(?:\.\d+)?)([gGmM])', str(value))
    if not match:
        return 3.0
    return float(match[1]) / (1 if match[2] in 'gG' else 1024)


def mem_gib(obligation):
    if obligation['kind'] == 'tla':
        return max((heap_gib(c.get('heap', '3g')) for c in obligation['checks']), default=3.0) \
            + TLC_OVERHEAD_GIB
    return KANI_GIB.get(obligation['id'], KANI_DEFAULT_GIB)


# --------------------------------------------------------------------------
# What a change alters: the receipt inputs, named as formal.changed_inputs does.
# --------------------------------------------------------------------------

class Revision:
    """The manifest entries, assumption entries and pins at a git revision."""

    def __init__(self, rev):
        self.rev, self._cache = rev, {}

    def _memo(self, key, compute):
        if key not in self._cache:
            self._cache[key] = compute()
        return self._cache[key]

    def text(self, path):
        def show():
            shown = subprocess.run(['git', 'show', f'{self.rev}:{path}'], cwd=ROOT,
                                   capture_output=True, text=True)
            return shown.stdout if shown.returncode == 0 else None
        return self._memo(('text', path), show)

    def entries(self):
        text = self.text(MANIFEST_PATH)
        return self._memo('entries', lambda: {o['id']: o for o in json.loads(text)['obligations']}
                          if text else {})

    def ledger(self):
        return self._memo('ledger', lambda: formal.ledger_at(self.rev))

    def pins(self):
        def parse():
            text = self.text(PINS_PATH)
            config = tomllib.loads(text) if text is not None else {}
            return {'formal': config.get('formal'), 'slatedb': config.get('slatedb')}
        return self._memo('pins', parse)


def input_changes(manifest, changed, before=None):
    """Obligation ID -> the receipt inputs `changed` (repo paths) alters.

    With `before` (a Revision) the manifest entry, assumption entries and pins
    are compared exactly, so the result is the set of receipts whose input
    digest differs from REV's. Without it the edit is hypothetical: an edited
    manifest, ledger or pins file is marked with '?' (it alters the receipt
    only if the edit touches that obligation's entry, a named assumption or the
    [formal]/[slatedb] pins), so the result is an upper bound."""
    changed, ledger = set(changed), formal.ledger_path()
    current_ledger = formal.assumption_entries() if ledger in changed and before is not None else {}
    pins_moved = PINS_PATH in changed and before is not None and before.pins() != formal.pinned()
    result = {}
    for obligation in manifest['obligations']:
        reasons = [f'files:{path}' for path in sorted(formal.digested_files(obligation) & changed)]
        named = obligation.get('assumptions', [])
        if ledger in changed and named:
            reasons += ['assumptions?'] if before is None else [
                f'assumptions:{asm}' for asm in named
                if current_ledger.get(asm) != before.ledger().get(asm)]
        if MANIFEST_PATH in changed:
            if before is None:
                reasons.append('manifest_entry?')
            elif (old := before.entries().get(obligation['id'])) is None:
                reasons.append('manifest_entry (new obligation)')
            elif formal.canonical_sha256(old) != formal.canonical_sha256(obligation):
                reasons.append('manifest_entry')
        if PINS_PATH in changed and (before is None or pins_moved):
            reasons.append('pins?' if before is None else 'pins')
        if reasons:
            result[obligation['id']] = reasons
    return result


def repo_paths(paths):
    """Repository-relative paths for command-line PATHs spelled relative to the
    current directory, to the repository root, or absolutely; a directory
    stands for every tracked file under it."""
    result = set()
    for raw in paths:
        absolute = Path(os.path.abspath(raw))
        if not Path(raw).is_absolute() and not absolute.exists():
            absolute = Path(os.path.normpath(ROOT / raw))  # a repository-relative spelling
        try:
            relative = absolute.relative_to(ROOT).as_posix()
        except ValueError:
            try:  # a symlinked spelling of a path inside the repository
                relative = absolute.resolve().relative_to(ROOT).as_posix()
            except ValueError:
                sys.exit(f'formal_batch: {raw} is outside the repository {ROOT}')
        if absolute.is_dir():
            listed = subprocess.check_output(['git', 'ls-files', '-z', '--', relative], cwd=ROOT)
            result |= {p.decode() for p in listed.split(b'\0') if p}
        else:
            result.add(relative)
    return result


# --------------------------------------------------------------------------
# Scheduling: strict longest-first admission under a job and memory budget.
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Job:
    id: str
    kind: str
    seconds: float   # estimate: the receipt's recorded check seconds
    gib: float       # estimated peak memory


def jobs_for(manifest, ids):
    by_id = {o['id']: o for o in manifest['obligations']}
    work = formal_shards.weights(ids)
    return [Job(oid, by_id[oid]['kind'], work[oid], mem_gib(by_id[oid]))
            for oid in formal_shards.lpt_order(work)]


def admissions(pending, running, slots, budget):
    """The jobs to start now, taken strictly from the head of `pending` (LPT
    order): the head starts while fewer than `slots` run and its memory fits
    beside the running jobs'. The head is never overtaken (no backfill), so the
    longest remaining obligation starts first; one larger than the whole
    budget starts alone once nothing else runs."""
    started, count, used = [], len(running), sum(job.gib for job in running)
    for job in pending:
        if count >= slots or (count and used + job.gib > budget):
            break
        started.append(job)
        count, used = count + 1, used + job.gib
    return started


def simulate(jobs, slots, budget):
    """The rerecord scheduler with every job taking its estimate:
    ([(job, lane, start, end)] in start order, makespan)."""
    pending, running, rows, now = list(jobs), [], [], 0.0
    while pending or running:
        lanes = sorted(set(range(slots)) - {lane for _, lane, _ in running})
        for job, lane in zip(admissions(pending, [j for _, _, j in running], slots, budget), lanes):
            pending.remove(job)
            running.append((now + job.seconds, lane, job))
            rows.append((job, lane, now, now + job.seconds))
        now = min(end for end, _, _ in running)
        running = [entry for entry in running if entry[0] > now]
    return rows, now


# --------------------------------------------------------------------------
# The worktree, the driver processes and the copy-back guard.
# --------------------------------------------------------------------------

def worktree_lock(path=WORKTREE_LOCK):
    """An exclusive lock held for a whole batch: two batches never share the worktree."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    handle = open(path, 'w')
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise SystemExit(f'formal_batch: another rerecord holds {rel(path)}; wait for it to finish')
    return handle


def registered_worktrees(root=ROOT):
    listing = git('worktree', 'list', '--porcelain', cwd=root)
    return {Path(line.removeprefix('worktree ')).resolve()
            for line in listing.splitlines() if line.startswith('worktree ')}


def prepare_worktree(rev, worktree=WORKTREE, root=ROOT):
    """A detached, clean checkout of `rev` at `worktree`, reusing it if present.

    Only a registered worktree of this repository other than the live checkout
    is ever moved (`checkout --force`) or cleaned (`clean -fd`, which keeps the
    ignored target/ and its warm build caches)."""
    worktree = Path(worktree).resolve()
    if worktree == Path(root).resolve():
        raise SystemExit('formal_batch: refusing to use the live checkout as the formal worktree')
    registered = worktree in registered_worktrees(root)
    if not registered and worktree.exists():
        raise SystemExit(f'formal_batch: {worktree} exists but is not a worktree of {root}; '
                         'remove it and retry')
    if not worktree.is_dir():
        worktree.parent.mkdir(parents=True, exist_ok=True)
        # --force only re-adds a registered worktree whose directory was deleted.
        git('worktree', 'add', '--detach', *(['--force'] if registered else []), str(worktree), rev,
            cwd=root)
    else:
        # A worktree whose .git link is broken would resolve to the enclosing
        # live checkout; the forced checkout and clean below must never reach it.
        if Path(git('rev-parse', '--show-toplevel', cwd=worktree)).resolve() != worktree:
            raise SystemExit(f'formal_batch: {worktree} is not the top of a checkout')
        git('checkout', '--detach', '--force', '--quiet', rev, cwd=worktree)
        git('clean', '-fdq', cwd=worktree)
    link, tools = worktree / 'target/quality-tools', Path(root) / 'target/quality-tools'
    link.parent.mkdir(parents=True, exist_ok=True)
    if link.is_symlink() and Path(os.readlink(link)) != tools:
        link.unlink()
    if link.exists() and not link.is_symlink():
        raise SystemExit(f'formal_batch: {link} is not a link to {tools}; remove it and retry')
    if not link.is_symlink():
        link.symlink_to(tools, target_is_directory=True)
    return worktree


def child_env(worktree, environ=None):
    """The driver's environment: Kani picks its own nightly (no RUSTUP_TOOLCHAIN),
    builds stay in the worktree (no CARGO_TARGET_DIR), pinned tools come first."""
    env = dict(os.environ if environ is None else environ)
    for name in ('RUSTUP_TOOLCHAIN', 'CARGO_TARGET_DIR'):
        env.pop(name, None)
    env['PATH'] = os.pathsep.join([str(Path(worktree) / 'target/quality-tools/bin'),
                                   env.get('PATH', '')])
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    return env


def child_command(oid):
    """The documented recording command, run with the worktree as cwd."""
    return [sys.executable, 'scripts/quality/formal.py', 'run', '--id', oid, '--record',
            '--out', f'{RERECORD_OUT}/{oid}']


@dataclass
class Outcome:
    job: Job
    code: int
    seconds: float
    recorded: Path | None   # the new receipt, moved out of the worktree's tree
    mismatches: int
    console: Path

    @property
    def ok(self):
        return self.code == 0 and self.recorded is not None


class Child:
    """One `formal.py run --id ID --record` process in the worktree."""

    def __init__(self, job, worktree, env, popen=subprocess.Popen):
        self.job = job
        self.out = Path(worktree) / RERECORD_OUT / job.id
        shutil.rmtree(self.out, ignore_errors=True)
        self.out.mkdir(parents=True)
        self.receipt = Path(worktree) / 'verification/receipts' / f'{job.id}.json'
        self.original = self.receipt.read_bytes() if self.receipt.is_file() else None
        self.console = self.out / 'console.log'
        with open(self.console, 'wb') as log:
            self.process = popen(child_command(job.id), cwd=worktree, env=env,
                                 stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                 start_new_session=True)

    def poll(self):
        return self.process.poll()

    def collect(self):
        """Move a receipt this run wrote out of the worktree's tracked tree and
        put back the committed bytes, so the next driver to start still sees a
        clean tree (its receipt records dirty_tree from `git status`)."""
        if not self.receipt.is_file() or self.receipt.read_bytes() == self.original:
            return None
        kept = self.out / 'recorded-receipt.json'
        kept.write_bytes(self.receipt.read_bytes())
        if self.original is None:
            self.receipt.unlink()
        else:
            self.receipt.write_bytes(self.original)
        return kept

    def finish(self, code, seconds):
        recorded = self.collect()
        text = self.console.read_text(errors='replace') if self.console.is_file() else ''
        mismatches = sum(line.endswith('... MISMATCH') for line in text.splitlines())
        return Outcome(self.job, code, seconds, recorded if code == 0 else None, mismatches,
                       self.console)

    def terminate(self):
        """SIGTERM: the driver kills its running check's process group and exits."""
        if self.process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                self.process.send_signal(signal.SIGTERM)

    def wait_or_kill(self, timeout):
        try:
            self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait()


def clock_text(seconds):
    seconds = int(seconds)
    return f'{seconds // 3600}:{seconds // 60 % 60:02}:{seconds % 60:02}'


def finish_text(outcome):
    took = f'{outcome.seconds:.1f} s'
    if outcome.ok:
        return f'done   {outcome.job.id:9} recorded  {took}'
    why = [f'exit {outcome.code}'] + ([f'{outcome.mismatches} mismatch(es)'] if outcome.mismatches
                                      else []) + ([] if outcome.code else ['no new receipt'])
    return f'done   {outcome.job.id:9} FAILED ({", ".join(why)})  {took}; log {rel(outcome.console)}'


def execute(jobs, slots, budget, launch, clock=time.monotonic, sleep=time.sleep, emit=print,
            poll=None, outcomes=None):
    """Run `jobs` (LPT order) under `admissions`; `launch(job)` returns a Child.
    Streams one line per start and finish and appends each Outcome to
    `outcomes` as it happens, so a caller keeps the finished ones if the batch
    is cut short. Any exception, an interrupt or a SIGTERM included, stops
    every running driver before it propagates."""
    outcomes = [] if outcomes is None else outcomes
    poll = POLL_SECONDS if poll is None else poll
    pending, running, began = list(jobs), [], clock()
    try:
        while pending or running:
            for entry in list(running):
                job, child, started = entry
                code = child.poll()
                if code is not None:
                    running.remove(entry)
                    outcomes.append(child.finish(code, clock() - started))
                    emit(f'[{clock_text(clock() - began)}] {finish_text(outcomes[-1])}', flush=True)
            for job in admissions(pending, [entry[0] for entry in running], slots, budget):
                pending.remove(job)
                running.append((job, launch(job), clock()))
                emit(f'[{clock_text(clock() - began)}] start  {job.id:9} ~{minutes(job.seconds)}, '
                     f'{job.gib:g} GiB ({len(running)}/{slots} running, '
                     f'{sum(e[0].gib for e in running):g}/{budget:g} GiB)', flush=True)
            if running:
                sleep(poll)
    except BaseException:
        children = [child for _, child, _ in running]
        for child in children:
            child.terminate()
        deadline = time.monotonic() + TERM_GRACE_SECONDS
        for child in children:
            child.wait_or_kill(max(0.0, deadline - time.monotonic()))
        raise
    return outcomes, clock() - began


def json_object(data):
    try:
        value = json.loads(data)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def live_obligations():
    """The live checkout's manifest, read now: a batch can outlive an edit."""
    return {o['id']: o for o in formal.load()['obligations']}


def copy_back(outcomes, by_id, receipts=None, emit=print, live=None):
    """Copy each new receipt into the live verification/receipts/ only when the
    digest it records equals `formal.input_digest()` of the live checkout now
    (and the live driver accepts it). The manifest is re-read at copy time, so
    an entry edited during the batch refuses its receipt. Returns (copied IDs,
    {ID: reason})."""
    destination = Path(receipts or formal.RECEIPTS)
    copied, refused = [], {}
    if live is None:
        try:
            live = live_obligations()
        except (OSError, ValueError, KeyError, TypeError) as error:
            for outcome in outcomes:
                if outcome.ok:
                    refused[outcome.job.id] = f'NOT COPIED: the live manifest is unreadable ({error})'
            live = {}
    for outcome in outcomes:
        if not outcome.ok or outcome.job.id in refused:
            continue
        oid, obligation = outcome.job.id, live.get(outcome.job.id)
        data = outcome.recorded.read_bytes()
        receipt = json_object(data)
        if receipt is None:
            refused[oid] = 'NOT COPIED: the recorded receipt is not a JSON object'
        elif obligation is None:
            refused[oid] = 'NOT COPIED: the live manifest has no such obligation'
        elif oid in by_id and formal.canonical_sha256(obligation) != formal.canonical_sha256(by_id[oid]):
            refused[oid] = 'NOT COPIED: the manifest entry changed during the batch'
        elif receipt.get('inputs_sha256') != formal.input_digest(obligation):
            moved = formal.changed_inputs(receipt.get('inputs') or {}, formal.input_snapshot(obligation))
            refused[oid] = f'NOT COPIED: live inputs differ ({", ".join(moved) or "digest"})'
        elif problems := formal.receipt_problems(obligation, receipt):
            refused[oid] = f'NOT COPIED: the live driver rejects it ({"; ".join(problems)})'
        else:
            staging = destination / f'.{oid}.json.partial'
            staging.write_bytes(data)
            os.replace(staging, destination / f'{oid}.json')
            copied.append(oid)
            emit(f'COPIED      {oid} -> {rel(destination / f"{oid}.json")}')
    for oid, reason in sorted(refused.items()):
        emit(f'{oid}: {reason}; kept at {rel(next(o.recorded for o in outcomes if o.job.id == oid))}')
    return copied, refused


# --------------------------------------------------------------------------
# Subcommands.
# --------------------------------------------------------------------------

def select_ids(manifest, args):
    ids = sorted(o['id'] for o in manifest['obligations'])
    if args.all:
        return ids, 'all'
    if args.ids:
        wanted = sorted({i for raw in args.ids for i in raw.replace(',', ' ').split()})
        if unknown := sorted(set(wanted) - set(ids)):
            raise SystemExit(f'formal_batch: unknown obligation id(s): {" ".join(unknown)}')
        return wanted, 'ids'
    return [o['id'] for o in manifest['obligations']
            if needs_recording(o, receipt_state(o)[0])], 'stale'


def schedule_summary(jobs, slots, budget):
    serial = sum(job.seconds for job in jobs)
    rows, span = simulate(jobs, slots, budget)
    speedup = f' ({serial / span:.1f}x)' if span else ''
    critical = f'; longest {jobs[0].id} {minutes(jobs[0].seconds)}' if jobs else ''
    return rows, (f'serial {minutes(serial)}; ~{minutes(span)} at {slots} job(s) within '
                  f'{budget:g} GiB{speedup}{critical}')


def cmd_status(args):
    manifest, rows = formal.load(), []
    for obligation in sorted(manifest['obligations'], key=lambda o: o['id']):
        state, detail = receipt_state(obligation)
        rows.append({'id': obligation['id'], 'kind': obligation['kind'],
                     'status': obligation['status'], 'checks': len(obligation['checks']),
                     'state': state, 'recorded_seconds': formal_shards.receipt_seconds(obligation['id']),
                     'needs_recording': needs_recording(obligation, state), 'detail': detail})
    if args.json:
        print(json.dumps(rows, indent=2))
        return int(any(row['state'] == 'invalid' for row in rows))
    print(f'{"id":9} {"kind":4} {"checks":>6} {"state":7} {"min":>6}  detail')
    for row in rows:
        recorded = '-' if row['recorded_seconds'] is None else f'{row["recorded_seconds"] / 60:.1f}'
        print(f'{row["id"]:9} {row["kind"]:4} {row["checks"]:>6} {row["state"]:7} {recorded:>6}  '
              f'{", ".join(row["detail"])}')
    counts = {state: sum(row['state'] == state for row in rows)
              for state in ('fresh', 'stale', 'missing', 'invalid')}
    todo = jobs_for(manifest, [row['id'] for row in rows if row['needs_recording']])
    total = sum(row['recorded_seconds'] or 0 for row in rows)
    print(f'{len(rows)} obligation(s): ' + ', '.join(f'{n} {s}' for s, n in counts.items())
          + f'; all recorded work {minutes(total)} serial')
    print(f'to re-record ({len(todo)}): ' + (schedule_summary(todo, DEFAULT_JOBS, DEFAULT_MEM_GIB)[1]
                                             if todo else 'nothing'))
    return int(counts['invalid'] > 0)


def cmd_cost(args):
    manifest = formal.load()
    if args.paths:
        changed, before, previous_ledger = repo_paths(args.paths), None, None
        heading = f'hypothetical edit of {len(changed)} path(s)'
    else:
        base = args.base or default_base()
        rev = resolve(base)
        changed, before = formal.changed_since(rev), Revision(rev)
        previous_ledger = before.ledger()
        heading = f'diff from {base} ({rev[:12]}) to the live checkout: {len(changed)} path(s)'
    stales = input_changes(manifest, changed, before)
    everything = sorted(changed & set(formal.SELECTION_INPUTS))
    ci = formal.select(manifest, changed, previous_ledger)
    jobs = jobs_for(manifest, sorted(stales))
    ci_work = formal_shards.weights(ci)
    ci_span = formal_shards.makespan(formal_shards.lpt(ci_work, CI_SHARDS), ci_work) if ci else 0.0
    if args.json:
        print(json.dumps({'changed_paths': sorted(changed), 'stales': stales,
                          'stale_serial_seconds': sum(j.seconds for j in jobs),
                          'stale_makespan_seconds': simulate(jobs, args.jobs, DEFAULT_MEM_GIB)[1],
                          'jobs': args.jobs, 'ci_selects_everything_because': everything,
                          'ci_selected': ci, 'ci_serial_seconds': sum(ci_work.values()),
                          'ci_shards': CI_SHARDS, 'ci_longest_shard_seconds': ci_span}, indent=2))
        return 0
    print(heading)
    bound = any(r.endswith('?') for reasons in stales.values() for r in reasons)
    print(f'receipts this change stales: {"up to " if bound else ""}{len(stales)} of '
          f'{len(manifest["obligations"])}'
          + (f'; {schedule_summary(jobs, args.jobs, DEFAULT_MEM_GIB)[1]}' if jobs else ''))
    for job in sorted(jobs, key=lambda j: j.id):
        print(f'  {job.id:9} {job.seconds / 60:6.1f} min  {", ".join(stales[job.id])}')
    if bound:
        print("  ('?' marks an input the edit alters only if it touches that obligation's "
              'manifest entry, a named assumption or the [formal]/[slatedb] pins: an upper bound)')
    scope = (f'EVERY obligation (the change touches {", ".join(everything)})' if everything
             else f'{len(ci)} obligation(s)')
    print(f'CI formal job runs {scope}' + (
        f': {minutes(sum(ci_work.values()))} serial, longest of {CI_SHARDS} shards ~{minutes(ci_span)} '
        '(local recorded seconds; CI runs TLC in roughly 0.6x)' if ci else ''))
    return 0


def apply_check(patch):
    done = subprocess.run(['git', 'apply', '--check', str(patch)], cwd=ROOT,
                          capture_output=True, text=True)
    return patch, done.returncode, done.stderr.strip()


def cmd_preflight(args):
    started = time.monotonic()
    patches = sorted((ROOT / CONTROLS).glob('*.patch'))
    with ThreadPoolExecutor(max_workers=8) as pool:
        check = pool.submit(subprocess.run, [sys.executable, str(ROOT / formal.DRIVER), 'check'],
                            cwd=ROOT, capture_output=True, text=True)
        applied = list(pool.map(apply_check, patches))
        checked = check.result()
    failed = checked.returncode != 0
    for line in checked.stderr.splitlines():
        print(line)
    print((checked.stdout.strip().splitlines() or ['formal.py check printed nothing'])[-1])
    referenced = {c.get('patch') for o in formal.load()['obligations'] for c in o['checks']}
    for patch, code, error in applied:
        if code:
            failed = True
            print(f'PREFLIGHT_FAIL: control patch no longer applies: {rel(patch)}: '
                  f'{error.splitlines()[0] if error else "git apply --check failed"}')
        elif rel(patch) not in referenced:
            print(f'PREFLIGHT_NOTE: no manifest check uses {rel(patch)}')
    if args.tools:
        problems, versions = formal.tool_problems({'tla', 'kani'})
        failed |= bool(problems)
        print('\n'.join(f'PREFLIGHT_FAIL: {p}' for p in problems) or
              'tools: ' + ', '.join(f'{k} {v}' for k, v in sorted(versions.items())
                                    if k in ('kani', 'tlc', 'java', 'platform')))
    print(f'PREFLIGHT_{"FAIL" if failed else "OK"}: formal.py check exit {checked.returncode}, '
          f'{len(patches)} control patch(es), {sum(1 for _, c, _ in applied if c)} broken '
          f'({time.monotonic() - started:.1f} s)')
    return int(failed)


def cmd_plan(args):
    manifest = formal.load()
    ids, label = select_ids(manifest, args)
    if not ids:
        print(f'nothing to schedule ({label})')
        return 0
    jobs = jobs_for(manifest, ids)
    rows, summary = schedule_summary(jobs, args.jobs, args.mem_gib)
    budget_note(args.mem_gib)
    print(f'{len(jobs)} obligation(s) ({label}); estimates are recorded receipt seconds '
          f'({formal_shards.FALLBACK_SECONDS:g} s without a receipt)')
    print(f'{"#":>3} {"id":9} {"kind":4} {"est min":>7} {"GiB":>4} {"lane":>4} {"start":>7} {"end":>7}')
    for number, (job, lane, start, end) in enumerate(rows, 1):
        print(f'{number:>3} {job.id:9} {job.kind:4} {job.seconds / 60:7.1f} {job.gib:4g} {lane:>4} '
              f'{start / 60:7.1f} {end / 60:7.1f}')
    print(summary)
    return 0


def cmd_rerecord(args):
    manifest = formal.load()
    if problems := formal.validate(manifest):
        print('\n'.join(f'FORMAL_FAIL: {p}' for p in problems), file=sys.stderr)
        return 1
    by_id = {o['id']: o for o in manifest['obligations']}
    ids, label = select_ids(manifest, args)
    if not ids:
        print(f'FORMAL_BATCH_OK: nothing to re-record ({label})')
        return 0
    rev = resolve(args.rev)
    changes = input_changes(manifest, formal.changed_since(rev), Revision(rev))
    dirty = {oid: changes[oid] for oid in ids if oid in changes}
    for oid, reasons in sorted(dirty.items()):
        print(f'{oid}: live inputs differ from {rev[:12]} ({", ".join(reasons)})')
    if dirty and not args.allow_dirty:
        sys.stdout.flush()
        advice = ('commit these inputs first' if rev == resolve('HEAD') else
                  'they may differ only by commits after it: pass --rev HEAD to verify the live '
                  'commit, or commit uncommitted inputs first')
        print(f'FORMAL_BATCH_REFUSED: the worktree verifies {args.rev} as committed ({rev[:12]}); '
              f'{advice}, or pass --allow-dirty to verify {rev[:12]} anyway',
              file=sys.stderr)
        return 2
    if dirty:
        print(f'--allow-dirty: each receipt records {rev[:12]} as committed, not the live checkout; '
              'one is copied back only if the live inputs equal its recorded inputs when the '
              'batch ends, so the obligations above will be left in the worktree')
    problems, _ = formal.tool_problems({by_id[oid]['kind'] for oid in ids})
    sys.stdout.flush()
    for problem in problems:
        print(f'FORMAL_FAIL: {problem}', file=sys.stderr)
    jobs = jobs_for(manifest, ids)
    budget_note(args.mem_gib)
    print(f'{len(jobs)} obligation(s) ({label}) at {rev[:12]}: '
          f'{schedule_summary(jobs, args.jobs, args.mem_gib)[1]}')
    if args.dry_run:
        for job in jobs:
            print(f'  (cd {rel(WORKTREE)} && {" ".join(child_command(job.id))})')
        return int(bool(problems))
    if problems:
        return 1
    with worktree_lock():
        return run_batch(jobs, by_id, rev, args)


def run_batch(jobs, by_id, rev, args):
    """Prepare the worktree, run the drivers, copy back and report (lock held)."""
    worktree = prepare_worktree(rev)
    env = child_env(worktree)
    outcomes, interrupted, began = [], None, time.monotonic()
    previous = signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(128 + signum))
    try:
        execute(jobs, args.jobs, args.mem_gib, lambda job: Child(job, worktree, env),
                outcomes=outcomes)
    except (KeyboardInterrupt, SystemExit) as stop:
        interrupted = stop.code if isinstance(stop, SystemExit) and isinstance(stop.code, int) else 130
        print(f'FORMAL_BATCH_INTERRUPTED: every running driver was stopped; {len(outcomes)} finished '
              f'obligation(s) still go through the copy-back guard, {len(jobs) - len(outcomes)} did not '
              'finish', file=sys.stderr)
    finally:
        signal.signal(signal.SIGTERM, previous)
    wall = time.monotonic() - began
    serial = sum(outcome.seconds for outcome in outcomes)
    print(f'FORMAL_BATCH: {len(outcomes)} obligation(s) at {rev[:12]} in {minutes(wall)} wall; '
          f'serial sum {minutes(serial)} ({serial / wall if wall else 0:.1f}x)')
    copied, refused = copy_back(outcomes, by_id)
    failed = [outcome for outcome in outcomes if not outcome.ok]
    for outcome in failed:
        print(f'FAILED      {finish_text(outcome).removeprefix("done   ")}')
    check = subprocess.run([sys.executable, str(ROOT / formal.DRIVER), 'check'], cwd=ROOT,
                           capture_output=True, text=True)
    for line in check.stderr.splitlines():
        print(line)
    print((check.stdout.strip().splitlines() or ['formal.py check printed nothing'])[-1])
    ok = not failed and not refused and interrupted is None
    print(f'FORMAL_BATCH_{"OK" if ok else "FAIL"}: {len(copied)} copied, {len(refused)} not copied, '
          f'{len(failed)} failed, {len(jobs) - len(outcomes)} not run')
    return interrupted if interrupted is not None else int(not ok)


def positive(kind):
    def parse(text):
        value = kind(text)
        if value <= 0:
            raise argparse.ArgumentTypeError(f'must be positive, got {text}')
        return value
    return parse


def parser():
    top = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = top.add_subparsers(dest='command', required=True)
    status = sub.add_parser('status', help='receipt state of every obligation')
    status.add_argument('--json', action='store_true')
    cost = sub.add_parser('cost', help='what a change stales and what CI then runs')
    cost.add_argument('--base', help='compare the live checkout with REV (default: upstream, else HEAD)')
    cost.add_argument('paths', nargs='*', help='a hypothetical edit of these paths instead of a diff')
    cost.add_argument('--jobs', type=positive(int), default=DEFAULT_JOBS)
    cost.add_argument('--json', action='store_true')
    preflight = sub.add_parser('preflight', help='formal.py check and every control patch applies')
    preflight.add_argument('--tools', action='store_true', help='also probe the pinned TLC and Kani')
    for name, text in (('plan', 'print the longest-first schedule'),
                       ('rerecord', 're-record receipts in the formal worktree')):
        command = sub.add_parser(name, help=text)
        which = command.add_mutually_exclusive_group()
        which.add_argument('--ids', nargs='+', metavar='ID', help='IDs (space or comma separated)')
        which.add_argument('--stale', action='store_true', help='the default: what needs recording')
        which.add_argument('--all', action='store_true')
        command.add_argument('--jobs', type=positive(int), default=DEFAULT_JOBS)
        command.add_argument('--mem-gib', type=positive(float), default=DEFAULT_MEM_GIB)
        if name == 'rerecord':
            command.add_argument('--rev', default='HEAD', help='the revision to verify (default HEAD)')
            command.add_argument('--allow-dirty', action='store_true',
                                 help='run although the live inputs differ from REV')
            command.add_argument('--dry-run', action='store_true',
                                 help='check and print the plan and commands; run nothing')
    return top


def main(argv=None):
    args = parser().parse_args(argv)
    if args.command == 'cost' and args.base and args.paths:
        parser().error('cost takes --base or PATHs, not both')
    return {'status': cmd_status, 'cost': cmd_cost, 'preflight': cmd_preflight,
            'plan': cmd_plan, 'rerecord': cmd_rerecord}[args.command](args)


if __name__ == '__main__':
    sys.exit(main())
