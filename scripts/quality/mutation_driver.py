#!/usr/bin/env python3
"""Execute mutation checks from the canonical owner table."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tomllib

from common import ROOT, write_json
import mutants_width
from mutation_owners import OWNERS, SIZES_COMMAND, SIZES_PATH, validate_plan, validate_sources

HARNESS_PREFIX = 'tools/quality-invariants/src/../../../'


def cargo_target_args(entry, out):
    build = out / ('pilot-build' if entry.target.startswith('pilot-') else 'build')
    args = ['--cargo-arg=--locked']
    if entry.target in {'service-lib', 'harness-lib'}:
        args.append('--cargo-arg=--lib')
    elif entry.target == 'pilot-benchmark':
        args.append('--cargo-arg=--bin=pilot')
    elif entry.target == 'pilot-generator':
        args.extend(('--cargo-arg=--bin=pilot', '--cargo-arg=--test=pilot_membership'))
    else:
        raise ValueError(f'unknown mutation target: {entry.target}')
    args.append(f'--cargo-arg=--target-dir={build}')
    return args


def cargo_test_args(filters):
    if not filters:
        return []
    args = [f'--cargo-test-arg={filters[0]}']
    if len(filters) > 1:
        args.append('--cargo-test-arg=--')
        args.extend(f'--cargo-test-arg={value}' for value in filters[1:])
    return args


def shard_args(share=None):
    """This job's share of an owner's mutants. The nightly runs its leg as
    several jobs of one selection (QUALITY_MUTANT_SHARD=k/n): each lists and
    tests every n-th mutant, dealt round-robin, so together they test each
    selected mutant exactly once. Unset: all of them. A push is dealt by
    owner instead (QUALITY_MUTANT_GROUP, group_work): a split owner's part
    arrives as `share` with n = 1.

    A scheduled owner too large for one night is dealt over several (share =
    (part, parts)): night `part`'s job k takes round-robin shard
    part * n + k of parts * n, so the nights' jobs together take each of the
    owner's mutants exactly once."""
    shard = os.environ.get('QUALITY_MUTANT_SHARD', '').strip()
    k, n = 0, 1
    if shard:
        k, _, n = shard.partition('/')
        if not (k.isdigit() and n.isdigit() and int(k) < int(n)):
            raise ValueError(f'invalid QUALITY_MUTANT_SHARD {shard!r}: expected k/n with k < n')
        k, n = int(k), int(n)
    part, parts = share or (0, 1)
    if n * parts == 1:
        return []
    return ['--shard', f'{part * n + k}/{parts * n}', '--sharding', 'round-robin']


def mutation_command(entry, out, output, diff, share=None):
    files = [f'{HARNESS_PREFIX}{path}' if entry.target == 'harness-lib' else path
             for path in entry.sources]
    command = ['cargo', 'mutants', *cargo_target_args(entry, out), '--baseline', 'run']
    if diff is not None:
        command.extend(('--in-diff', str(diff)))
    for path in files:
        command.extend(('--file', path))
    command.extend(('--package', entry.package))
    command.extend(cargo_test_args(entry.test_filters))
    command.extend(('--profile', 'quality', '--jobs', '1', '--timeout', '90',
                    '--build-timeout', '900', '--gitignore', 'true', '--output', str(output)))
    command.extend(shard_args(share))
    return command


def list_command(entry, diff, harness=False, share=None):
    command = ['cargo', 'mutants', '--list', '--json']
    if diff is not None:
        command.extend(('--in-diff', str(diff)))
    for path in entry.sources:
        command.extend(('--file', f'{HARNESS_PREFIX}{path}' if harness else path))
    command.extend(('--package', 'streams-quality-invariants' if harness else 'streams-slate'))
    command.extend(shard_args(share))
    return command


def selection(path):
    raw = path.read_text().strip()
    rows = json.loads(raw) if raw else []
    return sorted(json.dumps(dict(
        file=os.path.normpath(row['file']),
        **{key: row[key] for key in ('function', 'span', 'replacement', 'genre')},
    ), sort_keys=True) for row in rows)


def run_to_file(command, path):
    with path.open('w') as output:
        subprocess.run(command, cwd=ROOT, stdout=output, check=True)


def make_harness_diff(base, output):
    command = [
        'git', 'diff', '--no-ext-diff', '--binary',
        f'--src-prefix=a/{HARNESS_PREFIX}', f'--dst-prefix=b/{HARNESS_PREFIX}', base, '--',
    ]
    with output.open('wb') as target:
        subprocess.run(command, cwd=ROOT, stdout=target, check=True)


def check_tool_version():
    with (ROOT / 'quality-tools.toml').open('rb') as source:
        expected = tomllib.load(source)['tools']['cargo-mutants']
    actual = subprocess.check_output(['cargo', 'mutants', '--version'], cwd=ROOT, text=True).strip()
    if actual != f'cargo-mutants {expected}':
        raise ValueError(f'cargo-mutants version mismatch: expected {expected}, got {actual!r}')
    return actual


# The pinned cargo-mutants (27.1.0, src/exit_code.rs and LabOutcome::exit_code
# in src/outcome.rs) answers 0 when every viable mutant was caught, 2 when one
# was missed and 3 when one timed out (3 wins when both happened). Every other
# answer measured nothing: 1 a usage error or any error, 4 a failing unmutated
# baseline, 5 and 6 an unusable --in-diff, 70 an internal error, and a
# negative status a signal. A mutant that fails to build is unviable and does
# not change the answer.
SURVIVOR_STATUSES = {2: 'missed', 3: 'timeout'}

# cargo-mutants counts a mutant caught whenever `cargo test` failed, whatever
# stopped the tests (src/outcome.rs, mutant_caught). A test binary killed by a
# signal (an abort on a failed allocation, a stack overflow, a crash) makes
# cargo exit 101 with the line "process didn't exit successfully: `...`
# (signal: N, ...)", so the mutant lands in caught.txt and the answer stays 0;
# a mutant it cannot classify at all (summary "Failure": cargo itself died by
# a signal) it only warns about and counts nowhere. Neither is a failure an
# assertion detected (RUST-QUALITY.md), so the driver lists both in
# incomplete.txt, a survivor beside missed and timeout, whatever the answer.
# Only the test phase's output is read: the log also holds the mutation diff
# and the build's output, where source text such as
# `fn deliver_to_helper(signal: libc::c_int)` (src/tasks/signal.rs) matches.
INCOMPLETE_SIGNATURES = ('(signal: ', 'memory allocation of ')
SURVIVOR_KINDS = (*SURVIVOR_STATUSES.values(), 'incomplete')

# A verdict needs positive evidence (gate decision of 2026-11-11, H1): a catch
# is a test phase that ran tests to `test result: FAILED.`, an unviable mutant
# a build phase the compiler rejected (`error: could not compile `, which a
# parse error without an `error[E...]` code also carries). cargo-mutants
# counts any failing `cargo test` a catch and any failing build unviable, so a
# build tree broken under it (the temp copy losing a workspace member's
# sources mid-run, 2026-11-10: "no targets specified in the manifest") or a
# broken host passed as 14 unviable mutants and one catch. Either verdict is
# incomplete without its evidence, or when its phase shows one of these
# environment signatures. `NotFound` is not one: real catches panic with an
# object store's NotFound. rustc's source-snippet lines (`12 | ...`) are not
# read, so source text never counts.
ENVIRONMENT_SIGNATURES = (
    'failed to load manifest', 'failed to parse manifest', 'could not find `Cargo.toml`',
    'no targets specified in the manifest', '(os error 2)', '(os error 28)',
    'error[E0583]', 'error[E0463]', 'failed to run custom build command',
)
SNIPPET_LINE = re.compile(r'^\s*\d*\s*\|')
COMPILER_KILLED = re.compile(r"process didn't exit successfully: .*\(signal: \d+")


def last_phase_output(log):
    """The output after a scenario log's last command header: a caught
    mutant's last phase is its test phase (cargo-mutants writes each command
    and each result as a line starting with "*** ")."""
    start = 0
    for header in re.finditer(r'^\*\*\* (?!result: ).*$', log, re.MULTILINE):
        start = header.end()
    return log[start:]


def incomplete_reason(listing, outcome):
    """Why a mutant's verdict is incomplete, or None when it stands."""
    summary = outcome['summary']
    if summary == 'Failure':
        return 'cargo-mutants could not classify it'
    if summary not in ('CaughtMutant', 'Unviable'):
        return None
    log = listing / outcome['log_path']
    if not log.is_file():
        return 'its log is missing'
    lines = [line for line in last_phase_output(log.read_text(errors='replace')).splitlines()
             if not SNIPPET_LINE.match(line)]
    for signature in ENVIRONMENT_SIGNATURES:
        if any(signature in line for line in lines):
            return f'environment: {signature}'
    if summary == 'CaughtMutant':
        for signature in INCOMPLETE_SIGNATURES:
            if any(signature in line for line in lines):
                return f'its tests died: {signature.strip()}'
        if not any(line.startswith('test result: FAILED.') for line in lines):
            return 'no failed test result'
        return None
    if any(COMPILER_KILLED.search(line) for line in lines):
        return 'compiler killed by a signal'
    if not any(line.startswith('error: could not compile ') for line in lines):
        return 'no compiler error'
    return None


def incomplete_mutants(listing, listed):
    """Write listing/incomplete.txt from cargo-mutants' outcomes.json, one
    mutant name per line as missed.txt lists them, and return (name, reason,
    log) for each. An owner whose outcomes record another number of mutants
    than the `listed` the driver selected is incomplete as a whole."""
    path = listing / 'outcomes.json'
    recorded = json.loads(path.read_text()) if path.is_file() else {'outcomes': [], 'total_mutants': 0}
    found = []
    if recorded['total_mutants'] != listed:
        found.append((f'outcomes.json records {recorded["total_mutants"]} mutant(s); '
                      f'the driver listed {listed}', 'count mismatch', path))
    for outcome in recorded['outcomes']:
        scenario = outcome['scenario']
        if not (isinstance(scenario, dict) and 'Mutant' in scenario):
            continue
        reason = incomplete_reason(listing, outcome)
        if reason:
            found.append((scenario['Mutant']['name'], reason, listing / outcome['log_path']))
    listing.mkdir(parents=True, exist_ok=True)
    (listing / 'incomplete.txt').write_text(''.join(f'{name}\n' for name, _, _ in found))
    return found


def mutants_tmpdir(out):
    """The TMPDIR cargo-mutants copies the tree into, emptied once a leg.

    The host's temp directory is not the driver's: macOS's daily cleaner of
    /var/folders removed files from a copy that kept the sources' old times
    mid-run (2026-11-10, gate decision of 2026-11-11, H1), so every owner's
    run gets `<out>/tmp`. It must not be a path the copy itself would copy:
    inside the checkout it must be ignored by git (cargo-mutants copies with
    --gitignore true)."""
    tmp = out / 'tmp'
    if tmp.resolve().is_relative_to(ROOT.resolve()) and subprocess.run(
            ['git', 'check-ignore', '-q', str(tmp)], cwd=ROOT).returncode != 0:
        raise ValueError(f'{tmp} is inside the checkout and not ignored by git: cargo-mutants '
                         'would copy its own copies; use an ignored --out such as target/...')
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    return tmp


def run_owner(command, listing, listed, env=None):
    """Run one owner's `listed` mutants; True when the answer is a survivor
    status or a mutant's run was incomplete.

    Every selection, diff-scoped or scheduled, keeps going past survivors, so
    one run reports every owner it selected, once, at the end; an answer that
    measured nothing stops it at once. Each incomplete mutant is printed as its
    owner finishes, so a watcher can stop the leg at the first."""
    status = subprocess.run(command, cwd=ROOT, env=env).returncode
    if status and status not in SURVIVOR_STATUSES:
        raise subprocess.CalledProcessError(status, command)
    incomplete = incomplete_mutants(listing, listed)  # listed whatever the answer
    for name, reason, log in incomplete:
        print(f'INCOMPLETE {name} ({reason}) [{log}]', flush=True)
    return status in SURVIVOR_STATUSES or bool(incomplete)


def report_survivors(survivors, owners):
    print(f'Mutation survivors in {len(survivors)} of {owners} owner(s) of this selection:')
    for name, listing in survivors:
        listed = False
        for kind in SURVIVOR_KINDS:
            path = listing / f'{kind}.txt'
            lines = [line for line in path.read_text().splitlines() if line.strip()] \
                if path.is_file() else []
            if lines:
                listed = True
                print(f'  {name}: {kind} {len(lines)} ({path})')
                for line in lines:
                    print(f'    {line}')
        if not listed:
            print(f'  {name}: no mutant listed in missed.txt, timeout.txt or incomplete.txt '
                  f'under {listing}')


def prepare(out):
    """Plan the selection into `out` and validate it before any discovery:
    (plan, selected owners, scheduled, the night's shares, the canonical and
    harness diffs, None for a scheduled run)."""
    out.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [sys.executable, 'scripts/quality/verification_plan.py', '--out', str(out)],
        cwd=ROOT,
        check=True,
    )
    plan = json.loads((out / 'plan.json').read_text())

    # WC01: registration is a precondition, not a TOTAL==0 fallback. No
    # cargo-mutants discovery or execution occurs before this succeeds.
    validate_sources(ROOT)
    selected = validate_plan(plan)
    scheduled = plan.get('selection_kind') == 'scheduled-owner-rotation'
    # validate_plan proved a scheduled receipt's shares are the packing's.
    shares = plan.get('scheduled_owner_shares', {}) if scheduled else {}
    write_json(out / 'selected-owners.json', {
        'schema': 3,
        'event': plan.get('event', 'local'),
        'owners': [entry.name for entry in selected],
        'owner_shares': shares,
        'changed_sources': plan.get('mutation_source_files', []),
        'discovery_sources': plan.get('mutation_discovery_source_files', []),
        'deleted_critical_files': plan.get('deleted_critical_files', []),
        'renamed_source_files': plan.get('renamed_source_files', []),
        'possible_replacement_files': plan.get('possible_replacement_files', []),
        'deleted_source_dispositions': plan.get('deleted_source_dispositions', []),
    })
    check_tool_version()

    canonical_diff = None if scheduled else out / 'pr.diff'
    harness_diff = None
    if not scheduled:
        base = plan['comparison_revision']
        harness_diff = out / 'harness-pr.diff'
        make_harness_diff(base, harness_diff)
    return plan, selected, scheduled, shares, canonical_diff, harness_diff


def list_owner(entry, out, canonical_diff, harness_diff, share=None):
    """List one owner's mutants (its share of them) into out/<owner>: the
    selection and the diff its run mutates against."""
    output = out / entry.name
    output.mkdir(parents=True, exist_ok=True)
    canonical = output / 'selected.json'
    run_to_file(list_command(entry, canonical_diff, share=share), canonical)
    chosen = selection(canonical)
    mutation_diff = canonical_diff
    if entry.target == 'harness-lib':
        harness = output / 'harness-selected.json'
        run_to_file(list_command(entry, harness_diff, harness=True, share=share), harness)
        mirrored = selection(harness)
        if chosen != mirrored:
            raise ValueError(f'{entry.name}: harness mutation scope differs from canonical source')
        mutation_diff = harness_diff
    return chosen, mutation_diff


def in_diff_deal(out, selected, canonical_diff, harness_diff):
    """Every selected owner listed whole, packed as the push's deal."""
    counts = {entry.name: len(list_owner(entry, out, canonical_diff, harness_diff)[0])
              for entry in selected}
    return mutants_width.deal(counts, selected)


def plan_deal(out):
    """(plan, deal) for the mutants-plan job and `--deal`; the deal is None
    for a scheduled rotation or a plan that runs no mutants."""
    plan, selected, scheduled, _, canonical_diff, harness_diff = prepare(out)
    if scheduled or not plan.get('mutants'):
        return plan, None
    try:
        return plan, in_diff_deal(out, selected, canonical_diff, harness_diff)
    except mutants_width.Refused as refused:
        print(f'mutants deal refused: {refused}')
        raise SystemExit(1) from refused


def group_work(group, scheduled, selected, out, canonical_diff, harness_diff):
    """The (owner, share) pairs this job runs. Without a group, every
    selected owner (whole, or its share of the night). With QUALITY_MUTANT_GROUP
    g/G, the push's deal is packed again here and must be the plan job's
    (QUALITY_MUTANT_DIGEST); otherwise nothing is measured."""
    if group is None:
        return [(entry, None) for entry in selected]
    if scheduled:
        raise ValueError('QUALITY_MUTANT_GROUP deals a push; the nightly uses QUALITY_MUTANT_SHARD')
    if os.environ.get('QUALITY_MUTANT_SHARD', '').strip():
        raise ValueError('set QUALITY_MUTANT_GROUP or QUALITY_MUTANT_SHARD, not both')
    index, count = group
    dealt = in_diff_deal(out, selected, canonical_diff, harness_diff)
    expected = os.environ.get('QUALITY_MUTANT_DIGEST', '').strip()
    if (dealt.width, dealt.digest) != (count, expected):
        print(dealt.describe())
        raise SystemExit(
            f'mutants deal differs from the plan job\'s: {dealt.width} job(s), digest '
            f'{dealt.digest}, against {count} and {expected or "none"}; measured nothing')
    if not dealt.groups:
        return []
    return [(share.owner, (share.part, share.parts) if share.parts > 1 else None)
            for share in dealt.groups[index]]


def execute(out):
    group = mutants_width.parse_group(os.environ.get('QUALITY_MUTANT_GROUP'))
    plan, selected, scheduled, shares, canonical_diff, harness_diff = prepare(out)
    # Test processes inherit it; a test that breaks on the longer path fails
    # the baseline, which stops the leg at once.
    run_env = {**os.environ, 'TMPDIR': str(mutants_tmpdir(out))}
    work = group_work(group, scheduled, selected, out, canonical_diff, harness_diff)

    total = 0
    survivors = []
    for entry, share in work:
        output = out / entry.name
        if share is None and entry.name in shares:
            share = tuple(shares[entry.name])
        chosen, mutation_diff = list_owner(entry, out, canonical_diff, harness_diff, share)
        count = len(chosen)
        if count == 0:
            print(f'{entry.name}: no executable mutants in the selected scope')
            continue
        total += count
        if run_owner(mutation_command(entry, out, output, mutation_diff, share),
                     output / 'mutants.out', count, run_env):
            survivors.append((entry.name, output / 'mutants.out'))

    if total == 0:
        print('No executable mutations in the registered owners of this selection; '
              'no mutation experiment is claimed.')
    else:
        print(f'Mutation verification executed {total} selected mutant(s) across '
              f'{len(work)} owner share(s) of {len(selected)} registered owner(s).')
    if survivors:
        report_survivors(survivors, len(selected))
        raise SystemExit(1)


def measure_sizes(path=SIZES_PATH):
    """Record every registered owner's whole-scope mutant count, listed as the
    scheduled rotation lists it (no diff, no shard); only lists, never tests."""
    if os.environ.get('QUALITY_MUTANT_SHARD', '').strip():
        raise ValueError('sizes are whole-scope counts: unset QUALITY_MUTANT_SHARD')
    validate_sources(ROOT)
    tool = check_tool_version()
    sizes = {}
    for entry in OWNERS:
        listed = subprocess.run(list_command(entry, None), cwd=ROOT, stdout=subprocess.PIPE,
                                text=True, check=True).stdout
        sizes[entry.name] = len(json.loads(listed) if listed.strip() else [])
    write_json(path, {
        'schema': 1,
        'commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'measured_on': datetime.now(timezone.utc).date().isoformat(),
        'tool': tool,
        'command': SIZES_COMMAND,
        'owners': sizes,
    })
    print(f'{path.name}: {sum(sizes.values())} mutant(s) across {len(sizes)} owner(s)')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', default='target/quality-mutations')
    parser.add_argument('--measure-sizes', action='store_true',
                        help=f'list every owner and rewrite {SIZES_PATH.name}')
    parser.add_argument('--deal', action='store_true',
                        help="list only: print the push's deal as CI's mutants-plan job would")
    args = parser.parse_args()
    if args.measure_sizes:
        measure_sizes()
    elif args.deal:
        _, dealt = plan_deal(Path(args.out).resolve())
        print(dealt.describe() if dealt else 'mutants deal: none (scheduled, or no mutants)')
    else:
        execute(Path(args.out).resolve())


if __name__ == '__main__':
    main()
