#!/usr/bin/env python3
"""Execute mutation checks from the canonical owner table."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tomllib

from common import ROOT, write_json
from mutation_owners import validate_plan, validate_sources

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


def mutation_command(entry, out, output, diff):
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
                    '--build-timeout', '600', '--gitignore', 'true', '--output', str(output)))
    return command


def list_command(entry, diff, harness=False):
    command = ['cargo', 'mutants', '--list', '--json']
    if diff is not None:
        command.extend(('--in-diff', str(diff)))
    for path in entry.sources:
        command.extend(('--file', f'{HARNESS_PREFIX}{path}' if harness else path))
    command.extend(('--package', 'streams-quality-invariants' if harness else 'streams-slate'))
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


def execute(out):
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
    write_json(out / 'selected-owners.json', {
        'schema': 2,
        'event': plan.get('event', 'local'),
        'owners': [entry.name for entry in selected],
        'changed_sources': plan.get('mutation_source_files', []),
        'discovery_sources': plan.get('mutation_discovery_source_files', []),
        'deleted_critical_files': plan.get('deleted_critical_files', []),
        'renamed_source_files': plan.get('renamed_source_files', []),
        'possible_replacement_files': plan.get('possible_replacement_files', []),
        'deleted_source_dispositions': plan.get('deleted_source_dispositions', []),
    })
    check_tool_version()

    scheduled = plan.get('selection_kind') == 'scheduled-owner-rotation'
    canonical_diff = None if scheduled else out / 'pr.diff'
    harness_diff = None
    if not scheduled:
        base = plan['comparison_revision']
        harness_diff = out / 'harness-pr.diff'
        make_harness_diff(base, harness_diff)

    total = 0
    for entry in selected:
        output = out / entry.name
        output.mkdir(parents=True, exist_ok=True)
        canonical = output / 'selected.json'
        run_to_file(list_command(entry, canonical_diff), canonical)
        chosen = selection(canonical)
        mutation_diff = canonical_diff
        if entry.target == 'harness-lib':
            harness = output / 'harness-selected.json'
            run_to_file(list_command(entry, harness_diff, harness=True), harness)
            mirrored = selection(harness)
            if chosen != mirrored:
                raise ValueError(f'{entry.name}: harness mutation scope differs from canonical source')
            mutation_diff = harness_diff
        count = len(chosen)
        if count == 0:
            print(f'{entry.name}: no executable mutants in the selected scope')
            continue
        total += count
        subprocess.run(mutation_command(entry, out, output, mutation_diff), cwd=ROOT, check=True)

    if total == 0:
        print('No executable mutations in the registered owners of this selection; '
              'no mutation experiment is claimed.')
    else:
        print(f'Mutation verification executed {total} selected mutant(s) across '
              f'{len(selected)} registered owner(s).')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', default='target/quality-mutations')
    args = parser.parse_args()
    execute(Path(args.out).resolve())


if __name__ == '__main__':
    main()
