"""Addendum 70 (d) controls 2 and 4: each mutation owner's baseline, as the driver's cargo-mutants
baseline runs it (cargo test, profile quality, the owner's target and filters), timed per round.
Run from a checkout of the tree under test; this file is copied in by the workflow. Prints
`BASELINE <label> <owner> round <r> exit <code> <seconds>` and a failure's tail. Never lands."""
import argparse, subprocess, sys, time
sys.path.insert(0, 'scripts/quality')
from mutation_owners import OWNERS

def target_args(entry):
    if entry.target in {'service-lib', 'harness-lib'}:
        return ['--lib']
    if entry.target == 'pilot-benchmark':
        return ['--bin=pilot']
    if entry.target == 'pilot-generator':
        return ['--bin=pilot', '--test=pilot_membership']
    raise ValueError(entry.target)

def command(entry, extra=()):
    build = 'target/mb-pilot' if entry.target.startswith('pilot-') else 'target/mb'
    return ['cargo', 'test', '--locked', *target_args(entry), f'--target-dir={build}', '--package', entry.package,
            '--profile', 'quality', *extra, '--', *entry.test_filters]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--shard', required=True)
    parser.add_argument('--rounds', type=int, default=1)
    parser.add_argument('--label', required=True)
    parser.add_argument('--covering', default='',
                        help='comma-separated test paths: only the owners whose filters select one of them')
    args = parser.parse_args()
    k, n = map(int, args.shard.split('/'))
    tests = [t for t in args.covering.split(',') if t]
    owners = [entry for entry in OWNERS if not tests or any(
        token in test for token in entry.test_filters if not token.startswith('-') for test in tests)]
    mine = [entry for index, entry in enumerate(owners) if index % n == k]
    for entry in mine:
        subprocess.run(command(entry, ['--no-run'])[:-1 - len(entry.test_filters)], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
    failed = 0
    for round_ in range(1, args.rounds + 1):
        for entry in mine:
            started = time.monotonic()
            done = subprocess.run(command(entry), capture_output=True, text=True, errors='replace')
            seconds = time.monotonic() - started
            print(f'BASELINE {args.label} {entry.name} round {round_} exit {done.returncode} {seconds:.1f}', flush=True)
            if done.returncode != 0:
                failed += 1
                for line in (done.stdout + done.stderr).splitlines()[-40:]:
                    print(f'  FAILTAIL {entry.name} | {line}')
    print(f'BASELINES {args.label} shard {args.shard}: {len(mine)} owners x {args.rounds} rounds, {failed} failed')

main()
