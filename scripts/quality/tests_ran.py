#!/usr/bin/env python3
"""Fail closed unless a cargo test log shows the tests that were meant to run.

`cargo test <filter> -- --exact <name>` exits 0 and prints `test result: ok.
0 passed` when nothing matches, so an exit code or a grep for `test result: ok`
lets a renamed test silently drop the gate leg that named it. Every result line
must read ok, the passed counts must reach the floor, and every --exact name
must have its own `test <name> ... ok` line.
"""
import argparse
import json
import re
import sys
from pathlib import Path

RESULT = re.compile(r'^test result: (\S+)\. (\d+) passed; (\d+) failed;', re.MULTILINE)


def problems(log, floor=1, exact=()):
    found = []
    results = RESULT.findall(log)
    if not results:
        return ['no `test result:` line: the test binary did not report']
    for status, _passed, failed in results:
        if status != 'ok' or failed != '0':
            found.append(f'a test binary reported {status} with {failed} failed')
    passed = sum(int(count) for _status, count, _failed in results)
    if passed < floor:
        found.append(f'{passed} test(s) passed; at least {floor} must run '
                     '(a filter or --exact name that matches nothing still exits 0)')
    for name in exact:
        if not re.search(rf'^test {re.escape(name)} \.\.\. ok$', log, re.MULTILINE):
            found.append(f'`test {name} ... ok` is missing: renamed, moved or filtered out')
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('log', type=Path)
    parser.add_argument('--min', type=int, default=1, dest='floor')
    parser.add_argument('--exact', action='append', default=[])
    parser.add_argument('--inventory', type=Path,
                        help='raise the floor to the number of inventoried tests')
    parser.add_argument('--skipped', type=int, default=0,
                        help='inventoried tests this leg skips by design')
    args = parser.parse_args()
    floor = args.floor
    if args.inventory is not None:
        floor = max(floor, len(json.loads(args.inventory.read_text())) - args.skipped)
    found = problems(args.log.read_text(errors='replace'), floor, args.exact)
    for problem in found:
        print(f'TESTS_RAN_FAIL: {args.log}: {problem}', file=sys.stderr)
    if found:
        return 1
    print(f'TESTS_RAN_OK: {args.log}: floor {floor}, exact {len(args.exact)}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
