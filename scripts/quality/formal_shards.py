#!/usr/bin/env python3
"""Split the formal obligations a change selects over CI shards, longest first.

The CI formal job runs exactly what `formal.py run --changed-from <base>` would
run, split over parallel shards. Selection comes first and is the driver's own:
`formal.selection()` (git diff plus assumption-ledger comparison, then
`formal.select()`), imported and never re-implemented, so a shard can only be
handed an obligation the driver itself would run for that base.

The selected obligations are then spread by LPT (longest processing time
first): heaviest first, each goes to the shard with the least work so far.
Ties break by obligation ID and then by shard number, so every shard of a run
computes the same partition independently. An obligation's weight is the sum
of its receipt's recorded `checks[].seconds`; one without a readable receipt
(a new obligation) weighs FALLBACK_SECONDS, so it is placed as if it were long
rather than piled onto an already busy shard.

Sharding by position (sorted index mod N), as the job did before, put 10 to 90
check-minutes on the shards of one full run; LPT over the same work evens them
out, and no obligation can be lost: every call asserts that the shards
partition the selected set exactly.

  formal_shards.py --shards 6 --shard 2 --base <rev>   shard 2's IDs, one per line
  formal_shards.py --shards 6 --shard 2 --all          the same over every obligation
  formal_shards.py --shards 6 --check --all            every shard's IDs and load
"""
import sys

if sys.version_info < (3, 11):
    sys.exit('formal_shards.py needs Python >= 3.11 (formal.py imports tomllib); '
             'on macOS put /opt/homebrew/opt/python@3.12/libexec/bin first on PATH')

import argparse
import heapq
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import formal  # noqa: E402  (selection, manifest and receipt locations)

# Weight of an obligation with no readable receipt yet: an hour, longer than
# all but the heaviest recorded obligations, so a new one gets a shard to itself.
FALLBACK_SECONDS = 3600.0


def receipt_seconds(oid, receipts=None):
    """Sum of the recorded check seconds in `oid`'s receipt, or None."""
    path = Path(receipts or formal.RECEIPTS) / f'{oid}.json'
    try:
        checks = json.loads(path.read_text()).get('checks', [])
        return round(float(sum(check.get('seconds', 0) for check in checks)), 1)
    except (OSError, ValueError, TypeError, AttributeError):
        return None


def weight(oid, receipts=None):
    seconds = receipt_seconds(oid, receipts)
    return FALLBACK_SECONDS if seconds is None else seconds


def weights(ids, receipts=None):
    return {oid: weight(oid, receipts) for oid in ids}


def lpt_order(work):
    """IDs heaviest first; equal weights in ID order."""
    return sorted(work, key=lambda oid: (-work[oid], oid))


def lpt(work, shards):
    """Partition `work` (ID -> seconds) into `shards` lists by LPT.

    Each ID, heaviest first, goes to the least-loaded shard; a load tie goes to
    the lower shard number. Deterministic for a given mapping."""
    if shards < 1:
        raise ValueError('at least one shard is needed')
    partition = [[] for _ in range(shards)]
    loads = [(0.0, index) for index in range(shards)]  # already a valid heap
    for oid in lpt_order(work):
        load, index = heapq.heappop(loads)
        partition[index].append(oid)
        heapq.heappush(loads, (load + work[oid], index))
    return partition


def modulo(ids, shards):
    """The previous scheme: sorted IDs by index mod `shards` (for comparison)."""
    ordered = sorted(ids)
    return [[oid for index, oid in enumerate(ordered) if index % shards == shard]
            for shard in range(shards)]


def loads(partition, work):
    return [sum(work[oid] for oid in shard) for shard in partition]


def makespan(partition, work):
    return max(loads(partition, work), default=0.0)


def partition_problems(partition, selected):
    """Why `partition` is not an exact partition of `selected` (empty when it is)."""
    assigned = [oid for shard in partition for oid in shard]
    problems = []
    duplicates = sorted({oid for oid in assigned if assigned.count(oid) > 1})
    if duplicates:
        problems.append(f'assigned to more than one shard: {", ".join(duplicates)}')
    if lost := sorted(set(selected) - set(assigned)):
        problems.append(f'selected but in no shard: {", ".join(lost)}')
    if extra := sorted(set(assigned) - set(selected)):
        problems.append(f'in a shard but not selected: {", ".join(extra)}')
    return problems


def selected_ids(manifest, base=None, everything=False):
    """Every obligation, or exactly what `formal.py run --changed-from base` runs."""
    if everything:
        return sorted(o['id'] for o in manifest['obligations'])
    return formal.selection(manifest, base)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--shards', type=int, required=True, help='number of shards (N >= 1)')
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument('--shard', type=int, help='print this shard\'s IDs (0 <= K < N)')
    which.add_argument('--check', action='store_true',
                       help='assert the partition and print every shard with its load')
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument('--base', help='select what `formal.py run --changed-from BASE` would run')
    scope.add_argument('--all', action='store_true', help='select every obligation')
    args = parser.parse_args(argv)
    if args.shards < 1:
        parser.error('--shards must be at least 1')
    if args.shard is not None and not 0 <= args.shard < args.shards:
        parser.error(f'--shard must be in [0, {args.shards})')

    selected = selected_ids(formal.load(), args.base, args.all)
    work = weights(selected)
    partition = lpt(work, args.shards)
    problems = partition_problems(partition, selected)
    if problems:  # fail closed: a lost obligation would be a silent coverage gap
        print('\n'.join(f'FORMAL_SHARDS_FAIL: {p}' for p in problems), file=sys.stderr)
        return 1
    if args.check:
        for index, shard in enumerate(partition):
            print(f'shard {index}: {sum(work[o] for o in shard) / 60:6.1f} min  '
                  f'{" ".join(sorted(shard)) or "-"}')
        print(f'FORMAL_SHARDS_OK: {len(selected)} selected obligation(s) in {args.shards} shard(s); '
              f'longest shard {makespan(partition, work) / 60:.1f} min by LPT, '
              f'{makespan(modulo(selected, args.shards), work) / 60:.1f} min by index modulo '
              f'(recorded seconds)')
        return 0
    sys.stdout.write(''.join(f'{oid}\n' for oid in sorted(partition[args.shard])))
    return 0


if __name__ == '__main__':
    sys.exit(main())
