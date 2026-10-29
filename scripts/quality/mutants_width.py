#!/usr/bin/env python3
"""The mutants job's width for one run (round 7 D2.1, owner-reserved and
decided by the lead under the owner's delegation; built 2026-10-28 for
layout 6A's cutover push, batch 4 S14).

A push's mutation leg is dealt over `width` jobs of 360 min (GitHub's hosted
maximum), each testing every width-th mutant of the selection (cargo-mutants
--shard, round-robin). One job fits about 110 mutants, so a push takes
ceil(mutants / 110) jobs, at least 8 (the width before this change) and at
most 48. The selection's size is the sum of its owners' measured sizes
(`mutation-owner-sizes.json`): whole-owner counts, an upper bound on a
diff-scoped selection. The nightly rotation stays at 8: its groups are sized
for eight jobs (mutation_owners.schedule_groups).
"""
import argparse
import json
import math
import os
from pathlib import Path

from mutation_owners import owner_sizes

PER_JOB = 110
MIN_JOBS = 8
MAX_JOBS = 48


def width(mutants, scheduled):
    """Jobs for a selection of `mutants`; 8 for the nightly rotation."""
    if scheduled:
        return MIN_JOBS
    return max(MIN_JOBS, min(MAX_JOBS, math.ceil(mutants / PER_JOB)))


def selected_mutants(plan, sizes):
    """The measured mutants of the owners `plan` selects (none when the plan
    runs no mutants)."""
    if not plan.get('mutants'):
        return 0
    return sum(sizes[name] for name in plan['selected_mutation_owners'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', required=True)
    parser.add_argument('--event', default=os.environ.get('GITHUB_EVENT_NAME', 'local'))
    args = parser.parse_args()
    plan = json.loads(Path(args.plan).read_text())
    mutants = selected_mutants(plan, owner_sizes())
    jobs = width(mutants, args.event == 'schedule')
    print(f'mutants width: {jobs} jobs for {mutants} measured mutants ({args.event})')
    output = os.environ.get('GITHUB_OUTPUT')
    if output:
        with open(output, 'a') as out:
            out.write(f'width={jobs}\nshards={json.dumps(list(range(jobs)))}\n')


if __name__ == '__main__':
    main()
