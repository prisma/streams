#!/usr/bin/env python3
"""The mutants job's deal for one run (gate decision of 2026-11-11, H2, by
Fable under the owner's delegation of gate-selection decisions).

A push's (or a pull request's) in-diff mutants are dealt by owner. Each owner
is listed with the driver's own command. An owner without mutants is dropped.
The rest are packed by modeled cost into groups, one job each, with the
nightly's packer (mutation_owners.schedule_groups) and the push's cost model:
- 9.5 minutes of baseline per owner in a job (491-563 s of build plus 0-43 s
  of test on the 4-vCPU runner, run 42806184646);
- 5.1 minutes a mutant;
- 1.0 and 0.25 for harness-lib owners;
- every job's own 50 minutes (setup, the cold build) outside its cap.
An owner too large for the cap is split into round-robin parts. The cap is the
first of CAPS whose packing takes at most MAX_JOBS groups, and the width is
the number of groups. A selection the last cap cannot fit is refused: split
the push. Dealing by mutant (cargo-mutants --shard k/n per owner) gave job 0
one mutant of every owner and so every owner's baseline: layout 8's push
(384 mutants, 55 owners, 40 jobs) had five jobs cancelled at 360 minutes.

The nightly rotation keeps its own model and eight runners
(QUALITY_MUTANT_SHARD=k/8), and a plan that runs no mutants takes one job.
Each job of a push gets QUALITY_MUTANT_GROUP=g/G and the deal's digest
(QUALITY_MUTANT_DIGEST). The driver lists and packs again, and measures
nothing unless both agree. `mutation_driver.py --deal` prints the same deal
locally.
"""
import argparse
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path

from mutation_owners import OWNERS, SCHEDULE_JOBS, CostModel, owner_shares, schedule_groups

PUSH_MODEL = CostModel(1, (9.5, 5.1), (('harness-lib', (1.0, 0.25)),))
JOB_FIXED_MINUTES = 50.0
CAPS = (120.0, 150.0, 180.0, 210.0, 250.0)
MAX_JOBS = 48
NIGHT_JOBS = SCHEDULE_JOBS


class Refused(ValueError):
    """The selection does not fit MAX_JOBS jobs of the last cap."""


@dataclass(frozen=True)
class Deal:
    """A push's owners packed into groups (none when nothing has a mutant)."""

    counts: tuple[tuple[str, int], ...]
    cap: float | None
    groups: tuple

    @property
    def width(self):
        return max(1, len(self.groups))

    def minutes(self, group):
        """The modeled minutes of one job, its fixed cost included."""
        return JOB_FIXED_MINUTES + sum(share.minutes for share in group)

    @property
    def digest(self):
        """What a job checks it was dealt: the counts and the grouping."""
        blob = json.dumps({
            'counts': list(self.counts),
            'groups': [[[share.owner.name, share.part, share.parts] for share in group]
                       for group in self.groups],
        }, sort_keys=True).encode()
        return hashlib.sha256(blob).hexdigest()[:16]

    def describe(self):
        mutants = sum(count for _, count in self.counts)
        if not self.groups:
            return f'mutants deal: no in-diff mutant in {len(self.counts)} owner(s); width 1'
        lines = [f'mutants deal: {mutants} mutant(s) of {len(self.counts)} owner(s) in '
                 f'{self.width} job(s) under a {self.cap:g}-minute cap, digest {self.digest}']
        for index, group in enumerate(self.groups):
            owners = ', '.join(f'{share.owner.name}' + (f' {share.part}/{share.parts}'
                                                        if share.parts > 1 else '')
                               for share in group)
            lines.append(f'  job {index}: {self.minutes(group):.1f} min: {owners}')
        return '\n'.join(lines)


def deal(counts, owners=OWNERS):
    """Pack `counts` ({owner: in-diff mutants}) into the fewest-cap deal."""
    unknown = sorted(set(counts) - {entry.name for entry in owners})
    if unknown:
        raise ValueError(f'unknown mutation owner(s) in the counts: {unknown}')
    entries = tuple(entry for entry in owners if counts.get(entry.name, 0) > 0)
    kept = tuple((entry.name, counts[entry.name]) for entry in entries)
    if not entries:
        return Deal(kept, None, ())
    sizes = dict(kept)
    for cap in CAPS:
        # The packer's own lower bound: a cap it cannot meet is not packed.
        shares = [share for entry in entries
                  for share in owner_shares(entry, sizes[entry.name], cap, PUSH_MODEL)]
        total = sum(share.minutes for share in shares)
        jobs = max(math.ceil(total / cap), *(share.parts for share in shares))
        if jobs > MAX_JOBS:
            continue
        groups = schedule_groups(entries, sizes, cap, PUSH_MODEL)
        jobs = len(groups)
        if jobs <= MAX_JOBS:
            return Deal(kept, cap, groups)
    raise Refused(
        f'{sum(sizes.values())} mutant(s) of {len(entries)} owner(s) need at least {jobs} jobs '
        f'of {CAPS[-1]:g} net minutes ({total:.0f} modeled minutes), more than {MAX_JOBS}: '
        'split the push')


def parse_group(spec):
    """QUALITY_MUTANT_GROUP's g/G, or None when unset."""
    spec = (spec or '').strip()
    if not spec:
        return None
    g, _, n = spec.partition('/')
    if not (g.isdigit() and n.isdigit() and int(g) < int(n)):
        raise ValueError(f'invalid QUALITY_MUTANT_GROUP {spec!r}: expected g/G with g < G')
    return int(g), int(n)


def outputs(width, digest):
    return {'width': str(width), 'shards': json.dumps(list(range(width))), 'digest': digest}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', default='target/quality-plan')
    args = parser.parse_args()
    import mutation_driver  # imports this module: only main needs the driver

    plan, dealt = mutation_driver.plan_deal(Path(args.out).resolve())
    if dealt is None:
        scheduled = plan.get('selection_kind') == 'scheduled-owner-rotation'
        width = NIGHT_JOBS if scheduled else 1
        print(f'mutants width: {width} job(s) ({"scheduled rotation" if scheduled else "no mutants"})')
        values = outputs(width, '')
    else:
        print(dealt.describe())
        values = outputs(dealt.width, dealt.digest)
    output = os.environ.get('GITHUB_OUTPUT')
    if output:
        with open(output, 'a') as out:
            out.write(''.join(f'{key}={value}\n' for key, value in values.items()))


if __name__ == '__main__':
    main()
