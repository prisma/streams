"""Read lint groups and deny-by-default IDs from the actual pinned compiler."""
from functools import lru_cache
from pathlib import Path
import re
import subprocess
from common import DENIED


def parse_help(help_text):
    groups, denied = set(), set(DENIED)
    in_groups = False
    for line in help_text.splitlines():
        if line.startswith('Lint groups '):
            in_groups = True
        elif line.startswith('Lint checks '):
            in_groups = False
        match = re.match(r'\s*([a-z][a-z0-9_:-]+)\s{2,}(.+)', line)
        if not match:
            continue
        name, rest = match.groups()
        name = name.replace('-', '_')
        if in_groups and name != 'name':
            groups.add(name)
        elif rest.split()[0] in ('deny', 'forbid'):
            denied.add(name)
    if not {'unused', 'warnings', 'clippy::all', 'clippy::correctness'} <= groups:
        raise ValueError('compiler lint-group inventory is incomplete')
    if 'clippy::eq_op' not in denied:
        raise ValueError('compiler denied-lint inventory is incomplete')
    return frozenset(groups), frozenset(denied)


@lru_cache(maxsize=1)
def from_compiler():
    sysroot = subprocess.check_output(['rustc', '--print', 'sysroot'], text=True).strip()
    output = subprocess.check_output([str(Path(sysroot) / 'bin/clippy-driver'), '-W', 'help'], text=True)
    return parse_help(output)
