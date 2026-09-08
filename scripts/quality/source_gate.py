"""Source ratchet shared by the architecture and complete quality entry points."""
import json
from pathlib import Path
import subprocess
from common import digest, ROOT, merge_base, syntax, tracked_sources, write_json
import source_rules as rules


def previous(base, path):
    result = subprocess.run(['git', 'show', f'{base}:{path}'], cwd=ROOT,
                            text=True, capture_output=True)
    return json.loads(result.stdout) if result.returncode == 0 else None


def check(sources=None, facts=None, prune=False):
    sources = tracked_sources() if sources is None else sources
    facts = syntax(sources) if facts is None else facts
    policy = json.loads((ROOT / 'docs/quality/policy.json').read_text())
    immutable_errors = [f'adoption baseline changed: {path}'
                        for path, expected in policy['immutable_sha256'].items()
                        if digest((ROOT / path).read_text()) != expected]
    legacy = json.loads((ROOT / 'docs/quality/legacy-source.json').read_text())
    active_path = ROOT / 'docs/quality/source-allowances.json'
    active = rules.from_entries(json.loads(active_path.read_text())['occurrences'])
    ceiling = rules.from_entries(legacy['occurrences'])
    problems = immutable_errors + [f'legacy source allowance grew: {identity}' for identity, n in active.items() if n > ceiling[identity]]
    base = merge_base()
    prior = previous(base, 'docs/quality/source-allowances.json')
    if prior:
        old = rules.from_entries(prior['occurrences'])
        problems.extend(f'removed source debt returned: {identity}' for identity, n in active.items() if n > old[identity])
    before = legacy['lines'].copy()
    for path, entry in policy['adoption_line_additions'].items():
        if not all(entry.get(k) for k in ('owner', 'reason', 'extra_lines')):
            problems.append(f'incomplete bounded line exception: {path}')
        before[path] += entry['extra_lines']
    prior_lines = {}
    if prior is not None:
        for path in sources:
            result = subprocess.run(['git', 'show', f'{base}:{path}'], cwd=ROOT,
                                    text=True, capture_output=True)
            if result.returncode == 0:
                prior_lines[path] = len(result.stdout.splitlines())
    registered = active.copy()
    entries = json.loads((ROOT / 'docs/quality/owners.json').read_text())['occurrences']
    for entry in entries:
        if not entry.get('reason'):
            problems.append(f'owner rationale missing: {entry}')
    registered.update(rules.from_entries(entries))
    architecture = json.loads((ROOT / 'docs/refactor/architecture-policy.json').read_text())
    problems.extend(rules.violations(sources, facts, before, prior_lines, registered, architecture))
    current = rules.inventory(facts)
    stale = active - current
    if stale and not prune:
        problems.append(f'{sum(stale.values())} obsolete source allowances; run the quality ratchet with --prune')
    if prune and not problems:
        write_json(active_path, {'schema': 1, 'occurrences': rules.entries(active & current)})
    return problems
