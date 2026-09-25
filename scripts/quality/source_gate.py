"""Source ratchet shared by the architecture and complete quality entry points."""
import json
from pathlib import Path
import subprocess
from common import digest, ROOT, merge_base, source_path, syntax, tracked_sources, write_json
import source_rules as rules


def previous(base, path):
    result = subprocess.run(['git', 'show', f'{base}:{path}'], cwd=ROOT,
                            text=True, capture_output=True)
    return json.loads(result.stdout) if result.returncode == 0 else None


def base_sources(base):
    """Every parsed Rust source the base tracked, including files this checkout
    deleted or renamed, so a moved exception is compared with the contract it
    left. Unparsed templates, registered at the base or now, are skipped."""
    listing = subprocess.check_output(['git', 'ls-tree', '-r', '-z', '--name-only', base], cwd=ROOT)
    listing = listing.decode(errors='surrogateescape')  # git paths are bytes
    templates = set(json.loads((ROOT / 'docs/quality/syntax-fragments.json').read_text()))
    templates |= set(previous(base, 'docs/quality/syntax-fragments.json') or {})
    return [path for path in listing.split('\0')
            if path.endswith('.rs') and path not in templates and source_path(path)]


def base_facts(prior_sources):
    """The base's parsed sources. A base file that does not parse (a red push
    that this one repairs, in place or by removing the file) is left out: the
    base enforced no contract in it."""
    try:
        return syntax(prior_sources) if prior_sources else {}
    except subprocess.CalledProcessError:
        facts = {}
        for path, text in prior_sources.items():
            try:
                facts |= syntax({path: text})
            except subprocess.CalledProcessError:
                continue
        return facts


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
    prior_sources = {}
    for path in base_sources(base):
        text = subprocess.check_output(['git', 'show', f'{base}:{path}'], cwd=ROOT, text=True)
        prior_lines[path] = len(text.splitlines())
        prior_sources[path] = text
    entries = json.loads((ROOT / 'docs/quality/owners.json').read_text())['occurrences']
    for entry in entries:
        if not entry.get('reason'):
            problems.append(f'owner rationale missing: {entry}')
    # Overlapping registrations describe one ceiling, not additional sites.
    registered = active | rules.from_entries(entries)
    architecture = json.loads((ROOT / 'docs/refactor/architecture-policy.json').read_text())
    problems.extend(rules.violations(sources, facts, before, prior_lines, registered, architecture))
    rows = rules.growth_ledger(json.loads((ROOT / 'docs/quality/exception-growth.json').read_text()))
    prior_facts = base_facts(prior_sources)
    problems.extend(rules.exception_growth(
        rules.exception_contracts(sources, facts),
        rules.exception_contracts(prior_sources, prior_facts),
        rows,
    ))
    current = rules.inventory(facts)
    stale = active - current
    if stale and not prune:
        problems.append(f'{sum(stale.values())} obsolete source allowances; run the quality ratchet with --prune')
    if prune and not problems:
        write_json(active_path, {'schema': 1, 'occurrences': rules.entries(active & current)})
    return problems
