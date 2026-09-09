#!/usr/bin/env python3
"""Check the adoption ratchets. --prune can only remove obsolete allowances."""
import argparse
import json
from pathlib import Path
import subprocess
from common import diagnostic_paths, ROOT, ANCHOR, digest, git, merge_base, syntax, tracked_sources, write_json
import config
import diagnostics
import source_rules
import source_gate


def prior(base, path):
    result = subprocess.run(['git', 'show', f'{base}:{path}'], cwd=ROOT,
                            text=True, capture_output=True)
    return json.loads(result.stdout) if result.returncode == 0 else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--clippy', required=True)
    parser.add_argument('--prune', action='store_true')
    args = parser.parse_args()
    problems = config.check()
    policy = json.loads((ROOT / 'docs/quality/policy.json').read_text())
    for path, expected in policy['immutable_sha256'].items():
        if digest((ROOT / path).read_text()) != expected:
            problems.append(f'adoption baseline changed: {path}')
    genesis_path, allowances_path = diagnostic_paths()
    genesis = json.loads(genesis_path.read_text())
    if genesis['commit'] != ANCHOR:
        problems.append('legacy source anchor changed')
    ceiling = diagnostics.inventory(genesis['warnings'])
    allowed = diagnostics.inventory(json.loads(allowances_path.read_text())['warnings'])
    problems.extend(diagnostics.compare(allowed, ceiling))
    base = merge_base()
    previous = prior(base, str(allowances_path.relative_to(ROOT)))
    if previous:
        problems.extend(diagnostics.compare(allowed, diagnostics.inventory(previous['warnings'])))
    # Once adopted, immutable inventories must also match the actual merge base.
    for path in policy['immutable_sha256']:
        previous_data = prior(base, path)
        if previous_data is not None and previous_data != json.loads((ROOT / path).read_text()):
            problems.append(f'normal PR cannot regenerate adoption inventory: {path}')
    sources = tracked_sources()
    facts = syntax(sources)
    metrics = {}
    now, errors = diagnostics.parse(args.clippy, sources, facts, metrics=metrics)
    problems.extend(diagnostics.metric_growth(metrics, diagnostics.metric_limits(json.loads(allowances_path.read_text())['warnings'])))
    problems.extend(errors)
    problems.extend(diagnostics.compare(now, allowed))
    stale = allowed - now
    if stale and not args.prune:
        problems.append(f'{sum(stale.values())} obsolete warning allowances; rerun with --prune')
    problems.extend(source_gate.check(sources, facts, prune=args.prune and not problems))
    if args.prune and not problems:
        write_json(allowances_path, {'schema': 1, 'warnings': diagnostics.records(now, metrics)})
    report = {'anchor': ANCHOR, 'merge_base': base, 'warnings': sum(now.values()),
              'identities': len(now), 'obsolete': sum(stale.values()),
              'syntax_templates': list(json.loads((ROOT / 'docs/quality/syntax-fragments.json').read_text())),
              'source_occurrences': source_rules.entries(source_rules.inventory(facts)),
              'failures': problems}
    output = Path(args.clippy).with_suffix('.quality.json')
    write_json(output, report)
    for problem in problems:
        print(problem)
    print(f'quality ratchets: {"FAIL" if problems else "OK"}; {len(sources)} Rust files; '
          f'{sum(now.values())} legacy warning occurrences; base {base[:12]}')
    return bool(problems)


if __name__ == '__main__':
    raise SystemExit(main())
