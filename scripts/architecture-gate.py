#!/usr/bin/env python3
"""Enforce fixed-commit structural budgets and zero-leak extracted owners.

The review baseline is captured once from Git objects, never from the current
working tree. Exceptions name one metric and a bounded limit with an owner,
finding and rationale. A relocated obligation does not create a blanket waiver.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
BASELINE = ROOT / 'docs/refactor/architecture-review-baseline.json'
POLICY = ROOT / 'docs/refactor/architecture-policy.json'
BASE_COMMIT = 'a7e2070f3b4346b3e54d552069ff91c56e900130'
spec = importlib.util.spec_from_file_location('architecture', ROOT / 'scripts/architecture-report.py')
architecture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(architecture)


def git(*args: str) -> str:
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True)


def reverse_edges(source: str) -> dict[str, int]:
    clean = architecture.strip_noncode(source)
    roots = {'crate', 'streams_slate'}
    roots.update(re.findall(r'\buse\s+(?:crate|(?:super\s*::\s*)*super)\s+as\s+(\w+)\s*;', clean))
    roots.update(re.findall(r'\b((?:super\s*::\s*)*super)\s*::', clean))
    counts = Counter()
    for root in roots:
        counts.update(re.findall(r'\b' + re.escape(root) + r'\s*::\s*(http|product)\b', clean))
        # Braced root imports, including renamed modules. Masking prevents
        # comments and literal text from satisfying or hiding a dependency.
        for match in re.finditer(r'\buse\s+' + re.escape(root) + r'\s*::\s*\{', clean):
            depth, end = 1, match.end()
            while end < len(clean) and depth:
                depth += (clean[end] == '{') - (clean[end] == '}')
                end += 1
            group = clean[match.end():end-1]
            counts.update(re.findall(r'(?:^|,)\s*(http|product)\b', group))
    return dict(sorted(counts.items()))


def metric_source(path: str, source: str) -> dict:
    if architecture.is_test_only(ROOT / path, source):
        return {'test_only': True, 'lines': len(source.splitlines()), 'functions': {}, 'edges': {}}
    clean = architecture.strip_noncode(source)
    functions = {}
    for name, start, end in architecture.find_functions(clean):
        length = end - start + 1
        if length > architecture.FUNCTION_BUDGET:
            functions[name] = max(functions.get(name, 0), length)
    return {'test_only': False, 'lines': len(source.splitlines()),
            'functions': functions, 'edges': reverse_edges(source)}


def current_sources() -> dict[str, str]:
    return {str(p.relative_to(ROOT)): p.read_text() for p in sorted((ROOT / 'src').rglob('*.rs'))}


def adapter_http_symbols(source: str) -> set[str]:
    """A transport adapter may import reviewed wire types, never core callbacks."""
    clean = architecture.strip_noncode(source)
    symbols = set(re.findall(r'\bcrate\s*::\s*http\s*::\s*(\w+)', clean))
    for group in re.findall(r'\buse\s+crate\s*::\s*http\s*::\s*\{([^{}]+)\}', clean):
        symbols.update(re.findall(r'(?:^|,)\s*(\w+)', group))
    if re.search(r'\buse\s+crate\s*::\s*http\s*(?:as\b|;)|\bcrate\s*::\s*http\s*::\s*\*', clean):
        symbols.add('<module-or-wildcard-import>')
    return symbols


def capture() -> dict:
    files = [p for p in git('ls-tree', '-r', '--name-only', BASE_COMMIT, '--', 'src').splitlines() if p.endswith('.rs')]
    sources = {p: git('show', f'{BASE_COMMIT}:{p}') for p in files}
    return {'commit': BASE_COMMIT, 'schema': 1,
            'source_sha256': {p: hashlib.sha256(s.encode()).hexdigest() for p, s in sources.items()},
            'metrics': {p: metric_source(p, s) for p, s in sources.items()}}


def violations(sources: dict[str, str], baseline: dict, policy: dict) -> list[str]:
    failures = []
    exceptions = policy.get('budget_exceptions', {})
    for key, entry in exceptions.items():
        if not all(entry.get(field) for field in ('owner', 'finding', 'rationale', 'source_obligation')):
            failures.append(f'incomplete budget exception: {key}')
        if not isinstance(entry.get('limit'), int) or entry['limit'] <= 0:
            failures.append(f'invalid budget exception limit: {key}')
    for path, source in sources.items():
        clean = architecture.strip_noncode(source)
        now = metric_source(path, source)
        before = baseline.get('metrics', {}).get(path, {'lines': 1000, 'functions': {}, 'edges': {}})
        hard_owner = path.startswith('src/application/') or path in policy['sse_core_files']
        if hard_owner:
            for edge in now['edges']:
                failures.append(f'extracted owner reverse dependency: {path} -> crate::{edge}')
            if re.search(r'\bAppState\b|\baxum\s*::', clean):
                failures.append(f'extracted owner transport/state leak: {path}')
            # An aliased HTTP response import is a leak even without an axum::
            # prefix. reqwest::StatusCode in a peer adapter is not such a leak.
            if re.search(r'\b(?:HeaderMap|Response)\b', clean):
                failures.append(f'extracted owner protocol response type: {path}')
        if now['test_only']:
            continue
        if path in policy.get('adapter_http_exports', {}):
            extra = adapter_http_symbols(source) - set(policy['adapter_http_exports'][path])
            if extra:
                failures.append(f'transport adapter imported unreviewed HTTP capability: {path}: {sorted(extra)}')
        if path not in policy['transport_and_composition_files']:
            for edge, count in now['edges'].items():
                if count > before.get('edges', {}).get(edge, 0):
                    failures.append(f'reverse dependency growth: {path} -> crate::{edge}: {count} > {before.get("edges", {}).get(edge, 0)}')
        file_key = f'file:{path}'
        limit = exceptions.get(file_key, {}).get('limit', max(1000, before.get('lines', 0)))
        if now['lines'] > limit:
            failures.append(f'file budget growth: {path}: {now["lines"]} > {limit}')
        for name, count in now['functions'].items():
            key = f'function:{path}::{name}'
            limit = exceptions.get(key, {}).get('limit', max(200, before.get('functions', {}).get(name, 0)))
            if count > limit:
                failures.append(f'function budget growth: {path}::{name}: {count} > {limit}')
    return failures


def self_test() -> None:
    assert reverse_edges('// crate::http::AppState\nlet x="crate::product::perr";') == {}
    assert reverse_edges('use crate::http as boundary; boundary::AppState;') == {'http': 1}
    assert reverse_edges('use crate::{http as boundary, product::{self, X}};') == {'http': 1, 'product': 1}
    assert reverse_edges('use crate as root; use root::http::AppState;') == {'http': 1}
    assert reverse_edges('use crate::product_cursor::MessageId;') == {}
    assert reverse_edges('use super::super::{http as boundary};') == {'http': 1}
    assert reverse_edges('use super::super as root; root::product::X;') == {'product': 1}
    assert reverse_edges('streams_slate::http::AppState;') == {'http': 1}
    assert adapter_http_symbols('use crate::http::{AppState as State, err_resp}; crate::http::StartPos;') == {'AppState', 'err_resp', 'StartPos'}
    assert '<module-or-wildcard-import>' in adapter_http_symbols('use crate::http as transport;')
    p = {'budget_exceptions': {}, 'sse_core_files': ['src/sse/feed.rs'], 'transport_and_composition_files': []}
    baseline = {'metrics': {'src/old.rs': {'lines': 1100, 'functions': {}, 'edges': {'http': 1}}}}
    assert not violations({'src/old.rs': 'use crate::http::T;\n' + '\n'*1099}, baseline, p)
    assert violations({'src/old.rs': 'use crate::http::T;\nuse crate::http::U;'}, baseline, p)
    assert violations({'src/old.rs': '\n'*1101}, baseline, p)
    assert violations({'src/application/new.rs': 'use crate::{http as wire};'}, baseline, p)
    assert violations({'src/sse/feed.rs': 'fn x(state: AppState) {}'}, baseline, p)
    assert violations({'src/application/new.rs': 'fn x() -> Response {}'}, baseline, p)
    assert violations({'src/new.rs': 'fn huge(){\n'+'\n'*200+'}'}, baseline, p)
    p['budget_exceptions']['file:src/new.rs'] = {'limit': 1001, 'owner': 'test', 'finding': 'R24', 'rationale': 'controlled exception', 'source_obligation': 'fixture'}
    assert not violations({'src/new.rs': '\n'*1001}, baseline, p)
    assert violations({'src/new.rs': '\n'*1002}, baseline, p)
    p['adapter_http_exports'] = {'src/sse/session.rs': ['AppState']}
    p['transport_and_composition_files'] = ['src/sse/session.rs']
    assert not violations({'src/sse/session.rs': 'use crate::http::AppState;'}, baseline, p)
    assert violations({'src/sse/session.rs': 'use crate::http::{AppState, read_core};'}, baseline, p)
    print('architecture-gate self-test: OK (21 controls)')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture-review-baseline', action='store_true')
    parser.add_argument('--self-test', action='store_true')
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--json', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        self_test(); return 0
    if args.capture_review_baseline:
        if BASELINE.exists():
            parser.error('the fixed review baseline already exists; never renormalize it')
        BASELINE.write_text(json.dumps(capture(), indent=2)+'\n'); return 0
    baseline, policy = json.loads(BASELINE.read_text()), json.loads(POLICY.read_text())
    if baseline.get('commit') != BASE_COMMIT or policy.get('baseline_commit') != BASE_COMMIT:
        print('architecture-gate: baseline commit mismatch'); return 1
    # Pin the baseline payload itself, so editing a historic metric requires an
    # explicit policy change rather than a quiet baseline regeneration.
    digest = hashlib.sha256(BASELINE.read_bytes()).hexdigest()
    if digest != policy.get('baseline_sha256'):
        print('architecture-gate: baseline content hash mismatch'); return 1
    sources = current_sources()
    problems = violations(sources, baseline, policy)
    if args.json:
        print(json.dumps({'baseline_commit': BASE_COMMIT, 'failures': problems,
                          'metrics': {p: metric_source(p, s) for p, s in sources.items()}}, indent=2))
    else:
        for problem in problems: print(problem)
        print(f'architecture-gate: {"FAIL" if problems else "OK"} ({len(sources)} Rust files; fixed baseline {BASE_COMMIT[:12]})')
    return bool(problems)

if __name__ == '__main__':
    raise SystemExit(main())
