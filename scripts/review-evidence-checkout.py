#!/usr/bin/env python3
"""Run the actual provenance gate in CI-shaped fresh Git checkouts.

Depth one is a required negative control; full ancestor history must pass.
No source anchor is rebased, substituted or skipped in either checkout.
"""
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent


def git(*args, cwd=ROOT):
    return subprocess.check_output(['git', *args], cwd=cwd, text=True).strip()


def main():
    workflow = (ROOT / '.github/workflows/ci.yml').read_text()
    job = workflow.split('  architecture-report:', 1)[1].split('\n  rust:', 1)[0]
    assert re.search(r'uses: actions/checkout@v4\n\s+with:\n(?:\s+#.*\n)*\s+fetch-depth: 0\b', job), \
        'architecture-report must check out the full provenance history'
    revision = git('rev-parse', 'HEAD')
    source = ROOT.as_uri()
    with tempfile.TemporaryDirectory(prefix='streams-provenance-') as directory:
        for depth in (1, 0):
            checkout = Path(directory) / f'depth-{depth}'
            git('init', '--quiet', str(checkout))
            fetch = ['fetch', '--quiet'] + (['--depth=1'] if depth else [])
            git(*fetch, source, revision, cwd=checkout)
            git('checkout', '--detach', '--quiet', 'FETCH_HEAD', cwd=checkout)
            assert git('rev-parse', 'HEAD', cwd=checkout) == revision
            assert git('rev-parse', '--is-shallow-repository', cwd=checkout) == str(bool(depth)).lower()
            result = subprocess.run([sys.executable, 'scripts/review-evidence.py', '--check'],
                                    cwd=checkout, text=True, capture_output=True)
            if depth:
                assert result.returncode != 0, 'shallow clone silently accepted missing provenance'
                assert 'missing historical provenance:' in result.stdout, result.stdout + result.stderr
                assert 'source inventory: FAIL' in result.stdout
                assert 'Traceback' not in result.stderr, result.stderr
                print('depth=1: missing-provenance refusal PASS')
            else:
                assert result.returncode == 0, result.stdout + result.stderr
                assert 'source inventory: OK' in result.stdout
                print(f'depth=0: anchored comparisons PASS at {revision}')
    print('fresh-checkout evidence integration: OK (2 real Git checkouts)')


if __name__ == '__main__':
    main()
