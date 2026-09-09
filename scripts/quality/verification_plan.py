#!/usr/bin/env python3
"""Select required invariant checks from the actual PR merge-base diff."""
import argparse
import json
from pathlib import Path
import subprocess
from common import ROOT, merge_base, write_json


def plan(paths):
    source = [p for p in paths if p.endswith('.rs')]
    tooling = any(p.startswith(('tools/quality-invariants/', 'fuzz/', 'scripts/quality/'))
                  or p in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'quality-tools.toml',
                           '.github/workflows/rust-quality.yml') for p in paths)
    codec = any(p.startswith(('src/crypto', 'src/postings', 'src/application/read_', 'src/shard/record')) for p in source)
    lifecycle = any(p.startswith(('src/shard', 'src/tasks', 'src/runtime', 'src/bootstrap', 'src/sse')) for p in source)
    buffers = any(p.startswith(('src/retained_bytes', 'src/application/read_', 'src/crypto', 'src/bootstrap', 'src/fleet', 'src/http', 'src/ops')) for p in source)
    return {'compiler': bool(source) or tooling, 'properties_fuzz': codec or tooling,
            'loom': lifecycle or tooling, 'miri': buffers or tooling,
            'mutants': codec or lifecycle or buffers, 'changed_rust_files': source}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    base = merge_base()
    # Includes working changes during local development; CI has a clean checkout.
    diff = subprocess.check_output(['git', 'diff', '--no-ext-diff', '--binary', base, '--'], cwd=ROOT)
    (out / 'pr.diff').write_bytes(diff)
    paths = subprocess.check_output(['git', 'diff', '--name-only', base, '--'], cwd=ROOT, text=True).splitlines()
    result = dict(plan(paths), merge_base=base)
    write_json(out / 'plan.json', result)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
