#!/usr/bin/env python3
"""Select required invariant checks from the actual PR merge-base diff."""
import argparse
import json
from pathlib import Path
import subprocess
from common import ROOT, merge_base, syntax, write_json


def plan(paths, visibility_only=()):
    source = [p for p in paths if p.endswith('.rs')]
    implementation = [p for p in source if p not in visibility_only]
    tooling = any(p.startswith(('tools/quality-invariants/', 'fuzz/', 'scripts/quality/'))
                  or p in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'quality-tools.toml',
                           '.github/workflows/rust-quality.yml') for p in paths)
    codec = any(p.startswith(('src/crypto', 'src/postings', 'src/application/read_', 'src/shard/record')) for p in implementation)
    quota = any(p.startswith('src/quota') for p in implementation)
    lifecycle = any(p.startswith(('src/shard', 'src/tasks', 'src/runtime', 'src/bootstrap', 'src/sse')) for p in implementation)
    buffers = any(p.startswith(('src/retained_bytes', 'src/application/read_', 'src/crypto', 'src/bootstrap', 'src/fleet', 'src/http', 'src/ops')) for p in implementation)
    return {'compiler': bool(source) or tooling, 'properties_fuzz': codec or quota or tooling,
            'loom': lifecycle or tooling, 'miri': buffers or tooling,
            'mutants': codec or quota or lifecycle or buffers, 'changed_rust_files': source,
            'visibility_only_files': sorted(visibility_only)}


def mask_visibility(source, facts):
    """Mask only actual syn Visibility nodes; literals/macros remain byte-exact."""
    # proc-macro2's source columns count Unicode characters, not UTF-8 bytes.
    data = source
    offsets = [0, *[index + 1 for index, char in enumerate(data) if char == '\n']]
    spans = []
    for fact in facts['facts']:
        if fact['kind'] == 'visibility':
            loc = fact['location']
            start = offsets[loc['line'] - 1] + loc['column']
            end = offsets[loc['end_line'] - 1] + loc['end_column']
            if not data[start:end].startswith('pub'):
                raise ValueError('visibility span does not match the parsed source')
            spans.append((start, end, fact['value']))
    spans.sort()
    for start, end, _ in reversed(spans):
        data = data[:start] + '<visibility>' + data[end:]
    return data, [value for _, _, value in spans]


def is_visibility_only(before, after, old_facts, new_facts):
    old_text, old_vis = mask_visibility(before, old_facts)
    new_text, new_vis = mask_visibility(after, new_facts)
    if old_text != new_text or len(old_vis) != len(new_vis):
        return False
    changed = [(old, new) for old, new in zip(old_vis, new_vis) if old != new]
    return bool(changed) and all(old == 'pub' and new in ('pub (crate)', 'pub (super)', 'pub (self)')
                                 for old, new in changed)


def visibility_changes(base, paths):
    before, after = {}, {}
    for path in paths:
        if not path.endswith('.rs') or not (ROOT / path).is_file():
            continue
        old = subprocess.run(['git', 'show', f'{base}:{path}'], cwd=ROOT, capture_output=True)
        if old.returncode == 0:
            before[path], after[path] = old.stdout.decode(), (ROOT / path).read_bytes().decode()
    if not before:
        return []
    old_facts, new_facts = syntax(before), syntax(after)
    return [path for path in before if is_visibility_only(before[path], after[path], old_facts[path], new_facts[path])]


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
    result = dict(plan(paths, visibility_changes(base, paths)), merge_base=base)
    write_json(out / 'plan.json', result)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
