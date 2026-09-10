#!/usr/bin/env python3
"""Select required invariant checks from the actual PR merge-base diff."""
import argparse
import json
import re
from pathlib import Path
import subprocess
from common import ROOT, merge_base, syntax, write_json
from production_changes import unchanged_production


def plan(paths, visibility_only=(), production_unchanged=(), formatted_visibility=()):
    source = [p for p in paths if p.endswith('.rs')]
    implementation = [p for p in source if p not in set(visibility_only) | set(production_unchanged)]
    tooling = any(p.startswith(('tools/quality-invariants/', 'fuzz/', 'scripts/quality/'))
                  or p in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'quality-tools.toml',
                           '.github/workflows/rust-quality.yml') for p in paths)
    codec_prefixes = ('src/crypto', 'src/postings', 'src/application/read_', 'src/shard/record')
    lifecycle_prefixes = ('src/shard', 'src/tasks', 'src/runtime', 'src/bootstrap', 'src/sse', 'src/bin/pilot/benchmark')
    buffer_prefixes = ('src/retained_bytes', 'src/application/read_', 'src/crypto', 'src/bootstrap', 'src/fleet', 'src/http', 'src/ops')
    codec = any(p.startswith(codec_prefixes) for p in implementation)
    lifecycle = any(p.startswith(lifecycle_prefixes) for p in implementation)
    buffers = any(p.startswith(buffer_prefixes) for p in implementation)
    mutation_source = [p for p in implementation if p not in formatted_visibility]
    mutants = any(p.startswith(codec_prefixes + lifecycle_prefixes + buffer_prefixes) for p in mutation_source)
    return {'compiler': bool(source) or tooling, 'properties_fuzz': codec or tooling,
            'loom': lifecycle or tooling, 'miri': buffers or tooling,
            'mutants': mutants, 'changed_rust_files': source,
            'visibility_only_files': sorted(visibility_only),
            'production_unchanged_files': sorted(production_unchanged),
            'formatted_visibility_files': sorted(formatted_visibility)}


def mask_visibility(source, facts, replacement="<visibility>"):
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
        data = data[:start] + replacement + data[end:]
    return data, [value for _, _, value in spans]


def is_visibility_only(before, after, old_facts, new_facts):
    old_text, old_vis = mask_visibility(before, old_facts)
    new_text, new_vis = mask_visibility(after, new_facts)
    if old_text != new_text or len(old_vis) != len(new_vis):
        return False
    changed = [(old, new) for old, new in zip(old_vis, new_vis) if old != new]
    return bool(changed) and all(old == 'pub' and new in ('pub (crate)', 'pub (super)', 'pub (self)')
                                 for old, new in changed)


def is_formatted_visibility(before, after, old_facts, new_facts):
    """Prove no source mutation site changed, without claiming equal expansion.

    Formatting can move an unrelated derive's spans. Keep its runtime checks,
    but do not request mutations of a signature-only diff with no executable
    edit. A narrowed visibility inside opaque macro input is not eligible.
    """
    if 'tokens' not in old_facts or 'tokens' not in new_facts:
        return False
    old, old_vis = mask_visibility(before, old_facts, 'pub')
    new, new_vis = mask_visibility(after, new_facts, 'pub')
    if len(old_vis) != len(new_vis):
        return False
    changed = [i for i, (a, b) in enumerate(zip(old_vis, new_vis)) if a != b]
    if not changed or any(old_vis[i] != 'pub' or new_vis[i] not in (
            'pub (crate)', 'pub (super)', 'pub (self)') for i in changed):
        return False
    builtins = {'cfg', 'doc', 'repr', 'inline', 'cold', 'must_use', 'deprecated',
                'allow', 'expect', 'warn', 'deny', 'forbid', 'path', 'track_caller',
                'no_mangle', 'export_name', 'link', 'link_name', 'link_section', 'unsafe'}
    sensitive = {'line', 'column', 'file', 'include', 'include_str', 'include_bytes'}
    normalized = []
    for source, parsed in ((before, old_facts), (after, new_facts)):
        offsets = [0, *[i + 1 for i, char in enumerate(source) if char == '\n']]
        def span(node):
            loc = node['location']
            return (offsets[loc['line'] - 1] + loc['column'],
                    offsets[loc['end_line'] - 1] + loc['end_column'])
        visibilities = sorted(span(f) for f in parsed['facts'] if f['kind'] == 'visibility')
        sites = [visibilities[i] for i in changed]
        if any(item['kind'] == 'macro' for item in parsed['items']):
            return False
        for fact in parsed['facts']:
            kind, value = fact['kind'], fact['value']
            if kind in {'macro', 'import-target'} and value.split('::')[-1].strip() in sensitive:
                return False
            if kind == 'macro-tokens' and re.search(
                r'\b(?:line|column|file|include|include_str|include_bytes)\s*!', value
            ):
                return False
            if kind != 'attribute' or re.split(r'[ (=]', value, maxsplit=1)[0] in builtins:
                continue
            start, end = span(fact)
            owners = [span(item) for item in parsed['items']
                      if span(item)[0] <= start and end <= span(item)[1]]
            if not owners:
                return False  # Custom crate-level inner attribute.
            left, right = min(owners, key=lambda pair: pair[1] - pair[0])
            if any(left <= a and b <= right for a, b in sites):
                return False
        edits = [(a, b, 'pub') for a, b in visibilities]
        for fact in parsed['facts']:
            if fact['kind'] != 'parameter-trailing-comma':
                continue
            start, end = span(fact)
            owners = [span(item) for item in parsed['items'] if item['kind'] == 'function'
                      and span(item)[0] <= start and end <= span(item)[1]]
            if not owners:
                return False
            left, right = min(owners, key=lambda pair: pair[1] - pair[0])
            if any(left <= a and b <= right for a, b in sites):
                if source[start:end] != ',':
                    raise ValueError('parameter comma range does not match parsed source')
                edits.append((start, end, ''))
        for start, end, replacement in sorted(edits, reverse=True):
            source = source[:start] + replacement + source[end:]
        normalized.append(source)
    tokens = syntax({'before.rs': normalized[0], 'after.rs': normalized[1]})
    return tokens['before.rs']['tokens'] == tokens['after.rs']['tokens']


def source_changes(base, paths):
    before, after = {}, {}
    existing = set(subprocess.check_output(
        ['git', 'ls-tree', '-r', '--name-only', base], cwd=ROOT, text=True).splitlines())
    for path in paths:
        if not path.endswith('.rs'):
            continue
        # Missing new/deleted files have empty source; real Git read failures
        # propagate rather than being mistaken for a new test-only file.
        before[path] = subprocess.check_output(
            ['git', 'show', f'{base}:{path}'], cwd=ROOT).decode() if path in existing else ''
        after[path] = (ROOT / path).read_bytes().decode() if (ROOT / path).is_file() else ''
    if not before:
        return [], [], []
    old_facts, new_facts = syntax(before), syntax(after)
    visibility = [path for path in before
                  if is_visibility_only(before[path], after[path], old_facts[path], new_facts[path])]
    formatted = [path for path in before if path not in visibility
                 and is_formatted_visibility(before[path], after[path], old_facts[path], new_facts[path])]
    return visibility, unchanged_production(before, after, old_facts, new_facts), formatted


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
    visibility, production, formatted = source_changes(base, paths)
    result = dict(plan(paths, visibility, production, formatted), merge_base=base)
    write_json(out / 'plan.json', result)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
