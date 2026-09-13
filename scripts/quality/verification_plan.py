#!/usr/bin/env python3
"""Select invariant checks from an event-correct comparison or schedule."""
import argparse
from dataclasses import dataclass
import json
import os
import re
from pathlib import Path
import subprocess
from common import ROOT, syntax, verification_comparison, write_json
from mutation_owners import (
    OWNERS,
    declared_source_map,
    resolve_sources,
    scheduled_owners,
    source_map,
    validate_sources,
)
from production_changes import unchanged_production


CODEC_PREFIXES = ('src/crypto', 'src/postings', 'src/product_cursor', 'src/queue',
                  'src/application/read_', 'src/shard/record', 'src/rollup/allocation',
                  'src/rollup/storage')
QUOTA_PREFIXES = ('src/quota',)
LIFECYCLE_PREFIXES = ('src/shard', 'src/tasks', 'src/runtime', 'src/bootstrap', 'src/sse',
                      'src/touch.rs', 'src/billing/read_accumulator', 'src/billing/read_spool',
                      'src/bin/pilot/benchmark', 'src/bin/pilot/generator')
BUFFER_PREFIXES = ('src/retained_bytes', 'src/application/read_', 'src/crypto', 'src/bootstrap',
                   'src/fleet', 'src/http', 'src/ops')
CRITICAL_PREFIXES = CODEC_PREFIXES + QUOTA_PREFIXES + LIFECYCLE_PREFIXES + BUFFER_PREFIXES


@dataclass(frozen=True)
class SourceChange:
    status: str
    before: str = ''
    after: str = ''


def discover_changes(base, root=None):
    """Return rename-aware, NUL-safe Git change records."""
    root = ROOT if root is None else Path(root)
    raw = subprocess.check_output(
        ['git', 'diff', '--no-ext-diff', '--name-status', '-z', '-M', base, '--'],
        cwd=root,
    )
    fields = raw.rstrip(b'\0').split(b'\0') if raw else []
    changes = []
    index = 0
    while index < len(fields):
        status = fields[index].decode('ascii')
        index += 1
        kind = status[:1]
        if kind in {'R', 'C'}:
            if index + 1 >= len(fields):
                raise ValueError('truncated Git rename/copy record')
            before = fields[index].decode()
            after = fields[index + 1].decode()
            index += 2
        else:
            if kind not in {'A', 'D', 'M', 'T'} or index >= len(fields):
                raise ValueError(f'unsupported or truncated Git change status: {status!r}')
            path = fields[index].decode()
            index += 1
            before = '' if kind == 'A' else path
            after = '' if kind == 'D' else path
        changes.append(SourceChange(status, before, after))
    return changes


def plan(paths, visibility_only=(), production_unchanged=(), formatted_visibility=(), deleted=(),
         forced_critical=(), owners=OWNERS):
    source = [p for p in paths if p.endswith('.rs')]
    omitted = set(visibility_only) | set(production_unchanged) | set(deleted)
    implementation = [p for p in source if p not in omitted]
    tooling = any(p.startswith(('tools/quality-invariants/', 'fuzz/', 'scripts/quality/'))
                  or p in ('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'quality-tools.toml',
                           '.github/workflows/rust-quality.yml') for p in paths)
    codec = any(p.startswith(CODEC_PREFIXES) for p in implementation)
    quota = any(p.startswith(QUOTA_PREFIXES) for p in implementation)
    lifecycle = any(p.startswith(LIFECYCLE_PREFIXES) for p in implementation)
    buffers = any(p.startswith(BUFFER_PREFIXES) for p in implementation)
    mutation_source = [p for p in implementation if p not in formatted_visibility]
    by_source = source_map(owners)
    forced = set(forced_critical)
    mutation_source_files = sorted(
        p for p in mutation_source
        if p in forced or p in by_source or p.startswith(CRITICAL_PREFIXES)
    )
    selection = resolve_sources(mutation_source_files, owners)
    deleted_critical = sorted(
        p for p in deleted
        if p.endswith('.rs')
        and (p in forced or p in by_source or p.startswith(CRITICAL_PREFIXES))
    )
    mutants = bool(selection.changed_sources)
    return {'compiler': bool(source) or tooling, 'properties_fuzz': codec or quota or tooling,
            'loom': lifecycle or tooling, 'miri': buffers or tooling,
            'mutants': mutants, 'changed_rust_files': source,
            **selection.receipt(),
            'deleted_critical_files': deleted_critical,
            'visibility_only_files': sorted(visibility_only),
            'production_unchanged_files': sorted(production_unchanged),
            'formatted_visibility_files': sorted(formatted_visibility)}


def plan_schedule(slot, owners=OWNERS):
    """Resolve one full-owner bucket; no diff-oriented filter participates."""
    selected = scheduled_owners(slot, owners=owners)
    paths = sorted(path for entry in selected for path in entry.sources)
    result = plan(paths, owners=owners)
    result.update({
        'compiler': True,
        'properties_fuzz': True,
        'loom': True,
        'miri': True,
        'mutants': True,
        'changed_rust_files': [],
        'scheduled_source_files': paths,
        'schedule_slot': slot % 7,
        'selection_kind': 'scheduled-owner-rotation',
    })
    return result


def _is_critical(path, registered):
    return bool(path) and path.endswith('.rs') and (
        path in registered or path.startswith(CRITICAL_PREFIXES)
    )


def plan_changes(changes, visibility_only=(), production_unchanged=(), formatted_visibility=(),
                 previous_registered=None, owners=OWNERS):
    """Carry current and previous ownership through one change selection."""
    previous_registered = previous_registered or {}
    current_registered = source_map(owners)
    live = sorted({change.after for change in changes if change.after})
    deleted = []
    forced = set()
    renamed = []
    added = []
    for change in changes:
        kind = change.status[:1]
        before_critical = _is_critical(change.before, previous_registered)
        after_critical = _is_critical(change.after, current_registered)
        if kind in {'R', 'C'} and change.after.endswith('.rs'):
            current_owner = current_registered.get(change.after)
            renamed.append({
                'status': change.status,
                'before': change.before,
                'after': change.after,
                'previous_owner': previous_registered.get(change.before),
                'current_owner': current_owner.name if current_owner else None,
            })
            if before_critical or after_critical:
                forced.add(change.after)
        elif kind == 'R' and before_critical:
            deleted.append(change.before)
            forced.add(change.before)
        elif kind == 'D' and before_critical:
            deleted.append(change.before)
            forced.add(change.before)
        elif kind == 'A' and change.after.endswith('.rs'):
            added.append(change.after)

    # Git deliberately represents sufficiently dissimilar moves as delete/add.
    # When a critical owner disappeared, carry every otherwise-unpaired Rust
    # addition to registration instead of guessing which one replaced it.
    possible_replacements = sorted(added) if deleted else []
    forced.update(possible_replacements)
    owners_by_name = {entry.name: entry for entry in owners}
    deleted_dispositions = []
    for path in sorted(deleted):
        previous_owner = previous_registered.get(path)
        current_owner = owners_by_name.get(previous_owner)
        replacements = sorted(set(current_owner.sources) & set(added)) \
            if current_owner else []
        deleted_dispositions.append({
            'path': path,
            'previous_owner': previous_owner,
            'replacement_files': replacements,
            'disposition': 'owner-relocated' if replacements
            else 'owner-retired' if previous_owner and not current_owner
            else 'prefix-critical-deletion',
        })
    result = plan(
        [*live, *deleted],
        visibility_only,
        production_unchanged,
        formatted_visibility,
        deleted,
        forced,
        owners,
    )
    result.update({
        'renamed_source_files': [
            record | {'disposition': (
                'visibility-only' if record['after'] in visibility_only
                else 'production-unchanged' if record['after'] in production_unchanged
                else 'formatted-visibility' if record['after'] in formatted_visibility
                else 'mutation-selected'
            )}
            for record in renamed
        ],
        'possible_replacement_files': possible_replacements,
        'deleted_source_dispositions': deleted_dispositions,
        'selection_kind': 'changed-tree',
    })
    return result


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


def _change_records(paths):
    return [
        value if isinstance(value, SourceChange) else SourceChange('M', value, value)
        for value in paths
    ]


def source_changes(base, paths):
    before, after = {}, {}
    changes = _change_records(paths)
    existing = set(subprocess.check_output(
        ['git', 'ls-tree', '-r', '--name-only', base], cwd=ROOT, text=True).splitlines())
    for change in changes:
        path = change.after or change.before
        if not path.endswith('.rs'):
            continue
        # Missing new/deleted files have empty source; real Git read failures
        # propagate rather than being mistaken for a new test-only file.
        prior_path = change.before
        current_path = change.after
        before[path] = subprocess.check_output(
            ['git', 'show', f'{base}:{prior_path}'], cwd=ROOT
        ).decode() if prior_path in existing else ''
        after[path] = (ROOT / current_path).read_bytes().decode() \
            if current_path and (ROOT / current_path).is_file() else ''
    if not before:
        return [], [], []
    old_facts, new_facts = syntax(before), syntax(after)
    visibility = [path for path in before
                  if is_visibility_only(before[path], after[path], old_facts[path], new_facts[path])]
    formatted = [path for path in before if path not in visibility
                 and is_formatted_visibility(before[path], after[path], old_facts[path], new_facts[path])]
    return visibility, unchanged_production(before, after, old_facts, new_facts), formatted


def previous_registered_sources(base):
    path = 'scripts/quality/mutation_owners.py'
    listed = subprocess.check_output(
        ['git', 'ls-tree', '--name-only', base, '--', path],
        cwd=ROOT,
        text=True,
    ).strip()
    if not listed:
        return {}
    source = subprocess.check_output(['git', 'show', f'{base}:{path}'], cwd=ROOT, text=True)
    return declared_source_map(source)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    comparison = verification_comparison()
    validate_sources(ROOT)
    if comparison.event == 'schedule':
        slot = int(os.environ.get('QUALITY_SCHEDULE_SLOT', '0'))
        result = plan_schedule(slot)
        (out / 'pr.diff').write_bytes(b'')
    else:
        base = comparison.comparison_revision
        # Includes working changes during local development; CI has a clean checkout.
        diff = subprocess.check_output(
            ['git', 'diff', '--no-ext-diff', '--binary', base, '--'], cwd=ROOT
        )
        (out / 'pr.diff').write_bytes(diff)
        changes = discover_changes(base)
        visibility, production, formatted = source_changes(base, changes)
        result = plan_changes(
            changes,
            visibility,
            production,
            formatted,
            previous_registered_sources(base),
        )
    result.update({
        'event': comparison.event,
        'checkout_revision': comparison.checkout_revision,
        'comparison_revision': comparison.comparison_revision,
        'comparison_kind': comparison.kind,
        # Compatibility for existing receipt consumers; this is the exact
        # comparison revision on pushes, not necessarily a merge base.
        'merge_base': comparison.comparison_revision,
    })
    write_json(out / 'plan.json', result)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
