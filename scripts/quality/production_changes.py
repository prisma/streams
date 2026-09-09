"""Conservative source comparison for compiler-only and explicit test-only edits.

This does not relax compilation, tests, lint policy, or source ratchets. It only
identifies when an actual PR diff leaves production tokens unchanged, so the
invariant planner does not claim a mutation experiment on an empty code scope.
"""
import re
from common import syntax


def normalized_source(source, parsed):
    # Opaque templates cannot establish this proof. Do not use the scanner's
    # filename-based test hints: only a direct Rust cfg(test) can erase an item.
    if 'tokens' not in parsed:
        return None
    if parsed['test_only_file']:
        return '', []
    offsets = [0, *[i + 1 for i, char in enumerate(source) if char == '\n']]

    def span(node):
        loc = node['location']
        return (offsets[loc['line'] - 1] + loc['column'],
                offsets[loc['end_line'] - 1] + loc['end_column'])

    def attribute(node):
        # Attribute facts describe the Meta, not the enclosing #[...]. Require
        # that exact direct wrapper; cfg_attr/macro interiors are not eligible.
        start, end = span(node)
        if source[end:end + 1] != ']':
            return None
        if start >= 3 and source[start - 3:start] == '#![':
            return start - 3, end + 1, True
        if start >= 2 and source[start - 2:start] == '#[':
            return start - 2, end + 1, False
        return None

    erase = [span(item) for item in parsed['items']
             if item['explicit_test_cfg'] and item['kind'] in {
                 'module', 'function', 'impl', 'struct', 'enum', 'trait', 'const', 'static', 'type'
             }]
    for fact in parsed['facts']:
        if fact['kind'] != 'attribute':
            continue
        direct = attribute(fact)
        if direct is None:
            continue
        start, end, _inner = direct
        if fact['value'].startswith(('allow (', 'expect (')):
            erase.append((start, end))
    outer = []
    for start, end in sorted(erase, key=lambda interval: (interval[0], -interval[1])):
        if outer and start < outer[-1][1]:
            if end > outer[-1][1]:
                raise ValueError('partially overlapping parsed source ranges')
            continue
        outer.append((start, end))

    def erased(node):
        start, end = span(node)
        return any(begin <= start and end <= finish for begin, finish in outer)

    # Source introspection can change values when annotations move locations.
    # Item macro expansion is opaque; keep its checks. Token matching here is
    # deliberately conservative (even a quoted spelling can retain checks).
    sensitive = {'line', 'column', 'file', 'include', 'include_str', 'include_bytes'}
    if any(item['kind'] == 'macro' and not erased(item) for item in parsed['items']):
        return None
    builtins = {'cfg', 'doc', 'repr', 'inline', 'cold', 'must_use', 'deprecated',
                'allow', 'expect', 'warn', 'deny', 'forbid', 'path', 'track_caller',
                'no_mangle', 'export_name', 'link', 'link_name', 'link_section', 'unsafe'}
    for fact in parsed['facts']:
        if erased(fact):
            continue
        # A custom attribute/derive can inspect the annotations supplied to it.
        # Retain checks instead of assuming its expansion commutes with erasure.
        if fact['kind'] == 'attribute' and re.split(r'[ (=]', fact['value'], maxsplit=1)[0] not in builtins:
            return None
        if fact['kind'] in {'macro', 'import-target'} and fact['value'].split('::')[-1].strip() in sensitive:
            return None
        if fact['kind'] == 'macro-tokens' and re.search(
            r'\b(?:line|column|file|include|include_str|include_bytes)\s*!', fact['value']
        ):
            return None

    edits = [(start, end, '') for start, end in outer]
    visibilities = []
    for fact in parsed['facts']:
        if fact['kind'] != 'visibility':
            continue
        start, end = span(fact)
        if erased(fact):
            continue
        if not source[start:end].startswith('pub'):
            raise ValueError('visibility range does not match parsed source')
        edits.append((start, end, 'pub'))
        visibilities.append((start, fact['value']))
    for start, end, text in sorted(edits, reverse=True):
        source = source[:start] + text + source[end:]
    return source, [value for _, value in sorted(visibilities)]


def unchanged_production(before, after, old_facts, new_facts):
    candidates, normalized = {}, {}
    for path in before:
        old = normalized_source(before[path], old_facts[path])
        new = normalized_source(after[path], new_facts[path])
        if old is None or new is None:
            continue
        old_source, old_vis = old
        new_source, new_vis = new
        if len(old_vis) != len(new_vis) or any(
            a != b and not (a == 'pub' and b in ('pub (crate)', 'pub (super)', 'pub (self)'))
            for a, b in zip(old_vis, new_vis)
        ):
            continue
        old_key, new_key = 'before/' + path, 'after/' + path
        normalized[old_key], normalized[new_key] = old_source, new_source
        candidates[path] = old_key, new_key
    if not candidates:
        return []
    # syn's token stream preserves doc attributes, literal spellings and macro
    # bodies; a regex/whitespace fingerprint is not sufficient here.
    parsed = syntax(normalized)
    return [path for path, (old_key, new_key) in candidates.items()
            if parsed[old_key]['tokens'] == parsed[new_key]['tokens']]
