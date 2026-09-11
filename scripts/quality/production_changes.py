"""Conservative source comparison for compiler-only and explicit test-only edits.

This does not relax compilation, tests, lint policy, or source ratchets. It only
identifies when an actual PR diff leaves production tokens unchanged, so the
invariant planner does not claim a mutation experiment on an empty code scope.
"""
import re
from common import syntax


def attribute_key(fact):
    loc = fact['location']
    return (fact['value'], loc['line'], loc['column'], loc['end_line'], loc['end_column'])


def fixed_attribute_inputs(before, after, old_facts, new_facts):
    """Opaque outer attributes may observe their complete item and its spans.

    Freeze the complete enclosing declaration byte-for-byte, including every
    attribute and nested token, at identical line/column AND byte positions.
    This does not interpret a derive name or assume it is a standard macro.
    Inner attributes and items without a parsed enclosing declaration stay
    conservative. The normal comparison still rejects source introspection.
    """
    def inputs(source, parsed):
        if 'tokens' not in parsed:
            return {}
        offsets = [0, *[i + 1 for i, char in enumerate(source) if char == '\n']]

        def span(node):
            loc = node['location']
            return (offsets[loc['line'] - 1] + loc['column'],
                    offsets[loc['end_line'] - 1] + loc['end_column'])

        items = [(item, *span(item)) for item in parsed['items'] if item['kind'] in {
            'module', 'function', 'impl', 'struct', 'enum', 'trait', 'const', 'static', 'type'
        }]
        result = {}
        for fact in parsed['facts']:
            if fact['kind'] != 'attribute':
                continue
            start, end = span(fact)
            if source[max(0, start - 2):start] != '#[' or source[end:end + 1] != ']':
                continue  # Never treat an inner attribute or macro interior as an outer one.
            containers = [(item, left, right) for item, left, right in items
                          if left <= start - 2 and end + 1 <= right]
            if not containers:
                continue
            item, left, right = min(containers, key=lambda entry: entry[2] - entry[1])
            result[attribute_key(fact)] = (
                item['kind'], item['qualified'], item['location'],
                len(source[:left].encode('utf-8')), len(source[:right].encode('utf-8')),
                source[left:right],
            )
        return result

    old, new = inputs(before, old_facts), inputs(after, new_facts)
    return {key for key in old.keys() & new.keys() if old[key] == new[key]}


def normalized_source(source, parsed, fixed_attributes=()):
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
        # A custom attribute/derive can inspect its complete supplied item.
        # Retain checks unless both that item and all its positions are fixed.
        if (fact['kind'] == 'attribute'
                and re.split(r'[ (=]', fact['value'], maxsplit=1)[0] not in builtins
                and attribute_key(fact) not in fixed_attributes):
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


def trailing_test_prefix(source, parsed):
    """Keep opaque production inputs byte-exact; erase only a root test suffix.

    Unlike token normalization this cannot move or modify any production item,
    including the attributes and nested tokens supplied to a procedural macro.
    A test item inside such a macro's input is never eligible for this proof.
    Source-reading macros remain conservative even with unchanged locations.
    """
    if 'tokens' not in parsed:
        return None
    offsets = [0, *[i + 1 for i, char in enumerate(source) if char == '\n']]

    def span(node):
        loc = node['location']
        return (offsets[loc['line'] - 1] + loc['column'],
                offsets[loc['end_line'] - 1] + loc['end_column'])

    items = [(item, *span(item)) for item in parsed['items']]
    roots = [(start, end) for item, start, end in items
             if item['explicit_test_cfg'] and item['kind'] in {
                 'module', 'function', 'impl', 'struct', 'enum', 'trait', 'const', 'static', 'type'
             } and not any(other is not item and left <= start and end <= right
                           for other, left, right in items)]
    cursor = len(source)
    for start, end in sorted(roots, reverse=True):
        if source[end:cursor].strip():
            break
        cursor = start
    # No production token or span can shift. Whitespace after the final item
    # does not belong to any production macro input.
    prefix = source[:cursor].rstrip()
    sensitive = {'line', 'column', 'file', 'include', 'include_str', 'include_bytes'}
    if any(item['kind'] == 'macro' and start < cursor for item, start, _ in items):
        return None
    builtins = {'cfg', 'doc', 'repr', 'inline', 'cold', 'must_use', 'deprecated',
                'allow', 'expect', 'warn', 'deny', 'forbid', 'path', 'track_caller',
                'no_mangle', 'export_name', 'link', 'link_name', 'link_section', 'unsafe'}
    for fact in parsed['facts']:
        start, _end = span(fact)
        if start >= cursor:
            continue
        # A crate-level custom inner attribute can consume the whole file,
        # including the supposedly erased suffix, rather than one fixed item.
        if (fact['kind'] == 'attribute' and source[max(0, start - 3):start] == '#!['
                and re.split(r'[ (=]', fact['value'], maxsplit=1)[0] not in builtins):
            return None
        if fact['kind'] in {'macro', 'import-target'} and fact['value'].split('::')[-1].strip() in sensitive:
            return None
        if fact['kind'] == 'macro-tokens' and re.search(
            r'\b(?:line|column|file|include|include_str|include_bytes)\s*!', fact['value']
        ):
            return None
    return prefix


def unchanged_production(before, after, old_facts, new_facts):
    candidates, normalized, fixed_prefixes = {}, {}, []
    for path in before:
        old_prefix = trailing_test_prefix(before[path], old_facts[path])
        new_prefix = trailing_test_prefix(after[path], new_facts[path])
        if old_prefix is not None and new_prefix is not None and old_prefix == new_prefix:
            fixed_prefixes.append(path)
            continue
        fixed = fixed_attribute_inputs(before[path], after[path], old_facts[path], new_facts[path])
        old = normalized_source(before[path], old_facts[path], fixed)
        new = normalized_source(after[path], new_facts[path], fixed)
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
        return fixed_prefixes
    # syn's token stream preserves doc attributes, literal spellings and macro
    # bodies; a regex/whitespace fingerprint is not sufficient here.
    parsed = syntax(normalized)
    return fixed_prefixes + [path for path, (old_key, new_key) in candidates.items()
            if parsed[old_key]['tokens'] == parsed[new_key]['tokens']]
