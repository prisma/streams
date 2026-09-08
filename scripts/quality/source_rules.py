"""Syntax facts drive source rules; typed calls remain the compiler's job."""
from collections import Counter
import re
from common import digest

PROOFS = {
    'src/shard/record/checked.rs': ('crate::CheckedFrame::',),
    'src/postings/validated.rs': ('crate::ValidatedRuns::', 'crate::RunWindow::'),
    'src/application/read_batch.rs': ('crate::PlainPayload::', 'crate::PlainBatch::'),
    'src/history/span_cache/capture.rs': ('crate::CipherSpan::',),
}
# Known expression DSLs do not define service owners. Still counted/reported;
# typed Clippy sees their expansions. Every other macro needs an explicit entry.
EXPRESSION_MACROS = {'assert', 'assert_eq', 'assert_ne', 'debug_assert', 'debug_assert_eq',
                    'debug_assert_ne', 'vec', 'format', 'format_args', 'write', 'writeln',
                    'println', 'eprintln', 'panic', 'matches', 'include_str', 'include_bytes',
                    'env', 'option_env', 'concat', 'stringify', 'file', 'line', 'column',
                    'unreachable', 'todo', 'unimplemented', 'cfg', 'thread_local'}


def absolute(path, qualified, value):
    if not value.startswith(('super::', 'self::')):
        return value
    # Conventional module layout. By-path files/globs remain in the unresolved
    # inventory; this never claims to implement Rust's name/type resolution.
    modules = path.removeprefix('src/').removesuffix('.rs').split('/')
    if modules[-1] == 'mod':
        modules.pop()
    parts = value.split('::')
    if parts[0] == 'self':
        parts.pop(0)
    while parts and parts[0] == 'super':
        parts.pop(0)
        if modules:
            modules.pop()
    return '::'.join(['crate', *modules, *parts])


def classify(path, fact):
    kind, value = fact['kind'], fact['value']
    if kind == 'static':
        return 'global'  # All statics inventoried: do not guess interior mutability.
    if kind in ('path', 'import-target'):
        if value.startswith(('std::env::var', 'std::thread::', 'tokio::spawn', 'tokio::task::spawn')):
            return 'effect'
        if value == 'std::mem::forget':
            return 'effect'
    if kind == 'unresolved-glob':
        return 'unresolved-glob'
    if kind == 'macro':
        if value == 'thread_local':
            return 'global-macro'
        if value.split('::')[-1] not in EXPRESSION_MACROS and not value.startswith(('tracing::', 'anyhow::')):
            return 'macro-dsl'
    if kind in ('unparsed-macro-attribute', 'unparsed-attribute'):
        return kind
    if kind in ('attribute', 'macro-attribute'):
        if re.match(r'(allow|expect)\s*\(', value):
            return 'exception'
        if value.startswith('path '):
            return 'by-path-module'
    return None


def inventory(facts):
    result = Counter()
    for path, source in facts.items():
        for fact in source['facts']:
            category = classify(path, fact)
            if category:
                identity = (category, path, fact['qualified'], fact['value'])
                result[identity] += 1
    return result


def entries(counts):
    fields = ('category', 'path', 'owner', 'syntax')
    return [dict(zip(fields, identity), count=n) for identity, n in sorted(counts.items())]


def from_entries(rows):
    result = Counter()
    for row in rows:
        identity = tuple(row[k] for k in ('category', 'path', 'owner', 'syntax'))
        if identity in result or type(row['count']) is not int or row['count'] < 1:
            raise ValueError('invalid source allowance')
        result[identity] = row['count']
    return result


def violations(sources, facts, before_lines, prior_lines, allowed, architecture):
    failures = []
    for path, source in sources.items():
        now = len(source.splitlines())
        # Adoption is the first applicable ceiling on this existing long PR;
        # after adoption reaches the target, the actual merge base can lower it.
        limit = max(1000, before_lines.get(path, 0))
        if path in prior_lines and prior_lines[path] >= 0:
            limit = min(limit, max(1000, prior_lines[path]))
        if now > limit:
            failures.append(f'file growth: {path}: {now} > {limit}')
        hard = path.startswith('src/application/') or path in architecture['sse_core_files']
        for fact in facts[path]['facts']:
            value = fact['value']
            if fact['kind'] == 'field-visibility' and value:
                if fact['qualified'].startswith(PROOFS.get(path, ())):
                    failures.append(f'proof field visibility: {path}: {fact["qualified"]}: {value}')
            if hard and fact['kind'] in ('path', 'import-target'):
                resolved = absolute(path, fact['qualified'], value)
                if resolved.startswith(('crate::http::', 'crate::product::', 'axum::')) or resolved in ('crate::http', 'crate::product', 'axum'):
                    failures.append(f'owner transport dependency: {path}: {resolved}')
            if fact['kind'] in ('attribute', 'macro-attribute') and re.match(r'(allow|expect)\s*\(', value):
                head = value.split('reason', 1)[0]
                if re.search(r'\b(warnings|all|pedantic|restriction|nursery)\b', head):
                    failures.append(f'blanket lint suppression: {path}: {value}')
                identity = ('exception', path, fact['qualified'], value)
                if identity not in allowed and 'disallowed_methods' in head:
                    is_function = any(i['kind'] == 'function' and i['qualified'] == fact['qualified']
                                      for i in facts[path].get('items', []))
                    registered_owner = any(k[0] == 'effect' and k[1] == path and k[2] == fact['qualified']
                                           for k in allowed)
                    if not is_function or not registered_owner:
                        failures.append(f'primitive-spawn exception needs a registered function owner: {path}: {fact["qualified"]}')
                if identity not in allowed and ('reason =' not in value or not re.search(r'"[^";]+;[^";]+;[^";]+"', value)):
                    failures.append(f'exception needs owner; invariant; alternative: {path}: {value}')
    for identity, count in inventory(facts).items():
        # A new narrow reasoned expectation is reviewed in-source, not another
        # baseline entry. Unknown macros/effects/globs/statics fail until owned.
        if identity[0] == 'exception' and 'reason =' in identity[3]:
            continue
        if count > allowed[identity]:
            failures.append(f'unregistered source occurrence ({count - allowed[identity]}): {identity}')
    return failures
