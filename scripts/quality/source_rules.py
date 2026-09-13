"""Syntax facts drive source rules; typed calls remain the compiler's job."""
from collections import Counter
import hashlib
import re
from common import digest
from lint_contract import from_compiler

PROOFS = {
    'src/shard/record/checked.rs': ('crate::CheckedFrame::',),
    'src/postings/validated.rs': ('crate::ValidatedRuns::', 'crate::RunWindow::'),
    'src/application/read_batch.rs': ('crate::PlainPayload::', 'crate::PlainBatch::'),
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


def _inside(inner, outer):
    start = (inner['line'], inner['column'])
    end = (inner['end_line'], inner['end_column'])
    outer_start = (outer['line'], outer['column'])
    outer_end = (outer['end_line'], outer['end_column'])
    return outer_start <= start and end <= outer_end


def _exception_lints(value):
    head = value.split('reason', 1)[0]
    return {
        f'clippy::{name}' if clippy else name
        for clippy, name in re.findall(r'(?:(clippy)\s*::\s*)?([a-z][a-z0-9_]*)', head)
        if name not in {'allow', 'expect'}
    }


def _fingerprint_sites(metrics, prefix, facts):
    for fact in facts:
        site = f'{fact["qualified"]}\0{fact["value"]}'
        digest = hashlib.sha256(site.encode()).hexdigest()[:16]
        metrics[f'{prefix}:{fact["qualified"]}:{digest}'] += 1


def exception_contracts(sources, facts):
    """Measure the syntax living under each accepted exception.

    This is deliberately a source ratchet, not a substitute lint engine. The
    compiler still decides whether an unwrap is the Clippy lint or whether a
    field is dead. These counters ensure an existing item/impl expectation
    cannot silently cover one more candidate site or a larger structure.
    """
    contracts = {}
    for path, parsed in facts.items():
        file_end = max(1, len(sources[path].splitlines()))
        for attribute in parsed['facts']:
            value = attribute['value']
            if (attribute['kind'] not in ('attribute', 'macro-attribute')
                    or not re.match(r'(allow|expect)\s*\(', value)
                    or 'reason =' not in value):
                continue
            candidates = [item for item in parsed['items']
                          if _inside(attribute['location'], item['location'])]
            if candidates:
                scope = min(candidates, key=lambda item: (
                    item['location']['end_line'] - item['location']['line'],
                    item['location']['end_column'] - item['location']['column'],
                ))
                location = scope['location']
                kind = scope['kind']
            else:
                location = {'line': 1, 'column': 0, 'end_line': file_end,
                            'end_column': 1 << 30}
                kind = 'crate'
            lints = _exception_lints(value)
            scoped_facts = [fact for fact in parsed['facts']
                            if _inside(fact['location'], location)]
            scoped_items = [item for item in parsed['items']
                            if _inside(item['location'], location)]
            metrics = Counter({
                'scope_lines': location['end_line'] - location['line'] + 1,
                'nested_items': len(scoped_items),
                # A compiler-independent multiplicity ceiling for other lint
                # candidates (paths, calls, macros, attributes). Typed Clippy
                # still decides which of them actually trigger a lint.
                'syntax_facts': len(scoped_facts),
            })
            for lint, methods, total, prefix in (
                ('clippy::unwrap_used', {'unwrap', 'unwrap_err'}, 'unwrap_sites', 'unwrap_site'),
                ('clippy::expect_used', {'expect', 'expect_err'}, 'expect_sites', 'expect_site'),
            ):
                if lint not in lints:
                    continue
                sites = [
                    fact for fact in scoped_facts
                    if fact['kind'] in {'method-call-site', 'call-site'}
                    and fact['value'].partition('\t')[0].rsplit('::', 1)[-1] in methods
                ]
                metrics[total] = len(sites)
                _fingerprint_sites(metrics, prefix, sites)
                # Import aliases resolve above, but local function-pointer aliases
                # require type resolution. Preserve every ordinary call spelling
                # under this exceptional scope so such an alias cannot replace a
                # benign call without an explicit new exception decision.
                _fingerprint_sites(
                    metrics,
                    f'{prefix}:ordinary-call',
                    (fact for fact in scoped_facts if fact['kind'] == 'call-site'),
                )
                # A local binding can hide an associated function behind an
                # arbitrary callee name. Exact path sites make changing that
                # binding an explicit decision without guessing Rust types.
                _fingerprint_sites(
                    metrics,
                    f'{prefix}:path',
                    (fact for fact in scoped_facts if fact['kind'] == 'path'),
                )
            if 'dead_code' in lints:
                fields = [item for item in scoped_items if item['kind'] == 'field']
                metrics['fields'] = len(fields)
                for field in fields:
                    site = f'{field["qualified"]}\0{field["signature"]}'
                    digest = hashlib.sha256(site.encode()).hexdigest()[:16]
                    metrics[f'field_site:{field["qualified"]}:{digest}'] += 1
            identity = (path, attribute['qualified'], kind, value)
            if identity in contracts:
                contracts[identity].update(metrics)
            else:
                contracts[identity] = metrics
    return contracts


def exception_growth(current, previous):
    failures = []
    for identity, metrics in current.items():
        if identity not in previous:
            continue  # A new/changed reason is the explicit review decision.
        before = previous[identity]
        for metric, count in metrics.items():
            if count > before.get(metric, 0):
                failures.append(
                    f'accepted exception grew without a new decision: {identity}: '
                    f'{metric} {before.get(metric, 0)} -> {count}'
                )
    return failures


def violations(sources, facts, before_lines, prior_lines, allowed, architecture):
    failures = []
    groups, denied = from_compiler()
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
            if fact['kind'] in ('attribute', 'macro-attribute') and re.match(r'(allow|expect|warn)\s*\(', value):
                head = value.split('reason', 1)[0]
                names = set(re.findall(r'(?:clippy::)?[a-z][a-z0-9_]*', re.sub(r'\s+', '', head)))
                if names & groups:
                    failures.append(f'blanket lint-group override: {path}: {value}')
                if names & denied:
                    failures.append(f'denied lint cannot be suppressed or downgraded: {path}: {value}')
            if fact['kind'] == 'field-visibility' and value:
                if fact['qualified'].startswith(PROOFS.get(path, ())):
                    failures.append(f'proof field visibility: {path}: {fact["qualified"]}: {value}')
            if hard and fact['kind'] in ('path', 'import-target'):
                resolved = absolute(path, fact['qualified'], value)
                if resolved.startswith(('crate::http::', 'crate::product::', 'axum::')) or resolved in ('crate::http', 'crate::product', 'axum'):
                    failures.append(f'owner transport dependency: {path}: {resolved}')
            if fact['kind'] in ('attribute', 'macro-attribute') and re.match(r'(allow|expect)\s*\(', value):
                head = value.split('reason', 1)[0]
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
