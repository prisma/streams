"""Syntax facts drive source rules; typed calls remain the compiler's job."""
from collections import Counter
import hashlib
import posixpath
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


def _relative(qualified, owner):
    """A site's item path below the exception's owner: renaming the owner or
    narrowing onto an extracted item keeps every fingerprint."""
    if qualified == owner or qualified.startswith(f'{owner}::'):
        return qualified[len(owner):]
    return qualified


def _fingerprint_sites(metrics, prefix, facts, owner):
    for fact in facts:
        qualified = _relative(fact['qualified'], owner)
        site = f'{qualified}\0{fact["value"]}'
        digest = hashlib.sha256(site.encode()).hexdigest()[:16]
        metrics[f'{prefix}:{qualified}:{digest}'] += 1


def exception_scopes(sources, facts):
    """Yield (path, attribute, scope kind, location) for each reasoned exception.

    The scope is the smallest parsed item enclosing the attribute, or the file;
    a statement-level expectation measures its item.
    """
    for path, parsed in facts.items():
        file_end = max(1, len(sources[path].splitlines()))
        for attribute in parsed['facts']:
            value = attribute['value']
            if (attribute['kind'] not in ('attribute', 'macro-attribute')
                    or not re.match(r'(allow|expect)\s*\(', value)
                    or 'reason =' not in value):
                continue
            candidates = [item for item in parsed.get('items', [])
                          if _inside(attribute['location'], item['location'])]
            if candidates:
                scope = min(candidates, key=lambda item: (
                    item['location']['end_line'] - item['location']['line'],
                    item['location']['end_column'] - item['location']['column'],
                ))
                yield path, attribute, scope['kind'], scope['location']
            else:
                yield path, attribute, 'crate', {'line': 1, 'column': 0, 'end_line': file_end,
                                                 'end_column': 1 << 30}


def _directory_owner(path):
    """A file whose `mod x;` children live beside it rather than below it."""
    name = posixpath.basename(path)
    parent = posixpath.basename(posixpath.dirname(path))
    return name in ('mod.rs', 'lib.rs', 'main.rs') or parent in ('bin', 'examples', 'tests', 'benches', 'fuzz_targets')


def _child_modules(path, location, sources, facts):
    """(qualified, resolved file or None) for each out-of-line module declared
    inside `location` of `path`: a lint attribute on it, or on its parent,
    covers the child file too."""
    lines = sources[path].splitlines()
    for item in facts[path].get('items', []):
        where = item['location']
        if (item['kind'] != 'module' or not _inside(where, location)
                or not lines[where['end_line'] - 1].rstrip().endswith(';')):
            continue
        declared = [re.match(r'path\s*=\s*"([^"]*)"', fact['value']) for fact in facts[path]['facts']
                    if classify(path, fact) == 'by-path-module' and _inside(fact['location'], where)]
        folder = posixpath.dirname(path)
        if declared:
            candidates = [posixpath.normpath(posixpath.join(folder, match[1])) for match in declared if match]
        else:
            name = item['qualified'].rsplit('::', 1)[-1]
            below = posixpath.join(folder, posixpath.basename(path).removesuffix('.rs'))
            first, second = (folder, below) if _directory_owner(path) else (below, folder)
            candidates = [posixpath.join(first, f'{name}.rs'), posixpath.join(first, name, 'mod.rs'),
                          posixpath.join(second, f'{name}.rs'), posixpath.join(second, name, 'mod.rs')]
        yield item['qualified'], next((c for c in candidates if c in sources), None)


def _whole(path, sources):
    return {'line': 1, 'column': 0, 'end_line': max(1, len(sources[path].splitlines())), 'end_column': 1 << 30}


def scope_files(path, location, sources, facts):
    """The (file, location) pairs an exception covers: its own scope and,
    recursively, every out-of-line module file declared inside it; plus the
    qualified names of declared modules no ratcheted file holds."""
    covered, missing, pending, seen = [(path, location)], [], [(path, location)], {path}
    while pending:
        where, span = pending.pop()
        for qualified, child in _child_modules(where, span, sources, facts):
            if child is None:
                missing.append(qualified)
            elif child not in seen and child in facts:
                seen.add(child)
                covered.append((child, _whole(child, sources)))
                pending.append((child, _whole(child, sources)))
    return covered, missing


def _measured(path, location, sources, facts, attributes):
    """Code lines, and facts and items, of one covered span. Attributes (the
    exception's own, docs, cfg), comments and blank lines are not scope: a
    reworded or re-wrapped reason neither grows nor frees a ceiling."""
    covered = attributes[path][1]
    text = sources[path].splitlines()[location['line'] - 1:location['end_line']]
    lines = sum(1 for number, line in enumerate(text, location['line'])
                if number not in covered and line.strip() and not line.strip().startswith('//'))
    scoped_facts = [fact for index, fact in enumerate(facts[path]['facts'])
                    if index not in attributes[path][0] and _inside(fact['location'], location)]
    scoped_items = [item for item in facts[path]['items'] if _inside(item['location'], location)]
    return lines, scoped_facts, scoped_items


def _attribute_spans(path, facts):
    """(indices of facts inside attributes, line numbers attributes cover)."""
    by_line = {}
    for fact in facts[path]['facts']:
        if fact['kind'] in ('attribute', 'macro-attribute'):
            span = fact['location']
            for number in range(span['line'], span['end_line'] + 1):
                by_line.setdefault(number, []).append(span)
    inside = {index for index, fact in enumerate(facts[path]['facts'])
              if any(_inside(fact['location'], span) for span in by_line.get(fact['location']['line'], ()))}
    return inside, set(by_line)


def _lint_metrics(lint, owner, scoped_facts, scoped_items):
    metrics = Counter()
    for name, methods, total, prefix in (
        ('clippy::unwrap_used', {'unwrap', 'unwrap_err'}, 'unwrap_sites', 'unwrap_site'),
        ('clippy::expect_used', {'expect', 'expect_err'}, 'expect_sites', 'expect_site'),
    ):
        if lint != name:
            continue
        sites = [
            fact for fact in scoped_facts
            if fact['kind'] in {'method-call-site', 'call-site'}
            and fact['value'].partition('\t')[0].rsplit('::', 1)[-1] in methods
        ]
        # A macro's arguments are opaque tokens to the parser; Clippy still
        # lints a panic method written inside them.
        written = re.compile(r'\.\s*(?:' + '|'.join(sorted(methods, reverse=True)) + r')\s*\(')
        in_macros = [fact for fact in scoped_facts
                     if fact['kind'] == 'macro-tokens' and written.search(fact['value'])]
        metrics[total] = len(sites) + sum(len(written.findall(fact['value'])) for fact in in_macros)
        _fingerprint_sites(metrics, prefix, sites, owner)
        _fingerprint_sites(metrics, f'{prefix}:macro', in_macros, owner)
        # Import aliases resolve above, but local function-pointer aliases
        # require type resolution. Preserve every ordinary call spelling
        # under this exceptional scope so such an alias cannot replace a
        # benign call without an approved growth row.
        _fingerprint_sites(
            metrics,
            f'{prefix}:ordinary-call',
            (fact for fact in scoped_facts if fact['kind'] == 'call-site'),
            owner,
        )
        # A local binding can hide an associated function behind an
        # arbitrary callee name. Exact path sites make changing that
        # binding an explicit decision without guessing Rust types.
        _fingerprint_sites(
            metrics,
            f'{prefix}:path',
            (fact for fact in scoped_facts if fact['kind'] == 'path'),
            owner,
        )
    if lint == 'dead_code':
        fields = [item for item in scoped_items if item['kind'] == 'field']
        metrics['fields'] = len(fields)
        for field in fields:
            qualified = _relative(field['qualified'], owner)
            site = f'{qualified}\0{field["signature"]}'
            digest = hashlib.sha256(site.encode()).hexdigest()[:16]
            metrics[f'field_site:{qualified}:{digest}'] += 1
    return metrics


def exception_contracts(sources, facts):
    """Measure the syntax living under each accepted exception.

    This is deliberately a source ratchet, not a substitute lint engine. The
    compiler still decides whether an unwrap is the Clippy lint or whether a
    field is dead. These counters ensure an existing item/impl expectation
    cannot silently cover one more candidate site or a larger structure.

    A contract is (path, owner, scope kind, lint). The reason is explanation,
    never identity, and each lint of a multi-lint attribute is its own contract.
    """
    contracts, measured, attributes = {}, set(), {}
    for path, attribute, kind, location in exception_scopes(sources, facts):
        lines, scoped_facts, scoped_items = 0, [], []
        for where, span in scope_files(path, location, sources, facts)[0]:
            if where not in attributes:
                attributes[where] = _attribute_spans(where, facts)
            covered = _measured(where, span, sources, facts, attributes)
            lines += covered[0]
            scoped_facts += covered[1]
            scoped_items += covered[2]
        scope = Counter({
            'scope_lines': lines,
            'nested_items': len(scoped_items),
            # A compiler-independent multiplicity ceiling for other lint
            # candidates (paths, calls, macros). Typed Clippy still decides
            # which of them actually trigger a lint.
            'syntax_facts': len(scoped_facts),
        })
        span = (location['line'], location['column'], location['end_line'], location['end_column'])
        for lint in sorted(_exception_lints(attribute['value'])):
            identity = (path, attribute['qualified'], kind, lint)
            # One scope is measured once per lint, however many attributes on
            # it name that lint: a redundant attribute is not extra ceiling.
            if (identity, span) in measured:
                continue
            measured.add((identity, span))
            metrics = contracts.setdefault(identity, Counter())
            metrics.update(scope)
            metrics.update(_lint_metrics(lint, attribute['qualified'], scoped_facts, scoped_items))
    return contracts


GROWTH_ROW_FIELDS = frozenset(('path', 'owner', 'scope', 'lint', 'metrics', 'rationale', 'approver'))


def growth_ledger(document):
    """The rows of docs/quality/exception-growth.json, failing on any other shape."""
    if set(document) != {'schema', 'rows'} or document['schema'] != 1 or not isinstance(document['rows'], list):
        raise ValueError(f'invalid exception growth ledger: {sorted(document)}')
    return document['rows']


def _growth_rows(rows):
    approved = {}
    for row in rows:
        valid = (isinstance(row, dict) and set(row) == GROWTH_ROW_FIELDS
                 and all(isinstance(row[k], str) and row[k].strip()
                         for k in GROWTH_ROW_FIELDS - {'metrics'})
                 and isinstance(row['metrics'], dict) and row['metrics']
                 and all(isinstance(m, str) and m and type(n) is int and n > 0
                         for m, n in row['metrics'].items()))
        identity = (row['path'], row['owner'], row['scope'], row['lint']) if valid else None
        if not valid or identity in approved:
            raise ValueError(f'invalid exception growth row: {row}')
        approved[identity] = row['metrics']
    return approved


def _module(path):
    stem = path.removesuffix('.rs')
    for leaf in ('/mod', '/main', '/lib'):
        stem = stem.removesuffix(leaf)
    return stem


def _related(one, other):
    """One file's module is the other's, or encloses it (src/fleet.rs and
    src/fleet/mod.rs, src/product.rs and src/product/scan.rs)."""
    a, b = _module(one), _module(other)
    return a == b or a.startswith(f'{b}/') or b.startswith(f'{a}/')


def _origins(identity, candidates, previous, current):
    """The vanished contracts a moved one is compared with: those in a related
    module when there are any, else all of them (a move to an unrelated module
    is still compared), and among those an exact match when there is one."""
    related = [c for c in candidates if _related(c[0], identity[0])] or candidates
    exact = [c for c in related if previous[c] == current[identity]]
    return exact[:1] or related


def _floor(origins, previous):
    return Counter({m: min(previous[o][m] for o in origins) for o in origins for m in previous[o]})


def exception_predecessors(current, previous):
    """identity -> (the ceiling it is held to, its origins, a note naming them).

    A contract that vanished from one file and reappears with the same owner,
    scope kind and lint in another has moved. One that appears under another
    owner, in the same file or a related module, with the same scope kind and
    lint as a vanished one is paired with it: a rename, or a narrowing onto
    an extracted item. Among several candidates an exact match is the origin;
    otherwise each metric is held to the candidates' smallest value. A
    vanished contract may be the origin of more than one. Any other appeared
    contract is a new exception, reviewed in source.
    """
    vanished = [identity for identity in previous if identity not in current]
    result = {}
    for identity in current:
        if identity in previous:
            result[identity] = (previous[identity], (), '')
            continue
        moves = sorted(v for v in vanished if v[1:] == identity[1:])
        if moves:
            origins = _origins(identity, moves, previous, current)
            result[identity] = (_floor(origins, previous), tuple(origins),
                                f' (moved from {", ".join(o[0] for o in origins)})')
            continue
        renames = sorted(v for v in vanished if v[2:] == identity[2:] and _related(v[0], identity[0]))
        if renames:
            origins = _origins(identity, renames, previous, current)
            names = ', '.join(o[1] if o[0] == identity[0] else f'{o[0]} {o[1]}' for o in origins)
            result[identity] = (_floor(origins, previous), tuple(origins), f' (paired with vanished {names})')
    return result


def exception_growth(current, previous, rows=()):
    approved, failures = _growth_rows(rows), []
    for identity, (before, _, note) in exception_predecessors(current, previous).items():
        recorded = approved.get(identity, {})
        failures.extend(
            f'accepted exception grew without an approved growth row: {identity}{note}: '
            f'{metric} {before[metric]} -> {count}'
            for metric, count in current[identity].items()
            if count > before[metric] and recorded.get(metric) != count
        )
    # A row records its contract's current state: it stays valid while the
    # contract holds exactly those values, and goes stale when it changes.
    failures.extend(
        f'stale exception growth row (the contract no longer has these values): {identity}'
        for identity, metrics in approved.items()
        if identity not in current or any(current[identity][m] != n for m, n in metrics.items())
    )
    return failures


def harness_layout(facts):
    """Hold the one filename the syntax tool treats as a whole-file cfg.

    A `<module>/proofs.rs` is classified test-only without an attribute of its
    own, so the only proof is its parent: exactly one parent module file that
    declares `crate::proofs` once, under a direct `#[cfg(kani)]`, and never
    redirects it by `path`. Anything else, such as a production `mod proofs;`,
    a crate-root harness or a missing parent, fails rather than being trusted.
    """
    failures = []
    for path, parsed in facts.items():
        if not parsed.get('kani_harness'):
            continue
        directory = path.removesuffix('/proofs.rs')
        parents = [facts[candidate] for candidate in (f'{directory}.rs', f'{directory}/mod.rs')
                   if candidate in facts]
        declarations = [item for parent in parents for item in parent.get('items', ())
                        if item['kind'] == 'module' and item['qualified'] == 'crate::proofs']
        attributes = [fact['value'] for parent in parents for fact in parent['facts']
                      if fact['kind'] == 'attribute' and fact['qualified'] == 'crate::proofs']
        if (len(parents) != 1 or len(declarations) != 1
                or not declarations[0]['explicit_test_cfg'] or 'cfg (kani)' not in attributes
                or any(value.startswith('path') for value in attributes)):
            failures.append(f'Kani harness needs exactly `#[cfg(kani)] mod proofs;` '
                            f'in its parent module file: {path}')
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
    for path, attribute, _, location in exception_scopes(sources, facts):
        failures.extend(f'exception covers a module file the ratchet does not read: {path}: {qualified}'
                        for qualified in scope_files(path, location, sources, facts)[1])
    for path, parsed in facts.items():
        for fact in parsed['facts']:
            if classify(path, fact) != 'by-path-module':
                continue
            # An exception moved into a file the ratchet does not read would
            # leave every ceiling behind with it.
            target = re.match(r'path\s*=\s*"([^"]*)"', fact['value'])
            resolved = posixpath.normpath(posixpath.join(posixpath.dirname(path), target[1])) if target else None
            if resolved not in sources:
                failures.append(f'by-path module outside the ratcheted source set: {path}: '
                                f'{target[1] if target else fact["value"]}')
    for identity, count in inventory(facts).items():
        # A new narrow reasoned expectation is reviewed in-source, not another
        # baseline entry. Unknown macros/effects/globs/statics fail until owned.
        if identity[0] == 'exception' and 'reason =' in identity[3]:
            continue
        if count > allowed[identity]:
            failures.append(f'unregistered source occurrence ({count - allowed[identity]}): {identity}')
    return failures + harness_layout(facts)
