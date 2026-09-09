"""Occurrence-sensitive Cargo JSON ratchet, not a set of rendered warning text."""
from collections import Counter
import json
import os
from pathlib import Path
import re
from common import DENIED, ROOT, digest

FIELDS = ('lint', 'path', 'item', 'fingerprint')


def key(record):
    return tuple(record[field] for field in FIELDS)


def inventory(records):
    result = Counter()
    for record in records:
        identity = key(record)
        if identity in result or type(record['count']) is not int or record['count'] <= 0:
            raise ValueError('duplicate identity or invalid allowance multiplicity')
        result[identity] = record['count']
    return result


def records(counts, metrics=None):
    return [dict(zip(FIELDS, identity), count=count, **({'metric': metrics[identity]} if metrics and identity in metrics else {}))
            for identity, count in sorted(counts.items())]


def metric_limits(rows):
    return {key(row): row['metric'] for row in rows if 'metric' in row}


def metric_growth(now, before):
    return [f'structural warning grew: {identity}: {n} > {before.get(identity)}'
            for identity, n in now.items() if n > before.get(identity, 0)]


def owner(items, line, column):
    def covers(item):
        loc = item['location']
        return (loc['line'], loc['column']) <= (line, column) <= (loc['end_line'], loc['end_column'])
    matches = [item for item in items if covers(item)]
    if not matches:
        return 'crate'
    return min(matches, key=lambda i: (i['location']['end_line'] - i['location']['line'],
               i['location']['end_column'] - i['location']['column']))['qualified']


def parse(log, sources, facts, root=ROOT, capture=False, metrics=None):
    found, locations, failures = Counter(), set(), []
    finished = False
    for raw in Path(log).read_text().splitlines():
        message = json.loads(raw)  # Malformed/non-JSON output is never success.
        reason = message.get('reason')
        if reason == 'build-finished':
            finished = message.get('success') is True
        if reason != 'compiler-message':
            continue
        diag = message['message']
        if diag['level'] not in ('warning', 'error', 'failure-note'):
            continue
        lint = (diag.get('code') or {}).get('code')
        if not lint or (diag['level'] != 'warning' and not capture):
            failures.append(f"unallowable diagnostic: {lint}: {diag['message']}")
            continue
        if lint in DENIED:
            if not capture:
                failures.append(f"denied lint: {lint}: {diag['message']}")
            continue
        spans = [s for s in diag['spans'] if s['is_primary']]
        if not spans:
            failures.append(f'no source occurrence: {lint}')
        # Multi-primary diagnostics are one diagnostic with all physical anchors.
        physical, anchors, paths = [], [], []
        for span in spans:
            path = os.path.relpath(os.path.normpath(root / span['file_name']), root)
            if path not in sources:
                failures.append(f'unknown diagnostic source: {lint}: {path}')
                continue
            physical.append((path, span['byte_start'], span['byte_end']))
            paths.append(path)
            lines = sources[path].splitlines()
            excerpt = '\n'.join(lines[span['line_start']-1:span['line_end']])
            # Include message to catch growing numeric structural violations.
            anchors.append((owner(facts[path]['items'], span['line_start'], span['column_start']-1),
                            re.sub(r'\s+', ' ', excerpt).strip()))
        if not physical:
            continue
        occurrence = (lint, tuple(physical), diag['message'])
        if occurrence in locations:
            continue  # Same physical occurrence in lib/bin/test compilations.
        locations.add(occurrence)
        message_text = diag['message']
        magnitude = None
        if lint in ('clippy::too_many_lines', 'clippy::too_many_arguments'):
            match = re.search(r'\((\d+)/(\d+)\)', message_text)
            if match:
                magnitude = int(match[1])
                message_text = message_text[:match.start()] + '(COUNT/LIMIT)' + message_text[match.end():]
        identity = (lint, paths[0], anchors[0][0], digest(json.dumps([anchors, message_text])))
        if metrics is not None and magnitude is not None:
            metrics[identity] = max(metrics.get(identity, 0), magnitude)
        found[identity] += 1
    if not finished:
        failures.append('Cargo did not report a successful complete build')
    return found, failures


def compare(current, allowed):
    return [f'new warning ({count - allowed[identity]} occurrence(s)): {identity}'
            for identity, count in current.items() if count > allowed[identity]]
