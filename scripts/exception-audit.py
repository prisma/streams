#!/usr/bin/env python3
"""Classify exception reason edits, moves and renames on a first-parent range.

Usage: python3 scripts/exception-audit.py <start> <end>

Each commit is compared with its first parent using the source ratchet's own
contracts (scripts/quality/source_rules.py), over the Rust files it changed.
Prints Markdown: reason edits, moves and renames without a reason edit,
unmatched vanish/appear pairs, a per-commit roll-up and the reason texts.
"""
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / 'quality'))
from common import ROOT, source_path, syntax  # noqa: E402
import source_rules as rules  # noqa: E402

GREW = 'accepted exception grew without an approved growth row: '
RANK = ['explanation update', 'scope reduction', 'scope expansion (fingerprint-only)', 'scope expansion']


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True)


def templates(revision):
    shown = subprocess.run(['git', 'show', f'{revision}:docs/quality/syntax-fragments.json'],
                           cwd=ROOT, capture_output=True, text=True)
    return set(json.loads(shown.stdout)) if shown.returncode == 0 else set()


def changed(parent, commit):
    skipped = templates(parent) | templates(commit)
    skipped |= set(json.loads((ROOT / 'docs/quality/syntax-fragments.json').read_text()))
    names = git('diff', '--no-renames', '--name-only', '-z', parent, commit, '--', '*.rs').split('\0')
    return [p for p in names if p and p not in skipped and source_path(p)]


def load(revision, paths):
    shown = {p: subprocess.run(['git', 'show', f'{revision}:{p}'], cwd=ROOT, capture_output=True)
             for p in paths}
    return {p: r.stdout.decode() for p, r in shown.items() if r.returncode == 0}


def reasons(sources, facts):
    """identity -> the reason texts of its attributes."""
    found = {}
    for path, attribute, kind, _ in rules.exception_scopes(sources, facts):
        reason = attribute['value'].split('reason', 1)[1].strip().removeprefix('=').strip().rstrip(')').strip()
        for lint in rules._exception_lints(attribute['value']):
            found.setdefault((path, attribute['qualified'], kind, lint), set()).add(reason)
    return found


def classify(before, after):
    grown = [m for m, n in after.items() if n > before[m]]
    if grown:
        return RANK[2] if all(':' in m for m in grown) else RANK[3]
    return RANK[1] if any(after[m] < n for m, n in before.items()) else RANK[0]


def audit(start, end):
    edits, moves, unmatched, commits = [], [], [], []
    for commit in git('rev-list', '--first-parent', '--reverse', f'{start}..{end}').split():
        commits.append(commit)
        paths = changed(f'{commit}^1', commit)
        if not paths:
            continue
        before, after = load(f'{commit}^1', paths), load(commit, paths)
        facts_before = syntax(before) if before else {}
        facts_after = syntax(after) if after else {}
        old = rules.exception_contracts(before, facts_before)
        new = rules.exception_contracts(after, facts_after)
        text_before = reasons(before, facts_before)
        text_after = reasons(after, facts_after)
        failing = rules.exception_growth(new, old)
        pairs = rules.exception_predecessors(new, old)
        for identity, (floor, origins, note) in pairs.items():
            reasons_before = set().union(*(text_before[o] for o in origins or [identity]))
            reasons_after = text_after[identity]
            if reasons_before == reasons_after and not note:
                continue
            row = dict(commit=commit, identity=identity, note=note, before=floor, after=new[identity],
                       reasons=(sorted(reasons_before), sorted(reasons_after)),
                       cls=classify(floor, new[identity]),
                       gate='row needed' if any(f.startswith(f'{GREW}{identity}') for f in failing) else 'pass')
            (edits if reasons_before != reasons_after else moves).append(row)
        used = {o for _, origins, _ in pairs.values() for o in origins}
        vanished = [v for v in old if v not in new and v not in used]
        appeared = [a for a in new if a not in pairs]
        unmatched += [(commit, v, a) for v in vanished for a in appeared if v[0] == a[0] and v[3] == a[3]]
    return commits, edits, moves, unmatched


def subject(commit):
    text = git('log', '-1', '--format=%s', commit).strip()
    return text if len(text) <= 60 else text[:57] + '...'


def arrow(row, metric):
    return f'{row["before"][metric]}→{row["after"][metric]}'


def sites(row):
    lint = row['identity'][3]
    metric = {'clippy::unwrap_used': 'unwrap_sites', 'clippy::expect_used': 'expect_sites',
              'dead_code': 'fields'}.get(lint)
    return arrow(row, metric) if metric else '—'


def fingerprints(row):
    keys = {m for m in (*row['before'], *row['after']) if ':' in m}
    grown = sum(1 for m in keys if row['after'][m] > row['before'][m])
    gone = sum(1 for m in keys if row['after'][m] < row['before'][m])
    return f'+{grown} −{gone}' if grown or gone else '='


def contract(identity):
    return ' · '.join(f'`{part}`' for part in identity)


def table(rows, reasons, source_column):
    lines = [f'| # | Commit | Subject | Contract | {source_column} | Code lines | Items | Facts | Sites | '
             'Fingerprints | Class | New gate |',
             '| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |']
    for n, row in enumerate(rows, 1):
        if source_column == 'Reason':
            reasons.append((row, len(reasons) + 1))
            source = f'A-{len(reasons)}'
        else:
            source = row['note'].strip(' ()') or '—'
        lines.append(
            f'| {n} | {row["commit"][:8]} | {subject(row["commit"])} | {contract(row["identity"])} | '
            f'{source}{row["note"] if source_column == "Reason" and row["note"] else ""} | '
            f'{arrow(row, "scope_lines")} | {arrow(row, "nested_items")} | {arrow(row, "syntax_facts")} | '
            f'{sites(row)} | {fingerprints(row)} | {row["cls"]} | {row["gate"]} |')
    return lines


def main(start, end):
    commits, edits, moves, unmatched = audit(start, end)
    reasons = []
    out = ['## Table 1 — reason edits', '']
    out += table(edits, reasons, 'Reason')
    out += ['', '## Table 2 — moves and renames without a reason edit', '']
    out += table(moves, reasons, 'Moved or renamed from')
    out += ['', '## Table 3 — unmatched vanish/appear pairs (same file and lint)', '',
            '| Commit | Subject | Vanished contract | Appeared contract |', '| --- | --- | --- | --- |']
    out += [f'| {c[:8]} | {subject(c)} | {contract(v)} | {contract(a)} |' for c, v, a in unmatched]
    out += ['', '## Roll-up — worst class per commit', '',
            '| Commit | Subject | Reason edits (worst) | Moves/renames (worst) | New gate |', '| --- | --- | --- | --- | --- |']
    for commit in commits:
        mine_edits = [r for r in edits if r['commit'] == commit]
        mine_moves = [r for r in moves if r['commit'] == commit]
        if not mine_edits and not mine_moves:
            continue
        worst = [max((r['cls'] for r in rows), key=RANK.index, default='—') for rows in (mine_edits, mine_moves)]
        gate = 'row needed' if any(r['gate'] == 'row needed' for r in mine_edits + mine_moves) else 'pass'
        out.append(f'| {commit[:8]} | {subject(commit)} | {worst[0]} | {worst[1]} | {gate} |')
    out += ['', '## Appendix A — reason texts', '']
    for row, n in reasons:
        out.append(f'**A-{n}** {row["commit"][:8]} {contract(row["identity"])}')
        out.append('')
        for label, texts in (('before', row['reasons'][0]), ('after', row['reasons'][1])):
            for text in texts:
                out.append(f'- {label}: {text}')
        out.append('')
    out.append(f'<!-- commits: {len(commits)}, reason edits: {len(edits)}, moves/renames: {len(moves)}, '
               f'unmatched: {len(unmatched)} -->')
    print('\n'.join(out))


if __name__ == '__main__':
    main(*sys.argv[1:3])
