"""Shared provenance and parsed-source access; subprocess errors always propagate."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
from dataclasses import dataclass

ROOT = Path(__file__).resolve().parents[2]
ANCHOR = '5bdaf9684197ff84bd544fd0fcd69520001ea196'
DENIED = {'unsafe_op_in_unsafe_fn', 'unused_must_use', 'unfulfilled_lint_expectations',
          'clippy::await_holding_lock', 'clippy::await_holding_refcell_ref',
          'clippy::await_holding_invalid_type', 'clippy::undocumented_unsafe_blocks',
          # An unprefixed Clippy lint name still works with this warning
          # allowed, and would sit outside every clippy:: exception contract.
          'renamed_and_removed_lints'}


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True).strip()


def merge_base():
    # Source/debt ratchets retain merge-base semantics. Verification selection
    # uses verification_comparison() below because push and schedule events have
    # different meanings from a pull request. On a push origin/<branch> is the
    # pushed commit itself, so only the event's previous revision is a base;
    # without it the ratchet would compare HEAD with HEAD and pass any growth.
    target = os.environ.get('QUALITY_BASE_REF') or 'origin/slate'
    before = os.environ.get('QUALITY_BEFORE_SHA', '')
    event = os.environ.get('QUALITY_EVENT_NAME') or os.environ.get('GITHUB_EVENT_NAME', '')
    if event == 'push':
        if not before:
            raise ValueError('push ratchet requires QUALITY_BEFORE_SHA')
        if set(before) == {'0'}:
            # A branch-creating push: CI's base ref is the pushed commit.
            raise ValueError('push ratchet has no previous revision (branch creation)')
        target = before
    return git('merge-base', 'HEAD', target)


@dataclass(frozen=True)
class VerificationComparison:
    event: str
    checkout_revision: str
    comparison_revision: str
    kind: str


def _exists(revision, kind='commit'):
    return subprocess.run(
        ['git', 'cat-file', '-e', f'{revision}^{{{kind}}}'], cwd=ROOT,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ).returncode == 0


def _empty_tree():
    return subprocess.check_output(['git', 'mktree'], cwd=ROOT, input=b'').decode().strip()


def verification_comparison():
    """Resolve the event-correct tree comparison, failing closed.

    Pull requests use the target merge base. Pushes compare the exact previous
    pushed revision with the checkout, including non-ancestor force pushes.
    A branch-creation push deliberately compares with Git's empty tree. A
    missing previous push object is an error, never an empty HEAD-vs-HEAD plan.
    Schedules have no synthetic source diff; their owner rotation is selected
    by verification_plan instead.
    """
    event = os.environ.get('QUALITY_EVENT_NAME') or os.environ.get('GITHUB_EVENT_NAME') or 'local'
    checkout = git('rev-parse', 'HEAD')
    expected_checkout = os.environ.get('QUALITY_HEAD_SHA', '').strip()
    if expected_checkout:
        expected_checkout = git('rev-parse', expected_checkout)
        if checkout != expected_checkout:
            raise ValueError(
                f'quality checkout mismatch: HEAD={checkout}, event revision={expected_checkout}'
            )

    if event.startswith('pull_request'):
        target = os.environ.get('QUALITY_BASE_REF', '').strip()
        if not target:
            raise ValueError('pull_request verification requires QUALITY_BASE_REF')
        if not _exists(target):
            raise ValueError(f'pull_request base is unavailable: {target}')
        return VerificationComparison(event, checkout, git('merge-base', checkout, target),
                                      'pull-request-merge-base')

    if event == 'push':
        before = os.environ.get('QUALITY_BEFORE_SHA', '').strip()
        if not before:
            raise ValueError('push verification requires QUALITY_BEFORE_SHA')
        if set(before) == {'0'}:
            return VerificationComparison(event, checkout, _empty_tree(), 'push-branch-creation')
        if not _exists(before):
            raise ValueError(
                f'previous push revision is unavailable: {before}; fetch it or fail the run'
            )
        ancestor = subprocess.run(
            ['git', 'merge-base', '--is-ancestor', before, checkout], cwd=ROOT,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        ).returncode == 0
        kind = 'push-previous-revision' if ancestor else 'push-force-update'
        return VerificationComparison(event, checkout, git('rev-parse', before), kind)

    if event == 'schedule':
        return VerificationComparison(event, checkout, '', 'scheduled-owner-rotation')

    target = os.environ.get('QUALITY_BASE_REF') or 'origin/slate'
    if not _exists(target):
        raise ValueError(f'local verification base is unavailable: {target}')
    return VerificationComparison(event, checkout, git('merge-base', checkout, target),
                                  'local-merge-base')


def source_directory(name, top=False):
    """One rule for the checkout walk and base listings: hidden directories
    and dependency trees hold no ratcheted source, nor does Cargo's build
    directory at the root. A module directory named `target` below it does."""
    return not name.startswith('.') and name != 'node_modules' and not (top and name == 'target')


def source_path(path):
    parts = path.split('/')[:-1]
    return all(source_directory(part, index == 0) for index, part in enumerate(parts))


def tracked_sources(root=ROOT):
    # Include untracked new source, exclude build/dependency/generated directories.
    sources = {}
    for directory, subdirs, files in os.walk(root):
        top = Path(directory) == Path(root)
        subdirs[:] = sorted(d for d in subdirs if source_directory(d, top))
        for name in sorted(files):
            if name.endswith('.rs'):
                p = Path(directory) / name
                sources[str(p.relative_to(root))] = p.read_text()
    return sources


def syntax(sources):
    binary = Path(os.environ.get('QUALITY_SYNTAX', ROOT / 'target/debug/streams-quality-syntax'))
    fragments = json.loads((ROOT / 'docs/quality/syntax-fragments.json').read_text())
    skipped = {}
    for path, entry in fragments.items():
        if path in sources:
            if digest(sources[path]) != entry['sha256']:
                raise ValueError(f'changed unparsed template: {path}')
            skipped[path] = {'path': path, 'items': [], 'facts': []}
    result = subprocess.run([str(binary)], input=json.dumps([
        {'path': path, 'source': source} for path, source in sources.items() if path not in skipped]),
        text=True, capture_output=True, check=True)
    return skipped | {s['path']: s for s in json.loads(result.stdout)}


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def diagnostic_paths():
    host = next(line.removeprefix('host: ') for line in
                subprocess.check_output(['rustc', '-vV'], text=True).splitlines() if line.startswith('host: '))
    suffix = {'aarch64-apple-darwin': '', 'x86_64-unknown-linux-gnu': '-linux'}.get(host)
    if suffix is None:
        raise ValueError(f'unregistered quality target {host}; review a platform inventory first')
    return (ROOT / f'docs/quality/legacy-diagnostics{suffix}.json',
            ROOT / f'docs/quality/diagnostic-allowances{suffix}.json')
