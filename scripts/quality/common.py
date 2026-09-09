"""Shared provenance and parsed-source access; subprocess errors always propagate."""
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
ANCHOR = '5bdaf9684197ff84bd544fd0fcd69520001ea196'
DENIED = {'unsafe_op_in_unsafe_fn', 'unused_must_use', 'unfulfilled_lint_expectations',
          'clippy::await_holding_lock', 'clippy::await_holding_refcell_ref',
          'clippy::await_holding_invalid_type', 'clippy::undocumented_unsafe_blocks'}


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True).strip()


def merge_base():
    # CI supplies the actual target ref, never HEAD's parent or a guessed diff.
    target = os.environ.get('QUALITY_BASE_REF', 'origin/slate')
    return git('merge-base', 'HEAD', target)


def tracked_sources(root=ROOT):
    # Include untracked new source, exclude build/dependency/generated directories.
    excluded = {'.git', 'target', 'node_modules', '.agents', '.cursor'}
    sources = {}
    for directory, subdirs, files in os.walk(root):
        subdirs[:] = sorted(d for d in subdirs if d not in excluded and not d.startswith('.'))
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
