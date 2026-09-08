#!/usr/bin/env python3
"""Capture a platform inventory from the immutable adoption commit.

This never updates an acceptance file. The reviewer must inspect the resulting
metadata and approve the one-time platform inventory before the gate can pass.
Denied lints are excluded even though this legacy-only capture caps lints.
"""
import argparse
import collections
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
from common import ANCHOR, ROOT, digest, git, syntax, tracked_sources, write_json
from diagnostics import parse, records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    output = Path(args.out).resolve()
    if output.exists():
        raise RuntimeError('refusing to overwrite an inventory')
    output.parent.mkdir(parents=True, exist_ok=True)
    profile = json.loads((ROOT / 'docs/quality/legacy-diagnostics.json').read_text())
    version = subprocess.check_output(['rustc', '--version'], text=True)
    if not version.startswith(f"rustc {profile['rust']} "):
        raise RuntimeError('capture compiler differs from adopted pin')
    host = next(line.removeprefix('host: ') for line in subprocess.check_output(
        ['rustc', '-vV'], text=True).splitlines() if line.startswith('host: '))
    with tempfile.TemporaryDirectory(prefix='streams-legacy-capture-') as tmp:
        source = Path(tmp) / 'source'; source.mkdir()
        archive = subprocess.check_output(['git', 'archive', ANCHOR], cwd=ROOT)
        with tarfile.open(fileobj=io.BytesIO(archive)) as tree:
            tree.extractall(source, filter='data')
        manifest = (ROOT / 'Cargo.toml').read_text()
        lints = manifest[manifest.index('[workspace.lints.rust]'):manifest.index('[workspace]\n')]
        with (source / 'Cargo.toml').open('a') as cargo:
            cargo.write('\n' + lints.replace('[workspace.lints.', '[lints.'))
        for name in ('rust-toolchain.toml', 'clippy.toml'):
            shutil.copy2(ROOT / name, source / name)
        for path, names in profile['scope_overlay'].items():
            file = source / path
            file.write_text('#![warn(' + ', '.join('clippy::' + name for name in names) + ')]\n' + file.read_text())
        log = Path(tmp) / 'clippy.jsonl'
        with log.open('w') as stdout, (Path(tmp) / 'clippy.stderr').open('w') as stderr:
            subprocess.run(['cargo', 'clippy', '--locked', '--all-targets', '--message-format=json',
                            '--', '--cap-lints', 'warn'], cwd=source, stdout=stdout, stderr=stderr, check=True)
        sources = tracked_sources(source); facts = syntax(sources); metrics = {}
        warnings, errors = parse(log, sources, facts, source, capture=True, metrics=metrics)
        if errors:
            raise RuntimeError(errors)
        result = dict(profile, target=host, warnings=records(warnings, metrics),
                      source_sha256={p:digest(s) for p,s in sources.items()},
                      source_tree=git('rev-parse', f'{ANCHOR}^{{tree}}'))
        write_json(output, result)
        print(f'captured {sum(warnings.values())} legacy occurrences on {host}, from {ANCHOR}')


if __name__ == '__main__':
    main()
