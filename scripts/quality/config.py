"""Fail closed on tool/profile/source drift and an undeclared feature matrix."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tomllib
from common import ROOT


def read_toml(path):
    return tomllib.loads((ROOT / path).read_text())


def check(tools=True):
    problems = []
    pins = read_toml('quality-tools.toml')
    toolchain = read_toml('rust-toolchain.toml')['toolchain']
    if toolchain['channel'] != pins['rust'] or set(toolchain['components']) != {'clippy', 'rustfmt'}:
        problems.append('root Rust/component pin differs from quality-tools.toml')
    workspace = read_toml('Cargo.toml')
    banned = read_toml('deny.toml').get('bans', {}).get('deny', [])
    for crate in ('openssl', 'native-tls'):
        if not any(entry.get('crate') == crate and entry.get('reason')
                   and not entry.get('wrappers') for entry in banned):
            problems.append(f'rustls-only TLS policy requires an unconditional package ban: {crate}')
    genesis = json.loads((ROOT / 'docs/quality/legacy-diagnostics.json').read_text())
    if workspace['workspace']['lints'] != genesis['lint_profile']:
        problems.append('lint profile changed; explicit toolchain/profile migration required')
    if hashlib.sha256((ROOT / 'clippy.toml').read_bytes()).hexdigest() != genesis['clippy_sha256']:
        problems.append('Clippy thresholds/method paths differ from the adopted profile')
    for name in ('RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTC_WORKSPACE_WRAPPER', 'RUSTC_WRAPPER', 'RUSTC', 'CLIPPY_CONF_DIR'):
        if os.environ.get(name):
            problems.append(f'unapproved compiler/profile override: {name}')
    manifests = ['Cargo.toml', *[str(Path(p) / 'Cargo.toml') for p in workspace['workspace']['members']]]
    for manifest in manifests:
        value = read_toml(manifest)
        if value.get('lints') != {'workspace': True}:
            problems.append(f'workspace lint opt-in required: {manifest}')
        if value.get('features'):
            problems.append(f'new feature declaration requires a reviewed compatible matrix: {manifest}')
    # Cargo-deny permits the source repository; this gate pins its exact commit.
    expected = pins['slatedb']
    if workspace['patch']['crates-io']['slatedb'] != expected:
        problems.append('SlateDB patch differs from the reviewed exact revision')
    source = f"git+{expected['git']}?rev={expected['rev']}#{expected['rev']}"
    for package in read_toml('Cargo.lock')['package']:
        actual = package.get('source', '')
        if actual.startswith('git+') and actual != source:
            problems.append(f'unapproved Git revision: {package["name"]}: {actual}')
    skill = json.loads((ROOT / 'docs/quality/review-skill-pin.json').read_text())
    data = (ROOT / skill['local_path']).read_bytes()
    if hashlib.sha256(data).hexdigest() != skill['sha256']:
        problems.append('imported review skill differs from its pin')
    blob = hashlib.sha1(f'blob {len(data)}\0'.encode() + data).hexdigest()
    if blob != skill['git_blob']:
        problems.append('review skill Git blob mismatch')
    if tools:
        commands = [(['rustc', '--version'], f"rustc {pins['rust']} "),
                    (['cargo', '--version'], f"cargo {pins['rust']} "),
                    (['cargo', 'clippy', '--version'], 'clippy 0.1.98 '),
                    (['cargo', 'machete', '--version'], pins['tools']['cargo-machete']),
                    (['cargo', 'deny', '--version'], f"cargo-deny {pins['tools']['cargo-deny']}"),
                    (['actionlint', '--version'], pins['actionlint'])]
        for command, prefix in commands:
            try:
                output = subprocess.check_output(command, text=True).strip()
            except FileNotFoundError:
                problems.append(f'missing required tool: {command[0]}; run scripts/install-quality-tools.sh and add target/quality-tools/bin to PATH')
                continue
            except subprocess.CalledProcessError as error:
                problems.append(f'tool version check failed: {command}: exit {error.returncode}')
                continue
            if output.split()[:len(prefix.split())] != prefix.split():
                problems.append(f'tool version mismatch: {command}: {output}')
    return problems
