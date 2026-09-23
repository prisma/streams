#!/usr/bin/env python3
"""Install the version- and checksum-pinned formal-verification tools.

TLC is the pinned tla2tools.jar release. Kani is built from its pinned
crates.io release and set up from a checksum-verified release bundle, so no
floating download enters a receipt. Kani then installs its own dated nightly
through rustup: that compiler is a separate analysis configuration recorded in
quality-tools.toml; production gates keep the root toolchain pin.
"""
import hashlib
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import tomllib
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
TOOLS = ROOT / 'target/quality-tools'


BUNDLE_TARGETS = {
    ('Darwin', 'arm64'): 'aarch64-apple-darwin',
    ('Darwin', 'x86_64'): 'x86_64-apple-darwin',
    ('Linux', 'x86_64'): 'x86_64-unknown-linux-gnu',
    ('Linux', 'aarch64'): 'aarch64-unknown-linux-gnu',
}


def bundle_target():
    """The hardware's bundle, which is the one Kani's own setup selects. Python or
    the Rust toolchain may run translated by Rosetta and report x86_64."""
    system, machine = platform.system(), platform.machine()
    if system == 'Darwin' and subprocess.run(['sysctl', '-n', 'hw.optional.arm64'], text=True,
                                             capture_output=True).stdout.strip() == '1':
        machine = 'arm64'
    return BUNDLE_TARGETS.get((system, machine))


def fetch(url, expected):
    with urllib.request.urlopen(url) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != expected:
        raise SystemExit(f'checksum mismatch for {url}')
    return data


def install_tlc(pins):
    jar = TOOLS / 'tla2tools.jar'
    expected = pins['tla2tools-sha256']
    if jar.is_file() and hashlib.sha256(jar.read_bytes()).hexdigest() == expected:
        print(f'TLC {pins["tlc"]} already installed: {jar}')
        return
    url = f'https://github.com/tlaplus/tlaplus/releases/download/v{pins["tla2tools"]}/tla2tools.jar'
    jar.parent.mkdir(parents=True, exist_ok=True)
    jar.write_bytes(fetch(url, expected))
    print(f'Installed tla2tools {pins["tla2tools"]} (TLC {pins["tlc"]}): {jar}')


def install_kani(pins):
    version = pins['kani']
    subprocess.run(['cargo', 'install', 'kani-verifier', '--version', f'={version}', '--locked',
                    '--root', str(TOOLS)], check=True)
    if (Path.home() / f'.kani/kani-{version}').is_dir():
        print(f'Kani {version} bundle already set up')
        return
    target = bundle_target()
    if target not in pins['kani-bundle-sha256']:
        raise SystemExit(f'Kani has no reviewed bundle checksum for {platform.system()} {platform.machine()}')
    name = f'kani-{version}-{target}.tar.gz'
    url = f'https://github.com/model-checking/kani/releases/download/kani-{version}/{name}'
    data = fetch(url, pins['kani-bundle-sha256'][target])
    env = dict(os.environ, PATH=f'{TOOLS / "bin"}{os.pathsep}{os.environ.get("PATH", "")}')
    with tempfile.TemporaryDirectory() as scratch:
        bundle = Path(scratch) / name
        bundle.write_bytes(data)
        subprocess.run(['cargo', 'kani', 'setup', '--use-local-bundle', str(bundle)], check=True,
                       env=env)
    print(f'Installed Kani {version} ({pins["kani-rust"]}): {TOOLS / "bin/cargo-kani"}')


def main():
    pins = tomllib.loads((ROOT / 'quality-tools.toml').read_text())['formal']
    install_tlc(pins)
    install_kani(pins)


if __name__ == '__main__':
    main()
