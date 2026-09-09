#!/usr/bin/env python3
"""Install the version- and checksum-pinned workflow linter for local/CI gates."""
import hashlib
import io
from pathlib import Path
import platform
import tarfile
import tomllib
import urllib.request


def main():
    root = Path(__file__).resolve().parent.parent
    pins = tomllib.loads((root / 'quality-tools.toml').read_text())
    arch = {'x86_64': 'amd64', 'aarch64': 'arm64'}.get(platform.machine(), platform.machine())
    target = f'{platform.system().lower()}_{arch}'
    expected = pins['actionlint-sha256'].get(target)
    if expected is None:
        raise SystemExit(f'actionlint has no reviewed binary checksum for {target}')
    version = pins['actionlint']
    url = f'https://github.com/rhysd/actionlint/releases/download/v{version}/actionlint_{version}_{target}.tar.gz'
    with urllib.request.urlopen(url) as response:
        archive = response.read()
    if hashlib.sha256(archive).hexdigest() != expected:
        raise SystemExit(f'actionlint {version} checksum mismatch for {target}')
    destination = root / 'target/quality-tools/bin/actionlint'
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=io.BytesIO(archive), mode='r:gz') as bundle:
        destination.write_bytes(bundle.extractfile('actionlint').read())
    destination.chmod(0o755)
    print(f'Installed actionlint {version}: {destination}')


if __name__ == '__main__':
    main()
