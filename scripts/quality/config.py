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
    # Cargo-deny permits the source repositories; this gate pins their exact
    # commits. Every patched crate is SlateDB's or foyer's (`slatedb`,
    # `slatedb-*`, `foyer`, `foyer-*`) at its family's reviewed revision.
    families = {name: pins[name] for name in ('slatedb', 'foyer') if name in pins}
    for name, pin in families.items():
        if pin['git'] != pin.get('upstream', pin['git']) and not pin.get('carries'):
            problems.append(f'{name} fork pin names no upstream commits it carries')
    for crate, spec in workspace['patch']['crates-io'].items():
        pin = families.get(crate.split('-')[0], {})
        if spec != {'git': pin.get('git'), 'rev': pin.get('rev')}:
            problems.append(f'{crate} patch differs from the reviewed exact revision')
    sources = {f"git+{pin['git']}?rev={pin['rev']}#{pin['rev']}" for pin in families.values()}
    for package in read_toml('Cargo.lock')['package']:
        actual = package.get('source', '')
        if actual.startswith('git+') and actual not in sources:
            problems.append(f'unapproved Git revision: {package["name"]}: {actual}')
    skill = json.loads((ROOT / 'docs/quality/review-skill-pin.json').read_text())
    data = (ROOT / skill['local_path']).read_bytes()
    if hashlib.sha256(data).hexdigest() != skill['sha256']:
        problems.append('imported review skill differs from its pin')
    blob = hashlib.sha1(f'blob {len(data)}\0'.encode() + data).hexdigest()
    if blob != skill['git_blob']:
        problems.append('review skill Git blob mismatch')
    if tools:
        for name, pin in families.items():
            if 'upstream' in pin:
                problems.extend(fork_problems(name, pin, ROOT / 'target/quality/forks' / name))
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


def git(cache, *args):
    return subprocess.run(['git', '--git-dir', str(cache), *args], check=True,
                          capture_output=True, text=True).stdout.strip()


def fork_problems(name, pin, cache):
    """A fork pin's revision is its upstream base plus exactly the commits
    `carries` lists, in order, none a merge. An entry may name the fork
    commit itself (`fork`), and says what it is: `docs` (it touches only
    FORK.md and fork/), or an upstream `pr` or `issue`. A `commit` is an
    upstream commit of that PR whose patch (`git patch-id --stable`, so a
    cherry-pick's trailer does not count) the fork commit must equal;
    `pr_diff` asks the fork commit to equal the PR's whole diff. An entry
    with neither must name its fork commit. When the fork has a FORK.md,
    every PR or issue it carries is listed there. Fetches into `cache`."""
    carries = pin.get('carries', [])
    try:
        # A restored CI cache can hold the directory without its repository.
        if not (cache / 'HEAD').is_file():
            subprocess.run(['git', 'init', '--quiet', '--bare', str(cache)], check=True)
        prs = sorted({c['pr'] for c in carries if 'pr' in c})
        heads = [f"+refs/pull/{n}/head:refs/pr/{n}" for n in prs]
        named = [c['commit'] for c in carries if 'commit' in c]
        git(cache, 'fetch', '--quiet', '--no-tags', pin['upstream'], pin['base'], *named, *heads)
        git(cache, 'fetch', '--quiet', '--no-tags', pin['git'], pin['rev'])
        carried = git(cache, 'rev-list', '--reverse', '--first-parent',
                      f"{pin['base']}..{pin['rev']}").split()
        if git(cache, 'merge-base', pin['base'], pin['rev']) != git(cache, 'rev-parse', pin['base']):
            return [f"{name} fork revision does not descend from its upstream base {pin['base']}"]
        if len(carried) != len(carries):
            return [f'{name} fork carries {len(carried)} commits over its base, not the {len(carries)} it names']
        listing = subprocess.run(['git', '--git-dir', str(cache), 'show', f"{pin['rev']}:FORK.md"],
                                 capture_output=True, text=True)
        fork_md = listing.stdout if listing.returncode == 0 else None
        problems = []
        for commit, entry in zip(carried, carries):
            problems.extend(carried_problems(name, cache, pin, commit, entry, fork_md))
        return problems
    except subprocess.CalledProcessError as error:
        return [f'{name} fork history could not be verified: {error.cmd}: {error.stderr.strip()}']


def carried_problems(name, cache, pin, commit, entry, fork_md):
    if git(cache, 'rev-list', '--count', '--merges', f'{commit}^!') != '0':
        return [f'{name} fork commit {commit} is a merge']
    problems = []
    if 'fork' in entry and git(cache, 'rev-parse', entry['fork']) != commit:
        problems.append(f"{name} fork commit {commit} is not the named {entry['fork']}")
    if entry.get('docs'):
        touched = git(cache, 'diff-tree', '--no-commit-id', '--name-only', '-r', commit).split()
        if any(p != 'FORK.md' and not p.startswith('fork/') for p in touched):
            problems.append(f'{name} docs commit {commit} touches {touched}')
        return problems
    ref = entry.get('pr') or entry.get('issue')
    if ref is None:
        return problems + [f'{name} fork commit {commit} names no upstream PR or issue']
    if fork_md is not None and f'#{ref}' not in fork_md:
        problems.append(f'{name}: FORK.md does not list #{ref}')
    if 'commit' in entry:
        in_pr = subprocess.run(['git', '--git-dir', str(cache), 'merge-base', '--is-ancestor',
                                entry['commit'], f"refs/pr/{entry['pr']}"]).returncode == 0
        if not in_pr:
            problems.append(f"{name}: {entry['commit']} is not a commit of upstream PR #{entry['pr']}")
        if patch_id(cache, commit) != patch_id(cache, entry['commit']):
            problems.append(f"{name} fork commit {commit} is not upstream {entry['commit']}")
    elif entry.get('pr_diff'):
        head = f"refs/pr/{entry['pr']}"
        whole = git(cache, 'diff', git(cache, 'merge-base', pin['base'], head), head)
        if patch_id(cache, commit) != patch_id_of(whole):
            problems.append(f"{name} fork commit {commit} is not upstream PR #{entry['pr']}")
    elif 'fork' not in entry:
        problems.append(f'{name} fork commit {commit} must be named: it matches no upstream commit')
    return problems


def patch_id(cache, commit):
    return patch_id_of(git(cache, 'show', '--format=', commit))


def patch_id_of(diff):
    out = subprocess.run(['git', 'patch-id', '--stable'], input=diff + '\n', check=True,
                         capture_output=True, text=True).stdout.split()
    return out[0] if out else ''
