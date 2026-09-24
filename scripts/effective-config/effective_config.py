#!/usr/bin/env python3
"""Old-vs-new EFFECTIVE configuration, per deployment family.

The owner's deploy prerequisite (external review, 2026-09-24): no deploy of
the hardened binary until the configuration each deployed family really runs
with (real argv + env) has been compared between the last release
(v0.2.0-rc.4) and HEAD. Results for the owner:
docs/reviews/2026-09-hardening/effective-config-diff.md.

Sides
  new        examples/effective_config.rs at HEAD: the public facade
             (CliArgs -> ServerConfig::load -> {:#?} -> validate()).
  old-model  an INJECTED dumper (old/*.rs.in) in a temporary worktree of
             6bcaff69 (WP-01 PR 3, the first revision holding rc.4's
             configuration as one value), tied back to rc.4 by the
             equivalence gate E1/E2/E3. Its verdict is `not-evaluated`.
  rc.4/head  the real server binaries, booted against s3lite (Leg C, boot.py):
             the refusal truth for rc.4 and the validation notices.

Every revision builds into its OWN target directory, and every artifact is
copied and hashed right after its build; later steps run only the copies,
re-checking the hash (K13 proves old and new outputs come from different
binaries).

Family files (families/*.family), evaluated in file order, last setter wins
(the Compute CLI's "later --env wins"; Compute env is project-scoped and
merged, RUNBOOK §7.3, so other roles' names come first):
  source <script> "<anchor>"   where the family comes from (check-families)
  argv <args...>               the server's argv
  include <env file>           KEY=VALUE lines read from the tree at run time
  role server|gen|lb|project   whose --env the following env lines are
  env K=V                      a variable set by a deploy step
  supervisor K=V               consumed or set by the Compute supervisor
  platform K=V                 assumed platform-injected (PORT)
  omit K <reason>              a script name absent from this variant
  note <text>                  carried into the report
Nothing here writes a secret: a secret-class value must be a placeholder
(`placeholder-...` or a canned 32-byte key), or the family is refused before
anything is built or run (K7).
"""
from __future__ import annotations

import argparse
import ast
import base64
import concurrent.futures
import difflib
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
FAMILIES_DIR = HERE / 'families'
RENAME_MAP = HERE / 'rename-map.json'
OLD_TAG = 'v0.2.0-rc.4'
OLD_MODEL_REV = '6bcaff69'
TOOLCHAIN = '1.98.1'
DISK_FLOOR_GIB = 12
PROBE_WORKERS = 8
PROFILE = 'deploy/profiles/compute-1g.env'
COMPUTE_SCRIPTS = ['bench/fleet/deploy-fleet.sh', 'bench/soak/deploy-region.sh', 'bench/soak/mt-tenants.sh',
                   'bench/soak/wc-ladder.sh', 'scripts/bench-fra-ab.sh']
# Pins (K9, E1, E2): a new field, knob or reader changes one of these and the
# tool fails until the pin is updated deliberately.
PIN_OLD_LEAVES, PIN_NEW_LEAVES = 156, 155
PIN_ARGS_LINES = 522
PIN_RC4_NAMES, PIN_ENV_KNOBS, PIN_HELPERS = 72, 70, 6

# ---------------------------------------------------------------------------
# Families
# ---------------------------------------------------------------------------

CANNED_SECRETS = {
    # 32 x 0x09: the canary's own test key (bench/canary/livefeed-canary.mjs).
    'CQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQkJCQk=',
    'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
}
EXPLICIT_SECRETS = {
    'SLATE_S3_ACCESS_KEY_ID', 'SLATE_S3_SECRET_ACCESS_KEY', 'AUTH_TOKEN', 'FLEET_INTERNAL_TOKEN',
    'USAGE_STREAM_KEY', 'STREAMS_CURSOR_KEY', 'STREAM_KEY',
}
# Names that match KEY|CREDENTIAL|AUTH but carry no key material (C8): object
# keys, file paths, modes and cadences.
NON_SECRET_NAMES = {
    'L0_MAX_SSTS_PER_KEY', 'STREAMS_AUTH_MODE', 'STREAMS_AUTH_ISSUER',
    'STREAMS_AUTH_REFRESH_SECS', 'FLEET_AUTH_MODE',
}
NON_SECRET_SUFFIXES = ('_S3_KEY', '_FILE')
USERINFO = re.compile(r'://[^/@]*:[^/@]*@')
DIRECTIVES = ('source', 'argv', 'env', 'include', 'supervisor', 'platform', 'role', 'omit', 'note')
ROLES = ('server', 'gen', 'lb', 'project')
# Read outside the configuration graph on both sides (C5): tracing's filter,
# the allocator, reqwest's proxy discovery (peer.rs builds its client without
# .no_proxy()) and rustls-native-certs' trust-store overrides.
PROCESS_NAMES = {'RUST_LOG', 'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY', 'http_proxy',
                 'https_proxy', 'all_proxy', 'no_proxy', 'SSL_CERT_FILE', 'SSL_CERT_DIR'}
PROCESS_PREFIXES = ('MIMALLOC_',)


class FamilyError(Exception):
    pass


def is_secret_name(name):
    if name in EXPLICIT_SECRETS or name.endswith(('_ACCESS_KEY_ID', '_SECRET_ACCESS_KEY')):
        return True
    if re.search(r'SECRET|TOKEN|PASSWORD', name) and not name.endswith('_FILE'):
        return True
    if re.search(r'KEY|CREDENTIAL|AUTH', name):
        return not (name in NON_SECRET_NAMES or name.endswith(NON_SECRET_SUFFIXES))
    return False


def check_value(name, value):
    if value == '':
        raise FamilyError(f'{name}: empty value (the Compute CLI rejects --env KEY=)')
    if USERINFO.search(value):
        raise FamilyError(f'{name}: value carries URL userinfo (a credential); use a placeholder host')
    if is_secret_name(name) and not (value.startswith('placeholder-') or value in CANNED_SECRETS):
        raise FamilyError(f'{name} must be a placeholder')


class Family:
    def __init__(self, name):
        self.name = name
        self.sources = []        # (script, anchor)
        self.argv = []
        self.env = {}            # name -> value (the server process env)
        self.provenance = {}     # name -> server|gen|lb|project|include:<f>|supervisor|platform
        self.overridden = []     # (name, old, new)
        self.omitted = {}        # name -> reason
        self.includes = []
        self.notes = []
        self.sha256 = None

    def set(self, name, value, provenance):
        check_value(name, value)
        if name in self.env and self.env[name] != value:
            self.overridden.append((name, self.env[name], value))
        self.env[name] = value
        self.provenance[name] = provenance


def read_env_file(text):
    pairs = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        key, sep, value = line.partition('=')
        if not sep:
            raise FamilyError(f'include line without "=": {line!r}')
        pairs.append((key, value))
    return pairs


def split_assignment(rest, lineno):
    name, sep, value = rest.partition('=')
    if not sep or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', name):
        raise FamilyError(f'line {lineno}: expected NAME=VALUE, got {rest!r}')
    return name, value


def parse_family(name, text, root=ROOT):
    fam = Family(name)
    fam.sha256 = hashlib.sha256(text.encode()).hexdigest()
    role = 'server'
    for lineno, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        directive, _, rest = line.partition(' ')
        rest = rest.strip()
        if directive not in DIRECTIVES:
            raise FamilyError(f'line {lineno}: unknown directive {directive!r}')
        if directive == 'source':
            parts = shlex.split(rest)
            if len(parts) != 2:
                raise FamilyError(f'line {lineno}: source <script> "<anchor>"')
            fam.sources.append((parts[0], parts[1]))
        elif directive == 'argv':
            fam.argv.extend(shlex.split(rest))
        elif directive == 'role':
            if rest not in ROLES:
                raise FamilyError(f'line {lineno}: role must be one of {ROLES}')
            role = rest
        elif directive == 'include':
            fam.includes.append(rest)
            for key, value in read_env_file((root / rest).read_text()):
                fam.set(key, value, f'include:{rest}')
        elif directive in ('env', 'supervisor', 'platform'):
            key, value = split_assignment(rest, lineno)
            fam.set(key, value, role if directive == 'env' else directive)
        elif directive == 'omit':
            key, _, reason = rest.partition(' ')
            if not reason.strip():
                raise FamilyError(f'line {lineno}: omit NAME <reason>')
            fam.omitted[key] = reason.strip()
        else:
            fam.notes.append(rest)
    return fam


def load_family(path, root=ROOT):
    try:
        return parse_family(path.stem, path.read_text(), root)
    except FamilyError as error:
        raise FamilyError(f'family {path.stem}: {error}') from None


def load_families(directory=FAMILIES_DIR, only=None):
    paths = sorted(Path(directory).glob('*.family'))
    if only:
        paths = [p for p in paths if p.stem in only]
    if not paths:
        raise FamilyError(f'no .family files in {directory}')
    return [load_family(p) for p in paths]


def derive(base, name, env=None, drop=(), argv=None):
    fam = Family(name)
    fam.sources, fam.includes, fam.sha256 = list(base.sources), list(base.includes), base.sha256
    fam.argv = list(base.argv if argv is None else argv)
    fam.env, fam.provenance = dict(base.env), dict(base.provenance)
    for key in drop:
        fam.env.pop(key, None)
        fam.provenance.pop(key, None)
    for key, value in (env or {}).items():
        fam.set(key, value, 'server')
    return fam


# ---------------------------------------------------------------------------
# Flattening the pretty Debug output into `path = value` leaves
# ---------------------------------------------------------------------------

CLOSERS = {'}', '},', ')', '),', ']', '],'}
FIELD = re.compile(r'([A-Za-z_][A-Za-z0-9_]*): (.*)')
STRUCT_OPEN = re.compile(r'[A-Za-z_][A-Za-z0-9_:]*(<.*>)? \{')


def _opens(value):
    return not value.startswith('"') and value.endswith(('{', '(', '['))


def _render(opener, children, closer):
    if opener.endswith('{'):
        return f'{opener} {", ".join(children)} {closer}' if children else f'{opener}{closer}'
    return f'{opener}{", ".join(children)}{closer}'


def flatten_debug(lines, prefix='', start=1):
    """One pretty-Debug value -> {path: value}. Named structs expand into
    dotted paths (the root type name is dropped); every other value — Option,
    tuple, list, map, and structs inside them — is one leaf, rendered on one
    line. Any line that does not fit the grammar fails the run."""
    leaves, stack = {}, []

    def emit(path, value):
        if path in leaves:
            raise ValueError(f'duplicate Debug path {path}')
        leaves[path] = value

    def fail(lineno, raw):
        raise ValueError(f'unparsed Debug line {lineno}: {raw!r}')

    for offset, raw in enumerate(lines):
        lineno, s = start + offset, raw.strip()
        if not s:
            continue
        if not stack:
            if leaves:
                fail(lineno, raw)
            if STRUCT_OPEN.fullmatch(s):
                stack.append(['struct', prefix])
            elif _opens(s):
                stack.append(['compact', prefix, s, []])
            else:
                emit(prefix, s)
            continue
        top = stack[-1]
        if s in CLOSERS:
            frame = stack.pop()
            if frame[0] == 'compact':
                rendered = _render(frame[2], frame[3], s.rstrip(','))
                if stack and stack[-1][0] == 'compact':
                    stack[-1][3].append(rendered)
                else:
                    emit(frame[1], rendered)
            continue
        if top[0] == 'struct':
            match = FIELD.fullmatch(s)
            if not match:
                fail(lineno, raw)
            key, value = match.groups()
            path = f'{top[1]}.{key}' if top[1] else key
            if STRUCT_OPEN.fullmatch(value):
                stack.append(['struct', path])
            elif _opens(value):
                stack.append(['compact', path, value, []])
            elif value.endswith(','):
                emit(path, value[:-1])
            else:
                fail(lineno, raw)
        elif _opens(s):
            stack.append(['compact', None, s, []])
        elif s.endswith(','):
            top[3].append(s[:-1])
        else:
            fail(lineno, raw)
    if stack:
        raise ValueError(f'unterminated Debug value under {prefix or "root"!r}')
    return leaves


def normalize_verdict(lines):
    lines = [line.rstrip() for line in lines]
    while lines and not lines[-1]:
        lines.pop()
    return '\n'.join(lines)


def parse_dump(text):
    """`@@ <prefix>` sections, each one pretty-Debug value, then `@@ verdict`
    (raw text; println! adds a trailing blank line that is stripped)."""
    sections = []
    for lineno, line in enumerate(text.splitlines(), 1):
        if line.startswith('@@ '):
            sections.append((line[3:].strip(), lineno + 1, []))
        elif sections:
            sections[-1][2].append(line)
        elif line.strip():
            raise ValueError(f'unparsed Debug line {lineno}: output before the first section')
    leaves, verdict = {}, None
    for name, start, lines in sections:
        if name == 'verdict':
            verdict = normalize_verdict(lines)
            continue
        for path, value in flatten_debug(lines, '' if name == 'root' else name, start).items():
            if path in leaves:
                raise ValueError(f'duplicate Debug path {path}')
            leaves[path] = value
    return leaves, verdict


# ---------------------------------------------------------------------------
# Running the dumpers (always under env -i)
# ---------------------------------------------------------------------------

class Dump:
    def __init__(self, leaves, verdict, exit_code, stderr):
        self.leaves, self.verdict, self.exit_code, self.stderr = leaves, verdict, exit_code, stderr

    @property
    def verdict_word(self):
        return self.verdict.split('\n', 1)[0] if self.verdict else 'no-verdict'

    def signature(self):
        return (tuple(sorted(self.leaves.items())), self.verdict)


def env_argv(env):
    return ['env', '-i', *[f'{k}={v}' for k, v in env.items()]]


def run_dumper(binary, env, argv, timeout=60):
    proc = subprocess.run([*env_argv(env), str(binary), *argv], capture_output=True, text=True, timeout=timeout)
    leaves, verdict = parse_dump(proc.stdout)
    if verdict is None or proc.returncode != 0:
        tail = proc.stderr.strip().splitlines()[-3:]
        verdict = f'crashed\nexit {proc.returncode}\n' + '\n'.join(tail)
    return Dump(leaves, verdict, proc.returncode, proc.stderr[-4000:])


# ---------------------------------------------------------------------------
# Old vs new: rename map, diff, probe classification
# ---------------------------------------------------------------------------

class CoverageError(ValueError):
    pass


def load_rename_map(path=RENAME_MAP):
    data = json.loads(Path(path).read_text())
    for key in ('pairs', 'added', 'removed'):
        data.setdefault(key, [])
    return data


def unwrap_option(value, none_equals):
    if value == 'None':
        return none_equals
    match = re.fullmatch(r'Some\((.*)\)', value)
    return match.group(1) if match else value


def unaccounted(old, new, rename):
    old_ok = {p['old'] for p in rename['pairs']} | {r['old'] for r in rename['removed']}
    new_ok = {a['new'] for a in rename['added']}
    problems = [f'unaccounted old-only path {p}' for p in sorted(set(old) - set(new) - old_ok)]
    problems += [f'unaccounted new-only path {p}' for p in sorted(set(new) - set(old) - new_ok)]
    return problems


def diff_leaves(old, new, rename):
    """Rows of kind value / paired / added / removed. Every path present on one
    side only must be declared in rename-map.json, or the run fails (K9)."""
    problems = unaccounted(old, new, rename)
    if problems:
        raise CoverageError('; '.join(problems))
    rows = []
    for path in sorted(set(old) & set(new)):
        if old[path] != new[path]:
            rows.append({'kind': 'value', 'path': path, 'old': old[path], 'new': new[path]})
    for pair in rename['pairs']:
        if pair['old'] not in old:
            continue
        if pair['new'] not in new:
            raise CoverageError(f"paired new path {pair['new']} missing")
        effective = unwrap_option(old[pair['old']], pair['none_equals'])
        rows.append({'kind': 'paired', 'path': f"{pair['old']} ~ {pair['new']}", 'old': old[pair['old']],
                     'new': new[pair['new']],
                     'effective': 'equal-effective' if effective == new[pair['new']] else 'unequal',
                     'commit': pair.get('commit'), 'edge_record': pair.get('edge_record')})
    for added in rename['added']:
        if added['new'] in new and added['new'] not in old:
            rows.append({'kind': 'added', 'path': added['new'], 'old': '<absent>', 'new': new[added['new']],
                         'old_semantics': added.get('old_semantics'), 'commit': added.get('commit'),
                         'edge_record': added.get('edge_record')})
    for removed in rename['removed']:
        if removed['old'] in old and removed['old'] not in new:
            rows.append({'kind': 'removed', 'path': removed['old'], 'old': old[removed['old']], 'new': '<absent>'})
    return rows


def is_process_name(name):
    return name in PROCESS_NAMES or name.startswith(PROCESS_PREFIXES)


def classify(name, old_changed, new_changed, static_old=frozenset(), static_new=frozenset()):
    if is_process_name(name):
        return 'PROCESS'
    if old_changed and new_changed:
        return 'BOTH'
    if old_changed:
        return 'OLD_ONLY'
    if new_changed:
        return 'NEW_ONLY'
    if name in static_old or name in static_new:
        return 'NO_EFFECT_AT_VALUE'
    return 'NEITHER'


def perturb(value):
    return value + '1' if re.fullmatch(r'-?\d+(\.\d+)?', value) else value + 'x'


def probe(fam, binaries, baselines):
    """Per name and side: does removing the name, or perturbing its value,
    change the flattened output or the verdict?"""
    tasks = []
    for name in sorted(fam.env):
        for side in binaries:
            removed = {k: v for k, v in fam.env.items() if k != name}
            perturbed = dict(fam.env, **{name: perturb(fam.env[name])})
            tasks += [(name, side, removed), (name, side, perturbed)]
    changed = {}
    with concurrent.futures.ThreadPoolExecutor(PROBE_WORKERS) as pool:
        futures = {pool.submit(run_dumper, binaries[side], env, fam.argv): (name, side)
                   for name, side, env in tasks}
        for future in concurrent.futures.as_completed(futures):
            name, side = futures[future]
            differs = future.result().signature() != baselines[side].signature()
            changed[(name, side)] = changed.get((name, side), False) or differs
    return {name: (changed[(name, 'old')], changed[(name, 'new')]) for name in fam.env}


# ---------------------------------------------------------------------------
# Static name sets (a cross-check, never the classifier)
# ---------------------------------------------------------------------------

def git(*args, cwd=ROOT):
    return subprocess.check_output(['git', *args], cwd=cwd, text=True).strip()


def show(rev, path):
    return subprocess.check_output(['git', 'show', f'{rev}:{path}'], cwd=ROOT, text=True)


def clap_env_names(text):
    return set(re.findall(r'\benv\s*=\s*"([A-Z][A-Z0-9_]*)"', text))


def args_block(text, anchor_regex):
    """The derive line through the closing brace of the clap struct."""
    lines = text.splitlines()
    start = next(i for i, line in enumerate(lines) if re.match(anchor_regex, line))
    end = next(i for i in range(start, len(lines)) if lines[i] == '}')
    return start + 1, lines[start:end + 1]


def normalize_args(lines):
    """6bcaff69 made the Args fields pub(crate) when it moved the struct out
    of main.rs; that is the only difference E1 tolerates."""
    return [line.replace('pub(crate) ', '') for line in lines]


def env_knobs_list(text):
    match = re.search(r'const ENV_KNOBS: &\[&str\] = &\[(.*?)\];', text, re.S)
    return re.findall(r'"([A-Z][A-Z0-9_]*)"', match.group(1))


def static_names():
    head_cli = (ROOT / 'src/config/cli.rs').read_text()
    head_load = (ROOT / 'src/config/load.rs').read_text()
    new = clap_env_names(head_cli) | set(re.findall(r'"([A-Z][A-Z0-9_]{2,})"', head_load))
    _, old_args = args_block(show(OLD_TAG, 'src/main.rs'), r'#\[derive\(Parser, Debug\)\]')
    old = clap_env_names('\n'.join(old_args)) | set(env_knobs_list(show(OLD_MODEL_REV, 'src/config/mod.rs')))
    return old | {'TOKIO_WORKERS'}, new


# ---------------------------------------------------------------------------
# Build: one worktree + one target directory per revision, artifacts copied
# and hashed immediately (C2)
# ---------------------------------------------------------------------------

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def git_status(*paths, cwd=ROOT):
    # Porcelain lines keep their leading status column (never strip()).
    out = subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=all', '--', *paths],
                                  cwd=cwd, text=True)
    return [line for line in out.splitlines() if line]


def build_env(commit):
    env = dict(os.environ)
    for name in ('RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'CARGO_TARGET_DIR'):
        env.pop(name, None)
    env.update({
        'RUSTUP_TOOLCHAIN': TOOLCHAIN,
        # Non-test dev builds with release arithmetic (§2.3): no cfg(test)
        # fork, no debug assertions, no overflow checks, no debuginfo.
        'CARGO_PROFILE_DEV_OVERFLOW_CHECKS': 'false',
        'CARGO_PROFILE_DEV_DEBUG_ASSERTIONS': 'false',
        'CARGO_PROFILE_DEV_DEBUG': '0',
        'CARGO_INCREMENTAL': '0',
        'STREAMS_GIT_COMMIT': commit,
        'SOURCE_DATE_EPOCH': '0',
    })
    return env


def ensure_worktree(path, rev):
    commit = git('rev-parse', f'{rev}^{{commit}}')
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.check_call(['git', 'worktree', 'add', '--detach', str(path), commit], cwd=ROOT,
                              stdout=subprocess.DEVNULL)
    actual = git('rev-parse', 'HEAD', cwd=path)
    if actual != commit:
        raise SystemExit(f'worktree {path} is at {actual}, expected {rev} = {commit}')
    return commit


def inject_old_model(tree):
    """Two new files plus one appended module line, in a temporary worktree;
    nothing is ever committed there."""
    (tree / 'examples').mkdir(exist_ok=True)
    for source, dest in (('old/effective_config.rs.in', 'examples/effective_config.rs'),
                         ('old/effective_config_dump.rs.in', 'src/effective_config_dump.rs')):
        body = (HERE / source).read_bytes()
        if not (tree / dest).exists() or (tree / dest).read_bytes() != body:
            (tree / dest).write_bytes(body)
    lib = tree / 'src/lib.rs'
    text = lib.read_text()
    line = 'pub mod effective_config_dump;\n'
    if line not in text:
        lib.write_text(text + line)
    status = sorted(git_status(cwd=tree))
    expected = sorted([' M src/lib.rs', '?? examples/effective_config.rs', '?? src/effective_config_dump.rs'])
    if status != expected:
        raise SystemExit(f'old-model tree has unexpected changes: {status}')
    return git('diff', cwd=tree)


BUILDS = {
    # side: (rev (None = this checkout), target dir, cargo target args, artifacts)
    'old-model': (OLD_MODEL_REV, 'target-old-model', ['--example', 'effective_config'], ['examples/effective_config']),
    'head': (None, 'target-head', ['--example', 'effective_config'], ['examples/effective_config']),
    'rc4': (OLD_TAG, 'target-rc4', ['--bin', 'streams-slate'], ['streams-slate']),
    'head-bin': (None, 'target-head', ['--bin', 'streams-slate', '--bin', 's3lite'], ['streams-slate', 's3lite']),
}


def free_gib(path):
    return shutil.disk_usage(path).free / (1 << 30)


def du_gib(path):
    out = subprocess.run(['du', '-sk', str(path)], capture_output=True, text=True).stdout.split()
    return int(out[0]) / (1 << 20) if out else 0.0


def cmd_build(args):
    work, trees = Path(args.work).resolve(), Path(args.trees).resolve()
    work.mkdir(parents=True, exist_ok=True)
    free = free_gib(work)
    if free < DISK_FLOOR_GIB:
        raise SystemExit(f'refusing to build: {free:.0f} GiB free < {DISK_FLOOR_GIB} GiB floor')
    head_commit = git('rev-parse', 'HEAD')
    dirty = git_status('src', 'examples', 'Cargo.toml', 'Cargo.lock', 'build.rs')
    if dirty and not args.allow_dirty:
        raise SystemExit(f'HEAD checkout has uncommitted build inputs: {dirty}')
    record_path = work / 'build.json'
    record = json.loads(record_path.read_text()) if record_path.exists() else {}
    record['toolchain'] = subprocess.check_output(['rustc', f'+{TOOLCHAIN}', '--version'], text=True).strip()
    record['head_tree'] = {'commit': head_commit, 'dirty': dirty}
    sides = ['old-model', 'head'] + (['rc4', 'head-bin'] if args.binaries else [])
    print(f'free before {free:.0f} GiB (floor {DISK_FLOOR_GIB} GiB)')
    for side in sides:
        rev, target_name, targets, artifacts = BUILDS[side]
        started = time.monotonic()
        if rev is None:
            tree, commit, injected = ROOT, head_commit, None
        else:
            tree = trees / side
            commit = ensure_worktree(tree, rev)
            injected = inject_old_model(tree) if side == 'old-model' else None
            if side == 'rc4' and git_status(cwd=tree):
                raise SystemExit('rc.4 worktree is not clean; the rc.4 binary must be unmodified source')
        target = work / target_name
        subprocess.check_call(['cargo', 'build', '--locked', '-p', 'streams-slate', *targets],
                              cwd=tree, env=dict(build_env(commit), CARGO_TARGET_DIR=str(target)))
        entry = {'rev': rev or 'HEAD', 'commit': commit, 'tree': str(tree), 'target': str(target),
                 'seconds': round(time.monotonic() - started, 1), 'artifacts': {}}
        if injected is not None:
            entry['injected_lib_diff'] = injected
        for built in artifacts:
            name = Path(built).name
            copy = work / 'bin' / side / name
            copy.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(target / 'debug' / built, copy)
            entry['artifacts'][name] = {'path': str(copy), 'sha256': sha256_file(copy)}
            print(f'{side} {commit[:8]} {built} sha256={entry["artifacts"][name]["sha256"]}')
        record[side] = entry
        record_path.write_text(json.dumps(record, indent=2) + '\n')
        print(f'{side}: {entry["seconds"]:.0f}s')
    print(f'free after {free_gib(work):.0f} GiB; work dir {du_gib(work):.1f} GiB')


def artifact(work, side, name):
    """The hashed copy; refuses if it changed since its build was recorded."""
    record = json.loads((Path(work) / 'build.json').read_text())
    entry = record[side]['artifacts'][name]
    path = Path(entry['path'])
    if sha256_file(path) != entry['sha256']:
        raise SystemExit(f'{path}: sha256 changed since it was built and recorded')
    return path


def dumpers(work):
    return {'old': artifact(work, 'old-model', 'effective_config'), 'new': artifact(work, 'head', 'effective_config')}


def cmd_clean(args):
    trees = Path(args.trees).resolve()
    for side in ('old-model', 'rc4'):
        tree = trees / side
        if tree.exists():
            subprocess.check_call(['git', 'worktree', 'remove', '--force', str(tree)], cwd=ROOT)
            print(f'removed worktree {tree}')
    subprocess.check_call(['git', 'worktree', 'prune'], cwd=ROOT)
    work = Path(args.work).resolve()
    for child in sorted(work.glob('target-*')):
        shutil.rmtree(child)
        print(f'removed {child}')
    if work.exists() and not args.keep:
        shutil.rmtree(work)
        print(f'removed {work}')


# ---------------------------------------------------------------------------
# compare: dump, flatten, rename, probe, diff (per family)
# ---------------------------------------------------------------------------

def compare_family(fam, bins, rename, statics):
    baselines = {side: run_dumper(bins[side], fam.env, fam.argv) for side in ('old', 'new')}
    rows = diff_leaves(baselines['old'].leaves, baselines['new'].leaves, rename)
    probed = probe(fam, bins, baselines)
    names = []
    for name in sorted(fam.env):
        old_changed, new_changed = probed[name]
        cls = classify(name, old_changed, new_changed, statics[0], statics[1])
        names.append({'name': name, 'provenance': fam.provenance[name], 'class': cls,
                      'old_changed': old_changed, 'new_changed': new_changed,
                      'static_old': name in statics[0], 'static_new': name in statics[1]})
    return {
        'family': fam.name, 'family_sha256': fam.sha256, 'sources': fam.sources, 'notes': fam.notes,
        'argv': fam.argv, 'env': {k: {'value': v, 'provenance': fam.provenance[k]} for k, v in fam.env.items()},
        'overridden': fam.overridden, 'omitted': fam.omitted,
        'old': {'verdict': baselines['old'].verdict, 'leaves': len(baselines['old'].leaves)},
        'new': {'verdict': baselines['new'].verdict, 'leaves': len(baselines['new'].leaves)},
        'old_leaves': baselines['old'].leaves, 'new_leaves': baselines['new'].leaves,
        'rows': rows, 'names': names,
    }


def count_classes(names):
    counts = {}
    for n in names:
        counts[n['class']] = counts.get(n['class'], 0) + 1
    return counts


def cmd_compare(args):
    families = load_families(Path(args.families), args.only)   # K7: refused before anything runs
    out = Path(args.out)
    (out / 'families').mkdir(parents=True, exist_ok=True)
    bins, rename, statics = dumpers(args.work), load_rename_map(), static_names()
    lines = []
    for fam in families:
        started = time.monotonic()
        result = compare_family(fam, bins, rename, statics)
        (out / 'families' / f'{fam.name}.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
        counts = count_classes(result['names'])
        value_rows = sum(1 for r in result['rows'] if r['kind'] != 'paired' or r['effective'] != 'equal-effective')
        line = (f"{fam.name}: new={result['new']['verdict'].split(chr(10))[0]} value-rows={value_rows} "
                f"OLD_ONLY={counts.get('OLD_ONLY', 0)} NEW_ONLY={counts.get('NEW_ONLY', 0)} "
                f"NO_EFFECT_AT_VALUE={counts.get('NO_EFFECT_AT_VALUE', 0)} NEITHER={counts.get('NEITHER', 0)} "
                f"({time.monotonic() - started:.0f}s)")
        lines.append(line)
        print(line, flush=True)
    (out / 'compare.txt').write_text('\n'.join(lines) + '\n')


# ---------------------------------------------------------------------------
# controls K1-K9, K13 (K10-K12 are Leg C, boot.py)
# ---------------------------------------------------------------------------

def row_text(rows):
    return '[' + ', '.join(f"{r['path']}: {r['old']} -> {r['new']}" for r in rows) + ']'


def same_side_rows(a, b):
    return [{'path': p, 'old': a.leaves.get(p, '<absent>'), 'new': b.leaves.get(p, '<absent>')}
            for p in sorted(set(a.leaves) | set(b.leaves)) if a.leaves.get(p) != b.leaves.get(p)]


def control_k7(work, family_text, expected):
    with tempfile.TemporaryDirectory() as tmp:
        (Path(tmp) / 'k7.family').write_text(family_text)
        out = Path(tmp) / 'out'
        proc = subprocess.run([sys.executable, str(Path(__file__)), 'compare', '--work', str(work), '--out',
                               str(out), '--families', tmp], capture_output=True, text=True)
        ran = (out / 'families').exists()
    return proc.returncode == 2 and proc.stderr.strip() == expected and not ran, proc


def cmd_controls(args):
    work, bins, rename = args.work, dumpers(args.work), load_rename_map()
    statics = static_names()
    defaults = load_family(Path(args.families) / 'defaults.family')
    fleet1 = load_family(Path(args.families) / 'fleet-server-1.family')
    results = []

    def record(ok, text):
        results.append(f"{'PASS' if ok else 'FAIL'} {text}")
        print(results[-1].split(' ', 1)[0], text, flush=True)

    base = {side: run_dumper(bins[side], defaults.env, defaults.argv) for side in bins}
    a, b = run_dumper(bins['new'], fleet1.env, fleet1.argv), run_dumper(bins['new'], fleet1.env, fleet1.argv)
    record(a.signature() == b.signature() and a.leaves,
           f'K1 identity(new): fleet-server-1 vs fleet-server-1: {len(same_side_rows(a, b))} differences')
    for side in ('old', 'new'):
        bumped = run_dumper(bins[side], dict(defaults.env, SHARD_OPEN_WAIT_MS='10001'), defaults.argv)
        rows = same_side_rows(base[side], bumped)
        record(row_text(rows) == '[shard.open_wait_ms: 10000 -> 10001]',
               f'K2 sensitivity({side}): defaults vs defaults+SHARD_OPEN_WAIT_MS=10001: exactly {row_text(rows)}')
    for side in ('old', 'new'):
        argv_run = run_dumper(bins[side], defaults.env, [*defaults.argv, '--flush-interval-ms', '99'])
        rows = same_side_rows(base[side], argv_run)
        record(row_text(rows) == '[cli.flush_interval_ms: 25 -> 99]',
               f'K3 argv({side}): --flush-interval-ms 99: exactly {row_text(rows)}')
    saved = os.environ.get('FLEET_MIN')
    os.environ['FLEET_MIN'] = '9'
    try:
        ambient = {side: run_dumper(bins[side], defaults.env, defaults.argv) for side in bins}
    finally:
        if saved is None:
            os.environ.pop('FLEET_MIN')
        else:
            os.environ['FLEET_MIN'] = saved
    values = {side: ambient[side].leaves.get('fleet.fleet_min') for side in ambient}
    record(values == {'old': '1', 'new': '1'},
           f"K4 isolation: ambient FLEET_MIN=9 not observed (fleet.fleet_min = {values['old']} old, {values['new']} new)")
    budget, gathers = (base['old'].leaves.get(k) for k in ('history.absorb_global_budget_bytes',
                                                          'history.absorb_global_gathers'))
    record((budget, gathers) == ('67108864', '2'),
           f'K5 release-flavour pin(old): history.absorb_global_budget_bytes = {budget}, '
           f'history.absorb_global_gathers = {gathers} (a pin of the shipped non-test defaults; '
           'cfg(test) is unreachable from an example build, C7)')
    refused = run_dumper(bins['new'], dict(defaults.env, SSE_H1_MAX_BUF='4096'), defaults.argv)
    expected = ('refused\nconfiguration invalid (1 problem(s)):\n'
                "  - SSE_H1_MAX_BUF=4096 is below hyper's 8192-byte h1 buffer floor")
    record(refused.verdict == expected, 'K6 refusal(new): defaults+SSE_H1_MAX_BUF=4096 -> '
           + refused.verdict.replace('\n', ' / '))
    for text, want, label in [
        ('env AUTH_TOKEN=not-a-placeholder\n', 'family k7: AUTH_TOKEN must be a placeholder', 'AUTH_TOKEN=not-a-placeholder'),
        ('env SLATE_S3_ENDPOINT=https://user:pw@host.invalid\n',
         'family k7: SLATE_S3_ENDPOINT: value carries URL userinfo (a credential); use a placeholder host',
         'URL userinfo'),
        ('env PLATFORM_API_KEY=abc\n', 'family k7: PLATFORM_API_KEY must be a placeholder', 'unlisted *_KEY name'),
    ]:
        ok, proc = control_k7(work, text, want)
        record(ok, f'K7 secret policy: family with {label} refused before build/run '
               f'(exit {proc.returncode}: {proc.stderr.strip()})')
    k8 = derive(defaults, 'k8', env={'SHARD_OPEN_WAIT_MS': '10001', 'SSE_H1_HEADER_TIMEOUT_MS': '5000',
                                     'RUST_LOG': 'info'})
    k8.set('KEEP_AWAKE', '1', 'supervisor')
    k8_base = {side: run_dumper(bins[side], k8.env, k8.argv) for side in bins}
    probed = probe(k8, bins, k8_base)
    got = {n: classify(n, *probed[n], *statics) for n in probed}
    shown = ' '.join(f"{n}={got[n]}" + ('(supervisor)' if got[n] == 'NEITHER' else '')
                     for n in ('SHARD_OPEN_WAIT_MS', 'SSE_H1_HEADER_TIMEOUT_MS', 'KEEP_AWAKE', 'RUST_LOG'))
    record(shown == 'SHARD_OPEN_WAIT_MS=BOTH SSE_H1_HEADER_TIMEOUT_MS=NEW_ONLY KEEP_AWAKE=NEITHER(supervisor) '
           'RUST_LOG=PROCESS', f'K8 probe: {shown}')
    old, new = base['old'].leaves, base['new'].leaves
    only_old, only_new = sorted(set(old) - set(new)), sorted(set(new) - set(old))
    declared = not unaccounted(old, new, rename)
    record(len(old) == PIN_OLD_LEAVES and len(new) == PIN_NEW_LEAVES and declared,
           f"K9 coverage: old {len(old)} leaves, new {len(new)} leaves; unmatched old {{{', '.join(only_old)}}}, "
           f"new {{{', '.join(only_new)}}}; {'all declared in rename-map.json' if declared else 'UNDECLARED'}")
    record_json = json.loads((Path(work) / 'build.json').read_text())
    shas = {side: {k: v['sha256'] for k, v in record_json[side]['artifacts'].items()} for side in record_json
            if isinstance(record_json[side], dict) and 'artifacts' in record_json[side]}
    dumper_distinct = shas['old-model']['effective_config'] != shas['head']['effective_config']
    server_distinct = ('rc4' not in shas) or shas['rc4']['streams-slate'] != shas['head-bin']['streams-slate']
    marker = 'billing.mode_env' in old and 'billing.mode_env' not in new
    record(dumper_distinct and server_distinct and marker,
           'K13 artifact identity: old-model dumper sha != new dumper sha; rc.4 streams-slate sha != head '
           'streams-slate sha; old output contains billing.mode_env (new does not)')
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / 'controls.txt').write_text('\n'.join(results) + '\n')
    return 0 if all(r.startswith('PASS') for r in results) else 1


# ---------------------------------------------------------------------------
# equivalence: E1 (Args), E2 (env names), E3 (transcription table)
# ---------------------------------------------------------------------------

RC4_EXCLUDE = ('src/bin/', 'src/dst/', 'src/dst.rs')
KNOWN_HELPERS = {('src/scaler3.rs', 'envf'), ('src/backpressure.rs', 'v'), ('src/usage.rs', 'envf'),
                 ('src/main.rs', 'env_usize'), ('src/main.rs', 'env_u64'), ('src/main.rs', 'genv')}
LITERAL_READ = re.compile(r'env::var(?:_os)?\(\s*"([A-Z][A-Z0-9_]*)"\s*,?\s*\)')
GENERIC_READ = re.compile(r'env::var(?:_os)?\(\s*([a-z_][a-z0-9_]*)\s*\)')


def enclosing_helper(lines, index):
    for back in range(index, max(index - 4, -1), -1):
        match = re.search(r'\bfn (\w+)\s*\(|\blet (\w+) = \|', lines[back])
        if match:
            return match.group(1) or match.group(2)
    return None


def env_reads(files, known=KNOWN_HELPERS):
    """{name: [(path, line)]} for literal reads and known generic helpers'
    call sites; an env::var(<variable>) outside a known helper fails."""
    names, helpers = {}, set()
    for path, text in sorted(files.items()):
        lines = text.splitlines()
        offsets = [0]
        for line in lines:
            offsets.append(offsets[-1] + len(line) + 1)

        def line_of(pos):
            return next(i for i in range(len(offsets)) if offsets[i + 1] > pos)
        for match in LITERAL_READ.finditer(text):
            names.setdefault(match.group(1), []).append((path, line_of(match.start()) + 1))
        for match in GENERIC_READ.finditer(text):
            helper = enclosing_helper(lines, line_of(match.start()))
            if (path, helper) not in known:
                raise ValueError(f'unresolved generic env helper at {path}:{line_of(match.start()) + 1}')
            helpers.add((path, helper))
        for fpath, helper in known:
            if fpath != path:
                continue
            for match in re.finditer(rf'(?<![\w.]){helper}\(\s*"([A-Z][A-Z0-9_]*)"', text):
                names.setdefault(match.group(1), []).append((path, line_of(match.start()) + 1))
    return names, helpers


def rc4_sources():
    listed = git('ls-tree', '-r', '--name-only', OLD_TAG, 'src/').splitlines()
    return {p: show(OLD_TAG, p) for p in listed if p.endswith('.rs') and not p.startswith(RC4_EXCLUDE)}


def cfg_test_only(files, sites):
    for path, line in sites:
        lines = files[path].splitlines()
        if not any('#[cfg(test)]' in lines[i] for i in range(max(line - 4, 0), line - 1)):
            return False
    return True


def strip_strings(text):
    return re.sub(r'"(?:[^"\\]|\\.)*"', lambda m: '""' + '\n' * m.group(0).count('\n'), text, flags=re.S)


def statement(lines, index, limit=14):
    """From a site line: the enclosing `if` block or `;`-terminated statement
    (starting one line earlier when the site continues an assignment)."""
    if index > 0 and lines[index - 1].rstrip().endswith('='):
        index -= 1
    depth, taken = 0, []
    for line in lines[index:index + limit]:
        taken.append(line.strip())
        bare = strip_strings(line)
        depth += bare.count('{') + bare.count('(') - bare.count('}') - bare.count(')')
        if depth <= 0 and (line.rstrip().endswith((';', '}')) or (depth < 0)):
            break
    return ' '.join(taken)


def eval_literal(expr):
    expr = re.sub(r'(?<=\d)_(?=\d)', '', expr.strip())
    expr = re.sub(r'(?<=\d)(u64|u32|usize|i64|f64|u16|u8)\b', '', expr)
    try:
        tree = ast.parse(expr, mode='eval')
    except SyntaxError:
        return None

    def ev(node):
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return node.value
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            return -ev(node.operand)
        if isinstance(node, ast.BinOp):
            left, right = ev(node.left), ev(node.right)
            ops = {ast.Add: lambda: left + right, ast.Sub: lambda: left - right, ast.Mult: lambda: left * right,
                   ast.LShift: lambda: left << right,
                   ast.Div: lambda: left // right if isinstance(left, int) and isinstance(right, int) else left / right}
            if type(node.op) in ops:
                return ops[type(node.op)]()
        raise ValueError(node)
    try:
        return ev(tree.body)
    except (ValueError, TypeError, ZeroDivisionError):
        return None


def balanced_arg(text, start):
    depth, i = 0, start
    while i < len(text):
        ch = text[i]
        if ch in '([{':
            depth += 1
        elif ch in ')]}':
            if depth == 0:
                return text[start:i]
            depth -= 1
        elif ch == ',' and depth == 0:
            return text[start:i]
        i += 1
    return text[start:]


def rc4_default(snippet, name):
    match = re.search(rf'\w+\(\s*"{name}"\s*,\s*', snippet)
    if match and 'env::var' not in snippet[:match.start() + 12]:
        arg = balanced_arg(snippet, match.end())
        after = snippet[match.end() + len(arg):]
        # A helper default scaled at the call site (envf("X", 75.0) / 100.0).
        tail = re.match(r'\s*,?\s*\)\s*([*/])\s*([\d._]+)', after)
        return f'({arg.strip()}) {tail.group(1)} {tail.group(2)}' if tail else arg.strip()
    match = re.search(r'\.unwrap_or\(', snippet) or re.search(r'\.unwrap_or_else\(\|\|\s*', snippet)
    return balanced_arg(snippet, match.end()).strip() if match else None


def number(value):
    """A Debug scalar as a number: plain numbers, `Some(x)`, and Durations in
    seconds (`180s`, `250ms`)."""
    if value is None:
        return None
    match = re.fullmatch(r'Some\((.*)\)', value)
    value = match.group(1) if match else value
    match = re.fullmatch(r'(\d+(?:\.\d+)?)(ms|s)', value)
    if match:
        return float(match.group(1)) / (1000 if match.group(2) == 'ms' else 1)
    try:
        return float(value)
    except ValueError:
        return None


def resolve_constant(literal, files):
    """`crate::NAME` / `NAME` -> the expression of `const NAME: T = EXPR;` at rc.4."""
    name = literal.split('::')[-1]
    if not re.fullmatch(r'[A-Z][A-Z0-9_]*', name):
        return literal
    for text in files.values():
        match = re.search(rf'const {name}: [^=]+= ([^;]+);', text)
        if match:
            return match.group(1).strip()
    return literal


def compare_default(literal, executed, field, files):
    """'equal' / 'DIFFERENT' when both sides reduce to a number or a bool, else 'review'."""
    if literal is None or len(executed) != 1:
        return 'review'
    literal = resolve_constant(literal, files)
    if literal in ('true', 'false'):
        # A predicate's fallback (unwrap_or(false)) is comparable only with a
        # bool field, not with the raw Option<String> an overlay may keep.
        if executed[0] not in ('true', 'false'):
            return 'review'
        return 'equal' if executed[0] == literal else 'DIFFERENT'
    duration = re.fullmatch(r'Duration::from_(secs|millis)\((.*)\)', literal)
    value = eval_literal(duration.group(2) if duration else literal)
    expected = number(executed[0])
    if value is None or expected is None:
        return 'review'
    if duration:
        # Compare in seconds: a Debug Duration already is; a plain integer
        # field says its unit in its name.
        value = value / 1000 if duration.group(1) == 'millis' else value
        if not re.search(r'\d(ms|s)\)?$', executed[0]):
            if field.endswith('_ms'):
                expected /= 1000
            elif not field.endswith('_secs'):
                return 'review'
    return 'equal' if float(value) == expected else 'DIFFERENT'


def e3_rows(files, rc4_names, old_defaults):
    mod = show(OLD_MODEL_REV, 'src/config/mod.rs')
    lines, bare = mod.splitlines(), strip_strings(mod).splitlines()
    start = next(i for i, line in enumerate(lines) if 'fn overlay_env(' in line)
    depth, end = 0, start
    for end in range(start, len(bare)):
        depth += bare[end].count('{') - bare[end].count('}')
        if depth == 0 and end > start:
            break
    rows = []
    for knob in env_knobs_list(mod):
        old_sites = [i for i in range(start, end + 1) if f'"{knob}"' in lines[i]]
        old_text = ' || '.join(statement(lines, i) for i in old_sites)
        fields = sorted(set(re.findall(r'self\.([a-z_]+\.[a-z_0-9]+)\s*=', old_text)))
        if not fields and old_sites:
            # A `let` read inside a `{ ... }` block assigns later in that block.
            window, depth = [], 0
            for i in range(old_sites[0], end):
                depth += bare[i].count('{') - bare[i].count('}')
                if depth < 0:
                    break
                window.append(lines[i])
            fields = sorted(set(re.findall(r'self\.([a-z_]+\.[a-z_0-9]+)\s*=', '\n'.join(window))))
        rc4_sites = rc4_names.get(knob, [])
        rc4_text = ' || '.join(statement(files[p].splitlines(), n - 1) for p, n in rc4_sites)
        literal = rc4_default(rc4_text, knob) if rc4_sites else None
        evaluated = eval_literal(resolve_constant(literal, files)) if literal else None
        executed = [old_defaults.get(f, '<not a leaf>') for f in fields]
        auto = compare_default(literal, executed, fields[0] if fields else '', files)
        rows.append({'knob': knob, 'rc4_sites': [f'{p}:{n}' for p, n in rc4_sites], 'rc4_read': rc4_text,
                     'old_sites': [f'src/config/mod.rs:{i + 1}' for i in old_sites], 'old_overlay': old_text,
                     'fields': fields, 'old_model_default': executed, 'rc4_default_literal': literal,
                     'rc4_default_evaluated': evaluated, 'auto': auto})
    return rows


def md_cell(text):
    return str(text).replace('|', '\\|').replace('\n', ' ')


def cmd_equivalence(args):
    out = Path(args.out) / 'equivalence'
    out.mkdir(parents=True, exist_ok=True)
    ok = True
    rc4_start, rc4_args = args_block(show(OLD_TAG, 'src/main.rs'), r'#\[derive\(Parser, Debug\)\]')
    old_start, old_args = args_block(show(OLD_MODEL_REV, 'src/bootstrap.rs'), r'#\[derive\(Parser, Debug\)\]')
    normalized = normalize_args(old_args)
    same = normalize_args(rc4_args) == normalized and len(rc4_args) == PIN_ARGS_LINES
    ok &= same
    delta = list(difflib.unified_diff(rc4_args, normalized, 'rc.4', '6bcaff69', lineterm=''))
    line = (f"E1 {'PASS' if same else 'FAIL'}: Args identical ({len(rc4_args)} lines): {OLD_TAG} src/main.rs:{rc4_start}"
            f" == {OLD_MODEL_REV} src/bootstrap.rs:{old_start} (pub(crate) normalized; the block starts at the derive line)")
    (out / 'e1.txt').write_text(line + '\n' + '\n'.join(delta) + '\n')
    print(line)
    files = rc4_sources()
    names, helpers = env_reads(files)
    knobs = set(env_knobs_list(show(OLD_MODEL_REV, 'src/config/mod.rs')))
    tokio_rc4 = 'let workers: usize = std::env::var("TOKIO_WORKERS")\n        .ok()\n        .and_then(|v| v.parse().ok())'
    anchor_old = tokio_rc4 in show(OLD_MODEL_REV, 'src/main.rs')
    anchor_rc4 = tokio_rc4 in files['src/main.rs']
    dst_only = cfg_test_only(files, names.get('DST_DRAIN_TRACE', []))
    expected = knobs | {'TOKIO_WORKERS', 'DST_DRAIN_TRACE'}
    e2 = (set(names) == expected and len(names) == PIN_RC4_NAMES and len(knobs) == PIN_ENV_KNOBS
          and len(helpers) == PIN_HELPERS and anchor_old and anchor_rc4 and dst_only)
    ok &= e2
    sites = sum(len(v) for v in names.values())
    line = (f"E2 {'PASS' if e2 else 'FAIL'}: rc.4 production env names ({len(names)}) == {OLD_MODEL_REV} ENV_KNOBS "
            f"({len(knobs)}) + {{TOKIO_WORKERS: transcribed, anchors {OLD_MODEL_REV} src/main.rs "
            f"{'ok' if anchor_old else 'MISSING'} + {OLD_TAG} src/main.rs {'ok' if anchor_rc4 else 'MISSING'}; "
            f"DST_DRAIN_TRACE: {'#[cfg(test)] only' if dst_only else 'NOT test-only'}}}; "
            f"generic helpers resolved: {len(helpers)}/{PIN_HELPERS}; {sites} read sites")
    detail = [line, f'only rc.4: {sorted(set(names) - expected)}', f'only 6bcaff69: {sorted(expected - set(names))}',
              '', 'name -> rc.4 read sites']
    detail += [f'{n}: {", ".join(f"{p}:{ln}" for p, ln in s)}' for n, s in sorted(names.items())]
    (out / 'e2.txt').write_text('\n'.join(detail) + '\n')
    print(line)
    defaults = load_family(Path(args.families) / 'defaults.family')
    old_defaults = run_dumper(dumpers(args.work)['old'], defaults.env, defaults.argv).leaves
    rows = e3_rows(files, names, old_defaults)
    equal = sum(1 for r in rows if r['auto'] == 'equal')
    table = ['# E3: rc.4 read expression vs 6bcaff69 overlay, per knob', '',
             f'Generated by `effective_config.py equivalence`. {len(rows)} knobs: {equal} defaults equal by '
             f'evaluation (rc.4 literal arithmetic vs the old-model dumper\'s executed default), '
             f'{len(rows) - equal} for review. One owner decision covers the table (R5).', '',
             '| knob | rc.4 site | rc.4 read | 6bcaff69 overlay | field | old-model default | rc.4 literal | auto |',
             '| --- | --- | --- | --- | --- | --- | --- | --- |']
    for r in rows:
        table.append('| ' + ' | '.join(md_cell(x) for x in (
            r['knob'], ', '.join(r['rc4_sites']), f"`{r['rc4_read']}`", f"`{r['old_overlay']}`",
            ', '.join(r['fields']), ', '.join(r['old_model_default']),
            '' if r['rc4_default_literal'] is None else f"`{r['rc4_default_literal']}`", r['auto'])) + ' |')
    (out / 'e3-transcription.md').write_text('\n'.join(table) + '\n')
    (out / 'e3.json').write_text(json.dumps(rows, indent=2) + '\n')
    different = [r['knob'] for r in rows if r['auto'] == 'DIFFERENT']
    print(f'E3 wrote equivalence/e3-transcription.md: {len(rows)} knobs ({equal} defaults equal by evaluation, '
          f'{len(rows) - equal} for review{"; DIFFERENT: " + ", ".join(different) if different else ""})')
    return 0 if ok else 1


# ---------------------------------------------------------------------------
# drift and check-families
# ---------------------------------------------------------------------------

ENV_FLAG = re.compile(r'--env\s+["\']?([A-Z_][A-Z0-9_]*)=')


def sh_env_names(text):
    return set(ENV_FLAG.findall(text))


def mjs_block_names(text, anchor):
    lines = text.splitlines()
    start = next((i for i, line in enumerate(lines) if anchor in line), None)
    if start is None:
        return None, set()
    names, deleted = set(), set()
    for line in lines[start + 1:]:
        if re.fullmatch(r'\s*\}\)?;\s*', line):
            break
        match = re.match(r'\s*([A-Z_][A-Z0-9_]*):', line)
        if match:
            names.add(match.group(1))
    for line in lines[start + 1:start + 60]:
        match = re.match(r'\s*delete env\.([A-Z_][A-Z0-9_]*);', line)
        if match:
            deleted.add(match.group(1))
        if 'return env;' in line:
            break
    return names, deleted


def script_names(script, anchor, text):
    if script.endswith('.mjs'):
        return mjs_block_names(text, anchor)
    return sh_env_names(text), set()


def check_family(fam, root=ROOT):
    problems = []
    for script, anchor in fam.sources:
        path = root / script
        if not path.exists():
            problems.append(f'{fam.name}: source {script} does not exist')
            continue
        text = path.read_text()
        if anchor not in text:
            problems.append(f'{fam.name}: anchor {anchor!r} not found in {script}')
            continue
        names, deleted = script_names(script, anchor, text)
        missing = sorted(names - set(fam.env) - set(fam.omitted))
        if missing:
            problems.append(f'{fam.name}: {script} sets {missing} but the family neither sets nor omits them')
        stale = sorted(set(fam.omitted) - names)
        if stale:
            problems.append(f'{fam.name}: omits {stale} which {script} never sets')
        both = sorted(set(fam.omitted) & set(fam.env))
        if both:
            problems.append(f'{fam.name}: {both} both set and omitted')
        leaked = sorted(deleted & set(fam.env))
        if leaked:
            problems.append(f'{fam.name}: {script} deletes {leaked} but the family sets them')
        if PROFILE in text and PROFILE not in fam.includes:
            problems.append(f'{fam.name}: {script} sources {PROFILE} but the family does not include it')
        own = {n for n, p in fam.provenance.items() if p in ROLES or p == 'supervisor'}
        unsourced = sorted(own - names - {'APP_BINARY_SHA256'})
        if unsourced:
            problems.append(f'{fam.name}: sets {unsourced} which {script} never sets')
    return problems


def cmd_check_families(args):
    families = load_families(Path(args.families))
    problems = [p for fam in families for p in check_family(fam)]
    for p in problems:
        print(p)
    if problems:
        return 1
    print(f'check-families OK: {len(families)} families')
    return 0


def cmd_drift(args):
    lines = ['# Deploy-script drift: rc.4-era vs HEAD-era --env names (R6)', '']
    for script in COMPUTE_SCRIPTS:
        try:
            old = sh_env_names(show(OLD_TAG, script))
        except subprocess.CalledProcessError:
            lines.append(f'{script}: absent at {OLD_TAG}')
            continue
        new = sh_env_names((ROOT / script).read_text())
        lines.append(f'{script}: rc.4 {len(old)} names, HEAD {len(new)}; removed {sorted(old - new)}; '
                     f'added {sorted(new - old)}')
    old_profile = dict(read_env_file(show(OLD_TAG, PROFILE)))
    new_profile = dict(read_env_file((ROOT / PROFILE).read_text()))
    changed = {k: (old_profile.get(k, '<absent>'), new_profile.get(k, '<absent>'))
               for k in sorted(set(old_profile) | set(new_profile)) if old_profile.get(k) != new_profile.get(k)}
    lines.append(f'{PROFILE}: {len(old_profile)} -> {len(new_profile)} knob lines; changed {changed or "none"}')
    grep_old = subprocess.run(['git', 'grep', '-l', 'ABSORB_PASS_BYTES', OLD_TAG, '--', '*.sh', '*.mjs', '*.js',
                               '*.ts', '*.env', '*.py'], cwd=ROOT, capture_output=True, text=True).stdout
    grep_new = subprocess.run(['git', 'grep', '-l', 'ABSORB_PASS_BYTES', '--', '*.sh', '*.mjs', '*.js', '*.ts',
                               '*.env', '*.py'], cwd=ROOT, capture_output=True, text=True).stdout
    old_files = {line.split(':', 1)[1] for line in grep_old.splitlines()}
    new_files = set(grep_new.splitlines())
    removed = sorted(old_files - new_files)
    compute = [f for f in removed if f in COMPUTE_SCRIPTS]
    lines.append(f'ABSORB_PASS_BYTES removed: {len(removed)} scripts ({len(compute)} Compute): {removed}')
    lines.append(f'ABSORB_PASS_BYTES still set at HEAD in: {sorted(new_files) or "none"}')
    text = '\n'.join(lines) + '\n'
    if args.out:
        Path(args.out).mkdir(parents=True, exist_ok=True)
        (Path(args.out) / 'drift.txt').write_text(text)
    print(text, end='')


# ---------------------------------------------------------------------------
# redact: a platform env export (owner decision D2) -> a placeholder family,
# in memory; only the redacted text is ever written (C8)
# ---------------------------------------------------------------------------

def redact_value(name, value):
    value = USERINFO.sub('://', value)
    if not is_secret_name(name):
        return value
    try:
        if len(base64.b64decode(value, validate=True)) == 32:
            return 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA='
    except ValueError:
        pass
    return f'placeholder-{name.lower().replace("_", "-")}-redacted'


def redact_export(text, family, note):
    lines = [f'# Platform env export for {family}, redacted in memory by effective_config.py redact.',
             f'note {note}', 'argv --listen 0.0.0.0:8080', 'role server']
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        line = line.removeprefix('export ')
        name, sep, value = line.partition('=')
        if not sep or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', name):
            lines.append(f'# skipped an unparsable line of {len(line)} characters')
            continue
        value = value.strip().strip('"\'')
        if not value:
            lines.append(f'# {name}: empty in the export')
            continue
        redacted = redact_value(name, value)
        check_value(name, redacted)
        lines.append(f'env {name}={redacted}')
    return '\n'.join(lines) + '\n'


def cmd_redact(args):
    text = redact_export(sys.stdin.read(), args.family, args.note)
    parse_family(args.family, text)   # the written file must load
    path = Path(args.families) / f'{args.family}.family'
    path.write_text(text)
    print(f'wrote {path} ({text.count(chr(10) + "env ")} variables, secrets replaced by placeholders)')


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parser():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest='command', required=True)

    def add(name, help_text, work=True, out=True, families=True):
        s = sub.add_parser(name, help=help_text)
        if work:
            s.add_argument('--work', required=True, help='build outputs and hashed artifact copies')
        if out:
            s.add_argument('--out', required=name != 'drift', help='evidence directory')
        if families:
            s.add_argument('--families', default=str(FAMILIES_DIR))
        return s

    b = add('build', 'worktrees, injection, one target dir per revision, hashed copies', out=False, families=False)
    b.add_argument('--trees', required=True, help='parent dir for the temporary worktrees (outside the repo)')
    b.add_argument('--binaries', action='store_true', help='also build the rc.4/HEAD servers and s3lite (Leg C)')
    b.add_argument('--allow-dirty', action='store_true')
    c = add('clean', 'remove the temporary worktrees and build directories', out=False, families=False)
    c.add_argument('--trees', required=True)
    c.add_argument('--keep', action='store_true', help='keep the work dir, drop only target-*')
    add('equivalence', 'E1, E2 (mechanical) and the E3 transcription table')
    cmp_ = add('compare', 'dump, flatten, rename, probe and diff every family')
    cmp_.add_argument('--only', nargs='*')
    add('controls', 'K1-K9 and K13')
    add('drift', 'rc.4-era vs HEAD-era --env names per deploy script', work=False, families=False)
    add('check-families', 'every script --env name is set or omitted by its family', work=False, out=False)
    add('report', 'report.md and annotations.template.json from the evidence directory')
    r = add('redact', 'stdin KEY=VALUE platform export -> families/<family>.family, secrets as placeholders',
            work=False, out=False)
    r.add_argument('--family', required=True)
    r.add_argument('--note', required=True, help='where the export came from (project, date, who)')
    return p


def cmd_report(args):
    import report
    return report.cmd_report(args)


COMMANDS = {'build': cmd_build, 'clean': cmd_clean, 'equivalence': cmd_equivalence, 'compare': cmd_compare,
            'controls': cmd_controls, 'drift': cmd_drift, 'check-families': cmd_check_families,
            'report': cmd_report, 'redact': cmd_redact}


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        return COMMANDS[args.command.replace('_', '-')](args) or 0
    except FamilyError as error:
        print(error, file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
